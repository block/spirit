package move

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/move/check"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
)

// renameRetryWait is the pause between RENAME TABLE attempts while the
// source table locks are still held. A var (not a const) so tests can
// shorten it.
var renameRetryWait = 1 * time.Second

// errRenameRollbackFailed marks a rename failure whose rollback also failed,
// leaving the sources in a partially-renamed state. Retrying the rename in
// that state cannot converge, so the retry loop aborts immediately.
var errRenameRollbackFailed = errors.New("rename rollback failed")

// errCutoverRefused marks a cutover attempt refused by a check that ran under
// the source table locks. A retry would be refused for the same reason, so it
// is not retried.
var errCutoverRefused = errors.New("cutover refused")

// CutoverResult reports authoritative evidence from a caller-owned cutover
// callback, including failures after a durable mutation and failures whose
// ownership outcome cannot be determined.
type CutoverResult struct {
	DurableMutation    bool
	OwnershipAmbiguous bool
}

// CutoverResultCallback is the result-bearing cutover callback form.
//
// A cutover callback (this form or the plain one passed to SetCutover or
// SetReverseCutover) runs while the move holds LOCK TABLES on sessions it
// sends no statements on. The final flush of the change feeds and the callback
// together must complete within 10 minutes, the wait_timeout spirit sets on
// every connection. Otherwise MySQL closes the idle lock sessions and releases
// the locks before the tables are renamed, and writes can reach the tables the
// traffic switch moved away from.
type CutoverResultCallback func(context.Context) (CutoverResult, error)

// CutOverSource holds per-source state needed for the cutover.
type CutOverSource struct {
	DB         *sql.DB
	ReplClient change.Source
	Tables     []*table.TableInfo
}

type CutOver struct {
	sources              []CutOverSource
	cutoverFunc          func(ctx context.Context) error
	cutoverResultFunc    CutoverResultCallback
	cutoverFuncMutated   bool
	cutoverFuncAmbiguous bool
	dbConfig             *dbconn.DBConfig
	logger               *slog.Logger
	// cutoverFuncSucceeded tracks whether the cutover callback has returned nil.
	// The callback is a caller-supplied traffic switch (e.g. a Vitess routing
	// change) and is not assumed to be idempotent: once it has succeeded,
	// reported a durable mutation, or reported ambiguity it must never be
	// invoked again.
	cutoverFuncSucceeded bool

	// checksUnderLock, when set, runs under the source locks after the final
	// flush and before preSwitch, the traffic switch and the rename. A
	// refusal from it (check.ErrRefused) fails the cutover without a retry
	// (errCutoverRefused); any other error is retried like a failed attempt.
	// The locks
	// keep out DDL that needs a metadata lock on the locked tables, such as a
	// CREATE TRIGGER on them.
	checksUnderLock func(ctx context.Context) error

	// preSwitch runs under the source locks after the final flush and before
	// traffic can reach the target. The runner carries the AUTO_INCREMENT
	// counters over to the targets here, and reverse moves capture their start
	// positions here so writes committed during the switch are included. It
	// runs again on a safe retry, after that attempt's final flush.
	preSwitch func(ctx context.Context) error

	// postSwitch persists the captured positions after the switch succeeds,
	// before the source rename. It must run at most once.
	postSwitch     func(ctx context.Context) error
	postSwitchDone bool
}

// SetChecksUnderLock registers checks to run under the source locks after
// the final flush and before the traffic switch. See CutOver.checksUnderLock.
func (c *CutOver) SetChecksUnderLock(fn func(ctx context.Context) error) {
	c.checksUnderLock = fn
}

// SetPreSwitch registers a hook after the final source flush and before the
// traffic switch, under the source locks. See CutOver.preSwitch.
func (c *CutOver) SetPreSwitch(fn func(ctx context.Context) error) {
	c.preSwitch = fn
}

// SetPostSwitch registers a hook to run once under the source locks, after the
// traffic switch and before the source rename. See CutOver.postSwitch.
func (c *CutOver) SetPostSwitch(fn func(ctx context.Context) error) {
	c.postSwitch = fn
}

// SetCutoverWithResult installs a result-bearing forward cutover callback.
// It is mutually exclusive with the legacy error-only callback.
func (c *CutOver) SetCutoverWithResult(fn CutoverResultCallback) {
	c.cutoverFunc = nil
	c.cutoverResultFunc = fn
}

// NewCutOver creates a new CutOver that handles multiple sources.
func NewCutOver(sources []CutOverSource, cutoverFunc func(ctx context.Context) error, dbConfig *dbconn.DBConfig, logger *slog.Logger) (*CutOver, error) {
	if len(sources) == 0 {
		return nil, errors.New("at least one source must be provided")
	}
	for i, src := range sources {
		if src.DB == nil {
			return nil, fmt.Errorf("source %d: DB must be non-nil", i)
		}
		if src.ReplClient == nil {
			return nil, fmt.Errorf("source %d: repl client must be non-nil", i)
		}
		if len(src.Tables) == 0 {
			return nil, fmt.Errorf("source %d: at least one table must be provided", i)
		}
		for _, tbl := range src.Tables {
			if tbl == nil {
				return nil, fmt.Errorf("source %d: table must be non-nil", i)
			}
		}
	}
	if dbConfig == nil {
		return nil, errors.New("dbConfig must be non-nil")
	}
	// Run executes one attempt per MaxRetries iteration, so a value below 1
	// would mean the cutover loop never runs and the move would "succeed"
	// without locking, flushing, switching or renaming anything.
	if dbConfig.MaxRetries < 1 {
		return nil, fmt.Errorf("dbConfig.MaxRetries must be at least 1, got %d", dbConfig.MaxRetries)
	}
	return &CutOver{
		sources:     sources,
		cutoverFunc: cutoverFunc,
		dbConfig:    dbConfig,
		logger:      logger,
	}, nil
}

func (c *CutOver) Run(ctx context.Context) error {
	return c.runWithRetries(ctx, func(attempt int) error {
		// Flush all sources before attempting the cutover.
		for i, src := range c.sources {
			if err := src.ReplClient.Flush(ctx); err != nil {
				return fmt.Errorf("source %d: flush failed: %w", i, err)
			}
		}
		c.logger.Warn("Attempting final cut over operation",
			"attempt", attempt+1,
			"max-retries", c.dbConfig.MaxRetries)
		return c.algorithmCutover(ctx)
	})
}

// runWithRetries owns the cutover retry policy: which failures may be tried
// again from the top, and which have left table ownership in a state that a
// retry could make worse. runAttempt is a parameter so the policy can be
// tested without a live topology.
func (c *CutOver) runWithRetries(ctx context.Context, runAttempt func(attempt int) error) error {
	var err error
	for attempt := range c.dbConfig.MaxRetries {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err = runAttempt(attempt)
		if err != nil {
			if errors.Is(err, errCutoverRefused) {
				// Refused before the traffic switch: the source is still live and
				// a retry would be refused for the same reason.
				c.logger.Error("cutover refused under the source table locks; not retrying",
					"error", err.Error())
				return err
			}
			if errors.Is(err, errRenameRollbackFailed) || errors.Is(err, status.ErrOwnershipAmbiguous) {
				c.logger.Error("cutover rename left ownership unresolved; not retrying",
					"error", err.Error())
				ownershipErr := fmt.Errorf("%w: cutover rename left ownership unresolved: %w",
					status.ErrOwnershipAmbiguous, err)
				if c.cutoverFuncMutated {
					return errors.Join(status.ErrDurableMutation, ownershipErr)
				}
				return ownershipErr
			}
			if c.cutoverFuncAmbiguous && !c.cutoverFuncSucceeded {
				c.logger.Error("cutover callback failed with ambiguous ownership; not retrying",
					"error", err.Error())
				ownershipErr := fmt.Errorf("%w: cutover callback left ownership ambiguous: %w",
					status.ErrOwnershipAmbiguous, err)
				if c.cutoverFuncMutated {
					return errors.Join(status.ErrDurableMutation, ownershipErr)
				}
				return ownershipErr
			}
			if c.cutoverFuncMutated && !c.cutoverFuncSucceeded {
				c.logger.Error("cutover callback failed after reporting a durable mutation; not retrying",
					"error", err.Error())
				return fmt.Errorf("%w: cutover callback failed after a durable mutation: %w",
					status.ErrDurableMutation, err)
			}
			if c.cutoverFuncSucceeded {
				// Traffic may already be on the target. Retrying from the top
				// would release the source locks and could replay straggler
				// writes over newer target rows.
				c.logger.Error("cutover failed after the cutover function had already succeeded; not retrying",
					"error", err.Error())
				ownershipErr := fmt.Errorf("%w: cutover failed after the cutover function succeeded; "+
					"source tables are unlocked and not fully renamed; manual intervention required: %w",
					status.ErrOwnershipAmbiguous, err)
				if c.cutoverFuncMutated {
					return errors.Join(status.ErrDurableMutation, ownershipErr)
				}
				return ownershipErr
			}
			c.logger.Warn("cutover failed", "error", err.Error())
			continue
		}
		c.logger.Warn("final cut over operation complete")
		return nil
	}
	c.logger.Error("cutover failed, and retries exhausted")
	return err
}

func (c *CutOver) runCutoverCallback(ctx context.Context) error {
	if c.cutoverFuncSucceeded || (c.cutoverResultFunc == nil && c.cutoverFunc == nil) {
		return nil
	}
	c.logger.Info("Running cutover function")
	if c.cutoverResultFunc != nil {
		result, err := c.cutoverResultFunc(ctx)
		c.cutoverFuncMutated = c.cutoverFuncMutated || result.DurableMutation
		c.cutoverFuncAmbiguous = c.cutoverFuncAmbiguous || result.OwnershipAmbiguous
		if result.OwnershipAmbiguous && err == nil {
			err = status.ErrOwnershipAmbiguous
		}
		if err != nil {
			return err
		}
	} else if err := c.cutoverFunc(ctx); err != nil {
		// The legacy callback cannot report whether it mutated before failing.
		// Do not retry an unknown traffic-switch outcome.
		c.cutoverFuncAmbiguous = true
		return err
	}
	c.cutoverFuncSucceeded = true
	c.logger.Info("Cutover function complete")
	return nil
}

func (c *CutOver) algorithmCutover(ctx context.Context) error {
	// Lock tables on ALL sources.
	var sourceLocks []*dbconn.TableLock
	for i, src := range c.sources {
		lock, err := dbconn.NewTableLock(ctx, src.DB, src.Tables, c.dbConfig, c.logger)
		if err != nil {
			// Close any locks we already acquired.
			for _, l := range sourceLocks {
				utils.CloseAndLogWithContext(ctx, l)
			}
			return fmt.Errorf("failed to lock tables on source %d: %w", i, err)
		}
		sourceLocks = append(sourceLocks, lock)
	}
	defer func() {
		for _, l := range sourceLocks {
			utils.CloseAndLogWithContext(ctx, l)
		}
	}()

	// Flush ALL repl clients. No new changes will arrive because all sources are locked.
	for i, src := range c.sources {
		if err := src.ReplClient.Flush(ctx); err != nil {
			return fmt.Errorf("failed to flush repl client for source %d: %w", i, err)
		}
	}

	// Check ALL changes flushed.
	for i, src := range c.sources {
		if !src.ReplClient.AllChangesFlushed() {
			return fmt.Errorf("%w on source %d, final flush might be broken", change.ErrChangesNotFlushed, i)
		}
	}

	if c.checksUnderLock != nil {
		if err := c.checksUnderLock(ctx); err != nil {
			// Only a refusal is final. Any other error (a failed query, a
			// dropped connection) may be transient, so it takes the normal
			// retry path.
			if errors.Is(err, check.ErrRefused) {
				return fmt.Errorf("%w: %w", errCutoverRefused, err)
			}
			return fmt.Errorf("checks under the source locks: %w", err)
		}
	}

	if c.preSwitch != nil {
		if err := c.preSwitch(ctx); err != nil {
			return fmt.Errorf("pre-switch hook failed: %w", err)
		}
	}

	// Run the caller-owned traffic switch at most once. Result-bearing callers
	// retain durable-mutation and ambiguity evidence even when they return an
	// error; the legacy callback is conservatively ambiguous on any error.
	if err := c.runCutoverCallback(ctx); err != nil {
		return err
	}

	// Reverse-window hook: after the traffic switch and before retiring the
	// sources, persist the captured reverse-feed positions and record that the
	// move has entered its reverse window. Runs once, under the source locks,
	// while the sources are quiescent. If it fails the cutover fails: the
	// routing switch has already succeeded, so (like a rename failure past that
	// point) Run does not retry from the top and surfaces the error.
	if c.postSwitch != nil && !c.postSwitchDone {
		if err := c.postSwitch(ctx); err != nil {
			return fmt.Errorf("reverse-window post-switch hook failed: %w", err)
		}
		c.postSwitchDone = true
	}

	// Rename the source tables out of the way. Once the cutover function has
	// succeeded, traffic is on the target and the table locks held above are
	// the only thing preventing straggler writes (e.g. from a lagging router)
	// from landing on the source and being replayed over newer target rows.
	// So the rename is retried HERE, while the locks are still held, rather
	// than by returning to Run's retry loop (which would release the locks
	// between attempts and reopen that window).
	var err error
	for attempt := 1; attempt <= c.dbConfig.MaxRetries; attempt++ {
		if attempt > 1 {
			c.logger.Warn("retrying rename while still holding source table locks",
				"attempt", attempt,
				"max-retries", c.dbConfig.MaxRetries)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(renameRetryWait):
			}
		}
		if err = c.renameAllSources(ctx, sourceLocks); err == nil {
			// Every source is renamed and the source locks are still held, so no
			// write can be in flight. Stop the forward feeds inside that window
			// rather than after the deferred unlock, where the first straggler
			// write races us. A reverse window is unaffected: its feeds are
			// separate clients (ReverseFeed) reading the targets, and its start
			// positions were captured by preSwitch above. See change.Source's
			// Stop.
			c.stopSourceFeeds()
			return nil
		}
		if errors.Is(err, errRenameRollbackFailed) || errors.Is(err, status.ErrOwnershipAmbiguous) {
			// The sources are partially renamed, or a rename's outcome is
			// unknown; retrying cannot converge on either.
			return err
		}
		c.logger.Warn("rename failed", "error", err.Error())
	}
	return fmt.Errorf("rename failed after %d attempts under lock: %w", c.dbConfig.MaxRetries, err)
}

// stopSourceFeeds tells every source feed that its subscriptions have stopped
// describing reality. Called once the rename has fully succeeded, while the
// source locks are still held.
func (c *CutOver) stopSourceFeeds() {
	for _, src := range c.sources {
		src.ReplClient.Stop()
	}
}

// renameAllSources renames the tables on every source to their _old names,
// rolling back the completed renames if a later source fails. A rollback
// failure is wrapped with errRenameRollbackFailed because the sources are
// then left partially renamed and a retry of the full rename cannot succeed.
func (c *CutOver) renameAllSources(ctx context.Context, sourceLocks []*dbconn.TableLock) error {
	var completedRenames []int
	for i, src := range c.sources {
		renameFragments := make([]string, 0, len(src.Tables))
		for _, tbl := range src.Tables {
			oldQuotedName := sqlescape.EscapeIdentifier(check.CutoverOldName(tbl.TableName))
			renameFragments = append(renameFragments,
				fmt.Sprintf("%s TO %s", tbl.QuotedTableName, oldQuotedName),
			)
		}
		renameStatement := "RENAME TABLE " + strings.Join(renameFragments, ", ")
		if err := sourceLocks[i].ExecUnderLock(ctx, renameStatement); err != nil {
			// Rollback completed renames. Log failures since callers need to know
			// if rollback was incomplete for manual intervention.
			var rollbackErrors []string
			for _, j := range completedRenames {
				undoFragments := make([]string, 0, len(c.sources[j].Tables))
				for _, tbl := range c.sources[j].Tables {
					oldQuotedName := sqlescape.EscapeIdentifier(check.CutoverOldName(tbl.TableName))
					undoFragments = append(undoFragments,
						fmt.Sprintf("%s TO %s", oldQuotedName, tbl.QuotedTableName),
					)
				}
				undoStatement := "RENAME TABLE " + strings.Join(undoFragments, ", ")
				if undoErr := sourceLocks[j].ExecUnderLock(ctx, undoStatement); undoErr != nil {
					c.logger.Error("rollback rename failed", "source", j, "error", undoErr)
					rollbackErrors = append(rollbackErrors, fmt.Sprintf("source %d: %v", j, undoErr))
				}
			}
			if len(rollbackErrors) > 0 {
				return fmt.Errorf("%w: rename failed on source %d and rollback also failed (%s): %w",
					errRenameRollbackFailed, i, strings.Join(rollbackErrors, "; "), err)
			}
			if dbconn.IsConnectionLossError(err) {
				// The connection died, so the server may have committed this
				// source's rename before the OK packet was lost. The earlier
				// sources have definitively been rolled back, but this one's
				// state is unknown: retrying the whole rename could retire a
				// source that is already retired.
				return fmt.Errorf("%w: rename outcome unknown on source %d, rolled back %d completed renames: %w",
					status.ErrOwnershipAmbiguous, i, len(completedRenames), err)
			}
			return fmt.Errorf("rename failed on source %d, rolled back %d completed renames: %w",
				i, len(completedRenames), err)
		}
		completedRenames = append(completedRenames, i)
	}
	return nil
}

// autoIncrementTable is one copy of a table whose AUTO_INCREMENT counter
// carryAutoIncrement reads or raises.
type autoIncrementTable struct {
	db     *sql.DB
	schema string
	name   string
}

// carryAutoIncrement raises the AUTO_INCREMENT counter of every table in to
// to at least the highest counter among the tables in from, so that no copy
// in to ever issues an id that a copy in from has issued.
//
// Rows only carry ids. A destination's counter follows the highest id it has
// received, so any id issued above that on the source - rows inserted and then
// deleted before they were copied (their changes merge away), rolled back
// inserts, INSERT IGNORE and upsert gaps - would be issued again once traffic
// moves. The counter a target table was created with is stale by the time of
// the cutover.
//
// The caller must guarantee nothing can advance a counter in from after it has
// been read (the sources are locked and flushed) and that nothing but spirit
// writes to to yet (traffic has not been switched).
//
// With several copies on either side (a sharded source or target), every
// destination gets the maximum of all the source counters. That is the only
// value that rules out reissuing any id a source has issued, whichever shard
// a row lands on. It does not make per-shard counters globally unique between
// destinations: sharded tables need an external sequence (such as a Vitess
// sequence) for that, and those typically have no AUTO_INCREMENT column, in
// which case there is nothing to carry.
//
// Counters are only raised: InnoDB lowers the counter to any value above the
// table's current maximum, so an unconditional ALTER could move one backwards.
// Tables without an AUTO_INCREMENT column are skipped on either side. The
// cost is one SHOW CREATE TABLE per copy, plus a metadata-only ALTER TABLE ..
// AUTO_INCREMENT (no table rebuild) for each destination that is behind.
func carryAutoIncrement(ctx context.Context, logger *slog.Logger, from, to []autoIncrementTable) error {
	var maxNext uint64
	for _, t := range from {
		next, ok, err := table.NextAutoIncrement(ctx, t.db, t.schema, t.name)
		if err != nil {
			return err
		}
		if ok {
			maxNext = max(maxNext, next)
		}
	}
	if maxNext <= 1 {
		return nil // no AUTO_INCREMENT column, or no id issued yet
	}
	for _, t := range to {
		next, ok, err := table.NextAutoIncrement(ctx, t.db, t.schema, t.name)
		if err != nil {
			return err
		}
		if !ok || next >= maxNext {
			continue
		}
		if err := dbconn.Exec(ctx, t.db, "ALTER TABLE %n.%n AUTO_INCREMENT = %?", t.schema, t.name, maxNext); err != nil {
			return fmt.Errorf("failed to set AUTO_INCREMENT on %s.%s: %w", t.schema, t.name, err)
		}
		logger.Info("raised AUTO_INCREMENT so that ids the other side has issued are not issued again",
			"schema", t.schema,
			"table", t.name,
			"from", next,
			"auto_increment", maxNext)
	}
	return nil
}
