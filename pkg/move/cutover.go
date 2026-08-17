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

// CutOverSource holds per-source state needed for the cutover.
type CutOverSource struct {
	DB         *sql.DB
	ReplClient change.Source
	Tables     []*table.TableInfo
}

type CutOver struct {
	sources     []CutOverSource
	cutoverFunc func(ctx context.Context) error
	dbConfig    *dbconn.DBConfig
	logger      *slog.Logger
	// cutoverFuncSucceeded tracks whether cutoverFunc has been invoked and
	// returned nil. The cutover function is a caller-supplied traffic switch
	// (e.g. a Vitess routing change) and is not assumed to be idempotent:
	// once it has succeeded it must never be invoked again, and retrying
	// any later step must not reopen the window where straggler writes to
	// the (now stale) source could be replayed over newer rows on the target.
	cutoverFuncSucceeded bool

	// postSwitch, when set, runs once under the source locks after the traffic
	// switch (cutoverFunc) succeeds and before the sources are renamed out of
	// the way. The reverse-window move uses it to capture each target's binlog
	// position and persist that the move has entered its reverse window, while
	// the sources are quiescent (writes switched away, replication flushed) so
	// the captured positions cleanly bound the post-cutover writes. Like
	// cutoverFunc it must run at most once; postSwitchDone guards that.
	postSwitch     func(ctx context.Context) error
	postSwitchDone bool
}

// SetPostSwitch registers a hook to run once under the source locks, after the
// traffic switch and before the source rename. See CutOver.postSwitch.
func (c *CutOver) SetPostSwitch(fn func(ctx context.Context) error) {
	c.postSwitch = fn
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
			if errors.Is(err, errRenameRollbackFailed) || errors.Is(err, status.ErrOwnershipAmbiguous) {
				c.logger.Error("cutover rename left ownership unresolved; not retrying",
					"error", err.Error())
				return fmt.Errorf("%w: cutover rename left ownership unresolved: %w",
					status.ErrOwnershipAmbiguous, err)
			}
			if c.cutoverFuncSucceeded {
				// The traffic switch has already succeeded, so traffic may be
				// on the target. Another attempt would have released the
				// source table locks in between, reopening the window where
				// straggler writes to the source could be replayed over newer
				// rows on the target. The rename has already been retried
				// while the locks were held (see algorithmCutover), so do not
				// retry from the top: surface the error instead.
				c.logger.Error("cutover failed after the cutover function had already succeeded; not retrying",
					"error", err.Error())
				return fmt.Errorf("%w: cutover failed after the cutover function succeeded; "+
					"source tables are unlocked and not fully renamed (the rename may have "+
					"partially applied), manual intervention required: %w",
					status.ErrOwnershipAmbiguous, err)
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

	// Run the cutover function (Vitess coordination). It is invoked at most
	// once per CutOver: the traffic switch is not assumed to be idempotent,
	// so if a later step fails the retry must never re-run it.
	if c.cutoverFunc != nil && !c.cutoverFuncSucceeded {
		c.logger.Info("Running cutover function")
		if err := c.cutoverFunc(ctx); err != nil {
			return err
		}
		c.cutoverFuncSucceeded = true
		c.logger.Info("Cutover function complete")
	}

	// Reverse-window hook: after the traffic switch and before retiring the
	// sources, capture the reverse-feed start positions and record that the
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
			// positions were captured by postSwitch above. See change.Source's
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
