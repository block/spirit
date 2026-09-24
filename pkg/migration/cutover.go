package migration

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
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
)

const (
	// cutoverInitialBackoff is the starting per-retry delay before
	// re-attempting the rename. It doubles on each subsequent failure up
	// to cutoverMaxBackoff. Without backoff a contended table lock
	// produces a tight retry loop that worsens contention; this gives
	// the source of contention time to clear.
	cutoverInitialBackoff = 100 * time.Millisecond
	cutoverMaxBackoff     = 10 * time.Second
	// cutoverUnlockTimeout caps the duration of the deferred UNLOCK TABLES
	// in executeRenameUnderLock. The unlock runs under a ctx derived via
	// context.WithoutCancel so it still fires when the parent ctx is
	// cancelled mid-cutover; without an explicit timeout the call could
	// block indefinitely on an unhealthy connection.
	cutoverUnlockTimeout = 30 * time.Second
)

type CutOver struct {
	db       *sql.DB
	feed     change.Source
	config   []*cutoverConfig
	dbConfig *dbconn.DBConfig
	logger   *slog.Logger
	// testInjectRenameError is a test-only seam: when non-nil it is returned
	// in place of a successful rename's nil result, simulating a connection
	// that died after the server committed the RENAME TABLE but before the
	// client read the OK packet.
	testInjectRenameError error
	// testAfterRenameError is a test-only seam that runs immediately before
	// testInjectRenameError is returned. It lets a test make the subsequent
	// ownership verification unavailable, which is the state that separates
	// "the cutover failed" from "we cannot tell who owns the table".
	testAfterRenameError func()
}

type cutoverConfig struct {
	table          *table.TableInfo
	newTable       *table.TableInfo
	oldTableName   string
	useTestCutover bool
}

// NewCutOver contains the logic to perform the final cut over. It can cutover multiple tables
// at once based on config. A replication feed which is used to ensure consistency before the cut over.
func NewCutOver(db *sql.DB, config []*cutoverConfig, feed change.Source, dbConfig *dbconn.DBConfig, logger *slog.Logger) (*CutOver, error) {
	if feed == nil {
		return nil, errors.New("feed must be non-nil")
	}
	// validate the cutoverConfig
	for _, cfg := range config {
		if cfg.table == nil || cfg.newTable == nil {
			return nil, errors.New("table and newTable must be non-nil")
		}
		if cfg.oldTableName == "" {
			return nil, errors.New("oldTableName must be non-empty")
		}
	}
	return &CutOver{
		db:       db,
		config:   config,
		feed:     feed,
		dbConfig: dbConfig,
		logger:   logger,
	}, nil
}

func (c *CutOver) Run(ctx context.Context) error {
	if c.dbConfig.MaxOpenConnections < 5 {
		// The gh-ost cutover algorithm requires a minimum of 3 connections:
		// - The LOCK TABLES connection
		// - The RENAME TABLE connection
		// - The Flush() threads
		// Because we want to safely flush quickly, we set the limit to 5.
		//
		// A migration cannot reach here under that number: Migration.Validate
		// rejects a --max-connections below minPoolSize, which is this 5. The
		// guard stays for callers that build a CutOver directly, and it is the
		// one place spirit will exceed a configured pool size — because the
		// alternative is a cutover that cannot run at all.
		dbconn.SetPoolSize(c.db, 5)
	}
	// Collect every attempt's error and join them on exit, so an operator
	// debugging a flapping cutover sees the full failure history rather
	// than just whatever happened on the last try.
	var attemptErrs []error
	backoff := cutoverInitialBackoff
	// renameMayHaveCommitted is set when an attempt fails with a
	// connection-loss error. RENAME TABLE is atomic server-side, but if the
	// connection dies after the server commits the rename and before the
	// client reads the OK packet, the client observes a connection error for
	// a rename that actually succeeded. Once set, every subsequent decision
	// point first verifies the server state instead of blindly retrying.
	renameMayHaveCommitted := false
	// renameStateUnverified is set when an ambiguous attempt could not be
	// resolved because the *verification* itself failed. A verification that
	// succeeds and reports "not renamed" is conclusive, so it clears this: the
	// cutover simply failed and the caller may retry the whole migration.
	renameStateUnverified := false
	confirm := func() bool {
		completed, verified := c.confirmRenameCompleted(ctx)
		renameStateUnverified = !verified
		return completed
	}
	fail := func(errs ...error) error {
		all := append(attemptErrs, errs...) //nolint:gocritic // deliberately not appending back into attemptErrs
		if renameStateUnverified {
			all = append(all, status.ErrOwnershipAmbiguous)
		}
		return errors.Join(all...)
	}
	for i := range max(1, c.dbConfig.MaxRetries) {
		if ctx.Err() != nil {
			return fail(ctx.Err())
		}
		if i > 0 {
			// Exponential backoff between attempts. Without this a
			// contended lock produces a tight retry loop that worsens
			// contention. Sleeping ctx-aware so a cancel during backoff
			// returns promptly instead of waiting out the full delay.
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return fail(ctx.Err())
			case <-timer.C:
			}
			backoff *= 2
			if backoff > cutoverMaxBackoff {
				backoff = cutoverMaxBackoff
			}
		}
		// If a previous attempt failed ambiguously, re-check the server state
		// before doing anything else — in particular before feed.Flush below,
		// which would otherwise fail with ER_NO_SUCH_TABLE applying buffered
		// changes to the renamed-away _new table and abort the whole retry
		// loop ("cutover failed" after a cutover that actually succeeded).
		if renameMayHaveCommitted && confirm() {
			return nil
		}
		// Try and catch up before we attempt the cutover.
		// since we will need to catch up again with the lock held
		// and we want to minimize that.
		if err := c.feed.Flush(ctx); err != nil {
			return fail(err)
		}
		// We use maxCutoverRetries as our retrycount, but nested
		// within c.algorithmX() it may also have a retry for the specific statement
		c.logger.Warn("Attempting final cut over operation",
			"attempt", i+1,
			"max_retries", c.dbConfig.MaxRetries,
		)
		// if specified in c.config[0], we will use the test cutover for failure injection.
		// we don't need to exhaustively check all configs.
		var err error
		if len(c.config) > 0 && c.config[0].useTestCutover {
			err = c.partialRenameForTest(ctx)
		} else {
			err = c.algorithmRenameUnderLock(ctx)
		}
		if err != nil {
			attemptErrs = append(attemptErrs, fmt.Errorf("attempt %d: %w", i+1, err))
			if dbconn.IsConnectionLossError(err) {
				// Ambiguous failure: the connection died, so the client
				// cannot know whether the server committed the rename before
				// the OK packet was lost. Verify the actual server state on a
				// fresh connection before treating this as a failure.
				// Deterministic SQL errors (lock wait timeout, deadlock, ...)
				// deliberately skip this: for those the server positively
				// reported the statement failed, so the normal retry path is
				// correct.
				renameMayHaveCommitted = true
				if confirm() {
					return nil
				}
			}
			c.logger.Warn("cutover failed",
				"error", err.Error(),
				"next_backoff", backoff,
			)
			continue
		}
		c.logger.Warn("final cut over operation complete")
		return nil
	}
	// Retries are exhausted. If any attempt failed ambiguously, give the
	// state check one final chance before declaring failure: the server may
	// have committed the rename only after the last in-loop verification ran
	// (e.g. the dying rename was still waiting on metadata locks).
	if renameMayHaveCommitted && confirm() {
		return nil
	}
	c.logger.Error("cutover failed, and retries exhausted")
	return fail()
}

// confirmRenameCompleted wraps renameCompleted with logging for use in the
// retry loop after an ambiguous connection-loss failure. completed is true
// only if the server-side state proves the cutover rename was committed.
// verified reports whether the server state could be read at all: when it is
// false the outcome of the dying rename is still unknown, which is what makes
// a subsequent failure ownership-ambiguous rather than merely failed.
func (c *CutOver) confirmRenameCompleted(ctx context.Context) (completed, verified bool) {
	completed, err := c.renameCompleted(ctx)
	if err != nil {
		c.logger.Warn("could not verify whether the cutover rename was committed after a connection failure; continuing to retry",
			"error", err.Error())
		return false, false
	}
	if !completed {
		return false, true
	}
	c.logger.Warn("cutover rename was committed by the server even though the client connection failed; treating cutover as successful")
	return true, true
}

// renameCompleted reports whether the cutover RENAME TABLE has been committed
// on the server, by inspecting information_schema on a fresh connection from
// the pool. It is used when a cutover attempt fails with a connection-loss
// error and the outcome of the rename is therefore unknown to the client.
//
// The signature checked is, for every table in the cutover:
//   - `<table>` exists, and
//   - `_<table>_new` is gone, and
//   - `_<table>_old` exists.
//
// This combination is unambiguous evidence that the rename committed:
// `_<table>_old` is dropped immediately before the cutover starts (see
// (*Runner).run) and the cutover's atomic RENAME TABLE is the only statement
// spirit issues that re-creates it, while that same statement is the only
// thing that removes `_<table>_new`. We deliberately do not compare
// `<table>`'s schema against the expected post-ALTER schema: for
// idempotent-shaped ALTERs (e.g. ENGINE=InnoDB, MODIFY to the same type) the
// pre- and post-cutover schemas are identical, so a schema comparison cannot
// distinguish the two states, whereas table existence can. Concurrent
// operators renaming or dropping tables out from under a running migration
// are not a supported scenario.
func (c *CutOver) renameCompleted(ctx context.Context) (bool, error) {
	for _, cfg := range c.config {
		// The RENAME TABLE statement uses schema-unqualified names, resolved
		// against the connection's default database — the schema the
		// migration runs in (cfg.table.SchemaName). A single query keeps the
		// three existence checks in one consistent snapshot.
		var origExists, newExists, oldExists bool
		err := c.db.QueryRowContext(ctx, `SELECT
			COUNT(CASE WHEN table_name = ? THEN 1 END) > 0,
			COUNT(CASE WHEN table_name = ? THEN 1 END) > 0,
			COUNT(CASE WHEN table_name = ? THEN 1 END) > 0
			FROM information_schema.tables WHERE table_schema = ?`,
			cfg.table.TableName, cfg.newTable.TableName, cfg.oldTableName,
			cfg.table.SchemaName,
		).Scan(&origExists, &newExists, &oldExists)
		if err != nil {
			return false, err
		}
		if !origExists || newExists || !oldExists {
			return false, nil
		}
	}
	return true, nil
}

// algorithmRenameUnderLock is the preferred cutover algorithm.
// As of MySQL 8.0.13, you can rename tables locked with a LOCK TABLES statement
// https://dev.mysql.com/worklog/task/?id=9826
func (c *CutOver) algorithmRenameUnderLock(ctx context.Context) error {
	tablesToLock := []*table.TableInfo{}
	renameFragments := []string{}
	for _, cfg := range c.config {
		tablesToLock = append(tablesToLock, cfg.table, cfg.newTable)
		oldQuotedName := sqlescape.EscapeIdentifier(cfg.oldTableName)
		renameFragments = append(renameFragments,
			fmt.Sprintf("%s TO %s", cfg.table.QuotedTableName, oldQuotedName),
			fmt.Sprintf("%s TO %s", cfg.newTable.QuotedTableName, cfg.table.QuotedTableName),
		)
	}
	return c.executeRenameUnderLock(ctx, tablesToLock, renameFragments, true)
}

// executeRenameUnderLock is the shared implementation for performing renames under a table lock.
// It handles locking, binlog flushing, and executing the rename statement.
//
// stopFeed says whether a completed rename means the feed is finished with. It
// is true for the real cutover and false for partialRenameForTest, which leaves
// the tables half-renamed and expects Run's retry loop — and therefore the
// feed — to carry on.
func (c *CutOver) executeRenameUnderLock(ctx context.Context, tablesToLock []*table.TableInfo, renameFragments []string, stopFeed bool) error {
	tableLock, err := dbconn.NewTableLock(ctx, c.db, tablesToLock, c.dbConfig, c.logger)
	if err != nil {
		return err
	}
	// Run UNLOCK TABLES with a ctx that ignores the parent's cancellation
	// so a ctx cancel mid-cutover still releases the lock. ExecContext on
	// a cancelled ctx returns immediately without sending the statement;
	// the server would eventually release the lock when the connection
	// dies, but giving UNLOCK TABLES a fair chance is cleaner. Bound the
	// detached ctx with cutoverUnlockTimeout so an unhealthy connection
	// can't block the deferred close indefinitely.
	unlockCtx, cancelUnlock := context.WithTimeout(context.WithoutCancel(ctx), cutoverUnlockTimeout)
	defer cancelUnlock()
	defer utils.CloseAndLogWithContext(unlockCtx, tableLock)
	// FlushUnderTableLock itself does flush → BlockWait → flush, so the
	// flush's own binlog events (REPLACE INTO _new / DELETE FROM _new) are
	// already read back inside the BlockWait of the same call. The earlier
	// "second FlushUnderTableLock" workaround was added under the
	// assumption that an extra round-trip would close a position-bookkeeping
	// gap, but the real bug was upstream — events were being silently
	// dropped by KeyAboveHighWatermark in the chunkPtr.IsNil window
	// (issue #746, fixed in chunker_optimistic.go / chunker_composite.go).
	// Now that the source of the divergence is gone, a single
	// FlushUnderTableLock is sufficient.
	if err := c.feed.FlushUnderTableLock(ctx, []*dbconn.TableLock{tableLock}); err != nil {
		return err
	}
	if !c.feed.AllChangesFlushed() {
		return fmt.Errorf("%w, final flush might be broken", change.ErrChangesNotFlushed)
	}

	renameStatement := "RENAME TABLE " + strings.Join(renameFragments, ", ")
	if err := tableLock.ExecUnderLock(ctx, renameStatement); err != nil {
		return err
	}
	// The tables are swapped and the lock is still held, so no application
	// write can be in flight and nothing more can reach the changeset. Stop
	// the feed here, in that window, rather than after the deferred UNLOCK
	// TABLES: the first post-cutover write is a race with the unlock, and it
	// carries a row image for the *post*-ALTER table while the subscription
	// still holds the pre-ALTER TableInfo. See change.Source's Stop.
	if stopFeed {
		c.feed.Stop()
	}
	if c.testInjectRenameError != nil {
		// Test-only seam: the rename was committed by the server, but we
		// pretend the client never read the OK packet.
		if c.testAfterRenameError != nil {
			c.testAfterRenameError()
		}
		return c.testInjectRenameError
	}
	return nil
}

// partialRenameForTest performs a partial cutover (only renames original table to _old)
// This is intended for testing the atomicity/consistency of the cutover.
func (c *CutOver) partialRenameForTest(ctx context.Context) error {
	tablesToLock := []*table.TableInfo{}
	renameFragments := []string{}

	// Build the same locks and rename fragments as the normal algorithm,
	// but only include the first rename (original -> _old)
	for _, cfg := range c.config {
		tablesToLock = append(tablesToLock, cfg.table, cfg.newTable)
		oldQuotedName := sqlescape.EscapeIdentifier(cfg.oldTableName)
		// Only add the first rename: original table -> _old
		// Intentionally skip the second rename: _new -> original
		renameFragments = append(renameFragments,
			fmt.Sprintf("%s TO %s", cfg.table.QuotedTableName, oldQuotedName),
		)
	}
	// Execute the partial rename using the same code path
	if err := c.executeRenameUnderLock(ctx, tablesToLock, renameFragments, false); err != nil {
		return err
	}
	// Intentionally return an error to simulate a partial cutover failure
	return errors.New("intentional partial cutover failure for testing")
}
