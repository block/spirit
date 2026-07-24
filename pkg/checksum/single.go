// Package checksum provides online checksum functionality.
// Two tables on the same MySQL server can be compared with only an initial lock.
// It is not in the row/ package because it requires a replClient to be passed in,
// which would cause a circular dependency.
package checksum

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
	"golang.org/x/sync/errgroup"
)

type SingleChecker struct {
	sync.Mutex

	concurrency      int
	feed             change.Source
	db               *sql.DB
	trxPool          *dbconn.TrxPool // reader trx pool
	isInvalid        bool
	chunker          table.Chunker
	startTime        time.Time
	execTime         time.Duration
	dbConfig         *dbconn.DBConfig
	logger           *slog.Logger
	fixDifferences   bool
	differencesFound atomic.Uint64
	recopyLock       sync.Mutex
	maxRetries       int
	yieldTimeout     time.Duration
	yieldsPerformed  atomic.Uint64 // number of yield/resume cycles performed
}

var _ Checker = (*SingleChecker)(nil)

func (c *SingleChecker) ChecksumChunk(ctx context.Context, trxPool *dbconn.TrxPool, chunk *table.Chunk) error {
	startTime := time.Now()
	trx, err := trxPool.Get()
	if err != nil {
		return err
	}
	defer trxPool.Put(trx)
	c.logger.Debug("checksumming chunk", "chunk", chunk.String())
	sourceChecksumCols, targetChecksumCols, err := chunk.ColumnMapping.ChecksumExprs()
	if err != nil {
		return err
	}
	source := fmt.Sprintf("SELECT BIT_XOR(CRC32(CONCAT(%s))) as checksum, count(*) as c FROM %s WHERE %s",
		sourceChecksumCols,
		chunk.Table.QuotedTableName,
		chunk.String(),
	)
	target := fmt.Sprintf("SELECT BIT_XOR(CRC32(CONCAT(%s))) as checksum, count(*) as c FROM %s WHERE %s",
		targetChecksumCols,
		chunk.NewTable.QuotedTableName,
		chunk.String(),
	)
	var sourceChecksum, targetChecksum int64
	var sourceCount, targetCount uint64
	err = trx.QueryRowContext(ctx, source).Scan(&sourceChecksum, &sourceCount)
	if err != nil {
		return err
	}
	err = trx.QueryRowContext(ctx, target).Scan(&targetChecksum, &targetCount)
	if err != nil {
		return err
	}
	// Compare BOTH the checksum and the row count. The row count is already
	// returned by the query above, so comparing it is free, and it closes a
	// defense-in-depth gap: a row whose CRC32 is 0 contributes nothing to the
	// BIT_XOR, so its absence is invisible to the checksum but visible to the
	// count. A count mismatch is treated identically to a checksum mismatch.
	if mismatch := compareChunk(sourceChecksum, targetChecksum, sourceCount, targetCount); mismatch.mismatched() {
		// The source and target do not match, so we first need
		// to inspect closely and report on the differences.
		c.differencesFound.Add(1)
		c.logger.Warn("chunk verification failed", "chunk", chunk.String(), "reason", mismatch.reason(sourceCount, targetCount), "sourceChecksum", sourceChecksum, "targetChecksum", targetChecksum, "sourceCount", sourceCount, "targetCount", targetCount)
		if err := c.inspectDifferences(ctx, trx, chunk); err != nil {
			return err
		}
		// Are we allowed to fix the differences? If not, return an error.
		// This is mostly used by the test-suite.
		if !c.fixDifferences {
			return errors.New("checksum mismatch")
		}
		// Since we can fix differences, replace the chunk.
		if err = c.replaceChunk(ctx, chunk); err != nil {
			return err
		}
	}
	// When we give feedback, we need to say how many rows were in the chunk.
	c.chunker.Feedback(chunk, time.Since(startTime), targetCount)
	return nil
}

// GetProgress returns rows verified so far and the total to verify, proxied
// from the chunker.
func (c *SingleChecker) GetProgress() status.ChecksumProgress {
	rowsProcessed, _, totalRows := c.chunker.Progress()
	return status.ChecksumProgress{RowsChecked: rowsProcessed, RowsTotal: totalRows}
}

// inspectDifferences looks at the chunk and tries to find differences.
// For cross-database scenarios, it queries each database separately and compares in memory.
func (c *SingleChecker) inspectDifferences(ctx context.Context, trx *sql.Tx, chunk *table.Chunk) error {
	c.logger.Info("inspecting differences for chunk", "chunk", chunk.String())

	sourceChecksumCols, targetChecksumCols, err := chunk.ColumnMapping.ChecksumExprs()
	if err != nil {
		return err
	}
	sourceRows, err := trx.QueryContext(ctx, fmt.Sprintf(queryTemplate,
		sourceChecksumCols,
		table.QuoteColumns(chunk.Table.KeyColumns),
		chunk.Table.QuotedTableName,
		chunk.String(),
	))
	if err != nil {
		return fmt.Errorf("failed to query source rows: %w", err)
	}
	defer utils.CloseAndLog(sourceRows)

	// Build map of source checksums
	sourceChecksums := make(map[string]string) // pk -> checksum
	for sourceRows.Next() {
		var checksum, pk string
		if err := sourceRows.Scan(&checksum, &pk); err != nil {
			return fmt.Errorf("failed to scan source row: %w", err)
		}
		sourceChecksums[pk] = checksum
	}
	if err := sourceRows.Err(); err != nil {
		return fmt.Errorf("error iterating source rows: %w", err)
	}

	targetRows, err := trx.QueryContext(ctx, fmt.Sprintf(queryTemplate,
		targetChecksumCols,
		table.QuoteColumns(chunk.NewTable.KeyColumns),
		chunk.NewTable.QuotedTableName,
		chunk.String(),
	))
	if err != nil {
		return fmt.Errorf("failed to query target rows: %w", err)
	}
	defer utils.CloseAndLog(targetRows)

	// Build map of target checksums and compare
	targetChecksums := make(map[string]string) // pk -> checksum
	for targetRows.Next() {
		var checksum, pk string
		if err := targetRows.Scan(&checksum, &pk); err != nil {
			return fmt.Errorf("failed to scan target row: %w", err)
		}
		targetChecksums[pk] = checksum

		// Check if this row exists in source and has different checksum
		if sourceChecksum, exists := sourceChecksums[pk]; exists {
			if sourceChecksum != checksum {
				c.logger.Warn("inspection revealed row checksum mismatch", "pk", pk, "sourceChecksum", sourceChecksum, "targetChecksum", checksum)
			}
		} else {
			c.logger.Warn("inspection revealed row does not exist in source", "pk", pk)
		}
	}
	if err := targetRows.Err(); err != nil {
		return fmt.Errorf("error iterating target rows: %w", err)
	}

	// Check for rows that exist in source but not in target
	for pk, sourceChecksum := range sourceChecksums {
		if _, exists := targetChecksums[pk]; !exists {
			c.logger.Warn("inspection revealed row does not exist in target", "pk", pk, "sourceChecksum", sourceChecksum)
		}
	}

	return nil // managed to inspect differences
}

// replaceChunk recopies the data from table to newTable for a given chunk.
// For cross-database operations, this reads the data from the source and writes it to the target.
// Note that the chunk is dynamically sized based on the target-time that it took
// to *read* data in the checksum. This could be substantially longer than the time
// that it takes to copy the data. Maybe in future we could consider splitting
// the chunk here, but this is expected to be a very rare situation, so a small
// stall from an XL sized chunk is considered acceptable.
func (c *SingleChecker) replaceChunk(ctx context.Context, chunk *table.Chunk) error {
	c.logger.Warn("recopying chunk", "chunk", chunk.String())

	// We further prevent the chance of deadlocks from the recopying process by only re-copying one chunk at a time.
	// We may revisit this in future, but since conflicts are expected to be low, it should be fine for now.
	c.recopyLock.Lock()
	defer c.recopyLock.Unlock()

	// Construct a delete statement to remove existing rows in the target chunk
	deleteStmt := "DELETE FROM " + chunk.NewTable.QuotedTableName + " WHERE " + chunk.String()

	// Within the same database we use a REPLACE INTO .. SELECT approach.
	// Within database we also support intersecting columns (i.e.
	// there might be a schema change/transformation that applies).
	//
	// Note: historically this process has caused deadlocks between the DELETE statement
	// in one replaceChunk and the REPLACE statement of another chunk. Inspection of
	// SHOW ENGINE INNODB STATUS shows that this is not caused by locks on the PRIMARY KEY,
	// but a unique secondary key (in our case an idempotence key):
	//
	// ------------------------
	// LATEST DETECTED DEADLOCK
	// ------------------------
	// 2024-06-11 18:34:21 70676106989440
	// *** (1) TRANSACTION:
	// TRANSACTION 15106308424, ACTIVE 4 sec updating or deleting
	// mysql tables in use 1, locked 1
	// LOCK WAIT 620 lock struct(s), heap size 73848, 49663 row lock(s), undo log entries 49661
	// MySQL thread id 540806, OS thread handle 70369444421504, query id 409280999 10.137.84.232 <snip> updating
	// DELETE FROM `<snip>`.`_<snip>_new` WHERE `id` >= 1108588365 AND `id` < 1108688365
	//
	// *** (1) HOLDS THE LOCK(S):
	// RECORD LOCKS space id 1802 page no 26277057 n bits 232 index idempotence_key_idx of table `<snip>`.`_<snip>_new` trx id 15106308424 lock_mode X locks rec but not gap
	// Record lock, heap no 163 PHYSICAL RECORD: n_fields 2; compact format; info bits 32
	// 0: len 30; hex <snip>; asc <snip>; (total 62 bytes);
	// 1: len 8; hex <snip>; asc     <snip>;;
	//
	//
	// *** (1) WAITING FOR THIS LOCK TO BE GRANTED:
	// RECORD LOCKS space id 1802 page no 1945840 n bits 280 index idempotence_key_idx of table `<snip>`.`_<snip>_new` trx id 15106308424 lock_mode X locks rec but not gap waiting
	// Record lock, heap no 75 PHYSICAL RECORD: n_fields 2; compact format; info bits 0
	// 0: len 30; hex <snip>; asc <snip>; (total 62 bytes);
	// 1: len 8; hex <snip>; asc     <snip>;;
	//
	//
	// *** (2) TRANSACTION:
	// TRANSACTION 15106301192, ACTIVE 58 sec inserting
	// mysql tables in use 2, locked 1
	// LOCK WAIT 220020 lock struct(s), heap size 27680888, 409429 row lock(s), undo log entries 162834
	// MySQL thread id 540264, OS thread handle 70369485823872, query id 409127061 10.137.84.232 <snip> executing
	// REPLACE INTO `<snip>`.`_<snip>_new` (`id`, <snip> FROM `<snip>`.`<snip>` WHERE `id` >= 1106488365 AND `id` < 1106588365
	//
	// *** (2) HOLDS THE LOCK(S):
	// RECORD LOCKS space id 1802 page no 1945840 n bits 280 index idempotence_key_idx of table `<snip>`.`_<snip>_new` trx id 15106301192 lock_mode X
	// Record lock, heap no 75 PHYSICAL RECORD: n_fields 2; compact format; info bits 0
	// 0: len 30; hex <snip>; asc <snip>; (total 62 bytes);
	// 1: len 8; hex <snip>; asc     B yI;;
	//
	//
	// *** (2) WAITING FOR THIS LOCK TO BE GRANTED:
	// RECORD LOCKS space id 1802 page no 26277057 n bits 232 index idempotence_key_idx of table `<snip>`.`_<snip>_new` trx id 15106301192 lock_mode X waiting
	// Record lock, heap no 163 PHYSICAL RECORD: n_fields 2; compact format; info bits 32
	// 0: len 30; hex <snip>; asc <snip>; (total 62 bytes);
	// 1: len 8; hex <snip>; asc     <snip>;;
	//
	// *** WE ROLL BACK TRANSACTION (1)
	//
	// We don't need this to be an atomic transaction. We just need to delete from the _new table
	// first so that any since-deleted rows (which wouldn't get removed by replace) are removed first.
	// By doing this as two transactions we should be able to remove
	// the opportunity for deadlocks.
	//
	// The SELECT list comes from RepairExprs, not Columns(): JSON columns are
	// recopied through a text round-trip rather than byte-faithfully. The
	// checksum asserts that the target holds the one-text-round-trip image of
	// each source document (see table.ColumnMapping.RepairExprs), so a
	// byte-faithful recopy of a document whose text image differs from its
	// source bytes would be re-flagged by the next pass, indefinitely.
	sourceExprs, targetColumns, err := chunk.ColumnMapping.RepairExprs()
	if err != nil {
		return err
	}
	replaceStmt := fmt.Sprintf("REPLACE INTO %s (%s) SELECT %s FROM %s WHERE %s",
		chunk.NewTable.QuotedTableName,
		targetColumns,
		sourceExprs,
		chunk.Table.QuotedTableName,
		chunk.String(),
	)
	// The DELETE and REPLACE run as two separate transactions (to avoid the
	// deadlock pattern documented above). If the parent ctx is cancelled
	// between them, the target chunk would be left with rows DELETEd but not
	// yet REPLACEd. The continuous-checksum loop's cancellation on sentinel
	// drop hits this race, so we run both statements under a context that
	// ignores the parent's cancellation. The bounded timeout still protects
	// against a hung transaction.
	fixCtx, fixCancel := context.WithTimeout(context.WithoutCancel(ctx), fixChunkTimeout)
	defer fixCancel()
	if _, err := dbconn.RetryableTransaction(fixCtx, c.db, dbconn.ErrorOnDupKey, c.dbConfig, deleteStmt); err != nil {
		return fmt.Errorf("failed to delete existing rows: %w", err)
	}
	if _, err := dbconn.RetryableTransaction(fixCtx, c.db, dbconn.ErrorOnDupKey, c.dbConfig, replaceStmt); err != nil {
		return fmt.Errorf("failed to replace chunk data: %w", err)
	}
	return nil
}

func (c *SingleChecker) isHealthy(ctx context.Context) bool {
	c.Lock()
	defer c.Unlock()
	if ctx.Err() != nil {
		return false
	}
	return !c.isInvalid
}

func (c *SingleChecker) StartTime() time.Time {
	c.Lock()
	defer c.Unlock()
	return c.startTime
}

func (c *SingleChecker) ExecTime() time.Duration {
	c.Lock()
	defer c.Unlock()
	return c.execTime
}

// DifferencesFound returns the number of chunks where a source/target
// mismatch was detected in the most recent (or in-flight) pass. Used by
// the continuous-checksum loop to decide whether a cancellation swallow
// is safe.
func (c *SingleChecker) DifferencesFound() uint64 {
	return c.differencesFound.Load()
}

func (c *SingleChecker) setInvalid(newVal bool) {
	c.Lock()
	defer c.Unlock()
	c.isInvalid = newVal
}

func (c *SingleChecker) initConnPool(ctx context.Context) error {
	// Try and catch up before we apply a table lock,
	// since we will need to catch up again with the lock held
	// and we want to minimize that.
	if err := c.feed.Flush(ctx); err != nil {
		return err
	}
	// Lock the source and target table in a trx
	// so the connection is not used by others
	c.logger.Info("starting checksum operation, this will require a table lock")

	// Always acquire lock on the read database
	tableLock, err := dbconn.NewTableLock(ctx, c.db, c.chunker.Tables(), c.dbConfig, c.logger)
	if err != nil {
		return err
	}
	defer utils.CloseAndLogWithContext(ctx, tableLock)
	// We only have a reader, so flush the read connection.
	if err := c.feed.FlushUnderTableLock(ctx, []*dbconn.TableLock{tableLock}); err != nil {
		return err
	}

	// Assert that the change set is empty. This should always
	// be the case because we are under a lock.
	if !c.feed.AllChangesFlushed() {
		return change.ErrChangesNotFlushed
	}
	// Create a set of connections which can be used to checksum
	// The table. They MUST be created before the lock is released
	// with REPEATABLE-READ and a consistent snapshot (or dummy read)
	// to initialize the read-view.
	c.trxPool, err = dbconn.NewTrxPool(ctx, c.db, c.concurrency, c.dbConfig)
	if err != nil {
		return err
	}

	return nil
}

func (c *SingleChecker) Run(ctx context.Context) error {
	// Set startTime under lock to prevent race with StartTime() method
	c.Lock()
	c.startTime = time.Now()
	startTime := c.startTime // capture for defer
	c.Unlock()

	defer func() {
		c.execTime = time.Since(startTime)
	}()

	// A previous Run may have left the checker poisoned (isInvalid=true from
	// an errored attempt); every Run starts healthy.
	c.setInvalid(false)

	// Try the checksum up to n times if differences are found and we can fix them
	var lastErr error
	for attempt := 1; attempt <= c.maxRetries; attempt++ {
		if attempt > 1 {
			c.logger.Error("checksum failed, retrying", "attempt", attempt, "maxRetries", c.maxRetries)
			// Reset the chunker to start from the beginning
			if err := c.chunker.Reset(); err != nil {
				return fmt.Errorf("failed to reset chunker for retry: %w", err)
			}
			// Reset differences found counter
			c.differencesFound.Store(0)
			// Reset the invalid flag left set by the failed attempt: it makes
			// isHealthy() false, which would skip every chunk and turn this
			// retry into a vacuous pass.
			c.setInvalid(false)
		}

		// If the parent context is already cancelled, retrying is pointless —
		// every subsequent attempt will fail the same way at the first
		// ctx-aware call. Bail out with the cancellation cause directly so
		// the caller sees the real reason rather than the generic
		// "checksum failed after N attempts" wrapper below.
		if err := ctx.Err(); err != nil {
			return err
		}

		// Run the checksum with yield support. A single checksum pass may be
		// split across multiple runChecksum calls if the yield timeout fires.
		// Between yields we release the REPEATABLE READ transactions to limit
		// InnoDB history list length (HLL) growth, then re-acquire a table lock
		// and fresh snapshot before resuming from the low watermark.
		if err := c.runChecksumWithYield(ctx); err != nil {
			c.logger.Error("checksum encountered an error", "error", err)
			lastErr = err
			continue
		}

		// If we are here, the checksum passed.
		// But we don't know if differences were found and chunks were recopied.
		// We want to know it passed without finding differences.
		if c.differencesFound.Load() == 0 {
			c.logger.Info("checksum passed")
			return nil
		}
		// Differences were found and (because we got here) recopied. Record
		// this as the "last attempt outcome" so the exhausted-retries error
		// below can distinguish it from a hard error path.
		lastErr = nil
	}

	// Retries exhausted. There are two distinct shapes of failure here:
	//
	//   1. Every attempt returned an error (lastErr != nil) — e.g. a
	//      transient context cancellation, a connection issue, or a bug
	//      that surfaces as an error rather than a row diff. Surface the
	//      underlying error verbatim so it's actually triagable.
	//
	//   2. Every attempt completed but kept finding row differences
	//      (lastErr == nil) — this is the original "lossy ALTER or bug"
	//      shape (e.g. adding a UNIQUE INDEX to non-unique data, or a real
	//      bug in Spirit's copy phase). Keep the original guidance.
	if lastErr != nil {
		return fmt.Errorf("checksum errored on every attempt (%d/%d); last error: %w", c.maxRetries, c.maxRetries, lastErr)
	}
	return fmt.Errorf("checksum found differences on every attempt (%d/%d). This likely indicates either a bug in Spirit, or a manual modification to the _new table outside of Spirit. Please report @ github.com/block/spirit", c.maxRetries, c.maxRetries)
}

// runChecksumWithYield runs the checksum, automatically yielding and resuming
// when the yield timeout expires. Each yield releases the long-running
// REPEATABLE READ transactions (reducing HLL pressure), then re-acquires a
// table lock and fresh snapshot before resuming from the low watermark.
func (c *SingleChecker) runChecksumWithYield(ctx context.Context) error {
	for {
		err := c.runChecksum(ctx)
		if !errors.Is(err, ErrYieldTimeout) {
			return err
		}
		// The yield timeout fired. Get the low watermark so we can resume.
		watermark, wmErr := c.chunker.GetLowWatermark()
		if wmErr != nil {
			// If the watermark isn't ready (e.g. the timeout fired before any
			// chunks were processed), reset and start over rather than failing.
			if errors.Is(wmErr, table.ErrWatermarkNotReady) {
				c.yieldsPerformed.Add(1)
				c.logger.Info("checksum yielding but no watermark available, restarting from beginning",
					"yieldTimeout", c.yieldTimeout,
				)
				c.setInvalid(false)
				if resetErr := c.chunker.Reset(); resetErr != nil {
					return fmt.Errorf("failed to reset chunker after yield: %w", resetErr)
				}
				continue
			}
			return fmt.Errorf("failed to get low watermark after yield: %w", wmErr)
		}
		c.yieldsPerformed.Add(1)
		c.logger.Info("checksum yielding to release long-running transactions",
			"watermark", watermark,
			"yieldTimeout", c.yieldTimeout,
		)
		// Reset the isInvalid flag since we are resuming, not failing.
		c.setInvalid(false)
		// Re-open the chunker at the watermark position.
		if err := c.chunker.OpenAtWatermark(watermark); err != nil {
			return fmt.Errorf("failed to resume chunker from watermark after yield: %w", err)
		}
		// Loop back to runChecksum which will re-acquire the table lock
		// and create fresh REPEATABLE READ transactions.
	}
}

func (c *SingleChecker) runChecksum(ctx context.Context) error {
	// initConnPool initialize the connection pool.
	// This is done under a table lock which is acquired in this func.
	// It is released as the func is returned.
	if err := c.initConnPool(ctx); err != nil {
		return err
	}
	c.logger.Info("table unlocked, starting checksum")

	// Start the periodic flush *after* the table lock is released.
	// This must not run while initConnPool holds the table lock, because
	// the periodic flush executes DML (INSERT/DELETE) against the locked
	// table, which would deadlock with the lock holder.
	c.feed.StartPeriodicFlush(ctx, change.DefaultFlushInterval)
	defer c.feed.StopPeriodicFlush()

	// Create a yield-timeout context to limit how long a single checksum pass
	// can hold REPEATABLE READ transactions open. Long-running read views cause
	// InnoDB history list length (HLL) growth, so we periodically yield to
	// release them and re-acquire fresh ones.
	yieldCtx, yieldCancel := context.WithTimeout(ctx, c.yieldTimeout)
	defer yieldCancel()

	g, errGrpCtx := errgroup.WithContext(yieldCtx)
	g.SetLimit(c.concurrency)
	for !c.chunker.IsRead() && c.isHealthy(errGrpCtx) {
		g.Go(func() error {
			chunk, err := c.chunker.Next()
			if err != nil {
				if errors.Is(err, table.ErrTableIsRead) {
					return nil
				}
				c.setInvalid(true)
				return err
			}
			if err := c.ChecksumChunk(errGrpCtx, c.trxPool, chunk); err != nil {
				c.setInvalid(true)
				return err
			}
			return nil
		})
	}
	// wait for all work to finish
	err1 := g.Wait()
	// Regardless of err state, we should attempt to rollback the transactions.
	// They are likely holding metadata locks, which will block further operations
	// like cleanup or cut-over.
	closeErr := c.trxPool.Close()
	// Distinguish between the yield timeout expiring and the parent context
	// being canceled. If the parent context is still valid but the yield context
	// expired, this was a yield — not a failure. We resume from the watermark
	// when either the chunker hasn't finished, or an in-flight query/rollback
	// was cancelled by the deadline (err1/closeErr != nil): the latter happens
	// when the timeout fires on the final chunk after all chunks have been
	// dispatched (IsRead() is already true), so !IsRead() alone would miss it.
	// This check must come before inspecting closeErr or err1, because the yield
	// timeout can cause both transaction rollback errors and context errors from
	// in-flight queries.
	if ctx.Err() == nil && yieldCtx.Err() != nil && (!c.chunker.IsRead() || err1 != nil || closeErr != nil) {
		return ErrYieldTimeout
	}
	if closeErr != nil {
		return closeErr
	}
	if err1 != nil {
		c.logger.Error("checksum failed")
		return err1
	}
	return nil
}
