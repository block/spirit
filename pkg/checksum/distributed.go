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

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
	"golang.org/x/sync/errgroup"
)

// sourcePool pairs a source database connection with its transaction pool.
// Used during checksum to query all sources for a given chunk range.
type sourcePool struct {
	db      *sql.DB
	trxPool *dbconn.TrxPool
}

type DistributedChecker struct {
	sync.Mutex

	concurrency      int
	feeds            []change.Source
	sourceDBs        []*sql.DB // all source database connections
	applier          applier.Applier
	sourcePools      []sourcePool      // one per source DB, created during initConnPool
	targetTrxPools   []*dbconn.TrxPool // transaction pools for each target
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

var _ Checker = (*DistributedChecker)(nil)

func (c *DistributedChecker) ChecksumChunk(ctx context.Context, chunk *table.Chunk) error {
	startTime := time.Now()

	c.logger.Debug("checksumming chunk", "chunk", chunk.String())

	// Build the checksum query fragments. The same WHERE clause applies to
	// both sources and targets since all schemas are identical, but the
	// column expressions are side-dependent: JSON columns round-trip through
	// text on the source and render strictly on the target (see
	// table.castExpr). Move/sync writes are text-mediated — the applier
	// writes rendered text that the target re-parses — so the same
	// text-image contract as the single-server checksum applies here. This
	// does assume source and target servers parse JSON text identically; if
	// a future server version changed parse behavior (e.g. fixed the double
	// misrounding bugs), a cross-version move of affected values would fail
	// the checksum safely rather than pass silently.
	sourceChecksumCols, targetChecksumCols, err := chunk.ColumnMapping.ChecksumExprs()
	if err != nil {
		return err
	}
	whereClause := chunk.String()

	// Query ALL sources and aggregate results.
	// BIT_XOR is associative/commutative, so XOR-ing per-source checksums
	// produces the same result as checksumming all rows in one table.
	// The count is simply summed.
	var sourceChecksum int64
	var sourceCount uint64
	for i := range c.sourcePools {
		srcTrx, err := c.sourcePools[i].trxPool.Get()
		if err != nil {
			return fmt.Errorf("failed to get transaction for source %d: %w", i, err)
		}
		defer c.sourcePools[i].trxPool.Put(srcTrx)

		sourceQuery := fmt.Sprintf("SELECT BIT_XOR(CRC32(CONCAT(%s))) as checksum, count(*) as c FROM %s WHERE %s",
			sourceChecksumCols,
			chunk.Table.QuotedTableName,
			whereClause,
		)
		var cs int64
		var cnt uint64
		if err := srcTrx.QueryRowContext(ctx, sourceQuery).Scan(&cs, &cnt); err != nil {
			return fmt.Errorf("failed to query source %d: %w", i, err)
		}
		sourceChecksum ^= cs
		sourceCount += cnt
		c.logger.Debug("source checksum", "sourceID", i, "checksum", cs, "count", cnt)
	}

	// Query ALL targets and aggregate results.
	// Same aggregation logic: XOR checksums, sum counts.
	var targetChecksum int64
	var targetCount uint64
	for i, targetTrxPool := range c.targetTrxPools {
		targetTrx, err := targetTrxPool.Get()
		if err != nil {
			return fmt.Errorf("failed to get transaction for target %d: %w", i, err)
		}
		defer targetTrxPool.Put(targetTrx)

		targetQuery := fmt.Sprintf("SELECT BIT_XOR(CRC32(CONCAT(%s))) as checksum, count(*) as c FROM %s WHERE %s",
			targetChecksumCols,
			chunk.Table.QuotedTableName,
			whereClause,
		)
		var cs int64
		var cnt uint64
		if err := targetTrx.QueryRowContext(ctx, targetQuery).Scan(&cs, &cnt); err != nil {
			return fmt.Errorf("failed to query target %d: %w", i, err)
		}
		targetChecksum ^= cs
		targetCount += cnt
		c.logger.Debug("target checksum", "targetID", i, "checksum", cs, "count", cnt)
	}

	c.logger.Debug("aggregated checksums",
		"sourceChecksum", sourceChecksum, "sourceCount", sourceCount,
		"targetChecksum", targetChecksum, "targetCount", targetCount)

	// Compare BOTH the aggregated checksum and the summed row count. Comparing
	// the count is free (it is summed from the same per-source/per-target
	// queries above) and it is critical in the distributed case: BIT_XOR is
	// pair-cancelling, so if the SAME row exists on two source shards (a
	// resharding bug that violates shard disjointness) its CRCs cancel to
	// zero — a target with ZERO copies of that row would still match on the
	// checksum alone. The summed counts (src=2, target=0) catch it. A count
	// mismatch is treated identically to a checksum mismatch.
	if mismatch := compareChunk(sourceChecksum, targetChecksum, sourceCount, targetCount); mismatch.mismatched() {
		// The source and target do not match, so we first need
		// to inspect closely and report on the differences.
		c.differencesFound.Add(1)
		c.logger.Warn("chunk verification failed", "chunk", chunk.String(),
			"reason", mismatch.reason(sourceCount, targetCount),
			"sourceChecksum", sourceChecksum, "targetChecksum", targetChecksum,
			"sourceCount", sourceCount, "targetCount", targetCount)

		// For distributed case, we can't easily inspect differences across multiple sources/targets
		// So we'll just log the mismatch and proceed to fix
		c.logger.Warn("distributed chunk verification failed, will recopy chunk")

		// Are we allowed to fix the differences? If not, return an error.
		// This is mostly used by the test-suite.
		if !c.fixDifferences {
			return errors.New("checksum mismatch")
		}
		// Since we can fix differences, replace the chunk.
		if err := c.replaceChunk(ctx, chunk); err != nil {
			return err
		}
	}
	// When we give feedback, we need to say how many rows were in the chunk.
	c.chunker.Feedback(chunk, time.Since(startTime), targetCount)
	return nil
}

// GetProgress returns rows verified so far and the total to verify, proxied
// from the chunker.
func (c *DistributedChecker) GetProgress() status.ChecksumProgress {
	rowsProcessed, _, totalRows := c.chunker.Progress()
	return status.ChecksumProgress{RowsChecked: rowsProcessed, RowsTotal: totalRows}
}

// replaceChunk recopies the data from source to targets for a given chunk.
// In the distributed case, we first delete the entire chunk range from all targets,
// then use Apply to recopy the data from the source. This handles both missing rows
// and extra rows on the destination.
func (c *DistributedChecker) replaceChunk(ctx context.Context, chunk *table.Chunk) error {
	c.logger.Warn("recopying chunk via DELETE + Apply", "chunk", chunk.String())

	// We further prevent the chance of deadlocks from the recopying process by only re-copying one chunk at a time.
	// We may revisit this in future, but since conflicts are expected to be low, it should be fine for now.
	c.recopyLock.Lock()
	defer c.recopyLock.Unlock()

	// The fix is split into DELETE-from-targets and Apply-from-sources. If the
	// parent ctx is cancelled between or during these steps, the target side
	// would be left with rows DELETEd but not yet reapplied. The
	// continuous-checksum loop's cancellation on sentinel drop hits this race,
	// so we run the fix under a context that ignores the parent's
	// cancellation. The bounded timeout still protects against a hung apply.
	fixCtx, fixCancel := context.WithTimeout(context.WithoutCancel(ctx), fixChunkTimeout)
	defer fixCancel()

	// Step 1: Delete all rows in the chunk range from all targets
	// This ensures we remove any extra rows that shouldn't be there.
	// Use chunk.Table here to target the chunk's original table name consistently across targets.
	deleteStmt := fmt.Sprintf("DELETE FROM %s WHERE %s", chunk.Table.QuotedTableName, chunk.String())

	targets := c.applier.GetTargets()
	for i, target := range targets {
		c.logger.Debug("deleting chunk range from target", "targetID", i, "chunk", chunk.String(), "table", chunk.Table.TableName)
		_, err := dbconn.RetryableTransaction(fixCtx, target.DB, dbconn.ErrorOnDupKey, c.dbConfig, deleteStmt)
		if err != nil {
			return fmt.Errorf("failed to delete chunk from target %d: %w", i, err)
		}
	}

	// Step 2: Read all rows from ALL sources for the chunk range and merge them.
	// Use NonGeneratedColumns because the applier expects non-generated columns only.
	// This ensures the column ordinals match when the applier extracts the sharding column.
	//
	// JSON columns are deliberately read bare here — no text round-trip cast.
	// Unlike the single-server repair (see table.ColumnMapping.RepairExprs),
	// this path is already text-mediated: the SELECT renders each document to
	// text on the wire and the applier writes it back as a SQL literal that
	// the target re-parses. The repaired row therefore lands as exactly the
	// one-round-trip text image the checksum's source side predicts. Adding a
	// round-trip cast on top would apply parse∘render twice, which does not
	// converge for misparsed doubles.
	columnList := table.QuoteColumns(chunk.Table.NonGeneratedColumns)
	// Use the table name only; each source DB connection determines which database is queried.
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s",
		columnList,
		chunk.Table.QuotedTableName,
		chunk.String(),
	)

	var rowData [][]any
	for i := range c.sourcePools {
		c.logger.Debug("reading chunk data for recopy", "chunk", chunk.String(), "sourceID", i, "table", chunk.Table.TableName)

		rows, err := c.sourcePools[i].db.QueryContext(fixCtx, query)
		if err != nil {
			return fmt.Errorf("failed to query chunk data from source %d: %w", i, err)
		}

		for rows.Next() {
			values := make([]any, len(chunk.Table.NonGeneratedColumns))
			valuePtrs := make([]any, len(chunk.Table.NonGeneratedColumns))
			for j := range values {
				valuePtrs[j] = &values[j]
			}
			if err := rows.Scan(valuePtrs...); err != nil {
				utils.CloseAndLog(rows)
				return fmt.Errorf("failed to scan row from source %d: %w", i, err)
			}
			rowData = append(rowData, values)
		}
		if err := rows.Err(); err != nil {
			utils.CloseAndLog(rows)
			return fmt.Errorf("error iterating rows from source %d: %w", i, err)
		}
		utils.CloseAndLog(rows)
	}

	c.logger.Info("recopying chunk via applier", "chunk", chunk.String(), "rowCount", len(rowData), "sourceCount", len(c.sourcePools))

	// Step 3: Use the applier to write the rows to all targets
	// The applier will handle distribution across shards if needed.
	//
	// The applier's worker goroutines run under context.WithoutCancel(ctx)
	// (see Run() below), so a parent cancellation between the DELETEs above
	// and the worker writes does not by itself cancel the inserts. The
	// remaining limitation is that workers stop when the deferred Stop() at
	// the end of Run runs — if Run returns due to a continuous-checksum
	// cancel while writes are queued, those inserts may be dropped. The
	// continuous-checksum loop's DifferencesFound() gate keeps cutover
	// aborting in that case so the broken state stays internal and is
	// recopied on resume; a tighter fix would scope a worker context to
	// the repair window only.
	if len(rowData) > 0 {
		done := make(chan error, 1)
		applyErr := c.applier.Apply(fixCtx, chunk, rowData, func(affectedRows int64, err error) {
			if err != nil {
				c.logger.Error("failed to recopy chunk via applier", "error", err)
				done <- err
			} else {
				c.logger.Debug("successfully recopied chunk via applier", "affectedRows", affectedRows)
				done <- nil
			}
		})
		if applyErr != nil {
			return fmt.Errorf("failed to initiate recopy via applier: %w", applyErr)
		}

		// Wait for the apply to complete
		select {
		case err := <-done:
			if err != nil {
				return fmt.Errorf("recopy via applier failed: %w", err)
			}
		case <-fixCtx.Done():
			return fixCtx.Err()
		}
	}
	c.logger.Info("successfully recopied chunk", "chunk", chunk.String(), "rowCount", len(rowData))
	return nil
}

func (c *DistributedChecker) isHealthy(ctx context.Context) bool {
	c.Lock()
	defer c.Unlock()
	if ctx.Err() != nil {
		return false
	}
	return !c.isInvalid
}

func (c *DistributedChecker) StartTime() time.Time {
	c.Lock()
	defer c.Unlock()
	return c.startTime
}

func (c *DistributedChecker) ExecTime() time.Duration {
	c.Lock()
	defer c.Unlock()
	return c.execTime
}

// DifferencesFound returns the number of chunks where a source/target
// mismatch was detected in the most recent (or in-flight) pass. Used by
// the continuous-checksum loop to decide whether a cancellation swallow
// is safe.
func (c *DistributedChecker) DifferencesFound() uint64 {
	return c.differencesFound.Load()
}

func (c *DistributedChecker) setInvalid(newVal bool) {
	c.Lock()
	defer c.Unlock()
	c.isInvalid = newVal
}

func (c *DistributedChecker) initConnPool(ctx context.Context) error {
	// Try and catch up before we apply a table lock,
	// since we will need to catch up again with the lock held
	// and we want to minimize that.
	for _, feed := range c.feeds {
		if err := feed.Flush(ctx); err != nil {
			return err
		}
	}

	c.logger.Info("starting distributed checksum operation, this will require table locks")

	// Get all targets from the applier
	targets := c.applier.GetTargets()
	if len(targets) == 0 {
		return errors.New("no targets available from applier")
	}

	c.logger.Info("distributed checksum will lock tables on all targets", "targetCount", len(targets))

	// Collect unique table names from the chunker for locking.
	// Force-kill uses DATABASE() to resolve the schema, so we only need table names.
	allTables := c.chunker.Tables()
	var lockTables []*table.TableInfo
	seen := make(map[string]bool)
	for _, tbl := range allTables {
		if !seen[tbl.TableName] {
			lockTables = append(lockTables, tbl)
			seen[tbl.TableName] = true
		}
	}

	// Lock tables on each source DB. We use c.sourceDBs which is explicitly
	// provided (for N:M moves) or defaults to the single source DB.
	var sourceTableLocks []*dbconn.TableLock
	for i, srcDB := range c.sourceDBs {
		lock, err := dbconn.NewTableLock(ctx, srcDB, lockTables, c.dbConfig, c.logger)
		if err != nil {
			for _, l := range sourceTableLocks {
				utils.CloseAndLogWithContext(ctx, l)
			}
			return fmt.Errorf("failed to lock source tables on source %d: %w", i, err)
		}
		sourceTableLocks = append(sourceTableLocks, lock)
	}
	defer func() {
		for _, lock := range sourceTableLocks {
			utils.CloseAndLogWithContext(ctx, lock)
		}
	}()

	// Lock tables on all targets
	var targetTableLocks []*dbconn.TableLock
	for i, target := range targets {
		targetLock, err := dbconn.NewTableLock(ctx, target.DB, lockTables, c.dbConfig, c.logger)
		if err != nil {
			// Clean up any locks we've already acquired
			for _, lock := range targetTableLocks {
				utils.CloseAndLogWithContext(ctx, lock)
			}
			return fmt.Errorf("failed to lock tables on target %d: %w", i, err)
		}
		targetTableLocks = append(targetTableLocks, targetLock)
	}
	defer func() {
		for _, lock := range targetTableLocks {
			utils.CloseAndLogWithContext(ctx, lock)
		}
	}()

	// With the lock(s) held, flush all feeds one more time.
	// We pass ALL target locks: the applier matches each lock to the target
	// whose connection it was acquired on, so a sharded applier executes
	// each shard's statements under that shard's own lock. Passing only a
	// single lock would route every shard's writes through one server.
	for _, feed := range c.feeds {
		if err := feed.FlushUnderTableLock(ctx, targetTableLocks); err != nil {
			return fmt.Errorf("failed to flush under table lock: %w", err)
		}
	}

	// Assert that the change set is empty on all feeds.
	for i, feed := range c.feeds {
		if !feed.AllChangesFlushed() {
			return fmt.Errorf("feed %d: %w", i, change.ErrChangesNotFlushed)
		}
	}

	// Create transaction pools for each source
	c.sourcePools = make([]sourcePool, 0, len(c.sourceDBs))
	for i, srcDB := range c.sourceDBs {
		pool, err := dbconn.NewTrxPool(ctx, srcDB, c.concurrency, c.dbConfig)
		if err != nil {
			// Clean up pools already created
			for _, sp := range c.sourcePools {
				if err2 := sp.trxPool.Close(); err2 != nil {
					c.logger.Error("failed to close source transaction pool", "error", err2)
				}
			}
			return fmt.Errorf("failed to create source transaction pool for source %d: %w", i, err)
		}
		c.sourcePools = append(c.sourcePools, sourcePool{db: srcDB, trxPool: pool})
	}

	// Create transaction pools for each target
	// These MUST be created before the locks are released
	// with REPEATABLE-READ and a consistent snapshot
	c.targetTrxPools = make([]*dbconn.TrxPool, len(targets))
	for i, target := range targets {
		targetTrxPool, err := dbconn.NewTrxPool(ctx, target.DB, c.concurrency, c.dbConfig)
		if err != nil {
			// Clean up any pools we've already created
			for _, sp := range c.sourcePools {
				if err2 := sp.trxPool.Close(); err2 != nil {
					c.logger.Error("failed to close source transaction pool", "error", err2)
				}
			}
			for j := range i {
				if c.targetTrxPools[j] != nil {
					if err2 := c.targetTrxPools[j].Close(); err2 != nil {
						c.logger.Error("failed to close target transaction pool", "targetIndex", j, "error", err2)
					}
				}
			}
			return fmt.Errorf("failed to create transaction pool for target %d: %w", i, err)
		}
		c.targetTrxPools[i] = targetTrxPool
	}

	c.logger.Info("distributed checksum transaction pools created",
		"sourceCount", len(c.sourcePools),
		"targetCount", len(c.targetTrxPools))
	return nil
}

func (c *DistributedChecker) Run(ctx context.Context) error {
	// Set startTime under lock to prevent race with StartTime() method
	c.Lock()
	c.startTime = time.Now()
	startTime := c.startTime // capture for defer
	c.Unlock()

	// This is only really used if there are checksum failures
	// and chunks need to be recopied. We start the applier under a context
	// that is decoupled from `ctx` so that a parent-ctx cancellation in the
	// middle of a recopy (e.g. a sentinel drop during the continuous-checksum
	// loop) does not abort the applier's worker writes between the DELETE
	// step in replaceChunk and the actual reapply: replaceChunk builds its
	// own fixCtx via context.WithoutCancel, but the workers would otherwise
	// take their cancellation from the ctx that Start was given. The applier
	// is still cleanly shut down via the deferred Stop() when Run returns.
	if err := c.applier.Start(context.WithoutCancel(ctx)); err != nil {
		return fmt.Errorf("failed to start applier: %w", err)
	}

	defer func() {
		c.execTime = time.Since(startTime)
		_ = c.applier.Stop()
	}()

	// A previous Run may have left the checker poisoned (isInvalid=true from
	// an errored attempt); every Run starts healthy.
	c.setInvalid(false)

	// Try the checksum up to n times if differences are found and we can fix them
	for attempt := 1; attempt <= c.maxRetries; attempt++ {
		if attempt > 1 {
			c.logger.Error("checksum failed, retrying", "attempt", attempt, "maxRetries", c.maxRetries)
			// Reset the chunker to start from the beginning
			if err := c.chunker.Reset(); err != nil {
				return fmt.Errorf("failed to reset chunker for retry: %w", err)
			}
			// Reset differences found counter
			c.differencesFound.Store(0)
			// Reset the invalid flag left set by a failed attempt: it makes
			// isHealthy() false, which would skip every chunk and turn this
			// retry into a vacuous pass.
			c.setInvalid(false)
		}

		// If the parent context is already cancelled, retrying is pointless.
		if err := ctx.Err(); err != nil {
			return err
		}

		// Run the checksum with yield support. A single checksum pass may be
		// split across multiple runChecksum calls if the yield timeout fires.
		// Between yields we release the REPEATABLE READ transaction pools and
		// resume from the low watermark with fresh snapshots.
		if err := c.runChecksumWithYield(ctx); err != nil {
			// This is really not expected to fail, since if there are differences
			// it will run the resolver and report the differences in DifferencesFound().
			return err
		}

		// If we are here, the checksum passed.
		// But we don't know if differences were found and chunks were recopied.
		// We want to know it passed without finding differences.
		if c.differencesFound.Load() == 0 {
			c.logger.Info("checksum passed")
			return nil
		}
	}

	// Retries exhausted:
	// This used to say "checksum failed, this should never happen" but that's not entirely true.
	// If the user attempts a lossy schema change such as adding a UNIQUE INDEX to non-unique data,
	// then the checksum will fail. This is entirely expected, and not considered a bug. We should
	// do our best-case to differentiate that we believe this ALTER statement is lossy, and
	// customize the returned error based on it.
	return fmt.Errorf("checksum failed after %d attempts. This likely indicates either a bug in Spirit, or a manual modification to the _new table outside of Spirit. Please report @ github.com/block/spirit", c.maxRetries)
}

// runChecksumWithYield runs the checksum, automatically yielding and resuming
// when the yield timeout expires. Each yield releases the long-running
// REPEATABLE READ transaction pools on sources and targets, then re-acquires
// table locks and fresh snapshots before resuming from the low watermark.
func (c *DistributedChecker) runChecksumWithYield(ctx context.Context) error {
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
				c.logger.Info("distributed checksum yielding but no watermark available, restarting from beginning",
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
		c.logger.Info("distributed checksum yielding to release long-running transactions",
			"watermark", watermark,
			"yieldTimeout", c.yieldTimeout,
		)
		// Reset the isInvalid flag since we are resuming, not failing.
		c.setInvalid(false)
		// Re-open the chunker at the watermark position.
		if err := c.chunker.OpenAtWatermark(watermark); err != nil {
			return fmt.Errorf("failed to resume chunker from watermark after yield: %w", err)
		}
		// Loop back to runChecksum which will re-acquire the table locks
		// and create fresh REPEATABLE READ transactions.
	}
}

func (c *DistributedChecker) runChecksum(ctx context.Context) error {
	// initConnPool initialize the connection pool.
	// This is done under a table lock which is acquired in this func.
	// It is released as the func is returned.
	if err := c.initConnPool(ctx); err != nil {
		return err
	}
	c.logger.Info("table unlocked, starting checksum")

	// Start the periodic flush again *just* for the duration of the checksum.
	// If the checksum is long running, it could block flushing for too long:
	// - If we need to resume from checkpoint, the binlogs may not be there.
	// - If they are there, they will take a huge amount of time to flush
	// - The memory requirements for 1MM deltas seems reasonable, but for a multi-day
	//   checksum it is reasonable to assume it may exceed this.
	for _, feed := range c.feeds {
		feed.StartPeriodicFlush(ctx, change.DefaultFlushInterval)
	}
	defer func() {
		for _, feed := range c.feeds {
			feed.StopPeriodicFlush()
		}
	}()

	// Create a yield-timeout context to limit how long a single checksum pass
	// can hold distributed REPEATABLE READ transaction pools open. Long-running
	// read views cause InnoDB history list length (HLL) growth, so we
	// periodically yield to release them and re-acquire fresh ones.
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
			if err := c.ChecksumChunk(errGrpCtx, chunk); err != nil {
				c.setInvalid(true)
				return err
			}
			return nil
		})
	}
	// wait for all work to finish
	err1 := g.Wait()
	// Regardless of err state, we should attempt to rollback the transactions
	// in all transaction pools. They are likely holding metadata locks, which will block
	// further operations like cleanup or cut-over.
	for i, sp := range c.sourcePools {
		if err := sp.trxPool.Close(); err != nil {
			c.logger.Error("failed to close source transaction pool", "sourceID", i, "error", err)
		}
	}
	// Close all target transaction pools
	for i := range c.targetTrxPools {
		if c.targetTrxPools[i] != nil {
			if err := c.targetTrxPools[i].Close(); err != nil {
				c.logger.Error("failed to close target transaction pool", "targetID", i, "error", err)
				// Continue closing other pools even if one fails
			}
		}
	}
	// Distinguish between the yield timeout expiring and the parent context
	// being canceled. If the parent context is still valid but the yield context
	// expired, this was a yield, not a failure. We resume from the watermark
	// when either the chunker hasn't finished, or an in-flight chunk query was
	// cancelled by the deadline (err1 != nil): the latter happens when the
	// timeout fires on the final chunk after all chunks have been dispatched
	// (IsRead() is already true), so !IsRead() alone would miss it and surface
	// the deadline as a spurious failure. This check must come before inspecting
	// err1, because the yield timeout can cause context errors from in-flight
	// queries.
	if ctx.Err() == nil && yieldCtx.Err() != nil && (!c.chunker.IsRead() || err1 != nil) {
		return ErrYieldTimeout
	}
	if err1 != nil {
		c.logger.Error("checksum failed")
		return err1
	}
	return nil
}
