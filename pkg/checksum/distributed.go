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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/repl"
	"github.com/block/spirit/pkg/table"
	"golang.org/x/sync/errgroup"
)

type DistributedChecker struct {
	sync.Mutex
	concurrency      int
	feed             *repl.Client
	db               *sql.DB
	applier          applier.Applier
	trxPool          *dbconn.TrxPool   // reader trx pool (source)
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
}

var _ Checker = (*DistributedChecker)(nil)

func (c *DistributedChecker) ChecksumChunk(ctx context.Context, trxPool *dbconn.TrxPool, chunk *table.Chunk) error {
	startTime := time.Now()

	// Get source transaction
	srcTrx, err := trxPool.Get()
	if err != nil {
		return err
	}
	defer trxPool.Put(srcTrx)

	c.logger.Debug("checksumming chunk", "chunk", chunk.String())

	// Query source
	source := fmt.Sprintf("SELECT BIT_XOR(CRC32(CONCAT(%s))) as checksum, count(*) as c FROM %s WHERE %s",
		c.intersectColumns(chunk),
		chunk.Table.QuotedName,
		chunk.String(),
	)
	var sourceChecksum int64
	var sourceCount uint64
	err = srcTrx.QueryRow(source).Scan(&sourceChecksum, &sourceCount)
	if err != nil {
		return err
	}

	// Query all targets and aggregate results
	targetQuery := fmt.Sprintf("SELECT BIT_XOR(CRC32(CONCAT(%s))) as checksum, count(*) as c FROM %s WHERE %s",
		c.intersectColumns(chunk),
		chunk.NewTable.QuotedName,
		chunk.String(),
	)

	// Aggregate checksums and counts from all targets
	var aggregatedChecksum int64 = 0
	var aggregatedCount uint64 = 0

	for i, targetTrxPool := range c.targetTrxPools {
		targetTrx, err := targetTrxPool.Get()
		if err != nil {
			return fmt.Errorf("failed to get transaction for target %d: %w", i, err)
		}
		defer targetTrxPool.Put(targetTrx)

		var targetChecksum int64
		var targetCount uint64
		err = targetTrx.QueryRow(targetQuery).Scan(&targetChecksum, &targetCount)
		if err != nil {
			return fmt.Errorf("failed to query target %d: %w", i, err)
		}

		// Aggregate: XOR the checksums, sum the counts
		aggregatedChecksum ^= targetChecksum
		aggregatedCount += targetCount

		c.logger.Debug("target checksum", "targetID", i, "checksum", targetChecksum, "count", targetCount)
	}

	c.logger.Debug("aggregated checksum", "checksum", aggregatedChecksum, "count", aggregatedCount)

	if sourceChecksum != aggregatedChecksum {
		// The checksums do not match, so we first need
		// to inspect closely and report on the differences.
		c.differencesFound.Add(1)
		c.logger.Warn("checksum mismatch for chunk", "chunk", chunk.String(),
			"sourceChecksum", sourceChecksum, "targetChecksum", aggregatedChecksum,
			"sourceCount", sourceCount, "targetCount", aggregatedCount)

		// For distributed case, we can't easily inspect differences across multiple targets
		// So we'll just log the mismatch and proceed to fix
		c.logger.Warn("distributed checksum mismatch detected, will recopy chunk")

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
	c.chunker.Feedback(chunk, time.Since(startTime), aggregatedCount)
	return nil
}

// GetProgress returns the progress of the checker
// this is really just a proxy to the chunker progress.
func (c *DistributedChecker) GetProgress() string {
	rowsProcessed, _, totalRows := c.chunker.Progress()
	pct := float64(0)
	if totalRows > 0 {
		pct = float64(rowsProcessed) / float64(totalRows) * 100
	}
	return fmt.Sprintf("%d/%d %.2f%%", rowsProcessed, totalRows, pct)
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

	// Step 1: Delete all rows in the chunk range from all targets
	// This ensures we remove any extra rows that shouldn't be there
	deleteStmt := fmt.Sprintf("DELETE FROM %s WHERE %s", chunk.NewTable.QuotedName, chunk.String())

	targets := c.applier.GetTargets()
	for i, target := range targets {
		c.logger.Debug("deleting chunk range from target", "targetID", i, "chunk", chunk.String())
		_, err := dbconn.RetryableTransaction(ctx, target.DB, false, c.dbConfig, deleteStmt)
		if err != nil {
			return fmt.Errorf("failed to delete chunk from target %d: %w", i, err)
		}
	}

	// Step 2: Read all rows from the source chunk
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s",
		strings.Join(chunk.Table.Columns, ", "),
		chunk.Table.QuotedName,
		chunk.String(),
	)

	c.logger.Debug("reading chunk data for recopy", "chunk", chunk.String())
	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query chunk data: %w", err)
	}
	defer rows.Close()

	// Collect all rows
	var rowData [][]any
	for rows.Next() {
		values := make([]any, len(chunk.Table.Columns))
		valuePtrs := make([]any, len(chunk.Table.Columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
		rowData = append(rowData, values)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %w", err)
	}

	c.logger.Info("recopying chunk via applier", "chunk", chunk.String(), "rowCount", len(rowData))

	// Step 3: Use the applier to write the rows to all targets
	// The applier will handle distribution across shards if needed
	if len(rowData) > 0 {
		done := make(chan error, 1)
		err = c.applier.Apply(ctx, chunk, rowData, func(affectedRows int64, err error) {
			if err != nil {
				c.logger.Error("failed to recopy chunk via applier", "error", err)
				done <- err
			} else {
				c.logger.Debug("successfully recopied chunk via applier", "affectedRows", affectedRows)
				done <- nil
			}
		})
		if err != nil {
			return fmt.Errorf("failed to initiate recopy via applier: %w", err)
		}

		// Wait for the apply to complete
		select {
		case err := <-done:
			if err != nil {
				return fmt.Errorf("recopy via applier failed: %w", err)
			}
		case <-ctx.Done():
			return ctx.Err()
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

func (c *DistributedChecker) setInvalid(newVal bool) {
	c.Lock()
	defer c.Unlock()
	c.isInvalid = newVal
}

func (c *DistributedChecker) initConnPool(ctx context.Context) error {
	// Try and catch up before we apply a table lock,
	// since we will need to catch up again with the lock held
	// and we want to minimize that.
	if err := c.feed.Flush(ctx); err != nil {
		return err
	}

	c.logger.Info("starting distributed checksum operation, this will require table locks")

	// Get all targets from the applier
	targets := c.applier.GetTargets()
	if len(targets) == 0 {
		return errors.New("no targets available from applier")
	}

	c.logger.Info("distributed checksum will lock tables on all targets", "targetCount", len(targets))

	// Extract the source and target tables from the chunker's Tables() method
	// By convention, every second table is the "new" table.
	// However, in a move we don't usually specify the writeTable when creating the chunker,
	// so it automatically gets set to the same as the readTable.
	var readTables, writeTables []*table.TableInfo
	for i, tbl := range c.chunker.Tables() {
		if i%2 == 0 {
			readTables = append(readTables, tbl)
		} else {
			writeTables = append(writeTables, tbl)
		}
	}

	// Lock source tables
	sourceTableLock, err := dbconn.NewTableLock(ctx, c.db, readTables, c.dbConfig, c.logger)
	if err != nil {
		return fmt.Errorf("failed to lock source tables: %w", err)
	}
	defer sourceTableLock.Close()

	// Lock tables on all targets
	var targetTableLocks []*dbconn.TableLock
	for i, target := range targets {
		targetLock, err := dbconn.NewTableLock(ctx, target.DB, writeTables, c.dbConfig, c.logger)
		if err != nil {
			// Clean up any locks we've already acquired
			for _, lock := range targetTableLocks {
				lock.Close()
			}
			return fmt.Errorf("failed to lock tables on target %d: %w", i, err)
		}
		targetTableLocks = append(targetTableLocks, targetLock)
	}
	defer func() {
		for _, lock := range targetTableLocks {
			lock.Close()
		}
	}()

	// With the lock(s) held, flush one more time under the lock tables.
	// We use the first target's lock for flushing (they should all be consistent)
	if err := c.feed.FlushUnderTableLock(ctx, targetTableLocks[0]); err != nil {
		return fmt.Errorf("failed to flush under table lock: %w", err)
	}

	// Assert that the change set is empty. This should always
	// be the case because we are under a lock.
	if !c.feed.AllChangesFlushed() {
		return errors.New("not all changes flushed")
	}

	// Create a transaction pool for the source
	c.trxPool, err = dbconn.NewTrxPool(ctx, c.db, c.concurrency, c.dbConfig)
	if err != nil {
		return fmt.Errorf("failed to create source transaction pool: %w", err)
	}

	// Create transaction pools for each target
	// These MUST be created before the locks are released
	// with REPEATABLE-READ and a consistent snapshot
	c.targetTrxPools = make([]*dbconn.TrxPool, len(targets))
	for i, target := range targets {
		targetTrxPool, err := dbconn.NewTrxPool(ctx, target.DB, c.concurrency, c.dbConfig)
		if err != nil {
			// Clean up any pools we've already created
			if c.trxPool != nil {
				c.trxPool.Close()
			}
			for j := range i {
				if c.targetTrxPools[j] != nil {
					c.targetTrxPools[j].Close()
				}
			}
			return fmt.Errorf("failed to create transaction pool for target %d: %w", i, err)
		}
		c.targetTrxPools[i] = targetTrxPool
	}

	c.logger.Info("distributed checksum transaction pools created", "targetCount", len(c.targetTrxPools))
	return nil
}

func (c *DistributedChecker) Run(ctx context.Context) error {
	// Set startTime under lock to prevent race with StartTime() method
	c.Lock()
	c.startTime = time.Now()
	startTime := c.startTime // capture for defer
	c.Unlock()

	// This is only really used if there are checksum failures
	// and chunks need to be recopied.
	if err := c.applier.Start(ctx); err != nil {
		return fmt.Errorf("failed to start applier: %w", err)
	}

	defer func() {
		c.execTime = time.Since(startTime)
		_ = c.applier.Stop()
	}()

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
		}

		// Run the actual checksum
		if err := c.runChecksum(ctx); err != nil {
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
	go c.feed.StartPeriodicFlush(ctx, repl.DefaultFlushInterval)
	defer c.feed.StopPeriodicFlush()

	g, errGrpCtx := errgroup.WithContext(ctx)
	g.SetLimit(c.concurrency)
	for !c.chunker.IsRead() && c.isHealthy(errGrpCtx) {
		g.Go(func() error {
			chunk, err := c.chunker.Next()
			if err != nil {
				if err == table.ErrTableIsRead {
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
	// Regardless of err state, we should attempt to rollback the transactions
	// in all transaction pools. They are likely holding metadata locks, which will block
	// further operations like cleanup or cut-over.
	if err := c.trxPool.Close(); err != nil {
		return err
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
	if err1 != nil {
		c.logger.Error("checksum failed")
		return err1
	}
	return nil
}

// intersectColumns is similar to utils.IntersectColumns, but it
// wraps an IFNULL(), ISNULL() and cast operation around the columns.
// The cast is to c.newTable type.
func (c *DistributedChecker) intersectColumns(chunk *table.Chunk) string {
	var intersection []string
	for _, col := range chunk.Table.NonGeneratedColumns {
		for _, col2 := range chunk.NewTable.NonGeneratedColumns {
			if col == col2 {
				// Column exists in both, so we add intersection wrapped in
				// IFNULL, ISNULL and CAST.
				intersection = append(intersection, "IFNULL("+chunk.NewTable.WrapCastType(col)+",''), ISNULL(`"+col+"`)")
			}
		}
	}
	return strings.Join(intersection, ", ")
}
