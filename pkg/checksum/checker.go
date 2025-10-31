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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/repl"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
	"github.com/siddontang/loggers"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type Checker struct {
	sync.Mutex
	concurrency      int
	feed             *repl.Client
	db               *sql.DB
	writeDB          *sql.DB
	trxPool          *dbconn.TrxPool // reader trx pool
	writeTrxPool     *dbconn.TrxPool // optional writer trx pool
	isInvalid        bool
	chunker          table.Chunker
	startTime        time.Time
	ExecTime         time.Duration
	dbConfig         *dbconn.DBConfig
	logger           loggers.Advanced
	fixDifferences   bool
	differencesFound atomic.Uint64
	recopyLock       sync.Mutex
	maxRetries       int
}

type CheckerConfig struct {
	Concurrency     int
	TargetChunkTime time.Duration
	DBConfig        *dbconn.DBConfig
	Logger          loggers.Advanced
	FixDifferences  bool
	Watermark       string  // optional; defines a watermark to start from
	WriteDB         *sql.DB // optional; use a different DB for the "new" side.
	MaxRetries      int
}

func NewCheckerDefaultConfig() *CheckerConfig {
	return &CheckerConfig{
		Concurrency:     4,
		TargetChunkTime: 1000 * time.Millisecond,
		DBConfig:        dbconn.NewDBConfig(),
		Logger:          logrus.New(),
		FixDifferences:  false,
		MaxRetries:      3,
	}
}

// NewChecker creates a new checksum object.
func NewChecker(db *sql.DB, chunker table.Chunker, feed *repl.Client, config *CheckerConfig) (*Checker, error) {
	if feed == nil {
		return nil, errors.New("feed must be non-nil")
	}
	if chunker == nil {
		return nil, errors.New("chunker must be non-nil")
	}
	if config.DBConfig == nil {
		config.DBConfig = dbconn.NewDBConfig()
	}
	if config.MaxRetries == 0 {
		config.MaxRetries = 3
	}
	checksum := &Checker{
		concurrency:    config.Concurrency,
		db:             db,
		feed:           feed,
		chunker:        chunker,
		dbConfig:       config.DBConfig,
		logger:         config.Logger,
		fixDifferences: config.FixDifferences,
		writeDB:        config.WriteDB,
		maxRetries:     config.MaxRetries,
	}
	return checksum, nil
}

func (c *Checker) ChecksumChunk(ctx context.Context, trxPool *dbconn.TrxPool, chunk *table.Chunk) error {
	startTime := time.Now()
	srcTrx, err := trxPool.Get()
	if err != nil {
		return err
	}
	defer trxPool.Put(srcTrx)
	writeTrx := srcTrx // default the writeTrx to the srcTrx
	if c.writeTrxPool != nil {
		writeTrx, err = c.writeTrxPool.Get()
		if err != nil {
			return err
		}
		defer c.writeTrxPool.Put(writeTrx)
	}
	c.logger.Debugf("checksumming chunk: %s", chunk.String())
	source := fmt.Sprintf("SELECT BIT_XOR(CRC32(CONCAT(%s))) as checksum, count(*) as c FROM %s WHERE %s",
		c.intersectColumns(chunk),
		chunk.Table.QuotedName,
		chunk.String(),
	)
	target := fmt.Sprintf("SELECT BIT_XOR(CRC32(CONCAT(%s))) as checksum, count(*) as c FROM %s WHERE %s",
		c.intersectColumns(chunk),
		chunk.NewTable.QuotedName,
		chunk.String(),
	)
	var sourceChecksum, targetChecksum int64
	var sourceCount, targetCount uint64
	err = srcTrx.QueryRow(source).Scan(&sourceChecksum, &sourceCount)
	if err != nil {
		return err
	}
	err = writeTrx.QueryRow(target).Scan(&targetChecksum, &targetCount)
	if err != nil {
		return err
	}
	if sourceChecksum != targetChecksum {
		// The checksums do not match, so we first need
		// to inspect closely and report on the differences.
		c.differencesFound.Add(1)
		c.logger.Warnf("checksum mismatch for chunk %s: source %d != target %d sourceCount: %d targetCount: %d", chunk.String(), sourceChecksum, targetChecksum, sourceCount, targetCount)
		if err := c.inspectDifferences(srcTrx, writeTrx, chunk); err != nil {
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

// GetProgress returns the progress of the checker
// this is really just a proxy to the chunker progress.
func (c *Checker) GetProgress() string {
	rowsProcessed, _, totalRows := c.chunker.Progress()
	pct := float64(0)
	if totalRows > 0 {
		pct = float64(rowsProcessed) / float64(totalRows) * 100
	}
	return fmt.Sprintf("%d/%d %.2f%%", rowsProcessed, totalRows, pct)
}

// inspectDifferences looks at the chunk and tries to find differences.
// For cross-database scenarios, it queries each database separately and compares in memory.
func (c *Checker) inspectDifferences(srcTrx, writeTrx *sql.Tx, chunk *table.Chunk) error {
	c.logger.Infof("inspecting differences for chunk: %s", chunk.String())

	// Query source database for row checksums
	sourceQuery := fmt.Sprintf("SELECT CRC32(CONCAT(%s)) as row_checksum, CONCAT_WS(',', %s) as pk FROM %s WHERE %s",
		c.intersectColumns(chunk),
		strings.Join(chunk.Table.KeyColumns, ", "),
		chunk.Table.QuotedName,
		chunk.String(),
	)

	sourceRows, err := srcTrx.Query(sourceQuery)
	if err != nil {
		return fmt.Errorf("failed to query source rows: %w", err)
	}
	defer sourceRows.Close()

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

	// Query target database for row checksums
	targetQuery := fmt.Sprintf("SELECT CRC32(CONCAT(%s)) as row_checksum, CONCAT_WS(',', %s) as pk FROM %s WHERE %s",
		c.intersectColumns(chunk),
		strings.Join(chunk.NewTable.KeyColumns, ", "),
		chunk.NewTable.QuotedName,
		chunk.String(),
	)

	targetRows, err := writeTrx.Query(targetQuery)
	if err != nil {
		return fmt.Errorf("failed to query target rows: %w", err)
	}
	defer targetRows.Close()

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
				c.logger.Warnf("inspection revealed row checksum mismatch for pk: %s: source %s != target %s", pk, sourceChecksum, checksum)
			}
		} else {
			c.logger.Warnf("inspection revealed row does not exist in source for pk: %s", pk)
		}
	}
	if err := targetRows.Err(); err != nil {
		return fmt.Errorf("error iterating target rows: %w", err)
	}

	// Check for rows that exist in source but not in target
	for pk, sourceChecksum := range sourceChecksums {
		if _, exists := targetChecksums[pk]; !exists {
			c.logger.Warnf("inspection revealed row does not exist in target for pk: %s (source checksum: %s)", pk, sourceChecksum)
		}
	}

	return nil // managed to inspect differences
}

// rowData represents a single row with all its column values
type rowData struct {
	values []any
}

// readChunkData reads all rows from a chunk into memory
// we will write it shortly in writeChunkData.
func (c *Checker) readChunkData(ctx context.Context, chunk *table.Chunk) ([]rowData, error) {
	// Build the SELECT query to read full row data
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s",
		strings.Join(chunk.Table.Columns, ", "),
		chunk.Table.QuotedName,
		chunk.String(),
	)
	c.logger.Debugf("reading chunk data: %s, query: %s", chunk.String(), query)
	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query chunk data: %w", err)
	}
	defer rows.Close()

	var rowDataList []rowData
	for rows.Next() {
		// Create slice to hold the row values
		values := make([]any, len(chunk.Table.Columns))
		valuePtrs := make([]any, len(chunk.Table.Columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		// Scan the row
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		rowDataList = append(rowDataList, rowData{values: values})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}
	c.logger.Debugf("read %d rows from chunk %s", len(rowDataList), chunk.String())
	return rowDataList, nil
}

// writeRowsToTarget writes rows to the target database
// This is adapted from the buffered copier and subscription buffered implementations
// When there is a writeDB we currently expect the source/destination to have
// identical schema, so we don't need to get intersecting columns.
func (c *Checker) writeRowsToTarget(ctx context.Context, chunk *table.Chunk, rows []rowData) error {
	if len(rows) == 0 {
		return nil
	}

	// Build VALUES clauses for all rows
	var valuesClauses []string
	for _, row := range rows {
		var values []string
		for i, value := range row.values {
			columnType, ok := chunk.NewTable.GetColumnMySQLType(chunk.Table.Columns[i])
			if !ok {
				return fmt.Errorf("column %s not found in table info", chunk.Table.Columns[i])
			}
			values = append(values, utils.EscapeMySQLType(columnType, value))
		}
		valuesClauses = append(valuesClauses, fmt.Sprintf("(%s)", strings.Join(values, ", ")))
	}

	// Build the REPLACE statement for cross-database operation
	query := fmt.Sprintf("REPLACE INTO %s (%s) VALUES %s",
		chunk.NewTable.QuotedName,
		strings.Join(chunk.Table.Columns, ", "),
		strings.Join(valuesClauses, ", "),
	)
	c.logger.Debugf("writing %d rows to %s", len(rows), chunk.NewTable.QuotedName)

	// Execute the batch insert/replace
	_, err := dbconn.RetryableTransaction(ctx, c.writeDB, false, c.dbConfig, query)
	if err != nil {
		return fmt.Errorf("failed to execute batch replace: %w", err)
	}
	return nil
}

// replaceChunk recopies the data from table to newTable for a given chunk.
// For cross-database operations, this reads the data from the source and writes it to the target.
// Note that the chunk is dynamically sized based on the target-time that it took
// to *read* data in the checksum. This could be substantially longer than the time
// that it takes to copy the data. Maybe in future we could consider splitting
// the chunk here, but this is expected to be a very rare situation, so a small
// stall from an XL sized chunk is considered acceptable.
func (c *Checker) replaceChunk(ctx context.Context, chunk *table.Chunk) error {
	c.logger.Warnf("recopying chunk: %s", chunk.String())

	// We further prevent the chance of deadlocks from the recopying process by only re-copying one chunk at a time.
	// We may revisit this in future, but since conflicts are expected to be low, it should be fine for now.
	c.recopyLock.Lock()
	defer c.recopyLock.Unlock()

	// Construct a delete statement to remove existing rows in the target chunk
	deleteStmt := "DELETE FROM " + chunk.NewTable.QuotedName + " WHERE " + chunk.String()

	if c.writeDB != nil {
		// For cross-database operations, we use the following methodology:
		// 1. Delete existing rows in the target chunk
		// 2. Read all rows from the source chunk into memory
		// 3. Write all rows to the target chunk
		//
		// Technically between read and write we could introduce inconsistencies
		// from a lost-update. We currently accept this risk, since on any
		// checksum failure we always retry the checksum, but in future this could
		// be mitigated by pausing the replication feed briefly.
		if _, err := dbconn.RetryableTransaction(ctx, c.writeDB, false, c.dbConfig, deleteStmt); err != nil {
			return fmt.Errorf("failed to delete existing rows: %w", err)
		}
		rows, err := c.readChunkData(ctx, chunk)
		if err != nil {
			return fmt.Errorf("failed to read chunk data: %w", err)
		}
		if err := c.writeRowsToTarget(ctx, chunk, rows); err != nil {
			return fmt.Errorf("failed to write rows to target: %w", err)
		}
	} else {
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
		replaceStmt := fmt.Sprintf("REPLACE INTO %s (%s) SELECT %s FROM %s WHERE %s",
			chunk.NewTable.QuotedName,
			utils.IntersectNonGeneratedColumns(chunk.Table, chunk.NewTable),
			utils.IntersectNonGeneratedColumns(chunk.Table, chunk.NewTable),
			chunk.Table.QuotedName,
			chunk.String(),
		)
		if _, err := dbconn.RetryableTransaction(ctx, c.db, false, c.dbConfig, deleteStmt); err != nil {
			return fmt.Errorf("failed to delete existing rows: %w", err)
		}
		if _, err := dbconn.RetryableTransaction(ctx, c.db, false, c.dbConfig, replaceStmt); err != nil {
			return fmt.Errorf("failed to replace chunk data: %w", err)
		}
	}
	return nil
}

func (c *Checker) isHealthy(ctx context.Context) bool {
	c.Lock()
	defer c.Unlock()
	if ctx.Err() != nil {
		return false
	}
	return !c.isInvalid
}

func (c *Checker) StartTime() time.Time {
	c.Lock()
	defer c.Unlock()
	return c.startTime
}

func (c *Checker) setInvalid(newVal bool) {
	c.Lock()
	defer c.Unlock()
	c.isInvalid = newVal
}

func (c *Checker) initConnPool(ctx context.Context) error {
	// Try and catch up before we apply a table lock,
	// since we will need to catch up again with the lock held
	// and we want to minimize that.
	if err := c.feed.Flush(ctx); err != nil {
		return err
	}
	// Lock the source and target table in a trx
	// so the connection is not used by others
	c.logger.Info("starting checksum operation, this will require a table lock")

	// For multi-database operations, we need to acquire locks on both databases
	var tableLock, writeTableLock *dbconn.TableLock
	var err error

	// If we have a separate writeDB, also acquire lock on it
	if c.writeDB != nil {
		// Extract the new tables from the chunker's Tables() method
		// By convention, every second table is the "new" table
		var readTables, writeTables []*table.TableInfo
		for i, tbl := range c.chunker.Tables() {
			if i%2 == 0 {
				readTables = append(readTables, tbl)
			} else {
				writeTables = append(writeTables, tbl)
			}
		}
		tableLock, err = dbconn.NewTableLock(ctx, c.db, readTables, c.dbConfig, c.logger)
		if err != nil {
			return err
		}
		defer tableLock.Close()

		writeTableLock, err = dbconn.NewTableLock(ctx, c.writeDB, writeTables, c.dbConfig, c.logger)
		if err != nil {
			return err
		}
		defer writeTableLock.Close()

		// With the lock(s) held, flush one more time under the lock tables.
		// This now guarantees we have everything in the new table(s)
		if err := c.feed.FlushUnderTableLock(ctx, writeTableLock); err != nil {
			return err
		}
	} else {
		// Always acquire lock on the read database
		tableLock, err = dbconn.NewTableLock(ctx, c.db, c.chunker.Tables(), c.dbConfig, c.logger)
		if err != nil {
			return err
		}
		defer tableLock.Close()
		// We only have a reader, so flush the read connection.
		if err := c.feed.FlushUnderTableLock(ctx, tableLock); err != nil {
			return err
		}
	}

	// Assert that the change set is empty. This should always
	// be the case because we are under a lock.
	if !c.feed.AllChangesFlushed() {
		return errors.New("not all changes flushed")
	}
	// Create a set of connections which can be used to checksum
	// The table. They MUST be created before the lock is released
	// with REPEATABLE-READ and a consistent snapshot (or dummy read)
	// to initialize the read-view.
	c.trxPool, err = dbconn.NewTrxPool(ctx, c.db, c.concurrency, c.dbConfig)
	if err != nil {
		return err
	}

	// If we have a separate writeDB, create a transaction pool for it as well
	// to ensure cross-system consistent snapshots
	if c.writeDB != nil {
		c.writeTrxPool, err = dbconn.NewTrxPool(ctx, c.writeDB, c.concurrency, c.dbConfig)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Checker) Run(ctx context.Context) error {
	// Set startTime under lock to prevent race with StartTime() method
	c.Lock()
	c.startTime = time.Now()
	startTime := c.startTime // capture for defer
	c.Unlock()

	defer func() {
		c.ExecTime = time.Since(startTime)
	}()

	// Try the checksum up to n times if differences are found and we can fix them
	for attempt := 1; attempt <= c.maxRetries; attempt++ {
		if attempt > 1 {
			c.logger.Errorf("checksum failed, retrying %d/%d times", attempt, c.maxRetries)
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

func (c *Checker) runChecksum(ctx context.Context) error {
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
	// Regardless of err state, we should attempt to rollback the transaction
	// in checksumTxns. They are likely holding metadata locks, which will block
	// further operations like cleanup or cut-over.
	if err := c.trxPool.Close(); err != nil {
		return err
	}
	// Also close the writeTrxPool if it exists
	if c.writeTrxPool != nil {
		if err := c.writeTrxPool.Close(); err != nil {
			return err
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
func (c *Checker) intersectColumns(chunk *table.Chunk) string {
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
