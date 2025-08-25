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
	table            *table.TableInfo
	newTable         *table.TableInfo
	concurrency      int
	feed             *repl.Client
	db               *sql.DB
	trxPool          *dbconn.TrxPool
	isInvalid        bool
	chunker          table.Chunker
	startTime        time.Time
	ExecTime         time.Duration
	recentValue      any // used for status
	dbConfig         *dbconn.DBConfig
	logger           loggers.Advanced
	fixDifferences   bool
	differencesFound atomic.Uint64
	recopyLock       sync.Mutex
	isResume         bool
}

type CheckerConfig struct {
	Concurrency     int
	TargetChunkTime time.Duration
	DBConfig        *dbconn.DBConfig
	Logger          loggers.Advanced
	FixDifferences  bool
	Watermark       string // optional; defines a watermark to start from
}

func NewCheckerDefaultConfig() *CheckerConfig {
	return &CheckerConfig{
		Concurrency:     4,
		TargetChunkTime: 1000 * time.Millisecond,
		DBConfig:        dbconn.NewDBConfig(),
		Logger:          logrus.New(),
		FixDifferences:  false,
	}
}

// NewChecker creates a new checksum object.
func NewChecker(db *sql.DB, tbl, newTable *table.TableInfo, feed *repl.Client, config *CheckerConfig) (*Checker, error) {
	if feed == nil {
		return nil, errors.New("feed must be non-nil")
	}
	if newTable == nil || tbl == nil {
		return nil, errors.New("table and newTable must be non-nil")
	}
	if config.DBConfig == nil {
		config.DBConfig = dbconn.NewDBConfig()
	}
	chunker, err := table.NewChunker(tbl, config.TargetChunkTime, config.Logger)
	if err != nil {
		return nil, err
	}
	// If there is a watermark, we need to open the chunker at that watermark.
	// Overwrite the previously attached chunker with a new one.
	if config.Watermark != "" {
		config.Logger.Warnf("opening checksum chunker at watermark: %s", config.Watermark)
		if err := chunker.OpenAtWatermark(config.Watermark, newTable.MaxValue()); err != nil {
			return nil, err
		}
	}
	checksum := &Checker{
		table:          tbl,
		newTable:       newTable,
		concurrency:    config.Concurrency,
		db:             db,
		feed:           feed,
		chunker:        chunker,
		dbConfig:       config.DBConfig,
		logger:         config.Logger,
		fixDifferences: config.FixDifferences,
		isResume:       config.Watermark != "",
	}
	return checksum, nil
}

func (c *Checker) ChecksumChunk(ctx context.Context, trxPool *dbconn.TrxPool, chunk *table.Chunk) error {
	startTime := time.Now()
	trx, err := trxPool.Get()
	if err != nil {
		return err
	}
	defer trxPool.Put(trx)
	c.logger.Debugf("checksumming chunk: %s", chunk.String())
	source := fmt.Sprintf("SELECT BIT_XOR(CRC32(CONCAT(%s))) as checksum FROM %s WHERE %s",
		c.intersectColumns(),
		c.table.QuotedName,
		chunk.String(),
	)
	target := fmt.Sprintf("SELECT BIT_XOR(CRC32(CONCAT(%s))) as checksum FROM %s WHERE %s",
		c.intersectColumns(),
		c.newTable.QuotedName,
		chunk.String(),
	)
	var sourceChecksum, targetChecksum int64
	err = trx.QueryRow(source).Scan(&sourceChecksum)
	if err != nil {
		return err
	}
	err = trx.QueryRow(target).Scan(&targetChecksum)
	if err != nil {
		return err
	}
	if sourceChecksum != targetChecksum {
		// The checksums do not match, so we first need
		// to inspect closely and report on the differences.
		c.differencesFound.Add(1)
		c.logger.Warnf("checksum mismatch for chunk %s: source %d != target %d", chunk.String(), sourceChecksum, targetChecksum)
		if err := c.inspectDifferences(trx, chunk); err != nil {
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
	if chunk.LowerBound != nil {
		c.Lock()
		// For recent value we only use the first part of the key.
		c.recentValue = chunk.LowerBound.Value[0]
		c.Unlock()
	}
	c.chunker.Feedback(chunk, time.Since(startTime))
	return nil
}

func (c *Checker) DifferencesFound() uint64 {
	return c.differencesFound.Load()
}

func (c *Checker) RecentValue() string {
	c.Lock()
	defer c.Unlock()
	if c.recentValue == nil {
		return "TBD"
	}
	return fmt.Sprintf("%v", c.recentValue)
}

func (c *Checker) GetLowWatermark() (string, error) {
	if c.chunker == nil {
		return "", errors.New("chunker not initialized")
	}
	return c.chunker.GetLowWatermark()
}

func (c *Checker) inspectDifferences(trx *sql.Tx, chunk *table.Chunk) error {
	sourceSubquery := fmt.Sprintf("SELECT CRC32(CONCAT(%s)) as row_checksum, %s FROM %s WHERE %s",
		c.intersectColumns(),
		strings.Join(c.table.KeyColumns, ", "),
		c.table.QuotedName,
		chunk.String(),
	)
	targetSubquery := fmt.Sprintf("SELECT CRC32(CONCAT(%s)) as row_checksum, %s FROM %s WHERE %s",
		c.intersectColumns(),
		strings.Join(c.newTable.KeyColumns, ", "),
		c.newTable.QuotedName,
		chunk.String(),
	)
	// In MySQL 8.0 we could do this as a CTE, but because we kinda support
	// MySQL 5.7 we have to do it as a subquery. It should be a small amount of data
	// because its one chunk. Note: we technically have to do this twice, since
	// extra rows could exist on either side and we can't rely on FULL OUTER JOIN existing.
	stmt := fmt.Sprintf(`SELECT source.row_checksum as source_row_checksum, target.row_checksum as target_row_checksum, CONCAT_WS(",", %s) as pk FROM (%s) AS source LEFT JOIN (%s) AS target USING (%s) WHERE source.row_checksum != target.row_checksum OR target.row_checksum IS NULL
UNION
SELECT source.row_checksum as source_row_checksum, target.row_checksum as target_row_checksum, CONCAT_WS(",", %s) as pk FROM (%s) AS source RIGHT JOIN (%s) AS target USING (%s) WHERE source.row_checksum != target.row_checksum OR source.row_checksum IS NULL
`,
		strings.Join(c.table.KeyColumns, ", "),
		sourceSubquery,
		targetSubquery,
		strings.Join(c.table.KeyColumns, ", "),
		strings.Join(c.table.KeyColumns, ", "),
		sourceSubquery,
		targetSubquery,
		strings.Join(c.table.KeyColumns, ", "),
	)
	res, err := trx.Query(stmt)
	if err != nil {
		return err // can not debug issue
	}
	defer res.Close()
	var sourceRowChecksum, targetRowChecksum sql.NullString
	var pk string
	for res.Next() {
		err = res.Scan(&sourceRowChecksum, &targetRowChecksum, &pk)
		if err != nil {
			return err // can not debug issue
		}
		if sourceRowChecksum.Valid && !targetRowChecksum.Valid {
			c.logger.Warnf("inspection revealed row does not exist in target for pk: %s", pk)
		} else if !sourceRowChecksum.Valid && targetRowChecksum.Valid {
			c.logger.Warnf("inspection revealed row does not exist in source for pk: %s", pk)
		} else {
			c.logger.Warnf("inspection revealed row checksum mismatch for pk: %s: source %s != target %s", pk, sourceRowChecksum.String, targetRowChecksum.String)
		}
	}
	if res.Err() != nil {
		return res.Err()
	}
	return nil // managed to inspect differences
}

// replaceChunk recopies the data from table to newTable for a given chunk.
// Note that the chunk is dynamically sized based on the target-time that it took
// to *read* data in the checksum. This could be substantially longer than the time
// that it takes to copy the data. Maybe in future we could consider splitting
// the chunk here, but this is expected to be a very rare situation, so a small
// stall from an XL sized chunk is considered acceptable.
func (c *Checker) replaceChunk(ctx context.Context, chunk *table.Chunk) error {
	c.logger.Warnf("recopying chunk: %s", chunk.String())
	deleteStmt := "DELETE FROM " + c.newTable.QuotedName + " WHERE " + chunk.String()
	replaceStmt := fmt.Sprintf("REPLACE INTO %s (%s) SELECT %s FROM %s WHERE %s",
		c.newTable.QuotedName,
		utils.IntersectNonGeneratedColumns(c.table, c.newTable),
		utils.IntersectNonGeneratedColumns(c.table, c.newTable),
		c.table.QuotedName,
		chunk.String(),
	)
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
	// We further prevent the chance of deadlocks from the recopying process by only re-copying one chunk at a time.
	// We may revisit this in future, but since conflicts are expected to be low, it should be fine for now.
	c.recopyLock.Lock()
	defer c.recopyLock.Unlock()

	if _, err := dbconn.RetryableTransaction(ctx, c.db, false, dbconn.NewDBConfig(), deleteStmt); err != nil {
		return err
	}
	if _, err := dbconn.RetryableTransaction(ctx, c.db, false, dbconn.NewDBConfig(), replaceStmt); err != nil {
		return err
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
	tableLock, err := dbconn.NewTableLock(ctx, c.db, []*table.TableInfo{c.table, c.newTable}, c.dbConfig, c.logger)
	if err != nil {
		return err
	}
	defer tableLock.Close()
	// With the lock held, flush one more time under the lock tables.
	// Because we know canal is up to date this now guarantees
	// we have everything in the new table.
	if err := c.feed.FlushUnderTableLock(ctx, tableLock); err != nil {
		return err
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
	return err
}

func (c *Checker) Run(ctx context.Context) error {
	c.Lock()
	c.startTime = time.Now()
	defer func() {
		c.ExecTime = time.Since(c.startTime)
	}()
	// Open the chunker if it's not open.
	// It will already be open if this is a resume from checkpoint.
	// This is a little annoying, but just the way the chunker API works.
	if !c.isResume {
		if err := c.chunker.Open(); err != nil {
			return err
		}
	}
	c.Unlock()

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
	if err1 != nil {
		c.logger.Error("checksum failed")
		return err1
	}
	return nil
}

// intersectColumns is similar to utils.IntersectColumns, but it
// wraps an IFNULL(), ISNULL() and cast operation around the columns.
// The cast is to c.newTable type.
func (c *Checker) intersectColumns() string {
	var intersection []string
	for _, col := range c.table.NonGeneratedColumns {
		for _, col2 := range c.newTable.NonGeneratedColumns {
			if col == col2 {
				// Column exists in both, so we add intersection wrapped in
				// IFNULL, ISNULL and CAST.
				intersection = append(intersection, "IFNULL("+c.newTable.WrapCastType(col)+",''), ISNULL(`"+col+"`)")
			}
		}
	}
	return strings.Join(intersection, ", ")
}
