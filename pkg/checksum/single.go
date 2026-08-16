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
	"github.com/block/spirit/pkg/autoscale"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/throttler"
	"github.com/block/spirit/pkg/utils"
	"golang.org/x/sync/errgroup"
)

type SingleChecker struct {
	sync.Mutex

	concurrency int
	// maxConcurrency is the ceiling the autoscaler may grow to. The
	// transaction pool is provisioned at this size regardless of whether
	// scaling is enabled — see initConnPool for why it cannot be grown later.
	maxConcurrency int
	// autoscale enables the control loop. Without it the pool stays at
	// concurrency, but the throttler hard-stop below still applies.
	autoscale   bool
	throttler   throttler.Throttler
	metricsSink metrics.Sink
	// limiter gates live concurrency for the current pass; nil until Run.
	limiter          *autoscale.Limiter
	targetChunkTime  time.Duration
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
	// repairApplier is the write path a mismatched chunk is rewritten through
	// (see replaceChunk). It is started and stopped around each repair rather
	// than for the checker's lifetime, because repairs are rare and serialized:
	// see the lifecycle note in replaceChunk.
	repairApplier   applier.Applier
	recopyLock      sync.Mutex
	maxRetries      int
	yieldTimeout    time.Duration
	yieldsPerformed atomic.Uint64 // number of yield/resume cycles performed
	// chunks accumulates the chunk-size distribution for the pass in flight.
	chunks *chunkObserver
	// chunkSize is the row count of the most recently checksummed chunk,
	// reported on the runner status block. Sampled from the chunk rather than
	// read off the chunker so it works for every Chunker implementation,
	// including the multi-table one. Mirrors copier.Copier.ChunkSize.
	chunkSize atomic.Uint64
}

// SetThrottler installs the throttler the checksum paces itself against. It
// mirrors Copier.SetThrottler and exists for the same reason: the runner
// builds the checker before it opens the throttlers (the throttler needs the
// monitoring connection, which is set up later), so the wiring cannot happen
// at construction. A nil throttler is ignored, leaving the Noop in place.
//
// What is installed is the load-only narrowing of t, not t itself — see
// loadOnlyThrottler.
func (c *SingleChecker) SetThrottler(t throttler.Throttler) {
	if t == nil {
		return
	}
	c.Lock()
	defer c.Unlock()
	c.throttler = loadOnlyThrottler(t)
}

// getThrottler reads the throttler under lock, since SetThrottler may race
// with Run in principle.
func (c *SingleChecker) getThrottler() throttler.Throttler {
	c.Lock()
	defer c.Unlock()
	return c.throttler
}

// setLimiter publishes the limiter for the pass now starting. Guarded because
// the autoscaler and observers read it while the pass runs, and a yield/resume
// cycle replaces it.
func (c *SingleChecker) setLimiter(l *autoscale.Limiter) {
	c.Lock()
	defer c.Unlock()
	c.limiter = l
}

// currentLimiter returns the limiter for the pass in flight, or nil before the
// first pass starts.
func (c *SingleChecker) currentLimiter() *autoscale.Limiter {
	c.Lock()
	defer c.Unlock()
	return c.limiter
}

var (
	_ Checker       = (*SingleChecker)(nil)
	_ ThrottleAware = (*SingleChecker)(nil)
	_ Paced         = (*SingleChecker)(nil)
)

// Threads reports the live worker count: the limiter's current limit while a
// pass is running, falling back to the configured concurrency before the first
// pass starts.
func (c *SingleChecker) Threads() int {
	if l := c.currentLimiter(); l != nil {
		return l.Limit()
	}
	return c.concurrency
}

// ChunkSize reports the row count of the most recently checksummed chunk, or 0
// before the first one.
func (c *SingleChecker) ChunkSize() uint64 {
	return c.chunkSize.Load()
}

// IsThrottled reports whether the throttler is currently pausing dispatch.
func (c *SingleChecker) IsThrottled() bool {
	return c.getThrottler().IsThrottled()
}

func (c *SingleChecker) ChecksumChunk(ctx context.Context, trxPool *dbconn.TrxPool, chunk *table.Chunk) error {
	startTime := time.Now()
	c.chunkSize.Store(chunk.ChunkSize)
	trx, err := trxPool.Get()
	if err != nil {
		return err
	}
	// The transaction is only needed for the snapshot reads: the two checksum
	// queries and (on mismatch) the row-level inspection. It must go back to
	// the pool the moment those are done, not at function exit: the repair
	// path below serializes on recopyLock and can park for many minutes
	// behind other repairs, and a checked-out transaction is invisible to the
	// pool's keepalive — holding it there idled connections past Aurora's
	// wait_timeout in production and killed the whole pool. Guarded so the
	// early call and the deferred one compose to exactly one Put.
	trxReturned := false
	putTrx := func() {
		if !trxReturned {
			trxReturned = true
			trxPool.Put(trx)
		}
	}
	defer putTrx()
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
	// Record the scan cost before the mismatch branch below, so the sizing
	// distribution measures checksumming only. Repair is orders of magnitude
	// more expensive and rare; folding it in would make the p90 useless for
	// deciding chunk sizes. (The chunker's own Feedback call at the end of this
	// method deliberately keeps its existing behaviour of timing the whole
	// operation.)
	if c.chunks != nil {
		c.chunks.record(targetCount, time.Since(startTime))
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
		// The snapshot reads are done. The repair below reads current data
		// through the pooled connections (deliberately outside the snapshot),
		// so return the transaction now and let the keepalive cover it while
		// this worker queues on recopyLock.
		putTrx()
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

// replaceChunk recopies the data from table to newTable for a given chunk: the
// chunk's key range is DELETEd on the target, then the source rows are read into
// the client and rewritten through the applier — the same buffered write path
// the copier and the binlog apply use.
//
// This deliberately does NOT use `REPLACE INTO newTable ... SELECT FROM table`,
// which is what it did historically. That statement is a single unbuffered
// server-side operation, and it locks in two ways that scale with the chunk:
//
//   - The SELECT half takes shared next-key locks on every source row it reads
//     (it is a locking read, because it feeds a write), so application UPDATEs
//     to the *original* table block behind a repair for the whole statement.
//   - The write half holds its row locks on the target for the same duration.
//
// Reading into the client is a plain consistent read that locks nothing, and the
// applier splits the write into bounded statements (see applier chunklets), so
// neither half's lock footprint grows with the chunk any more. That is what makes
// a large checksum chunk safe to repair without splitting it first
// (block/spirit#1130). Rows are streamed in batches rather than materialized
// whole: the client holds one batch (repairBatchRows/repairBatchBytes) plus
// whatever the applier has accepted and not yet written, which its chunklet
// buffer bounds. Neither bound grows with the chunk, which is the property that
// matters here — it is not one batch's worth of memory.
//
// Note that the chunk is dynamically sized based on the target-time that it took
// to *read* data in the checksum. This could be substantially longer than the time
// that it takes to copy the data.
//
// Two behaviours worth knowing about, both inherited from the applier and shared
// with the other repair paths (DistributedChecker.replaceChunk, MySQLRecopier):
//
//   - The applier writes with INSERT IGNORE, not REPLACE. Rows inside the key
//     range were just deleted so there is nothing to conflict with there; a row
//     that collides on a UNIQUE secondary key with a row *outside* the range is
//     skipped rather than clobbering the other row. The repair then leaves the
//     chunk still diverged, which the next attempt re-flags and Run eventually
//     surfaces as a hard error — the correct outcome for e.g. a lossy ALTER that
//     adds a UNIQUE index to non-unique data. The skipped-rows count is logged.
//   - JSON columns are read bare, with no round-trip cast. The read+write pair
//     is already text-mediated (the SELECT renders each document to text and the
//     applier writes it back as a SQL literal the target re-parses), so the
//     repaired row lands as exactly the one-text-round-trip image the checksum's
//     source side predicts. Casting on top would apply parse∘render twice, which
//     does not converge for misparsed doubles — see castExpr in pkg/table.
//   - The read is not synchronized with the binlog feed, so a row deleted on the
//     source *after* it is read here is written back if the feed has already
//     applied that DELETE to the target. The chunk then stays diverged, the next
//     attempt re-flags it, and the repair converges as soon as the churn on that
//     key range stops. Cutover is gated on a pass that finds no differences at
//     all, so the cost is attempts, never a bad cutover. The window is wider than
//     the old locking read's (which held the source rows still for the duration
//     of its own statement) but it was never closed, and the other repair paths
//     have the same shape.
func (c *SingleChecker) replaceChunk(ctx context.Context, chunk *table.Chunk) error {
	start := time.Now()
	c.logger.Warn("recopying chunk via DELETE + Apply", "chunk", chunk.String())

	// We further prevent the chance of deadlocks from the recopying process by only re-copying one chunk at a time.
	// We may revisit this in future, but since conflicts are expected to be low, it should be fine for now.
	c.recopyLock.Lock()
	defer c.recopyLock.Unlock()

	// Construct a delete statement to remove existing rows in the target chunk
	deleteStmt := "DELETE FROM " + chunk.NewTable.QuotedTableName + " WHERE " + chunk.String()

	// The DELETE and the rewrite are two separate operations, and the historical
	// reason for that is the deadlock documented below.
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
	// the opportunity for deadlocks. The rewrite is now many small transactions
	// rather than one, which shrinks the lock footprint further, and the
	// serialization on recopyLock above still keeps two repairs from overlapping.
	//
	// If the parent ctx is cancelled after the DELETE, the target chunk would be
	// left with rows deleted but not yet rewritten. The continuous-checksum
	// loop's cancellation on sentinel drop hits this race, so the whole repair
	// runs under a context that ignores the parent's cancellation. The bounded
	// timeout still protects against a hung repair — and because the applier's
	// workers are started under fixCtx too (below), that bound covers the writes
	// as well: a cancelled parent cannot strand queued inserts.
	fixCtx, fixCancel := context.WithTimeout(context.WithoutCancel(ctx), fixChunkTimeout)
	defer fixCancel()
	if _, err := dbconn.RetryableTransaction(fixCtx, c.db, dbconn.ErrorOnDupKey, c.dbConfig, deleteStmt); err != nil {
		return fmt.Errorf("failed to delete existing rows: %w", err)
	}

	// The applier is started per repair rather than for the checker's lifetime.
	// Repairs are rare and serialized by recopyLock, so the goroutine churn is
	// irrelevant next to the work itself, and in exchange the repair is fully
	// self-contained: the workers live under fixCtx, and the Stop() below joins
	// them, so no write can outlive (or be dropped by) the repair that queued it.
	// The start is deferred until there is actually something to write — a source
	// range that is entirely empty is repaired by the DELETE alone.
	applierStarted := false
	defer func() {
		if !applierStarted {
			return
		}
		if err := c.repairApplier.Stop(); err != nil {
			c.logger.Warn("failed to stop repair applier", "error", err)
		}
	}()

	// Read the source rows for the chunk. This is deliberately a plain,
	// non-locking consistent read of *current* data — not a read inside the
	// checksum's snapshot: by the time we repair, the snapshot is stale and what
	// the target needs is the source as it is now.
	//
	// The column list is the source/target intersection (with renames applied on
	// the target side), which is exactly what the applier expects: row values are
	// positional, values[i] belongs to sourceColumns[i].
	//
	// FORCE INDEX (PRIMARY) for the same reason the copier's read of this shape
	// does (pkg/copier/buffered.go): the predicate is a range over the primary
	// key, and misleading statistics on a wide table can otherwise talk the
	// optimizer into a scan.
	sourceColumns, _ := chunk.ColumnMapping.ColumnsSlice()
	sourceColumnList, _ := chunk.ColumnMapping.Columns()
	query := fmt.Sprintf("SELECT %s FROM %s FORCE INDEX (PRIMARY) WHERE %s",
		sourceColumnList,
		chunk.Table.QuotedTableName,
		chunk.String(),
	)
	rows, err := c.db.QueryContext(fixCtx, query)
	if err != nil {
		return fmt.Errorf("failed to read source chunk: %w", err)
	}
	// Closed early on the success path (see below) to hand the connection back
	// before waiting on the writers; this covers the early returns until then.
	defer func() {
		if rows != nil {
			utils.CloseAndLog(rows)
		}
	}()

	// Apply() is asynchronous: it hands the batch to the write workers and
	// returns, so reads and writes pipeline. Callbacks run on the applier's
	// coordinator goroutine, hence the mutex; they must never block, so results
	// are accumulated here and inspected after Wait().
	var (
		resultMu      sync.Mutex
		firstApplyErr error
		appliedRows   int64
	)
	callback := func(affectedRows int64, applyErr error) {
		resultMu.Lock()
		defer resultMu.Unlock()
		appliedRows += affectedRows
		if applyErr != nil && firstApplyErr == nil {
			firstApplyErr = applyErr
		}
	}

	var (
		batch      [][]any
		batchBytes int
		sourceRows int64
	)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if !applierStarted {
			if err := c.repairApplier.Start(fixCtx); err != nil {
				return fmt.Errorf("failed to start repair applier: %w", err)
			}
			applierStarted = true
		}
		if err := c.repairApplier.Apply(fixCtx, chunk, batch, callback); err != nil {
			return fmt.Errorf("failed to submit rows for rewrite: %w", err)
		}
		// The applier owns the batch now (it keeps the slices), so start a fresh
		// one rather than reusing the backing array.
		batch, batchBytes = nil, 0
		return nil
	}
	for rows.Next() {
		values := make([]any, len(sourceColumns))
		valuePtrs := make([]any, len(sourceColumns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("failed to scan source row: %w", err)
		}
		batch = append(batch, values)
		batchBytes += applier.EstimateRowSize(values)
		sourceRows++
		if len(batch) >= repairBatchRows || batchBytes >= repairBatchBytes {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to read source chunk: %w", err)
	}
	if err := flush(); err != nil {
		return err
	}
	// Return the read connection before waiting on the writers. They take their
	// connections from the same pool, so holding this one across the wait would
	// be one connection of headroom given up for nothing. Nil it out so the
	// deferred close does not close it a second time.
	utils.CloseAndLog(rows)
	rows = nil

	// Wait for every submitted batch to be written and its callback to have run.
	// Repairs are serialized on recopyLock, so on the private applier there is no
	// other pending work this could be waiting on; on a shared one (the migration
	// runner passes the applier the copy phase used) the copy has long finished
	// and the binlog feed only uses the synchronous write methods, which do not
	// register pending work.
	if applierStarted {
		if err := c.repairApplier.Wait(fixCtx); err != nil {
			return fmt.Errorf("failed waiting for chunk rewrite: %w", err)
		}
	}
	resultMu.Lock()
	defer resultMu.Unlock()
	if firstApplyErr != nil {
		return fmt.Errorf("failed to rewrite chunk data: %w", firstApplyErr)
	}
	if appliedRows < sourceRows {
		// INSERT IGNORE skipped rows: see the note on UNIQUE secondary keys in
		// the function doc. The chunk is still diverged, so this is reported
		// rather than swallowed — the next attempt re-flags it.
		//
		// One-directional: appliedRows is a sum of SQL rows-affected, and
		// dbconn.RetryableTransaction accumulates that across its retry attempts
		// (a statement that succeeded but whose COMMIT was then retried counts
		// twice), so an inflated count can hide this warning. That costs a log
		// line, not correctness — the divergence itself is caught by the next
		// attempt's checksum either way.
		c.logger.Warn("recopying chunk did not rewrite every source row; a UNIQUE key conflict outside the chunk range is the usual cause",
			"chunk", chunk.String(),
			"sourceRows", sourceRows,
			"writtenRows", appliedRows,
		)
	}
	c.logger.Info("chunk recopied",
		"chunk", chunk.String(),
		"rowCount", sourceRows,
		"elapsed", time.Since(start).Round(time.Millisecond).String(),
	)
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
	//
	// The pool is sized to maxConcurrency, not concurrency, because it cannot
	// be grown afterwards: every transaction here takes its snapshot under the
	// table lock, so they all see the same point in time. One started later,
	// after the lock is released, would read a *newer* snapshot and could
	// compare a chunk against changes the others cannot see — a false
	// mismatch, or worse, a real difference masked. Over-provisioning costs a
	// connection per idle transaction and nothing else: all of these read views
	// pin history from the same instant, so the history list length floor is
	// identical whether the autoscaler ends up using four of them or sixteen.
	c.trxPool, err = dbconn.NewTrxPool(ctx, c.db, c.maxConcurrency, c.dbConfig, c.logger)
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
			// If the previous attempt errored without finding a single
			// difference — e.g. the transaction pool's connections were killed
			// mid-pass — the low watermark is still trustworthy: every chunk
			// below it verified clean. Resume there rather than discarding
			// hours of verified work; runChecksum re-acquires the table lock
			// and takes fresh snapshots either way, exactly as the yield path
			// does. An attempt that found differences (lastErr == nil, or an
			// error after repairs) still restarts from the beginning, so the
			// "clean pass over the whole table" guarantee after repairs is
			// unchanged.
			resumed := false
			if lastErr != nil && c.differencesFound.Load() == 0 {
				if watermark, wmErr := c.chunker.GetLowWatermark(); wmErr == nil {
					if openErr := c.chunker.OpenAtWatermark(watermark); openErr == nil {
						resumed = true
						c.logger.Error("checksum failed, retrying from low watermark",
							"attempt", attempt, "maxRetries", c.maxRetries, "watermark", watermark)
					} else {
						c.logger.Warn("failed to resume checksum at watermark, restarting from beginning", "error", openErr)
					}
				}
			}
			if !resumed {
				c.logger.Error("checksum failed, retrying", "attempt", attempt, "maxRetries", c.maxRetries)
				// Reset the chunker to start from the beginning
				if err := c.chunker.Reset(); err != nil {
					return fmt.Errorf("failed to reset chunker for retry: %w", err)
				}
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
	//
	// Each shape carries its own sentinel so a caller can tell them apart
	// without reading the message: only the second is reproducible, and a
	// caller that decides whether to retry needs to know which it has.
	if lastErr != nil {
		return fmt.Errorf("%w (%d/%d); last error: %w", ErrAttemptsExhausted, c.maxRetries, c.maxRetries, lastErr)
	}
	return fmt.Errorf("%w (%d/%d). This likely indicates either a bug in Spirit, or a manual modification to the _new table outside of Spirit. Please report @ github.com/block/spirit", ErrDifferencesExhausted, c.maxRetries, c.maxRetries)
}

// logChunkSummary reports the pass's chunk-size distribution. Info level: it
// is one line per pass (or per yield segment), and it is the data any future
// change to checksum chunk sizing has to be argued from.
func (c *SingleChecker) logChunkSummary() {
	if c.chunks == nil {
		return
	}
	if summary := c.chunks.summary(c.targetChunkTime); summary != "" {
		c.logger.Info("checksum chunk size distribution", "stats", summary)
	}
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
	// Live concurrency is governed by the limiter, not errgroup.SetLimit: the
	// errgroup contract forbids changing its limit while goroutines are
	// active, and the autoscaler needs to resize mid-pass. Acquiring before
	// g.Go keeps the number of live goroutines bounded by the limit just as
	// SetLimit did.
	limiter := autoscale.NewLimiter(c.concurrency)
	c.setLimiter(limiter)
	// Safe without synchronisation: assigned before any worker starts and not
	// replaced until every worker of this pass has been joined by g.Wait().
	c.chunks = &chunkObserver{}
	defer c.logChunkSummary()

	thr := c.getThrottler()
	if c.autoscale {
		// Scoped to this pass: a yield/resume re-enters runChecksum with a
		// fresh limiter, so the controller is rebuilt at the start value
		// rather than inheriting a stale count for a new snapshot.
		scalerCtx, stopScaler := context.WithCancel(errGrpCtx)
		defer stopScaler()
		scaler := newChecksumScaler(thr, limiter, c.feed.FlushResidual, c.concurrency, c.maxConcurrency, c.logger, c.metricsSink)
		go scaler.run(scalerCtx)
	}

	for !c.chunker.IsRead() && c.isHealthy(errGrpCtx) {
		// Hard stop, checked before we take a permit so a throttled checksum
		// holds no capacity while it waits. Chunks already in flight are never
		// interrupted — the checksum stops *dispatching* rather than
		// abandoning work, because an aborted chunk is wasted I/O that has to
		// be redone from the same watermark.
		thr.BlockWait(errGrpCtx)
		if err := limiter.Acquire(errGrpCtx); err != nil {
			// Context is done (yield deadline or cancellation). Stop
			// dispatching; the checks after g.Wait() classify why.
			break
		}
		g.Go(func() error {
			defer limiter.Release()
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
	// A pass that stopped dispatching before the chunker was exhausted has not
	// verified the whole table, so it must never be reported as a success —
	// Run treats a nil return as "checksum passed" and would let a cut-over
	// proceed on a partially-checked table.
	//
	// This became reachable when the dispatch loop learned to stop on a done
	// context (throttler hard-stop, then limiter.Acquire returning): a
	// cancellation that lands while no chunk is in flight leaves err1 == nil
	// with work outstanding. Previously an in-flight chunk query always failed
	// first and surfaced the cancellation through err1.
	if !c.chunker.IsRead() {
		if err := ctx.Err(); err != nil {
			return err
		}
		return errors.New("checksum stopped before the table was fully verified")
	}
	return nil
}
