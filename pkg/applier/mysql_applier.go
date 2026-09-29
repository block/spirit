package applier

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/table"
)

// MySQLApplier applies rows to one or more MySQL targets. Each target owns a
// key range, its own write connection and its own pool of write workers.
//
// With one target covering the whole key space (the migration, datasync and
// unsharded-move case) every row goes to that target and no routing happens.
// With several targets (a move to a Vitess-style sharded destination) each row
// is routed by hashing its sharding column and picking the target whose key
// range contains the hash. The sharding column and hash function are
// configured per table in TableInfo.ShardingColumn and TableInfo.HashFunc, so
// different tables in one multi-table move can use different sharding keys.
type MySQLApplier struct {
	sync.Mutex

	shards      []*shardTarget
	targets     []Target // Original target configurations
	dbConfig    *dbconn.DBConfig
	logger      *slog.Logger
	metricsSink metrics.Sink // nil disables the stats emitter

	// unsharded is true when there is exactly one target and it covers the
	// whole key space. Every row then belongs to shards[0], so routing is
	// skipped and tables need no ShardingColumn/HashFunc.
	unsharded bool

	// Pending work tracking (shared across all shards).
	//
	// Completion invariant (#765): a pendingWork entry is "claimed" by
	// deleting it from the map AND incrementing callbacksInFlight in the
	// same pendingMutex critical section. Exactly one path can claim an
	// entry — the feedbackCoordinator (error or success path) or Apply's
	// ctx-cancel cleanup — so the callback is invoked exactly once. The
	// claimer then invokes the callback without holding the lock (callbacks
	// may be slow or re-enter the applier) and decrements callbacksInFlight
	// when it returns. Wait() returns only when len(pendingWork) == 0 AND
	// callbacksInFlight == 0, so it cannot return before every callback has
	// finished running.
	pendingWork       map[int64]*pendingWork
	pendingMutex      sync.Mutex
	callbacksInFlight int          // claimed work whose callback has not returned yet; guarded by pendingMutex
	nextWorkID        atomic.Int64 // Atomic counter for work IDs

	// Context management
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup // tracks the feedbackCoordinator and stats-emitter goroutines

	// timings is a rolling window of per-chunklet queue-wait and write
	// durations across all shards, reported by Stats(). A single shared ring
	// is deliberate: the bottleneck question ("is the write side saturated?")
	// does not need per-shard attribution.
	timings timingRing

	// splits accumulates the chunklet/row counts behind Stats.RowsPerChunklet.
	// The caps (chunkletMaxRows/MaxStatementSizeBytes) are global, but the
	// splitting itself happens per shard: Apply routes rows to shards first and
	// then calls splitRowsIntoChunklets on each shard's share, so with several
	// shards one chunk yields more, shorter chunklets than it would unsharded.
	// The counter sums those per-shard splits, and the mean is correspondingly
	// lower — see the caveat on Stats.RowsPerChunklet.
	splits splitCounter

	// State management to make Start/Stop idempotent
	stopped bool
	started bool
}

// shardTarget represents a single target with its own connection, key range,
// and workers.
type shardTarget struct {
	shardID             int
	writeDB             *sql.DB
	keyRange            keyRange // Parsed key range for this shard
	chunkletBuffer      chan chunklet
	chunkletCompletions chan chunkletCompletion
	// writeWorkersCount is the worker count the next Start spawns, set at
	// construction from ApplierConfig.Threads or by SetInitialWriteWorkers.
	// The *live* count can change at runtime via SetWriteWorkers.
	writeWorkersCount int32
	workers           workerPool
	workerIDCounter   atomic.Int32 // monotonic worker id, for debug logging only
	logger            *slog.Logger
	dbConfig          *dbconn.DBConfig
}

// chunklet is a small batch of rows destined for one shard, limited by either
// chunkletMaxRows or MaxStatementSizeBytes, whichever is reached first.
type chunklet struct {
	workID     int64        // ID of the parent work
	shardID    int          // Which shard this belongs to
	chunk      *table.Chunk // Original chunk for column info
	rows       []rowData    // Rows for this shard
	enqueuedAt time.Time    // when Apply() offered this chunklet to the shard buffer; queue wait = dequeue - enqueuedAt
}

// chunkletCompletion represents a completed chunklet
type chunkletCompletion struct {
	workID       int64 // ID of the parent work
	shardID      int   // Which shard this came from
	affectedRows int64 // Rows affected by this chunklet
	err          error // Error if any
}

// pendingWork tracks a set of rows that are being processed
type pendingWork struct {
	callback           ApplyCallback
	totalChunklets     int   // Total number of chunklets for this work
	completedChunklets int   // Number of completed chunklets
	totalAffectedRows  int64 // Sum of affected rows from all chunklets
}

// New creates a MySQLApplier that writes to targets. There must be at least
// one target, and no two key ranges may overlap. A single target may leave
// KeyRange empty (or "0"): it then covers the whole key space.
//
// ApplierConfig.Threads is the write-worker count for EACH target, not a total
// divided between them (see pkg/move/move.go:WriteThreads).
func New(targets []Target, cfg *ApplierConfig) (*MySQLApplier, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if len(targets) == 0 {
		return nil, errors.New("at least one target must be provided")
	}
	shards := make([]*shardTarget, len(targets))
	for i, target := range targets {
		// A nil connection is only discovered when a row routes to this shard,
		// which may be long after construction (and never in a test that only
		// exercises other shards) — fail here instead.
		if target.DB == nil {
			return nil, fmt.Errorf("shard %d: target DB must be non-nil", i)
		}
		// Parse the key range
		kr, err := parseKeyRange(target.KeyRange)
		if err != nil {
			return nil, fmt.Errorf("failed to parse key range for shard %d: %w", i, err)
		}

		shards[i] = &shardTarget{
			shardID:             i,
			writeDB:             target.DB,
			keyRange:            kr,
			chunkletBuffer:      make(chan chunklet, defaultBufferSize),
			chunkletCompletions: make(chan chunkletCompletion, defaultBufferSize),
			writeWorkersCount:   int32(cfg.Threads),
			logger:              cfg.Logger,
			dbConfig:            cfg.DBConfig,
		}
	}

	// Validate that key ranges do not overlap
	for i := range shards {
		for j := i + 1; j < len(shards); j++ {
			if shards[i].keyRange.overlaps(shards[j].keyRange) {
				return nil, fmt.Errorf("key ranges overlap: shard %d (%s: %s) and shard %d (%s: %s)",
					i, targets[i].KeyRange, shards[i].keyRange,
					j, targets[j].KeyRange, shards[j].keyRange)
			}
		}
	}

	// Log the target-to-range mapping. With several targets it is the first
	// thing an operator checks when rows land on the wrong shard, so it is
	// logged at Info; a single target adds nothing worth an Info line.
	logLevel := slog.LevelDebug
	if len(shards) > 1 {
		logLevel = slog.LevelInfo
	}
	for i, shard := range shards {
		cfg.Logger.Log(context.Background(), logLevel, "parsed key range for shard",
			"shardID", i,
			"keyRange", targets[i].KeyRange,
			"parsed", shard.keyRange.String())
	}

	return &MySQLApplier{
		shards:      shards,
		targets:     targets,
		dbConfig:    cfg.DBConfig,
		logger:      cfg.Logger,
		metricsSink: cfg.MetricsSink,
		unsharded:   len(shards) == 1 && shards[0].keyRange.coversAll(),
		pendingWork: make(map[int64]*pendingWork),
	}, nil
}

// Start initializes all shard workers and begins processing.
// This does not control the synchronous methods like UpsertRows/DeleteKeys.
// This method is idempotent and can restart the applier after Stop() is called.
//
// Lifecycle: callers MUST call Stop() to terminate the per-shard write workers
// and the single feedbackCoordinator. Cancelling the ctx passed here does NOT
// by itself shut down the goroutine pipeline — it only aborts in-flight writes.
// Workers for a shard exit when its chunkletBuffer is closed (by Stop) or when
// their quit channel is closed (by SetWriteWorkers scaling down), and the
// coordinator exits when every shard's chunkletCompletions has been closed (by
// Stop after joining each shard's workers). Failing to call Stop() will leak
// goroutines.
func (a *MySQLApplier) Start(ctx context.Context) error {
	a.Lock()
	defer a.Unlock()

	// If already started, return without error
	if a.started {
		a.logger.Debug("MySQLApplier already started, skipping")
		return nil
	}

	// If previously stopped, we need to reinitialize channels
	if a.stopped {
		a.logger.Info("restarting MySQLApplier after previous stop")
		for _, shard := range a.shards {
			shard.chunkletBuffer = make(chan chunklet, defaultBufferSize)
			shard.chunkletCompletions = make(chan chunkletCompletion, defaultBufferSize)
			shard.workerIDCounter.Store(0)
		}
		a.stopped = false
	}

	workerCtx, cancelFunc := context.WithCancel(ctx)
	a.cancelFunc = cancelFunc

	a.started = true
	a.logger.Debug("starting MySQLApplier", "shardCount", len(a.shards))

	// Start a single feedback coordinator for all shards, before the workers,
	// so completions are always drained.
	a.wg.Add(1)
	go a.feedbackCoordinator(workerCtx)

	// Report pipeline gauges (aggregated across shards) for the applier's
	// lifetime. Exits on Stop()'s context cancellation; joined via a.wg.
	if a.metricsSink != nil {
		a.wg.Go(func() {
			emitStatsLoop(workerCtx, a, a.metricsSink, a.logger)
		})
	}

	// The configured count is per target, as with fixed pools.
	for _, shard := range a.shards {
		shard.workers.start(workerCtx, int(shard.writeWorkersCount), func(ctx context.Context, quit <-chan struct{}) { a.writeWorker(ctx, shard, quit) })
	}

	return nil
}

// shardForHash returns the index of the shard whose key range contains hash,
// or -1 if none does.
func (a *MySQLApplier) shardForHash(hash uint64) int {
	for i, shard := range a.shards {
		if shard.keyRange.contains(hash) {
			return i
		}
	}
	return -1
}

// Apply sends rows to be written to the target(s). With several targets, rows
// are distributed across them based on the sharding column and hash function
// configured in the chunk's Table.ShardingColumn and Table.HashFunc.
func (a *MySQLApplier) Apply(ctx context.Context, chunk *table.Chunk, rows [][]any, callback ApplyCallback) error {
	if len(rows) == 0 {
		// No rows to apply, invoke callback immediately
		callback(0, nil)
		return nil
	}

	// Group rows by shard
	shardRows := make([][]rowData, len(a.shards))
	if a.unsharded {
		shardRows[0] = make([]rowData, len(rows))
		for i, row := range rows {
			shardRows[0][i] = rowData{values: row}
		}
	} else if err := a.routeRows(chunk, rows, shardRows); err != nil {
		return err
	}

	// Assign a work ID for tracking
	workID := a.nextWorkID.Add(1)

	// Split rows into chunklets based on both row count and size thresholds
	var allChunklets []chunklet
	for shardID, rows := range shardRows {
		if len(rows) == 0 {
			continue
		}

		// Use shared helper to split rows into chunklets
		// Then convert row batches into chunklets with metadata
		rowBatches := splitRowsIntoChunklets(rows)
		a.splits.record(len(rowBatches), len(rows))
		for _, batch := range rowBatches {
			allChunklets = append(allChunklets, chunklet{
				workID:  workID,
				shardID: shardID,
				chunk:   chunk,
				rows:    batch,
			})
		}
	}

	// Register the pending work
	a.pendingMutex.Lock()
	a.pendingWork[workID] = &pendingWork{
		callback:           callback,
		totalChunklets:     len(allChunklets),
		completedChunklets: 0,
		totalAffectedRows:  0,
	}
	a.pendingMutex.Unlock()

	// Send chunklets to their respective shard buffers.
	// If ctx is cancelled mid-send, we must clean up pendingWork before
	// returning. Otherwise the entry remains with totalChunklets > completedChunklets
	// forever, hanging Wait(). Chunklets already in shard buffers may still be
	// processed; their completions arrive at the coordinator after pendingWork
	// has been deleted and are dropped (logged as "unknown work").
	//
	// The claim (delete + callbacksInFlight increment, see the completion
	// invariant on pendingWork) is atomic under pendingMutex, so this cleanup
	// and the feedbackCoordinator can never both invoke the callback: if the
	// coordinator already claimed the work (e.g. an error completion raced
	// this cancellation), `exists` is false and we return without invoking.
	for _, chunkletData := range allChunklets {
		// Stamp before the send so queue wait includes send-side
		// backpressure: when the shard buffer is full, time blocked here is
		// exactly "waiting for a write worker".
		chunkletData.enqueuedAt = time.Now()
		select {
		case a.shards[chunkletData.shardID].chunkletBuffer <- chunkletData:
		case <-ctx.Done():
			a.pendingMutex.Lock()
			pending, exists := a.pendingWork[workID]
			if exists {
				delete(a.pendingWork, workID)
				a.callbacksInFlight++
			}
			a.pendingMutex.Unlock()
			if exists {
				a.invokeCallback(pending.callback, 0, ctx.Err())
			}
			return ctx.Err()
		}
	}
	return nil
}

// routeRows distributes copied rows across shardRows by hashing each row's
// sharding column. The rows passed to Apply come from a SELECT that excludes
// generated columns, so the sharding column is located by its ordinal among
// the non-generated columns.
func (a *MySQLApplier) routeRows(chunk *table.Chunk, rows [][]any, shardRows [][]rowData) error {
	shardingColumn := chunk.Table.ShardingColumn
	hashFunc := chunk.Table.HashFunc
	if shardingColumn == "" {
		return errors.New("ShardingColumn not configured in TableInfo")
	}
	if hashFunc == nil {
		return errors.New("HashFunc not configured in TableInfo")
	}
	shardingOrdinal, err := chunk.Table.GetNonGeneratedColumnOrdinal(shardingColumn)
	if err != nil {
		return err
	}
	a.logger.Debug("routing rows", "rowCount", len(rows), "shardingColumn", shardingColumn,
		"ordinal", shardingOrdinal, "table", chunk.Table.TableName)

	for _, row := range rows {
		if shardingOrdinal >= len(row) {
			return fmt.Errorf("sharding column ordinal %d exceeds row length %d", shardingOrdinal, len(row))
		}
		shardingValue := row[shardingOrdinal]
		hashValue, err := hashFunc(shardingValue)
		if err != nil {
			return fmt.Errorf("hash function error: %w", err)
		}
		shardID := a.shardForHash(hashValue)
		if shardID == -1 {
			return fmt.Errorf("no shard found for hash value %x (sharding column: %s, value: %v)",
				hashValue, shardingColumn, shardingValue)
		}
		shardRows[shardID] = append(shardRows[shardID], rowData{values: row})
	}
	return nil
}

// invokeCallback runs the callback for work the caller has already claimed
// (deleted from pendingWork and counted in callbacksInFlight while holding
// pendingMutex), then decrements callbacksInFlight. Must be called WITHOUT
// pendingMutex held. See the completion invariant on pendingWork.
func (a *MySQLApplier) invokeCallback(callback ApplyCallback, affectedRows int64, err error) {
	// Decrement in a defer so callbacksInFlight is balanced on every exit path,
	// including a panicking callback. Without this, a recovered panic upstream
	// would leave callbacksInFlight stuck above zero and wedge Wait() forever.
	// The panic still propagates after the deferred decrement runs.
	defer func() {
		a.pendingMutex.Lock()
		a.callbacksInFlight--
		a.pendingMutex.Unlock()
	}()
	callback(affectedRows, err)
}

// Wait blocks until all pending work is complete and all callbacks have been invoked.
// Checking callbacksInFlight in addition to len(pendingWork) is what upholds
// the "all callbacks have been invoked" half of the contract: claimed work has
// already left the map, but its callback may still be running (#765).
func (a *MySQLApplier) Wait(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		a.pendingMutex.Lock()
		pendingCount := len(a.pendingWork)
		callbacksInFlight := a.callbacksInFlight
		a.pendingMutex.Unlock()

		if pendingCount == 0 && callbacksInFlight == 0 {
			a.logger.Debug("Wait: all pending work complete")
			return nil
		}

		a.logger.Debug("Wait: waiting for pending work", "pendingCount", pendingCount, "callbacksInFlight", callbacksInFlight)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Continue loop
		}
	}
}

// Stop signals the applier to shut down gracefully.
// This does not control the synchronous methods like UpsertRows/DeleteKeys,
// which can continue after Stop() is called.
// This method is idempotent - calling it multiple times is safe.
func (a *MySQLApplier) Stop() error {
	a.Lock()

	// If already stopped or never started, return without error
	if a.stopped || !a.started {
		a.Unlock()
		a.logger.Debug("MySQLApplier already stopped or never started, skipping")
		return nil
	}

	a.logger.Debug("stopping MySQLApplier")

	// Cancel the context to signal workers to stop
	if a.cancelFunc != nil {
		a.cancelFunc()
	}

	// Close all shard buffers. Workers blocked in their select see the closed
	// buffer (ok == false) and return; workers mid-write finish, send their
	// completion, then return.
	for _, shard := range a.shards {
		shard.workers.seal()
		close(shard.chunkletBuffer)
	}

	// Mark as stopped before releasing lock
	a.stopped = true
	a.started = false

	// Release the lock before waiting for workers to finish
	a.Unlock()

	// Stop owns completion-channel closure. Retiring workers must never close
	// it: other workers may still be writing, or the pool may grow again.
	for _, shard := range a.shards {
		shard.workers.wait()
		close(shard.chunkletCompletions)
	}
	a.wg.Wait()

	a.logger.Debug("MySQLApplier stopped")
	return nil
}

// SetInitialWriteWorkers sets the per-target worker count that subsequent
// starts spawn, without spawning workers. Call before Start (or after Stop has
// returned). A running applier is unchanged.
func (a *MySQLApplier) SetInitialWriteWorkers(n int) {
	a.Lock()
	defer a.Unlock()
	if a.started {
		return
	}
	for _, shard := range a.shards {
		shard.writeWorkersCount = int32(max(1, n))
	}
}

// SetWriteWorkers reconciles the live write-worker count of EACH target to n,
// spawning new workers or parking existing ones as needed. It is idempotent
// and safe to call repeatedly from an autoscaler. n is clamped to a minimum of
// 1 so every target always makes some progress. Calls before Start or after
// Stop() begins are no-ops.
//
// Parking is cooperative: closing a worker's quit channel makes it exit the
// next time it returns to its select (after finishing any chunklet currently
// in flight), so no completion is ever lost. With several targets a controller
// may drive all of them from the busiest target's load, so a busy shard slows
// the entire move.
func (a *MySQLApplier) SetWriteWorkers(n int) {
	for _, shard := range a.shards {
		shard.workers.resize(n)
	}
}

// ActiveWriteWorkers returns the number of live write workers summed across
// all targets. SetWriteWorkers is per target, so with N targets this is N
// times the count SetWriteWorkers asked for.
func (a *MySQLApplier) ActiveWriteWorkers() int {
	var n int
	for _, shard := range a.shards {
		n += shard.workers.count()
	}
	return n
}

// Stats returns a point-in-time snapshot of the write pipeline, aggregated
// across shards: queue depth/cap are summed, and active workers is the sum of
// each shard's pool count — goroutines currently running, which during a shrink
// can briefly exceed the number of quit channels the pool still tracks, since a
// retired worker is removed from quits before it returns. The embedded mutex is
// held so the buffer reads cannot race Start()'s channel reinitialization on
// restart; len/cap on a closed channel are safe.
func (a *MySQLApplier) Stats() Stats {
	a.Lock()
	var queueDepth, queueCap int
	for _, shard := range a.shards {
		queueDepth += len(shard.chunkletBuffer)
		queueCap += cap(shard.chunkletBuffer)
	}
	a.Unlock()

	a.pendingMutex.Lock()
	pending := len(a.pendingWork)
	a.pendingMutex.Unlock()

	t := a.timings.percentiles()
	return Stats{
		QueueDepth:      queueDepth,
		QueueCap:        queueCap,
		PendingWork:     pending,
		ActiveWorkers:   a.ActiveWriteWorkers(),
		RowsPerChunklet: a.splits.mean(),
		QueueWaitP50:    t.queueWaitP50,
		QueueWaitP90:    t.queueWaitP90,
		BuildTimeP50:    t.buildP50,
		BuildTimeP90:    t.buildP90,
		WriteTimeP50:    t.writeP50,
		WriteTimeP90:    t.writeP90,
		HandoffP50:      t.handoffP50,
		HandoffP90:      t.handoffP90,
	}
}

// writeWorker processes chunklets for a specific shard until either its quit
// channel is closed (scale-down) or the shard's buffer is closed by Stop().
func (a *MySQLApplier) writeWorker(ctx context.Context, shard *shardTarget, quit <-chan struct{}) {
	workerID := shard.workerIDCounter.Add(1)

	// Drain chunkletBuffer until it is closed by Stop(). We deliberately do not
	// select on ctx.Done() here: every chunklet that made it into the buffer was
	// already registered in pendingWork by Apply(), so it MUST produce a
	// completion or Wait() will hang. If ctx is cancelled, writeChunklet returns
	// quickly with ctx.Err() and we forward that as an error completion — the
	// feedbackCoordinator then invokes the callback with the error and clears
	// pendingWork. Stop() is the canonical shutdown path: it cancels ctx (so
	// in-flight writes abort) and closes chunkletBuffer (so workers exit). The
	// quit channel is the scale-down path: a parked worker stops pulling new
	// chunklets but any chunklet already in flight still completes.
	for {
		var chunkletData chunklet
		select {
		case <-quit:
			a.logger.Debug("writeWorker parked (scale-down), exiting", "shardID", shard.shardID, "workerID", workerID)
			return
		case next, ok := <-shard.chunkletBuffer:
			if !ok {
				a.logger.Debug("writeWorker channel closed, exiting", "shardID", shard.shardID, "workerID", workerID)
				return
			}
			chunkletData = next
		}
		a.logger.Debug("writeWorker processing chunklet", "shardID", shard.shardID,
			"workerID", workerID, "workID", chunkletData.workID, "rowCount", len(chunkletData.rows))

		queueWait := time.Since(chunkletData.enqueuedAt)
		writeStart := time.Now()
		affectedRows, buildTime, err := a.writeChunklet(ctx, shard, chunkletData)
		writeTime := time.Since(writeStart)

		// Timed separately from the write: a worker blocked here is waiting
		// on the single feedbackCoordinator (which invokes the chunk
		// callback inline), not on the target, and while blocked it is not
		// pulling from chunkletBuffer either. Folding it into writeTime
		// would attribute a completion-path stall to the database, and
		// leaving it untimed hides it from the status block altogether,
		// which is the shape of "more write workers changed nothing" that is
		// otherwise very hard to see.
		handoffStart := time.Now()
		shard.chunkletCompletions <- chunkletCompletion{
			workID:       chunkletData.workID,
			shardID:      shard.shardID,
			affectedRows: affectedRows,
			err:          err,
		}
		a.timings.record(queueWait, buildTime, writeTime, time.Since(handoffStart))
	}
}

// writeChunklet writes a single chunklet (up to chunkletMaxRows or
// MaxStatementSizeBytes) to a specific shard. It returns the affected row
// count and, separately, how long the client-side statement build took — that
// portion holds no connection and is spent on spirit's own CPU, so Stats()
// reports it apart from the round trip (see Stats.BuildTimeP50).
func (a *MySQLApplier) writeChunklet(ctx context.Context, shard *shardTarget, chunkletData chunklet) (int64, time.Duration, error) {
	if len(chunkletData.rows) == 0 {
		return 0, 0, nil
	}
	buildStart := time.Now()

	// Fast path on shutdown: if ctx is already cancelled, fail the chunklet
	// without building the (potentially large) INSERT statement or burning
	// retries against a dead context. writeWorker relies on chunklets failing
	// quickly after cancellation so Stop() can drain the buffer promptly.
	if err := ctx.Err(); err != nil {
		return 0, time.Since(buildStart), err
	}

	// Bound the write, retries included — see chunkTaskTimeout.
	ctx, cancel := context.WithTimeout(ctx, chunkTaskTimeout)
	defer cancel()

	// The intersected source and target column lists are parallel — row.values[i]
	// is a value for source column sourceColumnNames[i], which corresponds to
	// target column at the same ordinal in targetColumnList. With column renames
	// the two lists differ; without renames they are identical.
	mapping := chunkletData.chunk.ColumnMapping
	_, targetColumnList := mapping.Columns()
	sourceColumnNames, _ := mapping.ColumnsSlice()

	// Resolve each column's type once per chunklet, not once per value. The
	// type is a property of the column, so the inner loop was re-doing a map
	// lookup and a type-string parse for every value of every row — measured
	// as ~14x the cost of the whole build on a 12-column row, and the reason
	// applier-build-p50 dominates applier-write-p50 on wide tables.
	// deleteKeysInClause does the same hoist for the same reason.
	//
	// Type lookup uses the source table by the source column name — the value
	// came from a source SELECT, and MySQL coerces on the destination INSERT
	// if the target column type has widened.
	sourceTable := mapping.SourceTable()
	colTypes := make([]table.ColumnType, len(sourceColumnNames))
	for i, colName := range sourceColumnNames {
		typeStr, ok := sourceTable.GetColumnMySQLType(colName)
		if !ok {
			return 0, time.Since(buildStart), fmt.Errorf("column %s not found in source table info", colName)
		}
		colTypes[i] = table.NewColumnType(typeStr)
	}

	// Build VALUES clauses for all rows in the chunklet
	valuesClauses := make([]string, 0, len(chunkletData.rows))
	values := make([]string, len(sourceColumnNames))
	for _, row := range chunkletData.rows {
		if len(sourceColumnNames) != len(row.values) {
			return 0, time.Since(buildStart), fmt.Errorf("column count mismatch: chunk %s has %d columns, but chunklet has %d values",
				chunkletData.chunk.String(), len(sourceColumnNames), len(row.values))
		}
		for i, value := range row.values {
			datum, err := table.NewDatumFromValueWithType(value, colTypes[i])
			if err != nil {
				return 0, time.Since(buildStart), fmt.Errorf("failed to convert value to datum for column %s: %w", sourceColumnNames[i], err)
			}
			// datum.String() returns a complete pre-escaped SQL literal
			// (NULL, a numeric, 0x… hex, or a "..."-quoted string). Safe
			// to concatenate into the VALUES clause as-is — see the
			// contract on Datum.String.
			values[i] = datum.String()
		}
		valuesClauses = append(valuesClauses, "("+strings.Join(values, ", ")+")")
	}

	// Build the INSERT statement — target columns, with renames applied.
	// Note: We use just the table name, not the fully qualified name, because
	// the database connection (shard.writeDB) already determines which database to write to
	query := fmt.Sprintf("INSERT IGNORE INTO %s (%s) VALUES %s",
		mapping.TargetTable().QuotedTableName,
		targetColumnList,
		strings.Join(valuesClauses, ", "),
	)

	buildTime := time.Since(buildStart)

	a.logger.Debug("writing chunklet to shard", "shardID", shard.shardID,
		"rowCount", len(chunkletData.rows), "table", mapping.TargetTable().TableName)

	// Execute the batch insert on this shard's database
	result, err := dbconn.RetryableTransaction(ctx, shard.writeDB, dbconn.IgnoreDupKeyWarnings, shard.dbConfig, query)
	if err != nil {
		return 0, buildTime, fmt.Errorf("failed to execute chunklet insert on shard %d: %w", shard.shardID, err)
	}

	return result, buildTime, nil
}

// feedbackCoordinator tracks chunklet completions from all shards and invokes callbacks when work is done.
// ctx is the worker context; once it is cancelled, completions for already-cleaned-up
// work are an expected part of shutdown rather than a bug (see Apply's ctx-cancel cleanup).
func (a *MySQLApplier) feedbackCoordinator(ctx context.Context) {
	defer a.wg.Done()
	a.logger.Debug("feedbackCoordinator started")

	// processCompletion handles a single chunklet completion.
	processCompletion := func(completion chunkletCompletion) {
		a.logger.Debug("feedbackCoordinator received chunklet completion",
			"workID", completion.workID, "shardID", completion.shardID)

		// Update work completion status
		a.pendingMutex.Lock()
		pending, exists := a.pendingWork[completion.workID]
		if !exists {
			a.pendingMutex.Unlock()
			// On shutdown, Apply's ctx-cancel cleanup deletes pendingWork while
			// chunklets are still in flight; their completions then arrive here
			// for work that no longer exists. That is expected teardown, not a
			// bug, so log it quietly. Outside cancellation it is a real anomaly.
			if ctx.Err() != nil || errors.Is(completion.err, context.Canceled) {
				a.logger.Debug("feedbackCoordinator received completion for unknown work during shutdown", "workID", completion.workID)
			} else {
				a.logger.Error("feedbackCoordinator received completion for unknown work", "workID", completion.workID)
			}
			return
		}

		// If there was an error, claim the work and invoke the callback
		// immediately. The claim (delete + callbacksInFlight increment, see
		// the completion invariant on pendingWork) is atomic under
		// pendingMutex so that:
		//  (a) Apply's ctx-cancel cleanup cannot find the entry and invoke
		//      the callback a second time. The original #765 fix released
		//      the lock between invoking the callback and deleting the
		//      entry, which opened exactly that double-invocation window.
		//  (b) Wait() — which requires pendingWork empty AND
		//      callbacksInFlight zero — cannot return until the callback
		//      has finished running.
		if completion.err != nil {
			callback := pending.callback
			delete(a.pendingWork, completion.workID)
			a.callbacksInFlight++
			a.pendingMutex.Unlock()
			a.invokeCallback(callback, 0, completion.err)
			return
		}

		// Update completion count and affected rows
		pending.completedChunklets++
		pending.totalAffectedRows += completion.affectedRows

		a.logger.Debug("feedbackCoordinator work progress", "workID", completion.workID,
			"completedChunklets", pending.completedChunklets, "totalChunklets", pending.totalChunklets)

		// Check if all chunklets for this work are complete
		if pending.completedChunklets == pending.totalChunklets {
			a.logger.Debug("feedbackCoordinator all chunklets complete, invoking callback", "workID", completion.workID)

			callback := pending.callback
			affectedRows := pending.totalAffectedRows

			// Claim the work (delete + callbacksInFlight increment) under
			// the lock, then invoke the callback. See the completion
			// invariant on pendingWork and the comment in the error path
			// above — Wait() cannot return until the callback has finished,
			// and no other path can invoke it again.
			delete(a.pendingWork, completion.workID)
			a.callbacksInFlight++
			a.pendingMutex.Unlock()
			a.invokeCallback(callback, affectedRows, nil)
		} else {
			a.pendingMutex.Unlock()
		}
	}

	// With one shard, read its completions directly: no merge goroutines.
	if len(a.shards) == 1 {
		for completion := range a.shards[0].chunkletCompletions {
			processCompletion(completion)
		}
		a.logger.Debug("feedbackCoordinator chunklet completions channel closed, exiting")
		return
	}

	// Create a merged channel to receive completions from all shards
	mergedCompletions := make(chan chunkletCompletion, defaultBufferSize)

	// Start goroutines to forward completions from each shard to the merged channel.
	// These use a simple range loop (no ctx.Done select) to ensure all completions
	// are forwarded even during shutdown. The shard channels will be closed once all
	// write workers for that shard finish, which is the authoritative signal.
	var forwardWg sync.WaitGroup
	for _, shard := range a.shards {
		forwardWg.Go(func() {
			for completion := range shard.chunkletCompletions {
				mergedCompletions <- completion
			}
		})
	}

	// Close merged channel when all shard channels are closed
	go func() {
		forwardWg.Wait()
		close(mergedCompletions)
	}()

	// Main loop: process completions until the merged channel is closed.
	// We do NOT exit on ctx.Done() here because write workers may still be
	// sending completions after writing data. Exiting early would leave
	// entries in pendingWork that are never cleared, causing Wait() to hang
	// or report incorrect results.
	for completion := range mergedCompletions {
		processCompletion(completion)
	}
	a.logger.Debug("feedbackCoordinator merged completions channel closed, exiting")
}

// resolveShardLocks maps each shard to the table lock that was acquired on
// that shard's own database connection. A LOCK TABLES ... WRITE held on a
// shard blocks writes from every other connection, so each shard's
// statements MUST execute on the connection holding that shard's lock —
// executing them on another shard's lock connection would silently write
// the rows to the wrong server.
//
// Returns nil if no locks were supplied (callers then use the regular
// per-shard write connections). Otherwise each distinct shard connection must
// receive exactly one lock (shards that share a connection share its lock) and
// every lock must belong to a shard; anything else is a caller bug (typically a
// lock taken on a different server than this applier writes to) and is an
// error returned before anything is executed.
func (a *MySQLApplier) resolveShardLocks(locks []*dbconn.TableLock) (map[int]*dbconn.TableLock, error) {
	if len(locks) == 0 {
		return nil, nil
	}
	// Index the locks by the connection they were acquired on. Several shards
	// may share one connection (and so one lock); two locks on one connection
	// are ambiguous and refused.
	lockByDB := make(map[*sql.DB]*dbconn.TableLock, len(locks))
	for i, lock := range locks {
		if lock == nil {
			return nil, fmt.Errorf("table lock %d is nil", i)
		}
		shardID := slices.IndexFunc(a.shards, func(s *shardTarget) bool { return s.writeDB == lock.DB() })
		if shardID == -1 {
			return nil, fmt.Errorf("table lock %d was not acquired on any target's connection", i)
		}
		if lockByDB[lock.DB()] != nil {
			return nil, fmt.Errorf("more than one table lock supplied for shard %d", shardID)
		}
		lockByDB[lock.DB()] = lock
	}
	shardLocks := make(map[int]*dbconn.TableLock, len(a.shards))
	for _, shard := range a.shards {
		lock := lockByDB[shard.writeDB]
		if lock == nil {
			return nil, fmt.Errorf("no table lock supplied for shard %d: writing under lock requires one lock per shard, acquired on that shard's connection", shard.shardID)
		}
		shardLocks[shard.shardID] = lock
	}
	return shardLocks, nil
}

// DeleteKeys deletes rows by their key values synchronously, broadcasting to all shards.
// Each entry in keys is one primary-key tuple of the original (typed)
// column values, in sourceTable.KeyColumns order.
// If locks is non-empty, each shard's delete is executed under the table lock
// that was acquired on that shard's own connection (one lock per shard,
// matched via resolveShardLocks).
//
// Note: we only track modifications by PRIMARY KEY, not by shard key (aka primary vindex).
// For this reason we can't extract the vindex value, and must instead broadcast
// the deletes to all shards. The vindex value is considered immutable, and we will
// error if it changes on an update.
//
// targetTable may be nil, meaning the same name as sourceTable. A different
// name is used by migrations (the `_new` table) and by the reverse feed of a
// sharded-source move, which writes back to the source's retired `_old` tables.
func (a *MySQLApplier) DeleteKeys(ctx context.Context, sourceTable, targetTable *table.TableInfo, keys [][]any, locks []*dbconn.TableLock) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	// For move operations, targetTable may be nil - use sourceTable for both
	if targetTable == nil {
		targetTable = sourceTable
	}
	// Resolve which lock belongs to which shard before executing anything,
	// so a missing lock fails loudly instead of partially applying.
	shardLocks, err := a.resolveShardLocks(locks)
	if err != nil {
		return 0, err
	}
	// Render the key tuples into the IN(...) element list via table.Datum,
	// the same type-aware path UpsertRows uses (see deleteKeysInClause).
	inClause, err := deleteKeysInClause(sourceTable, keys)
	if err != nil {
		return 0, err
	}

	// Build DELETE statement
	// Use just the table name, not the fully qualified name, because
	// the database connection (shard.writeDB) already determines which database to write to
	deleteStmt := fmt.Sprintf("DELETE FROM %s WHERE (%s) IN (%s)",
		targetTable.QuotedTableName,
		sqlescape.EscapeIdentifierList(sourceTable.KeyColumns),
		inClause,
	)

	a.logger.Debug("executing delete", "keyCount", len(keys), "table", targetTable.TableName, "shardCount", len(a.shards))

	// Execute deletes on all shards in parallel (broadcast)
	type result struct {
		affected int64
		err      error
	}
	results := make(chan result, len(a.shards))
	defer close(results)
	for _, shard := range a.shards {
		go func(shard *shardTarget) {
			var affected int64
			var err error
			// Execute under this shard's own lock if locks were provided.
			// The lock connection is the only connection allowed to write
			// to this shard's table while LOCK TABLES is held.
			if shardLocks != nil {
				// ExecUnderLock does not report affected rows; the total
				// is reported as the key count after collection, below.
				if err = shardLocks[shard.shardID].ExecUnderLock(ctx, deleteStmt); err != nil {
					err = fmt.Errorf("failed to execute delete under lock on shard %d: %w", shard.shardID, err)
				}
			} else {
				// Execute as a retryable transaction
				affected, err = dbconn.RetryableTransaction(ctx, shard.writeDB, dbconn.ErrorOnDupKey, shard.dbConfig, deleteStmt)
				if err != nil {
					err = fmt.Errorf("failed to execute delete on shard %d: %w", shard.shardID, err)
				}
			}
			results <- result{affected: affected, err: err}
		}(shard)
	}

	// Collect results from all shards
	var totalAffected int64
	var errs []error
	for range len(a.shards) {
		res := <-results
		if res.err != nil {
			errs = append(errs, res.err)
		} else {
			totalAffected += res.affected
		}
	}
	if len(errs) > 0 {
		return 0, errors.Join(errs...)
	}
	if shardLocks != nil {
		// Under lock the per-shard counts are unknown. Report the key count:
		// each key lives on at most one shard, so it is the upper bound on
		// rows deleted across the broadcast.
		return int64(len(keys)), nil
	}
	return totalAffected, nil
}

// UpsertRows performs an upsert (REPLACE INTO ... VALUES) synchronously,
// distributing rows across shards. The rows are LogicalRow structs containing
// inline row images from the binlog. If locks is non-empty, each shard's
// upsert is executed under the table lock that was acquired on that shard's
// own connection (one lock per shard, matched via resolveShardLocks).
//
// REPLACE semantics, and why we use them:
//
// MySQL's `REPLACE INTO target (cols) VALUES (...)` treats each value
// tuple as an INSERT, except that for any row in `target` that conflicts
// with the new row on PRIMARY KEY *or any UNIQUE index*, the old row is
// deleted before the new row is inserted. Per the docs, conflicts on
// multiple unique indexes can lead to multiple deletions for a single
// new row.
//
// Two implications matter for callers reading this code:
//
//  1. A single REPLACE may delete rows whose PKs are *not* in the
//     `rows` argument. If row B's image collides on a unique key with
//     some other row A currently in the destination (because A was the
//     previous holder of that unique value), REPLACE deletes A while
//     inserting B. A is then transiently missing from the destination
//     until its own event arrives in a later flush (or a later batch in
//     the same flush) and re-inserts it. This is what restores the
//     order-independence the pre-#821 deltaMap had with `REPLACE INTO
//     ... SELECT`. See block/spirit#847.
//
//  2. Eventual consistency. Between the moment REPLACE deletes A and
//     the moment A's image is re-applied, the destination is not a
//     valid snapshot of source — it has fewer rows. Spirit relies on
//     the bufferedMap being an *up-to-date and disjoint* representation
//     of pending changes (each PK appears at most once, holding the
//     latest row image) so that every transiently-deleted row will be
//     re-inserted as flushes progress. The destination converges back
//     to source's current state once the last unflushed event for each
//     affected PK has been applied. The post-cutover checksum (with
//     `FixDifferences=true`) is the backstop that catches any
//     divergence that survives.
//
// We supply inline row images rather than `REPLACE INTO ... SELECT FROM
// source`, so the read-after-commit race that motivated #746 does not
// apply.
//
// Sharding: we only track modifications by PRIMARY KEY, not by shard key (aka
// primary vindex). For this reason we could get in trouble if there was a PK
// update that mutated the vindex column. This is because we would only see the
// last operation (modification) and not know to DELETE from one of the shards.
//
// The way we address this, is we consider the vindex column immutable. The replication client is told
// that it should error if there are any updates to it, and the entire operation is canceled.
// The enforcement lives in pkg/change: the subscription resolves the sharding column to an ordinal
// (Subscription.ImmutableColumnOrdinal) and both processRowsEvent implementations fail fatally when
// an UPDATE's before/after images differ at that position (see change.checkImmutableColumn).
//
// This is likely not too big of a limitation, as Vitess itself recommends that vindex columns be immutable.
// If it turns out to be a problem, we can revisit tracking by other columns later.
//
// The sharding column and hash always come from the mapping's SOURCE table —
// the watched table whose row images we are routing.
func (a *MySQLApplier) UpsertRows(ctx context.Context, mapping *table.ColumnMapping, rows []LogicalRow, locks []*dbconn.TableLock) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	// Resolve which lock belongs to which shard before executing anything,
	// so a missing lock fails loudly instead of partially applying.
	shardLocks, err := a.resolveShardLocks(locks)
	if err != nil {
		return 0, err
	}

	sourceTable := mapping.SourceTable()
	// RowImage from the binlog contains ALL columns, including STORED
	// generated columns, so we must index it via ordinal positions in
	// the full column list — not via positions in NonGeneratedColumns.
	sourceOrdinal := mapping.SourceOrdinalIndices()
	sourceColumnNames, _ := mapping.ColumnsSlice()

	// Group rows by shard
	shardRows := make([][]LogicalRow, len(a.shards))
	if a.unsharded {
		for _, row := range rows {
			if !row.IsDeleted {
				shardRows[0] = append(shardRows[0], row)
			}
		}
	} else if err := a.routeRowImages(sourceTable, rows, shardRows); err != nil {
		return 0, err
	}

	// Execute upserts on each shard in parallel
	type result struct {
		affected int64
		err      error
	}
	shardsToCopy := len(a.shards)
	results := make(chan result, shardsToCopy)
	defer close(results)

	// Build the column list for the upsert statement from the mapping so a
	// renamed target (a migration's `_new` table, or the reverse feed's `_old`
	// tables) uses its own column list.
	_, columnList := mapping.Columns()

	// Resolve each column's type once for the whole fan-out, not once per
	// value. In order to create a datum we need to know the MySQL type, which
	// we get from the source table. This matters more here than on the copy
	// path: an upsert batch can be a handful of rows, so there is far less to
	// amortize the resolution over, and the final flush runs under the table
	// lock at cutover. colTypes is only read from here on, so sharing it
	// across the goroutines is safe.
	//
	// The values are binlog row images, not query results: a string column in
	// a charset other than utf8mb4 carries its own bytes, which must be emitted
	// with the column's charset introducer instead of quoted (see
	// TableInfo.BinlogColumnType).
	colTypes := make([]table.ColumnType, len(sourceOrdinal))
	for i := range sourceOrdinal {
		ct, err := sourceTable.BinlogColumnType(sourceColumnNames[i])
		if err != nil {
			return 0, fmt.Errorf("column %s: %w", sourceColumnNames[i], err)
		}
		colTypes[i] = ct
	}

	for shardID, rows := range shardRows {
		if len(rows) == 0 {
			shardsToCopy--
			continue
		}
		go func(sid int, r []LogicalRow) {
			// Build the VALUES clause
			valuesClauses := make([]string, 0, len(r))
			values := make([]string, len(sourceOrdinal))
			for _, logicalRow := range r {
				for i, colIdx := range sourceOrdinal {
					if colIdx >= len(logicalRow.RowImage) {
						results <- result{err: fmt.Errorf("column index %d exceeds row image length %d", colIdx, len(logicalRow.RowImage))}
						return
					}
					datum, err := table.NewDatumFromValueWithType(logicalRow.RowImage[colIdx], colTypes[i])
					if err != nil {
						results <- result{err: fmt.Errorf("failed to convert value to datum for column %s: %w", sourceColumnNames[i], err)}
						return
					}
					// datum.String() returns a complete pre-escaped SQL
					// literal (NULL, a numeric, 0x… hex, or a "..."-quoted
					// string). Safe to concatenate into the VALUES clause
					// as-is — see the contract on Datum.String.
					values[i] = datum.String()
				}
				valuesClauses = append(valuesClauses, "("+strings.Join(values, ", ")+")")
			}

			// See the function-level doc for the REPLACE rationale and the
			// eventual-consistency implications. Just the table name here —
			// the per-shard DB connection already determines which database
			// to write to.
			upsertStmt := fmt.Sprintf("REPLACE INTO %s (%s) VALUES %s",
				mapping.TargetTable().QuotedTableName,
				columnList,
				strings.Join(valuesClauses, ", "),
			)
			a.logger.Debug("executing upsert on shard",
				"shardID", sid,
				"rowCount", len(valuesClauses),
				"table", mapping.TargetTable().TableName,
			)
			var affected int64
			var err error

			// Execute under this shard's own lock if locks were provided.
			// The lock connection is the only connection allowed to write
			// to this shard's table while LOCK TABLES is held.
			if shardLocks != nil {
				if err = shardLocks[sid].ExecUnderLock(ctx, upsertStmt); err != nil {
					err = fmt.Errorf("failed to execute upsert under lock on shard %d: %w", sid, err)
				} else {
					// ExecUnderLock does not report affected rows, so return
					// the row count.
					affected = int64(len(valuesClauses))
				}
			} else {
				// Execute as a retryable transaction
				affected, err = dbconn.RetryableTransaction(ctx, a.shards[sid].writeDB, dbconn.ErrorOnDupKey, a.shards[sid].dbConfig, upsertStmt)
				if err != nil {
					err = fmt.Errorf("failed to execute upsert on shard %d: %w", sid, err)
				}
			}
			results <- result{affected: affected, err: err}
		}(shardID, rows)
	}

	// Collect results from shards that have work
	var totalAffected int64
	var errs []error
	for range shardsToCopy {
		res := <-results
		if res.err != nil {
			errs = append(errs, res.err)
		} else {
			totalAffected += res.affected
		}
	}
	if len(errs) > 0 {
		return 0, errors.Join(errs...)
	}
	return totalAffected, nil
}

// routeRowImages distributes the non-deleted binlog row images in rows across
// shardRows by hashing each image's sharding column. A binlog row image holds
// ALL columns, generated ones included, so the sharding column is located by
// its ordinal in the full column list.
func (a *MySQLApplier) routeRowImages(sourceTable *table.TableInfo, rows []LogicalRow, shardRows [][]LogicalRow) error {
	if sourceTable.ShardingColumn == "" {
		return errors.New("ShardingColumn not configured in TableInfo")
	}
	if sourceTable.HashFunc == nil {
		return errors.New("HashFunc not configured in TableInfo")
	}
	shardingOrdinal := slices.Index(sourceTable.Columns, sourceTable.ShardingColumn)
	if shardingOrdinal == -1 {
		return fmt.Errorf("sharding column %s not found in columns", sourceTable.ShardingColumn)
	}
	for _, row := range rows {
		if row.IsDeleted {
			continue // Skip deleted rows
		}
		if shardingOrdinal >= len(row.RowImage) {
			return fmt.Errorf("sharding column ordinal %d exceeds row image length %d", shardingOrdinal, len(row.RowImage))
		}
		shardingValue := row.RowImage[shardingOrdinal]
		hashValue, err := sourceTable.HashFunc(shardingValue)
		if err != nil {
			return fmt.Errorf("hash function error: %w", err)
		}
		shardID := a.shardForHash(hashValue)
		if shardID == -1 {
			return fmt.Errorf("no shard found for hash value %x (sharding column: %s, value: %v)",
				hashValue, sourceTable.ShardingColumn, shardingValue)
		}
		shardRows[shardID] = append(shardRows[shardID], row)
	}
	return nil
}

// GetTargets returns the target database configurations for direct access.
// This is used by operations like checksum that need to query targets directly.
func (a *MySQLApplier) GetTargets() []Target {
	return a.targets
}
