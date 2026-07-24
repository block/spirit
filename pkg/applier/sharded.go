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
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/table"
)

// ShardedApplier applies rows to multiple target databases based on a Vitess-style vindex.
// It extracts a specific column value from each row, applies a hash function to it,
// and routes the row to the appropriate shard based on the hash value and key ranges.
//
// The sharding column and hash function are configured per-table in the TableInfo.ShardingColumn
// and TableInfo.HashFunc fields. This allows different tables to use different sharding keys
// in multi-table migrations.
type ShardedApplier struct {
	sync.Mutex

	shards      []*shardTarget
	targets     []Target // Original target configurations
	dbConfig    *dbconn.DBConfig
	logger      *slog.Logger
	metricsSink metrics.Sink // nil disables the stats emitter

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
	wg         sync.WaitGroup

	// timings is a rolling window of per-chunklet queue-wait and write
	// durations across all shards, reported by Stats(). A single shared ring
	// is deliberate: the bottleneck question ("is the write side saturated?")
	// does not need per-shard attribution.
	timings timingRing

	// State management to make Start/Stop idempotent
	stopped bool
	started bool
}

// shardTarget represents a single shard with its own connection, key range, and workers
type shardTarget struct {
	shardID              int
	writeDB              *sql.DB
	keyRange             keyRange // Parsed key range for this shard
	chunkletBuffer       chan shardedChunklet
	chunkletCompletions  chan shardedChunkletCompletion
	writeWorkersCount    int32
	writeWorkersFinished int32
	workerIDCounter      int32
	logger               *slog.Logger
	dbConfig             *dbconn.DBConfig
}

// shardedChunklet represents a chunklet destined for a specific shard
type shardedChunklet struct {
	workID     int64        // ID of the parent work
	shardID    int          // Which shard this belongs to
	chunk      *table.Chunk // Original chunk for column info
	rows       []rowData    // Rows for this shard
	enqueuedAt time.Time    // when Apply() offered this chunklet to the shard buffer; queue wait = dequeue - enqueuedAt
}

// shardedChunkletCompletion represents a completed sharded chunklet
type shardedChunkletCompletion struct {
	workID       int64 // ID of the parent work
	shardID      int   // Which shard this came from
	affectedRows int64 // Rows affected by this chunklet
	err          error // Error if any
}

// NewShardedApplier creates a new ShardedApplier with multiple target databases.
//
// The sharding column and hash function are configured per-table in the TableInfo.ShardingColumn
// and TableInfo.HashFunc fields. This allows different tables to use different sharding keys
// in multi-table migrations.
func NewShardedApplier(targets []Target, cfg *ApplierConfig) (*ShardedApplier, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	shards := make([]*shardTarget, len(targets))
	for i, target := range targets {
		// Parse the key range
		kr, err := parseKeyRange(target.KeyRange)
		if err != nil {
			return nil, fmt.Errorf("failed to parse key range for shard %d: %w", i, err)
		}

		shards[i] = &shardTarget{
			shardID:             i,
			writeDB:             target.DB,
			keyRange:            kr,
			chunkletBuffer:      make(chan shardedChunklet, defaultBufferSize),
			chunkletCompletions: make(chan shardedChunkletCompletion, defaultBufferSize),
			writeWorkersCount:   int32(cfg.Threads), // threads are not "divided" per shard, but are each shard. This is documented in pkg/move/move.go:WriteThreads.
			logger:              cfg.Logger,
			dbConfig:            cfg.DBConfig,
		}
	}

	// Validate that key ranges do not overlap
	for i := range shards {
		for j := i + 1; j < len(shards); j++ {
			if shards[i].keyRange.overlaps(shards[j].keyRange) {
				return nil, fmt.Errorf("key ranges overlap: shard %d (%s: [0x%016x, 0x%016x)) and shard %d (%s: [0x%016x, 0x%016x))",
					i, targets[i].KeyRange, shards[i].keyRange.start, shards[i].keyRange.end,
					j, targets[j].KeyRange, shards[j].keyRange.start, shards[j].keyRange.end)
			}
		}
	}

	// Log the parsed key ranges for debugging
	for i, shard := range shards {
		cfg.Logger.Info("parsed key range for shard",
			"shardID", i,
			"keyRange", targets[i].KeyRange,
			"start", fmt.Sprintf("0x%016x", shard.keyRange.start),
			"end", fmt.Sprintf("0x%016x", shard.keyRange.end))
	}

	return &ShardedApplier{
		shards:      shards,
		targets:     targets,
		dbConfig:    cfg.DBConfig,
		logger:      cfg.Logger,
		metricsSink: cfg.MetricsSink,
		pendingWork: make(map[int64]*pendingWork),
	}, nil
}

// Start initializes all shard workers and begins processing.
// This method is idempotent and can restart the applier after Stop() is called.
//
// Lifecycle: callers MUST call Stop() to terminate the per-shard write workers
// and the single feedbackCoordinator. Cancelling the ctx passed here does NOT
// by itself shut down the goroutine pipeline — it only aborts in-flight writes.
// Workers for a shard exit when its chunkletBuffer is closed (by Stop), and the
// coordinator exits when every shard's chunkletCompletions has been closed
// (by the last worker's defer per shard). Failing to call Stop() will leak
// goroutines.
func (a *ShardedApplier) Start(ctx context.Context) error {
	a.Lock()
	defer a.Unlock()

	// If already started, return without error
	if a.started {
		a.logger.Info("ShardedApplier already started, skipping")
		return nil
	}

	// If previously stopped, we need to reinitialize channels
	if a.stopped {
		a.logger.Info("restarting ShardedApplier after previous stop")
		for _, shard := range a.shards {
			shard.chunkletBuffer = make(chan shardedChunklet, defaultBufferSize)
			shard.chunkletCompletions = make(chan shardedChunkletCompletion, defaultBufferSize)
			shard.writeWorkersFinished = 0
			shard.workerIDCounter = 0
		}
		a.stopped = false
	}

	workerCtx, cancelFunc := context.WithCancel(ctx)
	a.cancelFunc = cancelFunc

	a.started = true
	a.logger.Info("starting ShardedApplier", "shardCount", len(a.shards))

	// Start workers for each shard
	for i := range a.shards {
		for range a.shards[i].writeWorkersCount {
			a.wg.Add(1)
			go a.writeWorker(workerCtx, a.shards[i])
		}
	}

	// Start a single feedback coordinator for all shards
	a.wg.Add(1)
	go a.feedbackCoordinator(workerCtx)

	// Report pipeline gauges (aggregated across shards) for the applier's
	// lifetime. Exits on Stop()'s context cancellation; joined via a.wg.
	if a.metricsSink != nil {
		a.wg.Go(func() {
			emitStatsLoop(workerCtx, a, a.metricsSink, a.logger)
		})
	}

	return nil
}

// Apply sends rows to be written to the appropriate target shards.
// Rows are distributed across shards based on the sharding column and hash function
// configured in the chunk's Table.ShardingColumn and Table.HashFunc.
func (a *ShardedApplier) Apply(ctx context.Context, chunk *table.Chunk, rows [][]any, callback ApplyCallback) error {
	if len(rows) == 0 {
		// No rows to apply, invoke callback immediately
		callback(0, nil)
		return nil
	}

	// Extract sharding configuration from the table
	shardingColumn := chunk.Table.ShardingColumn
	hashFunc := chunk.Table.HashFunc

	if shardingColumn == "" {
		return errors.New("ShardingColumn not configured in TableInfo")
	}
	if hashFunc == nil {
		return errors.New("HashFunc not configured in TableInfo")
	}

	a.logger.Info("Apply called", "rowCount", len(rows), "shardingColumn", shardingColumn, "table", chunk.Table.TableName)

	// Find the ordinal position of the sharding column within non-generated columns.
	// This is important because the rows passed to Apply() only contain non-generated columns
	// (they come from SELECT queries that exclude generated columns).
	shardingOrdinal, err := chunk.Table.GetNonGeneratedColumnOrdinal(shardingColumn)
	if err != nil {
		return err
	}

	a.logger.Info("Found sharding column", "shardingColumn", shardingColumn, "ordinal", shardingOrdinal)

	// Assign a work ID for tracking
	workID := a.nextWorkID.Add(1)

	// Group rows by shard
	shardRows := make([][]rowData, len(a.shards))
	for _, row := range rows {
		// Extract the sharding column value
		if shardingOrdinal >= len(row) {
			return fmt.Errorf("sharding column ordinal %d exceeds row length %d", shardingOrdinal, len(row))
		}
		shardingValue := row[shardingOrdinal]

		// Apply the hash function to get the hash value
		hashValue, err := hashFunc(shardingValue)
		if err != nil {
			return fmt.Errorf("hash function error: %w", err)
		}

		// Find which shard's key range contains this hash value
		shardID := -1
		for i, shard := range a.shards {
			if shard.keyRange.contains(hashValue) {
				shardID = i
				break
			}
		}
		if shardID == -1 {
			return fmt.Errorf("no shard found for hash value %x (sharding column: %s, value: %v)",
				hashValue, shardingColumn, shardingValue)
		}

		// Add to the appropriate shard's row list
		shardRows[shardID] = append(shardRows[shardID], rowData{values: row})
	}

	// Split rows into chunklets based on both row count and size thresholds
	var allChunklets []shardedChunklet
	for shardID, rows := range shardRows {
		if len(rows) == 0 {
			continue
		}

		// Use shared helper to split rows into chunklets
		// Then convert row batches into sharded chunklets with metadata
		rowBatches := splitRowsIntoChunklets(rows)
		for _, batch := range rowBatches {
			allChunklets = append(allChunklets, shardedChunklet{
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

// invokeCallback runs the callback for work the caller has already claimed
// (deleted from pendingWork and counted in callbacksInFlight while holding
// pendingMutex), then decrements callbacksInFlight. Must be called WITHOUT
// pendingMutex held. See the completion invariant on pendingWork.
func (a *ShardedApplier) invokeCallback(callback ApplyCallback, affectedRows int64, err error) {
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
func (a *ShardedApplier) Wait(ctx context.Context) error {
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

// Stop signals the applier to shut down gracefully
func (a *ShardedApplier) Stop() error {
	a.Lock()

	// If already stopped or never started, return without error
	if a.stopped || !a.started {
		a.Unlock()
		a.logger.Debug("ShardedApplier already stopped or never started, skipping")
		return nil
	}

	a.logger.Debug("Stopping ShardedApplier")

	if a.cancelFunc != nil {
		a.cancelFunc()
	}

	// Close all shard buffers
	for _, shard := range a.shards {
		close(shard.chunkletBuffer)
	}

	// Mark as stopped before releasing lock
	a.stopped = true
	a.started = false

	// Release the lock before waiting for workers to finish
	a.Unlock()

	// Wait for all workers to finish
	a.wg.Wait()

	a.logger.Debug("ShardedApplier stopped")
	return nil
}

// Stats returns a point-in-time snapshot of the write pipeline, aggregated
// across shards: queue depth/cap are summed, and active workers is the sum of
// each shard's live (started minus finished) workers. The embedded mutex is
// held so the buffer reads cannot race Start()'s channel reinitialization on
// restart; len/cap on a closed channel are safe.
func (a *ShardedApplier) Stats() Stats {
	a.Lock()
	var queueDepth, queueCap, activeWorkers int
	for _, shard := range a.shards {
		queueDepth += len(shard.chunkletBuffer)
		queueCap += cap(shard.chunkletBuffer)
		if a.started {
			activeWorkers += int(shard.writeWorkersCount - atomic.LoadInt32(&shard.writeWorkersFinished))
		}
	}
	a.Unlock()

	a.pendingMutex.Lock()
	pending := len(a.pendingWork)
	a.pendingMutex.Unlock()

	queueWaitP50, queueWaitP90, writeTimeP50, writeTimeP90 := a.timings.percentiles()
	return Stats{
		QueueDepth:    queueDepth,
		QueueCap:      queueCap,
		PendingWork:   pending,
		ActiveWorkers: activeWorkers,
		QueueWaitP50:  queueWaitP50,
		QueueWaitP90:  queueWaitP90,
		WriteTimeP50:  writeTimeP50,
		WriteTimeP90:  writeTimeP90,
	}
}

// writeWorker processes chunklets for a specific shard
func (a *ShardedApplier) writeWorker(ctx context.Context, shard *shardTarget) {
	defer a.wg.Done()
	workerID := atomic.AddInt32(&shard.workerIDCounter, 1)

	defer func() {
		finishedCount := atomic.AddInt32(&shard.writeWorkersFinished, 1)
		a.logger.Debug("writeWorker finished", "shardID", shard.shardID, "workerID", workerID,
			"finishedCount", finishedCount, "totalWorkers", shard.writeWorkersCount)

		// If all write workers for this shard are finished, close its completions channel
		if finishedCount == shard.writeWorkersCount {
			a.logger.Debug("writeWorker all workers finished for shard, closing completions channel",
				"shardID", shard.shardID)
			close(shard.chunkletCompletions)
		}
	}()

	// Drain chunkletBuffer until it is closed by Stop(). We deliberately do not
	// select on ctx.Done() here: every chunklet that made it into the buffer was
	// already registered in pendingWork by Apply(), so it MUST produce a
	// completion or Wait() will hang. If ctx is cancelled, writeChunklet returns
	// quickly with ctx.Err() and we forward that as an error completion — the
	// feedbackCoordinator then invokes the callback with the error and clears
	// pendingWork. Stop() is the canonical shutdown path: it cancels ctx (so
	// in-flight writes abort) and closes chunkletBuffer (so workers exit).
	for chunkletData := range shard.chunkletBuffer {
		a.logger.Debug("writeWorker processing chunklet", "shardID", shard.shardID,
			"workerID", workerID, "workID", chunkletData.workID, "rowCount", len(chunkletData.rows))

		queueWait := time.Since(chunkletData.enqueuedAt)
		writeStart := time.Now()
		affectedRows, err := a.writeChunklet(ctx, shard, chunkletData)
		a.timings.record(queueWait, time.Since(writeStart))

		shard.chunkletCompletions <- shardedChunkletCompletion{
			workID:       chunkletData.workID,
			shardID:      shard.shardID,
			affectedRows: affectedRows,
			err:          err,
		}
	}
	a.logger.Debug("writeWorker channel closed, exiting", "shardID", shard.shardID, "workerID", workerID)
}

// writeChunklet writes a single chunklet to a specific shard
func (a *ShardedApplier) writeChunklet(ctx context.Context, shard *shardTarget, chunkletData shardedChunklet) (int64, error) {
	if len(chunkletData.rows) == 0 {
		return 0, nil
	}

	// Fast path on shutdown: if ctx is already cancelled, fail the chunklet
	// without building the (potentially large) INSERT statement or burning
	// retries against a dead context. writeWorker relies on chunklets failing
	// quickly after cancellation so Stop() can drain the buffer promptly.
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	// Create a context with timeout for the entire operation
	ctx, cancel := context.WithTimeout(ctx, chunkTaskTimeout)
	defer cancel()

	// Get the intersected column names
	columnList, _ := chunkletData.chunk.ColumnMapping.Columns()
	columnNames, _ := chunkletData.chunk.ColumnMapping.ColumnsSlice()

	// Build VALUES clauses for all rows in the chunklet
	var valuesClauses []string
	for _, row := range chunkletData.rows {
		if len(columnNames) != len(row.values) {
			return 0, fmt.Errorf("column count mismatch: chunk %s has %d columns, but chunklet has %d values",
				chunkletData.chunk.String(), len(columnNames), len(row.values))
		}
		var values []string
		for i, value := range row.values {
			columnType, ok := chunkletData.chunk.ColumnMapping.TargetTable().GetColumnMySQLType(columnNames[i])
			if !ok {
				return 0, fmt.Errorf("column %s not found in table info", columnNames[i])
			}
			datum, err := table.NewDatumFromValue(value, columnType)
			if err != nil {
				return 0, fmt.Errorf("failed to convert value to datum for column %s: %w", columnNames[i], err)
			}
			// datum.String() returns a complete pre-escaped SQL literal
			// (NULL, a numeric, 0x… hex, or a "..."-quoted string). Safe
			// to concatenate into the VALUES clause as-is — see the
			// contract on Datum.String.
			values = append(values, datum.String())
		}
		valuesClauses = append(valuesClauses, fmt.Sprintf("(%s)", strings.Join(values, ", ")))
	}

	// Build the INSERT statement
	// Note: We use just the table name, not the fully qualified name, because
	// the database connection (shard.writeDB) already determines which database to write to
	query := fmt.Sprintf("INSERT IGNORE INTO %s (%s) VALUES %s",
		chunkletData.chunk.ColumnMapping.TargetTable().QuotedTableName,
		columnList,
		strings.Join(valuesClauses, ", "),
	)

	a.logger.Debug("writing chunklet to shard", "shardID", shard.shardID,
		"rowCount", len(chunkletData.rows), "table", chunkletData.chunk.ColumnMapping.TargetTable().TableName)

	// Execute the batch insert on this shard's database
	result, err := dbconn.RetryableTransaction(ctx, shard.writeDB, dbconn.IgnoreDupKeyWarnings, shard.dbConfig, query)
	if err != nil {
		return 0, fmt.Errorf("failed to execute chunklet insert on shard %d: %w", shard.shardID, err)
	}

	return result, nil
}

// feedbackCoordinator tracks chunklet completions from all shards and invokes callbacks when work is done.
// ctx is the worker context; once it is cancelled, completions for already-cleaned-up
// work are an expected part of shutdown rather than a bug (see Apply's ctx-cancel cleanup).
func (a *ShardedApplier) feedbackCoordinator(ctx context.Context) {
	defer a.wg.Done()
	a.logger.Debug("feedbackCoordinator started")

	// processCompletion handles a single chunklet completion.
	processCompletion := func(completion shardedChunkletCompletion) {
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
		//      the callback a second time.
		//  (b) Wait() — which requires pendingWork empty AND
		//      callbacksInFlight zero — cannot return until the callback
		//      has finished running (#765; the single-target applier was
		//      fixed first, this mirrors it).
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

	// Create a merged channel to receive completions from all shards
	mergedCompletions := make(chan shardedChunkletCompletion, defaultBufferSize)

	// Start goroutines to forward completions from each shard to the merged channel.
	// These use a simple range loop (no ctx.Done select) to ensure all completions
	// are forwarded even during shutdown. The shard channels will be closed once all
	// write workers for that shard finish, which is the authoritative signal.
	var forwardWg sync.WaitGroup
	for _, shard := range a.shards {
		forwardWg.Add(1)
		go func(s *shardTarget) {
			defer forwardWg.Done()
			for completion := range s.chunkletCompletions {
				mergedCompletions <- completion
			}
		}(shard)
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
// statements MUST execute on the transaction holding that shard's lock —
// executing them on another shard's lock connection would silently write
// the rows to the wrong server.
//
// Returns nil if no locks were supplied (callers then use the regular
// per-shard write connections). If locks were supplied but any shard has
// no matching lock, an error is returned before anything is executed.
func (a *ShardedApplier) resolveShardLocks(locks []*dbconn.TableLock) (map[int]*dbconn.TableLock, error) {
	if len(locks) == 0 {
		return nil, nil
	}
	shardLocks := make(map[int]*dbconn.TableLock, len(a.shards))
	for _, shard := range a.shards {
		for _, lock := range locks {
			if lock != nil && lock.DB() == shard.writeDB {
				shardLocks[shard.shardID] = lock
				break
			}
		}
		if shardLocks[shard.shardID] == nil {
			return nil, fmt.Errorf("no table lock supplied for shard %d: writing under lock requires one lock per shard, acquired on that shard's connection", shard.shardID)
		}
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
// Note: the sharded applier does not allow any transformations!
// The targetTable argument is intentionally ignored.
// This also means that table names between source and target must be the same.
func (a *ShardedApplier) DeleteKeys(ctx context.Context, sourceTable, _ *table.TableInfo, keys [][]any, locks []*dbconn.TableLock) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	// Resolve which lock belongs to which shard before executing anything,
	// so a missing lock fails loudly instead of partially applying.
	shardLocks, err := a.resolveShardLocks(locks)
	if err != nil {
		return 0, err
	}
	// Create a context with timeout for the entire operation
	// This prevents hanging indefinitely if shards are unresponsive
	ctx, cancel := context.WithTimeout(ctx, chunkTaskTimeout)
	defer cancel()

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
		sourceTable.QuotedTableName,
		table.QuoteColumns(sourceTable.KeyColumns),
		inClause,
	)

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
			// The lock transaction is the only connection allowed to write
			// to this shard's table while LOCK TABLES is held.
			if shardLocks != nil {
				if err = shardLocks[shard.shardID].ExecUnderLock(ctx, deleteStmt); err != nil {
					err = fmt.Errorf("failed to execute delete under lock on shard %d: %w", shard.shardID, err)
				} else {
					// We can't know the actual affected rows when using lock, so estimate
					affected = 0
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
	return totalAffected, nil
}

// UpsertRows performs upserts synchronously, distributing across shards.
// The rows are LogicalRow structs containing inline row images from the
// binlog. Each shard issues `REPLACE INTO target (cols) VALUES (...)`;
// the REPLACE semantics — and their eventual-consistency implications
// for callers — are documented on SingleTargetApplier.UpsertRows. The
// short version: REPLACE may delete rows whose PKs are not in the
// `rows` argument (via the unique-key conflict resolution) and those
// rows are re-inserted by their own events in subsequent batches.
//
// If locks is non-empty, each shard's upsert is executed under the table lock
// that was acquired on that shard's own connection (one lock per shard,
// matched via resolveShardLocks).
//
// Note: we only track modifications by PRIMARY KEY, not be shard key (aka primary vindex).
// For this reason we could get in trouble if there was a PK update that mutated the vindex column.
// This is because we would only see the last operation (modification) and not know to DELETE
// from one of the shards.
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
// Note: the sharded applier does not allow any transformations!
// The targetTable argument is intentionally ignored.
// This also means that table names between source and target must be the same.
func (a *ShardedApplier) UpsertRows(ctx context.Context, mapping *table.ColumnMapping, rows []LogicalRow, locks []*dbconn.TableLock) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	// Resolve which lock belongs to which shard before executing anything,
	// so a missing lock fails loudly instead of partially applying.
	shardLocks, err := a.resolveShardLocks(locks)
	if err != nil {
		return 0, err
	}

	// Create a context with timeout for the entire operation
	ctx, cancel := context.WithTimeout(ctx, chunkTaskTimeout)
	defer cancel()

	sourceTable := mapping.SourceTable()
	if sourceTable.ShardingColumn == "" {
		return 0, errors.New("ShardingColumn not configured in TableInfo")
	}
	if sourceTable.HashFunc == nil {
		return 0, errors.New("HashFunc not configured in TableInfo")
	}

	// Find the ordinal position of the sharding column within ALL columns
	// (since RowImage from binlog contains ALL columns, including generated ones)
	shardingOrdinal := slices.Index(sourceTable.Columns, sourceTable.ShardingColumn)
	sourceOrdinal := mapping.SourceOrdinalIndices()
	if shardingOrdinal == -1 {
		return 0, fmt.Errorf("sharding column %s not found in columns", sourceTable.ShardingColumn)
	}

	// Group rows by shard
	shardRows := make([][]LogicalRow, len(a.shards))
	for _, row := range rows {
		if row.IsDeleted {
			continue // Skip deleted rows
		}

		// Extract the sharding column value from the row image
		if shardingOrdinal >= len(row.RowImage) {
			return 0, fmt.Errorf("sharding column ordinal %d exceeds row image length %d", shardingOrdinal, len(row.RowImage))
		}
		shardingValue := row.RowImage[shardingOrdinal]

		// Apply the hash function to get the hash value
		hashValue, err := sourceTable.HashFunc(shardingValue)
		if err != nil {
			return 0, fmt.Errorf("hash function error: %w", err)
		}

		// Find which shard's key range contains this hash value
		shardID := -1
		for i, shard := range a.shards {
			if shard.keyRange.contains(hashValue) {
				shardID = i
				break
			}
		}
		if shardID == -1 {
			return 0, fmt.Errorf("no shard found for hash value %x (sharding column: %s, value: %v)",
				hashValue, sourceTable.ShardingColumn, shardingValue)
		}
		shardRows[shardID] = append(shardRows[shardID], row)
	}

	// Execute upserts on each shard in parallel
	type result struct {
		affected int64
		err      error
	}
	shardsToCopy := len(a.shards)
	results := make(chan result, shardsToCopy)
	defer close(results)

	// Build column list for the upsert statement
	columnList := table.QuoteColumns(sourceTable.NonGeneratedColumns)

	for shardID, rows := range shardRows {
		if len(rows) == 0 {
			shardsToCopy--
			continue
		}
		go func(sid int, r []LogicalRow) {
			// Build the VALUES clause
			var valuesClauses []string
			for _, logicalRow := range r {
				var values []string
				for i, colIdx := range sourceOrdinal {
					if colIdx >= len(logicalRow.RowImage) {
						results <- result{err: fmt.Errorf("column index %d exceeds row image length %d", colIdx, len(logicalRow.RowImage))}
						return
					}
					value := logicalRow.RowImage[colIdx]
					col := sourceTable.NonGeneratedColumns[i]
					columnType, ok := sourceTable.GetColumnMySQLType(col)
					if !ok {
						results <- result{err: fmt.Errorf("column %s not found in table info", col)}
						return
					}
					datum, err := table.NewDatumFromValue(value, columnType)
					if err != nil {
						results <- result{err: fmt.Errorf("failed to convert value to datum for column %s: %w", col, err)}
						return
					}
					// datum.String() returns a complete pre-escaped SQL
					// literal (NULL, a numeric, 0x… hex, or a "..."-quoted
					// string). Safe to concatenate into the VALUES clause
					// as-is — see the contract on Datum.String.
					values = append(values, datum.String())
				}
				valuesClauses = append(valuesClauses, fmt.Sprintf("(%s)", strings.Join(values, ", ")))
			}

			// See ShardedApplier.UpsertRows (and SingleTargetApplier.UpsertRows)
			// for the REPLACE rationale and the eventual-consistency
			// implications. Just the table name here — the per-shard DB
			// connection already determines which database to write to.
			upsertStmt := fmt.Sprintf("REPLACE INTO %s (%s) VALUES %s",
				sourceTable.QuotedTableName,
				columnList,
				strings.Join(valuesClauses, ", "),
			)
			a.logger.Debug("executing upsert on shard",
				"shardID", sid,
				"rowCount", len(valuesClauses),
				"table", sourceTable.TableName,
			)
			var affected int64
			var err error

			// Execute under this shard's own lock if locks were provided.
			// The lock transaction is the only connection allowed to write
			// to this shard's table while LOCK TABLES is held.
			if shardLocks != nil {
				if err = shardLocks[sid].ExecUnderLock(ctx, upsertStmt); err != nil {
					err = fmt.Errorf("failed to execute upsert under lock on shard %d: %w", sid, err)
				} else {
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

// GetTargets returns the target database configurations for direct access.
// This is used by operations like checksum that need to query targets directly.
func (a *ShardedApplier) GetTargets() []Target {
	return a.targets
}
