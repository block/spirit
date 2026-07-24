package applier

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/table"
)

// SingleTargetApplier applies rows to a single target database.
// It internally splits rows into chunklets for optimal batching and tracks
// completion to invoke callbacks when all chunklets for a set of rows are done.
type SingleTargetApplier struct {
	sync.Mutex

	target      Target
	dbConfig    *dbconn.DBConfig
	logger      *slog.Logger
	metricsSink metrics.Sink // nil disables the stats emitter

	// Internal chunklet processing
	chunkletBuffer      chan chunklet
	chunkletCompletions chan chunkletCompletion

	// Pending work tracking.
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

	// Worker management. writeWorkersCount is the initial worker count, set at
	// construction from ApplierConfig.Threads; the *live* count can change at
	// runtime via SetWriteWorkers (driven by the copier's autoscaler). The
	// fields below workerIDCounter are all guarded by scaleMu.
	writeWorkersCount int32        // initial worker count (start value)
	workerIDCounter   atomic.Int32 // monotonic worker id, for debug logging only

	scaleMu       sync.Mutex      // guards the worker pool: workerCtx, workerQuits, scalingClosed, and spawn/park
	workerCtx     context.Context // ctx the workers run under; set in Start
	workerQuits   []chan struct{} // one quit channel per live worker — closing one parks (exits) that worker
	activeWorkers atomic.Int32    // current live worker count
	scalingClosed bool            // set true when Stop begins, to block new spawns
	workersWg     sync.WaitGroup  // tracks write workers only

	// timings is a rolling window of per-chunklet queue-wait and write
	// durations, reported by Stats().
	timings timingRing

	// Context management
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup // tracks the feedbackCoordinator and stats-emitter goroutines

	// State management to make Start/Stop idempotent
	started bool
	stopped bool
}

// chunklet represents a small batch of rows for internal processing
// Limited by either chunkletMaxRows or MaxStatementSizeBytes, whichever is reached first
type chunklet struct {
	workID     int64        // ID of the parent work
	chunk      *table.Chunk // Original chunk for column info
	rows       []rowData    // Batch of rows limited by row count or size
	enqueuedAt time.Time    // when Apply() offered this chunklet to the buffer; queue wait = dequeue - enqueuedAt
}

// chunkletCompletion represents a completed chunklet
type chunkletCompletion struct {
	workID       int64 // ID of the parent work
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

// NewSingleTargetApplier creates a new SingleTargetApplier
func NewSingleTargetApplier(target Target, cfg *ApplierConfig) (*SingleTargetApplier, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &SingleTargetApplier{
		target:              target,
		dbConfig:            cfg.DBConfig,
		logger:              cfg.Logger,
		metricsSink:         cfg.MetricsSink,
		chunkletBuffer:      make(chan chunklet, defaultBufferSize),
		chunkletCompletions: make(chan chunkletCompletion, defaultBufferSize),
		pendingWork:         make(map[int64]*pendingWork),
		writeWorkersCount:   int32(cfg.Threads),
	}, nil
}

// Start initializes the applier's async write workers and begins processing.
// This does not control the synchronous methods like UpsertRows/DeleteKeys.
// This method is idempotent - calling it multiple times is safe.
//
// Lifecycle: callers MUST call Stop() to terminate the write workers and
// feedbackCoordinator. Cancelling the ctx passed here does NOT by itself
// shut down the goroutine pipeline — it only aborts in-flight writes.
// Workers exit when chunkletBuffer is closed (by Stop) or when their quit
// channel is closed (by SetWriteWorkers scaling down), and the coordinator
// exits when chunkletCompletions is closed (by Stop, after all workers exit).
// Failing to call Stop() will leak goroutines.
func (a *SingleTargetApplier) Start(ctx context.Context) error {
	a.Lock()
	defer a.Unlock()

	// If already started, return without error
	if a.started {
		a.logger.Debug("SingleTargetApplier already started, skipping")
		return nil
	}

	// If previously stopped, we need to reinitialize channels
	if a.stopped {
		a.logger.Info("restarting SingleTargetApplier after previous stop")
		a.chunkletBuffer = make(chan chunklet, defaultBufferSize)
		a.chunkletCompletions = make(chan chunkletCompletion, defaultBufferSize)
		a.workerIDCounter.Store(0)
		a.stopped = false
	}

	workerCtx, cancelFunc := context.WithCancel(ctx)
	a.cancelFunc = cancelFunc

	a.started = true
	a.logger.Debug("starting SingleTargetApplier", "writeWorkers", a.writeWorkersCount)

	// Reset the worker pool bookkeeping and record the context workers run
	// under, so SetWriteWorkers can spawn more later.
	a.scaleMu.Lock()
	a.workerCtx = workerCtx
	a.workerQuits = nil
	a.activeWorkers.Store(0)
	a.scalingClosed = false
	a.scaleMu.Unlock()

	// Start feedback coordinator before workers so completions are always
	// drained.
	a.wg.Add(1)
	go a.feedbackCoordinator(workerCtx)

	// Report pipeline gauges for the applier's lifetime. Exits on Stop()'s
	// context cancellation; joined via a.wg like the coordinator.
	if a.metricsSink != nil {
		a.wg.Go(func() {
			emitStatsLoop(workerCtx, a, a.metricsSink, a.logger)
		})
	}

	// Spawn the initial worker pool. SetWriteWorkers only takes scaleMu, so
	// calling it while holding the main lock is safe (no lock nesting on the
	// same mutex).
	a.SetWriteWorkers(int(a.writeWorkersCount))

	return nil
}

// Apply sends rows to be written to the target database
func (a *SingleTargetApplier) Apply(ctx context.Context, chunk *table.Chunk, rows [][]any, callback ApplyCallback) error {
	if len(rows) == 0 {
		// No rows to apply, invoke callback immediately
		callback(0, nil)
		return nil
	}

	// Assign a work ID for tracking
	workID := a.nextWorkID.Add(1)

	// Convert rows to rowData format
	rowDataList := make([]rowData, len(rows))
	for i, row := range rows {
		rowDataList[i] = rowData{values: row}
	}

	// Split into chunklets based on both row count and size thresholds
	// Then convert row batches into chunklets with metadata
	rowBatches := splitRowsIntoChunklets(rowDataList)
	chunklets := make([]chunklet, len(rowBatches))
	for i, batch := range rowBatches {
		chunklets[i] = chunklet{
			workID: workID,
			chunk:  chunk,
			rows:   batch,
		}
	}

	// Register the pending work
	a.pendingMutex.Lock()
	a.pendingWork[workID] = &pendingWork{
		callback:           callback,
		totalChunklets:     len(chunklets),
		completedChunklets: 0,
		totalAffectedRows:  0,
	}
	a.pendingMutex.Unlock()

	// Send chunklets to buffer.
	// If ctx is cancelled mid-send, we must clean up pendingWork before
	// returning. Otherwise the entry remains with totalChunklets > completedChunklets
	// forever, hanging Wait(). Chunklets already in the buffer may still be
	// processed; their completions arrive at the coordinator after pendingWork
	// has been deleted and are dropped (logged as "unknown work").
	//
	// The claim (delete + callbacksInFlight increment, see the completion
	// invariant on pendingWork) is atomic under pendingMutex, so this cleanup
	// and the feedbackCoordinator can never both invoke the callback: if the
	// coordinator already claimed the work (e.g. an error completion raced
	// this cancellation), `exists` is false and we return without invoking.
	for _, chunkletData := range chunklets {
		// Stamp before the send so queue wait includes send-side
		// backpressure: when the buffer is full, time blocked here is
		// exactly "waiting for a write worker".
		chunkletData.enqueuedAt = time.Now()
		select {
		case a.chunkletBuffer <- chunkletData:
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
func (a *SingleTargetApplier) invokeCallback(callback ApplyCallback, affectedRows int64, err error) {
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
func (a *SingleTargetApplier) Wait(ctx context.Context) error {
	// Wait until there's no pending work
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

		// Wait for next tick or context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Continue loop
		}
	}
}

// Stop signals the applier to shut down gracefully
// This does not control the synchronous methods like UpsertRows/DeleteKeys,
// which can continue after Stop() is called.
// This method is idempotent - calling it multiple times is safe.
func (a *SingleTargetApplier) Stop() error {
	a.Lock()

	// If already stopped or never started, return without error
	if a.stopped || !a.started {
		a.Unlock()
		a.logger.Debug("SingleTargetApplier already stopped or never started, skipping")
		return nil
	}

	a.logger.Debug("stopping SingleTargetApplier")

	// Cancel the context to signal workers to stop
	if a.cancelFunc != nil {
		a.cancelFunc()
	}

	// Mark as stopped before releasing lock
	a.stopped = true
	a.started = false

	// Release the lock before waiting for workers to finish
	a.Unlock()

	// Close scaling first and wait out any in-flight SetWriteWorkers, so no new
	// worker is spawned (no workersWg.Add) after we begin shutdown. Acquiring
	// scaleMu guarantees any concurrent scale call has finished its Adds before
	// we Wait below.
	a.scaleMu.Lock()
	a.scalingClosed = true
	a.scaleMu.Unlock()

	// Close the chunklet buffer to signal no more work. Workers blocked in their
	// select see the closed buffer (ok == false) and return; workers mid-write
	// finish, send their completion, then return.
	close(a.chunkletBuffer)

	// Wait for all write workers to exit, THEN close the completions channel.
	// This is now the sole owner of that close — decoupled from worker count,
	// which is what lets the count move at runtime. The coordinator drains any
	// remaining completions and exits when the channel closes.
	a.workersWg.Wait()
	close(a.chunkletCompletions)
	a.wg.Wait()

	a.logger.Debug("SingleTargetApplier stopped")
	return nil
}

// SetWriteWorkers reconciles the live write-worker count to n, spawning new
// workers or parking existing ones as needed. It is idempotent and safe to call
// repeatedly from the autoscaler. n is clamped to a minimum of 1 so the applier
// always makes some progress. Calls after Stop() begins are no-ops.
//
// Parking is cooperative: closing a worker's quit channel makes it exit the next
// time it returns to its select (after finishing any chunklet currently in
// flight), so no completion is ever lost.
func (a *SingleTargetApplier) SetWriteWorkers(n int) {
	if n < 1 {
		n = 1
	}
	a.scaleMu.Lock()
	defer a.scaleMu.Unlock()

	// No-op before Start has recorded the worker context (the exported API may
	// be called too early) or once Stop has begun shutting down. In both states
	// we must not spawn workers: a nil workerCtx would panic the worker on its
	// first writeChunklet (ctx.Err()), and post-shutdown spawns would race the
	// workersWg.Wait() in Stop.
	if a.workerCtx == nil || a.scalingClosed {
		return
	}

	cur := len(a.workerQuits)
	switch {
	case n > cur:
		for range n - cur {
			quit := make(chan struct{})
			a.workerQuits = append(a.workerQuits, quit)
			a.activeWorkers.Add(1)
			a.workersWg.Add(1)
			go a.writeWorker(a.workerCtx, quit)
		}
		a.logger.Info("scaled write workers up", "from", cur, "to", n)
	case n < cur:
		// Park the most-recently-added workers by closing their quit channels.
		for i := cur - 1; i >= n; i-- {
			close(a.workerQuits[i])
		}
		a.workerQuits = a.workerQuits[:n]
		a.logger.Info("scaled write workers down", "from", cur, "to", n)
	}
}

// ActiveWriteWorkers returns the current number of live write workers.
func (a *SingleTargetApplier) ActiveWriteWorkers() int {
	return int(a.activeWorkers.Load())
}

// Stats returns a point-in-time snapshot of the write pipeline. The embedded
// mutex is held so the buffer read cannot race Start()'s channel
// reinitialization on restart; len/cap on a closed channel are safe.
func (a *SingleTargetApplier) Stats() Stats {
	a.Lock()
	queueDepth := len(a.chunkletBuffer)
	queueCap := cap(a.chunkletBuffer)
	a.Unlock()

	a.pendingMutex.Lock()
	pending := len(a.pendingWork)
	a.pendingMutex.Unlock()

	queueWaitP50, queueWaitP90, writeTimeP50, writeTimeP90 := a.timings.percentiles()
	return Stats{
		QueueDepth:    queueDepth,
		QueueCap:      queueCap,
		PendingWork:   pending,
		ActiveWorkers: int(a.activeWorkers.Load()),
		QueueWaitP50:  queueWaitP50,
		QueueWaitP90:  queueWaitP90,
		WriteTimeP50:  writeTimeP50,
		WriteTimeP90:  writeTimeP90,
	}
}

// writeWorker processes chunklets from the buffer until either its quit channel
// is closed (scale-down) or the buffer is closed by Stop().
func (a *SingleTargetApplier) writeWorker(ctx context.Context, quit <-chan struct{}) {
	defer a.workersWg.Done()
	defer a.activeWorkers.Add(-1)
	workerID := a.workerIDCounter.Add(1)

	// Every chunklet that made it into the buffer was already registered in
	// pendingWork by Apply(), so it MUST produce a completion or Wait() will
	// hang. If ctx is cancelled, writeChunklet returns quickly with ctx.Err()
	// and we forward that as an error completion — the feedbackCoordinator then
	// invokes the callback with the error and clears pendingWork. Stop() is the
	// canonical shutdown path: it cancels ctx (so in-flight writes abort) and
	// closes chunkletBuffer (so workers exit). The quit channel is the
	// scale-down path: a parked worker stops pulling new chunklets but any
	// chunklet already in flight still completes.
	for {
		select {
		case <-quit:
			a.logger.Debug("writeWorker parked (scale-down), exiting", "workerID", workerID)
			return
		case chunkletData, ok := <-a.chunkletBuffer:
			if !ok {
				a.logger.Debug("writeWorker channel closed, exiting", "workerID", workerID)
				return
			}
			a.logger.Debug("writeWorker processing chunklet", "workerID", workerID, "workID", chunkletData.workID, "rowCount", len(chunkletData.rows))

			queueWait := time.Since(chunkletData.enqueuedAt)
			writeStart := time.Now()
			affectedRows, err := a.writeChunklet(ctx, chunkletData)
			a.timings.record(queueWait, time.Since(writeStart))

			a.chunkletCompletions <- chunkletCompletion{
				workID:       chunkletData.workID,
				affectedRows: affectedRows,
				err:          err,
			}
		}
	}
}

// writeChunklet writes a single chunklet (up to chunkletMaxRows or MaxStatementSizeBytes)
func (a *SingleTargetApplier) writeChunklet(ctx context.Context, chunkletData chunklet) (int64, error) {
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

	// The intersected source and target column lists are parallel — row.values[i]
	// is a value for source column sourceColumnNames[i], which corresponds to
	// target column at the same ordinal in targetColumnList. With column renames
	// the two lists differ; without renames they are identical.
	mapping := chunkletData.chunk.ColumnMapping
	_, targetColumnList := mapping.Columns()
	sourceColumnNames, _ := mapping.ColumnsSlice()

	// Build VALUES clauses for all rows in the chunklet
	var valuesClauses []string
	for _, row := range chunkletData.rows {
		if len(sourceColumnNames) != len(row.values) {
			return 0, fmt.Errorf("column count mismatch: chunk %s has %d columns, but chunklet has %d values",
				chunkletData.chunk.String(), len(sourceColumnNames), len(row.values))
		}
		var values []string
		for i, value := range row.values {
			// Type lookup uses the source table by the source column name —
			// the value came from a source SELECT, and MySQL coerces on the
			// destination INSERT if the target column type has widened.
			columnType, ok := mapping.SourceTable().GetColumnMySQLType(sourceColumnNames[i])
			if !ok {
				return 0, fmt.Errorf("column %s not found in source table info", sourceColumnNames[i])
			}
			datum, err := table.NewDatumFromValue(value, columnType)
			if err != nil {
				return 0, fmt.Errorf("failed to convert value to datum for column %s: %w", sourceColumnNames[i], err)
			}
			// datum.String() returns a complete pre-escaped SQL literal
			// (NULL, a numeric, 0x… hex, or a "..."-quoted string). Safe
			// to concatenate into the VALUES clause as-is — see the
			// contract on Datum.String.
			values = append(values, datum.String())
		}
		valuesClauses = append(valuesClauses, fmt.Sprintf("(%s)", strings.Join(values, ", ")))
	}

	// Build the INSERT statement — target columns, with renames applied.
	query := fmt.Sprintf("INSERT IGNORE INTO %s (%s) VALUES %s",
		mapping.TargetTable().QuotedTableName,
		targetColumnList,
		strings.Join(valuesClauses, ", "),
	)

	a.logger.Debug("writing chunklet", "rowCount", len(chunkletData.rows), "table", chunkletData.chunk.ColumnMapping.TargetTable().TableName)

	// Execute the batch insert
	result, err := dbconn.RetryableTransaction(ctx, a.target.DB, dbconn.IgnoreDupKeyWarnings, a.dbConfig, query)
	if err != nil {
		return 0, fmt.Errorf("failed to execute chunklet insert: %w", err)
	}

	return result, nil
}

// feedbackCoordinator tracks chunklet completions and invokes callbacks when work is done.
// ctx is the worker context; once it is cancelled, completions for already-cleaned-up
// work are an expected part of shutdown rather than a bug (see Apply's ctx-cancel cleanup).
func (a *SingleTargetApplier) feedbackCoordinator(ctx context.Context) {
	defer a.wg.Done()
	a.logger.Debug("feedbackCoordinator started")

	// processCompletion handles a single chunklet completion.
	processCompletion := func(completion chunkletCompletion) {
		a.logger.Debug("feedbackCoordinator received chunklet completion", "workID", completion.workID)

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

	// Main loop: process completions until the channel is closed.
	// We do NOT exit on ctx.Done() here because write workers may still be
	// sending completions after writing data. Exiting early would leave
	// entries in pendingWork that are never cleared, causing Wait() to hang
	// or report incorrect results. The channel will be closed once all write
	// workers finish (including their deferred close logic), which is the
	// authoritative signal that no more completions will arrive.
	for completion := range a.chunkletCompletions {
		processCompletion(completion)
	}
	a.logger.Debug("feedbackCoordinator chunklet completions channel closed, exiting")
}

// singleLock extracts the only lock from locks. It returns nil for an
// empty slice (no lock requested) and errors if more than one lock is
// supplied: the SingleTargetApplier writes through exactly one server,
// so receiving multiple locks indicates a caller bug (e.g. per-shard
// locks passed to a single-target applier).
func singleLock(locks []*dbconn.TableLock) (*dbconn.TableLock, error) {
	switch len(locks) {
	case 0:
		return nil, nil
	case 1:
		return locks[0], nil
	default:
		return nil, fmt.Errorf("SingleTargetApplier expects at most one table lock, got %d", len(locks))
	}
}

// Each entry in keys is one primary-key tuple of the original (typed)
// column values, in sourceTable.KeyColumns order.
// If locks contains a lock, the delete is executed under the table lock.
func (a *SingleTargetApplier) DeleteKeys(ctx context.Context, sourceTable, targetTable *table.TableInfo, keys [][]any, locks []*dbconn.TableLock) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	lock, err := singleLock(locks)
	if err != nil {
		return 0, err
	}
	// For move operations, targetTable may be nil - use sourceTable for both
	if targetTable == nil {
		targetTable = sourceTable
	}
	// Render the key tuples into the IN(...) element list via table.Datum,
	// the same type-aware path UpsertRows uses (see deleteKeysInClause).
	inClause, err := deleteKeysInClause(sourceTable, keys)
	if err != nil {
		return 0, err
	}

	// Build DELETE statement
	deleteStmt := fmt.Sprintf("DELETE FROM %s WHERE (%s) IN (%s)",
		targetTable.QuotedTableName,
		table.QuoteColumns(sourceTable.KeyColumns),
		inClause,
	)

	a.logger.Debug("executing delete", "keyCount", len(keys), "table", targetTable.TableName)

	// Execute under lock if provided
	if lock != nil {
		if err := lock.ExecUnderLock(ctx, deleteStmt); err != nil {
			return 0, fmt.Errorf("failed to execute delete under lock: %w", err)
		}
		// We don't get affected rows from ExecUnderLock, so return the key count
		return int64(len(keys)), nil
	}

	// Execute as a retryable transaction
	affectedRows, err := dbconn.RetryableTransaction(ctx, a.target.DB, dbconn.ErrorOnDupKey, a.dbConfig, deleteStmt)
	if err != nil {
		return 0, fmt.Errorf("failed to execute delete: %w", err)
	}

	return affectedRows, nil
}

// UpsertRows performs an upsert (REPLACE INTO ... VALUES) synchronously.
// The rows are LogicalRow structs containing inline row images from the
// binlog. If locks contains a lock, the upsert is executed under the table lock.
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
func (a *SingleTargetApplier) UpsertRows(ctx context.Context, mapping *table.ColumnMapping, rows []LogicalRow, locks []*dbconn.TableLock) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	lock, err := singleLock(locks)
	if err != nil {
		return 0, err
	}
	_, targetColumnList := mapping.Columns()
	sourceColumnNames, _ := mapping.ColumnsSlice()
	// RowImage from the binlog contains ALL columns, including STORED
	// generated columns, so we must index it via ordinal positions in
	// the full column list — not via positions in NonGeneratedColumns.
	// The sharded applier does the same; see sharded.go.
	intersectedColumns := mapping.SourceOrdinalIndices()

	// Build the VALUES clause from the row images
	var valuesClauses []string
	for _, logicalRow := range rows {
		if logicalRow.IsDeleted {
			continue // Skip deleted rows
		}
		// Convert the row image to a VALUES clause
		var values []string
		for i, colIndex := range intersectedColumns {
			if colIndex >= len(logicalRow.RowImage) {
				return 0, fmt.Errorf("column index %d exceeds row image length %d", colIndex, len(logicalRow.RowImage))
			}
			// In order to create a datum we need to know the MySQL type,
			// which we can get from the source table.
			columnType, ok := mapping.SourceTable().GetColumnMySQLType(sourceColumnNames[i])
			if !ok {
				return 0, fmt.Errorf("column %s not found in table info", sourceColumnNames[i])
			}
			datum, err := table.NewDatumFromValue(logicalRow.RowImage[colIndex], columnType)
			if err != nil {
				return 0, fmt.Errorf("failed to convert value to datum for column %s: %w", sourceColumnNames[i], err)
			}
			// datum.String() returns a complete pre-escaped SQL literal
			// (NULL, a numeric, 0x… hex, or a "..."-quoted string). Safe
			// to concatenate into the VALUES clause as-is — see the
			// contract on Datum.String.
			values = append(values, datum.String())
		}
		valuesClauses = append(valuesClauses, fmt.Sprintf("(%s)", strings.Join(values, ", ")))
	}

	if len(valuesClauses) == 0 {
		return 0, nil
	}

	// See the function-level doc for the REPLACE-vs-ODKU rationale and
	// the eventual-consistency implications of REPLACE deleting rows on
	// unique-key conflicts.
	upsertStmt := fmt.Sprintf("REPLACE INTO %s (%s) VALUES %s",
		mapping.TargetTable().QuotedTableName,
		targetColumnList,
		strings.Join(valuesClauses, ", "),
	)

	a.logger.Debug("executing upsert", "rowCount", len(valuesClauses), "table", mapping.TargetTable().TableName, "path", "replace-into")

	// Execute under lock if provided
	if lock != nil {
		if err := lock.ExecUnderLock(ctx, upsertStmt); err != nil {
			return 0, fmt.Errorf("failed to execute upsert under lock: %w", err)
		}
		// We don't get affected rows from ExecUnderLock, so return the row count
		return int64(len(valuesClauses)), nil
	}

	// Execute as a retryable transaction
	affectedRows, err := dbconn.RetryableTransaction(ctx, a.target.DB, dbconn.ErrorOnDupKey, a.dbConfig, upsertStmt)
	if err != nil {
		return 0, fmt.Errorf("failed to execute upsert: %w", err)
	}

	return affectedRows, nil
}

// GetTargets returns the target database configuration for direct access.
// This is used by operations like checksum that need to query targets directly.
func (a *SingleTargetApplier) GetTargets() []Target {
	return []Target{a.target}
}
