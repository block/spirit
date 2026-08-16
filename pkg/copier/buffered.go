package copier

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/autoscale"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/throttler"
	"github.com/block/spirit/pkg/utils"
)

// The buffered copier implements a producer/consumer pattern
// where multiple reader goroutines read chunks from the source table,
// and then send them to an Applier which breaks them into chunklets.
// It closely matches the DBLog algorithm:
// https://netflixtechblog.com/dblog-a-generic-change-data-capture-framework-69351fb9099b

type buffered struct {
	sync.Mutex

	applier       applier.Applier
	chunker       table.Chunker
	concurrency   int
	rowsPerSecond atomic.Uint64
	chunkSize     atomic.Uint64 // size of the most recently claimed chunk; see Copier.ChunkSize
	isInvalid     atomic.Bool
	errMu         sync.Mutex // guards firstErr
	firstErr      error      // first error that invalidated the copy (any goroutine)
	startTime     time.Time
	throttler     throttler.Throttler
	dbConfig      *dbconn.DBConfig
	logger        *slog.Logger
	metricsSink   metrics.Sink
	autoscale     AutoscaleConfig

	// Read-worker pool management, symmetric with the applier's write-worker
	// pool (SetWriteWorkers/ActiveWriteWorkers). concurrency above is the
	// initial reader count; the *live* count can change at runtime via
	// SetReadWorkers. Unlike write workers, readers also exit naturally when
	// the chunker is exhausted, so instead of a WaitGroup (whose Add would
	// race Wait once the count hits zero) the pool is a mutex-guarded count
	// with a condition variable: Run waits for liveReaders to reach zero and
	// closes scaling under the same mutex, so no reader can be spawned after
	// the drain has been observed.
	readScaleMu       sync.Mutex         // guards the fields below and spawn/park
	readersDone       *sync.Cond         // signalled on every reader exit; created in Run
	readerCtx         context.Context    // ctx readers run under; set in Run
	readerCancel      context.CancelFunc // cancels readerCtx; used to abort siblings on reader error
	readerQuits       []chan struct{}    // one quit channel per live reader — closing one parks (exits) that reader
	liveReaders       int                // current live reader count
	readScalingClosed bool               // set true when the pool has drained, to block new spawns
}

// Assert that buffered implements the Copier interface, and ChunkCopier for
// the tests that step through the copy one chunk at a time.
var (
	_ Copier      = (*buffered)(nil)
	_ ChunkCopier = (*buffered)(nil)
)

// CopyChunk copies a single chunk synchronously: it reads the chunk's rows,
// writes them through the applier, and blocks until the write has completed
// and chunker feedback has been sent. It exists for tests that need to drive
// the copy deterministically (see ChunkCopier); Run does not use it.
//
// It starts the applier if needed (Start is idempotent, so this composes with
// a later Run on the same copier). It does not stop it: write workers stay up
// for the next call, and the runner's Close (or Run's own Stop) tears them
// down.
//
// If ctx is cancelled while waiting for the applier, CopyChunk returns
// ctx.Err() without waiting for the callback — and if the apply then
// completes anyway, the callback still runs later on the applier's
// coordinator goroutine, feeding the chunker for a chunk whose caller was
// told it failed. The in-tree appliers make that branch unreachable (they
// guarantee callback delivery on every path, including cancellation); it
// exists as defense against a non-conforming applier.
func (c *buffered) CopyChunk(ctx context.Context, chunk *table.Chunk) error {
	if err := c.applier.Start(ctx); err != nil {
		return fmt.Errorf("failed to start applier: %w", err)
	}
	c.throttler.BlockWait(ctx)
	c.chunkSize.Store(chunk.ChunkSize)
	startTime := time.Now()
	rows, err := c.readChunkData(ctx, chunk)
	if err != nil {
		return fmt.Errorf("failed to read chunk data: %w", err)
	}
	chunk.ActualBytes = rowsByteSize(rows)
	// The callback runs on the applier's feedback coordinator goroutine; done
	// closes only after feedback and metrics are sent, so both have completed
	// before CopyChunk returns. Empty chunks take the same path — the applier
	// invokes the callback immediately.
	done := make(chan struct{})
	var applyErr error
	callback := func(affectedRows int64, err error) {
		defer close(done)
		if err != nil {
			applyErr = err
			return
		}
		totalTime := time.Since(startTime)
		c.chunker.Feedback(chunk, totalTime, uint64(affectedRows))
		if metricsErr := c.sendMetrics(ctx, totalTime, chunk.ChunkSize, uint64(affectedRows)); metricsErr != nil {
			// Metrics failures don't fail the copy; log and continue.
			c.logger.Error("error sending metrics from copier", "error", metricsErr)
		}
	}
	if err := c.applier.Apply(ctx, chunk, rows, callback); err != nil {
		return fmt.Errorf("failed to apply rows: %w", err)
	}
	// The applier guarantees the callback is invoked exactly once per Apply
	// that returned nil: worker errors and cancellation are delivered as
	// error completions, and its feedback coordinator drains until the
	// completions channel closes rather than exiting on ctx.Done(). The ctx
	// branch below is defense-in-depth against a non-conforming future
	// applier; if it fires, the callback (chunker feedback + metrics) may
	// still run later on the coordinator goroutine.
	select {
	case <-done:
		return applyErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// readChunkData reads all rows from a chunk into memory
func (c *buffered) readChunkData(ctx context.Context, chunk *table.Chunk) ([][]any, error) {
	// Build the SELECT query to read full row data
	columnList, _ := chunk.ColumnMapping.Columns()
	query := fmt.Sprintf("SELECT %s FROM %s FORCE INDEX (PRIMARY) WHERE %s",
		columnList,
		chunk.Table.QuotedTableName,
		chunk.String(),
	)

	c.logger.Debug("reading chunk data", "chunk", chunk.String(), "query", query)

	// Use the chunk's table DB connection so each chunk reads from its own source.
	// This is important for N:M moves where chunks from different sources
	// need to read from different database connections.
	rows, err := chunk.Table.DB().QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query chunk data: %w", err)
	}
	defer utils.CloseAndLog(rows)

	// Get column count for scanning
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	var rowDataList [][]any
	for rows.Next() {
		// Create slice to hold the row values
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// Scan the row
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		rowDataList = append(rowDataList, values)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	c.logger.Debug("read rows from chunk", "rowCount", len(rowDataList), "chunk", chunk.String())
	return rowDataList, nil
}

// rowsByteSize estimates the in-memory footprint of a chunk's rows, for the
// memory-based dynamic chunker. It is an approximation of payload size (the
// scanned column values), not exact Go heap accounting — good enough to servo
// chunk row-count toward a byte budget, and cheap because the rows are already
// in hand. database/sql scans into `any`, so the concrete types are whatever
// the driver yields (go-sql-driver/mysql: []byte, string, int64, float64,
// bool, time.Time, or nil).
func rowsByteSize(rows [][]any) uint64 {
	var total uint64
	for _, row := range rows {
		for _, v := range row {
			total += datumByteSize(v)
		}
	}
	return total
}

// datumByteSize returns the approximate byte size of a single scanned value.
// Variable-length values are sized by their contents; fixed-width scalars use a
// nominal width. The exact constants matter little: the servo cares about the
// relative size of chunks, and any consistent measure converges.
//
// Every value counts as at least 1 byte, even an empty string or []byte. This
// keeps the whole-chunk sum non-zero for any chunk that has rows, so a
// zero-byte total unambiguously means an empty (gap) chunk — which is how the
// byte sizer detects and skips gaps (see dynamicChunkSizer.feedbackBytes).
// Without the floor, a chunk of entirely empty variable-length values would sum
// to zero and be misread as a gap.
func datumByteSize(v any) uint64 {
	switch t := v.(type) {
	case nil:
		return 1
	case []byte:
		return max(1, uint64(len(t)))
	case string:
		return max(1, uint64(len(t)))
	case time.Time:
		return 16
	default:
		// int64, float64, bool, and other fixed-width scalars.
		return 8
	}
}

func (c *buffered) isHealthy(ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}
	return !c.isInvalid.Load()
}

func (c *buffered) StartTime() time.Time {
	c.Lock()
	defer c.Unlock()
	return c.startTime
}

// Run copies all rows from the source to the target table, blocking until
// the copy completes or fails. Run must not be called more than once per
// copier instance: it resets the read-worker pool state that SetReadWorkers
// reconciles against, so a second concurrent Run would corrupt the first's
// pool accounting.
func (c *buffered) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	c.Lock()
	c.startTime = time.Now()
	c.Unlock()
	go c.estimateRowsPerSecondLoop(ctx) // estimate rows while copying

	// Start the applier
	if err := c.applier.Start(ctx); err != nil {
		return fmt.Errorf("failed to start applier: %w", err)
	}

	// Experimental: start the dual read/write autoscaler. It runs for the lifetime
	// of the copy and stops when ctx is cancelled (deferred above). It only
	// engages when the applier supports dynamic scaling (SingleTargetApplier)
	// AND the throttler provides a continuous load signal (GradualThrottler);
	// otherwise the pool stays fixed.
	if as := c.autoscalerIfEnabled(); as != nil {
		go as.run(ctx)
	}

	// Start the read-worker pool. It starts at c.concurrency and can be
	// resized at runtime via SetReadWorkers. readerCtx replaces the old
	// errgroup context: a reader that fails cancels it so sibling readers
	// abort their in-flight reads promptly.
	readerCtx, readerCancel := context.WithCancel(ctx)
	defer readerCancel()
	c.readScaleMu.Lock()
	if c.readersDone == nil {
		c.readersDone = sync.NewCond(&c.readScaleMu)
	}
	c.readerCtx = readerCtx
	c.readerCancel = readerCancel
	c.readerQuits = nil
	c.liveReaders = 0
	c.readScalingClosed = false
	c.readScaleMu.Unlock()
	c.logger.Debug("starting read workers", "count", c.concurrency)
	c.SetReadWorkers(c.concurrency)

	// Wait for the reader pool to drain (chunker exhausted, error, or ctx
	// cancelled). Observing zero and closing scaling happen under the same
	// mutex, so a concurrent SetReadWorkers cannot spawn a reader after the
	// drain has been observed.
	c.readScaleMu.Lock()
	for c.liveReaders > 0 {
		c.readersDone.Wait()
	}
	c.readScalingClosed = true
	c.readerQuits = nil
	c.readScaleMu.Unlock()

	// Reader errors are recorded via setInvalid (first error wins) rather
	// than returned through an errgroup, so pick them up here. They take
	// precedence over applier.Wait/Stop errors below, as before.
	err := c.getFirstErr()

	// Wait for the applier to finish processing all pending work
	// This ensures all callbacks have been invoked before we return
	if waitErr := c.applier.Wait(ctx); waitErr != nil && err == nil {
		err = waitErr
	}

	// "Stop" the applier. This will free up the gouroutines that
	// were used for copying, but it won't close the DB connections
	if closeErr := c.applier.Stop(); closeErr != nil && err == nil {
		err = closeErr
	}

	// A failure inside an async applier callback (e.g. a chunklet the target
	// rejected with a warning) is reported to the callback in the applier's own
	// goroutine, which may run after the reader pool drained — too late for the
	// getFirstErr read above — and applier.Wait returns nil in that case.
	// Re-check so callers get the real root cause instead of a generic "copy
	// failed" message (or, worse, a nil error that looks like success).
	if err == nil {
		err = c.getFirstErr()
	}
	return err
}

// autoscalerIfEnabled returns the experimental dual read/write autoscaler to run
// for this copy, or nil when it should not engage: autoscaling disabled, an
// applier without dynamic scaling (ShardedApplier), or a throttler without a
// continuous load signal. Only GradualThrottler implementations (the Aurora
// throttlers, or a multi-throttler containing one) provide that signal —
// binary throttlers like replica lag protect via the hard-stop only, and
// scaling blind against them would just ramp to the maximum unguided.
func (c *buffered) autoscalerIfEnabled() *autoScaler {
	if !c.autoscale.Enabled {
		return nil
	}
	scaler, ok := c.applier.(writeScaler)
	if !ok {
		c.logger.Info("autoscaling enabled but this applier does not support dynamic write threads; running with a fixed pool")
		return nil
	}
	gradual, ok := c.throttler.(throttler.GradualThrottler)
	if !ok {
		c.logger.Warn("autoscaling enabled but no continuous load signal is available (requires an Aurora target); write threads stay fixed at the starting value",
			"write_threads", c.autoscale.StartThreads)
		return nil
	}
	c.logger.Info("starting experimental autoscaler: write-thread scaling engaged",
		"start", c.autoscale.StartThreads, "max", c.autoscale.MaxThreads,
		"low_watermark", autoscale.LowWatermark, "high_watermark", autoscale.HighWatermark)
	as := newAutoScaler(gradual, scaler, c.autoscale.StartThreads, c.autoscale.MaxThreads, c.logger, c.metricsSink)
	// Read scaling engages whenever the write side does: the copier's own
	// reader pool is runtime-resizable (SetReadWorkers) and every applier
	// reports the queue snapshot (Stats) the arbiter needs.
	maxRead := resolveReadCeiling(c.autoscale.MaxReadThreads, c.concurrency)
	c.logger.Info("read-worker scaling engaged",
		"start", c.concurrency, "max", maxRead)
	as.enableReadScaling(c, c.applier, c.concurrency, maxRead)
	return as
}

// readWorker reads chunks and sends them to the applier. It exits when the
// chunker is exhausted, the copy is invalidated, or its quit channel is closed
// (scale-down via SetReadWorkers). The quit check sits right after BlockWait —
// the spot where an idle reader parks — so a parked reader exits as soon as
// the throttler releases it, without claiming another chunk.
func (c *buffered) readWorker(ctx context.Context, quit <-chan struct{}) error {
	c.logger.Debug("readWorker started", "isRead", c.chunker.IsRead())

	for !c.chunker.IsRead() && c.isHealthy(ctx) {
		c.throttler.BlockWait(ctx)

		select {
		case <-quit:
			c.logger.Debug("readWorker parked (scale-down), exiting")
			return nil
		default:
		}

		// Re-check health after BlockWait: a reader can sit blocked in the
		// throttler for a long time, and the copy may have been cancelled or
		// invalidated while it waited. Exit here rather than claiming one
		// more chunk against a dead copy.
		if !c.isHealthy(ctx) {
			c.logger.Debug("readWorker unhealthy after BlockWait, exiting")
			return nil
		}

		c.logger.Debug("readWorker calling chunker.Next()")
		chunk, err := c.chunker.Next()
		if err != nil {
			if errors.Is(err, table.ErrTableIsRead) {
				c.logger.Debug("readWorker table is read, exiting")
				return nil
			}
			c.logger.Error("readWorker got error from chunker", "error", err)
			c.setInvalid(err)
			return err
		}
		c.logger.Debug("readWorker got chunk", "chunk", chunk.String())
		c.chunkSize.Store(chunk.ChunkSize)

		// Start timing from the beginning of the chunk processing (read + write)
		chunkStartTime := time.Now()
		rows, err := c.readChunkData(ctx, chunk)
		if err != nil {
			readErr := fmt.Errorf("failed to read chunk data: %w", err)
			c.setInvalid(readErr)
			return readErr
		}

		// Record the in-memory size of the rows we just read so the chunker can
		// size the next chunk against a byte budget (memory-based dynamic
		// chunking). Harmless when the chunker is in time mode — it ignores it.
		chunk.ActualBytes = rowsByteSize(rows)

		// Handle empty chunks immediately
		if len(rows) == 0 {
			totalTime := time.Since(chunkStartTime)
			c.logger.Debug("readWorker chunk is empty, sending immediate feedback", "chunk", chunk.String())
			c.chunker.Feedback(chunk, totalTime, 0)

			// Send metrics for empty chunk
			err := c.sendMetrics(ctx, totalTime, chunk.ChunkSize, 0)
			if err != nil {
				c.logger.Error("error sending metrics for empty chunk", "error", err)
			}
			continue
		}

		c.logger.Debug("readWorker sending rows to applier", "chunk", chunk.String(), "rowCount", len(rows))

		// Send rows to applier with callback
		// The callback will be invoked when all rows are safely flushed
		// Capture the loop variables to avoid data race when callback executes asynchronously
		capturedChunk := chunk
		capturedStartTime := chunkStartTime
		callback := func(affectedRows int64, err error) {
			if err != nil {
				// A context cancellation (Ctrl+C / graceful shutdown) tears down
				// in-flight chunklets deliberately — it is not a copy failure, so
				// log it quietly rather than alarming the user with an ERROR. We
				// still setInvalid to unwind Run; higher layers filter context
				// cancellation when deciding the command's exit status.
				if errors.Is(err, context.Canceled) || ctx.Err() != nil {
					c.logger.Debug("applier callback cancelled", "chunk", capturedChunk.String(), "error", err)
				} else {
					c.logger.Error("applier callback received error", "chunk", capturedChunk.String(), "error", err)
				}
				c.setInvalid(err)
				return
			}

			c.logger.Debug("applier callback invoked",
				"table", capturedChunk.Table.TableName, "chunk", capturedChunk.String(),
				"affected_rows", affectedRows, "duration", time.Since(capturedStartTime).String())

			// Calculate total time from read start to callback completion (read + write)
			totalTime := time.Since(capturedStartTime)

			// Send feedback to chunker with total processing time
			c.chunker.Feedback(capturedChunk, totalTime, uint64(affectedRows))

			// Send metrics with total processing time
			metricsErr := c.sendMetrics(ctx, totalTime, capturedChunk.ChunkSize, uint64(affectedRows))
			if metricsErr != nil {
				c.logger.Error("error sending metrics from copier", "error", metricsErr)
			}
		}

		// Apply the rows
		if err := c.applier.Apply(ctx, chunk, rows, callback); err != nil {
			applyErr := fmt.Errorf("failed to apply rows: %w", err)
			c.setInvalid(applyErr)
			return applyErr
		}
	}

	c.logger.Debug("readWorker exiting main loop")
	return nil
}

// SetReadWorkers reconciles the live read-worker count to n, spawning new
// readers or parking existing ones as needed. It is the read-side counterpart
// of SingleTargetApplier.SetWriteWorkers: idempotent, safe to call repeatedly,
// and n is clamped to a minimum of 1 so the copy always makes progress. Calls
// before Run has started the pool or after it has drained are no-ops.
//
// Parking is cooperative: closing a reader's quit channel makes it exit right
// after its next BlockWait returns, so a chunk already claimed is always read
// and submitted to the applier — no chunk is ever lost.
//
// Parking latency differs from the write pool: write workers observe quit
// inside their blocking select and park immediately, but a parked reader
// stays live (and counted by ActiveReadWorkers) until its in-flight
// BlockWait returns — up to the throttler's full block duration (~60s for
// the replica throttler). Scaling down and back up within that window
// briefly overlaps parked readers with their replacements: the overlap is
// bounded by the previous pool size, self-drains, and parked readers do no
// chunk work.
func (c *buffered) SetReadWorkers(n int) {
	if n < 1 {
		n = 1
	}
	c.readScaleMu.Lock()
	defer c.readScaleMu.Unlock()

	// No-op before Run has recorded the reader context or once the pool has
	// drained. In both states we must not spawn: before Run a nil readerCtx
	// would panic the reader, and post-drain spawns would race Run's
	// drain-wait, which has already observed zero.
	if c.readerCtx == nil || c.readScalingClosed {
		return
	}

	cur := len(c.readerQuits)
	switch {
	case n > cur:
		for range n - cur {
			c.spawnReadWorkerLocked()
		}
		c.logger.Info("scaled read workers up", "from", cur, "to", n)
	case n < cur:
		// Park the most-recently-added readers by closing their quit channels.
		for i := cur - 1; i >= n; i-- {
			close(c.readerQuits[i])
		}
		c.readerQuits = c.readerQuits[:n]
		c.logger.Info("scaled read workers down", "from", cur, "to", n)
	}
}

// ActiveReadWorkers returns the current number of live read workers. This
// includes readers parked by SetReadWorkers that are still waiting for their
// in-flight BlockWait to return (see SetReadWorkers), so it can briefly
// exceed the requested worker count after a scale-down.
func (c *buffered) ActiveReadWorkers() int {
	c.readScaleMu.Lock()
	defer c.readScaleMu.Unlock()
	return c.liveReaders
}

// spawnReadWorkerLocked starts one read worker. Callers must hold readScaleMu.
func (c *buffered) spawnReadWorkerLocked() {
	quit := make(chan struct{})
	c.readerQuits = append(c.readerQuits, quit)
	c.liveReaders++
	ctx := c.readerCtx
	cancel := c.readerCancel
	go func() {
		defer c.readerExited(quit)
		if err := c.readWorker(ctx, quit); err != nil {
			// The error itself was already recorded by setInvalid inside
			// readWorker; cancelling the shared reader context aborts sibling
			// readers' in-flight reads promptly (errgroup parity).
			cancel()
		}
	}()
}

// readerExited is the bookkeeping counterpart of spawnReadWorkerLocked, run as
// every reader's exit path. It removes the reader's quit channel from the pool
// (present only when the reader exited naturally — a parked reader's entry was
// already trimmed by SetReadWorkers), decrements the live count, and wakes
// Run's drain-wait.
func (c *buffered) readerExited(quit chan struct{}) {
	c.readScaleMu.Lock()
	defer c.readScaleMu.Unlock()
	for i, q := range c.readerQuits {
		if q == quit {
			c.readerQuits = append(c.readerQuits[:i], c.readerQuits[i+1:]...)
			break
		}
	}
	c.liveReaders--
	c.readersDone.Broadcast()
}

// setInvalid marks the copy as failed and records the first error that caused
// it. It is called from the read workers and from async applier callbacks, so
// it captures only the first error and is safe for concurrent use.
func (c *buffered) setInvalid(err error) {
	if err != nil {
		c.errMu.Lock()
		if c.firstErr == nil {
			c.firstErr = err
		}
		c.errMu.Unlock()
	}
	c.isInvalid.Store(true)
}

// getFirstErr returns the first error that invalidated the copy, or nil.
func (c *buffered) getFirstErr() error {
	c.errMu.Lock()
	defer c.errMu.Unlock()
	return c.firstErr
}

func (c *buffered) SetThrottler(throttler throttler.Throttler) {
	c.Lock()
	defer c.Unlock()
	c.throttler = throttler
}

func (c *buffered) getCopyStats() (uint64, uint64, float64) {
	// Get progress from the chunker instead of calculating it ourselves
	rowsProcessed, _, totalRows := c.chunker.Progress()

	// Calculate percentage
	pct := float64(0)
	if totalRows > 0 {
		pct = float64(rowsProcessed) / float64(totalRows) * 100
	}

	return rowsProcessed, totalRows, pct
}

// GetProgress returns the progress of the copier
func (c *buffered) GetProgress() string {
	return c.CopyProgress().String()
}

// CopyProgress satisfies Copier.
func (c *buffered) CopyProgress() status.CopyProgress {
	c.Lock()
	defer c.Unlock()
	copied, total, _ := c.getCopyStats()
	return status.CopyProgress{RowsCopied: copied, RowsTotal: total}
}

// ChunkSize satisfies Copier.
func (c *buffered) ChunkSize() uint64 {
	return c.chunkSize.Load()
}

func (c *buffered) GetETA() string {
	c.Lock()
	defer c.Unlock()
	copiedRows, totalRows, pct := c.getCopyStats()
	estimate, st := etaEstimate(copiedRows, totalRows, pct, c.rowsPerSecond.Load(), c.startTime)
	switch st {
	case status.ETADue:
		return "DUE"
	case status.ETAMeasuring:
		return "TBD"
	case status.ETAReady, status.ETANone:
		// A ready estimate is formatted below; ETANone cannot occur during copy.
	}
	return estimate.String()
}

func (c *buffered) GetETAState() status.ETA {
	c.Lock()
	defer c.Unlock()
	copiedRows, totalRows, pct := c.getCopyStats()
	estimate, st := etaEstimate(copiedRows, totalRows, pct, c.rowsPerSecond.Load(), c.startTime)
	return status.ETA{State: st, Duration: estimate}
}

func (c *buffered) estimateRowsPerSecondLoop(ctx context.Context) {
	// We take >10 second averages because with parallel copy it bounces around a lot.
	// Get progress from chunker since we no longer track rows locally
	prevRowsCount, _, _ := c.chunker.Progress()
	ticker := time.NewTicker(copyEstimateInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !c.isHealthy(ctx) {
				return
			}
			newRowsCount, _, _ := c.chunker.Progress()
			rowsPerInterval := float64(newRowsCount - prevRowsCount)
			intervalsDivisor := float64(copyEstimateInterval / time.Second) // should be something like 10 for 10 seconds
			rowsPerSecond := uint64(rowsPerInterval / intervalsDivisor)
			c.rowsPerSecond.Store(rowsPerSecond)
			prevRowsCount = newRowsCount
		}
	}
}

func (c *buffered) sendMetrics(ctx context.Context, processingTime time.Duration, logicalRowsCount uint64, affectedRowsCount uint64) error {
	m := &metrics.Metrics{
		Values: []metrics.MetricValue{
			{
				Name:  metrics.ChunkProcessingTimeMetricName,
				Type:  metrics.GAUGE,
				Value: float64(processingTime.Milliseconds()), // in milliseconds
			},
			{
				Name:  metrics.ChunkLogicalRowsCountMetricName,
				Type:  metrics.COUNTER,
				Value: float64(logicalRowsCount),
			},
			{
				Name:  metrics.ChunkAffectedRowsCountMetricName,
				Type:  metrics.COUNTER,
				Value: float64(affectedRowsCount),
			},
		},
	}

	contextWithTimeout, cancel := context.WithTimeout(ctx, metrics.SinkTimeout)
	defer cancel()

	return c.metricsSink.Send(contextWithTimeout, m)
}

// GetChunker returns the chunker for accessing progress information
func (c *buffered) GetChunker() table.Chunker {
	return c.chunker
}

func (c *buffered) GetThrottler() throttler.Throttler {
	c.Lock()
	defer c.Unlock()
	return c.throttler
}
