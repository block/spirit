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
	resume           snapshotResume
	// repairer is the write path a mismatched chunk is rewritten through (see
	// replaceChunk). It is shared with the lockless checker's Recopier so both
	// algorithms repair identically.
	repairer        *chunkRepairer
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
	_ Checker = (*SingleChecker)(nil)
	_ Paced   = (*SingleChecker)(nil)
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
	// path below serializes on the shared repairer's lock and can park for
	// many minutes behind other repairs, and a checked-out transaction is
	// invisible to the pool's keepalive — holding it there idled connections past the server's
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
		c.resume.observed.Add(1)
		c.differencesFound.Add(1)
		c.logger.Warn("chunk verification failed", "chunk", chunk.String(), "reason", mismatch.reason(sourceCount, targetCount), "sourceChecksum", sourceChecksum, "targetChecksum", targetChecksum, "sourceCount", sourceCount, "targetCount", targetCount)
		if err := c.inspectDifferences(ctx, trx, chunk); err != nil {
			return err
		}
		// The snapshot reads are done. The repair below reads current data
		// through the pooled connections (deliberately outside the snapshot),
		// so return the transaction now and let the keepalive cover it while
		// this worker queues for the repairer's lock.
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

// inspectDifferences logs a line per diverged row in the chunk, reading inside
// the snapshot transaction the mismatch was detected under. The implementation
// is shared with the lockless checker — see inspectDifferences in inspect.go.
func (c *SingleChecker) inspectDifferences(ctx context.Context, trx *sql.Tx, chunk *table.Chunk) error {
	c.logger.Info("inspecting differences for chunk", "chunk", chunk.String())
	return inspectDifferences(ctx, trx, chunk, c.logger)
}

// replaceChunk recopies a mismatched chunk from the source table onto the
// target. The implementation is shared with the lockless checker's repair path
// — see chunkRepairer, which documents the operation, its locking, and the two
// behaviours it inherits from the applier.
func (c *SingleChecker) replaceChunk(ctx context.Context, chunk *table.Chunk) error {
	return c.repairer.Recopy(ctx, chunk)
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
// the lockless-checksum loop to decide whether a cancellation swallow
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
		c.Lock()
		c.execTime = time.Since(startTime)
		c.Unlock()
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
			if err := c.resume.restart(&c.differencesFound, func() error {
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
				return nil
			}); err != nil {
				return err
			}
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

	// A cancellation that lands inside the final attempt leaves the loop here
	// rather than at the pre-attempt check above, which is the only reason
	// that check is not sufficient on its own. Report it the way that check
	// does, so a caller can tell a clean shutdown from a verification failure.
	//
	// Only when the attempt failed *because* of the cancellation, though:
	// checksumCanceled is the same predicate the caller filters with, so
	// anything it would not accept from this function is not collapsed into a
	// cancellation here either. A real error, or a cancellation joined to one,
	// keeps the exhausted-retries error below, and differences found on every
	// attempt (lastErr == nil) keep theirs. Those outcomes say something about
	// the data that a cancellation does not.
	if ctx.Err() != nil && checksumCanceled(lastErr) {
		return ctx.Err()
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

// ResumeWatermark returns evidence from one attempt, synchronized with retries.
func (c *SingleChecker) ResumeWatermark() (string, error) {
	return c.resume.capture(c.chunker, &c.differencesFound)
}

var _ Checker = (*SingleChecker)(nil)

func (c *SingleChecker) ContinuousActive() bool { return c.resume.active.Load() }

func (c *SingleChecker) RunContinuous(ctx context.Context) error {
	c.resume.continuous.Store(true)
	return runContinuousSnapshot(ctx, c, []change.Source{c.feed}, &c.resume, func() error {
		return c.resume.restart(&c.differencesFound, c.chunker.Reset)
	})
}
