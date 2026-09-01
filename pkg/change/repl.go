// Package change contains binary log subscription functionality.
package change

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"sync/atomic"
	"time"
)

const (
	binlogTrivialThreshold = 10000
	// DefaultBatchSize is the maximum number of rows in each batched
	// REPLACE/DELETE statement that the binlog applier emits against the
	// _new table. Larger is better, but we need to keep the run-time of
	// the statement well below dbconn.maximumLockTime so that it doesn't
	// prevent copy-row tasks from failing. On Aurora tables with
	// out-of-cache workloads that copy ~300 rows per second this is close
	// to the safe ceiling.
	//
	// Batches are additionally capped by their estimated rendered byte
	// size (applier.MaxStatementSizeBytes, shared with the copy path's
	// chunklet splitting) so that wide rows can't accumulate into a
	// statement larger than max_allowed_packet. Whichever cap is reached
	// first cuts the batch; see flushMapLocked / flushQueueLocked in
	// subscription_buffered.go.
	//
	// Was previously an initial value for an adaptive sizer (feedback()
	// driven by p90 apply time). That mechanism was meaningful when the
	// applier issued `REPLACE INTO _new ... SELECT FROM source` and S-locked
	// rows on the live table, but after #853 the applier emits inline
	// VALUES against _new only — no source-side locks — and the batches
	// are strictly serial inside flushBatch. There's nothing left to
	// throttle, so the batch size is just a constant. See issue #869.
	DefaultBatchSize = 1000

	// DefaultFlushInterval is the time that the client will flush all binlog changes to disk.
	// Longer values require more memory, but permit more merging.
	// I expect we will change this to 1hr-24hr in the future.
	DefaultFlushInterval = 30 * time.Second
	// DefaultFlushConcurrency is the number of applier batches a
	// map-mode flush keeps in flight concurrently. The binlog apply
	// path is synchronous REPLACE/DELETE statements — it does not use
	// the copy path's write worker pool — so each stream tops out at
	// DefaultBatchSize rows per statement round trip. On large tables
	// where secondary index maintenance dominates, that is only a few
	// hundred rows/s, which a busy source's distinct-key write rate can
	// permanently outrun: the buffer pins at the soft limit and the
	// migration never converges, however long it runs. Map-mode flush
	// batches are disjoint by key and order-free (REPLACE/DELETE on
	// distinct keys commute), so applying them concurrently is safe and
	// multiplies the ceiling. Queue-mode drains (non-memory-comparable
	// PK, post-copy) and under-lock (cutover) flushes remain serial.
	DefaultFlushConcurrency = 8
	// DefaultSubscriptionSoftLimitBytes caps the approximate memory held
	// per subscription before HasChanged starts blocking on the buffered
	// map's condition variable. The cap is "soft": a single oversized
	// row admitted when the buffer is empty will exceed the limit, and
	// the next caller will park until that row drains. This keeps wide
	// rows (LONGTEXT / BLOB / large JSON) from OOMing the migrator
	// while still guaranteeing forward progress regardless of row width.
	// See pkg/change/subscription_buffered.go for the accounting model.
	//
	// Three behaviours keep the cap from starving the change reader:
	// map-mode overwrites of already-buffered keys bypass it (dedup
	// stays live under backpressure), parking requests an immediate
	// flush rather than waiting for the periodic interval, and flushes
	// release capacity per applied batch — the reader resumes as soon
	// as the first batch lands, not when the whole buffer has drained.
	//
	// Operators should be aware that pausing the binlog reader for an
	// extended period risks falling past the source's binlog retention
	// (binlog_expire_logs_seconds). Tune this value, or the source's
	// retention, accordingly.
	DefaultSubscriptionSoftLimitBytes = 256 << 20
	// DefaultSubscriptionSoftLimitChanges caps the number of pending
	// changes per subscription before HasChanged parks, alongside
	// DefaultSubscriptionSoftLimitBytes. Whichever binds first parks the
	// reader.
	//
	// The byte cap alone is not enough because bytes and count measure
	// different costs. Bytes bound memory, which is what a handful of wide
	// LONGTEXT rows threatens. Count bounds how long the drain that empties
	// the buffer takes: the flush applies rows in batches of at most
	// DefaultBatchSize per round trip, so drain time scales with count and
	// is indifferent to row width. A narrow-row table therefore reaches an
	// unworkable drain long before it reaches 256MiB — in production, a
	// table averaging ~600 bytes per change filled to over 450k pending
	// changes while still well inside the byte cap, and the drain that
	// followed ran for 21m37s holding flushMu for its full duration.
	//
	// 50k targets a drain of roughly two minutes rather than twenty. Scaling
	// that production drain — at most 452,571 rows in 21m37s — down to 50k
	// gives about 2m23s, and that is a floor rather than an estimate: the
	// 452,571 figure is the backlog at flush *start*, so if fewer rows actually
	// landed the per-row cost is higher and the scaled time is longer.
	//
	// Two minutes is not "one flush interval", and it is not meant to be. What
	// matters is that the drain *completes*, because only a complete drain
	// reports allChangesFlushed=true and only that advances the flushed
	// position; a cap tight enough to fit one DefaultFlushInterval would
	// truncate every drain and freeze the position just as before. Overlapping
	// flushes are not a concern either — flushMu serializes them, so a tick
	// arriving mid-drain waits rather than piling on.
	//
	// It is well above binlogTrivialThreshold, so it does not interfere with
	// the "flush until trivial" loops, and it still lets dedup absorb hot-row
	// workloads — map-mode overwrites of already-buffered keys bypass the cap
	// entirely.
	DefaultSubscriptionSoftLimitChanges = 50000
	// DefaultTimeout is how long BlockWait is supposed to wait before returning errors.
	DefaultTimeout = 30 * time.Second
	// Maximum number of consecutive errors before recreating the streamer
	maxConsecutiveErrors = 5
	// Initial backoff duration for streamer recreation
	initialBackoffDuration = time.Second
	// Maximum backoff duration
	maxBackoffDuration = time.Minute
	// Backoff multiplier
	backoffMultiplier = 2
	// Sleep time between position checks in BlockWait
	blockWaitSleep = 100 * time.Millisecond
	// Number of consecutive blockWaitSleep intervals where the buffered position
	// hasn't advanced before BlockWait flushes binary logs to nudge the syncer.
	// 3 * blockWaitSleep (~300ms) tolerates brief syncer lag (e.g. CI load) while
	// remaining negligible relative to DefaultTimeout.
	blockWaitStallThreshold = 3
)

// periodicFlushStopping reports whether a flush that just failed inside
// runPeriodicFlush failed because the loop is shutting down, in which case the
// goroutine should return quietly rather than log.
//
// The loop only selects on ctx.Done() at the top, so a cancellation that
// arrives while a flush is in flight surfaces as a context error from the
// flush itself. That is the normal shutdown path — every migration passes
// through it at postCopyPhase, which calls StopPeriodicFlush before draining
// the backlog synchronously — and it was being logged at Error. In production
// that printed five "error flushing ..." lines at the exact moment the copy
// completed, which reads as a failure in the phase transition rather than as
// the phase transition working.
//
// It deliberately does not consult the error's identity, only the context's.
// StopPeriodicFlush cancels exactly the context this loop passes to the flush,
// so by the time a shutdown-origin error is observed ctx.Err() is already set —
// cancellation records the error before it wakes anything — and the same holds
// for the parent migration context dying. Testing errors.Is(err,
// context.Canceled) as well would add nothing for that case and would open one
// the loop cannot survive: a context.Canceled originating *below* this loop's
// context would end runPeriodicFlush with a bare return and no log, and the
// loop cannot be restarted, because it does not clear periodicFlushCancel on
// the way out and StartPeriodicFlush is a no-op while that is non-nil. The
// migration would then run on with no periodic flush at all — changeset
// growing, position frozen, nothing surfaced — which is this function's own
// failure class with the evidence removed.
func periodicFlushStopping(ctx context.Context) bool {
	return ctx.Err() != nil
}

// recordEventTime advances dst to the wall-clock timestamp carried in a binlog
// event header, which is when the source committed that transaction. Both
// clients call it once per event read; see FeedStats.BufferedEventAt for what
// the number is for.
//
// Two guards, both load-bearing:
//
//   - A zero timestamp is ignored. Artificial events the syncer manufactures
//     locally (the rotate it fabricates when re-opening a file) carry no source
//     time, and treating 0 as a timestamp would report the reader as 56 years
//     behind.
//   - It only ever moves forward. Timestamps are non-decreasing in commit
//     order, so this is a no-op on a healthy stream — but on reconnect the
//     server re-sends the FormatDescriptionEvent from the head of the file,
//     stamped when that file was created, which can be hours older than the
//     position we resumed at. Taking the max ignores it instead of reporting a
//     jump backwards in a field an operator is reading as progress.
func recordEventTime(dst *atomic.Int64, headerTimestamp uint32) {
	if headerTimestamp == 0 {
		return
	}
	secs := int64(headerTimestamp)
	for {
		prev := dst.Load()
		if secs <= prev {
			return
		}
		if dst.CompareAndSwap(prev, secs) {
			return
		}
	}
}

// eventTime reads back a timestamp stored by recordEventTime, or the zero time
// if no event has been seen yet.
func eventTime(src *atomic.Int64) time.Time {
	secs := src.Load()
	if secs == 0 {
		return time.Time{}
	}
	return time.Unix(secs, 0)
}

// backlogWorthDraining reports whether a Flush loop should drain again
// immediately rather than calling BlockWait. Both clients' Flush loops consult
// it between the drain and the wait.
//
// BlockWait waits for the buffered position to reach the source's current one.
// While a real backlog is still queued that wait is not merely unproductive,
// it is self-defeating: a subscription sitting at its soft memory limit parks
// the reader, a parked reader cannot advance the buffered position, and the
// flush this loop would skip is the only thing that can unpark it. The wait
// therefore blocks on the very condition it is waiting for, burns
// DefaultTimeout, and returns to the top of the loop having achieved nothing.
//
// Observed in production on a feed ~482M GTIDs behind: 30s in BlockWait per 4s
// drain, so the flush ran ~12% of the time while the reader stayed parked and
// the binlog retention window burned down eight times faster than it needed
// to. The catch-up loop was losing to a wait it was itself preventing.
//
// readerWasBlocked is the primary signal, because it is the exact condition
// rather than a proxy for it: if the reader parked, it stopped ingesting, and
// a position that cannot advance is one BlockWait cannot wait for. Note that
// the pending count could not stand in for it — the soft limit is applied on
// bytes as well as change count (SubscriptionSoftLimitBytes), so a wide-row
// table parks the reader at a pending count far below any threshold worth
// setting, which is precisely when the stall would go unnoticed.
//
// pending is kept as a second trigger for the case where park cannot fire at
// all: a caller that disabled the soft limits has no park signal, and a large
// buffered backlog is then the only evidence that draining beats waiting. The
// two are ORed rather than ANDed deliberately — a wrong "drain again" costs
// one more drain, a wrong "wait" costs DefaultTimeout, so this should err
// toward draining.
//
// redrainCanProgress is what keeps either trigger from becoming a hot loop, and
// it is a narrower question than "did the last flush drain everything". Three
// things make a drain report allChangesFlushed=false and they do not want the
// same answer:
//
//   - Keys deferred behind the copier's watermark, and batches that lost to
//     lock contention twice. Repeating the flush at once re-defers exactly the
//     same work and leaves the reader parked while it does, so these fall
//     through to BlockWait and let its poll pace the retry. That is the
//     pre-existing behaviour for those cases.
//   - A drain that spent its dispatch budget (drainDispatchBudget). Its
//     remaining batches were never *attempted*, and a fresh drain gets a fresh
//     budget that would land them — so this one must re-drain. It is also, by
//     construction, a feed that is not keeping up: the budget is a five-minute
//     backstop, and the production drain that motivated it ran 21m37s over
//     ~452k changes. Treating it as "nothing more to do" would switch this fix
//     off in part of the regime it exists for.
//
// The callers compute it as "last flush complete, or cut short by its budget";
// see drainHitBudget.
func backlogWorthDraining(pending int, readerWasBlocked, redrainCanProgress bool) bool {
	if !redrainCanProgress {
		return false
	}
	return readerWasBlocked || pending >= binlogTrivialThreshold
}

var (
	// maxRecreateAttempts is the maximum number of streamer recreation attempts before giving up.
	// This is really a const, but set to var for testing.
	maxRecreateAttempts = 10

	// ErrChangesNotFlushed indicates that not all changes have been flushed from the replication feed.
	ErrChangesNotFlushed = errors.New("not all changes flushed")
)

// serverIDCounter is an atomic counter used to help ensure unique server IDs
var serverIDCounter atomic.Uint32

// NewServerID generates a unique server ID to avoid conflicts with other binlog readers.
// Uses crypto/rand combined with an atomic counter to ensure uniqueness even when called
// concurrently. Returns a value in the range 1001-4294967295 to avoid conflicts with
// typical MySQL server IDs (0-1000).
func NewServerID() uint32 {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Fallback to nanosecond-based generation if crypto/rand fails (should never happen)
		rangeSize := int64(^uint32(0) - 1000)
		return uint32(time.Now().UnixNano()%rangeSize) + 1001
	}
	// Convert bytes to uint32, mix with counter, and map to valid range
	randomPart := binary.BigEndian.Uint32(b[:])
	counterPart := serverIDCounter.Add(1)

	// XOR the random and counter parts for better distribution
	result := randomPart ^ counterPart

	// Map result into the range [1001, max uint32]
	// Use modulo to constrain to the valid range, then add 1001
	result = (result % (^uint32(0) - 1000)) + 1001
	return result
}
