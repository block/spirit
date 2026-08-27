// Package change contains binary log subscription functionality.
package change

import (
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
