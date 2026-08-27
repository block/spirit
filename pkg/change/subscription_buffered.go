package change

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
	"golang.org/x/sync/errgroup"
)

// The bufferedMap avoids using REPLACE INTO .. SELECT.
// See: https://github.com/block/spirit/issues/451
// This has the advantage that we can use spirit for MoveTable operations
// across different MySQL servers. In combination with Atomic DDL,
// we have all the components needed for cloning sets of tables between servers.
//
// We switched to it being the *only* subscription type because of #746:
// MySQL does not always have read-after-commit safety, a violation of
// binlog_order_commits = ON. Storing the row image inline (rather than
// re-reading source via REPLACE INTO ... SELECT) sidesteps that race.
//
// Behaviour switches based on (watermarkOptimizationEnabled,
// pkIsMemoryComparable):
//
//   - pkIsMemoryComparable=true: always map mode. Map-key equality matches
//     MySQL row identity, so LWW dedup is correct.
//   - pkIsMemoryComparable=false: map mode during the copy phase
//     (watermark on), queue mode post-copy. The chunker's later SELECT
//     covers in-window case-collision races during the copy phase; the
//     post-cutover checksum repairs any divergence that slips through.
//
// Why queue mode at all? With case-insensitive collations "A" and "a"
// hash to distinct keys but resolve to the same row in MySQL, so a
// map's non-deterministic iteration would apply the events in the
// wrong order. FIFO ordering applies them in binlog order, which the
// target's own collation-aware uniqueness then collapses correctly.
// The queue keeps the row image inline and applies it via the applier
// — no REPLACE INTO ... SELECT round-trip, so #746 stays fixed and
// cross-server moves stay supported per #607.
//
// Applier idempotence: the applier issues `REPLACE INTO target VALUES ...`
// rather than `INSERT ... ON DUPLICATE KEY UPDATE`. REPLACE deletes any
// row that conflicts on PRIMARY KEY or any UNIQUE index before each
// insert, which makes the applier idempotent for any subset/order of
// events that land together in a multi-row batch. That's what restores
// the pre-#821 robustness for "swap" workloads (a source-side
// transaction that legally moves a unique value between two rows)
// without re-introducing the binlog/visibility race motivating #746 —
// we supply the inline row image, not a SELECT against source.
//
// Eventual consistency: REPLACE may delete more rows than appear in
// its VALUES list — specifically, any row in the destination that
// holds a unique value the new row is now claiming. That row is
// briefly missing from the destination until its own event arrives in
// a later batch (or a later row in the same batch) and re-inserts it.
// Spirit's correctness relies on each flushed snapshot being a
// *disjoint* representation of pending changes — within one flush,
// every PK appears at most once, holding the latest image buffered at
// swap time — so every transiently-deleted row is guaranteed to have
// its own event in the buffer (or arriving shortly). A newer image for
// a PK may arrive while its older image is mid-drain; it lands in the
// active store and is applied by a later flush, preserving per-key
// order. The destination converges to source's current state once the
// last unflushed event for each affected PK has been applied; the
// post-cutover checksum (with FixDifferences=true) catches any
// divergence that slips through.
//
// SetWatermarkOptimization owns the watermark-driven transition: when
// its toggle changes which store is active, it drains the outgoing
// store inline. Past that boundary the invariant holds — only the
// currently-active store may have entries — so HasChanged never has to
// merge into a stale map and Flush never has to drain both stores in
// the normal path.

type bufferedChange struct {
	logicalRow  applier.LogicalRow
	originalKey []any // preserve original typed key for watermark comparison
}

// queuedChange preserves the FIFO position of a single row event while
// still carrying the inline row image — applying via the applier keeps
// the #746 (read-after-commit) fix intact and lets cross-server moves
// continue to work for non-memory-comparable PKs (#607).
type queuedChange struct {
	key        string
	logicalRow applier.LogicalRow
	// originalKey is the typed PK tuple. Deletes need it to build the
	// DELETE statement via the applier's type-aware path; upserts use
	// logicalRow.RowImage instead, but we store it uniformly.
	originalKey []any
}

type bufferedMap struct {
	sync.Mutex // protects the subscription from changes.

	// cond signals waiters in HasChanged when sizeBytes drops below
	// softLimitBytes. Broadcast after every applied batch and at the
	// end of every flush path. L = &Mutex. Construction invariant:
	// every call site must wire this up immediately after the struct
	// literal, e.g.
	//   sub := &bufferedMap{...}
	//   sub.cond = sync.NewCond(&sub.Mutex)
	// HasChanged / Flush / SetWatermarkOptimization will panic on a
	// nil cond, so a missing init shows up loudly in tests.
	cond *sync.Cond

	// flushMu serializes whole-flush operations — Flush and the inline
	// drains in SetWatermarkOptimization — against each other. It is
	// held for the full duration of a drain, while Mutex is only held
	// for short buffer bookkeeping. That split is what lets HasChanged
	// keep buffering (and deduping) while a flush's applier round
	// trips are in flight. Lock order: flushMu before Mutex, never the
	// reverse.
	flushMu sync.Mutex

	// flushConcurrency is the maximum number of applier batches a
	// map-mode drain keeps in flight concurrently; <= 1 means serial.
	// Only drainMapSnapshot uses it: map batches are disjoint by key
	// and order-free. Queue-mode drains are FIFO and the under-lock
	// (cutover) path must stay atomic, so both remain serial.
	flushConcurrency int

	// logger is supplied by the change.Source that owns this subscription.
	// We keep only a *slog.Logger (not a back-pointer to the source) so the
	// bufferedMap stays source-agnostic and can be reused by alternative
	// implementations (e.g. a VStream change.Source).
	logger  *slog.Logger
	applier applier.Applier // applier for writing changes to the target

	table    *table.TableInfo
	newTable *table.TableInfo

	// changes accumulates events while in map mode. SetWatermarkOptimization
	// drains it on a transition out of map mode, so under normal operation
	// it is empty in queue mode.
	changes map[string]bufferedChange

	// queue accumulates events while in queue mode. SetWatermarkOptimization
	// drains it on a transition out of queue mode, so under normal operation
	// it is empty in map mode. See the file-level comment for when each
	// mode is selected.
	queue []queuedChange

	// flushingCount is the number of entries an in-flight Flush has
	// swapped out of changes/queue but not yet applied. Their bytes
	// remain in sizeBytes until each batch completes. Length() includes
	// it so AllChangesFlushed cannot report true while a drain is mid-
	// air. Zero whenever no Flush is in flight.
	flushingCount int

	// sizeBytes is an approximate count of memory currently held by
	// changes + queue, plus the still-unapplied portion of any snapshot
	// a Flush is currently draining. Maintained by HasChanged and the
	// flush paths; see estimateRowSize for the accounting.
	sizeBytes int64

	// softLimitBytes is the soft cap before HasChanged blocks waiting
	// on cond. Zero disables the cap. The limit is checked against the
	// pre-add sizeBytes, so a row is admitted whenever the buffer is
	// currently under the cap regardless of how much that row alone
	// will overshoot it. The cap only blocks *new* arrivals once
	// sizeBytes is already at or above the limit. This preserves
	// forward progress regardless of row width, but means peak memory
	// can briefly exceed the limit by up to one oversized row.
	softLimitBytes int64

	watermarkOptimization bool
	chunker               table.MappedChunker

	// closed is set by Close() to release any HasChanged caller parked on
	// the soft memory limit. Without it, Client.Close() deadlocks on
	// streamWG.Wait(): readStream → processRowsEvent → HasChanged would
	// remain blocked on the cond with no flush in flight to wake it.
	closed bool

	// flushRequest, when non-nil, receives this subscription (a
	// non-blocking send) each time HasChanged parks on the soft limit.
	// The owning client selects on it in its periodic-flush loop and
	// flushes the requesting subscription first, so a full buffer is
	// drained immediately rather than waiting out the remainder of the
	// flush interval — or, on multi-table clients, waiting behind
	// another saturated subscription's entire drain in the
	// nondeterministic all-subscription pass. A parked subscription
	// stalls the binlog reader, and every second parked burns
	// binlog-retention headroom.
	flushRequest chan<- Subscription

	// concurrencyPenalty is the number of halvings currently applied to
	// flushConcurrency by the drain's AIMD controller: the effective limit is
	// flushConcurrency >> concurrencyPenalty, floored at 1. A drain that hits
	// lock contention increments it; cleanDrains consecutive clean drains
	// decrement it. See adaptFlushConcurrency.
	//
	// Kept as an atomic rather than under Mutex because
	// effectiveFlushConcurrency is read at the top of a drain, outside the
	// short bookkeeping critical sections, and flushMu already serializes
	// drains against each other.
	concurrencyPenalty atomic.Int64
	cleanDrains        atomic.Int64

	// Counters for the bookend log emitted on watermark-optimization transitions.
	keysAdded        atomic.Int64
	keysDroppedAbove atomic.Int64
	keysSkippedBelow atomic.Int64
	timesParked      atomic.Int64 // HasChanged was parked at least once on the soft limit
	batchesContended atomic.Int64 // applier batches that failed on 1205/1213
	serialRecoveries atomic.Int64 // drains rescued by the serial retry pass

	// lastParkWarn is when the park/unpark log pair was last emitted at
	// Warn/Info level. Since flushes release capacity per batch, a
	// saturated applier produces frequent short parks rather than one
	// long one; without throttling, the pair would log every couple of
	// seconds for the lifetime of a long drain. Guarded by Mutex.
	lastParkWarn time.Time

	pkIsMemoryComparable bool
}

// Per-entry overheads applied on top of estimateRowSize so the soft
// limit tracks closer to real RSS for high-cardinality, narrow-row
// workloads (where the variable-width contents don't dominate). For
// wide-row workloads — the OOM scenario this cap was added to defend
// against — these constants are noise next to the BLOB / large-string
// payload sizes. Both are approximate; the cap is "soft" anyway.
const (
	// parkWarnInterval throttles the park/unpark log pair. Parks under
	// sustained backpressure are frequent and short (capacity returns
	// per applied batch), so the pair is emitted at Warn/Info level at
	// most once per interval per subscription and at Debug otherwise.
	// The timesParked counter still counts every park.
	parkWarnInterval = 30 * time.Second

	// bufferedChangeOverhead is the fixed per-entry cost for an item
	// in s.changes beyond what estimateRowSize captures: the hashed-
	// key string header (~16 B), the bufferedChange struct laid out
	// in the map's value slot (LogicalRow + originalKey slice header,
	// ~56 B), and Go's map bucket overhead (~48 B amortized).
	bufferedChangeOverhead = 120

	// queuedChangeOverhead is the fixed per-element cost for an item
	// in s.queue beyond estimateRowSize's contribution: the key
	// string header (~16 B), the LogicalRow struct (~32 B), and the
	// originalKey slice header (~24 B). Slice amortized-growth overhead
	// is not explicitly accounted for.
	queuedChangeOverhead = 72
)

// BufferedSubscriptionConfig configures NewBufferedSubscription.
type BufferedSubscriptionConfig struct {
	// CurrentTable is the source-side TableInfo. Required.
	CurrentTable *table.TableInfo

	// NewTable is the destination-side TableInfo. May be nil for
	// MoveTables/import flows where source and destination share the
	// same schema; in that case Subscription.Tables() returns just
	// [CurrentTable].
	NewTable *table.TableInfo

	// Applier writes batched changes to the target. Required.
	Applier applier.Applier

	// Chunker provides the watermark filter + column mapping. Required.
	Chunker table.MappedChunker

	// Logger receives diagnostic events. Defaults to slog.Default()
	// when nil.
	Logger *slog.Logger

	// SoftLimitBytes is the per-subscription byte cap before
	// HasChanged blocks waiting on the flush path. Zero disables the
	// cap. See bufferedMap.softLimitBytes for the semantics.
	SoftLimitBytes int64

	// FlushRequest, when non-nil, receives the parked subscription (a
	// non-blocking send) each time HasChanged parks on the soft limit.
	// Owners that flush on a periodic ticker should select on it and
	// flush the received subscription first, then run their normal
	// all-subscription pass — flushing others first would leave the
	// change reader parked for those entire drains. Optional; nil
	// disables the signal.
	FlushRequest chan<- Subscription

	// FlushConcurrency is the maximum number of applier batches a
	// map-mode flush keeps in flight concurrently. Zero or negative
	// means serial, preserving prior behaviour for callers that do not
	// set it; the in-tree clients pass DefaultFlushConcurrency. Queue-
	// mode and under-lock flushes are always serial regardless.
	FlushConcurrency int
}

// NewBufferedSubscription constructs the default bufferedMap-backed
// Subscription. It is the public counterpart to binlogClient's internal
// AddSubscription helper: out-of-tree change.Source implementations
// (e.g. strata's pkg/vstream) call this from their own AddSubscription to
// build a Subscription the runner / copier can drive.
//
// The returned Subscription is not yet wired into a registry — the caller
// is responsible for storing it and routing row events to its HasChanged
// method. The internal sync.Cond is initialised before return (matching
// subscriptionRegistry.AddBuffered) so HasChanged / Flush /
// SetWatermarkOptimization are safe to call immediately.
func NewBufferedSubscription(cfg BufferedSubscriptionConfig) (Subscription, error) {
	if cfg.CurrentTable == nil {
		return nil, fmt.Errorf("NewBufferedSubscription: CurrentTable is required")
	}
	if cfg.Applier == nil {
		return nil, fmt.Errorf("NewBufferedSubscription: Applier is required")
	}
	if cfg.Chunker == nil {
		return nil, fmt.Errorf("NewBufferedSubscription: Chunker is required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	// If the source table is sharded, its sharding (vindex) column must be
	// resolvable to an ordinal — the change source enforces the column's
	// immutability on every UPDATE event (see checkImmutableColumn via
	// ImmutableColumnOrdinal), so a misconfigured column must fail here at
	// setup time rather than per-event. Tables without a ShardingColumn
	// (migrations, single-target moves) skip this: no enforcement.
	if cfg.CurrentTable.ShardingColumn != "" &&
		!slices.Contains(cfg.CurrentTable.Columns, cfg.CurrentTable.ShardingColumn) {
		return nil, fmt.Errorf("NewBufferedSubscription: sharding column %s not found in columns of table %s.%s",
			cfg.CurrentTable.ShardingColumn, cfg.CurrentTable.SchemaName, cfg.CurrentTable.TableName)
	}
	sub := &bufferedMap{
		table:                cfg.CurrentTable,
		newTable:             cfg.NewTable,
		changes:              make(map[string]bufferedChange),
		logger:               logger,
		chunker:              cfg.Chunker,
		applier:              cfg.Applier,
		pkIsMemoryComparable: cfg.CurrentTable.PrimaryKeyIsMemoryComparable() == nil,
		softLimitBytes:       cfg.SoftLimitBytes,
		flushRequest:         cfg.FlushRequest,
		flushConcurrency:     cfg.FlushConcurrency,
	}
	sub.cond = sync.NewCond(&sub.Mutex)
	return sub, nil
}

// estimateRowSize returns a rough byte estimate for a []any column slice
// that bufferedMap holds in memory. The estimate is intentionally
// approximate — we only use it to bound the buffer, not to report exact
// memory usage. Costs accounted for:
//   - 24 bytes of slice header
//   - 16 bytes per element (interface header)
//   - len(b) for []byte / string values (the dominant cost for wide rows)
//   - 8 bytes for scalars, attributed to inline storage
func estimateRowSize(row []any) int64 {
	if len(row) == 0 {
		return 0
	}
	var n int64 = 24
	for _, v := range row {
		n += 16
		switch x := v.(type) {
		case []byte:
			n += int64(len(x))
		case string:
			n += int64(len(x))
		default:
			n += 8
		}
	}
	return n
}

func sizeOfBufferedChange(hashedKey string, c bufferedChange) int64 {
	return bufferedChangeOverhead + int64(len(hashedKey)) + estimateRowSize(c.logicalRow.RowImage) + estimateRowSize(c.originalKey)
}

func sizeOfQueuedChange(c queuedChange) int64 {
	return queuedChangeOverhead + int64(len(c.key)) + estimateRowSize(c.logicalRow.RowImage) + estimateRowSize(c.originalKey)
}

// estimateRenderedBytes returns a rough byte estimate of what a row image
// (or key tuple) will occupy once the applier renders it into a SQL
// statement. Binary values hex-encode at two characters per byte and quoted
// strings can double under escaping, so variable-width values are counted at
// twice their in-memory length; scalars render as short literals. Feeds the
// same applier.MaxStatementSizeBytes budget as the copy path's estimator
// (pkg/applier estimateValueSize) but with the opposite bias: that one counts
// variable-width values at 1x and leans on the budget's ~64x headroom below
// max_allowed_packet, while this one stays pessimistic — a flush batch cut
// short only costs an extra statement, and the binlog path has no throughput
// reason to run the estimate hot.
func estimateRenderedBytes(values []any) int64 {
	var n int64 = 2 // parentheses around the tuple
	for _, v := range values {
		n += 4 // separator plus quotes / 0x prefix
		switch x := v.(type) {
		case []byte:
			n += int64(len(x)) * 2
		case string:
			n += int64(len(x)) * 2
		default:
			n += 20 // numeric / temporal literals are short
		}
	}
	return n
}

// renderedBytesOfChange estimates the rendered-SQL contribution of one
// buffered change: deletes contribute their key tuple (the DELETE ... IN
// element list), upserts their full row image (the REPLACE ... VALUES list).
func renderedBytesOfChange(lr applier.LogicalRow, originalKey []any) int64 {
	if lr.IsDeleted {
		return estimateRenderedBytes(originalKey)
	}
	return estimateRenderedBytes(lr.RowImage)
}

// Assert that bufferedMap implements subscription
var _ Subscription = (*bufferedMap)(nil)

func (s *bufferedMap) Length() int {
	s.Lock()
	defer s.Unlock()

	// flushingCount covers entries an in-flight Flush has swapped out
	// but not yet applied — they are still pending changes, and callers
	// like AllChangesFlushed must not see the buffer as empty while a
	// drain is mid-air.
	return len(s.changes) + len(s.queue) + s.flushingCount
}

func (s *bufferedMap) Tables() []*table.TableInfo {
	if s.newTable == nil {
		// Move-flow subscriptions have no destination-side TableInfo (see
		// BufferedSubscriptionConfig.NewTable). Omit the nil rather than
		// returning [table, nil]: Tables() consumers — the DDL
		// subscription-match loops in the binlog/GTID clients, and
		// out-of-tree change.Source implementations routing row events per
		// the Source interface contract — iterate and dereference the
		// entries, and a nil entry panics the stream-reader goroutine.
		return []*table.TableInfo{s.table}
	}
	return []*table.TableInfo{s.table, s.newTable}
}

// ImmutableColumnOrdinal satisfies Subscription. The ordinal is derived
// from the source table's ShardingColumn on each call rather than stored,
// so a zero-value bufferedMap (tests build these directly) cannot
// accidentally declare column 0 immutable. The same derivation is used by
// the sharded applier when routing rows (see ShardedApplier.UpsertRows);
// NewBufferedSubscription validates at setup time that a configured column
// resolves, so -1 here always means "not sharded". TableInfo.Columns and
// ShardingColumn are fixed after setup, so no lock is required.
func (s *bufferedMap) ImmutableColumnOrdinal() int {
	if s.table.ShardingColumn == "" {
		return -1
	}
	return slices.Index(s.table.Columns, s.table.ShardingColumn)
}

// cleanDrainsToRecover is how many consecutive contention-free drains must
// pass before the AIMD controller gives a halving back. Recovery is
// deliberately slower than the decrease: re-entering the pathological state
// costs a whole flush interval (and, while the checkpoint cannot advance,
// binlog-retention headroom), whereas running one step under-parallel costs
// only throughput. Three drains at the default 30s interval is ~90s of proven
// quiet before stepping back up.
const cleanDrainsToRecover = 3

// minAdaptiveBatchSize floors the batch shrink. Below roughly this many rows
// the per-statement round trip dominates and the drain stops keeping up with a
// busy source, which trades one stall for another.
const minAdaptiveBatchSize = 50

// effectiveFlushConcurrency clamps flushConcurrency to at least 1 so
// the zero value (out-of-tree callers, bare test maps) stays serial, then
// applies any halvings the AIMD controller has accumulated.
func (s *bufferedMap) effectiveFlushConcurrency() int {
	return shiftDown(max(1, s.flushConcurrency), s.concurrencyPenalty.Load(), 1)
}

// effectiveBatchSize shrinks alongside concurrency, because batch size sets the
// *lock footprint* of a single REPLACE while concurrency only sets how many of
// those run at once. Both terms drive the collision probability between sibling
// batches, so narrowing one and leaving the other is a half measure.
//
// A production deadlock dump on a 1000-row batch showed `6803 lock struct(s)
// ... 6805 row lock(s), undo log entries 1942` held by a single statement that
// had been ACTIVE 13 sec — roughly one clustered-index lock plus one
// secondary-index lock per row per index. Every one of those locks is a record
// a sibling batch can block on, for as long as the holder runs.
func (s *bufferedMap) effectiveBatchSize() int {
	return shiftDown(DefaultBatchSize, s.concurrencyPenalty.Load(), minAdaptiveBatchSize)
}

// shiftDown halves start once per penalty step, never going below floor.
func shiftDown(start int, penalty int64, floor int) int {
	if penalty <= 0 {
		return start
	}
	if penalty >= 63 { // guard the shift width; the floor applies regardless
		return floor
	}
	return max(floor, start>>penalty)
}

// adaptFlushConcurrency feeds a completed drain's contention outcome back into
// the concurrency limit, AIMD-style: multiplicative decrease on contention,
// additive increase after a run of clean drains.
//
// The signal we control on is spirit's *own* lock contention, not server load.
// That is the whole point: an oversubscribed flush fan-out deadlocks against
// itself on secondary-index next-key locks while the server still looks idle,
// so every load-based signal (Threads_running, commit latency, the autoscaler's
// throttler) reports "healthy" throughout. Nothing else in the process can see
// this, so the drain has to police its own width.
//
// Called from drainMapSnapshot with flushMu held, so the read-modify-write on
// concurrencyPenalty needs no further synchronization.
func (s *bufferedMap) adaptFlushConcurrency(contended bool) {
	configured := max(1, s.flushConcurrency)
	if contended {
		s.cleanDrains.Store(0)
		// Stop halving once the limit is already 1: there is no narrower
		// setting, and letting the penalty run away would make recovery take
		// proportionally longer once the contention clears.
		if s.effectiveFlushConcurrency() > 1 || s.effectiveBatchSize() > minAdaptiveBatchSize {
			penalty := s.concurrencyPenalty.Add(1)
			s.logger.Warn("reducing flush concurrency after lock contention",
				"table", s.table.SchemaName+"."+s.table.TableName,
				"concurrency", s.effectiveFlushConcurrency(),
				"batch_size", s.effectiveBatchSize(),
				"configured", configured,
				"penalty", penalty,
			)
		}
		return
	}
	if s.concurrencyPenalty.Load() <= 0 {
		return // already at the configured width; nothing to recover
	}
	if s.cleanDrains.Add(1) < cleanDrainsToRecover {
		return
	}
	s.cleanDrains.Store(0)
	s.concurrencyPenalty.Add(-1)
	s.logger.Info("restoring flush concurrency after clean drains",
		"table", s.table.SchemaName+"."+s.table.TableName,
		"concurrency", s.effectiveFlushConcurrency(),
		"batch_size", s.effectiveBatchSize(),
		"configured", configured,
	)
}

// contentionBackoff is the delay before the serial retry pass makes its
// *second* and later attempts on a batch. The first attempt does not wait at
// all: errgroup.Wait has already returned by then, so no sibling batch from
// this drain is still in flight, and the lock that beat us is gone.
//
// The escalating delays only cover the cases where something outside this drain
// holds the lock — another table's subscription flushing, or a checksum repair.
// Those are bounded by a statement, not by a copy phase, so this tops out at a
// couple of seconds rather than trying to outlast anything long-lived.
//
// Var (not const/func) so tests can shorten it, mirroring
// throttler.threadsRunningPollInterval.
var contentionBackoff = func(attempt int) time.Duration {
	if attempt <= 0 {
		return 0
	}
	base := min(100*time.Millisecond<<(attempt-1), 2*time.Second)
	// Full jitter. Batches that deadlocked against each other were, by
	// definition, running concurrently; retrying them in lockstep would just
	// re-stage the same collision.
	return base/2 + time.Duration(rand.Int64N(int64(base/2)+1))
}

// queueModeActive reports whether new events should be appended to the
// FIFO queue rather than the map. Caller must hold s.Lock.
//
// Memory-comparable PKs are never queue-mode (map-key equality matches
// MySQL row identity). Non-memory-comparable PKs run in map mode during
// the copy phase (watermark on) and switch to queue mode post-copy.
func (s *bufferedMap) queueModeActive() bool {
	if s.pkIsMemoryComparable {
		return false
	}
	return !s.watermarkOptimization
}

func (s *bufferedMap) HasChanged(key, row []any, deleted bool) {
	s.Lock()
	defer s.Unlock()

	// The KeyAboveWatermark optimization has to be enabled
	// We enable it once all the setup has been done (since we create a repl client
	// earlier in setup to ensure binary logs are available).
	// We then disable the optimization after the copier phase has finished.
	// Watermark drops happen before the soft-limit wait — those rows never
	// enter the buffer, so there is no point parking on their behalf.
	//
	// CORRECTNESS CAVEAT: discarding relies on the copier's later chunk
	// SELECT seeing this event's transaction. MySQL only makes a
	// transaction's row versions visible at the engine-commit stage, which
	// happens *after* binlog subscribers receive its events (sync stage) —
	// so a chunk dispatched inside that window copies the old state while
	// the event is gone, and the flushed GTID/position still advances past
	// it. A repairing checksum is the only thing that closes the gap, and
	// how much it closes depends on the caller: migrate and move gate
	// cutover on a mandatory FixDifferences checksum, so the divergence
	// never reaches trusted data; continuous sync (pkg/datasync) only
	// repairs it eventually, so its target can serve a
	// missing/stale/phantom row until a later checksum pass covers that
	// chunk. Deterministic repro: TestKeyAboveWatermarkVisibilityWindow.
	// Analysis + fix directions: "Above-watermark discard vs. binlog
	// visibility" in this package's README.
	if s.watermarkOptimizationEnabled() && s.chunker.KeyAboveHighWatermark(key[0]) {
		s.keysDroppedAbove.Add(1)
		s.logger.Debug("key above watermark", "key", key[0])
		return
	}

	hashedKey := utils.HashKey(key)

	// A map-mode overwrite of a key that is already buffered is
	// ~memory-neutral — the new image replaces the old one and sizeBytes
	// is rebalanced below — so it is exempt from the soft limit. This
	// matters enormously for convergence on hot-row workloads: the
	// overwrite is exactly the traffic dedup collapses for free, and
	// parking it would clamp the effective apply rate to the applier's
	// raw drain rate. Keys mid-drain (swapped out by an in-flight Flush)
	// are not overwrites; admitting them grows the map, so they park
	// like any new key.
	dedupOverwrite := false
	if !s.queueModeActive() {
		_, dedupOverwrite = s.changes[hashedKey]
	}

	// Soft backpressure: park while the buffer is at or above the byte
	// threshold. See softLimitBytes on bufferedMap for the semantics.
	// We log on entry and exit because parking stalls the binlog reader
	// — the exit duration is the operator's main signal for binlog-
	// retention risk, and without these lines a stalled migrator looks
	// indistinguishable from one that's just slow.
	if s.softLimitBytes > 0 && !dedupOverwrite && s.sizeBytes >= s.softLimitBytes && !s.closed {
		s.timesParked.Add(1)
		s.requestFlush()
		parkEntryLog, parkExitLog := s.logger.Debug, s.logger.Debug
		if time.Since(s.lastParkWarn) >= parkWarnInterval {
			s.lastParkWarn = time.Now()
			parkEntryLog, parkExitLog = s.logger.Warn, s.logger.Info
		}
		parkEntryLog("subscription parked on soft memory limit",
			"table", s.table.SchemaName+"."+s.table.TableName,
			"size_bytes", s.sizeBytes,
			"soft_limit_bytes", s.softLimitBytes,
		)
		parkStart := time.Now()
		for s.sizeBytes >= s.softLimitBytes && !s.closed {
			s.cond.Wait()
		}
		parkExitLog("subscription unparked from soft memory limit",
			"table", s.table.SchemaName+"."+s.table.TableName,
			"parked_duration", time.Since(parkStart).String(),
			"size_bytes", s.sizeBytes,
			"closed", s.closed,
		)
	}
	// On close we fall through and admit the row even if it exceeds the
	// soft limit. The buffer will be discarded by the caller; admitting
	// keeps subscription.Length() consistent with the buffered position
	// that readStream advances after processRowsEvent returns, so a
	// concurrent flush cannot publish a flushedPos that skips this event.

	logicalRow := applier.LogicalRow{RowImage: row}
	if deleted {
		logicalRow = applier.LogicalRow{IsDeleted: true}
	}

	if s.queueModeActive() {
		qc := queuedChange{key: hashedKey, logicalRow: logicalRow, originalKey: key}
		s.queue = append(s.queue, qc)
		s.sizeBytes += sizeOfQueuedChange(qc)
		s.keysAdded.Add(1)
		return
	}

	bc := bufferedChange{
		logicalRow:  logicalRow,
		originalKey: key,
	}
	if old, ok := s.changes[hashedKey]; ok {
		// Map-mode dedup: subtract the outgoing image's bytes before
		// the new image takes its place. Keeps sizeBytes balanced
		// across overwrites.
		s.sizeBytes -= sizeOfBufferedChange(hashedKey, old)
	}
	s.changes[hashedKey] = bc
	s.sizeBytes += sizeOfBufferedChange(hashedKey, bc)
	s.keysAdded.Add(1)
}

// requestFlush performs a non-blocking send of this subscription on the
// flush-request channel, if one is configured. Caller holds s.Lock; the
// send must not block. Only one subscription per client can be parked
// at a time (the single change-reader goroutine is what parks), so a
// buffered channel of capacity 1 never drops a live request; if a stale
// token from an earlier park is still in flight, the receiver's
// subsequent all-subscription pass covers this subscription anyway.
func (s *bufferedMap) requestFlush() {
	if s.flushRequest == nil {
		return
	}
	select {
	case s.flushRequest <- s:
	default:
	}
}

// Flush writes the pending changes to the new table.
//
// The normal (not underLock) path does NOT hold the Mutex while the
// applier round trips are in flight. It swaps the pending stores out
// under the Mutex, drains the snapshot batch by batch, and releases
// bytes (broadcasting the cond) as each batch lands. New events keep
// buffering — and, in map mode, deduping — into the fresh stores the
// whole time, so a long drain no longer stalls the change reader for
// its full duration. Anything the drain could not apply (watermark-
// deferred keys, or the remainder after an applier error) is merged
// back into the active stores before returning; see reattachLocked.
//
// The reported allChangesFlushed covers the snapshot only: events that
// arrive during the drain are, by design, not part of this flush. That
// matches the position contract in the clients — the flushed position
// they publish on success is captured *before* Flush is called.
//
// Per-key ordering is preserved: an event arriving during the drain is
// strictly newer than the snapshot's image for the same key, lands in
// the active store, and is applied by a later flush. Cross-key ordering
// guarantees are unchanged (map mode is order-free across keys; queue
// mode drains FIFO and flushes are serialized by flushMu).
//
// When underLock is true the entire drain runs while holding the Mutex,
// exactly as before: the cutover flush must be atomic with respect to
// event delivery, and with the source table locked there is no
// concurrent traffic to win anyway.
//
// SetWatermarkOptimization drains the outgoing store inline before
// flipping the mode flag, so under normal operation only one of
// map/queue has entries when Flush runs. Both branches are still
// handled defensively in case anything ever leaves the inactive store
// non-empty.
func (s *bufferedMap) Flush(ctx context.Context, underLock bool, locks []*dbconn.TableLock) (allChangesFlushed bool, err error) {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()

	if underLock {
		s.Lock()
		defer s.Unlock()

		allChangesFlushed = true
		if len(s.changes) > 0 {
			mapAllFlushed, err := s.flushMapLocked(ctx, true, locks, false)
			if err != nil {
				return false, err
			}
			if !mapAllFlushed {
				allChangesFlushed = false
			}
		}
		if len(s.queue) > 0 {
			if err := s.flushQueueLocked(ctx, true, locks); err != nil {
				return false, err
			}
		}
		return allChangesFlushed, nil
	}

	// Swap the pending stores out under the Mutex. From here until the
	// deferred reattach, the snapshot belongs exclusively to this
	// goroutine; shared visibility of its pending entries is provided
	// via flushingCount and sizeBytes only.
	s.Lock()
	snapshot := s.changes
	if len(snapshot) > 0 {
		s.changes = make(map[string]bufferedChange)
	} else {
		snapshot = nil
	}
	snapshotQueue := s.queue
	s.queue = nil
	s.flushingCount = len(snapshot) + len(snapshotQueue)
	applyWatermarkFilter := s.watermarkOptimizationEnabled()
	s.Unlock()

	// Whatever the drains below leave in the snapshots — watermark-
	// deferred keys on success, the unapplied remainder on error — is
	// merged back into the active stores no matter how we return.
	defer func() {
		s.reattachLocked(snapshot, snapshotQueue)
	}()

	allChangesFlushed = true
	if len(snapshot) > 0 {
		mapAllFlushed, err := s.drainMapSnapshot(ctx, snapshot, applyWatermarkFilter)
		if err != nil {
			return false, err
		}
		if !mapAllFlushed {
			allChangesFlushed = false
		}
	}
	if len(snapshotQueue) > 0 {
		remainder, err := s.drainQueueSnapshot(ctx, snapshotQueue)
		snapshotQueue = remainder
		if err != nil {
			return false, err
		}
	}
	return allChangesFlushed, nil
}

// mapFlushBatch is one applier round trip's worth of a map-mode drain:
// disjoint keys, pre-partitioned into deletes and upserts, with the
// buffer-accounting byte total captured at build time. The batch only
// holds pointers into the snapshot's entries; the worker that lands it
// drops those entries from the snapshot and nils the batch's slices so
// the row images become collectible while the drain is still running.
type mapFlushBatch struct {
	keys        []string
	deleteKeys  [][]any
	upsertRows  []applier.LogicalRow
	storedBytes int64
}

// drainMapSnapshot applies a swapped-out map snapshot through the
// applier without holding s.Mutex. Batches are built serially, then
// applied with up to flushConcurrency applier calls in flight: a map
// snapshot holds exactly one image per key, so batches are disjoint by
// key, and map mode makes no cross-key ordering promises — REPLACE and
// DELETE against distinct keys commute. The binlog apply path is
// synchronous statements on one connection per call (it does not use
// the copy path's write worker pool), so a serial drain tops out at
// batch-rows / statement-round-trip; on large tables with secondary
// index maintenance that is a few hundred rows/s, which a busy source
// outruns. Parallel batches multiply that ceiling.
//
// Each batch releases its bytes/count under the Mutex (with a cond
// broadcast) and deletes its entries from the snapshot as it lands, so
// a parked HasChanged caller resumes after the first batch, not after
// the whole drain, and applied row images become collectible while the
// rest of the drain is still running. Entries deferred by the
// low-watermark filter (and the unapplied remainder after an error or
// cancellation) stay in the snapshot for the caller to merge back.
// Returns false when any entry was deferred.
func (s *bufferedMap) drainMapSnapshot(ctx context.Context, snapshot map[string]bufferedChange, applyWatermarkFilter bool) (bool, error) {
	var batches []*mapFlushBatch
	current := &mapFlushBatch{}
	var batchStmtBytes int64
	allChangesFlushed := true

	cutBatch := func() {
		if len(current.keys) > 0 {
			batches = append(batches, current)
			current = &mapFlushBatch{}
			batchStmtBytes = 0
		}
	}

	// Map iteration order is randomized, so batch membership differs between
	// flushes. That does not affect the retry below: pass 2 re-applies the very
	// same mapFlushBatch values pass 1 built, so a contended batch retries as
	// itself regardless of iteration order.
	//
	// An earlier revision sorted by primary key here, on the theory that a
	// consistent lock-acquisition order would help. It would not: the observed
	// deadlock cycle inverts between the clustered index and a secondary UNIQUE
	// index, and secondary key order is unrelated to primary key order, so
	// PK-sorted batches still interleave there. Narrowing concurrency is what
	// breaks the cycle. See block/spirit#1168.
	batchSize := s.effectiveBatchSize()
	for key, change := range snapshot {
		// Keys the copier may have a read in flight for are deferred to a
		// later flush; see mustDeferKey. The chunker is internally
		// synchronized, so no Mutex is needed here.
		if applyWatermarkFilter && s.mustDeferKey(change.originalKey[0]) {
			s.keysSkippedBelow.Add(1)
			s.logger.Debug("key deferred: copier read in flight", "key", change.originalKey[0])
			allChangesFlushed = false
			continue
		}
		// Cut the batch when either cap is reached: the (possibly reduced)
		// batch-size limit, or the estimated rendered statement size would
		// exceed the byte budget the copy path also uses. Without the byte
		// cap, buffered wide rows (LONGTEXT / BLOB) can render into a single
		// REPLACE larger than max_allowed_packet — a deterministic,
		// non-retryable failure. A single row over the budget still flushes,
		// alone in its own batch (a row can't be split).
		rowBytes := renderedBytesOfChange(change.logicalRow, change.originalKey)
		if batchLen := len(current.keys); batchLen >= batchSize ||
			(batchLen > 0 && batchStmtBytes+rowBytes > applier.MaxStatementSizeBytes) {
			cutBatch()
		}
		current.keys = append(current.keys, key)
		current.storedBytes += sizeOfBufferedChange(key, change)
		if change.logicalRow.IsDeleted {
			current.deleteKeys = append(current.deleteKeys, change.originalKey)
		} else {
			current.upsertRows = append(current.upsertRows, change.logicalRow)
		}
		batchStmtBytes += rowBytes
	}
	cutBatch()

	if len(batches) == 0 {
		// Every key was watermark-deferred. The drain produced no evidence about
		// contention either way, so it must not advance the clean-drain streak —
		// otherwise a run of all-deferred flushes would widen the concurrency
		// back out on the strength of having applied nothing at all.
		return allChangesFlushed, nil
	}

	// Pass 1: apply concurrently at the current (possibly reduced) width.
	// Batches that lose to lock contention are collected rather than failing
	// the drain — see retryContendedBatches for why that is safe and why the
	// retry has to be serial.
	contended, err := s.applyBatchesConcurrent(ctx, snapshot, batches)
	if err != nil {
		// Deliberately no adaptFlushConcurrency call here. A drain that failed
		// on something other than contention is not evidence of contention, and
		// it is not evidence of quiet either: feeding it in as "clean" would let
		// three consecutive hard failures — during which nothing flushed —
		// restore the full width, and feeding it in as "contended" would
		// penalise the width for an unrelated non-retryable error that merely
		// happened to land in the same drain as someone else's 1213.
		return false, err
	}

	// Pass 2: whatever contended in pass 1 goes again, one batch at a time.
	if len(contended) > 0 {
		s.batchesContended.Add(int64(len(contended)))
		s.logger.Warn("flush batches lost to lock contention; retrying serially",
			"table", s.table.SchemaName+"."+s.table.TableName,
			"contended_batches", len(contended),
			"total_batches", len(batches),
			"concurrency", s.effectiveFlushConcurrency(),
		)
		if err := s.retryContendedBatches(ctx, snapshot, contended); err != nil {
			// Contention that survived a serial retry is unambiguous evidence,
			// so this one does feed the controller before returning.
			s.adaptFlushConcurrency(true)
			return false, err
		}
		s.serialRecoveries.Add(1)
	}
	s.adaptFlushConcurrency(len(contended) > 0)
	return allChangesFlushed, nil
}

// applyBatchesConcurrent runs batches through the applier with up to
// effectiveFlushConcurrency in flight, returning the batches that failed on
// InnoDB lock contention (1205/1213) for the caller to retry serially.
//
// Contention does *not* cancel the group. Every other error class does, exactly
// as before: those are not self-inflicted and retrying at a narrower width
// would not help. Contention is different — spirit's own concurrent REPLACEs
// are both sides of the conflict, so the same work succeeds unconditionally
// once it stops racing itself. Letting one contended batch cancel its siblings
// (the previous behaviour) threw away batches that were about to land and made
// the whole drain fail, which is what pinned the buffer at its soft limit and
// froze the flushed position.
func (s *bufferedMap) applyBatchesConcurrent(ctx context.Context, snapshot map[string]bufferedChange, batches []*mapFlushBatch) ([]*mapFlushBatch, error) {
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(s.effectiveFlushConcurrency())
	// Workers delete their batch's entries as they land, so they only
	// contend with each other here: the build loop finished iterating the
	// snapshot before the first worker starts, and the deferred reattach
	// runs after Wait.
	var mu sync.Mutex
	var contended []*mapFlushBatch
	var skippedBatches bool
	for _, batch := range batches {
		if gctx.Err() != nil {
			skippedBatches = true
			break // a batch failed or ctx was canceled; don't queue the rest
		}
		g.Go(func() error {
			if err := s.flushBatch(gctx, batch.deleteKeys, batch.upsertRows, nil); err != nil {
				// The ctx.Err() half deliberately reads the *parent*, not gctx:
				// a sibling's failure cancels gctx, and that must not stop this
				// batch's own contention from reaching the retry pass. Only a
				// real shutdown does, because a 1213 racing a cancellation is a
				// symptom of the connection going away rather than something a
				// narrower flush would avoid — and pass 2 should not spend its
				// budget, or log a concurrency reduction, on a drain that is
				// already over. See TestContentionAtShutdownIsNotRetried.
				if dbconn.IsLockContentionError(err) && ctx.Err() == nil {
					mu.Lock()
					contended = append(contended, batch)
					mu.Unlock()
					return nil // handled by the serial retry pass
				}
				return err
			}
			s.releaseAppliedBatch(snapshot, &mu, batch)
			return nil
		})
	}
	err := g.Wait()
	if err == nil && skippedBatches {
		// Scheduling stopped early yet no scheduled batch failed, so the
		// group context can only have been canceled through ctx itself.
		// Report that rather than success: on a nil error the clients
		// publish the position captured before this flush as flushed,
		// which would cover the skipped batches — but those entries were
		// only reattached, not applied.
		err = ctx.Err()
	}
	return contended, err
}

// retryContendedBatches re-applies contended batches one at a time.
//
// Serial is the point, not a simplification. The contention is entirely
// self-inflicted — the observed deadlock cycle was three of this drain's own
// REPLACEs — so with a single writer there is no second transaction left to
// form a cycle with, and no sibling holding the records this batch needs. The
// retry is expected to succeed on its first, undelayed attempt; the escalating
// attempts after it exist only for locks held from outside this drain.
//
// A batch that still fails here returns the error, leaving its entries in the
// snapshot to be reattached and retried on the next flush. That is deliberate:
// the flushed position must not advance over changes that never landed.
func (s *bufferedMap) retryContendedBatches(ctx context.Context, snapshot map[string]bufferedChange, contended []*mapFlushBatch) error {
	// Bound the whole pass, not just each batch. Against an external 1205 holder
	// a single attempt is not cheap: flushBatch goes through
	// dbconn.RetryableTransaction, which burns its own MaxRetries against
	// innodb_lock_wait_timeout before returning, so one batch can consume tens
	// of seconds and N batches multiply that. flushMu is held for the entire
	// drain, so an unbounded pass 2 stalls every subsequent flush — trading the
	// frozen checkpoint this PR fixes for a slower version of the same thing.
	//
	// Giving up early is cheap by comparison: the remaining batches stay in the
	// snapshot, get reattached, and are retried on the next flush.
	passCtx, cancel := context.WithTimeout(ctx, contentionRetryBudget)
	defer cancel()

	var mu sync.Mutex
	for _, batch := range contended {
		var err error
		for attempt := range contentionRetries {
			select {
			case <-passCtx.Done():
				// Distinguish our own budget from real shutdown: on budget
				// expiry the parent is still live and the caller should treat
				// this as ordinary contention, not cancellation.
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return fmt.Errorf("serial contention retry budget (%s) exhausted with %w",
					contentionRetryBudget, errStillContended)
			case <-time.After(contentionBackoff(attempt)):
			}
			if err = s.flushBatch(passCtx, batch.deleteKeys, batch.upsertRows, nil); err == nil {
				s.releaseAppliedBatch(snapshot, &mu, batch)
				break
			}
			if !dbconn.IsLockContentionError(err) {
				return err
			}
		}
		if err != nil {
			return fmt.Errorf("flush batch still contended after %d serial retries: %w", contentionRetries, err)
		}
	}
	return nil
}

// errStillContended marks a pass-2 giveup so callers can tell "we ran out of
// patience with a lock holder" from a genuine cancellation.
var errStillContended = errors.New("batches still contended")

// contentionRetries is how many serial attempts a contended batch gets before
// the drain gives up and leaves it for the next flush. The first is immediate
// and is the one expected to land — for the self-inflicted deadlock this exists
// to fix, no sibling is left holding anything once pass 1 has joined.
//
// The sleeps between the remaining attempts add up to well under a second, but
// that is not the cost that matters: each *attempt* is a flushBatch that can
// block on a real lock for as long as dbconn.RetryableTransaction is willing to
// wait. contentionRetryBudget, not this count, is what bounds the pass.
const contentionRetries = 4

// contentionRetryBudget caps the total wall-clock time pass 2 may spend, across
// all contended batches, before giving up and deferring the rest to the next
// flush. Sized to comfortably cover the self-inflicted case (which returns
// almost immediately) while keeping flushMu out of the multi-minute territory
// that N batches against an external lock holder would otherwise reach.
const contentionRetryBudget = 20 * time.Second

// releaseAppliedBatch drops a landed batch's entries from the snapshot and
// returns its bytes to the buffer.
//
// Dropping from the snapshot means the deferred reattach merges back only what
// did not land (uncertain batches stay and re-apply on a later flush, which is
// safe: REPLACE and DELETE are idempotent). Nilling the batch's slices lets the
// row images become collectible now rather than when the whole drain finishes —
// without it the snapshot retains up to a full soft limit of applied images
// while the live map refills toward another, transiently doubling the memory
// the cap is meant to bound.
func (s *bufferedMap) releaseAppliedBatch(snapshot map[string]bufferedChange, mu *sync.Mutex, batch *mapFlushBatch) {
	mu.Lock()
	for _, key := range batch.keys {
		delete(snapshot, key)
	}
	mu.Unlock()
	flushedKeys := len(batch.keys)
	batch.keys, batch.deleteKeys, batch.upsertRows = nil, nil, nil
	s.Lock()
	s.sizeBytes -= batch.storedBytes
	s.flushingCount -= flushedKeys
	s.cond.Broadcast()
	s.Unlock()
}

// drainQueueSnapshot applies a swapped-out queue snapshot through the
// applier in FIFO order without holding s.Mutex, coalescing consecutive
// same-type operations exactly like flushQueueLocked. Bytes and counts
// are released per applied segment. Returns the unapplied remainder
// (nil on success) so the caller can merge it back ahead of any events
// that arrived during the drain.
func (s *bufferedMap) drainQueueSnapshot(ctx context.Context, snapshot []queuedChange) ([]queuedChange, error) {
	var deleteKeys [][]any
	var upsertRows []applier.LogicalRow
	var batchBytes int64
	segmentStart := 0 // index of the first snapshot entry in the current segment
	applied := 0      // count of snapshot entries applied so far

	flushSegment := func(segmentEnd int) error {
		if segmentEnd == segmentStart {
			return nil
		}
		if err := s.flushBatch(ctx, deleteKeys, upsertRows, nil); err != nil {
			return err
		}
		var drainedBytes int64
		for _, qc := range snapshot[segmentStart:segmentEnd] {
			drainedBytes += sizeOfQueuedChange(qc)
		}
		s.Lock()
		s.sizeBytes -= drainedBytes
		s.flushingCount -= segmentEnd - segmentStart
		s.cond.Broadcast()
		s.Unlock()
		applied = segmentEnd
		segmentStart = segmentEnd
		deleteKeys = nil
		upsertRows = nil
		batchBytes = 0
		return nil
	}

	prevIsDelete := snapshot[0].logicalRow.IsDeleted
	for i, change := range snapshot {
		rowBytes := renderedBytesOfChange(change.logicalRow, change.originalKey)
		typeFlip := change.logicalRow.IsDeleted != prevIsDelete
		batchFull := i-segmentStart >= DefaultBatchSize
		overBudget := i > segmentStart && batchBytes+rowBytes > applier.MaxStatementSizeBytes
		if typeFlip || batchFull || overBudget {
			if err := flushSegment(i); err != nil {
				return snapshot[applied:], err
			}
		}
		if change.logicalRow.IsDeleted {
			deleteKeys = append(deleteKeys, change.originalKey)
		} else {
			upsertRows = append(upsertRows, change.logicalRow)
		}
		batchBytes += rowBytes
		prevIsDelete = change.logicalRow.IsDeleted
	}
	if err := flushSegment(len(snapshot)); err != nil {
		return snapshot[applied:], err
	}
	return nil, nil
}

// reattachLocked merges the unapplied remainder of an in-flight flush
// snapshot back into the active stores and clears flushingCount.
//
// Map entries follow newer-wins: any event that arrived during the drain
// is strictly newer than the snapshot's image for the same key, so when
// the active map already holds the key the snapshot's stale image is
// dropped (releasing its bytes). Queue remainders are prepended so
// binlog order is preserved ahead of events that arrived during the
// drain. Safe to call with empty/nil snapshots.
func (s *bufferedMap) reattachLocked(snapshot map[string]bufferedChange, snapshotQueue []queuedChange) {
	s.Lock()
	defer s.Unlock()
	for key, change := range snapshot {
		if _, ok := s.changes[key]; ok {
			s.sizeBytes -= sizeOfBufferedChange(key, change)
		} else {
			s.changes[key] = change
		}
	}
	if len(snapshotQueue) > 0 {
		s.queue = append(snapshotQueue, s.queue...)
	}
	s.flushingCount = 0
	s.cond.Broadcast()
}

// flushMapLocked drains s.changes through the applier. Caller must hold s.Lock.
//
// bypassWatermark forces every entry to flush regardless of the low-watermark
// filter and irrespective of the current value of s.watermarkOptimization.
// SetWatermarkOptimization uses this to drain the outgoing store before
// flipping the flag — the flag is still `true` at that point, so the normal
// filter would skip keys above the low watermark and leave them behind in the
// store we are about to abandon. underLock (cutover) implies bypass for the
// same reason.
func (s *bufferedMap) flushMapLocked(ctx context.Context, underLock bool, locks []*dbconn.TableLock, bypassWatermark bool) (bool, error) {
	var deleteKeys [][]any
	var upsertRows []applier.LogicalRow
	var keysFlushed []string
	var batchBytes int64
	allChangesFlushed := true

	var locksToUse []*dbconn.TableLock
	if underLock {
		locksToUse = locks
	}
	applyWatermarkFilter := !underLock && !bypassWatermark && s.watermarkOptimizationEnabled()

	for key, change := range s.changes {
		// In bufferedMap, keys the copier may have a read in flight for are
		// deferred to a later flush (see mustDeferKey). It is only safe to
		// skip when we are not under cutover lock and the caller has not
		// asked us to drain everything (bypassWatermark).
		if applyWatermarkFilter && s.mustDeferKey(change.originalKey[0]) {
			s.keysSkippedBelow.Add(1)
			s.logger.Debug("key deferred: copier read in flight", "key", change.originalKey[0])
			allChangesFlushed = false
			continue
		}
		// Cut the batch when either cap is reached: DefaultBatchSize rows,
		// or the estimated rendered statement size would exceed the byte
		// budget the copy path also uses. Without the byte cap, buffered
		// wide rows (LONGTEXT / BLOB) can render into a single REPLACE
		// larger than max_allowed_packet — a deterministic, non-retryable
		// failure. A single row over the budget still flushes, alone in
		// its own batch (a row can't be split).
		rowBytes := renderedBytesOfChange(change.logicalRow, change.originalKey)
		if batchLen := len(deleteKeys) + len(upsertRows); batchLen >= DefaultBatchSize ||
			(batchLen > 0 && batchBytes+rowBytes > applier.MaxStatementSizeBytes) {
			if err := s.flushBatch(ctx, deleteKeys, upsertRows, locksToUse); err != nil {
				return false, err
			}
			deleteKeys = nil
			upsertRows = nil
			batchBytes = 0
		}
		keysFlushed = append(keysFlushed, key) // we are going to flush this key (hashed map key)
		if change.logicalRow.IsDeleted {
			deleteKeys = append(deleteKeys, change.originalKey)
		} else {
			upsertRows = append(upsertRows, change.logicalRow)
		}
		batchBytes += rowBytes
	}

	if err := s.flushBatch(ctx, deleteKeys, upsertRows, locksToUse); err != nil {
		return false, err
	}

	var drainedBytes int64
	for _, key := range keysFlushed {
		if c, ok := s.changes[key]; ok {
			drainedBytes += sizeOfBufferedChange(key, c)
			delete(s.changes, key)
		}
	}
	if drainedBytes > 0 {
		s.sizeBytes -= drainedBytes
		s.cond.Broadcast()
	}
	return allChangesFlushed, nil
}

// flushBatch flushes a batch of deletes and upserts using the applier.
// deleteKeys holds the typed PK tuples of the rows to delete (one tuple
// per entry, in KeyColumns order).
// If locks is non-empty, the operations are executed under the table
// lock(s) — one lock per target server, matched to its target by the
// applier (see applier.Applier.DeleteKeys for the contract).
func (s *bufferedMap) flushBatch(ctx context.Context, deleteKeys [][]any, upsertRows []applier.LogicalRow, locks []*dbconn.TableLock) error {
	if len(deleteKeys) == 0 && len(upsertRows) == 0 {
		return nil
	}
	startTime := time.Now()
	var deleteAffected, upsertAffected int64

	// Execute deletes
	if len(deleteKeys) > 0 {
		affectedRows, err := s.applier.DeleteKeys(ctx, s.table, s.newTable, deleteKeys, locks)
		if err != nil {
			return fmt.Errorf("failed to delete keys: %w", err)
		}
		deleteAffected = affectedRows
	}

	// Execute upserts
	if len(upsertRows) > 0 {
		affectedRows, err := s.applier.UpsertRows(ctx, s.chunker.ColumnMapping(), upsertRows, locks)
		if err != nil {
			return fmt.Errorf("failed to upsert rows: %w", err)
		}
		upsertAffected = affectedRows
	}

	s.logger.Debug("flushBatch executed",
		"table", s.table.TableName,
		"underLock", len(locks) > 0,
		"deleteKeyCount", len(deleteKeys),
		"deleteAffectedRows", deleteAffected,
		"upsertRowCount", len(upsertRows),
		"upsertAffectedRows", upsertAffected,
		"duration", time.Since(startTime).String(),
	)

	return nil
}

// flushQueueLocked drains s.queue through the applier in FIFO order. We
// keep the row images that HasChanged stored — the queue only exists to
// preserve order for non-memory-comparable PKs (collation-equivalent keys
// like "A" and "a" hash to different map slots but resolve to the same
// MySQL row, so the map's non-deterministic iteration would apply events
// out of order). FIFO + the target's collation-aware uniqueness gives the
// correct end state without a SELECT against source. Caller must hold s.Lock.
//
// To preserve order while still batching for throughput, we coalesce
// consecutive same-type operations into one applier call, e.g.
// UPSERT<1>, UPSERT<2>, DELETE<3>, UPSERT<4> becomes
// UpsertRows([1,2]); DeleteKeys([3]); UpsertRows([4]).
func (s *bufferedMap) flushQueueLocked(ctx context.Context, underLock bool, locks []*dbconn.TableLock) error {
	if len(s.queue) == 0 {
		return nil
	}
	var locksToUse []*dbconn.TableLock
	if underLock {
		locksToUse = locks
	}

	var deleteKeys [][]any
	var upsertRows []applier.LogicalRow
	var batchBytes int64
	flushSegment := func() error {
		if err := s.flushBatch(ctx, deleteKeys, upsertRows, locksToUse); err != nil {
			return err
		}
		deleteKeys = nil
		upsertRows = nil
		batchBytes = 0
		return nil
	}

	prevIsDelete := s.queue[0].logicalRow.IsDeleted
	var drainedBytes int64
	for _, change := range s.queue {
		// The byte cap mirrors flushMapLocked: cut the segment before the
		// estimated rendered statement would exceed the budget, so wide
		// rows can't produce a REPLACE/DELETE over max_allowed_packet. An
		// oversized single row still flushes alone in its own segment.
		rowBytes := renderedBytesOfChange(change.logicalRow, change.originalKey)
		typeFlip := change.logicalRow.IsDeleted != prevIsDelete
		batchFull := len(deleteKeys)+len(upsertRows) >= DefaultBatchSize
		overBudget := len(deleteKeys)+len(upsertRows) > 0 && batchBytes+rowBytes > applier.MaxStatementSizeBytes
		if typeFlip || batchFull || overBudget {
			if err := flushSegment(); err != nil {
				return err
			}
		}
		if change.logicalRow.IsDeleted {
			deleteKeys = append(deleteKeys, change.originalKey)
		} else {
			upsertRows = append(upsertRows, change.logicalRow)
		}
		batchBytes += rowBytes
		drainedBytes += sizeOfQueuedChange(change)
		prevIsDelete = change.logicalRow.IsDeleted
	}
	if err := flushSegment(); err != nil {
		return err
	}

	s.queue = nil
	if drainedBytes > 0 {
		s.sizeBytes -= drainedBytes
		s.cond.Broadcast()
	}
	return nil
}

// mustDeferKey reports whether a buffered change for key0 has to sit out this
// flush because the copier may have a read in flight for it.
//
// Only the *in-flight band* has to wait:
//
//	[ copied & committed ) [ dispatched, not yet committed ) [ not dispatched )
//	        flush now                    defer                    flush now
//
// The right-hand region is what KeyNotYetDispatched adds. Deferring it too
// (the pre-fix behaviour) is what pinned the checkpoint's binlog position for
// a whole copy: every change buffered before the copier dispatched its first
// chunk — the window between SetWatermarkOptimization(true) and the first
// chunker.Next(), which the throttler can stretch arbitrarily — sits above the
// low watermark until the copier physically reaches its key, and one such
// entry is enough to make every flush report allChangesFlushed=false.
func (s *bufferedMap) mustDeferKey(key0 any) bool {
	if s.chunker.KeyBelowLowWatermark(key0) {
		return false // already copied and committed
	}
	return !s.chunker.KeyNotYetDispatched(key0)
}

// watermarkOptimizationEnabled returns true if the watermark optimization
// is enabled. This is already called under a mutex.
func (s *bufferedMap) watermarkOptimizationEnabled() bool {
	return s.watermarkOptimization && s.chunker != nil
}

// Close releases any HasChanged caller parked on the soft memory limit so
// the binlog reader goroutine can exit on Client.Close(). Pending changes
// are not flushed; they are discarded along with the subscription. Safe
// to call more than once.
func (s *bufferedMap) Close() {
	s.Lock()
	s.closed = true
	if s.cond != nil {
		s.cond.Broadcast()
	}
	s.Unlock()
}

// SetWatermarkOptimization toggles the watermark filter and, if the toggle
// changes which store is active, fully drains the *outgoing* store before
// returning. After a successful call the invariant holds: only the active
// store may have entries.
//
// Ordering: the outgoing store is drained *before* the flag is flipped, so
// a drain failure leaves the subscription in its prior mode rather than a
// half-toggled state (flag flipped, old store still dirty). New events
// continue to land in the old store until the caller successfully retries
// the toggle. The call is still idempotent — retrying with the same
// `enabled` value recomputes the same target mode and re-runs the drain.
//
// The drain MUST bypass the watermark filter. When leaving map mode,
// s.watermarkOptimization is still `true` and the normal filter would skip
// any key not below the low watermark and leave it stranded in s.changes
// while subsequent events land in s.queue. That stranded map entry would
// then be applied out of order with respect to the queue (queue mode exists
// precisely to preserve order for non-memory-comparable PKs). The bypass
// flag on flushMapLocked closes that gap, and we assert s.changes is empty
// after the drain to catch any future regression.
//
// flushMu is taken first (same order as Flush): a mode transition must
// not begin while a swapped-out flush snapshot is mid-drain, and its own
// inline drain must hold the Mutex throughout — the transition invariant
// ("only the active store may have entries afterwards") only holds if no
// events can land in the outgoing store during the drain.
func (s *bufferedMap) SetWatermarkOptimization(ctx context.Context, enabled bool) error {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	s.Lock()
	defer s.Unlock()

	// Compute the target mode from `enabled` without flipping the flag,
	// so a failed drain leaves watermarkOptimization unchanged.
	// queueModeActive() = !pkIsMemoryComparable && !watermarkOptimization,
	// so the target mode under `enabled` mirrors that formula.
	targetQueueMode := !s.pkIsMemoryComparable && !enabled
	currentQueueMode := s.queueModeActive()

	if currentQueueMode != targetQueueMode {
		// Mode transition: drain the store we're leaving so the invariant
		// "only the active store may have entries" holds after the flip.
		if currentQueueMode {
			// Leaving queue mode; queue is the outgoing store.
			if len(s.queue) > 0 {
				if err := s.flushQueueLocked(ctx, false, nil); err != nil {
					return fmt.Errorf("draining queue on watermark toggle: %w", err)
				}
			}
		} else {
			// Leaving map mode; map is the outgoing store. We must bypass
			// the watermark filter here: s.watermarkOptimization is still
			// `true` at this point (we have not flipped it yet), so without
			// the bypass flushMapLocked would skip any key not below the
			// low watermark and leave it in the store we are about to
			// abandon — violating the post-toggle invariant that only the
			// active store has entries, and risking out-of-order apply
			// against the queue we are switching into.
			if len(s.changes) > 0 {
				if _, err := s.flushMapLocked(ctx, false, nil, true); err != nil {
					return fmt.Errorf("draining map on watermark toggle: %w", err)
				}
				if len(s.changes) > 0 {
					return fmt.Errorf("draining map on watermark toggle: %d entries remained after bypass drain", len(s.changes))
				}
			}
		}
	}

	// Drain succeeded (or no drain needed) — safe to flip the flag now.
	s.watermarkOptimization = enabled

	s.logger.Debug("watermark optimization toggled",
		"table", s.table.TableName,
		"enabled", enabled,
		"keys_added", s.keysAdded.Swap(0),
		"keys_dropped_above_high", s.keysDroppedAbove.Swap(0),
		"keys_skipped_not_below_low", s.keysSkippedBelow.Swap(0),
		"times_parked_on_soft_limit", s.timesParked.Swap(0),
		"batches_lock_contended", s.batchesContended.Swap(0),
		"drains_rescued_serially", s.serialRecoveries.Swap(0),
		"flush_concurrency", s.effectiveFlushConcurrency(),
		"delta_len", len(s.changes)+len(s.queue),
		"size_bytes", s.sizeBytes,
	)
	return nil
}
