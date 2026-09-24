package change

import (
	"context"
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

	// batchSize is the maximum rows one flush batch renders into a
	// single statement, before the AIMD penalty shifts it down. Zero
	// means DefaultBatchSize. See effectiveBatchSize.
	batchSize int

	// partitioner holds the unique secondary indexes a drain may partition
	// its batches by, resolved lazily on first use. See
	// subscription_buffered_partition.go.
	partitioner flushPartitioner

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

	// softLimitChanges is the second soft cap, on pending change *count*
	// (Length(), so in-flight drain entries included). Zero disables it.
	// Same admit-then-park semantics as softLimitBytes. See
	// overSoftLimitLocked for why both caps exist.
	softLimitChanges int

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

	// loadPenalty is the number of halvings applied to flushConcurrency on
	// account of *server load* rather than lock contention, from the UnderLoad
	// signal. It is a second, independent term rather than more steps on
	// concurrencyPenalty because the two want opposite things from batch size:
	// contention narrows the batch as well, to shrink a statement's lock
	// footprint, while load widens it, to hold the same rows in flight across
	// fewer statements. Folding them together would mean one signal silently
	// undoing the other's batch decision. See adaptFlushLoad and
	// effectiveBatchSize.
	loadPenalty     atomic.Int64
	cleanLoadDrains atomic.Int64

	// underLoad is ClientConfig.UnderLoad, or nil when the caller supplied no
	// signal — in which case loadPenalty never moves and the drain behaves
	// exactly as it did before load shedding existed.
	underLoad func() bool

	// Counters for the bookend log emitted on watermark-optimization transitions.
	keysAdded        atomic.Int64
	keysDroppedAbove atomic.Int64
	keysSkippedBelow atomic.Int64
	// timesParked counts every park on a soft limit, cumulatively for the
	// life of the subscription. Unlike its neighbours it is not reset by the
	// watermark-toggle bookend log, because the status block reports it
	// periodically and a counter that silently restarts mid-migration reads
	// as the parking having stopped.
	timesParked      atomic.Int64
	batchesContended atomic.Int64 // applier batches that failed on 1205/1213
	batchesDeferred  atomic.Int64 // contended batches left for the next flush
	serialRecoveries atomic.Int64 // drains rescued by the serial retry pass
	drainsTimedOut   atomic.Int64 // drains cut short by drainDispatchBudget

	// parked is true while a HasChanged caller is sitting on the soft
	// limit. Guarded by Mutex, which the parked goroutine releases inside
	// cond.Wait(), so a status reader can observe it while the park is in
	// progress — that is the whole point of reporting it.
	parked bool

	// lastDrainHitBudget records whether the most recent Flush left work
	// behind because it ran out of dispatch budget, rather than because that
	// work was not eligible. Written once per Flush under flushMu and read
	// afterwards by the clients' Flush loops; see LastDrainHitBudget.
	lastDrainHitBudget atomic.Bool

	pkIsMemoryComparable bool
}

// Per-entry overheads applied on top of estimateRowSize so the soft
// limit tracks closer to real RSS for high-cardinality, narrow-row
// workloads (where the variable-width contents don't dominate). For
// wide-row workloads — the OOM scenario this cap was added to defend
// against — these constants are noise next to the BLOB / large-string
// payload sizes. Both are approximate; the cap is "soft" anyway.
const (
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

	// SoftLimitChanges is the per-subscription cap on pending change
	// *count* before HasChanged parks, applied alongside SoftLimitBytes;
	// whichever binds first parks the reader. Zero disables it. See
	// bufferedMap.overSoftLimitLocked for why both exist.
	SoftLimitChanges int

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

	// BatchSize is the maximum number of rows one flush batch renders
	// into a single statement. Zero means DefaultBatchSize, preserving
	// prior behaviour for callers that do not set it.
	//
	// It is not independent of FlushConcurrency: their product is the
	// rows a drain has in flight, so a caller raising one should lower
	// the other. autoscale.FlushBounds returns the pair.
	BatchSize int

	// UnderLoad is ClientConfig.UnderLoad: the server-load signal the drain
	// narrows itself on. Optional; nil disables load shedding entirely.
	UnderLoad func() bool
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
		softLimitChanges:     cfg.SoftLimitChanges,
		flushRequest:         cfg.FlushRequest,
		flushConcurrency:     cfg.FlushConcurrency,
		batchSize:            cfg.BatchSize,
		underLoad:            cfg.UnderLoad,
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
	return s.lengthLocked()
}

// ParkStats satisfies ParkReporter, so the runner's status block can report
// backpressure instead of the subscription logging every park itself.
//
// Taking the Mutex is what makes `parked` readable: the parked goroutine is
// inside cond.Wait(), which releases the Mutex for the duration of the park.
// A drain holds the Mutex only for short bookkeeping sections, so this does
// not queue behind one.
func (s *bufferedMap) ParkStats() (int64, bool) {
	s.Lock()
	defer s.Unlock()
	return s.timesParked.Load(), s.parked
}

// LastDrainHitBudget satisfies DrainBudgetReporter: it reports whether the most
// recent Flush left work behind because it ran out of dispatch budget rather
// than because the work was not eligible to go yet.
//
// No lock, and deliberately no flushMu: the value is written once per Flush
// while flushMu is held, and the caller is the client's own Flush loop reading
// it immediately after that Flush returned. Taking flushMu here would queue the
// read behind the next drain — up to drainDispatchBudget — which is the opposite
// of what a loop deciding whether to drain again needs.
func (s *bufferedMap) LastDrainHitBudget() bool {
	return s.lastDrainHitBudget.Load()
}

// FlushShapes satisfies FlushShapeReporter with the drain width as it stands
// now and as it would stand with no AIMD penalty outstanding.
//
// No lock: flushConcurrency and batchSize are set once at construction and
// never written again, and concurrencyPenalty is atomic. The two effective
// figures are therefore read independently and could in principle straddle a
// penalty step taken between them, rendering a concurrency from before it and
// a batch size from after. That is accepted rather than locked out — flushMu is
// held for a whole drain, so taking it here would block the status block behind
// a 20-minute flush, which is precisely the situation an operator is reading
// the status block to diagnose.
func (s *bufferedMap) FlushShapes() (effective, configured FlushShape) {
	return FlushShape{
			Concurrency: s.effectiveFlushConcurrency(),
			BatchSize:   s.effectiveBatchSize(),
		}, FlushShape{
			Concurrency: s.configuredFlushConcurrency(),
			BatchSize:   s.configuredBatchSize(),
		}
}

// lengthLocked is Length without the Mutex, for callers that already hold it.
//
// flushingCount covers entries an in-flight Flush has swapped out but not yet
// applied — they are still pending changes, and callers like AllChangesFlushed
// must not see the buffer as empty while a drain is mid-air.
func (s *bufferedMap) lengthLocked() int {
	return len(s.changes) + len(s.queue) + s.flushingCount
}

// overSoftLimitLocked reports whether the buffer has reached either soft cap.
//
// Two caps because bytes and count measure different costs. Bytes bound the
// migrator's memory, which is what a few wide LONGTEXT rows threaten. Count
// bounds how long the resulting drain takes, which is set by applier round
// trips and is indifferent to row width — and a narrow-row table hits the
// second wall long before the first. On a production table averaging ~600
// bytes per buffered change, 256MiB of bytes is over 450k changes, and a drain
// of that size ran for 21m37s holding flushMu throughout.
func (s *bufferedMap) overSoftLimitLocked() bool {
	if s.softLimitBytes > 0 && s.sizeBytes >= s.softLimitBytes {
		return true
	}
	return s.softLimitChanges > 0 && s.lengthLocked() >= s.softLimitChanges
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

// drainDispatchBudget is a backstop on how long one drain spends dispatching
// batches. Batches not yet scheduled when it expires stay in the snapshot, are
// reattached, and go again on the next flush.
//
// The primary control on drain length is not here: it is
// DefaultSubscriptionSoftLimitChanges, which bounds how large the buffer — and
// therefore the snapshot a drain works through — is allowed to get in the first
// place. That is the right lever, because a drain that finishes is worth far
// more than a drain that is merely short. Only a complete drain reports
// allChangesFlushed=true, and only that advances the flushed position; a
// truncated one protects the checkpoint but does not move it. Capping *time*
// too aggressively would therefore reproduce the frozen checkpoint it is meant
// to prevent, just with a livelier status line. So this budget is set well
// above the time a full buffer is expected to take (~50k changes drained in
// roughly two minutes on the production table this was tuned against) and is
// expected never to fire in normal operation.
//
// It exists for the case the count cap cannot cover: per-row cost is not
// bounded. Wide rows, many secondary indexes, or a struggling target can make
// 50k changes take an hour, and flushMu is held for the whole drain, so
// everything else in the flush path queues behind it —
// SetWatermarkOptimization takes flushMu as its first act, and the runner calls
// it the moment the copy finishes. In production a 21m37s drain over ~452k
// changes left the post-copy phase blocked on that mutex for over half an hour
// after the copy had already completed. It also starves the AIMD controller,
// which is fed once per drain and needs cleanDrainsToRecover of them to give a
// halving back: at one sample per 20 minutes, "~90s of proven quiet" becomes an
// hour, so a width reduced by a single transient 1213 stays reduced for the
// rest of the migration.
//
// The budget does not apply to flushMapLocked, the serial path used under the
// cutover lock and by SetWatermarkOptimization. Those must drain completely.
//
// A var so tests can shorten it.
var drainDispatchBudget = 10 * DefaultFlushInterval

// minAdaptiveBatchSize floors the batch shrink. Below roughly this many rows
// the per-statement round trip dominates and the drain stops keeping up with a
// busy source, which trades one stall for another.
const minAdaptiveBatchSize = 50

// effectiveFlushConcurrency clamps flushConcurrency to at least 1 so
// the zero value (out-of-tree callers, bare test maps) stays serial, then
// applies any halvings the AIMD controller has accumulated.
func (s *bufferedMap) effectiveFlushConcurrency() int {
	return shiftDown(s.contendedFlushConcurrency(), s.loadPenalty.Load(), s.loadShedFloor())
}

// contendedFlushConcurrency is the width after the contention controller but
// before load shedding. It is the input to the load shift and the reference the
// batch size is re-paired against, so it exists separately rather than being
// inlined into both.
func (s *bufferedMap) contendedFlushConcurrency() int {
	return shiftDown(s.configuredFlushConcurrency(), s.concurrencyPenalty.Load(), 1)
}

// loadShedFloor is how narrow load shedding alone may make the drain:
// DefaultFlushConcurrency, the width every flush ran at before the width became
// instance-derived.
//
// The floor is the whole safety argument. Load shedding can give back the
// widening that autoscaling introduced and nothing more, so its worst case is
// the shape spirit shipped for years — it cannot invent a new starvation mode
// for a change feed that must keep advancing the binlog position. On an instance
// too small to have been widened the floor equals the configured width and the
// signal does nothing at all, which is right: that migration never acquired the
// problem.
//
// min() against the contention width keeps the two controllers from fighting.
// shiftDown's floor is a max(), so a floor above the value it was handed would
// *raise* it — a contention penalty that had already narrowed below 8 would be
// silently undone by a load signal asking for less concurrency, not more.
func (s *bufferedMap) loadShedFloor() int {
	return min(s.contendedFlushConcurrency(), DefaultFlushConcurrency)
}

// configuredFlushConcurrency is the width the drain would run at with no AIMD
// penalty outstanding: what autoscale.FlushBounds derived, or what the caller
// asked for. Clamped to at least 1 so the zero value (out-of-tree callers, bare
// test maps) stays serial.
func (s *bufferedMap) configuredFlushConcurrency() int {
	return max(1, s.flushConcurrency)
}

// configuredBatchSize is the batch the drain would render with no AIMD penalty
// outstanding. Zero means the caller never set one.
func (s *bufferedMap) configuredBatchSize() int {
	if s.batchSize <= 0 {
		return DefaultBatchSize // out-of-tree callers, bare test maps
	}
	return s.batchSize
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
//
// The starting point is batchSize, which the runner may have already lowered
// from DefaultBatchSize in exchange for a wider flushConcurrency (see
// autoscale.FlushBounds). Shrinking from a smaller start is not a problem: the
// two paths reduce different things — FlushBounds re-shapes a fixed number of
// rows in flight, the penalty here removes rows from flight.
func (s *bufferedMap) effectiveBatchSize() int {
	start := s.configuredBatchSize()
	// The floor can never exceed the start. minAdaptiveBatchSize floors
	// *shrinking*; a caller that deliberately configured fewer rows than that
	// has not asked to be overruled, least of all by the contention path,
	// where raising a statement's lock footprint is the opposite of the
	// intent. Before BatchSize was configurable the start was always
	// DefaultBatchSize, so the two could never cross.
	batch := shiftDown(start, s.concurrencyPenalty.Load(), min(minAdaptiveBatchSize, start))
	return s.repairedForLoad(batch)
}

// repairedForLoad widens a batch by however much load shedding narrowed the
// concurrency, so that the same number of rows stays in flight across fewer,
// larger statements.
//
// This is the entire reason shedding on load is affordable. autoscale.FlushBounds
// holds concurrency x batch constant at FlushRowsInFlight, so 32x250 and 8x1000
// move the same rows; what differs is how many statements — and therefore how
// many server threads and connections — it takes to move them. The load signal
// is a thread count, so narrowing the concurrency addresses the signal almost
// exactly, while re-pairing the batch keeps the drain's throughput. A feed that
// must not fall behind its retention window gets to shed load without shedding
// progress.
//
// The lock-footprint cost of the wider statement is real but bounded: the cap is
// DefaultBatchSize, the batch spirit used before any of this was derivable, so
// the widest statement load shedding can produce is one the pre-derivation code
// produced routinely.
//
// For a FlushBounds-derived pair that is the same thing as "the batch this drain
// would have used at DefaultFlushConcurrency" — concurrency x batch is
// FlushRowsInFlight, so at a width of 8 the batch is DefaultBatchSize either
// way. The cap is written as the constant rather than derived from the ratio
// because a caller that sets FlushConcurrency and BatchSize itself is under no
// obligation to sit on that budget, and re-pairing such a shape by its own
// implied ratio could produce a statement far wider than anything spirit has
// shipped.
//
// The max() is the other half of that bargain: a caller who already configured a
// batch above DefaultBatchSize keeps it. The cap is here to stop load shedding
// widening statements, not to shrink one the caller chose. It compares against
// the *configured* batch rather than the one passed in, because the one passed
// in has already been through the contention controller — capping against a
// halved batch would quietly retract the allowance the moment contention landed
// a single step.
//
// Past the cap — or on a floor-clamped shed — the re-pairing stops and rows
// genuinely leave flight, which is the correct behaviour once there is no cheap
// trade left to make.
func (s *bufferedMap) repairedForLoad(batch int) int {
	before, after := s.contendedFlushConcurrency(), s.effectiveFlushConcurrency()
	if after <= 0 || before <= after {
		return batch // no load shed outstanding, or it was clamped to nothing
	}
	// Scale first, divide last. The ratio is not a whole number at most widths
	// FlushBounds derives — it is WriteStart(vCPUs), not a power of two, so a
	// 16-vCPU instance sheds 14 -> 8 and (before/after) would truncate to 1,
	// returning the batch untouched and dropping 43% of the rows in flight on
	// the one path that must not fall behind its retention window.
	//
	// Flooring the product keeps what the truncated ratio was there to protect:
	// the result is never more rows in flight than the drain had before it
	// started shedding, since after*((batch*before)/after) <= batch*before.
	// Rounding down leaves a few rows behind, which is the safe direction for a
	// controller whose whole job is to take load off.
	return min((batch*before)/after, max(s.configuredBatchSize(), DefaultBatchSize))
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

// adaptFlushLoad feeds the server's load signal into the concurrency limit, the
// same AIMD shape adaptFlushConcurrency uses for lock contention but on the
// other signal and against a much higher floor.
//
// It exists because the flush was the one write path that could not see load.
// The drain is deliberately never throttled — the binlog position has to keep
// advancing — and the original reasoning was that the throttler would slow the
// *copier* on the flush's behalf. That works only while the flush is a small,
// fixed share of the load. Once its width became instance-derived the two write
// paths were sized independently from the same vCPU count, and under sustained
// load the copier would shed to a handful of workers while the flush held full
// width: the side that could yield did all the yielding, and the side that could
// not became the dominant load. The total barely moved, and the copy — the only
// thing whose completion would have ended the load — was the part that starved.
//
// So the drain sheds too, but differently from the copier: narrower, never
// paused, floored at DefaultFlushConcurrency (see loadShedFloor), and with the
// batch re-paired so rows in flight are preserved (see repairedForLoad). It gives
// up statements, not progress.
//
// Sampled at the top of a drain rather than the bottom, unlike the contention
// controller. Contention is an outcome — it can only be known once the drain has
// run — while load is a condition, and a drain that may hold flushMu for minutes
// should be shaped by what the server looks like when it starts, not by what it
// looked like before the previous one.
//
// The responsiveness bound follows from that, and is worth stating plainly: one
// step per drain, so the controller moves at drain frequency and no faster. Under
// the load this was written for that has been observed in the tens of seconds, so
// a drain that starts clean runs at full width through a spike that arrives just
// after it begins, and a shed takes a drain or two to reach the floor. That is
// the deliberate trade — the alternative is re-reading the signal mid-drain and
// resizing an errgroup whose limit its own workers are already holding — but it
// means this controller damps sustained load rather than catching transients.
func (s *bufferedMap) adaptFlushLoad() {
	if s.underLoad == nil {
		return // no signal wired: the width is whatever the caller configured
	}
	if s.underLoad() {
		s.cleanLoadDrains.Store(0)
		// Once the floor is reached there is nothing left to give, and further
		// steps would only make recovery take proportionally longer when the
		// load clears. Same guard as the contention path, for the same reason.
		if s.effectiveFlushConcurrency() <= s.loadShedFloor() {
			return
		}
		penalty := s.loadPenalty.Add(1)
		s.logger.Warn("narrowing flush concurrency under server load",
			"table", s.table.SchemaName+"."+s.table.TableName,
			"concurrency", s.effectiveFlushConcurrency(),
			"batch_size", s.effectiveBatchSize(),
			"floor", s.loadShedFloor(),
			"penalty", penalty,
		)
		return
	}
	if s.loadPenalty.Load() <= 0 {
		return // already at the contention controller's width
	}
	if s.cleanLoadDrains.Add(1) < cleanDrainsToRecover {
		return
	}
	s.cleanLoadDrains.Store(0)
	s.loadPenalty.Add(-1)
	s.logger.Info("restoring flush concurrency after load cleared",
		"table", s.table.SchemaName+"."+s.table.TableName,
		"concurrency", s.effectiveFlushConcurrency(),
		"batch_size", s.effectiveBatchSize(),
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

	// Soft backpressure: park while the buffer is at or above either soft
	// limit. See overSoftLimitLocked for the semantics.
	//
	// Both log lines are Debug. Parking stalls the binlog reader, so it does
	// need to be visible — but flushes release capacity per applied batch, so
	// sustained backpressure produces a park/unpark pair every few seconds for
	// the lifetime of a long drain, and at Warn/Info that buried the periodic
	// status block it was meant to complement. The status block now carries
	// `parks=` and `is-parked=` on its binlog row instead, which answers the
	// same question — is the reader being held back, and how often — at the
	// cadence an operator actually reads.
	if !dedupOverwrite && !s.closed && s.overSoftLimitLocked() {
		s.timesParked.Add(1)
		s.requestFlush()
		s.logger.Debug("subscription parked on soft limit",
			"table", s.table.SchemaName+"."+s.table.TableName,
			"size_bytes", s.sizeBytes,
			"soft_limit_bytes", s.softLimitBytes,
			"changes", s.lengthLocked(),
			"soft_limit_changes", s.softLimitChanges,
		)
		parkStart := time.Now()
		s.parked = true
		for !s.closed && s.overSoftLimitLocked() {
			s.cond.Wait()
		}
		s.parked = false
		s.logger.Debug("subscription unparked from soft limit",
			"table", s.table.SchemaName+"."+s.table.TableName,
			"parked_duration", time.Since(parkStart).String(),
			"size_bytes", s.sizeBytes,
			"changes", s.lengthLocked(),
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

	// Publish why this drain fell short, however it returns. The clients' Flush
	// loops read it to tell a drain that merely ran out of budget — where a
	// fresh drain lands the batches this one never attempted — from one whose
	// leftovers are not eligible yet, where an immediate retry would defer the
	// same work again. See backlogWorthDraining. It stays false on the underLock
	// path, which is serial, has no budget, and must drain completely.
	hitBudget := false
	defer func() { s.lastDrainHitBudget.Store(hitBudget) }()

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
		mapAllFlushed, mapHitBudget, err := s.drainMapSnapshot(ctx, snapshot, applyWatermarkFilter)
		// Recorded before the error check: a drain can spend its budget and then
		// fail, and the flag describes what the drain left behind rather than how
		// it ended.
		hitBudget = mapHitBudget
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
		if len(remainder) > 0 {
			// A remainder with no error means the drain gave up its dispatch
			// budget. Reporting success here would publish a position covering
			// entries that were only reattached, so it has to hold the position
			// back — the same contract the map drain's truncation uses.
			allChangesFlushed = false
			hitBudget = true
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
//
// The first result is false when any entry was deferred, for any reason. The
// second narrows that to the one reason an immediate re-drain gets past —
// batches the dispatch budget never let it start — which is what the clients'
// Flush loops need to tell "come back later" from "come back now"; see
// backlogWorthDraining.
func (s *bufferedMap) drainMapSnapshot(ctx context.Context, snapshot map[string]bufferedChange, applyWatermarkFilter bool) (bool, bool, error) {
	allChangesFlushed := true
	hitDispatchBudget := false

	// Shape this drain to the load the server is under right now, before either
	// width is read below. The contention controller runs at the other end, on
	// the outcome; see adaptFlushLoad for why this one cannot wait for that.
	s.adaptFlushLoad()

	// Map iteration order is randomized, so unpartitioned batch membership
	// differs between flushes. That does not affect the retry below: pass 2
	// re-applies the very same mapFlushBatch values pass 1 built, so a contended
	// batch retries as itself regardless of how it was assembled.
	//
	// An earlier revision sorted by *primary key* here, on the theory that a
	// consistent lock-acquisition order would help. It would not, and the reason
	// is worth keeping because it is what points at the sort below: the observed
	// deadlock cycle inverts between the clustered index and a secondary UNIQUE
	// index, and secondary key order is unrelated to primary key order, so
	// PK-sorted batches still interleave there. The clustered index was never
	// the conflict surface to begin with — a REPLACE's conflict there is with an
	// exact PK, so under READ COMMITTED it takes a record lock and no gap, and
	// PK neighbours cannot collide at all. Sorting by a *unique secondary* index
	// is the version of that idea which addresses the surface that does collide.
	// See block/spirit#1168 and subscription_buffered_partition.go.
	rows := make([]drainRow, 0, len(snapshot))
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
		rows = append(rows, drainRow{key: key, change: change})
	}

	// Pick the unique secondary index whose values are most clustered across
	// this drain's own rows, and sort by it. Sorting is what turns "batches are
	// disjoint by key" into "batches are disjoint by *index range*": rows that
	// sit close together in that index end up in the same statement, where they
	// cannot conflict, instead of being scattered across concurrent ones by map
	// iteration order. nil means there is nothing worth partitioning by — no
	// usable unique secondary index, or a drain with no rows — in which case
	// batching stays exactly as it was.
	chosen := choosePartitionIndex(s.partitionIndexes(ctx), rows)
	if chosen != nil {
		sortRowsByIndex(rows, chosen)
	}
	batches := s.buildBatches(rows, chosen)

	if len(batches) == 0 {
		// Every key was watermark-deferred. The drain produced no evidence about
		// contention either way, so it must not advance the clean-drain streak —
		// otherwise a run of all-deferred flushes would widen the concurrency
		// back out on the strength of having applied nothing at all.
		return allChangesFlushed, false, nil
	}

	// Pass 1: apply concurrently at the current (possibly reduced) width.
	// Batches that lose to lock contention are collected rather than failing
	// the drain — see retryContendedBatches for why that is safe and why the
	// retry has to be serial.
	//
	// When the drain is partitioned, the batches are contiguous ranges of the
	// chosen index and go out in two stripes — evens, then odds — so that no
	// two batches in flight together are neighbours in that index. Handing the
	// sorted list straight to the limiter would undo most of the benefit: the
	// in-flight window is roughly contiguous, so neighbours would run together
	// and every batch boundary would become a candidate collision. See
	// stripeBatches.
	//
	// The dispatch budget spans the whole drain, not each stripe. It is
	// computed here and passed down for that reason — two stripes each granted
	// a full drainDispatchBudget would silently double how long one flush may
	// hold flushMu.
	stripes := [][]*mapFlushBatch{batches}
	if chosen != nil {
		stripes = stripeBatches(batches)
	}
	deadline := time.Now().Add(drainDispatchBudget)
	var (
		contended []*mapFlushBatch
		complete  = true
		err       error
	)
	for _, stripe := range stripes {
		var (
			stripeContended []*mapFlushBatch
			stripeComplete  bool
		)
		stripeContended, stripeComplete, err = s.applyBatchesConcurrent(ctx, snapshot, stripe, deadline)
		contended = append(contended, stripeContended...)
		if err != nil {
			break
		}
		if !stripeComplete {
			// The budget is spent. Do not start the next stripe: its batches are
			// still in the snapshot and will be reattached, which is strictly
			// better than beginning work the budget does not cover.
			complete = false
			break
		}
	}
	if !complete {
		// The dispatch budget expired. Everything unscheduled is still in the
		// snapshot and will be reattached; report the drain as incomplete so no
		// client publishes a position that covers it, and report *why* so the
		// client's Flush loop re-drains rather than waiting — those batches were
		// never attempted, and a fresh drain gets a fresh budget for them.
		allChangesFlushed = false
		hitDispatchBudget = true
		s.logger.Warn("flush drain exceeded its dispatch budget; deferring the remainder",
			"table", s.table.SchemaName+"."+s.table.TableName,
			"budget", drainDispatchBudget.String(),
			"total_batches", len(batches),
		)
	}
	if err != nil {
		// Deliberately no adaptFlushConcurrency call here. A drain that failed
		// on something other than contention is not evidence of contention, and
		// it is not evidence of quiet either: feeding it in as "clean" would let
		// three consecutive hard failures — during which nothing flushed —
		// restore the full width, and feeding it in as "contended" would
		// penalise the width for an unrelated non-retryable error that merely
		// happened to land in the same drain as someone else's 1213.
		return false, hitDispatchBudget, err
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
		deferred, err := s.retryContendedBatches(ctx, snapshot, contended)
		if err != nil {
			// A hard error or a real cancellation, not contention: same
			// reasoning as the pass-1 error path above, so the controller is
			// left alone.
			//
			// The count still lands in the counter. Batches the pass had already
			// given up on before the error arrived are reattached and retried
			// like any other deferral, so leaving them out understated the
			// counter for no benefit. It is only an accounting figure — the
			// error is what decides the drain.
			s.batchesDeferred.Add(int64(deferred))
			return false, hitDispatchBudget, err
		}
		if deferred > 0 {
			// Contention that outlived the serial pass is deferred, not failed.
			//
			// Reporting it as an error would throw away the whole drain, and a
			// drain is not a cheap thing to throw away: with the copier taking
			// the target's write capacity, one pass over a full buffer has been
			// observed to run for minutes, so a single batch contending at the
			// end would discard hundreds of batches' worth of *recorded*
			// progress. The rows they
			// wrote stay written either way, but on the error path flushedGTID
			// never advances and the flush is never recorded, which is the
			// frozen-checkpoint symptom this whole path exists to prevent —
			// re-entered from the other side.
			//
			// allChangesFlushed=false protects the checkpoint exactly as well
			// as an error does: clients gate position advancement on it, so a
			// deferred batch holds the position back just as a failed drain
			// would. It is the same mechanism watermark-deferred keys already
			// use. The difference is only that the batches which *did* land
			// count, and the leftovers are retried on the next flush rather
			// than after a whole failed drain's worth of lost ground.
			s.batchesDeferred.Add(int64(deferred))
			s.logger.Warn("flush batches still contended; deferring to the next flush",
				"table", s.table.SchemaName+"."+s.table.TableName,
				"deferred_batches", deferred,
				"contended_batches", len(contended),
				"total_batches", len(batches),
			)
			s.adaptFlushConcurrency(true)
			// Contended batches were attempted and lost, so they are not a reason
			// to re-drain at once; hitDispatchBudget still travels, because a drain
			// can spend its budget and contend in the same pass.
			return false, hitDispatchBudget, nil
		}
		s.serialRecoveries.Add(1)
	}
	s.adaptFlushConcurrency(len(contended) > 0)
	return allChangesFlushed, hitDispatchBudget, nil
}

// buildBatches turns the drain's rows into applier round trips.
//
// When idx is non-nil the rows arrive sorted by that index and each batch is a
// contiguous range of it, with cut points nudged to fall where the leading key
// value changes (cutAtValueBoundary) so a run of rows sharing a leading value —
// physically adjacent records, the guaranteed-collision case — is not split
// across two batches.
//
// Both caps from before still apply and in the same order of precedence: the
// (possibly AIMD-reduced) batch size, and the estimated rendered statement
// size. The byte cap is not negotiable for correctness the way the row cap is —
// buffered wide rows (LONGTEXT / BLOB) can render into a single REPLACE larger
// than max_allowed_packet, which is a deterministic, non-retryable failure — so
// it overrides the boundary alignment and cuts early. A single row over the
// budget still flushes alone in its own batch, since a row cannot be split.
func (s *bufferedMap) buildBatches(rows []drainRow, idx *partitionIndex) []*mapFlushBatch {
	batchSize := s.effectiveBatchSize()
	var batches []*mapFlushBatch
	for i := 0; i < len(rows); {
		// Every batch must consume at least one row. The loop's only progress
		// is i = j at the bottom, so a batch that ended where it started would
		// spin here forever, allocating an empty batch per turn until the
		// process died.
		//
		// It cannot happen today: effectiveBatchSize floors at 1, and every
		// return in cutAtValueBoundary is past its start. The clamp is here
		// because that invariant lives in two other functions — one of them in
		// another file — and neither says a caller's loop termination depends
		// on it. One comparison per batch against a hung migration is worth
		// making even at probability zero.
		//
		// Clamped *before* the cut rather than after, because
		// cutAtValueBoundary reads rows[hardEnd-1] and so needs the same
		// guarantee its caller does; a guard placed after the call would only
		// have replaced the hang with a panic.
		end := max(min(i+batchSize, len(rows)), i+1)
		if idx != nil {
			end = cutAtValueBoundary(rows, idx, i, end)
		}
		batch := &mapFlushBatch{}
		var batchStmtBytes int64
		j := i
		for ; j < end; j++ {
			r := rows[j]
			rowBytes := renderedBytesOfChange(r.change.logicalRow, r.change.originalKey)
			if len(batch.keys) > 0 && batchStmtBytes+rowBytes > applier.MaxStatementSizeBytes {
				break
			}
			batch.keys = append(batch.keys, r.key)
			batch.storedBytes += sizeOfBufferedChange(r.key, r.change)
			if r.change.logicalRow.IsDeleted {
				batch.deleteKeys = append(batch.deleteKeys, r.change.originalKey)
			} else {
				batch.upsertRows = append(batch.upsertRows, r.change.logicalRow)
			}
			batchStmtBytes += rowBytes
		}
		batches = append(batches, batch)
		i = j
	}
	return batches
}

// applyBatchesConcurrent runs batches through the applier with up to
// effectiveFlushConcurrency in flight, returning the batches that failed on
// InnoDB lock contention (1205/1213) for the caller to retry serially.
//
// deadline bounds *dispatch* and is supplied by the caller rather than computed
// here, because a partitioned drain calls this once per stripe and the budget
// belongs to the drain.
//
// Contention does *not* cancel the group. Every other error class does, exactly
// as before: those are not self-inflicted and retrying at a narrower width
// would not help. Contention is different — spirit's own concurrent REPLACEs
// are both sides of the conflict, so the same work succeeds unconditionally
// once it stops racing itself. Letting one contended batch cancel its siblings
// (the previous behaviour) threw away batches that were about to land and made
// the whole drain fail, which is what pinned the buffer at its soft limit and
// froze the flushed position.
func (s *bufferedMap) applyBatchesConcurrent(ctx context.Context, snapshot map[string]bufferedChange, batches []*mapFlushBatch, deadline time.Time) ([]*mapFlushBatch, bool, error) {
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(s.effectiveFlushConcurrency())
	// Workers delete their batch's entries as they land, so they only
	// contend with each other here: the build loop finished iterating the
	// snapshot before the first worker starts, and the deferred reattach
	// runs after Wait.
	var mu sync.Mutex
	var contended []*mapFlushBatch
	var skippedBatches bool
	// Stop starting new batches once the budget is spent; batches already in
	// flight are waited out below. Unlike the cancellation above this is not an
	// error — the unstarted batches stay in the snapshot, are reattached, and go
	// again on the next flush — and the caller reports the drain as incomplete
	// so no position advances past them.
	//
	// Checked in two places, because the loop's own check goes stale. g.Go
	// blocks once effectiveFlushConcurrency batches are running, so a check that
	// passes can be followed by an arbitrarily long wait for a slot: at
	// concurrency 1 with hour-long batches, the loop tests the deadline at t≈0,
	// blocks in g.Go for an hour, and then launches a batch the budget no longer
	// covers. The re-check inside the worker is what actually enforces "no new
	// batch *starts* after the deadline"; the loop check is just a cheap early
	// exit. Overrun is therefore one in-flight batch's latency, which is the
	// least that can be promised without abandoning work already under way.
	var budgetSpent atomic.Bool
	for _, batch := range batches {
		if gctx.Err() != nil {
			skippedBatches = true
			break // a batch failed or ctx was canceled; don't queue the rest
		}
		if budgetSpent.Load() || time.Now().After(deadline) {
			budgetSpent.Store(true)
			break
		}
		g.Go(func() error {
			if time.Now().After(deadline) {
				// The slot opened after the budget expired. Leave this batch's
				// entries in the snapshot for the next flush rather than
				// starting work the budget does not cover.
				budgetSpent.Store(true)
				return nil
			}
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
	if budgetSpent.Load() {
		s.drainsTimedOut.Add(1)
	}
	return contended, !budgetSpent.Load(), err
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
// Returns how many batches still could not land. Deferring them is not
// something this function does to them, it is something it declines to do: only
// releaseAppliedBatch takes a batch's keys out of the snapshot, so a batch that
// never lands is still there when drainMapSnapshot's deferred reattach runs.
// The count is for the caller's accounting and logging.
//
// A non-nil error means something other than contention went wrong — a hard
// error, or a genuine cancellation of the parent context.
func (s *bufferedMap) retryContendedBatches(ctx context.Context, snapshot map[string]bufferedChange, contended []*mapFlushBatch) (int, error) {
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
	//
	// The budget is a plain deadline rather than a context, and this is the
	// important part. An earlier revision derived a context.WithTimeout from ctx
	// and handed *that* to flushBatch, which meant the budget did not merely
	// stop the pass — it killed whatever REPLACE was running when it expired.
	// Production showed the result as
	//
	//	failed to upsert rows: failed to execute upsert: context deadline exceeded
	//
	// which reads like a statement timeout and is nothing of the sort. On a
	// table whose batches take longer than the budget, that fires on a
	// perfectly healthy statement, every time. Checking the error's provenance
	// before classifying it stopped the misdiagnosis but not the abort.
	//
	// So the budget now gates only the decision to *start* another attempt.
	// Attempts run on ctx and are allowed to finish, which is safe because an
	// attempt is already bounded from below the flush: RetryableTransaction
	// makes at most MaxRetries tries, each capped by innodb_lock_wait_timeout.
	// Overrun past the budget is therefore one attempt's worth, and no deadline
	// of our own can ever surface as an apply failure. Same shape as
	// drainDispatchBudget, which likewise stops scheduling rather than
	// cancelling.
	deadline := time.Now().Add(contentionRetryBudget)

	var mu sync.Mutex
	deferred := 0
	// giveUpAt reports the total deferral count when the budget expires while
	// batch i is the one in hand: the batches already counted, plus batch i and
	// every batch after it, none of which has landed. Named and shared rather
	// than written out at each check — the two checks below are the same
	// decision made at different moments, and an accumulator dropped from one of
	// them would understate how much of the drain is stuck without breaking
	// anything a test would notice.
	giveUpAt := func(i int) int { return deferred + len(contended) - i }
	for i, batch := range contended {
		var err error
		for attempt := range contentionRetries {
			if time.Now().After(deadline) {
				// Our own impatience, not a failure: the batches not reached
				// yet — this one included — go back in the buffer.
				return giveUpAt(i), nil
			}
			select {
			case <-ctx.Done():
				// A real shutdown. That *is* an error, and the caller must not
				// treat the drain as having merely deferred work.
				return 0, ctx.Err()
			case <-time.After(contentionBackoff(attempt)):
			}
			// Re-checked after the wait, and this is the check that enforces the
			// contract. The backoff escalates to a couple of seconds, so the
			// check above can pass with a millisecond left and the attempt would
			// then start well past the deadline. The pre-wait check is only a
			// cheap early exit that avoids sleeping for a budget already gone.
			// Same division of labour as drainDispatchBudget's loop check versus
			// its worker check.
			if time.Now().After(deadline) {
				return giveUpAt(i), nil
			}
			if err = s.flushBatch(ctx, batch.deleteKeys, batch.upsertRows, nil); err == nil {
				s.releaseAppliedBatch(snapshot, &mu, batch)
				break
			}
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
			if !dbconn.IsLockContentionError(err) {
				// A hard error fails the drain, but the batches this pass had
				// already given up on are still deferred — they are back in the
				// buffer either way. Reporting them keeps the deferral counter
				// honest; the caller decides what to do with a count that
				// arrives alongside an error.
				return deferred, err
			}
		}
		if err != nil {
			// Still contended after every attempt. Defer this one but keep
			// going: the batches are disjoint, so one stubborn lock holder
			// says nothing about whether the next batch can land.
			deferred++
		}
	}
	return deferred, nil
}

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
// flush. It bounds when a new attempt may *start*; see retryContendedBatches
// for why it is not a context, and why that distinction matters more than the
// number.
//
// The number still has to be in scale with one attempt, or the budget expires
// before the first batch has had a fair try and pass 2 becomes decorative. It
// was 20s, which was too small on the table this was tuned against: a drain
// there worked through at most 452,571 rows in 21m37s, so with batches of
// DefaultBatchSize and the reduced concurrency in force at the time, average
// per-batch latency was *at least* ~11s — and a single flushBatch can spend
// several times that inside RetryableTransaction, which retries up to
// MaxRetries with innodb_lock_wait_timeout on each.
//
// Two minutes gives a handful of batches a genuine serial retry while staying
// well inside drainDispatchBudget, so the drain's own bound is still the outer
// one. Expiring remains cheap — the leftovers are deferred, not failed — so
// this is a "how much is worth trying before handing back" figure, not a
// deadline anything depends on.
//
// A var so tests can shorten it, as with contentionBackoff.
var contentionRetryBudget = 2 * time.Minute

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

	// Same dispatch backstop the map drain applies, for the same reason: this
	// runs with flushMu held, so an unbounded queue drain blocks every
	// subsequent flush and the post-copy phase behind it. Queue mode is where
	// non-memory-comparable PKs spend the post-copy phase, so leaving it
	// unbounded would exempt exactly those tables.
	//
	// A segment boundary is the only place it is safe to stop. The queue is
	// ordered and drained FIFO, so an applied prefix is a valid amount of work
	// and reattachLocked prepends the remainder ahead of anything that arrived
	// during the drain, preserving binlog order. Stopping mid-segment would
	// split a batch that has already been partly built.
	deadline := time.Now().Add(drainDispatchBudget)

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
			if time.Now().After(deadline) {
				// Not an error: the remainder is handed back for the next
				// flush, and Flush reports the drain as incomplete so no
				// position advances past it.
				s.drainsTimedOut.Add(1)
				return snapshot[applied:], nil
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
		"times_parked_on_soft_limit_total", s.timesParked.Load(),
		"batches_lock_contended", s.batchesContended.Swap(0),
		"batches_contention_deferred", s.batchesDeferred.Swap(0),
		"drains_rescued_serially", s.serialRecoveries.Swap(0),
		"drains_truncated_by_time_budget", s.drainsTimedOut.Swap(0),
		"flush_concurrency", s.effectiveFlushConcurrency(),
		"delta_len", len(s.changes)+len(s.queue),
		"size_bytes", s.sizeBytes,
	)
	return nil
}
