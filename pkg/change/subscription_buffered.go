package change

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
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
// Spirit's correctness relies on the bufferedMap being an up-to-date
// and *disjoint* representation of pending changes — every PK appears
// at most once at flush time, holding the latest row image — so every
// transiently-deleted row is guaranteed to have its own event in the
// buffer (or arriving shortly). The destination converges to source's
// current state once the last unflushed event for each affected PK
// has been applied; the post-cutover checksum (with
// FixDifferences=true) catches any divergence that slips through.
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
	// softLimitBytes. Broadcast at the end of every flush path. L =
	// &Mutex. Construction invariant: every call site must wire this
	// up immediately after the struct literal, e.g.
	//   sub := &bufferedMap{...}
	//   sub.cond = sync.NewCond(&sub.Mutex)
	// HasChanged / Flush / SetWatermarkOptimization will panic on a
	// nil cond, so a missing init shows up loudly in tests.
	cond *sync.Cond

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

	// sizeBytes is an approximate count of memory currently held by
	// changes + queue. Maintained by HasChanged and the flush paths;
	// see estimateRowSize for the accounting.
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

	// Counters for the bookend log emitted on watermark-optimization transitions.
	keysAdded        atomic.Int64
	keysDroppedAbove atomic.Int64
	keysSkippedBelow atomic.Int64
	timesParked      atomic.Int64 // HasChanged was parked at least once on the soft limit

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
// twice their in-memory length; scalars render as short literals. Mirrors
// the copy path's estimateRowSize (pkg/applier): deliberately approximate,
// because the applier.MaxStatementSizeBytes budget it feeds is conservative
// against the typical 64 MiB max_allowed_packet.
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

	return len(s.changes) + len(s.queue)
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
	// it. Every current caller runs a repairing checksum before the data is
	// trusted, which is what makes this safe end-to-end. Deterministic
	// repro: TestKeyAboveWatermarkVisibilityWindow. Analysis + fix
	// directions: docs/key-above-watermark-visibility.md.
	if s.watermarkOptimizationEnabled() && s.chunker.KeyAboveHighWatermark(key[0]) {
		s.keysDroppedAbove.Add(1)
		s.logger.Debug("key above watermark", "key", key[0])
		return
	}

	// Soft backpressure: park while the buffer is at or above the byte
	// threshold. See softLimitBytes on bufferedMap for the semantics.
	// We log on entry and exit because parking stalls the binlog reader
	// — the exit duration is the operator's main signal for binlog-
	// retention risk, and without these lines a stalled migrator looks
	// indistinguishable from one that's just slow.
	if s.softLimitBytes > 0 && s.sizeBytes >= s.softLimitBytes && !s.closed {
		s.timesParked.Add(1)
		s.logger.Warn("subscription parked on soft memory limit",
			"table", s.table.SchemaName+"."+s.table.TableName,
			"size_bytes", s.sizeBytes,
			"soft_limit_bytes", s.softLimitBytes,
		)
		parkStart := time.Now()
		for s.sizeBytes >= s.softLimitBytes && !s.closed {
			s.cond.Wait()
		}
		s.logger.Info("subscription unparked from soft memory limit",
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

	hashedKey := utils.HashKey(key)

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

// Flush writes the pending changes to the new table.
// We do this under a mutex, which means that unfortunately pending changes
// are blocked from being collected while we do this. In future we may
// come up with a more sophisticated approach to allow concurrent
// collection of changes while we flush.
//
// SetWatermarkOptimization drains the outgoing store inline before
// flipping the mode flag, so under normal operation only one of
// map/queue has entries when Flush runs. Both branches are still
// iterated defensively in case anything ever leaves the inactive store
// non-empty.
func (s *bufferedMap) Flush(ctx context.Context, underLock bool, locks []*dbconn.TableLock) (allChangesFlushed bool, err error) {
	s.Lock()
	defer s.Unlock()

	allChangesFlushed = true

	if len(s.changes) > 0 {
		mapAllFlushed, err := s.flushMapLocked(ctx, underLock, locks, false)
		if err != nil {
			return false, err
		}
		if !mapAllFlushed {
			allChangesFlushed = false
		}
	}

	if len(s.queue) > 0 {
		if err := s.flushQueueLocked(ctx, underLock, locks); err != nil {
			return false, err
		}
	}

	return allChangesFlushed, nil
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
		// In bufferedMap, the low-watermark check defers flushing keys that
		// are still being copied (KeyBelowLowWatermark returns false). It is
		// only safe to skip when we are not under cutover lock and the caller
		// has not asked us to drain everything (bypassWatermark).
		if applyWatermarkFilter && !s.chunker.KeyBelowLowWatermark(change.originalKey[0]) {
			s.keysSkippedBelow.Add(1)
			s.logger.Debug("key not below watermark", "key", change.originalKey[0])
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
func (s *bufferedMap) SetWatermarkOptimization(ctx context.Context, enabled bool) error {
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
		"delta_len", len(s.changes)+len(s.queue),
		"size_bytes", s.sizeBytes,
	)
	return nil
}
