package change

import (
	"context"
	"errors"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
)

// ErrPositionNotFound is returned by StartFromPosition when the
// underlying source can no longer resume from the requested opaque
// position — most commonly because the binlog file has been purged on
// a MySQL source. Wrapped with %w so callers can errors.Is against it.
var ErrPositionNotFound = errors.New("change.Source: cannot resume from position; it is no longer available on the source")

// Source is the abstraction spirit uses to consume a stream of row
// changes from a source database. It exists so spirit's replication
// pipeline is not pinned to the MySQL binlog protocol — alternative
// implementations (e.g. Vitess VStream) can plug in without touching the
// applier, the bufferedMap, or any other spirit-side machinery.
//
// The built-in implementation that uses go-mysql's BinlogSyncer lives in
// this package and backs the existing Client. Out-of-tree implementations
// construct their own Source value and pass it to spirit via the
// Move/Migration config.
//
// Lifecycle: construct → AddSubscription(...)* → Start(ctx) OR
// StartFromPosition(ctx, pos) → Flush / BlockWait /
// FlushUnderTableLock as needed → Stop() → Close().
//
// Events flow PUSH-style: when a row event matching one of the
// subscribed tables arrives, the source implementation looks up the
// Subscription whose Tables() includes that (schema, table) and calls
// sub.HasChanged(key, row, deleted) directly. There is no Next() / Recv()
// loop on this interface — the caller registers subscriptions and lets
// the source drive them.
//
// The surface area is intentionally broad to match the existing
// binlog-backed implementation so all spirit consumers (pkg/migration,
// pkg/move, pkg/checksum) program against the interface. Resume-time
// positions are opaque strings (Position / StartFromPosition) so that
// alternative implementations can encode whatever they need
// (file+offset, GTID, VStream position, etc.) without leaking to
// callers.
type Source interface {
	// AddSubscription constructs a bufferedMap from (currentTable,
	// newTable, chunker) and registers it. ROW events matching the
	// registered (schema, table) pair are pushed to the subscription's
	// HasChanged. Must be called before Start / StartFromPosition.
	AddSubscription(currentTable, newTable *table.TableInfo, chunker table.MappedChunker) error

	// Start begins streaming from the current source head and spawns the
	// reader goroutine. Returns once the reader is running; the stream
	// itself continues until Close is called or ctx is cancelled.
	// Implementations perform any required validation (privileges,
	// connectivity, server settings) before returning.
	Start(ctx context.Context) error

	// StartFromPosition is the resume-time entry point. It primes the
	// source's internal position to the opaque string previously
	// returned by Position(), then begins streaming as if Start had
	// been called. Implementations validate the position is still
	// resumable (e.g. MySQL: the binlog file has not been purged); an
	// unresumable position is returned wrapped with ErrPositionNotFound.
	StartFromPosition(ctx context.Context, pos string) error

	// Position returns the latest safe-to-resume position as an opaque
	// string. The implementation owns the encoding; spirit never parses
	// it. Advances only at transaction commit boundaries. Returns "" if
	// no position has been observed yet, signaling that a fresh Start is
	// required.
	//
	// Position reports this *running* feed's in-memory progress and does no
	// server I/O — contrast CurrentPosition, which reads the live server head.
	Position() string

	// CurrentPosition queries the source server for its current head position
	// and returns it in the same opaque encoding as Position (so the result is
	// a valid StartFromPosition input for this implementation).
	//
	// It is mechanically different from Position:
	//   - Position returns in-memory state: the safe-to-resume point a *running*
	//     feed has flushed, advancing only at commit boundaries and "" before
	//     the feed has observed anything. No server round-trip.
	//   - CurrentPosition issues a live query and needs no running feed. In
	//     binlog mode it FLUSHes and reads SHOW [BINARY LOG|MASTER] STATUS
	//     (file:offset); in GTID mode it reads @@GLOBAL.gtid_executed (a GTID
	//     set). Because the encoding is per-implementation, this is why the
	//     capture belongs on Source rather than a binlog-only helper.
	//
	// Its purpose is to capture a "start here, as of now" point to hand to a
	// later StartFromPosition — e.g. seeding a reverse feed at cutover, before
	// that feed has been started.
	CurrentPosition(ctx context.Context) (string, error)

	// Flush requests that all registered subscriptions flush their
	// buffered changes to their targets. Blocks until the flush
	// completes or ctx cancels.
	Flush(ctx context.Context) error

	// FlushUnderTableLock is the cutover-time variant of Flush: the
	// caller holds table locks and we drain the in-flight backlog
	// against that quiescent state. locks carries one lock per target
	// server being written to (a single lock for single-target
	// migrations; one per shard for sharded moves) — the applier
	// executes each target's statements under that target's own lock,
	// since LOCK TABLES blocks writes from every other connection.
	FlushUnderTableLock(ctx context.Context, locks []*dbconn.TableLock) error

	// BlockWait blocks until all events received from the underlying
	// stream up to call-time have been delivered to their subscriptions.
	// Used by the runner around cutover to drain the in-flight backlog.
	// Returns when drained or ctx cancels.
	BlockWait(ctx context.Context) error

	// GetDeltaLen returns the total number of pending changes across
	// all registered subscriptions. Used by callers to decide whether
	// the backlog is small enough to consider cutover.
	GetDeltaLen() int

	// FlushResidual reports what the most recently completed flush left
	// behind: residual is the pending-change count observed immediately
	// after that flush, and flushes is a monotonic count of completed
	// flushes. Both are 0 before the first flush completes.
	//
	// This is the quantity that says whether the feed is keeping up, and it
	// has to be sampled here rather than polled by the caller. GetDeltaLen
	// is a sawtooth: it climbs on every sample between flushes and drops
	// when one lands, so a poller observes the residual plus however many
	// writes arrived since the flush. That second term is large enough on a
	// busy table to swamp the residual itself, and it does not average out
	// — a polling ticker and the flush ticker hold a fixed phase
	// relationship whenever their intervals are commensurate, which at the
	// defaults (30s flush) they are for any poll interval that divides it.
	//
	// A caller watching for a feed that is losing ground should compare
	// residuals only across distinct flushes, which is what flushes is for.
	// A residual that stays near zero means the feed is keeping up however
	// heavy the write load; one that climbs flush over flush means work is
	// surviving flushes and accumulating.
	FlushResidual() (residual, flushes int)

	// SetWatermarkOptimization toggles the high/low watermark
	// optimization across all subscriptions. Disabled before
	// checksum/cutover to ensure all changes are flushed regardless of
	// watermark position.
	SetWatermarkOptimization(ctx context.Context, enabled bool) error

	// StartPeriodicFlush spawns a background goroutine that flushes the
	// changeset at the given interval. Used by the migrator to advance
	// the safe-flushed position. Calling Start while a periodic flush
	// is already running or after Close has been called is a no-op.
	StartPeriodicFlush(ctx context.Context, interval time.Duration)

	// StopPeriodicFlush stops the goroutine started by
	// StartPeriodicFlush. Safe to call when no periodic flush is
	// running (no-op).
	StopPeriodicFlush()

	// AllChangesFlushed reports whether the buffered position has been
	// caught up to the flushed position (i.e. no in-flight changes
	// remain). For non-binlog implementations, this is equivalent to
	// "have all received events been applied?".
	AllChangesFlushed() bool

	// Stop ends delivery of events to subscriptions. Everything else stays
	// live: the source keeps reading and tracking its position, and Flush /
	// BlockWait / AllChangesFlushed / Position keep working. Close, not Stop,
	// releases resources. One-way and idempotent.
	//
	// Cutover calls it once the tables are renamed and while it still holds
	// the exclusive lock, so no write can be in flight — after UNLOCK TABLES
	// the first post-cutover write is a race, and those events no longer
	// decode against the subscriptions' TableInfo (see cutover.go). Hence two
	// requirements: Stop must not block, because every write to the table is
	// stalled behind it, and it must leave the source flushable, because a
	// rename that fails ambiguously is retried via Flush and BlockWait.
	Stop()

	// Close releases all resources, cancelling and joining the reader and any
	// periodic flush loop. Subsequent StartPeriodicFlush calls are no-ops.
	// Safe to call more than once.
	Close()
}
