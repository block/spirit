package change

import (
	"context"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
)

// Subscription defines how the replication changes are tracked. The single
// implementation is bufferedMap; row images come straight from the binlog
// and are written via the applier (no SELECT-from-source round trip).
//
// For non-memory-comparable PKs, bufferedMap uses LWW map dedup during the
// copy phase and switches to an internal FIFO queue post-copy. The queue
// exists because collation-equivalent keys ("A" and "a") hash to different
// map slots but resolve to the same MySQL row — map iteration would apply
// events out of order. During the copy phase the chunker's later SELECT
// covers in-window case-collision races, and the post-cutover checksum
// (with FixDifferences=true) repairs any residual divergence. The
// transition happens inside SetWatermarkOptimization, which drains the
// outgoing store inline.
//
// Memory-comparable PKs always use the map. See
// pkg/change/subscription_buffered.go for the routing rules.

type Subscription interface {
	HasChanged(key, row []any, deleted bool)
	Length() int
	// Flush writes the pending changes to the target(s) via the applier.
	// When underLock is true, locks carries the table locks the caller is
	// holding — one per target server — and the applier executes each
	// target's statements under that target's own lock.
	Flush(ctx context.Context, underLock bool, locks []*dbconn.TableLock) (allChangesFlushed bool, err error)
	// Tables returns the tables related to the subscription in
	// currentTable, newTable order. Move-flow subscriptions have no
	// destination-side TableInfo, in which case only [currentTable] is
	// returned. Entries are never nil: consumers (the clients' DDL
	// subscription-match loops, out-of-tree change.Source event routing)
	// iterate and dereference them.
	Tables() []*table.TableInfo

	// ImmutableColumnOrdinal returns the position (an index into
	// Tables()[0].Columns, and thus into each full binlog row image) of a
	// column whose value must never change between the before and after
	// image of an UPDATE, or -1 when no such column is configured.
	//
	// This backs the sharded applier's vindex contract (see
	// applier.MySQLApplier.UpsertRows): modifications are tracked by
	// PRIMARY KEY only, so an UPDATE that changed the sharding column
	// would flush the new row image to its new shard while the old shard
	// silently kept a stale copy. The change source is expected to treat
	// such an UPDATE as a fatal error and cancel the operation — see
	// checkImmutableColumn.
	ImmutableColumnOrdinal() int

	// SetWatermarkOptimization toggles both high and low watermark
	// optimizations. For non-memory-comparable PKs toggling switches the
	// subscription between map mode and queue mode; on such a transition
	// it drains the outgoing store via the applier so only one store has
	// pending entries at a time. Returns the drain error if any.
	SetWatermarkOptimization(ctx context.Context, enabled bool) error

	// Close signals that no further events will be delivered. Any HasChanged
	// caller currently parked on backpressure (e.g. the bufferedMap soft
	// memory limit) is unblocked so the binlog reader goroutine can exit.
	// Close does NOT flush; pending changes are discarded along with the
	// subscription. It is safe to call more than once.
	Close()
}
