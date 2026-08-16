package move

import (
	"context"
	"fmt"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
)

type Move struct {
	SourceDSN       string        `name:"source-dsn" help:"Where to copy the tables from." default:"spirit:spirit@tcp(127.0.0.1:3306)/src"`
	TargetDSN       string        `name:"target-dsn" help:"Where to copy the tables to." default:"spirit:spirit@tcp(127.0.0.1:3306)/dest"`
	TargetChunkTime time.Duration `name:"target-chunk-time" help:"Target time for each checksum chunk. The copy phase is sized by an in-memory byte budget and does not use this." default:"5s"`
	// TargetChunkSize is the in-memory byte budget the buffered copier sizes each
	// copy chunk against (see table.DefaultTargetChunkBytes). Move always uses the
	// buffered copier. A zero value means "use the default" (NewRunner fills it
	// in). The Kong default below must stay equal to table.DefaultTargetChunkBytes.
	TargetChunkSize       uint64        `name:"target-chunk-size" help:"In-memory byte budget per copy chunk (in bytes)." default:"16777216"`
	Threads               int           `name:"threads" help:"How many chunks to copy in parallel" default:"2"`
	WriteThreads          int           `name:"write-threads" help:"How many concurrent write threads to use per target" default:"4"`
	CreateSentinel        bool          `name:"create-sentinel" help:"Create a sentinel table on the first target database to block after table copy" default:"false"`
	DeferSecondaryIndexes bool          `name:"defer-secondary-indexes" help:"Create target tables without secondary indexes, add them before cutover" default:"false"`
	CheckpointMaxAge      time.Duration `name:"checkpoint-max-age" help:"Maximum age of a checkpoint before refusing to resume from it" optional:"" default:"168h"`
	// Force makes the runner wipe the target tables and start the copy fresh when
	// it cannot resume from a checkpoint (e.g. the checkpoint is from an
	// incompatible spirit version, or the target is in a state resume can't
	// validate). Without it, an unresumable non-empty target is a hard error.
	Force bool `name:"force" help:"When the target cannot resume from a checkpoint, wipe the target tables and start the copy fresh instead of failing." default:"false"`

	// ReverseWindow, when > 0, keeps the move alive after cutover in change-only
	// reverse mode (targets→source) for this long, so the move can be rolled back
	// before the source is retired. 0 (the default) is a normal cutover. During
	// the window the source's now-retired _old tables are kept current from the
	// targets; an operator rolls back by creating the _spirit_move_revert table on
	// the first target (see revertmarker.go), otherwise the window elapses and the
	// move finalizes forward. A sharded (multi-DSN) source additionally requires
	// ReverseShardingProvider and SourceKeyRanges so the reverse feed can route
	// rows back to the correct source shard — see the guard in Runner.Run. The
	// data plane is ReverseFeed (reversefeed.go); the post-cutover driver is
	// reverseWindow (reversewindow.go).
	ReverseWindow time.Duration `name:"reverse-window" help:"After cutover, reverse the move (change-only) and keep it alive for this long to allow rollback. 0 disables (normal cutover)." default:"0"`

	// SourceTables optionally specifies a list of tables to move.
	// If empty, all tables in the source database will be moved.
	// This is useful for Vitess MoveTables operations where only specific tables should be moved.
	SourceTables []string

	// ShardingProvider optionally provides vindex metadata for resharding operations.
	// If nil, tables will not have vindex configuration (suitable for simple MoveTables 1:1 operations).
	// For resharding operations (1:many), this should be set to provide sharding key information.
	// The provider is called during table discovery to configure ShardingColumn and HashFunc
	// on each TableInfo.
	// SourceDSNs optionally specifies multiple source DSNs for N:M resharding operations.
	// When set, each DSN represents a separate source shard. All sources must have identical
	// table schemas. If empty, SourceDSN is used as the single source.
	SourceDSNs []string `kong:"-"`

	// SourceKeyRanges optionally specifies each source shard's Vitess-style key
	// range ("-80", "80-", ...), parallel to SourceDSNs (SourceKeyRanges[i] is
	// SourceDSNs[i]'s range). Required, together with ReverseShardingProvider,
	// when ReverseWindow > 0 and the source is sharded (len(SourceDSNs) > 1):
	// the reverse feed routes rows flowing back from the targets to the source
	// shard whose range contains the row's hash. Unused otherwise.
	SourceKeyRanges []string `kong:"-"`

	ShardingProvider table.ShardingMetadataProvider `kong:"-"`

	// ReverseShardingProvider provides the SOURCE keyspace's sharding metadata
	// (vindex column + hash) for the reverse feed of a reverse-window move with
	// a sharded source. It is consulted for each moved table when the window
	// opens; a table without metadata is a hard error there, because reverse
	// writes could not be routed to a source shard. Note the asymmetry with
	// ShardingProvider, which describes the TARGET keyspace for the forward copy.
	ReverseShardingProvider table.ShardingMetadataProvider `kong:"-"`

	Targets []applier.Target `kong:"-"`
}

// Validate is called by Kong after parsing to check for invalid flag values.
// Zero values mean "use the default" (NewRunner fills them in), so they are not
// rejected here; only explicitly-negative or otherwise invalid values are
// caught. Mirrors migration.Migration.Validate.
func (m *Move) Validate() error {
	if m.Threads < 0 {
		return fmt.Errorf("--threads must be non-negative, got %d", m.Threads)
	}
	if m.WriteThreads < 0 {
		return fmt.Errorf("--write-threads must be non-negative, got %d", m.WriteThreads)
	}
	if m.TargetChunkTime < 0 {
		return fmt.Errorf("--target-chunk-time must be non-negative, got %s", m.TargetChunkTime)
	}
	if m.ReverseWindow < 0 {
		return fmt.Errorf("--reverse-window must be non-negative, got %s", m.ReverseWindow)
	}
	return nil
}

func (m *Move) Run() error {
	move, err := NewRunner(m)
	if err != nil {
		return err
	}
	defer utils.CloseAndLog(move)
	if err := move.Run(context.TODO()); err != nil {
		return err
	}
	return nil
}
