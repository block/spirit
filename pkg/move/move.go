package move

import (
	"context"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/table"
)

type Move struct {
	SourceDSN       string        `name:"source-dsn" help:"Where to copy the tables from." default:"spirit:spirit@tcp(127.0.0.1:3306)/src"`
	TargetDSN       string        `name:"target-dsn" help:"Where to copy the tables to." default:"spirit:spirit@tcp(127.0.0.1:3306)/dest"`
	TargetChunkTime time.Duration `name:"target-chunk-time" help:"How long each chunk should take to copy" default:"5s"`
	Threads         int           `name:"threads" help:"How many chunks to copy in parallel" default:"2"`
	CreateSentinel  bool          `name:"create-sentinel" help:"Create a sentinel table on the source database to block after table copy" default:"false"`

	// SourceTables optionally specifies a list of tables to move.
	// If empty, all tables in the source database will be moved.
	// This is useful for Vitess MoveTables operations where only specific tables should be moved.
	SourceTables []string

	// VindexProvider optionally provides vindex metadata for resharding operations.
	// If nil, tables will not have vindex configuration (suitable for simple MoveTables 1:1 operations).
	// For resharding operations (1:many), this should be set to provide sharding key information.
	// The provider is called during table discovery to configure VindexColumn and VindexFunc
	// on each TableInfo.
	VindexProvider table.VindexMetadataProvider
	Targets        []applier.Target
}

func (m *Move) Run() error {
	move, err := NewRunner(m)
	if err != nil {
		return err
	}
	defer move.Close()
	if err := move.Run(context.TODO()); err != nil {
		return err
	}
	return nil
}
