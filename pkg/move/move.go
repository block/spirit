package move

import (
	"context"
	"time"
)

type Move struct {
	SourceDSN       string        `name:"source-dsn" help:"Where to copy the tables from." default:"spirit:spirit@tcp(127.0.0.1:3306)/src"`
	TargetDSN       string        `name:"target-dsn" help:"Where to copy the tables to." default:"spirit:spirit@tcp(127.0.0.1:3306)/dest"`
	TargetChunkTime time.Duration `name:"target-chunk-time" help:"How long each chunk should take to copy" default:"5s"`
	Threads         int           `name:"threads" help:"How many chunks to copy in parallel" default:"2"`
	CreateSentinel  bool          `name:"create-sentinel" help:"Create a sentinel table in the target database to block after table copy" default:"true"`
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
