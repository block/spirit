package datasync

import (
	"context"
	"database/sql"
	"testing"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/require"
)

// TestCheckpointWriteFailureRecordedAsFatal pins the fix for the swallowed-
// checkpoint-failure gap (block/spirit#995 review): status.WatchTask cancels
// the run when DumpCheckpoint returns a fatal error, but the sync's shutdown
// paths map a bare ctx-cancel to nil — so a checkpoint write failure could stop
// the sync yet still return nil / exit 0. DumpCheckpoint must therefore record
// the failure as fatal (and cancel the run) so Run surfaces it.
func TestCheckpointWriteFailureRecordedAsFatal(t *testing.T) {
	r, err := NewRunner(&Sync{})
	require.NoError(t, err)

	// A closed pool makes the checkpoint write fail deterministically with no
	// live server (and no retry — the driver reports the pool closed).
	badDB, err := sql.Open("block-mysql", "u:p@tcp(127.0.0.1:1)/x")
	require.NoError(t, err)
	require.NoError(t, badDB.Close())
	r.target = applier.Target{DB: badDB, Config: &mysql.Config{DBName: "x"}}
	// Non-nil chunker so DumpCheckpoint proceeds past its "pipeline not built"
	// early return; its methods are never reached (createCheckpointTable fails
	// first).
	r.copyChunker = table.NewMockChunker("t1", 100)

	var cancelled bool
	r.cancelFunc = func() { cancelled = true }

	err = r.DumpCheckpoint(context.Background())
	require.Error(t, err, "a failed checkpoint write must return an error to WatchTask")
	require.ErrorContains(t, r.fatal(), "checkpoint write failed",
		"the failure must be recorded as fatal so Run surfaces it instead of exit 0")
	require.True(t, cancelled, "recordFatal must cancel the run")
}
