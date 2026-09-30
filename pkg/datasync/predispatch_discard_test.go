package datasync

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/throttler"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// gateThrottler holds the copier's read worker in BlockWait until the test
// releases it: one chunk per token, or everything once opened. It is binary
// (no GradualThrottler), so the change feed's flush never sees it as load.
type gateThrottler struct {
	tokens   chan struct{}
	open     chan struct{}
	openOnce sync.Once
	waits    atomic.Int64 // BlockWait calls entered so far
}

var _ throttler.Throttler = (*gateThrottler)(nil)

func newGateThrottler() *gateThrottler {
	return &gateThrottler{tokens: make(chan struct{}, 1), open: make(chan struct{})}
}

func (g *gateThrottler) Open(context.Context) error      { return nil }
func (g *gateThrottler) Close() error                    { return nil }
func (g *gateThrottler) UpdateLag(context.Context) error { return nil }
func (g *gateThrottler) release()                        { g.openOnce.Do(func() { close(g.open) }) }

func (g *gateThrottler) IsThrottled() bool {
	select {
	case <-g.open:
		return false
	default:
		return true
	}
}

func (g *gateThrottler) BlockWait(ctx context.Context) {
	g.waits.Add(1)
	select {
	case <-g.open:
	case <-g.tokens:
	case <-ctx.Done():
	}
}

// TestSyncPreDispatchChangeThenAboveHighWatermark: a change reaches the
// target before the initial copy dispatches its first chunk (sync's periodic
// flush starts before the copy), and a second change to the same row arrives
// once the copy has started.
//
// Before the fix the second change was discarded as above the high watermark,
// and the copier's INSERT IGNORE skipped the row because the first change had
// already put it on the target. The target entered continuous sync serving the
// first image (UPDATE) or a row the source had deleted (DELETE) until the
// lockless checksum reached that chunk and recopied it.
func TestSyncPreDispatchChangeThenAboveHighWatermark(t *testing.T) {
	for _, secondIsDelete := range []bool{false, true} {
		name := "update"
		if secondIsDelete {
			name = "delete"
		}
		t.Run(name, func(t *testing.T) {
			runSyncPreDispatchScenario(t, secondIsDelete)
		})
	}
}

func runSyncPreDispatchScenario(t *testing.T, secondIsDelete bool) {
	const key = 16384 // the top of the key space; far above the first chunk
	srcDB, destDB := "sync_predisp_src", "sync_predisp_dest"
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	src := cfg.Clone()
	src.DBName = srcDB
	dest := cfg.Clone()
	dest.DBName = destDB

	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+srcDB)
	testutils.RunSQL(t, "CREATE DATABASE "+srcDB)
	testutils.RunSQL(t, "CREATE TABLE "+srcDB+".t1 (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, b INT NOT NULL)")
	testutils.RunSQL(t, "INSERT INTO "+srcDB+".t1 (b) VALUES (1)")
	for range 14 {
		testutils.RunSQL(t, "INSERT INTO "+srcDB+".t1 (b) SELECT 1 FROM "+srcDB+".t1")
	}
	testutils.RunSQL(t, "ANALYZE TABLE "+srcDB+".t1")
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+destDB)
	testutils.RunSQL(t, "CREATE DATABASE "+destDB)
	t.Cleanup(func() {
		testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+srcDB)
		testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+destDB)
	})

	tgt, err := sql.Open("block-mysql", dest.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(tgt)
	// targetRow returns the row's b and whether it exists; ok is false if the
	// query failed (for example, the table is not created yet).
	targetRow := func() (b int, exists, ok bool) {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		err := tgt.QueryRowContext(ctx, fmt.Sprintf("SELECT b FROM t1 WHERE id = %d", key)).Scan(&b)
		if err == sql.ErrNoRows {
			return 0, false, true
		}
		if err != nil {
			return 0, false, false
		}
		return b, true, true
	}

	gate := newGateThrottler()
	runner, err := NewRunner(&Sync{
		SourceDSN:      src.FormatDSN(),
		TargetDSN:      dest.FormatDSN(),
		Threads:        1, // one read worker, so one token is one chunk
		WriteThreads:   2,
		MaxConnections: 16,
		Target:         &applier.Target{DB: tgt, Config: dest},
		FlushInterval:  50 * time.Millisecond,
	})
	require.NoError(t, err)
	fakeAurora(runner, 2, throttler.AuroraResult{Throttlers: []throttler.Throttler{gate}})
	h := startRunner(t, runner)
	defer gate.release()

	// The read worker is parked before its first chunker.Next(). The target
	// table exists and the periodic flush is running.
	h.eventually(func() bool { return gate.waits.Load() >= 1 }, 30*time.Second, "copier parks before its first chunk")

	// (1) The first change reaches the target ahead of the copier.
	testutils.RunSQL(t, fmt.Sprintf("UPDATE %s.t1 SET b = 99 WHERE id = %d", srcDB, key))
	h.eventually(func() bool {
		b, exists, ok := targetRow()
		return ok && exists && b == 99
	}, 30*time.Second, "the first change is flushed to the target")

	// (2) Let exactly one chunk through. The read worker is back in BlockWait
	// once it has dispatched and processed it.
	gate.tokens <- struct{}{}
	h.eventually(func() bool { return gate.waits.Load() >= 2 }, 30*time.Second, "copier dispatches its first chunk")

	// (3) The second change, with the key now above the high watermark. Wait
	// until the change feed has read it.
	if secondIsDelete {
		testutils.RunSQL(t, fmt.Sprintf("DELETE FROM %s.t1 WHERE id = %d", srcDB, key))
	} else {
		testutils.RunSQL(t, fmt.Sprintf("UPDATE %s.t1 SET b = 100 WHERE id = %d", srcDB, key))
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	require.NoError(t, runner.replClient.BlockWait(ctx))

	// (4) Finish the copy and enter continuous sync.
	gate.release()
	h.awaitContinuous(60 * time.Second)

	// The copy must hand continuous sync a correct target. This read races
	// the lockless checksum, which starts with the continuous phase and would
	// repair the row once it reaches its chunk, so the check after
	// FirstCleanPass below is the one that cannot miss.
	requireTargetMatches := func(when string) {
		t.Helper()
		b, exists, ok := targetRow()
		require.True(t, ok)
		if secondIsDelete {
			require.False(t, exists, "%s: the row deleted on the source is still on the target", when)
			return
		}
		require.True(t, exists, when)
		require.Equal(t, 100, b, "%s: the target kept the first change's image", when)
	}
	requireTargetMatches("after the copy")

	// The lockless checksum must not have had to repair anything.
	h.await(runner.FirstCleanPass(), 60*time.Second, "FirstCleanPass")
	stats := runner.ChecksumStats()
	requireTargetMatches("after the first clean pass")
	require.Zero(t, stats.MismatchesDetected,
		"the lockless checksum found (and repaired) a divergence the copy left behind: %+v", stats)
	h.stop()
}
