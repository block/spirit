package change

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	mysql2 "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// killGTIDSyncer closes the syncer out from under the gtidClient's
// readStream. GetEvent then fails until maxConsecutiveErrors accumulate
// (~500ms), at which point readStream calls recreateStreamer and the
// dump resumes from bufferedGTID.
func killGTIDSyncer(t *testing.T, client *gtidClient) {
	t.Helper()
	client.mu.Lock()
	syncer := client.syncer
	client.mu.Unlock()
	require.NotNil(t, syncer)
	syncer.Close()
}

// TestGTIDRecreateStreamerRecovers drives the gtidClient's real
// consecutive-error path (readStream -> recreateStreamer), the GTID
// analog of TestRecreateStreamerSkipsFlushedReplay:
//
//  1. Stream and flush one row so the client has a non-trivial
//     bufferedGTID/flushedGTID.
//  2. Kill the syncer so GetEvent fails repeatedly and readStream
//     recreates the streamer at bufferedGTID.
//  3. Write another row. Its transaction can only be observed through
//     the recreated stream, so BlockWait returning proves the recovery
//     completed (BlockWait targets the server's executed GTID set,
//     which includes the new transaction).
//  4. Flush and assert both rows landed on the target table.
func TestGTIDRecreateStreamerRecovers(t *testing.T) {
	skipUnlessGTIDEnabled(t)
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS gtidrecreatet1, gtidrecreatet2")
	testutils.RunSQL(t, "CREATE TABLE gtidrecreatet1 (a INT NOT NULL, b INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE gtidrecreatet2 (a INT NOT NULL, b INT, PRIMARY KEY (a))")

	t1 := table.NewTableInfo(db, cfg.DBName, "gtidrecreatet1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, cfg.DBName, "gtidrecreatet2")
	require.NoError(t, t2.SetInfo(t.Context()))

	client := NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*gtidClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()

	// Establish a healthy stream: one row buffered and flushed.
	testutils.RunSQL(t, "INSERT INTO gtidrecreatet1 (a, b) VALUES (1, 1)")
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, 1, client.GetDeltaLen())
	require.NoError(t, client.Flush(t.Context()))
	positionBeforeKill := client.Position()
	require.NotEmpty(t, positionBeforeKill)

	killGTIDSyncer(t, client)

	// This transaction is only observable through the recreated stream.
	testutils.RunSQL(t, "INSERT INTO gtidrecreatet1 (a, b) VALUES (2, 5)")

	// BlockWait targets the server's current executed GTID set (which
	// contains the post-kill INSERT), so returning without error means
	// readStream went through recreateStreamer and caught back up.
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, 1, client.GetDeltaLen(),
		"the post-kill INSERT must be buffered after the streamer recreation")

	require.NoError(t, client.Flush(t.Context()))
	var count int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM gtidrecreatet2").Scan(&count))
	require.Equal(t, 2, count, "both rows must reach the target across the recreation")

	// The resume coordinate must have advanced past the pre-kill flush.
	require.NotEqual(t, positionBeforeKill, client.Position(),
		"flushed GTID set must advance after the recreated stream catches up")
}

// TestGTIDMaxRecreateAttemptsError is the GTID analog of
// TestMaxRecreateAttemptsError: when recreateStreamer keeps failing
// (the server address is unreachable), readStream must exhaust
// maxRecreateAttempts (lowered to 3 in TestMain), signal a fatal error
// through the caller's CancelFunc, and exit cleanly.
func TestGTIDMaxRecreateAttemptsError(t *testing.T) {
	skipUnlessGTIDEnabled(t)
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS gtidrecreateerrt1, gtidrecreateerrt2")
	testutils.RunSQL(t, "CREATE TABLE gtidrecreateerrt1 (a INT NOT NULL PRIMARY KEY)")
	testutils.RunSQL(t, "CREATE TABLE gtidrecreateerrt2 (a INT NOT NULL PRIMARY KEY)")

	t1 := table.NewTableInfo(db, cfg.DBName, "gtidrecreateerrt1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, cfg.DBName, "gtidrecreateerrt2")
	require.NoError(t, t2.SetInfo(t.Context()))

	// The repl client calls CancelFunc on fatal stream errors, which
	// cancels the context (mimicking what the migration runner does).
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// Also capture the reason: exhausted recreate attempts are a stream
	// error, NOT a schema change, so callers must not invalidate checkpoints.
	var gotReason atomic.Int64
	gotReason.Store(-1)
	clientConfig := NewClientDefaultConfig()
	clientConfig.CancelFunc = func(reason FatalReason) bool {
		gotReason.Store(int64(reason))
		cancel()
		return true
	}
	client := NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), clientConfig).(*gtidClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(ctx))

	// Point the syncer config at a dead address (closed port for an
	// instant connection-refused, no DNS involved) and kill the live
	// syncer. Every recreateStreamer attempt now fails. Mutate cfg under
	// client.mu because recreateStreamer reads it under the same lock.
	client.mu.Lock()
	client.cfg.Host = "127.0.0.1"
	client.cfg.Port = 1
	syncer := client.syncer
	client.mu.Unlock()
	require.NotNil(t, syncer)
	syncer.Close()

	// Wait for the readStream goroutine to exit after exhausting the
	// recreation attempts (3 in TestMain; failures are immediate, the
	// dominant cost is the 2s backoff between attempts).
	client.streamWG.Wait()

	require.Error(t, ctx.Err(), "caller context should be cancelled via CancelFunc on fatal stream error")
	require.Equal(t, int64(FatalReasonStreamError), gotReason.Load(),
		"exhausted recreate attempts must be reported as a stream error")

	client.Close()
}
