//go:build semisync

package migration

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// TestCutoverAtomicitySemiSync is the semi-sync regression variant of
// TestCutoverAtomicityWithConcurrentWrites. It is gated behind the
// `semisync` build tag and intended to be run only by the dedicated
// `mysql-semisync-docker.yml` workflow.
//
// The workflow brings up a MySQL source with semi-sync enabled, a
// connected `mysql_replica`, and a netem sidecar that adds 100ms of
// outbound delay on the replica's network namespace. The semi-sync ACK
// round-trip therefore pays ~100ms per commit on the source, between
// binlog write and InnoDB commit-visibility — deterministically widening
// the binlog/visibility race documented in issue #746 from a rare CI
// flake to a near-certain event on every commit.
//
// (An earlier draft used a zero-replica configuration, which sounds
// equivalent but isn't: with no replica ever connected, MySQL's
// semi-sync plugin falls back to async after a handful of timed-out
// commits and `Rpl_semi_sync_source_status` flips OFF — see
// `compose/semisync.yml` for the why.)
//
// Spirit's production code path now reads row images directly from the
// binlog via the buffered replication subscription (#821, #823), so it no
// longer issues `REPLACE INTO _new ... SELECT FROM original ...` after
// observing a binlog row event. The binlog/visibility window is therefore
// irrelevant: the row payload spirit needs is the binlog event itself.
//
// If a future change reintroduced any code path that SELECTed from
// `original` based on a key learned from the binlog stream, this test
// would fail with row-count or checksum divergence between `_old` and
// `_new` on every run, instead of needing CI parallel load to surface it
// once in N runs.
func TestCutoverAtomicitySemiSync(t *testing.T) {
	requireSemiSyncSourceActive(t)

	t.Parallel()

	cases := []struct {
		name      string
		tableName string
		schema    string
	}{
		{"optimistic", "t1semisync_opt", cutoverAtomicityOptimisticSchema},
		{"composite", "t1semisync_comp", cutoverAtomicityCompositeSchema},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runCutoverAtomicityTest(t, tc.tableName, tc.schema)
		})
	}
}

// requireSemiSyncSourceActive fails the test if the source isn't
// configured with semi-sync, doesn't have a connected replica, or isn't
// actually injecting the per-commit latency the test depends on. A
// silent skip would make the dedicated workflow look green without
// exercising the case it exists to cover.
//
// The final check — a real timed INSERT — is the load-bearing one. The
// variable/status checks pass even in MySQL's "semi-sync configured but
// fell back to async" mode (which is what happens when no replica is
// connected, or when the replica ACKs faster than the timeout). Only an
// observed commit latency on the order of the configured timeout proves
// the binlog/visibility window is actually being widened.
func requireSemiSyncSourceActive(t *testing.T) {
	t.Helper()

	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	var enabled string
	err = db.QueryRowContext(ctx, "SELECT @@global.rpl_semi_sync_source_enabled").Scan(&enabled)
	require.NoError(t, err, "rpl_semi_sync_source_enabled lookup failed — is the semi-sync plugin loaded?")
	require.Equal(t, "1", enabled, "rpl_semi_sync_source_enabled must be ON")

	var timeoutMs int
	err = db.QueryRowContext(ctx, "SELECT @@global.rpl_semi_sync_source_timeout").Scan(&timeoutMs)
	require.NoError(t, err)
	require.LessOrEqual(t, timeoutMs, 1000,
		"rpl_semi_sync_source_timeout=%dms is too high — the workflow should pin it to ~100ms", timeoutMs)

	// At least one replica must be connected. Without a replica, the
	// plugin falls back to async after a handful of commits and stops
	// adding any latency.
	var clientsVar, clientsVal string
	err = db.QueryRowContext(ctx, "SHOW GLOBAL STATUS LIKE 'Rpl_semi_sync_source_clients'").Scan(&clientsVar, &clientsVal)
	require.NoError(t, err)
	require.NotEqual(t, "0", clientsVal,
		"Rpl_semi_sync_source_clients=0 — no replica connected, semi-sync will fall back to async on the first commit")

	// Status ON means semi-sync hasn't fallen back to async yet.
	var statusVar, statusVal string
	err = db.QueryRowContext(ctx, "SHOW GLOBAL STATUS LIKE 'Rpl_semi_sync_source_status'").Scan(&statusVar, &statusVal)
	require.NoError(t, err)
	require.Equal(t, "ON", statusVal, "Rpl_semi_sync_source_status must be ON; got %q", statusVal)

	// E2E proof: commits actually pay the ACK-delay tax. Three commits in
	// a row should each take roughly the semi-sync timeout window (the
	// replica's outbound ACK is delayed by netem). One commit might be a
	// fluke (group-commit batching, warmup); three in a row averaging
	// above 50ms rules out the "fell back to async" failure mode.
	_, err = db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS _semisync_preflight (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY)")
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = db.ExecContext(cleanupCtx, "DROP TABLE IF EXISTS _semisync_preflight")
	})

	var total time.Duration
	const probes = 3
	for i := 0; i < probes; i++ {
		start := time.Now()
		_, err = db.ExecContext(ctx, "INSERT INTO _semisync_preflight () VALUES ()")
		require.NoError(t, err)
		total += time.Since(start)
	}
	avg := total / probes
	require.Greater(t, avg, 50*time.Millisecond,
		"semi-sync preflight: avg commit latency across %d probes is %v (timeout=%dms). "+
			"netem qdisc on the replica may have failed to apply, or the replica is ACKing too fast.",
		probes, avg, timeoutMs)

	t.Logf("semi-sync verified: timeout=%dms, replica clients=%s, avg commit latency=%v",
		timeoutMs, clientsVal, avg)
}
