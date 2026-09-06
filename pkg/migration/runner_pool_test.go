package migration

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/block/spirit/pkg/autoscale"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// TestControlPlaneConns documents the connection headroom the main pool
// reserves above the copy hot path: a fixed +2 (checkpoint INSERT and the
// replication-flush poll) plus one per change table (AutoUpdateStatistics runs
// a goroutine each). A single fixed spare could not cover these once the
// copier + applier saturated the budget — see controlPlaneConns.
func TestControlPlaneConns(t *testing.T) {
	// Single-table: checkpoint + replication poll + one stats updater.
	single := &Runner{changes: make([]*tableChange, 1)}
	require.Equal(t, 3, single.controlPlaneConns())

	// Multi-table: the stats-updater term scales with the number of tables, so
	// a multi-table ALTER does not starve them behind the fixed headroom.
	multi := &Runner{changes: make([]*tableChange, 3)}
	require.Equal(t, 5, multi.controlPlaneConns())
}

// TestAutoscalingLeavesThreadFlagsAloneWhenItCannotEngage is the other half of
// "autoscaling owns the thread counts": it only owns them when it can actually
// steer. Asking for autoscaling against a target with no continuous load signal
// (any non-Aurora server, which is what the test suite runs against) must leave
// --threads and --write-threads exactly as configured, because the pools will
// run fixed at those values for the whole migration.
//
// The engaged path cannot be exercised here — it needs a real Aurora instance to
// read a vCPU count from. autoscale.ReadBounds covers the sizing it derives, and
// the assertion below covers the branch that decides whether to apply it.
func TestAutoscalingLeavesThreadFlagsAloneWhenItCannotEngage(t *testing.T) {
	testutils.NewTestTable(t, "autoscale_flags",
		`CREATE TABLE autoscale_flags (id INT NOT NULL PRIMARY KEY, pad VARCHAR(32))`)
	testutils.RunSQL(t, `INSERT INTO autoscale_flags VALUES (1, 'a'), (2, 'b')`)

	// Values distinct from both the defaults and each other, so an override
	// would be unmistakable.
	const threads, writeThreads = 3, 5
	m := NewTestRunner(t, "autoscale_flags", "ENGINE=InnoDB",
		WithAutoscaling(), WithThreads(threads), WithWriteThreads(writeThreads))
	require.NoError(t, m.Run(t.Context()))
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	require.Equal(t, threads, m.migration.Threads,
		"--threads must survive an autoscaling request that could not engage")
	require.Equal(t, writeThreads, m.migration.WriteThreads,
		"--write-threads must survive an autoscaling request that could not engage")
	// The pool is not derived from either flag, engaged or not: it is
	// --max-connections. What autoscaling would have changed here is the
	// ceilings the copier scales its own workers against, not the pool they
	// check connections out of.
	require.Equal(t, defaultMaxConnections, m.dbConfig.MaxOpenConnections)
}

// TestPoolSizeIsExactlyMaxConnections is the property an operator budgets
// against: spirit's main pool is --max-connections and stays there, so a user
// with a max_user_connections can subtract one number and be done.
//
// It asserts the live pool rather than the config, and after a full run rather
// than at setup, because a phase that resizes r.db does so through
// dbconn.SetPoolSize and would leave the config untouched.
func TestPoolSizeIsExactlyMaxConnections(t *testing.T) {
	testutils.NewTestTable(t, "pool_verbatim",
		`CREATE TABLE pool_verbatim (id INT NOT NULL PRIMARY KEY, pad VARCHAR(32))`)
	testutils.RunSQL(t, `INSERT INTO pool_verbatim VALUES (1, 'a'), (2, 'b')`)

	// Not the default, not any sum of the thread counts, and comfortably above
	// minPoolSize — a number nothing could arrive at by deriving it.
	const maxConnections = 37
	m := NewTestRunner(t, "pool_verbatim", "ENGINE=InnoDB", WithMaxConnections(maxConnections))
	require.NoError(t, m.Run(t.Context()))
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	require.Equal(t, maxConnections, m.dbConfig.MaxOpenConnections)
	require.Equal(t, maxConnections, m.db.Stats().MaxOpenConnections,
		"the live pool must still be the configured number after copy, checksum and cutover have all run")
}

// TestFlushBoundsPreservesChangeDefaults pins the agreement between
// autoscale's rows-in-flight budget and the change package's defaults. It lives
// here because it is the assertion neither package can make: pkg/change imports
// pkg/autoscale, so autoscale cannot name change.DefaultBatchSize, and the
// budget would otherwise be a bare 8000 that drifts silently if either default
// moved.
//
// The consequence of drift is not a crash, it is a behaviour change nobody asked
// for: an instance small enough to hit the concurrency floor is supposed to
// receive exactly the pre-derivation values, so that this whole mechanism is a
// no-op below 16 vCPUs.
func TestFlushBoundsPreservesChangeDefaults(t *testing.T) {
	require.Equal(t, change.DefaultFlushConcurrency*change.DefaultBatchSize, autoscale.FlushRowsInFlight,
		"the rows-in-flight budget must remain the historical concurrency x batch size")
	require.Equal(t, change.DefaultFlushConcurrency, autoscale.MinFlushConcurrency,
		"the floor must be the historical default, so a small instance is unaffected")
	require.Greater(t, autoscale.MinFlushBatchSize, minAdaptiveBatchSizeForTest,
		"a derived batch size must stay well above the AIMD controller's distress floor")

	// Every instance size at or below the floor gets exactly today's pair.
	for vCPUs := 1; vCPUs <= change.DefaultFlushConcurrency+autoscale.VCPUReserve; vCPUs++ {
		concurrency, batchSize := autoscale.FlushBounds(vCPUs)
		require.Equal(t, change.DefaultFlushConcurrency, concurrency, "at %d vCPUs", vCPUs)
		require.Equal(t, change.DefaultBatchSize, batchSize, "at %d vCPUs", vCPUs)
	}
}

// minAdaptiveBatchSizeForTest mirrors the change package's unexported
// minAdaptiveBatchSize. Duplicated rather than exported: it is the AIMD
// controller's private distress floor, and the only thing outside that package
// with an interest in it is the assertion above.
const minAdaptiveBatchSizeForTest = 50

// TestMaxConnectionsDefaultMatchesItsFlag pins the constant to the kong tag it
// says it mirrors. The two are only connected by a comment, and the failure
// mode of drift is silent: the CLI would keep bounding at one number while
// every embedding orchestrator bounded at another.
func TestMaxConnectionsDefaultMatchesItsFlag(t *testing.T) {
	field, ok := reflect.TypeFor[Migration]().FieldByName("MaxConnections")
	require.True(t, ok)
	require.Equal(t, strconv.Itoa(defaultMaxConnections), field.Tag.Get("default"))

	// And an unset field lands on it, so a programmatic caller is bounded too.
	// That is not incidental: the derived ceilings only overflow on the large
	// instances, whose migrations are as likely to be driven by an embedder.
	m := &Migration{Statement: "ALTER TABLE t1 ENGINE=InnoDB", Database: "test"}
	_, err := m.normalizeOptions()
	require.NoError(t, err)
	require.Equal(t, defaultMaxConnections, m.MaxConnections)
}

// TestReadBoundsForPool covers the one worker count that cannot be left to
// queue. The checksum opens a transaction per read thread, serially, under the
// table lock, and each holds its connection for the whole phase — so bounds the
// pool cannot hold block on checkout with that lock held.
//
// Validate cannot catch this case: under autoscaling both numbers come from an
// instance vCPU count read long after flag parsing, and have nothing to do with
// --threads.
func TestReadBoundsForPool(t *testing.T) {
	// A single-table migration, which is what minChecksumPhaseReserve describes.
	const reserve = minChecksumPhaseReserve

	// Room to spare: the bounds are whatever was derived.
	start, ceiling := readBoundsForPool(16, 32, defaultMaxConnections, reserve)
	require.Equal(t, 16, start)
	require.Equal(t, 32, ceiling)

	// Exactly enough, counting the reserve the checksum phase cannot run without.
	start, ceiling = readBoundsForPool(16, 32-reserve, 32, reserve)
	require.Equal(t, 16, start)
	require.Equal(t, 32-reserve, ceiling)

	// One short. The ceiling gives way, not the pool: the ceiling is a number
	// spirit derived for itself, the pool is one the operator set.
	_, ceiling = readBoundsForPool(16, 32-reserve+1, 32, reserve)
	require.Equal(t, 32-reserve, ceiling)

	// The start is fitted too. This is the case that used to survive the fit:
	// a 96-vCPU instance derives (24, 48) from autoscale.ReadBounds, the
	// operator's pool is 20, and both consumers floor the ceiling back up to the
	// start — so lowering only the ceiling changed nothing.
	start, ceiling = readBoundsForPool(24, 48, 20, reserve)
	require.Equal(t, 20-reserve, start)
	require.Equal(t, 20-reserve, ceiling)

	// Never zero readers. A pool too small to honour the reserve still has to
	// run: contending with the control plane beats not copying at all.
	start, ceiling = readBoundsForPool(24, 48, reserve, reserve)
	require.Equal(t, 1, start)
	require.Equal(t, 1, ceiling)

	// No pool size resolved (a Runner built directly, bypassing
	// normalizeOptions): there is nothing to fit to, so nothing is changed.
	start, ceiling = readBoundsForPool(24, 48, 0, reserve)
	require.Equal(t, 24, start)
	require.Equal(t, 48, ceiling)
	start, ceiling = readBoundsForPool(24, 48, -1, reserve)
	require.Equal(t, 24, start)
	require.Equal(t, 48, ceiling)
}

// TestReadBoundsSurviveTheirConsumers is the assertion the unit test above
// cannot make on its own: that the fitted numbers are still the numbers the
// checksum builds its transaction pool with.
//
// Both consumers deliberately floor the ceiling at the start —
// checksum.NewChecker takes max(Autoscale.MaxThreads, Concurrency) and the
// copier's resolveReadCeiling does the same, because a pool that begins above
// its cap cannot be controlled. That is correct on its own terms and it is
// exactly why the start has to be fitted as well: fitting the ceiling alone is
// undone one call later, silently, in the autoscaling regime the fit exists for.
//
// So this walks the real derivation — autoscale.ReadBounds for the instance,
// readBoundsForPool for the pool, then the consumers' max() — and asserts the
// composed result leaves the reserve intact.
func TestReadBoundsSurviveTheirConsumers(t *testing.T) {
	const reserve = minChecksumPhaseReserve

	// vCPU counts spanning autoscale.ReadBounds, against pools from far too
	// small to comfortable. The small pools are the point: they are the ones
	// where the derived bounds and the operator's budget disagree.
	for _, vCPUs := range []int{16, 32, 64, 96, 128} {
		for _, maxConnections := range []int{8, 16, 20, 32, 64, defaultMaxConnections} {
			readStart, readCeiling := autoscale.ReadBounds(vCPUs)
			start, ceiling := readBoundsForPool(readStart, readCeiling, maxConnections, reserve)

			// What checksum.NewChecker and copier.resolveReadCeiling resolve to.
			effective := max(ceiling, start)

			require.LessOrEqual(t, effective+reserve, maxConnections,
				"at %d vCPUs with --max-connections=%d: the checksum pins %d connections for the whole phase, leaving %d of %d for the reserve",
				vCPUs, maxConnections, effective, maxConnections-effective, maxConnections)
			require.GreaterOrEqual(t, effective, 1,
				"at %d vCPUs with --max-connections=%d: a migration needs at least one reader",
				vCPUs, maxConnections)
		}
	}
}

// TestValidateMaxConnections covers the checks that took over from the runtime
// floor. The pool is now the flag verbatim, so a number too small to work is
// not a slower migration — it is one that stalls partway through, holding a
// table lock or an open read view while it does. Validate is the last place
// that can say so cheaply, which is why these are here rather than treated as
// the operator's problem to discover.
func TestValidateMaxConnections(t *testing.T) {
	// Kong applies defaults before Validate, so a CLI invocation always arrives
	// with the thread counts filled in. Mirror that.
	valid := func(maxConnections int) *Migration {
		return &Migration{
			Statement:      "ALTER TABLE t1 ENGINE=InnoDB",
			Threads:        4,
			WriteThreads:   4,
			MaxConnections: maxConnections,
		}
	}

	require.NoError(t, valid(defaultMaxConnections).Validate())
	require.NoError(t, valid(4+minChecksumPhaseReserve).Validate(),
		"a small but workable pool is the operator asking for a slow migration, which is allowed")

	// Zero is "use the default" (normalizeOptions fills it in), on the same
	// footing as Threads and WriteThreads. Checking it against the minimums
	// would reject every programmatic caller that left the field unset.
	require.NoError(t, valid(0).Validate())

	require.ErrorContains(t, valid(-1).Validate(), "must be non-negative",
		"negative is no longer a way to ask for an unbounded pool")

	require.ErrorContains(t, valid(minPoolSize-1).Validate(), "for the cutover to run",
		"below the cutover's minimum the migration cannot finish, only fail late")

	// Above minPoolSize but below what the checksum phase needs: its read
	// transactions hold their connections for the whole phase, so the control
	// plane and the drain would have nothing left to check out.
	pinning := valid(minPoolSize + 1)
	pinning.Threads = 32
	err := pinning.Validate()
	require.ErrorContains(t, err, "below what the checksum phase needs")
	require.ErrorContains(t, err, "lower --threads", "the error must name a way out")

	// A zero Threads means "use the default", and Validate runs before
	// normalizeOptions fills it in. Checking the 0 rather than the 4 it becomes
	// would accept a pool the migration then stalls on.
	unset := valid(defaultThreads + minChecksumPhaseReserve - 1)
	unset.Threads = 0
	require.ErrorContains(t, unset.Validate(), "below what the checksum phase needs",
		"an unset --threads must be validated as the default it resolves to")
}
