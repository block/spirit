package migration

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/block/spirit/pkg/autoscale"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/throttler"
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
// "autoscaling owns the thread counts": it only owns them when there is an
// instance to size them from. Asking for autoscaling against a target with no
// continuous load signal (any non-Aurora server, which is what the test suite
// runs against) must leave --threads and --write-threads exactly as configured,
// because nothing read a vCPU count and the pools will run fixed at those
// values for the whole migration.
//
// The sized path cannot be exercised here — it needs a real Aurora instance to
// read a vCPU count from. sizePoolsFromInstance covers the numbers it derives
// and autoscale.ReadBounds the bounds underneath them; the assertion below
// covers the branch that decides whether there is an instance at all.
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

// TestSizePoolsFromInstance covers the two answers the instance size decides,
// and that they are decided separately.
//
// Whether the pools may be *re-sized* on the utilization signal needs an
// instance the signal can resolve, which is what MinVCPUs is. What size they
// start at is arithmetic, and it is what keeps them under the hard-stop
// threshold the same instance sets (throttler.MinThreadsThrottleThreshold, and
// TestSmallInstancePoolsFitTheThreshold over there). A small instance is where
// that matters most: a pool left at a constant while its threshold comes from
// the box holds more threads than the threshold allows, and the copy throttles
// on nothing but its own workers — at any table size, a few hundred rows
// included. So the sizing applies below MinVCPUs as well as above it, and only
// the ceiling the controller would grow into goes away.
func TestSizePoolsFromInstance(t *testing.T) {
	// Far above anything these instances derive, so what is under test is the
	// instance arithmetic and not this host's core count.
	const clientCeiling = 1000

	// Every instance size too small for the controller: Aurora's smallest is 2
	// vCPUs, and the write side floors at one thread rather than deriving zero.
	for vCPUs := 2; vCPUs < autoscale.MinVCPUs; vCPUs++ {
		pools, growable := sizePoolsFromInstance(vCPUs, clientCeiling)

		require.Falsef(t, growable, "at %d vCPUs the utilization signal is too coarse to steer on", vCPUs)
		require.Zerof(t, pools.readCeiling,
			"at %d vCPUs nothing moves the read pool, so it must not reserve room to grow into", vCPUs)

		// Both sides are still sized from the instance, and the total fits under
		// the threshold that instance sets.
		require.LessOrEqualf(t, pools.read+pools.write, throttler.MinThreadsThrottleThreshold(vCPUs),
			"at %d vCPUs: %d read + %d write threads against a hard-stop that trips above %d",
			vCPUs, pools.read, pools.write, throttler.MinThreadsThrottleThreshold(vCPUs))

		// They land on their floors at these sizes: two readers, the smallest
		// start that overlaps read and apply work from the first chunk, against a
		// single write thread.
		require.Equalf(t, autoscale.MinReadStartThreads, pools.read, "at %d vCPUs", vCPUs)
		require.Equalf(t, 1, pools.write, "at %d vCPUs", vCPUs)
		require.Falsef(t, pools.hostBound(), "at %d vCPUs nothing was capped by the client ceiling", vCPUs)
	}

	// At MinVCPUs the controller engages and the read side gets a ceiling. Its
	// bounds meet at 2 there, which is the intended reading of a 4-vCPU
	// instance: two readers is already half of it.
	pools, growable := sizePoolsFromInstance(autoscale.MinVCPUs, clientCeiling)
	require.True(t, growable)
	require.Equal(t, 2, pools.read)
	require.Equal(t, 2, pools.readCeiling)
	require.Equal(t, autoscale.MinVCPUs-autoscale.VCPUReserve, pools.write)

	// A client too small to drive what the target would justify: the starting
	// sizes come down to it and the caller is told which numbers the target
	// would have supported.
	pools, _ = sizePoolsFromInstance(96, 3)
	require.True(t, pools.hostBound())
	require.Equal(t, 3, pools.read)
	require.Equal(t, 3, pools.write)
	require.Equal(t, 24, pools.instanceRead)
	require.Equal(t, 94, pools.instanceWrite)
	require.Equal(t, 3, pools.readCeiling,
		"the ceiling is bounded by the client too, but never below the start")
}

// TestSmallestInstancePoolsNeverOutgrowTheHardStop is the case the sizing
// exists for, taken through to the counts the copy actually runs with: on the
// smallest instance spirit meets, the threads hard-stop trips at a lower count
// than either thread flag's default supplies, so pools that are not sized from
// that instance cannot fit under it.
//
// Everything the pools can ever reach has to fit, not only the size they start
// at. No controller engages below MinVCPUs, so nothing sheds a thread that does
// not fit: a pool over the threshold is over it for the whole copy, every
// sample trips the hard-stop, and each chunk then waits out BlockWait before
// the next one starts. A copy that advances roughly a chunk a minute does not
// finish inside a deadline of minutes however few rows the table holds — which
// is why the symptom is a stalled copy rather than a slow one, on a table a
// native ALTER would have finished in well under a second.
//
// The ceilings are resolved here rather than assumed, over both privilege-probe
// answers that feed them, because those answers are settled after the pools are
// sized and must not be able to widen one past the threshold.
//
// Scope is the copy's own pools. The change-feed drain floors at
// MinFlushConcurrency — the historical default, which the derivation does not
// size down — so it can exceed this threshold on an instance this small, but
// only with binlog changes to apply. This test does not claim otherwise.
func TestSmallestInstancePoolsNeverOutgrowTheHardStop(t *testing.T) {
	const smallestInstanceVCPUs = 2
	threshold := throttler.MinThreadsThrottleThreshold(smallestInstanceVCPUs)

	pools, growable := sizePoolsFromInstance(smallestInstanceVCPUs, autoscale.ClientCeiling())
	require.False(t, growable, "nothing steers the pools at this size")
	require.Zero(t, pools.readCeiling, "the marker that says nothing can grow the read pool")

	for _, redoAware := range []bool{false, true} {
		for _, commitLatency := range []bool{false, true} {
			// The zero ceiling above is the marker the runner's maxRead
			// fallback reads, so this is the branch it takes here.
			maxRead := copier.ResolveMaxReadThreads(pools.read, growable)
			maxWrite := throttler.ResolveMaxWriteThreads(pools.write, growable, redoAware, commitLatency)

			require.LessOrEqualf(t, maxRead+maxWrite, threshold,
				"redo_aware=%t commit_latency=%t: %d read + %d write threads at their widest, against a hard-stop that trips above %d",
				redoAware, commitLatency, maxRead, maxWrite, threshold)
		}
	}

	// The other half of the comparison, and the reason the derivation has to run
	// at this size: what the pools hold when nothing sizes them from the
	// instance is a pair of defaults written for a much larger one, and that
	// total does not fit.
	require.Greater(t, defaultThreads+defaultWriteThreads, threshold,
		"the thread flag defaults must not be mistaken for a size that fits the smallest instance")
}

// TestEveryPhasePoolFitsTheHardStopOnTheSmallestInstance is the sibling test
// widened from the copy phase to every phase that holds threads open against
// the target, because fitting under the hard-stop is only worth anything if it
// holds for all of them. A phase that does not fit puts the copy back in the
// regime the sizing exists to leave: its threads carry the sample over, nothing
// sheds one below MinVCPUs, and the run pays the backoff per loop.
//
// The counts are the ones the runner installs, not restatements of them — the
// derived read count becomes the copier's worker count and the checksum's
// Concurrency, and the derived write count becomes the applier's, which both the
// copy and a checksum repair write through.
//
// A new phase with its own pool belongs in this table. Two things deliberately
// stay out of it:
//
//   - The change-feed drain, which is a cap on the batches one flush keeps in
//     flight rather than a resident pool (see change.ClientConfig.FlushConcurrency),
//     so it holds threads only while there are pending changes to apply. Its
//     agreement with the change package's own default is pinned by
//     TestFlushBoundsPreservesChangeDefaults.
//   - Spirit's monitoring connections, which are what the mode headroom in the
//     threshold is for.
func TestEveryPhasePoolFitsTheHardStopOnTheSmallestInstance(t *testing.T) {
	const smallestInstanceVCPUs = 2
	threshold := throttler.MinThreadsThrottleThreshold(smallestInstanceVCPUs)
	pools, growable := sizePoolsFromInstance(smallestInstanceVCPUs, autoscale.ClientCeiling())
	require.False(t, growable, "nothing steers the pools at this size")

	for _, phase := range []struct {
		name    string
		threads int
	}{
		// The copy runs its read workers and the applier's write workers at
		// the same time.
		{"copy", pools.read + pools.write},
		// The checksum's own workers, plus the applier a mismatched chunk is
		// repaired through. The copier has stopped by then, so these do not
		// add to the copy's.
		{"checksum", pools.read + pools.write},
	} {
		require.LessOrEqualf(t, phase.threads, threshold,
			"%s phase: %d threads against a hard-stop that trips above %d",
			phase.name, phase.threads, threshold)
	}
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
	start, ceiling := dbconn.ReadBoundsForPool(16, 32, defaultMaxConnections, reserve)
	require.Equal(t, 16, start)
	require.Equal(t, 32, ceiling)

	// Exactly enough, counting the reserve the checksum phase cannot run without.
	start, ceiling = dbconn.ReadBoundsForPool(16, 32-reserve, 32, reserve)
	require.Equal(t, 16, start)
	require.Equal(t, 32-reserve, ceiling)

	// One short. The ceiling gives way, not the pool: the ceiling is a number
	// spirit derived for itself, the pool is one the operator set.
	_, ceiling = dbconn.ReadBoundsForPool(16, 32-reserve+1, 32, reserve)
	require.Equal(t, 32-reserve, ceiling)

	// The start is fitted too. This is the case that used to survive the fit:
	// a 96-vCPU instance derives (24, 48) from autoscale.ReadBounds, the
	// operator's pool is 20, and both consumers floor the ceiling back up to the
	// start — so lowering only the ceiling changed nothing.
	start, ceiling = dbconn.ReadBoundsForPool(24, 48, 20, reserve)
	require.Equal(t, 20-reserve, start)
	require.Equal(t, 20-reserve, ceiling)

	// Never zero readers. A pool too small to honour the reserve still has to
	// run: contending with the control plane beats not copying at all.
	start, ceiling = dbconn.ReadBoundsForPool(24, 48, reserve, reserve)
	require.Equal(t, 1, start)
	require.Equal(t, 1, ceiling)

	// No pool size resolved (a Runner built directly, bypassing
	// normalizeOptions): there is nothing to fit to, so nothing is changed.
	start, ceiling = dbconn.ReadBoundsForPool(24, 48, 0, reserve)
	require.Equal(t, 24, start)
	require.Equal(t, 48, ceiling)
	start, ceiling = dbconn.ReadBoundsForPool(24, 48, -1, reserve)
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
			start, ceiling := dbconn.ReadBoundsForPool(readStart, readCeiling, maxConnections, reserve)

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
