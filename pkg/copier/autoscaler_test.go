package copier

import (
	"context"
	"io"
	"log/slog"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/throttler"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// fakeScaler records SetWriteWorkers calls and reports them back.
type fakeScaler struct {
	n int
}

func (f *fakeScaler) SetWriteWorkers(n int) { f.n = n }

// utilThrottler is a GradualThrottler stub whose Utilization is scripted by
// the test. Only Utilization is exercised by the autoscaler; the rest satisfy
// the interface. The value is stored atomically so the integration test can
// move it while the autoscaler goroutine reads it concurrently (-race).
type utilThrottler struct{ utilBits atomic.Uint64 }

var _ throttler.GradualThrottler = &utilThrottler{}

func (u *utilThrottler) setUtil(v float64)               { u.utilBits.Store(math.Float64bits(v)) }
func (u *utilThrottler) Open(context.Context) error      { return nil }
func (u *utilThrottler) Close() error                    { return nil }
func (u *utilThrottler) IsThrottled() bool               { return u.Utilization() >= 1.0 }
func (u *utilThrottler) Utilization() float64            { return math.Float64frombits(u.utilBits.Load()) }
func (u *utilThrottler) BlockWait(context.Context)       {}
func (u *utilThrottler) UpdateLag(context.Context) error { return nil }

// fakeScalingApplier satisfies applier.Applier (embedded, never called) plus
// the writeScaler capability, mimicking the SingleTargetApplier for gate tests.
type fakeScalingApplier struct {
	applier.Applier
	fakeScaler
}

// fakeReadScaler records SetReadWorkers calls, mirroring fakeScaler.
type fakeReadScaler struct {
	n int
}

func (f *fakeReadScaler) SetReadWorkers(n int) { f.n = n }

// stubStats is a statsProvider whose snapshot is scripted by the test.
type stubStats struct {
	s applier.Stats
}

func (p *stubStats) Stats() applier.Stats { return p.s }

// Queue snapshots for the three arbitration states. Occupancy and wait/write
// relationships are chosen well inside each zone, away from the thresholds
// (TestClassifyQueue pins the boundaries).
func starvedStats() applier.Stats {
	return applier.Stats{QueueDepth: 0, QueueCap: 128, QueueWaitP90: 0, WriteTimeP90: 5 * time.Millisecond}
}

func fullStats() applier.Stats {
	return applier.Stats{QueueDepth: 120, QueueCap: 128, QueueWaitP90: 50 * time.Millisecond, WriteTimeP90: 5 * time.Millisecond}
}

func balancedStats() applier.Stats {
	return applier.Stats{QueueDepth: 64, QueueCap: 128, QueueWaitP90: 2 * time.Millisecond, WriteTimeP90: 5 * time.Millisecond}
}

func newTestScaler(start, max int) (*autoScaler, *fakeScaler, *utilThrottler) {
	fs := &fakeScaler{n: start}
	ut := &utilThrottler{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	as := newAutoScaler(ut, fs, start, max, logger, &metrics.NoopSink{})
	return as, fs, ut
}

// newTestDualScaler builds a controller with read scaling engaged: the dual
// law with a scripted queue snapshot arbitrating.
func newTestDualScaler(wStart, wMax, rStart, rMax int) (*autoScaler, *fakeScaler, *fakeReadScaler, *stubStats, *utilThrottler) {
	as, fs, ut := newTestScaler(wStart, wMax)
	fr := &fakeReadScaler{n: rStart}
	st := &stubStats{s: balancedStats()}
	as.enableReadScaling(fr, st, rStart, rMax)
	return as, fs, fr, st, ut
}

func TestAutoScaler_IncreasesBelowLowWatermarkAfterCooldown(t *testing.T) {
	as, fs, ut := newTestScaler(2, 8)
	ut.setUtil(0.2) // well below low watermark

	// First sub-low tick increases immediately (cooldown starts at 0).
	as.tick(t.Context())
	require.Equal(t, 3, as.current)
	require.Equal(t, 3, fs.n)

	// Cooldown is now in effect: the next ticks hold despite continued headroom.
	as.tick(t.Context())
	require.Equal(t, 3, as.current, "should hold during cooldown tick 1")
	as.tick(t.Context())
	require.Equal(t, 3, as.current, "should hold during cooldown tick 2")

	// Cooldown elapsed → increase again.
	as.tick(t.Context())
	require.Equal(t, 4, as.current)
}

func TestAutoScaler_ShedsOneAtHighWatermark(t *testing.T) {
	// Soft overload (at/above high, below panic) is an additive -1, the
	// mirror image of the increase path — NOT a halving. Halving on a signal
	// our own workers largely produce is the sawtooth from issue #831.
	as, fs, ut := newTestScaler(8, 16)
	ut.setUtil(0.8)

	as.tick(t.Context())
	require.Equal(t, 7, as.current, "first breach sheds one immediately")
	require.Equal(t, 7, fs.n)

	// Consecutive sheds are cooldown-spaced so the signal can catch up.
	as.tick(t.Context())
	require.Equal(t, 7, as.current, "should hold during cooldown tick 1")
	as.tick(t.Context())
	require.Equal(t, 7, as.current, "should hold during cooldown tick 2")

	as.tick(t.Context())
	require.Equal(t, 6, as.current, "cooldown elapsed, shed another")
}

func TestAutoScaler_HalvesAtPanicThreshold(t *testing.T) {
	as, fs, ut := newTestScaler(8, 16)
	ut.setUtil(1.2) // at/above panic: the hard-stop zone

	as.tick(t.Context())
	require.Equal(t, 4, as.current, "8 should halve to 4 immediately")
	require.Equal(t, 4, fs.n)

	// Consecutive halvings are cooldown-spaced: the signal updates on the same
	// cadence we tick on, so reacting every tick would halve repeatedly on one
	// stale window. Sustained overload halves again only after the cooldown.
	as.tick(t.Context())
	require.Equal(t, 4, as.current, "should hold during cooldown tick 1")
	as.tick(t.Context())
	require.Equal(t, 4, as.current, "should hold during cooldown tick 2")

	as.tick(t.Context())
	require.Equal(t, 2, as.current, "cooldown elapsed, halve again")
}

func TestAutoScaler_DecreaseNotBlockedByIncreaseCooldown(t *testing.T) {
	// An increase's cooldown must not delay a backoff: if the increase tipped
	// the server over the high watermark, the very next tick sheds — and over
	// the panic threshold, halves.
	as, _, ut := newTestScaler(4, 8)
	ut.setUtil(0.2)
	as.tick(t.Context())
	require.Equal(t, 5, as.current, "increase under low watermark")

	ut.setUtil(0.8)
	as.tick(t.Context())
	require.Equal(t, 4, as.current, "shed one immediately despite increase cooldown")

	ut.setUtil(0.2)
	as.tick(t.Context()) // hold: up cooldown from the shed
	as.tick(t.Context()) // hold
	as.tick(t.Context())
	require.Equal(t, 5, as.current, "increase again after cooldown")

	ut.setUtil(1.2)
	as.tick(t.Context())
	require.Equal(t, 3, as.current, "halve immediately despite increase cooldown: ceil(5/2)=3")
}

func TestAutoScaler_HoldsInDeadBand(t *testing.T) {
	as, _, ut := newTestScaler(4, 16)
	ut.setUtil(0.55) // between low (0.4) and high (0.7)

	for range 5 {
		as.tick(t.Context())
	}
	require.Equal(t, 4, as.current, "dead-band should hold steady")
}

func TestAutoScaler_ClampsAtMax(t *testing.T) {
	as, _, ut := newTestScaler(3, 4)
	ut.setUtil(0.0) // maximum headroom, always wants to increase

	// Drive many ticks; should climb to the cap and stop.
	for range 30 {
		as.tick(t.Context())
	}
	require.Equal(t, 4, as.current)
}

func TestAutoScaler_ClampsAtMinOne(t *testing.T) {
	as, _, ut := newTestScaler(2, 8)
	ut.setUtil(1.5) // way over

	for range 10 {
		as.tick(t.Context())
	}
	require.Equal(t, 1, as.current, "must never drop below 1")
}

func TestAutoScaler_MaxFlooredAtStart(t *testing.T) {
	// A max below the start value is nonsensical; it must be floored at start so
	// we never scale below where we began except via the >high backoff path.
	as, _, _ := newTestScaler(6, 2)
	require.Equal(t, 6, as.max)
}

// TestAutoScaler_DeadBandBoundaries pins the documented zone-edge semantics:
// tick() uses `util < low` for increases, `util >= high` for the additive
// shed, and `util >= panic` for the halve — so exactly-low must HOLD,
// exactly-high must SHED ONE, and exactly-panic must HALVE. The epsilon cases
// guard against any comparison being accidentally flipped to <= / >.
func TestAutoScaler_DeadBandBoundaries(t *testing.T) {
	const eps = 1e-9
	tests := []struct {
		name string
		util float64
		want int // expected current after one tick, starting from 4
	}{
		{"just below low increases", acLowWatermark - eps, 5},
		{"exactly low holds", acLowWatermark, 4},
		{"just below high holds", acHighWatermark - eps, 4},
		{"exactly high sheds one", acHighWatermark, 3},
		{"just below panic sheds one", acPanicThreshold - eps, 3},
		{"exactly panic halves", acPanicThreshold, 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			as, _, ut := newTestScaler(4, 16)
			ut.setUtil(tc.util)
			as.tick(t.Context())
			require.Equal(t, tc.want, as.current)
		})
	}
}

// TestAutoScaler_StaleHoldValueParksInDeadBand pins the cross-package
// invariant the staleness guard depends on: the utilization a stale throttler
// reports (throttler.StaleUtilizationHold) must sit inside this controller's
// dead band [low, high), so a dead signal freezes the thread count rather
// than ramping it to the cap or shrinking it to 1. If either side moves and
// breaks the relationship, this fails loudly.
func TestAutoScaler_StaleHoldValueParksInDeadBand(t *testing.T) {
	require.GreaterOrEqual(t, throttler.StaleUtilizationHold, acLowWatermark,
		"stale hold below the low watermark would scale up blind on a dead signal")
	require.Less(t, throttler.StaleUtilizationHold, acHighWatermark,
		"stale hold at/above the high watermark would halve on a dead signal")

	as, _, ut := newTestScaler(4, 16)
	ut.setUtil(throttler.StaleUtilizationHold)
	for range 5 {
		as.tick(t.Context())
	}
	require.Equal(t, 4, as.current, "stale hold utilization must freeze the thread count")
}

// TestAutoScaler_ConvergesOnSelfInducedSignal replays the failure mode seen
// in staging (issue #831): on an otherwise idle server the utilization signal
// is produced almost entirely by the controller's own write threads, plus
// sampling noise from housekeeping queries and worker duty-cycle flicker. The
// old halve-at-the-watermark controller sawtoothed on this loop indefinitely
// (ramp to the watermark, halve, ramp again). The reworked controller must
// converge into the dead band and then hold the thread count steady despite
// the noise.
func TestAutoScaler_ConvergesOnSelfInducedSignal(t *testing.T) {
	const vCPUs = 8.0
	as, _, ut := newTestScaler(2, 16)

	noise := []float64{-0.1, 0.1, 0.05, -0.05}
	last, stableFor := 0, 0
	for i := range 200 {
		// Self-induced load: each write thread contributes ~one active thread
		// (worst-case duty cycle), plus alternating sampling noise.
		ut.setUtil(float64(as.current)/vCPUs + noise[i%len(noise)])
		as.tick(t.Context())
		if as.current == last {
			stableFor++
		} else {
			last, stableFor = as.current, 0
		}
	}
	require.GreaterOrEqual(t, stableFor, 150,
		"controller must converge once and then hold steady on a self-induced signal")
	require.Equal(t, 4, as.current,
		"steady state parks just above the low watermark: 4 threads / 8 vCPUs = 0.5")
}

func TestCeilDiv(t *testing.T) {
	require.Equal(t, 1, ceilDiv(1, 2))
	require.Equal(t, 1, ceilDiv(2, 2))
	require.Equal(t, 2, ceilDiv(3, 2))
	require.Equal(t, 2, ceilDiv(4, 2))
	require.Equal(t, 3, ceilDiv(5, 2))
}

// TestResolveMaxReadThreads pins the pool-size formula the migration runner
// budgets connections with: dropping the doubling would silently queue
// scaled-up readers on the sql.DB pool (the runner and the copier's reader
// cap must agree, and both call this).
func TestResolveMaxReadThreads(t *testing.T) {
	// Autoscaling disabled: the pool cannot move, so the cap is the start.
	require.Equal(t, 4, ResolveMaxReadThreads(4, false))
	require.Equal(t, 1, ResolveMaxReadThreads(1, false))
	// Autoscaling enabled: 2x the start, mirroring ResolveMaxWriteThreads.
	require.Equal(t, 8, ResolveMaxReadThreads(4, true))
	require.Equal(t, 2, ResolveMaxReadThreads(1, true))
}

// TestEnableReadScalingClamps pins the defensive clamps: a start below 1 is
// raised to 1 (the copy must keep making progress) and a cap below the start
// is raised to the start (mirroring newAutoScaler's write-side clamp).
func TestEnableReadScalingClamps(t *testing.T) {
	as, _, _ := newTestScaler(2, 4)
	as.enableReadScaling(&fakeReadScaler{}, &stubStats{}, 0, 0)
	require.Equal(t, 1, as.readCurrent, "start must clamp up to 1")
	require.Equal(t, 1, as.readMin)
	require.Equal(t, 1, as.readMax, "cap must clamp up to the (clamped) start")

	as, _, _ = newTestScaler(2, 4)
	as.enableReadScaling(&fakeReadScaler{}, &stubStats{}, 4, 2)
	require.Equal(t, 4, as.readCurrent)
	require.Equal(t, 4, as.readMax, "cap below start must be raised to start")
}

// TestClassifyQueue pins the arbitration thresholds and their boundary
// semantics: starved needs occupancy <= acQueueStarvedOccupancy AND
// queue-wait strictly under the epsilon; full needs occupancy >=
// acQueueFullOccupancy AND queue-wait at/above write time AND at least one
// completed write as evidence (both-zero percentiles must not read as full);
// a zero-cap snapshot (applier not started) must read balanced, never starved.
func TestClassifyQueue(t *testing.T) {
	ms := func(n int) time.Duration { return time.Duration(n) * time.Millisecond }
	tests := []struct {
		name string
		s    applier.Stats
		want queueState
	}{
		{"zero cap is balanced", applier.Stats{QueueDepth: 0, QueueCap: 0}, queueBalanced},
		{"empty queue zero wait is starved", applier.Stats{QueueDepth: 0, QueueCap: 100, QueueWaitP90: 0}, queueStarved},
		{"exactly starved occupancy is starved", applier.Stats{QueueDepth: 10, QueueCap: 100, QueueWaitP90: 0}, queueStarved},
		{"above starved occupancy is balanced", applier.Stats{QueueDepth: 11, QueueCap: 100, QueueWaitP90: 0}, queueBalanced},
		{"empty queue but wait at epsilon is balanced", applier.Stats{QueueDepth: 0, QueueCap: 100, QueueWaitP90: acQueueWaitEpsilon}, queueBalanced},
		{"exactly full occupancy with wait >= write is full", applier.Stats{QueueDepth: 80, QueueCap: 100, QueueWaitP90: ms(10), WriteTimeP90: ms(10)}, queueFull},
		{"full occupancy with zero write evidence is balanced", applier.Stats{QueueDepth: 100, QueueCap: 100, QueueWaitP90: 0, WriteTimeP90: 0}, queueBalanced},
		{"full occupancy with waits but no completed write is balanced", applier.Stats{QueueDepth: 100, QueueCap: 100, QueueWaitP90: ms(50), WriteTimeP90: 0}, queueBalanced},
		{"below full occupancy is balanced", applier.Stats{QueueDepth: 79, QueueCap: 100, QueueWaitP90: ms(50), WriteTimeP90: ms(10)}, queueBalanced},
		{"full occupancy but wait below write is balanced", applier.Stats{QueueDepth: 90, QueueCap: 100, QueueWaitP90: ms(5), WriteTimeP90: ms(10)}, queueBalanced},
		{"mid occupancy is balanced", applier.Stats{QueueDepth: 50, QueueCap: 100, QueueWaitP90: ms(3), WriteTimeP90: ms(5)}, queueBalanced},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, classifyQueue(tc.s))
		})
	}
}

func TestDualAutoScaler_StarvedGrowsReaders(t *testing.T) {
	as, fs, fr, st, ut := newTestDualScaler(2, 4, 2, 4)
	ut.setUtil(0.2) // headroom: the queue decides which pool grows
	st.s = starvedStats()

	// Tick 1: starved observed but not yet persisted → hold.
	as.tick(t.Context())
	require.Equal(t, 2, as.readCurrent, "unconfirmed queue state must not arbitrate")
	require.Equal(t, 2, as.current)

	// Tick 2: starved confirmed → grow the read pool, not the write pool.
	as.tick(t.Context())
	require.Equal(t, 3, as.readCurrent)
	require.Equal(t, 3, fr.n, "SetReadWorkers must be driven")
	require.Equal(t, 2, as.current, "write pool must not grow on a starved queue")
	require.Equal(t, 2, fs.n)

	// Cooldown, then grow again.
	as.tick(t.Context())
	as.tick(t.Context())
	require.Equal(t, 3, as.readCurrent, "cooldown must space reader increases")
	as.tick(t.Context())
	require.Equal(t, 4, as.readCurrent)
}

func TestDualAutoScaler_FullGrowsWriters(t *testing.T) {
	as, _, fr, st, ut := newTestDualScaler(2, 4, 2, 4)
	ut.setUtil(0.2)
	st.s = fullStats()

	as.tick(t.Context()) // observe, unconfirmed → hold
	require.Equal(t, 2, as.current)
	as.tick(t.Context()) // confirmed → +1 writer
	require.Equal(t, 3, as.current)
	require.Equal(t, 2, as.readCurrent, "read pool must not grow on a full queue")
	require.Equal(t, 2, fr.n)
}

func TestDualAutoScaler_BalancedHoldsDespiteHeadroom(t *testing.T) {
	// With read scaling engaged, a balanced queue holds both pools even under
	// utilization headroom — growing either side of a balanced pipeline just
	// moves the queue off its equilibrium. (Write-only mode keeps growing;
	// TestAutoScaler_IncreasesBelowLowWatermarkAfterCooldown pins that.)
	as, _, _, st, ut := newTestDualScaler(2, 8, 2, 8)
	ut.setUtil(0.1)
	st.s = balancedStats()

	for range 6 {
		as.tick(t.Context())
	}
	require.Equal(t, 2, as.current, "balanced queue must hold the write pool")
	require.Equal(t, 2, as.readCurrent, "balanced queue must hold the read pool")
}

func TestDualAutoScaler_FlappingStateNeverArbitrates(t *testing.T) {
	// A queue state must persist acQueueStatePersistTicks consecutive ticks
	// before it acts. Alternating starved/full every tick — transient
	// chunk-size swings — must leave both pools untouched.
	as, _, _, st, ut := newTestDualScaler(2, 8, 2, 8)
	ut.setUtil(0.1)

	for i := range 10 {
		if i%2 == 0 {
			st.s = starvedStats()
		} else {
			st.s = fullStats()
		}
		as.tick(t.Context())
	}
	require.Equal(t, 2, as.current)
	require.Equal(t, 2, as.readCurrent)
}

func TestDualAutoScaler_ShedBlamesStarvedReadSide(t *testing.T) {
	// Soft overload with a starved queue: the writers are idle, so the load
	// is coming from the read side — shed a reader, not a writer. The state
	// is confirmed in the dead band first (observeQueue runs every tick,
	// whatever the utilization zone).
	as, fs, fr, st, ut := newTestDualScaler(4, 8, 4, 8)
	st.s = starvedStats()
	ut.setUtil(0.55) // dead band: confirm the state without acting
	as.tick(t.Context())
	as.tick(t.Context())
	require.Equal(t, 4, as.current)
	require.Equal(t, 4, as.readCurrent)

	ut.setUtil(0.8)
	as.tick(t.Context())
	require.Equal(t, 3, as.readCurrent, "starved queue blames the read side")
	require.Equal(t, 3, fr.n)
	require.Equal(t, 4, as.current, "write pool must be untouched")
	require.Equal(t, 4, fs.n)
}

func TestDualAutoScaler_ShedDefaultsToWriteSide(t *testing.T) {
	// Soft overload with a balanced (or unconfirmed) queue sheds a writer —
	// the write-only default.
	as, _, fr, st, ut := newTestDualScaler(4, 8, 4, 8)
	st.s = balancedStats()
	ut.setUtil(0.8)

	as.tick(t.Context())
	require.Equal(t, 3, as.current)
	require.Equal(t, 4, as.readCurrent)
	require.Equal(t, 4, fr.n)
}

func TestDualAutoScaler_ShedFallsThroughAtReaderFloor(t *testing.T) {
	// Soft overload with a confirmed-starved queue but the read pool already
	// at its floor: blaming the reader would clamp into a no-op that still
	// burns the cooldowns — a phantom action. The blame must fall through to
	// the writer so the zone actually sheds a thread.
	as, fs, fr, st, ut := newTestDualScaler(4, 8, 1, 8)
	st.s = starvedStats()
	ut.setUtil(0.55) // dead band: confirm the state without acting
	as.tick(t.Context())
	as.tick(t.Context())

	ut.setUtil(0.8)
	as.tick(t.Context())
	require.Equal(t, 1, as.readCurrent, "read pool must hold at its floor")
	require.Equal(t, 1, fr.n)
	require.Equal(t, 3, as.current, "shed must fall through to the write pool")
	require.Equal(t, 3, fs.n)
}

func TestDualAutoScaler_ShedUnconfirmedStarvedShedsWriter(t *testing.T) {
	// Soft overload on the very first starved observation: the state has not
	// persisted yet, so it must not arbitrate — the shed lands on the writer
	// (the write-only default), not the reader.
	as, _, fr, st, ut := newTestDualScaler(4, 8, 4, 8)
	st.s = starvedStats()
	ut.setUtil(0.8)

	as.tick(t.Context())
	require.Equal(t, 3, as.current, "unconfirmed starved state must shed a writer")
	require.Equal(t, 4, as.readCurrent, "read pool must be untouched")
	require.Equal(t, 4, fr.n)
}

func TestDualAutoScaler_PanicHalvesBothPools(t *testing.T) {
	as, fs, fr, st, ut := newTestDualScaler(8, 16, 6, 12)
	st.s = balancedStats()
	ut.setUtil(1.2)

	as.tick(t.Context())
	require.Equal(t, 4, as.current, "write pool halves at panic")
	require.Equal(t, 4, fs.n)
	require.Equal(t, 3, as.readCurrent, "read pool halves at panic")
	require.Equal(t, 3, fr.n)

	// Cooldown-spaced like the write-only law.
	as.tick(t.Context())
	as.tick(t.Context())
	require.Equal(t, 3, as.readCurrent)
	as.tick(t.Context())
	require.Equal(t, 2, as.readCurrent)
	require.Equal(t, 2, as.current)
}

func TestDualAutoScaler_ReaderBounds(t *testing.T) {
	// Sustained starvation with maximum headroom climbs the read pool to its
	// cap and stops; sustained panic floors both pools at 1.
	as, _, fr, st, ut := newTestDualScaler(2, 4, 2, 4)
	st.s = starvedStats()
	ut.setUtil(0.0)
	for range 30 {
		as.tick(t.Context())
	}
	require.Equal(t, 4, as.readCurrent, "read pool must clamp at its cap")
	require.Equal(t, 4, fr.n)
	require.Equal(t, 2, as.current, "write pool never grows while starved")

	ut.setUtil(1.5)
	for range 10 {
		as.tick(t.Context())
	}
	require.Equal(t, 1, as.readCurrent, "read pool must never drop below 1")
	require.Equal(t, 1, as.current, "write pool must never drop below 1")
}

func TestDualAutoScaler_SharedCooldownAcrossPools(t *testing.T) {
	// The up cooldown is shared: a reader increase delays a subsequent writer
	// increase just like another reader increase — one action per window,
	// whichever side it lands on.
	as, _, _, st, ut := newTestDualScaler(2, 8, 2, 8)
	ut.setUtil(0.1)
	st.s = starvedStats()
	as.tick(t.Context()) // observe starved (unconfirmed)
	as.tick(t.Context()) // confirmed → +1 reader, upCooldown starts
	require.Equal(t, 3, as.readCurrent)

	st.s = fullStats()
	as.tick(t.Context()) // full unconfirmed; cooldown 2→1
	as.tick(t.Context()) // full confirmed but cooldown 1→0: hold
	require.Equal(t, 2, as.current, "writer increase must wait out the reader increase's cooldown")
	as.tick(t.Context()) // cooldown elapsed → +1 writer
	require.Equal(t, 3, as.current)
	require.Equal(t, 3, as.readCurrent, "read pool unchanged by the writer increase")
}

// TestAutoscalerIfEnabled_Gating covers the three conditions that must all
// hold for the autoscaler to engage: the flag is on, the applier supports
// dynamic write threads, and the throttler provides a continuous load signal
// (GradualThrottler). Missing any one of them means a fixed pool.
func TestAutoscalerIfEnabled_Gating(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	gradual := &utilThrottler{}
	scalingApplier := &fakeScalingApplier{}

	// Disabled (the default): no autoscaler.
	c := &buffered{logger: logger, throttler: gradual, applier: scalingApplier, concurrency: 3}
	require.Nil(t, c.autoscalerIfEnabled())

	// Enabled + scaling applier + gradual throttler: engages with the
	// configured bounds — and read scaling engages alongside, bounded at
	// 2x the copier's read concurrency.
	c.autoscale = AutoscaleConfig{Enabled: true, StartThreads: 2, MaxThreads: 4}
	as := c.autoscalerIfEnabled()
	require.NotNil(t, as)
	require.Equal(t, 2, as.current)
	require.Equal(t, 4, as.max)
	require.NotNil(t, as.reader, "read scaling must engage with the write side")
	require.Equal(t, 3, as.readCurrent)
	require.Equal(t, 6, as.readMax)

	// Binary-only throttler (Noop here; replica lag and Mock behave the same):
	// no continuous signal to control on, so the pool stays fixed.
	c.throttler = &throttler.Noop{}
	require.Nil(t, c.autoscalerIfEnabled())

	// Applier without the dynamic-scaling capability: stays fixed.
	c.throttler = gradual
	c.applier = nil
	require.Nil(t, c.autoscalerIfEnabled())
}

// gatedUtilThrottler extends utilThrottler with a BlockWait that parks the
// copier's read workers until the test closes the gate. That keeps the copy
// alive (without burning rows) while the autoscaler — which ticks
// independently of chunk flow — makes its scaling decisions, so the test can
// observe them deterministically before letting the copy finish.
type gatedUtilThrottler struct {
	utilThrottler
	gate chan struct{}
}

func (g *gatedUtilThrottler) BlockWait(ctx context.Context) {
	select {
	case <-g.gate:
	case <-ctx.Done():
	}
}

// TestAutoScalerIntegrationEngaged runs the autoscaler for real: a buffered
// copy of a real table through a real SingleTargetApplier, with the
// autoscaler goroutine (run/tick on a ticker) driving the dual control law
// from a test-controlled utilization signal. The gated throttler parks the
// readers, so the applier queue stays empty (read-starved): under low
// utilization the controller must grow the live READ-worker pool to its cap
// while leaving the write pool alone, and at the panic threshold it must
// halve both pools. It then asserts the copy completes correctly. goleak in
// TestMain verifies nothing leaks.
func TestAutoScalerIntegrationEngaged(t *testing.T) {
	// Shorten the control-loop tick (production default 5s) so scaling
	// happens in milliseconds. Copier tests do not run in parallel, so
	// mutating the package var with a restore is safe.
	prevTick := acTick
	acTick = 20 * time.Millisecond
	t.Cleanup(func() { acTick = prevTick })

	testutils.RunSQL(t, "DROP TABLE IF EXISTS autoscale_src, autoscale_dst")
	testutils.RunSQL(t, "CREATE TABLE autoscale_src (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, val VARCHAR(64) NOT NULL)")
	testutils.RunSQL(t, "CREATE TABLE autoscale_dst (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, val VARCHAR(64) NOT NULL)")
	// Seed a few thousand small rows by doubling: 2^12 = 4096.
	testutils.RunSQL(t, "INSERT INTO autoscale_src (val) VALUES ('seed')")
	for range 12 {
		testutils.RunSQL(t, "INSERT INTO autoscale_src (val) SELECT val FROM autoscale_src")
	}

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	// Resolve the schema from the DSN rather than hardcoding it, so the test
	// works against any test database.
	dsnCfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	t1 := table.NewTableInfo(db, dsnCfg.DBName, "autoscale_src")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, dsnCfg.DBName, "autoscale_dst")
	require.NoError(t, t2.SetInfo(t.Context()))

	const start, maxThreads = 2, 4 // mirrors ResolveMaxWriteThreads: cap = 2x start

	applierCfg := applier.NewApplierDefaultConfig()
	applierCfg.Threads = start
	app, err := applier.NewSingleTargetApplier(applier.Target{DB: db, KeyRange: "0"}, applierCfg)
	require.NoError(t, err)

	gated := &gatedUtilThrottler{gate: make(chan struct{})}
	gated.setUtil(0.2) // below the low watermark: the controller wants to grow

	cfg := NewCopierDefaultConfig()
	cfg.Applier = app
	cfg.Throttler = gated
	cfg.Concurrency = 2
	cfg.Autoscale = AutoscaleConfig{Enabled: true, StartThreads: start, MaxThreads: maxThreads}

	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2, TargetChunkTime: cfg.TargetChunkTime, Logger: cfg.Logger})
	require.NoError(t, err)
	require.NoError(t, chunker.Open())
	copier, err := NewCopier(db, chunker, cfg)
	require.NoError(t, err)

	copyDone := make(chan error, 1)
	go func() { copyDone <- copier.Run(t.Context()) }()
	buf, ok := copier.(*buffered)
	require.True(t, ok)

	// Phase 1: sustained low utilization with a read-starved queue (readers
	// parked in BlockWait, so no chunklets ever reach the applier) → the
	// queue arbitrates the headroom to the READ pool: additive +1 per
	// cooldown until it reaches its cap (2x the starting concurrency).
	// ActiveReadWorkers observes the real goroutine pool, so this proves
	// run() drove SetReadWorkers on the copier (not just controller-internal
	// state). Newly spawned readers park in BlockWait alongside the others.
	const maxReadThreads = 2 * 2 // mirrors autoscalerIfEnabled: 2x Concurrency
	require.Eventually(t, func() bool { return buf.ActiveReadWorkers() == maxReadThreads },
		10*time.Second, 5*time.Millisecond,
		"autoscaler should grow the live read-worker pool to the cap under low utilization with a starved queue")
	// The write pool must not have grown: the starved queue blames the read
	// side, and balanced/unconfirmed states hold.
	require.Equal(t, start, app.ActiveWriteWorkers(),
		"write pool must stay at its starting size while the queue is read-starved")

	// Phase 2: utilization at/above the panic threshold → multiplicative
	// backoff of BOTH pools. Parked write workers exit asynchronously, so
	// wait for convergence. (Sustained overload may halve again,
	// cooldown-spaced, hence <=.) The reader halve is issued too, but parked
	// readers cannot exit while the throttler gate holds them in BlockWait —
	// their prompt exit-on-park is pinned by the PR 1 pool tests — so the
	// observable assertion here is on the write pool.
	gated.setUtil(1.2)
	require.Eventually(t, func() bool { return app.ActiveWriteWorkers() <= ceilDiv(start, 2) },
		10*time.Second, 5*time.Millisecond,
		"autoscaler should halve the live write-worker pool at the panic threshold")

	// Park the signal in the dead band and release the gate: the copy now
	// proceeds to completion with the scaled-down pool.
	gated.setUtil(0.55)
	close(gated.gate)
	select {
	case err := <-copyDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Minute):
		t.Fatal("copy did not complete after releasing the throttler gate")
	}

	// The copy must be complete and correct despite the mid-copy rescaling.
	var srcRows, dstRows int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM autoscale_src").Scan(&srcRows))
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM autoscale_dst").Scan(&dstRows))
	require.Equal(t, 4096, srcRows)
	require.Equal(t, srcRows, dstRows, "destination row count must match source after autoscaled copy")

	var checksumSrc, checksumDst string
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT BIT_XOR(CRC32(CONCAT(id, val))) FROM autoscale_src").Scan(&checksumSrc))
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT BIT_XOR(CRC32(CONCAT(id, val))) FROM autoscale_dst").Scan(&checksumDst))
	require.Equal(t, checksumSrc, checksumDst, "checksum mismatch between source and destination")
	testutils.RunSQL(t, "DROP TABLE IF EXISTS autoscale_src, autoscale_dst")
}
