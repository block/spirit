package throttler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync/atomic"
	"time"

	"github.com/block/spirit/pkg/autoscale"
)

// The Aurora "threads" throttler — the second Aurora signal, from issue #831.
//
// Like gh-ost and pt-osc, we use a running-thread count to detect server
// busyness and back off when the box is under load. What's different from those
// tools is that we compare the count to the instance vCPU count — which on
// Aurora we read deterministically from @@innodb_buffer_pool_instances — and
// that we feed it to the write-thread autoscaler as a continuous utilization
// signal, not only as a binary stop/go.
//
// The throttler runs in one of two modes, chosen once at setup by
// AuroraSetup.Build from a privilege probe (see CanReadRedoAwareThreads):
//
//   - redoAwareMode (preferred): counts Query-state threads from
//     performance_schema and SUBTRACTS those parked on redo-log flush. Those
//     waiters are IO-bound, not on CPU, and Aurora group-commits them to
//     storage, so the redo log can be safely oversubscribed — the #971 staging
//     A/B measured ~18% more copy throughput this way (~12 workers / ~25m vs ~8
//     workers / ~30.5m on an r6g.4xlarge). It requires SELECT on
//     performance_schema.threads and events_waits_current.
//
//   - globalStatusMode (fallback): counts ALL running threads via the
//     Threads_running status variable from global_status. It needs no grant
//     beyond the global_status access that IsAurora and the commit-latency
//     throttler already require, so it is always available once Aurora is
//     detected. Redo-log waiters read as load here, so it caps concurrency near
//     vCPUs rather than oversubscribing the log — more conservative, but it
//     works for accounts without the perf-schema grants above.
//
// History: this began (issue #831) as the redo-aware signal, was briefly
// replaced wholesale by Threads_running when staging diagnostics were misread as
// showing the redo-flush wait event never matched on Aurora (commit 11afb7bf),
// then the #971 A/B established that the event does match on Aurora and the
// exclusion works (commit 893f49f7). This revives the redo-aware signal as the
// default while keeping Threads_running as the no-extra-grant fallback.
//
// The commit-latency throttler, reading the same global_status, watches storage
// saturation directly; in redoAwareMode it is also the backstop that lets the
// autoscaler scale write threads above the starting value (see
// ResolveMaxWriteThreads).
const (
	// redoAwareThreadsQuery counts query-running threads on the server and
	// subtracts those parked on redo-log waits. LEFT JOIN handles the case
	// where events_waits_current has no row for a thread (e.g., the wait
	// consumer is disabled) — that thread is then counted as on-CPU, which is
	// the safe-conservative behavior.
	//
	// The sampling connection excludes itself (CONNECTION_ID()) — without this
	// the monitor's own query always counts as one active thread, which on
	// small instances (vCPUs=2) eats half the headroom and can throttle a
	// migration with no other activity on the server. The sibling
	// commit-latency poller on the shared monitor pool is NOT excluded (it has
	// a different connection id); redoAwareMode's headroom covers it.
	//
	// wait/io/redo_log_flush is the Aurora redo-flush wait event; it does not
	// exist on community MySQL, but this query only runs on Aurora (gated by
	// IsAurora). The #971 staging A/B confirmed the event matches on Aurora and
	// that excluding these waiters oversubscribes the redo log for ~18% more
	// copy throughput.
	redoAwareThreadsQuery = `SELECT
		GREATEST(COUNT(*) - COALESCE(SUM(CASE WHEN ewc.EVENT_NAME = 'wait/io/redo_log_flush' AND ewc.END_EVENT_ID IS NULL THEN 1 ELSE 0 END), 0), 0) AS active_cpu_threads
	FROM performance_schema.threads pps
	LEFT JOIN performance_schema.events_waits_current ewc ON pps.THREAD_ID = ewc.THREAD_ID
	WHERE pps.PROCESSLIST_ID IS NOT NULL
	  AND pps.PROCESSLIST_ID <> CONNECTION_ID()
	  AND pps.PROCESSLIST_COMMAND = 'Query'`

	// threadsRunningQuery reads the Threads_running status variable: the count
	// of non-sleeping threads. It comes from performance_schema.global_status
	// (the same source as the commit-latency throttler and IsAurora), so it
	// needs no privilege beyond what those already require.
	//
	// Self-counting: unlike redoAwareThreadsQuery this cannot exclude spirit's
	// own connections — the monitor's polling queries (this one, plus the
	// commit-latency poller / GTID-changeset flush) each count as a running
	// thread at the instant they sample. The EWMA Utilization() signal absorbs
	// this small over-count and the autoscaler only engages at 4+ vCPUs where
	// ±1–2 is in the noise, so Utilization carries no correction. The binary
	// hard-stop, however, has no averaging to lean on and must work down to the
	// smallest instances, so globalStatusMode compensates explicitly with
	// selfMonitoringHeadroom.
	threadsRunningQuery = `SELECT CAST(VARIABLE_VALUE AS UNSIGNED)
	FROM performance_schema.global_status
	WHERE VARIABLE_NAME = 'Threads_running'`

	// auroraVCPUsQuery reads the vCPU count from @@innodb_buffer_pool_instances,
	// which Aurora pins to the instance vCPU count (see issue #831).
	//
	// Do NOT use @@innodb_purge_threads for this: Aurora sizes it by a
	// stepped formula, not the vCPU count (r7g family: large=1, xlarge=1,
	// 2xlarge=3, 4xlarge=3, 8xlarge=6, 16xlarge=12), so it badly
	// under-reads CPU capacity at every size. On community MySQL
	// innodb_buffer_pool_instances is sized by buffer pool size / CPU
	// hints instead — we gate the throttler on IsAurora, so this query is
	// only ever issued there.
	auroraVCPUsQuery = `SELECT @@innodb_buffer_pool_instances`
)

// selfMonitoringHeadroom is the number of threads added to the vCPU count to
// form globalStatusMode's hard-stop threshold, so the throttle fires on the
// production workload saturating the box — not on spirit's own baseline.
//
// Threads_running is a whole-server gauge, so it includes spirit's own
// monitoring connections: this throttler's polling query is itself a running
// thread at the instant it samples (see threadsRunningQuery), and the
// commit-latency poller and periodic GTID-changeset flush share the monitor
// pool. That self-count is ~1–2 threads and does not grow with the instance.
//
// On 4+ vCPU instances that over-count is in the noise. On small instances it
// is the entire margin: at vCPUs=2 spirit's own polling alone holds
// Threads_running at or above a bare vCPU threshold, so the copy throttles
// against itself with zero production load and creeps forward only via
// BlockWait's 60s backoff (the "allowing one copy loop to make progress" log).
// This fixed headroom lets the copy run when only spirit is busy while still
// backing off once the production workload piles real work onto the box. The
// copy's own read/apply threads are deliberately NOT excluded — those are
// genuine CPU load that should count toward saturation; only spirit's
// (instance-size-independent) monitoring footprint is.
//
// redoAwareMode excludes its own sampling connection inside the query, so it
// carries a smaller headroom (just the sibling commit-latency poller) — see
// redoAwareMode.
const selfMonitoringHeadroom = 2

// threadsMode selects how the Aurora threads throttler counts server load and
// how it accounts for spirit's own monitoring footprint. The throttler is fixed
// to one mode at construction (chosen by AuroraSetup.Build from a privilege
// probe) and never switches at runtime.
type threadsMode struct {
	// query returns the current count of on-CPU threads to compare against the
	// instance vCPU count.
	query string
	// headroom is added to vCPUs to form the hard-stop threshold, covering the
	// spirit monitoring threads that query cannot exclude.
	headroom int64
	// label names the signal in logs and error strings.
	label string
}

var (
	// redoAwareMode is the preferred signal: it counts Query-state threads from
	// performance_schema and subtracts those parked on redo-log flush (safe to
	// oversubscribe — they are IO-bound, not on CPU). Requires SELECT on
	// performance_schema.threads and events_waits_current. The query already
	// excludes its own sampling connection (CONNECTION_ID()), so only 1 of
	// headroom is needed — to cover the sibling commit-latency poller on the
	// shared monitor pool, which it cannot single out.
	redoAwareMode = threadsMode{query: redoAwareThreadsQuery, headroom: 1, label: "redo-aware"}

	// globalStatusMode is the fallback when the perf-schema grants above are
	// absent: it counts ALL running threads via Threads_running from
	// global_status. It cannot exclude any of spirit's monitoring threads, so
	// it carries the full selfMonitoringHeadroom.
	globalStatusMode = threadsMode{query: threadsRunningQuery, headroom: selfMonitoringHeadroom, label: "threads-running"}
)

// AuroraVCPUs reads the instance vCPU count from @@innodb_buffer_pool_instances,
// which Aurora pins to the vCPU count (issue #831). It returns an error if the
// value is non-positive. This is only meaningful on Aurora — callers should gate
// on IsAurora first.
func AuroraVCPUs(ctx context.Context, db *sql.DB) (int, error) {
	var vCPUs int
	if err := db.QueryRowContext(ctx, auroraVCPUsQuery).Scan(&vCPUs); err != nil {
		return 0, fmt.Errorf("reading @@innodb_buffer_pool_instances for vCPU count: %w", err)
	}
	if vCPUs <= 0 {
		return 0, fmt.Errorf("@@innodb_buffer_pool_instances returned non-positive value %d", vCPUs)
	}
	return vCPUs, nil
}

// ResolveMaxWriteThreads resolves the upper bound the write-thread autoscaler
// may scale to: autoscale.Ceiling (start when scaling is off, 2 × start when on
// — deliberately not configurable for now, to keep the experimental surface
// small), plus one rule that is specific to the write side. See issue #831.
//
// That rule: scaling above the starting value additionally requires the
// commit-latency throttler when the redo-aware signal is in use. That signal
// deliberately ignores threads parked on redo-log waits — which is what makes
// oversubscribing the log safe to attempt — so it will not self-limit if the
// extra write threads saturate the log, and commit-latency is then the only
// signal that would notice. Without that backstop the cap stays at start (the
// autoscaler may still shed threads under CPU pressure, but never adds any). The
// Threads_running fallback counts redo-log waiters as load, so it self-limits and
// needs no such backstop.
func ResolveMaxWriteThreads(start int, autoscaleEnabled, redoAware, commitLatencyEnabled bool) int {
	unguardedRedo := redoAware && !commitLatencyEnabled
	return autoscale.Ceiling(start, autoscaleEnabled && !unguardedRedo)
}

// threadsRunningPollInterval mirrors commitLatencyPollInterval — fast enough to
// catch sustained pressure without hammering the source. Var (not const) so
// tests can shorten it.
var threadsRunningPollInterval = 5 * time.Second

// threadsRunningEWMAAlpha is the smoothing factor for the exponentially
// weighted moving average that Utilization() reports. The thread count is an
// instantaneous gauge with high variance — it blips on brief latch/lock waits
// and on spirit's own housekeeping (checkpoints, GTID flushes, status
// queries). The autoscaler must control on the sustained average, not the
// variance — the commit-latency sibling signal is already window-averaged for
// the same reason. 0.3 gives a time constant of ~3 samples (~15s at the 5s
// poll), matching the autoscaler's cooldown spacing so the controller never
// acts twice on effectively the same information.
//
// Only Utilization() is smoothed. IsThrottled/BlockWait stay on the raw
// sample: the hard-stop protects the production workload and must react
// within one poll.
const threadsRunningEWMAAlpha = 0.3

// CanReadRedoAwareThreads probes whether the current user can run the redo-aware
// query. Returns nil when it runs cleanly, or a wrapped error when it fails
// (typically: SELECT on performance_schema.threads or events_waits_current
// denied). AuroraSetup.Build uses it to choose the redo-aware signal when the
// grants are present and fall back to Threads_running otherwise; the migration
// runner uses it to decide the autoscaler growth cap (see ResolveMaxWriteThreads).
func CanReadRedoAwareThreads(ctx context.Context, db *sql.DB) error {
	var n int64
	if err := db.QueryRowContext(ctx, redoAwareThreadsQuery).Scan(&n); err != nil {
		return fmt.Errorf("probing redo-aware threads query (requires SELECT on performance_schema.threads, events_waits_current): %w", err)
	}
	return nil
}

// AuroraThreads throttles when the count of running threads exceeds the instance
// vCPU count plus a small headroom for spirit's own monitoring connections. It
// samples one of two signals depending on mode (see threadsMode): the redo-aware
// perf_schema count (preferred) or Threads_running from global_status
// (fallback). Aurora-only — depends on @@innodb_buffer_pool_instances matching
// vCPUs.
type AuroraThreads struct {
	db     *sql.DB
	logger *slog.Logger

	// mode fixes the sampling query and the self-monitoring headroom for the
	// throttler's lifetime; it is chosen once at construction.
	mode threadsMode

	// vCPUs is the throttle threshold base, captured once at Open and treated as
	// immutable for the migration's lifetime. Aurora instance type changes
	// require a restart so it cannot move under us.
	vCPUs int64

	isThrottled atomic.Bool
	poller      monitorLoop

	// lastSample holds the most recent observation for logging and the raw
	// hard-stop comparison.
	lastSample atomic.Int64

	// smoothedBits holds math.Float64bits of the EWMA of the thread count that
	// Utilization() reports (see threadsRunningEWMAAlpha). ewmaSeeded records
	// whether smoothedBits holds a real value yet — 0.0 is a valid average,
	// so a sentinel won't do.
	smoothedBits atomic.Uint64
	ewmaSeeded   atomic.Bool

	// stale guards Utilization() against the cached value freezing when
	// sampling fails persistently. See stale.go.
	stale staleGuard
}

var _ GradualThrottler = (*AuroraThreads)(nil)
var _ ReasonedThrottler = (*AuroraThreads)(nil)

// newAuroraThreadsThrottler returns a throttler that polls the given mode's
// signal and throttles when it exceeds the instance vCPU count (plus the mode's
// headroom). It is unexported because mode is an internal type chosen by a
// privilege probe: AuroraSetup.Build is the public entry point that selects the
// mode and constructs the throttler.
func newAuroraThreadsThrottler(db *sql.DB, mode threadsMode, logger *slog.Logger) (*AuroraThreads, error) {
	if db == nil {
		return nil, errors.New("AuroraThreads throttler requires a non-nil DB")
	}
	return &AuroraThreads{
		db:     db,
		mode:   mode,
		logger: logger,
	}, nil
}

func (a *AuroraThreads) Open(ctx context.Context) error {
	if err := a.poller.checkOpen(); err != nil {
		return err
	}
	vCPUs, err := AuroraVCPUs(ctx, a.db)
	if err != nil {
		return err
	}
	a.vCPUs = int64(vCPUs)
	a.logger.Info("Aurora threads throttler enabled",
		"mode", a.mode.label, "vCPUs", a.vCPUs, "throttle_threshold", a.throttleThreshold())
	if err := a.UpdateLag(ctx); err != nil {
		return err
	}
	return a.poller.start(ctx, a.run)
}

func (a *AuroraThreads) run(ctx context.Context) {
	ticker := time.NewTicker(threadsRunningPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := a.UpdateLag(ctx); err != nil {
				if isShutdownError(ctx, err) {
					return // teardown cancelled the in-flight sample; not a monitoring failure
				}
				a.logger.Error("error sampling Aurora threads; keeping the last reading (the autoscaler holds steady if sampling stays stale)",
					"mode", a.mode.label, "error", err)
			}
		}
	}
}

func (a *AuroraThreads) Close() error {
	a.poller.close()
	return nil
}

func (a *AuroraThreads) IsThrottled() bool {
	return a.isThrottled.Load()
}

// ThrottleReason implements ReasonedThrottler. It quotes the raw sample against
// the hard-stop threshold — the comparison applySample actually throttles on —
// not the EWMA that Utilization reports, and names the mode so the reason says
// which signal was read ("redo-aware 24 > 17", "threads-running 24 > 20").
func (a *AuroraThreads) ThrottleReason() string {
	if !a.isThrottled.Load() {
		return ""
	}
	return fmt.Sprintf("%s %d > %d", a.mode.label, a.lastSample.Load(), a.throttleThreshold())
}

// Utilization reports the smoothed (EWMA) thread count as a fraction of the
// instance vCPU count. 1.0 means the sustained average equals vCPUs; the raw
// hard-stop trips a little above this, at vCPUs + the mode's headroom. Returns 0
// if vCPUs is not yet known. The smoothing exists for the autoscaler, this
// method's only consumer: it must see sustained load, not single-sample spikes
// (see threadsRunningEWMAAlpha). The binary hard-stop deliberately does not
// share it.
//
// When sampling has failed for longer than staleSignalThreshold the cached
// average is no longer trustworthy, so this reports StaleUtilizationHold
// instead — parking the autoscaler in its dead band rather than letting a
// frozen value drive scaling. IsThrottled/BlockWait are unaffected.
func (a *AuroraThreads) Utilization() float64 {
	if stale, entering := a.stale.check(staleSignalThreshold); stale {
		if entering {
			a.logger.Warn("Aurora threads signal is stale; holding autoscaler utilization steady until sampling recovers",
				"mode", a.mode.label,
				"last_successful_sample_age", a.stale.age(),
				"hold_utilization", StaleUtilizationHold)
		}
		return StaleUtilizationHold
	}
	if a.vCPUs <= 0 {
		return 0
	}
	return math.Float64frombits(a.smoothedBits.Load()) / float64(a.vCPUs)
}

// throttleThreshold is the thread count the hard-stop trips above: the instance
// vCPU count plus the mode's headroom for spirit's own monitoring connections.
// Single source of truth for applySample's comparison and the log lines, so
// they can never drift apart.
func (a *AuroraThreads) throttleThreshold() int64 {
	return a.vCPUs + a.mode.headroom
}

// BlockWait blocks until the thread count falls to or below the throttle
// threshold (vCPUs + headroom), or up to 60s. Matches the commit-latency
// throttler's loop shape so the multi-throttler waits uniformly across signals.
func (a *AuroraThreads) BlockWait(ctx context.Context) {
	timer := time.NewTimer(blockWaitInterval)
	defer timer.Stop()

	for range 60 {
		if !a.isThrottled.Load() {
			return
		}
		timer.Reset(blockWaitInterval)
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
	}
	a.logger.Info("Aurora threads signal stayed above threshold for the full backoff; allowing one copy loop to make progress before throttling again",
		"mode", a.mode.label,
		"count", a.lastSample.Load(),
		"vCPUs", a.vCPUs,
		"throttle_threshold", a.throttleThreshold())
}

// UpdateLag samples the mode's thread-count signal and updates throttled state.
func (a *AuroraThreads) UpdateLag(ctx context.Context) error {
	var count int64
	if err := a.db.QueryRowContext(ctx, a.mode.query).Scan(&count); err != nil {
		return fmt.Errorf("sampling Aurora threads (%s): %w", a.mode.label, err)
	}
	a.applySample(count)
	return nil
}

// applySample updates state from a single observation. Split out so tests can
// drive the calculation without a real Aurora.
//
// The thread count is an instantaneous gauge (both signals report a current
// count, not a delta of cumulative counters), so a failover cannot produce a
// bogus value — only an error, which the staleness guard covers. There is
// therefore no counter-reset case to handle here.
func (a *AuroraThreads) applySample(count int64) {
	// Capture whether this sample arrives after a stale gap before marking it
	// fresh: the EWMA must be reseeded then, not extended — blending a live
	// sample into pre-outage history would report a fiction made of dead data.
	staleGap := a.stale.gapExceeds(staleSignalThreshold)
	if a.stale.markFresh() {
		a.logger.Info("Aurora threads sampling recovered; resuming live utilization signal", "mode", a.mode.label)
	}
	a.lastSample.Store(count)

	if sample := float64(count); !a.ewmaSeeded.Load() || staleGap {
		a.smoothedBits.Store(math.Float64bits(sample))
		a.ewmaSeeded.Store(true)
	} else {
		prev := math.Float64frombits(a.smoothedBits.Load())
		a.smoothedBits.Store(math.Float64bits(prev + threadsRunningEWMAAlpha*(sample-prev)))
	}

	// The hard-stop compares the raw sample (against vCPUs plus the mode's
	// headroom for spirit's own monitoring threads), not the EWMA: it protects
	// the production workload and must engage within one poll of a load spike.
	throttled := count > a.throttleThreshold()
	prev := a.isThrottled.Swap(throttled)
	if throttled && !prev {
		a.logger.Warn("Aurora threads exceed throttle threshold, throttling",
			"mode", a.mode.label,
			"count", count,
			"vCPUs", a.vCPUs,
			"throttle_threshold", a.throttleThreshold())
	}
}
