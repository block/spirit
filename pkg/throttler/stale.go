package throttler

import (
	"sync/atomic"
	"time"
)

// Staleness guard for polled throttle signals.
//
// The throttlers sample on a background loop and cache the result; readers
// only see the cache. If sampling fails persistently (monitor connections
// partitioned, grants revoked mid-migration, a failover the pool won't
// reconnect from), the poll loop logs and continues — and the cached value
// freezes at whatever it was when things were last healthy. The guard tracks
// the time of the last successful sample so a throttler can stop trusting a
// frozen value once it is older than staleSignalThreshold. What "stop
// trusting" means depends on what the signal protects:
//
//   - The Aurora gradual throttlers (CommitLatency, AuroraThreads) apply the
//     guard to Utilization() only. A value frozen below the autoscaler's low
//     watermark would add a write thread every cooldown until the 2x cap,
//     ramping blind on a dead signal — so while stale, Utilization() reports
//     StaleUtilizationHold, a value inside the autoscaler's dead band,
//     freezing scaling in place (no growth, no shrink) until sampling
//     recovers. Their binary IsThrottled()/BlockWait() are deliberately NOT
//     touched by the guard: these signals are best-effort load protection,
//     and the hard-stop keeps reporting the last computed state exactly as
//     before, fresh or stale.
//
//   - The Replica throttler applies the guard to IsThrottled() itself and
//     fails closed. Its lag tolerance (--replica-max-lag) is a hard budget,
//     and a lag value frozen at a healthy level would let the copy run at
//     full speed against a budget nobody is measuring — so while stale,
//     IsThrottled() reports true, pausing the copy until polling recovers.
const (
	// staleSignalThreshold is how old the last successful sample may be
	// before the cached value is no longer trusted. It is three poll intervals
	// (commitLatencyPollInterval, threadsRunningPollInterval and the replica
	// throttler's loopInterval are all 5s), so one or two failed or slow polls —
	// a brief failover blip, one stalled status query — don't flap the guard.
	//
	// It is deliberately NOT derived from the autoscaler's cooldown, because the
	// poll interval is what it measures. But it does have to stay short enough
	// that the signal is declared stale before the autoscaler can take more than
	// one blind step on the frozen value, which bounds it by the spacing of the
	// autoscaler's increases — see TestStaleThresholdBoundsBlindAutoscalerSteps,
	// which pins that relationship so moving either side fails loudly.
	staleSignalThreshold = 15 * time.Second

	// StaleUtilizationHold is the utilization reported while the signal is
	// stale. It must sit inside the autoscaler's dead band [low, high) —
	// currently [0.4, 0.7) — so a stale signal holds the write-thread count
	// steady rather than ramping it or shrinking it. 0.55 is the midpoint,
	// keeping maximum distance from both watermarks if they ever move.
	// Exported so the autoscaler tests can pin this invariant.
	StaleUtilizationHold = 0.55
)

// staleGuard tracks the freshness of a polled signal. It is embedded by the
// gradual throttlers (CommitLatency, AuroraThreads — applySample marks the
// signal fresh, Utilization checks it) and by Replica (applyLag marks fresh,
// IsThrottled checks it). All methods are safe for concurrent use.
type staleGuard struct {
	lastSampleAt atomic.Int64 // unixnano of the last successful sample; 0 = never sampled
	warned       atomic.Bool  // true once the current stale period has been logged
}

// markFresh records a successful sample at the current time. It returns true
// when the signal was previously declared stale — i.e. this sample is a
// recovery — so the caller can log the transition exactly once.
func (s *staleGuard) markFresh() (recovered bool) {
	s.lastSampleAt.Store(time.Now().UnixNano())
	return s.warned.Swap(false)
}

// gapExceeds reports whether the time since the last successful sample is at
// least threshold. False if no sample has ever been recorded. Unlike check()
// it has no warn-once side effects — samplers call it on arrival of a fresh
// sample to decide whether derived state (e.g. an EWMA) spans a dead gap and
// must be rebuilt rather than extended.
func (s *staleGuard) gapExceeds(threshold time.Duration) bool {
	last := s.lastSampleAt.Load()
	return last != 0 && time.Since(time.Unix(0, last)) >= threshold
}

// check reports whether the signal is stale (no successful sample within
// threshold). entering is true only for the first check that finds the signal
// stale, so callers can log a warning once per stale period rather than on
// every call.
//
// A guard that has never seen a sample is not stale: the throttlers fail
// Open() if their very first sample fails, so "no sample yet" means the
// throttler isn't open, not that a working signal died.
func (s *staleGuard) check(threshold time.Duration) (stale, entering bool) {
	last := s.lastSampleAt.Load()
	if last == 0 {
		return false, false
	}
	if time.Since(time.Unix(0, last)) < threshold {
		return false, false
	}
	return true, s.warned.CompareAndSwap(false, true)
}

// age returns the time since the last successful sample, for logging.
func (s *staleGuard) age() time.Duration {
	return time.Since(time.Unix(0, s.lastSampleAt.Load()))
}
