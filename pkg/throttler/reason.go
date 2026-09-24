package throttler

import "strings"

// ReasonedThrottler is an optional extension implemented by throttlers that can
// explain why they are currently throttling. It exists for the status API: a
// wrapper (GUI, orchestration) polling status.Progress can report "paused:
// commit-latency 128ms >= 100ms" rather than leaving the operator to explain a
// mysteriously long ETA from the logs. See issue #844.
//
// It is optional for the same reason GradualThrottler is — a custom throttler
// that does not implement it still works, it just reports being throttled
// without a reason.
type ReasonedThrottler interface {
	Throttler
	// ThrottleReason names the signal and the comparison that tripped it, in the
	// form "<signal> <observed> <op> <threshold>", e.g.
	// "commit-latency 128ms >= 100ms". It returns "" when this throttler is not
	// currently throttling.
	//
	// It must be free of side effects beyond the warn-once logging the throttlers
	// already do: it is called from status polling, not from the throttle path,
	// and must not disturb the signal it reports on.
	ThrottleReason() string
}

// Describe reports a throttler's current state for the status API:
//
//   - throttled is what IsThrottled reports — the binary "is the copy paused
//     right now" answer, and the field callers should branch on.
//   - reason explains it, and is "" both when not throttled and when the
//     throttler does not implement ReasonedThrottler. Never branch on it.
//   - utilization is load relative to the throttle point (0 = idle, 1.0 = at
//     the point throttling begins) for throttlers implementing
//     GradualThrottler, and 0 for those that do not — so 0 means "no continuous
//     signal available", not "idle". Replica-lag-only throttling reads 0 here
//     by design; see GradualThrottler.
//
// A nil throttler reads as not throttled, which is what the runners want: a
// migration with no throttlers configured paces itself against the copier's
// Noop.
func Describe(t Throttler) (throttled bool, reason string, utilization float64) {
	if t == nil {
		return false, "", 0
	}
	throttled = t.IsThrottled()
	if throttled {
		if rt, ok := t.(ReasonedThrottler); ok {
			reason = rt.ThrottleReason()
		}
	}
	if gt, ok := t.(GradualThrottler); ok {
		utilization = gt.Utilization()
	}
	return throttled, reason, utilization
}

// joinReasons renders the reasons of every currently-throttling child of a
// composite throttler, so that a copy paused by two signals at once names both
// rather than only whichever child happens to sort first. Children that are not
// throttling, and children that cannot explain themselves, contribute nothing —
// so the result is "" if no child can be attributed, matching the
// ThrottleReason contract for a throttler that has no reason to offer.
func joinReasons(throttlers []Throttler) string {
	var reasons []string
	for _, t := range throttlers {
		if !t.IsThrottled() {
			continue
		}
		rt, ok := t.(ReasonedThrottler)
		if !ok {
			continue
		}
		if reason := rt.ThrottleReason(); reason != "" {
			reasons = append(reasons, reason)
		}
	}
	return strings.Join(reasons, "; ")
}
