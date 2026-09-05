package change

import (
	"fmt"
	"strings"
)

// catchUpDiagnostics is best effort: reporting a timeout must never wait for a
// subscription mutex held by the flush whose progress we are investigating.
// Unknown subscription implementations are explicitly counted, not treated as
// empty. The fields are an observation, not a diagnosis of the timeout's cause.
func catchUpDiagnostics(subs []Subscription) string {
	var out []string
	for i, subscription := range subs {
		sub, ok := subscription.(*bufferedMap)
		if !ok {
			out = append(out, fmt.Sprintf("subscription[%d]=unavailable", i))
			continue
		}
		parks := sub.timesParked.Load()
		if !sub.TryLock() {
			out = append(out, fmt.Sprintf("subscription[%d]=busy parks=%d", i, parks))
			continue
		}
		out = append(out, fmt.Sprintf("subscription[%d]: pending=%d flushing=%d bytes=%d parked=%t parks=%d drain-budget-hit=%t",
			i, len(sub.changes)+len(sub.queue), sub.flushingCount, sub.sizeBytes, sub.parked, parks, sub.lastDrainHitBudget.Load()))
		sub.Unlock()
	}
	if len(out) == 0 {
		return "no subscriptions"
	}
	return strings.Join(out, "; ")
}
