package status

import (
	"fmt"
	"time"
)

// Progress is returned as a struct because we may add more to it later.
// It is designed for wrappers (like a GUI) to be able to summarize the
// current status without parsing log output.
// ETAState describes the availability of the row-copy ETA estimate, so callers
// can distinguish "still measuring" from a real estimate without parsing the
// Summary string. It mirrors the cases GetETA renders as text.
type ETAState string

const (
	// ETANone means there is no copy ETA because the migration is not in the
	// row-copy phase. Duration is 0.
	ETANone ETAState = ""
	// ETAMeasuring means a copy is in progress but no copy rate has been measured
	// yet, so no estimate is available (Summary shows "ETA TBD"). Duration is 0.
	ETAMeasuring ETAState = "measuring"
	// ETAReady means Duration holds a current remaining-time estimate.
	ETAReady ETAState = "ready"
	// ETADue means the copy is essentially complete (Summary shows "ETA DUE").
	// Duration is 0.
	ETADue ETAState = "due"
)

// ETA is the structured form of the ETA embedded in Summary. State reports
// whether Duration is available yet — e.g. ETAMeasuring during the initial
// window before a copy rate is known — so callers can show "calculating" rather
// than a misleading 0. Duration is the estimated remaining row-copy time, valid
// only when State is ETAReady and 0 otherwise.
type ETA struct {
	State    ETAState
	Duration time.Duration
}

// ThrottleStatus reports whether the current phase is paused by a throttler,
// and why. Before this, throttling was only visible in the logs, so a wrapper
// polling status saw a migration that had gone quiet with no way to say why
// (issue #844).
type ThrottleStatus struct {
	// Throttled is true while a throttler is telling the current phase to
	// pause. This is the field to branch on.
	//
	// It is false in phases that do not pace themselves against a throttler at
	// all — which is every phase except the row copy and the checksum. A loaded
	// server is not reported as pausing a cutover or a sentinel wait, because
	// nothing there is reading that signal.
	Throttled bool

	// Reason names the signal and the comparison that tripped it, in the form
	// "<signal> <observed> <op> <threshold>" — e.g. "commit-latency 128ms >= 100ms"
	// or "redo-aware 24 > 17". When several signals throttle at once they are
	// joined with "; ", because clearing only one of them will not resume the
	// copy.
	//
	// It is intended for display, not for branching: it is "" when Throttled is
	// false, and may also be "" when the configured throttler cannot explain
	// itself (see throttler.ReasonedThrottler).
	//
	// It is also sampled independently of Throttled rather than atomically with
	// it, so on a signal that is changing underneath the poll the two can
	// disagree: a throttler that clears in between yields Throttled with an
	// empty Reason, and a reason can quote a comparison that has just stopped
	// holding. Both are display-level staleness on a value that is a snapshot
	// anyway — not a bug to report.
	Reason string

	// Utilization is load relative to the point at which throttling begins:
	// 1.0 is exactly at that point, >1.0 is over it, and lower values are
	// further below it. It lets a wrapper show "running at 40% of the load
	// limit" rather than only a long ETA.
	//
	// 0 is ambiguous and must not be rendered as idle. It is also what this
	// field reports when no continuous load signal exists at all — notably when
	// throttling is replica-lag-only, which is a budget rather than a load gauge
	// (see throttler.GradualThrottler). A copy paused on replica lag therefore
	// reports Throttled with Utilization 0, so a wrapper drawing a load gauge
	// should treat 0 as "unknown" and hide it rather than show an idle server.
	Utilization float64
}

type Progress struct {
	CurrentState State  // current state, i.e. CopyRows
	Summary      string // text based representation, i.e. "12.5% copyRows ETA 1h 30m"

	// Resume is true when this run resumed from a checkpoint left by an earlier
	// run, rather than starting the copy from scratch.
	//
	// It exists because a resumed run walks the whole state machine again
	// (CopyRows, Checksum, ...) even when those phases are near-instant, so a
	// wrapper watching CurrentState sees what looks like a migration starting
	// over. CurrentState is deliberately left alone — callers parse it for phase
	// display — and this reports the fact alongside it: pair it with
	// Tables/Checksum progress to decide whether to render the run as
	// "recovering" rather than "starting".
	Resume bool

	// Throttle reports whether the current phase is paused by a throttler, and
	// why. Which signals count depends on the phase: the copy honours all of
	// them, while a checksum only honours load signals (a read-only snapshot
	// pass cannot cause replica lag, so pausing it on lag would only hold the
	// snapshot open for longer).
	Throttle ThrottleStatus

	// ETA is the structured remaining row-copy estimate and its availability.
	ETA ETA

	// Checksum is the structured progress of the post-copy checksum phase,
	// populated while CurrentState is Checksum and zero otherwise. It is the
	// structured form of the checksum progress embedded in Summary.
	Checksum ChecksumProgress

	// Tables contains per-table progress for multi-table migrations.
	// For single-table migrations, this will have one entry.
	Tables []TableProgress
}

// ChecksumProgress tracks progress of the checksum phase, where Spirit verifies
// the copied data against the source before cutover. RowsChecked and RowsTotal
// are 0 outside the checksum phase.
type ChecksumProgress struct {
	RowsChecked uint64 // rows verified so far
	RowsTotal   uint64 // total rows to verify
}

// String renders the checksum progress for the human-readable summary line,
// e.g. "71436/221193 32.30%".
func (c ChecksumProgress) String() string {
	pct := float64(0)
	if c.RowsTotal > 0 {
		pct = float64(c.RowsChecked) / float64(c.RowsTotal) * 100
	}
	return fmt.Sprintf("%d/%d %.2f%%", c.RowsChecked, c.RowsTotal, pct)
}

// TableProgress tracks progress for a single table in the migration.
type TableProgress struct {
	TableName  string // name of the table being migrated
	RowsCopied uint64 // rows copied so far
	RowsTotal  uint64 // total rows expected
	IsComplete bool   // true if this table's copy is complete
}
