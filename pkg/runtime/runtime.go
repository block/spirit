// Package runtime holds the runner pieces that pkg/migration, pkg/move and
// pkg/datasync would otherwise each keep a copy of, and let drift apart.
//
// The finite runners (migration and move) walk the same states with the same
// subsystems, so their periodic status block and Progress report are built
// here from a Snapshot rather than from two copies of the same switch.
// FatalGate is the state transition both make when their change feed fails,
// and SharedThrottler the throttler both resolve while already being polled.
// All three runners use Lifecycle, the cancel function and correctness
// evidence of a Run invocation, and RecordCopyCompleted.
//
// The name matches the standard library's runtime package. A file that needs
// both must import one under an alias.
package runtime

import (
	"fmt"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/checksum"
	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/sentinel"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/throttler"
)

// Source gives a Snapshot the runner's subsystems. Setup assigns them while an
// API caller may already be polling, so they are read through functions, and a
// Snapshot only calls the ones the current state reports on, which are the
// ones setup has finished assigning. Every function but SentinelSchema must be
// set.
type Source struct {
	Copier    func() copier.Copier
	Applier   func() applier.Applier
	Checker   func() checksum.Checker
	Feeds     func() []change.Source
	Throttler func() throttler.Throttler
	// SentinelSchema, when set and not empty, adds the sentinel table's name
	// to the sentinel row. A move's sources can span several schemas, so it
	// leaves it nil.
	SentinelSchema func() string
}

func (s *Source) sentinelSchema() string {
	if s.SentinelSchema == nil {
		return ""
	}
	return s.SentinelSchema()
}

// Snapshot is what a runner reports on. The runner builds one per call, and
// the state is read once when it does, so every field of a report describes
// the same state rather than whichever state each field happened to observe.
type Snapshot struct {
	// Noun names the workflow in the status header ("migration", "move").
	Noun    string
	State   status.State
	Tracker *status.Tracker
	// Tables is the copy chunker's per-table progress. Progress and Status
	// both derive their copy figures from it, so the API and the log block
	// report one measure: settled rows against the tables' cardinality
	// estimates, kept past the end of the copy.
	Tables     []status.TableProgress
	Checkpoint *status.LastCheckpoint
	Resumed    bool
	Source     Source
}

// continuousChecksum reports whether a continuous checksum is running during
// the sentinel wait.
func (s *Snapshot) continuousChecksum() bool {
	checker := s.Source.Checker()
	return checker != nil && checker.ContinuousActive()
}

func (s *Snapshot) deltaLen() int {
	total := 0
	for _, feed := range s.Source.Feeds() {
		if feed != nil {
			total += feed.GetDeltaLen()
		}
	}
	return total
}

// binlogRow renders the change-feed row. change.StatusRow merges several
// feeds into one set of fields, so a 16-shard move does not print 16 rows.
func (s *Snapshot) binlogRow(b *status.Block) {
	b.Row("binlog", "deltas=%d  %s", s.deltaLen(), change.StatusRow(s.Source.Feeds()...))
}

// header starts the block with the total time and, when label is set, the
// time spent in the current state.
func (s *Snapshot) header(label string) *status.Block {
	if label == "" {
		return status.NewBlock("%s status: state=%s total-time=%s",
			s.Noun,
			s.State.String(),
			s.Tracker.TotalElapsed().Round(time.Second),
		)
	}
	return status.NewBlock("%s status: state=%s total-time=%s %s=%s",
		s.Noun,
		s.State.String(),
		s.Tracker.TotalElapsed().Round(time.Second),
		label,
		s.Tracker.Elapsed().Round(time.Second),
	)
}

// Status returns the periodic report on the whole run: a header line plus one
// indented row per subsystem (see status.Block). It absorbs what used to be
// separate periodic lines from the change feeds (flushes, rotations) and the
// checkpoint dumper, which each ran on their own interval — see
// github.com/block/spirit/issues/329.
func (s *Snapshot) Status() string {
	if s.State > status.CutOver {
		return ""
	}
	var b *status.Block
	switch s.State { //nolint: exhaustive
	case status.CopyRows:
		progress := status.CopyFromTables(s.Tables)
		c := s.Source.Copier()
		b = s.header("copier-time")
		b.Row("copier", "%6.2f%%  %d/%d  chunk-size=%d  eta=%s  throttled=%v",
			progress.Fraction()*100,
			progress.RowsCopied,
			progress.RowsTotal,
			c.ChunkSize(),
			c.GetETA(),
			c.GetThrottler().IsThrottled(),
		)
		b.Row("applier", "%s", applier.StatusRow(s.Source.Applier()))
	case status.WaitingOnSentinelTable:
		b = s.header("")
		if schema := s.Source.sentinelSchema(); schema != "" {
			b.Row("sentinel", "table=%s.%s  waiting=%s  max-wait=%s",
				schema,
				sentinel.TableName,
				s.Tracker.Elapsed().Round(time.Second),
				sentinel.WaitLimit,
			)
		} else {
			b.Row("sentinel", "waiting=%s  max-wait=%s",
				s.Tracker.Elapsed().Round(time.Second),
				sentinel.WaitLimit,
			)
		}
		if s.continuousChecksum() {
			b.Row("checksum", "%s", checksum.StatusRow(s.Source.Checker()))
			if throttle := s.ThrottleStatus(); throttle.Throttled {
				b.Row("throttle", "%s", throttle.Reason)
			}
		}
	case status.ApplyChangeset, status.PostChecksum:
		// We've finished copying rows, and we are now trying to reduce the
		// number of binlog deltas before proceeding to the checksum and then
		// the final cutover.
		b = s.header("")
		b.Row("applier", "%s", applier.StatusRow(s.Source.Applier()))
	case status.RestoreSecondaryIndexes, status.AnalyzeTable:
		// Neither phase has progress to report, but both can block behind
		// other work on the server, and with the per-dump checkpoint line at
		// DEBUG this is the only INFO output they produce. Keep it minimal but
		// present, so log-based liveness checks still see the run.
		label := "state-time"
		if s.State == status.AnalyzeTable {
			label = "analyze-time"
		}
		b = s.header(label)
	case status.Checksum:
		// This could take a while if it's a large table. threads/throttled
		// mirror the copier row's throttled=: without them a checksum that is
		// deliberately paused or scaled down looks identical to one that is
		// simply slow.
		b = s.header("checksum-time")
		b.Row("checksum", "%s", checksum.StatusRow(s.Source.Checker()))
	default:
		return ""
	}
	s.binlogRow(b)
	// The dumper keeps checkpointing in every state above, and a long drain
	// under heavy rotation is exactly when the resume position can fall off
	// the source's binlog retention — so the ckpt row belongs in all of them.
	b.Row("ckpt", "%s", s.Checkpoint.Row())
	return b.String()
}

// Progress returns the structured report for API callers.
func (s *Snapshot) Progress() status.Progress {
	copyProgress := status.CopyFromTables(s.Tables)

	var summary string
	var eta status.ETA
	var checksumProgress status.ChecksumProgress
	switch s.State { //nolint: exhaustive
	case status.CopyRows:
		// One copier read, so the ETA in Summary and the ETA field describe
		// the same instant.
		eta = s.Source.Copier().GetETAState()
		summary = fmt.Sprintf("%s %s ETA %s", copyProgress.String(), s.State.String(), eta.String())
	case status.WaitingOnSentinelTable:
		summary = "Waiting on Sentinel Table"
		if s.continuousChecksum() {
			// The pass ETA stays out of the ETA field here: the continuous
			// checksum re-walks until the sentinel is dropped, so its pass
			// does not end the wait. Summary still carries it.
			var checksumSummary string
			checksumProgress, _, checksumSummary = checksum.Report(s.Source.Checker())
			summary += "; " + checksumSummary
		}
	case status.ApplyChangeset, status.PostChecksum:
		summary = fmt.Sprintf("Applying Changeset Deltas=%v", s.deltaLen())
	case status.Checksum:
		checksumProgress, eta, summary = checksum.Report(s.Source.Checker())
	}
	return status.Progress{
		CurrentState: s.State,
		Summary:      summary,
		Resume:       s.Resumed,
		Throttle:     s.ThrottleStatus(),
		ETA:          eta,
		Copy:         copyProgress,
		Checksum:     checksumProgress,
		Tables:       s.Tables,
	}
}

// ThrottleStatus reports only signals honored by the work currently running.
// Checksum passes honor load signals; interval waits and cutover are unpaced.
// The sentinel wait only reports pacing while a continuous checksum runs.
func (s *Snapshot) ThrottleStatus() status.ThrottleStatus {
	var t throttler.Throttler
	switch s.State { //nolint:exhaustive // only paced phases report throttling
	case status.CopyRows:
		t = s.Source.Throttler()
	case status.Checksum:
		t = throttler.GradualOnly(s.Source.Throttler())
	case status.WaitingOnSentinelTable:
		if !s.continuousChecksum() {
			return status.ThrottleStatus{}
		}
		t = throttler.GradualOnly(s.Source.Throttler())
	default:
		return status.ThrottleStatus{}
	}
	throttled, reason, utilization := throttler.Describe(t)
	return status.ThrottleStatus{
		Throttled:   throttled,
		Reason:      reason,
		Utilization: utilization,
	}
}
