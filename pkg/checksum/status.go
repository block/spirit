package checksum

import (
	"fmt"

	"github.com/block/spirit/pkg/status"
)

// StatusReporter optionally exposes algorithm-specific verification state.
// Callers depend on this capability rather than concrete checker types.
type StatusReporter interface{ ChecksumStatus() ChecksumStatus }

// ChecksumStatus separates optimistic traversal from completed verification.
// Optimistic is a point-in-time copy, not mutable checker state.
type ChecksumStatus struct {
	Progress status.ChecksumProgress
	// ETA is the remaining time of the current pass. It is ETANone for a
	// checker that does not estimate one.
	ETA        status.ETA
	Optimistic *LocklessCheckerStats
}

func (s ChecksumStatus) String() string {
	if s.Optimistic == nil {
		return s.Progress.String()
	}
	stats := s.Optimistic
	phase := "scanning"
	if stats.ScanComplete {
		phase = "waiting for verification"
	}
	if !stats.FirstCleanPassAt.IsZero() {
		phase = "verified"
	}
	// settled= is shown next to deferred= because neither reads correctly
	// alone: a falling deferred count means the checker is converging only if
	// those ranges reached a verdict rather than simply stopping being hot.
	// ETANone is only the zero value: a lockless checker's estimate is always
	// measuring, ready or due.
	var eta string
	if s.ETA.State != status.ETANone {
		eta = " eta=" + s.ETA.String()
	}
	return fmt.Sprintf("checksum: %s scan≈%.1f%%%s passed=%d retrying=%d in-flight=%d deferred=%d settled=%d", phase, float64(stats.ProgressBasisPoints)/100, eta, stats.ChunksPassedThisPass, stats.RetryQueueDepth, stats.InFlight, stats.HotChunksDeferredThisPass, stats.HotChunksSettledThisPass)
}

// StatusSummary formats rich status when supported, otherwise basic progress.
func StatusSummary(c Checker) string {
	_, _, summary := Report(c)
	return summary
}

// Report reads the checker once and returns its progress, its ETA and the
// summary line, so the three describe the same instant. A checker that is not
// a StatusReporter reports no ETA (ETANone).
func Report(c Checker) (status.ChecksumProgress, status.ETA, string) {
	if reporter, ok := c.(StatusReporter); ok {
		s := reporter.ChecksumStatus()
		return s.Progress, s.ETA, s.String()
	}
	progress := c.GetProgress()
	return progress, status.ETA{}, "Checksum Progress=" + progress.String()
}

// StatusRow formats the checksum's row in a runner status block.
func StatusRow(c Checker) string {
	if reporter, ok := c.(StatusReporter); ok {
		return reporter.ChecksumStatus().String()
	}
	progress := c.GetProgress()
	return fmt.Sprintf("%6.2f%%  %d/%d%s", progress.Fraction()*100, progress.RowsChecked, progress.RowsTotal, StatusSuffix(c))
}
