package throttler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/block/mysql"
)

// MySQL error codes for "you don't have permission" failures. The redo-aware
// probe distinguishes these from other errors so the log message can suggest a
// concrete fix (grant SELECT) when it's actually a grants problem, and avoid
// that misleading suggestion otherwise.
const (
	errAccessDenied         = 1045 // ER_ACCESS_DENIED_ERROR
	errDBAccessDenied       = 1044 // ER_DBACCESS_DENIED_ERROR
	errTableAccessDenied    = 1142 // ER_TABLEACCESS_DENIED_ERROR (SELECT on a table denied)
	errSpecificAccessDenied = 1227 // ER_SPECIFIC_ACCESS_DENIED_ERROR
)

// AuroraSetup orchestrates probing for Aurora and assembling the Aurora-
// specific throttlers (commit-latency + the Aurora threads signal). It exists
// so both the migration runner and the move runner can wire up the same
// throttlers without duplicating the IsAurora / monitor-pool / construct dance.
//
// The throttler package intentionally does not import dbconn — opening the
// monitor pool happens via the caller-supplied OpenMonitor closure, which
// lets the caller own DSN, TLS, and pool sizing.
//
// The two Aurora throttlers are independent signals and have independent
// gates. Disabling one does not disable the other — see Build for details.
type AuroraSetup struct {
	// Source is the caller's main *sql.DB. Used only for the one-shot IsAurora
	// probe and the redo-aware privilege probe — cheap and run once at setup,
	// so sharing the main pool is fine.
	Source *sql.DB

	// OpenMonitor opens a dedicated *sql.DB used exclusively by the Aurora
	// throttlers for recurring polls. Called at most once, only after
	// IsAurora has returned true and at least one Aurora throttler is
	// going to be constructed, so non-Aurora callers never pay the
	// connect cost. The caller owns closing the returned DB — see
	// AuroraResult.MonitorDB.
	OpenMonitor func() (*sql.DB, error)

	// CommitLatencyThreshold gates the commit-latency throttler. A non-
	// positive value disables that throttler only — the Aurora threads
	// throttler is independent and is always enabled once Aurora is detected
	// (it reads only performance_schema, which the IsAurora probe already
	// proved is at least partly readable).
	CommitLatencyThreshold time.Duration

	Logger *slog.Logger
}

// AuroraResult is the output of AuroraSetup.Build. When Throttlers is empty
// MonitorDB is nil — there's no pool to close. When Throttlers is non-empty
// MonitorDB is non-nil and the caller owns its lifecycle.
type AuroraResult struct {
	Throttlers []Throttler
	MonitorDB  *sql.DB
}

// Build probes the source for Aurora and assembles the Aurora throttlers.
//
// On a confirmed Aurora source the threads throttler is always built; the
// commit-latency throttler is built only when CommitLatencyThreshold > 0.
//
// The threads throttler is built in one of two modes, chosen by a privilege
// probe (CanReadRedoAwareThreads):
//   - redo-aware (preferred) when the user has SELECT on
//     performance_schema.threads and events_waits_current — it excludes
//     redo-log waiters so the copy can oversubscribe the log;
//   - Threads_running from global_status otherwise — the more conservative
//     fallback, which needs no grant beyond what IsAurora already exercised.
//
// Returns a zero AuroraResult (nil throttlers, nil monitor DB, nil error) when
// the source is not Aurora — either the IsAurora probe failed (non-Aurora
// source, or perf_schema not readable; logged at Debug so the common case
// stays quiet) or it returned false. In those cases the monitor pool is never
// opened.
//
// Returns a non-nil error only for setup failures the caller almost
// certainly wants to surface: nil required fields, OpenMonitor failing, or
// throttler construction failing.
func (s AuroraSetup) Build(ctx context.Context) (AuroraResult, error) {
	// Validate required fields up-front. AuroraSetup is an exported struct
	// and these are all dereferenced unconditionally inside Build; a
	// descriptive error beats a nil-pointer panic.
	if s.Source == nil {
		return AuroraResult{}, errors.New("AuroraSetup.Source is required")
	}
	if s.OpenMonitor == nil {
		return AuroraResult{}, errors.New("AuroraSetup.OpenMonitor is required")
	}
	if s.Logger == nil {
		return AuroraResult{}, errors.New("AuroraSetup.Logger is required")
	}

	isAurora, err := IsAurora(ctx, s.Source)
	switch {
	case err != nil:
		// Non-Aurora MySQL with locked-down perf_schema lands here too;
		// keep it at Debug so the common case isn't noisy.
		s.Logger.Debug("Aurora probe failed, skipping Aurora throttlers", "error", err)
		return AuroraResult{}, nil
	case !isAurora:
		return AuroraResult{}, nil
	}

	// The threads throttler is always built on Aurora, so at least one
	// throttler is always produced here and opening the monitor pool is never
	// wasted. Commit-latency is the only gated one.
	enableCommitLatency := s.CommitLatencyThreshold > 0

	// Choose the threads signal: prefer the redo-aware perf_schema count, fall
	// back to Threads_running when the extra grants are missing. The probe runs
	// on Source (cheap, one-shot); the throttler then polls monitorDB.
	mode := selectThreadsMode(ctx, s.Source, s.Logger)

	monitorDB, err := s.OpenMonitor()
	if err != nil {
		return AuroraResult{}, fmt.Errorf("could not open monitor DB for Aurora throttlers: %w", err)
	}

	var throttlers []Throttler

	if enableCommitLatency {
		cl, err := NewCommitLatencyThrottler(monitorDB, s.CommitLatencyThreshold, s.Logger)
		if err != nil {
			_ = monitorDB.Close()
			return AuroraResult{}, fmt.Errorf("could not create commit-latency throttler: %w", err)
		}
		s.Logger.Info("Aurora detected, enabling commit-latency throttler",
			"threshold", s.CommitLatencyThreshold)
		throttlers = append(throttlers, cl)
	}

	tr, err := newAuroraThreadsThrottler(monitorDB, mode, s.Logger)
	if err != nil {
		_ = monitorDB.Close()
		return AuroraResult{}, fmt.Errorf("could not create Aurora threads throttler: %w", err)
	}
	throttlers = append(throttlers, tr)

	return AuroraResult{Throttlers: throttlers, MonitorDB: monitorDB}, nil
}

// selectThreadsMode picks the redo-aware signal when the user can read the
// perf-schema tables it needs, and falls back to Threads_running otherwise. It
// logs the choice at Info because Aurora is confirmed and the operator likely
// wants to know which signal is running — and, on a permissions failure, how to
// unlock the preferred one.
func selectThreadsMode(ctx context.Context, source *sql.DB, logger *slog.Logger) threadsMode {
	probeErr := CanReadRedoAwareThreads(ctx, source)
	if probeErr == nil {
		logger.Info("Aurora threads throttler: using the redo-aware perf_schema signal (excludes redo-log waiters)")
		return redoAwareMode
	}
	// Distinguish "looks like a grants problem" from other failures so the log
	// message only suggests GRANT when that's plausibly the fix — a transient
	// network error shouldn't send operators down a fruitless permissions
	// investigation.
	if isPrivilegeDeniedError(probeErr) {
		logger.Info("Aurora threads throttler: falling back to Threads_running; grant SELECT on performance_schema.threads and performance_schema.events_waits_current to enable the redo-aware signal",
			"error", probeErr)
	} else {
		logger.Info("Aurora threads throttler: falling back to Threads_running; redo-aware probe failed",
			"error", probeErr)
	}
	return globalStatusMode
}

// isPrivilegeDeniedError reports whether err looks like the MySQL server
// refusing the query for permissions reasons (vs. network, syntax, or
// missing-table errors). Used to tailor the redo-aware probe-failure log
// message.
func isPrivilegeDeniedError(err error) bool {
	me, ok := errors.AsType[*mysql.MySQLError](err)
	if !ok {
		return false
	}
	switch me.Number {
	case errAccessDenied, errDBAccessDenied, errTableAccessDenied, errSpecificAccessDenied:
		return true
	default:
		return false
	}
}
