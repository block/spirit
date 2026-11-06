package throttler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/block/mysql"
	parsermysql "github.com/block/spirit/pkg/parser/mysql"
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

	// ProbeErr is the IsAurora probe's error, when it failed. Throttlers is
	// then empty. Build treats a failed probe as "not Aurora" so throttling
	// stays quiet on community MySQL, but an autoscaling caller wants to warn:
	// it was asked to scale and cannot tell whether it should.
	ProbeErr error

	// RedoAware reports whether the threads throttler runs the redo-aware
	// perf_schema signal rather than the Threads_running fallback. That signal
	// ignores redo-log waiters, so it cannot see write threads oversubscribing
	// the log; ResolveMaxWriteThreads takes it to decide whether growth needs
	// the commit-latency backstop. False when Throttlers is empty.
	RedoAware bool
}

// Close closes r's throttlers, then its monitor pool, in that order so no
// poller is still reading the pool when it closes. It is safe on a zero result
// and after the throttlers were already closed (a multi-throttler closing its
// children, or a failed Open): their Close is idempotent, and so is
// (*sql.DB).Close.
func (r AuroraResult) Close() error {
	var errs []error
	for _, t := range r.Throttlers {
		if err := t.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if r.MonitorDB != nil {
		if err := r.MonitorDB.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
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
// Returns an AuroraResult with no throttlers, a nil monitor DB, and a nil
// error when the source is not Aurora, and the monitor pool is never opened.
// That covers two cases:
//   - the IsAurora probe returned false: the result is zero;
//   - the probe failed (non-Aurora source, or perf_schema not readable;
//     logged at Debug so the common case stays quiet): the result is zero
//     except ProbeErr, which holds the probe's error. Callers that only
//     throttle can ignore it; callers that were asked to autoscale should
//     warn, since they cannot tell whether the source is Aurora.
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
		return AuroraResult{ProbeErr: err}, nil
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

	return AuroraResult{Throttlers: throttlers, MonitorDB: monitorDB, RedoAware: mode == redoAwareMode}, nil
}

// BuildAurora is setup.Build as a plain function, so a runner can hold it in a
// probe field that tests replace (CI has no Aurora to probe).
func BuildAurora(ctx context.Context, setup AuroraSetup) (AuroraResult, error) {
	return setup.Build(ctx)
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
	case parsermysql.ErrAccessDenied, parsermysql.ErrDBaccessDenied, parsermysql.ErrTableaccessDenied, parsermysql.ErrSpecificAccessDenied:
		return true
	default:
		return false
	}
}
