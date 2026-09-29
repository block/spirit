// Package checksum verifies that a copy of a table matches its source while the
// source keeps taking writes. It is not in the row/ package because it requires
// a change feed to be passed in, which would cause a circular dependency.
//
// There are two checkers. LocklessChecker (CheckerConfig.Lockless) takes no
// locks, spans servers, and is what move and sync use. SingleChecker, the
// default, compares two tables on one server under a snapshot taken behind a
// brief table lock. The intent is for lockless to replace it, so Single-only
// code is kept in the single*.go files, which can then be deleted whole.
package checksum

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/throttler"
)

var (
	// ErrDifferencesExhausted is returned by Run when every attempt completed
	// but kept finding row differences. The table is diverging in a way the
	// repairs cannot close, so a further attempt reproduces it: a lossy ALTER
	// (adding a UNIQUE index to non-unique data being the common one), or a
	// bug. Callers that decide whether to retry should not.
	ErrDifferencesExhausted = errors.New("checksum found differences on every attempt")

	// ErrAttemptsExhausted is returned by Run when every attempt errored before
	// it could compare the whole table — killed connections, a cancelled
	// context, a failure inside a pass. Nothing has been proven about the data,
	// and the condition may well be gone by the next attempt. It wraps the last
	// attempt's error, which is the one worth triaging.
	ErrAttemptsExhausted = errors.New("checksum errored on every attempt")

	// fixChunkTimeout bounds the DELETE + Apply pair that recopies a mismatched
	// chunk. The pair runs under a context derived from context.WithoutCancel so
	// a sentinel-drop cancellation can't leave the target in a partial state
	// between the two steps. The bound still catches the case where one of them
	// is hung. This applies to every repair path (initial and continuous
	// checksum), so it has to be generous enough for legitimate large/slow
	// recopies on busy or distant replicas.
	fixChunkTimeout = 10 * time.Minute
)

const (
	// repairBatchRows and repairBatchBytes bound how much of a mismatched chunk
	// chunkRepairer.Recopy holds in memory at once: source rows are read in
	// batches and each batch is handed to the applier, which splits it further
	// into the statements it writes. Both bounds are deliberately of the same
	// order as the applier's own chunklet limits, so a batch is roughly one
	// statement's worth of rows and the read stays a little ahead of the writers
	// without buffering a whole (possibly enormous) chunk. The byte half is
	// measured with utils.EstimateRenderedRowSize, the same (approximate, cheap)
	// accounting the applier uses to cut its own statements.
	repairBatchRows  = 1000
	repairBatchBytes = applier.MaxStatementSizeBytes

	// DefaultConcurrency is the worker count every checker starts at when the
	// caller does not choose one. Four readers is enough to keep a chunk in
	// flight per available connection on a small instance without being a
	// meaningful share of a large one's capacity; the autoscaler moves it from
	// there when it is enabled.
	DefaultConcurrency = 4

	// defaultMaxRetries is how many whole-run attempts every checker makes
	// before giving up. Retrying is for transient infrastructure failures, so
	// the useful range is small: a third attempt that fails the way the first
	// two did is not going to be fixed by a fourth.
	defaultMaxRetries = 3
)

type Checker interface {
	// SetThrottler installs pacing before Run. Every finite checker supports it.
	SetThrottler(throttler.Throttler)
	// ResumeWatermark returns safe verification progress, or an empty string when
	// a resumed run must recheck everything. Read it instead of the walker watermark.
	ResumeWatermark() (string, error)
	// Run performs finite verification. A nil result authorizes completion;
	// deferred ranges and repairs alone are not verification.
	Run(ctx context.Context) error
	// RunContinuous reuses a successfully completed checker for background
	// verification. Calls to Run and RunContinuous must be sequential. A nil
	// return means cancellation was safe, not that the interrupted pass verified
	// every row. Other errors abort cutover. Once started, ResumeWatermark stays
	// empty: restarting requires full initial verification.
	RunContinuous(context.Context) error
	// ContinuousActive distinguishes a running pass from interval pacing.
	// It is safe to query concurrently with RunContinuous.
	ContinuousActive() bool
	// GetProgress returns the structured checksum progress — rows verified so far
	// and the total to verify. Call String() on the result for the display form.
	GetProgress() status.ChecksumProgress
	StartTime() time.Time
	ExecTime() time.Duration
	// DifferencesFound reports observed mismatches, including transient ones.
	// Snapshot checkers count the current pass; optimistic verification counts
	// across passes within Run. This is not resume evidence: use ResumeWatermark.
	DifferencesFound() uint64
}

// AutoscaleConfig controls the checksum phase's worker-count control loop. It
// mirrors copier.AutoscaleConfig, minus a StartThreads field — the checksum
// starts at CheckerConfig.Concurrency.
//
// Enabled only turns on *scaling*. The throttler hard-stop applies either way:
// a checksum with autoscaling disabled still pauses when the throttler says to,
// which before this existed it did not do at all.
type AutoscaleConfig struct {
	Enabled bool
	// MaxThreads is the ceiling scaling may reach. The transaction pools are
	// provisioned at this size whether or not Enabled is set, so callers must
	// budget connections for it (see SingleChecker.initConnPool for why the
	// pools cannot grow on demand). Values below Concurrency are raised to it.
	MaxThreads int
}

// loadOnlyThrottler narrows a throttler to the signals a checksum should react
// to, and is applied to every throttler a checker is given (at construction and
// via SetThrottler) so the rule holds however the checker was wired. nil yields
// a Noop.
//
// A checksum reacts to *load* and ignores binary budget signals — in practice,
// replica lag. It reads inside a REPEATABLE READ snapshot and writes nothing to
// the binlog, so it cannot be the cause of replica lag and pausing it cannot
// reduce that lag; what the pause does do is extend the pass, holding the
// snapshot open and pinning undo that the purge thread cannot advance past. The
// replica-lag throttler also fails closed on stale polling, so an unreachable
// replica would stall dispatch until the yield timeout with the snapshot still
// held. Load is different in kind: a checksum genuinely adds read load to the
// primary, so backing off on load both works and is warranted.
//
// The one part of a checksum that does replicate is a chunk repair, and it is
// deliberately left unpaced: repairs are rare and small, and blocking one incurs
// exactly the snapshot-hold cost this narrowing exists to avoid.
func loadOnlyThrottler(t throttler.Throttler) throttler.Throttler {
	if t == nil {
		return &throttler.Noop{}
	}
	return throttler.GradualOnly(t)
}

// Paced is the optional capability a Checker exposes when it can report how it
// is currently being paced. The runner status block uses it so a slow checksum can be
// told apart from a throttled or scaled-down one — the same question the copier
// row's throttled= answers for the copy phase.
type Paced interface {
	// Threads is the live worker count, which the autoscaler may have moved
	// away from the configured concurrency.
	Threads() int
	// IsThrottled reports whether the throttler is currently telling the
	// checksum to pause.
	IsThrottled() bool
	// ChunkSize is the row count of the most recently checksummed chunk. The
	// checksum sizes its chunks dynamically just as the copy does, so the same
	// field is worth watching in both phases.
	ChunkSize() uint64
}

// StatusSuffix renders the pacing fields for the checksum row of a runner
// status block, or "" if the checker does not report them. It keeps the leading
// two spaces used between fields within a row, so callers can append it
// unconditionally.
func StatusSuffix(c Checker) string {
	p, ok := c.(Paced)
	if !ok {
		return ""
	}
	return fmt.Sprintf("  chunk-size=%d  threads=%d  throttled=%v", p.ChunkSize(), p.Threads(), p.IsThrottled())
}

type CheckerConfig struct {
	// Lockless selects LocklessChecker; false (the zero value) selects
	// SingleChecker. It is the only thing that selects a checker — the Applier
	// is a repair write path, not a switch. Lockless is the only one that
	// spans servers: a sync (TargetDB), and a move, which reads N sources
	// against the applier's M targets and aggregates each chunk across all of
	// them. SingleChecker is expected to be removed in its favour.
	Lockless    bool
	Concurrency int
	// TargetChunkTime is reporting-only: it is the target the chunk-size
	// distribution summary is compared against at the end of each pass, so it
	// should match the TargetChunkTime the caller built the chunker with
	// (table.ChunkerDefaultTarget unless the caller overrode it). It does not
	// itself size anything — chunk sizing lives entirely in the chunker.
	TargetChunkTime time.Duration
	DBConfig        *dbconn.DBConfig
	Logger          *slog.Logger
	// FixDifferences is the repair policy, and it means the same thing to both
	// checkers: when set, a mismatched chunk is rewritten from the source and
	// re-verified; when unset, a mismatch is reported as an error. The factory
	// turns it into the Recopier the checker actually repairs through — see
	// newRecopier, which also says which applier field that write path needs.
	FixDifferences bool
	// Watermark is verification evidence from a previous run: every row below
	// it was read on both sides and observed equal. Supplying it makes the
	// factory open the chunker there, so verification resumes rather than
	// restarting; leave the chunker unopened when supplying it. Every checker
	// honours it — the claim it encodes does not depend on which checker
	// observed it. Take it from Checker.ResumeWatermark, never from the
	// chunker's traversal watermark.
	Watermark string
	// MaxRetries bounds whole-run attempts for every checker: a transient
	// infrastructure failure costs an attempt rather than the migration.
	MaxRetries int
	// Applier is the write path a mismatched chunk is rewritten through. It is
	// the caller's to supply because every runner already has one, and building
	// a second here would hide which one a repair actually goes through.
	//
	// Required when FixDifferences is set — a checker that cannot repair should
	// fail to build, not on the first mismatch hours in. A repair starts and
	// stops it around each rewrite, since repairs are rare and serialized.
	// Lockless also reads GetTargets from it to find the copy being verified
	// (see resolveLocklessTargets), which is how a move names its M targets.
	Applier applier.Applier
	// Throttler paces the checksum. Optional: nil installs a Noop, and callers
	// that build the checker before their throttlers are open should use
	// SetThrottler instead (the migration runner does).
	//
	// Whatever is passed is narrowed by loadOnlyThrottler — a checksum reacts to
	// load signals and ignores binary ones such as replica lag.
	Throttler throttler.Throttler
	// Autoscale configures the worker-count control loop.
	Autoscale AutoscaleConfig
	// MetricsSink is where the control loop reports its gauges. Optional.
	MetricsSink metrics.Sink

	// ---------------------------------------------------------------------
	// Single-only. Ignored when Lockless is set.
	// ---------------------------------------------------------------------

	// YieldTimeout is the maximum duration for a single checksum pass before
	// yielding to release long-running transactions. Lockless reads are short
	// by construction and hold no snapshot to yield.
	YieldTimeout time.Duration

	// ---------------------------------------------------------------------
	// Lockless-only. Ignored unless Lockless is set.
	// ---------------------------------------------------------------------

	// RetryDelay is the minimum wait between attempts for any given chunk —
	// measured from the *last* attempt of that chunk, not from the original
	// failure. Default DefaultLocklessRetryDelay (5s). It is also what paces
	// the re-walk between finite passes.
	RetryDelay time.Duration

	// RetryFlushWait bounds how long a retry additionally waits for every
	// change feed to complete a flush since the attempt that queued it. The
	// target only moves when a feed flushes, so a retry before then re-reads
	// an image it has already seen. It is an upper bound, not a delay: the
	// retry runs as soon as every feed has flushed (and RetryDelay has
	// passed), and without feeds there is no wait at all. Default
	// DefaultLocklessRetryFlushWait (2 × change.DefaultFlushInterval), which
	// assumes the default flush interval: a caller whose feeds flush less
	// often should set it to twice their interval.
	RetryFlushWait time.Duration

	// MaxQueueSize is the cap on entries in the delayed-retry queue. Reaching
	// it stalls the walker until retries drain rather than failing the run.
	// Default 1024.
	MaxQueueSize int

	// MaxHotAttempts is the number of observations allowed for a chunk whose
	// source signature keeps changing. Once reached, the chunk is deferred to
	// the next pass rather than holding the current pass open forever. Default
	// 10; a positive value below 2 is clamped to 2 because detecting a source
	// change requires an initial read and a retry. Deferral never counts as
	// verification and makes the pass not clean.
	MaxHotAttempts int

	// MinPassInterval is the minimum wall-clock time between the start of one
	// pass and the start of the next, measured from the previous pass's start
	// (a pass that already ran longer incurs no extra wait). Zero means passes
	// run back-to-back, which is convenient for tests but heavy in production:
	// RunContinuous substitutes LocklessMinPassInterval (1h) so a small table
	// whose pass finishes in seconds does not re-scan continuously, and the
	// finite gate substitutes RetryDelay. The wait honours cancellation.
	MinPassInterval time.Duration

	// MaxPasses bounds Run and RunUntilClean: once this many passes have
	// completed without one of them being clean, verification gives up with
	// ErrVerificationUnresolved rather than walking the table again. Zero means
	// unbounded, which is what RunContinuous always is; NewChecker defaults it
	// to DefaultLocklessMaxPasses so the finite gate terminates.
	MaxPasses int

	// TargetDB is the server holding the copy being verified, when that is not
	// the server being read from. Nil — the usual case — means the target lives
	// on sourceDBs[0], which is true of a migration (the shadow table sits
	// beside the original). A sync verifies two servers, so it supplies one.
	//
	// Setting it also changes the shape of a repair: a cross-server rewrite has
	// no ColumnMapping to apply, because both sides hold the same logical
	// table. See newRecopier.
	TargetDB *sql.DB

	// ExternalFlushLoop says the caller runs the feed's periodic flush itself,
	// for longer than any one run, so a run must not start and stop it. The
	// default (false) is that the checker owns flushing for the duration of a
	// run, which is what a migration and a move want: nothing else is driving
	// the feed while verification is the only thing happening.
	//
	// A sync is the opposite — its flush loop runs for the whole process at its
	// own configured interval, and a verification pass stopping it on the way
	// out would be stopping someone else's goroutine.
	ExternalFlushLoop bool
}

// defaultedConcurrency resolves a configured worker count. A concurrency of at
// least 1 is required for the limiter and the transaction pools to be usable —
// historically a zero here produced a pool of zero transactions and a checksum
// that could not run — and an unset one means the caller did not choose, so it
// gets the default rather than the minimum.
func defaultedConcurrency(n int) int {
	if n <= 0 {
		return DefaultConcurrency
	}
	return n
}

// applySharedDefaults fills in the settings that mean the same thing to both
// checkers. The checker-specific ones are newLocklessChecker's and
// newSingleChecker's; keeping the sets apart is what stops a default that only
// one checker reads from looking like part of the common contract.
func applySharedDefaults(cfg *CheckerConfig) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = defaultMaxRetries
	}
	cfg.Concurrency = defaultedConcurrency(cfg.Concurrency)
	// The ceiling can never be below the start value: the pools are sized to
	// it, and a pool smaller than the starting worker count would starve.
	cfg.Autoscale.MaxThreads = max(cfg.Autoscale.MaxThreads, cfg.Concurrency)
	cfg.Throttler = loadOnlyThrottler(cfg.Throttler)
}

func NewCheckerDefaultConfig() *CheckerConfig {
	return &CheckerConfig{
		Concurrency:     DefaultConcurrency,
		TargetChunkTime: table.ChunkerDefaultTarget,
		DBConfig:        dbconn.NewDBConfig(),
		Logger:          slog.Default(),
		FixDifferences:  false,
		MaxRetries:      defaultMaxRetries,
		YieldTimeout:    DefaultYieldTimeout,
	}
}

// newRecopier builds the repair path a mismatched chunk is rewritten through,
// or returns nil when the caller did not ask for repairs. Every checker goes
// through this one function, so the answer to "what happens on a divergence?"
// does not depend on which checker the config selected: a nil recopier means
// the mismatch is reported as an error, and a non-nil one means the chunk is
// rewritten and re-verified.
//
// Only the shape of the rewrite depends on the topology. A cross-server target named by TargetDB reads from one
// server and writes to another, and has no ColumnMapping to apply because both
// sides hold the same logical table. Everything else — one server holding both
// copies (a migration), or a lockless check of N sources routed onto the
// applier's M targets (a move) — deletes the range on every target and rewrites
// the merged source rows through the ColumnMapping, which is exactly what makes
// a migration's repair correct across a rename or a drop.
//
// config.DBConfig is the write side's in every case: it configures the DELETE
// that clears the range on the target before the rows are rewritten.
func newRecopier(sourceDBs, targetDBs []*sql.DB, config *CheckerConfig) (Recopier, error) {
	if !config.FixDifferences {
		return nil, nil
	}
	if config.Applier == nil {
		return nil, errors.New("applier must be non-nil to repair differences")
	}
	switch {
	case config.TargetDB != nil:
		return newMySQLRecopier(sourceDBs[0], config.TargetDB, config.Applier, config.DBConfig, config.Logger)
	default:
		return newChunkRepairer(sourceDBs, targetDBs, config.Applier, config.DBConfig, config.Logger), nil
	}
}

// resolveLocklessTargets decides which servers hold the copy a lockless check
// verifies. TargetDB wins when set (a sync); otherwise the applier's targets,
// so the checksum reads back from exactly where the copy and a repair write (a
// move, or a migration, whose applier targets the source's own handle);
// otherwise the copy sits beside the one source.
//
// Without a TargetDB or applier targets nothing says where N sources' rows
// went, and guessing sourceDBs[0] would verify one shard's slice and report
// the whole topology clean, so that shape is refused.
//
// Applier targets are reduced to distinct handles. A chunk read carries no key
// range, so two targets that share a handle (disjoint ranges written into one
// table) would each return every row of the chunk: the summed count would
// double and a correct copy would be judged divergent, and a repair would
// delete the same range twice. Reading each handle once reads each row once.
func resolveLocklessTargets(sourceDBs []*sql.DB, config *CheckerConfig) ([]*sql.DB, error) {
	if config.TargetDB != nil {
		if len(sourceDBs) != 1 {
			return nil, fmt.Errorf("TargetDB requires exactly one source, got %d", len(sourceDBs))
		}
		return []*sql.DB{config.TargetDB}, nil
	}
	if config.Applier != nil {
		if targets := config.Applier.GetTargets(); len(targets) != 0 {
			dbs := make([]*sql.DB, 0, len(targets))
			for i, target := range targets {
				if target.DB == nil {
					return nil, fmt.Errorf("applier target %d has no connection", i)
				}
				if !slices.Contains(dbs, target.DB) {
					dbs = append(dbs, target.DB)
				}
			}
			return dbs, nil
		}
	}
	if len(sourceDBs) != 1 {
		return nil, fmt.Errorf("lockless verification of %d sources requires an applier to name the targets", len(sourceDBs))
	}
	return []*sql.DB{sourceDBs[0]}, nil
}

// NewChecker creates a new checksum object. CheckerConfig.Lockless picks which
// one, and nothing else does; the zero value is SingleChecker.
//
// sourceDBs contains the source database connections (one for single-source
// migrations, multiple for N:M moves), each paired with the feed at the same
// index. Lockless aggregates checksums across all of them; Single accepts
// exactly one. For Single the copy being verified lives on sourceDBs[0] too;
// for Lockless it is wherever resolveLocklessTargets says — TargetDB (how a
// sync verifies across two servers), else the applier's targets, else
// sourceDBs[0]. Open the chunker before construction unless supplying
// Watermark, in which case the factory opens it according to policy.
func NewChecker(sourceDBs []*sql.DB, chunker table.Chunker, feeds []change.Source, config *CheckerConfig) (Checker, error) {
	if config == nil {
		return nil, errors.New("config must be non-nil")
	}
	if len(sourceDBs) == 0 {
		return nil, errors.New("at least one source database must be provided")
	}
	if len(feeds) == 0 {
		return nil, errors.New("at least one feed must be provided")
	}
	if chunker == nil {
		return nil, errors.New("chunker must be non-nil")
	}
	if config.DBConfig == nil {
		return nil, errors.New("dbconfig must be non-nil")
	}
	var targetDBs []*sql.DB
	if config.Lockless {
		if err := checkLocklessTopology(sourceDBs, feeds); err != nil {
			return nil, err
		}
		var err error
		if targetDBs, err = resolveLocklessTargets(sourceDBs, config); err != nil {
			return nil, err
		}
	} else {
		if err := checkSingleTopology(sourceDBs, feeds, config); err != nil {
			return nil, err
		}
		targetDBs = []*sql.DB{sourceDBs[0]}
	}

	// Everything from here is shared: the same defaults, the same repair
	// policy, the same resume and pacing wiring, whichever checker the config
	// selects. Work on a copy so none of it is visible to the caller's config.
	cfg := *config
	applySharedDefaults(&cfg)
	recopier, err := newRecopier(sourceDBs, targetDBs, &cfg)
	if err != nil {
		return nil, err
	}
	// A watermark is verification evidence, whichever checker produced it:
	// every row below it was observed equal and the change feed has kept it
	// that way since. Optimistic verification only publishes one for a prefix
	// it has actually resolved (see LocklessChecker.ResumeWatermark), so
	// resuming at it is the same trade the snapshot checker makes.
	if cfg.Watermark != "" {
		if err := chunker.OpenAtWatermark(cfg.Watermark); err != nil {
			return nil, err
		}
	}

	if !cfg.Lockless {
		return newSingleChecker(sourceDBs[0], chunker, feeds[0], recopier, &cfg), nil
	}
	checker := newLocklessChecker(sourceDBs, targetDBs, chunker, feeds, recopier, &cfg)
	checker.ownsFeedFlush = !cfg.ExternalFlushLoop
	return checker, nil
}

// checkLocklessTopology validates the sources and feeds a lockless checker
// reads. Every source is read, and each one's feed is what keeps its apply lag
// from being judged divergence, so they must pair up. A source handle may not
// repeat: it would be read once per occurrence and its rows counted twice,
// and unlike a shared target it cannot be collapsed, because each occurrence
// is paired with its own feed.
func checkLocklessTopology(sourceDBs []*sql.DB, feeds []change.Source) error {
	if len(feeds) != len(sourceDBs) {
		return fmt.Errorf("lockless verification requires one feed per source, got %d sources and %d feeds", len(sourceDBs), len(feeds))
	}
	for i := range sourceDBs {
		if sourceDBs[i] == nil || feeds[i] == nil {
			return fmt.Errorf("lockless verification requires a non-nil source and feed, source %d has none", i)
		}
		if slices.Contains(sourceDBs[:i], sourceDBs[i]) {
			return fmt.Errorf("lockless verification requires distinct sources, source %d repeats an earlier handle", i)
		}
	}
	return nil
}

func waitForChecksum(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(max(0, delay))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return ctx.Err() == nil
	}
}

// Accept wrapped cancellation, but not a joined cancellation plus a real error.
func checksumCanceled(err error) bool {
	var joined interface{ Unwrap() []error }
	return !errors.As(err, &joined) && errors.Is(err, context.Canceled)
}
