package checksum

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
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
	// Query template for row checksums. The first %s is the column expression
	// list from table.ColumnMapping.ChecksumExprs(), which already interleaves
	// a '#' separator between values so content cannot shift across column
	// boundaries undetected.
	queryTemplate = "SELECT CRC32(CONCAT(%s)) as row_checksum, CONCAT_WS(',', %s) as pk FROM %s WHERE %s"

	// ErrYieldTimeout is returned by runChecksum when the yield timeout expires.
	// This is distinct from the parent context being canceled, and signals that
	// the checksum should resume from the current watermark after releasing
	// long-running transactions to reduce HLL (history list length) growth.
	ErrYieldTimeout = errors.New("checksum yield timeout")

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

	// DefaultYieldTimeout is the default maximum duration for a single checksum
	// pass before yielding to release long-running REPEATABLE READ transactions.
	DefaultYieldTimeout = 24 * time.Hour

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
	// SingleChecker.replaceChunk holds in memory at once: source rows are read in
	// batches and each batch is handed to the applier, which splits it further
	// into the statements it writes. Both bounds are deliberately of the same
	// order as the applier's own chunklet limits, so a batch is roughly one
	// statement's worth of rows and the read stays a little ahead of the writers
	// without buffering a whole (possibly enormous) chunk. The byte half is
	// measured with applier.EstimateRowSize, the same (approximate, cheap)
	// accounting the applier uses to cut its own statements.
	repairBatchRows  = 1000
	repairBatchBytes = applier.MaxStatementSizeBytes
)

// chunkMismatch describes why a chunk's source and target disagreed. It is
// returned by compareChunk so the caller can log a debuggable reason while
// treating any mismatch (checksum OR row count) identically — same retry,
// recopy, and differencesFound accounting.
type chunkMismatch struct {
	// checksumDiffers is true when the (aggregated) source and target CRC
	// differ.
	checksumDiffers bool
	// countDiffers is true when the (aggregated) source and target row
	// counts differ. This is the defense-in-depth signal that the CRC alone
	// can miss: BIT_XOR is pair-cancelling, so a row duplicated across two
	// sources (violating disjointness) or a row whose CRC32 happens to be 0
	// contributes nothing to the XOR, yet the count still moves.
	countDiffers bool
}

// mismatched reports whether the chunk is divergent for any reason.
func (m chunkMismatch) mismatched() bool {
	return m.checksumDiffers || m.countDiffers
}

// reason returns a human-readable description distinguishing a checksum
// mismatch from a row-count mismatch (and reporting both when both differ)
// for log/error debuggability. Only meaningful when mismatched() is true.
func (m chunkMismatch) reason(srcCount, tgtCount uint64) string {
	switch {
	case m.checksumDiffers && m.countDiffers:
		return fmt.Sprintf("checksum mismatch and row count mismatch (src=%d, target=%d)", srcCount, tgtCount)
	case m.countDiffers:
		return fmt.Sprintf("row count mismatch (src=%d, target=%d)", srcCount, tgtCount)
	default:
		return "checksum mismatch"
	}
}

// compareChunk is the central decision function used by every checker to
// decide whether a chunk's source and target agree. It compares BOTH the
// (aggregated) CRC and the (aggregated) row count. Comparing the count is
// free — the count is already returned alongside the CRC in the same query —
// and it closes a defense-in-depth gap where the CRC alone is insufficient
// (see chunkMismatch.countDiffers). A count mismatch is treated exactly like
// a checksum mismatch by callers.
func compareChunk(srcCRC, tgtCRC int64, srcCount, tgtCount uint64) chunkMismatch {
	return chunkMismatch{
		checksumDiffers: srcCRC != tgtCRC,
		countDiffers:    srcCount != tgtCount,
	}
}

type Checker interface {
	// Run performs the checksum operation.
	Run(ctx context.Context) error
	// GetProgress returns the structured checksum progress — rows verified so far
	// and the total to verify. Call String() on the result for the display form.
	GetProgress() status.ChecksumProgress
	StartTime() time.Time
	ExecTime() time.Duration
	// DifferencesFound returns the number of chunks where a source/target
	// mismatch was detected during the most recent (or in-flight) pass.
	// Useful for callers that need to distinguish "clean cancellation" from
	// "cancellation while a fix may have been mid-flight" — the continuous-
	// checksum loop uses it to decide whether a sentinel-drop swallow is
	// safe.
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

// ThrottleAware is the optional capability a Checker exposes when it can pace
// itself against a throttler installed after construction. SingleChecker and
// DistributedChecker implement it; runners build their checker before the
// throttlers are open, so they type-assert for this and wire it later.
//
// It is an optional interface rather than part of Checker so that test doubles
// and the continuous checker (which manages its own pacing) do not have to
// carry a method they have no use for.
type ThrottleAware interface {
	SetThrottler(t throttler.Throttler)
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
	Concurrency     int
	TargetChunkTime time.Duration
	DBConfig        *dbconn.DBConfig
	Logger          *slog.Logger
	FixDifferences  bool
	Watermark       string // optional; defines a watermark to start from
	MaxRetries      int
	Applier         applier.Applier // optional; indicates it is a distributed checker
	// RepairApplier is the write path the single-server checker rewrites a
	// mismatched chunk through (see SingleChecker.replaceChunk). Required for
	// that checker, whether or not FixDifferences is set — a checker that cannot
	// repair should fail to build, not on the first mismatch hours in. Ignored
	// when Applier is set, because that selects the distributed checker, which
	// repairs through Applier itself.
	RepairApplier applier.Applier
	YieldTimeout  time.Duration // maximum duration for a single checksum pass before yielding to release long-running transactions
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
}

func NewCheckerDefaultConfig() *CheckerConfig {
	return &CheckerConfig{
		Concurrency:     4,
		TargetChunkTime: 1000 * time.Millisecond,
		DBConfig:        dbconn.NewDBConfig(),
		Logger:          slog.Default(),
		FixDifferences:  false,
		MaxRetries:      3,
		YieldTimeout:    DefaultYieldTimeout,
	}
}

// NewChecker creates a new checksum object.
// sourceDBs contains the source database connections (one for single-source migrations,
// multiple for N:M moves). The distributed checker aggregates checksums across all sources.
// The single checker uses sourceDBs[0].
func NewChecker(sourceDBs []*sql.DB, chunker table.Chunker, feeds []change.Source, config *CheckerConfig) (Checker, error) {
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
	if config.MaxRetries == 0 {
		config.MaxRetries = 3
	}
	if config.YieldTimeout == 0 {
		config.YieldTimeout = DefaultYieldTimeout
	}
	// A concurrency of at least 1 is required for the limiter and the
	// transaction pools to be usable; historically a zero here produced a pool
	// of zero transactions and a checksum that could not run.
	concurrency := max(config.Concurrency, 1)
	// The ceiling can never be below the start value: the pools are sized to
	// it, and a pool smaller than the starting worker count would starve.
	maxConcurrency := max(config.Autoscale.MaxThreads, concurrency)
	thr := loadOnlyThrottler(config.Throttler)
	if config.Applier != nil {
		return &DistributedChecker{
			concurrency:     concurrency,
			maxConcurrency:  maxConcurrency,
			autoscale:       config.Autoscale.Enabled,
			throttler:       thr,
			metricsSink:     config.MetricsSink,
			targetChunkTime: config.TargetChunkTime,
			sourceDBs:       sourceDBs,
			feeds:           feeds,
			chunker:         chunker,
			dbConfig:        config.DBConfig,
			logger:          config.Logger,
			fixDifferences:  config.FixDifferences,
			maxRetries:      config.MaxRetries,
			applier:         config.Applier,
			yieldTimeout:    config.YieldTimeout,
		}, nil
	}
	// The single-server checker repairs a mismatched chunk through an applier
	// (see SingleChecker.replaceChunk), and it is the caller's to supply: every
	// runner already has one, and building a second write path here would hide
	// which one a repair actually goes through.
	if config.RepairApplier == nil {
		return nil, errors.New("repair applier must be non-nil")
	}
	return &SingleChecker{
		concurrency:     concurrency,
		maxConcurrency:  maxConcurrency,
		autoscale:       config.Autoscale.Enabled,
		throttler:       thr,
		metricsSink:     config.MetricsSink,
		targetChunkTime: config.TargetChunkTime,
		db:              sourceDBs[0],
		feed:            feeds[0],
		chunker:         chunker,
		dbConfig:        config.DBConfig,
		logger:          config.Logger,
		fixDifferences:  config.FixDifferences,
		maxRetries:      config.MaxRetries,
		repairApplier:   config.RepairApplier,
		yieldTimeout:    config.YieldTimeout,
	}, nil
}
