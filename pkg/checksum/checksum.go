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
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
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

	// DefaultYieldTimeout is the default maximum duration for a single checksum
	// pass before yielding to release long-running REPEATABLE READ transactions.
	DefaultYieldTimeout = 24 * time.Hour

	// fixChunkTimeout bounds the DELETE + REPLACE (or DELETE + Apply) pair that
	// recopies a mismatched chunk. The pair runs under a context derived from
	// context.WithoutCancel so a sentinel-drop cancellation can't leave the
	// target in a partial state between the two transactions. The bound still
	// catches the case where one transaction is hung. This applies to every
	// repair path (initial and continuous checksum), so it has to be generous
	// enough for legitimate large/slow recopies on busy or distant replicas.
	fixChunkTimeout = 10 * time.Minute
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

type CheckerConfig struct {
	Concurrency     int
	TargetChunkTime time.Duration
	DBConfig        *dbconn.DBConfig
	Logger          *slog.Logger
	FixDifferences  bool
	Watermark       string // optional; defines a watermark to start from
	MaxRetries      int
	Applier         applier.Applier // optional; indicates it is a distributed checker
	YieldTimeout    time.Duration   // maximum duration for a single checksum pass before yielding to release long-running transactions
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
	if config.Applier != nil {
		return &DistributedChecker{
			concurrency:    config.Concurrency,
			sourceDBs:      sourceDBs,
			feeds:          feeds,
			chunker:        chunker,
			dbConfig:       config.DBConfig,
			logger:         config.Logger,
			fixDifferences: config.FixDifferences,
			maxRetries:     config.MaxRetries,
			applier:        config.Applier,
		}, nil
	}
	return &SingleChecker{
		concurrency:    config.Concurrency,
		db:             sourceDBs[0],
		feed:           feeds[0],
		chunker:        chunker,
		dbConfig:       config.DBConfig,
		logger:         config.Logger,
		fixDifferences: config.FixDifferences,
		maxRetries:     config.MaxRetries,
		yieldTimeout:   config.YieldTimeout,
	}, nil
}
