// Package copier copies rows from one table to another.
// it makes use of tableinfo.Chunker, and does the parallelism
// and retries here. It fails on the first error.
package copier

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/throttler"
)

const (
	copyEstimateInterval   = 10 * time.Second // how frequently to re-estimate copy speed
	copyETAInitialWaitTime = 1 * time.Minute  // how long to wait before first estimating copy speed (to allow for fast start)
)

// etaEstimate returns the estimated remaining copy time and the state of that
// estimate. The duration is meaningful only when the state is status.ETAReady.
// No estimate is available before a copy rate has been measured (the first
// copyETAInitialWaitTime, or while no rows have been timed; status.ETAMeasuring)
// or once the copy is essentially complete (pct > 99.99; status.ETADue) — the
// callers present each case (GetETA renders "TBD"/"DUE", GetETAState returns the
// state and 0 seconds).
func etaEstimate(copiedRows, totalRows uint64, pct float64, rowsPerSecond uint64, startTime time.Time) (time.Duration, status.ETAState) {
	if pct > 99.99 {
		return 0, status.ETADue
	}
	if rowsPerSecond == 0 || time.Since(startTime) < copyETAInitialWaitTime {
		return 0, status.ETAMeasuring
	}
	// Divide the remaining rows by how many rows we copied in the last interval
	// per second. "remainingRows" might be the actual rows or the logical rows
	// since getCopyStats() and rowsPerSecond change estimation method when the PK
	// is auto-inc.
	remainingRows := totalRows - copiedRows
	remainingSeconds := math.Floor(float64(remainingRows) / float64(rowsPerSecond))
	return time.Duration(remainingSeconds * float64(time.Second)), status.ETAReady
}

// Copier is the interface which copiers use. The single implementation
// streams rows from the source through an applier to the target (the
// DBLog-style buffered algorithm; see buffered.go). The legacy unbuffered
// copier (INSERT IGNORE .. SELECT directly in MySQL) was removed after the
// buffered copier became the default (issue #908).
type Copier interface {
	Run(ctx context.Context) error
	GetETA() string
	// GetETAState returns the structured copy ETA: its availability (so callers
	// can distinguish "still measuring" from a real estimate or a near-complete
	// copy) and, when available, the estimated remaining time. It is the
	// structured counterpart of GetETA, computed in a single read so the state
	// and duration are always consistent.
	GetETAState() status.ETA
	GetChunker() table.Chunker
	SetThrottler(throttler throttler.Throttler)
	GetThrottler() throttler.Throttler
	StartTime() time.Time
	GetProgress() string
	// CopyProgress returns the same progress as GetProgress in numeric form,
	// which the status block needs in order to lay the percentage and the
	// row counts out as separate fields.
	CopyProgress() status.CopyProgress
	// ChunkSize returns the row count of the most recently claimed chunk, or
	// 0 before the first one. This is the dynamic chunker's current sizing
	// decision, and it is reported on the runner status block: it used to be
	// visible only inside the checkpoint line's watermark JSON, which is no
	// longer logged at INFO (#329).
	//
	// It is sampled from the chunk rather than read off the chunker so that
	// it works for every Chunker implementation, including the multi-table
	// chunker that fans out over per-table chunkers with sizes of their own.
	ChunkSize() uint64
}

// ChunkCopier is the incremental counterpart of Copier.Run: copying exactly
// one chunk, synchronously, with chunker feedback sent before it returns.
// Low-level tests (binlog watermark ordering, checkpoint stepping) assert on
// it to drive the copy one chunk at a time in a controlled order, which
// Run's parallel pipeline cannot do. The copier returned by NewCopier
// implements it.
type ChunkCopier interface {
	CopyChunk(ctx context.Context, chunk *table.Chunk) error
}

type CopierConfig struct {
	Concurrency int
	Throttler   throttler.Throttler
	Logger      *slog.Logger
	MetricsSink metrics.Sink
	DBConfig    *dbconn.DBConfig
	// Applier is used by the copier to write rows to the destination. It is
	// required (non-nil). It is also used by callers (migration/move runner)
	// for the replication client; construction is shared so that both paths
	// use the same applier.
	Applier applier.Applier
	// Autoscale configures experimental dynamic write-thread scaling. When
	// disabled (the default) the copier behaves exactly as before. See
	// AutoscaleConfig and issue #831.
	Autoscale AutoscaleConfig
}

// AutoscaleConfig controls the experimental write-thread autoscaler driven by
// throttler utilization. It only applies when the Applier implements the
// dynamic-scaling capability (SingleTargetApplier).
type AutoscaleConfig struct {
	// Enabled gates the whole feature (the --enable-experimental-autoscaling
	// flag). Off by default.
	Enabled bool
	// StartThreads is the resolved write-thread count the applier was started
	// at; the controller scales from here.
	StartThreads int
	// MaxThreads is the cap the controller may scale up to.
	MaxThreads int
	// MaxReadThreads is the cap for the read-worker pool, which scales from
	// CopierConfig.Concurrency. Zero means "derive it from Concurrency"
	// (ResolveMaxReadThreads) — for callers with no view of the instance size.
	// The migration runner passes an instance-derived ceiling instead, since a
	// read pool sized off a flag default has no relationship to the cores it is
	// competing for (see autoscale.ReadBounds).
	MaxReadThreads int
}

// NewCopierDefaultConfig returns a default config for the copier. Callers
// must supply an Applier (see CopierConfig.Applier).
func NewCopierDefaultConfig() *CopierConfig {
	return &CopierConfig{
		Concurrency: 4,
		Throttler:   &throttler.Noop{},
		Logger:      slog.Default(),
		MetricsSink: &metrics.NoopSink{},
		DBConfig:    dbconn.NewDBConfig(),
	}
}

// NewCopier creates a new copier object with the provided chunker.
// The chunker could have been opened at a watermark, we are agnostic to that.
// It could also return different tables on each Next() call in future,
// so we don't save any fields related to the table. Reads use each chunk's
// own table connection (chunk.Table.DB()) and writes go through the applier,
// so no database handle is passed here.
func NewCopier(chunker table.Chunker, config *CopierConfig) (Copier, error) {
	if chunker == nil {
		return nil, errors.New("chunker must be non-nil")
	}
	if config.DBConfig == nil {
		return nil, errors.New("dbConfig must be non-nil")
	}
	if config.Applier == nil {
		return nil, errors.New("copier requires a non-nil Applier")
	}
	return &buffered{
		concurrency: config.Concurrency,
		throttler:   config.Throttler,
		chunker:     chunker,
		logger:      config.Logger,
		metricsSink: config.MetricsSink,
		dbConfig:    config.DBConfig,
		applier:     config.Applier,
		autoscale:   config.Autoscale,
	}, nil
}
