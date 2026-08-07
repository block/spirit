// Package copier copies rows from one table to another.
// it makes use of tableinfo.Chunker, and does the parallelism
// and retries here. It fails on the first error.
package copier

import (
	"context"
	"database/sql"
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

// Copier is the interface which copiers use. Currently we only have
// one implementation, which we call unbuffered because it uses
// INSERT .. SELECT without any intermediate buffering in spirit.
// In future we may have another implementation, see:
// https://github.com/block/spirit/issues/451
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
}

// CompletedWorkReporter is an optional capability implemented by copiers that
// can report exact terminal aggregate work.
type CompletedWorkReporter interface {
	CompletedWork() (rows uint64, chunks uint64)
}

// CompletedWork returns authoritative aggregate rows and chunks completed by a
// built-in copier. The boolean is false for Copier implementations that do not
// implement CompletedWorkReporter.
func CompletedWork(c Copier) (rows uint64, chunks uint64, available bool) {
	reporter, ok := c.(CompletedWorkReporter)
	if !ok {
		return 0, 0, false
	}
	rows, chunks = reporter.CompletedWork()
	return rows, chunks, true
}

type CopierConfig struct {
	Concurrency     int
	TargetChunkTime time.Duration
	Throttler       throttler.Throttler
	Logger          *slog.Logger
	MetricsSink     metrics.Sink
	DBConfig        *dbconn.DBConfig
	// Applier is used by the buffered copier to write rows to the destination.
	// It is also used by callers (migration/move runner) for the replication
	// client. Construction is shared so that both paths use the same applier
	// for buffered copy (the default). When the unbuffered copier is selected
	// (Unbuffered=true) the copier ignores the applier.
	Applier applier.Applier
	// Unbuffered selects the legacy unbuffered copier, which issues
	// INSERT IGNORE INTO _new ... SELECT FROM original directly and ignores
	// Applier. When false (the default), the buffered copier is used, which
	// streams row images through Applier and therefore requires a non-nil
	// Applier. NewCopierDefaultConfig leaves this false (buffered), matching the
	// production default.
	Unbuffered bool
	// Autoscale configures experimental dynamic write-thread scaling. When
	// disabled (the default) the copier behaves exactly as before. See
	// AutoscaleConfig and issue #831.
	Autoscale AutoscaleConfig
}

// AutoscaleConfig controls the experimental write-thread autoscaler driven by
// throttler utilization. It only applies to the buffered copier whose Applier
// implements the dynamic-scaling capability (SingleTargetApplier).
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

// NewCopierDefaultConfig returns a default config for the copier. It defaults
// to the buffered copier (Unbuffered=false), matching the production default,
// so callers must supply an Applier (see CopierConfig.Applier). Tests that want
// the legacy unbuffered copier set Unbuffered=true, which needs no Applier.
func NewCopierDefaultConfig() *CopierConfig {
	return &CopierConfig{
		Concurrency:     4,
		TargetChunkTime: 1000 * time.Millisecond,
		Throttler:       &throttler.Noop{},
		Logger:          slog.Default(),
		MetricsSink:     &metrics.NoopSink{},
		DBConfig:        dbconn.NewDBConfig(),
	}
}

// NewCopier creates a new copier object with the provided chunker.
// The chunker could have been opened at a watermark, we are agnostic to that.
// It could also return different tables on each Next() call in future,
// so we don't save any fields related to the table.
func NewCopier(db *sql.DB, chunker table.Chunker, config *CopierConfig) (Copier, error) {
	if chunker == nil {
		return nil, errors.New("chunker must be non-nil")
	}
	if config.DBConfig == nil {
		return nil, errors.New("dbConfig must be non-nil")
	}
	if config.Unbuffered {
		return &Unbuffered{
			db:               db,
			concurrency:      config.Concurrency,
			throttler:        config.Throttler,
			chunker:          chunker,
			logger:           config.Logger,
			metricsSink:      config.MetricsSink,
			dbConfig:         config.DBConfig,
			copierEtaHistory: newcopierEtaHistory(),
		}, nil
	}
	if config.Applier == nil {
		return nil, errors.New("buffered copier requires a non-nil Applier")
	}
	return &buffered{
		db:               db,
		concurrency:      config.Concurrency,
		throttler:        config.Throttler,
		chunker:          chunker,
		logger:           config.Logger,
		metricsSink:      config.MetricsSink,
		dbConfig:         config.DBConfig,
		copierEtaHistory: newcopierEtaHistory(),
		applier:          config.Applier,
		autoscale:        config.Autoscale,
	}, nil
}
