package copier

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/throttler"
	"github.com/siddontang/go-log/loggers"
	"github.com/sirupsen/logrus"
)

const (
	copyEstimateInterval   = 10 * time.Second // how frequently to re-estimate copy speed
	copyETAInitialWaitTime = 1 * time.Minute  // how long to wait before first estimating copy speed (to allow for fast start)
)

// Copier is the interface which copiers use. Currently we only have
// one implementation, which we call unbuffered because it uses
// INSERT .. SELECT without any intermediate buffering in spirit.
// In future we may have another implementation, see:
// https://github.com/block/spirit/issues/451
type Copier interface {
	Run(ctx context.Context) error
	GetETA() string
	GetChunker() table.Chunker
	SetThrottler(throttler throttler.Throttler)
	GetThrottler() throttler.Throttler
	StartTime() time.Time
	GetProgress() string
}

type CopierConfig struct {
	Concurrency     int
	TargetChunkTime time.Duration
	FinalChecksum   bool
	Throttler       throttler.Throttler
	Logger          loggers.Advanced
	MetricsSink     metrics.Sink
	DBConfig        *dbconn.DBConfig
}

// NewCopierDefaultConfig returns a default config for the copier.
func NewCopierDefaultConfig() *CopierConfig {
	return &CopierConfig{
		Concurrency:     4,
		TargetChunkTime: 1000 * time.Millisecond,
		FinalChecksum:   true,
		Throttler:       &throttler.Noop{},
		Logger:          logrus.New(),
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
	return &Unbuffered{
		db:               db,
		concurrency:      config.Concurrency,
		finalChecksum:    config.FinalChecksum,
		throttler:        config.Throttler,
		chunker:          chunker,
		logger:           config.Logger,
		metricsSink:      config.MetricsSink,
		dbConfig:         config.DBConfig,
		copierEtaHistory: newcopierEtaHistory(),
	}, nil
}
