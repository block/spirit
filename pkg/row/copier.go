// Package row copies rows from one table to another.
// it makes use of tableinfo.Chunker, and does the parallelism
// and retries here. It fails on the first error.
package row

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/throttler"
	"github.com/block/spirit/pkg/utils"
	"github.com/siddontang/go-log/loggers"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

const (
	copyEstimateInterval   = 10 * time.Second // how frequently to re-estimate copy speed
	copyETAInitialWaitTime = 1 * time.Minute  // how long to wait before first estimating copy speed (to allow for fast start)
)

type Copier struct {
	sync.Mutex
	db                *sql.DB
	chunker           table.Chunker
	concurrency       int
	finalChecksum     bool
	CopyRowsStartTime time.Time
	CopyRowsExecTime  time.Duration
	rowsPerSecond     uint64
	isInvalid         bool
	startTime         time.Time
	ExecTime          time.Duration
	Throttler         throttler.Throttler
	dbConfig          *dbconn.DBConfig
	logger            loggers.Advanced
	metricsSink       metrics.Sink
	copierEtaHistory  *copierEtaHistory
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
func NewCopier(db *sql.DB, chunker table.Chunker, config *CopierConfig) (*Copier, error) {
	if chunker == nil {
		return nil, errors.New("chunker must be non-nil")
	}
	if config.DBConfig == nil {
		return nil, errors.New("dbConfig must be non-nil")
	}
	return &Copier{
		db:               db,
		concurrency:      config.Concurrency,
		finalChecksum:    config.FinalChecksum,
		Throttler:        config.Throttler,
		chunker:          chunker,
		logger:           config.Logger,
		metricsSink:      config.MetricsSink,
		dbConfig:         config.DBConfig,
		copierEtaHistory: newcopierEtaHistory(),
	}, nil
}

// CopyChunk copies a chunk from the table to the newTable.
// it is public so it can be used in tests incrementally.
func (c *Copier) CopyChunk(ctx context.Context, chunk *table.Chunk) error {
	c.Throttler.BlockWait()
	startTime := time.Now()
	// INSERT INGORE because we can have duplicate rows in the chunk because in
	// resuming from checkpoint we will be re-applying some of the previous executed work.
	query := fmt.Sprintf("INSERT IGNORE INTO %s (%s) SELECT %s FROM %s FORCE INDEX (PRIMARY) WHERE %s",
		chunk.NewTable.QuotedName,
		utils.IntersectNonGeneratedColumns(chunk.Table, chunk.NewTable),
		utils.IntersectNonGeneratedColumns(chunk.Table, chunk.NewTable),
		chunk.Table.QuotedName,
		chunk.String(),
	)
	c.logger.Debugf("running chunk: %s, query: %s", chunk.String(), query)
	var affectedRows int64
	var err error
	if affectedRows, err = dbconn.RetryableTransaction(ctx, c.db, c.finalChecksum, c.dbConfig, query); err != nil {
		return err
	}
	// Send feedback which can be used by the chunker
	// and infoschema to create a low watermark.
	chunkProcessingTime := time.Since(startTime)

	// Send feedback to chunker with processing time and statistics
	c.chunker.Feedback(chunk, chunkProcessingTime, uint64(affectedRows))

	// Send metrics
	err = c.sendMetrics(ctx, chunkProcessingTime, chunk.ChunkSize, uint64(affectedRows))
	if err != nil {
		// we don't want to stop processing if metrics sending fails, log and continue
		c.logger.Errorf("error sending metrics from copier: %v", err)
	}
	return nil
}

func (c *Copier) isHealthy(ctx context.Context) bool {
	c.Lock()
	defer c.Unlock()
	if ctx.Err() != nil {
		return false
	}
	return !c.isInvalid
}

func (c *Copier) StartTime() time.Time {
	c.Lock()
	defer c.Unlock()
	return c.startTime
}

func (c *Copier) Run(ctx context.Context) error {
	c.startTime = time.Now()
	defer func() {
		c.ExecTime = time.Since(c.startTime)
	}()
	go c.estimateRowsPerSecondLoop(ctx) // estimate rows while copying
	g, errGrpCtx := errgroup.WithContext(ctx)
	g.SetLimit(c.concurrency)
	for !c.chunker.IsRead() && c.isHealthy(errGrpCtx) {
		g.Go(func() error {
			chunk, err := c.chunker.Next()
			if err != nil {
				if err == table.ErrTableIsRead {
					return nil
				}
				c.setInvalid(true)
				return err
			}
			if err := c.CopyChunk(errGrpCtx, chunk); err != nil {
				c.setInvalid(true)
				return err
			}
			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		return err
	}

	return nil
}

func (c *Copier) setInvalid(newVal bool) {
	c.Lock()
	defer c.Unlock()
	c.isInvalid = newVal
}

func (c *Copier) SetThrottler(throttler throttler.Throttler) {
	c.Lock()
	defer c.Unlock()
	c.Throttler = throttler
}

func (c *Copier) getCopyStats() (uint64, uint64, float64) {
	// Get progress from the chunker instead of calculating it ourselves
	rowsProcessed, _, totalRows := c.chunker.Progress()

	// Calculate percentage
	pct := float64(0)
	if totalRows > 0 {
		pct = float64(rowsProcessed) / float64(totalRows) * 100
	}

	return rowsProcessed, totalRows, pct
}

// GetProgress returns the progress of the copier
func (c *Copier) GetProgress() string {
	c.Lock()
	defer c.Unlock()
	copied, total, pct := c.getCopyStats()
	return fmt.Sprintf("%d/%d %.2f%%", copied, total, pct)
}

func (c *Copier) GetETA() string {
	c.Lock()
	defer c.Unlock()
	copiedRows, totalRows, pct := c.getCopyStats()
	rowsPerSecond := atomic.LoadUint64(&c.rowsPerSecond)
	if pct > 99.99 {
		return "DUE"
	}
	if rowsPerSecond == 0 || time.Since(c.startTime) < copyETAInitialWaitTime {
		return "TBD"
	}
	// divide the remaining rows by how many rows we copied in the last interval per second
	// "remainingRows" might be the actual rows or the logical rows since
	// c.getCopyStats() and rowsPerSecond change estimation method when the PK is auto-inc.
	remainingRows := totalRows - copiedRows
	remainingSeconds := math.Floor(float64(remainingRows) / float64(rowsPerSecond))

	estimate := time.Duration(remainingSeconds * float64(time.Second))
	comparison := c.copierEtaHistory.addCurrentEstimateAndCompare(estimate)
	if comparison != "" {
		return fmt.Sprintf("%s (%s)", estimate.String(), comparison)
	}
	return estimate.String()
}

func (c *Copier) estimateRowsPerSecondLoop(ctx context.Context) {
	// We take >10 second averages because with parallel copy it bounces around a lot.
	// Get progress from chunker since we no longer track rows locally
	prevRowsCount, _, _ := c.chunker.Progress()
	ticker := time.NewTicker(copyEstimateInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !c.isHealthy(ctx) {
				return
			}
			newRowsCount, _, _ := c.chunker.Progress()
			rowsPerInterval := float64(newRowsCount - prevRowsCount)
			intervalsDivisor := float64(copyEstimateInterval / time.Second) // should be something like 10 for 10 seconds
			rowsPerSecond := uint64(rowsPerInterval / intervalsDivisor)
			atomic.StoreUint64(&c.rowsPerSecond, rowsPerSecond)
			prevRowsCount = newRowsCount
		}
	}
}

// The following funcs proxy to the chunker.
// This is done, so we don't need to export the chunker,
// KeyAboveHighWatermark returns true if the key is above where the chunker is currently at.
func (c *Copier) KeyAboveHighWatermark(key any) bool {
	return c.chunker.KeyAboveHighWatermark(key)
}

// GetLowWatermark returns the low watermark of the chunker, i.e. the lowest key that has been
// guaranteed to be written to the new table.
func (c *Copier) GetLowWatermark() (string, error) {
	return c.chunker.GetLowWatermark()
}

func (c *Copier) sendMetrics(ctx context.Context, processingTime time.Duration, logicalRowsCount uint64, affectedRowsCount uint64) error {
	m := &metrics.Metrics{
		Values: []metrics.MetricValue{
			{
				Name:  metrics.ChunkProcessingTimeMetricName,
				Type:  metrics.GAUGE,
				Value: float64(processingTime.Milliseconds()), // in milliseconds
			},
			{
				Name:  metrics.ChunkLogicalRowsCountMetricName,
				Type:  metrics.COUNTER,
				Value: float64(logicalRowsCount),
			},
			{
				Name:  metrics.ChunkAffectedRowsCountMetricName,
				Type:  metrics.COUNTER,
				Value: float64(affectedRowsCount),
			},
		},
	}

	contextWithTimeout, cancel := context.WithTimeout(ctx, metrics.SinkTimeout)
	defer cancel()

	return c.metricsSink.Send(contextWithTimeout, m)
}

// Next4Test is typically only used in integration tests that don't want to actually migrate data,
// but need to advance the chunker.
func (c *Copier) Next4Test() (*table.Chunk, error) {
	return c.chunker.Next()
}

// GetChunker returns the chunker for accessing progress information
func (c *Copier) GetChunker() table.Chunker {
	return c.chunker
}
