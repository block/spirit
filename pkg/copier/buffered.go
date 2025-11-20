package copier

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/throttler"
	"github.com/block/spirit/pkg/utils"
	"golang.org/x/sync/errgroup"
)

const (
	chunkletSize      = 1000 // Number of rows per chunklet
	defaultBufferSize = 128  // Size of the shared buffer channel for chunklets
	writeWorkers      = 40   // Number of write workers
)

// The buffered copier implements a producer/consumer pattern
// where multiple reader goroutines read chunks from the source table,
// break them into chunklets of 1000 rows, and send them to a shared buffer channel.
// It closely matches the DBLog algorithm:
// https://netflixtechblog.com/dblog-a-generic-change-data-capture-framework-69351fb9099b
//
// The main difference being that we currently only support a High watermark,
// and have not yet implemented support for Low watermark. This means that it can
// technically cause consistency issues that will be caught by the checksum.
// We will fix this before the feature is considered stable.

type buffered struct {
	sync.Mutex
	db               *sql.DB
	writeDB          *sql.DB // for move command
	chunker          table.Chunker
	concurrency      int
	rowsPerSecond    uint64
	isInvalid        bool
	startTime        time.Time
	throttler        throttler.Throttler
	dbConfig         *dbconn.DBConfig
	logger           *slog.Logger
	metricsSink      metrics.Sink
	copierEtaHistory *copierEtaHistory

	// Aurora algorithm specific fields - Producer/Consumer pattern
	sharedBuffer    chan chunklet // Channel for buffering chunklets between read and write pools
	bufferSize      int           // Size of the shared buffer (1024 for chunklets)
	readersFinished int32         // Atomic counter for finished readers
	workerIDCounter int32         // Atomic counter for assigning worker IDs

	// Chunklet-based processing
	chunkletCompletions  chan chunkletCompletion // Channel for completed chunklets
	pendingChunks        map[int64]*pendingChunk // Map of chunks being processed
	pendingMutex         sync.Mutex              // Protects pendingChunks map
	nextChunkID          int64                   // Atomic counter for chunk ordering
	writeWorkersCount    int32                   // Number of write workers
	writeWorkersFinished int32                   // Atomic counter for finished write workers
}

// rowData represents a single row with all its column values
type rowData struct {
	values []any
}

// chunklet represents a small piece of a chunk (up to 1000 rows)
type chunklet struct {
	chunkID  int64         // ID of the parent chunk
	chunk    *table.Chunk  // Original chunk for column info
	rows     []rowData     // Up to 1000 rows of data
	readTime time.Duration // How long the read operation took (for the entire chunk)
}

// chunkletCompletion represents a completed chunklet
type chunkletCompletion struct {
	chunkID      int64 // ID of the parent chunk
	affectedRows int64 // Rows affected by this chunklet
	err          error // Error if any
}

// pendingChunk tracks chunks that are being processed via chunklets
type pendingChunk struct {
	chunk              *table.Chunk
	readTime           time.Duration
	totalChunklets     int   // Total number of chunklets for this chunk
	completedChunklets int   // Number of completed chunklets
	totalAffectedRows  int64 // Sum of affected rows from all chunklets
}

// Assert that buffered implements the Copier interface
var _ Copier = (*buffered)(nil)

// readChunkData reads all rows from a chunk into memory
func (c *buffered) readChunkData(ctx context.Context, chunk *table.Chunk) ([]rowData, error) {
	// Build the SELECT query to read full row data
	columnList := utils.IntersectNonGeneratedColumns(chunk.Table, chunk.NewTable)
	query := fmt.Sprintf("SELECT %s FROM %s FORCE INDEX (PRIMARY) WHERE %s",
		columnList,
		chunk.Table.QuotedName,
		chunk.String(),
	)

	c.logger.Debug("reading chunk data", "chunk", chunk.String(), "query", query)

	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query chunk data: %w", err)
	}
	defer rows.Close()

	// Get column count for scanning
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	var rowDataList []rowData
	for rows.Next() {
		// Create slice to hold the row values
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// Scan the row
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		rowDataList = append(rowDataList, rowData{values: values})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	c.logger.Debug("read rows from chunk", "rowCount", len(rowDataList), "chunk", chunk.String())
	return rowDataList, nil
}

func (c *buffered) isHealthy(ctx context.Context) bool {
	c.Lock()
	defer c.Unlock()
	if ctx.Err() != nil {
		return false
	}
	return !c.isInvalid
}

func (c *buffered) StartTime() time.Time {
	c.Lock()
	defer c.Unlock()
	return c.startTime
}

func (c *buffered) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	c.startTime = time.Now()
	go c.estimateRowsPerSecondLoop(ctx) // estimate rows while copying

	// Initialize chunklet-based pipeline
	c.bufferSize = defaultBufferSize // Large buffer for chunklets
	c.sharedBuffer = make(chan chunklet, c.bufferSize)
	c.chunkletCompletions = make(chan chunkletCompletion, c.bufferSize)
	c.pendingChunks = make(map[int64]*pendingChunk)

	// Start buffer monitoring goroutine
	go c.monitorBuffers(ctx)

	// Start read workers (producers that break chunks into chunklets)
	g, errGrpCtx := errgroup.WithContext(ctx)
	c.logger.Info("starting read workers", "count", c.concurrency)
	for range c.concurrency {
		g.Go(func() error {
			return c.readWorker(errGrpCtx)
		})
	}

	// Start write workers that process chunklets
	c.writeWorkersCount = writeWorkers
	c.logger.Info("starting write workers", "count", writeWorkers)
	for range writeWorkers {
		g.Go(func() error {
			return c.writeWorker(errGrpCtx)
		})
	}

	// Start feedback coordinator that tracks chunklet completions
	g.Go(func() error {
		return c.feedbackCoordinator(errGrpCtx)
	})
	err := g.Wait()

	// The completions channel is now closed by the last writeWorker to finish
	// so we don't need to close it here anymore

	return err
}

// readWorker reads chunks and breaks them into chunklets of 1000 rows
func (c *buffered) readWorker(ctx context.Context) error {
	workerID := atomic.AddInt32(&c.workerIDCounter, 1)

	defer func() {
		finishedCount := atomic.AddInt32(&c.readersFinished, 1)
		c.logger.Debug("readWorker finished", "workerID", workerID, "finishedCount", finishedCount, "totalWorkers", c.concurrency)
		if finishedCount == int32(c.concurrency) {
			c.logger.Debug("readWorker closing shared buffer channel", "workerID", workerID)
			close(c.sharedBuffer)
		}
	}()

	for !c.chunker.IsRead() && c.isHealthy(ctx) {
		c.throttler.BlockWait()

		chunk, err := c.chunker.Next()
		if err != nil {
			if err == table.ErrTableIsRead {
				c.logger.Debug("readWorker table is read, exiting", "workerID", workerID)
				return nil
			}
			c.setInvalid(true)
			return err
		}

		readStart := time.Now()
		rows, err := c.readChunkData(ctx, chunk)
		if err != nil {
			c.setInvalid(true)
			return fmt.Errorf("failed to read chunk data: %w", err)
		}
		readTime := time.Since(readStart)

		chunkID := atomic.AddInt64(&c.nextChunkID, 1)

		// Handle empty chunks immediately - no need to go through chunklet pipeline
		if len(rows) == 0 {
			c.logger.Debug("readWorker chunk is empty, sending immediate feedback",
				"workerID", workerID, "chunkID", chunkID, "chunk", chunk.String())

			// Send feedback immediately for empty chunks
			c.chunker.Feedback(chunk, readTime, 0)

			// Send metrics for empty chunk
			err := c.sendMetrics(ctx, readTime, chunk.ChunkSize, 0)
			if err != nil {
				c.logger.Error("error sending metrics for empty chunk", "error", err)
			}

			continue // Skip to next chunk
		}

		// Break chunk into chunklets of 1000 rows each
		totalChunklets := (len(rows) + chunkletSize - 1) / chunkletSize // Ceiling division

		if chunkID%20 == 0 {
			c.logger.Debug("readWorker breaking chunk into chunklets",
				"workerID", workerID, "chunkID", chunkID, "chunk", chunk.String(), "rowCount", len(rows), "totalChunklets", totalChunklets)
		}
		// Register the chunk with its expected chunklet count
		c.pendingMutex.Lock()
		c.pendingChunks[chunkID] = &pendingChunk{
			chunk:              chunk,
			readTime:           readTime,
			totalChunklets:     totalChunklets,
			completedChunklets: 0,
			totalAffectedRows:  0,
		}
		c.pendingMutex.Unlock()

		// Send chunklets to shared buffer
		for i := 0; i < len(rows); i += chunkletSize {
			end := i + chunkletSize
			if end > len(rows) {
				end = len(rows)
			}

			chunkletRows := rows[i:end]
			chunkletData := chunklet{
				chunkID:  chunkID,
				chunk:    chunk,
				rows:     chunkletRows,
				readTime: readTime,
			}

			c.logger.Debug("readWorker sending chunklet",
				"workerID", workerID, "chunkletNum", (i/chunkletSize)+1, "totalChunklets", totalChunklets, "chunkID", chunkID, "rowCount", len(chunkletRows))

			select {
			case c.sharedBuffer <- chunkletData:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	c.logger.Info("readWorker exiting main loop", "workerID", workerID)
	return nil
}

// writeWorker processes chunklets from the shared buffer
func (c *buffered) writeWorker(ctx context.Context) error {
	workerID := atomic.AddInt32(&c.workerIDCounter, 1)

	defer func() {
		finishedCount := atomic.AddInt32(&c.writeWorkersFinished, 1)
		c.logger.Debug("writeWorker finished", "workerID", workerID, "finishedCount", finishedCount, "totalWorkers", c.writeWorkersCount)

		// If all write workers are finished, close the completions channel
		// This signals the feedbackCoordinator that no more completions will come
		if finishedCount == c.writeWorkersCount {
			c.logger.Debug("writeWorker all write workers finished, closing completions channel", "workerID", workerID)
			close(c.chunkletCompletions)
		}
	}()

	for {
		select {
		case chunkletData, ok := <-c.sharedBuffer:
			if !ok {
				c.logger.Debug("writeWorker channel closed, exiting", "workerID", workerID)
				return nil
			}

			c.logger.Debug("writeWorker processing chunklet", "workerID", workerID, "chunkID", chunkletData.chunkID, "rowCount", len(chunkletData.rows))

			// Write chunklet directly (no need for writeBufferedRows since it's already a small batch)
			affectedRows, err := c.writeChunklet(ctx, chunkletData)

			// Send completion to feedback coordinator
			completion := chunkletCompletion{
				chunkID:      chunkletData.chunkID,
				affectedRows: affectedRows,
				err:          err,
			}

			c.logger.Debug("writeWorker attempting to send completion for chunklet", "workerID", workerID, "chunkID", chunkletData.chunkID, "affectedRows", affectedRows)

			select {
			case c.chunkletCompletions <- completion:
				c.logger.Debug("writeWorker successfully sent completion for chunklet", "workerID", workerID, "chunkID", chunkletData.chunkID)
			case <-ctx.Done():
				c.logger.Warn("writeWorker context cancelled while sending completion", "workerID", workerID, "chunkID", chunkletData.chunkID)
				return ctx.Err()
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// writeChunklet writes a single chunklet (up to 1000 rows)
func (c *buffered) writeChunklet(ctx context.Context, chunkletData chunklet) (int64, error) {
	if len(chunkletData.rows) == 0 {
		return 0, nil
	}

	// Get the intersected column names to match with the values
	// We can't cache this unfortunately because the chunk.Table and chunk.NewTable
	// might be different with each chunk (due to multi-chunker).
	columnNames := utils.IntersectNonGeneratedColumnsAsSlice(chunkletData.chunk.Table, chunkletData.chunk.NewTable)
	columnList := utils.IntersectNonGeneratedColumns(chunkletData.chunk.Table, chunkletData.chunk.NewTable)

	// Build VALUES clauses for all rows in the chunklet
	var valuesClauses []string
	for _, row := range chunkletData.rows {
		if len(columnNames) != len(row.values) {
			return 0, fmt.Errorf("column count mismatch: chunk %s has %d columns, but chunklet has %d values",
				chunkletData.chunk.String(), len(columnNames), len(chunkletData.rows[0].values))
		}
		var values []string
		for i, value := range row.values {
			columnType, ok := chunkletData.chunk.NewTable.GetColumnMySQLType(columnNames[i])
			if !ok {
				return 0, fmt.Errorf("column %s not found in table info", columnNames[i])
			}
			values = append(values, utils.EscapeMySQLType(columnType, value))
		}
		valuesClauses = append(valuesClauses, fmt.Sprintf("(%s)", strings.Join(values, ", ")))
	}

	// Build the INSERT statement
	query := fmt.Sprintf("INSERT IGNORE INTO %s (%s) VALUES %s",
		chunkletData.chunk.NewTable.QuotedName,
		columnList,
		strings.Join(valuesClauses, ", "),
	)

	c.logger.Debug("writing chunklet", "rowCount", len(chunkletData.rows), "table", chunkletData.chunk.NewTable.QuotedName)

	// Execute the batch insert
	result, err := dbconn.RetryableTransaction(ctx, c.writeDB, true, c.dbConfig, query)
	if err != nil {
		return 0, fmt.Errorf("failed to execute chunklet insert: %w", err)
	}

	return result, nil
}

// feedbackCoordinator tracks chunklet completions and sends feedback when all chunklets for a chunk are done
func (c *buffered) feedbackCoordinator(ctx context.Context) error {
	c.logger.Debug("feedbackCoordinator started")

	for {
		select {
		case completion, ok := <-c.chunkletCompletions:
			if !ok {
				c.logger.Debug("feedbackCoordinator chunklet completions channel closed, exiting")
				return nil
			}

			if completion.err != nil {
				c.setInvalid(true)
				return fmt.Errorf("chunklet for chunk %d failed: %w", completion.chunkID, completion.err)
			}

			c.logger.Debug("feedbackCoordinator received chunklet completion", "chunkID", completion.chunkID)

			// Update chunk completion status
			c.pendingMutex.Lock()
			pending, exists := c.pendingChunks[completion.chunkID]
			if !exists {
				c.pendingMutex.Unlock()
				c.logger.Error("feedbackCoordinator received completion for unknown chunk", "chunkID", completion.chunkID)
				continue
			}

			// Update completion count and affected rows
			pending.completedChunklets++
			pending.totalAffectedRows += completion.affectedRows

			c.logger.Debug("feedbackCoordinator chunk progress", "chunkID", completion.chunkID, "completedChunklets", pending.completedChunklets, "totalChunklets", pending.totalChunklets)

			// Check if all chunklets for this chunk are complete
			if pending.completedChunklets == pending.totalChunklets {
				c.logger.Debug("feedbackCoordinator all chunklets complete, sending feedback", "chunkID", completion.chunkID)

				// Send feedback for the complete chunk
				c.chunker.Feedback(pending.chunk, pending.readTime, uint64(pending.totalAffectedRows))

				// Send metrics
				err := c.sendMetrics(ctx, pending.readTime, pending.chunk.ChunkSize, uint64(pending.totalAffectedRows))
				if err != nil {
					c.logger.Error("error sending metrics from copier", "error", err)
				}

				c.logger.Debug("feedbackCoordinator completed feedback for chunk", "chunkID", completion.chunkID, "chunk", pending.chunk.String(), "totalAffectedRows", pending.totalAffectedRows)

				// Remove completed chunk from pending map
				delete(c.pendingChunks, completion.chunkID)
			}
			c.pendingMutex.Unlock()

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *buffered) setInvalid(newVal bool) {
	c.Lock()
	defer c.Unlock()
	c.isInvalid = newVal
}

func (c *buffered) SetThrottler(throttler throttler.Throttler) {
	c.Lock()
	defer c.Unlock()
	c.throttler = throttler
}

func (c *buffered) getCopyStats() (uint64, uint64, float64) {
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
func (c *buffered) GetProgress() string {
	c.Lock()
	defer c.Unlock()
	copied, total, pct := c.getCopyStats()
	return fmt.Sprintf("%d/%d %.2f%%", copied, total, pct)
}

func (c *buffered) GetETA() string {
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

func (c *buffered) estimateRowsPerSecondLoop(ctx context.Context) {
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

// monitorBuffers prints buffer status every 10 seconds to help diagnose producer/consumer issues
func (c *buffered) monitorBuffers(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !c.isHealthy(ctx) {
				return
			}

			// Get buffer lengths
			sharedBufferLen := len(c.sharedBuffer)

			// Get pending chunks count and details
			c.pendingMutex.Lock()
			pendingChunksCount := len(c.pendingChunks)

			// Find the oldest and newest chunk IDs
			var oldestChunkID, newestChunkID int64 = -1, -1
			var partiallyCompleted int
			for chunkID, pending := range c.pendingChunks {
				if oldestChunkID == -1 || chunkID < oldestChunkID {
					oldestChunkID = chunkID
				}
				if newestChunkID == -1 || chunkID > newestChunkID {
					newestChunkID = chunkID
				}
				if pending.completedChunklets > 0 && pending.completedChunklets < pending.totalChunklets {
					partiallyCompleted++
				}
			}
			c.pendingMutex.Unlock()

			// Get the current chunk ID that's being processed
			currentChunkID := atomic.LoadInt64(&c.nextChunkID)

			c.logger.Debug("BUFFER MONITOR",
				"sharedBufferLen", sharedBufferLen,
				"sharedBufferSize", c.bufferSize,
				"pendingChunks", pendingChunksCount,
				"oldestChunkID", oldestChunkID,
				"newestChunkID", newestChunkID,
				"currentChunkID", currentChunkID,
				"partiallyCompleted", partiallyCompleted)
		}
	}
}

func (c *buffered) sendMetrics(ctx context.Context, processingTime time.Duration, logicalRowsCount uint64, affectedRowsCount uint64) error {
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

// GetChunker returns the chunker for accessing progress information
func (c *buffered) GetChunker() table.Chunker {
	return c.chunker
}

func (c *buffered) GetThrottler() throttler.Throttler {
	c.Lock()
	defer c.Unlock()
	return c.throttler
}
