package applier

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
)

const (
	chunkletSize        = 1000 // Number of rows per chunklet
	defaultBufferSize   = 128  // Size of the shared buffer channel for chunklets
	defaultWriteWorkers = 40   // Number of write workers
)

// SingleTargetApplier applies rows to a single target database.
// It internally splits rows into chunklets for optimal batching and tracks
// completion to invoke callbacks when all chunklets for a set of rows are done.
type SingleTargetApplier struct {
	target   Target
	dbConfig *dbconn.DBConfig
	logger   *slog.Logger

	// Internal chunklet processing
	chunkletBuffer      chan chunklet
	chunkletCompletions chan chunkletCompletion

	// Pending work tracking
	pendingWork  map[int64]*pendingWork
	pendingMutex sync.Mutex
	nextWorkID   int64 // Atomic counter for work IDs

	// Worker management
	writeWorkersCount    int32
	writeWorkersFinished int32
	workerIDCounter      int32

	// Context management
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup

	// State management to make Start/Stop idempotent
	started    bool
	stopped    bool
	stateMutex sync.Mutex
}

// rowData represents a single row with all its column values
type rowData struct {
	values []any
}

// chunklet represents a small batch of rows (up to 1000 rows) for internal processing
type chunklet struct {
	workID int64        // ID of the parent work
	chunk  *table.Chunk // Original chunk for column info
	rows   []rowData    // Up to 1000 rows of data
}

// chunkletCompletion represents a completed chunklet
type chunkletCompletion struct {
	workID       int64 // ID of the parent work
	affectedRows int64 // Rows affected by this chunklet
	err          error // Error if any
}

// pendingWork tracks a set of rows that are being processed
type pendingWork struct {
	callback           ApplyCallback
	totalChunklets     int   // Total number of chunklets for this work
	completedChunklets int   // Number of completed chunklets
	totalAffectedRows  int64 // Sum of affected rows from all chunklets
}

// NewSingleTargetApplier creates a new SingleTargetApplier
func NewSingleTargetApplier(target Target, dbConfig *dbconn.DBConfig, logger *slog.Logger) *SingleTargetApplier {
	return &SingleTargetApplier{
		target:              target,
		dbConfig:            dbConfig,
		logger:              logger,
		chunkletBuffer:      make(chan chunklet, defaultBufferSize),
		chunkletCompletions: make(chan chunkletCompletion, defaultBufferSize),
		pendingWork:         make(map[int64]*pendingWork),
		writeWorkersCount:   defaultWriteWorkers,
	}
}

// Start initializes the applier's async write workers and begins processing
// This does not control the synchronous methods like UpsertRows/DeleteKeys
// This method is idempotent - calling it multiple times is safe.
func (a *SingleTargetApplier) Start(ctx context.Context) error {
	a.stateMutex.Lock()
	defer a.stateMutex.Unlock()

	// If already started, return without error
	if a.started {
		a.logger.Debug("SingleTargetApplier already started, skipping")
		return nil
	}

	// If previously stopped, we need to reinitialize channels
	if a.stopped {
		a.logger.Info("restarting SingleTargetApplier after previous stop")
		a.chunkletBuffer = make(chan chunklet, defaultBufferSize)
		a.chunkletCompletions = make(chan chunkletCompletion, defaultBufferSize)
		a.writeWorkersFinished = 0
		a.workerIDCounter = 0
		a.stopped = false
	}

	workerCtx, cancelFunc := context.WithCancel(ctx)
	a.cancelFunc = cancelFunc

	a.logger.Info("starting SingleTargetApplier", "writeWorkers", a.writeWorkersCount)

	// Start write workers
	for range a.writeWorkersCount {
		a.wg.Add(1)
		go a.writeWorker(workerCtx)
	}

	// Start feedback coordinator
	a.wg.Add(1)
	go a.feedbackCoordinator(workerCtx)

	a.started = true
	return nil
}

// Apply sends rows to be written to the target database
func (a *SingleTargetApplier) Apply(ctx context.Context, chunk *table.Chunk, rows [][]any, callback ApplyCallback) error {
	if len(rows) == 0 {
		// No rows to apply, invoke callback immediately
		callback(0, nil)
		return nil
	}

	// Assign a work ID for tracking
	workID := atomic.AddInt64(&a.nextWorkID, 1)

	// Convert rows to rowData format
	rowDataList := make([]rowData, len(rows))
	for i, row := range rows {
		rowDataList[i] = rowData{values: row}
	}

	// Calculate how many chunklets we'll create
	totalChunklets := (len(rows) + chunkletSize - 1) / chunkletSize

	// Register the pending work
	a.pendingMutex.Lock()
	a.pendingWork[workID] = &pendingWork{
		callback:           callback,
		totalChunklets:     totalChunklets,
		completedChunklets: 0,
		totalAffectedRows:  0,
	}
	a.pendingMutex.Unlock()

	// Split into chunklets and send to buffer
	for i := 0; i < len(rowDataList); i += chunkletSize {
		end := min(i+chunkletSize, len(rowDataList))

		chunkletData := chunklet{
			workID: workID,
			chunk:  chunk,
			rows:   rowDataList[i:end],
		}

		select {
		case a.chunkletBuffer <- chunkletData:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// Wait blocks until all pending work is complete and all callbacks have been invoked
func (a *SingleTargetApplier) Wait(ctx context.Context) error {
	// Wait until there's no pending work
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		a.pendingMutex.Lock()
		pendingCount := len(a.pendingWork)
		a.pendingMutex.Unlock()

		if pendingCount == 0 {
			a.logger.Debug("Wait: all pending work complete")
			return nil
		}

		a.logger.Debug("Wait: waiting for pending work", "pendingCount", pendingCount)

		// Wait for next tick or context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Continue loop
		}
	}
}

// Stop signals the applier to shut down gracefully
// This does not control the synchronous methods like UpsertRows/DeleteKeys,
// which can continue after Stop() is called.
// This method is idempotent - calling it multiple times is safe.
func (a *SingleTargetApplier) Stop() error {
	a.stateMutex.Lock()
	defer a.stateMutex.Unlock()

	// If already stopped or never started, return without error
	if a.stopped || !a.started {
		a.logger.Debug("SingleTargetApplier already stopped or never started, skipping")
		return nil
	}

	a.logger.Info("stopping SingleTargetApplier")

	// Cancel the context to signal workers to stop
	if a.cancelFunc != nil {
		a.cancelFunc()
	}

	// Close the chunklet buffer to signal no more work
	close(a.chunkletBuffer)

	// Mark as stopped before waiting to avoid deadlock
	a.stopped = true
	a.started = false

	// Wait for all workers to finish
	a.wg.Wait()

	a.logger.Info("SingleTargetApplier stopped")
	return nil
}

// writeWorker processes chunklets from the buffer
func (a *SingleTargetApplier) writeWorker(ctx context.Context) {
	defer a.wg.Done()
	workerID := atomic.AddInt32(&a.workerIDCounter, 1)

	defer func() {
		finishedCount := atomic.AddInt32(&a.writeWorkersFinished, 1)
		a.logger.Debug("writeWorker finished", "workerID", workerID, "finishedCount", finishedCount, "totalWorkers", a.writeWorkersCount)

		// If all write workers are finished, close the completions channel
		if finishedCount == a.writeWorkersCount {
			a.logger.Debug("writeWorker all write workers finished, closing completions channel", "workerID", workerID)
			close(a.chunkletCompletions)
		}
	}()

	for {
		select {
		case chunkletData, ok := <-a.chunkletBuffer:
			if !ok {
				a.logger.Debug("writeWorker channel closed, exiting", "workerID", workerID)
				return
			}

			a.logger.Debug("writeWorker processing chunklet", "workerID", workerID, "workID", chunkletData.workID, "rowCount", len(chunkletData.rows))

			// Write chunklet
			affectedRows, err := a.writeChunklet(ctx, chunkletData)

			// Send completion
			completion := chunkletCompletion{
				workID:       chunkletData.workID,
				affectedRows: affectedRows,
				err:          err,
			}

			select {
			case a.chunkletCompletions <- completion:
			case <-ctx.Done():
				return
			}

		case <-ctx.Done():
			return
		}
	}
}

// writeChunklet writes a single chunklet (up to 1000 rows)
func (a *SingleTargetApplier) writeChunklet(ctx context.Context, chunkletData chunklet) (int64, error) {
	if len(chunkletData.rows) == 0 {
		return 0, nil
	}

	// Get the intersected column names to match with the values
	columnNames := utils.IntersectNonGeneratedColumnsAsSlice(chunkletData.chunk.Table, chunkletData.chunk.NewTable)
	columnList := utils.IntersectNonGeneratedColumns(chunkletData.chunk.Table, chunkletData.chunk.NewTable)

	// Build VALUES clauses for all rows in the chunklet
	var valuesClauses []string
	for _, row := range chunkletData.rows {
		if len(columnNames) != len(row.values) {
			return 0, fmt.Errorf("column count mismatch: chunk %s has %d columns, but chunklet has %d values",
				chunkletData.chunk.String(), len(columnNames), len(row.values))
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
		chunkletData.chunk.NewTable.TableName,
		columnList,
		strings.Join(valuesClauses, ", "),
	)

	a.logger.Debug("writing chunklet", "rowCount", len(chunkletData.rows), "table", chunkletData.chunk.NewTable.TableName)

	// Execute the batch insert
	result, err := dbconn.RetryableTransaction(ctx, a.target.DB, true, a.dbConfig, query)
	if err != nil {
		return 0, fmt.Errorf("failed to execute chunklet insert: %w", err)
	}

	return result, nil
}

// feedbackCoordinator tracks chunklet completions and invokes callbacks when work is done
func (a *SingleTargetApplier) feedbackCoordinator(ctx context.Context) {
	defer a.wg.Done()
	a.logger.Debug("feedbackCoordinator started")

	for {
		select {
		case completion, ok := <-a.chunkletCompletions:
			if !ok {
				a.logger.Debug("feedbackCoordinator chunklet completions channel closed, exiting")
				return
			}

			a.logger.Debug("feedbackCoordinator received chunklet completion", "workID", completion.workID)

			// Update work completion status
			a.pendingMutex.Lock()
			pending, exists := a.pendingWork[completion.workID]
			if !exists {
				a.pendingMutex.Unlock()
				a.logger.Error("feedbackCoordinator received completion for unknown work", "workID", completion.workID)
				continue
			}

			// If there was an error, invoke callback immediately
			if completion.err != nil {
				callback := pending.callback
				a.pendingMutex.Unlock()
				callback(0, completion.err)
				continue
			}

			// Update completion count and affected rows
			pending.completedChunklets++
			pending.totalAffectedRows += completion.affectedRows

			a.logger.Debug("feedbackCoordinator work progress", "workID", completion.workID,
				"completedChunklets", pending.completedChunklets, "totalChunklets", pending.totalChunklets)

			// Check if all chunklets for this work are complete
			if pending.completedChunklets == pending.totalChunklets {
				a.logger.Debug("feedbackCoordinator all chunklets complete, invoking callback", "workID", completion.workID)

				// Invoke the callback
				callback := pending.callback
				affectedRows := pending.totalAffectedRows

				// Remove completed work from pending map
				delete(a.pendingWork, completion.workID)
				a.pendingMutex.Unlock()

				// Invoke callback outside the lock
				callback(affectedRows, nil)
			} else {
				a.pendingMutex.Unlock()
			}

		case <-ctx.Done():
			return
		}
	}
}

// DeleteKeys deletes rows by their key values synchronously.
// The keys are hashed key strings (from utils.HashKey).
// If lock is non-nil, the delete is executed under the table lock.
func (a *SingleTargetApplier) DeleteKeys(ctx context.Context, sourceTable, targetTable *table.TableInfo, keys []string, lock *dbconn.TableLock) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	// For move operations, targetTable may be nil - use sourceTable for both
	if targetTable == nil {
		targetTable = sourceTable
	}
	// Convert hashed keys to row value constructor format
	var pkValues []string
	for _, key := range keys {
		pkValues = append(pkValues, utils.UnhashKeyToString(key))
	}

	// Build DELETE statement
	deleteStmt := fmt.Sprintf("DELETE FROM %s WHERE (%s) IN (%s)",
		targetTable.TableName,
		table.QuoteColumns(sourceTable.KeyColumns),
		strings.Join(pkValues, ","),
	)

	a.logger.Debug("executing delete", "keyCount", len(keys), "table", targetTable.TableName)

	// Execute under lock if provided
	if lock != nil {
		if err := lock.ExecUnderLock(ctx, deleteStmt); err != nil {
			return 0, fmt.Errorf("failed to execute delete under lock: %w", err)
		}
		// We don't get affected rows from ExecUnderLock, so return the key count
		return int64(len(keys)), nil
	}

	// Execute as a retryable transaction
	affectedRows, err := dbconn.RetryableTransaction(ctx, a.target.DB, false, a.dbConfig, deleteStmt)
	if err != nil {
		return 0, fmt.Errorf("failed to execute delete: %w", err)
	}

	return affectedRows, nil
}

// UpsertRows performs an upsert (INSERT ... ON DUPLICATE KEY UPDATE) synchronously.
// The rows are LogicalRow structs containing the row images.
// If lock is non-nil, the upsert is executed under the table lock.
func (a *SingleTargetApplier) UpsertRows(ctx context.Context, sourceTable, targetTable *table.TableInfo, rows []LogicalRow, lock *dbconn.TableLock) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	// For move operations, targetTable may be nil - use sourceTable for both
	if targetTable == nil {
		targetTable = sourceTable
	}
	// Get the columns that exist in both source and destination tables
	columnList := utils.IntersectNonGeneratedColumns(sourceTable, targetTable)
	columnNames := utils.IntersectNonGeneratedColumnsAsSlice(sourceTable, targetTable)

	// Get the intersected column indices
	var intersectedColumns []int
	for i, sourceCol := range sourceTable.NonGeneratedColumns {
		if slices.Contains(targetTable.NonGeneratedColumns, sourceCol) {
			intersectedColumns = append(intersectedColumns, i)
		}
	}

	// Build the VALUES clause from the row images
	var valuesClauses []string
	for _, logicalRow := range rows {
		if logicalRow.IsDeleted {
			continue // Skip deleted rows
		}

		// Convert the row image to a VALUES clause, but only for intersected columns
		var values []string
		for i, colIndex := range intersectedColumns {
			if colIndex >= len(logicalRow.RowImage) {
				return 0, fmt.Errorf("column index %d exceeds row image length %d", colIndex, len(logicalRow.RowImage))
			}
			value := logicalRow.RowImage[colIndex]
			if value == nil {
				values = append(values, "NULL")
			} else {
				// Get the column type for proper escaping
				if i >= len(columnNames) {
					return 0, fmt.Errorf("column index %d exceeds columnNames length %d", i, len(columnNames))
				}
				columnType, ok := sourceTable.GetColumnMySQLType(columnNames[i])
				if !ok {
					return 0, fmt.Errorf("column %s not found in table info", columnNames[i])
				}
				values = append(values, utils.EscapeMySQLType(columnType, value))
			}
		}
		valuesClauses = append(valuesClauses, fmt.Sprintf("(%s)", strings.Join(values, ", ")))
	}

	if len(valuesClauses) == 0 {
		return 0, nil
	}

	// Build the ON DUPLICATE KEY UPDATE clause using MySQL 8.0+ syntax
	var updateClauses []string
	for _, col := range targetTable.NonGeneratedColumns {
		// Skip primary key columns in the UPDATE clause
		if !slices.Contains(targetTable.KeyColumns, col) {
			// Check if this column exists in both tables
			if slices.Contains(sourceTable.NonGeneratedColumns, col) {
				updateClauses = append(updateClauses, fmt.Sprintf("`%s` = new.`%s`", col, col))
			}
		}
	}

	upsertStmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s AS new ON DUPLICATE KEY UPDATE %s",
		targetTable.TableName,
		columnList,
		strings.Join(valuesClauses, ", "),
		strings.Join(updateClauses, ", "),
	)

	a.logger.Debug("executing upsert", "rowCount", len(valuesClauses), "table", targetTable.TableName)

	// Execute under lock if provided
	if lock != nil {
		if err := lock.ExecUnderLock(ctx, upsertStmt); err != nil {
			return 0, fmt.Errorf("failed to execute upsert under lock: %w", err)
		}
		// We don't get affected rows from ExecUnderLock, so return the row count
		return int64(len(valuesClauses)), nil
	}

	// Execute as a retryable transaction
	affectedRows, err := dbconn.RetryableTransaction(ctx, a.target.DB, false, a.dbConfig, upsertStmt)
	if err != nil {
		return 0, fmt.Errorf("failed to execute upsert: %w", err)
	}

	return affectedRows, nil
}

// GetTargets returns the target database configuration for direct access.
// This is used by operations like checksum that need to query targets directly.
func (a *SingleTargetApplier) GetTargets() []Target {
	return []Target{a.target}
}
