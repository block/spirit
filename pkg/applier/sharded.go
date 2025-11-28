package applier

import (
	"context"
	"database/sql"
	"errors"
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

// ShardedApplier applies rows to multiple target databases based on a Vitess-style vindex.
// It extracts a specific column value from each row, applies a hash function to it,
// and routes the row to the appropriate shard based on the hash value and key ranges.
//
// The vindex column and hash function are configured per-table in the TableInfo.VindexColumn
// and TableInfo.VindexFunc fields. This allows different tables to use different sharding keys
// in multi-table migrations.
type ShardedApplier struct {
	shards   []*shardTarget
	targets  []Target // Original target configurations
	dbConfig *dbconn.DBConfig
	logger   *slog.Logger

	// Pending work tracking (shared across all shards)
	pendingWork  map[int64]*pendingWork
	pendingMutex sync.Mutex
	nextWorkID   int64 // Atomic counter for work IDs

	// Context management
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup
	stopped    atomic.Bool // Track if Stop() has been called
}

// shardTarget represents a single shard with its own connection, key range, and workers
type shardTarget struct {
	shardID              int
	writeDB              *sql.DB
	keyRange             keyRange // Parsed key range for this shard
	chunkletBuffer       chan shardedChunklet
	chunkletCompletions  chan shardedChunkletCompletion
	writeWorkersCount    int32
	writeWorkersFinished int32
	workerIDCounter      int32
	logger               *slog.Logger
	dbConfig             *dbconn.DBConfig
}

// shardedChunklet represents a chunklet destined for a specific shard
type shardedChunklet struct {
	workID  int64        // ID of the parent work
	shardID int          // Which shard this belongs to
	chunk   *table.Chunk // Original chunk for column info
	rows    []rowData    // Rows for this shard
}

// shardedChunkletCompletion represents a completed sharded chunklet
type shardedChunkletCompletion struct {
	workID       int64 // ID of the parent work
	shardID      int   // Which shard this came from
	affectedRows int64 // Rows affected by this chunklet
	err          error // Error if any
}

// NewShardedApplier creates a new ShardedApplier with multiple target databases.
//
// Parameters:
//   - targets: Slice of Target structs containing DB and key range for each shard
//   - dbConfig: Database configuration
//   - logger: Logger instance
//
// The vindex column and hash function are configured per-table in the TableInfo.VindexColumn
// and TableInfo.VindexFunc fields. This allows different tables to use different sharding keys
// in multi-table migrations.
func NewShardedApplier(targets []Target, dbConfig *dbconn.DBConfig, logger *slog.Logger) (*ShardedApplier, error) {
	shards := make([]*shardTarget, len(targets))
	for i, target := range targets {
		// Parse the key range
		kr, err := parseKeyRange(target.KeyRange)
		if err != nil {
			return nil, fmt.Errorf("failed to parse key range for shard %d: %w", i, err)
		}

		shards[i] = &shardTarget{
			shardID:             i,
			writeDB:             target.DB,
			keyRange:            kr,
			chunkletBuffer:      make(chan shardedChunklet, defaultBufferSize),
			chunkletCompletions: make(chan shardedChunkletCompletion, defaultBufferSize),
			writeWorkersCount:   defaultWriteWorkers / int32(len(targets)), // Divide workers among shards
			logger:              logger,
			dbConfig:            dbConfig,
		}
		// Ensure at least 1 worker per shard
		if shards[i].writeWorkersCount < 1 {
			shards[i].writeWorkersCount = 1
		}
	}

	// Validate that key ranges do not overlap
	for i := range shards {
		for j := i + 1; j < len(shards); j++ {
			if shards[i].keyRange.overlaps(shards[j].keyRange) {
				return nil, fmt.Errorf("key ranges overlap: shard %d (%s: [0x%016x, 0x%016x)) and shard %d (%s: [0x%016x, 0x%016x))",
					i, targets[i].KeyRange, shards[i].keyRange.start, shards[i].keyRange.end,
					j, targets[j].KeyRange, shards[j].keyRange.start, shards[j].keyRange.end)
			}
		}
	}

	// Log the parsed key ranges for debugging
	for i, shard := range shards {
		logger.Info("parsed key range for shard",
			"shardID", i,
			"keyRange", targets[i].KeyRange,
			"start", fmt.Sprintf("0x%016x", shard.keyRange.start),
			"end", fmt.Sprintf("0x%016x", shard.keyRange.end))
	}

	return &ShardedApplier{
		shards:      shards,
		targets:     targets,
		dbConfig:    dbConfig,
		logger:      logger,
		pendingWork: make(map[int64]*pendingWork),
	}, nil
}

// Start initializes all shard workers and begins processing
func (a *ShardedApplier) Start(ctx context.Context) error {
	workerCtx, cancelFunc := context.WithCancel(ctx)
	a.cancelFunc = cancelFunc

	a.logger.Info("starting ShardedApplier", "shardCount", len(a.shards))

	// Start workers for each shard
	for i := range a.shards {
		for range a.shards[i].writeWorkersCount {
			a.wg.Add(1)
			go a.writeWorker(workerCtx, a.shards[i])
		}
	}

	// Start a single feedback coordinator for all shards
	a.wg.Add(1)
	go a.feedbackCoordinator(workerCtx)

	return nil
}

// Apply sends rows to be written to the appropriate target shards.
// Rows are distributed across shards based on the vindex column and hash function
// configured in the chunk's Table.VindexColumn and Table.VindexFunc.
func (a *ShardedApplier) Apply(ctx context.Context, chunk *table.Chunk, rows [][]any, callback ApplyCallback) error {
	if len(rows) == 0 {
		// No rows to apply, invoke callback immediately
		callback(0, nil)
		return nil
	}

	// Extract vindex configuration from the table
	vindexColumn := chunk.Table.VindexColumn
	vindexFunc := chunk.Table.VindexFunc

	if vindexColumn == "" {
		return errors.New("VindexColumn not configured in TableInfo")
	}
	if vindexFunc == nil {
		return errors.New("VindexFunc not configured in TableInfo")
	}

	a.logger.Info("Apply called", "rowCount", len(rows), "vindexColumn", vindexColumn, "table", chunk.Table.TableName)

	// Find the ordinal position of the vindex column
	vindexOrdinal, err := chunk.Table.GetColumnOrdinal(vindexColumn)
	if err != nil {
		return err
	}

	a.logger.Info("Found vindex column", "vindexColumn", vindexColumn, "ordinal", vindexOrdinal)

	// Assign a work ID for tracking
	workID := atomic.AddInt64(&a.nextWorkID, 1)

	// Group rows by shard
	shardRows := make([][]rowData, len(a.shards))
	for _, row := range rows {
		// Extract the vindex column value
		if vindexOrdinal >= len(row) {
			return fmt.Errorf("vindex column ordinal %d exceeds row length %d", vindexOrdinal, len(row))
		}
		vindexValue := row[vindexOrdinal]

		// Apply the hash function to get the hash value
		hashValue, err := vindexFunc(vindexValue)
		if err != nil {
			return fmt.Errorf("vindex function error: %w", err)
		}

		// Find which shard's key range contains this hash value
		shardID := -1
		for i, shard := range a.shards {
			contains := shard.keyRange.contains(hashValue)
			a.logger.Info("checking shard for hash",
				"vindexValue", vindexValue,
				"hashValue", fmt.Sprintf("0x%016x", hashValue),
				"shardID", i,
				"shardStart", fmt.Sprintf("0x%016x", shard.keyRange.start),
				"shardEnd", fmt.Sprintf("0x%016x", shard.keyRange.end),
				"contains", contains)
			if contains {
				shardID = i
				break
			}
		}
		if shardID == -1 {
			return fmt.Errorf("no shard found for hash value %x (vindex column: %s, value: %v)",
				hashValue, vindexColumn, vindexValue)
		}

		// Add to the appropriate shard's row list
		shardRows[shardID] = append(shardRows[shardID], rowData{values: row})
	}

	// Count total chunklets across all shards
	totalChunklets := 0
	for _, rows := range shardRows {
		if len(rows) > 0 {
			totalChunklets += (len(rows) + chunkletSize - 1) / chunkletSize
		}
	}

	// Register the pending work
	a.pendingMutex.Lock()
	a.pendingWork[workID] = &pendingWork{
		callback:           callback,
		totalChunklets:     totalChunklets,
		completedChunklets: 0,
		totalAffectedRows:  0,
	}
	a.pendingMutex.Unlock()

	// Send chunklets to each shard
	for shardID, rows := range shardRows {
		if len(rows) == 0 {
			continue
		}

		// Split into chunklets and send to the shard's buffer
		for i := 0; i < len(rows); i += chunkletSize {
			end := min(i+chunkletSize, len(rows))

			chunkletData := shardedChunklet{
				workID:  workID,
				shardID: shardID,
				chunk:   chunk,
				rows:    rows[i:end],
			}

			select {
			case a.shards[shardID].chunkletBuffer <- chunkletData:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	return nil
}

// Wait blocks until all pending work is complete and all callbacks have been invoked
func (a *ShardedApplier) Wait(ctx context.Context) error {
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

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Continue loop
		}
	}
}

// Stop signals the applier to shut down gracefully
func (a *ShardedApplier) Stop() error {
	// Check if already stopped to prevent double-close panic
	if !a.stopped.CompareAndSwap(false, true) {
		a.logger.Debug("Stop called but applier already stopped, skipping")
		return nil
	}

	a.logger.Debug("Stopping ShardedApplier")

	if a.cancelFunc != nil {
		a.cancelFunc()
	}

	// Close all shard buffers
	for _, shard := range a.shards {
		close(shard.chunkletBuffer)
	}

	a.wg.Wait()
	a.logger.Debug("ShardedApplier stopped")
	return nil
}

// writeWorker processes chunklets for a specific shard
func (a *ShardedApplier) writeWorker(ctx context.Context, shard *shardTarget) {
	defer a.wg.Done()
	workerID := atomic.AddInt32(&shard.workerIDCounter, 1)

	defer func() {
		finishedCount := atomic.AddInt32(&shard.writeWorkersFinished, 1)
		a.logger.Debug("writeWorker finished", "shardID", shard.shardID, "workerID", workerID,
			"finishedCount", finishedCount, "totalWorkers", shard.writeWorkersCount)

		// If all write workers for this shard are finished, close its completions channel
		if finishedCount == shard.writeWorkersCount {
			a.logger.Debug("writeWorker all workers finished for shard, closing completions channel",
				"shardID", shard.shardID)
			close(shard.chunkletCompletions)
		}
	}()

	for {
		select {
		case chunkletData, ok := <-shard.chunkletBuffer:
			if !ok {
				a.logger.Debug("writeWorker channel closed, exiting", "shardID", shard.shardID, "workerID", workerID)
				return
			}

			a.logger.Debug("writeWorker processing chunklet", "shardID", shard.shardID,
				"workerID", workerID, "workID", chunkletData.workID, "rowCount", len(chunkletData.rows))

			// Write chunklet to this shard
			affectedRows, err := a.writeChunklet(ctx, shard, chunkletData)

			// Send completion
			completion := shardedChunkletCompletion{
				workID:       chunkletData.workID,
				shardID:      shard.shardID,
				affectedRows: affectedRows,
				err:          err,
			}

			select {
			case shard.chunkletCompletions <- completion:
			case <-ctx.Done():
				return
			}

		case <-ctx.Done():
			return
		}
	}
}

// writeChunklet writes a single chunklet to a specific shard
func (a *ShardedApplier) writeChunklet(ctx context.Context, shard *shardTarget, chunkletData shardedChunklet) (int64, error) {
	if len(chunkletData.rows) == 0 {
		return 0, nil
	}

	// Get the intersected column names
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
	// Note: We use just the table name, not the fully qualified name, because
	// the database connection (shard.writeDB) already determines which database to write to
	tableName := fmt.Sprintf("`%s`", chunkletData.chunk.NewTable.TableName)
	query := fmt.Sprintf("INSERT IGNORE INTO %s (%s) VALUES %s",
		tableName,
		columnList,
		strings.Join(valuesClauses, ", "),
	)

	a.logger.Debug("writing chunklet to shard", "shardID", shard.shardID,
		"rowCount", len(chunkletData.rows), "table", chunkletData.chunk.NewTable.TableName)

	// Execute the batch insert on this shard's database
	result, err := dbconn.RetryableTransaction(ctx, shard.writeDB, true, shard.dbConfig, query)
	if err != nil {
		return 0, fmt.Errorf("failed to execute chunklet insert on shard %d: %w", shard.shardID, err)
	}

	return result, nil
}

// feedbackCoordinator tracks chunklet completions from all shards and invokes callbacks when work is done
func (a *ShardedApplier) feedbackCoordinator(ctx context.Context) {
	defer a.wg.Done()
	a.logger.Debug("feedbackCoordinator started")

	// Create a merged channel to receive completions from all shards
	mergedCompletions := make(chan shardedChunkletCompletion, defaultBufferSize)

	// Start goroutines to forward completions from each shard to the merged channel
	var forwardWg sync.WaitGroup
	for _, shard := range a.shards {
		forwardWg.Add(1)
		go func(s *shardTarget) {
			defer forwardWg.Done()
			for completion := range s.chunkletCompletions {
				select {
				case mergedCompletions <- completion:
				case <-ctx.Done():
					return
				}
			}
		}(shard)
	}

	// Close merged channel when all shard channels are closed
	go func() {
		forwardWg.Wait()
		close(mergedCompletions)
	}()

	// Process completions
	for {
		select {
		case completion, ok := <-mergedCompletions:
			if !ok {
				a.logger.Debug("feedbackCoordinator merged completions channel closed, exiting")
				return
			}

			a.logger.Debug("feedbackCoordinator received chunklet completion",
				"workID", completion.workID, "shardID", completion.shardID)

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

// DeleteKeys deletes rows by their key values synchronously, broadcasting to all shards.
// The keys are hashed key strings (from utils.HashKey).
// If lock is non-nil, operations are executed under table locks (one per shard).
//
// Note: we only track modifications by PRIMARY KEY, not by shard key (aka primary vindex).
// For this reason we can't extract the vindex value, and must instead broadcast
// the deletes to all shards. The vindex value is considered immutable, and we will
// error if it changes on an update.
func (a *ShardedApplier) DeleteKeys(ctx context.Context, sourceTable, targetTable *table.TableInfo, keys []string, lock *dbconn.TableLock) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}

	// In move operations, targetTable may be nil because there isn't a single target table.
	// In this case, use sourceTable since the schema is the same.
	if targetTable == nil {
		targetTable = sourceTable
	}

	// Convert hashed keys to row value constructor format
	var pkValues []string
	for _, key := range keys {
		pkValues = append(pkValues, utils.UnhashKeyToString(key))
	}

	// Build DELETE statement
	// Use just the table name, not the fully qualified name, because
	// the database connection (shard.writeDB) already determines which database to write to
	tableName := fmt.Sprintf("`%s`", targetTable.TableName)
	deleteStmt := fmt.Sprintf("DELETE FROM %s WHERE (%s) IN (%s)",
		tableName,
		table.QuoteColumns(sourceTable.KeyColumns),
		strings.Join(pkValues, ","),
	)

	// Execute deletes on all shards in parallel (broadcast)
	var totalAffected int64
	var mu sync.Mutex
	var errGroup error

	var wg sync.WaitGroup
	for _, shard := range a.shards {
		wg.Add(1)
		go func(shard *shardTarget) {
			defer wg.Done()

			var affected int64
			var err error
			// Execute under lock if provided
			if lock != nil {
				if err = lock.ExecUnderLock(ctx, deleteStmt); err != nil {
					err = fmt.Errorf("failed to execute delete under lock on shard %d: %w", shard.shardID, err)
				} else {
					// We can't know the actual affected rows when using lock, so estimate
					affected = 0
				}
			} else {
				// Execute as a retryable transaction
				affected, err = dbconn.RetryableTransaction(ctx, shard.writeDB, false, shard.dbConfig, deleteStmt)
				if err != nil {
					err = fmt.Errorf("failed to execute delete on shard %d: %w", shard.shardID, err)
				}
			}

			mu.Lock()
			if err != nil && errGroup == nil {
				errGroup = err
			}
			totalAffected += affected
			mu.Unlock()
		}(shard)
	}
	wg.Wait()
	if errGroup != nil {
		return 0, errGroup
	}
	return totalAffected, nil
}

// UpsertRows performs upserts synchronously, distributing across shards.
// The rows are LogicalRow structs containing the row images.
// If lock is non-nil, operations are executed under table locks (one per shard).
//
// Note: we only track modifications by PRIMARY KEY, not be shard key (aka primary vindex).
// For this reason we could get in trouble if there was a PK update that mutated the vindex column.
// This is because we would only see the last operation (modification) and not know to DELETE
// from one of the shards.
//
// The way we address this, is we consider the vindex column immutable. The replication client is told
// that it should error if there are any updates to it, and the entire operation is canceled.
//
// This is likely not too big of a limitation, as Vitess itself recommends that vindex columns be immutable.
// If it turns out to be a problem, we can revisit tracking by other columns later.
func (a *ShardedApplier) UpsertRows(ctx context.Context, sourceTable, targetTable *table.TableInfo, rows []LogicalRow, lock *dbconn.TableLock) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	// In move operations, targetTable may be nil because there isn't a single target table.
	// In this case, use sourceTable since the schema is the same.
	if targetTable == nil {
		targetTable = sourceTable
	}

	if sourceTable.VindexColumn == "" {
		return 0, errors.New("VindexColumn not configured in TableInfo")
	}
	if sourceTable.VindexFunc == nil {
		return 0, errors.New("VindexFunc not configured in TableInfo")
	}

	// Find the ordinal position of the vindex column within NonGeneratedColumns
	// (since RowImage is structured according to NonGeneratedColumns)
	vindexOrdinal := -1
	for i, col := range sourceTable.NonGeneratedColumns {
		if col == sourceTable.VindexColumn {
			vindexOrdinal = i
			break
		}
	}
	if vindexOrdinal == -1 {
		return 0, fmt.Errorf("vindex column %s not found in non-generated columns", sourceTable.VindexColumn)
	}

	// Get the intersected column indices
	var intersectedColumns []int
	for i, sourceCol := range sourceTable.NonGeneratedColumns {
		if slices.Contains(targetTable.NonGeneratedColumns, sourceCol) {
			intersectedColumns = append(intersectedColumns, i)
		}
	}

	// Group rows by shard
	shardRows := make([][]LogicalRow, len(a.shards))
	for _, row := range rows {
		if row.IsDeleted {
			continue // Skip deleted rows
		}

		// Extract the vindex column value from the row image
		if vindexOrdinal >= len(row.RowImage) {
			return 0, fmt.Errorf("vindex column ordinal %d exceeds row image length %d", vindexOrdinal, len(row.RowImage))
		}
		vindexValue := row.RowImage[vindexOrdinal]

		// Apply the hash function to get the hash value
		hashValue, err := sourceTable.VindexFunc(vindexValue)
		if err != nil {
			return 0, fmt.Errorf("vindex function error: %w", err)
		}

		// Find which shard's key range contains this hash value
		shardID := -1
		for i, shard := range a.shards {
			if shard.keyRange.contains(hashValue) {
				shardID = i
				break
			}
		}
		if shardID == -1 {
			return 0, fmt.Errorf("no shard found for hash value %x (vindex column: %s, value: %v)",
				hashValue, sourceTable.VindexColumn, vindexValue)
		}
		shardRows[shardID] = append(shardRows[shardID], row)
	}

	// Execute upserts on each shard in parallel
	var totalAffected int64
	var mu sync.Mutex
	var errGroup error

	var wg sync.WaitGroup
	for shardID, rows := range shardRows {
		if len(rows) == 0 {
			continue
		}

		wg.Add(1)
		go func(sid int, r []LogicalRow) {
			defer wg.Done()

			// Build the upsert statement for this shard
			columnList := utils.IntersectNonGeneratedColumns(sourceTable, targetTable)
			columnNames := utils.IntersectNonGeneratedColumnsAsSlice(sourceTable, targetTable)

			// Build the VALUES clause
			var valuesClauses []string
			for _, logicalRow := range r {
				var values []string
				for i, colIndex := range intersectedColumns {
					if colIndex >= len(logicalRow.RowImage) {
						mu.Lock()
						if errGroup == nil {
							errGroup = fmt.Errorf("column index %d exceeds row image length %d", colIndex, len(logicalRow.RowImage))
						}
						mu.Unlock()
						return
					}
					value := logicalRow.RowImage[colIndex]
					if value == nil {
						values = append(values, "NULL")
					} else {
						if i >= len(columnNames) {
							mu.Lock()
							if errGroup == nil {
								errGroup = fmt.Errorf("column index %d exceeds columnNames length %d", i, len(columnNames))
							}
							mu.Unlock()
							return
						}
						columnType, ok := sourceTable.GetColumnMySQLType(columnNames[i])
						if !ok {
							mu.Lock()
							if errGroup == nil {
								errGroup = fmt.Errorf("column %s not found in table info", columnNames[i])
							}
							mu.Unlock()
							return
						}
						values = append(values, utils.EscapeMySQLType(columnType, value))
					}
				}
				valuesClauses = append(valuesClauses, fmt.Sprintf("(%s)", strings.Join(values, ", ")))
			}

			// Build the ON DUPLICATE KEY UPDATE clause
			var updateClauses []string
			for _, col := range targetTable.NonGeneratedColumns {
				isPrimaryKey := slices.Contains(targetTable.KeyColumns, col)
				if !isPrimaryKey && slices.Contains(sourceTable.NonGeneratedColumns, col) {
					updateClauses = append(updateClauses, fmt.Sprintf("`%s` = new.`%s`", col, col))
				}
			}

			// Use just the table name, not the fully qualified name, because
			// the database connection (shard.writeDB) already determines which database to write to
			tableName := fmt.Sprintf("`%s`", targetTable.TableName)
			upsertStmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s AS new ON DUPLICATE KEY UPDATE %s",
				tableName,
				columnList,
				strings.Join(valuesClauses, ", "),
				strings.Join(updateClauses, ", "),
			)

			a.logger.Debug("executing upsert on shard", "shardID", sid, "rowCount", len(valuesClauses), "table", targetTable.TableName)

			var affected int64
			var err error

			// Execute under lock if provided
			if lock != nil {
				if err = lock.ExecUnderLock(ctx, upsertStmt); err != nil {
					err = fmt.Errorf("failed to execute upsert under lock on shard %d: %w", sid, err)
				} else {
					affected = int64(len(valuesClauses))
				}
			} else {
				// Execute as a retryable transaction
				affected, err = dbconn.RetryableTransaction(ctx, a.shards[sid].writeDB, false, a.shards[sid].dbConfig, upsertStmt)
				if err != nil {
					err = fmt.Errorf("failed to execute upsert on shard %d: %w", sid, err)
				}
			}

			mu.Lock()
			if err != nil && errGroup == nil {
				errGroup = err
			}
			totalAffected += affected
			mu.Unlock()
		}(shardID, rows)
	}

	wg.Wait()

	if errGroup != nil {
		return 0, errGroup
	}

	return totalAffected, nil
}

// GetTargets returns the target database configurations for direct access.
// This is used by operations like checksum that need to query targets directly.
func (a *ShardedApplier) GetTargets() []Target {
	return a.targets
}
