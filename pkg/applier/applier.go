package applier

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/go-sql-driver/mysql"
)

const (
	chunkletMaxRows = 1000 // Maximum number of rows per chunklet

	// MaxStatementSizeBytes is the byte budget for the estimated rendered
	// size of a single multi-row DML statement (1 MiB). Both write paths
	// batch against it: the copy path splits chunks into chunklets
	// (splitRowsIntoChunklets) and the binlog-apply path cuts flush
	// batches (pkg/change) so a REPLACE/DELETE can't grow unbounded with
	// wide rows. Deliberately far below the typical 64 MiB
	// max_allowed_packet because the size estimates are rough; a single
	// row larger than the budget still goes in its own statement.
	MaxStatementSizeBytes = 1024 * 1024

	defaultBufferSize   = 128              // Size of the shared buffer channel for chunklets
	defaultWriteWorkers = 2                // Number of write workers, default low for tests, but in practice we can use 40+
	chunkTaskTimeout    = time.Second * 60 // Timeout for any task (copy chunk, delete keys, upsert rows)
)

// Target represents a shard target with its database connection, configuration, and key range.
// Key ranges are expressed as Vitess-style strings (e.g., "-80", "80-", "80-c0").
// An empty string or "0" means all key space (unsharded).
type Target struct {
	DB       *sql.DB
	Config   *mysql.Config
	KeyRange string // Vitess-style key range: "-80", "80-", "80-c0", or "0" for unsharded
}

// ApplyCallback is invoked when rows have been safely flushed to the target(s).
// affectedRows is the total number of rows affected across all targets.
// err is non-nil if there was an error applying the rows.
type ApplyCallback func(affectedRows int64, err error)

// Applier is an interface for applying rows to one or more target databases.
// Implementations can apply to a single target (SingleTargetApplier) or fan out to
// multiple targets based on a hash function (ShardedApplier).
//
// The Applier is responsible for:
// - Batching/splitting rows into optimal write sizes
// - Tracking pending writes
// - Invoking callbacks when writes are complete
type Applier interface {
	// Start initializes the applier and starts its workers
	Start(ctx context.Context) error

	// Apply sends rows to be written to the target(s).
	// The chunk parameter provides metadata about the source table and target table.
	// The rows parameter contains the actual row data to be written.
	// The callback is invoked when all rows are safely flushed.
	//
	// For the copier: callback will call chunker.Feedback()
	// For the subscription: callback will update binlog coordinates
	Apply(ctx context.Context, chunk *table.Chunk, rows [][]any, callback ApplyCallback) error

	// Stats returns a point-in-time snapshot of the write pipeline: queue
	// occupancy, pending work, live workers, and rolling queue-wait /
	// write-time percentiles. Safe to call concurrently with Apply; values
	// are approximate. See the Stats type for field semantics.
	Stats() Stats

	// DeleteKeys deletes rows by their key values synchronously. Each entry
	// in keys is one key tuple of the original (typed) column values, in
	// sourceTable.KeyColumns order.
	// An empty locks slice means no under-lock flush: the delete runs on the
	// regular write connection(s). When locks is non-empty, the delete is
	// executed under the supplied table lock(s):
	//   - Single-target implementations accept zero or one lock. Zero means no
	//     under-lock flush; more than one lock is a caller bug and is an error.
	//   - Multi-target (sharded) implementations expect one lock per target,
	//     each acquired on that target's own connection, and execute each
	//     target's statements under that target's lock (matched by connection
	//     identity). A missing lock for any shard is an error.
	// Returns the number of rows affected and any error.
	DeleteKeys(ctx context.Context, sourceTable, targetTable *table.TableInfo, keys [][]any, locks []*dbconn.TableLock) (int64, error)

	// UpsertRows performs an upsert (REPLACE INTO ... VALUES) synchronously.
	// The rows are LogicalRow structs containing the row images.
	// An empty locks slice means no under-lock flush; when locks is non-empty
	// the upsert is executed under the supplied table lock(s). See DeleteKeys
	// for the per-implementation lock contract (single-target accepts zero or
	// one lock; sharded expects one lock per shard).
	// Returns the number of rows affected and any error.
	UpsertRows(ctx context.Context, mapping *table.ColumnMapping, rows []LogicalRow, locks []*dbconn.TableLock) (int64, error)

	// Wait blocks until all pending work is complete and all callbacks have been invoked
	Wait(ctx context.Context) error

	// Stops the applier workers
	Stop() error

	// GetTargets returns target information for direct database access.
	// This is used by operations like checksum that need to query targets directly.
	// For SingleTargetApplier, this returns a single target.
	// For ShardedApplier, this returns all shards.
	GetTargets() []Target
}

// LogicalRow represents the current state of a row in the subscription buffer.
// This could be that it is deleted, or that it has RowImage that describes it.
// If there is a RowImage, then it needs to be converted into the RowImage of the
// newTable.
type LogicalRow struct {
	IsDeleted bool
	RowImage  []any
}

// deleteKeysInClause renders key value tuples into the element list of a
// `(keycols) IN (...)` clause. Values go through table.Datum so binary
// keys are hex-encoded; a quoted non-UTF-8 literal would trip MySQL's
// utf8mb4 warning (block/spirit#948). Single-column keys render as a bare
// literal, composite keys as a parenthesized tuple.
func deleteKeysInClause(sourceTable *table.TableInfo, keys [][]any) (string, error) {
	// Resolve each key column's type once, not per key: parsing the type
	// string is the dominant cost of building a Datum.
	colTypes := make([]table.ColumnType, len(sourceTable.KeyColumns))
	for j, colName := range sourceTable.KeyColumns {
		typeStr, ok := sourceTable.GetColumnMySQLType(colName)
		if !ok {
			return "", fmt.Errorf("key column %s not found in table %s", colName, sourceTable.TableName)
		}
		colTypes[j] = table.NewColumnType(typeStr)
	}

	pkValues := make([]string, 0, len(keys))
	for _, keyTuple := range keys {
		if len(keyTuple) != len(colTypes) {
			return "", fmt.Errorf("delete key has %d component(s) but table %s has %d key column(s)",
				len(keyTuple), sourceTable.TableName, len(colTypes))
		}
		parts := make([]string, len(keyTuple))
		for j := range keyTuple {
			datum, err := table.NewDatumFromValueWithType(keyTuple[j], colTypes[j])
			if err != nil {
				return "", fmt.Errorf("failed to convert delete key value for column %s: %w", sourceTable.KeyColumns[j], err)
			}
			parts[j] = datum.String()
		}
		if len(parts) == 1 {
			pkValues = append(pkValues, parts[0])
		} else {
			pkValues = append(pkValues, "("+strings.Join(parts, ",")+")")
		}
	}
	return strings.Join(pkValues, ","), nil
}

type ApplierConfig struct {
	Threads         int // number of write threads
	ChunkletMaxRows int
	ChunkletMaxSize int
	Logger          *slog.Logger
	DBConfig        *dbconn.DBConfig
}

// NewApplierDefaultConfig returns a default config for the applier.
func NewApplierDefaultConfig() *ApplierConfig {
	return &ApplierConfig{
		Threads:         defaultWriteWorkers,
		ChunkletMaxRows: chunkletMaxRows,       // will be renamed soon.
		ChunkletMaxSize: MaxStatementSizeBytes, // will be supported soon.
		Logger:          slog.Default(),
		DBConfig:        dbconn.NewDBConfig(),
	}
}

// Validate checks the ApplierConfig for required fields.
func (cfg *ApplierConfig) Validate() error {
	if cfg.DBConfig == nil {
		return errors.New("dbConfig must be non-nil")
	}
	if cfg.Logger == nil {
		return errors.New("logger must be non-nil")
	}
	// We can set defaults for other fields.
	// If they are not set its not important.
	if cfg.Threads <= 0 {
		cfg.Threads = defaultWriteWorkers
	}
	if cfg.ChunkletMaxRows <= 0 {
		cfg.ChunkletMaxRows = chunkletMaxRows
	}
	if cfg.ChunkletMaxSize <= 0 {
		cfg.ChunkletMaxSize = MaxStatementSizeBytes
	}
	return nil
}

// estimateRowSize estimates the size in bytes of a row's values.
// This is a simple heuristic based on string representation length.
// Since we use a conservative 1 MiB threshold vs typical 64 MiB max_allowed_packet,
// we don't need precise measurements. This is much better than
// just hoping 1000 rows will fit under max_allowed_packet.
func estimateRowSize(values []any) int {
	size := 2 // minimal overhead for parentheses
	for _, value := range values {
		// Estimate size based on string representation
		// +2 bytes for quotes and 2 bytes for comma/separator
		size += len(fmt.Sprintf("%v", value)) + 4
	}
	return size
}

// rowData represents a single row with all its column values
type rowData struct {
	values []any
}

// splitRowsIntoChunklets splits rows into chunklets based on both row count and size thresholds.
// Returns a slice of row batches where each batch respects both chunkletMaxRows and MaxStatementSizeBytes limits.
//
// Note: A single row can exceed MaxStatementSizeBytes by itself. In this case, the row will be placed
// in its own chunklet regardless of size. This is an edge case where we rely on max_allowed_packet
// being large enough (typically 64 MiB default vs our 1 MiB threshold).
func splitRowsIntoChunklets(rows []rowData) [][]rowData {
	if len(rows) == 0 {
		return nil
	}
	var chunklets [][]rowData
	currentChunklet := make([]rowData, 0, chunkletMaxRows)
	currentSize := 0

	for _, row := range rows {
		rowSize := estimateRowSize(row.values)
		// Check if adding this row would exceed either threshold
		if len(currentChunklet) >= chunkletMaxRows ||
			(len(currentChunklet) > 0 && currentSize+rowSize > MaxStatementSizeBytes) {
			// Save current chunklet and start a new one
			chunklets = append(chunklets, currentChunklet)
			currentChunklet = make([]rowData, 0, chunkletMaxRows)
			currentSize = 0
		}
		// Add row to current chunklet
		currentChunklet = append(currentChunklet, row)
		currentSize += rowSize
	}
	// Don't forget the last chunklet
	if len(currentChunklet) > 0 {
		chunklets = append(chunklets, currentChunklet)
	}
	return chunklets
}
