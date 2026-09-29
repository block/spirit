package applier

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
)

const (
	chunkletMaxRows = 1000 // Maximum number of rows per chunklet

	// MaxStatementSizeBytes is the byte budget for the estimated rendered
	// size of a single multi-row DML statement (1 MiB). Both write paths
	// batch against it: the copy path splits chunks into chunklets
	// (splitRowsIntoChunklets) and the binlog-apply path cuts flush
	// batches (pkg/change) so a REPLACE/DELETE can't grow unbounded with
	// wide rows. Still far below the typical 64 MiB max_allowed_packet
	// because the size estimates are rough — utils.EstimateRenderedRowSize is
	// deliberately biased low (hex encoding, string escaping and wide
	// integers all under-measure; see its doc) and leans on that ~64x
	// headroom — and a single row larger than the budget still goes in
	// its own statement.
	//
	// One bound to respect when changing this: chunklets must keep the write
	// pool fed, so chunks-in-flight x chunklets-per-chunk has to stay above the
	// write worker count. At a 16 MiB target chunk, ~22 chunks in flight and a
	// write ceiling of 188 that means >= ~8.5 chunklets per chunk, i.e. the
	// budget can only roughly double before the largest pools start starving.
	// (When chunkletMaxRows binds instead, chunklets-per-chunk is set by the
	// row count and this bound is much looser.)
	MaxStatementSizeBytes = 1024 * 1024

	defaultBufferSize   = 128 // Size of the shared buffer channel for chunklets
	defaultWriteWorkers = 2   // Number of write workers, default low for tests, but in practice we can use 40+
)

// chunkTaskTimeout bounds one copy-path write (a chunklet INSERT), retries
// included. A var only so tests can shorten it.
//
// It deliberately does not bound DeleteKeys or UpsertRows. Those run the
// change feed's flushes, and pkg/change relies on no spirit-owned deadline
// ever killing a flush statement: a healthy but slow REPLACE must finish, and
// lock contention must surface as 1205/1213 so the batch is deferred rather
// than failing the drain (see retryContendedBatches). Each flush attempt is
// already bounded by RetryableTransaction: MaxRetries tries, each capped by
// innodb_lock_wait_timeout.
var chunkTaskTimeout = time.Second * 60

// Target represents a shard target with its database connection, configuration, and key range.
// Key ranges are expressed as Vitess-style strings (e.g., "-80", "80-", "80-c0").
// An empty string, "0" or "-" means all key space (unsharded).
type Target struct {
	DB       *sql.DB
	Config   *mysql.Config
	KeyRange string // Vitess-style key range: "-80", "80-", "80-c0"; "", "0" or "-" for unsharded
}

// ApplyCallback is invoked when rows have been safely flushed to the target(s).
// affectedRows is the total number of rows affected across all targets.
// err is non-nil if there was an error applying the rows.
type ApplyCallback func(affectedRows int64, err error)

// Applier is an interface for applying rows to one or more target databases.
// MySQLApplier is the implementation: it writes to a single target, or fans out
// to multiple targets based on a hash function.
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
	// occupancy, pending work, live workers, mean rows per chunklet, and
	// rolling percentiles of the four per-chunklet phases (queue-wait, build,
	// write, handoff). Safe to call concurrently with Apply; values are
	// approximate. See the Stats type for field semantics.
	Stats() Stats

	// DeleteKeys deletes rows by their key values synchronously. Each entry
	// in keys is one key tuple of the original (typed) column values, in
	// sourceTable.KeyColumns order.
	// An empty locks slice means no under-lock flush: the delete runs on the
	// regular write connection(s). When locks is non-empty, the delete is
	// executed under the supplied table lock(s): one lock per target, each
	// acquired on that target's own connection (the same *sql.DB as
	// Target.DB). Each target's statements run under that target's lock,
	// matched by connection identity. A missing lock for any target, or a lock
	// that matches no target, is an error.
	// Returns the number of rows affected and any error.
	DeleteKeys(ctx context.Context, sourceTable, targetTable *table.TableInfo, keys [][]any, locks []*dbconn.TableLock) (int64, error)

	// UpsertRows performs an upsert (REPLACE INTO ... VALUES) synchronously.
	// The rows are LogicalRow structs containing the row images.
	// An empty locks slice means no under-lock flush; when locks is non-empty
	// the upsert is executed under the supplied table lock(s). See DeleteKeys
	// for the lock contract (one lock per target).
	// Returns the number of rows affected and any error.
	UpsertRows(ctx context.Context, mapping *table.ColumnMapping, rows []LogicalRow, locks []*dbconn.TableLock) (int64, error)

	// Wait blocks until all pending work is complete and all callbacks have been invoked
	Wait(ctx context.Context) error

	// Stops the applier workers
	Stop() error

	// GetTargets returns target information for direct database access.
	// This is used by operations like checksum that need to query targets directly.
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
// utf8mb4 warning (block/spirit#948). String keys in a charset other than
// utf8mb4 are emitted with their charset introducer, so a latin1 key
// matches its own row rather than the one its bytes spell in utf8mb4. Single-column keys render as a bare
// literal, composite keys as a parenthesized tuple.
func deleteKeysInClause(sourceTable *table.TableInfo, keys [][]any) (string, error) {
	// Resolve each key column's type once, not per key: parsing the type
	// string is the dominant cost of building a Datum. The keys come from
	// binlog row images, so a string key in a charset other than utf8mb4
	// carries the column's own bytes (see TableInfo.BinlogColumnType).
	colTypes := make([]table.ColumnType, len(sourceTable.KeyColumns))
	for j, colName := range sourceTable.KeyColumns {
		ct, err := sourceTable.BinlogColumnType(colName)
		if err != nil {
			return "", fmt.Errorf("key column %s: %w", colName, err)
		}
		colTypes[j] = ct
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
	// MetricsSink, when non-nil, makes the applier periodically report its
	// Stats() snapshot as gauges (see pkg/metrics applier_* names). Nil
	// disables emission entirely — no goroutine is started.
	MetricsSink metrics.Sink
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
		rowSize := utils.EstimateRenderedRowSize(row.values)
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
