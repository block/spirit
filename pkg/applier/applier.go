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
	"github.com/block/spirit/pkg/metrics"
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
	// wide rows. Still far below the typical 64 MiB max_allowed_packet
	// because the size estimates are rough — estimateValueSize is
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

// EstimateRowSize estimates the size in bytes of a row's values as they will
// be rendered into a VALUES clause. It does not need to be precise: the budget
// it feeds (MaxStatementSizeBytes, 1 MiB) sits ~64x below a typical
// max_allowed_packet, so the estimate only has to be the right order of
// magnitude to keep a statement well clear of the wire limit.
//
// It is exported for callers that batch rows before handing them to Apply and
// so need to bound a batch by the same measure the applier itself uses — the
// checksum's chunk repair does this.
//
// It does need to be cheap. It runs on every value of every copied row, once
// per row on top of the rendering writeChunklet does anyway, so it is pure
// overhead on the hottest client-side path. The previous implementation
// measured len(fmt.Sprintf("%v", value)), which was neither cheap nor
// accurate: a text-protocol Scan into *any hands back []byte for essentially
// every column, and %v renders a []byte as "[49 50 51 …]" — roughly four
// characters per byte. That cost ~2.2us and ~12 allocations per row and
// over-estimated by ~2.7x, so chunklets were being cut well short of the
// budget they were supposed to fill. A type switch is ~290x cheaper, allocates
// nothing, and lands much closer to what datum.String() actually emits.
func EstimateRowSize(values []any) int {
	size := 2 // the tuple's parentheses
	for _, value := range values {
		// +2 for the ", " separator. That over-counts by one separator per
		// row (values are joined, not terminated), which exactly covers the
		// ", " between this row's tuple and the next in the statement. Quote
		// characters are estimateValueSize's job, not counted here.
		size += estimateValueSize(value) + 2
	}
	return size
}

// estimateValueSize approximates the rendered length of one value.
//
// Two cases deliberately under-estimate rather than pad: a []byte bound to a
// binary column renders as 0x-hex (two characters per byte), and a string
// containing quotes or backslashes grows by escaping. Both are bounded by the
// 64x headroom above, and padding for them would penalise the common text case
// the way the old %v behaviour did — an over-estimate is not free, it shrinks
// every chunklet.
func estimateValueSize(value any) int {
	switch v := value.(type) {
	case nil:
		return 4 // NULL
	case []byte:
		// +2 for the surrounding quotes. Slightly over for the numeric column
		// types (which the text protocol also delivers as []byte but which
		// render unquoted); telling them apart would need the column type,
		// which this deliberately doesn't take.
		return len(v) + 2
	case string:
		return len(v) + 2 // +2 for the surrounding quotes
	case time.Time:
		return 28 // '2026-07-30 15:12:27.123456' — quoted, unlike numerics
	case float32, float64:
		// Typical rather than worst case: a float64 can render as wide as 24
		// ("-1.7976931348623157e+308") but usually lands around 6-9 ("3.14159",
		// "-2.71828"). Same bias as the two cases above — under-estimating is
		// covered by the headroom, over-estimating shrinks every chunklet on a
		// table of DOUBLE columns. DECIMAL arrives as []byte on the copy path
		// and is measured exactly; this branch is mostly the binlog path.
		return 8
	case bool:
		return 1
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		// Typical, not worst case, on the same bias as above: 10 digits covers
		// an ordinary ID exactly, and a full-width int64 (19-20 characters)
		// under-estimates by about 2x. Counting the digits instead is exact and
		// costs ~22ns per row, but it buys nothing where it matters — the copy
		// path receives integers as []byte from the text protocol and measures
		// them exactly in the branch above, so this case only fires on the
		// binlog path, where batches are a handful of rows.
		return 10
	default:
		// Not produced by either the SQL driver or the binlog reader today.
		// Cheap and slightly generous rather than reflective.
		return 32
	}
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
		rowSize := EstimateRowSize(row.values)
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
