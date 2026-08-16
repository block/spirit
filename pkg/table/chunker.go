package table

import (
	"log/slog"
	"time"
)

const (
	// StartingChunkSize is the initial chunkSize
	StartingChunkSize = 1000
	// MaxDynamicStepFactor is the maximum amount each recalculation of the dynamic chunkSize can
	// increase by. For example, if the newTarget is 5000 but the current target is 1000, the newTarget
	// will be capped back down to 1500. Over time the number 5000 will be reached, but not straight away.
	MaxDynamicStepFactor = 1.5
	// MinDynamicRowSize is the minimum chunkSize that can be used when dynamic chunkSize is enabled.
	// This helps prevent a scenario where the chunk size is too small (it can never be less than 1).
	MinDynamicRowSize = 10
	// MaxDynamicRowSize is the max allowed chunkSize that can be used when dynamic chunkSize is enabled.
	// This seems like a safe upper bound for now
	MaxDynamicRowSize = 100000
	// DynamicPanicFactor is the factor by which the feedback process takes immediate action when
	// the chunkSize appears to be too large. For example, if the PanicFactor is 5, and the target *time*
	// is 50ms, an actual time 250ms+ will cause the dynamic chunk size to immediately be reduced.
	DynamicPanicFactor = 5

	// ChunkerDefaultTarget is the default chunker target
	ChunkerDefaultTarget = 100 * time.Millisecond

	// DefaultTargetChunkBytes is the in-memory byte budget the buffered copier
	// sizes each chunk against (see dynamicChunkSizer.TargetChunkBytes). Because
	// it is a byte budget, it maps almost directly to pages read per chunk
	// (bytes / innodb_page_size) regardless of row width — the natural unit for
	// read-ahead. At 16 MiB a chunk is ~1024 16KB pages (~16 InnoDB extents),
	// which sits solidly inside InnoDB linear read-ahead (the
	// innodb_read_ahead_threshold, ~56 of 64 sequential pages per extent) and
	// keeps Aurora's batched logical prefetch continuously engaged across the
	// scan; a smaller budget barely warms the prefetcher before the chunk ends.
	//
	// Memory is not the binding constraint on this value. The hard per-read
	// ceiling is MaxDynamicRowSize (100k rows), and the dominant steady-state
	// term is the applier's 128-deep chunklet buffer (~128 MiB) — both
	// independent of this budget. Raising it costs only ~concurrency × delta on
	// the read side. Unlike the time budget, it is load-independent, so it does
	// not collapse under write-side backpressure. Tunable; a real-workload sweep
	// may revise it.
	DefaultTargetChunkBytes = 16 * 1024 * 1024
)

type Chunker interface {
	Open() error
	IsRead() bool
	Close() error
	Next() (*Chunk, error)
	Feedback(chunk *Chunk, duration time.Duration, actualRows uint64)
	Progress() (rowsRead uint64, chunksCopied uint64, totalRowsExpected uint64)
	OpenAtWatermark(watermark string) error
	GetLowWatermark() (watermark string, err error)
	// Reset resets the chunker to start from the beginning, as if Open() was just called.
	// This is used when retrying operations like checksums.
	Reset() error
	// Tables return a list of table names
	// By convention the first table is the "current" table,
	// and the second table (if any) is the "new" table.
	// There could be more than 2 tables in the case of multi-chunker.
	// In which case every second table is the "new" table, etc.
	Tables() []*TableInfo
}

// MappedChunker is a Chunker that operates on a single source→target table pair
// and carries a ColumnMapping describing the column relationship between them.
// The multiChunker does not implement this interface because it wraps multiple
// independent table pairs, each with their own mapping.
type MappedChunker interface {
	Chunker
	// ColumnMapping returns the column mapping between source and target tables,
	// including any column renames.
	ColumnMapping() *ColumnMapping
	KeyAboveHighWatermark(key0 any) bool
	KeyBelowLowWatermark(key0 any) bool
}

// ChunkerConfig holds optional configuration for creating a Chunker.
// Only the source table (passed as the first argument to NewChunker) is required;
// all other fields have sensible defaults.
type ChunkerConfig struct {
	// NewTable is the destination table. If nil, defaults to the source table
	// (for move operations where there is no distinct new table).
	NewTable *TableInfo
	// TargetChunkTime is the target duration for each chunk. Defaults to ChunkerDefaultTarget.
	TargetChunkTime time.Duration
	// TargetChunkBytes, when non-zero, switches the dynamic chunker from the
	// wall-clock signal to an in-memory byte-budget signal (the copier
	// default, table.DefaultTargetChunkBytes). Only meaningful for the
	// copier, which reads full rows into memory; the checksum path never
	// sees row bytes and keeps the time signal. See
	// dynamicChunkSizer.TargetChunkBytes.
	TargetChunkBytes uint64
	// Logger is the structured logger. Defaults to slog.Default().
	Logger *slog.Logger
	// ColumnMapping describes the column relationship between source and target tables,
	// including any renames. If nil, a default mapping with no renames is created.
	ColumnMapping *ColumnMapping
	// Key and Where are used for composite chunkers to specify a non-primary key index.
	// When Key is set, the composite chunker is always used regardless of whether the
	// table has an auto-increment primary key.
	Key   string
	Where string
}

// NewChunker creates a new MappedChunker for the given source table.
// It selects the optimistic chunker for single-column auto-increment primary keys
// (unless Key/Where overrides are specified), and the composite chunker otherwise.
func NewChunker(t *TableInfo, config ChunkerConfig) (MappedChunker, error) {
	if config.TargetChunkTime == 0 {
		config.TargetChunkTime = ChunkerDefaultTarget
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	if config.ColumnMapping == nil {
		config.ColumnMapping = NewColumnMapping(t, config.NewTable, nil)
	}
	newTable := config.NewTable
	if newTable == nil {
		// This supports moveTable cases where there is no new table per-se.
		// But the chunker needs a non-nil newTable for some operations,
		// like resuming from a checkpoint.
		newTable = t
	}
	// Use the optimistic chunker for auto_increment tables with a single
	// column key, unless a specific key/where is requested.
	if len(t.KeyColumns) == 1 && t.KeyIsAutoInc && config.Key == "" && config.Where == "" {
		return &chunkerOptimistic{
			Ti:                t,
			NewTi:             newTable,
			columnMapping:     config.ColumnMapping,
			dynamicChunkSizer: dynamicChunkSizer{ChunkerTarget: config.TargetChunkTime, TargetChunkBytes: config.TargetChunkBytes},
			watermarkTracker:  watermarkTracker{lowerBoundWatermarkMap: make(map[string]*Chunk)},
			logger:            config.Logger,
		}, nil
	}
	return &chunkerComposite{
		Ti:                t,
		NewTi:             newTable,
		columnMapping:     config.ColumnMapping,
		keyName:           config.Key,
		where:             config.Where,
		dynamicChunkSizer: dynamicChunkSizer{ChunkerTarget: config.TargetChunkTime},
		watermarkTracker:  watermarkTracker{lowerBoundWatermarkMap: make(map[string]*Chunk)},
		logger:            config.Logger,
	}, nil
}
