package table

import (
	"time"

	"github.com/siddontang/loggers"
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
)

type Chunker interface {
	Open() error
	IsRead() bool
	Close() error
	Next() (*Chunk, error)
	Feedback(chunk *Chunk, duration time.Duration, actualRows uint64)
	KeyAboveHighWatermark(key any) bool
	Progress() (rowsRead uint64, chunksCopied uint64, totalRowsExpected uint64)
	OpenAtWatermark(watermark string, datum Datum, rowsCopied uint64) error
	GetLowWatermark() (string, error)
	// Tables return a list of table names
	// By convention the first table is the "current" table,
	// and the second table (if any) is the "new" table.
	// There could be more than 2 tables in the case of multi-chunker.
	// In which case every second table is the "new" table, etc.
	Tables() []*TableInfo
}

func newChunker(t *TableInfo, chunkerTarget time.Duration, logger loggers.Advanced) (Chunker, error) {
	return NewChunker(t, nil, chunkerTarget, logger)
}

func NewChunker(t *TableInfo, newTable *TableInfo, chunkerTarget time.Duration, logger loggers.Advanced) (Chunker, error) {
	if chunkerTarget == 0 {
		chunkerTarget = ChunkerDefaultTarget
	}
	// Use the optimistic chunker for auto_increment
	// tables with a single column key.
	if len(t.KeyColumns) == 1 && t.KeyIsAutoInc {
		return &chunkerOptimistic{
			Ti:                     t,
			NewTi:                  newTable,
			ChunkerTarget:          chunkerTarget,
			lowerBoundWatermarkMap: make(map[string]*Chunk, 0),
			logger:                 logger,
		}, nil
	}
	return newCompositeChunkerWithDestination(t, newTable, chunkerTarget, logger, "", "")
}

// NewCompositeChunker returns a chunkerComposite ,
// setting its Key if keyName and where conditions are provided
func NewCompositeChunker(t *TableInfo, chunkerTarget time.Duration, logger loggers.Advanced, keyName string, whereCondition string) (Chunker, error) {
	return newCompositeChunkerWithDestination(t, nil, chunkerTarget, logger, keyName, whereCondition)
}

// NewCompositeChunkerWithDestination returns a chunkerComposite with destination table info,
// setting its Key if keyName and where conditions are provided
func newCompositeChunkerWithDestination(t *TableInfo, newTable *TableInfo, chunkerTarget time.Duration, logger loggers.Advanced, keyName string, whereCondition string) (Chunker, error) {
	c := chunkerComposite{
		Ti:                     t,
		NewTi:                  newTable,
		ChunkerTarget:          chunkerTarget,
		lowerBoundWatermarkMap: make(map[string]*Chunk, 0),
		logger:                 logger,
	}
	var err error
	if keyName != "" && whereCondition != "" {
		err = c.SetKey(keyName, whereCondition)
	}
	return &c, err
}
