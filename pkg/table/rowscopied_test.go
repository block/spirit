package table

import (
	"database/sql"
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// TestRowsCopiedIsNotProgressForOptimisticChunker pins the reason RowsCopied
// exists as its own accessor. The optimistic chunker measures progress as
// distance travelled through the auto-increment key space, so on a sparse
// table Progress's first return is far larger than the rows actually copied,
// and cannot stand in for a row count.
func TestRowsCopiedIsNotProgressForOptimisticChunker(t *testing.T) {
	t1 := newTableInfo4Test("test", "t1")
	t1.minValue = Datum{Val: int64(1), Tp: signedType}
	t1.maxValue = Datum{Val: int64(1000000), Tp: signedType}
	t1.EstimatedRows = 1000000
	t1.KeyColumns = []string{"id"}
	t1.keyColumnsMySQLTp = []string{"bigint"}
	t1.keyDatums = []datumTp{signedType}
	t1.KeyIsAutoInc = true
	t1.Columns = []string{"id", "name"}
	require.NoError(t, t1.PrimaryKeyIsMemoryComparable())

	chunker := &chunkerOptimistic{
		Ti:                t1,
		dynamicChunkSizer: dynamicChunkSizer{ChunkerTarget: ChunkerDefaultTarget},
		watermarkTracker:  watermarkTracker{lowerBoundWatermarkMap: make(map[string]*Chunk)},
		logger:            slog.Default(),
	}
	chunker.SetDynamicChunking(false)
	require.NoError(t, chunker.Open())

	// Walk a few chunks whose key ranges are mostly empty.
	const copiedPerChunk = 3
	for range 3 {
		chunk, err := chunker.Next()
		require.NoError(t, err)
		chunker.Feedback(chunk, time.Millisecond, copiedPerChunk)
	}

	rowsRead, chunksCopied, _ := chunker.Progress()
	require.Equal(t, uint64(3), chunksCopied)
	require.Equal(t, uint64(3*copiedPerChunk), chunker.RowsCopied(),
		"settled rows are summed from the applier's feedback")
	require.Greater(t, rowsRead, chunker.RowsCopied(),
		"Progress reports key-space distance travelled, not rows")
}

// TestRowsCopiedCompositeChunker: the composite chunker already counts actual
// rows, so RowsCopied and Progress's first return agree there. This is what
// makes RowsCopied a safe single accessor across both chunker types.
func TestRowsCopiedCompositeChunker(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS rowscopied_composite")
	testutils.RunSQL(t, `CREATE TABLE rowscopied_composite (
		a varbinary(40) NOT NULL,
		b int NOT NULL,
		PRIMARY KEY (a)
	)`)
	testutils.RunSQL(t, `INSERT INTO rowscopied_composite (a, b) SELECT UUID(), 1 FROM dual`)

	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	t1 := NewTableInfo(db, "test", "rowscopied_composite")
	require.NoError(t, t1.SetInfo(t.Context()))
	chunker, err := NewChunker(t1, ChunkerConfig{})
	require.NoError(t, err)
	require.IsType(t, &chunkerComposite{}, chunker)
	require.NoError(t, chunker.Open())

	chunk, err := chunker.Next()
	require.NoError(t, err)
	chunker.Feedback(chunk, time.Millisecond, 42)

	rowsRead, _, _ := chunker.Progress()
	require.Equal(t, uint64(42), chunker.RowsCopied())
	require.Equal(t, rowsRead, chunker.RowsCopied())
}
