package status

import (
	"testing"

	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/require"
)

func TestTablesFromChunker(t *testing.T) {
	require.Empty(t, TablesFromChunker(nil))
	a := table.NewMockChunker("items", 100)
	require.Equal(t, []TableProgress{{TableName: "items", RowsTotal: 100}}, TablesFromChunker(a))
	b := table.NewMockChunker("items", 200)
	a.Tables()[0].Host = "source-a"
	b.Tables()[0].Host = "source-b"
	multi := table.NewMultiChunker(b, a)
	require.Equal(t, []TableProgress{
		{TableName: a.Tables()[0].QualifiedName(), RowsTotal: 100},
		{TableName: b.Tables()[0].QualifiedName(), RowsTotal: 200},
	}, TablesFromChunker(multi))
}

// Sparse keyspace progress must not masquerade as the count of copied rows.
type sparseProgressChunker struct{ *table.MockChunker }

func (s sparseProgressChunker) Progress() (uint64, uint64, uint64) { return 50000, 1, 100000 }
func (s sparseProgressChunker) RowsCopied() uint64                 { return 2 }

func TestTablesFromChunkerReportsActualRows(t *testing.T) {
	sparse := sparseProgressChunker{table.NewMockChunker("sparse", 4)}
	require.Equal(t, []TableProgress{{TableName: "sparse", RowsCopied: 2, RowsTotal: 4}}, TablesFromChunker(sparse))
	other := table.NewMockChunker("other", 10)
	rows := TablesFromChunker(table.NewMultiChunker(sparse, other))
	require.EqualValues(t, 2, rows[1].RowsCopied)
	require.EqualValues(t, 4, rows[1].RowsTotal)
}
