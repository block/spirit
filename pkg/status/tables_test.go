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
