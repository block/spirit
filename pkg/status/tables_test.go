package status

import (
	"fmt"
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

func TestTablesFromChunkerExcludesShadowEstimate(t *testing.T) {
	for _, autoInc := range []bool{false, true} {
		for _, shadow := range []bool{false, true} {
			name := fmt.Sprintf("autoInc=%v/shadow=%v", autoInc, shadow)
			t.Run(name, func(t *testing.T) {
				source := &table.TableInfo{TableName: "items", SchemaName: "test", KeyColumns: []string{"id"}, KeyIsAutoInc: autoInc, EstimatedRows: 100}
				config := table.ChunkerConfig{}
				if shadow {
					config.NewTable = &table.TableInfo{TableName: "_items_new", EstimatedRows: 75}
				}
				chunker, err := table.NewChunker(source, config)
				require.NoError(t, err)
				require.Len(t, chunker.Tables(), 2)
				if !shadow {
					require.Same(t, source, chunker.Tables()[1])
				}
				require.EqualValues(t, 100, TablesFromChunker(chunker)[0].RowsTotal)
				multi := table.NewMultiChunker(chunker, table.NewMockChunker("other", 20))
				rows := TablesFromChunker(multi)
				require.Len(t, rows, 2)
				require.EqualValues(t, 100, rows[0].RowsTotal)
				require.EqualValues(t, 20, rows[1].RowsTotal)
			})
		}
	}
}
