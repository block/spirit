package status

import (
	"slices"
	"strings"

	"github.com/block/spirit/pkg/table"
)

// TablesFromChunker returns a stable, structured snapshot for runner progress.
// Multi-source chunkers preserve their source-qualified table identifiers.
func TablesFromChunker(chunker table.Chunker) []TableProgress {
	var rows []TableProgress
	if multi, ok := chunker.(interface{ PerTableProgress() []table.TableProgress }); ok {
		for _, tp := range multi.PerTableProgress() {
			rows = append(rows, TableProgress{TableName: tp.TableName, RowsCopied: tp.RowsCopied, RowsTotal: tp.RowsTotal, IsComplete: tp.IsComplete})
		}
	} else if chunker != nil {
		copied, _, total := chunker.Progress()
		name := ""
		if tables := chunker.Tables(); len(tables) > 0 {
			name = tables[0].TableName
		}
		rows = append(rows, TableProgress{TableName: name, RowsCopied: copied, RowsTotal: total, IsComplete: chunker.IsRead()})
	}
	slices.SortFunc(rows, func(a, b TableProgress) int { return strings.Compare(a.TableName, b.TableName) })
	return rows
}
