package statement

import (
	"fmt"

	"github.com/block/spirit/pkg/table"
)

// ToTableInfo builds a connection-less table.TableInfo from the parsed CREATE
// TABLE, carrying the column types and primary key columns that Spirit's
// checks read from table metadata. schemaName names the schema the table lives
// in; it is only used for error messages and by checks that query MySQL, which
// cannot run against the returned TableInfo anyway (see
// table.NewTableInfoFromMeta).
//
// This lets a caller holding a table's DDL — typically its SHOW CREATE TABLE —
// supply check.Resources.Table without opening a connection.
func (ct *CreateTable) ToTableInfo(schemaName string) (*table.TableInfo, error) {
	columns := make([]table.ColumnMeta, 0, len(ct.Columns))
	for i := range ct.Columns {
		col := &ct.Columns[i]
		columns = append(columns, table.ColumnMeta{
			Name:      col.Name,
			MySQLType: formatColumnTypeAsMetadata(col),
			Generated: col.GeneratedExpr != nil,
		})
	}
	ti, err := table.NewTableInfoFromMeta(schemaName, ct.TableName, columns, ct.primaryKeyColumns())
	if err != nil {
		return nil, fmt.Errorf("build table metadata for %q: %w", ct.TableName, err)
	}
	return ti, nil
}

// primaryKeyColumns returns the table's primary key columns in key order, or
// nil when the table has no primary key. An inline column-level PRIMARY KEY has
// already been materialized into a table-level index by primaryKeyNormalizer,
// so reading the PRIMARY KEY index covers both spellings.
func (ct *CreateTable) primaryKeyColumns() []string {
	pk := ct.getPrimaryKeyIndex()
	if pk == nil {
		return nil
	}
	return pk.Columns
}
