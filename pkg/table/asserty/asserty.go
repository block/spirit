// Package asserty offers functionality to assert for certain DB properties.
package asserty

import (
	"context"
	"database/sql"
	"errors"
	"slices"

	"github.com/block/spirit/pkg/table"
	_ "github.com/go-sql-driver/mysql"
)

type Table struct {
	ti *table.TableInfo
}

func LoadTable(db *sql.DB, schema, tableName string) (*Table, error) {
	ti := table.NewTableInfo(db, schema, tableName)
	if err := ti.SetInfo(context.TODO()); err != nil {
		return nil, err
	}
	return &Table{ti: ti}, nil
}

func (t *Table) ContainsColumns(columnNames ...string) error {
	for _, col := range columnNames {
		if !slices.Contains(t.ti.Columns, col) {
			return errors.New("missing column " + col + " on table " + t.ti.QuotedName)
		}
	}
	return nil
}

func (t *Table) NotContainsColumns(columnNames ...string) error {
	for _, col := range columnNames {
		if slices.Contains(t.ti.Columns, col) {
			return errors.New("unexpected column " + col + " on table " + t.ti.QuotedName)
		}
	}
	return nil
}

func (t *Table) ContainsIndexes(indexNames ...string) error {
	for _, idx := range indexNames {
		if !slices.Contains(t.ti.Indexes, idx) {
			return errors.New("missing index " + idx + " on table " + t.ti.QuotedName)
		}
	}
	return nil
}

func (t *Table) NotContainsIndexes(indexNames ...string) error {
	for _, idx := range indexNames {
		if slices.Contains(t.ti.Indexes, idx) {
			return errors.New("unexpected index " + idx + " on table " + t.ti.QuotedName)
		}
	}
	return nil
}
