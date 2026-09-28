package check

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"github.com/block/spirit/pkg/utils"
)

func init() {
	registerCheck("primarykeycollation", primaryKeyCollationCheck, ScopePostSetup)
}

// primaryKeyCollationCheck refuses an ALTER that changes the collation of a
// primary key column (issue #1282).
//
// Spirit chunks on the primary key, and a chunk is a range of key values.
// Whether a key is greater or less than a chunk bound depends on the collation,
// and the checksum evaluates each range on both the source and the new table.
// If the collation differs, the same range selects different rows on each side,
// so the checksum reports differences that do not exist, and its repair
// (DELETE the range on the new table, re-insert the range from the source)
// deletes rows from the new table that it never re-inserts. A collation change
// that keeps every key unique still changes the ordering, so it is refused too.
//
// The check compares the tables after MySQL has applied the ALTER to the new
// table, because only MySQL resolves the column's final collation: a MODIFY
// without COLLATE takes the table default, and CONVERT TO CHARACTER SET takes
// the character set's default collation. A change between a string and a
// non-string type (VARCHAR to INT, VARCHAR to VARBINARY) adds or removes a
// collation, so it is refused as well.
//
// Any change of collation is refused, including one between two collations
// that happen to order keys the same way (utf8mb3_bin to utf8mb4_bin). Telling
// those apart would copy MySQL's collation rules into Spirit, which is what
// reading the result from MySQL avoids.
func primaryKeyCollationCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	if r.Table == nil || r.NewTable == nil {
		return errors.New("check primarykeycollation cannot run: the table and the new table were not loaded")
	}
	oldKey, err := primaryKeyCollations(ctx, r.DB, r.Table.SchemaName, r.Table.TableName)
	if err != nil {
		return err
	}
	newKey, err := primaryKeyCollations(ctx, r.DB, r.NewTable.SchemaName, r.NewTable.TableName)
	if err != nil {
		return err
	}
	// The primarykey check already refuses DROP PRIMARY KEY, so the key's
	// columns can not change today. This is a defence in case that changes.
	if len(oldKey) != len(newKey) {
		return fmt.Errorf("changing the columns of the primary key of table %q is not supported", r.Table.TableName)
	}
	for i, oldCol := range oldKey {
		newCol := newKey[i]
		if oldCol.collation == newCol.collation {
			continue
		}
		return fmt.Errorf("changing the collation of primary key column %q from %s to %s is not supported: "+
			"spirit chunks on primary key ranges, and the collation decides which rows fall in each range",
			oldCol.name, oldCol.collationString(), newCol.collationString())
	}
	return nil
}

// primaryKeyColumn is a primary key column and its collation, which is NULL for
// non-string types.
type primaryKeyColumn struct {
	name      string
	collation sql.NullString
}

func (c primaryKeyColumn) collationString() string {
	if !c.collation.Valid {
		return "no collation"
	}
	return c.collation.String
}

// primaryKeyCollations returns the primary key columns of a table in key order.
func primaryKeyCollations(ctx context.Context, db *sql.DB, schema, tableName string) ([]primaryKeyColumn, error) {
	rows, err := db.QueryContext(ctx, `SELECT k.COLUMN_NAME, c.COLLATION_NAME
		FROM information_schema.KEY_COLUMN_USAGE k
		JOIN information_schema.COLUMNS c
		  ON c.TABLE_SCHEMA = k.TABLE_SCHEMA AND c.TABLE_NAME = k.TABLE_NAME AND c.COLUMN_NAME = k.COLUMN_NAME
		WHERE k.TABLE_SCHEMA = ? AND k.TABLE_NAME = ? AND k.CONSTRAINT_NAME = 'PRIMARY'
		ORDER BY k.ORDINAL_POSITION`, schema, tableName)
	if err != nil {
		return nil, err
	}
	defer utils.CloseAndLog(rows)
	var cols []primaryKeyColumn
	for rows.Next() {
		var col primaryKeyColumn
		if err := rows.Scan(&col.name, &col.collation); err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	return cols, rows.Err()
}
