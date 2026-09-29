package check

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/block/spirit/pkg/parser/mysql"
)

func init() {
	registerCheck("primarykeyfloat", primaryKeyFloatCheck, ScopeStatement)
}

// primaryKeyFloatCheck refuses every ALTER against a table whose primary key
// has a FLOAT column, and any ALTER that changes a primary key column to a
// FLOAT. gh-ost refuses both for the same reason: see
// table.TableInfo.FloatPrimaryKeyError.
//
// The migration runner refuses the first case when it sets up the table,
// before attempting MySQL's native DDL, so it holds for every ALTER shape on
// every server, including ones the native DDL could complete. It refuses the
// second when it sets up the new table: changing a column's type always
// requires a table copy, which the native DDL attempt (INSTANT, then a safe
// INPLACE subset) never performs. Both verdicts need the table's current
// definition, so the check runs only at statement scope, where the caller may
// supply Resources.Table; without it the check skips rather than guesses.
func primaryKeyFloatCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	if r.Table == nil {
		logger.Debug("skipping check: no table metadata supplied, cannot tell the types of the primary key columns",
			"check", "primarykeyfloat")
		return nil
	}
	if err := r.Table.FloatPrimaryKeyError(); err != nil {
		return err
	}
	if r.Statement == nil || r.Statement.StmtNode == nil {
		return nil
	}
	for _, col := range findModifiedColumns(*r.Statement.StmtNode) {
		if col.ColDef.Tp.GetType() != mysql.TypeFloat {
			continue
		}
		for _, keyCol := range r.Table.KeyColumns {
			if strings.EqualFold(keyCol, col.LookupName) {
				return fmt.Errorf("changing primary key column %q of table %q to a FLOAT is not supported: "+
					"a FLOAT does not compare equal to its text form, so rows cannot be located by key", keyCol, r.Table.TableName)
			}
		}
	}
	return nil
}
