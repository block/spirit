package check

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/block/spirit/pkg/parser/mysql"
)

func init() {
	registerCheck("primarykeyfloat", primaryKeyFloatCheck, ScopeStatement|ScopePostSetup)
}

// primaryKeyFloatCheck refuses every ALTER against a table whose primary key
// has a FLOAT column, and any ALTER that leaves a FLOAT in the primary key.
// gh-ost refuses both for the same reason: see
// table.TableInfo.FloatPrimaryKeyError.
//
// At statement scope it reads the table's current definition and the
// statement: a FLOAT already in the key, or a MODIFY or CHANGE of a key column
// to a FLOAT. The migration runner runs the statement-scope checks before it
// attempts MySQL's native DDL, so the refusal holds for every ALTER shape on
// every server, including ones the native DDL could complete. The verdict
// needs the table's current definition; a statement-scope caller that does not
// supply Resources.Table gets no verdict rather than a guess.
//
// At post-setup it also reads the new table, which MySQL has already altered,
// so a FLOAT that reaches the key by a route the statement does not spell out
// as a MODIFY or CHANGE of a key column is refused before any row is copied.
// Changing the key's columns or types always requires a table copy, which the
// native DDL attempt (INSTANT, then a safe INPLACE subset) never performs, so
// the native attempt cannot complete such a statement ahead of this scope.
func primaryKeyFloatCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	// Post-setup runs inside a migration, which loads both tables first. A
	// missing one there means the check would not run at all, so fail
	// rather than pass quietly.
	if r.scope&ScopePostSetup != 0 && (r.Table == nil || r.NewTable == nil) {
		return errors.New("check primarykeyfloat cannot run: the table and the new table were not loaded")
	}
	if r.Table == nil {
		logger.Debug("skipping check: no table metadata supplied, cannot tell the types of the primary key columns",
			"check", "primarykeyfloat")
		return nil
	}
	if err := r.Table.FloatPrimaryKeyError(); err != nil {
		return err
	}
	if r.Statement != nil && r.Statement.StmtNode != nil {
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
	}
	if r.NewTable != nil {
		if err := r.NewTable.FloatPrimaryKeyError(); err != nil {
			return fmt.Errorf("altering table %q so that its primary key includes a FLOAT is not supported: %w", r.Table.TableName, err)
		}
	}
	return nil
}
