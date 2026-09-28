package check

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
)

func init() {
	registerCheck("primarykeycollationstatement", primaryKeyCollationStatementCheck, ScopeStatement)
}

// primaryKeyCollationStatementCheck predicts, from the statement and the
// table's current definition, the refusal primarykeycollation makes once the
// new table is set up: an ALTER that changes the collation of a primary key
// column. It lets a caller learn of the refusal before an apply rather than
// from a run that fails at setup.
//
// primarykeycollation stays the authority. It reads the collation MySQL
// actually resolved on the new table, and it refuses every run this check
// refuses, so the prediction is not registered for a run of its own. This check
// only refuses when the statement determines the key column's collation
// afterwards; when that depends on a default the table metadata does not carry
// (CONVERT TO CHARACTER SET DEFAULT takes the schema's), it stays silent and
// leaves the refusal to setup.
//
// MySQL's native DDL cannot complete the statement ahead of setup: changing a
// primary key column's collation, or changing it between a string and a
// binary or non-string type, rebuilds the table, which MySQL only permits as
// ALGORITHM=COPY. That makes the refusal certain, as ScopeStatement requires.
//
// The reason names the column only as the statement spells it and repeats
// nothing read from the table, so a caller can show it to whoever wrote the
// statement.
func primaryKeyCollationStatementCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	haveTypes, err := requireCurrentColumnTypes(r, logger, "primarykeycollationstatement")
	if err != nil {
		return err
	}
	if !haveTypes {
		return nil
	}
	for _, key := range r.Table.KeyColumns {
		column, ok := tableColumnNamed(r, key)
		if !ok {
			return cannotClassify("unable to validate collation change for primary key column %q: column not found in table metadata", key)
		}
		current, _ := r.Table.GetColumnCollation(column)
		change, determined, err := r.Statement.ColumnCollationChange(column, current, r.Table.DefaultCollation)
		if err != nil {
			return fmt.Errorf("resolve the collation of primary key column %q after the statement: %w", key, err)
		}
		if !determined {
			logger.Debug("skipping primary key collation prediction for column: its collation after the statement depends on a default the table metadata does not carry",
				"table", r.Table.TableName, "column", column)
			continue
		}
		if !change.Changed() {
			continue
		}
		// A CONVERT TO CHARACTER SET re-collates the key without naming it.
		if change.DeclaredAs == "" {
			return errors.New("converting the table's character set changes the collation of its primary key, which is not supported: " +
				primaryKeyCollationUnsupported)
		}
		return fmt.Errorf("changing the collation of primary key column %q is not supported: %s",
			change.DeclaredAs, primaryKeyCollationUnsupported)
	}
	return nil
}

// tableColumnNamed returns the table's own spelling of column. MySQL column
// names are case-insensitive, and a primary key written by hand need not spell
// its columns the way their definitions do.
func tableColumnNamed(r Resources, column string) (string, bool) {
	for _, name := range r.Table.Columns {
		if strings.EqualFold(name, column) {
			return name, true
		}
	}
	return "", false
}
