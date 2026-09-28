package check

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
)

func init() {
	registerCheck("primaryKeyCollation", primaryKeyCollationCheck, ScopePreflight|ScopeStatement)
}

// primaryKeyCollationChangeUnsupported explains why a statement that changes
// the collation of the primary key is refused.
const primaryKeyCollationChangeUnsupported = "the checksum compares both tables over the same primary key ranges, " +
	"which select different rows from each table when the key sorts differently"

// primaryKeyCollationCheck refuses a statement that changes the collation a
// primary key column compares under.
//
// Spirit copies and verifies a table in primary key ranges. The checksum reads
// the source and the new table over the same key bounds, and each table
// evaluates those bounds under its own column collation. When the statement
// changes how the key sorts — utf8mb4_0900_ai_ci to utf8mb4_bin, say, where
// every key starting with an uppercase letter moves ahead of every key
// starting with a lowercase one — the same bounds select different rows on
// each side, the checksum finds differences on every attempt, and the schema
// change can never cut over. A table small enough to fit in one chunk is
// checksummed over an unbounded range and passes, so the failure would only
// show on larger tables. It is refused up front instead, as the rule that the
// schema change cannot alter the primary key already says.
//
// MySQL's native DDL cannot complete the statement ahead of preflight:
// changing the collation of a primary key column rebuilds the table, which
// MySQL only permits as ALGORITHM=COPY. That makes the refusal certain, so the
// check is safe in ScopeStatement.
//
// Changing a key column between a character type and a binary or non-string
// type is a type change rather than a collation change, and is not refused
// here.
func primaryKeyCollationCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	haveTypes, err := requireCurrentColumnTypes(r, logger, "primaryKeyCollation")
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
			logger.Debug("skipping primary key collation check for column: its collation after the statement depends on a default the table metadata does not carry",
				"table", r.Table.TableName, "column", column)
			continue
		}
		if !change.Recollated() {
			continue
		}
		// The reason names the column only as the statement spells it: a
		// CONVERT TO CHARACTER SET re-collates the key without naming it.
		if change.DeclaredAs == "" {
			return errors.New("converting the table's character set changes the collation of its primary key, which is not supported: " +
				primaryKeyCollationChangeUnsupported)
		}
		return fmt.Errorf("changing the collation of primary key column %q is not supported: %s",
			change.DeclaredAs, primaryKeyCollationChangeUnsupported)
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
