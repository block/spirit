package check

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
)

// StatementRefusal reports whether Spirit deterministically refuses stmt, and
// the reason it would give.
//
// It runs the ScopeStatement checks: the subset of Spirit's preflight checks
// that decide from the statement — plus, when currentCreateTable is supplied,
// the table's existing column definitions — and that MySQL's native DDL cannot
// complete behind their back. That makes it the entry point for a caller which
// needs to know an apply's outcome before starting it, such as a planning tool
// classifying DDL. Checks Spirit adds to the scope later are picked up without
// any change here.
//
// currentCreateTable is the table's current definition, normally its
// SHOW CREATE TABLE. It must describe the table stmt alters. Pass an empty
// string when it is not available: coverage then narrows to the checks that need
// only the statement — the ENUM/SET checks, which compare a redeclared column
// against its current type, and the missing-primary-key refusal, which reads
// the current key definition, are skipped. The definition must also reflect
// the table's true key set: SHOW CREATE TABLE output collected with
// show_gipk_in_create_table_and_information_schema disabled omits a generated
// invisible primary key and misreports the table as unkeyed.
//
// logger may be nil, in which case the checks' own logging is discarded.
//
// refused is true only when Spirit will refuse the statement, so a caller may
// act on it. A statement that is not an ALTER TABLE is never refused: Spirit
// runs CREATE TABLE and DROP TABLE as native DDL rather than through the copy
// process. err reports input that cannot be classified — an unparseable
// statement, more than one statement, a currentCreateTable for a different
// table, or one that does not describe a column the statement redeclares — and
// is never itself a refusal.
func StatementRefusal(ctx context.Context, stmt, currentCreateTable string, logger *slog.Logger) (reason string, refused bool, err error) {
	stmts, err := statement.New(stmt)
	if err != nil {
		return "", false, fmt.Errorf("parse statement to check for refusal: %w", err)
	}
	if len(stmts) != 1 {
		return "", false, fmt.Errorf("refusal check requires exactly one statement, got %d", len(stmts))
	}
	abs := stmts[0]
	if !abs.IsAlterTable() {
		return "", false, nil
	}

	resources := Resources{Statement: abs}
	if currentCreateTable != "" {
		resources.Table, err = tableMetadataFor(abs, currentCreateTable)
		if err != nil {
			return "", false, err
		}
	}
	if checkErr := RunChecks(ctx, resources, logger, ScopeStatement); checkErr != nil {
		var unclassified *classificationError
		if errors.As(checkErr, &unclassified) {
			return "", false, fmt.Errorf("classify statement against the current definition of table %q: %w", abs.Table, checkErr)
		}
		return checkErr.Error(), true, nil
	}
	return "", false, nil
}

// classificationError marks a check error that is not a refusal: the check could
// not reach a verdict, because the inputs do not describe what it has to
// compare — typically a column the statement redeclares that the supplied table
// definition does not carry. A refusal is a claim about the statement, so
// StatementRefusal reports one of these as an error rather than letting a
// caller act on it and block an apply that may well succeed.
//
// It keeps the wrapped error's message: a migration reaches these through
// preflight, where the distinction does not apply and the check's own wording
// is what the operator needs.
type classificationError struct{ err error }

func (e *classificationError) Error() string { return e.err.Error() }
func (e *classificationError) Unwrap() error { return e.err }

// cannotClassify marks an error as a failure to reach a verdict rather than a
// refusal. A check uses it for a condition that says nothing about whether
// Spirit accepts the statement.
func cannotClassify(format string, args ...any) error {
	return &classificationError{err: fmt.Errorf(format, args...)}
}

// tableMetadataFor parses the table's current definition into the metadata the
// ENUM/SET checks read. It rejects a definition for a different table than the
// ALTER targets: those checks compare the ALTER's columns against the current
// ones by name, so a mismatched definition would report refusals — or miss
// them — for columns that have nothing to do with each other.
func tableMetadataFor(abs *statement.AbstractStatement, currentCreateTable string) (*table.TableInfo, error) {
	ct, err := statement.ParseCreateTable(currentCreateTable)
	if err != nil {
		return nil, fmt.Errorf("parse current definition of table %q: %w", abs.Table, err)
	}
	if !strings.EqualFold(ct.TableName, abs.Table) {
		return nil, fmt.Errorf("current definition is for table %q but the statement alters table %q", ct.TableName, abs.Table)
	}
	ti, err := ct.ToTableInfo(abs.Schema)
	if err != nil {
		return nil, err
	}
	return ti, nil
}
