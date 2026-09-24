package lint

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/parser/mysql"
	"github.com/block/spirit/pkg/statement"
)

type HasTimestampLinter struct{}

func init() {
	Register(&HasTimestampLinter{})
}

func (l *HasTimestampLinter) String() string {
	return Stringer(l)
}

func (l *HasTimestampLinter) Name() string {
	return "has_timestamp"
}

func (l *HasTimestampLinter) Description() string {
	return "Detects usage of TIMESTAMP data type which overflows on 2038-01-19"
}

// Lint operates on a post-state view of the schema. For each TIMESTAMP column
// in the post-state, the severity is:
//
//   - Warning, if the column existed in the pre-state with TIMESTAMP type and
//     no incoming change touches it (legacy schema — don't boil the ocean).
//   - Error, if the column is newly added in this changeset, or modified to
//     TIMESTAMP, or appears in a CREATE TABLE in the changeset.
//
// Columns that were TIMESTAMP in the pre-state but are being dropped or
// converted to a different type in the changeset don't appear in the post-state
// and therefore produce no violation — which is exactly the desired behavior
// for migrations that fix legacy TIMESTAMP columns.
func (l *HasTimestampLinter) Lint(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) (violations []Violation) {
	post := PostState(existingTables, changes)
	pre := PreStateColumns(existingTables)
	createdInChanges := newTablesInChanges(changes)
	addedOrModifiedCols := columnsAddedOrModifiedInChanges(changes)

	for _, ct := range post {
		tKey := strings.ToLower(ct.TableName)
		for _, col := range ct.Columns {
			if col.Raw == nil || col.Raw.Tp == nil {
				continue
			}
			if col.Raw.Tp.GetType() != mysql.TypeTimestamp {
				continue
			}
			colName := col.Name
			severity := l.severityFor(tKey, colName, pre, createdInChanges, addedOrModifiedCols)
			violations = append(violations, Violation{
				Linter: l,
				Location: &Location{
					Table:  ct.TableName,
					Column: &colName,
				},
				Message:  fmt.Sprintf(`Column %q uses "TIMESTAMP" which overflows on 2038-01-19. Consider using "DATETIME" instead.`, colName),
				Severity: severity,
			})
		}
	}

	return violations
}

func (l *HasTimestampLinter) severityFor(tableKey, colName string, pre map[string]map[string]*statement.Column, createdTables map[string]bool, addedOrModified map[string]map[string]bool) Severity {
	colKey := strings.ToLower(colName)
	if createdTables[tableKey] {
		return SeverityError
	}
	if tableMods, ok := addedOrModified[tableKey]; ok {
		if tableMods[colKey] {
			return SeverityError
		}
	}
	if preTbl, ok := pre[tableKey]; ok {
		if preCol, ok := preTbl[colKey]; ok && preCol.Raw != nil && preCol.Raw.Tp != nil && preCol.Raw.Tp.GetType() == mysql.TypeTimestamp {
			return SeverityWarning
		}
	}
	return SeverityError
}
