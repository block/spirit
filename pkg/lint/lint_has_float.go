package lint

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/parser/mysql"
	"github.com/block/spirit/pkg/statement"
)

type HasFloatLinter struct{}

func init() {
	Register(&HasFloatLinter{})
}

func (l *HasFloatLinter) String() string {
	return Stringer(l)
}

func (l *HasFloatLinter) Name() string {
	return "has_float"
}

func (l *HasFloatLinter) Description() string {
	return "Detects usage of FLOAT or DOUBLE data types in table definitions"
}

// Lint operates on a post-state view of the schema, so a column being
// converted away from FLOAT/DOUBLE in an ALTER doesn't generate a false
// positive. The violation message uses three forms to preserve
// actionability:
//
//   - "New column …" when the column did not exist in the pre-state
//     (ADD COLUMN, or a column in a CREATE TABLE).
//   - "Column … modified to use …" when the column existed pre-state and
//     is being retyped by MODIFY / CHANGE COLUMN.
//   - "Column … uses …" for pre-existing untouched columns.
func (l *HasFloatLinter) Lint(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) (violations []Violation) {
	pre := PreStateColumns(existingTables)
	modified := columnsModifiedInChanges(changes)

	for _, ct := range PostState(existingTables, changes) {
		tKey := strings.ToLower(ct.TableName)
		for _, col := range ct.Columns {
			if col.Raw == nil || col.Raw.Tp == nil {
				continue
			}
			tp := col.Raw.Tp.GetType()
			if tp != mysql.TypeFloat && tp != mysql.TypeDouble {
				continue
			}
			colName := col.Name
			colKey := strings.ToLower(colName)
			_, preExisted := pre[tKey][colKey]
			var message string
			switch {
			case modified[tKey][colKey]:
				// MODIFY / CHANGE COLUMN — the column existed pre-state
				// (or is asserted to) and is being retyped.
				message = fmt.Sprintf("Column %q in table %q modified to use floating-point data type", colName, ct.TableName)
			case !preExisted:
				// ADD COLUMN, or a column inside a new CREATE TABLE.
				message = fmt.Sprintf("New column %q in table %q uses floating-point data type", colName, ct.TableName)
			default:
				message = fmt.Sprintf("Column %q in table %q uses %q data type", colName, ct.TableName, col.Raw.Tp.String())
			}
			violations = append(violations, Violation{
				Linter: l,
				Location: &Location{
					Table:  ct.TableName,
					Column: &colName,
				},
				Message:  message,
				Severity: SeverityWarning,
			})
		}
	}
	return violations
}
