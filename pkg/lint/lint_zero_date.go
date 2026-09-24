package lint

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/format"
	"github.com/block/spirit/pkg/parser/mysql"
	"github.com/block/spirit/pkg/statement"
)

type ZeroDateLinter struct{}

func init() {
	Register(&ZeroDateLinter{})
}

func (l *ZeroDateLinter) Name() string {
	return "zero_date"
}

func (l *ZeroDateLinter) Description() string {
	return "Check for columns with zero-date default values"
}

func (l *ZeroDateLinter) String() string {
	return Stringer(l)
}

// Lint operates on a post-state view of the schema so that ALTERs which fix
// zero-date columns don't produce false positives on the pre-state.
func (l *ZeroDateLinter) Lint(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) (violations []Violation) {
	for _, ct := range PostState(existingTables, changes) {
		for _, column := range ct.Columns {
			if column.Raw == nil {
				continue
			}
			v := l.checkColumnZeroDate(column.Raw, ct.TableName)
			if v != nil {
				violations = append(violations, *v)
			}
		}
	}
	return violations
}

func (l *ZeroDateLinter) checkColumnZeroDate(column *ast.ColumnDef, tableName string) *Violation {
	tp := column.Tp.GetType()
	if tp != mysql.TypeDatetime && tp != mysql.TypeTimestamp && tp != mysql.TypeDate {
		return nil
	}

	columnName := column.Name.Name.O
	nullable := true
	var defaultValue *string

	// Check if column is nullable and extract default value
	for _, option := range column.Options {
		switch option.Tp { //nolint:exhaustive
		case ast.ColumnOptionNotNull:
			nullable = false
		case ast.ColumnOptionNull:
			nullable = true
		case ast.ColumnOptionDefaultValue:
			if option.Expr != nil {
				// Unwrap the expression-default parentheses so
				// DEFAULT ('0000-00-00') is inspected like DEFAULT '0000-00-00'.
				expr := option.Expr
				for {
					paren, ok := expr.(*ast.ParenthesesExpr)
					if !ok {
						break
					}
					expr = paren.Expr
				}
				// Extract default value from expression using Restore
				var sb strings.Builder
				rCtx := format.NewRestoreCtx(format.DefaultRestoreFlags|format.RestoreStringWithoutCharset, &sb)
				err := expr.Restore(rCtx)
				if err == nil {
					val := sb.String()
					// Remove surrounding quotes if present for string literals
					if len(val) >= 2 && val[0] == '\'' && val[len(val)-1] == '\'' {
						val = val[1 : len(val)-1]
					}
					defaultValue = &val
				}
			}
		}
	}

	if nullable && defaultValue != nil && strings.EqualFold(*defaultValue, "NULL") {
		// If the column is nullable and the default value is NULL, we're good
		return nil
	}

	if defaultValue != nil {
		if *defaultValue == l.getZeroDate(tp) {
			return &Violation{
				Linter: l,
				Location: &Location{
					Table:  tableName,
					Column: &columnName,
				},
				Severity: SeverityWarning,
				Message:  fmt.Sprintf("column %q with type %q has a zero default value", columnName, column.Tp.String()),
			}
		}
	}

	return nil
}

func (l *ZeroDateLinter) getZeroDate(tp byte) string {
	switch tp {
	case mysql.TypeDate:
		return "0000-00-00"
	case mysql.TypeTimestamp, mysql.TypeDatetime:
		return "0000-00-00 00:00:00"
	default:
		return ""
	}
}
