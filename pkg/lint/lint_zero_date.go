package lint

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/statement"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
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

func (l *ZeroDateLinter) Lint(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) (violations []Violation) {
	for ct := range CreateTableStatements(existingTables, changes) {
		for _, column := range ct.Columns {
			v := l.checkColumnZeroDate(column.Raw, ct.TableName)
			if v != nil {
				violations = append(violations, *v)
			}
		}
	}

	// Check ALTER TABLE statements
	for _, change := range changes {
		at, ok := change.AsAlterTable()
		if !ok {
			continue
		}
		for _, spec := range at.Specs {
			switch spec.Tp { //nolint:exhaustive
			case ast.AlterTableAddColumns, ast.AlterTableModifyColumn, ast.AlterTableChangeColumn:
				for _, column := range spec.NewColumns {
					v := l.checkColumnZeroDate(column, at.Table.Name.String())
					if v != nil {
						violations = append(violations, *v)
					}
				}
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
				// Extract default value from expression using Restore
				var sb strings.Builder
				rCtx := format.NewRestoreCtx(format.DefaultRestoreFlags|format.RestoreStringWithoutCharset, &sb)
				err := option.Expr.Restore(rCtx)
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

	if !nullable && defaultValue == nil {
		// If the column is *not* nullable and has no default, MySQL will try to assign a zero date default value
		// *unless* strict mode and NO_ZERO_DATE are enabled
		return &Violation{
			Linter: l,
			Location: &Location{
				Table:  tableName,
				Column: &columnName,
			},
			Severity: SeverityWarning,
			Message:  fmt.Sprintf("date column %s is not nullable and has no default", columnName),
		}
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
				Message:  fmt.Sprintf("column %s with type %q has a zero default value", columnName, column.Tp.String()),
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
