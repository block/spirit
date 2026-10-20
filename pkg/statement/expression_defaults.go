package statement

import (
	"fmt"

	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/format"
)

// SplitAddColumnExpressionDefaults separates expression defaults from an ALTER
// that only adds ordinary columns. The returned clauses must only be applied to
// an empty table, before any inserts: removing a default on a populated table
// would change the values assigned to existing rows.
//
// Empty strings mean the ALTER cannot be split. Mixed operations, generated
// columns, checks, foreign keys and defaults that read columns are excluded:
// splitting those can change dependency validation or the meaning of the ALTER.
// Parsing a private AST leaves the caller's original statement unchanged.
func SplitAddColumnExpressionDefaults(alter string) (add, defaults string, err error) {
	stmts, err := New(fmt.Sprintf("ALTER TABLE `%s` %s", rewritePlaceholderTable, alter))
	if err != nil {
		return "", "", err
	}
	if len(stmts) != 1 {
		return "", "", nil
	}
	node, ok := stmts[0].AsAlterTable()
	if !ok {
		return "", "", nil
	}
	var deferred []*ast.AlterTableSpec
	for _, spec := range node.Specs {
		if spec.Tp != ast.AlterTableAddColumns || len(spec.NewConstraints) != 0 {
			return "", "", nil
		}
		for _, col := range spec.NewColumns {
			options := make([]*ast.ColumnOption, 0, len(col.Options))
			defaultCount := 0
			for _, option := range col.Options {
				switch option.Tp { //nolint:exhaustive
				case ast.ColumnOptionGenerated, ast.ColumnOptionCheck, ast.ColumnOptionReference, ast.ColumnOptionAutoIncrement:
					return "", "", nil
				case ast.ColumnOptionDefaultValue:
					defaultCount++
					if defaultCount > 1 {
						return "", "", nil // Do not turn an invalid definition into a valid one.
					}
					if isExpressionDefault(option.Expr) {
						var columns columnNameCollector
						if _, ok := option.Expr.Accept(&columns); !ok || len(columns.names) != 0 {
							return "", "", nil
						}
						deferred = append(deferred, &ast.AlterTableSpec{
							Tp:         ast.AlterTableAlterColumn,
							NewColumns: []*ast.ColumnDef{{Name: col.Name, Options: []*ast.ColumnOption{{Expr: option.Expr}}}},
						})
						continue
					}
				}
				options = append(options, option)
			}
			col.Options = options
		}
	}
	if len(deferred) == 0 {
		return "", "", nil
	}
	add, err = alterClauses(node, format.DefaultRestoreFlags)
	if err != nil {
		return "", "", err
	}
	node.Specs = deferred
	defaults, err = alterClauses(node, format.DefaultRestoreFlags)
	return add, defaults, err
}
