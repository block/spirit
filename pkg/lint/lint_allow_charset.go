package lint

import (
	"fmt"
	"slices"
	"strings"

	"github.com/block/spirit/pkg/statement"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

func init() {
	Register(&AllowCharset{
		charsets: []string{"utf8mb4"},
	})
}

type AllowCharset struct {
	charsets []string
}

func (l *AllowCharset) Name() string {
	return "allow_charset"
}

func (l *AllowCharset) Description() string {
	return "Checks that only allowed character sets are used"
}

func (l *AllowCharset) String() string {
	return Stringer(l)
}

func (l *AllowCharset) Configure(config map[string]string) error {
	for k, v := range config {
		switch k {
		case "charsets":
			l.charsets = strings.Split(v, ",")
		default:
			return fmt.Errorf("unknown configuration key for %q: %s", l.Name(), k)
		}
	}
	return nil
}

func (l *AllowCharset) DefaultConfig() map[string]string {
	return map[string]string{
		"charsets": "utf8mb4",
	}
}

func (l *AllowCharset) Lint(createTables []*statement.CreateTable, changes []*statement.AbstractStatement) (violations []Violation) {
	// TODO: do we care about supporting character sets that are valid for MySQL but not valid for the TiDB parser? For example big5
	suggestion := "Use a supported character set: " + strings.Join(l.charsets, ", ")
	for ct := range CreateTableStatements(createTables, changes) {
		if ct.TableOptions != nil && ct.TableOptions.Charset != nil && !slices.Contains(l.charsets, *ct.TableOptions.Charset) {
			violations = append(violations, Violation{
				Linter:     l,
				Location:   &Location{Table: ct.TableName},
				Message:    fmt.Sprintf("Character set %q given for table %q is not allowed", *ct.TableOptions.Charset, ct.TableName),
				Severity:   SeverityWarning,
				Suggestion: &suggestion,
			})
		}
		for _, column := range ct.Columns {
			v := l.checkColumnCharset(column.Raw, fmt.Sprintf("Column %q has unsupported character set", column.Name))
			if v != nil {
				if v.Location != nil {
					v.Location.Table = ct.TableName
				}
				violations = append(violations, *v)
			}
		}
	}

	for _, change := range changes {
		at, ok := change.AsAlterTable()
		if !ok {
			continue
		}
		for _, spec := range at.Specs {
			var message string
			switch spec.Tp {
			case ast.AlterTableAddColumns:
				message = "New column %q in table %q uses a disallowed character set"
			case ast.AlterTableModifyColumn, ast.AlterTableChangeColumn:
				message = "Column %q in table %q modified to use a disallowed character set"
			default:
				continue
			}

			for _, column := range spec.NewColumns {
				v := l.checkColumnCharset(column, message)
				if v != nil {
					if v.Location != nil && at.Table != nil {
						v.Location.Table = at.Table.Name.String()
					}
					violations = append(violations, *v)
				}
			}
		}
	}
	return violations
}

func (l *AllowCharset) checkColumnCharset(column *ast.ColumnDef, message string) *Violation {
	suggestion := "Use a supported character set: " + strings.Join(l.charsets, ", ")
	charset := column.Tp.GetCharset()
	if charset != "" && !slices.Contains(l.charsets, charset) {
		return &Violation{
			Linter: l,
			Location: &Location{
				Column: &column.Name.Name.O,
			},
			Message:    fmt.Sprintf("%s: %q", message, charset),
			Severity:   SeverityWarning,
			Suggestion: &suggestion,
		}
	}
	return nil
}
