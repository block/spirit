package lint

import (
	"fmt"
	"slices"
	"strings"

	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/statement"
)

func init() {
	Register(&AllowCharset{
		charsets: []string{"utf8mb4", "binary"},
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
		"charsets": "utf8mb4,binary",
	}
}

// Lint walks the post-state of the schema. The message form preserves the
// pre/post-state distinction that the old ALTER pass made explicit:
//
//   - "New column …" when the column did not exist in the pre-state.
//   - "Column … modified to use …" when MODIFY/CHANGE COLUMN retypes an
//     existing column.
//   - "Column … has unsupported character set" for pre-existing untouched
//     columns and columns inside a new CREATE TABLE.
func (l *AllowCharset) Lint(createTables []*statement.CreateTable, changes []*statement.AbstractStatement) (violations []Violation) {
	// TODO: do we care about supporting character sets that are valid for MySQL but not valid for the TiDB parser? For example big5
	suggestion := "Use a supported character set: " + strings.Join(l.charsets, ", ")
	pre := PreStateColumns(createTables)
	newTables := newTablesInChanges(changes)
	modified := columnsModifiedInChanges(changes)

	for _, ct := range PostState(createTables, changes) {
		if ct.TableOptions != nil && ct.TableOptions.Charset != nil && !slices.Contains(l.charsets, *ct.TableOptions.Charset) {
			violations = append(violations, Violation{
				Linter:     l,
				Location:   &Location{Table: ct.TableName},
				Message:    fmt.Sprintf("Character set %q given for table %q is not allowed", *ct.TableOptions.Charset, ct.TableName),
				Severity:   SeverityWarning,
				Suggestion: &suggestion,
			})
		}
		tKey := strings.ToLower(ct.TableName)
		for _, column := range ct.Columns {
			if column.Raw == nil {
				continue
			}
			colKey := strings.ToLower(column.Name)
			_, preExisted := pre[tKey][colKey]
			var message string
			switch {
			case modified[tKey][colKey]:
				message = fmt.Sprintf("Column %q in table %q modified to use a disallowed character set", column.Name, ct.TableName)
			case !preExisted && !newTables[tKey]:
				// ADD COLUMN against a pre-existing table.
				message = fmt.Sprintf("New column %q in table %q uses a disallowed character set", column.Name, ct.TableName)
			default:
				message = fmt.Sprintf("Column %q has unsupported character set", column.Name)
			}
			v := l.checkColumnCharset(column.Raw, message)
			if v != nil {
				v.Location.Table = ct.TableName
				violations = append(violations, *v)
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
