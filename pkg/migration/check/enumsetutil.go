package check

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/mysql"
)

// requireCurrentColumnTypes reports whether Resources carries the table metadata
// the ENUM/SET checks need to compare a redeclared column against its current
// definition.
//
// A ScopeStatement caller may not have the table's DDL to build it from, so the
// check is skipped rather than guessing at the current type (see
// ScopeStatement). Every other scope runs inside a migration, which loads the
// table before it runs any check: metadata missing there means these three
// guards against a data-corrupting ENUM/SET change would not run at all, so the
// check fails rather than passing quietly.
func requireCurrentColumnTypes(r Resources, logger *slog.Logger, checkName string) (bool, error) {
	if r.Table != nil {
		return true, nil
	}
	if r.scope&ScopeStatement != 0 {
		logger.Debug("skipping check: no table metadata supplied, cannot compare against the current column types",
			"check", checkName)
		return false, nil
	}
	return false, fmt.Errorf("check %s cannot run: the table's current column types were not loaded", checkName)
}

// isEnumOrSetType returns true if the MySQL type string represents an ENUM or SET type.
func isEnumOrSetType(mysqlType string) bool {
	lower := strings.ToLower(mysqlType)
	return strings.HasPrefix(lower, "enum(") || strings.HasPrefix(lower, "set(")
}

// isEnumType returns true if the MySQL type string represents an ENUM type.
func isEnumType(mysqlType string) bool {
	return strings.HasPrefix(strings.ToLower(mysqlType), "enum(")
}

// isSetType returns true if the MySQL type string represents a SET type.
func isSetType(mysqlType string) bool {
	return strings.HasPrefix(strings.ToLower(mysqlType), "set(")
}

// parseEnumSetValues extracts the element values from a MySQL type string
// like "enum('a','b','c')" or "set('x','y','z')".
//
// It uses a small state machine to correctly handle values containing
// commas and escaped (doubled) single quotes within enum/set elements.
//
// It is fail-closed: any unexpected characters or malformed structure returns
// an error so that callers (safety checks) are never silently bypassed.
func parseEnumSetValues(mysqlType string) ([]string, error) {
	if !isEnumOrSetType(mysqlType) {
		return nil, fmt.Errorf("not an enum/set type: %q", mysqlType)
	}

	start := strings.IndexByte(mysqlType, '(')
	if start < 0 {
		return nil, fmt.Errorf("missing opening parenthesis in type: %q", mysqlType)
	}
	end := strings.LastIndexByte(mysqlType, ')')
	if end <= start {
		return nil, fmt.Errorf("missing closing parenthesis in type: %q", mysqlType)
	}

	inner := mysqlType[start+1 : end]
	if len(inner) == 0 {
		return nil, nil // e.g. enum() — valid but empty
	}

	return parseSQLQuotedList(inner)
}

// parseSQLQuotedList parses a comma-separated list of single-quoted SQL
// strings, correctly handling embedded commas and escaped (doubled) quotes.
//
// It is fail-closed: any unexpected character outside of quotes causes the
// parser to return an error rather than silently producing a partial result.
func parseSQLQuotedList(s string) ([]string, error) {
	var elems []string
	i := 0
	n := len(s)

	// expectValue tracks whether we are currently expecting the start of a
	// quoted value (true) or a delimiter/completion after a value (false).
	expectValue := true

	for {
		// Skip whitespace outside of values.
		for i < n && (s[i] == ' ' || s[i] == '\t') {
			i++
		}

		if expectValue {
			// We are expecting the start of a quoted value.
			if i >= n {
				// Empty input is handled by the caller; reaching EOF here
				// indicates a trailing delimiter or otherwise malformed input.
				if len(elems) == 0 {
					return nil, fmt.Errorf("empty quoted list %q", s)
				}
				return nil, fmt.Errorf("trailing delimiter in quoted list %q", s)
			}
			if s[i] != '\'' {
				return nil, fmt.Errorf("unexpected character %q at position %d in quoted list %q", s[i], i, s)
			}

			// Opening quote found; collect the value.
			i++ // skip opening quote
			var buf strings.Builder
			closed := false
			for i < n {
				if s[i] == '\'' {
					// Check for doubled quote (escape sequence).
					if i+1 < n && s[i+1] == '\'' {
						buf.WriteByte('\'')
						i += 2
						continue
					}
					// Closing quote.
					i++
					closed = true
					break
				}
				buf.WriteByte(s[i])
				i++
			}
			if !closed {
				return nil, fmt.Errorf("unterminated quoted string in quoted list %q", s)
			}
			elems = append(elems, buf.String())
			// Next we expect either a comma delimiter or the end of the list.
			expectValue = false
		} else {
			// We have just parsed a value; now expect either a comma or EOF.
			if i >= n {
				// No more input; successfully parsed all elements.
				break
			}
			if s[i] != ',' {
				return nil, fmt.Errorf("unexpected character %q at position %d in quoted list %q", s[i], i, s)
			}
			// Consume the comma and loop back to parse the next value.
			i++
			expectValue = true
		}
	}

	return elems, nil
}

// isPrefix returns true if oldElems is a prefix of newElems.
// This is the safe case: all existing values remain at the same ordinal
// positions, and new values are only appended at the end.
func isPrefix(oldElems, newElems []string) bool {
	if len(newElems) < len(oldElems) {
		return false
	}
	for i, elem := range oldElems {
		if newElems[i] != elem {
			return false
		}
	}
	return true
}

// isCompatibleEnumChange returns true if newElems is a binlog-ordinal-safe
// modification of existingElems:
//
//   - Values present in BOTH lists must appear in the same RELATIVE ORDER
//     (existing values may be dropped from anywhere, but not moved).
//   - Values in newElems that are NOT in existingElems must appear only
//     AFTER all retained existing values (new values are appended at end,
//     never interleaved among or before existing values).
//
// The buffered replay path decodes binlog ENUM ordinals against the SOURCE
// table's element list and writes the resulting string into the target.
// Any retained value therefore decodes to the same string the target
// accepts. Rows whose value was dropped from the new enum land in the
// target as ” (the empty string — MySQL's invalid-ENUM sentinel, ordinal
// 0), which the post-cutover checksum catches as a mismatch, so the
// migration fails closed if such rows existed.
func isCompatibleEnumChange(existingElems, newElems []string) bool {
	existingSet := make(map[string]struct{}, len(existingElems))
	for _, e := range existingElems {
		existingSet[e] = struct{}{}
	}

	j := 0
	sawNew := false
	for _, e := range newElems {
		if _, isExisting := existingSet[e]; isExisting {
			if sawNew {
				return false // an existing value appears after a brand-new value
			}
			for j < len(existingElems) && existingElems[j] != e {
				j++
			}
			if j >= len(existingElems) {
				return false // existing value out of relative order, or repeated in newElems
			}
			j++
		} else {
			sawNew = true
		}
	}
	return true
}

// modifiedColumn describes a column in an ALTER TABLE that is being
// modified via MODIFY COLUMN or CHANGE COLUMN.
type modifiedColumn struct {
	LookupName string         // column name to look up in the existing table
	ColDef     *ast.ColumnDef // the new column definition from the ALTER
}

// findModifiedColumns extracts all MODIFY COLUMN and CHANGE COLUMN
// specs from an ALTER TABLE statement.
// These are the only two ALTER specs that can redefine a column's type.
func findModifiedColumns(stmtNode ast.StmtNode) []modifiedColumn {
	alterStmt, ok := stmtNode.(*ast.AlterTableStmt)
	if !ok {
		return nil
	}

	var result []modifiedColumn
	for _, spec := range alterStmt.Specs {
		var colDef *ast.ColumnDef
		var lookupName string

		switch spec.Tp { //nolint: exhaustive
		case ast.AlterTableModifyColumn:
			if len(spec.NewColumns) > 0 {
				colDef = spec.NewColumns[0]
				lookupName = colDef.Name.Name.O
			}
		case ast.AlterTableChangeColumn:
			if len(spec.NewColumns) > 0 {
				colDef = spec.NewColumns[0]
				if spec.OldColumnName != nil {
					lookupName = spec.OldColumnName.Name.O
				} else {
					lookupName = colDef.Name.Name.O
				}
			}
		}

		if colDef == nil || colDef.Tp == nil {
			continue
		}

		result = append(result, modifiedColumn{
			LookupName: lookupName,
			ColDef:     colDef,
		})
	}
	return result
}

// findModifiedEnumSetColumns returns only the modified columns whose new type
// is ENUM or SET.
func findModifiedEnumSetColumns(stmtNode ast.StmtNode) []modifiedColumn {
	var result []modifiedColumn
	for _, col := range findModifiedColumns(stmtNode) {
		tp := col.ColDef.Tp.GetType()
		if tp == mysql.TypeEnum || tp == mysql.TypeSet {
			result = append(result, col)
		}
	}
	return result
}
