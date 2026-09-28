package statement

import (
	"strings"

	"github.com/block/spirit/pkg/parser/ast"
)

// ColumnCollationChange is how an ALTER TABLE changes the collation one
// existing column compares under.
type ColumnCollationChange struct {
	// Before is the collation the column compares under now, and After the
	// one it compares under once the statement applies. Either is empty when
	// the column carries no charset at that point (numeric, temporal, binary
	// string, ...).
	Before, After string

	// DeclaredAs is the column's name as the statement spells it, or empty
	// when the statement changes the column without naming it — a
	// CONVERT TO CHARACTER SET re-collates every character column.
	DeclaredAs string
}

// Recollated reports whether a column that carries a charset both before and
// after the statement compares under a different collation afterwards, so the
// same values sort, and compare equal, differently.
func (c ColumnCollationChange) Recollated() bool {
	return c.Before != "" && c.After != "" && c.Before != c.After
}

// ColumnCollationChange resolves the collation column compares under once the
// ALTER TABLE applies, following MySQL's rules:
//
//   - CONVERT TO CHARACTER SET re-collates every column that carries a
//     charset once the statement applies, overriding a collation the same
//     statement declares on the column.
//   - A MODIFY or CHANGE COLUMN resolves the redeclared definition the way a
//     CREATE TABLE would, against the table's defaults as the statement
//     leaves them. A redeclaration that omits COLLATE therefore drops a
//     collation the column declared explicitly, and one that omits both
//     CHARACTER SET and COLLATE picks up a default the same statement
//     changes.
//   - Any other column keeps the collation it has.
//
// currentCollation is the collation the column compares under now, empty for
// a column that carries no charset, and tableCollation the table's current
// default collation, empty when it is not known.
//
// determined is false when the result depends on a default the inputs do not
// carry: CONVERT TO CHARACTER SET DEFAULT uses the schema's default, and a
// redeclaration that inherits the table default needs tableCollation.
func (a *AbstractStatement) ColumnCollationChange(column, currentCollation, tableCollation string) (change ColumnCollationChange, determined bool, err error) {
	alter, ok := a.AsAlterTable()
	if !ok {
		return ColumnCollationChange{}, false, ErrNotAlterTable
	}
	change.Before = normalizeCollationName(strings.ToLower(currentCollation))
	change.After = change.Before

	defaults, convert := alteredTableDefaults(alter, normalizeCollationName(strings.ToLower(tableCollation)))

	if colDef, spelledAs := redeclaredColumn(alter, column); colDef != nil {
		change.DeclaredAs = spelledAs
		ct := &CreateTable{TableOptions: defaults.tableOptions()}
		ct.Columns = Columns{ct.parseColumn(colDef)}
		binaryAttributeNormalizer{}.Normalize(ct)
		redeclared := &ct.Columns[0]
		if !redeclared.CarriesCharset() {
			change.After = ""
			return change, true, nil
		}
		if convert {
			change.After = defaults.collation
			return change, defaults.determined, nil
		}
		_, change.After = redeclared.EffectiveCharsetCollation(ct)
		return change, change.After != "", nil
	}

	if convert && change.Before != "" {
		change.After = defaults.collation
		return change, defaults.determined, nil
	}
	return change, true, nil
}

// tableDefaults is a table's default charset and collation. determined is
// false when the default is not known.
type tableDefaults struct {
	charset, collation string
	determined         bool
}

// defaultsForCollation is the table default a collation implies. Every
// collation belongs to exactly one charset, which its name leads with.
func defaultsForCollation(collation string) tableDefaults {
	if collation == "" {
		return tableDefaults{}
	}
	return tableDefaults{charset: charsetOfCollation(collation), collation: collation, determined: true}
}

// tableOptions renders the defaults as the options of a CREATE TABLE, so a
// column resolved against them follows the same rules as one parsed from a
// table definition. An unknown default is left unset, which is how a table
// definition that does not declare one reads.
func (d tableDefaults) tableOptions() *TableOptions {
	if !d.determined {
		return &TableOptions{}
	}
	charset, collation := d.charset, d.collation
	return &TableOptions{Charset: &charset, Collation: &collation}
}

// TableDefaultCollation returns the collation a column declared without a
// charset or collation takes in this table, or "" when the definition does not
// determine it — a hand-written definition that declares no DEFAULT CHARSET
// inherits the schema's default. SHOW CREATE TABLE always declares one.
func (ct *CreateTable) TableDefaultCollation() string {
	if collation := ct.TableOptions.getCollation(); collation != nil {
		return normalizeCollationName(strings.ToLower(*collation))
	}
	if charset := ct.TableOptions.getCharset(); charset != nil {
		if _, collation, ok := DefaultCollationForCharset(*charset); ok {
			return collation
		}
	}
	return ""
}

// alteredTableDefaults returns the table's default charset and collation as
// the ALTER leaves them, and whether it converts the existing columns to that
// default (CONVERT TO CHARACTER SET). tableCollation is the current default,
// empty when it is not known.
//
// MySQL resolves the table options of a statement together, so the order they
// are written in does not matter: an explicit collation wins, and a charset
// written without one selects that charset's default collation.
func alteredTableDefaults(alter *ast.AlterTableStmt, tableCollation string) (defaults tableDefaults, convert bool) {
	var charset, collation string
	charsetIsSchemaDefault := false
	for _, spec := range alter.Specs {
		if spec.Tp != ast.AlterTableOption {
			continue
		}
		for _, opt := range spec.Options {
			if opt.Tp == ast.TableOptionCollate {
				collation = normalizeCollationName(strings.ToLower(opt.StrValue))
				continue
			}
			if opt.Tp != ast.TableOptionCharset {
				continue
			}
			if opt.UintValue == ast.TableOptionCharsetWithConvertTo {
				convert = true
			}
			if opt.Default {
				charsetIsSchemaDefault = true
				continue
			}
			charset = strings.ToLower(opt.StrValue)
		}
	}
	switch {
	case collation != "":
		return defaultsForCollation(collation), convert
	case charsetIsSchemaDefault:
		return tableDefaults{}, convert
	case charset != "":
		cs, def, ok := DefaultCollationForCharset(charset)
		if !ok {
			return tableDefaults{}, convert
		}
		return tableDefaults{charset: cs, collation: def, determined: true}, convert
	default:
		return defaultsForCollation(tableCollation), convert
	}
}

// redeclaredColumn returns the definition a MODIFY or CHANGE COLUMN gives
// column — matched case-insensitively against the column's current name, which
// is the old name of a CHANGE — along with the name as the statement spells
// it, or nil when the statement does not redeclare the column.
func redeclaredColumn(alter *ast.AlterTableStmt, column string) (*ast.ColumnDef, string) {
	for _, spec := range alter.Specs {
		if len(spec.NewColumns) == 0 {
			continue
		}
		colDef := spec.NewColumns[0]
		var name string
		switch {
		case spec.Tp == ast.AlterTableModifyColumn:
			name = colDef.Name.Name.O
		case spec.Tp == ast.AlterTableChangeColumn && spec.OldColumnName != nil:
			name = spec.OldColumnName.Name.O
		default:
			continue
		}
		if strings.EqualFold(name, column) {
			return colDef, name
		}
	}
	return nil, ""
}
