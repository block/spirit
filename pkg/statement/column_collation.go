package statement

import (
	"strings"

	"github.com/block/spirit/pkg/parser/ast"
)

// CharsetCollation is a charset and a collation of it. Collation is empty when
// only the charset is known: a definition that names utf8mb4 without a
// collation leaves it to the server's default_collation_for_utf8mb4.
type CharsetCollation struct {
	Charset, Collation string
}

// normalized spells both names the way this package compares them, and takes
// the charset from the collation when only the collation is given: every
// collation belongs to one charset, which its name leads with.
func (c CharsetCollation) normalized() CharsetCollation {
	collation := normalizeCollationName(strings.ToLower(c.Collation))
	charset := NormalizeCharsetName(c.Charset)
	if charset == "" {
		charset = charsetOfCollation(collation)
	}
	return CharsetCollation{Charset: charset, Collation: collation}
}

// collationKnown reports whether c names its collation, or carries no charset
// and so has none.
func (c CharsetCollation) collationKnown() bool {
	return c.Collation != "" || c.Charset == ""
}

// ColumnCollationChange is how an ALTER TABLE changes the collation one
// existing column compares under.
type ColumnCollationChange struct {
	// Before is what the column compares under now, and After what it
	// compares under once the statement applies. Either is the zero value
	// when the column carries no charset at that point (numeric, temporal,
	// binary string, ...), and either can know its charset but not its
	// collation.
	Before, After CharsetCollation

	// DeclaredAs is the column's name as the statement spells it, or empty
	// when the statement changes the column without naming it — a
	// CONVERT TO CHARACTER SET re-collates every character column.
	DeclaredAs string
}

// Changed reports whether the column compares under a different collation
// once the statement applies, so the same values may sort, and compare equal,
// differently. Gaining or losing a collation counts: a change between a
// character type and a binary or non-string type is one. When either side's
// collation is not known, a change of charset still decides it, since no two
// charsets share a collation.
func (c ColumnCollationChange) Changed() bool {
	if c.Before.collationKnown() && c.After.collationKnown() {
		return c.Before != c.After
	}
	return c.Before.Charset != c.After.Charset
}

// resolveTo records what the statement leaves a column that carries a charset
// under, and reports whether that decides Changed. A charset that is not known
// decides nothing, and a collation that is not known decides it only when the
// charset changes.
func (c ColumnCollationChange) resolveTo(after CharsetCollation) (ColumnCollationChange, bool) {
	c.After = after
	if after.Charset == "" {
		return c, false
	}
	return c, (c.Before.collationKnown() && c.After.collationKnown()) || c.Before.Charset != c.After.Charset
}

// tableOptionsFor renders a table default as the options of a CREATE TABLE, so
// a column resolved against it follows the same rules as one parsed from a
// table definition. What is not known is left unset, which is how a table
// definition that does not declare it reads.
func tableOptionsFor(d CharsetCollation) *TableOptions {
	options := &TableOptions{}
	if d.Charset != "" {
		charset := d.Charset
		options.Charset = &charset
	}
	if d.Collation != "" {
		collation := d.Collation
		options.Collation = &collation
	}
	return options
}

// alteredTableDefaults returns the table's default charset and collation as
// the ALTER leaves them, and whether it converts the existing columns to that
// default (CONVERT TO CHARACTER SET). current is the default now, normalized,
// the zero value when it is not known. The result's collation is empty when
// it is not known, and its charset too when neither is.
//
// MySQL resolves the table options of a statement together, so the order they
// are written in does not matter: an explicit collation wins, and a charset
// written without one selects that charset's default collation.
func alteredTableDefaults(alter *ast.AlterTableStmt, current CharsetCollation) (defaults CharsetCollation, convert bool) {
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
		return CharsetCollation{Collation: collation}.normalized(), convert
	case charsetIsSchemaDefault:
		return CharsetCollation{}, convert
	case charset != "":
		if !charsetDefaultCollationIsFixed(charset) {
			return CharsetCollation{Charset: charset}.normalized(), convert
		}
		cs, def, ok := DefaultCollationForCharset(charset)
		if !ok {
			return CharsetCollation{}, convert
		}
		return CharsetCollation{Charset: cs, Collation: def}, convert
	default:
		return current, convert
	}
}

// redeclaredColumn returns the definition a MODIFY or CHANGE COLUMN gives
// column — matched case-insensitively against the column's current name, which
// is the old name of a CHANGE — along with the name as the statement spells
// it, or nil when the statement does not redeclare the column. The first match
// is the only one: MySQL rejects a statement that redeclares a column twice.
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
