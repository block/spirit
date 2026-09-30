package statement

import "github.com/block/spirit/pkg/parser/types"

func init() { registerNormalizer(integerDisplayWidthNormalizer{}) }

// integerDisplayWidthNormalizer drops the deprecated display width from integer
// columns, matching MySQL 8.0.19+ SHOW CREATE TABLE. The parser always
// populates a width — INT and INT(11) both arrive as int(11) — but MySQL
// reports a plain `int`, so an int(11) written in a schema file would otherwise
// produce a spurious MODIFY when diffed against the live int. Stripping also
// makes the unspecified and specified spellings converge (INT == INT(11)).
//
// Two widths are preserved, because MySQL preserves them:
//   - tinyint(1): the canonical BOOLEAN form (also how the parser folds BOOL).
//   - any integer with ZEROFILL: the width drives the zero-padding, so it is
//     semantically meaningful and kept in SHOW CREATE TABLE. Two ZEROFILL
//     widths are rewritten to the type's default *unsigned* width, because
//     MySQL stores that width for both (int zerofill and int(0) zerofill are
//     stored as int(10) unsigned zerofill): no width at all, which the parser
//     fills in with the *signed* default (int(11)), and a width of 0. Any other
//     explicit width, int(11) zerofill included, is kept.
type integerDisplayWidthNormalizer struct{}

// zerofillDefaultWidths is the display width MySQL gives each integer type
// under ZEROFILL (which implies UNSIGNED) when no width, or a width of 0, is
// declared: the number of digits in the type's largest unsigned value.
var zerofillDefaultWidths = map[string]int{
	"tinyint":   3,
	"smallint":  5,
	"mediumint": 8,
	"int":       10,
	"bigint":    20,
}

func (integerDisplayWidthNormalizer) Name() string { return "integer-display-width" }

func (integerDisplayWidthNormalizer) Normalize(ct *CreateTable) *CreateTable {
	for i := range ct.Columns {
		c := &ct.Columns[i]
		if !isIntegerColumnType(c.Type) {
			continue // not an integer type
		}
		if c.Zerofill != nil && *c.Zerofill {
			if width, ok := zerofillDefaultWidths[c.Type]; ok && (widthUnspecified(c) || (c.Length != nil && *c.Length == 0)) {
				c.Length = &width
			}
			continue // width is meaningful under ZEROFILL
		}
		if c.Type == "tinyint" && c.Length != nil && *c.Length == 1 {
			continue // tinyint(1) is preserved by MySQL
		}
		c.Length = nil
	}
	return ct
}

// widthUnspecified reports whether the column was declared without a width.
// Column.Length cannot tell: the parser renders int and int(11) as the same
// int(11). The raw field type keeps the difference, as an unspecified length.
func widthUnspecified(c *Column) bool {
	return c.Raw != nil && c.Raw.Tp.GetFlen() == types.UnspecifiedLength
}
