package statement

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
//     semantically meaningful and kept in SHOW CREATE TABLE.
type integerDisplayWidthNormalizer struct{}

func (integerDisplayWidthNormalizer) Name() string { return "integer-display-width" }

func (integerDisplayWidthNormalizer) Normalize(ct *CreateTable) *CreateTable {
	for i := range ct.Columns {
		c := &ct.Columns[i]
		if !isIntegerColumnType(c.Type) {
			continue // not an integer type
		}
		if c.Zerofill != nil && *c.Zerofill {
			continue // width is meaningful under ZEROFILL
		}
		if c.Type == "tinyint" && c.Length != nil && *c.Length == 1 {
			continue // tinyint(1) is preserved by MySQL
		}
		c.Length = nil
	}
	return ct
}
