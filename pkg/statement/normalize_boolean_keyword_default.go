package statement

import "strings"

func init() { registerNormalizer(booleanKeywordDefaultNormalizer{}) }

// booleanKeywordDefaultNormalizer folds a bare TRUE/FALSE keyword DEFAULT to
// the 1/0 MySQL stores, on the column types that store it as exactly that. The
// keywords are literal aliases for those integers and SHOW CREATE TABLE reports
// the stored value, so `active BOOLEAN NOT NULL DEFAULT FALSE` comes back as
// `active tinyint(1) NOT NULL DEFAULT '0'`. Unfolded, the keyword diffs against
// the live column and emits a MODIFY COLUMN that stores the same '0' and diffs
// again on the next run — a change that can never converge.
//
// The parser already folds the BOOLEAN type itself to tinyint(1) (see the
// package comment in normalize.go), which is why only the default is left here.
//
// How MySQL renders the stored default splits the folding types in two. On a
// numeric column it reports the value bare, so the fold is on the value alone.
// On a string column it reports a quoted literal, so the fold also has to mark
// the default as a string — otherwise the value matches and [columnsEqual]
// rejects the pair on quotedness instead, which is part of column identity
// there.
//
// A type only folds if it stores the keyword as exactly 1/0. Deliberately left
// alone, each reading taken from a live server:
//
//   - scaled decimal, which pads the default to the column's scale:
//     decimal(4,2) DEFAULT TRUE stores '1.00'. An unscaled decimal has nothing
//     to pad and does fold. Canonicalizing numeric scale is a separate rule.
//   - year, which puts the keyword through YEAR's own interpretation:
//     year DEFAULT TRUE stores '2001', not 1.
//   - binary, which pads to the column width with NULs: binary(4) DEFAULT TRUE
//     stores '1\0\0\0'. varbinary has nothing to pad and does fold.
//   - bit, which stores a bit literal: bit(1) DEFAULT TRUE stores b'1'. That
//     is a literal kind this layer cannot represent on emission (see the caveat
//     on [needsQuotes]), so folding to it would trade a diff that never
//     converges for a MODIFY COLUMN that will not parse.
//   - enum and set, where the keyword is read as a member index rather than a
//     value: enum('0','1') DEFAULT TRUE stores '0', the member at index 1, and
//     DEFAULT FALSE is rejected outright because no member sits at index 0.
//     Folding TRUE to 1 would silently mean a different member.
type booleanKeywordDefaultNormalizer struct{}

func (booleanKeywordDefaultNormalizer) Name() string { return "boolean-keyword-default" }

func (booleanKeywordDefaultNormalizer) Normalize(ct *CreateTable) *CreateTable {
	for i := range ct.Columns {
		c := &ct.Columns[i]
		if c.Default == nil {
			continue // no default to fold
		}
		if c.DefaultIsString {
			continue // a quoted 'TRUE' is a string value, not the keyword
		}
		if c.DefaultIsExpr {
			continue // an expression default keeps MySQL's own stored form
		}
		var stored string
		switch strings.ToUpper(*c.Default) {
		case "TRUE":
			stored = "1"
		case "FALSE":
			stored = "0"
		default:
			continue // not the keyword
		}
		switch {
		case storesKeywordAsNumber(c):
			c.Default = &stored
		case storesKeywordAsStringLiteral(c):
			c.Default = &stored
			c.DefaultIsString = true
		}
	}
	return ct
}

// storesKeywordAsNumber reports whether the column's type stores a TRUE/FALSE
// keyword default as exactly 1/0 and reports it bare. See
// [booleanKeywordDefaultNormalizer] for the types this excludes and why.
func storesKeywordAsNumber(c *Column) bool {
	if isIntegerColumnType(c.Type) {
		return true
	}
	switch strings.ToLower(c.Type) {
	case "double", "float":
		return true
	case "decimal":
		// Only an unscaled decimal, which has no scale to pad the value out to.
		return c.Scale == nil || *c.Scale == 0
	}
	return false
}

// storesKeywordAsStringLiteral reports whether the column's type stores a
// TRUE/FALSE keyword default as exactly 1/0 and reports it as a quoted string.
// See [booleanKeywordDefaultNormalizer] for the types this excludes and why.
func storesKeywordAsStringLiteral(c *Column) bool {
	switch strings.ToLower(c.Type) {
	case "varchar", "char", "varbinary":
		return true
	}
	return false
}
