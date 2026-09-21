package statement

import "strings"

func init() { registerNormalizer(booleanKeywordDefaultNormalizer{}) }

// booleanKeywordDefaultNormalizer folds a bare TRUE/FALSE keyword DEFAULT on an
// integer column to the 1/0 MySQL stores. The keywords are literal aliases for
// those integers and SHOW CREATE TABLE reports the integer, so
// `active BOOLEAN NOT NULL DEFAULT FALSE` comes back as
// `active tinyint(1) NOT NULL DEFAULT '0'`. Unfolded, the keyword diffs against
// the live column and emits a MODIFY COLUMN that stores the same '0' and diffs
// again on the next run — a change that can never converge.
//
// The parser already folds the BOOLEAN type itself to tinyint(1) (see the
// package comment in normalize.go), which is why only the default is left here.
//
// Only integer columns fold, because they are the types that store the keyword
// as exactly 1/0. Deliberately left alone:
//
//   - decimal, which applies the column's scale: decimal(4,2) DEFAULT TRUE
//     stores '1.00', so folding to 1 would not converge either. Canonicalizing
//     numeric scale is a separate rule, not this one.
//   - year, which puts the keyword through YEAR's own interpretation:
//     year DEFAULT TRUE stores '2001', not 1.
//   - string columns, which store the keyword cast to its string form ('0').
//     Quotedness is part of column identity on those types — DEFAULT 'FALSE'
//     is a genuine string value there — so the bare and quoted spellings must
//     not be folded together in one rule.
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
		if !isIntegerColumnType(c.Type) {
			continue // only integers store the keyword as exactly 1/0
		}
		switch strings.ToUpper(*c.Default) {
		case "TRUE":
			folded := "1"
			c.Default = &folded
		case "FALSE":
			folded := "0"
			c.Default = &folded
		}
	}
	return ct
}
