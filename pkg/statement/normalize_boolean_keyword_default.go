package statement

import "strings"

func init() { registerNormalizer(booleanKeywordDefaultNormalizer{}) }

// booleanKeywordDefaultNormalizer rewrites a bare TRUE/FALSE keyword DEFAULT to
// the 1/0 MySQL stores, in the literal form the column's type reports it as.
// The keywords are aliases for those integers and SHOW CREATE TABLE reports the
// stored value, so `active BOOLEAN NOT NULL DEFAULT FALSE` comes back as
// `active tinyint(1) NOT NULL DEFAULT '0'`. Unfolded, the keyword diffs against
// the live column and emits a MODIFY COLUMN that stores the same '0' and diffs
// again on the next run — a change that can never converge.
//
// The parser already folds the BOOLEAN type itself to tinyint(1) (see the
// package comment in normalize.go), which is why only the default is left here.
//
// How MySQL reports the stored value depends on the type, so the fold sets the
// default's kind as well as its value (see [storedKeywordForm]): bare on a
// numeric column, quoted on a string column, and as a bit literal on a bit
// column. Setting the kind is what makes the two sides compare equal —
// [columnsEqual] treats the literal form as part of column identity, so folding
// the value alone would still diff on the form.
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
//   - enum and set, where the keyword is read as a member index rather than a
//     value: enum('0','1') DEFAULT TRUE stores '0', the member at index 1, and
//     DEFAULT FALSE is rejected outright because no member sits at index 0.
//     Folding TRUE to 1 would silently mean a different member.
type booleanKeywordDefaultNormalizer struct{}

func (booleanKeywordDefaultNormalizer) Name() string { return "boolean-keyword-default" }

func (booleanKeywordDefaultNormalizer) Normalize(ct *CreateTable) *CreateTable {
	for i := range ct.Columns {
		c := &ct.Columns[i]
		// The recorded kind is the whole guard: a quoted 'TRUE' is a string
		// value rather than the keyword, and the 1 it aliases is a number, so
		// neither reaches here.
		if c.DefaultKind != DefaultKindKeywordBool {
			continue
		}
		if c.DefaultIsExpr {
			continue // an expression default keeps MySQL's own stored form
		}
		var value bool
		switch strings.ToUpper(*c.Default) {
		case "TRUE":
			value = true
		case "FALSE":
			value = false
		default:
			continue // the keyword has no other spelling
		}
		form := storedKeywordForm(c)
		if form == DefaultKindUnknown {
			continue // a type that converts the keyword to something else
		}
		stored := boolAsNumber(value)
		if form == DefaultKindBitLiteral {
			stored = boolAsBitLiteral(value)
		}
		c.Default = &stored
		c.DefaultKind = form
	}
	return ct
}

// storedKeywordForm reports the literal form MySQL stores a TRUE/FALSE keyword
// default as on this column's type, or DefaultKindUnknown where the type puts
// the keyword through a conversion of its own. See
// [booleanKeywordDefaultNormalizer] for the types that excludes and why.
func storedKeywordForm(c *Column) DefaultKind {
	if isIntegerColumnType(c.Type) {
		return DefaultKindNumber
	}
	switch strings.ToLower(c.Type) {
	case "double", "float":
		return DefaultKindNumber
	case "decimal":
		// Only an unscaled decimal, which has no scale to pad the value out to.
		if c.Scale == nil || *c.Scale == 0 {
			return DefaultKindNumber
		}
		return DefaultKindUnknown
	case "varchar", "char", "varbinary":
		return DefaultKindString
	case "bit":
		return DefaultKindBitLiteral
	}
	return DefaultKindUnknown
}

// boolAsNumber is the keyword's value as MySQL stores it on a type that holds
// it as a number, whether that is reported bare or quoted.
func boolAsNumber(value bool) string {
	if value {
		return "1"
	}
	return "0"
}

// boolAsBitLiteral is the keyword's value as MySQL stores it on a bit column,
// which reports a bit literal in its minimal form: bit(1) DEFAULT TRUE and
// bit(8) DEFAULT TRUE both come back as b'1'.
func boolAsBitLiteral(value bool) string {
	if value {
		return "b'1'"
	}
	return "b'0'"
}
