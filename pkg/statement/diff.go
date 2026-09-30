package statement

import (
	"strings"
)

// DiffOptions controls the behavior of the Diff operation.
type DiffOptions struct {
	// IgnoreAutoIncrement skips diffing the AUTO_INCREMENT table option
	// (the table-level next-value counter, e.g. `AUTO_INCREMENT=100`).
	// Default: true (via NewDiffOptions).
	IgnoreAutoIncrement bool

	// IgnoreColumnAutoIncrement skips diffing the column-level AUTO_INCREMENT
	// attribute (whether a column carries the AUTO_INCREMENT flag). This is
	// distinct from IgnoreAutoIncrement, which only covers the table-option
	// counter. Default: false (via NewDiffOptions) — for general schema diffing
	// a column gaining or losing AUTO_INCREMENT is a real change. It is enabled
	// by consumers like the move-tables target-state check, where an unsharded
	// source legitimately differs from a sharded target that drops
	// AUTO_INCREMENT in favor of a Vitess sequence: the difference does not
	// affect copy correctness and must not block the move.
	IgnoreColumnAutoIncrement bool

	// IgnoreNotNullRelaxation lets the schema being validated be STRICTER than
	// its reference on nullability, and only stricter: a validated column
	// declared NOT NULL where the reference permits NULL is accepted, while one
	// that permits NULL where the reference is NOT NULL remains a real
	// difference. The option therefore can never quietly accept a schema that
	// lost a NOT NULL the reference had.
	//
	// Default: false (via NewDiffOptions) — for general schema diffing a column
	// gaining or losing NOT NULL is a real change.
	//
	// It is enabled by the move-tables target checks (see
	// move/check.TargetSchemaDiff), where the reference is the move's SOURCE and
	// the validated schema is its physical TARGET. What that permits is a target
	// column declared NOT NULL where the source still permits NULL — an
	// unsharded source moving into a sharded target whose shard key must be
	// NOT NULL, because a Vitess primary vindex cannot map NULL to a keyspace
	// id.
	//
	// In terms of Diff's own arguments the reference is the parameter and the
	// validated schema is the receiver, because DiffCreateTables diffs
	// got->want. Stating the direction that way inverts it, which is why the
	// wording above and TestDiff_IgnoreNotNullRelaxation both name the two
	// schemas by role instead.
	//
	// That is safe for a move because nullability is metadata, not row bytes:
	// the copy and the checksum compare values, and every column's NULL-ness is
	// compared explicitly (see ColumnMapping.ChecksumExprs, which emits an
	// ISNULL() digit per column). A tightened column whose source data holds no
	// NULLs is therefore identical on both sides, and this option hides nothing
	// about the rows themselves. One that does hold a NULL fails the move
	// instead of being accepted, and fails before the checksum ever runs — see
	// move/check.TargetSchemaDiff for where and why.
	IgnoreNotNullRelaxation bool

	// IgnoreEngine skips diffing the ENGINE table option.
	// Default: true (via NewDiffOptions).
	IgnoreEngine bool

	// IgnoreCharsetCollation skips diffing CHARSET and COLLATION table options.
	// Default: false (via NewDiffOptions).
	IgnoreCharsetCollation bool

	// IgnorePartitioning skips diffing partition options entirely.
	// Default: false (via NewDiffOptions).
	IgnorePartitioning bool

	// IgnoreRowFormat skips diffing the ROW_FORMAT table option.
	// Default: true (via NewDiffOptions).
	// ROW_FORMAT=DYNAMIC is the InnoDB default in MySQL 8.0+, so differences
	// between an unspecified ROW_FORMAT and an explicit DYNAMIC are cosmetic.
	IgnoreRowFormat bool
}

// NewDiffOptions returns DiffOptions with sensible defaults.
// By default, AUTO_INCREMENT, ENGINE, and ROW_FORMAT differences are ignored.
func NewDiffOptions() *DiffOptions {
	return &DiffOptions{
		IgnoreAutoIncrement:       true,
		IgnoreColumnAutoIncrement: false,
		IgnoreNotNullRelaxation:   false,
		IgnoreEngine:              true,
		IgnoreCharsetCollation:    false,
		IgnorePartitioning:        false,
		IgnoreRowFormat:           true,
	}
}

// charsetCarryingTypes are the column types that store text and therefore
// carry a real charset/collation, inheriting the owning table's defaults when
// the column declares neither. Type names are the parser's canonical
// spellings. Binary and JSON types are excluded: they carry only a synthetic
// "binary" charset that is identical on both sides of a same-type compare,
// and spatial/vector types have theirs stripped by charsetlessTypeNormalizer.
var charsetCarryingTypes = map[string]bool{
	"char":       true,
	"varchar":    true,
	"tinytext":   true,
	"text":       true,
	"mediumtext": true,
	"longtext":   true,
	"enum":       true,
	"set":        true,
}

// charsetOfCollation returns the character set a collation name belongs to.
// MySQL collation names are the owning charset name followed by an
// underscore-separated suffix (utf8mb4_general_ci -> utf8mb4); the sole
// exception is "binary", which is both a charset and its only collation.
func charsetOfCollation(collation string) string {
	if idx := strings.IndexByte(collation, '_'); idx > 0 {
		return collation[:idx]
	}
	return collation
}

// resolvedCharsetCollation returns the charset and collation a column
// actually uses, following MySQL's resolution rules: explicit column values
// win; an explicit column collation implies its charset; a column with
// neither inherits the owning table's defaults, where a table collation
// likewise implies the table charset. A value that cannot be determined from
// the statement alone is returned as "": a column or table with a utf8mb4
// charset but no collation uses utf8mb4's default collation, and a table with
// neither option uses the server defaults — both depend on server version
// and configuration. Every other charset's default collation is fixed, and
// defaultCollationNormalizer has already filled it in.
func resolvedCharsetCollation(col *Column, table *CreateTable) (charset, collation string) {
	switch {
	case col.Collation != nil:
		collation = strings.ToLower(*col.Collation)
		if col.Charset != nil {
			charset = strings.ToLower(*col.Charset)
		} else {
			charset = charsetOfCollation(collation)
		}
	case col.Charset != nil:
		// An explicit column charset without a collation selects the
		// charset's default collation — not the table's collation.
		charset = strings.ToLower(*col.Charset)
	default:
		if tableCollation := table.TableOptions.getCollation(); tableCollation != nil {
			collation = strings.ToLower(*tableCollation)
		}
		if tableCharset := table.TableOptions.getCharset(); tableCharset != nil {
			charset = strings.ToLower(*tableCharset)
		} else if collation != "" {
			charset = charsetOfCollation(collation)
		}
	}
	return charset, collation
}

// explicitUnlessTableDefault returns a column-level charset/collation value
// with the redundant spelling of the owning table's default normalized to
// nil, so an explicit value that merely restates the table default compares
// equal to an inherited (nil) one.
func explicitUnlessTableDefault(value, tableDefault *string) *string {
	if value != nil && tableDefault != nil && *value == *tableDefault {
		return nil
	}
	return value
}

// comparedCollation returns the collation a column is compared under: the
// resolved one, or the written one when IgnoreCharsetCollation skips
// resolution.
func comparedCollation(col *Column, resolved string) string {
	if resolved == "" && col.Collation != nil {
		return strings.ToLower(*col.Collation)
	}
	return resolved
}

// charsetCollationEqual reports whether two columns have the same effective
// charset and collation given their owning tables' defaults. Equality is
// decided on the RESOLVED values, not the written ones: a column that
// inherits its table default and a column that matches a *different* default
// on the other table are genuinely different columns, and since a
// table-level DEFAULT CHARSET / COLLATE clause only affects columns added
// later, converging them requires a MODIFY COLUMN in the same ALTER as the
// table-option change.
//
// Each attribute is compared strictly when both sides resolve to a concrete
// value. When a side is underdetermined (see resolvedCharsetCollation), that
// attribute falls back to comparing the written values with redundant
// table-default spellings normalized away — an unexpressed preference is
// treated as a match rather than guessed at, which keeps the diff from
// emitting a MODIFY it could never prove converged. Two exceptions follow
// from default_collation_for_utf8mb4 accepting only two collations: a column
// that names utf8mb4 without a COLLATE matches either of them, whatever the
// table defaults are; and a column inheriting a DEFAULT CHARSET=utf8mb4
// declared without a COLLATE does not match one whose table names any other
// collation.
func charsetCollationEqual(a, b *Column, source, target *CreateTable, opts *DiffOptions) bool {
	if !charsetCarryingTypes[strings.ToLower(a.Type)] {
		// Non-character types have no table default to inherit, so compare
		// the written values directly.
		return ptrEqual(a.Charset, b.Charset) && ptrEqual(a.Collation, b.Collation)
	}

	// IgnoreCharsetCollation suppresses the table-option diff, so the table
	// defaults it ignores must not leak into the column comparison through
	// resolution either — a column inheriting a difference between the two
	// (ignored) table defaults is not a column change in this mode. Explicit
	// column-level differences are still compared by the written-value
	// comparisons below.
	sourceCharset, sourceCollation := "", ""
	targetCharset, targetCollation := "", ""
	if !opts.IgnoreCharsetCollation {
		sourceCharset, sourceCollation = resolvedCharsetCollation(a, source)
		targetCharset, targetCollation = resolvedCharsetCollation(b, target)
	}

	if sourceCharset != "" && targetCharset != "" {
		if sourceCharset != targetCharset {
			return false
		}
	} else if !ptrEqual(
		explicitUnlessTableDefault(a.Charset, source.TableOptions.getCharset()),
		explicitUnlessTableDefault(b.Charset, target.TableOptions.getCharset()),
	) {
		return false
	}

	if sourceCollation != "" && targetCollation != "" {
		return sourceCollation == targetCollation
	}
	// A column that names utf8mb4 without a COLLATE takes the server's
	// default_collation_for_utf8mb4, not the table's collation, so when the
	// other side's collation is known it matches exactly the collations that
	// variable can hold. The written-value comparison below cannot decide
	// this: the live form spells the server's choice out as a COLLATE, which
	// differs from the table default whenever the table uses another charset
	// or collation, so a MODIFY restating the bare charset would never
	// converge against it; and a live column that inherits a table default
	// the bare column can never take (utf8mb4_bin) writes no COLLATE at all,
	// so the two would compare equal. The charset was compared above, and
	// both server defaults are utf8mb4 collations.
	if takesServerUTF8MB4Default(a) {
		if other := comparedCollation(b, targetCollation); other != "" {
			return utf8mb4ServerDefaultCollations[other]
		}
	}
	if takesServerUTF8MB4Default(b) {
		if other := comparedCollation(a, sourceCollation); other != "" {
			return utf8mb4ServerDefaultCollations[other]
		}
	}
	// A column that inherits a DEFAULT CHARSET=utf8mb4 declared without a
	// COLLATE has its table's server default. When the other table names a
	// collation that default can never be (utf8mb4_bin, or another charset's),
	// the table-option diff changes the table's collation, and a table
	// default only applies to columns added later, so this column needs a
	// MODIFY in the same ALTER. The written values cannot show this: the live
	// form of an inheriting column writes no COLLATE. IgnoreCharsetCollation
	// suppresses that table-option diff, so the rule does not apply there.
	if !opts.IgnoreCharsetCollation {
		if inheritsServerUTF8MB4Default(a, source) && isNonServerUTF8MB4Collation(target) {
			return false
		}
		if inheritsServerUTF8MB4Default(b, target) && isNonServerUTF8MB4Collation(source) {
			return false
		}
	}
	return ptrEqual(
		explicitUnlessTableDefault(a.Collation, source.TableOptions.getCollation()),
		explicitUnlessTableDefault(b.Collation, target.TableOptions.getCollation()),
	)
}
