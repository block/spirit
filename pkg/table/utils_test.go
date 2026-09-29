package table

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type castableTpTest struct {
	tp       string
	expected string
}

func TestCastableTp(t *testing.T) {
	tps := []castableTpTest{
		{"tinyint", "signed"},
		{"smallint", "signed"},
		{"mediumint", "signed"},
		{"int", "signed"},
		{"bigint", "signed"},
		{"tinyint unsigned", "unsigned"},
		{"smallint unsigned", "unsigned"},
		{"mediumint unsigned", "unsigned"},
		{"int unsigned", "unsigned"},
		{"bigint unsigned", "unsigned"},
		{"timestamp", "datetime"},
		// DATETIME/TIMESTAMP keep their fractional-second precision:
		// CAST(… AS datetime) rounds to the second, hiding sub-second
		// divergence (see checksumCastTp for the source/target widening).
		{"timestamp(6)", "datetime(6)"},
		{"timestamp(3)", "datetime(3)"},
		{"varchar(100)", "char CHARACTER SET utf8mb4"},
		{"text", "char CHARACTER SET utf8mb4"},
		{"mediumtext", "char CHARACTER SET utf8mb4"},
		{"longtext", "char CHARACTER SET utf8mb4"},
		{"tinyblob", "binary"},
		{"blob", "binary"},
		{"mediumblob", "binary"},
		{"longblob", "binary"},
		{"varbinary", "binary"},
		{"varbinary(255)", "binary"}, // variable-length: no padding, width not needed
		{"char(100)", "char CHARACTER SET utf8mb4"},
		// binary(N) must preserve its width in the cast: binary(0) would
		// truncate every value to zero bytes (checksum blindness), and a
		// plain binary cast would not zero-pad, breaking widening migrations.
		{"binary(100)", "binary(100)"},
		{"binary(16)", "binary(16)"},
		{"binary(1)", "binary(1)"},
		{"datetime", "datetime"},
		{"datetime(6)", "datetime(6)"},
		{"datetime(1)", "datetime(1)"},
		{"year", "char CHARACTER SET utf8mb4"},
		// FLOAT and DOUBLE compare as their exact DOUBLE value: a char cast
		// renders a FLOAT with 6 significant digits.
		{"float", "double"},
		{"double", "double"},
		{"float unsigned", "double"},
		{"double unsigned", "double"},
		{"float(7,4) zerofill", "double"},
		{"json", "json"},
		{"int(11)", "signed"},
		{"int(11) unsigned", "unsigned"},
		{"int(11) zerofill", "signed"},
		{"int(11) unsigned zerofill", "unsigned"},
		{"enum('a', 'b', 'c')", "char CHARACTER SET utf8mb4"},
		{"set('a', 'b', 'c')", "char CHARACTER SET utf8mb4"},
		{"decimal(6,2)", "decimal(6,2)"},
		// CAST rejects decimal(M,D) UNSIGNED (1064), but the scale must be
		// kept so a scale widening does not compare 169.09 with 169.0900.
		// ZEROFILL implies UNSIGNED, so it is reported with both.
		{"decimal(12,4) unsigned", "decimal(12,4)"},
		{"decimal(12,4) unsigned zerofill", "decimal(12,4)"},
		// CAST(bit AS char) returns the raw bytes at the column's own width,
		// so BIT(8) -> BIT(16) would compare 0x01 with 0x0001.
		{"bit(1)", "unsigned"},
		{"bit(8)", "unsigned"},
		{"bit(64)", "unsigned"},
		// VECTOR (MySQL 9.7+) has no char cast at all — the server rejects
		// CAST(v AS char) with ER_WRONG_ARGUMENTS, which would fail the
		// checksum query for any table holding one. Binary is exact, and
		// unlike binary(N) it must not carry the width: the declared
		// dimension is a maximum, not a padded length.
		{"vector", "binary"},
		{"vector(3)", "binary"},
		{"vector(2048)", "binary"},
	}
	for _, tp := range tps {
		require.Equal(t, tp.expected, castableTp(tp.tp), "tp failed: %s, expected: %s", tp.tp, tp.expected)
	}
}

func TestCastExpr(t *testing.T) {
	// JSON casts are side-dependent (the "text-image contract", see castExpr):
	// the source side is normalized through a text round-trip — predicting
	// the text-degraded form the copier/applier writes — while the target
	// side renders the stored document strictly, so it is never re-parsed.
	// Everything else is a single CAST to the resolved type on both sides.
	require.Equal(t, "CAST(CAST(`j` AS char CHARACTER SET utf8mb4) AS json)", castExpr("j", "json", castSource))
	require.Equal(t, "CAST(`j` AS json)", castExpr("j", "json", castTarget))
	require.Equal(t, "CAST(`id` AS signed)", castExpr("id", "signed", castSource))
	require.Equal(t, "CAST(`id` AS signed)", castExpr("id", "signed", castTarget))
	require.Equal(t, "CAST(`name` AS char CHARACTER SET utf8mb4)", castExpr("name", "char CHARACTER SET utf8mb4", castSource))
	require.Equal(t, "CAST(`b` AS binary(16))", castExpr("b", "binary(16)", castTarget))
	require.Equal(t, "CAST(`d` AS decimal(6,2))", castExpr("d", "decimal(6,2)", castSource))
	require.Equal(t, "CAST(`ts` AS datetime(6))", castExpr("ts", "datetime(6)", castTarget))
	// DOUBLE is rendered as an addition, not a CAST, which only accepts
	// DOUBLE from MySQL 8.0.17.
	require.Equal(t, "(`f` + 0E0)", castExpr("f", "double", castSource))
	require.Equal(t, "(`f` + 0E0)", castExpr("f", "double", castTarget))
	// A narrowing to FLOAT rounds the source to FLOAT precision.
	require.Equal(t, roundToFloatExpr("(`d` + 0E0)"), castExpr("d", "float", castSource))
	require.Equal(t, "(`d` + 0E0)", castExpr("d", "float", castTarget))
}

func TestChecksumCastTp(t *testing.T) {
	for _, tc := range []struct {
		sourceTp, targetTp, expected string
	}{
		// DATETIME/TIMESTAMP cast to the wider of the two precisions.
		{"timestamp", "timestamp(6)", "datetime(6)"},    // widening
		{"timestamp(6)", "timestamp", "datetime(6)"},    // narrowing: must detect the lost fraction
		{"datetime(6)", "datetime(3)", "datetime(6)"},   // partial narrowing
		{"datetime(3)", "datetime(6)", "datetime(6)"},   // partial widening
		{"timestamp(6)", "timestamp(6)", "datetime(6)"}, // same precision
		{"timestamp", "timestamp", "datetime"},          // no precision on either side
		{"timestamp(6)", "datetime", "datetime(6)"},     // cross-type
		{"datetime(4)", "timestamp(2)", "datetime(4)"},  // cross-type
		// Only a temporal source widens a temporal target cast; any other
		// source keeps the target's own cast.
		{"varchar(100)", "datetime", "datetime"},
		{"varchar(100)", "datetime(3)", "datetime(3)"},
		{"date", "datetime", "datetime"},
		{"time(6)", "datetime", "datetime"},
		// A temporal source never changes a non-temporal target cast.
		{"datetime(6)", "varchar(100)", "char CHARACTER SET utf8mb4"},
		{"timestamp(6)", "date", "char CHARACTER SET utf8mb4"},
		// Everything else is castableTp of the target.
		{"int", "bigint", "signed"},
		{"decimal(10,2) unsigned", "decimal(12,4) unsigned", "decimal(12,4)"},
		{"bit(8)", "bit(16)", "unsigned"},
		{"binary(50)", "binary(100)", "binary(100)"},
		// A BIT target stores a string/binary source as its bytes and every
		// other source as its numeric value.
		{"varchar(8)", "bit(8)", "binary"},
		{"char(1)", "bit(8)", "binary"},
		{"text", "bit(8)", "binary"},
		{"varbinary(8)", "bit(8)", "binary"},
		{"binary(1)", "bit(8)", "binary"},
		{"blob", "bit(8)", "binary"},
		{"json", "bit(8)", "binary"},
		{"int", "bit(8)", "unsigned"},
		{"bigint unsigned", "bit(64)", "unsigned"},
		{"decimal(5,0)", "bit(8)", "unsigned"},
		{"float", "bit(8)", "unsigned"},
		{"double", "bit(8)", "unsigned"},
		{"year", "bit(16)", "unsigned"},
		{"enum('a','b')", "bit(8)", "unsigned"},
		{"set('a','b')", "bit(8)", "unsigned"},
		{"time", "bit(64)", "unsigned"},
		// A FLOAT target with any other source is a narrowing, compared at
		// FLOAT precision; FLOAT with FLOAT, or into DOUBLE, is exact.
		{"double", "float", "float"},
		{"varchar(20)", "float", "float"},
		{"decimal(10,4)", "float unsigned", "float"},
		{"bigint", "float(7,4)", "float"},
		{"float", "float", "double"},
		{"float unsigned", "float", "double"},
		{"float", "double", "double"},
		{"double", "double", "double"},
		// The exception is only for a BIT target.
		{"varchar(8)", "int", "signed"},
	} {
		require.Equal(t, tc.expected, checksumCastTp(tc.sourceTp, tc.targetTp), "source: %s, target: %s", tc.sourceTp, tc.targetTp)
	}
}

func TestExpandRowConstructorComparison(t *testing.T) {
	require.Equal(t, "((`a` > 1)\n OR (`a` = 1 AND `b` >= 2))",
		expandRowConstructorComparison([]string{"a", "b"},
			OpGreaterEqual,
			[]Datum{{Val: 1, Tp: signedType}, {Val: 2, Tp: signedType}}))

	require.Equal(t, "((`a` > 1)\n OR (`a` = 1 AND `b` > 2))",
		expandRowConstructorComparison([]string{"a", "b"},
			OpGreaterThan,
			[]Datum{{Val: 1, Tp: signedType}, {Val: 2, Tp: signedType}}))

	require.Equal(t, "((`a` > \"PENDING\")\n OR (`a` = \"PENDING\" AND `b` > 2))",
		expandRowConstructorComparison([]string{"a", "b"},
			OpGreaterThan,
			[]Datum{{Val: "PENDING", Tp: binaryType}, {Val: 2, Tp: signedType}}))

	require.Equal(t, "((`id1` > 2)\n OR (`id1` = 2 AND `id2` > 2)\n OR (`id1` = 2 AND `id2` = 2 AND `id3` > 4)\n OR (`id1` = 2 AND `id2` = 2 AND `id3` = 4 AND `id4` >= 5))",
		expandRowConstructorComparison([]string{"id1", "id2", "id3", "id4"},
			OpGreaterEqual,
			[]Datum{{Val: 2, Tp: signedType}, {Val: 2, Tp: signedType}, {Val: 4, Tp: signedType}, {Val: 5, Tp: signedType}}))

	require.Equal(t, "((`id1` < 2)\n OR (`id1` = 2 AND `id2` < 2)\n OR (`id1` = 2 AND `id2` = 2 AND `id3` < 4)\n OR (`id1` = 2 AND `id2` = 2 AND `id3` = 4 AND `id4` <= 5))",
		expandRowConstructorComparison([]string{"id1", "id2", "id3", "id4"},
			OpLessEqual,
			[]Datum{{Val: 2, Tp: signedType}, {Val: 2, Tp: signedType}, {Val: 4, Tp: signedType}, {Val: 5, Tp: signedType}}))

	require.Equal(t, "((`id1` < 2)\n OR (`id1` = 2 AND `id2` < 2)\n OR (`id1` = 2 AND `id2` = 2 AND `id3` < 4)\n OR (`id1` = 2 AND `id2` = 2 AND `id3` = 4 AND `id4` < 5))",
		expandRowConstructorComparison([]string{"id1", "id2", "id3", "id4"},
			OpLessThan,
			[]Datum{{Val: 2, Tp: signedType}, {Val: 2, Tp: signedType}, {Val: 4, Tp: signedType}, {Val: 5, Tp: signedType}}))

	require.Equal(t, "((`id1` > 2)\n OR (`id1` = 2 AND `id2` > 2)\n OR (`id1` = 2 AND `id2` = 2 AND `id3` > 4)\n OR (`id1` = 2 AND `id2` = 2 AND `id3` = 4 AND `id4` > 5))",
		expandRowConstructorComparison([]string{"id1", "id2", "id3", "id4"},
			OpGreaterThan,
			[]Datum{{Val: 2, Tp: signedType}, {Val: 2, Tp: signedType}, {Val: 4, Tp: signedType}, {Val: 5, Tp: signedType}}))
}

// TestWidthRegexpsAreCompiledOnce guards a cost that is invisible from the
// call site. removeWidth reads like a schema-time helper, but NewColumnType
// reaches it for every value of every row on the copy path, so compiling the
// pattern inside the function made type resolution ~14x the cost of building
// an INSERT statement — visible only as applier-build-p50 dominating
// applier-write-p50 on a wide table.
//
// The exact counts are not the contract; the order of magnitude is. Compiling
// a pattern per call costs ~40 extra allocations, so these bounds catch a
// regression without breaking on an unrelated allocation being added or saved.
func TestWidthRegexpsAreCompiledOnce(t *testing.T) {
	for _, tc := range []struct {
		name string
		fn   func()
	}{
		{"removeWidth", func() { _ = removeWidth("varchar(255)") }},
		{"removeDecimalWidth", func() { _ = removeDecimalWidth("decimal(20,6)") }},
		{"NewColumnType", func() { _ = NewColumnType("varchar(255)") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			allocs := testing.AllocsPerRun(200, tc.fn)
			require.Less(t, allocs, 15.0,
				"%s looks like it is compiling a regexp per call (was ~48 allocs before the patterns moved to package level)", tc.name)
		})
	}
}
