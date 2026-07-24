package table

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFindP90(t *testing.T) {
	times := []time.Duration{
		1 * time.Second,
		2 * time.Second,
		1 * time.Second,
		3 * time.Second,
		10 * time.Second,
		1 * time.Second,
		1 * time.Second,
		1 * time.Second,
		1 * time.Second,
		1 * time.Second,
	}
	require.Equal(t, 3*time.Second, LazyFindP90(times))
}

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
		{"timestamp(6)", "datetime"},
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
		{"datetime(6)", "datetime"},
		{"year", "char CHARACTER SET utf8mb4"},
		{"float", "char"},
		{"double", "char"},
		{"json", "json"},
		{"int(11)", "signed"},
		{"int(11) unsigned", "unsigned"},
		{"int(11) zerofill", "signed"},
		{"int(11) unsigned zerofill", "unsigned"},
		{"enum('a', 'b', 'c')", "char CHARACTER SET utf8mb4"},
		{"set('a', 'b', 'c')", "char CHARACTER SET utf8mb4"},
		{"decimal(6,2)", "decimal(6,2)"},
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
	// Everything else is a single CAST to castableTp on both sides.
	require.Equal(t, "CAST(CAST(`j` AS char CHARACTER SET utf8mb4) AS json)", castExpr("j", "json", castSource))
	require.Equal(t, "CAST(`j` AS json)", castExpr("j", "json", castTarget))
	require.Equal(t, "CAST(`id` AS signed)", castExpr("id", "int(11)", castSource))
	require.Equal(t, "CAST(`id` AS signed)", castExpr("id", "int(11)", castTarget))
	require.Equal(t, "CAST(`name` AS char CHARACTER SET utf8mb4)", castExpr("name", "varchar(100)", castSource))
	require.Equal(t, "CAST(`b` AS binary(16))", castExpr("b", "binary(16)", castTarget))
	require.Equal(t, "CAST(`d` AS decimal(6,2))", castExpr("d", "decimal(6,2)", castSource))
}

func TestQuoteCols(t *testing.T) {
	cols := []string{"a", "b", "c"}
	require.Equal(t, "`a`, `b`, `c`", QuoteColumns(cols))

	cols = []string{"a"}
	require.Equal(t, "`a`", QuoteColumns(cols))

	// Identifiers containing a backtick must have it doubled, otherwise the
	// quoting breaks out and produces invalid SQL.
	require.Equal(t, "`a``b`", QuoteColumns([]string{"a`b"}))
	require.Equal(t, "`a``b`, `c`", QuoteColumns([]string{"a`b", "c"}))
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
