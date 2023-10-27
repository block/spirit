package table

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
	assert.Equal(t, 3*time.Second, lazyFindP90(times))
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
		{"varchar(100)", "char"},
		{"text", "char"},
		{"mediumtext", "char"},
		{"longtext", "char"},
		{"tinyblob", "binary"},
		{"blob", "binary"},
		{"mediumblob", "binary"},
		{"longblob", "binary"},
		{"varbinary", "binary"},
		{"char(100)", "char"},
		{"binary(100)", "binary"},
		{"datetime", "datetime"},
		{"datetime(6)", "datetime"},
		{"year", "char"},
		{"float", "char"},
		{"double", "char"},
		{"json", "json"},
		{"int(11)", "signed"},
		{"int(11) unsigned", "unsigned"},
		{"int(11) zerofill", "signed"},
		{"int(11) unsigned zerofill", "unsigned"},
		{"enum('a', 'b', 'c')", "char"},
		{"set('a', 'b', 'c')", "char"},
		{"decimal(6,2)", "decimal(6,2)"},
	}
	for _, tp := range tps {
		assert.Equal(t, tp.expected, castableTp(tp.tp))
	}
}

func TestQuoteCols(t *testing.T) {
	cols := []string{"a", "b", "c"}
	assert.Equal(t, "`a`, `b`, `c`", QuoteColumns(cols))

	cols = []string{"a"}
	assert.Equal(t, "`a`", QuoteColumns(cols))
}

func TestExpandRowConstructorComparison(t *testing.T) {
	assert.Equal(t, "((`a` > 1)\n OR (`a` = 1 AND `b` >= 2))",
		expandRowConstructorComparison([]string{"a", "b"},
			OpGreaterEqual,
			[]Datum{newDatum(1, signedType), newDatum(2, signedType)}))

	assert.Equal(t, "((`a` > 1)\n OR (`a` = 1 AND `b` > 2))",
		expandRowConstructorComparison([]string{"a", "b"},
			OpGreaterThan,
			[]Datum{newDatum(1, signedType), newDatum(2, signedType)}))

	assert.Equal(t, "((`a` > \"PENDING\")\n OR (`a` = \"PENDING\" AND `b` > 2))",
		expandRowConstructorComparison([]string{"a", "b"},
			OpGreaterThan,
			[]Datum{newDatum("PENDING", binaryType), newDatum(2, signedType)}))

	assert.Equal(t, "((`id1` > 2)\n OR (`id1` = 2 AND `id2` > 2)\n OR (`id1` = 2 AND `id2` = 2 AND `id3` > 4)\n OR (`id1` = 2 AND `id2` = 2 AND `id3` = 4 AND `id4` >= 5))",
		expandRowConstructorComparison([]string{"id1", "id2", "id3", "id4"},
			OpGreaterEqual,
			[]Datum{newDatum(2, signedType), newDatum(2, signedType), newDatum(4, signedType), newDatum(5, signedType)}))

	assert.Equal(t, "((`id1` < 2)\n OR (`id1` = 2 AND `id2` < 2)\n OR (`id1` = 2 AND `id2` = 2 AND `id3` < 4)\n OR (`id1` = 2 AND `id2` = 2 AND `id3` = 4 AND `id4` <= 5))",
		expandRowConstructorComparison([]string{"id1", "id2", "id3", "id4"},
			OpLessEqual,
			[]Datum{newDatum(2, signedType), newDatum(2, signedType), newDatum(4, signedType), newDatum(5, signedType)}))

	assert.Equal(t, "((`id1` < 2)\n OR (`id1` = 2 AND `id2` < 2)\n OR (`id1` = 2 AND `id2` = 2 AND `id3` < 4)\n OR (`id1` = 2 AND `id2` = 2 AND `id3` = 4 AND `id4` < 5))",
		expandRowConstructorComparison([]string{"id1", "id2", "id3", "id4"},
			OpLessThan,
			[]Datum{newDatum(2, signedType), newDatum(2, signedType), newDatum(4, signedType), newDatum(5, signedType)}))

	assert.Equal(t, "((`id1` > 2)\n OR (`id1` = 2 AND `id2` > 2)\n OR (`id1` = 2 AND `id2` = 2 AND `id3` > 4)\n OR (`id1` = 2 AND `id2` = 2 AND `id3` = 4 AND `id4` > 5))",
		expandRowConstructorComparison([]string{"id1", "id2", "id3", "id4"},
			OpGreaterThan,
			[]Datum{newDatum(2, signedType), newDatum(2, signedType), newDatum(4, signedType), newDatum(5, signedType)}))
}
