package statement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVectorDefaultDimension(t *testing.T) {
	tests := []struct {
		sql     string
		wantLen *int
	}{
		{"CREATE TABLE t (v vector)", new(2048)}, // MySQL stores vector(2048)
		{"CREATE TABLE t (v VECTOR)", new(2048)}, // case-insensitive
		{"CREATE TABLE t (v vector(3))", new(3)}, // explicit dimension kept
		{"CREATE TABLE t (v vector(2048))", new(2048)},
		{"CREATE TABLE t (v varchar(10))", new(10)}, // not a vector
	}
	for _, tc := range tests {
		t.Run(tc.sql, func(t *testing.T) {
			ct, err := ParseCreateTable(tc.sql)
			require.NoError(t, err)
			require.Len(t, ct.Columns, 1)
			assert.Equal(t, tc.wantLen, ct.Columns[0].Length)
		})
	}
}

// TestVectorDefaultDimensionConverges is the payoff: `VECTOR` authored in a
// schema file must not diff against the live `vector(2048)` MySQL reports. The
// generated statement would be a no-op MODIFY, so the diff would never
// converge — it would be re-emitted on every run.
func TestVectorDefaultDimensionConverges(t *testing.T) {
	authored, err := ParseCreateTable("CREATE TABLE t (id int NOT NULL, v vector NOT NULL, PRIMARY KEY (id))")
	require.NoError(t, err)
	live, err := ParseCreateTable("CREATE TABLE t (id int NOT NULL, v vector(2048) NOT NULL, PRIMARY KEY (id))")
	require.NoError(t, err)

	stmts, err := live.Diff(authored, nil)
	require.NoError(t, err)
	assert.Nil(t, stmts, "vector should normalize to vector(2048) and produce no diff")
}

// TestVectorDiffOmitsCharset guards the other half of the VECTOR schema
// handling, which charsetlessTypeNormalizer owns: the parser assigns VECTOR a
// synthetic "binary" charset/collation (as it does for spatial types), which
// is not valid SQL to emit. Emitting it produced `MODIFY COLUMN v vector(6)
// CHARACTER SET binary COLLATE binary`, which MySQL — and Spirit's own parser,
// when it re-parses the generated statement — rejects. See
// normalize_charsetless_types_test.go for the hand-written COLLATE route.
func TestVectorDiffOmitsCharset(t *testing.T) {
	ct, err := ParseCreateTable("CREATE TABLE t (id int NOT NULL, v vector(3) NOT NULL, PRIMARY KEY (id))")
	require.NoError(t, err)
	require.Len(t, ct.Columns, 2)
	assert.Nil(t, ct.Columns[1].Charset, "VECTOR must not carry the parser's synthetic binary charset")
	assert.Nil(t, ct.Columns[1].Collation, "VECTOR must not carry the parser's synthetic binary collation")

	live, err := ParseCreateTable("CREATE TABLE t (id int NOT NULL, v vector(3) NOT NULL, PRIMARY KEY (id))")
	require.NoError(t, err)
	desired, err := ParseCreateTable("CREATE TABLE t (id int NOT NULL, v vector(6) NOT NULL, w vector NULL, PRIMARY KEY (id))")
	require.NoError(t, err)

	stmts, err := live.Diff(desired, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Equal(t,
		"ALTER TABLE `t` MODIFY COLUMN `v` vector(6) NOT NULL, ADD COLUMN `w` vector(2048) NULL",
		stmts[0].Statement)
}
