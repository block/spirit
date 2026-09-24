package statement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCharsetlessTypesStripped covers both routes a charset/collation can
// reach a type that cannot carry one: the parser's own synthetic "binary"
// charset, and one the author wrote by hand. MySQL 9.7 accepts
// `VECTOR(3) COLLATE binary` (and the spatial equivalent), silently drops it,
// and reports the column back bare.
func TestCharsetlessTypesStripped(t *testing.T) {
	for _, sql := range []string{
		"CREATE TABLE t (v vector(3))",
		"CREATE TABLE t (v vector(3) COLLATE binary)",
		"CREATE TABLE t (v geometry)",
		"CREATE TABLE t (v geometry COLLATE binary)",
		"CREATE TABLE t (v point)",
		"CREATE TABLE t (v multipolygon COLLATE binary)",
	} {
		t.Run(sql, func(t *testing.T) {
			ct, err := ParseCreateTable(sql)
			require.NoError(t, err)
			require.Len(t, ct.Columns, 1)
			assert.Nil(t, ct.Columns[0].Charset, "charset must be stripped")
			assert.Nil(t, ct.Columns[0].Collation, "collation must be stripped")
		})
	}
}

// TestCharsetlessTypesLeaveOthersAlone guards the obvious over-reach: a real
// character type must keep what it declared.
func TestCharsetlessTypesLeaveOthersAlone(t *testing.T) {
	ct, err := ParseCreateTable("CREATE TABLE t (c varchar(10) CHARACTER SET latin1 COLLATE latin1_bin)")
	require.NoError(t, err)
	require.Len(t, ct.Columns, 1)
	require.NotNil(t, ct.Columns[0].Charset)
	assert.Equal(t, "latin1", *ct.Columns[0].Charset)
	require.NotNil(t, ct.Columns[0].Collation)
	assert.Equal(t, "latin1_bin", *ct.Columns[0].Collation)
}

// TestCharsetlessTypesConverge is the payoff. Without the rule the authored
// COLLATE survives into the diff and emits `MODIFY COLUMN ... COLLATE binary`;
// MySQL applies it, drops it, and still reports the column bare — so the same
// statement is emitted on the next run, and every run after that, each one
// paying for a full table copy.
func TestCharsetlessTypesConverge(t *testing.T) {
	authored, err := ParseCreateTable(
		"CREATE TABLE t (id int NOT NULL, v vector(3) COLLATE binary, g geometry COLLATE binary, PRIMARY KEY (id))")
	require.NoError(t, err)
	// What MySQL reports back after accepting the statement above.
	live, err := ParseCreateTable(
		"CREATE TABLE t (id int NOT NULL, v vector(3), g geometry, PRIMARY KEY (id))")
	require.NoError(t, err)

	stmts, err := live.Diff(authored, nil)
	require.NoError(t, err)
	assert.Nil(t, stmts, "a hand-written COLLATE on a charsetless type must not produce a diff")
}
