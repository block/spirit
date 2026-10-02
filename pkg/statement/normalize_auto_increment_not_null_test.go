package statement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAutoIncrementNotNull: every form MySQL stores as `int NOT NULL
// AUTO_INCREMENT` (verified against MySQL 8.0.43) parses NOT NULL with no
// default, and a NULL written after the AUTO_INCREMENT keeps the column
// nullable, as MySQL does.
func TestAutoIncrementNotNull(t *testing.T) {
	tests := []struct {
		name       string
		definition string // the id column; the table adds x INT PRIMARY KEY, UNIQUE KEY k (id)
		nullable   bool
		hasDefault bool
	}{
		{"Bare", "id INT AUTO_INCREMENT", false, false},
		{"NotNull", "id INT NOT NULL AUTO_INCREMENT", false, false},
		{"NullBefore", "id INT NULL AUTO_INCREMENT", false, false},
		{"NullBeforeThenNotNull", "id INT NULL AUTO_INCREMENT NOT NULL", false, false},
		{"DefaultNull", "id INT AUTO_INCREMENT DEFAULT NULL", false, false},
		{"DefaultNullBefore", "id INT DEFAULT NULL AUTO_INCREMENT", false, false},
		// MySQL applies the attributes in order, so a NULL after the
		// AUTO_INCREMENT clears the NOT NULL it implied.
		{"NullAfter", "id INT AUTO_INCREMENT NULL", true, false},
		{"NullAfterDefaultNull", "id INT AUTO_INCREMENT NULL DEFAULT NULL", true, false},
		{"DefaultNullThenNullAfter", "id INT AUTO_INCREMENT DEFAULT NULL NULL", true, false},
		{"NullAfterThenNotNull", "id INT AUTO_INCREMENT NULL NOT NULL", false, false},
		// A column without AUTO_INCREMENT keeps its nullability and default.
		{"NotAutoIncrement", "id INT DEFAULT NULL", true, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ct, err := ParseCreateTable("CREATE TABLE t (" + tc.definition + ", x INT PRIMARY KEY, UNIQUE KEY k (id))")
			require.NoError(t, err)
			col := ct.Columns.ByName("id")
			require.NotNil(t, col)
			assert.Equal(t, tc.nullable, col.Nullable, "nullable")
			assert.Equal(t, tc.hasDefault, col.Default != nil, "has default")
		})
	}
}

// TestAutoIncrementNotNullLeavesPrimaryKeyToItsRule: an explicit NULL on an
// AUTO_INCREMENT primary key column is a table MySQL refuses to create (error
// 1171) whatever the attribute order, so it stays nullable for
// checkPrimaryKeyNullability to report rather than being promoted here.
func TestAutoIncrementNotNullLeavesPrimaryKeyToItsRule(t *testing.T) {
	for _, definition := range []string{
		"id INT NULL AUTO_INCREMENT",
		"id INT AUTO_INCREMENT NULL",
		"id INT AUTO_INCREMENT DEFAULT NULL",
	} {
		t.Run(definition, func(t *testing.T) {
			ct, err := ParseCreateTable("CREATE TABLE t (" + definition + ", PRIMARY KEY (id))")
			require.NoError(t, err)
			assert.True(t, ct.Columns.ByName("id").Nullable)
			require.Error(t, checkPrimaryKeyNullability(ct))
		})
	}
	ct, err := ParseCreateTable("CREATE TABLE t (id INT AUTO_INCREMENT, PRIMARY KEY (id))")
	require.NoError(t, err)
	assert.False(t, ct.Columns.ByName("id").Nullable)
	require.NoError(t, checkPrimaryKeyNullability(ct))
}

// TestAutoIncrementNotNullConverges: the authored forms of a NOT NULL
// AUTO_INCREMENT column match the live table in both directions.
func TestAutoIncrementNotNullConverges(t *testing.T) {
	live := "CREATE TABLE `t` (`id` int NOT NULL AUTO_INCREMENT, `x` int NOT NULL, PRIMARY KEY (`x`), UNIQUE KEY `k` (`id`))"
	for _, authored := range []string{
		"CREATE TABLE t (id INT AUTO_INCREMENT, x INT PRIMARY KEY, UNIQUE KEY k (id))",
		"CREATE TABLE t (id INT NULL AUTO_INCREMENT, x INT PRIMARY KEY, UNIQUE KEY k (id))",
		"CREATE TABLE t (id INT AUTO_INCREMENT DEFAULT NULL, x INT PRIMARY KEY, UNIQUE KEY k (id))",
	} {
		t.Run(authored, func(t *testing.T) {
			src, err := ParseCreateTable(live)
			require.NoError(t, err)
			dst, err := ParseCreateTable(authored)
			require.NoError(t, err)
			stmts, err := src.Diff(dst, nil)
			require.NoError(t, err)
			assert.Empty(t, stmts, "live -> authored")
			stmts, err = dst.Diff(src, nil)
			require.NoError(t, err)
			assert.Empty(t, stmts, "authored -> live")
		})
	}
}
