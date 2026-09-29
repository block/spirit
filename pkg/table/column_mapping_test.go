package table

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestColumnMappingColumns(t *testing.T) {
	t1 := NewTableInfo(nil, "test", "t1")
	t1new := NewTableInfo(nil, "test", "t1_new")
	setColumns(t1, "a", "b", "c")
	setColumns(t1new, "a", "b", "c")
	m := NewColumnMapping(t1, t1new, nil)
	src, _ := m.Columns()
	require.Equal(t, "`a`, `b`, `c`", src)

	setColumns(t1new, "a", "c")
	m = NewColumnMapping(t1, t1new, nil)
	src, _ = m.Columns()
	require.Equal(t, "`a`, `c`", src)

	setColumns(t1new, "a", "c", "d")
	m = NewColumnMapping(t1, t1new, nil)
	src, _ = m.Columns()
	require.Equal(t, "`a`, `c`", src)
}

func TestColumnMappingColumnsSlice(t *testing.T) {
	t1 := NewTableInfo(nil, "test", "t1")
	t1new := NewTableInfo(nil, "test", "t1_new")
	setColumns(t1, "a", "b", "c")
	setColumns(t1new, "a", "b", "c")
	m := NewColumnMapping(t1, t1new, nil)
	cols, _ := m.ColumnsSlice()
	require.Equal(t, []string{"a", "b", "c"}, cols)

	setColumns(t1new, "a", "c")
	m = NewColumnMapping(t1, t1new, nil)
	cols, _ = m.ColumnsSlice()
	require.Equal(t, []string{"a", "c"}, cols)

	setColumns(t1new, "a", "c", "d")
	m = NewColumnMapping(t1, t1new, nil)
	cols, _ = m.ColumnsSlice()
	require.Equal(t, []string{"a", "c"}, cols)
}

func TestColumnMappingWithRenames(t *testing.T) {
	t1 := NewTableInfo(nil, "test", "t1")
	t1new := NewTableInfo(nil, "test", "t1_new")

	// Simple rename: a→x
	setColumns(t1, "a", "b", "c")
	setColumns(t1new, "x", "b", "c")
	renames := map[string]string{"a": "x"}

	m := NewColumnMapping(t1, t1new, renames)
	srcStr, tgtStr := m.Columns()
	require.Equal(t, "`a`, `b`, `c`", srcStr)
	require.Equal(t, "`x`, `b`, `c`", tgtStr)

	srcSlice, tgtSlice := m.ColumnsSlice()
	require.Equal(t, []string{"a", "b", "c"}, srcSlice)
	require.Equal(t, []string{"x", "b", "c"}, tgtSlice)

	// Multiple renames: a→x, c→z
	setColumns(t1, "a", "b", "c")
	setColumns(t1new, "x", "b", "z")
	renames = map[string]string{"a": "x", "c": "z"}

	m = NewColumnMapping(t1, t1new, renames)
	srcStr, tgtStr = m.Columns()
	require.Equal(t, "`a`, `b`, `c`", srcStr)
	require.Equal(t, "`x`, `b`, `z`", tgtStr)

	// No renames (nil map) - should behave like original
	setColumns(t1, "a", "b", "c")
	setColumns(t1new, "a", "b", "c")

	m = NewColumnMapping(t1, t1new, nil)
	srcStr, tgtStr = m.Columns()
	require.Equal(t, "`a`, `b`, `c`", srcStr)
	require.Equal(t, "`a`, `b`, `c`", tgtStr)

	// Empty renames map - should behave like original
	m = NewColumnMapping(t1, t1new, map[string]string{})
	srcStr, tgtStr = m.Columns()
	require.Equal(t, "`a`, `b`, `c`", srcStr)
	require.Equal(t, "`a`, `b`, `c`", tgtStr)

	// Rename with column added in new table (d is new, not in source)
	setColumns(t1, "a", "b", "c")
	setColumns(t1new, "x", "b", "c", "d")
	renames = map[string]string{"a": "x"}

	m = NewColumnMapping(t1, t1new, renames)
	srcSlice, tgtSlice = m.ColumnsSlice()
	require.Equal(t, []string{"a", "b", "c"}, srcSlice)
	require.Equal(t, []string{"x", "b", "c"}, tgtSlice)

	// Rename with column dropped from new table (c dropped)
	setColumns(t1, "a", "b", "c")
	setColumns(t1new, "x", "b")
	renames = map[string]string{"a": "x"}

	m = NewColumnMapping(t1, t1new, renames)
	srcSlice, tgtSlice = m.ColumnsSlice()
	require.Equal(t, []string{"a", "b"}, srcSlice)
	require.Equal(t, []string{"x", "b"}, tgtSlice)

	// Dangerous pattern: RENAME COLUMN c1 TO n1, ADD COLUMN c1 varchar(100)
	// The old name "c1" now exists in BOTH the source table AND the target table
	// (as a new column). The rename must take priority: source c1 → target n1.
	// The new c1 in the target must NOT get matched to the old c1 in the source.
	setColumns(t1, "id", "c1")
	setColumns(t1new, "id", "n1", "c1") // n1 is renamed from c1; c1 is brand new
	renames = map[string]string{"c1": "n1"}

	m = NewColumnMapping(t1, t1new, renames)
	srcSlice, tgtSlice = m.ColumnsSlice()
	// Source c1 must map to target n1 (via rename), NOT to target c1 (identity match).
	// The new target c1 has no source counterpart — it should get its DEFAULT value.
	require.Equal(t, []string{"id", "c1"}, srcSlice)
	require.Equal(t, []string{"id", "n1"}, tgtSlice)

	// Reverse dangerous pattern: RENAME COLUMN a TO c (where c already existed)
	// Source: [id, a, c], Target: [id, c] where a→c is the rename
	// source.a → target.c (rename). source.c must NOT identity-match target.c
	// because target.c is already claimed by the rename from source.a.
	setColumns(t1, "id", "a", "c")
	setColumns(t1new, "id", "c") // c is renamed from a; old c is dropped
	renames = map[string]string{"a": "c"}

	m = NewColumnMapping(t1, t1new, renames)
	srcSlice, tgtSlice = m.ColumnsSlice()
	// source.a → target.c (rename), source.c is excluded (target.c is claimed)
	require.Equal(t, []string{"id", "a"}, srcSlice)
	require.Equal(t, []string{"id", "c"}, tgtSlice)
}

func TestColumnMappingCaseInsensitive(t *testing.T) {
	t1 := NewTableInfo(nil, "test", "t1")
	t1new := NewTableInfo(nil, "test", "t1_new")

	// Rename key typed with different case than the declared column:
	// table declares "foo", user typed "RENAME COLUMN Foo TO bar".
	// MySQL identifiers are case-insensitive, so the rename must still apply.
	setColumns(t1, "id", "foo")
	setColumns(t1new, "id", "bar")
	m := NewColumnMapping(t1, t1new, map[string]string{"Foo": "bar"})
	srcCols, tgtCols := m.ColumnsSlice()
	require.Equal(t, []string{"id", "foo"}, srcCols)
	require.Equal(t, []string{"id", "bar"}, tgtCols)

	// Rename value typed with different case than the target declares:
	// the mapping must emit the declared target name so downstream exact-name
	// lookups (e.g. column type maps) succeed.
	m = NewColumnMapping(t1, t1new, map[string]string{"foo": "BAR"})
	srcCols, tgtCols = m.ColumnsSlice()
	require.Equal(t, []string{"id", "foo"}, srcCols)
	require.Equal(t, []string{"id", "bar"}, tgtCols)

	// Identity match with a case difference between source and target
	// declarations (e.g. a case-only CHANGE COLUMN foo FOO ...).
	setColumns(t1, "id", "foo")
	setColumns(t1new, "id", "FOO")
	m = NewColumnMapping(t1, t1new, nil)
	srcCols, tgtCols = m.ColumnsSlice()
	require.Equal(t, []string{"id", "foo"}, srcCols)
	require.Equal(t, []string{"id", "FOO"}, tgtCols)

	// Claimed-target exclusion is case-insensitive: source [id, a, c],
	// target [id, C] where A→c is the rename. source.c must NOT identity
	// match target.C because it is claimed by the rename from source.a.
	setColumns(t1, "id", "a", "c")
	setColumns(t1new, "id", "C")
	m = NewColumnMapping(t1, t1new, map[string]string{"A": "c"})
	srcCols, tgtCols = m.ColumnsSlice()
	require.Equal(t, []string{"id", "a"}, srcCols)
	require.Equal(t, []string{"id", "C"}, tgtCols)
}

func TestColumnMappingTargetNil(t *testing.T) {
	// When target is nil, source is used as target
	t1 := NewTableInfo(nil, "test", "t1")
	setColumns(t1, "a", "b", "c")

	m := NewColumnMapping(t1, nil, nil)
	src, tgt := m.Columns()
	require.Equal(t, "`a`, `b`, `c`", src)
	require.Equal(t, "`a`, `b`, `c`", tgt)
	require.Equal(t, t1, m.TargetTable())
}

func TestColumnMappingChecksumExprsJSONAsymmetric(t *testing.T) {
	// JSON columns are checksummed asymmetrically (see castExpr): the source
	// expression predicts the one-text-round-trip image the copier/applier
	// writes, while the target expression renders the stored document
	// strictly — so the two sides differ even without renames.
	t1 := NewTableInfo(nil, "test", "t1")
	t1new := NewTableInfo(nil, "test", "t1_new")
	setColumns(t1, "id", "j")
	t1.columnsMySQLTps = map[string]string{"id": "int", "j": "json", "old_j": "json"}
	setColumns(t1new, "id", "j")
	t1new.columnsMySQLTps = map[string]string{"id": "int", "j": "json"}

	m := NewColumnMapping(t1, t1new, nil)
	src, tgt, err := m.ChecksumExprs()
	require.NoError(t, err)
	require.Contains(t, src, "CAST(CAST(`j` AS char CHARACTER SET utf8mb4) AS json)")
	require.Contains(t, tgt, "CAST(`j` AS json)")
	require.NotContains(t, tgt, "CAST(CAST(`j`")
	// Non-JSON columns cast identically on both sides.
	require.Contains(t, src, "CAST(`id` AS signed)")
	require.Contains(t, tgt, "CAST(`id` AS signed)")

	// With a rename the source expression references the old name but the
	// asymmetry is unchanged.
	setColumns(t1, "id", "old_j")
	m = NewColumnMapping(t1, t1new, map[string]string{"old_j": "j"})
	src, tgt, err = m.ChecksumExprs()
	require.NoError(t, err)
	require.Contains(t, src, "CAST(CAST(`old_j` AS char CHARACTER SET utf8mb4) AS json)")
	require.Contains(t, tgt, "CAST(`j` AS json)")
}

func TestColumnMappingChecksumExprsTemporalPrecision(t *testing.T) {
	// DATETIME/TIMESTAMP are cast to the wider of the source and target
	// fractional-second precisions (see checksumCastTp), and both sides use
	// the same cast. The source's precision is looked up under the source's
	// own column name, so it also applies across a rename.
	t1 := NewTableInfo(nil, "test", "t1")
	t1new := NewTableInfo(nil, "test", "t1_new")
	setColumns(t1, "id", "narrowed", "old_widened")
	t1.columnsMySQLTps = map[string]string{"id": "int", "narrowed": "timestamp(6)", "old_widened": "datetime"}
	setColumns(t1new, "id", "narrowed", "widened")
	t1new.columnsMySQLTps = map[string]string{"id": "int", "narrowed": "timestamp", "widened": "datetime(3)"}

	m := NewColumnMapping(t1, t1new, map[string]string{"old_widened": "widened"})
	src, tgt, err := m.ChecksumExprs()
	require.NoError(t, err)
	require.Contains(t, src, "CAST(`narrowed` AS datetime(6))")
	require.Contains(t, tgt, "CAST(`narrowed` AS datetime(6))")
	require.Contains(t, src, "CAST(`old_widened` AS datetime(3))")
	require.Contains(t, tgt, "CAST(`widened` AS datetime(3))")

	// A source column missing from the source's type map is an error, not a
	// silent fallback to the target's precision.
	delete(t1.columnsMySQLTps, "narrowed")
	_, _, err = m.ChecksumExprs()
	require.Error(t, err)
}

// setColumns declares cols as the table's columns, none of them generated.
func setColumns(ti *TableInfo, cols ...string) {
	ti.Columns = cols
	ti.NonGeneratedColumns = cols
}

// genTable builds a TableInfo whose columns are all INT; the names listed in
// generated are generated columns.
func genTable(t *testing.T, name string, cols []string, generated ...string) *TableInfo {
	t.Helper()
	meta := make([]ColumnMeta, len(cols))
	for i, col := range cols {
		meta[i] = ColumnMeta{Name: col, MySQLType: "int", Generated: slices.Contains(generated, col)}
	}
	ti, err := NewTableInfoFromMeta("test", name, meta, []string{"id"})
	require.NoError(t, err)
	return ti
}

func TestColumnMappingGeneratedColumns(t *testing.T) {
	cols := []string{"id", "a", "g", "r"}

	// Generated on both sides (unchanged, or with a changed expression): the
	// target computes g itself, so it is neither read nor written.
	m := NewColumnMapping(genTable(t, "t1", cols, "g"), genTable(t, "t1_new", cols, "g"), nil)
	src, tgt := m.ColumnsSlice()
	require.Equal(t, []string{"id", "a", "r"}, src)
	require.Equal(t, []string{"id", "a", "r"}, tgt)
	require.Equal(t, []int{0, 1, 3}, m.SourceOrdinalIndices())

	// Generated on the source, regular on the target (MODIFY g INT): MySQL
	// keeps the values, so g must be copied, replayed and checksummed. Its
	// ordinal indexes the binlog row image, which carries every column.
	m = NewColumnMapping(genTable(t, "t1", cols, "g"), genTable(t, "t1_new", cols), nil)
	src, tgt = m.ColumnsSlice()
	require.Equal(t, []string{"id", "a", "g", "r"}, src)
	require.Equal(t, []string{"id", "a", "g", "r"}, tgt)
	require.Equal(t, []int{0, 1, 2, 3}, m.SourceOrdinalIndices())
	srcExpr, tgtExpr, err := m.ChecksumExprs()
	require.NoError(t, err)
	require.Contains(t, srcExpr, "ISNULL(`g`)")
	require.Contains(t, tgtExpr, "ISNULL(`g`)")

	// Regular on the source, generated on the target (MODIFY r INT AS (...)
	// STORED): r can't be written, so it is excluded.
	m = NewColumnMapping(genTable(t, "t1", cols), genTable(t, "t1_new", cols, "r"), nil)
	src, tgt = m.ColumnsSlice()
	require.Equal(t, []string{"id", "a", "g"}, src)
	require.Equal(t, []string{"id", "a", "g"}, tgt)

	// Generated on the source, renamed to a regular column on the target
	// (CHANGE g g2 INT): the rename is honoured.
	m = NewColumnMapping(genTable(t, "t1", cols, "g"), genTable(t, "t1_new", []string{"id", "a", "g2", "r"}), map[string]string{"g": "g2"})
	src, tgt = m.ColumnsSlice()
	require.Equal(t, []string{"id", "a", "g", "r"}, src)
	require.Equal(t, []string{"id", "a", "g2", "r"}, tgt)
	require.Equal(t, []int{0, 1, 2, 3}, m.SourceOrdinalIndices())

	// A generated source column dropped from the target is not mapped.
	m = NewColumnMapping(genTable(t, "t1", cols, "g"), genTable(t, "t1_new", []string{"id", "a", "r"}), nil)
	src, _ = m.ColumnsSlice()
	require.Equal(t, []string{"id", "a", "r"}, src)
	require.Equal(t, []int{0, 1, 3}, m.SourceOrdinalIndices())
}
