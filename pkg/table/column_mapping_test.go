package table

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestColumnMappingColumns(t *testing.T) {
	t1 := NewTableInfo(nil, "test", "t1")
	t1new := NewTableInfo(nil, "test", "t1_new")
	t1.NonGeneratedColumns = []string{"a", "b", "c"}
	t1new.NonGeneratedColumns = []string{"a", "b", "c"}
	m := NewColumnMapping(t1, t1new, nil)
	src, _ := m.Columns()
	require.Equal(t, "`a`, `b`, `c`", src)

	t1new.NonGeneratedColumns = []string{"a", "c"}
	m = NewColumnMapping(t1, t1new, nil)
	src, _ = m.Columns()
	require.Equal(t, "`a`, `c`", src)

	t1new.NonGeneratedColumns = []string{"a", "c", "d"}
	m = NewColumnMapping(t1, t1new, nil)
	src, _ = m.Columns()
	require.Equal(t, "`a`, `c`", src)
}

func TestColumnMappingColumnsSlice(t *testing.T) {
	t1 := NewTableInfo(nil, "test", "t1")
	t1new := NewTableInfo(nil, "test", "t1_new")
	t1.NonGeneratedColumns = []string{"a", "b", "c"}
	t1new.NonGeneratedColumns = []string{"a", "b", "c"}
	m := NewColumnMapping(t1, t1new, nil)
	cols, _ := m.ColumnsSlice()
	require.Equal(t, []string{"a", "b", "c"}, cols)

	t1new.NonGeneratedColumns = []string{"a", "c"}
	m = NewColumnMapping(t1, t1new, nil)
	cols, _ = m.ColumnsSlice()
	require.Equal(t, []string{"a", "c"}, cols)

	t1new.NonGeneratedColumns = []string{"a", "c", "d"}
	m = NewColumnMapping(t1, t1new, nil)
	cols, _ = m.ColumnsSlice()
	require.Equal(t, []string{"a", "c"}, cols)
}

func TestColumnMappingWithRenames(t *testing.T) {
	t1 := NewTableInfo(nil, "test", "t1")
	t1new := NewTableInfo(nil, "test", "t1_new")

	// Simple rename: a→x
	t1.NonGeneratedColumns = []string{"a", "b", "c"}
	t1new.NonGeneratedColumns = []string{"x", "b", "c"}
	renames := map[string]string{"a": "x"}

	m := NewColumnMapping(t1, t1new, renames)
	srcStr, tgtStr := m.Columns()
	require.Equal(t, "`a`, `b`, `c`", srcStr)
	require.Equal(t, "`x`, `b`, `c`", tgtStr)

	srcSlice, tgtSlice := m.ColumnsSlice()
	require.Equal(t, []string{"a", "b", "c"}, srcSlice)
	require.Equal(t, []string{"x", "b", "c"}, tgtSlice)

	// Multiple renames: a→x, c→z
	t1.NonGeneratedColumns = []string{"a", "b", "c"}
	t1new.NonGeneratedColumns = []string{"x", "b", "z"}
	renames = map[string]string{"a": "x", "c": "z"}

	m = NewColumnMapping(t1, t1new, renames)
	srcStr, tgtStr = m.Columns()
	require.Equal(t, "`a`, `b`, `c`", srcStr)
	require.Equal(t, "`x`, `b`, `z`", tgtStr)

	// No renames (nil map) - should behave like original
	t1.NonGeneratedColumns = []string{"a", "b", "c"}
	t1new.NonGeneratedColumns = []string{"a", "b", "c"}

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
	t1.NonGeneratedColumns = []string{"a", "b", "c"}
	t1new.NonGeneratedColumns = []string{"x", "b", "c", "d"}
	renames = map[string]string{"a": "x"}

	m = NewColumnMapping(t1, t1new, renames)
	srcSlice, tgtSlice = m.ColumnsSlice()
	require.Equal(t, []string{"a", "b", "c"}, srcSlice)
	require.Equal(t, []string{"x", "b", "c"}, tgtSlice)

	// Rename with column dropped from new table (c dropped)
	t1.NonGeneratedColumns = []string{"a", "b", "c"}
	t1new.NonGeneratedColumns = []string{"x", "b"}
	renames = map[string]string{"a": "x"}

	m = NewColumnMapping(t1, t1new, renames)
	srcSlice, tgtSlice = m.ColumnsSlice()
	require.Equal(t, []string{"a", "b"}, srcSlice)
	require.Equal(t, []string{"x", "b"}, tgtSlice)

	// Dangerous pattern: RENAME COLUMN c1 TO n1, ADD COLUMN c1 varchar(100)
	// The old name "c1" now exists in BOTH the source table AND the target table
	// (as a new column). The rename must take priority: source c1 → target n1.
	// The new c1 in the target must NOT get matched to the old c1 in the source.
	t1.NonGeneratedColumns = []string{"id", "c1"}
	t1new.NonGeneratedColumns = []string{"id", "n1", "c1"} // n1 is renamed from c1; c1 is brand new
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
	t1.NonGeneratedColumns = []string{"id", "a", "c"}
	t1new.NonGeneratedColumns = []string{"id", "c"} // c is renamed from a; old c is dropped
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
	t1.NonGeneratedColumns = []string{"id", "foo"}
	t1new.NonGeneratedColumns = []string{"id", "bar"}
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
	t1.NonGeneratedColumns = []string{"id", "foo"}
	t1new.NonGeneratedColumns = []string{"id", "FOO"}
	m = NewColumnMapping(t1, t1new, nil)
	srcCols, tgtCols = m.ColumnsSlice()
	require.Equal(t, []string{"id", "foo"}, srcCols)
	require.Equal(t, []string{"id", "FOO"}, tgtCols)

	// Claimed-target exclusion is case-insensitive: source [id, a, c],
	// target [id, C] where A→c is the rename. source.c must NOT identity
	// match target.C because it is claimed by the rename from source.a.
	t1.NonGeneratedColumns = []string{"id", "a", "c"}
	t1new.NonGeneratedColumns = []string{"id", "C"}
	m = NewColumnMapping(t1, t1new, map[string]string{"A": "c"})
	srcCols, tgtCols = m.ColumnsSlice()
	require.Equal(t, []string{"id", "a"}, srcCols)
	require.Equal(t, []string{"id", "C"}, tgtCols)
}

func TestColumnMappingTargetNil(t *testing.T) {
	// When target is nil, source is used as target
	t1 := NewTableInfo(nil, "test", "t1")
	t1.NonGeneratedColumns = []string{"a", "b", "c"}

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
	t1.NonGeneratedColumns = []string{"id", "j"}
	t1new.NonGeneratedColumns = []string{"id", "j"}
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
	t1.NonGeneratedColumns = []string{"id", "old_j"}
	m = NewColumnMapping(t1, t1new, map[string]string{"old_j": "j"})
	src, tgt, err = m.ChecksumExprs()
	require.NoError(t, err)
	require.Contains(t, src, "CAST(CAST(`old_j` AS char CHARACTER SET utf8mb4) AS json)")
	require.Contains(t, tgt, "CAST(`j` AS json)")
}

func TestColumnMappingRepairExprs(t *testing.T) {
	// The repair (REPLACE INTO ... SELECT) must store the text image of JSON
	// documents, not the source bytes — so JSON columns are wrapped in the
	// text round-trip while all other columns copy byte-faithfully.
	t1 := NewTableInfo(nil, "test", "t1")
	t1new := NewTableInfo(nil, "test", "t1_new")
	t1.NonGeneratedColumns = []string{"id", "name", "j"}
	t1new.NonGeneratedColumns = []string{"id", "name", "j"}
	t1new.columnsMySQLTps = map[string]string{"id": "int", "name": "varchar(100)", "j": "json"}

	m := NewColumnMapping(t1, t1new, nil)
	srcExprs, tgtCols, err := m.RepairExprs()
	require.NoError(t, err)
	require.Equal(t, "`id`, `name`, CAST(CAST(`j` AS char CHARACTER SET utf8mb4) AS json)", srcExprs)
	require.Equal(t, "`id`, `name`, `j`", tgtCols)

	// Renames: the SELECT references the old source name, the INSERT list
	// the new target name; the JSON decision comes from the target type.
	t1.NonGeneratedColumns = []string{"id", "old_j"}
	t1new.NonGeneratedColumns = []string{"id", "j"}
	m = NewColumnMapping(t1, t1new, map[string]string{"old_j": "j"})
	srcExprs, tgtCols, err = m.RepairExprs()
	require.NoError(t, err)
	require.Equal(t, "`id`, CAST(CAST(`old_j` AS char CHARACTER SET utf8mb4) AS json)", srcExprs)
	require.Equal(t, "`id`, `j`", tgtCols)

	// A target column with no known type is an error (mirrors wrapCastType).
	t1new.columnsMySQLTps = map[string]string{"id": "int"}
	_, _, err = m.RepairExprs()
	require.ErrorContains(t, err, "not found")
}
