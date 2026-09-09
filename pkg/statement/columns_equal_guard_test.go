package statement

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// columnsEqualWithContext (in diff.go) field-by-field compares the parsed
// Column struct to decide whether a column differs between the source and
// target schema. If a newly added Column field is NOT wired into that
// comparison, two columns that differ only in that field are silently treated
// as EQUAL — a real ALTER gets dropped and Spirit applies the wrong schema
// diff. The tests below are the safety net for that class of mistake.

// columnFieldsCompared lists the exported Column fields that the equality logic
// (columnsEqualWithContext + the shared columnExtendedAttributesEqual helper)
// compares.
var columnFieldsCompared = map[string]struct{}{
	"Name":            {},
	"Type":            {},
	"Length":          {},
	"Precision":       {},
	"Scale":           {},
	"Unsigned":        {},
	"Zerofill":        {},
	"EnumValues":      {},
	"SetValues":       {},
	"Nullable":        {},
	"Default":         {},
	"DefaultIsExpr":   {},
	"DefaultKind":     {},
	"OnUpdate":        {},
	"GeneratedExpr":   {},
	"GeneratedStored": {},
	"SRID":            {},
	"AutoInc":         {},
	"PrimaryKey":      {},
	"Comment":         {},
	"Charset":         {},
	"Collation":       {},
}

// columnFieldsNotCompared lists exported Column fields that are deliberately
// excluded from the column equality logic, with the reason.
var columnFieldsNotCompared = map[string]string{
	// Raw is the underlying *ast.ColumnDef pointer; it is parser-internal state,
	// not part of the column's logical definition, so comparing it is meaningless.
	"Raw": "raw parser AST pointer, not part of logical column definition",
	// Column-level CHECK is hoisted into table-level CreateTable.Constraints by
	// the parser (columnCheckNormalizer) and diffed by diffConstraints instead —
	// see the comment on columnExtendedAttributesEqual. So it is intentionally not
	// part of per-column equality.
	"Check": "hoisted to table-level Constraints; diffed by diffConstraints, not here",
	// Options is a catch-all map for column options the parser did not model
	// explicitly. It is currently NOT compared by either columnsEqual function.
	// If you start populating Options with semantically meaningful data, it must
	// be added to both comparisons and removed from this list.
	"Options": "catch-all map for unmodeled options; currently not compared by diff.go",
	// Column-level UNIQUE is representation, not state: MySQL canonicalizes it
	// into a table-level UNIQUE KEY, and MODIFY COLUMN cannot express it. It is
	// materialized into a table-level index by indexNormalizer and diffed by
	// diffIndexes instead of being part of per-column equality.
	"Unique": "materialized into a table-level index by indexNormalizer; diffed by diffIndexes, not here",
}

// TestColumnsEqualAllFieldsAccounted is the primary tripwire: it enumerates the
// exported fields of Column via reflection and asserts that EVERY one is
// accounted for — either in columnFieldsCompared or in columnFieldsNotCompared,
// and never both. Adding a field to Column breaks this test until a human
// decides how the comparison logic should treat it.
//
// IF THIS FAILS BECAUSE A FIELD WAS ADDED TO Column:
// update columnsEqualWithContext in diff.go (plus columnExtendedAttributesEqual
// for extended attributes) to compare the new field, then add it to
// columnFieldsCompared. If the new field is intentionally NOT part of column
// equality (like Raw / Check / Options), add it to columnFieldsNotCompared with
// a justifying comment instead.
func TestColumnsEqualAllFieldsAccounted(t *testing.T) {
	typ := reflect.TypeFor[Column]()

	var exported []string
	for f := range typ.Fields() {
		if f.IsExported() {
			exported = append(exported, f.Name)
		}
	}

	for _, name := range exported {
		_, compared := columnFieldsCompared[name]
		_, notCompared := columnFieldsNotCompared[name]
		require.False(t, compared && notCompared,
			"Column field %q is listed as both compared and not-compared; fix this guard", name)
		require.True(t, compared || notCompared,
			"Column field %q is not accounted for. Wire it into BOTH columnsEqual functions "+
				"in diff.go and add it to columnFieldsCompared, or add it to "+
				"columnFieldsNotCompared with a reason.", name)
	}

	// Lock the total so removing a field (or renaming one out of both maps) is
	// also caught, not just additions.
	require.Len(t, exported, len(columnFieldsCompared)+len(columnFieldsNotCompared),
		"Column exported field count changed. Reconcile columnFieldsCompared / "+
			"columnFieldsNotCompared with the struct. Fields: %v", exported)

	// Guard against the maps rotting: every listed name must be a real field.
	for name := range columnFieldsCompared {
		_, ok := typ.FieldByName(name)
		require.True(t, ok, "columnFieldsCompared lists %q but Column has no such field", name)
	}
	for name := range columnFieldsNotCompared {
		_, ok := typ.FieldByName(name)
		require.True(t, ok, "columnFieldsNotCompared lists %q but Column has no such field", name)
	}
}

// baseColumn returns a Column with every comparable field set to a non-zero,
// non-nil value so that flipping any single attribute is a detectable change.
// GeneratedExpr is non-nil so that GeneratedStored is meaningful (it is only
// compared when a generation expression is present). Type is a non-numeric type
// so that DefaultKind participates in the comparison.
func baseColumn() Column {
	return Column{
		Name:            "c",
		Type:            "varchar",
		Length:          new(255),
		Precision:       new(10),
		Scale:           new(2),
		Unsigned:        new(true),
		Zerofill:        new(true),
		EnumValues:      []string{"a", "b"},
		SetValues:       []string{"x", "y"},
		Nullable:        false,
		Default:         new("foo"),
		DefaultIsExpr:   true,
		DefaultKind:     DefaultKindString,
		OnUpdate:        new("current_timestamp"),
		GeneratedExpr:   new("(1 + 1)"),
		GeneratedStored: true,
		SRID:            new(uint32(4326)),
		AutoInc:         true,
		PrimaryKey:      true,
		Unique:          true,
		Comment:         new("hi"),
		Charset:         new("utf8mb4"),
		Collation:       new("utf8mb4_bin"),
	}
}

// everyComparedFieldMutation returns one mutation per comparable field that, when
// applied to a baseColumn(), must make it differ. Used by both behavioral tests
// below. The mutations cover the same field set as columnFieldsCompared.
func everyComparedFieldMutation() []struct {
	name   string
	mutate func(c *Column)
} {
	return []struct {
		name   string
		mutate func(c *Column)
	}{
		{"Name", func(c *Column) { c.Name = "different" }},
		{"Type", func(c *Column) { c.Type = "char" }},
		{"Length", func(c *Column) { c.Length = new(100) }},
		{"Precision", func(c *Column) { c.Precision = new(20) }},
		{"Scale", func(c *Column) { c.Scale = new(4) }},
		{"Unsigned", func(c *Column) { c.Unsigned = new(false) }},
		{"Zerofill", func(c *Column) { c.Zerofill = new(false) }},
		{"EnumValues", func(c *Column) { c.EnumValues = []string{"a", "c"} }},
		{"SetValues", func(c *Column) { c.SetValues = []string{"x", "z"} }},
		{"Nullable", func(c *Column) { c.Nullable = true }},
		{"Default", func(c *Column) { c.Default = new("bar") }},
		{"DefaultIsExpr", func(c *Column) { c.DefaultIsExpr = false }},
		{"DefaultKind", func(c *Column) { c.DefaultKind = DefaultKindNumber }},
		{"OnUpdate", func(c *Column) { c.OnUpdate = new("now()") }},
		{"GeneratedExpr", func(c *Column) { c.GeneratedExpr = new("(2 + 2)") }},
		{"GeneratedStored", func(c *Column) { c.GeneratedStored = false }},
		{"SRID", func(c *Column) { c.SRID = new(uint32(3857)) }},
		{"AutoInc", func(c *Column) { c.AutoInc = false }},
		{"PrimaryKey", func(c *Column) { c.PrimaryKey = false }},
		{"Comment", func(c *Column) { c.Comment = new("bye") }},
		{"Charset", func(c *Column) { c.Charset = new("latin1") }},
		{"Collation", func(c *Column) { c.Collation = new("latin1_swedish_ci") }},
	}
}

// TestColumnsEqualWithContextDetectsEveryField is behavioral coverage backing
// the reflection guard: columnsEqualWithContext must return false when ANY single
// comparable field differs. If a field is added to Column and wired into
// baseColumn/the mutation list but NOT into columnsEqualWithContext, the matching
// case fails (the mutated column is still reported equal).
func TestColumnsEqualWithContextDetectsEveryField(t *testing.T) {
	// Empty table context (nil TableOptions): the charset/collation
	// normalization is skipped, so those comparisons behave like plain pointer
	// equality — exactly what this test wants.
	ct := &CreateTable{TableName: "t"}
	target := &CreateTable{TableName: "t"}
	opts := &DiffOptions{}

	a := baseColumn()
	b := baseColumn()
	require.True(t, ct.columnsEqualWithContext(&a, &b, target, opts),
		"identical columns must compare equal")

	// Unique is deliberately ignored: inline column-level UNIQUE is folded
	// into index-level diffing (effectiveIndexes/diffIndexes), so a
	// Unique-only difference is a representation difference, not a change.
	uniqueOnly := baseColumn()
	uniqueOnly.Unique = !uniqueOnly.Unique
	require.True(t, ct.columnsEqualWithContext(&a, &uniqueOnly, target, opts),
		"columnsEqualWithContext must ignore a Unique-only difference")

	for _, m := range everyComparedFieldMutation() {
		t.Run(m.name, func(t *testing.T) {
			src := baseColumn()
			tgt := baseColumn()
			m.mutate(&tgt)
			require.False(t, ct.columnsEqualWithContext(&src, &tgt, target, opts),
				"columnsEqualWithContext must return false when %s differs; "+
					"if you added field %s, make sure diff.go compares it", m.name, m.name)
		})
	}
}
