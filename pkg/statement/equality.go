package statement

import (
	"reflect"
	"slices"
	"strings"
)

// This file holds the comparison helpers used by Diff to decide whether two
// parsed schema elements (columns, indexes, constraints, partitions) are
// equivalent. They are free functions rather than CreateTable methods; the
// CreateTable receivers that drive the diff live in diff.go.

// columnExtendedAttributesEqual compares the column attributes beyond the
// basic type/nullability/default set: ON UPDATE (TIMESTAMP/DATETIME
// auto-update), GENERATED ALWAYS AS expressions (including STORED vs
// VIRTUAL), and SRID. These are semantically critical — omitting them from a
// MODIFY COLUMN silently removes the behavior from the live table.
//
// Column-level CHECK constraints are intentionally NOT compared here: the
// parser hoists them into table-level CreateTable.Constraints (see
// columnCheckNormalizer), so they are diffed by diffConstraints instead. This
// matches MySQL's SHOW CREATE TABLE, which always reports CHECKs at table
// level, and keeps a re-diff convergent.
func columnExtendedAttributesEqual(a, b *Column) bool {
	if !ptrEqual(a.OnUpdate, b.OnUpdate) {
		return false
	}
	if !ptrEqual(a.GeneratedExpr, b.GeneratedExpr) {
		return false
	}
	if a.GeneratedExpr != nil && a.GeneratedStored != b.GeneratedStored {
		return false
	}
	if !ptrEqual(a.SRID, b.SRID) {
		return false
	}
	return true
}

// indexesEqual checks if two indexes are equal
func indexesEqual(a, b *Index) bool {
	if a.Name != b.Name {
		return false
	}
	if a.Type != b.Type {
		return false
	}
	// Compare using ColumnList if available, otherwise fall back to Columns.
	// Referenced column names are matched case-insensitively to mirror
	// MySQL's column-identifier semantics.
	if len(a.ColumnList) > 0 && len(b.ColumnList) > 0 {
		if !indexColumnListsEqual(a.ColumnList, b.ColumnList) {
			return false
		}
	} else if !slices.EqualFunc(a.Columns, b.Columns, strings.EqualFold) {
		return false
	}
	if !ptrEqual(a.Invisible, b.Invisible) {
		return false
	}
	if !ptrEqual(a.Using, b.Using) {
		return false
	}
	if !ptrEqual(a.Comment, b.Comment) {
		return false
	}
	if !ptrEqual(a.KeyBlockSize, b.KeyBlockSize) {
		return false
	}
	if !ptrEqual(a.ParserName, b.ParserName) {
		return false
	}
	return true
}

// indexesEqualIgnoreVisibility checks if two indexes are equal, ignoring the Invisible attribute
func indexesEqualIgnoreVisibility(a, b *Index) bool {
	if a.Name != b.Name {
		return false
	}
	if a.Type != b.Type {
		return false
	}
	// Compare using ColumnList if available, otherwise fall back to Columns.
	// Referenced column names are matched case-insensitively.
	if len(a.ColumnList) > 0 && len(b.ColumnList) > 0 {
		if !indexColumnListsEqual(a.ColumnList, b.ColumnList) {
			return false
		}
	} else if !slices.EqualFunc(a.Columns, b.Columns, strings.EqualFold) {
		return false
	}
	// Skip Invisible comparison
	if !ptrEqual(a.Using, b.Using) {
		return false
	}
	if !ptrEqual(a.Comment, b.Comment) {
		return false
	}
	if !ptrEqual(a.KeyBlockSize, b.KeyBlockSize) {
		return false
	}
	if !ptrEqual(a.ParserName, b.ParserName) {
		return false
	}
	return true
}

// indexColumnListIdentical reports whether two indexes have the same name,
// type, and column list — regardless of their options.
func indexColumnListIdentical(a, b *Index) bool {
	if a.Name != b.Name {
		return false
	}
	return indexColumnsIdenticalIgnoreName(a, b)
}

// indexColumnsIdenticalIgnoreName reports whether two indexes have the same
// type and column list, regardless of their names or options. Used to pair a
// unique index synthesized from an inline column-level UNIQUE (whose name is
// only a guess at the server-assigned one) with an equivalent live index.
func indexColumnsIdenticalIgnoreName(a, b *Index) bool {
	if a.Type != b.Type {
		return false
	}
	if len(a.ColumnList) > 0 && len(b.ColumnList) > 0 {
		return indexColumnListsEqual(a.ColumnList, b.ColumnList)
	}
	return slices.EqualFunc(a.Columns, b.Columns, strings.EqualFold)
}

// indexNeedsSeparateRebuild reports whether a changed index must be emitted as
// two separate ALTER statements (DROP then ADD) rather than combined into one.
//
// When the column list is unchanged, MySQL pairs a combined `DROP INDEX x, ADD
// INDEX x (<same cols>)` up and keeps the existing index — but only some
// options are silently ignored this way. Verified against MySQL 8.0:
//   - WITH PARSER  → ignored by the combined ALTER (must split)
//   - KEY_BLOCK_SIZE → ignored by the combined ALTER (must split)
//   - COMMENT      → applied by the combined ALTER (no split needed)
//   - visibility   → never reaches here; handled via ALTER INDEX VISIBLE/INVISIBLE
//
// If the column list itself changes, MySQL really rebuilds the index, so a
// combined ALTER is fine and this returns false.
func indexNeedsSeparateRebuild(source, target *Index) bool {
	if !indexColumnListIdentical(source, target) {
		return false
	}
	if !ptrEqual(source.ParserName, target.ParserName) {
		return true
	}
	if !ptrEqual(source.KeyBlockSize, target.KeyBlockSize) {
		return true
	}
	return false
}

// indexColumnListsEqual checks if two index column lists are equal
func indexColumnListsEqual(a, b []IndexColumn) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !indexColumnsEqual(&a[i], &b[i]) {
			return false
		}
	}
	return true
}

// indexColumnsEqual checks if two index columns are equal
func indexColumnsEqual(a, b *IndexColumn) bool {
	// Column identifiers are case-insensitive in MySQL.
	if !strings.EqualFold(a.Name, b.Name) {
		return false
	}
	if !ptrEqual(a.Expression, b.Expression) {
		return false
	}
	if !ptrEqual(a.Length, b.Length) {
		return false
	}
	// KEY (a) and KEY (a DESC) are physically different indexes.
	if a.Desc != b.Desc {
		return false
	}
	return true
}

// constraintsEqual checks if two constraints are equal
func constraintsEqual(a, b *Constraint) bool {
	if a.Name != b.Name {
		return false
	}
	return constraintsEqualIgnoreName(a, b)
}

// constraintsEqualIgnoreName compares two constraints on everything except their name.
// This is used to detect constraints that are logically identical but have different
// auto-generated names (e.g., MySQL generates different CHECK constraint names when
// the original expression text differs only in charset introducers like _utf8mb3).
func constraintsEqualIgnoreName(a, b *Constraint) bool {
	// CHECK constraint enforcement state ([NOT] ENFORCED)
	if a.NotEnforced != b.NotEnforced {
		return false
	}
	return constraintsEqualIgnoreNameAndEnforcement(a, b)
}

// constraintsEqualExceptEnforcement reports whether two same-named CHECK
// constraints are identical apart from their [NOT] ENFORCED state. Such a
// pair is applied with a targeted ALTER CHECK clause instead of DROP+ADD.
func constraintsEqualExceptEnforcement(a, b *Constraint) bool {
	if a.Type != "CHECK" || b.Type != "CHECK" {
		return false
	}
	if a.NotEnforced == b.NotEnforced {
		return false // not an enforcement change
	}
	return a.Name == b.Name && constraintsEqualIgnoreNameAndEnforcement(a, b)
}

// constraintsEqualIgnoreNameAndEnforcement compares all constraint attributes
// except the name and the CHECK enforcement state.
func constraintsEqualIgnoreNameAndEnforcement(a, b *Constraint) bool {
	if a.Type != b.Type {
		return false
	}
	if !slices.Equal(a.Columns, b.Columns) {
		return false
	}
	if !ptrEqual(a.Expression, b.Expression) {
		return false
	}
	// Compare foreign key references
	if (a.References == nil) != (b.References == nil) {
		return false
	}
	if a.References != nil {
		if a.References.Table != b.References.Table {
			return false
		}
		if !slices.Equal(a.References.Columns, b.References.Columns) {
			return false
		}
		// Compare ON DELETE and ON UPDATE actions
		if !ptrEqual(a.References.OnDelete, b.References.OnDelete) {
			return false
		}
		if !ptrEqual(a.References.OnUpdate, b.References.OnUpdate) {
			return false
		}
	}
	return true
}

// isPartitionCountOnlyChange checks if only the partition count changed for HASH/KEY partitions
// Returns true if this is a count-only change, along with the difference in count
func isPartitionCountOnlyChange(source, target *PartitionOptions) (bool, int) {
	// Must be same partition type
	if source.Type != target.Type {
		return false, 0
	}

	// Only applies to HASH and KEY partitions
	if source.Type != "HASH" && source.Type != "KEY" {
		return false, 0
	}

	// Must have same expression/columns
	if !ptrEqual(source.Expression, target.Expression) {
		return false, 0
	}
	if !slices.Equal(source.Columns, target.Columns) {
		return false, 0
	}

	// Must have same linear flag
	if source.Linear != target.Linear {
		return false, 0
	}

	// Both must not have explicit definitions (just partition count)
	if len(source.Definitions) > 0 || len(target.Definitions) > 0 {
		return false, 0
	}

	// Subpartitioning cannot be reached by ADD/COALESCE PARTITION. MySQL only
	// allows subpartitions under RANGE/LIST, so this is unreachable today; the
	// guard is here so a subpartitioning difference can never be silently
	// swallowed by a partition-count ALTER.
	if source.SubPartition != nil || target.SubPartition != nil {
		return false, 0
	}

	// Only the partition count differs
	if source.Partitions == target.Partitions {
		return false, 0
	}

	return true, int(target.Partitions) - int(source.Partitions)
}

// partitionOptionsEqual checks if two partition options are equal
func partitionOptionsEqual(a, b *PartitionOptions) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Compare partition type
	if a.Type != b.Type {
		return false
	}

	// Compare expression (for HASH and RANGE)
	if !ptrEqual(a.Expression, b.Expression) {
		return false
	}

	// Compare columns (for KEY, RANGE COLUMNS, LIST COLUMNS)
	if !slices.Equal(a.Columns, b.Columns) {
		return false
	}

	// Compare linear flag
	if a.Linear != b.Linear {
		return false
	}

	// Compare number of partitions (for HASH/KEY without explicit definitions)
	if a.Partitions != b.Partitions {
		return false
	}

	// Compare partition definitions
	if len(a.Definitions) != len(b.Definitions) {
		return false
	}

	for i := range a.Definitions {
		if !partitionDefinitionEqual(&a.Definitions[i], &b.Definitions[i]) {
			return false
		}
	}

	// Compare subpartitioning
	if !subPartitionOptionsEqual(a.SubPartition, b.SubPartition) {
		return false
	}

	return true
}

// partitionDefinitionEqual checks if two partition definitions are equal
func partitionDefinitionEqual(a, b *PartitionDefinition) bool {
	if a.Name != b.Name {
		return false
	}

	// Compare partition values
	if !partitionValuesEqual(a.Values, b.Values) {
		return false
	}

	// Compare comment
	if !ptrEqual(a.Comment, b.Comment) {
		return false
	}

	// The per-partition ENGINE clause is deliberately not compared. MySQL
	// requires every partition to use the table's storage engine, so the clause
	// carries no information beyond the table-level ENGINE — but SHOW CREATE
	// TABLE always prints it (`PARTITION p0 VALUES LESS THAN (2020) ENGINE =
	// InnoDB`) while human-authored SQL almost never does. Comparing it made
	// every partitioned table diff against its own live definition, emitting a
	// REMOVE PARTITIONING + PARTITION BY pair on every run.

	// Compare explicitly named subpartitions. This is symmetric: MySQL echoes
	// subpartition names back from SHOW CREATE TABLE when, and only when, they
	// were named explicitly, so a named-on-both-sides table compares names and
	// an auto-named one compares empty lists.
	if len(a.SubPartitions) != len(b.SubPartitions) {
		return false
	}

	for i := range a.SubPartitions {
		if !subPartitionDefinitionEqual(&a.SubPartitions[i], &b.SubPartitions[i]) {
			return false
		}
	}

	return true
}

// subPartitionDefinitionEqual checks if two named subpartitions are equal. Like
// partitions, a subpartition's ENGINE is not compared (see
// partitionDefinitionEqual).
func subPartitionDefinitionEqual(a, b *SubPartitionDefinition) bool {
	return a.Name == b.Name && ptrEqual(a.Comment, b.Comment)
}

// partitionValuesEqual checks if two partition values are equal
func partitionValuesEqual(a, b *PartitionValues) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	if a.Type != b.Type {
		return false
	}

	if len(a.Values) != len(b.Values) {
		return false
	}

	// Today both sides come from the same parsePartitionClause path and
	// are always Go strings, so reflect.DeepEqual and the old
	// fmt.Sprintf("%v") string compare are equivalent. The change is
	// forward-compatibility: if parsePartitionClause ever preserves the
	// AST literal kind (so e.g. an int literal stays an int rather than
	// being Restored to its string form), DeepEqual will distinguish
	// "5" from 5 where %v would collapse them. No behaviour change today.
	for i := range a.Values {
		if !reflect.DeepEqual(a.Values[i], b.Values[i]) {
			return false
		}
	}

	return true
}

// subPartitionOptionsEqual checks if two subpartition options are equal
func subPartitionOptionsEqual(a, b *SubPartitionOptions) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	if a.Type != b.Type {
		return false
	}

	if !ptrEqual(a.Expression, b.Expression) {
		return false
	}

	if !slices.Equal(a.Columns, b.Columns) {
		return false
	}

	if a.Linear != b.Linear {
		return false
	}

	if a.Count != b.Count {
		return false
	}

	return true
}

// pkColumnSet returns the lowercased set of columns in the given PRIMARY KEY
// index, or nil if idx is nil. Names are lowercased because MySQL column
// identifiers are case-insensitive. Reading a missing key from the returned
// (possibly nil) map yields false, so callers can index it directly.
func pkColumnSet(idx *Index) map[string]bool {
	if idx == nil {
		return nil
	}
	set := make(map[string]bool, len(idx.Columns))
	for _, c := range idx.Columns {
		set[strings.ToLower(c)] = true
	}
	return set
}
