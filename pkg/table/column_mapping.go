package table

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
)

// ColumnMapping represents the column relationship between a source and target table,
// including any column renames. It is computed once and shared across chunker,
// copier, applier, checksum, and repl subscription.
//
// A nil *ColumnMapping is safe to use — all methods return sensible defaults
// (empty columns, nil renames).
type ColumnMapping struct {
	sourceTable *TableInfo
	targetTable *TableInfo
	renames     map[string]string // old→new, may be nil

	// Pre-computed intersection results
	sourceColumns []string // non-generated source columns that exist in target
	targetColumns []string // corresponding target column names (renamed where applicable)
}

// NewColumnMapping creates a ColumnMapping between source and target tables,
// with an optional rename map (old→new). The column intersection is computed
// immediately. If target is nil, source is used as the target.
func NewColumnMapping(source, target *TableInfo, renames map[string]string) *ColumnMapping {
	if target == nil {
		target = source
	}
	m := &ColumnMapping{
		sourceTable: source,
		targetTable: target,
		renames:     renames,
	}
	m.sourceColumns, m.targetColumns = m.computeIntersection()
	return m
}

// computeIntersection calculates the column intersection between source and target.
// MySQL column identifiers are case-insensitive, so all matching is performed
// on lower-cased names: the rename map comes from the user's ALTER statement
// and may use different case than the columns were declared with. The returned
// slices always use the declared (information_schema) case so that downstream
// exact-name lookups (e.g. column type maps) succeed.
func (m *ColumnMapping) computeIntersection() ([]string, []string) {
	// Map of lower-cased target column name → declared target column name.
	t2Set := make(map[string]string, len(m.targetTable.NonGeneratedColumns))
	for _, col := range m.targetTable.NonGeneratedColumns {
		t2Set[strings.ToLower(col)] = col
	}

	// Lower-cased copy of the rename map (old→new).
	renames := make(map[string]string, len(m.renames))
	for oldName, newName := range m.renames {
		renames[strings.ToLower(oldName)] = strings.ToLower(newName)
	}

	// Build a set of target columns that are "claimed" by renames.
	// These cannot be used for identity matching by other source columns.
	claimedTargets := make(map[string]struct{}, len(renames))
	for _, newName := range renames {
		claimedTargets[newName] = struct{}{}
	}

	var srcCols, tgtCols []string
	for _, srcCol := range m.sourceTable.NonGeneratedColumns {
		srcLower := strings.ToLower(srcCol)
		// Check if this column was renamed
		if newName, ok := renames[srcLower]; ok {
			if declared, exists := t2Set[newName]; exists {
				srcCols = append(srcCols, srcCol)
				tgtCols = append(tgtCols, declared)
				continue
			}
		}
		// Identity match (same name in both tables), but only if the target
		// column is not already claimed by a rename from another source column.
		if declared, exists := t2Set[srcLower]; exists {
			if _, claimed := claimedTargets[srcLower]; !claimed {
				srcCols = append(srcCols, srcCol)
				tgtCols = append(tgtCols, declared)
			}
		}
	}
	return srcCols, tgtCols
}

// Columns returns two comma-separated, backtick-quoted column lists
// for source and target. When there are no renames, both strings are identical.
func (m *ColumnMapping) Columns() (source, target string) {
	srcQuoted := make([]string, len(m.sourceColumns))
	tgtQuoted := make([]string, len(m.targetColumns))
	for i := range m.sourceColumns {
		srcQuoted[i] = sqlescape.EscapeIdentifier(m.sourceColumns[i])
		tgtQuoted[i] = sqlescape.EscapeIdentifier(m.targetColumns[i])
	}
	return strings.Join(srcQuoted, ", "), strings.Join(tgtQuoted, ", ")
}

// ColumnsSlice returns parallel slices of source and target column names.
// sourceColumns[i] corresponds to targetColumns[i].
func (m *ColumnMapping) ColumnsSlice() (sourceColumns, targetColumns []string) {
	return m.sourceColumns, m.targetColumns
}

// checksumSeparator is interleaved between every value in the checksum
// CONCAT() so that content cannot shift across adjacent column boundaries
// undetected. Without it, the rows ('x0','y') and ('x','0y') concatenate to
// the same string and produce identical CRC32 values. This is the same reason
// pt-table-checksum uses CONCAT_WS with a '#' separator. A value containing
// '#' can still theoretically produce an ambiguous concatenation, but the
// fixed value/ISNULL-digit/separator structure makes an accidental collision
// require precisely-placed separator-and-digit patterns inside the diverged
// data — far weaker than the previous any-boundary-shift collision, and well
// below the CRC32 collision floor the checksum already accepts.
const checksumSeparator = ", '#', "

// ChecksumExprs returns two checksum column expressions (argument lists for
// CONCAT()) for source and target, wrapping each column in IFNULL(), ISNULL()
// and CAST, with a '#' separator literal between every value (see
// checksumSeparator). The CAST type always comes from the target table's type
// definition, but the cast itself is side-dependent for JSON columns (see
// castExpr), so the two expressions can differ even without renames.
func (m *ColumnMapping) ChecksumExprs() (source, target string, err error) {
	sourceExprs := make([]string, len(m.sourceColumns))
	targetExprs := make([]string, len(m.targetColumns))
	for i := range m.sourceColumns {
		// CAST type comes from the target table for both source and target
		// so that type conversions (e.g. INT→BIGINT) are applied consistently.
		// For source: SQL references the old column name, type from target's new column name.
		// For target: both SQL reference and type lookup use the new column name.
		srcCast, err := m.targetTable.wrapCastTypeAs(m.sourceColumns[i], m.targetColumns[i], castSource)
		if err != nil {
			return "", "", err
		}
		tgtCast, err := m.targetTable.wrapCastType(m.targetColumns[i], castTarget)
		if err != nil {
			return "", "", err
		}
		sourceExprs[i] = "IFNULL(" + srcCast + ",'')" + checksumSeparator + "ISNULL(`" + m.sourceColumns[i] + "`)"
		targetExprs[i] = "IFNULL(" + tgtCast + ",'')" + checksumSeparator + "ISNULL(`" + m.targetColumns[i] + "`)"
	}
	return strings.Join(sourceExprs, checksumSeparator), strings.Join(targetExprs, checksumSeparator), nil
}

// RepairExprs returns the SELECT expression list and the INSERT column list
// for the checksum's same-server chunk repair:
//
//	REPLACE INTO target (<targetColumns>) SELECT <sourceExprs> FROM source ...
//
// For most columns the source expression is the bare quoted column — a
// byte-faithful server-side copy. JSON columns are instead wrapped in the
// same text round-trip the checksum's source side uses (see castExpr): the
// checksum asserts that the target holds the one-text-round-trip image of
// each source document, so a repair that copied the raw bytes would store a
// value the next checksum pass flags again — misparsed doubles would re-flag
// forever. Like ChecksumExprs, the type decision comes from the target
// table's column type.
func (m *ColumnMapping) RepairExprs() (sourceExprs, targetColumns string, err error) {
	srcExprs := make([]string, len(m.sourceColumns))
	tgtQuoted := make([]string, len(m.targetColumns))
	for i := range m.sourceColumns {
		tgtQuoted[i] = sqlescape.EscapeIdentifier(m.targetColumns[i])
		tp, ok := m.targetTable.columnsMySQLTps[m.targetColumns[i]]
		if !ok {
			return "", "", fmt.Errorf("column %q not found in table %s", m.targetColumns[i], m.targetTable.TableName)
		}
		srcExprs[i] = sqlescape.EscapeIdentifier(m.sourceColumns[i])
		if castableTp(tp) == "json" {
			srcExprs[i] = textRoundTripCast(srcExprs[i])
		}
	}
	return strings.Join(srcExprs, ", "), strings.Join(tgtQuoted, ", "), nil
}

// SourceColumnIndices returns the indices into sourceTable.NonGeneratedColumns
// for each intersected column. This is used when row data only contains
// non-generated columns (e.g., from SELECT statements).
func (m *ColumnMapping) SourceColumnIndices() []int {
	indexMap := make(map[string]int, len(m.sourceTable.NonGeneratedColumns))
	for i, col := range m.sourceTable.NonGeneratedColumns {
		indexMap[col] = i
	}
	indices := make([]int, len(m.sourceColumns))
	for i, col := range m.sourceColumns {
		indices[i] = indexMap[col]
	}
	return indices
}

// SourceOrdinalIndices returns the indices into sourceTable.Columns (all columns,
// including generated) for each intersected column. This is needed when working
// with binlog row images, which contain ALL columns including generated ones.
func (m *ColumnMapping) SourceOrdinalIndices() []int {
	indexMap := make(map[string]int, len(m.sourceTable.Columns))
	for i, col := range m.sourceTable.Columns {
		indexMap[col] = i
	}
	indices := make([]int, len(m.sourceColumns))
	for i, col := range m.sourceColumns {
		indices[i] = indexMap[col]
	}
	return indices
}

// Renames returns the column rename mapping (old→new), or nil if there are none.
func (m *ColumnMapping) Renames() map[string]string {
	return m.renames
}

// TargetTable returns the target table.
func (m *ColumnMapping) TargetTable() *TableInfo {
	return m.targetTable
}

// SourceTable returns the source table.
func (m *ColumnMapping) SourceTable() *TableInfo {
	return m.sourceTable
}
