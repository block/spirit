package lint

import (
	"fmt"
	"slices"
	"strings"

	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/statement"
)

func init() {
	Register(&IndexVisibilityMixedLinter{})
}

// IndexVisibilityMixedLinter detects index visibility changes (ALTER INDEX ..
// VISIBLE/INVISIBLE) that are combined with table-rebuilding operations in the
// same ALTER TABLE statement.
//
// Making an index invisible is usually an experiment: you hide the index from
// the optimizer, observe the workload, and then either drop the index or make
// it visible again. Combining that experiment with a table rebuild muddies the
// result — the rebuild changes query plans and table statistics on its own, so
// there is no clean before/after to compare against. It also takes far longer
// than the metadata-only change the visibility flip would have been.
//
// Mixing with other metadata-only operations (DROP INDEX, RENAME INDEX,
// partition maintenance, VARCHAR length changes) is not flagged: those keep the
// whole statement metadata-only.
//
// This was previously a hard preflight check that refused the migration
// (see https://github.com/block/spirit/issues/283). It is a warning now:
// declarative workflows generate the ALTER from a schema diff, so the user does
// not necessarily control which clauses end up batched together.
type IndexVisibilityMixedLinter struct{}

func (l *IndexVisibilityMixedLinter) String() string {
	return Stringer(l)
}

func (l *IndexVisibilityMixedLinter) Name() string {
	return "index_visibility_mixed"
}

func (l *IndexVisibilityMixedLinter) Description() string {
	return "Detects index visibility changes mixed with table-rebuilding operations in the same ALTER TABLE"
}

func (l *IndexVisibilityMixedLinter) Lint(_ []*statement.CreateTable, changes []*statement.AbstractStatement) (violations []Violation) {
	for _, change := range changes {
		alter, ok := change.AsAlterTable()
		if !ok {
			continue
		}

		var visibilityIndexes []string
		var rebuildOperations []string

		for _, spec := range alter.Specs {
			switch spec.Tp { //nolint:exhaustive
			case ast.AlterTableIndexInvisible:
				// ALTER INDEX <name> VISIBLE/INVISIBLE puts the index name in
				// IndexName, not Name.
				visibilityIndexes = append(visibilityIndexes, spec.IndexName.O)
			case ast.AlterTableDropIndex,
				ast.AlterTableRenameIndex,
				ast.AlterTableDropPartition,
				ast.AlterTableTruncatePartition,
				ast.AlterTableAddPartitions,
				ast.AlterTableAlterColumn:
				// These are metadata-only operations, so they don't turn the
				// visibility change into a table rebuild.
				continue
			case ast.AlterTableModifyColumn, ast.AlterTableChangeColumn:
				// A VARCHAR redeclaration is metadata-only, but not if it also
				// reorders the column or declares NOT NULL. Shared with the
				// classifier that routes execution, so the two can't drift.
				if statement.ModifyColumnIsMetadataOnly(spec) {
					continue
				}
				rebuildOperations = append(rebuildOperations, AlterTableTypeToString(spec.Tp))
			case ast.AlterTableOption:
				// ENGINE=, ROW_FORMAT=, AUTO_INCREMENT= etc rebuild the table,
				// but a COMMENT-only change is metadata-only.
				if statement.SpecOnlyChangesComment(spec) {
					continue
				}
				rebuildOperations = append(rebuildOperations, AlterTableTypeToString(spec.Tp))
			default:
				// All other operations (ADD COLUMN, ADD INDEX, ADD CONSTRAINT, ENGINE, etc) are table-rebuilding
				rebuildOperations = append(rebuildOperations, AlterTableTypeToString(spec.Tp))
			}
		}

		if len(visibilityIndexes) == 0 || len(rebuildOperations) == 0 {
			continue
		}

		// Sorted+deduplicated so that the same ALTER always reports the same
		// operation list, regardless of clause order.
		operations := strings.Join(slices.Compact(slices.Sorted(slices.Values(rebuildOperations))), ", ")
		suggestion := fmt.Sprintf("Apply the index visibility change separately from the %s — as its own statement, or as its own schema revision in a declarative workflow", operations)

		location := &Location{Table: change.Table}
		// Only pin the location to an index when the statement changes exactly
		// one. With several, naming the first would be misleading; the message
		// lists them all.
		if len(visibilityIndexes) == 1 {
			location.Index = &visibilityIndexes[0]
		}

		violations = append(violations, Violation{
			Linter:   l,
			Severity: SeverityWarning,
			Message: fmt.Sprintf("Index visibility change on %q is mixed with table-rebuilding operations (%s). A visibility change is usually an experiment, and rebuilding the table at the same time makes the result difficult to interpret",
				strings.Join(visibilityIndexes, ", "), operations),
			Location:   location,
			Suggestion: &suggestion,
		})
	}

	return violations
}
