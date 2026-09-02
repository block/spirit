package statement

import (
	"fmt"
	"slices"
	"strings"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/parser"
)

// DiffOptions controls the behavior of the Diff operation.
type DiffOptions struct {
	// IgnoreAutoIncrement skips diffing the AUTO_INCREMENT table option
	// (the table-level next-value counter, e.g. `AUTO_INCREMENT=100`).
	// Default: true (via NewDiffOptions).
	IgnoreAutoIncrement bool

	// IgnoreColumnAutoIncrement skips diffing the column-level AUTO_INCREMENT
	// attribute (whether a column carries the AUTO_INCREMENT flag). This is
	// distinct from IgnoreAutoIncrement, which only covers the table-option
	// counter. Default: false (via NewDiffOptions) — for general schema diffing
	// a column gaining or losing AUTO_INCREMENT is a real change. It is enabled
	// by consumers like the move-tables target-state check, where an unsharded
	// source legitimately differs from a sharded target that drops
	// AUTO_INCREMENT in favor of a Vitess sequence: the difference does not
	// affect copy correctness and must not block the move.
	IgnoreColumnAutoIncrement bool

	// IgnoreNotNullRelaxation lets the schema being validated be STRICTER than
	// its reference on nullability, and only stricter: a validated column
	// declared NOT NULL where the reference permits NULL is accepted, while one
	// that permits NULL where the reference is NOT NULL remains a real
	// difference. The option therefore can never quietly accept a schema that
	// lost a NOT NULL the reference had.
	//
	// Default: false (via NewDiffOptions) — for general schema diffing a column
	// gaining or losing NOT NULL is a real change.
	//
	// It is enabled by the move-tables target checks (see
	// move/check.targetSchemaDiff), where the reference is the move's SOURCE and
	// the validated schema is its physical TARGET. What that permits is a target
	// column declared NOT NULL where the source still permits NULL — an
	// unsharded source moving into a sharded target whose shard key must be
	// NOT NULL, because a Vitess primary vindex cannot map NULL to a keyspace
	// id.
	//
	// In terms of Diff's own arguments the reference is the parameter and the
	// validated schema is the receiver, because DiffCreateTables diffs
	// got->want. Stating the direction that way inverts it, which is why the
	// wording above and TestDiff_IgnoreNotNullRelaxation both name the two
	// schemas by role instead.
	//
	// That is safe for a move because nullability is metadata, not row bytes:
	// the copy and the checksum compare values, and every column's NULL-ness is
	// compared explicitly (see ColumnMapping.ChecksumExprs, which emits an
	// ISNULL() digit per column). A tightened column whose source data holds no
	// NULLs is therefore identical on both sides, and one that does hold a NULL
	// is reported as a mismatch rather than hidden by this option.
	IgnoreNotNullRelaxation bool

	// IgnoreEngine skips diffing the ENGINE table option.
	// Default: true (via NewDiffOptions).
	IgnoreEngine bool

	// IgnoreCharsetCollation skips diffing CHARSET and COLLATION table options.
	// Default: false (via NewDiffOptions).
	IgnoreCharsetCollation bool

	// IgnorePartitioning skips diffing partition options entirely.
	// Default: false (via NewDiffOptions).
	IgnorePartitioning bool

	// IgnoreRowFormat skips diffing the ROW_FORMAT table option.
	// Default: true (via NewDiffOptions).
	// ROW_FORMAT=DYNAMIC is the InnoDB default in MySQL 8.0+, so differences
	// between an unspecified ROW_FORMAT and an explicit DYNAMIC are cosmetic.
	IgnoreRowFormat bool
}

// NewDiffOptions returns DiffOptions with sensible defaults.
// By default, AUTO_INCREMENT, ENGINE, and ROW_FORMAT differences are ignored.
func NewDiffOptions() *DiffOptions {
	return &DiffOptions{
		IgnoreAutoIncrement:       true,
		IgnoreColumnAutoIncrement: false,
		IgnoreNotNullRelaxation:   false,
		IgnoreEngine:              true,
		IgnoreCharsetCollation:    false,
		IgnorePartitioning:        false,
		IgnoreRowFormat:           true,
	}
}

// Diff compares this CreateTable (source) with another CreateTable (target)
// and returns ALTER TABLE statements needed to transform source into target.
// Most changes produce a single statement, but some (e.g. changing partition type)
// require multiple sequential statements.
// Returns nil if the tables are identical.
// If opts is nil, NewDiffOptions() defaults are used.
func (ct *CreateTable) Diff(target *CreateTable, opts *DiffOptions) ([]*AbstractStatement, error) {
	if opts == nil {
		opts = NewDiffOptions()
	}
	if ct.TableName != target.TableName {
		return nil, fmt.Errorf("cannot diff tables with different names: %s vs %s", ct.TableName, target.TableName)
	}

	var alterClauses []string

	// 1. Diff columns (DROP, ADD, MODIFY)
	columnClauses := ct.diffColumns(target, opts)
	alterClauses = append(alterClauses, columnClauses...)

	// 2. Diff indexes (DROP, ADD). Option-only index changes (same column
	// list, different WITH PARSER / KEY_BLOCK_SIZE / etc.) are returned as
	// separate statements because MySQL no-ops a combined DROP+ADD of the same
	// index in a single ALTER.
	indexClauses, separateIndexStatements := ct.diffIndexes(target)
	alterClauses = append(alterClauses, indexClauses...)

	// 3. Diff constraints (DROP, ADD)
	constraintClauses := ct.diffConstraints(target)
	alterClauses = append(alterClauses, constraintClauses...)

	// 4. Diff table options
	tableOptionClauses := ct.diffTableOptions(target, opts)
	alterClauses = append(alterClauses, tableOptionClauses...)

	// 5. Diff partition options — may produce additional statements
	var additionalStatements [][]string
	if !opts.IgnorePartitioning {
		partitionClauses, extraStatements := ct.diffPartitionOptions(target)
		alterClauses = append(alterClauses, partitionClauses...)
		additionalStatements = extraStatements
	}

	// Option-only index changes run as their own ALTER statements, after the
	// primary ALTER so they observe any column changes the re-add depends on.
	additionalStatements = append(additionalStatements, separateIndexStatements...)

	// Build the result
	var results []*AbstractStatement

	// Primary statement (columns, indexes, constraints, table options, and simple partition changes)
	if len(alterClauses) > 0 {
		stmt, err := ct.buildAlterStatement(alterClauses)
		if err != nil {
			return nil, err
		}
		results = append(results, stmt)
	}

	// Additional statements (e.g. second ALTER for partition type changes)
	for _, clauses := range additionalStatements {
		stmt, err := ct.buildAlterStatement(clauses)
		if err != nil {
			return nil, err
		}
		results = append(results, stmt)
	}

	if len(results) == 0 {
		return nil, nil
	}

	return results, nil
}

// buildAlterStatement constructs and parses an ALTER TABLE statement from clauses.
func (ct *CreateTable) buildAlterStatement(clauses []string) (*AbstractStatement, error) {
	alter := strings.Join(clauses, ", ")
	alterStmt := fmt.Sprintf("ALTER TABLE %s %s", sqlescape.EscapeIdentifier(ct.TableName), alter)

	p := parser.New()
	stmtNodes, _, err := p.Parse(alterStmt, "", "")
	if err != nil {
		return nil, fmt.Errorf("failed to parse generated ALTER statement: %w (SQL: %s)", err, alterStmt)
	}

	if len(stmtNodes) != 1 {
		return nil, fmt.Errorf("expected exactly one statement, got %d", len(stmtNodes))
	}

	return &AbstractStatement{
		Table:     ct.TableName,
		Alter:     alter,
		Statement: alterStmt,
		StmtNode:  &stmtNodes[0],
	}, nil
}

// diffColumns compares columns and returns ALTER clauses for differences
func (ct *CreateTable) diffColumns(target *CreateTable, opts *DiffOptions) []string {
	var clauses []string

	// Build maps for easier lookup. Keys are lowercased so identifier
	// matching is case-insensitive — MySQL treats column names that
	// differ only in case as the same column.
	sourceColumns := make(map[string]*Column)
	for i := range ct.Columns {
		sourceColumns[strings.ToLower(ct.Columns[i].Name)] = &ct.Columns[i]
	}

	targetColumns := make(map[string]*Column)
	for i := range target.Columns {
		targetColumns[strings.ToLower(target.Columns[i].Name)] = &target.Columns[i]
	}

	// Adding or dropping the PRIMARY KEY itself is emitted by diffIndexes (the
	// PK is always a table-level index after normalization). We only need the
	// source/target PK column sets here to recognize the implicit NOT NULL ->
	// NULL relaxation that dropping a PK produces, and suppress the redundant
	// MODIFY COLUMN it would otherwise generate.
	sourcePKColumns := pkColumnSet(ct.getPrimaryKeyIndex())
	targetPKColumns := pkColumnSet(target.getPrimaryKeyIndex())

	// Collect DROP operations and sort by name for deterministic output
	var dropClauses []string
	for _, sourceCol := range ct.Columns {
		if _, exists := targetColumns[strings.ToLower(sourceCol.Name)]; !exists {
			dropClauses = append(dropClauses, fmt.Sprintf("DROP COLUMN %s", sqlescape.EscapeIdentifier(sourceCol.Name)))
		}
	}
	slices.Sort(dropClauses)
	clauses = append(clauses, dropClauses...)

	// Determine which columns need explicit positioning
	// A column needs explicit positioning if:
	// 1. It's a new column (ADD) - always needs position
	// 2. Its previous column changed (explicit reorder)
	needsExplicitPosition := ct.calculateColumnPositioning(target, sourceColumns, targetColumns)

	// Generate the ALTER clauses in target order
	var prevColumn string
	for i, targetCol := range target.Columns {
		sourceCol, existsInSource := sourceColumns[strings.ToLower(targetCol.Name)]

		if !existsInSource {
			// ADD new column
			clause := fmt.Sprintf("ADD COLUMN %s", formatColumnDefinition(&targetCol))
			// Add positioning - only if not at the end
			isLastColumn := i == len(target.Columns)-1
			if prevColumn == "" {
				clause += " FIRST"
			} else if !isLastColumn {
				clause += fmt.Sprintf(" AFTER %s", sqlescape.EscapeIdentifier(prevColumn))
			}
			// If it's the last column, omit AFTER clause (implicit)
			clauses = append(clauses, clause)
		} else {
			// Suppress the MODIFY when a column's only change is the implicit
			// NOT NULL -> NULL relaxation from dropping the primary key it
			// belonged to (a PK column is NOT NULL; once the PK is gone the
			// target can declare it NULL). The PK drop itself is emitted by
			// diffIndexes.
			//
			// This is the same predicate IgnoreNotNullRelaxation applies in
			// columnsEqualWithContext, gated on PK membership rather than on
			// the option — so with that option on, this case is subsumed.
			// Change one and check the other.
			lower := strings.ToLower(targetCol.Name)
			pkDroppedNullabilityChange := sourcePKColumns[lower] && !targetPKColumns[lower] &&
				!sourceCol.Nullable && targetCol.Nullable

			// MODIFY existing column if:
			// 1. Column definition changed (and not just the PK-drop nullability relaxation)
			// 2. Column needs explicit positioning
			// needsExplicitPosition is keyed by lowercased column name, so
			// look up with the same normalization to avoid missing a
			// position-only change when the target's spelling is in mixed
			// or upper case.
			needsModify := (!ct.columnsEqualWithContext(sourceCol, &targetCol, target, opts) && !pkDroppedNullabilityChange) ||
				needsExplicitPosition[strings.ToLower(targetCol.Name)]

			if needsModify {
				clause := fmt.Sprintf("MODIFY COLUMN %s", formatColumnDefinition(&targetCol))
				if needsExplicitPosition[strings.ToLower(targetCol.Name)] {
					if prevColumn == "" {
						clause += " FIRST"
					} else {
						clause += fmt.Sprintf(" AFTER %s", sqlescape.EscapeIdentifier(prevColumn))
					}
				}
				clauses = append(clauses, clause)
			}
		}

		prevColumn = targetCol.Name
	}

	return clauses
}

// calculateColumnPositioning determines which columns need explicit positioning (FIRST/AFTER).
// Returns a map of column names (lowercased) that need explicit positioning.
// Map keys are lowercased to match the source/target column maps built by
// the caller, since column identifiers in MySQL are case-insensitive.
func (ct *CreateTable) calculateColumnPositioning(target *CreateTable, sourceColumns, targetColumns map[string]*Column) map[string]bool {
	needsExplicitPosition := make(map[string]bool)

	var prevColumn string
	for _, targetCol := range target.Columns {
		_, existsInSource := sourceColumns[strings.ToLower(targetCol.Name)]

		if !existsInSource {
			// New columns always need explicit positioning
			needsExplicitPosition[strings.ToLower(targetCol.Name)] = true
		} else {
			// Existing column - check if its position changed
			sourcePrevCol := getPreviousColumn(ct.Columns, targetCol.Name)

			// Check if this is an implicit or explicit position change
			_, prevColExistedInSource := sourceColumns[strings.ToLower(prevColumn)]
			_, sourcePrevColStillExists := targetColumns[strings.ToLower(sourcePrevCol)]

			implicitChange := false
			switch {
			case prevColumn == "" && sourcePrevCol == "":
				// Both first, no real change
				implicitChange = true
			case prevColumn != "" && !prevColExistedInSource:
				// Previous column is new, position change is implicit
				implicitChange = true
			case sourcePrevCol != "" && !sourcePrevColStillExists:
				// Previous column was dropped, position change is implicit
				implicitChange = true
			case strings.EqualFold(prevColumn, sourcePrevCol):
				// Same previous column, check if we need cascading
				// Cascading happens if the previous column was repositioned
				if prevColumn != "" && needsExplicitPosition[strings.ToLower(prevColumn)] && prevColExistedInSource {
					// Previous column was repositioned, so this column needs repositioning too
					needsExplicitPosition[strings.ToLower(targetCol.Name)] = true
				}
				implicitChange = true
			default:
				// Explicit reorder - previous column changed and both exist
				implicitChange = false
			}

			if !implicitChange {
				needsExplicitPosition[strings.ToLower(targetCol.Name)] = true
			}
		}

		prevColumn = targetCol.Name
	}

	return needsExplicitPosition
}

// diffIndexes compares indexes and returns ALTER clauses for differences.
//
// Most index changes are emitted into the combined ALTER (the returned
// []string). However, an index whose column list is identical but whose
// options differ (e.g. WITH PARSER or KEY_BLOCK_SIZE) cannot be changed by a
// combined `DROP INDEX x, ADD INDEX x (<same cols>)` in a single ALTER: MySQL
// pairs the two clauses up and keeps the existing index, silently ignoring the
// option change. To make such a change actually take effect, the DROP and ADD
// must run as two separate ALTER statements. Those are returned via the second
// value as standalone clause-lists (each becomes its own ALTER statement).
func (ct *CreateTable) diffIndexes(target *CreateTable) (clauses []string, separateStatements [][]string) {
	// Inline column-level UNIQUE / PRIMARY KEY have already been materialized
	// into ct.Indexes by normalization (see indexNormalizer, primaryKeyNormalizer),
	// so both index sets can be walked directly.
	sourceIdxList := ct.Indexes
	targetIdxList := target.Indexes

	// Build maps for easier lookup
	sourceIndexes := make(map[string]*Index)
	for i := range sourceIdxList {
		sourceIndexes[sourceIdxList[i].Name] = &sourceIdxList[i]
	}

	targetIndexes := make(map[string]*Index)
	for i := range targetIdxList {
		targetIndexes[targetIdxList[i].Name] = &targetIdxList[i]
	}

	// Safety net for inline-derived names: the synthesized name is only a
	// guess at what the server assigned. Pair unique indexes that cover the
	// same column set but carry different names whenever at least one side's
	// name came from an inline declaration, so we never DROP a live unique
	// index (or ADD a duplicate) that the other side's inline UNIQUE already
	// expresses. Two explicitly named unique indexes that differ only in name
	// are NOT paired — that is a real rename (DROP + ADD).
	matchedSourceUnique := make(map[string]bool) // source name -> matched
	matchedTargetUnique := make(map[string]bool) // target name -> matched
	for i := range sourceIdxList {
		sourceIdx := &sourceIdxList[i]
		if sourceIdx.Type != "UNIQUE" {
			continue
		}
		if _, exactMatch := targetIndexes[sourceIdx.Name]; exactMatch {
			continue // handled by the normal name-based path
		}
		for j := range targetIdxList {
			targetIdx := &targetIdxList[j]
			if targetIdx.Type != "UNIQUE" {
				continue
			}
			if _, exactMatch := sourceIndexes[targetIdx.Name]; exactMatch {
				continue // this target index already has a name match in source
			}
			if matchedTargetUnique[targetIdx.Name] {
				continue // already paired with another source index
			}
			if !sourceIdx.InlineDerived && !targetIdx.InlineDerived {
				continue // both explicitly named: a genuine rename
			}
			if indexColumnsIdenticalIgnoreName(sourceIdx, targetIdx) {
				matchedSourceUnique[sourceIdx.Name] = true
				matchedTargetUnique[targetIdx.Name] = true
				break
			}
		}
	}

	// Collect DROP operations and sort by name for deterministic output
	var dropClauses []string

	// The primary key is a table-level index on both sides after normalization
	// (see primaryKeyNormalizer), so its add/drop/change falls out of the normal
	// index diff below — no inline-PK special cases are needed. pkDropAdded just
	// guards against emitting "DROP PRIMARY KEY" twice from the source loop.
	pkDropAdded := false

	// optionOnlyChanged tracks index names whose column list is unchanged but
	// whose options differ (e.g. WITH PARSER / KEY_BLOCK_SIZE). These must be
	// emitted as separate DROP + ADD statements rather than combined into one
	// ALTER, because MySQL no-ops a combined DROP+ADD of the same name and
	// column list. Such indexes are routed out of dropClauses/addClauses below.
	optionOnlyChanged := make(map[string]bool)

	for i := range sourceIdxList {
		sourceIdx := &sourceIdxList[i]
		if matchedSourceUnique[sourceIdx.Name] {
			continue // equivalent unique index exists in target under an inline-derived pairing
		}
		targetIdx, existsInTarget := targetIndexes[sourceIdx.Name]

		if !existsInTarget {
			// Index removed completely
			if sourceIdx.Type == "PRIMARY KEY" {
				if !pkDropAdded {
					dropClauses = append(dropClauses, "DROP PRIMARY KEY")
					pkDropAdded = true
				}
			} else {
				dropClauses = append(dropClauses, fmt.Sprintf("DROP INDEX %s", sqlescape.EscapeIdentifier(sourceIdx.Name)))
			}
		} else if !indexesEqual(sourceIdx, targetIdx) && !indexesEqualIgnoreVisibility(sourceIdx, targetIdx) {
			// Index exists but changed (and not just visibility) - need to drop and re-add
			switch {
			case sourceIdx.Type == "PRIMARY KEY":
				// Only add if not already added above
				if !pkDropAdded {
					dropClauses = append(dropClauses, "DROP PRIMARY KEY")
					pkDropAdded = true
				}
			case indexNeedsSeparateRebuild(sourceIdx, targetIdx):
				// A no-op-prone option (WITH PARSER / KEY_BLOCK_SIZE) changed on
				// an unchanged column list. A combined DROP+ADD in one ALTER
				// would be a MySQL no-op, so emit two separate statements that
				// MySQL will actually apply.
				optionOnlyChanged[sourceIdx.Name] = true
				separateStatements = append(separateStatements,
					[]string{fmt.Sprintf("DROP INDEX %s", sqlescape.EscapeIdentifier(sourceIdx.Name))},
					[]string{formatAddIndex(targetIdx)},
				)
			default:
				dropClauses = append(dropClauses, fmt.Sprintf("DROP INDEX %s", sqlescape.EscapeIdentifier(sourceIdx.Name)))
			}
		}
	}
	slices.Sort(dropClauses)
	clauses = append(clauses, dropClauses...)

	// Collect ADD operations and sort by name for deterministic output
	var addClauses []string
	for _, targetIdx := range targetIdxList {
		if matchedTargetUnique[targetIdx.Name] {
			continue // equivalent unique index exists in source under an inline-derived pairing
		}
		sourceIdx, existsInSource := sourceIndexes[targetIdx.Name]

		if !existsInSource {
			// New index - add it
			addClauses = append(addClauses, formatAddIndex(&targetIdx))
		} else if !indexesEqual(sourceIdx, &targetIdx) {
			// Index exists but changed - check if only visibility changed
			if indexesEqualIgnoreVisibility(sourceIdx, &targetIdx) {
				// Only visibility changed - skip for now, handle in ALTER INDEX section
				continue
			}
			// Option-only changes are emitted as separate statements above.
			if optionOnlyChanged[targetIdx.Name] {
				continue
			}
			// Other changes - need to drop and re-add (drop already handled above)
			addClauses = append(addClauses, formatAddIndex(&targetIdx))
		}
	}
	slices.Sort(addClauses)
	clauses = append(clauses, addClauses...)

	// Collect ALTER INDEX operations for visibility changes (must come after DROP/ADD)
	var alterClauses []string
	for _, targetIdx := range targetIdxList {
		sourceIdx, existsInSource := sourceIndexes[targetIdx.Name]

		if existsInSource && !indexesEqual(sourceIdx, &targetIdx) && indexesEqualIgnoreVisibility(sourceIdx, &targetIdx) {
			// Only visibility changed
			targetVisible := targetIdx.Invisible == nil || !*targetIdx.Invisible
			if targetVisible {
				alterClauses = append(alterClauses, fmt.Sprintf("ALTER INDEX %s VISIBLE", sqlescape.EscapeIdentifier(targetIdx.Name)))
			} else {
				alterClauses = append(alterClauses, fmt.Sprintf("ALTER INDEX %s INVISIBLE", sqlescape.EscapeIdentifier(targetIdx.Name)))
			}
		}
	}
	slices.Sort(alterClauses)
	clauses = append(clauses, alterClauses...)

	return clauses, separateStatements
}

// diffConstraints compares constraints and returns ALTER clauses for differences
func (ct *CreateTable) diffConstraints(target *CreateTable) []string {
	var clauses []string

	// Build maps for easier lookup
	sourceConstraints := make(map[string]*Constraint)
	for i := range ct.Constraints {
		sourceConstraints[ct.Constraints[i].Name] = &ct.Constraints[i]
	}

	targetConstraints := make(map[string]*Constraint)
	for i := range target.Constraints {
		targetConstraints[target.Constraints[i].Name] = &target.Constraints[i]
	}

	// Build a set of source constraint names that have an equivalent match in the
	// target under a different name. This handles the case where MySQL generates
	// different auto-names for CHECK constraints whose original expression text
	// differs only cosmetically (e.g. charset introducers like _utf8mb3 that are
	// stripped during parsing). Without this, such constraints would produce a
	// spurious DROP + ADD.
	matchedSourceByExpression := make(map[string]bool) // source name -> matched
	matchedTargetByExpression := make(map[string]bool) // target name -> matched
	for i := range ct.Constraints {
		sourceConstr := &ct.Constraints[i]
		if _, exactMatch := targetConstraints[sourceConstr.Name]; exactMatch {
			continue // will be handled by the normal name-based path
		}
		// No exact name match — look for an expression-equivalent target constraint
		for j := range target.Constraints {
			targetConstr := &target.Constraints[j]
			if _, exactMatch := sourceConstraints[targetConstr.Name]; exactMatch {
				continue // this target constraint already has a name match in source
			}
			if matchedTargetByExpression[targetConstr.Name] {
				continue // already paired with another source constraint
			}
			if constraintsEqualIgnoreName(sourceConstr, targetConstr) {
				matchedSourceByExpression[sourceConstr.Name] = true
				matchedTargetByExpression[targetConstr.Name] = true
				break
			}
		}
	}

	// Collect DROP operations and sort by name for deterministic output.
	// CHECK constraints that differ only in enforcement are not dropped;
	// they are flipped in place with ALTER CHECK (collected separately).
	var dropClauses []string
	var enforcementClauses []string
	for i := range ct.Constraints {
		sourceConstr := &ct.Constraints[i]
		if matchedSourceByExpression[sourceConstr.Name] {
			continue // equivalent constraint exists in target under a different name
		}
		targetConstr, exists := targetConstraints[sourceConstr.Name]

		// Drop if constraint doesn't exist in target OR if it changed
		if !exists || !constraintsEqual(sourceConstr, targetConstr) {
			if exists && constraintsEqualExceptEnforcement(sourceConstr, targetConstr) {
				// Only the [NOT] ENFORCED state changed: use MySQL's targeted
				// ALTER CHECK clause instead of DROP+ADD. Flipping to NOT
				// ENFORCED is then metadata-only (INSTANT-capable); flipping
				// to ENFORCED validates existing rows either way, exactly as
				// an enforced re-ADD would.
				if targetConstr.NotEnforced {
					enforcementClauses = append(enforcementClauses, fmt.Sprintf("ALTER CHECK %s NOT ENFORCED", sqlescape.EscapeIdentifier(sourceConstr.Name)))
				} else {
					enforcementClauses = append(enforcementClauses, fmt.Sprintf("ALTER CHECK %s ENFORCED", sqlescape.EscapeIdentifier(sourceConstr.Name)))
				}
				continue
			}
			switch sourceConstr.Type {
			case "FOREIGN KEY":
				dropClauses = append(dropClauses, fmt.Sprintf("DROP FOREIGN KEY %s", sqlescape.EscapeIdentifier(sourceConstr.Name)))
			case "CHECK":
				dropClauses = append(dropClauses, fmt.Sprintf("DROP CHECK %s", sqlescape.EscapeIdentifier(sourceConstr.Name)))
			}
		}
	}
	slices.Sort(dropClauses)
	clauses = append(clauses, dropClauses...)
	slices.Sort(enforcementClauses)
	clauses = append(clauses, enforcementClauses...)

	// Collect ADD operations and sort by name for deterministic output
	var addClauses []string
	for _, targetConstr := range target.Constraints {
		if matchedTargetByExpression[targetConstr.Name] {
			continue // equivalent constraint exists in source under a different name
		}
		sourceConstr, existsInSource := sourceConstraints[targetConstr.Name]

		if !existsInSource || !constraintsEqual(sourceConstr, &targetConstr) {
			if existsInSource && constraintsEqualExceptEnforcement(sourceConstr, &targetConstr) {
				continue // enforcement-only change; handled by ALTER CHECK above
			}
			addClauses = append(addClauses, formatAddConstraint(&targetConstr))
		}
	}
	slices.Sort(addClauses)
	clauses = append(clauses, addClauses...)

	return clauses
}

// diffTableOptions compares table options and returns ALTER clauses for differences.
// The opts parameter controls which table options are compared.
func (ct *CreateTable) diffTableOptions(target *CreateTable, opts *DiffOptions) []string {
	var clauses []string

	// Compare ENGINE
	if !opts.IgnoreEngine {
		if !ptrEqual(ct.TableOptions.getEngine(), target.TableOptions.getEngine()) {
			if engine := target.TableOptions.getEngine(); engine != nil {
				clauses = append(clauses, fmt.Sprintf("ENGINE=%s", *engine))
			}
		}
	}

	// Compare CHARSET and COLLATION
	if !opts.IgnoreCharsetCollation {
		if !ptrEqual(ct.TableOptions.getCharset(), target.TableOptions.getCharset()) {
			if charset := target.TableOptions.getCharset(); charset != nil {
				clauses = append(clauses, fmt.Sprintf("DEFAULT CHARSET=%s", *charset))
			}
		}

		if !ptrEqual(ct.TableOptions.getCollation(), target.TableOptions.getCollation()) {
			if collation := target.TableOptions.getCollation(); collation != nil {
				clauses = append(clauses, fmt.Sprintf("COLLATE=%s", *collation))
			}
		}
	}

	// Compare COMMENT (always compared — not controlled by an ignore option)
	if !ptrEqual(ct.TableOptions.getComment(), target.TableOptions.getComment()) {
		if comment := target.TableOptions.getComment(); comment != nil {
			clauses = append(clauses, fmt.Sprintf("COMMENT='%s'", sqlescape.EscapeString(*comment)))
		} else {
			// The source has a comment but the target does not: emit an
			// explicit empty comment to clear it. Without this clause the
			// difference would be detected but silently dropped.
			clauses = append(clauses, "COMMENT=''")
		}
	}

	// Compare ROW_FORMAT
	if !opts.IgnoreRowFormat {
		if !ptrEqual(ct.TableOptions.getRowFormat(), target.TableOptions.getRowFormat()) {
			if rowFormat := target.TableOptions.getRowFormat(); rowFormat != nil {
				clauses = append(clauses, fmt.Sprintf("ROW_FORMAT=%s", *rowFormat))
			}
		}
	}

	// Compare AUTO_INCREMENT
	// Note: AUTO_INCREMENT is ignored by default (like vitess schemadiff)
	if !opts.IgnoreAutoIncrement {
		if !ptrEqual(ct.TableOptions.getAutoIncrement(), target.TableOptions.getAutoIncrement()) {
			if autoInc := target.TableOptions.getAutoIncrement(); autoInc != nil {
				clauses = append(clauses, fmt.Sprintf("AUTO_INCREMENT=%s", *autoInc))
			}
		}
	}

	return clauses
}

// columnsEqualWithContext checks if two columns are equal, considering table
// context for charset/collation: a column with no explicit charset/collation
// inherits its owning table's defaults, so those attributes are compared on
// their resolved values (see charsetCollationEqual).
func (ct *CreateTable) columnsEqualWithContext(a, b *Column, target *CreateTable, opts *DiffOptions) bool {
	// Column names are case-insensitive in MySQL, so `id` and `ID` refer
	// to the same column.
	if !strings.EqualFold(a.Name, b.Name) {
		return false
	}
	if a.Type != b.Type {
		return false
	}
	if !ptrEqual(a.Length, b.Length) {
		return false
	}
	if !ptrEqual(a.Precision, b.Precision) {
		return false
	}
	if !ptrEqual(a.Scale, b.Scale) {
		return false
	}
	if !ptrEqual(a.Unsigned, b.Unsigned) {
		return false
	}
	if !ptrEqual(a.Zerofill, b.Zerofill) {
		return false
	}
	// A nullability difference is normally a real change. With
	// IgnoreNotNullRelaxation it is forgiven in exactly one direction: `a` is
	// NOT NULL and `b` permits NULL, so the only thing the MODIFY would do is
	// weaken the column (the same NOT NULL -> NULL relaxation diffColumns
	// already suppresses for a dropped primary key). The opposite direction —
	// a difference the MODIFY would need to *tighten* — stays a real change.
	//
	// `a` belongs to the receiver and `b` to the diffed-to table, which for a
	// DiffCreateTables caller means `a` is the schema under validation and `b`
	// the reference: this forgives a validated schema that is stricter, which
	// is the direction the option documents.
	if a.Nullable != b.Nullable {
		forgivenRelaxation := opts.IgnoreNotNullRelaxation && !a.Nullable && b.Nullable
		if !forgivenRelaxation {
			return false
		}
	}
	// Normalize default values for nullable columns:
	// For nullable columns, nil and the NULL *keyword* are semantically
	// equivalent. User might write `VARCHAR(255) NULL` but MySQL outputs
	// `VARCHAR(255) DEFAULT NULL`. A quoted string literal 'NULL'
	// (DefaultIsString) is NOT the keyword and must not collapse — it is a
	// real default that differs from no-default.
	//
	// Each side is normalized on its OWN nullability. Until
	// IgnoreNotNullRelaxation existed the two were always equal here (the check
	// above returned early otherwise), so this is the same normalization as
	// before for every other comparison. It matters for a forgiven relaxation:
	// there the nullable side renders `DEFAULT NULL` while the NOT NULL side
	// carries no default, and gating on one side's nullability alone would
	// report that as a default difference — reintroducing the very MODIFY the
	// option exists to suppress.
	sourceDefault := a.Default
	targetDefault := b.Default
	if a.Nullable && sourceDefault != nil && *sourceDefault == "NULL" && !a.DefaultIsString {
		sourceDefault = nil
	}
	if b.Nullable && targetDefault != nil && *targetDefault == "NULL" && !b.DefaultIsString {
		targetDefault = nil
	}
	if !ptrEqual(sourceDefault, targetDefault) {
		return false
	}
	if a.DefaultIsExpr != b.DefaultIsExpr {
		return false
	}
	if !columnExtendedAttributesEqual(a, b) {
		return false
	}
	// On a string column a quoted string literal default ('TRUE') is a
	// different value than the same text as a keyword/number default (TRUE),
	// so quotedness is part of column identity. On a numeric column it is not:
	// MySQL always renders the default quoted, so `DEFAULT 0` (bare) and
	// `DEFAULT '0'` (the SHOW CREATE TABLE form) are the same default and must
	// compare equal — the value itself is already compared above.
	if a.DefaultIsString != b.DefaultIsString && !isNumericColumnType(a.Type) {
		return false
	}
	if !opts.IgnoreColumnAutoIncrement && a.AutoInc != b.AutoInc {
		return false
	}
	if a.PrimaryKey != b.PrimaryKey {
		return false
	}
	// Unique is intentionally NOT compared: a column-level UNIQUE is
	// representation, not state. MySQL canonicalizes `c int UNIQUE` into a
	// table-level UNIQUE KEY (that is what SHOW CREATE TABLE reports), and a
	// MODIFY COLUMN cannot express uniqueness anyway (formatColumnDefinition
	// never emits UNIQUE). Inline uniques are materialized into table-level
	// indexes by normalization — see indexNormalizer / diffIndexes.
	if !ptrEqual(a.Comment, b.Comment) {
		return false
	}

	if !charsetCollationEqual(a, b, ct, target, opts) {
		return false
	}

	if !slices.Equal(a.EnumValues, b.EnumValues) {
		return false
	}
	if !slices.Equal(a.SetValues, b.SetValues) {
		return false
	}
	return true
}

// charsetCarryingTypes are the column types that store text and therefore
// carry a real charset/collation, inheriting the owning table's defaults when
// the column declares neither. Type names are the parser's canonical
// spellings. Binary and JSON types are excluded: they carry only a synthetic
// "binary" charset that is identical on both sides of a same-type compare,
// and spatial/vector types have theirs stripped by charsetlessTypeNormalizer.
var charsetCarryingTypes = map[string]bool{
	"char":       true,
	"varchar":    true,
	"tinytext":   true,
	"text":       true,
	"mediumtext": true,
	"longtext":   true,
	"enum":       true,
	"set":        true,
}

// charsetOfCollation returns the character set a collation name belongs to.
// MySQL collation names are the owning charset name followed by an
// underscore-separated suffix (utf8mb4_general_ci -> utf8mb4); the sole
// exception is "binary", which is both a charset and its only collation.
func charsetOfCollation(collation string) string {
	if idx := strings.IndexByte(collation, '_'); idx > 0 {
		return collation[:idx]
	}
	return collation
}

// resolvedCharsetCollation returns the charset and collation a column
// actually uses, following MySQL's resolution rules: explicit column values
// win; an explicit column collation implies its charset; a column with
// neither inherits the owning table's defaults, where a table collation
// likewise implies the table charset. A value that cannot be determined from
// the statement alone is returned as "": a column or table with a charset
// but no collation uses that charset's *default* collation, and a table with
// neither option uses the server defaults — both depend on server version
// and configuration.
func resolvedCharsetCollation(col *Column, table *CreateTable) (charset, collation string) {
	switch {
	case col.Collation != nil:
		collation = strings.ToLower(*col.Collation)
		if col.Charset != nil {
			charset = strings.ToLower(*col.Charset)
		} else {
			charset = charsetOfCollation(collation)
		}
	case col.Charset != nil:
		// An explicit column charset without a collation selects the
		// charset's default collation — not the table's collation.
		charset = strings.ToLower(*col.Charset)
	default:
		if tableCollation := table.TableOptions.getCollation(); tableCollation != nil {
			collation = strings.ToLower(*tableCollation)
		}
		if tableCharset := table.TableOptions.getCharset(); tableCharset != nil {
			charset = strings.ToLower(*tableCharset)
		} else if collation != "" {
			charset = charsetOfCollation(collation)
		}
	}
	return charset, collation
}

// explicitUnlessTableDefault returns a column-level charset/collation value
// with the redundant spelling of the owning table's default normalized to
// nil, so an explicit value that merely restates the table default compares
// equal to an inherited (nil) one.
func explicitUnlessTableDefault(value, tableDefault *string) *string {
	if value != nil && tableDefault != nil && *value == *tableDefault {
		return nil
	}
	return value
}

// charsetCollationEqual reports whether two columns have the same effective
// charset and collation given their owning tables' defaults. Equality is
// decided on the RESOLVED values, not the written ones: a column that
// inherits its table default and a column that matches a *different* default
// on the other table are genuinely different columns, and since a
// table-level DEFAULT CHARSET / COLLATE clause only affects columns added
// later, converging them requires a MODIFY COLUMN in the same ALTER as the
// table-option change.
//
// Each attribute is compared strictly when both sides resolve to a concrete
// value. When a side is underdetermined (see resolvedCharsetCollation), that
// attribute falls back to comparing the written values with redundant
// table-default spellings normalized away — an unexpressed preference is
// treated as a match rather than guessed at, which keeps the diff from
// emitting a MODIFY it could never prove converged.
func charsetCollationEqual(a, b *Column, source, target *CreateTable, opts *DiffOptions) bool {
	if !charsetCarryingTypes[strings.ToLower(a.Type)] {
		// Non-character types have no table default to inherit, so compare
		// the written values directly.
		return ptrEqual(a.Charset, b.Charset) && ptrEqual(a.Collation, b.Collation)
	}

	// IgnoreCharsetCollation suppresses the table-option diff, so the table
	// defaults it ignores must not leak into the column comparison through
	// resolution either — a column inheriting a difference between the two
	// (ignored) table defaults is not a column change in this mode. Explicit
	// column-level differences are still compared by the written-value
	// comparisons below.
	sourceCharset, sourceCollation := "", ""
	targetCharset, targetCollation := "", ""
	if !opts.IgnoreCharsetCollation {
		sourceCharset, sourceCollation = resolvedCharsetCollation(a, source)
		targetCharset, targetCollation = resolvedCharsetCollation(b, target)
	}

	if sourceCharset != "" && targetCharset != "" {
		if sourceCharset != targetCharset {
			return false
		}
	} else if !ptrEqual(
		explicitUnlessTableDefault(a.Charset, source.TableOptions.getCharset()),
		explicitUnlessTableDefault(b.Charset, target.TableOptions.getCharset()),
	) {
		return false
	}

	if sourceCollation != "" && targetCollation != "" {
		return sourceCollation == targetCollation
	}
	return ptrEqual(
		explicitUnlessTableDefault(a.Collation, source.TableOptions.getCollation()),
		explicitUnlessTableDefault(b.Collation, target.TableOptions.getCollation()),
	)
}

// getPrimaryKeyIndex returns the PRIMARY KEY index if it exists (table-level PK), nil otherwise
func (ct *CreateTable) getPrimaryKeyIndex() *Index {
	for i := range ct.Indexes {
		if ct.Indexes[i].Type == "PRIMARY KEY" {
			return &ct.Indexes[i]
		}
	}
	return nil
}

// diffPartitionOptions compares partition options and returns ALTER clauses for differences.
// The first return value contains clauses for the primary ALTER statement.
// The second return value contains clause sets for additional ALTER statements needed
// when a change cannot be expressed in a single statement (e.g. changing partition type
// requires REMOVE PARTITIONING followed by a separate PARTITION BY).
func (ct *CreateTable) diffPartitionOptions(target *CreateTable) ([]string, [][]string) {
	sourcePartition := ct.Partition
	targetPartition := target.Partition

	// Case 1: No partitioning in either table - no changes
	if sourcePartition == nil && targetPartition == nil {
		return nil, nil
	}

	// Case 2: Remove partitioning (source has partitioning, target doesn't)
	if sourcePartition != nil && targetPartition == nil {
		return []string{"REMOVE PARTITIONING"}, nil
	}

	// Case 3: Add partitioning (source doesn't have partitioning, target does)
	if sourcePartition == nil && targetPartition != nil {
		return []string{formatPartitionOptions(targetPartition)}, nil
	}

	// Case 4: Both have partitioning - check if they're different
	if !partitionOptionsEqual(sourcePartition, targetPartition) {
		// Special case: For HASH/KEY partitions where only the partition count changed
		// (no explicit definitions), we can use ADD PARTITION or COALESCE PARTITION
		if isCountOnly, countDiff := isPartitionCountOnlyChange(sourcePartition, targetPartition); isCountOnly {
			if countDiff > 0 {
				return []string{fmt.Sprintf("ADD PARTITION PARTITIONS %d", countDiff)}, nil
			}
			return []string{fmt.Sprintf("COALESCE PARTITION %d", -countDiff)}, nil
		}

		// For all other partition changes (e.g. changing partition type from HASH to RANGE),
		// MySQL requires two separate ALTER TABLE statements:
		// 1. REMOVE PARTITIONING
		// 2. PARTITION BY ...
		// The first goes into the primary statement, the second is returned as an additional statement.
		return []string{"REMOVE PARTITIONING"}, [][]string{{formatPartitionOptions(targetPartition)}}
	}

	return nil, nil
}
