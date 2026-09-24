package lint

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/statement"
)

func init() {
	Register(&RedundantIndexLinter{})
}

// RedundantIndexLinter checks for redundant indexes in tables.
// An index is considered redundant if:
//  1. Its columns are a prefix of the PRIMARY KEY columns
//  2. Another index's column list starts with this index's columns (prefix match)
//  3. Another index has exactly the same columns in the same order (duplicate)
//  4. The index ends with PRIMARY KEY columns (suffix redundancy) — InnoDB
//     auto-appends PK columns to secondary indexes, so spelling them out is
//     wasteful.
//  5. The index starts with the full PRIMARY KEY (prefix redundancy) — point
//     and equality lookups by PK are already served by the clustered index,
//     so the leading PK columns add no capability that a non-PK-leading
//     variant of the index wouldn't already provide.
//
// The linter evaluates the *post-state* of the schema (existing tables with
// CREATE TABLE / ALTER TABLE changes applied) so warnings fire when a
// redundant index is being introduced, and stay silent when it is being
// removed — matching the post-state convention established for other linters
// in #840.
type RedundantIndexLinter struct{}

func (l *RedundantIndexLinter) Name() string {
	return "redundant_indexes"
}

func (l *RedundantIndexLinter) Description() string {
	return "Detects redundant indexes including duplicates, prefix matches, and unnecessary PRIMARY KEY suffixes or prefixes"
}

func (l *RedundantIndexLinter) Lint(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) []Violation {
	var violations []Violation
	for _, table := range PostState(existingTables, changes) {
		violations = append(violations, l.checkTableIndexes(table)...)
	}
	return violations
}

func (l *RedundantIndexLinter) String() string {
	return Stringer(l)
}

// checkTableIndexes checks a single table for redundant indexes
func (l *RedundantIndexLinter) checkTableIndexes(table *statement.CreateTable) []Violation {
	var violations []Violation
	indexes := table.GetIndexes()
	primaryKey := indexes.ByName("PRIMARY")

	// Track which indexes we've already reported as redundant
	// to avoid duplicate violations
	reportedRedundant := make(map[string]bool)

	for _, index := range indexes {
		if reportedRedundant[index.Name] {
			continue
		}

		// PRIMARY KEY itself never participates in suffix/prefix-of-PK checks
		// against itself, and the checks below are only meaningful for
		// secondary indexes.
		//
		// UNIQUE indexes are also excluded from the partial-PK-suffix branch
		// (handled inside hasRedundantPKSuffix): a UNIQUE index's trailing
		// columns are part of its uniqueness scope, not auto-appended PK
		// bookkeeping, so trimming them would silently drop a data-integrity
		// constraint.
		if primaryKey != nil && index.Type != "PRIMARY KEY" {
			// Check: Redundant PK suffix
			// The index explicitly includes PK columns at its end. InnoDB
			// already appends PK columns to secondary indexes, so spelling
			// them out wastes space.
			if hasRedundant, colCount := hasRedundantPKSuffix(index, *primaryKey); hasRedundant {
				violations = append(violations, createPKSuffixViolation(
					table.GetTableName(),
					index,
					*primaryKey,
					colCount,
				))
				// Continue checking - index might also be fully redundant
			}

			// Check: Redundant PK prefix
			// The index leads with the full PK. Point and equality lookups by
			// PK are already served by the clustered index, and a non-PK-leading
			// variant of this index would auto-append PK anyway.
			if hasRedundant, colCount := hasRedundantPKPrefix(index, *primaryKey); hasRedundant {
				violations = append(violations, createPKPrefixViolation(
					table.GetTableName(),
					index,
					*primaryKey,
					colCount,
				))
			}
		}

		// Check: Redundant to another index
		for _, otherIndex := range indexes {
			if index.Name == otherIndex.Name {
				continue
			}

			if isRedundantToIndex(index, otherIndex) {
				isDuplicate := indexColumnsEqual(indexParts(index), indexParts(otherIndex))
				violations = append(violations, createRedundancyViolation(
					table.GetTableName(),
					index,
					otherIndex,
					isDuplicate,
					otherIndex.Type == "PRIMARY KEY",
				))
				reportedRedundant[index.Name] = true
				break
			}
		}
	}

	return violations
}

// indexParts returns the structured key parts of an index. The deprecated
// Index.Columns field is a flat []string that silently drops expression key
// parts (functional indexes) and prefix lengths (col(n)), so all comparison
// logic in this linter operates on ColumnList instead. Index.Columns is only
// used for human-readable violation messages.
func indexParts(index statement.Index) []statement.IndexColumn {
	if len(index.ColumnList) > 0 {
		return index.ColumnList
	}
	// Synthesized indexes (column-level PRIMARY KEY / UNIQUE produced by
	// GetIndexes) only populate the flat Columns slice. Build equivalent plain
	// key parts so comparison logic still sees the columns.
	if len(index.Columns) == 0 {
		return nil
	}
	parts := make([]statement.IndexColumn, 0, len(index.Columns))
	for _, c := range index.Columns {
		parts = append(parts, statement.IndexColumn{Name: c})
	}
	return parts
}

// renderIndexPart formats a single key part for human-readable messages,
// including prefix lengths (col(10)), expression parts ((LOWER(name))), and
// descending key parts (col DESC).
func renderIndexPart(p statement.IndexColumn) string {
	var rendered string
	switch {
	case p.Expression != nil:
		rendered = "(" + *p.Expression + ")"
	case p.Length != nil:
		rendered = fmt.Sprintf("%s(%d)", p.Name, *p.Length)
	default:
		rendered = p.Name
	}
	if p.Desc {
		rendered += " DESC"
	}
	return rendered
}

// renderedIndexParts renders each key part for human-readable messages.
func renderedIndexParts(parts []statement.IndexColumn) []string {
	rendered := make([]string, 0, len(parts))
	for _, p := range parts {
		rendered = append(rendered, renderIndexPart(p))
	}
	return rendered
}

// renderIndexParts formats a list of key parts as a bare comma-separated list
// for SQL-snippet suggestions (Redefine index ... as INDEX (...)) — quoting
// inside a runnable statement would corrupt it. Message prose uses the quoted
// phrase helpers below instead.
func renderIndexParts(parts []statement.IndexColumn) string {
	return strings.Join(renderedIndexParts(parts), ", ")
}

// keyPartsPhrase formats key parts for message prose, quoting each rendered
// part the way %q quotes a single identifier so a column list carries the
// same marker as the quoted index name beside it, with the "column"/"columns"
// noun in number agreement.
func keyPartsPhrase(parts []statement.IndexColumn) string {
	return columnsPhrase(renderedIndexParts(parts))
}

// indexColumnsPhrase formats an index's columns for message prose. It uses the
// structured key parts (which carry prefix lengths and expressions), falling
// back to the flat Columns slice for synthesized indexes (e.g. column-level
// PRIMARY KEY / UNIQUE) via indexParts.
func indexColumnsPhrase(index statement.Index) string {
	return keyPartsPhrase(indexParts(index))
}

// indexColumnPartCovers reports whether key part b can serve every lookup that
// key part a serves, when both occupy the same position in two indexes.
//
// Semantics:
//   - Expression parts are opaque: an expression is covered only by an
//     identical expression. A plain column never covers (or is covered by) an
//     expression.
//   - A prefix part col(n) is covered by the full column col (no length) or by
//     a longer-or-equal prefix col(m) where m >= n. The reverse is false: a
//     full column is NOT covered by a shorter prefix, because the prefix lacks
//     covering-scan and full-ordering capability.
//   - Column-name comparison is case-insensitive, matching MySQL identifier
//     semantics.
//   - Key-part direction must match: a DESC key part stores the opposite
//     ordering from an ASC one, so at the same position in two multi-column
//     indexes they do not produce interchangeable orderings.
func indexColumnPartCovers(a, b statement.IndexColumn) bool {
	// Direction must match for both column and expression key parts.
	if a.Desc != b.Desc {
		return false
	}

	// Expression parts only match identical expressions.
	if a.Expression != nil || b.Expression != nil {
		if a.Expression == nil || b.Expression == nil {
			return false
		}
		return *a.Expression == *b.Expression
	}

	// Both are plain column references.
	if !strings.EqualFold(a.Name, b.Name) {
		return false
	}

	// b must cover at least as much of the column as a.
	// nil Length means the full column (covers any prefix length).
	if b.Length == nil {
		return true
	}
	if a.Length == nil {
		// a needs the full column but b only indexes a prefix → not covered.
		return false
	}
	return *b.Length >= *a.Length
}

// indexColumnsEqual reports whether two key-part lists are identical, including
// column names (case-insensitive), prefix lengths, and expressions. This is the
// exact-duplicate test: i1(name(10)) and i2(name) are NOT equal, and two
// identical functional indexes ARE equal.
func indexColumnsEqual(a, b []statement.IndexColumn) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !indexColumnPartCovers(a[i], b[i]) || !indexColumnPartCovers(b[i], a[i]) {
			return false
		}
	}
	return true
}

// isColumnListPrefixCoveredBy reports whether every leading key part of a is
// covered by the corresponding key part of b (a is a "coverable prefix" of b).
// Used to decide whether index a is redundant to index b.
func isColumnListPrefixCoveredBy(a, b []statement.IndexColumn) bool {
	if len(a) > len(b) {
		return false
	}
	for i := range a {
		if !indexColumnPartCovers(a[i], b[i]) {
			return false
		}
	}
	return true
}

// isRedundantToIndex checks if indexA is redundant to indexB.
// Returns true if indexA is a coverable prefix of indexB OR if they're exact
// duplicates, AND any constraint indexA enforces is also enforced by indexB.
//
// Comparison operates on ColumnList so that expression key parts and prefix
// lengths are honoured: a functional index is only redundant to another index
// carrying the identical expression part, and a prefix index col(n) is covered
// by col but not vice versa.
func isRedundantToIndex(indexA statement.Index, indexB statement.Index) bool {
	// FULLTEXT and SPATIAL indexes serve specialized purposes and are not
	// interchangeable with B-tree indexes (or each other).
	if indexA.Type == "FULLTEXT" || indexA.Type == "SPATIAL" ||
		indexB.Type == "FULLTEXT" || indexB.Type == "SPATIAL" {
		return false
	}

	// PRIMARY KEY is a table-level constraint (clustered key, uniqueness,
	// implicit NOT NULL) that cannot be dropped in favor of another index —
	// not even a column-for-column duplicate. Reporting it as redundant
	// would suggest an unsafe rewrite, so never flag it.
	if indexA.Type == "PRIMARY KEY" {
		return false
	}

	partsA := indexParts(indexA)
	partsB := indexParts(indexB)

	// indexA cannot be redundant if it has MORE key parts than indexB.
	if len(partsA) > len(partsB) {
		return false
	}

	// A UNIQUE index enforces uniqueness over its exact key parts. Dropping it
	// is only safe when another index enforces an *identical* uniqueness
	// guarantee, which requires an EXACT key-part match — same columns, same
	// prefix lengths, same expressions, same order. Prefix coverage is NOT
	// sufficient here: UNIQUE(name) does not enforce uniqueness of a 10-char
	// prefix, so UNIQUE(name(10)) is not redundant to it even though name(10)
	// is "covered" by name for read purposes. Likewise a wider covering index
	// (UNIQUE or PK on more columns) enforces a different uniqueness scope and
	// does not subsume the constraint.
	if indexA.Type == "UNIQUE" {
		if indexB.Type != "UNIQUE" && indexB.Type != "PRIMARY KEY" {
			return false
		}
		// Exact key-part equality is required so dropping indexA never drops a
		// uniqueness guarantee that indexB does not also provide.
		return indexColumnsEqual(partsA, partsB)
	}

	// indexA is redundant when it is a coverable prefix of (or exact duplicate
	// of) indexB.
	return isColumnListPrefixCoveredBy(partsA, partsB)
}

// isPlainPKColumn reports whether index key part p is a plain column reference
// (no prefix length, no expression, ascending) whose name matches the given PK
// column. InnoDB auto-appends the *full* PK column to secondary indexes in
// ascending order, so a prefixed, expression, or DESC key part does not stand
// in for a PK column.
func isPlainPKColumn(p statement.IndexColumn, pkColumn string) bool {
	return p.Expression == nil && p.Length == nil && !p.Desc && strings.EqualFold(p.Name, pkColumn)
}

// hasRedundantPKPrefix checks if a secondary index leads with the full
// PRIMARY KEY. Returns true and the count of leading PK columns if so.
//
// Rationale: InnoDB's PRIMARY KEY is the clustered index, so point and
// equality lookups by PK are best served by PK directly. A secondary index
// of the form INDEX (pk, x, y) covers no lookup that either PK alone or
// INDEX (x, y) (with PK auto-appended) cannot already serve. The leading
// PK columns are therefore redundant.
//
// Unlike the suffix check, this only matches the *full* PK at the start —
// a partial PK prefix at the start is a defensible covering index pattern.
// The index must have additional columns beyond the PK (an index that *is*
// the PK is caught by the duplicate / prefix-of-PK rules instead).
func hasRedundantPKPrefix(index statement.Index, primaryKey statement.Index) (bool, int) {
	pkLen := len(primaryKey.Columns)
	if pkLen == 0 {
		return false, 0
	}
	parts := indexParts(index)
	if len(parts) <= pkLen {
		return false, 0
	}
	for i := range pkLen {
		if !isPlainPKColumn(parts[i], primaryKey.Columns[i]) {
			return false, 0
		}
	}
	return true, pkLen
}

// hasRedundantPKSuffix checks if an index has a redundant PRIMARY KEY suffix.
// Returns true and the count of redundant columns if the index ends with PK columns
// (or a prefix of PK columns).
//
// In InnoDB, secondary indexes automatically include PK columns, so explicitly
// adding them at the end is redundant.
//
// UNIQUE indexes are excluded from the *partial*-PK-suffix branch: the trailing
// columns of a UNIQUE index are part of its uniqueness scope (UNIQUE (x, y, a)
// is not implied by PK (a, b)), so trimming them would drop a data-integrity
// constraint. A full-PK suffix on a UNIQUE index is still flagged because the
// PK already guarantees uniqueness, making the spelled-out suffix vacuous.
func hasRedundantPKSuffix(index statement.Index, primaryKey statement.Index) (bool, int) {
	if len(primaryKey.Columns) == 0 {
		return false, 0
	}

	parts := indexParts(index)
	pkLen := len(primaryKey.Columns)
	indexLen := len(parts)

	// Index must be longer than PK to have a suffix.
	if indexLen <= pkLen {
		return false, 0
	}

	// Try to match the full PK at the end.
	fullMatch := true
	for i := range pkLen {
		if !isPlainPKColumn(parts[indexLen-pkLen+i], primaryKey.Columns[i]) {
			fullMatch = false
			break
		}
	}

	if fullMatch {
		return true, pkLen // Full PK suffix is redundant
	}

	// A partial PK suffix on a UNIQUE index is part of its uniqueness scope,
	// not redundant auto-append bookkeeping. Only the full-PK case above is
	// vacuous for UNIQUE; stop here.
	if index.Type == "UNIQUE" {
		return false, 0
	}

	// Check if index ends with a PREFIX of PK
	// e.g., PK (a, b, c) and INDEX (x, a, b) - the (a, b) suffix is redundant
	for prefixLen := pkLen - 1; prefixLen > 0; prefixLen-- {
		match := true
		for i := range prefixLen {
			if !isPlainPKColumn(parts[indexLen-prefixLen+i], primaryKey.Columns[i]) {
				match = false
				break
			}
		}
		if match {
			return true, prefixLen // PK prefix suffix is redundant
		}
	}

	return false, 0
}

// createRedundancyViolation creates a violation for prefix or duplicate redundancy
func createRedundancyViolation(tableName string, redundantIndex statement.Index, coveringIndex statement.Index, isDuplicate bool, redundantToPK bool) Violation {
	var message, suggestion string

	redundantCols := indexColumnsPhrase(redundantIndex)
	coveringCols := indexColumnsPhrase(coveringIndex)

	if isDuplicate {
		if redundantToPK {
			message = fmt.Sprintf(
				"Index %q on %s is a duplicate of the PRIMARY KEY",
				redundantIndex.Name,
				redundantCols,
			)
			suggestion = fmt.Sprintf(
				"Drop index %q as it duplicates the PRIMARY KEY. "+
					"The PRIMARY KEY already provides all the functionality of this index.",
				redundantIndex.Name,
			)
		} else {
			message = fmt.Sprintf(
				"Index %q on %s is a duplicate of index %q",
				redundantIndex.Name,
				redundantCols,
				coveringIndex.Name,
			)
			suggestion = fmt.Sprintf(
				"Drop index %q as it is an exact duplicate of %q. "+
					"Duplicate indexes waste space and slow down writes with no benefit.",
				redundantIndex.Name,
				coveringIndex.Name,
			)
		}
	} else {
		// Prefix redundancy
		if redundantToPK {
			message = fmt.Sprintf(
				"Index %q on %s is redundant - covered by PRIMARY KEY on %s",
				redundantIndex.Name,
				redundantCols,
				coveringCols,
			)
			suggestion = fmt.Sprintf(
				"Consider dropping index %q as it is fully covered by the PRIMARY KEY. "+
					"In InnoDB, the PRIMARY KEY is the clustered index and provides prefix lookup capability.",
				redundantIndex.Name,
			)
		} else {
			message = fmt.Sprintf(
				"Index %q on %s is redundant - covered by index %q on %s",
				redundantIndex.Name,
				redundantCols,
				coveringIndex.Name,
				coveringCols,
			)
			suggestion = fmt.Sprintf(
				"Consider dropping index %q as it is fully covered by %q. "+
					"The longer index can satisfy all queries that would use the shorter index.",
				redundantIndex.Name,
				coveringIndex.Name,
			)
		}
	}

	return Violation{
		Linter:     &RedundantIndexLinter{},
		Severity:   SeverityWarning,
		Message:    message,
		Location:   &Location{Table: tableName, Index: &redundantIndex.Name},
		Suggestion: &suggestion,
		Context: map[string]any{
			"redundant_index": redundantIndex.Name,
			"covering_index":  coveringIndex.Name,
			"is_duplicate":    isDuplicate,
			"redundant_to_pk": redundantToPK,
		},
	}
}

// createPKSuffixViolation creates a violation for redundant PRIMARY KEY suffix
func createPKSuffixViolation(tableName string, index statement.Index, primaryKey statement.Index, redundantColCount int) Violation {
	// Slice over the structured key parts so prefix lengths / expression parts
	// are positioned correctly even when the flat Columns slice would drop them.
	parts := indexParts(index)
	redundantSuffix := parts[len(parts)-redundantColCount:]
	usefulPrefix := parts[:len(parts)-redundantColCount]
	pkCols := primaryKey.Columns[:redundantColCount]

	fullColsPhrase := keyPartsPhrase(parts)
	quotedSuffix := quotedList(renderedIndexParts(redundantSuffix))
	redundantSuffixStr := renderIndexParts(redundantSuffix)
	usefulPrefixStr := renderIndexParts(usefulPrefix)

	var message, suggestion string

	if redundantColCount == len(primaryKey.Columns) {
		// Full PK is redundant
		message = fmt.Sprintf(
			"Index %q on %s has redundant PRIMARY KEY suffix %s. "+
				"InnoDB automatically appends PK columns to secondary indexes.",
			index.Name,
			fullColsPhrase,
			quotedSuffix,
		)
		suggestion = fmt.Sprintf(
			"Redefine index %q as INDEX (%s). "+
				"InnoDB will automatically append the PK %s internally, "+
				"so explicitly including the PK wastes space and provides no benefit.",
			index.Name,
			usefulPrefixStr,
			columnsPhrase(pkCols),
		)
	} else {
		// Prefix of PK is redundant
		message = fmt.Sprintf(
			"Index %q on %s has a redundant PRIMARY KEY suffix %s — a leading prefix of the PRIMARY KEY appearing at the end of the index. "+
				"InnoDB automatically appends the full PK %s to secondary indexes, "+
				"so spelling out part of the PK at the end of the index is redundant.",
			index.Name,
			fullColsPhrase,
			quotedSuffix,
			columnsPhrase(primaryKey.Columns),
		)
		suggestion = fmt.Sprintf(
			"Redefine index %q as INDEX (%s). "+
				"InnoDB will automatically append the full PK %s internally.",
			index.Name,
			usefulPrefixStr,
			quotedList(primaryKey.Columns),
		)
	}

	return Violation{
		Linter:     &RedundantIndexLinter{},
		Severity:   SeverityWarning,
		Message:    message,
		Location:   &Location{Table: tableName, Index: &index.Name},
		Suggestion: &suggestion,
		Context: map[string]any{
			"index_name":          index.Name,
			"full_columns":        index.Columns,
			"useful_columns":      usefulPrefixStr,
			"redundant_suffix":    redundantSuffixStr,
			"primary_key_columns": primaryKey.Columns,
			"redundant_col_count": redundantColCount,
		},
	}
}

// createPKPrefixViolation creates a violation for a secondary index that
// leads with the full PRIMARY KEY.
func createPKPrefixViolation(tableName string, index statement.Index, primaryKey statement.Index, redundantColCount int) Violation {
	parts := indexParts(index)
	redundantPrefix := parts[:redundantColCount]
	usefulSuffix := parts[redundantColCount:]

	redundantPrefixStr := renderIndexParts(redundantPrefix)
	usefulSuffixStr := renderIndexParts(usefulSuffix)

	message := fmt.Sprintf(
		"Index %q on %s leads with PRIMARY KEY %s. "+
			"Point and equality lookups by PRIMARY KEY are served by the clustered index directly, "+
			"and InnoDB appends PK columns to secondary indexes — the leading PK columns add no capability.",
		index.Name,
		keyPartsPhrase(parts),
		keyPartsPhrase(redundantPrefix),
	)
	suggestion := fmt.Sprintf(
		"Redefine index %q as INDEX (%s) — InnoDB will append PRIMARY KEY (%s) internally — "+
			"or drop it entirely if the PRIMARY KEY alone covers the queries that use it.",
		index.Name,
		usefulSuffixStr,
		strings.Join(primaryKey.Columns, ", "),
	)

	return Violation{
		Linter:     &RedundantIndexLinter{},
		Severity:   SeverityWarning,
		Message:    message,
		Location:   &Location{Table: tableName, Index: &index.Name},
		Suggestion: &suggestion,
		Context: map[string]any{
			"index_name":          index.Name,
			"full_columns":        index.Columns,
			"redundant_prefix":    redundantPrefixStr,
			"useful_columns":      usefulSuffixStr,
			"primary_key_columns": primaryKey.Columns,
			"redundant_col_count": redundantColCount,
		},
	}
}
