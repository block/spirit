package statement

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/format"
)

// This file holds the free functions that rewrite a CREATE TABLE statement's
// secondary indexes (used by the copy process, which builds the table without
// secondary indexes and adds them back afterward).

// RemoveSecondaryIndexes takes a CREATE TABLE statement and returns a modified version
// without secondary indexes (regular INDEX only). PRIMARY KEY, UNIQUE, and FULLTEXT
// indexes are preserved, as is an index needed to support AUTO_INCREMENT.
func RemoveSecondaryIndexes(createStmt string) (string, error) {
	return removeSecondaryIndexes(createStmt, true)
}

// RemoveSecondaryIndexesForComparison removes every regular index, including
// AUTO_INCREMENT support. Its output is for schema comparison, not execution.
func RemoveSecondaryIndexesForComparison(createStmt string) (string, error) {
	return removeSecondaryIndexes(createStmt, false)
}

// DropDeferrableSecondaryIndexes returns the ALTER TABLE that drops from
// tableName the indexes RemoveSecondaryIndexes would leave out of createStmt,
// or "" if there are none. It defers the indexes of a table that already
// exists, such as one pre-created by a declarative schema workflow, the same
// way RemoveSecondaryIndexes defers them for a table about to be created.
// createStmt should come from SHOW CREATE TABLE, which names every index.
func DropDeferrableSecondaryIndexes(createStmt, tableName string) (string, error) {
	ct, err := ParseCreateTable(createStmt)
	if err != nil {
		return "", fmt.Errorf("failed to parse CREATE TABLE: %w", err)
	}
	var clauses []string
	for _, constraint := range deferrableIndexes(ct, true) {
		if constraint.Name == "" {
			return "", fmt.Errorf("cannot drop an unnamed index from %s", tableName)
		}
		clauses = append(clauses, "DROP INDEX "+sqlescape.EscapeIdentifier(constraint.Name))
	}
	if len(clauses) == 0 {
		return "", nil
	}
	return fmt.Sprintf("ALTER TABLE %s %s", sqlescape.EscapeIdentifier(tableName), strings.Join(clauses, ", ")), nil
}

// RemoveDeferredIndexes returns sourceCreate without the deferrable secondary
// indexes that targetCreate lacks: the source as a target whose indexes were
// deferred should look, however many of them it has gained back. An index
// counts as deferrable without regard to AUTO_INCREMENT support (as in
// RemoveSecondaryIndexesForComparison): a sharded target may not carry the
// source's AUTO_INCREMENT, and so may have dropped the index that supports
// it, while MySQL refuses a table whose AUTO_INCREMENT column leads no index.
// PRIMARY, UNIQUE, FULLTEXT and SPATIAL indexes are never removed, and an index
// the target has under a source index's name is compared as it is. Only the
// target's indexes count toward that name match, not its CHECK or FOREIGN KEY
// constraints.
func RemoveDeferredIndexes(sourceCreate, targetCreate string) (string, error) {
	target, err := ParseCreateTable(targetCreate)
	if err != nil {
		return "", fmt.Errorf("failed to parse target CREATE TABLE: %w", err)
	}
	present := make(map[string]bool)
	for _, constraint := range target.Raw.Constraints {
		if isIndex(constraint) {
			present[strings.ToLower(constraint.Name)] = true
		}
	}
	source, err := ParseCreateTable(sourceCreate)
	if err != nil {
		return "", fmt.Errorf("failed to parse source CREATE TABLE: %w", err)
	}
	missing := make(map[*ast.Constraint]bool)
	for _, constraint := range deferrableIndexes(source, false) {
		if constraint.Name != "" && !present[strings.ToLower(constraint.Name)] {
			missing[constraint] = true
		}
	}
	if len(missing) == 0 {
		return sourceCreate, nil
	}
	kept := make([]*ast.Constraint, 0, len(source.Raw.Constraints))
	for _, constraint := range source.Raw.Constraints {
		if !missing[constraint] {
			kept = append(kept, constraint)
		}
	}
	source.Raw.Constraints = kept
	var sb strings.Builder
	if err := source.Raw.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
		return "", fmt.Errorf("failed to restore CREATE TABLE: %w", err)
	}
	return sb.String(), nil
}

func removeSecondaryIndexes(createStmt string, preserveAutoIncrement bool) (string, error) {
	ct, err := ParseCreateTable(createStmt)
	if err != nil {
		return "", fmt.Errorf("failed to parse CREATE TABLE: %w", err)
	}
	deferred := make(map[*ast.Constraint]bool)
	for _, constraint := range deferrableIndexes(ct, preserveAutoIncrement) {
		deferred[constraint] = true
	}
	filteredConstraints := make([]*ast.Constraint, 0)
	for _, constraint := range ct.Raw.Constraints {
		if !deferred[constraint] {
			filteredConstraints = append(filteredConstraints, constraint)
		}
	}

	// Update the AST with filtered constraints
	ct.Raw.Constraints = filteredConstraints

	// Restore the modified AST back to SQL
	var sb strings.Builder
	rCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	if err := ct.Raw.Restore(rCtx); err != nil {
		return "", fmt.Errorf("failed to restore CREATE TABLE: %w", err)
	}

	return sb.String(), nil
}

// deferrableIndexes returns ct's regular (non-UNIQUE, non-FULLTEXT,
// non-SPATIAL) indexes in declaration order, less the one kept to support an
// AUTO_INCREMENT column when preserveAutoIncrement is set, and less every one
// that can support a foreign key.
func deferrableIndexes(ct *CreateTable, preserveAutoIncrement bool) []*ast.Constraint {
	// InnoDB requires AUTO_INCREMENT to lead an index. Retained primary
	// and unique keys can satisfy it; otherwise keep one supporting regular
	// index. ParseCreateTable normalizes inline PRIMARY KEY and UNIQUE clauses.
	needsIndex := make(map[string]bool)
	for _, column := range ct.GetColumns() {
		if preserveAutoIncrement && column.AutoInc {
			needsIndex[strings.ToLower(column.Name)] = true
		}
	}
	for _, index := range ct.GetIndexes() {
		if (index.Type == "PRIMARY KEY" || index.Type == "UNIQUE") && len(index.ColumnList) > 0 {
			delete(needsIndex, strings.ToLower(index.ColumnList[0].Name))
		}
	}

	// Prefer the fewest key parts; ties retain source declaration order.
	support := make(map[string]*ast.Constraint)
	for _, constraint := range ct.Raw.Constraints {
		if constraint.Tp != ast.ConstraintKey && constraint.Tp != ast.ConstraintIndex {
			continue
		}
		if len(constraint.Keys) == 0 || constraint.Keys[0].Column == nil {
			continue
		}
		name := strings.ToLower(constraint.Keys[0].Column.Name.O)
		if needsIndex[name] && (support[name] == nil || len(constraint.Keys) < len(support[name].Keys)) {
			support[name] = constraint
		}
	}
	var deferrable []*ast.Constraint
	for _, constraint := range ct.Raw.Constraints {
		if constraint.Tp != ast.ConstraintKey && constraint.Tp != ast.ConstraintIndex {
			continue
		}
		if len(constraint.Keys) > 0 && constraint.Keys[0].Column != nil && support[strings.ToLower(constraint.Keys[0].Column.Name.O)] == constraint {
			continue
		}
		if supportsForeignKey(ct, constraint) {
			continue
		}
		deferrable = append(deferrable, constraint)
	}
	return deferrable
}

// supportsForeignKey reports whether index could be the index one of ct's
// foreign keys uses: its leading key parts are that foreign key's columns, in
// order. InnoDB requires such an index and refuses to drop the last one
// (error 1553), so none of them is deferred. Keeping every candidate, not only
// the one MySQL would pick, is the conservative choice: it never leaves a
// foreign key without an index.
func supportsForeignKey(ct *CreateTable, index *ast.Constraint) bool {
	for _, fk := range ct.Raw.Constraints {
		if fk.Tp != ast.ConstraintForeignKey || len(fk.Keys) == 0 || len(index.Keys) < len(fk.Keys) {
			continue
		}
		matches := true
		for i, key := range fk.Keys {
			part := index.Keys[i]
			if key.Column == nil || part.Column == nil || !strings.EqualFold(key.Column.Name.O, part.Column.Name.O) {
				matches = false
				break
			}
		}
		if matches {
			return true
		}
	}
	return false
}

// GetMissingSecondaryIndexes compares two CREATE TABLE statements (source and target)
// and returns an ALTER TABLE statement that adds any missing secondary indexes.
// Returns an empty string if no indexes need to be added.
// Considers UNIQUE, FULLTEXT, SPATIAL, and regular INDEX types. PRIMARY KEY is excluded as it's fundamental to table structure.
func GetMissingSecondaryIndexes(sourceCreateTable, targetCreateTable, tableName string) (string, error) {
	// Parse both CREATE TABLE statements
	sourceCT, err := ParseCreateTable(sourceCreateTable)
	if err != nil {
		return "", fmt.Errorf("failed to parse source CREATE TABLE: %w", err)
	}

	targetCT, err := ParseCreateTable(targetCreateTable)
	if err != nil {
		return "", fmt.Errorf("failed to parse target CREATE TABLE: %w", err)
	}

	// Build a map of existing indexes on the target (all secondary indexes)
	targetIndexes := make(map[string]bool)
	targetDefinitions := make(map[string]bool)
	for _, constraint := range targetCT.Raw.Constraints {
		// Skip PRIMARY KEY, but include all other index types
		if constraint.Tp == ast.ConstraintPrimaryKey || !isIndex(constraint) {
			continue
		}
		if constraint.Name != "" {
			targetIndexes[constraint.Name] = true
		}
		definition, err := unnamedIndexDefinition(constraint)
		if err != nil {
			return "", err
		}
		targetDefinitions[definition] = true
	}

	// Find missing indexes from source
	var missingIndexes []*ast.Constraint
	for _, constraint := range sourceCT.Raw.Constraints {
		// Consider all secondary indexes (UNIQUE, FULLTEXT, and regular INDEX)
		// Skip only PRIMARY KEY as it's fundamental to table structure
		if constraint.Tp == ast.ConstraintPrimaryKey {
			continue
		}
		// Include: UNIQUE, FULLTEXT, SPATIAL, and regular INDEX
		if !isIndex(constraint) {
			continue
		}

		// Check if this index exists on target
		exists := targetIndexes[constraint.Name]
		if constraint.Name == "" {
			definition, err := unnamedIndexDefinition(constraint)
			if err != nil {
				return "", err
			}
			exists = targetDefinitions[definition]
		}
		if !exists {
			missingIndexes = append(missingIndexes, constraint)
		}
	}

	// If no missing indexes, return empty string
	if len(missingIndexes) == 0 {
		return "", nil
	}

	// Build a single ALTER TABLE statement with all missing indexes
	var alterClauses []string
	for _, constraint := range missingIndexes {
		var sb strings.Builder

		// Add appropriate keyword based on constraint type
		switch constraint.Tp { //nolint:exhaustive
		case ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
			sb.WriteString("ADD UNIQUE INDEX")
		case ast.ConstraintFulltext:
			sb.WriteString("ADD FULLTEXT INDEX")
		case ast.ConstraintSpatial:
			sb.WriteString("ADD SPATIAL INDEX")
		default: // ast.ConstraintKey, ast.ConstraintIndex
			sb.WriteString("ADD INDEX")
		}

		// Add index name if present
		if constraint.Name != "" {
			fmt.Fprintf(&sb, " %s", sqlescape.EscapeIdentifier(constraint.Name))
		}

		// Add columns
		sb.WriteString(" (")
		for i, key := range constraint.Keys {
			if i > 0 {
				sb.WriteString(", ")
			}
			switch {
			case key.Column != nil:
				// Regular column reference
				sb.WriteString(sqlescape.EscapeIdentifier(key.Column.Name.String()))
				// Add length if specified
				if key.Length > 0 {
					fmt.Fprintf(&sb, "(%d)", key.Length)
				}
			case key.Expr != nil:
				// Functional (expression) index part, e.g. KEY ((lower(b))).
				// Column is nil in this case; MySQL requires the expression
				// to be wrapped in its own parentheses.
				exprText, ok := restoreExprText(key.Expr, format.DefaultRestoreFlags)
				if !ok {
					return "", fmt.Errorf("failed to restore expression for index %q", constraint.Name)
				}
				fmt.Fprintf(&sb, "(%s)", exprText)
			default:
				return "", fmt.Errorf("index %q has a key part with neither a column nor an expression", constraint.Name)
			}
			// Descending key part (MySQL 8.0+). ASC is MySQL's canonical
			// default and is never emitted explicitly.
			if key.Desc {
				sb.WriteString(" DESC")
			}
		}
		sb.WriteString(")")

		// Add index options if present
		if constraint.Option != nil {
			opt := constraint.Option

			// Add USING clause
			if opt.Tp != ast.IndexTypeInvalid && opt.Tp.String() != "" {
				sb.WriteString(" USING " + opt.Tp.String())
			}

			// Add KEY_BLOCK_SIZE
			if opt.KeyBlockSize > 0 {
				fmt.Fprintf(&sb, " KEY_BLOCK_SIZE=%d", opt.KeyBlockSize)
			}

			// Add WITH PARSER (FULLTEXT indexes)
			if opt.ParserName.L != "" {
				fmt.Fprintf(&sb, " WITH PARSER %s", opt.ParserName.String())
			}

			// Add COMMENT
			if opt.Comment != "" {
				fmt.Fprintf(&sb, " COMMENT '%s'", sqlescape.EscapeString(opt.Comment))
			}

			// Add VISIBLE/INVISIBLE
			if opt.Visibility == ast.IndexVisibilityInvisible {
				sb.WriteString(" INVISIBLE")
			}
		}
		alterClauses = append(alterClauses, sb.String())
	}
	// Combine all ADD INDEX clauses into a single ALTER TABLE statement
	return fmt.Sprintf("ALTER TABLE %s %s", sqlescape.EscapeIdentifier(tableName), strings.Join(alterClauses, ", ")), nil
}

// unnamedIndexDefinition compares unnamed indexes by their full definition,
// including key order, expressions, prefix lengths, and options.
// isIndex reports whether constraint is an index. A CHECK or FOREIGN KEY
// constraint is not, and its name is in a separate namespace from index
// names: a table may have KEY positive (amount) and CONSTRAINT positive
// CHECK (amount >= 0), so a constraint's name must never stand in for an
// index's.
func isIndex(constraint *ast.Constraint) bool {
	switch constraint.Tp { //nolint:exhaustive
	case ast.ConstraintPrimaryKey, ast.ConstraintKey, ast.ConstraintIndex,
		ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex,
		ast.ConstraintFulltext, ast.ConstraintSpatial:
		return true
	}
	return false
}

func unnamedIndexDefinition(constraint *ast.Constraint) (string, error) {
	bare := *constraint
	bare.Name = ""
	switch bare.Tp { //nolint:exhaustive
	case ast.ConstraintKey:
		bare.Tp = ast.ConstraintIndex
	case ast.ConstraintUniq, ast.ConstraintUniqKey:
		bare.Tp = ast.ConstraintUniqIndex
	}
	var sb strings.Builder
	if err := bare.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
		return "", fmt.Errorf("failed to restore index definition: %w", err)
	}
	return sb.String(), nil
}
