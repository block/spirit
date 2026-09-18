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
	ct, err := ParseCreateTable(createStmt)
	if err != nil {
		return "", fmt.Errorf("failed to parse CREATE TABLE: %w", err)
	}

	// InnoDB requires AUTO_INCREMENT to lead an index. Retained primary
	// and unique keys can satisfy it; otherwise keep one supporting regular
	// index. GetIndexes also includes inline PRIMARY KEY and UNIQUE clauses.
	needsIndex := make(map[string]bool)
	for _, column := range ct.GetColumns() {
		if column.AutoInc {
			needsIndex[strings.ToLower(column.Name)] = true
		}
	}
	for _, index := range ct.GetIndexes() {
		if (index.Type == "PRIMARY KEY" || index.Type == "UNIQUE") && len(index.ColumnList) > 0 {
			delete(needsIndex, strings.ToLower(index.ColumnList[0].Name))
		}
	}

	// Filter out regular INDEX entries from the constraints
	filteredConstraints := make([]*ast.Constraint, 0)
	for _, constraint := range ct.Raw.Constraints {
		// Keep everything except regular INDEX
		switch constraint.Tp { //nolint:exhaustive
		case ast.ConstraintKey, ast.ConstraintIndex:
			if len(constraint.Keys) == 0 || constraint.Keys[0].Column == nil {
				continue
			}
			name := strings.ToLower(constraint.Keys[0].Column.Name.O)
			if !needsIndex[name] {
				continue
			}
			delete(needsIndex, name)
			filteredConstraints = append(filteredConstraints, constraint)
		default:
			// Keep PRIMARY KEY, UNIQUE, FULLTEXT, SPATIAL, etc.
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
	for _, constraint := range targetCT.Raw.Constraints {
		// Skip PRIMARY KEY, but include all other index types
		if constraint.Tp == ast.ConstraintPrimaryKey {
			continue
		}
		targetIndexes[constraint.Name] = true
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
		if constraint.Tp != ast.ConstraintKey &&
			constraint.Tp != ast.ConstraintIndex &&
			constraint.Tp != ast.ConstraintUniq &&
			constraint.Tp != ast.ConstraintUniqKey &&
			constraint.Tp != ast.ConstraintUniqIndex &&
			constraint.Tp != ast.ConstraintFulltext &&
			constraint.Tp != ast.ConstraintSpatial {
			continue
		}

		// Check if this index exists on target
		if !targetIndexes[constraint.Name] {
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
				var exprSb strings.Builder
				rCtx := format.NewRestoreCtx(format.DefaultRestoreFlags|format.RestoreStringWithoutCharset, &exprSb)
				if err := key.Expr.Restore(rCtx); err != nil {
					return "", fmt.Errorf("failed to restore expression for index %q: %w", constraint.Name, err)
				}
				fmt.Fprintf(&sb, "(%s)", exprSb.String())
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
