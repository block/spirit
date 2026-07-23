package check

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

func init() {
	registerCheck("rename", renameCheck, ScopePreflight|ScopeStatement)
}

// renameCheck validates rename operations in ALTER TABLE statements.
// Table renames are always blocked. Column renames are allowed for
// non-PK columns in both the buffered and unbuffered copier paths.
// PK renames are blocked. Additionally, it blocks dangerous patterns
// where a rename's old or new name overlaps with an added column
// name, which could cause data corruption.
func renameCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	alterStmt, ok := (*r.Statement.StmtNode).(*ast.AlterTableStmt)
	if !ok {
		return errors.New("not a valid alter table statement")
	}

	// Build set of primary key columns for quick lookup (if table info is available).
	// MySQL column identifiers are case-insensitive, so all comparisons in this
	// check are performed on lower-cased names (the ALTER may type a column with
	// different case than it was declared with). The TiDB parser's Name.L is the
	// pre-lowercased form of Name.O.
	pkColumns := make(map[string]struct{})
	if r.Table != nil {
		for _, col := range r.Table.KeyColumns {
			pkColumns[strings.ToLower(col)] = struct{}{}
		}
	}

	// Collect all renames and added columns to check for dangerous overlaps.
	// All names are lower-cased.
	// renamedFrom maps old_name → new_name for all renames in this ALTER.
	renamedFrom := make(map[string]string)
	// renamedTo maps new_name → old_name (the reverse) for overlap detection.
	renamedTo := make(map[string]string)
	// addedColumns tracks column names being added in this ALTER.
	addedColumns := make(map[string]struct{})

	for _, spec := range alterStmt.Specs {
		// Table renames are never supported
		if spec.Tp == ast.AlterTableRenameTable {
			return errors.New("table renames are not supported")
		}

		if spec.Tp == ast.AlterTableRenameColumn {
			if spec.OldColumnName != nil {
				if _, isPK := pkColumns[spec.OldColumnName.Name.L]; isPK {
					return fmt.Errorf("renaming primary key column %q is not supported", spec.OldColumnName.Name.O)
				}
				// A case-only rename (foo → FOO) is not a rename for data-mapping
				// purposes, since identifiers are case-insensitive.
				if spec.NewColumnName != nil && spec.OldColumnName.Name.L != spec.NewColumnName.Name.L {
					renamedFrom[spec.OldColumnName.Name.L] = spec.NewColumnName.Name.L
					renamedTo[spec.NewColumnName.Name.L] = spec.OldColumnName.Name.L
				}
			}
			continue
		}

		if spec.Tp == ast.AlterTableChangeColumn {
			if spec.OldColumnName != nil && len(spec.NewColumns) > 0 {
				oldName := spec.OldColumnName.Name.L
				newName := spec.NewColumns[0].Name.Name.L
				if oldName != newName {
					if _, isPK := pkColumns[oldName]; isPK {
						return fmt.Errorf("renaming primary key column %q is not supported", spec.OldColumnName.Name.O)
					}
					renamedFrom[oldName] = newName
					renamedTo[newName] = oldName
				}
			}
			continue
		}

		if spec.Tp == ast.AlterTableAddColumns {
			for _, col := range spec.NewColumns {
				addedColumns[col.Name.Name.L] = struct{}{}
			}
		}
	}

	// Check for dangerous overlaps between renames and added columns.
	// Pattern 1: RENAME COLUMN c1 TO n1, ADD COLUMN c1 ...
	//   The old name "c1" is reused by a new column. The intersection logic
	//   could identity-match source.c1 to the new target.c1 instead of
	//   following the rename to target.n1.
	for oldName, newName := range renamedFrom {
		if _, added := addedColumns[oldName]; added {
			return fmt.Errorf("column rename %q to %q conflicts with added column %q: "+
				"the old column name is reused, which could cause data corruption", oldName, newName, oldName)
		}
	}

	// Pattern 2: RENAME COLUMN a TO c, ADD COLUMN ... (where c already exists)
	//   or more generally, a rename target name that collides with another
	//   source column. This is caught by MySQL itself (duplicate column name),
	//   but we check it defensively.
	for newName, oldName := range renamedTo {
		if _, added := addedColumns[newName]; added {
			return fmt.Errorf("column rename %q to %q conflicts with added column %q: "+
				"the new column name collides with an added column", oldName, newName, newName)
		}
	}

	return nil // no unsupported renames
}
