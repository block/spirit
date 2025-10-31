package lint

import (
	"fmt"

	"github.com/block/spirit/pkg/statement"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

type UnsafeLinter struct {
	allowUnsafe bool
}

func init() {
	Register(&UnsafeLinter{})
}

func (l *UnsafeLinter) String() string {
	return Stringer(l)
}

func (l *UnsafeLinter) Name() string {
	return "unsafe"
}

func (l *UnsafeLinter) Description() string {
	return "Detects usage of unsafe operations in database schema changes"
}

func (l *UnsafeLinter) Configure(config map[string]string) error {
	for k, v := range config {
		switch k {
		case "allowUnsafe":
			boolVal, err := ConfigBool(v, k)
			if err != nil {
				return err
			}

			l.allowUnsafe = boolVal
		default:
			return fmt.Errorf("unknown config key for %s: %s", l.Name(), k)
		}
	}

	return nil
}

func (l *UnsafeLinter) DefaultConfig() map[string]string {
	return map[string]string{
		"allowUnsafe": "false",
	}
}

func (l *UnsafeLinter) Lint(_ []*statement.CreateTable, changes []*statement.AbstractStatement) (violations []Violation) {
	if l.allowUnsafe {
		return nil // No violations if unsafe operations are allowed
	}
	for _, change := range changes {
		switch node := (*change.StmtNode).(type) {
		case *ast.TruncateTableStmt, *ast.DropTableStmt, *ast.DropDatabaseStmt:
			violations = append(violations, Violation{
				Linter:   l,
				Location: &Location{Table: change.Table},
				Message:  fmt.Sprintf("Unsafe operation detected: %q", node.OriginalText()),
				Severity: SeverityError,
			})
		case *ast.DropIndexStmt, *ast.RenameTableStmt:
			// In some definitions of "safe" these might be unsafe,
			// but since none lose data we consider them safe.
			continue
		case *ast.AlterTableStmt:
			for _, spec := range node.Specs {
				switch spec.Tp { //nolint: exhaustive
				case ast.AlterTableDropColumn, ast.AlterTableDropPrimaryKey, ast.AlterTableDropPartition, ast.AlterTableDiscardPartitionTablespace,
					ast.AlterTableDiscardTablespace, ast.AlterTableCoalescePartitions:
					violations = append(violations, Violation{
						Linter:   l,
						Location: &Location{Table: change.Table},
						Message:  "Unsafe operation detected: " + AlterTableTypeToString(spec.Tp),
						Severity: SeverityError,
					})
				case ast.AlterTableModifyColumn, ast.AlterTableChangeColumn:
					// These could be "lossy" changes in theory, but because spirit
					// doesn't support lossy changes we return false. In future
					// we might want to analyze the change to detect potential loss.
					//
					// As examples:
					// * If you shrink a VARCHAR(20) to VARCHAR(10), that is a lossy change.
					// But spirit will refuse to do it if any data has a length > 10.
					// If the data is all <= 10, then it's not lossy.
					// * If you change a column from INT to BIGINT, that is not lossy.
					// * If you change an ENUM/SET to remove values, that is lossy, but
					//   only if you used the values being removed.
					// * If you change character set/collation that could change uniqueness
					//   constraints, effectively making it lossy.
					//
					// The lossy vs. non-lossy is detection is done at runtime via a checksum.
					// It is not computed in advance.
					//
					// I do not believe that PlanetScale has this detection, so we may decide to
					// implement it here in future.
					/*
						v := Violation{
							Linter:   l,
							Location: &Location{Table: change.Table},
							Message:  "Changing column type could lead to data loss by truncation",
							Severity: SeverityWarning,
						}
						if spec.OldColumnName != nil {
							v.Location.Column = &spec.OldColumnName.Name.O
						} else if len(spec.NewColumns) > 0 {
							v.Location.Column = &spec.NewColumns[0].Name.Name.O
						}
						violations = append(violations, v)

					*/
				case ast.AlterTableDropForeignKey, ast.AlterTableRenameColumn,
					ast.AlterTableRenameTable, ast.AlterTableDropIndex, ast.AlterTableDropCheck,
					ast.AlterTableOption:
					// In some definitions of "safe" these might be unsafe,
					// but since none lose data we consider them safe.
					continue
				}
			}
		}
	}
	return violations
}
