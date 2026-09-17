package lint

import (
	"fmt"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/statement"
)

// UnsafeLinter detects operations this linter's definition of "safe" excludes.
//
// By default that definition is about data: an operation is unsafe when it
// destroys rows. Removals and renames that lose no data — dropping an index, a
// foreign key, a check or any other named constraint, and renaming a table,
// column or index — are safe under it, even though each one removes something
// a reader may depend on.
//
// includeNonLossyRemovals selects the other definition, for callers whose
// exposure is availability rather than data. Dropping an index that live
// queries plan around loses no rows and can still take a database down, and
// because the drop is metadata-only it completes in milliseconds with nothing
// looking wrong. A rename is a drop and an add to every client still reading
// the old name. The wider definition is off by default, so the two arms below
// stay exactly as permissive as they have always been unless a caller asks
// otherwise.
type UnsafeLinter struct {
	allowUnsafe             bool
	includeNonLossyRemovals bool
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
		case "includeNonLossyRemovals":
			boolVal, err := ConfigBool(v, k)
			if err != nil {
				return err
			}

			l.includeNonLossyRemovals = boolVal
		default:
			return fmt.Errorf("unknown config key for %s: %s", l.Name(), k)
		}
	}

	return nil
}

func (l *UnsafeLinter) DefaultConfig() map[string]string {
	return map[string]string{
		"allowUnsafe":             "false",
		"includeNonLossyRemovals": "false",
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
			if !l.includeNonLossyRemovals {
				continue
			}

			violations = append(violations, Violation{
				Linter:   l,
				Location: &Location{Table: change.Table},
				Message:  fmt.Sprintf("Unsafe operation detected: %q", node.OriginalText()),
				Severity: SeverityError,
			})
		case *ast.AlterTableStmt:
			for _, spec := range node.Specs {
				switch spec.Tp { //nolint: exhaustive
				case ast.AlterTableDropColumn:
					violations = append(violations, unsafeDropColumnViolation(l, change.Table, spec))
				case ast.AlterTableDropPrimaryKey, ast.AlterTableDropPartition, ast.AlterTableTruncatePartition,
					ast.AlterTableDiscardPartitionTablespace, ast.AlterTableDiscardTablespace, ast.AlterTableCoalescePartitions:
					violations = append(violations, Violation{
						Linter:   l,
						Location: &Location{Table: change.Table},
						Message:  fmt.Sprintf("Unsafe operation detected: %q", AlterTableTypeToString(spec.Tp)),
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
				case ast.AlterTableOption:
					// A table option removes nothing, so it is safe under both
					// definitions. It is also what a differ emits for routine
					// convergence on engine, charset or row format, and refusing
					// it would strand a schema on a property mismatch.
					continue
				case ast.AlterTableDropForeignKey, ast.AlterTableRenameColumn,
					ast.AlterTableRenameTable, ast.AlterTableDropIndex, ast.AlterTableDropCheck,
					ast.AlterTableDropConstraint, ast.AlterTableRenameIndex:
					// In some definitions of "safe" these might be unsafe,
					// but since none lose data we consider them safe.
					if !l.includeNonLossyRemovals {
						continue
					}

					violations = append(violations, nonLossyRemovalViolation(l, change.Table, spec))
				}
			}
		}
	}
	return violations
}

func unsafeDropColumnViolation(l *UnsafeLinter, tableName string, spec *ast.AlterTableSpec) Violation {
	location := &Location{Table: tableName}
	operation := "DROP COLUMN"
	if spec.OldColumnName != nil {
		columnName := spec.OldColumnName.Name.O
		location.Column = &columnName
		operation += " " + sqlescape.EscapeIdentifier(columnName)
	}

	return Violation{
		Linter:   l,
		Location: location,
		Message:  fmt.Sprintf("Unsafe operation detected: %q", operation),
		Severity: SeverityError,
	}
}

// nonLossyRemovalViolation renders a violation for an ALTER TABLE clause that
// removes or renames a named schema object without losing rows. The object is
// named in the message and, where the clause identifies which kind of object it
// acts on, in the location too, so a caller can report what a reader is about to
// lose access to rather than only which table it was on.
func nonLossyRemovalViolation(l *UnsafeLinter, tableName string, spec *ast.AlterTableSpec) Violation {
	location := &Location{Table: tableName}
	operation := AlterTableTypeToString(spec.Tp)

	if name := nonLossyRemovalTarget(spec, location); name != "" {
		operation += " " + sqlescape.EscapeIdentifier(name)
	}

	return Violation{
		Linter:   l,
		Location: location,
		Message:  fmt.Sprintf("Unsafe operation detected: %q", operation),
		Severity: SeverityError,
	}
}

// nonLossyRemovalTarget names the object a non-lossy removal or rename clause
// acts on, and records it on the location under the kind of object it is. The
// name is empty for a clause the parser left unnamed, which callers render as
// the bare operation.
//
// RENAME TABLE names the table it is moving to: the name it is moving from is
// the statement's own table, which the location already carries.
func nonLossyRemovalTarget(spec *ast.AlterTableSpec, location *Location) string {
	switch spec.Tp { //nolint: exhaustive
	case ast.AlterTableDropIndex:
		location.Index = &spec.Name
		return spec.Name
	case ast.AlterTableRenameIndex:
		if spec.FromKey.O == "" {
			return ""
		}

		name := spec.FromKey.O
		location.Index = &name

		return name
	case ast.AlterTableDropForeignKey:
		location.Constraint = &spec.Name
		return spec.Name
	case ast.AlterTableDropCheck, ast.AlterTableDropConstraint:
		if spec.Constraint == nil {
			return ""
		}

		location.Constraint = &spec.Constraint.Name

		return spec.Constraint.Name
	case ast.AlterTableRenameColumn:
		if spec.OldColumnName == nil {
			return ""
		}

		name := spec.OldColumnName.Name.O
		location.Column = &name

		return name
	case ast.AlterTableRenameTable:
		if spec.NewTable == nil {
			return ""
		}

		return spec.NewTable.Name.O
	default:
		return ""
	}
}
