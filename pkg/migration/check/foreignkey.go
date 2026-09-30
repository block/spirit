package check

import (
	"context"
	"errors"
	"log/slog"

	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/utils"
)

func init() {
	registerCheck("addforeignkey", addForeignKeyCheck, ScopePreflight|ScopeStatement)
	// Re-run before cutover and again under the cutover lock: the binlog
	// clients cancel on a foreign key they can parse, but skip statements
	// they cannot, and are not acted on once the cutover starts. An inbound
	// foreign key added during the migration would follow the cutover
	// RENAME to the _old table.
	registerCheck("hasforeignkeys", hasForeignKeysCheck, ScopePreflight|ScopeCutover|ScopeCutoverLocked)
}

// The spirit OSC algorithm does not support foreign key constraints.
// That's either pre-existing foreign keys, or adding new ones.

// hasForeignKeysCheck refuses a table that is either end of a foreign key
// relationship.
//
// In referential_constraints, constraint_schema is the *child* table's schema
// and unique_constraint_schema is the *referenced* (parent) table's schema, so
// the two halves have to be matched on different columns. Binding both to the
// migrated table's schema - as this check used to - makes an inbound foreign
// key from a child in another schema invisible, and the migration runs. MySQL
// then follows the cutover RENAME and repoints that child's foreign key at the
// _old table, which spirit cannot drop and which no longer receives writes:
// referential integrity ends up enforced against a stale snapshot. See #1182.
func hasForeignKeysCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	sql := `SELECT 1 FROM information_schema.referential_constraints WHERE
	(constraint_schema=? AND table_name=?)
	or (unique_constraint_schema=? AND referenced_table_name=?)
	LIMIT 1`
	rows, err := r.DB.QueryContext(ctx, sql, r.Table.SchemaName, r.Table.TableName, r.Table.SchemaName, r.Table.TableName)
	if err != nil {
		return err
	}
	defer utils.CloseAndLog(rows)
	if rows.Next() {
		if r.scope&(ScopeCutover|ScopeCutoverLocked) != 0 {
			return refuse(errors.New("a foreign key was created during the migration: tables with existing foreign key constraints are not supported"))
		}
		return refuse(errors.New("tables with existing foreign key constraints are not supported"))
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	return nil
}

func addForeignKeyCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	alterStmt, ok := (*r.Statement.StmtNode).(*ast.AlterTableStmt)
	if !ok {
		return errors.New("not a valid alter table statement")
	}
	for _, spec := range alterStmt.Specs {
		if spec.Constraint != nil && spec.Constraint.Refer != nil {
			return errors.New("adding foreign key constraints is not supported")
		}
		if spec.NewConstraints != nil {
			for _, constraint := range spec.NewConstraints {
				if constraint.Refer != nil {
					return errors.New("adding foreign key constraints is not supported")
				}
			}
		}
	}
	return nil // no problems
}
