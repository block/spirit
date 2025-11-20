package check

import (
	"context"
	"errors"
	"log/slog"

	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

func init() {
	registerCheck("addforeignkey", addForeignKeyCheck, ScopePreflight)
	registerCheck("hasforeignkeys", hasForeignKeysCheck, ScopePreflight)
}

// The spirit OSC algorithm does not support foreign key constraints.
// That's either pre-existing foreign keys, or adding new ones.

func hasForeignKeysCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	sql := `SELECT * FROM information_schema.referential_constraints WHERE 
	(constraint_schema=? AND table_name=?)
	or (constraint_schema=? AND referenced_table_name=?)`
	rows, err := r.DB.QueryContext(ctx, sql, r.Table.SchemaName, r.Table.TableName, r.Table.SchemaName, r.Table.TableName)
	if err != nil {
		return err
	}
	defer rows.Close()
	if rows.Next() {
		return errors.New("tables with existing foreign key constraints are not supported")
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
