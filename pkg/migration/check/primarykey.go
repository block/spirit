package check

import (
	"context"
	"errors"
	"log/slog"

	"github.com/block/spirit/pkg/parser/ast"
)

func init() {
	registerCheck("primarykey", primaryKeyCheck, ScopePreflight|ScopeStatement)
}

func primaryKeyCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	alterStmt, ok := (*r.Statement.StmtNode).(*ast.AlterTableStmt)
	if !ok {
		return errors.New("not a valid alter table statement")
	}
	for _, spec := range alterStmt.Specs {
		if spec.Tp == ast.AlterTableDropPrimaryKey {
			return errors.New("dropping primary key is not supported")
		}
	}
	return nil // no problems
}
