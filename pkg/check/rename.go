package check

import (
	"context"
	"errors"
	"log/slog"

	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

func init() {
	registerCheck("rename", renameCheck, ScopePreflight)
}

// renameCheck checks for any renames, which are not supported.
func renameCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	alterStmt, ok := (*r.Statement.StmtNode).(*ast.AlterTableStmt)
	if !ok {
		return errors.New("not a valid alter table statement")
	}
	for _, spec := range alterStmt.Specs {
		if spec.Tp == ast.AlterTableRenameTable || spec.Tp == ast.AlterTableRenameColumn {
			return errors.New("renames are not supported")
		}
		// ALTER TABLE CHANGE COLUMN can be used to rename a column.
		// But they can also be used commonly without a rename, so the check needs to be deeper.
		if spec.Tp == ast.AlterTableChangeColumn {
			if spec.NewColumns[0].Name.String() != spec.OldColumnName.String() {
				return errors.New("renames are not supported")
			}
		}
	}
	return nil // no renames
}
