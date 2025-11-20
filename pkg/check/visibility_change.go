package check

import (
	"context"
	"errors"
	"log/slog"

	"github.com/block/spirit/pkg/statement"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

// ErrVisibilityMixedWithOtherChanges is returned when index visibility changes are mixed with table-rebuilding operations
var ErrVisibilityMixedWithOtherChanges = errors.New("the ALTER operation contains a change to index visibility mixed with table-rebuilding operations. This creates semantic issues for experiments. Please split the ALTER statement into separate statements for changing the invisible index and other operations")

func init() {
	registerCheck("visibility", visibilityCheck, ScopePostSetup)
}

// visibilityCheck validates index visibility changes in ALTER statements.
// It allows index visibility changes when mixed with other metadata-only operations,
// but blocks them when mixed with table-rebuilding operations to avoid semantic issues.
// It likely means the user is combining this operation with other unsafe operations,
// which is not a good idea. We need to protect them by not allowing it.
// https://github.com/block/spirit/issues/283
func visibilityCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	return AlterContainsIndexVisibility(r.Statement)
}

// AlterContainsIndexVisibility checks to see if there are any clauses of an ALTER to change index visibility.
// Allows index visibility changes when mixed with other metadata-only operations,
// but blocks them when mixed with table-rebuilding operations to avoid semantic issues.
func AlterContainsIndexVisibility(stmt *statement.AbstractStatement) error {
	alterStmt, ok := (*stmt.StmtNode).(*ast.AlterTableStmt)
	if !ok {
		return statement.ErrNotAlterTable
	}

	hasIndexVisibility := false
	hasNonMetadataOperation := false

	for _, spec := range alterStmt.Specs {
		switch spec.Tp {
		case ast.AlterTableIndexInvisible:
			hasIndexVisibility = true
		case ast.AlterTableDropIndex,
			ast.AlterTableRenameIndex,
			ast.AlterTableDropPartition,
			ast.AlterTableTruncatePartition,
			ast.AlterTableAddPartitions,
			ast.AlterTableAlterColumn:
			// These are safe metadata-only operations, don't block
			continue
		case ast.AlterTableModifyColumn, ast.AlterTableChangeColumn:
			// Only VARCHAR modifications are metadata-only, others are table-rebuilding
			if spec.NewColumns[0].Tp != nil && spec.NewColumns[0].Tp.GetType() == mysql.TypeVarchar {
				continue // VARCHAR is metadata-only
			}
			hasNonMetadataOperation = true
		default:
			// All other operations (ADD COLUMN, ADD INDEX, ADD CONSTRAINT, ENGINE, etc) are table-rebuilding
			hasNonMetadataOperation = true
		}
	}

	// Block index visibility if mixed with any table-rebuilding operations
	if hasIndexVisibility && hasNonMetadataOperation {
		return ErrVisibilityMixedWithOtherChanges
	}

	return nil
}
