// Package statement is a wrapper around the parser with some added functionality.
package statement

import (
	"errors"
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/mysql"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

type AbstractStatement struct {
	Schema    string // this will be empty unless the table name is fully qualified (ALTER TABLE test.t1 ...)
	Table     string // for statements that affect multiple tables (DROP TABLE t1, t2), only the first is set here!
	Alter     string // may be empty.
	Statement string
	StmtNode  *ast.StmtNode
}

var (
	ErrNotSupportedStatement = errors.New("not a supported statement type")
	ErrNotAlterTable         = errors.New("not an ALTER TABLE statement")
)

func New(statement string) ([]*AbstractStatement, error) {
	p := parser.New()
	stmtNodes, _, err := p.Parse(statement, "", "")
	if err != nil {
		return nil, err
	}
	stmts := make([]*AbstractStatement, 0, len(stmtNodes))
	var mustBeOnlyStatement bool
	for i, node := range stmtNodes {
		switch node := node.(type) {
		case *ast.AlterTableStmt:
			// type assert node as an AlterTableStmt and then
			// extract the table and alter from it.
			alterStmt := node
			// if the schema name is included it could be different from the --database
			// specified, which causes all sorts of problems. The easiest way to handle this
			// it just to not permit it.
			var sb strings.Builder
			sb.Reset()
			rCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
			if err = alterStmt.Restore(rCtx); err != nil {
				return nil, fmt.Errorf("could not restore alter clause statement: %s", err)
			}
			normalizedStmt := sb.String()
			trimLen := len(alterStmt.Table.Name.String()) + 15 // len ALTER TABLE + quotes
			if len(alterStmt.Table.Schema.String()) > 0 {
				trimLen += len(alterStmt.Table.Schema.String()) + 3 // len schema + quotes and dot.
			}
			stmts = append(stmts, &AbstractStatement{
				Schema:    alterStmt.Table.Schema.String(),
				Table:     alterStmt.Table.Name.String(),
				Alter:     normalizedStmt[trimLen:],
				Statement: statement,
				StmtNode:  &stmtNodes[i],
			})
		case *ast.CreateIndexStmt:
			// Need to rewrite to a corresponding ALTER TABLE statement
			stmt, err := convertCreateIndexToAlterTable(node)
			if err != nil {
				return nil, err
			}
			stmts = append(stmts, stmt)
		// returning an empty alter means we are allowed to run it
		// but it's not a spirit migration. But the table should be specified.
		case *ast.CreateTableStmt:
			mustBeOnlyStatement = true
			stmts = append(stmts, &AbstractStatement{
				Schema:    node.Table.Schema.String(),
				Table:     node.Table.Name.String(),
				Statement: statement,
				StmtNode:  &stmtNodes[i],
			})
		case *ast.DropTableStmt:
			mustBeOnlyStatement = true
			distinctSchemas := make(map[string]struct{})
			for _, table := range node.Tables {
				distinctSchemas[table.Schema.String()] = struct{}{}
			}
			if len(distinctSchemas) > 1 {
				return nil, errors.New("statement attempts to drop tables from multiple schemas")
			}
			stmts = append(stmts, &AbstractStatement{
				Schema:    node.Tables[0].Schema.String(),
				Table:     node.Tables[0].Name.String(),
				Statement: statement,
				StmtNode:  &stmtNodes[i],
			})
		case *ast.RenameTableStmt:
			mustBeOnlyStatement = true
			stmt := node
			distinctSchemas := make(map[string]struct{})
			for _, clause := range stmt.TableToTables {
				if clause.OldTable.Schema.String() != clause.NewTable.Schema.String() {
					return nil, errors.New("statement attempts to move table between schemas")
				}
				distinctSchemas[clause.OldTable.Schema.String()] = struct{}{}
			}
			if len(distinctSchemas) > 1 {
				return nil, errors.New("statement attempts to rename tables in multiple schemas")
			}
			stmts = append(stmts, &AbstractStatement{
				Schema:    stmt.TableToTables[0].OldTable.Schema.String(),
				Table:     stmt.TableToTables[0].OldTable.Name.String(),
				Statement: statement,
				StmtNode:  &stmtNodes[i],
			})
		}
	}

	if len(stmts) > 1 && mustBeOnlyStatement {
		return nil, errors.New("statement must be executed alone")
	}
	if len(stmts) < 1 {
		return nil, errors.New("could not find any compatible statements to execute")
	}

	return stmts, nil
}

// MustNew is like New but panics if the statement cannot be parsed.
// It is used by tests.
func MustNew(statement string) []*AbstractStatement {
	stmt, err := New(statement)
	if err != nil {
		panic(err)
	}
	return stmt
}

func (a *AbstractStatement) IsAlterTable() bool {
	_, ok := (*a.StmtNode).(*ast.AlterTableStmt)
	return ok
}

// AlgorithmInplaceConsideredSafe checks to see if all clauses of an ALTER
// statement are "safe". We consider an operation to be "safe" if it is "In
// Place" and "Only Modifies Metadata". See
// https://dev.mysql.com/doc/refman/8.0/en/innodb-online-ddl-operations.html
// for details.
// INPLACE DDL is not generally safe for online use in MySQL 8.0, because ADD
// INDEX can block replicas.
func (a *AbstractStatement) AlgorithmInplaceConsideredSafe() error {
	alterStmt, ok := (*a.StmtNode).(*ast.AlterTableStmt)
	if !ok {
		return ErrNotAlterTable
	}
	// There can be multiple clauses in a single ALTER TABLE statement.
	// If all of them are safe, we can attempt to use INPLACE.
	unsafeClauses := 0
	for _, spec := range alterStmt.Specs {
		switch spec.Tp {
		case ast.AlterTableRenameIndex,
			ast.AlterTableIndexInvisible,
			ast.AlterTableDropPartition,
			ast.AlterTableTruncatePartition,
			ast.AlterTableAddPartitions,
			ast.AlterTableDropIndex:
			continue
		case ast.AlterTableModifyColumn, ast.AlterTableChangeColumn:
			// Only safe if changing length of a VARCHAR column. We don't know the type of the column
			// or its length, so we cannot determine if this is safe only by parsing. We can simply try
			// INPLACE, and if it fails we will retry with our own schema change process.
			if spec.NewColumns[0].Tp != nil && spec.NewColumns[0].Tp.GetType() == mysql.TypeVarchar {
				continue
			}
			unsafeClauses++
		default:
			unsafeClauses++
		}
	}
	if unsafeClauses > 0 {
		if len(alterStmt.Specs) > 1 {
			return errors.New("ALTER contains multiple clauses. Combinations of INSTANT and INPLACE operations cannot be detected safely. Consider executing these as separate ALTER statements")
		}
		return errors.New("ALTER statement contains operations that are not safe for INPLACE algorithm")
	}
	return nil
}

// AlterContainsUnsupportedClause checks to see if any clauses of an ALTER
// statement are unsupported by Spirit. These include clauses like ALGORITHM
// and LOCK, because they step on the toes of Spirit's own locking and
// algorithm selection.
func (a *AbstractStatement) AlterContainsUnsupportedClause() error {
	alterStmt, ok := (*a.StmtNode).(*ast.AlterTableStmt)
	if !ok {
		return ErrNotAlterTable
	}
	var unsupportedClauses []string
	for _, spec := range alterStmt.Specs {
		switch spec.Tp {
		case ast.AlterTableAlgorithm:
			unsupportedClauses = append(unsupportedClauses, "ALGORITHM=")
		case ast.AlterTableLock:
			unsupportedClauses = append(unsupportedClauses, "LOCK=")
		default:
		}
	}
	if len(unsupportedClauses) > 0 {
		unsupportedClause := strings.Join(unsupportedClauses, ", ")
		return fmt.Errorf("ALTER contains unsupported clause(s): %s", unsupportedClause)
	}
	return nil
}

// AlterContainsAddUnique checks to see if any clauses of an ALTER contains add UNIQUE index.
// We use this to customize the error returned from checksum fails.
func (a *AbstractStatement) AlterContainsAddUnique() error {
	alterStmt, ok := (*a.StmtNode).(*ast.AlterTableStmt)
	if !ok {
		return ErrNotAlterTable
	}
	for _, spec := range alterStmt.Specs {
		if spec.Tp == ast.AlterTableAddConstraint && spec.Constraint.Tp == ast.ConstraintUniq {
			return errors.New("contains adding a unique index")
		}
	}
	return nil
}

// AlterContainsIndexVisibility checks to see if there are any clauses of an ALTER to change index visibility.
// It now allows index visibility changes when mixed with other metadata-only operations,
// but blocks them when mixed with table-rebuilding operations to avoid semantic issues.
func (a *AbstractStatement) AlterContainsIndexVisibility() error {
	alterStmt, ok := (*a.StmtNode).(*ast.AlterTableStmt)
	if !ok {
		return ErrNotAlterTable
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
			// These are metadata-only operations - safe to mix with index visibility
			continue
		case ast.AlterTableModifyColumn, ast.AlterTableChangeColumn:
			// Only safe if changing length of a VARCHAR column
			if spec.NewColumns[0].Tp != nil && spec.NewColumns[0].Tp.GetType() == mysql.TypeVarchar {
				continue
			}
			hasNonMetadataOperation = true
		case ast.AlterTableAddConstraint: // ADD INDEX operations are table-rebuilding
			hasNonMetadataOperation = true
		default:
			// All other operations are considered non-metadata (table rebuilding)
			hasNonMetadataOperation = true
		}
	}

	// Only fail if index visibility is mixed with non-metadata operations
	if hasIndexVisibility && hasNonMetadataOperation {
		return errors.New("the ALTER operation contains a change to index visibility mixed with table-rebuilding operations. This creates semantic issues for experiments. Please split the ALTER statement into separate statements for changing the invisible index and other operations")
	}

	return nil
}

func (a *AbstractStatement) TrimAlter() string {
	return strings.TrimSuffix(strings.TrimSpace(a.Alter), ";")
}

func convertCreateIndexToAlterTable(stmt ast.StmtNode) (*AbstractStatement, error) {
	ciStmt, isCreateIndexStmt := stmt.(*ast.CreateIndexStmt)
	if !isCreateIndexStmt {
		return nil, errors.New("not a CREATE INDEX statement")
	}
	var columns []string
	var keyType string
	for _, part := range ciStmt.IndexPartSpecifications {
		if part.Column == nil {
			return nil, errors.New("cannot convert functional index to ALTER TABLE statement; please use ALTER TABLE ADD INDEX … instead")
		}
		columns = append(columns, part.Column.Name.String())
	}
	switch ciStmt.KeyType {
	case ast.IndexKeyTypeUnique:
		keyType = "UNIQUE INDEX"
	case ast.IndexKeyTypeFulltext:
		keyType = "FULLTEXT INDEX"
	case ast.IndexKeyTypeSpatial:
		keyType = "SPATIAL INDEX"
	default:
		keyType = "INDEX"
	}
	alterStmt := fmt.Sprintf("ADD %s %s (%s)", keyType, ciStmt.IndexName, strings.Join(columns, ", "))
	// We hint in the statement that it's been rewritten
	// and in the stmtNode we reparse from the alterStmt.
	// TODO: identifiers should be quoted/escaped in case a maniac includes a backtick in a table name.
	statement := fmt.Sprintf("/* rewritten from CREATE INDEX */ ALTER TABLE `%s` %s", ciStmt.Table.Name, alterStmt)
	p := parser.New()
	stmtNodes, _, err := p.Parse(statement, "", "")
	if err != nil {
		return nil, errors.New("could not parse SQL statement: " + statement)
	}
	if len(stmtNodes) != 1 {
		return nil, errors.New("only one statement may be specified at once")
	}
	return &AbstractStatement{
		Schema:    ciStmt.Table.Schema.String(),
		Table:     ciStmt.Table.Name.String(),
		Alter:     alterStmt,
		Statement: statement,
		StmtNode:  &stmtNodes[0],
	}, err
}
