package statement

import (
	"github.com/block/spirit/pkg/parser"
	"github.com/block/spirit/pkg/parser/ast"
)

// StatementType represents the type of a SQL statement.
type StatementType int

const (
	StatementUnknown       StatementType = iota
	StatementAlterTable                  // ALTER TABLE ...
	StatementCreateTable                 // CREATE TABLE ...
	StatementDropTable                   // DROP TABLE ...
	StatementRenameTable                 // RENAME TABLE ...
	StatementTruncateTable               // TRUNCATE TABLE ...
	StatementCreateIndex                 // CREATE INDEX ...
	StatementDropIndex                   // DROP INDEX ...
	StatementCreateView                  // CREATE VIEW ...
	StatementInsert                      // INSERT ...
	StatementUpdate                      // UPDATE ...
	StatementDelete                      // DELETE ...
)

// String returns the human-readable name for a StatementType.
func (t StatementType) String() string {
	switch t {
	case StatementUnknown:
		return "UNKNOWN"
	case StatementAlterTable:
		return "ALTER TABLE"
	case StatementCreateTable:
		return "CREATE TABLE"
	case StatementDropTable:
		return "DROP TABLE"
	case StatementRenameTable:
		return "RENAME TABLE"
	case StatementTruncateTable:
		return "TRUNCATE TABLE"
	case StatementCreateIndex:
		return "CREATE INDEX"
	case StatementDropIndex:
		return "DROP INDEX"
	case StatementCreateView:
		return "CREATE VIEW"
	case StatementInsert:
		return "INSERT"
	case StatementUpdate:
		return "UPDATE"
	case StatementDelete:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

// IsDDL returns true if this is a DDL statement type.
func (t StatementType) IsDDL() bool {
	switch t { //nolint:exhaustive
	case StatementAlterTable, StatementCreateTable, StatementDropTable,
		StatementRenameTable, StatementTruncateTable, StatementCreateIndex,
		StatementDropIndex, StatementCreateView:
		return true
	}
	return false
}

// IsDML returns true if this is a DML statement type.
func (t StatementType) IsDML() bool {
	switch t { //nolint:exhaustive
	case StatementInsert, StatementUpdate, StatementDelete:
		return true
	}
	return false
}

// Classification holds the result of classifying a single SQL statement.
type Classification struct {
	Type   StatementType
	Table  string // First table referenced (empty for unparseable statements)
	Schema string // Schema if fully qualified (e.g. "test" from "test.t1")
}

// Classify parses one or more SQL statements and returns their classifications.
// Unlike New(), this accepts any statement type including DML and TRUNCATE.
func Classify(sql string) ([]Classification, error) {
	p := parser.New()
	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return nil, err
	}
	if len(stmtNodes) == 0 {
		return nil, ErrNoStatements
	}

	results := make([]Classification, 0, len(stmtNodes))
	for _, node := range stmtNodes {
		results = append(results, classifyNode(node))
	}
	return results, nil
}

// classifyNode classifies a single parsed AST node.
func classifyNode(node ast.StmtNode) Classification {
	switch stmt := node.(type) {
	case *ast.AlterTableStmt:
		return Classification{
			Type:   StatementAlterTable,
			Table:  stmt.Table.Name.String(),
			Schema: stmt.Table.Schema.String(),
		}
	case *ast.CreateTableStmt:
		return Classification{
			Type:   StatementCreateTable,
			Table:  stmt.Table.Name.String(),
			Schema: stmt.Table.Schema.String(),
		}
	case *ast.DropTableStmt:
		c := Classification{Type: StatementDropTable}
		if len(stmt.Tables) > 0 {
			c.Table = stmt.Tables[0].Name.String()
			c.Schema = stmt.Tables[0].Schema.String()
		}
		return c
	case *ast.RenameTableStmt:
		c := Classification{Type: StatementRenameTable}
		if len(stmt.TableToTables) > 0 {
			c.Table = stmt.TableToTables[0].OldTable.Name.String()
			c.Schema = stmt.TableToTables[0].OldTable.Schema.String()
		}
		return c
	case *ast.TruncateTableStmt:
		return Classification{
			Type:   StatementTruncateTable,
			Table:  stmt.Table.Name.String(),
			Schema: stmt.Table.Schema.String(),
		}
	case *ast.CreateIndexStmt:
		return Classification{
			Type:   StatementCreateIndex,
			Table:  stmt.Table.Name.String(),
			Schema: stmt.Table.Schema.String(),
		}
	case *ast.DropIndexStmt:
		return Classification{
			Type:   StatementDropIndex,
			Table:  stmt.Table.Name.String(),
			Schema: stmt.Table.Schema.String(),
		}
	case *ast.CreateViewStmt:
		return Classification{
			Type:   StatementCreateView,
			Table:  stmt.ViewName.Name.String(),
			Schema: stmt.ViewName.Schema.String(),
		}
	case *ast.InsertStmt:
		return classifyDMLWithTableRefs(StatementInsert, stmt.Table)
	case *ast.UpdateStmt:
		return classifyDMLWithTableRefs(StatementUpdate, stmt.TableRefs)
	case *ast.DeleteStmt:
		return classifyDMLWithTableRefs(StatementDelete, stmt.TableRefs)
	default:
		return Classification{Type: StatementUnknown}
	}
}

// classifyDMLWithTableRefs extracts table info from a DML statement's TableRefs.
func classifyDMLWithTableRefs(typ StatementType, refs *ast.TableRefsClause) Classification {
	c := Classification{Type: typ}
	if refs == nil || refs.TableRefs == nil {
		return c
	}
	if src, ok := refs.TableRefs.Left.(*ast.TableSource); ok {
		if tn, ok := src.Source.(*ast.TableName); ok {
			c.Table = tn.Name.String()
			c.Schema = tn.Schema.String()
		}
	}
	return c
}
