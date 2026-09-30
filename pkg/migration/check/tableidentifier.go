package check

import (
	"context"
	"errors"
	"log/slog"

	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/utils"
)

// Tagged ScopeStatement: the refusal depends only on names the statement
// carries, so it needs no database connection. The migration runner runs the
// statement-scope checks before it attempts MySQL's native DDL, so a name this
// check refuses is refused for every ALTER, including one the native DDL could
// complete, and no earlier stage can bypass it.
func init() {
	registerCheck("tableidentifier", tableIdentifierCheck, ScopePreflight|ScopeStatement)
}

// namedIdentifier is a schema or table name the statement touches, with a
// description of its role for the error message.
type namedIdentifier struct {
	kind string
	name string
}

// tableIdentifierCheck refuses a schema or table name containing a '.' or a
// backtick (see utils.UnsupportedIdentifierError for why each is refused).
//
// The refusal covers the table being altered, its schema, and the new name
// given by an ALTER TABLE ... RENAME [TO|AS], so a migration cannot produce a
// table Spirit would then refuse. Names come from
// Resources.Statement and, when set, Resources.Table. A statement-scope caller
// may supply only the statement; if that statement does not qualify the table
// with a schema, there is no schema name to check. The migration runner always
// supplies the table, which carries the schema from --database.
func tableIdentifierCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	if r.Table == nil && r.Statement == nil {
		return errors.New("check tableidentifier cannot run: neither the table nor the statement was supplied")
	}
	for _, id := range identifiersToCheck(r) {
		if err := utils.UnsupportedIdentifierError(id.kind, id.name); err != nil {
			return err
		}
	}
	return nil
}

// identifiersToCheck returns every non-empty schema and table name in r that
// must not contain a '.' or a backtick. The order is fixed, so a statement with
// more than one such name always reports the same one.
func identifiersToCheck(r Resources) []namedIdentifier {
	var ids []namedIdentifier
	add := func(kind, name string) {
		if name != "" {
			ids = append(ids, namedIdentifier{kind: kind, name: name})
		}
	}
	if r.Table != nil {
		add("table name", r.Table.TableName)
		add("schema name", r.Table.SchemaName)
	}
	if r.Statement == nil {
		return ids
	}
	add("table name", r.Statement.Table)
	add("schema name", r.Statement.Schema)
	if r.Statement.StmtNode == nil {
		return ids
	}
	alterStmt, ok := (*r.Statement.StmtNode).(*ast.AlterTableStmt)
	if !ok {
		return ids
	}
	for _, spec := range alterStmt.Specs {
		if spec.Tp != ast.AlterTableRenameTable || spec.NewTable == nil {
			continue
		}
		add("new table name", spec.NewTable.Name.O)
		add("new schema name", spec.NewTable.Schema.O)
	}
	return ids
}
