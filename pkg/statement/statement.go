// Package statement is a wrapper around the parser with some added functionality.
package statement

import (
	"errors"
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/parser"
	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/format"
	"github.com/block/spirit/pkg/parser/mysql"
)

type AbstractStatement struct {
	Schema    string // this will be empty unless the table name is fully qualified (ALTER TABLE test.t1 ...)
	Table     string // for statements that affect multiple tables (DROP TABLE t1, t2), only the first is set here!
	Alter     string // may be empty.
	Statement string
	StmtNode  *ast.StmtNode
}

var (
	ErrNotSupportedStatement   = errors.New("not a supported statement type")
	ErrNotAlterTable           = errors.New("not an ALTER TABLE statement")
	ErrMultipleSchemas         = errors.New("statement attempts to modify tables across multiple schemas")
	ErrNoStatements            = errors.New("could not find any compatible statements to execute")
	ErrMixMatchMultiStatements = errors.New("when performing atomic schema changes, all statements must be of type ALTER TABLE")
	ErrUnsafeForInplace        = errors.New("statement contains operations that are not safe for INPLACE algorithm")
	ErrAlterNoSpecs            = errors.New("ALTER TABLE does not specify any changes to make")
	ErrMultipleAlterClauses    = errors.New("ALTER contains multiple clauses. Combinations of INSTANT and INPLACE operations cannot be detected safely. Consider executing these as separate ALTER statements")
	ErrAlterContainsUnique     = errors.New("ALTER contains adding a unique index")
)

// Options configures the behavior of statement parsing.
type Options struct {
	// AllowMixedStatementTypes permits multi-statement input containing different
	// DDL types (e.g., CREATE TABLE + ALTER TABLE). By default, multi-statement
	// input must be all ALTER TABLE statements (required for atomic schema changes).
	// Enable this when using New() to split/parse schema files that may contain
	// a mix of DDL statement types.
	AllowMixedStatementTypes bool
}

func New(statement string) ([]*AbstractStatement, error) {
	return NewWithOptions(statement, Options{})
}

func NewWithOptions(statement string, opts Options) ([]*AbstractStatement, error) {
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
			alterClause, err := alterClauses(alterStmt, format.DefaultRestoreFlags)
			if err != nil {
				return nil, err
			}
			stmts = append(stmts, &AbstractStatement{
				Schema:    alterStmt.Table.Schema.String(),
				Table:     alterStmt.Table.Name.String(),
				Alter:     alterClause,
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
		case *ast.DropIndexStmt:
			// Need to rewrite to a corresponding ALTER TABLE statement
			stmt, err := convertDropIndexToAlterTable(node)
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
				return nil, ErrMultipleSchemas
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
					return nil, ErrMultipleSchemas
				}
				distinctSchemas[clause.OldTable.Schema.String()] = struct{}{}
			}
			if len(distinctSchemas) > 1 {
				return nil, ErrMultipleSchemas
			}
			stmts = append(stmts, &AbstractStatement{
				Schema:    stmt.TableToTables[0].OldTable.Schema.String(),
				Table:     stmt.TableToTables[0].OldTable.Name.String(),
				Statement: statement,
				StmtNode:  &stmtNodes[i],
			})
		default:
			return nil, fmt.Errorf("%w: %v (%T)", ErrNotSupportedStatement, node.Text(), node)
		}
	}

	if len(stmts) > 1 && mustBeOnlyStatement && !opts.AllowMixedStatementTypes {
		return nil, ErrMixMatchMultiStatements
	}
	if len(stmts) < 1 {
		return nil, ErrNoStatements
	}

	return stmts, nil
}

// alterClauses returns just the alter clauses of an ALTER TABLE statement,
// i.e. the "ADD COLUMN ..." in "ALTER TABLE `t` ADD COLUMN ...".
//
// AlterTableStmt.Restore writes "ALTER TABLE " + the table reference + " " +
// the comma-separated specs (see its implementation in pkg/parser/ast/ddl.go),
// so the clauses are whatever follows that prefix. Both the statement and the
// prefix are restored here, from one flags value and through the same writer
// calls Restore itself uses, so the two cannot render differently: whatever
// flags do to keyword case or identifier quoting, they do to both sides.
//
// Counting characters off the raw identifier is not a workable substitute:
// restore back-quotes identifiers and doubles any embedded back-quote, so a
// name containing one restores longer than it is raw.
//
// A statement with no specs at all has no prefix to trim. MySQL rejects
// "ALTER TABLE t" as a syntax error but our parser accepts it, so reject it
// here rather than returning an empty alter — downstream an empty Alter means
// "not a schema change" (see the CREATE TABLE case in NewWithOptions), which
// would silently do the wrong thing.
func alterClauses(alterStmt *ast.AlterTableStmt, flags format.RestoreFlags) (string, error) {
	var stmtSB strings.Builder
	if err := alterStmt.Restore(format.NewRestoreCtx(flags, &stmtSB)); err != nil {
		return "", fmt.Errorf("could not restore alter clause statement: %w", err)
	}
	if len(alterStmt.Specs) == 0 {
		return "", fmt.Errorf("%w: %s", ErrAlterNoSpecs, stmtSB.String())
	}
	// Mirror the opening of AlterTableStmt.Restore exactly: the keyword, the
	// table reference, then the single space before the first spec.
	var prefixSB strings.Builder
	prefixCtx := format.NewRestoreCtx(flags, &prefixSB)
	prefixCtx.WriteKeyWord("ALTER TABLE ")
	if err := alterStmt.Table.Restore(prefixCtx); err != nil {
		return "", fmt.Errorf("could not restore alter table name: %w", err)
	}
	prefixCtx.WritePlain(" ")
	alterClause, ok := strings.CutPrefix(stmtSB.String(), prefixSB.String())
	if !ok {
		// Unreachable unless AlterTableStmt.Restore changes the shape this
		// mirrors; fail loudly rather than cutting at a guessed offset.
		return "", fmt.Errorf("could not find the alter clauses in restored statement: %s", stmtSB.String())
	}
	return alterClause, nil
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

// AsAlterTable is a helper function that simply wraps the type case so the caller
// doesn't have to import the ast package and use the cast syntax
func (a *AbstractStatement) AsAlterTable() (*ast.AlterTableStmt, bool) {
	at, ok := (*a.StmtNode).(*ast.AlterTableStmt)
	return at, ok
}

func (a *AbstractStatement) IsCreateTable() bool {
	_, ok := (*a.StmtNode).(*ast.CreateTableStmt)
	return ok
}

func (a *AbstractStatement) IsDropTable() bool {
	_, ok := (*a.StmtNode).(*ast.DropTableStmt)
	return ok
}

func (a *AbstractStatement) IsRenameTable() bool {
	_, ok := (*a.StmtNode).(*ast.RenameTableStmt)
	return ok
}

func (a *AbstractStatement) ParseCreateTable() (*CreateTable, error) {
	createStmt, ok := (*a.StmtNode).(*ast.CreateTableStmt)
	if !ok {
		return nil, errors.New("not a CREATE TABLE statement")
	}

	ct := &CreateTable{
		Raw: createStmt,
	}
	// Parse into structured format
	ct.parseToStruct()
	return ct, nil
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
		switch spec.Tp { //nolint:exhaustive
		case ast.AlterTableRenameIndex,
			ast.AlterTableIndexInvisible,
			ast.AlterTableDropPartition,
			ast.AlterTableTruncatePartition,
			ast.AlterTableAddPartitions,
			ast.AlterTableDropIndex:
			continue
		case ast.AlterTableOption:
			// A table COMMENT change is in-place and metadata-only. Note it is
			// not INSTANT: MySQL rejects ALGORITHM=INSTANT but accepts INPLACE.
			// AlterTableOption also covers rebuild-class options (ENGINE=,
			// ROW_FORMAT=, AUTO_INCREMENT=, ...), so only treat it as safe when
			// every option in this spec is COMMENT.
			if SpecOnlyChangesComment(spec) {
				continue
			}
			unsafeClauses++
		case ast.AlterTableModifyColumn, ast.AlterTableChangeColumn:
			// Only safe if changing length of a VARCHAR column. We don't know the type of the column
			// or its length, so we cannot determine if this is safe only by parsing. We can simply try
			// INPLACE, and if it fails we will retry with our own schema change process.
			//
			// "Try INPLACE and let MySQL reject it" does not protect against
			// rebuild-class changes that MySQL *accepts* under ALGORITHM=INPLACE
			// though: converting a column to NOT NULL and reordering a column
			// (FIRST/AFTER) both succeed as INPLACE with a full table rebuild,
			// which can block replicas. A NOT NULL redeclaration can't be proven
			// unchanged from the statement alone (we don't have the current
			// nullability here), so it conservatively routes to the copy process.
			if ModifyColumnIsMetadataOnly(spec) {
				continue
			}
			unsafeClauses++
		default:
			unsafeClauses++
		}
	}
	if unsafeClauses > 0 {
		if len(alterStmt.Specs) > 1 {
			return ErrMultipleAlterClauses
		}
		return ErrUnsafeForInplace
	}
	return nil
}

// ModifyColumnIsMetadataOnly returns true if a MODIFY/CHANGE COLUMN spec only
// changes metadata: a VARCHAR redeclaration that neither reorders the column
// nor declares NOT NULL. Both of those are accepted by MySQL under
// ALGORITHM=INPLACE but performed with a full table rebuild.
//
// This is a statement-level judgement: without the current column definition we
// can't tell a VARCHAR length change from an INT-to-VARCHAR conversion, so a
// true result means "not provably a rebuild" rather than "provably not".
func ModifyColumnIsMetadataOnly(spec *ast.AlterTableSpec) bool {
	if len(spec.NewColumns) == 0 || spec.NewColumns[0].Tp == nil {
		return false
	}
	return spec.NewColumns[0].Tp.GetType() == mysql.TypeVarchar &&
		!columnReordered(spec) && !columnDeclaredNotNull(spec.NewColumns[0])
}

// SpecOnlyChangesComment returns true if every table option in an
// AlterTableOption spec is a COMMENT change. A table comment change is in-place
// and metadata-only, but other table options (ENGINE=, ROW_FORMAT=,
// AUTO_INCREMENT=, ...) can force a table rebuild, so a spec that mixes any of
// them in is not safe for INPLACE.
func SpecOnlyChangesComment(spec *ast.AlterTableSpec) bool {
	if len(spec.Options) == 0 {
		return false
	}
	for _, opt := range spec.Options {
		if opt.Tp != ast.TableOptionComment {
			return false
		}
	}
	return true
}

// columnReordered returns true if a MODIFY/CHANGE COLUMN spec moves the
// column with a FIRST or AFTER clause.
func columnReordered(spec *ast.AlterTableSpec) bool {
	return spec.Position != nil && spec.Position.Tp != ast.ColumnPositionNone
}

// columnDeclaredNotNull returns true if the new column definition
// declares NOT NULL.
func columnDeclaredNotNull(col *ast.ColumnDef) bool {
	for _, opt := range col.Options {
		if opt.Tp == ast.ColumnOptionNotNull {
			return true
		}
	}
	return false
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
		switch spec.Tp { //nolint:exhaustive
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
			return ErrAlterContainsUnique
		}
	}
	return nil
}

// ColumnRenameMap returns a mapping of old column name → new column name
// for any RENAME COLUMN or CHANGE COLUMN (with a different name) specs
// in this ALTER TABLE statement. Returns nil if there are no renames
// or if this is not an ALTER TABLE statement.
// MySQL column identifiers are case-insensitive, so a case-only change
// (e.g. foo → FOO) is not considered a rename: the data mapping is
// unaffected and case-insensitive identity matching handles it. The map
// keys and values keep the case as typed in the ALTER; consumers must
// match them against declared column names case-insensitively.
func (a *AbstractStatement) ColumnRenameMap() map[string]string {
	alterStmt, ok := (*a.StmtNode).(*ast.AlterTableStmt)
	if !ok {
		return nil
	}
	var renames map[string]string
	for _, spec := range alterStmt.Specs {
		switch spec.Tp { //nolint:exhaustive
		case ast.AlterTableRenameColumn:
			// ALTER TABLE t RENAME COLUMN old TO new
			if spec.OldColumnName != nil && spec.NewColumnName != nil {
				// Name.L is the lower-cased form of Name.O.
				if spec.OldColumnName.Name.L != spec.NewColumnName.Name.L {
					if renames == nil {
						renames = make(map[string]string)
					}
					renames[spec.OldColumnName.Name.O] = spec.NewColumnName.Name.O
				}
			}
		case ast.AlterTableChangeColumn:
			// ALTER TABLE t CHANGE COLUMN old new <type>
			if spec.OldColumnName != nil && len(spec.NewColumns) > 0 {
				if spec.OldColumnName.Name.L != spec.NewColumns[0].Name.Name.L {
					if renames == nil {
						renames = make(map[string]string)
					}
					renames[spec.OldColumnName.Name.O] = spec.NewColumns[0].Name.Name.O
				}
			}
		}
	}
	return renames
}

func (a *AbstractStatement) TrimAlter() string {
	return strings.TrimSuffix(strings.TrimSpace(a.Alter), ";")
}

func convertCreateIndexToAlterTable(stmt ast.StmtNode) (*AbstractStatement, error) {
	ciStmt, isCreateIndexStmt := stmt.(*ast.CreateIndexStmt)
	if !isCreateIndexStmt {
		return nil, errors.New("not a CREATE INDEX statement")
	}
	var parts []string
	var keyType string
	for _, part := range ciStmt.IndexPartSpecifications {
		var sb strings.Builder
		rCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
		if err := part.Restore(rCtx); err != nil {
			return nil, fmt.Errorf("could not restore index part: %w", err)
		}
		parts = append(parts, sb.String())
	}
	switch ciStmt.KeyType { //nolint:exhaustive
	case ast.IndexKeyTypeUnique:
		keyType = "UNIQUE INDEX"
	case ast.IndexKeyTypeFulltext:
		keyType = "FULLTEXT INDEX"
	case ast.IndexKeyTypeSpatial:
		keyType = "SPATIAL INDEX"
	default:
		keyType = "INDEX"
	}
	alterStmt := fmt.Sprintf("ADD %s %s (%s)", keyType, sqlescape.EscapeIdentifier(ciStmt.IndexName), strings.Join(parts, ", "))
	// We hint in the statement that it's been rewritten
	// and in the stmtNode we reparse from the alterStmt.
	statement := fmt.Sprintf("/* rewritten from CREATE INDEX */ ALTER TABLE %s %s", sqlescape.EscapeIdentifier(ciStmt.Table.Name.String()), alterStmt)
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

func convertDropIndexToAlterTable(stmt ast.StmtNode) (*AbstractStatement, error) {
	diStmt, isDropIndexStmt := stmt.(*ast.DropIndexStmt)
	if !isDropIndexStmt {
		return nil, errors.New("not a DROP INDEX statement")
	}
	alterStmt := fmt.Sprintf("DROP INDEX %s", sqlescape.EscapeIdentifier(diStmt.IndexName))
	// We hint in the statement that it's been rewritten
	// and in the stmtNode we reparse from the alterStmt.
	statement := fmt.Sprintf("/* rewritten from DROP INDEX */ ALTER TABLE %s %s", sqlescape.EscapeIdentifier(diStmt.Table.Name.String()), alterStmt)
	p := parser.New()
	stmtNodes, _, err := p.Parse(statement, "", "")
	if err != nil {
		return nil, errors.New("could not parse SQL statement: " + statement)
	}
	if len(stmtNodes) != 1 {
		return nil, errors.New("only one statement may be specified at once")
	}
	return &AbstractStatement{
		Schema:    diStmt.Table.Schema.String(),
		Table:     diStmt.Table.Name.String(),
		Alter:     alterStmt,
		Statement: statement,
		StmtNode:  &stmtNodes[0],
	}, err
}
