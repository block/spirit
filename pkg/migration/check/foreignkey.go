package check

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/utils"
)

// MinForeignKeyVersion is the oldest MySQL version a migration supports a table
// with foreign keys on, under --enable-experimental-foreign-keys.
//
// MySQL 9.6 moved foreign key checks and cascades out of InnoDB and into the
// SQL layer (WL#11249). Before that, a change a cascade made to a child table
// (ON DELETE CASCADE, ON UPDATE CASCADE, SET NULL) was made inside InnoDB and
// never written to the binary log, so the copy of a child table missed it.
// From 9.6 the cascaded changes are logged as row events of the child table,
// like any other change, unless the server was started with
// innodb_native_foreign_keys. 9.7 is the first LTS release with the change.
const MinForeignKeyVersion = "9.7"

func init() {
	registerCheck("addforeignkey", addForeignKeyCheck, ScopePreflight|ScopeStatement)
	// Re-run before cutover and again under the cutover lock: the binlog
	// clients cancel on a foreign key they can parse, but skip statements
	// they cannot, and are not acted on once the cutover starts. An inbound
	// foreign key added during the migration would follow the cutover
	// RENAME to the _old table, and an outbound one would be dropped by it.
	// After setup and before the cutover it checks that the new table has no
	// foreign keys, and under the cutover lock that the ones the cutover
	// added to it match the table's.
	registerCheck("hasforeignkeys", hasForeignKeysCheck, ScopePreflight|ScopePostSetup|ScopeCutover|ScopeCutoverLocked)
}

// The spirit OSC algorithm does not support adding foreign keys, or tables
// that other tables' foreign keys reference. A table's own foreign keys are
// supported under --enable-experimental-foreign-keys on MySQL 9.7 and later.

// hasForeignKeysCheck refuses a table that is either end of a foreign key
// relationship, or with --enable-experimental-foreign-keys, a table that is
// the referenced (parent) end of one.
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
	duringMigration := r.scope&(ScopeCutover|ScopeCutoverLocked) != 0
	if !r.ExperimentalForeignKeys {
		sql := `SELECT 1 FROM information_schema.referential_constraints WHERE
	(constraint_schema=? AND table_name=?)
	or (unique_constraint_schema=? AND referenced_table_name=?)
	LIMIT 1`
		found, err := anyRow(ctx, r.DB, sql, r.Table.SchemaName, r.Table.TableName, r.Table.SchemaName, r.Table.TableName)
		if err != nil {
			return err
		}
		if found {
			if duringMigration {
				return refuse(errors.New("a foreign key was created during the migration: tables with existing foreign key constraints are not supported"))
			}
			return refuse(errors.New("tables with existing foreign key constraints are not supported"))
		}
		// The change feed does not treat an ALTER that only adds or drops
		// foreign keys on the new table as a schema change (the experimental
		// cutover runs one), so a foreign key added to it from outside is
		// caught here instead.
		if r.NewTable != nil {
			found, err := anyRow(ctx, r.DB, `SELECT 1 FROM information_schema.referential_constraints WHERE
	constraint_schema=? AND table_name=? LIMIT 1`, r.NewTable.SchemaName, r.NewTable.TableName)
			if err != nil {
				return err
			}
			if found {
				return refuse(fmt.Errorf("the new table %s has a foreign key, which the migration did not add: tables with foreign key constraints are not supported", r.NewTable.TableName))
			}
		}
		return nil
	}

	// The cutover's RENAME TABLE repoints the foreign keys that reference the
	// table at the _old table, whatever foreign_key_checks is set to, so a
	// parent table is refused even with experimental support. This includes a
	// table with a foreign key to itself.
	found, err := anyRow(ctx, r.DB, `SELECT 1 FROM information_schema.referential_constraints WHERE
	unique_constraint_schema=? AND referenced_table_name=? LIMIT 1`, r.Table.SchemaName, r.Table.TableName)
	if err != nil {
		return err
	}
	if found {
		if duringMigration {
			return refuse(errors.New("a foreign key referencing the table was created during the migration: tables referenced by a foreign key are not supported, even with --enable-experimental-foreign-keys"))
		}
		return refuse(errors.New("tables referenced by a foreign key are not supported, even with --enable-experimental-foreign-keys: the cutover RENAME TABLE would repoint the referencing foreign keys at the _old table"))
	}

	source, err := tableDefinition(ctx, r.DB, r.Table.SchemaName, r.Table.TableName)
	if err != nil {
		return err
	}
	foreignKeys := foreignKeyConstraints(source)
	if len(foreignKeys) > 0 {
		// Checked in every scope, not only at preflight: a foreign key created
		// after preflight is copied to the new table too, and the server must
		// support it as much as one that was there from the start.
		if err := foreignKeySupport(ctx, r.DB); err != nil {
			return err
		}
	}
	if len(foreignKeys) > 0 && r.scope&ScopePreflight != 0 {
		if err := foreignKeyStatementSupport(r.Statement, foreignKeys); err != nil {
			return err
		}
	}
	if r.NewTable == nil {
		return nil
	}
	newTable, err := tableDefinition(ctx, r.DB, r.NewTable.SchemaName, r.NewTable.TableName)
	if err != nil {
		return err
	}
	// The new table only has foreign keys under the cutover lock, which adds
	// them after the final flush. Before that, a foreign key on it would hold
	// the application's changes to the parent tables to the copy's rows.
	if r.scope&ScopeCutoverLocked == 0 {
		if copies := foreignKeyConstraints(newTable); len(copies) > 0 {
			return refuse(fmt.Errorf("the new table %s has foreign key %s: the migration only adds the foreign keys of %s to it under the cutover lock",
				r.NewTable.TableName, copies[0].Name, r.Table.TableName))
		}
		return nil
	}
	if err := newTableForeignKeysMatch(r.Statement, r.Table.TableName, foreignKeys, foreignKeyConstraints(newTable)); err != nil {
		return refuse(fmt.Errorf("the foreign keys added to %s under the cutover lock do not match those of %s, so the cutover would change them: %w",
			r.NewTable.TableName, r.Table.TableName, err))
	}
	return nil
}

// foreignKeySupport refuses a server whose change feed does not carry the
// changes foreign key cascades make (see MinForeignKeyVersion).
func foreignKeySupport(ctx context.Context, db *sql.DB) error {
	var version string
	if err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		return err
	}
	if utils.CompareMySQLVersions(version, MinForeignKeyVersion) < 0 {
		return refuse(fmt.Errorf("tables with foreign keys require MySQL %s or later, even with --enable-experimental-foreign-keys: the server is %s, which does not write changes made by foreign key cascades to the binary log", MinForeignKeyVersion, version))
	}
	var native int
	if err := db.QueryRowContext(ctx, "SELECT @@innodb_native_foreign_keys").Scan(&native); err != nil {
		return fmt.Errorf("could not read innodb_native_foreign_keys: %w", err)
	}
	if native != 0 {
		return refuse(errors.New("tables with foreign keys are not supported when the server runs with innodb_native_foreign_keys=ON: InnoDB then makes the changes foreign key cascades require without writing them to the binary log"))
	}
	return nil
}

// foreignKeyStatementSupport refuses the parts of an ALTER that the copy of a
// table with foreign keys cannot apply to the new table.
func foreignKeyStatementSupport(stmt *statement.AbstractStatement, foreignKeys statement.Constraints) error {
	if stmt == nil {
		return nil
	}
	// DROP CONSTRAINT is resolved through the check constraint names of the
	// new table, not its foreign key names (see newTableAlter).
	for _, name := range stmt.GenericConstraintDrops() {
		for _, fk := range foreignKeys {
			if strings.EqualFold(fk.Name, name) {
				return refuse(fmt.Errorf("DROP CONSTRAINT %s names a foreign key, which --enable-experimental-foreign-keys does not support: use DROP FOREIGN KEY %s", name, name))
			}
		}
	}
	return nil
}

// newTableForeignKeysMatch reports how the foreign keys of the new table
// differ from the ones the table being altered says it should have: each of
// source's foreign keys, under the name utils.NewForeignKeyName gives it and on
// the columns the ALTER renames its columns to, except those the ALTER drops.
func newTableForeignKeysMatch(stmt *statement.AbstractStatement, tableName string, source, newTable statement.Constraints) error {
	var dropped, renames map[string]string
	if stmt != nil {
		dropped = make(map[string]string)
		for _, name := range stmt.ForeignKeysDropped() {
			dropped[strings.ToLower(name)] = name
		}
		renames = make(map[string]string)
		for from, to := range stmt.ColumnRenameMap() {
			renames[strings.ToLower(from)] = to
		}
	}
	remaining := slices.Clone(newTable)
	for _, fk := range source {
		if _, ok := dropped[strings.ToLower(fk.Name)]; ok {
			continue
		}
		name := utils.NewForeignKeyName(tableName, fk.Name)
		i := slices.IndexFunc(remaining, func(c statement.Constraint) bool { return strings.EqualFold(c.Name, name) })
		if i < 0 {
			return fmt.Errorf("foreign key %s has no copy %s", fk.Name, name)
		}
		if !foreignKeysEqual(fk, remaining[i], renames) {
			return fmt.Errorf("foreign key %s is %s but its copy %s is %s", fk.Name, definition(fk), name, definition(remaining[i]))
		}
		remaining = slices.Delete(remaining, i, i+1)
	}
	if len(remaining) > 0 {
		return fmt.Errorf("foreign key %s is not a copy of a foreign key of %s", remaining[0].Name, tableName)
	}
	return nil
}

// foreignKeysEqual reports whether foreign key b is foreign key a with a's
// columns renamed through renames (lower-cased old name to new name).
func foreignKeysEqual(a, b statement.Constraint, renames map[string]string) bool {
	if len(a.Columns) != len(b.Columns) || a.References == nil || b.References == nil {
		return false
	}
	for i, col := range a.Columns {
		if to, ok := renames[strings.ToLower(col)]; ok {
			col = to
		}
		if !strings.EqualFold(col, b.Columns[i]) {
			return false
		}
	}
	// Schema and table names are compared exactly: with lower_case_table_names=0
	// they are case-sensitive, so parent and Parent can be different tables.
	// Both definitions come from SHOW CREATE TABLE on the same server, in the
	// same schema, so a copy renders its reference the same way. Column names
	// are case-insensitive in MySQL.
	ra, rb := a.References, b.References
	return ra.Schema == rb.Schema &&
		ra.Table == rb.Table &&
		slices.EqualFunc(ra.Columns, rb.Columns, strings.EqualFold) &&
		equalOption(ra.OnDelete, rb.OnDelete) &&
		equalOption(ra.OnUpdate, rb.OnUpdate)
}

func equalOption(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return strings.EqualFold(*a, *b)
}

func definition(c statement.Constraint) string {
	if c.Definition == nil {
		return "a FOREIGN KEY"
	}
	return *c.Definition
}

// foreignKeyConstraints returns the foreign keys among def's constraints.
func foreignKeyConstraints(def *statement.CreateTable) statement.Constraints {
	var foreignKeys statement.Constraints
	for _, c := range def.GetConstraints() {
		if c.Type == "FOREIGN KEY" {
			foreignKeys = append(foreignKeys, c)
		}
	}
	return foreignKeys
}

// tableDefinition returns the table's SHOW CREATE TABLE, parsed.
func tableDefinition(ctx context.Context, db *sql.DB, schemaName, tableName string) (*statement.CreateTable, error) {
	var name, createTable string
	if err := db.QueryRowContext(ctx, sqlescape.MustEscapeSQL("SHOW CREATE TABLE %n.%n", schemaName, tableName)).Scan(&name, &createTable); err != nil {
		return nil, fmt.Errorf("could not read the definition of table %s.%s: %w", schemaName, tableName, err)
	}
	def, err := statement.ParseCreateTable(createTable)
	if err != nil {
		return nil, fmt.Errorf("could not parse the definition of table %s.%s: %w", schemaName, tableName, err)
	}
	return def, nil
}

// anyRow reports whether query returns a row.
func anyRow(ctx context.Context, db *sql.DB, query string, args ...any) (bool, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return false, err
	}
	defer utils.CloseAndLog(rows)
	found := rows.Next()
	return found, rows.Err()
}

// addForeignKeyCheck refuses an ALTER that adds a foreign key: a FOREIGN KEY
// constraint, or a column added or redefined with an inline REFERENCES. MySQL
// 8.0 parses and ignores an inline REFERENCES, but MySQL 9.0 creates a foreign
// key for it, so it is refused on every version.
//
// It is refused with --enable-experimental-foreign-keys too: the cutover adds
// the table's foreign keys to the new table without checking its rows, which
// is only safe for foreign keys the rows already hold.
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
		// ADD COLUMN, MODIFY and CHANGE all carry their column definitions in
		// NewColumns.
		for _, col := range spec.NewColumns {
			for _, opt := range col.Options {
				if opt.Tp == ast.ColumnOptionReference {
					return fmt.Errorf("adding foreign key constraints is not supported: column %q is declared with an inline REFERENCES, which MySQL 9.0 and later create a foreign key for", col.Name.Name.O)
				}
			}
		}
	}
	return nil // no problems
}
