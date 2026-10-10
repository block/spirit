package check

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestAddForeignKey(t *testing.T) {
	var err error
	r := Resources{
		Statement: statement.MustNew("ALTER TABLE t1 ADD FOREIGN KEY (customer_id) REFERENCES customers (id)")[0],
	}
	err = addForeignKeyCheck(t.Context(), r, slog.Default())
	require.Error(t, err) // add foreign key
	require.ErrorContains(t, err, "adding foreign key constraints is not supported")

	r.Statement = statement.MustNew("ALTER TABLE t1 DROP COLUMN foo")[0]
	err = addForeignKeyCheck(t.Context(), r, slog.Default())
	require.NoError(t, err) // regular DDL
}

// TestAddForeignKeyInlineReference checks that a column declared with an
// inline REFERENCES is refused. MySQL 8.0 ignores an inline REFERENCES, but
// MySQL 9.0 creates a foreign key for it, so the new table would get one.
func TestAddForeignKeyInlineReference(t *testing.T) {
	for _, stmt := range []string{
		"ALTER TABLE t1 ADD COLUMN customer_id INT REFERENCES customers (id)",
		"ALTER TABLE t1 ADD COLUMN (a INT, customer_id INT REFERENCES customers (id))",
		"ALTER TABLE t1 MODIFY customer_id INT REFERENCES customers (id)",
		"ALTER TABLE t1 CHANGE cust_id customer_id INT REFERENCES customers (id)",
		"ALTER TABLE t1 ADD INDEX (b), MODIFY customer_id INT NOT NULL REFERENCES customers (id) ON DELETE CASCADE",
	} {
		t.Run(stmt, func(t *testing.T) {
			r := Resources{Statement: statement.MustNew(stmt)[0]}
			err := addForeignKeyCheck(t.Context(), r, slog.Default())
			require.ErrorContains(t, err, "adding foreign key constraints is not supported")
			require.ErrorContains(t, err, `column "customer_id" is declared with an inline REFERENCES`)
		})
	}
	// Columns without an inline REFERENCES are fine.
	for _, stmt := range []string{
		"ALTER TABLE t1 ADD COLUMN customer_id INT NOT NULL DEFAULT 0",
		"ALTER TABLE t1 MODIFY customer_id BIGINT",
		"ALTER TABLE t1 CHANGE cust_id customer_id INT COMMENT 'references customers (id)'",
	} {
		t.Run(stmt, func(t *testing.T) {
			r := Resources{Statement: statement.MustNew(stmt)[0]}
			require.NoError(t, addForeignKeyCheck(t.Context(), r, slog.Default()))
		})
	}
}

func TestHasForeignKey(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)

	_, err = db.ExecContext(t.Context(), `drop table if exists customers, customer_contacts`)
	require.NoError(t, err)
	sql := `CREATE TABLE customers (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	);`
	_, err = db.ExecContext(t.Context(), sql)
	require.NoError(t, err)
	sql = `CREATE TABLE customer_contacts (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		customer_id INT NOT NULL,
		PRIMARY KEY (id),
		INDEX  (customer_id),  
		CONSTRAINT fk_customer FOREIGN KEY (customer_id)  
		REFERENCES customers(id)  
		ON DELETE CASCADE  
		ON UPDATE CASCADE  
	);`
	_, err = db.ExecContext(t.Context(), sql)
	require.NoError(t, err)

	// Under this model, both customers and customer_contacts are said to have foreign keys.
	r := Resources{
		DB:        db,
		Table:     &table.TableInfo{SchemaName: "test", TableName: "customers"},
		Statement: statement.MustNew("ALTER TABLE customers ENGINE=innodb")[0],
	}
	err = hasForeignKeysCheck(t.Context(), r, slog.Default())
	require.Error(t, err) // already has foreign keys.

	// Re-run before cutover, the refusal says the foreign key is new.
	cutover := r
	cutover.scope = ScopeCutover
	err = hasForeignKeysCheck(t.Context(), cutover, slog.Default())
	require.ErrorContains(t, err, "a foreign key was created during the migration")
	require.Contains(t, ChecksInScope(ScopeCutover), "hasforeignkeys")
	require.Contains(t, ChecksInScope(ScopeCutoverLocked), "hasforeignkeys")

	r.Table.TableName = "customer_contacts"
	r.Statement = statement.MustNew("ALTER TABLE customer_contacts ENGINE=innodb")[0]
	err = hasForeignKeysCheck(t.Context(), r, slog.Default())
	require.Error(t, err) // already has foreign keys.

	_, err = db.ExecContext(t.Context(), `drop table if exists customer_contacts`)
	require.NoError(t, err)
	r.Table.TableName = "customers"
	r.Statement = statement.MustNew("ALTER TABLE customers ENGINE=innodb")[0]
	err = hasForeignKeysCheck(t.Context(), r, slog.Default())
	require.NoError(t, err) // no longer said to have foreign keys.
}

// TestHasForeignKeyCrossSchema covers issue #1182: an inbound foreign key whose
// child table lives in another schema. referential_constraints records the
// child's schema in constraint_schema and the parent's in
// unique_constraint_schema, so matching the inbound half on constraint_schema
// only ever found children in the migrated table's own schema. The migration
// was allowed to proceed and the cutover rename repointed the child's foreign
// key at the _old table.
func TestHasForeignKeyCrossSchema(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	// Registered first so it runs last: cleanups are LIFO, and the drop below
	// still needs the connection.
	t.Cleanup(func() { db.Close() }) //nolint:errcheck // test cleanup

	const otherSchema = "test_fk_other_schema"
	// Not t.Context(): it is already cancelled by the time cleanups run.
	drop := func() {
		_, err := db.ExecContext(context.Background(), `DROP DATABASE IF EXISTS `+otherSchema)
		require.NoError(t, err)
		_, err = db.ExecContext(context.Background(), `DROP TABLE IF EXISTS xs_parent`)
		require.NoError(t, err)
	}
	drop()
	t.Cleanup(drop)

	_, err = db.ExecContext(t.Context(), `CREATE TABLE xs_parent (id INT NOT NULL PRIMARY KEY)`)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), `CREATE DATABASE `+otherSchema)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), `CREATE TABLE `+otherSchema+`.xs_child (
		id INT NOT NULL PRIMARY KEY,
		pid INT,
		CONSTRAINT fk_xs FOREIGN KEY (pid) REFERENCES test.xs_parent(id)
	)`)
	require.NoError(t, err)

	// The parent is in test; its only child is in another schema.
	r := Resources{
		DB:        db,
		Table:     &table.TableInfo{SchemaName: "test", TableName: "xs_parent"},
		Statement: statement.MustNew("ALTER TABLE xs_parent ENGINE=innodb")[0],
	}
	err = hasForeignKeysCheck(t.Context(), r, slog.Default())
	require.Error(t, err, "an inbound foreign key from another schema must be refused")
	require.ErrorContains(t, err, "tables with existing foreign key constraints are not supported")

	// The outbound half was always caught, since constraint_schema is the
	// child's own schema regardless of where the parent lives. Check it still is.
	r.Table = &table.TableInfo{SchemaName: otherSchema, TableName: "xs_child"}
	r.Statement = statement.MustNew("ALTER TABLE xs_child ENGINE=innodb")[0]
	err = hasForeignKeysCheck(t.Context(), r, slog.Default())
	require.Error(t, err, "an outbound foreign key to another schema must be refused")

	// A same-named table in an unrelated schema must not be dragged in: the
	// match is on schema *and* name, not name alone.
	_, err = db.ExecContext(t.Context(), `CREATE TABLE `+otherSchema+`.xs_parent (id INT NOT NULL PRIMARY KEY)`)
	require.NoError(t, err)
	r.Table = &table.TableInfo{SchemaName: otherSchema, TableName: "xs_parent"}
	r.Statement = statement.MustNew("ALTER TABLE xs_parent ENGINE=innodb")[0]
	err = hasForeignKeysCheck(t.Context(), r, slog.Default())
	require.NoError(t, err, "a same-named table in another schema has no foreign keys of its own")
}

func TestNewTableForeignKeysMatch(t *testing.T) {
	parse := func(sql string) statement.Constraints {
		ct, err := statement.ParseCreateTable(sql)
		require.NoError(t, err)
		return foreignKeyConstraints(ct)
	}
	source := parse(`CREATE TABLE child (id INT PRIMARY KEY, pid INT, pid2 INT,
		CONSTRAINT fk_parent FOREIGN KEY (pid) REFERENCES parent (id) ON DELETE CASCADE,
		CONSTRAINT child_ibfk_1 FOREIGN KEY (pid2) REFERENCES parent (id))`)
	for _, test := range []struct {
		name, alter, newTable, err string
	}{
		{"copied", "ALTER TABLE child ADD COLUMN c INT",
			`CREATE TABLE _child_new (id INT PRIMARY KEY, pid INT, pid2 INT, c INT,
				CONSTRAINT _fk_parent_new FOREIGN KEY (pid) REFERENCES parent (id) ON DELETE CASCADE,
				CONSTRAINT _child_new_ibfk_1 FOREIGN KEY (pid2) REFERENCES parent (id))`, ""},
		{"dropped", "ALTER TABLE child DROP FOREIGN KEY FK_PARENT",
			`CREATE TABLE _child_new (id INT PRIMARY KEY, pid INT, pid2 INT,
				CONSTRAINT _child_new_ibfk_1 FOREIGN KEY (pid2) REFERENCES parent (id))`, ""},
		{"renamed column", "ALTER TABLE child RENAME COLUMN pid TO parent_id",
			`CREATE TABLE _child_new (id INT PRIMARY KEY, parent_id INT, pid2 INT,
				CONSTRAINT _fk_parent_new FOREIGN KEY (parent_id) REFERENCES parent (id) ON DELETE CASCADE,
				CONSTRAINT _child_new_ibfk_1 FOREIGN KEY (pid2) REFERENCES parent (id))`, ""},
		{"missing", "ALTER TABLE child ADD COLUMN c INT",
			`CREATE TABLE _child_new (id INT PRIMARY KEY, pid INT, pid2 INT,
				CONSTRAINT _child_new_ibfk_1 FOREIGN KEY (pid2) REFERENCES parent (id))`,
			"foreign key fk_parent has no copy _fk_parent_new"},
		{"different action", "ALTER TABLE child ADD COLUMN c INT",
			`CREATE TABLE _child_new (id INT PRIMARY KEY, pid INT, pid2 INT,
				CONSTRAINT _fk_parent_new FOREIGN KEY (pid) REFERENCES parent (id) ON DELETE SET NULL,
				CONSTRAINT _child_new_ibfk_1 FOREIGN KEY (pid2) REFERENCES parent (id))`,
			"foreign key fk_parent is"},
		{"different on update", "ALTER TABLE child ADD COLUMN c INT",
			`CREATE TABLE _child_new (id INT PRIMARY KEY, pid INT, pid2 INT,
				CONSTRAINT _fk_parent_new FOREIGN KEY (pid) REFERENCES parent (id) ON DELETE CASCADE ON UPDATE CASCADE,
				CONSTRAINT _child_new_ibfk_1 FOREIGN KEY (pid2) REFERENCES parent (id))`,
			"foreign key fk_parent is"},
		{"different referenced schema", "ALTER TABLE child ADD COLUMN c INT",
			`CREATE TABLE _child_new (id INT PRIMARY KEY, pid INT, pid2 INT,
				CONSTRAINT _fk_parent_new FOREIGN KEY (pid) REFERENCES other_schema.parent (id) ON DELETE CASCADE,
				CONSTRAINT _child_new_ibfk_1 FOREIGN KEY (pid2) REFERENCES parent (id))`,
			"foreign key fk_parent is"},
		{"different parent", "ALTER TABLE child ADD COLUMN c INT",
			`CREATE TABLE _child_new (id INT PRIMARY KEY, pid INT, pid2 INT,
				CONSTRAINT _fk_parent_new FOREIGN KEY (pid) REFERENCES _parent_old (id) ON DELETE CASCADE,
				CONSTRAINT _child_new_ibfk_1 FOREIGN KEY (pid2) REFERENCES parent (id))`,
			"foreign key fk_parent is"},
		{"differently cased parent", "ALTER TABLE child ADD COLUMN c INT",
			`CREATE TABLE _child_new (id INT PRIMARY KEY, pid INT, pid2 INT,
				CONSTRAINT _fk_parent_new FOREIGN KEY (pid) REFERENCES Parent (id) ON DELETE CASCADE,
				CONSTRAINT _child_new_ibfk_1 FOREIGN KEY (pid2) REFERENCES parent (id))`,
			"foreign key fk_parent is"},
		{"differently cased column", "ALTER TABLE child ADD COLUMN c INT",
			`CREATE TABLE _child_new (id INT PRIMARY KEY, PID INT, pid2 INT,
				CONSTRAINT _fk_parent_new FOREIGN KEY (PID) REFERENCES parent (ID) ON DELETE CASCADE,
				CONSTRAINT _child_new_ibfk_1 FOREIGN KEY (pid2) REFERENCES parent (id))`, ""},
		{"extra", "ALTER TABLE child ADD COLUMN c INT",
			`CREATE TABLE _child_new (id INT PRIMARY KEY, pid INT, pid2 INT,
				CONSTRAINT _fk_parent_new FOREIGN KEY (pid) REFERENCES parent (id) ON DELETE CASCADE,
				CONSTRAINT _child_new_ibfk_1 FOREIGN KEY (pid2) REFERENCES parent (id),
				CONSTRAINT fk_other FOREIGN KEY (pid2) REFERENCES other (id))`,
			"foreign key fk_other is not a copy of a foreign key of child"},
		{"dropped but kept", "ALTER TABLE child DROP FOREIGN KEY fk_parent",
			`CREATE TABLE _child_new (id INT PRIMARY KEY, pid INT, pid2 INT,
				CONSTRAINT _fk_parent_new FOREIGN KEY (pid) REFERENCES parent (id) ON DELETE CASCADE,
				CONSTRAINT _child_new_ibfk_1 FOREIGN KEY (pid2) REFERENCES parent (id))`,
			"foreign key _fk_parent_new is not a copy of a foreign key of child"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := newTableForeignKeysMatch(statement.MustNew(test.alter)[0], "child", source, parse(test.newTable))
			if test.err == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, test.err)
			}
		})
	}
}

// TestForeignKeySupportInEveryScope checks the server for a table with foreign
// keys in every scope the check runs in, not only at preflight: one created
// after preflight is copied to the new table too.
func TestForeignKeySupportInEveryScope(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	var version string
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT VERSION()").Scan(&version))
	if utils.CompareMySQLVersions(version, MinForeignKeyVersion) >= 0 {
		t.Skipf("MySQL %s supports foreign keys", version)
	}
	testutils.RunSQL(t, "DROP TABLE IF EXISTS fkscope_child, fkscope_parent")
	testutils.RunSQL(t, "CREATE TABLE fkscope_parent (id INT PRIMARY KEY)")
	testutils.RunSQL(t, "CREATE TABLE fkscope_child (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES fkscope_parent (id))")
	t.Cleanup(func() { testutils.RunSQL(t, "DROP TABLE IF EXISTS fkscope_child, fkscope_parent") })
	for _, scope := range []ScopeFlag{ScopePreflight, ScopePostSetup, ScopeCutover, ScopeCutoverLocked} {
		r := Resources{
			DB:                      db,
			Table:                   &table.TableInfo{SchemaName: "test", TableName: "fkscope_child"},
			Statement:               statement.MustNew("ALTER TABLE fkscope_child ENGINE=InnoDB")[0],
			ExperimentalForeignKeys: true,
			scope:                   scope,
		}
		require.ErrorContains(t, hasForeignKeysCheck(t.Context(), r, slog.Default()), "require MySQL 9.7 or later", "scope %v", scope)
	}
}

// TestNewTableForeignKeysOnlyUnderCutoverLock checks that the new table may
// only have foreign keys under the cutover lock, which adds them: before that,
// one would hold the application's changes to the parent table to the copy's
// rows.
func TestNewTableForeignKeysOnlyUnderCutoverLock(t *testing.T) {
	testutils.SkipBeforeMySQLVersion(t, MinForeignKeyVersion, "foreign keys are only supported from MySQL 9.7")
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	drop := "DROP TABLE IF EXISTS _fkonly_child_new, fkonly_child, fkonly_parent"
	testutils.RunSQL(t, drop)
	t.Cleanup(func() { testutils.RunSQL(t, drop) })
	testutils.RunSQL(t, "CREATE TABLE fkonly_parent (id INT PRIMARY KEY)")
	testutils.RunSQL(t, "CREATE TABLE fkonly_child (id INT PRIMARY KEY, pid INT, CONSTRAINT fk_only FOREIGN KEY (pid) REFERENCES fkonly_parent (id))")
	testutils.RunSQL(t, "CREATE TABLE _fkonly_child_new LIKE fkonly_child")
	resources := func(scope ScopeFlag) Resources {
		return Resources{
			DB:                      db,
			Table:                   &table.TableInfo{SchemaName: "test", TableName: "fkonly_child"},
			NewTable:                &table.TableInfo{SchemaName: "test", TableName: "_fkonly_child_new"},
			Statement:               statement.MustNew("ALTER TABLE fkonly_child ENGINE=InnoDB")[0],
			ExperimentalForeignKeys: true,
			scope:                   scope,
		}
	}
	// No foreign keys on the new table: right until the cutover lock.
	for _, scope := range []ScopeFlag{ScopePostSetup, ScopeCutover} {
		require.NoError(t, hasForeignKeysCheck(t.Context(), resources(scope), slog.Default()), "scope %v", scope)
	}
	require.ErrorContains(t, hasForeignKeysCheck(t.Context(), resources(ScopeCutoverLocked), slog.Default()), "has no copy _fk_only_new")

	testutils.RunSQL(t, "ALTER TABLE _fkonly_child_new ADD CONSTRAINT _fk_only_new FOREIGN KEY (pid) REFERENCES fkonly_parent (id)")
	for _, scope := range []ScopeFlag{ScopePostSetup, ScopeCutover} {
		err := hasForeignKeysCheck(t.Context(), resources(scope), slog.Default())
		require.ErrorIs(t, err, ErrRefused, "scope %v", scope)
		require.ErrorContains(t, err, "only adds the foreign keys", "scope %v", scope)
	}
	require.NoError(t, hasForeignKeysCheck(t.Context(), resources(ScopeCutoverLocked), slog.Default()))
}

// TestNewTableForeignKeyWithoutExperimentalSupport adds a foreign key to the
// new table of a table that has none, without
// --enable-experimental-foreign-keys. The change feed does not treat that
// ALTER as a schema change, because the experimental cutover runs one, so this
// check has to refuse it.
func TestNewTableForeignKeyWithoutExperimentalSupport(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	drop := "DROP TABLE IF EXISTS _nofk_child_new, nofk_child, nofk_parent"
	testutils.RunSQL(t, drop)
	t.Cleanup(func() { testutils.RunSQL(t, drop) })
	testutils.RunSQL(t, "CREATE TABLE nofk_parent (id INT PRIMARY KEY)")
	testutils.RunSQL(t, "CREATE TABLE nofk_child (id INT PRIMARY KEY, pid INT, KEY (pid))")
	testutils.RunSQL(t, "CREATE TABLE _nofk_child_new LIKE nofk_child")
	resources := func(scope ScopeFlag) Resources {
		return Resources{
			DB:        db,
			Table:     &table.TableInfo{SchemaName: "test", TableName: "nofk_child"},
			NewTable:  &table.TableInfo{SchemaName: "test", TableName: "_nofk_child_new"},
			Statement: statement.MustNew("ALTER TABLE nofk_child ENGINE=InnoDB")[0],
			scope:     scope,
		}
	}
	scopes := []ScopeFlag{ScopePostSetup, ScopeCutover, ScopeCutoverLocked}
	for _, scope := range scopes {
		require.NoError(t, hasForeignKeysCheck(t.Context(), resources(scope), slog.Default()), "scope %v", scope)
	}
	testutils.RunSQL(t, "ALTER TABLE _nofk_child_new ADD CONSTRAINT fk_nofk FOREIGN KEY (pid) REFERENCES nofk_parent (id)")
	for _, scope := range scopes {
		err := hasForeignKeysCheck(t.Context(), resources(scope), slog.Default())
		require.ErrorIs(t, err, ErrRefused, "scope %v", scope)
		require.ErrorContains(t, err, "the new table _nofk_child_new has a foreign key", "scope %v", scope)
	}
}
