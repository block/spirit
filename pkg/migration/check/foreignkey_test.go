package check

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
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
