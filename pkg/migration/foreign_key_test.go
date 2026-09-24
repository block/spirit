package migration

import (
	"context"
	"database/sql"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// TestForeignKeyCrossSchemaInboundRefused is the end-to-end guard for issue
// #1182. A table whose only foreign key relationship is an inbound one from a
// child in another schema used to pass the hasforeignkeys preflight, because
// both halves of its query matched on constraint_schema — the *child's* schema.
// The migration then ran to completion and exited 0, while MySQL followed the
// cutover RENAME and repointed the child's foreign key at _<table>_old: a table
// spirit cannot drop and that receives no further writes, so integrity ended up
// enforced against a stale snapshot.
//
// Spirit does not support foreign keys, so the correct outcome is a refusal.
func TestForeignKeyCrossSchemaInboundRefused(t *testing.T) {
	// Not parallel: it creates and drops its own schema.
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() }) //nolint:errcheck // test cleanup

	const childSchema = "test_fk_inbound_child"
	// Cleanups run LIFO and t.Context() is already cancelled by then, so this
	// uses a background context and is registered after the db close above.
	drop := func() {
		// The child schema goes first: while it exists, its foreign key blocks
		// dropping whichever parent-side table it currently references.
		_, err := db.ExecContext(context.Background(), `DROP DATABASE IF EXISTS `+childSchema)
		require.NoError(t, err)
		// Spirit's own artifacts are dropped too. A run of this test against a
		// build without the fix leaves an undroppable _old table behind, and
		// the next run must not inherit it.
		_, err = db.ExecContext(context.Background(), `DROP TABLE IF EXISTS fk_inbound_parent,
			_fk_inbound_parent_old, _fk_inbound_parent_new, _fk_inbound_parent_chkpnt`)
		require.NoError(t, err)
	}
	drop()
	t.Cleanup(drop)

	testutils.RunSQL(t, `CREATE TABLE fk_inbound_parent (id INT NOT NULL PRIMARY KEY, name VARCHAR(50))`)
	testutils.RunSQL(t, `INSERT INTO fk_inbound_parent VALUES (1, 'a'), (2, 'b')`)
	_, err = db.ExecContext(t.Context(), `CREATE DATABASE `+childSchema)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), `CREATE TABLE `+childSchema+`.child (
		id INT NOT NULL PRIMARY KEY,
		pid INT,
		CONSTRAINT fk_inbound FOREIGN KEY (pid) REFERENCES test.fk_inbound_parent(id)
	)`)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), `INSERT INTO `+childSchema+`.child VALUES (10, 1), (11, 2)`)
	require.NoError(t, err)

	// A copy-path ALTER: MODIFY to TEXT is neither INSTANT nor safe-INPLACE,
	// so without the refusal this reaches cutover.
	m := NewTestRunner(t, "fk_inbound_parent", "MODIFY name TEXT")
	err = m.Run(t.Context())
	require.Error(t, err, "a table with an inbound foreign key from another schema must be refused")
	require.ErrorContains(t, err, "tables with existing foreign key constraints are not supported")
	require.NoError(t, m.Close())

	// The refusal happened before cutover, so the child's foreign key still
	// points at the live parent and no _old table was left behind.
	var referenced string
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT referenced_table_name FROM information_schema.referential_constraints
		 WHERE constraint_schema = ? AND constraint_name = 'fk_inbound'`, childSchema).Scan(&referenced))
	require.Equal(t, "fk_inbound_parent", referenced,
		"the child's foreign key must still reference the live parent table")

	var leftover int
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM information_schema.TABLES
		 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '_fk_inbound_parent_old'`).Scan(&leftover))
	require.Equal(t, 0, leftover, "no _old table should have been left behind")
}
