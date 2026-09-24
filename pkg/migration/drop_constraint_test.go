package migration

import (
	"database/sql"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// Tests for issue #1183. MySQL's DROP CONSTRAINT resolves a name against the
// table's CHECK, FOREIGN KEY and UNIQUE constraints, but the parser used to
// fold it into the same AST node as DROP CHECK and restore it as DROP CHECK.
// Because spirit sends the restored clause rather than the user's text, that
// turned a statement MySQL accepts into error 3821 for anything that was not a
// check constraint.

// TestDropConstraintUnique drops a UNIQUE constraint by the DROP CONSTRAINT
// spelling. Before the fix this failed with
// "Error 3821: Check constraint 'uq_dc_name' is not found in the table".
func TestDropConstraintUnique(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "dc_unique", `CREATE TABLE dc_unique (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(50),
		CONSTRAINT uq_dc_name UNIQUE (name)
	)`)
	testutils.RunSQL(t, `INSERT INTO dc_unique (name) VALUES ('a'), ('b')`)

	// The constraint is enforced to begin with.
	_, err := tt.DB.ExecContext(t.Context(), `INSERT INTO dc_unique (name) VALUES ('a')`)
	require.Error(t, err, "UNIQUE constraint should reject a duplicate before the migration")

	m := NewTestRunner(t, "dc_unique", "DROP CONSTRAINT uq_dc_name")
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())

	require.Empty(t, tableConstraintsOfType(t, tt.DB, "dc_unique", "UNIQUE"),
		"DROP CONSTRAINT should have removed the UNIQUE constraint")

	// Rows survived, and duplicates are now accepted.
	var count int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM dc_unique").Scan(&count))
	require.Equal(t, 2, count)
	_, err = tt.DB.ExecContext(t.Context(), `INSERT INTO dc_unique (name) VALUES ('a')`)
	require.NoError(t, err, "the dropped UNIQUE constraint should no longer be enforced")
}

// TestDropConstraintCheck covers the spelling that already worked, so the fix
// does not regress it. A check constraint is renamed on the _new table (#1181),
// so this also exercises DROP CONSTRAINT through that rewrite.
func TestDropConstraintCheck(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "dc_check", `CREATE TABLE dc_check (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		age INT,
		CONSTRAINT chk_dc_age CHECK (age >= 18)
	)`)
	testutils.RunSQL(t, `INSERT INTO dc_check (age) VALUES (20), (30)`)

	// Force the copy path: an INSTANT drop would not exercise the rewrite.
	m := NewTestRunner(t, "dc_check", "DROP CONSTRAINT chk_dc_age, ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.NoError(t, m.Close())

	require.Empty(t, tableCheckConstraints(t, tt.DB, "dc_check"),
		"DROP CONSTRAINT should have removed the CHECK constraint")

	_, err := tt.DB.ExecContext(t.Context(), `INSERT INTO dc_check (age) VALUES (1)`)
	require.NoError(t, err, "the dropped CHECK constraint should no longer be enforced")
}

// TestDropConstraintMixedSpellings drops a UNIQUE and a CHECK constraint in one
// ALTER, one clause in each spelling. Each clause has to keep its own keyword.
func TestDropConstraintMixedSpellings(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "dc_mixed", `CREATE TABLE dc_mixed (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(50),
		age INT,
		CONSTRAINT uq_dcm_name UNIQUE (name),
		CONSTRAINT chk_dcm_age CHECK (age >= 18)
	)`)
	testutils.RunSQL(t, `INSERT INTO dc_mixed (name, age) VALUES ('a', 20), ('b', 30)`)

	m := NewTestRunner(t, "dc_mixed", "DROP CONSTRAINT uq_dcm_name, DROP CHECK chk_dcm_age")
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())

	require.Empty(t, tableConstraintsOfType(t, tt.DB, "dc_mixed", "UNIQUE"))
	require.Empty(t, tableCheckConstraints(t, tt.DB, "dc_mixed"))

	var count int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM dc_mixed").Scan(&count))
	require.Equal(t, 2, count)
}

// tableConstraintsOfType returns the names of the table's constraints of the
// given information_schema CONSTRAINT_TYPE.
func tableConstraintsOfType(t *testing.T, db *sql.DB, tableName, constraintType string) []string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), `SELECT CONSTRAINT_NAME
		FROM information_schema.TABLE_CONSTRAINTS
		WHERE CONSTRAINT_SCHEMA = DATABASE() AND TABLE_NAME = ? AND CONSTRAINT_TYPE = ?`,
		tableName, constraintType)
	require.NoError(t, err)
	defer rows.Close() //nolint:errcheck // test cleanup
	var names []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, rows.Err())
	return names
}
