package check

import (
	"database/sql"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTablePhaseMembership pins the checks PhaseTable carries. A check joining
// the phase has to clear the phase's contract first: it decides from the table
// as it stands on the server, with only one source connection and the table's
// schema and name supplied, and its failure says the copy process refuses the
// table.
func TestTablePhaseMembership(t *testing.T) {
	assert.Equal(t, []string{"hasforeignkeys", "hastriggers"}, Checks(Migration, PhaseTable))
}

// TestTableRefusal runs the table checks the way an external caller does: a
// connection, a schema, and a table name. Both ends of a foreign key and a
// table with a trigger are refused, with the reason preflight would give; a
// table outside any of those relationships is not.
func TestTableRefusal(t *testing.T) {
	dbName, db := testutils.CreateUniqueTestDatabase(t)
	for _, stmt := range []string{
		"CREATE TABLE groups_parent (id BIGINT NOT NULL PRIMARY KEY, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)",
		"CREATE TABLE owners_child (id BIGINT NOT NULL PRIMARY KEY, group_id BIGINT NOT NULL, " +
			"CONSTRAINT fk_owners_group FOREIGN KEY (group_id) REFERENCES groups_parent (id) ON DELETE CASCADE)",
		"CREATE TABLE audited (id BIGINT NOT NULL PRIMARY KEY, amount DECIMAL(10,2))",
		"CREATE TRIGGER audited_ins BEFORE INSERT ON audited FOR EACH ROW SET @total = @total + NEW.amount",
		"CREATE TABLE standalone (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(100))",
	} {
		testutils.RunSQLInDatabase(t, dbName, stmt)
	}

	tests := []struct {
		table      string
		wantReason string
	}{
		{table: "groups_parent", wantReason: "tables with existing foreign key constraints are not supported"},
		{table: "owners_child", wantReason: "tables with existing foreign key constraints are not supported"},
		{table: "audited", wantReason: "tables with triggers associated are not supported"},
		{table: "standalone"},
	}
	for _, tt := range tests {
		t.Run(tt.table, func(t *testing.T) {
			reason, refused, err := TableRefusal(t.Context(), db, dbName, tt.table, nil)
			require.NoError(t, err)
			if tt.wantReason == "" {
				assert.False(t, refused)
				assert.Empty(t, reason)
				return
			}
			assert.True(t, refused)
			assert.Equal(t, tt.wantReason, reason)
		})
	}
}

// TestTableRefusalFollowsTheServer checks that the verdict is read from the
// server each time: once the foreign key is dropped, neither of its former
// ends is refused.
func TestTableRefusalFollowsTheServer(t *testing.T) {
	dbName, db := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY)")
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE child (id BIGINT NOT NULL PRIMARY KEY, parent_id BIGINT NOT NULL, "+
		"CONSTRAINT fk_child_parent FOREIGN KEY (parent_id) REFERENCES parent (id))")

	_, refused, err := TableRefusal(t.Context(), db, dbName, "parent", nil)
	require.NoError(t, err)
	require.True(t, refused)

	testutils.RunSQLInDatabase(t, dbName, "ALTER TABLE child DROP FOREIGN KEY fk_child_parent")
	for _, table := range []string{"parent", "child"} {
		reason, refused, err := TableRefusal(t.Context(), db, dbName, table, nil)
		require.NoError(t, err)
		assert.False(t, refused, "table %s: %s", table, reason)
	}
}

// TestTableRefusalReadFailure checks that a server the checks cannot read is
// an error, not a refusal.
func TestTableRefusalReadFailure(t *testing.T) {
	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE standalone (id BIGINT NOT NULL PRIMARY KEY)")
	db, openErr := sql.Open("block-mysql", testutils.DSNForDatabase(dbName))
	require.NoError(t, openErr)
	require.NoError(t, db.Close())

	_, refused, err := TableRefusal(t.Context(), db, dbName, "standalone", nil)
	require.Error(t, err)
	assert.False(t, refused)

	_, refused, err = TableRefusal(t.Context(), nil, dbName, "standalone", nil)
	require.Error(t, err)
	assert.False(t, refused)
}
