package migration

import (
	"context"
	"testing"

	"github.com/block/mysql"
	parsermysql "github.com/block/spirit/pkg/parser/mysql"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

func TestAddExpressionDefault(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "expression_defaults", `CREATE TABLE expression_defaults (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, payload VARCHAR(32) NOT NULL)`)
	tt.SeedRows(t, "INSERT INTO expression_defaults (payload) SELECT 'original'", 128)
	r := NewTestRunner(t, "expression_defaults", "ADD COLUMN uuid CHAR(36) NOT NULL DEFAULT (UUID()) AFTER id, ADD COLUMN token BINARY(16) NOT NULL DEFAULT (UUID_TO_BIN(UUID()))")
	t.Cleanup(func() { require.NoError(t, r.Close()) })
	require.NoError(t, r.Run(t.Context()))
	require.False(t, r.usedInstantDDL)
	var count, uuids, tokens, valid int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), `SELECT COUNT(*), COUNT(DISTINCT uuid), COUNT(DISTINCT token), SUM(IS_UUID(uuid)) FROM expression_defaults`).Scan(&count, &uuids, &tokens, &valid))
	require.Equal(t, 128, count)
	require.Equal(t, count, uuids)
	require.Equal(t, count, tokens)
	require.Equal(t, count, valid)
	_, err := tt.DB.ExecContext(t.Context(), "INSERT INTO expression_defaults (payload) VALUES ('after cutover')")
	require.NoError(t, err)
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), `SELECT IS_UUID(uuid), LENGTH(token) FROM expression_defaults WHERE payload = 'after cutover'`).Scan(&valid, &count))
	require.Equal(t, 1, valid)
	require.Equal(t, 16, count)
}

// Resume must use the original ALTER and the fully prepared shadow. Source
// updates replay only the shared columns; the new UUID is generated on insert.
func TestAddExpressionDefaultResume(t *testing.T) {
	t.Parallel()
	dbName, db := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE uuid_resume (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, payload INT NOT NULL)")
	testutils.RunSQLInDatabase(t, dbName, "INSERT INTO uuid_resume (payload) VALUES (10)")
	for range 12 {
		testutils.RunSQLInDatabase(t, dbName, "INSERT INTO uuid_resume (payload) SELECT payload FROM uuid_resume")
	}
	const alter = "ADD COLUMN uuid CHAR(36) NOT NULL DEFAULT (UUID()) AFTER id"
	r := NewTestRunner(t, "uuid_resume", alter, WithDBName(dbName), WithDeferCutOver())
	running := startTestRun(t, r.Run, r.Close)
	waitForStatus(t, r, status.WaitingOnSentinelTable, running)
	require.NoError(t, r.DumpCheckpoint(t.Context()))
	running.cancel()
	require.ErrorIs(t, running.wait(t), context.Canceled)
	require.NoError(t, r.Close())

	// These changes must be replayed from the checkpoint when the run resumes.
	testutils.RunSQLInDatabase(t, dbName, "UPDATE uuid_resume SET payload = 11 WHERE id = 1")
	testutils.RunSQLInDatabase(t, dbName, "DELETE FROM uuid_resume WHERE id = 2")
	testutils.RunSQLInDatabase(t, dbName, "INSERT INTO uuid_resume (payload) VALUES (40)")
	testutils.RunSQLInDatabase(t, dbName, "DROP TABLE _spirit_sentinel")
	r2 := NewTestRunner(t, "uuid_resume", alter, WithDBName(dbName))
	t.Cleanup(func() { require.NoError(t, r2.Close()) })
	require.NoError(t, r2.Run(t.Context()))
	require.True(t, r2.usedResumeFromCheckpoint.Load())
	var count, distinct, valid, total int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*), COUNT(DISTINCT uuid), SUM(IS_UUID(uuid)), SUM(payload) FROM uuid_resume").Scan(&count, &distinct, &valid, &total))
	require.Equal(t, 4096, count)
	require.Equal(t, count, distinct)
	require.Equal(t, count, valid)
	require.Equal(t, 40991, total)
}

func TestExpressionDefaultMixedAlterStillFails(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "uuid_mixed", "CREATE TABLE uuid_mixed (id BIGINT NOT NULL PRIMARY KEY, payload INT NOT NULL)")
	_, err := tt.DB.ExecContext(t.Context(), "INSERT INTO uuid_mixed VALUES (1, 42)")
	require.NoError(t, err)
	r := NewTestRunner(t, "uuid_mixed", "ADD COLUMN uuid CHAR(36) NOT NULL DEFAULT (UUID()), DROP COLUMN payload")
	t.Cleanup(func() { require.NoError(t, r.Close()) })
	require.ErrorIs(t, r.Run(t.Context()), &mysql.MySQLError{Number: parsermysql.ErrBinlogUnsafeSystemFunction})
	var payload int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT payload FROM uuid_mixed WHERE id = 1").Scan(&payload))
	require.Equal(t, 42, payload)
	var columns int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'uuid_mixed' AND COLUMN_NAME = 'uuid'").Scan(&columns))
	require.Zero(t, columns)
}
