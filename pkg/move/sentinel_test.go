package move

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/block/spirit/pkg/sentinel"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func sentinelTestTableExists(t *testing.T, db *sql.DB, schema, name string) bool {
	t.Helper()
	var one int
	err := db.QueryRowContext(t.Context(),
		"SELECT 1 FROM information_schema.tables WHERE table_schema=? AND table_name=?", schema, name).Scan(&one)
	if err == sql.ErrNoRows {
		return false
	}
	require.NoError(t, err)
	return true
}

// TestMoveSentinelDropReleasesCutover: with --defer-cutover, dropping the
// sentinel must RELEASE the cutover and let the move finish — not be seen as a
// schema change that cancels it. The sentinel lives on targets[0], so the drop
// is a target-side DDL that the source-watching change feed must ignore. Uses a
// move-all move (no explicit table list), so the source feed cancels on any
// source DDL — the strongest case.
func TestMoveSentinelDropReleasesCutover(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	src := cfg.Clone()
	src.DBName = "sentrel_src"
	dst := cfg.Clone()
	dst.DBName = "sentrel_dst"

	testutils.RunSQL(t, "DROP DATABASE IF EXISTS sentrel_src")
	testutils.RunSQL(t, "CREATE DATABASE sentrel_src")
	testutils.RunSQL(t, "CREATE TABLE sentrel_src.t1 (id INT PRIMARY KEY, val VARCHAR(255))")
	testutils.RunSQL(t, "INSERT INTO sentrel_src.t1 VALUES (1,'one'),(2,'two')")
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS sentrel_dst")
	testutils.RunSQL(t, "CREATE DATABASE sentrel_dst")

	ctl, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(ctl)

	m := &Move{
		SourceDSN:    src.FormatDSN(),
		TargetDSN:    dst.FormatDSN(),
		Threads:      1,
		WriteThreads: 1,
		DeferCutOver: true,
	}
	runner, err := NewRunner(m)
	require.NoError(t, err)
	defer utils.CloseAndLog(runner)
	var cutoverCalled bool
	runner.SetCutover(func(context.Context) error { cutoverCalled = true; return nil })

	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(context.Background()) }()

	// Wait until the move is blocked on the sentinel.
	waitForMoveStatus(t, runner, status.WaitingOnSentinelTable, errCh)

	// Drop the sentinel on targets[0] to release the cutover.
	testutils.RunSQL(t, "DROP TABLE sentrel_dst."+sentinel.TableName)

	select {
	case err := <-errCh:
		require.NoError(t, err, "dropping the sentinel must release the cutover, not cancel the move")
	case <-time.After(60 * time.Second):
		t.Fatal("move did not complete after the sentinel was dropped")
	}
	require.True(t, cutoverCalled, "cutover must run once the sentinel is dropped")
	require.True(t, sentinelTestTableExists(t, ctl, "sentrel_src", "t1_old"), "source retired after cutover")
	require.True(t, sentinelTestTableExists(t, ctl, "sentrel_dst", "t1"), "target serving after cutover")
}
