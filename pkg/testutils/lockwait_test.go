package testutils

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/block/mysql"
	parsermysql "github.com/block/spirit/pkg/parser/mysql"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A statement blocked on a leaked table lock must fail within the bound and
// name the session holding the lock, instead of hanging until the test binary
// times out.
func TestHelperLockWaitIsBoundedAndDiagnosed(t *testing.T) {
	tt := NewTestTable(t, "testutils_lockwait",
		`CREATE TABLE testutils_lockwait (id INT NOT NULL PRIMARY KEY)`)

	holder, err := tt.DB.Conn(t.Context())
	require.NoError(t, err)
	defer func() {
		_, _ = holder.ExecContext(context.Background(), "UNLOCK TABLES")
		_ = holder.Close()
	}()
	var holderID int64
	require.NoError(t, holder.QueryRowContext(t.Context(), "SELECT CONNECTION_ID()").Scan(&holderID))
	_, err = holder.ExecContext(t.Context(), "LOCK TABLES testutils_lockwait WRITE")
	require.NoError(t, err)

	dsn, err := boundedDSN(DSN(), time.Second)
	require.NoError(t, err)
	db, err := sql.Open(driverName, dsn)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	start := time.Now()
	err = execBounded(t.Context(), db, DSN(), "DROP TABLE IF EXISTS testutils_lockwait")
	require.Error(t, err)
	assert.Less(t, time.Since(start), 20*time.Second)
	myErr, ok := errors.AsType[*mysql.MySQLError](err)
	require.True(t, ok)
	assert.Equal(t, parsermysql.ErrLockWaitTimeout, int(myErr.Number))
	assert.Contains(t, err.Error(), "testutils_lockwait")
	assert.Contains(t, err.Error(), fmt.Sprintf("processlist_id=%d", holderID))
}

func TestBoundedDSNKeepsExplicitLockWaitTimeout(t *testing.T) {
	dsn, err := boundedDSN("u:p@tcp(127.0.0.1:3306)/test?lock_wait_timeout=5", time.Minute)
	require.NoError(t, err)
	assert.Contains(t, dsn, "lock_wait_timeout=5")
	dsn, err = boundedDSN("u:p@tcp(127.0.0.1:3306)/test", 1500*time.Millisecond)
	require.NoError(t, err)
	assert.Contains(t, dsn, "lock_wait_timeout=2")
}

// TestHelperLockWaitIsShorterThanCleanupDeadline guards the ordering the
// lock holder diagnostics depend on: the server's 1205 must arrive before the
// client's context deadline on setup and cleanup drops.
func TestHelperLockWaitIsShorterThanCleanupDeadline(t *testing.T) {
	require.Less(t, helperLockWaitTimeout, testCleanupTimeout)
}

// TestDSNForDatabaseKeepsParameters verifies that only the database name is
// replaced, so an explicit lock_wait_timeout in the DSN still wins.
func TestDSNForDatabaseKeepsParameters(t *testing.T) {
	t.Setenv("MYSQL_DSN", "u:p@tcp(127.0.0.1:3306)/test?lock_wait_timeout=5")
	assert.Equal(t, "u:p@tcp(127.0.0.1:3306)/other?lock_wait_timeout=5", DSNForDatabase("other"))
	assert.Equal(t, "u:p@tcp(127.0.0.1:3306)/?lock_wait_timeout=5", DSNForDatabase(""))
	bounded, err := boundedDSN(DSNForDatabase("other"), helperLockWaitTimeout)
	require.NoError(t, err)
	assert.Contains(t, bounded, "lock_wait_timeout=5")
}

// TestHelperConnectionsAreBounded pins the wiring: the connections the
// helpers open must carry the bounded lock_wait_timeout.
func TestHelperConnectionsAreBounded(t *testing.T) {
	want := int(helperLockWaitTimeout / time.Second)
	read := func(db *sql.DB) int {
		var got int
		require.NoError(t, db.QueryRowContext(t.Context(), "SELECT @@session.lock_wait_timeout").Scan(&got))
		return got
	}

	db, err := openBounded(DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	assert.Equal(t, want, read(db))

	tt := NewTestTable(t, "testutils_wiring",
		`CREATE TABLE testutils_wiring (id INT NOT NULL PRIMARY KEY)`)
	assert.Equal(t, want, read(tt.DB))

	// The DSN handed to spirit itself must stay unbounded.
	plain, err := sql.Open(driverName, DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(plain)
	assert.Greater(t, read(plain), want)
}

// TestDropArtifactsReportsLockHolders exercises the cleanup path from #1327:
// dropArtifacts must surface the 1205 and the holder, not just a timeout.
// tt.DB is swapped for a 1s-bounded handle to keep the test fast.
func TestDropArtifactsReportsLockHolders(t *testing.T) {
	tt := NewTestTable(t, "testutils_dropdiag",
		`CREATE TABLE testutils_dropdiag (id INT NOT NULL PRIMARY KEY)`)

	holder, err := tt.DB.Conn(t.Context())
	require.NoError(t, err)
	defer func() {
		_, _ = holder.ExecContext(context.Background(), "UNLOCK TABLES")
		_ = holder.Close()
	}()
	var holderID int64
	require.NoError(t, holder.QueryRowContext(t.Context(), "SELECT CONNECTION_ID()").Scan(&holderID))
	_, err = holder.ExecContext(t.Context(), "LOCK TABLES testutils_dropdiag WRITE")
	require.NoError(t, err)

	dsn, err := boundedDSN(DSN(), time.Second)
	require.NoError(t, err)
	fast, err := sql.Open(driverName, dsn)
	require.NoError(t, err)
	defer utils.CloseAndLog(fast)
	probe := &TestTable{Name: tt.Name, DB: fast, dsn: DSN()}

	ctx, cancel := newTestCleanupContext()
	defer cancel()
	err = probe.dropArtifacts(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "metadata lock holders")
	assert.Contains(t, err.Error(), fmt.Sprintf("processlist_id=%d", holderID))
}
