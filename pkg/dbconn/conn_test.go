package dbconn

import (
	"context"
	"crypto/x509"
	"database/sql"
	"encoding/pem"
	"fmt"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func assertDSNConfig(t *testing.T, dsnStr string, user, password, addr, dbName, tlsConfig string, interpolateParams bool) {
	t.Helper()
	cfg, err := mysql.ParseDSN(dsnStr)
	require.NoError(t, err)
	if cfg == nil {
		return
	}
	require.Equal(t, user, cfg.User)
	require.Equal(t, password, cfg.Passwd)
	require.Equal(t, addr, cfg.Addr)
	require.Equal(t, dbName, cfg.DBName)
	require.Equal(t, tlsConfig, cfg.TLSConfig)
	require.True(t, cfg.AllowNativePasswords)
	require.True(t, cfg.RejectReadOnly)
	require.Equal(t, interpolateParams, cfg.InterpolateParams)
	require.Equal(t, "utf8mb4_bin", cfg.Collation)
	require.Equal(t, `"NO_AUTO_VALUE_ON_ZERO"`, cfg.Params["sql_mode"])
	require.Equal(t, `"+00:00"`, cfg.Params["time_zone"])
	require.Equal(t, `"read-committed"`, cfg.Params["transaction_isolation"])
}

func TestNewDSN(t *testing.T) {
	// Start with a basic example
	dsn := "root:password@tcp(127.0.0.1:3306)/test"
	resp, err := newDSN(dsn, NewDBConfig())
	require.NoError(t, err)
	assertDSNConfig(t, resp, "root", "password", "127.0.0.1:3306", "test", "custom", false)

	// With interpolate on.
	config := NewDBConfig()
	config.InterpolateParams = true
	resp, err = newDSN(dsn, config)
	require.NoError(t, err)
	assertDSNConfig(t, resp, "root", "password", "127.0.0.1:3306", "test", "custom", true)

	// Also with TLS for non-RDS hosts (now includes tls=custom)
	dsn = "root:password@tcp(mydbhost.internal:3306)/test"
	resp, err = newDSN(dsn, NewDBConfig())
	require.NoError(t, err)
	assertDSNConfig(t, resp, "root", "password", "mydbhost.internal:3306", "test", "custom", false)

	// However, if it is RDS - it will be changed to use rds bundle.
	dsn = "root:password@tcp(tern-001.cluster-ro-ckxxxxxxvm.us-west-2.rds.amazonaws.com)/test"
	resp, err = newDSN(dsn, NewDBConfig())
	require.NoError(t, err)
	assertDSNConfig(t, resp, "root", "password", "tern-001.cluster-ro-ckxxxxxxvm.us-west-2.rds.amazonaws.com:3306", "test", "rds", false)

	// This is with optional port too
	dsn = "root:password@tcp(tern-001.cluster-ro-ckxxxxxxvm.us-west-2.rds.amazonaws.com:12345)/test"
	resp, err = newDSN(dsn, NewDBConfig())
	require.NoError(t, err)
	assertDSNConfig(t, resp, "root", "password", "tern-001.cluster-ro-ckxxxxxxvm.us-west-2.rds.amazonaws.com:12345", "test", "rds", false)

	// Password with special characters (e.g. AWS IAM auth token with ?, @, &)
	iamToken := "dbhost.rds.amazonaws.com:3306/?Action=connect&DBUser=iam_user&X-Amz-Signature=abc123"
	dsn = fmt.Sprintf("iam_user:%s@tcp(host.docker.internal:8410)/mydb", iamToken)
	resp, err = newDSN(dsn, NewDBConfig())
	require.NoError(t, err)
	assertDSNConfig(t, resp, "iam_user", iamToken, "host.docker.internal:8410", "mydb", "custom", false)

	// DSN with explicit tls parameter — TLS config preserved, but session vars still applied
	dsn = "root:password@tcp(127.0.0.1:3306)/test?tls=skip-verify"
	resp, err = newDSN(dsn, NewDBConfig())
	require.NoError(t, err)
	assertDSNConfig(t, resp, "root", "password", "127.0.0.1:3306", "test", "skip-verify", false)

	// Invalid DSN, can't parse.
	dsn = "invalid"
	resp, err = newDSN(dsn, NewDBConfig())
	require.Error(t, err)
	require.Empty(t, resp)
}

func TestNewDSNAllowNativePasswords(t *testing.T) {
	// Verify AllowNativePasswords is true for both TLS-enabled and TLS-disabled DSNs.
	// This is important because Spirit's PREFERRED TLS mode falls back to a DISABLED
	// DSN when TLS is unavailable, and both paths must support mysql_native_password.
	dsn := "root:password@tcp(127.0.0.1:3306)/test"

	// Default (PREFERRED) mode — TLS enabled
	resp, err := newDSN(dsn, NewDBConfig())
	require.NoError(t, err)
	cfg, err := mysql.ParseDSN(resp)
	require.NoError(t, err)
	require.True(t, cfg.AllowNativePasswords, "AllowNativePasswords must be true with TLS enabled")

	// DISABLED mode — the fallback path used when TLS is unavailable
	config := NewDBConfig()
	config.TLSMode = "DISABLED"
	resp, err = newDSN(dsn, config)
	require.NoError(t, err)
	cfg, err = mysql.ParseDSN(resp)
	require.NoError(t, err)
	require.True(t, cfg.AllowNativePasswords, "AllowNativePasswords must be true with TLS disabled (fallback path)")
	require.NotContains(t, resp, "allowNativePasswords=false",
		"DSN must not contain allowNativePasswords=false")
}

func TestNewDSNAllowCleartextPasswords(t *testing.T) {
	// With TLS enabled (default PREFERRED mode), AllowCleartextPasswords should be true
	dsn := "root:password@tcp(127.0.0.1:3306)/test"
	resp, err := newDSN(dsn, NewDBConfig())
	require.NoError(t, err)
	cfg, err := mysql.ParseDSN(resp)
	require.NoError(t, err)
	require.NotEmpty(t, cfg.TLSConfig, "TLS should be configured in default mode")
	require.True(t, cfg.AllowCleartextPasswords, "AllowCleartextPasswords should be true when TLS is enabled")

	// With TLS disabled, AllowCleartextPasswords should be false
	config := NewDBConfig()
	config.TLSMode = "DISABLED"
	resp, err = newDSN(dsn, config)
	require.NoError(t, err)
	cfg, err = mysql.ParseDSN(resp)
	require.NoError(t, err)
	require.Empty(t, cfg.TLSConfig, "TLS should not be configured in DISABLED mode")
	require.False(t, cfg.AllowCleartextPasswords, "AllowCleartextPasswords should be false when TLS is disabled")
}

func TestNewConn(t *testing.T) {
	db, err := New("invalid", NewDBConfig())
	require.Error(t, err)
	require.Nil(t, db)

	db, err = New(testutils.DSN(), NewDBConfig())
	require.NoError(t, err)
	require.NotNil(t, db)
	defer utils.CloseAndLog(db)
	for range 10 {
		var resp int
		err = db.QueryRowContext(t.Context(), "SELECT 1").Scan(&resp)
		require.NoError(t, err)
		require.Equal(t, 1, resp)
	}
	// New on syntactically valid but won't respond to ping.
	db, err = New("root:wrongpassword@tcp(127.0.0.1)/doesnotexist", NewDBConfig())
	require.Error(t, err)
	require.Nil(t, db)
}

// TestSetPoolSizeKeepsIdleWithOpen pins the reason SetPoolSize exists: an idle
// limit below the open limit makes database/sql close connections on release
// and redial on the next acquire, which during the copy phase is continuous
// churn against the target. The two limits must move together at every call
// site, including the ones that ratchet a pool after it was created.
func TestSetPoolSizeKeepsIdleWithOpen(t *testing.T) {
	db, err := New(testutils.DSN(), NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	// New() itself must not leave the pool churning: the default open limit is
	// 32, so a pool that retains only the old hardcoded 10 would fail here.
	require.Equal(t, 32, db.Stats().MaxOpenConnections)
	require.Equal(t, 16, idleAfterCycling(t, db, 16))

	SetPoolSize(db, 20)
	require.Equal(t, 20, db.Stats().MaxOpenConnections)
	require.Equal(t, 20, idleAfterCycling(t, db, 20),
		"a ratcheted pool must retain everything it is allowed to open")

	// Shrinking (the cutover path) applies to both, so a smaller pool does not
	// retain more idle connections than it may open.
	SetPoolSize(db, 5)
	require.Equal(t, 5, db.Stats().MaxOpenConnections)
	require.Equal(t, 5, idleAfterCycling(t, db, 5))

	// Non-positive means unlimited to SetMaxOpenConns. The idle limit is left
	// alone rather than set to a nonsense value, since database/sql has no
	// "unlimited idle" setting.
	SetPoolSize(db, 0)
	require.Equal(t, 0, db.Stats().MaxOpenConnections)
	require.Equal(t, 5, idleAfterCycling(t, db, 8),
		"a non-positive size must leave the idle limit untouched")
}

// idleAfterCycling opens n connections, returns them all, and reports how many
// the pool kept. database/sql does not expose MaxIdleConns, but it discards
// anything returned beyond that limit, so the retained count is min(n,
// MaxIdleConns) — which is exactly the churn behaviour under test.
func idleAfterCycling(t *testing.T, db *sql.DB, n int) int {
	t.Helper()
	conns := make([]*sql.Conn, 0, n)
	for range n {
		c, err := db.Conn(t.Context())
		require.NoError(t, err)
		conns = append(conns, c)
	}
	for _, c := range conns {
		require.NoError(t, c.Close())
	}
	return db.Stats().Idle
}

func TestNewConnRejectsReadOnlyConnections(t *testing.T) {
	// Database connection check
	db, err := New(testutils.DSN(), NewDBConfig())
	require.NoError(t, err)
	if db != nil {
		utils.CloseAndLog(db)
	}

	testutils.RunSQL(t, "DROP TABLE IF EXISTS conn_read_only")
	testutils.RunSQL(t, "CREATE TABLE conn_read_only (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")

	config := NewDBConfig()
	// Setting the connection pool size = 1 && transaction_read_only = 1 for the session.
	// This ensures that if the test passes, the connection was definitely recycled by rejectReadOnly=true.
	config.MaxOpenConnections = 1
	db, err = New(testutils.DSN(), config)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	_, err = db.ExecContext(context.Background(), "set session transaction_read_only = 1")
	require.NoError(t, err)

	// This would error, but `database/sql` automatically retries on a
	// new connection which is not read-only, and eventually succeed.
	// See also: rejectReadOnly test in `go-sql-driver/mysql`: https://github.com/go-sql-driver/mysql/blob/52c1917d99904701db2b0e4f14baffa948009cd7/driver_test.go#L2270-L2301
	_, err = db.ExecContext(context.Background(), "insert into conn_read_only values (1, 2, 3)")
	require.NoError(t, err)

	var count int
	err = db.QueryRowContext(context.Background(), "select count(*) from conn_read_only where a = 1").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestValidCertificateBundle(t *testing.T) {
	// parse certificate bundle
	var block *pem.Block
	foundCertificates := false
	remaining := rdsGlobalBundle
	for {
		block, remaining = pem.Decode(remaining)
		if block == nil {
			break
		}
		_, err := x509.ParseCertificate(block.Bytes)
		require.NoError(t, err, "Failed to parse certificate")
		foundCertificates = true
	}

	// ensure that at least one certificate was parsed
	require.True(t, foundCertificates, "No certificates found in bundle")
}
