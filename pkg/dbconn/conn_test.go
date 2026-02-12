package dbconn

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
)

func assertDSNConfig(t *testing.T, dsnStr string, user, password, addr, dbName, tlsConfig string, interpolateParams bool) {
	t.Helper()
	cfg, err := mysql.ParseDSN(dsnStr)
	assert.NoError(t, err)
	if cfg == nil {
		return
	}
	assert.Equal(t, user, cfg.User)
	assert.Equal(t, password, cfg.Passwd)
	assert.Equal(t, addr, cfg.Addr)
	assert.Equal(t, dbName, cfg.DBName)
	assert.Equal(t, tlsConfig, cfg.TLSConfig)
	assert.Equal(t, true, cfg.AllowNativePasswords)
	assert.Equal(t, true, cfg.RejectReadOnly)
	assert.Equal(t, interpolateParams, cfg.InterpolateParams)
	assert.Equal(t, "utf8mb4_bin", cfg.Collation)
	assert.Equal(t, `""`, cfg.Params["sql_mode"])
	assert.Equal(t, `"+00:00"`, cfg.Params["time_zone"])
	assert.Equal(t, `"read-committed"`, cfg.Params["transaction_isolation"])
}

func TestNewDSN(t *testing.T) {
	// Start with a basic example
	dsn := "root:password@tcp(127.0.0.1:3306)/test"
	resp, err := newDSN(dsn, NewDBConfig())
	assert.NoError(t, err)
	assertDSNConfig(t, resp, "root", "password", "127.0.0.1:3306", "test", "custom", false)

	// With interpolate on.
	config := NewDBConfig()
	config.InterpolateParams = true
	resp, err = newDSN(dsn, config)
	assert.NoError(t, err)
	assertDSNConfig(t, resp, "root", "password", "127.0.0.1:3306", "test", "custom", true)

	// Also with TLS for non-RDS hosts (now includes tls=custom)
	dsn = "root:password@tcp(mydbhost.internal:3306)/test"
	resp, err = newDSN(dsn, NewDBConfig())
	assert.NoError(t, err)
	assertDSNConfig(t, resp, "root", "password", "mydbhost.internal:3306", "test", "custom", false)

	// However, if it is RDS - it will be changed to use rds bundle.
	dsn = "root:password@tcp(tern-001.cluster-ro-ckxxxxxxvm.us-west-2.rds.amazonaws.com)/test"
	resp, err = newDSN(dsn, NewDBConfig())
	assert.NoError(t, err)
	assertDSNConfig(t, resp, "root", "password", "tern-001.cluster-ro-ckxxxxxxvm.us-west-2.rds.amazonaws.com:3306", "test", "rds", false)

	// This is with optional port too
	dsn = "root:password@tcp(tern-001.cluster-ro-ckxxxxxxvm.us-west-2.rds.amazonaws.com:12345)/test"
	resp, err = newDSN(dsn, NewDBConfig())
	assert.NoError(t, err)
	assertDSNConfig(t, resp, "root", "password", "tern-001.cluster-ro-ckxxxxxxvm.us-west-2.rds.amazonaws.com:12345", "test", "rds", false)

	// Password with special characters (e.g. AWS IAM auth token with ?, @, &)
	iamToken := "dbhost.rds.amazonaws.com:3306/?Action=connect&DBUser=iam_user&X-Amz-Signature=abc123"
	dsn = fmt.Sprintf("iam_user:%s@tcp(host.docker.internal:8410)/mydb", iamToken)
	resp, err = newDSN(dsn, NewDBConfig())
	assert.NoError(t, err)
	assertDSNConfig(t, resp, "iam_user", iamToken, "host.docker.internal:8410", "mydb", "custom", false)

	// DSN with explicit tls parameter should be preserved as-is
	dsn = "root:password@tcp(127.0.0.1:3306)/test?tls=skip-verify"
	resp, err = newDSN(dsn, NewDBConfig())
	assert.NoError(t, err)
	assert.Equal(t, dsn, resp, "DSN with explicit tls parameter should be returned unchanged")

	// Invalid DSN, can't parse.
	dsn = "invalid"
	resp, err = newDSN(dsn, NewDBConfig())
	assert.Error(t, err)
	assert.Empty(t, resp)
}

func TestNewDSNAllowCleartextPasswords(t *testing.T) {
	// With TLS enabled (default PREFERRED mode), AllowCleartextPasswords should be true
	dsn := "root:password@tcp(127.0.0.1:3306)/test"
	resp, err := newDSN(dsn, NewDBConfig())
	assert.NoError(t, err)
	cfg, err := mysql.ParseDSN(resp)
	assert.NoError(t, err)
	assert.NotEmpty(t, cfg.TLSConfig, "TLS should be configured in default mode")
	assert.True(t, cfg.AllowCleartextPasswords, "AllowCleartextPasswords should be true when TLS is enabled")

	// With TLS disabled, AllowCleartextPasswords should be false
	config := NewDBConfig()
	config.TLSMode = "DISABLED"
	resp, err = newDSN(dsn, config)
	assert.NoError(t, err)
	cfg, err = mysql.ParseDSN(resp)
	assert.NoError(t, err)
	assert.Empty(t, cfg.TLSConfig, "TLS should not be configured in DISABLED mode")
	assert.False(t, cfg.AllowCleartextPasswords, "AllowCleartextPasswords should be false when TLS is disabled")
}

func TestNewConn(t *testing.T) {
	db, err := New("invalid", NewDBConfig())
	assert.Error(t, err)
	assert.Nil(t, db)

	db, err = New(testutils.DSN(), NewDBConfig())
	assert.NoError(t, err)
	assert.NotNil(t, db)
	defer utils.CloseAndLog(db)
	for range 10 {
		var resp int
		err = db.QueryRowContext(t.Context(), "SELECT 1").Scan(&resp)
		assert.NoError(t, err)
		assert.Equal(t, 1, resp)
	}
	// New on syntactically valid but won't respond to ping.
	db, err = New("root:wrongpassword@tcp(127.0.0.1)/doesnotexist", NewDBConfig())
	assert.Error(t, err)
	assert.Nil(t, db)
}

func TestNewConnRejectsReadOnlyConnections(t *testing.T) {
	// Database connection check
	db, err := New(testutils.DSN(), NewDBConfig())
	assert.NoError(t, err)
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
	assert.NoError(t, err)
	defer utils.CloseAndLog(db)
	_, err = db.ExecContext(context.Background(), "set session transaction_read_only = 1")
	assert.NoError(t, err)

	// This would error, but `database/sql` automatically retries on a
	// new connection which is not read-only, and eventually succeed.
	// See also: rejectReadOnly test in `go-sql-driver/mysql`: https://github.com/go-sql-driver/mysql/blob/52c1917d99904701db2b0e4f14baffa948009cd7/driver_test.go#L2270-L2301
	_, err = db.ExecContext(context.Background(), "insert into conn_read_only values (1, 2, 3)")
	assert.NoError(t, err)

	var count int
	err = db.QueryRowContext(context.Background(), "select count(*) from conn_read_only where a = 1").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestValidCertificateBundle(t *testing.T) {
	// parse certificate bundle
	var block *pem.Block
	foundCertificates := false
	for {
		block, rdsGlobalBundle = pem.Decode(rdsGlobalBundle)
		if block == nil {
			break
		}
		_, err := x509.ParseCertificate(block.Bytes)
		assert.NoError(t, err, "Failed to parse certificate")
		foundCertificates = true
	}

	// ensure that at least one certificate was parsed
	assert.True(t, foundCertificates, "No certificates found in bundle")
}
