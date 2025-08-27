package dbconn

import (
	"crypto/x509"
	"encoding/pem"
	"testing"

	"github.com/block/spirit/pkg/testutils"

	"github.com/stretchr/testify/assert"
)

func TestNewDSN(t *testing.T) {
	// Start with a basic example
	dsn := "root:password@tcp(127.0.0.1:3306)/test"
	resp, err := newDSN(dsn, NewDBConfig())
	assert.NoError(t, err)
	assert.Equal(t, "root:password@tcp(127.0.0.1:3306)/test?sql_mode=%22%22&time_zone=%22%2B00%3A00%22&innodb_lock_wait_timeout=3&lock_wait_timeout=30&range_optimizer_max_mem_size=0&transaction_isolation=%22read-committed%22&charset=binary&collation=binary&rejectReadOnly=true&interpolateParams=false", resp)

	// With interpolate on.
	config := NewDBConfig()
	config.InterpolateParams = true
	resp, err = newDSN(dsn, config)
	assert.NoError(t, err)
	assert.Equal(t, "root:password@tcp(127.0.0.1:3306)/test?sql_mode=%22%22&time_zone=%22%2B00%3A00%22&innodb_lock_wait_timeout=3&lock_wait_timeout=30&range_optimizer_max_mem_size=0&transaction_isolation=%22read-committed%22&charset=binary&collation=binary&rejectReadOnly=true&interpolateParams=true", resp)

	// Also without TLS options
	dsn = "root:password@tcp(mydbhost.internal:3306)/test"
	resp, err = newDSN(dsn, NewDBConfig())
	assert.NoError(t, err)
	assert.Equal(t, "root:password@tcp(mydbhost.internal:3306)/test?sql_mode=%22%22&time_zone=%22%2B00%3A00%22&innodb_lock_wait_timeout=3&lock_wait_timeout=30&range_optimizer_max_mem_size=0&transaction_isolation=%22read-committed%22&charset=binary&collation=binary&rejectReadOnly=true&interpolateParams=false", resp) // unchanged

	// However, if it is RDS - it will be changed.
	dsn = "root:password@tcp(tern-001.cluster-ro-ckxxxxxxvm.us-west-2.rds.amazonaws.com)/test"
	resp, err = newDSN(dsn, NewDBConfig())
	assert.NoError(t, err)
	assert.Equal(t, "root:password@tcp(tern-001.cluster-ro-ckxxxxxxvm.us-west-2.rds.amazonaws.com)/test?tls=rds&sql_mode=%22%22&time_zone=%22%2B00%3A00%22&innodb_lock_wait_timeout=3&lock_wait_timeout=30&range_optimizer_max_mem_size=0&transaction_isolation=%22read-committed%22&charset=binary&collation=binary&rejectReadOnly=true&interpolateParams=false", resp)

	// This is with optional port too
	dsn = "root:password@tcp(tern-001.cluster-ro-ckxxxxxxvm.us-west-2.rds.amazonaws.com:12345)/test"
	resp, err = newDSN(dsn, NewDBConfig())
	assert.NoError(t, err)
	assert.Equal(t, "root:password@tcp(tern-001.cluster-ro-ckxxxxxxvm.us-west-2.rds.amazonaws.com:12345)/test?tls=rds&sql_mode=%22%22&time_zone=%22%2B00%3A00%22&innodb_lock_wait_timeout=3&lock_wait_timeout=30&range_optimizer_max_mem_size=0&transaction_isolation=%22read-committed%22&charset=binary&collation=binary&rejectReadOnly=true&interpolateParams=false", resp)

	// Invalid DSN, can't parse.
	dsn = "invalid"
	resp, err = newDSN(dsn, NewDBConfig())
	assert.Error(t, err)
	assert.Empty(t, resp)
}

func TestNewConn(t *testing.T) {
	db, err := New("invalid", NewDBConfig())
	assert.Error(t, err)
	assert.Nil(t, db)

	db, err = New(testutils.DSN(), NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()

	var resp int
	err = db.QueryRow("SELECT 1").Scan(&resp)
	assert.NoError(t, err)
	assert.Equal(t, 1, resp)

	// New on syntactically valid but won't respond to ping.
	db, err = New("root:wrongpassword@tcp(127.0.0.1)/doesnotexist", NewDBConfig())
	assert.Error(t, err)
	assert.Nil(t, db)
}

func TestNewConnRejectsReadOnlyConnections(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS conn_read_only")
	testutils.RunSQL(t, "CREATE TABLE conn_read_only (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")

	config := NewDBConfig()
	// Setting the connection pool size = 1 && transaction_read_only = 1 for the session.
	// This ensures that if the test passes, the connection was definitely recycled by rejectReadOnly=true.
	config.MaxOpenConnections = 1
	db, err := New(testutils.DSN(), config)
	assert.NoError(t, err)
	defer db.Close()
	_, err = db.Exec("set session transaction_read_only = 1")
	assert.NoError(t, err)

	// This would error, but `database/sql` automatically retries on a
	// new connection which is not read-only, and eventually succeed.
	// See also: rejectReadOnly test in `go-sql-driver/mysql`: https://github.com/go-sql-driver/mysql/blob/52c1917d99904701db2b0e4f14baffa948009cd7/driver_test.go#L2270-L2301
	_, err = db.Exec("insert into conn_read_only values (1, 2, 3)")
	assert.NoError(t, err)

	var count int
	err = db.QueryRow("select count(*) from conn_read_only where a = 1").Scan(&count)
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
