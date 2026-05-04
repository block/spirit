package migration

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"

	"github.com/go-sql-driver/mysql"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	// Tests run at Debug level so diagnostic logs in the binlog applier
	// path (HasChanged add/drop, deltaMap.Flush stmt + affected_rows) and
	// the copier (per-chunk affected_rows) are captured in CI output.
	// See issue #746.
	slog.SetLogLoggerLevel(slog.LevelDebug)
	status.CheckpointDumpInterval = 100 * time.Millisecond
	status.StatusInterval = 10 * time.Millisecond // the status will be accurate to 1ms
	sentinelCheckInterval = 100 * time.Millisecond
	goleak.VerifyTestMain(m)
}

func TestE2ENullAlterEmpty(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "t1e2e", `CREATE TABLE t1e2e (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	m := NewTestMigration(t, WithTable("t1e2e"), WithAlter("ENGINE=InnoDB"))
	require.NoError(t, m.Run())
}

func TestE2EExplicitAutoIncrementInAlter(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "t1explicit_autoinc", `CREATE TABLE t1explicit_autoinc (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB AUTO_INCREMENT=1000`)

	m := NewTestMigration(t, WithTable("t1explicit_autoinc"),
		WithAlter("ADD COLUMN test_col VARCHAR(255), AUTO_INCREMENT=5000"))
	require.NoError(t, m.Run())

	// Verify that new inserts start from 5000
	testutils.RunSQL(t, "INSERT INTO t1explicit_autoinc (name) VALUES ('test')")
	var insertedID int64
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT MAX(id) FROM t1explicit_autoinc").Scan(&insertedID))
	require.Equal(t, int64(5000), insertedID, "User-specified AUTO_INCREMENT=5000 should be preserved")
}

func TestMissingAlter(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "t1missing", `CREATE TABLE t1missing (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	m := NewTestMigration(t, WithTable("t1missing"), WithAlter(""))
	err := m.Run()
	require.Error(t, err)
	require.ErrorContains(t, err, "alter statement is required")
}

func TestBadDatabaseCredentials(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "t1bad", `CREATE TABLE t1bad (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	m := NewTestMigration(t, WithTable("t1bad"), WithAlter("ENGINE=InnoDB"), WithHost("127.0.0.1:9999"))
	err := m.Run()
	require.Error(t, err)
	require.ErrorContains(t, err, "connect: connection refused")
}

func TestE2ENullAlter1Row(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "t1nullalter", `CREATE TABLE t1nullalter (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	testutils.RunSQL(t, `INSERT INTO t1nullalter (id, name) VALUES (1, 'aaa')`)
	m := NewTestMigration(t, WithTable("t1nullalter"), WithAlter("ENGINE=InnoDB"))
	require.NoError(t, m.Run())
}

func TestE2ENullAlterWithReplicas(t *testing.T) {
	t.Parallel()
	replicaDSN := os.Getenv("REPLICA_DSN")
	if replicaDSN == "" {
		t.Skip("skipping replica tests because REPLICA_DSN not set")
	}
	testutils.WaitForReplicaHealthy(t, replicaDSN, 30*time.Second)
	testutils.NewTestTable(t, "replicatest", `CREATE TABLE replicatest (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	m := NewTestMigration(t, WithTable("replicatest"), WithAlter("ENGINE=InnoDB"),
		WithReplicaDSN(replicaDSN), WithReplicaMaxLag(10*time.Second))
	require.NoError(t, m.Run())
}

// TestRenameInMySQL80 tests that even though renames are not supported,
// if the version is 8.0 it will apply the instant operation before
// the rename check applies. It's only when it needs to actually migrate
// that it won't allow renames.
func TestRenameInMySQL80(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "renamet1", `CREATE TABLE renamet1 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	m := NewTestMigration(t, WithTable("renamet1"), WithAlter("CHANGE name nameNew varchar(255) not null"))
	require.NoError(t, m.Run())
}

// TestUniqueOnNonUniqueData tests that we:
// 1. Fail trying to add a unique index on non-unique data.
// 2. The error does not blame spirit, but is instead suggestive of user-data error.
func TestUniqueOnNonUniqueData(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "uniquet1", `CREATE TABLE uniquet1 (id int not null primary key auto_increment, b int not null, pad1 varbinary(1024))`)
	tt.SeedRows(t, "INSERT INTO uniquet1 (b, pad1) SELECT 1, RANDOM_BYTES(1024)", 100000)
	testutils.RunSQL(t, `UPDATE uniquet1 SET b = id`)
	testutils.RunSQL(t, `UPDATE uniquet1 SET b = 12345 ORDER BY RAND() LIMIT 2`)

	m := NewTestMigration(t, WithTable("uniquet1"), WithAlter("ADD UNIQUE (b)"))
	err := m.Run()
	require.Error(t, err)
	require.ErrorContains(t, err, "checksum failed after several attempts. This is likely related to your statement adding a UNIQUE index on non-unique data")
}

func TestGeneratedColumns(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) {
		testGeneratedColumns(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		testGeneratedColumns(t, true)
	})
}

func testGeneratedColumns(t *testing.T, enableBuffered bool) {
	testutils.NewTestTable(t, "t1generated", `CREATE TABLE t1generated (
		id int not null primary key auto_increment,
		b int not null,
		c int GENERATED ALWAYS AS (b + 1),
		d int
	)`)
	testutils.RunSQL(t, `INSERT INTO t1generated (b, d) VALUES (1, 10), (2, 20), (3, 30)`)
	m := NewTestMigration(t, WithTable("t1generated"), WithAlter("ENGINE=InnoDB"), WithBuffered(enableBuffered))
	require.NoError(t, m.Run())
}

func TestStoredGeneratedColumns(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) {
		testStoredGeneratedColumns(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		testStoredGeneratedColumns(t, true)
	})
}

func testStoredGeneratedColumns(t *testing.T, enableBuffered bool) {
	testutils.NewTestTable(t, "t1stored", `CREATE TABLE t1stored (
		id bigint NOT NULL AUTO_INCREMENT,
		pa bigint DEFAULT NULL,
		p1 bigint DEFAULT NULL,
		p2 bigint DEFAULT NULL,
		s1 tinyint(1) GENERATED ALWAYS AS (if((pa is not null),1,NULL)) STORED,
		s2 tinyint(1) GENERATED ALWAYS AS (if((p1 is not null),1,NULL)) STORED,
		s3 tinyint(1) GENERATED ALWAYS AS (if((p2 is not null),1,NULL)) STORED,
		s4 tinyint(1) GENERATED ALWAYS AS (if((pa <> p2),1,NULL)) STORED,
		p3 int not null,
		PRIMARY KEY (id)
	)`)
	//nolint: dupword
	testutils.RunSQL(t, `INSERT INTO t1stored (pa, p1, p2, p3) VALUES
		(1, 1, 1, 99), (1, NULL, 1, 98), (1, 1, NULL, 97), (1, NULL, NULL, 96),
		(1, 1, 0, 95), (1, 0, 1, 94), (1, 0, 0, 93), (1, NULL, 0, 92),
		(1, 0, NULL, 91), (1, NULL, NULL, 90), (NULL, NULL, NULL, 89)`)

	m := NewTestMigration(t, WithBuffered(enableBuffered),
		WithStatement(`ALTER TABLE t1stored MODIFY COLUMN s4 TINYINT(1) GENERATED ALWAYS AS (IF(pa <> p2 OR (pa IS NULL AND p2 IS NOT NULL) OR (pa IS NOT NULL AND p2 IS NULL), 1, NULL)) STORED`))
	require.NoError(t, m.Run())
}

type testcase struct {
	OldType string
	NewType string
}

// TestBinaryChecksum tests that we can alter a binary column and still get a checksum match.
// It works fine from varbinary(50)->varbinary(100), but not from binary(50)->binary(100),
// without an intermediate cast.
func TestBinaryChecksum(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) {
		testBinaryChecksum(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		testBinaryChecksum(t, true)
	})
}

func testBinaryChecksum(t *testing.T, enableBuffered bool) {
	tests := []testcase{
		{"binary(50)", "varbinary(100)"},
		{"binary(50)", "binary(100)"},
		{"varbinary(100)", "varbinary(50)"},
		{"varbinary(100)", "binary(50)"},
		{"blob", "tinyblob"},
		{"tinyblob", "blob"},
		{"mediumblob", "tinyblob"},
		{"longblob", "mediumblob"},
		{"binary(100)", "blob"},
		{"blob", "binary(100)"},
	}
	for _, test := range tests {
		testutils.NewTestTable(t, "t1varbin", fmt.Sprintf(`CREATE TABLE t1varbin (
			id int not null primary key auto_increment,
			b %s not null
		)`, test.OldType))
		testutils.RunSQL(t, `INSERT INTO t1varbin VALUES (null, 'abcdefg')`)

		m := NewTestMigration(t, WithTable("t1varbin"), WithBuffered(enableBuffered),
			WithAlter(fmt.Sprintf("CHANGE b b %s not null", test.NewType))) //nolint: dupword
		require.NoError(t, m.Run())
	}
}

// TestConvertCharset tests that we can change the character set from latin1 to utf8mb4,
// and that the non 7-bit characters that can be represented in latin1 as 1 byte,
// checksum correctly against their multi-byte utf8mb4 representations
func TestConvertCharset(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) {
		testConvertCharset(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		testConvertCharset(t, true)
	})
}

func testConvertCharset(t *testing.T, enableBuffered bool) {
	testutils.NewTestTable(t, "t1charset", `CREATE TABLE t1charset (
		id int not null primary key auto_increment,
		b varchar(100) not null
	) charset=latin1`)
	testutils.RunSQL(t, `INSERT INTO t1charset VALUES (null, 'à'), (null, '€')`)

	m := NewTestMigration(t, WithTable("t1charset"), WithBuffered(enableBuffered),
		WithAlter("CONVERT TO CHARACTER SET UTF8MB4"))
	require.NoError(t, m.Run())

	// Because utf8mb4 is the superset, it doesn't matter that that's
	// what the checksum casts to. We should be able to convert back as well.
	m = NewTestMigration(t, WithTable("t1charset"), WithBuffered(enableBuffered),
		WithAlter("CONVERT TO CHARACTER SET latin1"))
	require.NoError(t, m.Run())
}

func TestStmtWorkflow(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1s`)

	// Test CREATE TABLE via Statement.
	m := NewTestMigration(t, WithStatement(`CREATE TABLE t1s (
		id int not null primary key auto_increment,
		b varchar(100) not null
	)`))
	require.NoError(t, m.Run())

	// Test ALTER TABLE via Statement.
	m = NewTestMigration(t, WithStatement("ALTER TABLE t1s ADD COLUMN c int"))
	require.NoError(t, m.Run())
	// cleanup
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1s`)
}

// TestUnparsableStatements tests that the behavior is expected in cases
// where we know the TiDB parser does not support the statement. We document
// that we require the TiDB parser to parse the statement for it to execute,
// which feels like a reasonable limitation based on its capabilities.
// Example TiDB bug: https://github.com/pingcap/tidb/issues/54700
func TestUnparsableStatements(t *testing.T) {
	t.Parallel()
	// CREATE TABLE with BLOB DEFAULT — TiDB parser doesn't support this but MySQL does.
	m := NewTestMigration(t, WithStatement(`CREATE TABLE t1parse (id int not null primary key auto_increment, b BLOB DEFAULT ('abc'))`))
	require.NoError(t, m.Run())

	// ALTER TABLE with BLOB DEFAULT via --statement — fails because TiDB parser rejects it.
	m = NewTestMigration(t, WithStatement("ALTER TABLE t1parse ADD COLUMN c BLOB DEFAULT ('abc')"))
	err := m.Run()
	require.Error(t, err)
	require.ErrorContains(t, err, "can't have a default value")

	// ALTER TABLE with BLOB DEFAULT via --table/--alter — works (bypasses parser limitation).
	m = NewTestMigration(t, WithTable("t1parse"),
		WithAlter("ADD COLUMN c BLOB DEFAULT ('abc')"))
	require.NoError(t, m.Run())

	// CREATE TRIGGER — not supported.
	m = NewTestMigration(t, WithStatement("CREATE TRIGGER ins_sum BEFORE INSERT ON t1parse FOR EACH ROW SET @sum = @sum + NEW.b;"))
	err = m.Run()
	require.Error(t, err)
	require.ErrorContains(t, err, "line 1 column 14 near \"TRIGGER")

	// https://github.com/pingcap/tidb/pull/61498
	m = NewTestMigration(t, WithTable("t1parse"),
		WithAlter(`ADD COLUMN src_col timestamp NULL DEFAULT NULL, add column new_col timestamp NULL DEFAULT(src_col)`))
	require.NoError(t, m.Run())

	// Cleanup
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1parse`)
}

func TestCreateIndexIsRewritten(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "t1createindex", `CREATE TABLE t1createindex (
		id int not null primary key auto_increment,
		b int not null
	)`)

	m := NewTestMigration(t)
	m.Statement = fmt.Sprintf("CREATE INDEX idx ON %s.t1createindex (b)", m.Database)
	require.NoError(t, m.Run())
}

func TestSchemaNameIncluded(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "t1schemaname", `CREATE TABLE t1schemaname (
		id int not null primary key auto_increment,
		b int not null
	)`)

	m := NewTestMigration(t)
	m.Statement = fmt.Sprintf("ALTER TABLE %s.t1schemaname ADD COLUMN c int", m.Database)
	require.NoError(t, m.Run())
}

// TestSecondaryEngineAttribute tests that we can add a secondary engine attribute.
// https://github.com/block/spirit/issues/405
func TestSecondaryEngineAttribute(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "t1secondary", `CREATE /*vt+ QUERY_TIMEOUT_MS=0 */ TABLE t1secondary (
		id int not null primary key auto_increment,
		title VARCHAR(250)
	)`)
	m := NewTestMigration(t, WithStatement(`ALTER TABLE t1secondary ADD KEY (title) SECONDARY_ENGINE_ATTRIBUTE='{"type":"spann", "distance":"l2", "product_quantization":{"dimensions":96}}'`))
	require.NoError(t, m.Run())
}

// TestLargeNumberOfMultiChanges tests the interaction between force kill and multiple table changes.
// and supporting a large number of changes in general. Apart from the MDL locks (currently 1 per table),
// this shouldn't require more connections than regular single table changes.
// https://github.com/block/spirit/issues/502
func TestLargeNumberOfMultiChanges(t *testing.T) {
	var alterStmts []string
	for i := range 50 {
		testutils.RunSQL(t, fmt.Sprintf(`DROP TABLE IF EXISTS mt_%d, _mt_%d_new, _mt_%d_old, _mt_%d_chkpnt`, i, i, i, i))
		testutils.RunSQL(t, fmt.Sprintf(`CREATE TABLE mt_%d (id int not null primary key auto_increment, b INT NOT NULL)`, i))
		alterStmts = append(alterStmts, fmt.Sprintf(`ALTER TABLE mt_%d ENGINE=InnoDB`, i))
	}
	t.Cleanup(func() {
		for i := range 50 {
			// only need to drop success artifacts
			testutils.RunSQL(t, fmt.Sprintf(`DROP TABLE IF EXISTS mt_%d`, i))
		}
	})

	m := NewTestMigration(t, WithTargetChunkTime(2*time.Second),
		WithStatement(strings.Join(alterStmts, "; ")))
	require.NoError(t, m.Run())
}

func TestBufferedMultiTableMigration(t *testing.T) {
	tt := testutils.NewTestTable(t, "bmt_t1", `CREATE TABLE bmt_t1 (
		id int not null primary key auto_increment,
		name varchar(100) not null,
		val int not null
	)`)
	testutils.NewTestTable(t, "bmt_t2", `CREATE TABLE bmt_t2 (
		id int not null primary key auto_increment,
		description varchar(200) not null,
		amount decimal(10,2) not null
	)`)
	testutils.RunSQL(t, `INSERT INTO bmt_t1 (name, val) SELECT CONCAT('row_', seq), seq*10 FROM (
		WITH RECURSIVE seq_cte AS (SELECT 0 AS seq UNION ALL SELECT seq+1 FROM seq_cte WHERE seq < 99)
		SELECT seq FROM seq_cte) t`)
	testutils.RunSQL(t, `INSERT INTO bmt_t2 (description, amount) SELECT CONCAT('item_', seq), seq + (seq % 100) / 100.0 FROM (
		WITH RECURSIVE seq_cte AS (SELECT 0 AS seq UNION ALL SELECT seq+1 FROM seq_cte WHERE seq < 99)
		SELECT seq FROM seq_cte) t`)

	m := NewTestMigration(t, WithBuffered(true),
		WithStatement("ALTER TABLE bmt_t1 ADD COLUMN extra int DEFAULT 0; ALTER TABLE bmt_t2 ADD COLUMN extra int DEFAULT 0"))
	require.NoError(t, m.Run())

	// Verify both tables were altered correctly.
	var count1 int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM bmt_t1 WHERE extra = 0`).Scan(&count1))
	require.Equal(t, 100, count1)

	var count2 int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM bmt_t2 WHERE extra = 0`).Scan(&count2))
	require.Equal(t, 100, count2)
}

func TestMigrationParamsDefaultsUsed(t *testing.T) {
	t.Parallel()
	migration := &Migration{Table: "test_table", Alter: "ENGINE=INNODB"}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	require.Equal(t, defaultUsername, migration.Username)
	require.Equal(t, defaultPassword, *migration.Password)
	require.Equal(t, fmt.Sprintf("%s:%d", defaultHost, defaultPort), migration.Host)
	require.Equal(t, defaultDatabase, migration.Database)
	require.Equal(t, defaultTLSMode, migration.TLSMode)
	require.Empty(t, migration.TLSCertificatePath)
}

func TestMigrationParamsCLIUsed(t *testing.T) {
	t.Parallel()
	migration := &Migration{
		Host:               "cli-host:3306",
		Username:           "cli-user",
		Password:           mkPtr("cli-password"),
		Database:           "cli-db",
		Table:              "testtable",
		Alter:              "ENGINE=InnoDB",
		TLSMode:            "VERIFY_CA",
		TLSCertificatePath: "/path/to/ca",
	}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	require.Equal(t, "cli-host:3306", migration.Host)
	require.Equal(t, "cli-user", migration.Username)
	require.Equal(t, "cli-password", *migration.Password)
	require.Equal(t, "cli-db", migration.Database)
	require.Equal(t, "VERIFY_CA", migration.TLSMode)
	require.Equal(t, "/path/to/ca", migration.TLSCertificatePath)
}

func TestMigrationParamsEmptyPasswordUsedIfProvided(t *testing.T) {
	t.Parallel()
	migration := &Migration{
		Password: mkPtr(""),
		Table:    "test_table",
		Alter:    "ENGINE=INNODB",
	}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	require.Equal(t, defaultUsername, migration.Username)
	require.Empty(t, *migration.Password)
	require.Equal(t, fmt.Sprintf("%s:%d", defaultHost, defaultPort), migration.Host)
	require.Equal(t, defaultDatabase, migration.Database)
	require.Equal(t, defaultTLSMode, migration.TLSMode)
	require.Empty(t, migration.TLSCertificatePath)
}

func TestMigrationParamsIniFileInvalidFile(t *testing.T) {
	t.Parallel()
	migration := &Migration{
		Host:     "localhost:3306",
		Username: "defaultuser",
		Password: mkPtr("defaultpass"),
		Database: "testdb",
		Table:    "testtable",
		Alter:    "ENGINE=InnoDB",
		ConfFile: "/nonexistent/file.cnf",
	}

	_, err := migration.normalizeOptions()
	require.Error(t, err)
	require.ErrorContains(t, err, "no such file or directory")
}

func TestMigrationParamsIniFilePreferCommandLineOptions(t *testing.T) {
	t.Parallel()
	confPath := mkIniFile(t, `[client]
user = fileuser
password = filepass
host = filehost
database = filedb
port = 5678
tls-mode = VERIFY_IDENTITY
tls-ca = /path/from/file
`)

	migration := &Migration{
		Host:               "cli-host:1234",
		Username:           "cli-user",
		Password:           mkPtr("cli-password"),
		Database:           "cli-db",
		Table:              "testtable",
		Alter:              "ENGINE=InnoDB",
		ConfFile:           confPath,
		TLSMode:            "REQUIRED",
		TLSCertificatePath: "/path/to/cert",
	}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	require.Equal(t, "cli-user", migration.Username)
	require.Equal(t, "cli-password", *migration.Password)
	require.Equal(t, "cli-host:1234", migration.Host)
	require.Equal(t, "cli-db", migration.Database)
	require.Equal(t, "REQUIRED", migration.TLSMode)
	require.Equal(t, "/path/to/cert", migration.TLSCertificatePath)
}

func TestMigrationParamsIniFileNoCommandLineOptions(t *testing.T) {
	t.Parallel()
	confPath := mkIniFile(t, `[client]
user = fileuser
password = filepass
host = filehost
database = filedb
port = 5678
tls-mode = REQUIRED
tls-ca = /path/to/cert
`)

	migration := &Migration{
		Table:    "testtable",
		Alter:    "ENGINE=InnoDB",
		ConfFile: confPath,
	}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	require.Equal(t, "fileuser", migration.Username)
	require.Equal(t, "filepass", *migration.Password)
	require.Equal(t, "filehost:5678", migration.Host)
	require.Equal(t, "filedb", migration.Database)
	require.Equal(t, "REQUIRED", migration.TLSMode)
	require.Equal(t, "/path/to/cert", migration.TLSCertificatePath)
}

func TestMigrationParamsIniFileUseDefaultPort(t *testing.T) {
	t.Parallel()
	confPath := mkIniFile(t, `[client]
user = fileuser
password = filepass
host = filehost
database = filedb
tls-mode = VERIFY_IDENTITY
tls-ca = /path/to/another/ca
`)

	migration := &Migration{
		Table:    "testtable",
		Alter:    "ENGINE=InnoDB",
		ConfFile: confPath,
	}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	require.Equal(t, "fileuser", migration.Username)
	require.Equal(t, "filepass", *migration.Password)
	require.Equal(t, "filehost:3306", migration.Host)
	require.Equal(t, "filedb", migration.Database)
	require.Equal(t, "VERIFY_IDENTITY", migration.TLSMode)
	require.Equal(t, "/path/to/another/ca", migration.TLSCertificatePath)
}

func TestMigrationParamsIniFileOnlyUserSpecifiedInFile(t *testing.T) {
	t.Parallel()
	// Test with only username in creds file
	confPath := mkIniFile(t, `[client]
user = fileuser
`)

	migration := &Migration{
		Host:     "cli-host:3306",
		Password: mkPtr("cli-pass"),
		Database: "cli-db",
		Table:    "testtable",
		Alter:    "ENGINE=InnoDB",
		ConfFile: confPath,
	}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	require.Equal(t, "fileuser", migration.Username)
	require.Equal(t, "cli-pass", *migration.Password)
	require.Equal(t, "cli-host:3306", migration.Host)
	require.Equal(t, "cli-db", migration.Database)
	require.Equal(t, "PREFERRED", migration.TLSMode)
	require.Empty(t, migration.TLSCertificatePath)
}

func TestMigrationParamsIniFileOnlyPasswordSpecifiedInFile(t *testing.T) {
	t.Parallel()
	confPath := mkIniFile(t, `[client]
password = filepass
`)

	migration := &Migration{
		Host:     "cli-host:3306",
		Username: "cli-user",
		Database: "cli-db",
		Table:    "testtable",
		Alter:    "ENGINE=InnoDB",
		ConfFile: confPath,
	}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	require.Equal(t, "filepass", *migration.Password)
	require.Equal(t, "cli-user", migration.Username)
	require.Equal(t, "cli-host:3306", migration.Host)
	require.Equal(t, "cli-db", migration.Database)
	require.Equal(t, "PREFERRED", migration.TLSMode)
	require.Empty(t, migration.TLSCertificatePath)
}

func TestMigrationParamsIniFileEmptyPasswordPassedThrough(t *testing.T) {
	t.Parallel()
	confPath := mkIniFile(t, `[client]
password =
`)
	// File will be cleaned up by t.TempDir()

	migration := &Migration{
		Host:     "cli-host:3306",
		Username: "cli-user",
		Database: "cli-db",
		Table:    "testtable",
		Alter:    "ENGINE=InnoDB",
		ConfFile: confPath,
	}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	require.Empty(t, *migration.Password)
	require.Equal(t, "cli-user", migration.Username)
	require.Equal(t, "cli-host:3306", migration.Host)
	require.Equal(t, "cli-db", migration.Database)
	require.Equal(t, "PREFERRED", migration.TLSMode)
	require.Empty(t, migration.TLSCertificatePath)
}

func TestMigrationParamsIniFileEmptyPasswordOverridenByCommandLine(t *testing.T) {
	t.Parallel()
	confPath := mkIniFile(t, `[client]
password =
`)
	// File will be cleaned up by t.TempDir()

	migration := &Migration{
		Host:     "cli-host:3306",
		Password: mkPtr("cli-password"),
		Username: "cli-user",
		Database: "cli-db",
		Table:    "testtable",
		Alter:    "ENGINE=InnoDB",
		ConfFile: confPath,
	}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	require.Equal(t, "cli-password", *migration.Password)
	require.Equal(t, "cli-user", migration.Username)
	require.Equal(t, "cli-host:3306", migration.Host)
	require.Equal(t, "cli-db", migration.Database)
	require.Equal(t, "PREFERRED", migration.TLSMode)
	require.Empty(t, migration.TLSCertificatePath)
}

func TestMigrationParamsIniFileOnlyPortUsedFromFile(t *testing.T) {
	t.Parallel()
	confPath := mkIniFile(t, `[client]
port=1234
`)
	// File will be cleaned up by t.TempDir()

	migration := &Migration{
		Host:     "cli-host",
		Username: "cli-user",
		Password: mkPtr("cli-password"),
		Database: "cli-db",
		Table:    "testtable",
		Alter:    "ENGINE=InnoDB",
		ConfFile: confPath,
	}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	require.Equal(t, "cli-password", *migration.Password)
	require.Equal(t, "cli-user", migration.Username)
	require.Equal(t, "cli-host:1234", migration.Host)
	require.Equal(t, "cli-db", migration.Database)
	require.Equal(t, "PREFERRED", migration.TLSMode)
	require.Empty(t, migration.TLSCertificatePath)
}

func TestMigrationParamsIniFileEmptyClientSection(t *testing.T) {
	t.Parallel()
	// Test with empty client section
	confPath := mkIniFile(t, `[client]
`)
	// File will be cleaned up by t.TempDir()

	migration := &Migration{
		Host:     "cli-host:3306",
		Username: "cli-user",
		Password: mkPtr("cli-password"),
		Database: "cli-db",
		Table:    "testtable",
		Alter:    "ENGINE=InnoDB",
		ConfFile: confPath,
	}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	require.Equal(t, "cli-host:3306", migration.Host)
	require.Equal(t, "cli-user", migration.Username)
	require.Equal(t, "cli-password", *migration.Password)
	require.Equal(t, "cli-db", migration.Database)
	require.Equal(t, "PREFERRED", migration.TLSMode)
	require.Empty(t, migration.TLSCertificatePath)
}

func TestMigrationParamsIniFileHasNoClientSection(t *testing.T) {
	t.Parallel()
	// Test with no client section at all
	confPath := mkIniFile(t, `[mysql]
user = mysqluser
password = mysqlpass
`)
	// File will be cleaned up by t.TempDir()

	migration := &Migration{
		Host:     "cli-host:3306",
		Username: "cli-user",
		Password: mkPtr("cli-password"),
		Database: "cli-db",
		Table:    "testtable",
		Alter:    "ENGINE=InnoDB",
		ConfFile: confPath,
	}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	require.Equal(t, "cli-host:3306", migration.Host)
	require.Equal(t, "cli-user", migration.Username)
	require.Equal(t, "cli-password", *migration.Password)
	require.Equal(t, "cli-db", migration.Database)
	require.Equal(t, "PREFERRED", migration.TLSMode)
	require.Empty(t, migration.TLSCertificatePath)
}

// --- Configuration and validation tests (extracted from runner_test.go) ---

// TestBadOptions tests that NewRunner validates required fields.
func TestBadOptions(t *testing.T) {
	t.Parallel()
	_, err := NewRunner(&Migration{})
	require.Error(t, err)
	require.ErrorContains(t, err, "table name is required")

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	_, err = NewRunner(&Migration{Host: cfg.Addr})
	require.Error(t, err)
	require.ErrorContains(t, err, "table name is required")

	_, err = NewRunner(&Migration{Host: cfg.Addr, Database: "mytable"})
	require.Error(t, err)
	require.ErrorContains(t, err, "table name is required")

	_, err = NewRunner(&Migration{Host: cfg.Addr, Database: "mydatabase", Table: "mytable"})
	require.Error(t, err)
	require.ErrorContains(t, err, "alter statement is required")
}

// TestBadAlter tests various invalid ALTER statement scenarios.
func TestBadAlter(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "bot1", `CREATE TABLE bot1 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	testutils.NewTestTable(t, "bot2", `CREATE TABLE bot2 (
		id int(11) NOT NULL,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)

	// Completely invalid ALTER — should fail at parse time.
	m := NewTestMigration(t, WithTable("bot1"), WithAlter("badalter"))
	r, err := NewRunner(m)
	require.Nil(t, r)
	require.Error(t, err)

	// RENAME COLUMN + ADD INDEX referencing old name — MySQL rejects this.
	runner := NewTestRunner(t, "bot1", "RENAME COLUMN name TO name2, ADD INDEX(name)")
	err = runner.Run(t.Context())
	require.Error(t, err)
	require.NoError(t, runner.Close())

	// CHANGE COLUMN + ADD INDEX referencing old name — same issue.
	runner = NewTestRunner(t, "bot1", "CHANGE name name2 VARCHAR(255), ADD INDEX(name)")
	err = runner.Run(t.Context())
	require.Error(t, err)
	require.NoError(t, runner.Close())

	// CHANGE without rename (valid) — should succeed.
	runner = NewTestRunner(t, "bot1", "CHANGE name name VARCHAR(200), ADD INDEX(name)") //nolint: dupword
	err = runner.Run(t.Context())
	require.NoError(t, err)
	require.NoError(t, runner.Close())

	// DROP PRIMARY KEY — not supported.
	runner = NewTestRunner(t, "bot2", "DROP PRIMARY KEY")
	err = runner.Run(t.Context())
	require.Error(t, err)
	require.ErrorContains(t, err, "dropping primary key")
	require.NoError(t, runner.Close())
}

// TestDefaultPort tests that the default MySQL port (3306) is appended when not specified.
func TestDefaultPort(t *testing.T) {
	t.Parallel()
	m, err := NewRunner(&Migration{
		Host:     "localhost",
		Username: "root",
		Password: mkPtr("mypassword"),
		Database: "test",
		Threads:  2,
		Table:    "t1",
		Alter:    "DROP COLUMN b, ENGINE=InnoDB",
	})
	require.NoError(t, err)
	require.Equal(t, "localhost:3306", m.migration.Host)
}

// TestPasswordMasking tests that passwords are masked in DSN strings.
func TestPasswordMasking(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"basic DSN with password", "user:password@tcp(localhost:3306)/database", "user:***@tcp(localhost:3306)/database"},
		{"DSN with complex password", "myuser:c0mplex!Pa$$w0rd@tcp(db.example.com:3306)/mydb", "myuser:***@tcp(db.example.com:3306)/mydb"},
		{"DSN without password", "user@tcp(localhost:3306)/database", "user@tcp(localhost:3306)/database"},
		{"DSN with empty password", "user:@tcp(localhost:3306)/database", "user:***@tcp(localhost:3306)/database"},
		{"empty DSN", "", ""},
		{"malformed DSN without @", "user:password", "user:password"},
		{"DSN with colon in password", "user:pass:word@tcp(localhost:3306)/database", "user:***@tcp(localhost:3306)/database"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.expected, maskPasswordInDSN(tc.input))
		})
	}
}

// TestDSN tests that DSN construction correctly round-trips all fields,
// including passwords with special characters.
func TestDSN(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		user     string
		password string
		host     string
		schema   string
	}{
		{"simple password", "root", "secret", "127.0.0.1:3306", "testdb"},
		{"password with @", "root", "p@ssword", "127.0.0.1:3306", "testdb"},
		{"password with multiple @", "root", "p@ss@word", "127.0.0.1:3306", "testdb"},
		{"password with special characters", "root", "p@ss:word/with#special!chars", "127.0.0.1:3306", "testdb"},
		{"empty password", "root", "", "127.0.0.1:3306", "testdb"},
		{"AWS IAM-style token", "iam_user", "aaa@bbb.ccc.us-east-1.rds.amazonaws.com:3306/?Action=connect&DBUser=iam_user", "mydb.cluster-xyz.us-east-1.rds.amazonaws.com:3306", "production"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pw := tc.password
			r := &Runner{
				migration: &Migration{
					Username: tc.user,
					Password: &pw,
					Host:     tc.host,
				},
				changes: []*change{
					{
						stmt: &statement.AbstractStatement{
							Schema: tc.schema,
						},
					},
				},
			}
			dsn := r.dsn()
			cfg, err := mysql.ParseDSN(dsn)
			require.NoError(t, err)
			require.Equal(t, tc.user, cfg.User)
			require.Equal(t, tc.password, cfg.Passwd)
			require.Equal(t, tc.host, cfg.Addr)
			require.Equal(t, tc.schema, cfg.DBName)
			require.Equal(t, "tcp", cfg.Net)
		})
	}
}

func TestSplitReplicaDSNs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "empty",
			input:    "",
			expected: nil,
		},
		{
			name:     "single DSN",
			input:    "root:pass@tcp(localhost:3307)/test",
			expected: []string{"root:pass@tcp(localhost:3307)/test"},
		},
		{
			name:  "two DSNs",
			input: "root:pass@tcp(replica1:3306)/db,root:pass@tcp(replica2:3306)/db",
			expected: []string{
				"root:pass@tcp(replica1:3306)/db",
				"root:pass@tcp(replica2:3306)/db",
			},
		},
		{
			name:  "three DSNs with spaces",
			input: "root:pass@tcp(r1:3306)/db, root:pass@tcp(r2:3306)/db , root:pass@tcp(r3:3306)/db",
			expected: []string{
				"root:pass@tcp(r1:3306)/db",
				"root:pass@tcp(r2:3306)/db",
				"root:pass@tcp(r3:3306)/db",
			},
		},
		{
			name:     "trailing comma",
			input:    "root:pass@tcp(localhost:3306)/db,",
			expected: []string{"root:pass@tcp(localhost:3306)/db"},
		},
		{
			name:     "only commas and spaces",
			input:    " , , , ",
			expected: []string{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := splitReplicaDSNs(tc.input)
			require.Equal(t, tc.expected, result)
		})
	}
}
