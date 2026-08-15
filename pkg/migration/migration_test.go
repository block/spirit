package migration

import (
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/block/spirit/pkg/checksum"
	"github.com/block/spirit/pkg/sentinel"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
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
	sentinel.CheckInterval = 100 * time.Millisecond
	// Continuous-checksum pacing/retry are tuned here once, before any test
	// runs, rather than mutated inside individual tests — so the package globals
	// can't race the many t.Parallel() tests. Only the divergence test depends
	// on these; the defer-cutover tests drop the sentinel well before the first
	// inter-pass interval, so they run a single pass regardless of the value.
	checksum.ContinuousMinPassInterval = 2 * time.Second
	checksum.DefaultContinuousRetryDelay = 1 * time.Second
	goleak.VerifyTestMain(m)
}

func TestE2ENullAlterEmpty(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "t1e2e", `CREATE TABLE t1e2e (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	m := NewTestMigration(t, WithStatement("ALTER TABLE t1e2e ENGINE=InnoDB"))
	require.NoError(t, m.Run())
}

func TestE2EExplicitAutoIncrementInAlter(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "t1explicit_autoinc", `CREATE TABLE t1explicit_autoinc (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB AUTO_INCREMENT=1000`)

	m := NewTestMigration(t,
		WithStatement("ALTER TABLE t1explicit_autoinc ADD COLUMN test_col VARCHAR(255), AUTO_INCREMENT=5000"))
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
	// Deliberately exercises the legacy --table/--alter path: an empty alter
	// must be rejected. Dies with the legacy path.
	m := NewTestMigration(t)
	m.Table = "t1missing"
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
	m := NewTestMigration(t, WithStatement("ALTER TABLE t1bad ENGINE=InnoDB"), WithHost("127.0.0.1:9999"))
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
	m := NewTestMigration(t, WithStatement("ALTER TABLE t1nullalter ENGINE=InnoDB"))
	require.NoError(t, m.Run())
}

// TestE2EAutoscalingEnabled runs a full migration with the experimental
// write-thread autoscaler turned on. The local (non-Aurora) target has no
// GradualThrottler, so this exercises the downgrade path end-to-end: the
// autoscaler declines to engage (a warning is logged), write threads stay at
// the starting value, the connection pool is still sized for the ceiling, and
// the migration completes correctly (goleak in TestMain catches leaks). The
// engaged path is covered by the autoscaler unit tests.
func TestE2EAutoscalingEnabled(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "t1autoscale", `CREATE TABLE t1autoscale (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	testutils.RunSQL(t, `INSERT INTO t1autoscale (name) VALUES ('a'), ('b'), ('c'), ('d'), ('e')`)
	m := NewTestMigration(t, WithStatement("ALTER TABLE t1autoscale ENGINE=InnoDB"),
		WithWriteThreads(2), WithAutoscaling())
	require.NoError(t, m.Run())

	var count int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t1autoscale").Scan(&count))
	require.Equal(t, 5, count)
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
	m := NewTestMigration(t, WithStatement("ALTER TABLE replicatest ENGINE=InnoDB"),
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
	m := NewTestMigration(t, WithStatement("ALTER TABLE renamet1 CHANGE name nameNew varchar(255) not null"))
	require.NoError(t, m.Run())
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
	m := NewTestMigration(t, WithStatement("ALTER TABLE t1generated ENGINE=InnoDB"), WithBuffered(enableBuffered))
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

		m := NewTestMigration(t, WithBuffered(enableBuffered),
			WithStatement(fmt.Sprintf("ALTER TABLE t1varbin CHANGE b b %s not null", test.NewType))) //nolint: dupword
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

	m := NewTestMigration(t, WithBuffered(enableBuffered),
		WithStatement("ALTER TABLE t1charset CONVERT TO CHARACTER SET UTF8MB4"))
	require.NoError(t, m.Run())

	// Because utf8mb4 is the superset, it doesn't matter that that's
	// what the checksum casts to. We should be able to convert back as well.
	m = NewTestMigration(t, WithBuffered(enableBuffered),
		WithStatement("ALTER TABLE t1charset CONVERT TO CHARACTER SET latin1"))
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
// and supporting a large number of changes in general. Apart from the advisory locks (currently 1 per table),
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
	migration := &Migration{Statement: "ALTER TABLE test_table ENGINE=INNODB"}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	require.Equal(t, defaultUsername, migration.Username)
	require.Equal(t, defaultPassword, *migration.Password)
	require.Equal(t, fmt.Sprintf("%s:%d", defaultHost, defaultPort), migration.Host)
	require.Equal(t, defaultDatabase, migration.Database)
	require.Equal(t, defaultTLSMode, migration.TLSMode)
	require.Empty(t, migration.TLSCertificatePath)
	// A zero TargetChunkSize (programmatic callers that bypass the Kong default)
	// is filled in with the buffered-copier byte budget, so the copy chunker is
	// never accidentally left on the time signal.
	require.Equal(t, uint64(table.DefaultTargetChunkBytes), migration.TargetChunkSize)
}

// TestTargetChunkSizeKongDefault pins the hardcoded Kong default on
// --target-chunk-size to table.DefaultTargetChunkBytes. The Kong tag must be a
// literal, so this guards against it drifting from the constant (which also
// backs the zero-value default in normalizeOptions).
func TestTargetChunkSizeKongDefault(t *testing.T) {
	t.Parallel()
	field, ok := reflect.TypeFor[Migration]().FieldByName("TargetChunkSize")
	require.True(t, ok)
	require.Equal(t,
		strconv.FormatUint(table.DefaultTargetChunkBytes, 10),
		field.Tag.Get("default"),
		"Kong default for --target-chunk-size must equal table.DefaultTargetChunkBytes")
}

func TestMigrationParamsCLIUsed(t *testing.T) {
	t.Parallel()
	migration := &Migration{
		Host:               "cli-host:3306",
		Username:           "cli-user",
		Password:           new("cli-password"),
		Database:           "cli-db",
		Statement:          "ALTER TABLE testtable ENGINE=InnoDB",
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
		Password:  new(""),
		Statement: "ALTER TABLE test_table ENGINE=INNODB",
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
		Host:      "localhost:3306",
		Username:  "defaultuser",
		Password:  new("defaultpass"),
		Database:  "testdb",
		Statement: "ALTER TABLE testtable ENGINE=InnoDB",
		ConfFile:  "/nonexistent/file.cnf",
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
		Password:           new("cli-password"),
		Database:           "cli-db",
		Statement:          "ALTER TABLE testtable ENGINE=InnoDB",
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
		Statement: "ALTER TABLE testtable ENGINE=InnoDB",
		ConfFile:  confPath,
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
		Statement: "ALTER TABLE testtable ENGINE=InnoDB",
		ConfFile:  confPath,
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
		Host:      "cli-host:3306",
		Password:  new("cli-pass"),
		Database:  "cli-db",
		Statement: "ALTER TABLE testtable ENGINE=InnoDB",
		ConfFile:  confPath,
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
		Host:      "cli-host:3306",
		Username:  "cli-user",
		Database:  "cli-db",
		Statement: "ALTER TABLE testtable ENGINE=InnoDB",
		ConfFile:  confPath,
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
		Host:      "cli-host:3306",
		Username:  "cli-user",
		Database:  "cli-db",
		Statement: "ALTER TABLE testtable ENGINE=InnoDB",
		ConfFile:  confPath,
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
		Host:      "cli-host:3306",
		Password:  new("cli-password"),
		Username:  "cli-user",
		Database:  "cli-db",
		Statement: "ALTER TABLE testtable ENGINE=InnoDB",
		ConfFile:  confPath,
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
		Host:      "cli-host",
		Username:  "cli-user",
		Password:  new("cli-password"),
		Database:  "cli-db",
		Statement: "ALTER TABLE testtable ENGINE=InnoDB",
		ConfFile:  confPath,
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
		Host:      "cli-host:3306",
		Username:  "cli-user",
		Password:  new("cli-password"),
		Database:  "cli-db",
		Statement: "ALTER TABLE testtable ENGINE=InnoDB",
		ConfFile:  confPath,
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
		Host:      "cli-host:3306",
		Username:  "cli-user",
		Password:  new("cli-password"),
		Database:  "cli-db",
		Statement: "ALTER TABLE testtable ENGINE=InnoDB",
		ConfFile:  confPath,
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
	m := NewTestMigration(t, WithStatement("ALTER TABLE bot1 badalter"))
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
		Host:      "localhost",
		Username:  "root",
		Password:  new("mypassword"),
		Database:  "test",
		Threads:   2,
		Statement: "ALTER TABLE t1 DROP COLUMN b, ENGINE=InnoDB",
	})
	require.NoError(t, err)
	require.Equal(t, "localhost:3306", m.migration.Host)
}

// TestPasswordMasking tests that passwords are masked in DSN strings.
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
				changes: []*tableChange{
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

// TestE2EGTIDChangeSource exercises the experimental --gtid path end-to-end.
// Same shape as TestE2ENullAlterEmpty but with the GTID change source wired in.
func TestE2EGTIDChangeSource(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "t1e2egtid", `CREATE TABLE t1e2egtid (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	m := NewTestMigration(t, WithStatement("ALTER TABLE t1e2egtid ENGINE=InnoDB"), WithGTID(true))
	require.NoError(t, m.Run())
}

// TestMigrationValidate covers the Kong Validate() hook: explicitly-negative
// numeric/duration flags are rejected, while zero values (meaning "use the
// default") pass. Validate() has no cross-flag combination checks today; add a
// case here alongside the first one.
func TestMigrationValidate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		m       Migration
		wantErr string
	}{
		{name: "zero values are valid"},
		{name: "typical values are valid", m: Migration{
			Threads:          4,
			WriteThreads:     4,
			TargetChunkTime:  500 * time.Millisecond,
			ReplicaMaxLag:    120 * time.Second,
			CheckpointMaxAge: 168 * time.Hour,
		}},
		{name: "negative threads", m: Migration{Threads: -5},
			wantErr: "--threads must be non-negative, got -5"},
		{name: "negative write-threads", m: Migration{WriteThreads: -1},
			wantErr: "--write-threads must be non-negative, got -1"},
		{name: "negative target-chunk-time", m: Migration{TargetChunkTime: -time.Second},
			wantErr: "--target-chunk-time must be non-negative, got -1s"},
		{name: "negative replica-max-lag", m: Migration{ReplicaMaxLag: -time.Minute},
			wantErr: "--replica-max-lag must be non-negative, got -1m0s"},
		{name: "negative checkpoint-max-age", m: Migration{CheckpointMaxAge: -time.Hour},
			wantErr: "--checkpoint-max-age must be non-negative, got -1h0m0s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.m.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tt.wantErr)
			}
		})
	}
}
