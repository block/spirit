package migration

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	_ "github.com/pingcap/tidb/pkg/parser/test_driver"

	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/go-sql-driver/mysql"

	"github.com/stretchr/testify/assert"
)

func mkPtr[T any](t T) *T {
	return &t
}

func mkIniFile(t *testing.T, content string) *os.File {
	tmpFile, err := os.CreateTemp(t.TempDir(), "test_creds_*.cnf")
	require.NoError(t, err)
	_, err = tmpFile.WriteString(content)
	require.NoError(t, err)

	return tmpFile
}

func TestMain(m *testing.M) {
	status.CheckpointDumpInterval = 100 * time.Millisecond
	status.StatusInterval = 10 * time.Millisecond // the status will be accurate to 1ms
	sentinelCheckInterval = 100 * time.Millisecond
	sentinelWaitLimit = 10 * time.Second
	goleak.VerifyTestMain(m)
	os.Exit(m.Run())
}

func TestE2ENullAlterEmpty(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1e2e, _t1e2e_new`)
	table := `CREATE TABLE t1e2e (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	migration := &Migration{}
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	migration.Host = cfg.Addr
	migration.Username = cfg.User
	migration.Password = &cfg.Passwd
	migration.Database = cfg.DBName
	migration.Threads = 1
	migration.Table = "t1e2e"
	migration.Alter = "ENGINE=InnoDB"

	err = migration.Run()
	assert.NoError(t, err)
}

func TestMissingAlter(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1missing, _t1missing_new`)
	table := `CREATE TABLE t1missing (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	migration := &Migration{}
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	migration.Host = cfg.Addr
	migration.Username = cfg.User
	migration.Password = &cfg.Passwd
	migration.Database = cfg.DBName
	migration.Threads = 2
	migration.Table = "t1missing"
	migration.Alter = ""

	err = migration.Run()
	assert.Error(t, err) // missing alter
	assert.ErrorContains(t, err, "alter statement is required")
}

func TestBadDatabaseCredentials(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1bad, _t1bad_new`)
	table := `CREATE TABLE t1bad (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	migration := &Migration{}
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	migration.Host = "127.0.0.1:9999"
	migration.Username = cfg.User
	migration.Password = &cfg.Passwd
	migration.Database = cfg.DBName
	migration.Threads = 2
	migration.Table = "t1bad"
	migration.Alter = "ENGINE=InnoDB"

	err = migration.Run()
	assert.Error(t, err)                                        // bad database credentials
	assert.ErrorContains(t, err, "connect: connection refused") // could be no host or temporary resolution failure.
}

func TestE2ENullAlter1Row(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1nullalter, _t1nullalter_new`)
	table := `CREATE TABLE t1nullalter (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, `insert into t1nullalter (id,name) values (1, 'aaa')`)
	migration := &Migration{}
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	migration.Host = cfg.Addr
	migration.Username = cfg.User
	migration.Password = &cfg.Passwd
	migration.Database = cfg.DBName
	migration.Threads = 2
	migration.Table = "t1nullalter"
	migration.Alter = "ENGINE=InnoDB"

	err = migration.Run()
	assert.NoError(t, err)
}

func TestE2ENullAlterWithReplicas(t *testing.T) {
	t.Parallel()
	replicaDSN := os.Getenv("REPLICA_DSN")
	if replicaDSN == "" {
		t.Skip("skipping replica tests because REPLICA_DSN not set")
	}
	testutils.RunSQL(t, `DROP TABLE IF EXISTS replicatest, _replicatest_new`)
	table := `CREATE TABLE replicatest (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	migration := &Migration{}
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	migration.Host = cfg.Addr
	migration.Username = cfg.User
	migration.Password = &cfg.Passwd
	migration.Database = cfg.DBName
	migration.Threads = 2
	migration.Table = "replicatest"
	migration.Alter = "ENGINE=InnoDB"
	migration.ReplicaDSN = replicaDSN
	migration.ReplicaMaxLag = 10 * time.Second

	err = migration.Run()
	assert.NoError(t, err)
}

// TestRenameInMySQL80 tests that even though renames are not supported,
// if the version is 8.0 it will apply the instant operation before
// the rename check applies. It's only when it needs to actually migrate
// that it won't allow renames.
func TestRenameInMySQL80(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)
	defer db.Close()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS renamet1, _renamet1_new`)
	table := `CREATE TABLE renamet1 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	migration := &Migration{}
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	migration.Host = cfg.Addr
	migration.Username = cfg.User
	migration.Password = &cfg.Passwd
	migration.Database = cfg.DBName
	migration.Threads = 2
	migration.Table = "renamet1"
	migration.Alter = "CHANGE name nameNew varchar(255) not null"

	err = migration.Run()
	assert.NoError(t, err)
}

// TestUniqueOnNonUniqueData tests that we:
// 1. Fail trying to add a unique index on non-unique data.
// 2. The error does not blame spirit, but is instead suggestive of user-data error.
func TestUniqueOnNonUniqueData(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)
	defer db.Close()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS uniquet1, _uniquet1_new`)
	testutils.RunSQL(t, `CREATE TABLE uniquet1 (id int not null primary key auto_increment, b int not null, pad1 varbinary(1024));`)
	testutils.RunSQL(t, `INSERT INTO uniquet1 SELECT NULL, 1, RANDOM_BYTES(1024) from dual;`)
	testutils.RunSQL(t, `INSERT INTO uniquet1 SELECT NULL, 1, RANDOM_BYTES(1024) from uniquet1 a join uniquet1 b join uniquet1 c limit 100000;`)
	testutils.RunSQL(t, `INSERT INTO uniquet1 SELECT NULL, 1, RANDOM_BYTES(1024) from uniquet1 a join uniquet1 b join uniquet1 c limit 100000;`)
	testutils.RunSQL(t, `INSERT INTO uniquet1 SELECT NULL, 1, RANDOM_BYTES(1024) from uniquet1 a join uniquet1 b join uniquet1 c limit 100000;`)
	testutils.RunSQL(t, `INSERT INTO uniquet1 SELECT NULL, 1, RANDOM_BYTES(1024) from uniquet1 a join uniquet1 b join uniquet1 c limit 100000;`)
	testutils.RunSQL(t, `UPDATE uniquet1 SET b = id;`)
	testutils.RunSQL(t, `UPDATE uniquet1 SET b = 12345 ORDER BY RAND() LIMIT 2;`)
	migration := &Migration{}
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	migration.Host = cfg.Addr
	migration.Username = cfg.User
	migration.Password = &cfg.Passwd
	migration.Database = cfg.DBName
	migration.Threads = 2
	migration.Table = "uniquet1"
	migration.Alter = "ADD UNIQUE (b)"
	err = migration.Run()
	assert.Error(t, err)
	assert.ErrorContains(t, err, "checksum failed after several attempts. This is likely related to your statement adding a UNIQUE index on non-unique data")
}

func TestGeneratedColumns(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1generated, _t1generated_new`)
	table := `CREATE TABLE t1generated (
	 id int not null primary key auto_increment,
    b int not null,
    c int GENERATED ALWAYS AS  (b + 1)
	)`
	testutils.RunSQL(t, table)
	migration := &Migration{}
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	migration.Host = cfg.Addr
	migration.Username = cfg.User
	migration.Password = &cfg.Passwd
	migration.Database = cfg.DBName
	migration.Threads = 1
	migration.Table = "t1generated"
	migration.Alter = "ENGINE=InnoDB"

	err = migration.Run()
	assert.NoError(t, err)
}

func TestStoredGeneratedColumns(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1stored, _t1stored_new`)
	table := `CREATE TABLE t1stored (
  id bigint NOT NULL AUTO_INCREMENT,
  pa bigint DEFAULT NULL,
  p1 bigint DEFAULT NULL,
  p2 bigint DEFAULT NULL,
  s1 tinyint(1) GENERATED ALWAYS AS (if((pa is not null),1,NULL)) STORED,
  s2 tinyint(1) GENERATED ALWAYS AS (if((p1 is not null),1,NULL)) STORED,
  s3 tinyint(1) GENERATED ALWAYS AS (if((p2 is not null),1,NULL)) STORED,
  s4 tinyint(1) GENERATED ALWAYS AS (if((pa <> p2),1,NULL)) STORED,
  PRIMARY KEY (id)
);`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, `INSERT INTO t1stored (pa, p1, p2)
VALUES
(1, 1, 1),
(1, NULL, 1),
(1, 1, NULL),
(1, NULL, NULL),
(1, 1, 0),
(1, 0, 1),
(1, 0, 0),
(1, NULL, 0),
(1, 0, NULL),
(1, NULL, NULL),
(NULL, NULL, NULL)
`)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	migration := &Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Statement: `ALTER TABLE t1stored
MODIFY COLUMN s4 TINYINT(1)
GENERATED ALWAYS AS (
IF(
 pa <> p2
     OR (pa IS NULL AND p2 IS NOT NULL)
     OR (pa IS NOT NULL AND p2 IS NULL),
 1,
 NULL
)
) STORED`,
	}
	err = migration.Run()
	assert.NoError(t, err)
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
		testutils.RunSQL(t, `DROP TABLE IF EXISTS t1varbin, _t1varbin_new`)
		table := fmt.Sprintf(`CREATE TABLE t1varbin (
	 id int not null primary key auto_increment,
    b %s not null
	)`, test.OldType)
		testutils.RunSQL(t, table)
		testutils.RunSQL(t, `insert into t1varbin values (null, 'abcdefg')`)
		migration := &Migration{}
		cfg, err := mysql.ParseDSN(testutils.DSN())
		assert.NoError(t, err)
		migration.Host = cfg.Addr
		migration.Username = cfg.User
		migration.Password = &cfg.Passwd
		migration.Database = cfg.DBName
		migration.Threads = 1
		migration.Table = "t1varbin"
		migration.Alter = fmt.Sprintf("CHANGE b b %s not null", test.NewType) //nolint: dupword
		err = migration.Run()
		assert.NoError(t, err)
	}
}

// TestConvertCharset tests that we can change the character set from latin1 to utf8mb4,
// and that the non 7-bit characters that can be represented in latin1 as 1 byte,
// checksum correctly against their multi-byte utf8mb4 representations
func TestConvertCharset(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1charset, _t1charset_new`)
	table := `CREATE TABLE t1charset (
	 id int not null primary key auto_increment,
    b varchar(100) not null
	) charset=latin1`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, `insert into t1charset values (null, 'à'), (null, '€')`)
	migration := &Migration{}
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	migration.Host = cfg.Addr
	migration.Username = cfg.User
	migration.Password = &cfg.Passwd
	migration.Database = cfg.DBName
	migration.Threads = 1
	migration.Table = "t1charset"
	migration.Alter = "CONVERT TO CHARACTER SET UTF8MB4"
	err = migration.Run()
	assert.NoError(t, err)

	// Because utf8mb4 is the superset, it doesn't matter that that's
	// what the checksum casts to. We should be able to convert back as well.
	migration = &Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  1,
		Table:    "t1charset",
		Alter:    "CONVERT TO CHARACTER SET latin1",
	}
	err = migration.Run()
	assert.NoError(t, err)
}

func TestStmtWorkflow(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1s, _t1s_new`)
	table := `CREATE TABLE t1s (
	 id int not null primary key auto_increment,
    b varchar(100) not null
	)`
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	migration := &Migration{
		Host:      cfg.Addr,
		Username:  cfg.User,
		Password:  &cfg.Passwd,
		Database:  cfg.DBName,
		Threads:   1,
		Statement: table, // CREATE TABLE.
	}
	err = migration.Run()
	assert.NoError(t, err)
	// We can also specify ALTER options in the statement.
	migration = &Migration{
		Host:      cfg.Addr,
		Username:  cfg.User,
		Password:  &cfg.Passwd,
		Database:  cfg.DBName,
		Threads:   1,
		Statement: "ALTER TABLE t1s ADD COLUMN c int", // ALTER TABLE.
	}
	err = migration.Run()
	assert.NoError(t, err)
}

// TestUnparsableStatements tests that the behavior is expected in cases
// where we know the TiDB parser does not support the statement. We document
// that we require the TiDB parser to parse the statement for it to execute,
// which feels like a reasonable limitation based on its capabilities.
// Example TiDB bug: https://github.com/pingcap/tidb/issues/54700
func TestUnparsableStatements(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1parse, _t1parse_new`)
	table := `CREATE TABLE t1parse (id int not null primary key auto_increment, b BLOB DEFAULT ('abc'))`
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	migration := &Migration{
		Host:      cfg.Addr,
		Username:  cfg.User,
		Password:  &cfg.Passwd,
		Database:  cfg.DBName,
		Threads:   1,
		Statement: table,
	}
	err = migration.Run()
	assert.NoError(t, err)

	// Try again as ALTER TABLE, with --statement
	migration = &Migration{
		Host:      cfg.Addr,
		Username:  cfg.User,
		Password:  &cfg.Passwd,
		Database:  cfg.DBName,
		Threads:   1,
		Statement: "ALTER TABLE t1parse ADD COLUMN c BLOB DEFAULT ('abc')",
	}
	err = migration.Run()
	assert.Error(t, err)
	// this is permitted by MySQL now, it's the TiDB parser that won't allow it.
	// TODO: we should figure this out so that usage between --statement and --alter is orthogonal.
	assert.ErrorContains(t, err, "can't have a default value")

	// With ALTER TABLE as --table and --alter
	migration = &Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  1,
		Table:    "t1parse",
		Alter:    "ADD COLUMN c BLOB DEFAULT ('abc')",
	}
	err = migration.Run()
	assert.NoError(t, err) // in this context it works! This is a problem.

	// With CREATE TRIGGER.
	migration = &Migration{
		Host:      cfg.Addr,
		Username:  cfg.User,
		Password:  &cfg.Passwd,
		Database:  cfg.DBName,
		Threads:   1,
		Statement: "CREATE TRIGGER ins_sum BEFORE INSERT ON t1parse FOR EACH ROW SET @sum = @sum + NEW.b;",
	}
	err = migration.Run()
	assert.Error(t, err)
	assert.ErrorContains(t, err, "line 1 column 14 near \"TRIGGER")

	//https://github.com/pingcap/tidb/pull/61498
	migration = &Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  1,
		Table:    "t1parse",
		Alter:    `ADD COLUMN src_col timestamp NULL DEFAULT NULL, add column new_col timestamp NULL DEFAULT(src_col)`}
	err = migration.Run()
	assert.NoError(t, err)
}

func TestCreateIndexIsRewritten(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1createindex, _t1createindex_new`)
	tbl := `CREATE TABLE t1createindex (
	 id int not null primary key auto_increment,
	 b int not null
	)`
	testutils.RunSQL(t, tbl)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	require.NotEmpty(t, cfg.DBName)
	migration := &Migration{
		Host:      cfg.Addr,
		Username:  cfg.User,
		Password:  &cfg.Passwd,
		Database:  cfg.DBName,
		Threads:   1,
		Statement: "CREATE INDEX idx ON " + cfg.DBName + ".t1createindex (b)",
	}
	err = migration.Run()
	assert.NoError(t, err)
}

func TestSchemaNameIncluded(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1schemaname, _t1schemaname_new`)
	tbl := `CREATE TABLE t1schemaname (
	 id int not null primary key auto_increment,
	b int not null
	)`
	testutils.RunSQL(t, tbl)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	migration := &Migration{
		Host:      cfg.Addr,
		Username:  cfg.User,
		Password:  &cfg.Passwd,
		Database:  cfg.DBName,
		Threads:   1,
		Statement: "ALTER TABLE test.t1schemaname ADD COLUMN c int",
	}
	err = migration.Run()
	assert.NoError(t, err)
}

// TestSecondaryEngineAttribute tests that we can add a secondary engine attribute
// We can't quite test to the original bug report, because the vector type + index
// may not be supported in the version of MySQL we are using:
// https://github.com/block/spirit/issues/405
func TestSecondaryEngineAttribute(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1secondary, _t1secondary_new`)
	tbl := `CREATE /*vt+ QUERY_TIMEOUT_MS=0 */ TABLE t1secondary (
	id int not null primary key auto_increment,
	title VARCHAR(250)
	)`
	testutils.RunSQL(t, tbl)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	migration := &Migration{
		Host:      cfg.Addr,
		Username:  cfg.User,
		Password:  &cfg.Passwd,
		Database:  cfg.DBName,
		Threads:   1,
		Statement: `ALTER TABLE t1secondary ADD KEY (title) SECONDARY_ENGINE_ATTRIBUTE='{"type":"spann", "distance":"l2", "product_quantization":{"dimensions":96}}'`,
		ForceKill: true, // this happens to be instant, tests this code path too.
	}
	err = migration.Run()
	assert.NoError(t, err)
}

// TestLargeNumberOfMultiChanges tests the interaction between force kill and multiple table changes.
// and supporting a large number of changes in general. Apart from the MDL locks (currently 1 per table),
// this shouldn't require more connections than regular single table changes.
// https://github.com/block/spirit/issues/502
func TestLargeNumberOfMultiChanges(t *testing.T) {
	var alterStmts []string
	for i := range 50 {
		testutils.RunSQL(t, fmt.Sprintf(`DROP TABLE IF EXISTS mt_%d`, i))
		testutils.RunSQL(t, fmt.Sprintf(`CREATE TABLE mt_%d (id int not null primary key auto_increment, b INT NOT NULL)`, i))
		alterStmts = append(alterStmts, fmt.Sprintf(`ALTER TABLE mt_%d ENGINE=InnoDB`, i))
	}
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	migration := &Migration{
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        &cfg.Passwd,
		Database:        cfg.DBName,
		Threads:         2,
		TargetChunkTime: 2 * time.Second,
		Statement:       strings.Join(alterStmts, "; "),
		ForceKill:       true,
	}
	err = migration.Run()
	assert.NoError(t, err)
}

func TestMigrationParamsDefaultsUsed(t *testing.T) {
	migration := &Migration{Table: "test_table", Alter: "ENGINE=INNODB"}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	assert.Equal(t, defaultUsername, migration.Username)
	assert.Equal(t, defaultPassword, *migration.Password)
	assert.Equal(t, fmt.Sprintf("%s:%d", defaultHost, defaultPort), migration.Host)
	assert.Equal(t, defaultDatabase, migration.Database)
	assert.Equal(t, defaultTLSMode, migration.TLSMode)
	assert.Empty(t, migration.TLSCertificatePath)
}

func TestMigrationParamsCLIUsed(t *testing.T) {
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
	assert.NoError(t, err)

	assert.Equal(t, "cli-host:3306", migration.Host)
	assert.Equal(t, "cli-user", migration.Username)
	assert.Equal(t, "cli-password", *migration.Password)
	assert.Equal(t, "cli-db", migration.Database)
	assert.Equal(t, "VERIFY_CA", migration.TLSMode)
	assert.Equal(t, "/path/to/ca", migration.TLSCertificatePath)
}

func TestMigrationParamsEmptyPasswordUsedIfProvided(t *testing.T) {
	migration := &Migration{
		Password: mkPtr(""),
		Table:    "test_table",
		Alter:    "ENGINE=INNODB",
	}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	assert.Equal(t, defaultUsername, migration.Username)
	assert.Empty(t, *migration.Password)
	assert.Equal(t, fmt.Sprintf("%s:%d", defaultHost, defaultPort), migration.Host)
	assert.Equal(t, defaultDatabase, migration.Database)
	assert.Equal(t, defaultTLSMode, migration.TLSMode)
	assert.Empty(t, migration.TLSCertificatePath)
}

func TestMigrationParamsIniFileInvalidFile(t *testing.T) {
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
	assert.Error(t, err)
	assert.ErrorContains(t, err, "no such file or directory")
}

func TestMigrationParamsIniFilePreferCommandLineOptions(t *testing.T) {
	tmpFile := mkIniFile(t, `[client]
user = fileuser
password = filepass
host = filehost
database = filedb
port = 5678
tls-mode = VERIFY_IDENTITY
tls-ca = /path/from/file
`)
	defer os.Remove(tmpFile.Name())
	require.NoError(t, tmpFile.Close())

	migration := &Migration{
		Host:               "cli-host:1234",
		Username:           "cli-user",
		Password:           mkPtr("cli-password"),
		Database:           "cli-db",
		Table:              "testtable",
		Alter:              "ENGINE=InnoDB",
		ConfFile:           tmpFile.Name(),
		TLSMode:            "REQUIRED",
		TLSCertificatePath: "/path/to/cert",
	}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	assert.Equal(t, "cli-user", migration.Username)
	assert.Equal(t, "cli-password", *migration.Password)
	assert.Equal(t, "cli-host:1234", migration.Host)
	assert.Equal(t, "cli-db", migration.Database)
	assert.Equal(t, "REQUIRED", migration.TLSMode)
	assert.Equal(t, "/path/to/cert", migration.TLSCertificatePath)
}

func TestMigrationParamsIniFileNoCommandLineOptions(t *testing.T) {
	tmpFile := mkIniFile(t, `[client]
user = fileuser
password = filepass
host = filehost
database = filedb
port = 5678
tls-mode = REQUIRED
tls-ca = /path/to/cert
`)
	defer os.Remove(tmpFile.Name())
	require.NoError(t, tmpFile.Close())

	migration := &Migration{
		Table:    "testtable",
		Alter:    "ENGINE=InnoDB",
		ConfFile: tmpFile.Name(),
	}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	assert.Equal(t, "fileuser", migration.Username)
	assert.Equal(t, "filepass", *migration.Password)
	assert.Equal(t, "filehost:5678", migration.Host)
	assert.Equal(t, "filedb", migration.Database)
	assert.Equal(t, "REQUIRED", migration.TLSMode)
	assert.Equal(t, "/path/to/cert", migration.TLSCertificatePath)
}

func TestMigrationParamsIniFileUseDefaultPort(t *testing.T) {
	tmpFile := mkIniFile(t, `[client]
user = fileuser
password = filepass
host = filehost
database = filedb
tls-mode = VERIFY_IDENTITY
tls-ca = /path/to/another/ca
`)
	defer os.Remove(tmpFile.Name())
	require.NoError(t, tmpFile.Close())

	migration := &Migration{
		Table:    "testtable",
		Alter:    "ENGINE=InnoDB",
		ConfFile: tmpFile.Name(),
	}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	assert.Equal(t, "fileuser", migration.Username)
	assert.Equal(t, "filepass", *migration.Password)
	assert.Equal(t, "filehost:3306", migration.Host)
	assert.Equal(t, "filedb", migration.Database)
	assert.Equal(t, "VERIFY_IDENTITY", migration.TLSMode)
	assert.Equal(t, "/path/to/another/ca", migration.TLSCertificatePath)
}

func TestMigrationParamsIniFileOnlyUserSpecifiedInFile(t *testing.T) {
	// Test with only username in creds file
	tmpFile := mkIniFile(t, `[client]
user = fileuser
`)
	defer os.Remove(tmpFile.Name())
	require.NoError(t, tmpFile.Close())

	migration := &Migration{
		Host:     "cli-host:3306",
		Password: mkPtr("cli-pass"),
		Database: "cli-db",
		Table:    "testtable",
		Alter:    "ENGINE=InnoDB",
		ConfFile: tmpFile.Name(),
	}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	assert.Equal(t, "fileuser", migration.Username)
	assert.Equal(t, "cli-pass", *migration.Password)
	assert.Equal(t, "cli-host:3306", migration.Host)
	assert.Equal(t, "cli-db", migration.Database)
	assert.Equal(t, "PREFERRED", migration.TLSMode)
	assert.Empty(t, migration.TLSCertificatePath)
}

func TestMigrationParamsIniFileOnlyPasswordSpecifiedInFile(t *testing.T) {
	tmpFile := mkIniFile(t, `[client]
password = filepass
`)
	defer os.Remove(tmpFile.Name())
	require.NoError(t, tmpFile.Close())

	migration := &Migration{
		Host:     "cli-host:3306",
		Username: "cli-user",
		Database: "cli-db",
		Table:    "testtable",
		Alter:    "ENGINE=InnoDB",
		ConfFile: tmpFile.Name(),
	}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	assert.Equal(t, "filepass", *migration.Password)
	assert.Equal(t, "cli-user", migration.Username)
	assert.Equal(t, "cli-host:3306", migration.Host)
	assert.Equal(t, "cli-db", migration.Database)
	assert.Equal(t, "PREFERRED", migration.TLSMode)
	assert.Empty(t, migration.TLSCertificatePath)
}

func TestMigrationParamsIniFileEmptyPasswordPassedThrough(t *testing.T) {
	tmpFile := mkIniFile(t, `[client]
password =
`)
	defer os.Remove(tmpFile.Name())
	require.NoError(t, tmpFile.Close())

	migration := &Migration{
		Host:     "cli-host:3306",
		Username: "cli-user",
		Database: "cli-db",
		Table:    "testtable",
		Alter:    "ENGINE=InnoDB",
		ConfFile: tmpFile.Name(),
	}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	assert.Empty(t, *migration.Password)
	assert.Equal(t, "cli-user", migration.Username)
	assert.Equal(t, "cli-host:3306", migration.Host)
	assert.Equal(t, "cli-db", migration.Database)
	assert.Equal(t, "PREFERRED", migration.TLSMode)
	assert.Empty(t, migration.TLSCertificatePath)
}

func TestMigrationParamsIniFileEmptyPasswordOverridenByCommandLine(t *testing.T) {
	tmpFile := mkIniFile(t, `[client]
password =
`)
	defer os.Remove(tmpFile.Name())
	require.NoError(t, tmpFile.Close())

	migration := &Migration{
		Host:     "cli-host:3306",
		Password: mkPtr("cli-password"),
		Username: "cli-user",
		Database: "cli-db",
		Table:    "testtable",
		Alter:    "ENGINE=InnoDB",
		ConfFile: tmpFile.Name(),
	}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	assert.Equal(t, "cli-password", *migration.Password)
	assert.Equal(t, "cli-user", migration.Username)
	assert.Equal(t, "cli-host:3306", migration.Host)
	assert.Equal(t, "cli-db", migration.Database)
	assert.Equal(t, "PREFERRED", migration.TLSMode)
	assert.Empty(t, migration.TLSCertificatePath)
}

func TestMigrationParamsIniFileOnlyPortUsedFromFile(t *testing.T) {
	tmpFile := mkIniFile(t, `[client]
port=1234
`)
	defer os.Remove(tmpFile.Name())
	require.NoError(t, tmpFile.Close())

	migration := &Migration{
		Host:     "cli-host",
		Username: "cli-user",
		Password: mkPtr("cli-password"),
		Database: "cli-db",
		Table:    "testtable",
		Alter:    "ENGINE=InnoDB",
		ConfFile: tmpFile.Name(),
	}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	assert.Equal(t, "cli-password", *migration.Password)
	assert.Equal(t, "cli-user", migration.Username)
	assert.Equal(t, "cli-host:1234", migration.Host)
	assert.Equal(t, "cli-db", migration.Database)
	assert.Equal(t, "PREFERRED", migration.TLSMode)
	assert.Empty(t, migration.TLSCertificatePath)
}

func TestMigrationParamsIniFileEmptyClientSection(t *testing.T) {
	// Test with empty client section
	tmpFile := mkIniFile(t, `[client]
`)
	defer os.Remove(tmpFile.Name())
	require.NoError(t, tmpFile.Close())

	migration := &Migration{
		Host:     "cli-host:3306",
		Username: "cli-user",
		Password: mkPtr("cli-password"),
		Database: "cli-db",
		Table:    "testtable",
		Alter:    "ENGINE=InnoDB",
		ConfFile: tmpFile.Name(),
	}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	assert.Equal(t, "cli-host:3306", migration.Host)
	assert.Equal(t, "cli-user", migration.Username)
	assert.Equal(t, "cli-password", *migration.Password)
	assert.Equal(t, "cli-db", migration.Database)
	assert.Equal(t, "PREFERRED", migration.TLSMode)
	assert.Empty(t, migration.TLSCertificatePath)
}

func TestMigrationParamsIniFileHasNoClientSection(t *testing.T) {
	// Test with no client section at all
	tmpFile := mkIniFile(t, `[mysql]
user = mysqluser
password = mysqlpass
`)
	defer os.Remove(tmpFile.Name())
	require.NoError(t, tmpFile.Close())

	migration := &Migration{
		Host:     "cli-host:3306",
		Username: "cli-user",
		Password: mkPtr("cli-password"),
		Database: "cli-db",
		Table:    "testtable",
		Alter:    "ENGINE=InnoDB",
		ConfFile: tmpFile.Name(),
	}

	_, err := migration.normalizeOptions()
	require.NoError(t, err)

	assert.Equal(t, "cli-host:3306", migration.Host)
	assert.Equal(t, "cli-user", migration.Username)
	assert.Equal(t, "cli-password", *migration.Password)
	assert.Equal(t, "cli-db", migration.Database)
	assert.Equal(t, "PREFERRED", migration.TLSMode)
	assert.Empty(t, migration.TLSCertificatePath)
}
