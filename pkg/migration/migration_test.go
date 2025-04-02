package migration

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	_ "github.com/pingcap/tidb/pkg/parser/test_driver"

	"github.com/cashapp/spirit/pkg/testutils"
	"github.com/go-sql-driver/mysql"

	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	checkpointDumpInterval = 100 * time.Millisecond
	statusInterval = 10 * time.Millisecond // the status will be accurate to 1ms
	sentinelCheckInterval = 100 * time.Millisecond
	sentinelWaitLimit = 10 * time.Second
	os.Exit(m.Run())
}

func TestE2ENullAlterEmpty(t *testing.T) {
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
	migration.Password = cfg.Passwd
	migration.Database = cfg.DBName
	migration.Threads = 1
	migration.Checksum = true
	migration.Table = "t1e2e"
	migration.Alter = "ENGINE=InnoDB"

	err = migration.Run()
	assert.NoError(t, err)
}

func TestMissingAlter(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1, _t1_new`)
	table := `CREATE TABLE t1 (
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
	migration.Password = cfg.Passwd
	migration.Database = cfg.DBName
	migration.Threads = 16
	migration.Checksum = true
	migration.Table = "t1"
	migration.Alter = ""

	err = migration.Run()
	assert.Error(t, err) // missing alter
	assert.ErrorContains(t, err, "alter statement is required")
}

func TestBadDatabaseCredentials(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1, _t1_new`)
	table := `CREATE TABLE t1 (
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
	migration.Password = cfg.Passwd
	migration.Database = cfg.DBName
	migration.Threads = 16
	migration.Checksum = true
	migration.Table = "t1"
	migration.Alter = "ENGINE=InnoDB"

	err = migration.Run()
	assert.Error(t, err)                                        // bad database credentials
	assert.ErrorContains(t, err, "connect: connection refused") // could be no host or temporary resolution failure.
}

func TestE2ENullAlter1Row(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1, _t1_new`)
	table := `CREATE TABLE t1 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, `insert into t1 (id,name) values (1, 'aaa')`)
	migration := &Migration{}
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	migration.Host = cfg.Addr
	migration.Username = cfg.User
	migration.Password = cfg.Passwd
	migration.Database = cfg.DBName
	migration.Threads = 16
	migration.Checksum = true
	migration.Table = "t1"
	migration.Alter = "ENGINE=InnoDB"

	err = migration.Run()
	assert.NoError(t, err)
}

func TestE2ENullAlterWithReplicas(t *testing.T) {
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
	migration.Password = cfg.Passwd
	migration.Database = cfg.DBName
	migration.Threads = 16
	migration.Checksum = true
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
	migration.Password = cfg.Passwd
	migration.Database = cfg.DBName
	migration.Threads = 16
	migration.Checksum = true
	migration.Table = "renamet1"
	migration.Alter = "CHANGE name nameNew varchar(255) not null"

	err = migration.Run()
	assert.NoError(t, err)
}

// TestUniqueOnNonUniqueData tests that we:
// 1. Fail trying to add a unique index on non-unique data.
// 2. The error does not blame spirit, but is instead suggestive of user-data error.
func TestUniqueOnNonUniqueData(t *testing.T) {
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
	migration.Password = cfg.Passwd
	migration.Database = cfg.DBName
	migration.Threads = 16
	migration.Checksum = true
	migration.Table = "uniquet1"
	migration.Alter = "ADD UNIQUE (b)"
	err = migration.Run()
	assert.Error(t, err)
	assert.ErrorContains(t, err, "Check that the ALTER statement is not adding a UNIQUE INDEX to non-unique data")
}

func TestGeneratedColumns(t *testing.T) {
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
	migration.Password = cfg.Passwd
	migration.Database = cfg.DBName
	migration.Threads = 1
	migration.Checksum = true
	migration.Table = "t1generated"
	migration.Alter = "ENGINE=InnoDB"

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
		migration.Password = cfg.Passwd
		migration.Database = cfg.DBName
		migration.Threads = 1
		migration.Checksum = true
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
	migration.Password = cfg.Passwd
	migration.Database = cfg.DBName
	migration.Threads = 1
	migration.Checksum = true
	migration.Table = "t1charset"
	migration.Alter = "CONVERT TO CHARACTER SET UTF8MB4"
	err = migration.Run()
	assert.NoError(t, err)

	// Because utf8mb4 is the superset, it doesn't matter that that's
	// what the checksum casts to. We should be able to convert back as well.
	migration = &Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  1,
		Checksum: true,
		Table:    "t1charset",
		Alter:    "CONVERT TO CHARACTER SET latin1",
	}
	err = migration.Run()
	assert.NoError(t, err)
}

func TestStmtWorkflow(t *testing.T) {
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
		Password:  cfg.Passwd,
		Database:  cfg.DBName,
		Threads:   1,
		Checksum:  true,
		Statement: table, // CREATE TABLE.
	}
	err = migration.Run()
	assert.NoError(t, err)
	// We can also specify ALTER options in the statement.
	migration = &Migration{
		Host:      cfg.Addr,
		Username:  cfg.User,
		Password:  cfg.Passwd,
		Database:  cfg.DBName,
		Threads:   1,
		Checksum:  true,
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
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1parse, _t1parse_new`)
	table := `CREATE TABLE t1parse (b BLOB DEFAULT ('abc'))`
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	migration := &Migration{
		Host:      cfg.Addr,
		Username:  cfg.User,
		Password:  cfg.Passwd,
		Database:  cfg.DBName,
		Threads:   1,
		Checksum:  true,
		Statement: table,
	}
	err = migration.Run()
	assert.Error(t, err)
	assert.ErrorContains(t, err, "could not parse SQL statement")

	// Try again as ALTER TABLE, with --statement
	testutils.RunSQL(t, table)
	migration = &Migration{
		Host:      cfg.Addr,
		Username:  cfg.User,
		Password:  cfg.Passwd,
		Database:  cfg.DBName,
		Threads:   1,
		Checksum:  true,
		Statement: "ALTER TABLE t1parse ADD COLUMN c BLOB DEFAULT ('abc')",
	}
	err = migration.Run()
	assert.Error(t, err)
	assert.ErrorContains(t, err, "could not parse SQL statement")

	// With ALTER TABLE as --table and --alter
	migration = &Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  1,
		Checksum: true,
		Table:    "t1parse",
		Alter:    "ADD COLUMN c BLOB DEFAULT ('abc')",
	}
	err = migration.Run()
	assert.Error(t, err)
	assert.ErrorContains(t, err, "could not parse SQL statement")

	// With CREATE TRIGGER.
	migration = &Migration{
		Host:      cfg.Addr,
		Username:  cfg.User,
		Password:  cfg.Passwd,
		Database:  cfg.DBName,
		Threads:   1,
		Checksum:  true,
		Statement: "CREATE TRIGGER ins_sum BEFORE INSERT ON t1parse FOR EACH ROW SET @sum = @sum + NEW.b;",
	}
	err = migration.Run()
	assert.Error(t, err)
	assert.ErrorContains(t, err, "could not parse SQL statement")
}

func TestCreateIndexIsRewritten(t *testing.T) {
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
		Password:  cfg.Passwd,
		Database:  cfg.DBName,
		Threads:   1,
		Checksum:  true,
		Statement: "CREATE INDEX idx ON " + cfg.DBName + ".t1createindex (b)",
	}
	err = migration.Run()
	assert.NoError(t, err)
}

func TestSchemaNameIncluded(t *testing.T) {
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
		Password:  cfg.Passwd,
		Database:  cfg.DBName,
		Threads:   1,
		Checksum:  true,
		Statement: "ALTER TABLE test.t1schemaname ADD COLUMN c int",
	}
	err = migration.Run()
	assert.NoError(t, err)
}
