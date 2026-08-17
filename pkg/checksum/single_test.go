package checksum

import (
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	mysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// newTestCheckerConfig is NewCheckerDefaultConfig plus the repair applier that
// the single-server checker requires (see CheckerConfig.RepairApplier). The
// applier writes to db, which in these tests holds both tables.
func newTestCheckerConfig(t *testing.T, db *sql.DB) *CheckerConfig {
	t.Helper()
	config := NewCheckerDefaultConfig()
	config.RepairApplier = applier.NewSingleTargetForTest(t, db)
	return config
}

func TestBasicChecksum(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS basic_checksum, _basic_checksum_new, _basic_checksum_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE basic_checksum (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _basic_checksum_new (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _basic_checksum_chkpnt (a INT)") // for binlog advancement
	testutils.RunSQL(t, "INSERT INTO basic_checksum VALUES (1, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO _basic_checksum_new VALUES (1, 2, 3)")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "basic_checksum")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_basic_checksum_new")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), change.NewClientDefaultConfig())
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t2, chunker))
	require.NoError(t, feed.Start(t.Context()))
	require.NoError(t, chunker.Open())
	checker, err := NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, newTestCheckerConfig(t, db))
	require.NoError(t, err)

	require.NoError(t, checker.Run(t.Context()))
}

func TestBasicValidation(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS basic_validation, basic_validation2, _basic_validation_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE basic_validation (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE basic_validation2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _basic_validation_chkpnt (a INT)") // for binlog advancement
	testutils.RunSQL(t, "INSERT INTO basic_validation VALUES (1, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO basic_validation2 VALUES (1, 2, 3)")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "basic_validation")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "basic_validation2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), change.NewClientDefaultConfig())
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t2, chunker))
	require.NoError(t, feed.Start(t.Context()))

	_, err = NewChecker(nil, chunker, []change.Source{feed}, newTestCheckerConfig(t, db)) // no source DBs
	require.EqualError(t, err, "at least one source database must be provided")

	_, err = NewChecker([]*sql.DB{db}, nil, []change.Source{feed}, newTestCheckerConfig(t, db))
	require.EqualError(t, err, "chunker must be non-nil")

	_, err = NewChecker([]*sql.DB{db}, chunker, nil, newTestCheckerConfig(t, db)) // no feed
	require.EqualError(t, err, "at least one feed must be provided")

	// The single checker cannot repair without an applier, and that has to fail
	// here rather than on the first mismatch hours into a migration.
	_, err = NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, NewCheckerDefaultConfig())
	require.EqualError(t, err, "repair applier must be non-nil")

	// ... but the distributed checker repairs through its own applier, so it does
	// not need one.
	distConfig := NewCheckerDefaultConfig()
	distConfig.Applier = applier.NewSingleTargetForTest(t, db)
	_, err = NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, distConfig)
	require.NoError(t, err)
}

func TestUnfixableUniqueChecksum(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS uniqfailuret1, uniqfailuret2`)
	table1 := `CREATE TABLE uniqfailuret1 (
				id int NOT NULL AUTO_INCREMENT,
				name varchar(255) NOT NULL,
				b varchar(255) NOT NULL,
				PRIMARY KEY (id)
			)`
	table2 := `CREATE TABLE uniqfailuret2 (
				id int NOT NULL AUTO_INCREMENT,
				name varchar(255) NOT NULL,
				b varchar(255) NOT NULL,
				PRIMARY KEY (id),
				UNIQUE (b)
			)`
	testutils.RunSQL(t, table1)
	testutils.RunSQL(t, table2)
	testutils.RunSQL(t, "INSERT INTO uniqfailuret1 (name, b) VALUES ('a', REPEAT('a', 200))")
	testutils.RunSQL(t, "INSERT INTO uniqfailuret1 (name, b) VALUES ('a', REPEAT('b', 200))")
	testutils.RunSQL(t, "INSERT INTO uniqfailuret1 (name, b) VALUES ('a', REPEAT('c', 200))")
	testutils.RunSQL(t, "INSERT INTO uniqfailuret1 (name, b) VALUES ('a', REPEAT('a', 200))") // will cause unique index failure
	testutils.RunSQL(t, `INSERT IGNORE INTO uniqfailuret2 SELECT * FROM uniqfailuret1`)       // will not copy all data

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "uniqfailuret1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "uniqfailuret2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), change.NewClientDefaultConfig())
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t2, chunker))
	require.NoError(t, feed.Start(t.Context()))
	require.NoError(t, chunker.Open())

	config := newTestCheckerConfig(t, db)
	config.FixDifferences = true
	checker, err := NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, config)
	require.NoError(t, err)
	err = checker.Run(t.Context())
	// Adding a UNIQUE INDEX to non-unique data: every attempt finds row
	// differences (and recopies don't help), so we exhaust retries on the
	// "found differences" path. The migration layer wraps this into a more
	// user-friendly "lossy unique-index" message; here we just assert the
	// underlying checksum-layer error.
	require.ErrorIs(t, err, ErrDifferencesExhausted)
}

func TestFixCorrupt(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS fixcorruption_t1, _fixcorruption_t1_new, _fixcorruption_t1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE fixcorruption_t1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _fixcorruption_t1_new (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _fixcorruption_t1_chkpnt (a INT)") // for binlog advancement
	testutils.RunSQL(t, "INSERT INTO fixcorruption_t1 VALUES (1, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO _fixcorruption_t1_new VALUES (1, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO _fixcorruption_t1_new VALUES (2, 2, 3)") // corrupt

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "fixcorruption_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_fixcorruption_t1_new")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), change.NewClientDefaultConfig())
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t2, chunker))
	require.NoError(t, feed.Start(t.Context()))
	require.NoError(t, chunker.Open())

	config := newTestCheckerConfig(t, db)
	config.FixDifferences = true
	config.MaxRetries = 2
	checker, err := NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, config)
	require.NoError(t, err)
	err = checker.Run(t.Context())
	require.NoError(t, err) // yes there is corruption, but it was fixed.

	// Type assert the checker to *SingleChecker to access differencesFound
	singleChecker, ok := checker.(*SingleChecker)
	require.True(t, ok, "checker is not of type *SingleChecker")
	require.Equal(t, uint64(0), singleChecker.differencesFound.Load()) // this is "0", because we fixed it.

	// If we run the checker again, it will report zero differences.
	checker2, err := NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, config)
	require.NoError(t, err)
	err = checker2.Run(t.Context())
	require.NoError(t, err)
	singleChecker, ok = checker2.(*SingleChecker)
	require.True(t, ok, "checker2 is not of type *SingleChecker")
	require.Equal(t, uint64(0), singleChecker.differencesFound.Load())
}

// TestRetryDoesNotVacuouslyPass is a regression test for the retry loop in
// Run. A failed attempt leaves isInvalid=true (set by the errgroup workers),
// and the retry reset previously did not clear it. Because isHealthy()
// returns false while isInvalid is set, the next attempt dispatched zero
// chunks and completed with differencesFound==0 — logging "checksum passed"
// and returning nil without having verified a single row. With
// FixDifferences=false a persistent mismatch must fail every attempt and
// surface an error, never nil.
func TestRetryDoesNotVacuouslyPass(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS retrypoison_t1, _retrypoison_t1_new, _retrypoison_t1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE retrypoison_t1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _retrypoison_t1_new (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _retrypoison_t1_chkpnt (a INT)") // for binlog advancement
	testutils.RunSQL(t, "INSERT INTO retrypoison_t1 VALUES (1, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO _retrypoison_t1_new VALUES (1, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO _retrypoison_t1_new VALUES (2, 2, 3)") // corrupt: row not in source

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "retrypoison_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_retrypoison_t1_new")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), change.NewClientDefaultConfig())
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t2, chunker))
	require.NoError(t, feed.Start(t.Context()))
	require.NoError(t, chunker.Open())

	config := newTestCheckerConfig(t, db)
	config.FixDifferences = false // surface the mismatch as an error on every attempt
	config.MaxRetries = 2
	checker, err := NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, config)
	require.NoError(t, err)

	err = checker.Run(t.Context())
	require.Error(t, err)
	require.ErrorIs(t, err, ErrAttemptsExhausted)
	require.NotErrorIs(t, err, ErrDifferencesExhausted)
	require.ErrorContains(t, err, "checksum mismatch")

	// The final attempt must have actually re-verified chunks: its counter
	// was reset at the start of the attempt, so a non-zero value proves the
	// mismatch was re-detected rather than skipped.
	singleChecker, ok := checker.(*SingleChecker)
	require.True(t, ok, "checker is not of type *SingleChecker")
	require.Positive(t, singleChecker.differencesFound.Load())
}

// TestRunResetsPriorInvalidState covers the cross-Run leak of isInvalid: a
// prior Run that errored WITHOUT recording differences (e.g. a transient
// connection failure) leaves isInvalid=true and differencesFound==0. A
// subsequent Run on the same checker must start healthy — without the reset
// at the top of Run, attempt 1 skipped every chunk (isHealthy()==false), saw
// differencesFound==0, and returned nil having verified zero rows. Run must
// instead do real work: the chunker ends fully read with rows checked.
func TestRunResetsPriorInvalidState(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS runpoison_t1, _runpoison_t1_new, _runpoison_t1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE runpoison_t1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _runpoison_t1_new (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _runpoison_t1_chkpnt (a INT)") // for binlog advancement
	testutils.RunSQL(t, "INSERT INTO runpoison_t1 VALUES (1, 2, 3), (2, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO _runpoison_t1_new VALUES (1, 2, 3), (2, 2, 3)")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "runpoison_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_runpoison_t1_new")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), change.NewClientDefaultConfig())
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t2, chunker))
	require.NoError(t, feed.Start(t.Context()))
	require.NoError(t, chunker.Open())

	checker, err := NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, newTestCheckerConfig(t, db))
	require.NoError(t, err)
	singleChecker, ok := checker.(*SingleChecker)
	require.True(t, ok, "checker is not of type *SingleChecker")
	// Simulate the state left by a prior errored Run that found no differences.
	singleChecker.setInvalid(true)

	// The data is identical, so the pass must succeed — with real work done.
	require.NoError(t, checker.Run(t.Context()))
	require.True(t, chunker.IsRead(), "the chunker must be fully read; a vacuous pass reads no chunks")
	require.Positive(t, checker.GetProgress().RowsChecked, "rows must actually be verified")
}

func TestCorruptChecksum(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS chkpcorruptt1, _chkpcorruptt1_new, _chkpcorruptt1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE chkpcorruptt1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _chkpcorruptt1_new (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _chkpcorruptt1_chkpnt (a INT)") // for binlog advancement
	testutils.RunSQL(t, "INSERT INTO chkpcorruptt1 VALUES (1, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO _chkpcorruptt1_new VALUES (1, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO _chkpcorruptt1_new VALUES (2, 2, 3)") // corrupt

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "chkpcorruptt1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_chkpcorruptt1_new")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), change.NewClientDefaultConfig())
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t2, chunker))
	require.NoError(t, feed.Start(t.Context()))
	require.NoError(t, chunker.Open())

	checker, err := NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, newTestCheckerConfig(t, db))
	require.NoError(t, err)
	singleChecker, ok := checker.(*SingleChecker)
	require.True(t, ok, "checker is not of type *SingleChecker")
	err = singleChecker.runChecksum(t.Context())
	require.ErrorContains(t, err, "checksum mismatch")
}

// TestCorruptBinaryChecksum tests that the checksum detects corruption in a
// fixed-length BINARY(N) column. Previously the checksum cast binary columns
// to binary(0), which truncates every value to zero bytes — so any two values
// produced identical CRCs and the contents of BINARY(N) columns were
// completely invisible to the checksum.
func TestCorruptBinaryChecksum(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS chkpcorruptbin1, _chkpcorruptbin1_new, _chkpcorruptbin1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE chkpcorruptbin1 (a INT NOT NULL, b BINARY(16) NOT NULL, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _chkpcorruptbin1_new (a INT NOT NULL, b BINARY(16) NOT NULL, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _chkpcorruptbin1_chkpnt (a INT)") // for binlog advancement
	testutils.RunSQL(t, "INSERT INTO chkpcorruptbin1 VALUES (1, UNHEX('00112233445566778899AABBCCDDEEFF'))")
	testutils.RunSQL(t, "INSERT INTO _chkpcorruptbin1_new SELECT * FROM chkpcorruptbin1")
	// Corrupt the binary value on the target: same length, different contents.
	testutils.RunSQL(t, "UPDATE _chkpcorruptbin1_new SET b = UNHEX('FFEEDDCCBBAA99887766554433221100') WHERE a = 1")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "chkpcorruptbin1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_chkpcorruptbin1_new")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), change.NewClientDefaultConfig())
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t2, chunker))
	require.NoError(t, feed.Start(t.Context()))
	require.NoError(t, chunker.Open())

	checker, err := NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, newTestCheckerConfig(t, db))
	require.NoError(t, err)
	singleChecker, ok := checker.(*SingleChecker)
	require.True(t, ok, "checker is not of type *SingleChecker")
	err = singleChecker.runChecksum(t.Context())
	require.ErrorContains(t, err, "checksum mismatch")
}

func TestBoundaryCases(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS checkert1, _checkert1_new, _checkert1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE checkert1 (a INT NOT NULL, b FLOAT, c VARCHAR(255), PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _checkert1_new (a INT NOT NULL, b FLOAT, c VARCHAR(255), PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _checkert1_chkpnt (a INT NOT NULL)")
	testutils.RunSQL(t, "INSERT INTO checkert1 VALUES (1, 2.2, '')")        // null vs empty string
	testutils.RunSQL(t, "INSERT INTO _checkert1_new VALUES (1, 2.2, NULL)") // should not compare

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "checkert1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_checkert1_new")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), change.NewClientDefaultConfig())
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t2, chunker))
	require.NoError(t, feed.Start(t.Context()))
	require.NoError(t, chunker.Open())

	checker, err := NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, newTestCheckerConfig(t, db))
	require.NoError(t, err)
	// Type assert to *SingleChecker to access runChecksum
	singleChecker, ok := checker.(*SingleChecker)
	require.True(t, ok, "checker is not of type *SingleChecker")
	require.Error(t, singleChecker.runChecksum(t.Context()))

	// UPDATE t1 to also be NULL
	testutils.RunSQL(t, "UPDATE checkert1 SET c = NULL")
	checker, err = NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, newTestCheckerConfig(t, db))
	require.NoError(t, err)
	// Type assert to *SingleChecker to access runChecksum
	singleChecker, ok = checker.(*SingleChecker)
	require.True(t, ok, "checker is not of type *SingleChecker")
	require.NoError(t, singleChecker.runChecksum(t.Context()))
}

func TestChangeDataTypeDatetime(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS tdatetime, _tdatetime_new")
	testutils.RunSQL(t, `CREATE TABLE tdatetime (
	id bigint NOT NULL AUTO_INCREMENT primary key,
	created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	issued_at timestamp NULL DEFAULT NULL,
	activated_at timestamp NULL DEFAULT NULL,
	deactivated_at timestamp NULL DEFAULT NULL
	)`)
	testutils.RunSQL(t, `CREATE TABLE _tdatetime_new (
	id bigint NOT NULL AUTO_INCREMENT primary key,
	created_at timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
	updated_at timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
	issued_at timestamp(6) NULL DEFAULT NULL,
	activated_at timestamp(6) NULL DEFAULT NULL,
	deactivated_at timestamp(6) NULL DEFAULT NULL
	)`)
	testutils.RunSQL(t, `INSERT INTO tdatetime (created_at, updated_at, issued_at, activated_at, deactivated_at) VALUES
	('2023-05-18 09:28:46', '2023-05-18 09:33:27', '2023-05-18 09:28:45', '2023-05-18 09:28:45', NULL               ),
	('2023-05-18 09:34:38', '2023-05-24 07:38:25', '2023-05-18 09:34:37', '2023-05-18 09:34:37', '2023-05-24 07:38:25'),
	('2023-05-24 07:34:36', '2023-05-24 07:34:36', '2023-05-24 07:34:35', NULL               , NULL               ),
	('2023-05-24 07:41:05', '2023-05-25 06:15:37', '2023-05-24 07:41:04', '2023-05-24 07:41:04', '2023-05-25 06:15:37'),
	('2023-05-25 06:17:30', '2023-05-25 06:17:30', '2023-05-25 06:17:29', '2023-05-25 06:17:29', NULL               ),
	('2023-05-25 06:18:33', '2023-05-25 06:41:13', '2023-05-25 06:18:32', '2023-05-25 06:18:32', '2023-05-25 06:41:13'),
	('2023-05-25 06:24:23', '2023-05-25 06:24:23', '2023-05-25 06:24:22', NULL               , NULL               ),
	('2023-05-25 06:41:35', '2023-05-28 23:45:09', '2023-05-25 06:41:34', '2023-05-25 06:41:34', '2023-05-28 23:45:09'),
	('2023-05-25 06:44:41', '2023-05-28 23:45:03', '2023-05-25 06:44:40', '2023-05-25 06:46:48', '2023-05-28 23:45:03'),
	('2023-05-26 06:24:24', '2023-05-28 23:45:01', '2023-05-26 06:24:23', '2023-05-26 06:24:42', '2023-05-28 23:45:01'),
	('2023-05-28 23:46:07', '2023-05-29 00:57:55', '2023-05-28 23:46:05', '2023-05-28 23:46:05', NULL               ),
	('2023-05-28 23:53:34', '2023-05-29 00:57:56', '2023-05-28 23:53:33', '2023-05-28 23:58:09', NULL               );`)
	testutils.RunSQL(t, `INSERT INTO _tdatetime_new SELECT * FROM tdatetime`)
	// The checkpoint table is required for blockwait, structure doesn't matter.
	testutils.RunSQL(t, "CREATE TABLE IF NOT EXISTS _tdatetime_chkpnt (id int)")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "tdatetime")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_tdatetime_new")
	require.NoError(t, t2.SetInfo(t.Context())) // fails

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), change.NewClientDefaultConfig())
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t2, chunker))
	require.NoError(t, feed.Start(t.Context()))
	require.NoError(t, chunker.Open())

	checker, err := NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, newTestCheckerConfig(t, db))
	require.NoError(t, err)
	require.NoError(t, checker.Run(t.Context())) // fails
}

func TestYieldTimeout(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS yield_t1, _yield_t1_new, _yield_t1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE yield_t1 (a INT NOT NULL AUTO_INCREMENT, b VARCHAR(255), c VARCHAR(255), PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _yield_t1_new (a INT NOT NULL AUTO_INCREMENT, b VARCHAR(255), c VARCHAR(255), PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _yield_t1_chkpnt (a INT)") // for binlog advancement

	// Insert enough rows with wide data to produce multiple chunks and ensure
	// the checksum takes long enough for the yield timeout to fire mid-pass.
	// Starting chunk size is 1000, so 100k rows should produce many chunks.
	testutils.RunSQL(t, "INSERT INTO yield_t1 (b, c) SELECT REPEAT('x', 200), REPEAT('y', 200) FROM information_schema.columns a, information_schema.columns b LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO _yield_t1_new SELECT * FROM yield_t1")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "yield_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_yield_t1_new")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), change.NewClientDefaultConfig())
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t2, chunker))
	require.NoError(t, feed.Start(t.Context()))
	require.NoError(t, chunker.Open())

	config := newTestCheckerConfig(t, db)
	config.Concurrency = 1
	// Use a short yield timeout. The initConnPool phase uses the parent
	// context (not the yield context), so lock acquisition always succeeds.
	// The yield context only governs the chunk-processing loop. 100ms is
	// long enough for at least one chunk to complete (setting the watermark)
	// but short enough to trigger multiple yields over 100k rows.
	config.YieldTimeout = 100 * time.Millisecond
	checker, err := NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, config)
	require.NoError(t, err)

	// The checksum should still pass despite yielding — it resumes from the watermark.
	require.NoError(t, checker.Run(t.Context()))

	// Verify that at least one yield actually occurred.
	singleChecker := checker.(*SingleChecker)
	require.Positive(t, singleChecker.yieldsPerformed.Load(), "expected at least one yield to occur")
	t.Logf("yields performed: %d", singleChecker.yieldsPerformed.Load())
}

func TestFromWatermark(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS tfromwatermark, _tfromwatermark_new, _tfromwatermark_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE tfromwatermark (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _tfromwatermark_new (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "INSERT INTO tfromwatermark VALUES (1, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO _tfromwatermark_new VALUES (1, 2, 3)")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "tfromwatermark")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_tfromwatermark_new")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), change.NewClientDefaultConfig())
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t2, chunker))
	require.NoError(t, feed.Start(t.Context()))
	require.NoError(t, chunker.Open())

	config := newTestCheckerConfig(t, db)
	config.Watermark = "{\"Key\":[\"a\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\": [\"2\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"3\"],\"Inclusive\":false}}"
	checker, err := NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, config)
	require.NoError(t, err)
	require.NoError(t, checker.Run(t.Context()))
}

// TestColumnBoundaryShift is a regression test for the missing inter-column
// separator in the checksum expression. Without a separator, CONCAT() lets
// content shift across adjacent column boundaries undetected: the rows
// ('x0', 'y') and ('x', '0y') concatenate to the same string ("x00y0",
// including the ISNULL digits) and therefore the same CRC32. The checksum
// must report these rows as different.
func TestColumnBoundaryShift(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS colshift_t1, _colshift_t1_new, _colshift_t1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE colshift_t1 (id INT NOT NULL, a VARCHAR(255), b VARCHAR(255), PRIMARY KEY (id))")
	testutils.RunSQL(t, "CREATE TABLE _colshift_t1_new (id INT NOT NULL, a VARCHAR(255), b VARCHAR(255), PRIMARY KEY (id))")
	testutils.RunSQL(t, "CREATE TABLE _colshift_t1_chkpnt (a INT)") // for binlog advancement
	// The trailing '0' of column a has migrated to the front of column b
	// in the target table. The data is different, so the checksum must fail.
	testutils.RunSQL(t, "INSERT INTO colshift_t1 VALUES (1, 'x0', 'y')")
	testutils.RunSQL(t, "INSERT INTO _colshift_t1_new VALUES (1, 'x', '0y')")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "colshift_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_colshift_t1_new")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), change.NewClientDefaultConfig())
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t2, chunker))
	require.NoError(t, feed.Start(t.Context()))
	require.NoError(t, chunker.Open())

	checker, err := NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, newTestCheckerConfig(t, db))
	require.NoError(t, err)
	singleChecker, ok := checker.(*SingleChecker)
	require.True(t, ok, "checker is not of type *SingleChecker")
	err = singleChecker.runChecksum(t.Context())
	require.ErrorContains(t, err, "checksum mismatch")
}

// TestChecksumChunkReleasesTrxDuringRepair reproduces the production failure
// mode where checksum transactions died to wait_timeout DESPITE the pool
// keepalive: repairs serialize on recopyLock, so a worker that hit a mismatch
// could park for many minutes holding its transaction — checked out and
// therefore invisible to the keepalive. The fix returns the transaction to
// the pool the moment the snapshot reads are done. This test holds recopyLock
// (simulating another slow repair), drives a mismatched chunk through
// ChecksumChunk, and requires the full pool to be available while the repair
// is still queued.
func TestChecksumChunkReleasesTrxDuringRepair(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS trxrelease, _trxrelease_new, _trxrelease_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE trxrelease (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _trxrelease_new (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _trxrelease_chkpnt (a INT)")
	testutils.RunSQL(t, "INSERT INTO trxrelease VALUES (1, 2, 3), (2, 2, 3), (3, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO _trxrelease_new VALUES (1, 2, 3), (2, 2, 3)") // row 3 missing: mismatch

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "trxrelease")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_trxrelease_new")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), change.NewClientDefaultConfig())
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t2, chunker))
	require.NoError(t, feed.Start(t.Context()))
	require.NoError(t, chunker.Open())

	config := newTestCheckerConfig(t, db)
	config.FixDifferences = true
	checkerIntf, err := NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, config)
	require.NoError(t, err)
	checker, ok := checkerIntf.(*SingleChecker)
	require.True(t, ok)

	chunk, err := chunker.Next() // small table: one chunk covers everything
	require.NoError(t, err)

	pool, err := dbconn.NewTrxPool(t.Context(), db, 2, config.DBConfig, config.Logger)
	require.NoError(t, err)
	defer func() { require.NoError(t, pool.Close()) }()

	// Simulate another worker's long-running repair.
	checker.recopyLock.Lock()
	errCh := make(chan error, 1)
	go func() {
		errCh <- checker.ChecksumChunk(t.Context(), pool, chunk)
	}()

	// Once the mismatch has been inspected, the transaction must be back in
	// the pool even though the repair is still queued on recopyLock. The
	// differencesFound guard ensures we don't probe before the worker has
	// taken (and must have returned) its transaction.
	require.Eventually(t, func() bool {
		if checker.differencesFound.Load() == 0 {
			return false
		}
		trx1, err := pool.Get()
		if err != nil {
			return false
		}
		trx2, err := pool.Get()
		if err != nil {
			pool.Put(trx1)
			return false
		}
		pool.Put(trx1)
		pool.Put(trx2)
		return true
	}, 30*time.Second, 25*time.Millisecond, "transaction was not returned to the pool while the repair was queued")

	// The repair itself must still be blocked on recopyLock.
	select {
	case err := <-errCh:
		t.Fatalf("ChecksumChunk returned while recopyLock was held: %v", err)
	default:
	}

	checker.recopyLock.Unlock()
	require.NoError(t, <-errCh)

	// And the repair actually repaired.
	var cnt int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _trxrelease_new").Scan(&cnt))
	require.Equal(t, 3, cnt)
}

// flakyChunker wraps a real chunker and injects one transient Next() error at
// a chosen call number, recording whether the retry that follows resumed from
// the low watermark or reset to the beginning.
type flakyChunker struct {
	table.MappedChunker
	sync.Mutex
	failAtCall int // 1-indexed Next() call to fail once; 0 disables
	nextCalls  int
	wmResumes  int
	resets     int
}

func (c *flakyChunker) Next() (*table.Chunk, error) {
	c.Lock()
	c.nextCalls++
	inject := c.failAtCall != 0 && c.nextCalls == c.failAtCall
	if inject {
		c.failAtCall = 0
	}
	c.Unlock()
	if inject {
		return nil, errors.New("injected transient error")
	}
	return c.MappedChunker.Next()
}

func (c *flakyChunker) OpenAtWatermark(watermark string) error {
	c.Lock()
	c.wmResumes++
	c.Unlock()
	return c.MappedChunker.OpenAtWatermark(watermark)
}

func (c *flakyChunker) Reset() error {
	c.Lock()
	c.resets++
	c.Unlock()
	return c.MappedChunker.Reset()
}

// checksumRetryHarness builds a single-checker over a seeded 4096-row table
// pair wrapped in a flakyChunker that errors on the third Next() call. By
// then two chunks have completed: the first chunk has no lower bound (and so
// cannot be expressed as a watermark on its own), the second has real bounds,
// so the low watermark is ready when the injected error fires.
//
// mutateSQL (optional) plants a difference in the target table. It runs
// before the binlog feed starts: a write to the _new table after the feed is
// running is captured as a pending change, and the checksum's own
// flush-under-lock would faithfully re-copy those keys from the source —
// reverting the planted difference before any chunk gets to see it.
func checksumRetryHarness(t *testing.T, name string, mutateSQL string) (*flakyChunker, Checker, func()) {
	t.Helper()
	testutils.RunSQL(t, "DROP TABLE IF EXISTS "+name+", _"+name+"_new, _"+name+"_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE "+name+" (a INT NOT NULL AUTO_INCREMENT, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "INSERT INTO "+name+" (b, c) VALUES (2, 3)")
	for range 12 { // 2^12 = 4096 rows: several chunks at the 1000-row starting size
		testutils.RunSQL(t, "INSERT INTO "+name+" (b, c) SELECT b, c FROM "+name)
	}
	testutils.RunSQL(t, "CREATE TABLE _"+name+"_new LIKE "+name)
	testutils.RunSQL(t, "INSERT INTO _"+name+"_new SELECT * FROM "+name)
	testutils.RunSQL(t, "CREATE TABLE _"+name+"_chkpnt (a INT)")
	if mutateSQL != "" {
		testutils.RunSQL(t, mutateSQL)
	}

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)

	t1 := table.NewTableInfo(db, "test", name)
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_"+name+"_new")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), change.NewClientDefaultConfig())
	inner, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	chunker := &flakyChunker{MappedChunker: inner, failAtCall: 3}
	require.NoError(t, feed.AddSubscription(t1, t2, chunker))
	require.NoError(t, feed.Start(t.Context()))
	require.NoError(t, chunker.Open())

	config := newTestCheckerConfig(t, db)
	config.Concurrency = 1 // deterministic: chunks 1-2 complete (watermark ready) before Next() call 3 injects
	config.FixDifferences = true
	checker, err := NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, config)
	require.NoError(t, err)
	return chunker, checker, func() {
		feed.Close()
		utils.CloseAndLog(db)
	}
}

// TestChecksumRetryResumesFromWatermark verifies that an attempt which errors
// WITHOUT having found any differences (the connection-massacre shape from
// production) resumes its retry from the low watermark instead of discarding
// all verified work.
func TestChecksumRetryResumesFromWatermark(t *testing.T) {
	chunker, checker, cleanup := checksumRetryHarness(t, "retryresume", "")
	defer cleanup()

	require.NoError(t, checker.Run(t.Context()))

	chunker.Lock()
	defer chunker.Unlock()
	require.Equal(t, 1, chunker.wmResumes, "error-only retry should resume from the low watermark")
	require.Zero(t, chunker.resets, "error-only retry should not reset to the beginning")
}

// TestChecksumRetryResetsAfterDifferences verifies the guard on the resume
// path: once an attempt has found (and repaired) differences, a retry must
// restart from the beginning so the final pass provably verifies the whole
// table clean — including the repaired chunks.
func TestChecksumRetryResetsAfterDifferences(t *testing.T) {
	// A difference inside the first bounded chunk's range, found and repaired
	// before the injected error on Next() call 3. Row a=1 is the only id the
	// seeding guarantees: the doubling INSERT..SELECTs leave auto-inc gaps.
	chunker, checker, cleanup := checksumRetryHarness(t, "retryreset",
		"UPDATE _retryreset_new SET b = 999 WHERE a = 1")
	defer cleanup()

	require.NoError(t, checker.Run(t.Context()))

	chunker.Lock()
	defer chunker.Unlock()
	require.Zero(t, chunker.wmResumes, "retry after repairs must not skip re-verification")
	require.GreaterOrEqual(t, chunker.resets, 1, "retry after repairs should reset to the beginning")
}
