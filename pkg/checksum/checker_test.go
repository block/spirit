package checksum

import (
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/repl"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	mysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
	os.Exit(m.Run())
}

func TestBasicChecksum(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS basic_checksum, _basic_checksum_new, _basic_checksum_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE basic_checksum (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _basic_checksum_new (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _basic_checksum_chkpnt (a INT)") // for binlog advancement
	testutils.RunSQL(t, "INSERT INTO basic_checksum VALUES (1, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO _basic_checksum_new VALUES (1, 2, 3)")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()

	t1 := table.NewTableInfo(db, "test", "basic_checksum")
	assert.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_basic_checksum_new")
	assert.NoError(t, t2.SetInfo(t.Context()))
	logger := slog.Default()

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	feed := repl.NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, &repl.ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        repl.NewServerID(),
	})
	assert.NoError(t, feed.Run(t.Context()))
	defer feed.Close()
	assert.NoError(t, feed.AddSubscription(t1, t2, nil))

	chunker, err := table.NewChunker(t1, t2, 0, slog.Default())
	assert.NoError(t, err)
	assert.NoError(t, chunker.Open())
	checker, err := NewChecker(db, chunker, feed, NewCheckerDefaultConfig())
	assert.NoError(t, err)

	assert.NoError(t, checker.Run(t.Context()))
}

func TestBasicValidation(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS basic_validation, basic_validation2, _basic_validation_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE basic_validation (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE basic_validation2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _basic_validation_chkpnt (a INT)") // for binlog advancement
	testutils.RunSQL(t, "INSERT INTO basic_validation VALUES (1, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO basic_validation2 VALUES (1, 2, 3)")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()

	t1 := table.NewTableInfo(db, "test", "basic_validation")
	assert.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "basic_validation2")
	assert.NoError(t, t2.SetInfo(t.Context()))
	logger := slog.Default()

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	feed := repl.NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, &repl.ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        repl.NewServerID(),
	})
	defer feed.Close()
	assert.NoError(t, feed.AddSubscription(t1, t2, nil))
	assert.NoError(t, feed.Run(t.Context()))

	chunker, err := table.NewChunker(t1, t2, 0, slog.Default())
	assert.NoError(t, err)

	_, err = NewChecker(db, nil, feed, NewCheckerDefaultConfig())
	assert.EqualError(t, err, "chunker must be non-nil")

	_, err = NewChecker(db, chunker, nil, NewCheckerDefaultConfig()) // no feed
	assert.EqualError(t, err, "feed must be non-nil")
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
	assert.NoError(t, err)
	defer db.Close()

	t1 := table.NewTableInfo(db, "test", "uniqfailuret1")
	assert.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "uniqfailuret2")
	assert.NoError(t, t2.SetInfo(t.Context()))
	logger := slog.Default()

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	feed := repl.NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, &repl.ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        repl.NewServerID(),
	})
	defer feed.Close()
	assert.NoError(t, feed.AddSubscription(t1, t2, nil))
	assert.NoError(t, feed.Run(t.Context()))

	chunker, err := table.NewChunker(t1, t2, 0, slog.Default())
	assert.NoError(t, err)
	assert.NoError(t, chunker.Open())

	config := NewCheckerDefaultConfig()
	config.FixDifferences = true
	checker, err := NewChecker(db, chunker, feed, config)
	assert.NoError(t, err)
	err = checker.Run(t.Context())
	assert.ErrorContains(t, err, "checksum failed")
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
	assert.NoError(t, err)
	defer db.Close()

	t1 := table.NewTableInfo(db, "test", "fixcorruption_t1")
	assert.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_fixcorruption_t1_new")
	assert.NoError(t, t2.SetInfo(t.Context()))
	logger := slog.Default()

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	feed := repl.NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, &repl.ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        repl.NewServerID(),
	})
	defer feed.Close()
	assert.NoError(t, feed.AddSubscription(t1, t2, nil))
	assert.NoError(t, feed.Run(t.Context()))

	chunker, err := table.NewChunker(t1, t2, 0, slog.Default())
	assert.NoError(t, err)
	assert.NoError(t, chunker.Open())

	config := NewCheckerDefaultConfig()
	config.FixDifferences = true
	config.MaxRetries = 2
	checker, err := NewChecker(db, chunker, feed, config)
	assert.NoError(t, err)
	err = checker.Run(t.Context())
	assert.NoError(t, err)                                      // yes there is corruption, but it was fixed.
	assert.Equal(t, uint64(0), checker.differencesFound.Load()) // this is "0", because we fixed it.

	// If we run the checker again, it will report zero differences.
	checker2, err := NewChecker(db, chunker, feed, config)
	assert.NoError(t, err)
	err = checker2.Run(t.Context())
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), checker2.differencesFound.Load())
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
	assert.NoError(t, err)
	defer db.Close()

	t1 := table.NewTableInfo(db, "test", "chkpcorruptt1")
	assert.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_chkpcorruptt1_new")
	assert.NoError(t, t2.SetInfo(t.Context()))
	logger := slog.Default()

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	feed := repl.NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, &repl.ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        repl.NewServerID(),
	})
	defer feed.Close()
	assert.NoError(t, feed.AddSubscription(t1, t2, nil))
	assert.NoError(t, feed.Run(t.Context()))

	chunker, err := table.NewChunker(t1, t2, 0, slog.Default())
	assert.NoError(t, err)
	assert.NoError(t, chunker.Open())

	checker, err := NewChecker(db, chunker, feed, NewCheckerDefaultConfig())
	assert.NoError(t, err)
	err = checker.Run(t.Context())
	assert.ErrorContains(t, err, "checksum mismatch")
}

func TestBoundaryCases(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS checkert1, _checkert1_new, _checkert1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE checkert1 (a INT NOT NULL, b FLOAT, c VARCHAR(255), PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _checkert1_new (a INT NOT NULL, b FLOAT, c VARCHAR(255), PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _checkert1_chkpnt (a INT NOT NULL)")
	testutils.RunSQL(t, "INSERT INTO checkert1 VALUES (1, 2.2, '')")        // null vs empty string
	testutils.RunSQL(t, "INSERT INTO _checkert1_new VALUES (1, 2.2, NULL)") // should not compare

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()

	t1 := table.NewTableInfo(db, "test", "checkert1")
	assert.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_checkert1_new")
	assert.NoError(t, t2.SetInfo(t.Context()))
	logger := slog.Default()

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	feed := repl.NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, &repl.ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        repl.NewServerID(),
	})
	defer feed.Close()
	assert.NoError(t, feed.AddSubscription(t1, t2, nil))
	assert.NoError(t, feed.Run(t.Context()))

	chunker, err := table.NewChunker(t1, t2, 0, slog.Default())
	assert.NoError(t, err)
	assert.NoError(t, chunker.Open())

	checker, err := NewChecker(db, chunker, feed, NewCheckerDefaultConfig())
	assert.NoError(t, err)
	assert.Error(t, checker.Run(t.Context()))

	// UPDATE t1 to also be NULL
	testutils.RunSQL(t, "UPDATE checkert1 SET c = NULL")
	checker, err = NewChecker(db, chunker, feed, NewCheckerDefaultConfig())
	assert.NoError(t, err)
	assert.NoError(t, checker.Run(t.Context()))
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
	assert.NoError(t, err)
	defer db.Close()

	t1 := table.NewTableInfo(db, "test", "tdatetime")
	assert.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_tdatetime_new")
	assert.NoError(t, t2.SetInfo(t.Context())) // fails
	logger := slog.Default()

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	feed := repl.NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, &repl.ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        repl.NewServerID(),
	})
	defer feed.Close()
	assert.NoError(t, feed.AddSubscription(t1, t2, nil))
	assert.NoError(t, feed.Run(t.Context()))

	chunker, err := table.NewChunker(t1, t2, 0, slog.Default())
	assert.NoError(t, err)
	assert.NoError(t, chunker.Open())

	checker, err := NewChecker(db, chunker, feed, NewCheckerDefaultConfig())
	assert.NoError(t, err)
	assert.NoError(t, checker.Run(t.Context())) // fails
}

func TestFromWatermark(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS tfromwatermark, _tfromwatermark_new, _tfromwatermark_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE tfromwatermark (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _tfromwatermark_new (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "INSERT INTO tfromwatermark VALUES (1, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO _tfromwatermark_new VALUES (1, 2, 3)")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()

	t1 := table.NewTableInfo(db, "test", "tfromwatermark")
	assert.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_tfromwatermark_new")
	assert.NoError(t, t2.SetInfo(t.Context()))
	logger := slog.Default()

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	feed := repl.NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, &repl.ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        repl.NewServerID(),
	})
	defer feed.Close()
	assert.NoError(t, feed.AddSubscription(t1, t2, nil))
	assert.NoError(t, feed.Run(t.Context()))

	chunker, err := table.NewChunker(t1, t2, 0, slog.Default())
	assert.NoError(t, err)
	assert.NoError(t, chunker.Open())

	config := NewCheckerDefaultConfig()
	config.Watermark = "{\"Key\":[\"a\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\": [\"2\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"3\"],\"Inclusive\":false}}"
	checker, err := NewChecker(db, chunker, feed, config)
	assert.NoError(t, err)
	assert.NoError(t, checker.Run(t.Context()))
}

func TestFixCorruptWithWriter(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	newDBName := testutils.CreateUniqueTestDatabase(t)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS corruptt1")
	testutils.RunSQL(t, "CREATE TABLE corruptt1 (a INT NOT NULL , b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "INSERT INTO corruptt1 VALUES (1, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO corruptt1 VALUES (2, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO corruptt1 VALUES (3, 2, 3)")

	testutils.RunSQL(t, "CREATE TABLE "+newDBName+".corruptt1 (a INT NOT NULL , b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "INSERT INTO "+newDBName+".corruptt1 VALUES (1, 2, 3)")
	// row 2 is missing
	testutils.RunSQL(t, "INSERT INTO "+newDBName+".corruptt1 VALUES (3, 9, 9)")

	destDB := cfg.Clone()
	destDB.DBName = newDBName

	src, err := dbconn.New(cfg.FormatDSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer src.Close()
	dest, err := dbconn.New(destDB.FormatDSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer dest.Close()

	t1 := table.NewTableInfo(src, "test", "corruptt1")
	assert.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(dest, newDBName, "corruptt1")
	assert.NoError(t, t2.SetInfo(t.Context()))
	logger := slog.Default()

	feed := repl.NewClient(src, cfg.Addr, cfg.User, cfg.Passwd, &repl.ClientConfig{
		Logger:                     logger,
		Concurrency:                4,
		TargetBatchTime:            time.Second,
		ServerID:                   repl.NewServerID(),
		UseExperimentalBufferedMap: true,
		WriteDB:                    dest,
	})
	defer feed.Close()
	assert.NoError(t, feed.AddSubscription(t1, t2, nil))
	assert.NoError(t, feed.Run(t.Context()))

	chunker, err := table.NewChunker(t1, t2, 0, slog.Default())
	assert.NoError(t, err)
	assert.NoError(t, chunker.Open())

	config := NewCheckerDefaultConfig()
	config.WriteDB = dest // there is a write DB.
	config.FixDifferences = true

	checker, err := NewChecker(src, chunker, feed, config)
	assert.Equal(t, "0/3 0.00%", checker.GetProgress())
	assert.NoError(t, err)
	assert.NoError(t, checker.Run(t.Context())) // should be fixed!
	assert.Equal(t, "3/3 100.00%", checker.GetProgress())
}
