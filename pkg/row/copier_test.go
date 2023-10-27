package row

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cashapp/spirit/pkg/dbconn"
	"github.com/cashapp/spirit/pkg/metrics"
	"github.com/cashapp/spirit/pkg/testutils"

	"github.com/cashapp/spirit/pkg/table"
	"github.com/cashapp/spirit/pkg/throttler"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestMetricsSink struct {
	sync.Mutex
	called int
}

func (t *TestMetricsSink) Send(ctx context.Context, m *metrics.Metrics) error {
	t.Lock()
	defer t.Unlock()
	t.called += 1
	return nil
}

func TestCopier(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS copiert1, copiert2")
	testutils.RunSQL(t, "CREATE TABLE copiert1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE copiert2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "INSERT INTO copiert1 VALUES (1, 2, 3)")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	require.Equal(t, 0, db.Stats().InUse) // no connections in use.

	t1 := table.NewTableInfo(db, "test", "copiert1")
	assert.NoError(t, t1.SetInfo(context.TODO()))
	t2 := table.NewTableInfo(db, "test", "copiert2")
	assert.NoError(t, t2.SetInfo(context.TODO()))

	copierConfig := NewCopierDefaultConfig()
	testMetricsSink := &TestMetricsSink{}
	copierConfig.MetricsSink = testMetricsSink
	copier, err := NewCopier(db, t1, t2, copierConfig)
	assert.NoError(t, err)
	assert.NoError(t, copier.Run(context.Background())) // works

	// Verify that t2 has one row.
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM copiert2").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	// Verify that testMetricsSink.Send was called >0 times
	// It will be 1 with the composite chunker, 3 with optimistic.
	assert.True(t, testMetricsSink.called > 0)
	require.Equal(t, 0, db.Stats().InUse) // no connections in use.
}

func TestThrottler(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS throttlert1, throttlert2")
	testutils.RunSQL(t, "CREATE TABLE throttlert1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE throttlert2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "INSERT INTO throttlert1 VALUES (1, 2, 3)")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)

	t1 := table.NewTableInfo(db, "test", "throttlert1")
	assert.NoError(t, t1.SetInfo(context.TODO()))
	t2 := table.NewTableInfo(db, "test", "throttlert2")
	assert.NoError(t, t2.SetInfo(context.TODO()))

	copier, err := NewCopier(db, t1, t2, NewCopierDefaultConfig())
	assert.NoError(t, err)
	copier.SetThrottler(&throttler.Noop{})
	assert.NoError(t, copier.Run(context.Background())) // works

	// Verify that t2 has one row.
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM throttlert2").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestCopierUniqueDestination(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS copieruniqt1, copieruniqt2")
	testutils.RunSQL(t, "CREATE TABLE copieruniqt1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE copieruniqt2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a), UNIQUE(b))")
	testutils.RunSQL(t, "INSERT INTO copieruniqt1 VALUES (1, 2, 3), (2,2,3)")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	require.Equal(t, 0, db.Stats().InUse) // no connections in use.

	t1 := table.NewTableInfo(db, "test", "copieruniqt1")
	assert.NoError(t, t1.SetInfo(context.TODO()))
	t2 := table.NewTableInfo(db, "test", "copieruniqt2")
	assert.NoError(t, t2.SetInfo(context.TODO()))

	// if the checksum is FALSE, the unique violation will cause an error.
	cfg := NewCopierDefaultConfig()
	cfg.FinalChecksum = false
	copier, err := NewCopier(db, t1, t2, cfg)
	assert.NoError(t, err)
	assert.Error(t, copier.Run(context.Background())) // fails

	// however, if the checksum is TRUE, the unique violation will be ignored.
	// This is because it's not possible to differentiate between a resume from checkpoint
	// causing a duplicate key, and the DDL being applied causing it.
	t1 = table.NewTableInfo(db, "test", "copieruniqt1")
	assert.NoError(t, t1.SetInfo(context.TODO()))
	t2 = table.NewTableInfo(db, "test", "copieruniqt2")
	assert.NoError(t, t2.SetInfo(context.TODO()))
	copier, err = NewCopier(db, t1, t2, NewCopierDefaultConfig())
	assert.NoError(t, err)
	assert.NoError(t, copier.Run(context.Background())) // works
	require.Equal(t, 0, db.Stats().InUse)               // no connections in use.
}

func TestCopierLossyDataTypeConversion(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS datatpt1, datatpt2")
	testutils.RunSQL(t, "CREATE TABLE datatpt1 (a INT NOT NULL, b INT, c VARCHAR(255), PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE datatpt2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "INSERT INTO datatpt1 VALUES (1, 2, 'aaa'), (2,2,'bbb')")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	require.Equal(t, 0, db.Stats().InUse) // no connections in use.

	t1 := table.NewTableInfo(db, "test", "datatpt1")
	assert.NoError(t, t1.SetInfo(context.TODO()))
	t2 := table.NewTableInfo(db, "test", "datatpt2")
	assert.NoError(t, t2.SetInfo(context.TODO()))

	// Checksum flag does not affect this error.
	copier, err := NewCopier(db, t1, t2, NewCopierDefaultConfig())
	assert.NoError(t, err)
	err = copier.Run(context.Background())
	assert.Contains(t, err.Error(), "unsafe warning migrating chunk")
	require.Equal(t, 0, db.Stats().InUse) // no connections in use.
}

func TestCopierNullToNotNullConversion(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS null2notnullt1, null2notnullt2")
	testutils.RunSQL(t, "CREATE TABLE null2notnullt1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE null2notnullt2 (a INT NOT NULL, b INT, c INT NOT NULL, PRIMARY KEY (a))")
	testutils.RunSQL(t, "INSERT INTO null2notnullt1 VALUES (1, 2, 123), (2,2,NULL)")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	require.Equal(t, 0, db.Stats().InUse) // no connections in use.

	t1 := table.NewTableInfo(db, "test", "null2notnullt1")
	assert.NoError(t, t1.SetInfo(context.TODO()))
	t2 := table.NewTableInfo(db, "test", "null2notnullt2")
	assert.NoError(t, t2.SetInfo(context.TODO()))

	// Checksum flag does not affect this error.
	copier, err := NewCopier(db, t1, t2, NewCopierDefaultConfig())
	assert.NoError(t, err)
	err = copier.Run(context.Background())
	assert.Contains(t, err.Error(), "unsafe warning migrating chunk")
	require.Equal(t, 0, db.Stats().InUse) // no connections in use.
}

func TestSQLModeAllowZeroInvalidDates(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS invaliddt1, invaliddt2")
	testutils.RunSQL(t, "CREATE TABLE invaliddt1 (a INT NOT NULL, b INT, c DATETIME, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE invaliddt2 (a INT NOT NULL, b INT, c DATETIME, PRIMARY KEY (a))")
	testutils.RunSQL(t, "INSERT IGNORE INTO invaliddt1 VALUES (1, 2, '0000-00-00')")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)

	t1 := table.NewTableInfo(db, "test", "invaliddt1")
	assert.NoError(t, t1.SetInfo(context.TODO()))
	t2 := table.NewTableInfo(db, "test", "invaliddt2")
	assert.NoError(t, t2.SetInfo(context.TODO()))

	// Checksum flag does not affect this error.
	copier, err := NewCopier(db, t1, t2, NewCopierDefaultConfig())
	assert.NoError(t, err)
	err = copier.Run(context.Background())
	assert.NoError(t, err)
	// Verify that t2 has one row.
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM invaliddt2").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestLockWaitTimeoutIsRetyable(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS lockt1, lockt2")
	testutils.RunSQL(t, "CREATE TABLE lockt1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE lockt2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "INSERT IGNORE INTO lockt1 VALUES (1, 2, 3)")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)

	t1 := table.NewTableInfo(db, "test", "lockt1")
	assert.NoError(t, t1.SetInfo(context.TODO()))
	t2 := table.NewTableInfo(db, "test", "lockt2")
	assert.NoError(t, t2.SetInfo(context.TODO()))

	// Lock table t2 for 2 seconds.
	// This should be enough to retry, but it will eventually be successful.
	go func() {
		tx, err := db.Begin()
		assert.NoError(t, err)
		_, err = tx.Exec("SELECT * FROM lockt2 WHERE a = 1 FOR UPDATE")
		assert.NoError(t, err)
		time.Sleep(2 * time.Second)
		err = tx.Rollback()
		assert.NoError(t, err)
	}()
	copier, err := NewCopier(db, t1, t2, NewCopierDefaultConfig())
	assert.NoError(t, err)
	err = copier.Run(context.Background())
	assert.NoError(t, err) // succeeded within retry.
}

func TestLockWaitTimeoutRetryExceeded(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS lock2t1, lock2t2")
	testutils.RunSQL(t, "CREATE TABLE lock2t1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE lock2t2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "INSERT INTO lock2t1 VALUES (1, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO lock2t2 VALUES (1, 2, 3)")

	config := dbconn.NewDBConfig()
	config.MaxRetries = 2
	config.InnodbLockWaitTimeout = 1

	db, err := dbconn.New(testutils.DSN(), config)
	assert.NoError(t, err)

	require.Equal(t, config.MaxOpenConnections, db.Stats().MaxOpenConnections)
	require.Equal(t, 0, db.Stats().InUse)

	t1 := table.NewTableInfo(db, "test", "lock2t1")
	assert.NoError(t, t1.SetInfo(context.TODO()))
	t2 := table.NewTableInfo(db, "test", "lock2t2")
	assert.NoError(t, t2.SetInfo(context.TODO()))

	// Lock again but for 60 seconds.
	// This will cause a failure because the retry is less than this (2 retries * 1 sec + backoff)
	var wg sync.WaitGroup
	wg.Add(1) // wait for the goroutine to acquire the lock
	go func() {
		tx, err := db.Begin()
		assert.NoError(t, err)
		_, err = tx.Exec("SELECT * FROM lock2t2 WHERE a = 1 FOR UPDATE")
		assert.NoError(t, err)
		wg.Done()
		time.Sleep(60 * time.Second)
		err = tx.Rollback()
		assert.NoError(t, err)
	}()

	wg.Wait() // Wait only for the lock to be acquired.
	copier, err := NewCopier(db, t1, t2, NewCopierDefaultConfig())
	assert.NoError(t, err)
	err = copier.Run(context.Background())
	assert.Error(t, err) // exceeded retry.
}

func TestCopierValidation(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)

	t1 := table.NewTableInfo(db, "test", "t1")

	// if the checksum is FALSE, the unique violation will cause an error.
	_, err = NewCopier(db, t1, nil, NewCopierDefaultConfig())
	assert.Error(t, err)
}

func TestETA(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS testeta1, testeta2, _testeta1_new, _testeta2_new")
	testutils.RunSQL(t, "CREATE TABLE testeta1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE testeta2 (a INT NOT NULL auto_increment, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _testeta1_new (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _testeta2_new (a INT NOT NULL auto_increment, b INT, c INT, PRIMARY KEY (a))")
	// high max value
	testutils.RunSQL(t, "INSERT IGNORE INTO testeta2 VALUES (10000, 2, 3)")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)

	t1 := table.NewTableInfo(db, "test", "testeta1")
	assert.NoError(t, t1.SetInfo(context.TODO()))
	t2 := table.NewTableInfo(db, "test", "testeta2")
	assert.NoError(t, t2.SetInfo(context.TODO()))
	t1new := table.NewTableInfo(db, "test", "_testeta1_new")
	assert.NoError(t, t1new.SetInfo(context.TODO()))
	t2new := table.NewTableInfo(db, "test", "_testeta2_new")
	assert.NoError(t, t2new.SetInfo(context.TODO()))

	t1.EstimatedRows = 1000
	t2.EstimatedRows = 1000

	copier1, err := NewCopier(db, t1, t1new, NewCopierDefaultConfig())
	assert.NoError(t, err)
	copier2, err := NewCopier(db, t2, t2new, NewCopierDefaultConfig())
	assert.NoError(t, err)

	// set the start time to -copyETAInitialWaitTime ago so the ETAs will show.
	copier1.CopyRowsStartTime = time.Now().Add(-time.Hour)
	copier2.CopyRowsStartTime = time.Now().Add(-time.Hour)

	// Ask for the ETA, it should be "TBD" because the perSecond estimate is not set yet.
	assert.Equal(t, "TBD", copier1.GetETA())
	assert.Equal(t, "TBD", copier2.GetETA())
	assert.Equal(t, "0/1000 0.00%", copier1.GetProgress())
	assert.Equal(t, "0/10000 0.00%", copier2.GetProgress())

	// Imply we copied 90 rows (in a chunk of 100)
	copier1.CopyRowsLogicalCount = 100
	copier1.CopyRowsCount = 90
	copier2.CopyRowsLogicalCount = 100
	copier2.CopyRowsCount = 90

	copied, estimated, pct := copier1.getCopyStats()
	assert.Equal(t, uint64(90), copied)
	assert.Equal(t, uint64(1000), estimated)
	assert.Equal(t, float64(9), pct)

	copied, estimated, pct = copier2.getCopyStats()
	assert.Equal(t, uint64(100), copied)
	assert.Equal(t, uint64(10000), estimated)
	assert.Equal(t, float64(1), pct) // 1%

	copier1.rowsPerSecond = 10
	copier2.rowsPerSecond = 10

	assert.Equal(t, "1m31s", copier1.GetETA())
	assert.Equal(t, "16m30s", copier2.GetETA())
	assert.Equal(t, "90/1000 9.00%", copier1.GetProgress())
	assert.Equal(t, "100/10000 1.00%", copier2.GetProgress())
}

func TestCopierFromCheckpoint(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS copierchkpt1, _copierchkpt1_new")
	testutils.RunSQL(t, "CREATE TABLE copierchkpt1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _copierchkpt1_new (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "INSERT INTO copierchkpt1 VALUES (1, 2, 3), (2, 3, 4), (3, 4, 5), (4, 5, 6), (5, 6, 7), (6, 7, 8), (7, 8, 9), (8, 9, 10), (9, 10, 11), (10, 11, 12)")
	testutils.RunSQL(t, "INSERT INTO _copierchkpt1_new VALUES (1, 2, 3),(2,3,4),(3,4,5)") // 1-3 row is already copied

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)

	t1 := table.NewTableInfo(db, "test", "copierchkpt1")
	assert.NoError(t, t1.SetInfo(context.TODO()))
	t1new := table.NewTableInfo(db, "test", "_copierchkpt1_new")
	assert.NoError(t, t1new.SetInfo(context.TODO()))

	lowWatermark := `{"Key":["a"],"ChunkSize":1,"LowerBound":{"Value":["3"],"Inclusive":true},"UpperBound":{"Value":["4"],"Inclusive":false}}`
	copier, err := NewCopierFromCheckpoint(db, t1, t1new, NewCopierDefaultConfig(), lowWatermark, 3, 3)
	assert.NoError(t, err)
	assert.NoError(t, copier.Run(context.Background())) // works

	// Verify that t1new has 10 rows
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM _copierchkpt1_new").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 10, count)
}
