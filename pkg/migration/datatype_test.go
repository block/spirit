package migration

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// TestChangeDatatypeNoData tests that a lossy datatype change succeeds when the table is empty.
func TestChangeDatatypeNoData(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "cdatatypemytable", `CREATE TABLE cdatatypemytable (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)

	m := NewTestRunner(t, "cdatatypemytable", "CHANGE b b INT") //nolint: dupword
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.NoError(t, m.Close())
}

// TestChangeDatatypeDataLoss tests that a lossy datatype change fails when data would be truncated.
func TestChangeDatatypeDataLoss(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "cdatalossmytable", `CREATE TABLE cdatalossmytable (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	testutils.RunSQL(t, "INSERT INTO cdatalossmytable (name, b) VALUES ('a', 'b')")

	m := NewTestRunner(t, "cdatalossmytable", "CHANGE b b INT") //nolint: dupword
	require.Error(t, m.Run(t.Context()))                        // value 'b' cannot convert cleanly to int
	require.NoError(t, m.Close())
}

// TestChangeDatatypeLossyNoAutoInc is a good test of how much the chunker will
// boil the ocean:
//   - There is a MIN(key)=1 and a MAX(key)=8589934592
//   - There is no auto-increment so the chunker is allowed to expand each chunk
//     based on estimated rows (which is low).
//
// Only the key=8589934592 will fail to be converted. The generated number of
// chunks should be very low because of prefetching.
func TestChangeDatatypeLossyNoAutoInc(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "lossychange2", `CREATE TABLE lossychange2 (
		id BIGINT NOT NULL,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	testutils.RunSQL(t, "INSERT INTO lossychange2 (id, name, b) VALUES (1, 'a', REPEAT('a', 200))")
	testutils.RunSQL(t, "INSERT INTO lossychange2 (id, name, b) VALUES (8589934592, 'a', REPEAT('a', 200))") // exceeds INT range

	m := NewTestRunner(t, "lossychange2", "CHANGE COLUMN id id INT NOT NULL auto_increment") //nolint: dupword
	err := m.Run(t.Context())
	require.Error(t, err)
	require.ErrorContains(t, err, "Out of range value") // Error 1264
	// Check that the chunker processed fewer than 500 chunks (fail-early optimization)
	_, chunksCopied, _ := m.copier.GetChunker().Progress()
	require.Less(t, chunksCopied, uint64(500))
	require.NoError(t, m.Close())
}

// TestChangeDatatypeLossless tests a lossy datatype change that succeeds because
// the actual stored data fits within the target type's constraints.
func TestChangeDatatypeLossless(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "lossychange3", `CREATE TABLE lossychange3 (
		id BIGINT NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NULL,
		PRIMARY KEY (id)
	)`)
	testutils.RunSQL(t, "INSERT INTO lossychange3 (name, b) VALUES ('a', REPEAT('a', 200))")
	testutils.RunSQL(t, "INSERT INTO lossychange3 (id, name, b) VALUES (8589934592, 'a', REPEAT('a', 200))")

	m := NewTestRunner(t, "lossychange3", "CHANGE COLUMN b b varchar(200) NOT NULL") //nolint: dupword
	err := m.Run(t.Context())
	require.NoError(t, err) // works because all stored data fits in varchar(200)
	// Check that the chunker processed fewer than 500 chunks
	_, chunksCopied, _ := m.copier.GetChunker().Progress()
	require.Less(t, chunksCopied, uint64(500))
	require.NoError(t, m.Close())
}

// TestChangeDatatypeLossyFailEarly tests that a lossy change fails quickly when
// the first row violates the constraint (NULL → NOT NULL).
func TestChangeDatatypeLossyFailEarly(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "lossychange4", `CREATE TABLE lossychange4 (
		id BIGINT NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NULL,
		PRIMARY KEY (id)
	)`)
	testutils.RunSQL(t, "INSERT INTO lossychange4 (name) VALUES ('a')")                                      // b is NULL
	testutils.RunSQL(t, "INSERT INTO lossychange4 (name, b) VALUES ('a', REPEAT('a', 200))")                 // b has data
	testutils.RunSQL(t, "INSERT INTO lossychange4 (id, name, b) VALUES (8589934592, 'a', REPEAT('a', 200))") // far-off ID

	m := NewTestRunner(t, "lossychange4", "CHANGE COLUMN b b varchar(255) NOT NULL") //nolint: dupword
	err := m.Run(t.Context())
	require.Error(t, err) // row 1 has NULL in b
	require.NoError(t, m.Close())
}

// TestNullToNotNull tests that converting a NULL column to NOT NULL fails when NULL data exists.
func TestNullToNotNull(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "autodatetime", `CREATE TABLE autodatetime (
		id INT NOT NULL AUTO_INCREMENT,
		created_at DATETIME(3) NULL,
		PRIMARY KEY (id)
	)`)
	testutils.RunSQL(t, `INSERT INTO autodatetime (created_at) VALUES (NULL)`)

	m := NewTestRunner(t, "autodatetime", "modify column created_at datetime(3) not null default current_timestamp(3)")
	err := m.Run(t.Context())
	require.Error(t, err)
	require.ErrorContains(t, err, "Column 'created_at' cannot be null")
	require.NoError(t, m.Close())
}

// TestTpConversion tests timestamp precision conversion and varchar→int type changes.
func TestTpConversion(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "tpconvert", `CREATE TABLE tpconvert (
		id bigint NOT NULL AUTO_INCREMENT primary key,
		created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		issued_at timestamp NULL DEFAULT NULL,
		activated_at timestamp NULL DEFAULT NULL,
		deactivated_at timestamp NULL DEFAULT NULL,
		intasstring varchar(255) NULL DEFAULT NULL,
		floatcol FLOAT NULL DEFAULT NULL
	)`)
	testutils.RunSQL(t, `INSERT INTO tpconvert (created_at, updated_at, issued_at, activated_at, deactivated_at, intasstring, floatcol) VALUES
	('2023-05-18 09:28:46', '2023-05-18 09:33:27', '2023-05-18 09:28:45', '2023-05-18 09:28:45', NULL, '0001', 9.3),
	('2023-05-18 09:34:38', '2023-05-24 07:38:25', '2023-05-18 09:34:37', '2023-05-18 09:34:37', '2023-05-24 07:38:25', '10', 9.3),
	('2023-05-24 07:34:36', '2023-05-24 07:34:36', '2023-05-24 07:34:35', NULL, null, '01234', 9.3),
	('2023-05-24 07:41:05', '2023-05-25 06:15:37', '2023-05-24 07:41:04', '2023-05-24 07:41:04', '2023-05-25 06:15:37', '10', 2.2),
	('2023-05-25 06:17:30', '2023-05-25 06:17:30', '2023-05-25 06:17:29', '2023-05-25 06:17:29', NULL, '10', 9.3),
	('2023-05-25 06:18:33', '2023-05-25 06:41:13', '2023-05-25 06:18:32', '2023-05-25 06:18:32', '2023-05-25 06:41:13', '10', 1.1),
	('2023-05-25 06:24:23', '2023-05-25 06:24:23', '2023-05-25 06:24:22', NULL, null, '10', 9.3),
	('2023-05-25 06:41:35', '2023-05-28 23:45:09', '2023-05-25 06:41:34', '2023-05-25 06:41:34', '2023-05-28 23:45:09', '10', 9.3),
	('2023-05-25 06:44:41', '2023-05-28 23:45:03', '2023-05-25 06:44:40', '2023-05-25 06:46:48', '2023-05-28 23:45:03', '10', 9.3),
	('2023-05-26 06:24:24', '2023-05-28 23:45:01', '2023-05-26 06:24:23', '2023-05-26 06:24:42', '2023-05-28 23:45:01', '10', 9.3),
	('2023-05-28 23:46:07', '2023-05-29 00:57:55', '2023-05-28 23:46:05', '2023-05-28 23:46:05', NULL, '10', 9.3),
	('2023-05-28 23:53:34', '2023-05-29 00:57:56', '2023-05-28 23:53:33', '2023-05-28 23:58:09', NULL, '10', 9.3)`)

	m := NewTestRunner(t, "tpconvert", `MODIFY COLUMN created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
		MODIFY COLUMN updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
		MODIFY COLUMN issued_at TIMESTAMP(6) NULL,
		MODIFY COLUMN activated_at TIMESTAMP(6) NULL,
		MODIFY COLUMN deactivated_at TIMESTAMP(6) NULL,
		MODIFY COLUMN intasstring INT NULL DEFAULT NULL`)
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())
}

// TestEnumReorder tests that reordering ENUM values in an ALTER TABLE
// produces correct data after migration.
//
// This test only works correctly in unbuffered mode because of the way
// ENUM values are represented in the binlog. We test *both* unbuffered and buffered modes
// though and we accept a pre-flight failure as a "pass", since it's not corruption.
// i.e. it's OK to refuse changes you can't handle.
//
// The unbuffered path uses REPLACE INTO ... SELECT (SQL-level string operations) which
// handles ENUM reordering correctly. The buffered path uses UpsertRows with raw binlog
// values, where ENUM values are represented as int64 ordinals. If the ENUM is reordered,
// the ordinals map to different string values in the target table, causing data corruption.
//
// This test exercises both the copier path (initial data) and the binlog
// replay path (concurrent DML during migration) to verify correctness.
func TestEnumReorder(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) {
		testEnumReorder(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		testEnumReorder(t, true)
	})
}

func testEnumReorder(t *testing.T, enableBuffered bool) {
	tt := testutils.NewTestTable(t, "enumreorder", `CREATE TABLE enumreorder (
		id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
		status ENUM('active', 'inactive', 'pending') NOT NULL
	)`)
	// Seed with all three ENUM values so post-migration assertions don't depend
	// on best-effort concurrent DML succeeding.
	tt.SeedRows(t, "INSERT INTO enumreorder (status) SELECT 'active'", 5998)
	_, err := tt.DB.ExecContext(t.Context(), "INSERT INTO enumreorder (status) VALUES ('inactive'), ('pending')")
	require.NoError(t, err)

	m := NewTestRunner(t, "enumreorder", "MODIFY COLUMN status ENUM('pending', 'active', 'inactive') NOT NULL",
		WithThreads(1),
		WithTargetChunkTime(100*time.Millisecond),
		WithBuffered(enableBuffered),
		WithTestThrottler())

	// Concurrent DML during copy phase to exercise binlog replay.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	dmlDone := make(chan struct{})
	go func() {
		defer close(dmlDone)
		for m.status.Get() < status.CopyRows {
			time.Sleep(time.Millisecond)
			if ctx.Err() != nil {
				return
			}
		}
		for i := range 50 {
			if ctx.Err() != nil {
				return
			}
			_, _ = tt.DB.ExecContext(ctx, `INSERT INTO enumreorder (status) VALUES ('active')`)
			_, _ = tt.DB.ExecContext(ctx, `INSERT INTO enumreorder (status) VALUES ('inactive')`)
			_, _ = tt.DB.ExecContext(ctx, `INSERT INTO enumreorder (status) VALUES ('pending')`)
			_, _ = tt.DB.ExecContext(ctx, fmt.Sprintf(`UPDATE enumreorder SET status = 'active' WHERE id = %d`, i*3+1))
			_, _ = tt.DB.ExecContext(ctx, fmt.Sprintf(`UPDATE enumreorder SET status = 'inactive' WHERE id = %d`, i*3+2))
			_, _ = tt.DB.ExecContext(ctx, fmt.Sprintf(`UPDATE enumreorder SET status = 'pending' WHERE id = %d`, i*3+3))
		}
	}()

	migrationErr := m.Run(ctx)
	cancel()
	<-dmlDone
	require.NoError(t, m.Close())

	if enableBuffered {
		require.Error(t, migrationErr)
		require.ErrorContains(t, migrationErr, "unsafe ENUM value reorder")
		return
	}

	// Unbuffered mode: migration should succeed and data should be correct.
	require.NoError(t, migrationErr)

	// Verify that every row has a valid ENUM string value.
	var activeCount, inactiveCount, pendingCount int
	err = tt.DB.QueryRowContext(t.Context(), `SELECT
		SUM(status = 'active'),
		SUM(status = 'inactive'),
		SUM(status = 'pending')
		FROM enumreorder`).Scan(&activeCount, &inactiveCount, &pendingCount)
	require.NoError(t, err)

	var totalCount int
	err = tt.DB.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM enumreorder`).Scan(&totalCount)
	require.NoError(t, err)

	require.Equal(t, totalCount, activeCount+inactiveCount+pendingCount,
		"all rows should have a valid ENUM value (no empty strings from ordinal corruption)")
	require.Greater(t, activeCount, 0, "should have 'active' rows")
	require.Greater(t, inactiveCount, 0, "should have 'inactive' rows")
	require.Greater(t, pendingCount, 0, "should have 'pending' rows")
}

// TestSetReorder mirrors TestEnumReorder but for SET columns.
// Both buffered and unbuffered modes refuse SET reordering because the string
// representation changes cause checksum failures.
func TestSetReorder(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) {
		testSetReorder(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		testSetReorder(t, true)
	})
}

func testSetReorder(t *testing.T, enableBuffered bool) {
	tt := testutils.NewTestTable(t, "setreorder", `CREATE TABLE setreorder (
		id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
		perms SET('read', 'write', 'execute') NOT NULL
	)`)
	tt.SeedRows(t, "INSERT INTO setreorder (perms) SELECT 'read,write'", 7000)

	m := NewTestRunner(t, "setreorder", "MODIFY COLUMN perms SET('execute', 'read', 'write') NOT NULL",
		WithThreads(1),
		WithTargetChunkTime(100*time.Millisecond),
		WithBuffered(enableBuffered),
		WithTestThrottler())

	// Concurrent DML during copy phase.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	dmlDone := make(chan struct{})
	go func() {
		defer close(dmlDone)
		for m.status.Get() < status.CopyRows {
			time.Sleep(time.Millisecond)
			if ctx.Err() != nil {
				return
			}
		}
		for i := range 50 {
			if ctx.Err() != nil {
				return
			}
			_, _ = tt.DB.ExecContext(ctx, `INSERT INTO setreorder (perms) VALUES ('read')`)
			_, _ = tt.DB.ExecContext(ctx, `INSERT INTO setreorder (perms) VALUES ('write,execute')`)
			_, _ = tt.DB.ExecContext(ctx, `INSERT INTO setreorder (perms) VALUES ('read,write,execute')`)
			_, _ = tt.DB.ExecContext(ctx, fmt.Sprintf(`UPDATE setreorder SET perms = 'read,write' WHERE id = %d`, i*3+1))
			_, _ = tt.DB.ExecContext(ctx, fmt.Sprintf(`UPDATE setreorder SET perms = 'execute' WHERE id = %d`, i*3+2))
			_, _ = tt.DB.ExecContext(ctx, fmt.Sprintf(`UPDATE setreorder SET perms = 'read,write,execute' WHERE id = %d`, i*3+3))
		}
	}()

	migrationErr := m.Run(ctx)
	cancel()
	<-dmlDone
	require.NoError(t, m.Close())

	require.Error(t, migrationErr)
	require.ErrorContains(t, migrationErr, "unsafe SET value reorder")
}

// TestBufferedMigrationFailsGracefullyWithMinimalRBR verifies that a buffered
// migration fails gracefully when it receives minimal RBR events from a rogue session.
func TestBufferedMigrationFailsGracefullyWithMinimalRBR(t *testing.T) {
	t.Parallel()

	tt := testutils.NewTestTable(t, "minrbr_buffered", `CREATE TABLE minrbr_buffered (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		val INT NOT NULL DEFAULT 0
	)`)
	tt.SeedRows(t, "INSERT INTO minrbr_buffered (name, val) SELECT CONCAT('seed-', FLOOR(RAND()*99999)), 1", 256)

	// Open a dedicated connection with session-level minimal RBR.
	minimalDB, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(minimalDB)
	minimalDB.SetMaxOpenConns(1)
	_, err = minimalDB.ExecContext(t.Context(), "SET binlog_row_image = 'MINIMAL'")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	m := NewTestRunner(t, "minrbr_buffered", "ENGINE=InnoDB",
		WithThreads(1),
		WithTargetChunkTime(100*time.Millisecond),
		WithBuffered(true),
		WithTestThrottler())

	// Run the migration in a goroutine so we can inject minimal-RBR writes.
	var migrationErr error
	migrationDone := make(chan struct{})
	go func() {
		defer close(migrationDone)
		migrationErr = m.Run(ctx)
	}()

	waitForStatus(t, m, status.CopyRows)

	// Continuously write using the minimal-RBR session during the copy phase.
	var writerWg sync.WaitGroup
	writerWg.Add(1)
	go func() {
		defer writerWg.Done()
		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				_, _ = minimalDB.ExecContext(ctx, `UPDATE minrbr_buffered SET val = val + 1 WHERE id = ?`, (i%100)+1)
			}
		}
	}()

	<-migrationDone
	cancel()
	writerWg.Wait()
	require.NoError(t, m.Close())

	require.Error(t, migrationErr)
}

// --- Primary Key Datatype Change Tests (issue #360) ---
// Spirit supports changing the datatype on a primary key column (e.g. INT→BIGINT)
// as long as the primary key itself doesn't change (no ADD/DROP PRIMARY KEY).

func TestAlterPKIntToBigInt(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) { testAlterPKIntToBigInt(t, false) })
	t.Run("buffered", func(t *testing.T) {
		testAlterPKIntToBigInt(t, true)
	})
}

func testAlterPKIntToBigInt(t *testing.T, enableBuffered bool) {
	tt := testutils.NewTestTable(t, "altpk_int2big", `CREATE TABLE altpk_int2big (
		id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name varchar(255) NOT NULL,
		val int NOT NULL DEFAULT 0
	)`)
	tt.SeedRows(t, "INSERT INTO altpk_int2big (name, val) SELECT 'a', 1", 3)

	m := NewTestRunner(t, "altpk_int2big", "MODIFY COLUMN id BIGINT NOT NULL AUTO_INCREMENT",
		WithBuffered(enableBuffered))
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.NoError(t, m.Close())

	var count int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM altpk_int2big").Scan(&count))
	require.GreaterOrEqual(t, count, 3)

	var colType string
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		"SELECT COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='altpk_int2big' AND COLUMN_NAME='id'",
	).Scan(&colType))
	require.Equal(t, "bigint", colType)
}

func TestAlterPKIntToBigIntUnsigned(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) { testAlterPKIntToBigIntUnsigned(t, false) })
	t.Run("buffered", func(t *testing.T) {
		testAlterPKIntToBigIntUnsigned(t, true)
	})
}

func testAlterPKIntToBigIntUnsigned(t *testing.T, enableBuffered bool) {
	tt := testutils.NewTestTable(t, "altpk_int2bigu", `CREATE TABLE altpk_int2bigu (
		id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name varchar(255) NOT NULL
	)`)
	tt.SeedRows(t, "INSERT INTO altpk_int2bigu (name) SELECT 'a'", 5)

	m := NewTestRunner(t, "altpk_int2bigu", "MODIFY COLUMN id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT",
		WithBuffered(enableBuffered))
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.NoError(t, m.Close())

	var colType string
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		"SELECT COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='altpk_int2bigu' AND COLUMN_NAME='id'",
	).Scan(&colType))
	require.Equal(t, "bigint unsigned", colType)
}

func TestAlterPKTinyIntToInt(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) { testAlterPKTinyIntToInt(t, false) })
	t.Run("buffered", func(t *testing.T) {
		testAlterPKTinyIntToInt(t, true)
	})
}

func testAlterPKTinyIntToInt(t *testing.T, enableBuffered bool) {
	tt := testutils.NewTestTable(t, "altpk_tiny2int", `CREATE TABLE altpk_tiny2int (
		id tinyint unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
		data varchar(100) NOT NULL
	)`)
	tt.SeedRows(t, "INSERT INTO altpk_tiny2int (data) SELECT 'test'", 50)

	m := NewTestRunner(t, "altpk_tiny2int", "MODIFY COLUMN id INT UNSIGNED NOT NULL AUTO_INCREMENT",
		WithBuffered(enableBuffered))
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.NoError(t, m.Close())

	var colType string
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		"SELECT COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='altpk_tiny2int' AND COLUMN_NAME='id'",
	).Scan(&colType))
	require.Equal(t, "int unsigned", colType)
}

func TestAlterPKIntToBigIntWithDML(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) { testAlterPKIntToBigIntWithDML(t, false) })
	t.Run("buffered", func(t *testing.T) {
		testAlterPKIntToBigIntWithDML(t, true)
	})
}

func testAlterPKIntToBigIntWithDML(t *testing.T, enableBuffered bool) {
	tt := testutils.NewTestTable(t, "altpk_dml", `CREATE TABLE altpk_dml (
		id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name varchar(255) NOT NULL,
		val int NOT NULL DEFAULT 0
	)`)
	tt.SeedRows(t, "INSERT INTO altpk_dml (name, val) SELECT 'seed', 1", 4096)
	m := NewTestRunner(t, "altpk_dml", "MODIFY COLUMN id BIGINT NOT NULL AUTO_INCREMENT",
		WithBuffered(enableBuffered),
		WithTestThrottler())

	var wg sync.WaitGroup
	wg.Go(func() {
		for m.status.Get() < status.CopyRows {
			time.Sleep(time.Millisecond)
		}
		for i := range 100 {
			_, _ = tt.DB.ExecContext(t.Context(), fmt.Sprintf("INSERT INTO altpk_dml (name, val) VALUES ('dml_%d', %d)", i, i))
			_, _ = tt.DB.ExecContext(t.Context(), fmt.Sprintf("UPDATE altpk_dml SET val = val + 1 WHERE id = %d", i+1))
			_, _ = tt.DB.ExecContext(t.Context(), fmt.Sprintf("DELETE FROM altpk_dml WHERE id = %d", i+4000))
		}
	})

	require.NoError(t, m.Run(t.Context()))
	wg.Wait()
	require.NoError(t, m.Close())

	var colType string
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		"SELECT COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='altpk_dml' AND COLUMN_NAME='id'",
	).Scan(&colType))
	require.Equal(t, "bigint", colType)
}

func TestAlterPKCompositeDatatypeChange(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) { testAlterPKCompositeDatatypeChange(t, false) })
	t.Run("buffered", func(t *testing.T) {
		testAlterPKCompositeDatatypeChange(t, true)
	})
}

func testAlterPKCompositeDatatypeChange(t *testing.T, enableBuffered bool) {
	tt := testutils.NewTestTable(t, "altpk_comp", `CREATE TABLE altpk_comp (
		id1 int NOT NULL,
		id2 int NOT NULL,
		data varchar(100) NOT NULL,
		PRIMARY KEY (id1, id2)
	)`)
	// Composite PK needs unique pairs — can't use SeedRows.
	for i := range 200 {
		testutils.RunSQL(t, fmt.Sprintf("INSERT INTO altpk_comp (id1, id2, data) VALUES (%d, %d, 'row%d')", i/10, i%10, i))
	}

	m := NewTestRunner(t, "altpk_comp", "MODIFY COLUMN id1 BIGINT NOT NULL",
		WithBuffered(enableBuffered))
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())

	var colType string
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		"SELECT COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='altpk_comp' AND COLUMN_NAME='id1'",
	).Scan(&colType))
	require.Equal(t, "bigint", colType)

	var count int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM altpk_comp").Scan(&count))
	require.Equal(t, 200, count)
}

func TestAlterPKIntToBigIntWithDMLAndAdditionalColumnChange(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) { testAlterPKIntToBigIntWithDMLAndAdditionalColumnChange(t, false) })
	t.Run("buffered", func(t *testing.T) {
		testAlterPKIntToBigIntWithDMLAndAdditionalColumnChange(t, true)
	})
}

func testAlterPKIntToBigIntWithDMLAndAdditionalColumnChange(t *testing.T, enableBuffered bool) {
	tt := testutils.NewTestTable(t, "altpk_multi", `CREATE TABLE altpk_multi (
		id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name varchar(100) NOT NULL,
		val int NOT NULL DEFAULT 0
	)`)
	tt.SeedRows(t, "INSERT INTO altpk_multi (name, val) SELECT 'seed', 1", 4096)

	m := NewTestRunner(t, "altpk_multi",
		"MODIFY COLUMN id BIGINT NOT NULL AUTO_INCREMENT, MODIFY COLUMN name VARCHAR(255) NOT NULL",
		WithThreads(2),
		WithTargetChunkTime(100*time.Millisecond),
		WithBuffered(enableBuffered),
		WithTestThrottler())

	var wg sync.WaitGroup
	wg.Go(func() {
		for m.status.Get() < status.CopyRows {
			time.Sleep(time.Millisecond)
		}
		for i := range 50 {
			_, _ = tt.DB.ExecContext(t.Context(), fmt.Sprintf("INSERT INTO altpk_multi (name, val) VALUES ('dml_%d', %d)", i, i))
			_, _ = tt.DB.ExecContext(t.Context(), fmt.Sprintf("UPDATE altpk_multi SET name = 'updated' WHERE id = %d", i+1))
		}
	})

	require.NoError(t, m.Run(t.Context()))
	wg.Wait()
	require.NoError(t, m.Close())

	var colType string
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		"SELECT COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='altpk_multi' AND COLUMN_NAME='id'",
	).Scan(&colType))
	require.Equal(t, "bigint", colType)
}
