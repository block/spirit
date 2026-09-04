package migration

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"

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

// TestEnumReorder verifies that ENUM reordering ALTERs are refused at preflight.
//
// The binlog replay path (bufferedMap) represents ENUM values as int64
// ordinals from the binlog. Reordering the ENUM definition makes those ordinals point at
// different strings in the target, which would corrupt rows. The preflight
// check refuses these ALTERs unconditionally — it's better to fail fast than
// to corrupt data.
func TestEnumReorder(t *testing.T) {
	t.Parallel()
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
		WithTestThrottler())

	// Concurrent DML during copy phase to exercise binlog replay.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	dmlDone := make(chan struct{})
	go func() {
		defer close(dmlDone)
		if !waitForCopyRows(t, ctx, m) {
			return
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

	require.Error(t, migrationErr)
	require.ErrorContains(t, migrationErr, "unsafe ENUM value reorder")
}

// TestSetReorder mirrors TestEnumReorder but for SET columns.
// SET reordering is refused because the string representation changes
// cause checksum failures.
func TestSetReorder(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "setreorder", `CREATE TABLE setreorder (
		id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
		perms SET('read', 'write', 'execute') NOT NULL
	)`)
	tt.SeedRows(t, "INSERT INTO setreorder (perms) SELECT 'read,write'", 7000)

	m := NewTestRunner(t, "setreorder", "MODIFY COLUMN perms SET('execute', 'read', 'write') NOT NULL",
		WithThreads(1),
		WithTestThrottler())

	// Concurrent DML during copy phase.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	dmlDone := make(chan struct{})
	go func() {
		defer close(dmlDone)
		if !waitForCopyRows(t, ctx, m) {
			return
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

// TestEnumDrop verifies that dropping an ENUM value from anywhere in the
// definition is accepted by the preflight check and that the migration
// succeeds end-to-end when no rows hold the dropped value.
//
// The change-replay path decodes ENUM ordinals against the SOURCE table's
// element list, so retained values still arrive at the target as their
// original string and MySQL maps them onto the new (smaller) enum without
// data corruption.
//
// The change source (binlog file+position vs GTID) is auto-detected from the
// server, so each CI server configuration exercises its own source
// implementation; the decode happens in TableInfo.DecodeBinlogRow, which both
// pkg/change clients call.
func TestEnumDrop(t *testing.T) {
	t.Parallel()
	tableName := "enumdrop"
	tt := testutils.NewTestTable(t, tableName, fmt.Sprintf(`CREATE TABLE %s (
		id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
		status ENUM('active', 'inactive', 'pending', 'archived') NOT NULL
	)`, tableName))
	// Seed only with retained values; 'inactive' will be dropped and must
	// not appear in any row when the checksum runs. A few hundred rows is
	// plenty: WithTestThrottler() paces the copy (1s per chunk), so the runtime
	// is driven by the chunk count, not the row count — a larger seed just adds
	// I/O without changing what's exercised. The concurrent DML below targets
	// ids well within this range.
	tt.SeedRows(t, fmt.Sprintf("INSERT INTO %s (status) SELECT 'active'", tableName), 200)
	tt.SeedRows(t, fmt.Sprintf("INSERT INTO %s (status) SELECT 'pending'", tableName), 200)
	_, err := tt.DB.ExecContext(t.Context(), fmt.Sprintf("INSERT INTO %s (status) VALUES ('archived')", tableName))
	require.NoError(t, err)

	m := NewTestRunner(t, tableName, "MODIFY COLUMN status ENUM('active', 'pending', 'archived') NOT NULL",
		WithThreads(1),
		WithTestThrottler())

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	dmlDone := make(chan struct{})
	go func() {
		defer close(dmlDone)
		if !waitForCopyRows(t, ctx, m) {
			return
		}
		// Concurrent DML uses only retained values; binlog ordinals for
		// 'pending' (3) and 'archived' (4) under the source enum must
		// still resolve to the right strings in the target.
		for i := 1; i <= 100; i++ {
			if ctx.Err() != nil {
				return
			}
			_, _ = tt.DB.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (status) VALUES ('active')`, tableName))
			_, _ = tt.DB.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (status) VALUES ('pending')`, tableName))
			_, _ = tt.DB.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (status) VALUES ('archived')`, tableName))
			_, _ = tt.DB.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET status = 'pending' WHERE id = %d`, tableName, i))
			_, _ = tt.DB.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET status = 'archived' WHERE id = %d`, tableName, 100+i))
		}
	}()

	require.NoError(t, m.Run(ctx))
	cancel()
	<-dmlDone
	require.NoError(t, m.Close())

	// The new column definition must omit 'inactive'.
	var colType string
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		`SELECT column_type FROM information_schema.columns
		 WHERE table_schema='test' AND table_name=? AND column_name='status'`, tableName).Scan(&colType))
	require.Contains(t, colType, "enum('active','pending','archived')")

	// No row should land on the empty-string sentinel ('', MySQL's
	// invalid-ENUM value at ordinal 0), which would mean an ordinal
	// pointed at a value that no longer exists in the target.
	var blanks int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE status = ''`, tableName)).Scan(&blanks))
	require.Zero(t, blanks, "rows landed as the empty enum value — likely a change ordinal decode mismatch")
}

// TestEnumDropWithDroppedValueInData verifies the safety net: when rows
// actually hold the dropped value, the migration must fail (either at
// insert time in strict mode, or at the post-cutover checksum). The
// preflight check is intentionally permissive — it relies on data-level
// verification to catch genuine data loss.
func TestEnumDropWithDroppedValueInData(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "enumdrop_unsafe", `CREATE TABLE enumdrop_unsafe (
		id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
		status ENUM('active', 'inactive', 'pending') NOT NULL
	)`)
	tt.SeedRows(t, "INSERT INTO enumdrop_unsafe (status) SELECT 'active'", 1000)
	_, err := tt.DB.ExecContext(t.Context(), "INSERT INTO enumdrop_unsafe (status) VALUES ('inactive')")
	require.NoError(t, err)

	m := NewTestRunner(t, "enumdrop_unsafe", "MODIFY COLUMN status ENUM('active', 'pending') NOT NULL",
		WithThreads(1),
		WithTestThrottler())

	err = m.Run(t.Context())
	require.Error(t, err, "migration with rows holding a dropped enum value must not silently succeed")
	require.NoError(t, m.Close())
}

// TestEnumToVarchar verifies that ENUM → VARCHAR migrations correctly
// decode binlog ordinals into element strings so that buffered replay
// writes the original values (e.g. "active") to the new VARCHAR column
// rather than the raw int64 ordinal that the go-mysql binlog reader
// emits. Regression test for the gap reported in
// ppe-schema-change-development on Slack (2026-05-19).
func TestEnumToVarchar(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "enumtovarchar", `CREATE TABLE enumtovarchar (
		id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
		status ENUM('active', 'inactive', 'pending') NOT NULL
	)`)
	// Seed with every ENUM value so we can assert on the full mapping.
	tt.SeedRows(t, "INSERT INTO enumtovarchar (status) SELECT 'active'", 3000)
	tt.SeedRows(t, "INSERT INTO enumtovarchar (status) SELECT 'inactive'", 3000)
	tt.SeedRows(t, "INSERT INTO enumtovarchar (status) SELECT 'pending'", 3000)

	m := NewTestRunner(t, "enumtovarchar", "MODIFY COLUMN status VARCHAR(32) NOT NULL",
		WithThreads(1),
		WithTestThrottler())

	// Concurrent DML during the copy phase exercises the binlog replay
	// path — the bug we're regression-testing only surfaces for rows that
	// reach the target via the bufferedMap, not via the chunker's SELECT.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	dmlDone := make(chan struct{})
	go func() {
		defer close(dmlDone)
		if !waitForCopyRows(t, ctx, m) {
			return
		}
		for i := 1; i <= 100; i++ {
			if ctx.Err() != nil {
				return
			}
			_, _ = tt.DB.ExecContext(ctx, `INSERT INTO enumtovarchar (status) VALUES ('active')`)
			_, _ = tt.DB.ExecContext(ctx, `INSERT INTO enumtovarchar (status) VALUES ('inactive')`)
			_, _ = tt.DB.ExecContext(ctx, `INSERT INTO enumtovarchar (status) VALUES ('pending')`)
			_, _ = tt.DB.ExecContext(ctx, fmt.Sprintf(`UPDATE enumtovarchar SET status = 'pending' WHERE id = %d`, i))
			_, _ = tt.DB.ExecContext(ctx, fmt.Sprintf(`UPDATE enumtovarchar SET status = 'inactive' WHERE id = %d`, 3000+i))
			_, _ = tt.DB.ExecContext(ctx, fmt.Sprintf(`UPDATE enumtovarchar SET status = 'active' WHERE id = %d`, 6000+i))
		}
	}()

	require.NoError(t, m.Run(ctx))
	cancel()
	<-dmlDone
	require.NoError(t, m.Close())

	// The column must now be VARCHAR and the values must be the original
	// element strings — not their integer ordinals.
	var colType string
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		`SELECT column_type FROM information_schema.columns
		 WHERE table_schema='test' AND table_name='enumtovarchar' AND column_name='status'`).Scan(&colType))
	require.Contains(t, colType, "varchar")

	// No row should have a numeric-looking status — that would mean the
	// ordinal leaked through. Cast to numeric and look for any non-zero
	// match; ENUM ordinals would all be 1/2/3.
	var leaked int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM enumtovarchar WHERE status IN ('1','2','3')`).Scan(&leaked))
	require.Zero(t, leaked, "binlog ENUM ordinals leaked into VARCHAR column")

	// Every value should be one of the original element strings.
	var nonString int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM enumtovarchar WHERE status NOT IN ('active','inactive','pending')`).Scan(&nonString))
	require.Zero(t, nonString)
}

// TestSetToVarchar verifies that SET → VARCHAR migrations decode the
// bitmask wire format into a comma-joined element string. Companion to
// TestEnumToVarchar.
func TestSetToVarchar(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "settovarchar", `CREATE TABLE settovarchar (
		id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
		perms SET('read', 'write', 'execute') NOT NULL
	)`)
	tt.SeedRows(t, "INSERT INTO settovarchar (perms) SELECT 'read,write'", 5000)
	_, err := tt.DB.ExecContext(t.Context(), `INSERT INTO settovarchar (perms) VALUES ('read'),('write'),('execute'),('read,write,execute')`)
	require.NoError(t, err)

	m := NewTestRunner(t, "settovarchar", "MODIFY COLUMN perms VARCHAR(64) NOT NULL",
		WithThreads(1),
		WithTestThrottler())

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	dmlDone := make(chan struct{})
	go func() {
		defer close(dmlDone)
		if !waitForCopyRows(t, ctx, m) {
			return
		}
		for i := 1; i <= 50; i++ {
			if ctx.Err() != nil {
				return
			}
			_, _ = tt.DB.ExecContext(ctx, `INSERT INTO settovarchar (perms) VALUES ('execute')`)
			_, _ = tt.DB.ExecContext(ctx, `INSERT INTO settovarchar (perms) VALUES ('read,execute')`)
			_, _ = tt.DB.ExecContext(ctx, fmt.Sprintf(`UPDATE settovarchar SET perms = 'read,write,execute' WHERE id = %d`, i))
		}
	}()

	require.NoError(t, m.Run(ctx))
	cancel()
	<-dmlDone
	require.NoError(t, m.Close())

	var colType string
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		`SELECT column_type FROM information_schema.columns
		 WHERE table_schema='test' AND table_name='settovarchar' AND column_name='perms'`).Scan(&colType))
	require.Contains(t, colType, "varchar")

	// No row should contain a numeric-looking bitmask value. Any bitmask
	// leaking through would render as a decimal string like "1", "3", "7".
	var leaked int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM settovarchar WHERE perms REGEXP '^[0-9]+$'`).Scan(&leaked))
	require.Zero(t, leaked, "binlog SET bitmask leaked into VARCHAR column")
}

// TestEnumToSet verifies that ENUM → SET migrations work end-to-end.
// Each ENUM ordinal decodes to a single-element string which the
// destination SET column accepts unchanged. Companion to
// TestEnumToVarchar; covers the same binlog replay path with a SET
// target instead of VARCHAR.
func TestEnumToSet(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "enumtoset_mig", `CREATE TABLE enumtoset_mig (
		id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
		status ENUM('active', 'inactive', 'pending') NOT NULL
	)`)
	tt.SeedRows(t, "INSERT INTO enumtoset_mig (status) SELECT 'active'", 3000)
	tt.SeedRows(t, "INSERT INTO enumtoset_mig (status) SELECT 'inactive'", 3000)
	tt.SeedRows(t, "INSERT INTO enumtoset_mig (status) SELECT 'pending'", 3000)

	m := NewTestRunner(t, "enumtoset_mig",
		"MODIFY COLUMN status SET('active', 'inactive', 'pending') NOT NULL",
		WithThreads(1),
		WithTestThrottler())

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	dmlDone := make(chan struct{})
	go func() {
		defer close(dmlDone)
		if !waitForCopyRows(t, ctx, m) {
			return
		}
		for i := 1; i <= 100; i++ {
			if ctx.Err() != nil {
				return
			}
			_, _ = tt.DB.ExecContext(ctx, `INSERT INTO enumtoset_mig (status) VALUES ('active')`)
			_, _ = tt.DB.ExecContext(ctx, `INSERT INTO enumtoset_mig (status) VALUES ('inactive')`)
			_, _ = tt.DB.ExecContext(ctx, `INSERT INTO enumtoset_mig (status) VALUES ('pending')`)
			_, _ = tt.DB.ExecContext(ctx, fmt.Sprintf(`UPDATE enumtoset_mig SET status = 'pending' WHERE id = %d`, i))
			_, _ = tt.DB.ExecContext(ctx, fmt.Sprintf(`UPDATE enumtoset_mig SET status = 'inactive' WHERE id = %d`, 3000+i))
		}
	}()

	require.NoError(t, m.Run(ctx))
	cancel()
	<-dmlDone
	require.NoError(t, m.Close())

	var colType string
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		`SELECT column_type FROM information_schema.columns
		 WHERE table_schema='test' AND table_name='enumtoset_mig' AND column_name='status'`).Scan(&colType))
	require.Contains(t, colType, "set(")

	// Every row must hold one of the three known element strings. A
	// failed decode would render as an integer-looking value, or the
	// empty SET if an ordinal pointed at a missing element.
	var bad int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM enumtoset_mig
		 WHERE status NOT IN ('active','inactive','pending')`).Scan(&bad))
	require.Zero(t, bad)
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
		WithTestThrottler())

	// Run the migration in a goroutine so we can inject minimal-RBR writes.
	running := startTestRun(t, m.Run, m.Close)

	waitForStatus(t, m, status.CopyRows, running)

	// Continuously write using the minimal-RBR session during the copy phase.
	var writerWg sync.WaitGroup
	writerWg.Go(func() {
		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				_, _ = minimalDB.ExecContext(ctx, `UPDATE minrbr_buffered SET val = val + 1 WHERE id = ?`, (i%100)+1)
			}
		}
	})

	migrationErr := running.wait(t)
	cancel()
	writerWg.Wait()

	require.Error(t, migrationErr)
}

// --- Primary Key Datatype Change Tests (issue #360) ---
// Spirit supports changing the datatype on a primary key column (e.g. INT→BIGINT)
// as long as the primary key itself doesn't change (no ADD/DROP PRIMARY KEY).

func TestAlterPKIntToBigInt(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "altpk_int2big", `CREATE TABLE altpk_int2big (
		id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name varchar(255) NOT NULL,
		val int NOT NULL DEFAULT 0
	)`)
	tt.SeedRows(t, "INSERT INTO altpk_int2big (name, val) SELECT 'a', 1", 3)

	m := NewTestRunner(t, "altpk_int2big", "MODIFY COLUMN id BIGINT NOT NULL AUTO_INCREMENT")
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
	tt := testutils.NewTestTable(t, "altpk_int2bigu", `CREATE TABLE altpk_int2bigu (
		id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name varchar(255) NOT NULL
	)`)
	tt.SeedRows(t, "INSERT INTO altpk_int2bigu (name) SELECT 'a'", 5)

	m := NewTestRunner(t, "altpk_int2bigu", "MODIFY COLUMN id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT")
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
	tt := testutils.NewTestTable(t, "altpk_tiny2int", `CREATE TABLE altpk_tiny2int (
		id tinyint unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
		data varchar(100) NOT NULL
	)`)
	tt.SeedRows(t, "INSERT INTO altpk_tiny2int (data) SELECT 'test'", 50)

	m := NewTestRunner(t, "altpk_tiny2int", "MODIFY COLUMN id INT UNSIGNED NOT NULL AUTO_INCREMENT")
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
	tt := testutils.NewTestTable(t, "altpk_dml", `CREATE TABLE altpk_dml (
		id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name varchar(255) NOT NULL,
		val int NOT NULL DEFAULT 0
	)`)
	tt.SeedRows(t, "INSERT INTO altpk_dml (name, val) SELECT 'seed', 1", 4096)
	m := NewTestRunner(t, "altpk_dml", "MODIFY COLUMN id BIGINT NOT NULL AUTO_INCREMENT",
		WithTestThrottler())

	var wg sync.WaitGroup
	wg.Go(func() {
		if !waitForCopyRows(t, t.Context(), m) {
			return
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

	m := NewTestRunner(t, "altpk_comp", "MODIFY COLUMN id1 BIGINT NOT NULL")
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
	tt := testutils.NewTestTable(t, "altpk_multi", `CREATE TABLE altpk_multi (
		id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name varchar(100) NOT NULL,
		val int NOT NULL DEFAULT 0
	)`)
	tt.SeedRows(t, "INSERT INTO altpk_multi (name, val) SELECT 'seed', 1", 4096)

	m := NewTestRunner(t, "altpk_multi",
		"MODIFY COLUMN id BIGINT NOT NULL AUTO_INCREMENT, MODIFY COLUMN name VARCHAR(255) NOT NULL",
		WithThreads(2),
		WithTestThrottler())

	var wg sync.WaitGroup
	wg.Go(func() {
		if !waitForCopyRows(t, t.Context(), m) {
			return
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

// TestSpatialGeneratedColumnAndIndex verifies that the parser accepts a combined
// ALTER TABLE that adds a GEOMETRY generated column with SRID and a SPATIAL INDEX
// in one statement. Geometry data lives in a text column, so this exercises
// parsing + DDL execution; binary geometry round-tripping is covered separately
// in the copier and subscription tests.
func TestSpatialGeneratedColumnAndIndex(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "t1spatial", `CREATE TABLE t1spatial (
		id bigint NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		geometry_wkt varchar(500) NOT NULL,
		PRIMARY KEY (id)
	)`)
	testutils.RunSQL(t, `INSERT INTO t1spatial (name, geometry_wkt) VALUES
		('Statue of Liberty', 'POINT(-74.0445 40.6892)'),
		('Eiffel Tower', 'POINT(2.2945 48.8584)'),
		('Big Ben', 'POINT(-0.1246 51.5007)'),
		('Colosseum', 'POINT(12.4924 41.8902)'),
		('Sydney Opera House', 'POINT(151.2153 -33.8568)'),
		('Great Wall of China', 'POINT(116.5704 40.4319)'),
		('Machu Picchu', 'POINT(-72.5450 -13.1631)'),
		('Taj Mahal', 'POINT(78.0421 27.1751)'),
		('Christ the Redeemer', 'POINT(-43.2105 -22.9519)'),
		('Golden Gate Bridge', 'POINT(-122.4783 37.8199)')`)

	m := NewTestRunnerFromStatement(t, `ALTER TABLE t1spatial
ADD COLUMN points_of_interest GEOMETRY GENERATED ALWAYS AS (ST_GeomFromText(geometry_wkt, 4326, 'axis-order=long-lat')) STORED NOT NULL SRID 4326,
ADD SPATIAL INDEX idx_points_of_interest (points_of_interest)`)
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())

	var count int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM t1spatial`).Scan(&count))
	require.Equal(t, 10, count)
}

// TestBinaryToVarbinaryConcurrentDML ports gh-ost's localtests/binary-to-varbinary
// scenario (gh-ost issue #909) as positive regression coverage. A BINARY(N)
// source column is migrated to VARBINARY(M) while concurrent DML touches
// rows with trailing-zero values.
//
// Two write paths exist for these rows:
//   - Rowcopy: INSERT IGNORE INTO new SELECT FROM source. MySQL handles the
//     conversion server-side; the BINARY(N)'s right-padded value lands in
//     VARBINARY verbatim.
//   - Binlog replay: the applier reads the row image from the binlog and
//     emits REPLACE INTO new VALUES (0x<bytes>). gh-ost's #909 was that
//     trailing zeros were stripped from the row image, producing shorter
//     VARBINARY values than the rowcopy path for the same source bytes.
//
// Spirit's current applier path (NewDatumFromValue with the BINARY source
// type → forceHexEncode → %#x on the full byte string) does preserve the
// 20-byte width, so this test passes on main. It exists to lock that
// behavior in: any future change that drops the trailing zeros, switches
// the source-type lookup, or changes how the binlog row image is shaped
// would regress this test.
func TestBinaryToVarbinaryConcurrentDML(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "bin2varbin", `CREATE TABLE bin2varbin (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		info VARCHAR(64) NOT NULL,
		data BINARY(20) NOT NULL
	)`)

	// Seed rows that will be the targets of concurrent UPDATEs. Their
	// initial data is non-zero-trailing so the binlog event during UPDATE
	// produces a row image with trailing zeros — the failure mode.
	_, err := tt.DB.ExecContext(t.Context(), `
		INSERT INTO bin2varbin (info, data) VALUES
		  ('pre-existing-1',  X'01020304050607080910111213141516171819ff'),
		  ('pre-existing-2',  X'0102030405060708091011121314151617181900'),
		  ('update-target-1', X'ffffffffffffffffffffffffffffffffffffffff'),
		  ('update-target-2', X'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee')`)
	require.NoError(t, err)
	// Add bulk rows so the copier actually has work to do and replay can
	// race with rowcopy.
	tt.SeedRows(t, "INSERT INTO bin2varbin (info, data) SELECT 'bulk', X'aabbccddeeff00000000000000000000000000ff'", 5000)

	m := NewTestRunner(t, "bin2varbin", "MODIFY data VARBINARY(32) NOT NULL",
		WithThreads(1),
		WithTestThrottler())

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	dmlDone := make(chan struct{})
	go func() {
		defer close(dmlDone)
		if !waitForCopyRows(t, ctx, m) {
			return
		}
		for range 50 {
			if ctx.Err() != nil {
				return
			}
			// INSERTs with trailing-zero data — exercise the binlog INSERT
			// row image path.
			_, _ = tt.DB.ExecContext(ctx, `INSERT INTO bin2varbin (info, data) VALUES ('insert-during', X'aabbccdd00000000000000000000000000000000')`)
			_, _ = tt.DB.ExecContext(ctx, `INSERT INTO bin2varbin (info, data) VALUES ('insert-during', X'11223344556677889900000000000000000000ee')`)
			// UPDATEs of pre-existing rows to trailing-zero values —
			// exercise the binlog UPDATE row image path.
			_, _ = tt.DB.ExecContext(ctx, `UPDATE bin2varbin SET data = X'ffeeddcc00000000000000000000000000000000' WHERE info = 'update-target-1'`)
			_, _ = tt.DB.ExecContext(ctx, `UPDATE bin2varbin SET data = X'aabbccdd11111111111111111100000000000000' WHERE info = 'update-target-2'`)
		}
	}()

	migrationErr := m.Run(ctx)
	cancel()
	<-dmlDone
	require.NoError(t, m.Close())
	require.NoError(t, migrationErr, "BINARY(N) → VARBINARY(M) with concurrent DML must preserve trailing zeros (gh-ost #909 analog)")

	// Target column must now be varbinary.
	var colType string
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		`SELECT column_type FROM information_schema.columns
		 WHERE table_schema=DATABASE() AND table_name='bin2varbin' AND column_name='data'`).Scan(&colType))
	require.Contains(t, colType, "varbinary")

	// The known update-target rows must contain the post-UPDATE bytes
	// (with their trailing zeros). Comparing the full 20-byte value — not
	// just OCTET_LENGTH — ensures the binlog UPDATE path actually
	// replayed: the rows started life with non-zero-trailing data, so a
	// missed UPDATE leaves the rowcopied initial value behind and this
	// assertion fails.
	var data1, data2 []byte
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		`SELECT data FROM bin2varbin WHERE info = 'update-target-1' LIMIT 1`).Scan(&data1))
	wantData1, err := hex.DecodeString("ffeeddcc00000000000000000000000000000000")
	require.NoError(t, err)
	require.Equal(t, wantData1, data1, "update-target-1 must hold its post-UPDATE 20-byte value")
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		`SELECT data FROM bin2varbin WHERE info = 'update-target-2' LIMIT 1`).Scan(&data2))
	wantData2, err := hex.DecodeString("aabbccdd11111111111111111100000000000000")
	require.NoError(t, err)
	require.Equal(t, wantData2, data2, "update-target-2 must hold its post-UPDATE 20-byte value")

	// At least one 'insert-during' row must exist — otherwise the binlog
	// INSERT path was not exercised and the next assertion is vacuous.
	var insertCount int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM bin2varbin WHERE info = 'insert-during'`).Scan(&insertCount))
	require.Positive(t, insertCount, "no concurrent INSERTs reached the binlog path — test is vacuous")

	// And every one of them must keep its full 20-byte width.
	var shortRows int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM bin2varbin WHERE info = 'insert-during' AND OCTET_LENGTH(data) <> 20`).Scan(&shortRows))
	require.Zero(t, shortRows, "rows inserted during migration must keep their full 20-byte width")

	// The bulk-seeded rows take the rowcopy path; they too should round-trip
	// at their full 20-byte width. A regression in the rowcopy SELECT or in
	// MySQL's BINARY→VARBINARY conversion would surface here, separately
	// from the binlog-replay assertions above.
	var bulkShortRows int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM bin2varbin WHERE info = 'bulk' AND OCTET_LENGTH(data) <> 20`).Scan(&bulkShortRows))
	require.Zero(t, bulkShortRows, "rowcopied rows must keep their full 20-byte width")
}

// TestBitColumnDML ports gh-ost's localtests/bit-dml. The applier reads BIT
// values from the binlog as int64 (per go-mysql's decodeBit) and feeds them
// to Spirit's Datum, which has no specific BIT arm in mySQLTypeToDatumTp.
// The int64 falls into the unknownType branch in NewDatum, gets formatted
// with fmt.Sprint, and is then emitted as a *quoted SQL string literal* —
// e.g. REPLACE INTO t (b) VALUES (..., "5", ...).
//
// MySQL interprets a string assigned to BIT(N) as the literal bit pattern
// of the bytes, NOT as a numeric coercion. So `"5"` (a 1-byte string of
// ASCII '5' = 0x35) lands in a BIT(8) column as bit pattern 0x35 (decimal
// 53), and `"127"` (a 3-byte string) is 24 bits wide — wider than BIT(8) —
// triggering MySQL's "Out of range value" warning that Spirit's strict
// applier promotes to a fatal error.
//
// The failure mode is therefore:
//   - For values whose textual representation in bytes is wider than N
//     bits: Spirit aborts the migration with "Out of range value for
//     column ...". Fail-loud, but blocks any concurrent-write workload
//     from migrating a table with a BIT column.
//   - For values that fit: the destination receives the wrong bit pattern.
//     Caught by the checksum, also fail-loud.
//
// All subtests failed before commit fd73e807. The fix was to give BIT a
// dedicated arm in mySQLTypeToDatumTp (most cleanly: unsignedType,
// reinterpreting the int64 as uint64) so the SQL literal is emitted as
// a numeric, not a quoted string.
func TestBitColumnDML(t *testing.T) {
	t.Parallel()
	t.Run("BIT(1)", func(t *testing.T) {
		t.Parallel()
		runBitDMLTest(t, "bit1col", "b1", "BIT(1) NOT NULL DEFAULT 0", []uint64{0, 1})
	})

	t.Run("BIT(8)", func(t *testing.T) {
		t.Parallel()
		runBitDMLTest(t, "bit8col", "b8", "BIT(8) NOT NULL DEFAULT 0", []uint64{0, 1, 127, 200, 255})
	})

	t.Run("BIT(64) high bit set", func(t *testing.T) {
		t.Parallel()
		// Values with the top bit set become negative int64 after decoding,
		// which is where the string-coercion path goes wrong.
		runBitDMLTest(t, "bit64col", "b64", "BIT(64) NOT NULL DEFAULT 0",
			[]uint64{0, 1, 1 << 63, (1 << 63) | 1, ^uint64(0)})
	})

	// Reproduces the exact column definition from the incident report:
	//   `summary_backfill` bit(1) NOT NULL DEFAULT b'0'
	// This ensures Spirit can migrate BIT columns declared with a binary-literal
	// DEFAULT (b'0'), in case schema/column-definition handling differs from a
	// numeric DEFAULT 0.
	t.Run("BIT(1) production default b'0'", func(t *testing.T) {
		t.Parallel()
		runBitDMLTest(t, "bit1prod", "summary_backfill", "BIT(1) NOT NULL DEFAULT b'0'", []uint64{0, 1})
	})
}

func runBitDMLTest(t *testing.T, tableName, colName, colDef string, values []uint64) {
	tt := testutils.NewTestTable(t, tableName, fmt.Sprintf(`CREATE TABLE %s (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		v INT NOT NULL,
		%s %s
	)`, tableName, colName, colDef))

	// Seed enough rows to give the chunker work; the bit value here doesn't
	// matter — the assertions cover rows written during migration via the
	// binlog path.
	tt.SeedRows(t, fmt.Sprintf("INSERT INTO %s (v, %s) SELECT 0, 0", tableName, colName), 5000)

	// One row per test value, inserted before migration starts with the
	// BIT column at 0 — a sentinel that's different from every non-zero
	// test value. We then UPDATE these mid-migration to flip the BIT to
	// `val` via the binlog applier. If the UPDATE never replays, the
	// post-migration BIT value remains at the seed (0) instead of `val`,
	// and the assertion at the bottom catches it. The companion `v=v+1`
	// in the UPDATE statement gives us a second signal that's monotone
	// regardless of the BIT value involved (covers the val=0 case where
	// seed and target both happen to be zero).
	type marker struct {
		id  int64
		val uint64
	}
	markers := make([]marker, len(values))
	for i, v := range values {
		res, err := tt.DB.ExecContext(t.Context(),
			fmt.Sprintf("INSERT INTO %s (v, %s) VALUES (?, 0)", tableName, colName), -1-i)
		require.NoError(t, err)
		id, err := res.LastInsertId()
		require.NoError(t, err)
		markers[i] = marker{id: id, val: v}
	}

	// Capture the row count before the migration starts so we can assert
	// no rows are lost or duplicated. SeedRows reaches the target by
	// doubling, so the actual seeded count is not the requested number.
	var rowsBeforeMigration int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&rowsBeforeMigration))

	m := NewTestRunner(t, tableName, "ENGINE=InnoDB",
		WithThreads(1),
		WithTestThrottler())

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	dmlDone := make(chan struct{})
	go func() {
		defer close(dmlDone)
		if !waitForCopyRows(t, ctx, m) {
			return
		}
		// UPDATE the marker rows mid-migration. Each update produces a
		// binlog row image carrying the BIT value as int64; the applier
		// REPLACEs the corresponding row in the shadow table.
		for range 20 {
			if ctx.Err() != nil {
				return
			}
			for _, mk := range markers {
				_, _ = tt.DB.ExecContext(ctx,
					fmt.Sprintf("UPDATE %s SET v = v + 1, %s = ? WHERE id = ?", tableName, colName),
					mk.val, mk.id)
			}
		}
	}()

	migrationErr := m.Run(ctx)
	cancel()
	<-dmlDone
	require.NoError(t, m.Close())
	require.NoError(t, migrationErr, "BIT column DML migration must not fail checksum")

	// Row count must match what was there before migration. The DML
	// goroutine issues UPDATEs only — no inserts or deletes — so the
	// count must be identical. INSERT IGNORE silently dropping a
	// rowcopied row, or REPLACE INTO deleting one via a unique-key
	// collision on a corrupted bit pattern, would surface here.
	var rowsAfterMigration int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&rowsAfterMigration))
	require.Equal(t, rowsBeforeMigration, rowsAfterMigration, "row count after migration must match")

	// Each marker row must show two things: (1) at least one UPDATE
	// replayed — `v` was seeded negative and is only ever incremented,
	// so v > -1-i proves the UPDATE path fired; (2) the stored BIT value
	// matches what the UPDATE wrote. The combination is what makes the
	// test non-vacuous: a missed UPDATE leaves v at the seed; a buggy
	// BIT serialization leaves a different bit pattern in the column.
	for i, mk := range markers {
		var got uint64
		var v int
		require.NoError(t, tt.DB.QueryRowContext(t.Context(),
			fmt.Sprintf("SELECT v, CAST(%s AS UNSIGNED) FROM %s WHERE id = ?", colName, tableName), mk.id).
			Scan(&v, &got))
		require.Greaterf(t, v, -1-i,
			"marker id=%d: v stayed at seed (%d) — no UPDATE replayed for this row",
			mk.id, -1-i)
		require.Equalf(t, mk.val, got,
			"marker id=%d expected %s value 0x%016x but got 0x%016x",
			mk.id, colDef, mk.val, got)
	}
}

// The VECTOR type (MySQL 9.7+) is a packed array of 4-byte little-endian
// floats. Spirit sees it as raw bytes on every path — the copier's SELECT, the
// binlog row image, and the checksum's CAST — and MySQL only accepts those
// bytes back as a binary-charset literal:
//
//	INSERT ... VALUES (0x0000c03f...)         -- accepted
//	INSERT ... VALUES ("<the same 12 bytes>") -- ER_INVALID_CAST_TO_VECTOR
//	                                             "Value of type 'string, size: 12'
//	                                              cannot be converted to 'vector' type"
//
// so a VECTOR value must be classified as a binary type in table.Datum rather
// than left to the "hex-encode whatever isn't valid UTF-8" fallback: the
// all-zeros vector [0,0,0] *is* valid UTF-8 and would be emitted as the second
// form, aborting the copy or the binlog flush. The checksum has the mirror
// constraint — a VECTOR cannot be cast to char at all (ER_WRONG_ARGUMENTS,
// "Incorrect arguments to cast_as_char"), so castableTp must map it to binary.
//
// The tests below cover the three write paths end to end: row copy, binlog
// replay of concurrent DML, and the checksum that gates cutover.

// vectorSeedValues are the values every test in this file seeds. The first is
// the all-zeros vector — the value whose byte image is valid UTF-8 and which
// therefore fails on any path that hex-encodes only "binary-looking" data.
var vectorSeedValues = []string{
	"[0,0,0]",
	"[1.5,-2,0.0003]",
	"[1e30,-1e-5,123456789.5]",
	"[0,1,0]",
}

// hexVectors reads id -> HEX(col) for the whole table, which is how these
// tests compare vector contents: HEX() is exact (VECTOR_TO_STRING rounds to
// 5 decimal places) and works the same before and after a migration.
func hexVectors(t *testing.T, db *sql.DB, table, col string) map[int]string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(),
		fmt.Sprintf("SELECT id, IFNULL(HEX(%s), 'NULL') FROM %s", col, table))
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)
	out := make(map[int]string)
	for rows.Next() {
		var id int
		var hexVal string
		require.NoError(t, rows.Scan(&id, &hexVal))
		out[id] = hexVal
	}
	require.NoError(t, rows.Err())
	return out
}

// TestVectorE2ECopy migrates a table holding VECTOR columns through the copy
// path and asserts every value survives byte-for-byte. The migration also runs
// the checksum, so this covers the CAST side too: before castableTp knew about
// VECTOR the checksum query itself failed with "Incorrect arguments to
// cast_as_char".
func TestVectorE2ECopy(t *testing.T) {
	t.Parallel()
	testutils.SkipUnlessVectorSupported(t)

	tt := testutils.NewTestTable(t, "vector_copy", `CREATE TABLE vector_copy (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		pad VARCHAR(64) NOT NULL,
		embedding VECTOR(3) NOT NULL,
		optional_embedding VECTOR(4) NULL,
		default_dim VECTOR NULL
	)`)

	for _, v := range vectorSeedValues {
		testutils.RunSQL(t, fmt.Sprintf(`INSERT INTO vector_copy (pad, embedding, optional_embedding, default_dim)
			VALUES ('seed', STRING_TO_VECTOR('%s'), NULL, STRING_TO_VECTOR('[0,0]'))`, v))
	}
	testutils.RunSQL(t, `INSERT INTO vector_copy (pad, embedding, optional_embedding, default_dim)
		VALUES ('nulls', STRING_TO_VECTOR('[0,0,0]'), STRING_TO_VECTOR('[0,0,0,0]'), NULL)`)
	// Bulk rows so the copier does real chunked work rather than a single
	// chunk of 5 rows.
	tt.SeedRows(t, "INSERT INTO vector_copy (pad, embedding, optional_embedding, default_dim) SELECT 'bulk', STRING_TO_VECTOR('[0,0,0]'), STRING_TO_VECTOR('[1,2,3,4]'), STRING_TO_VECTOR('[0]')", 2000)

	before := hexVectors(t, tt.DB, "vector_copy", "embedding")
	beforeOptional := hexVectors(t, tt.DB, "vector_copy", "optional_embedding")
	require.GreaterOrEqual(t, len(before), 2000)

	// MODIFY of a VARCHAR across the 255-byte length-prefix boundary is not
	// INSTANT or INPLACE, so this forces the copy path.
	m := NewTestRunner(t, "vector_copy", "MODIFY COLUMN pad VARCHAR(300) NOT NULL", WithThreads(2))
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())
	require.False(t, m.usedInstantDDL)
	require.False(t, m.usedInplaceDDL)

	require.Equal(t, before, hexVectors(t, tt.DB, "vector_copy", "embedding"),
		"copied VECTOR values must be byte-identical to the source")
	require.Equal(t, beforeOptional, hexVectors(t, tt.DB, "vector_copy", "optional_embedding"),
		"copied nullable VECTOR values (including NULLs) must be preserved")

	// The stored values must still be readable as vectors — a corrupted
	// byte image would either fail VECTOR_DIM or report the wrong width.
	var badDims int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM vector_copy WHERE VECTOR_DIM(embedding) <> 3`).Scan(&badDims))
	require.Zero(t, badDims, "every copied embedding must still be a 3-dimension vector")
}

// TestVectorE2EDimensionChange widens a VECTOR column's declared dimension.
// The stored images do not change (a VECTOR is only as wide as the value
// written into it), but source and target column types now differ, which is
// the case where the checksum's CAST type comes from the *target* table —
// exercising the VECTOR arm of castableTp on a genuine type change.
func TestVectorE2EDimensionChange(t *testing.T) {
	t.Parallel()
	testutils.SkipUnlessVectorSupported(t)

	tt := testutils.NewTestTable(t, "vector_widen", `CREATE TABLE vector_widen (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		embedding VECTOR(3) NOT NULL
	)`)
	for _, v := range vectorSeedValues {
		testutils.RunSQL(t, fmt.Sprintf(
			"INSERT INTO vector_widen (embedding) VALUES (STRING_TO_VECTOR('%s'))", v))
	}
	tt.SeedRows(t, "INSERT INTO vector_widen (embedding) SELECT STRING_TO_VECTOR('[0,0,0]')", 1000)
	before := hexVectors(t, tt.DB, "vector_widen", "embedding")

	m := NewTestRunner(t, "vector_widen", "MODIFY COLUMN embedding VECTOR(8) NOT NULL", WithThreads(1))
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())

	var colType string
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		`SELECT column_type FROM information_schema.columns
		 WHERE table_schema=DATABASE() AND table_name='vector_widen' AND column_name='embedding'`).Scan(&colType))
	require.Equal(t, "vector(8)", colType)

	require.Equal(t, before, hexVectors(t, tt.DB, "vector_widen", "embedding"),
		"widening the declared dimension must not alter the stored vectors")
}

// TestVectorE2EAddColumn adds a VECTOR column to an existing table. The new
// column only exists on the target, so the copier writes its DEFAULT and the
// checksum compares the intersected columns — a cheap guard that a VECTOR
// column present on one side only does not break either.
func TestVectorE2EAddColumn(t *testing.T) {
	t.Parallel()
	testutils.SkipUnlessVectorSupported(t)

	tt := testutils.NewTestTable(t, "vector_add", `CREATE TABLE vector_add (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		pad VARCHAR(64) NOT NULL
	)`)
	tt.SeedRows(t, "INSERT INTO vector_add (pad) SELECT 'bulk'", 1000)

	m := NewTestRunner(t, "vector_add",
		"ADD COLUMN embedding VECTOR(3) NULL, MODIFY COLUMN pad VARCHAR(300) NOT NULL",
		WithThreads(1))
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())

	var nonNull int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM vector_add WHERE embedding IS NOT NULL`).Scan(&nonNull))
	require.Zero(t, nonNull, "the added VECTOR column has no source data, so every row must be NULL")

	// The column must be usable afterwards.
	testutils.RunSQL(t, "UPDATE vector_add SET embedding = STRING_TO_VECTOR('[0,0,0]') WHERE id = 1")
	var dim int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		`SELECT VECTOR_DIM(embedding) FROM vector_add WHERE id = 1`).Scan(&dim))
	require.Equal(t, 3, dim)
}

// TestVectorConcurrentDML is the binlog-capture half of the issue: rows
// written *while* the migration is copying reach the target through the
// buffered subscription and the applier's REPLACE INTO, not through the
// copier. Every concurrent write here carries an all-zeros vector, the value
// whose byte image is valid UTF-8 — before the Datum fix the applier emitted
// it as a quoted string literal and the flush failed with
// ER_INVALID_CAST_TO_VECTOR, aborting the migration.
func TestVectorConcurrentDML(t *testing.T) {
	t.Parallel()
	testutils.SkipUnlessVectorSupported(t)

	tt := testutils.NewTestTable(t, "vector_dml", `CREATE TABLE vector_dml (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		info VARCHAR(64) NOT NULL,
		embedding VECTOR(3) NOT NULL
	)`)

	// Rows the concurrent UPDATEs target. They start with a non-zero vector
	// so a missed UPDATE leaves the row-copied value behind and the
	// assertions below can tell the two apart.
	testutils.RunSQL(t, `INSERT INTO vector_dml (info, embedding) VALUES
		('update-target-1', STRING_TO_VECTOR('[1.5,-2,0.0003]')),
		('update-target-2', STRING_TO_VECTOR('[1e30,-1e-5,123456789.5]')),
		('delete-target', STRING_TO_VECTOR('[9,9,9]'))`)
	tt.SeedRows(t, "INSERT INTO vector_dml (info, embedding) SELECT 'bulk', STRING_TO_VECTOR('[0,0,0]')", 2000)

	m := NewTestRunner(t, "vector_dml", "MODIFY COLUMN info VARCHAR(300) NOT NULL",
		WithThreads(1),
		WithTestThrottler())

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	dmlDone := make(chan struct{})
	go func() {
		defer close(dmlDone)
		if !waitForCopyRows(t, ctx, m) {
			return
		}
		// The DELETE happens once, up front, so the row is gone from the
		// source before the copier necessarily reached it.
		_, _ = tt.DB.ExecContext(ctx, `DELETE FROM vector_dml WHERE info = 'delete-target'`)
		for range 50 {
			if ctx.Err() != nil {
				return
			}
			_, _ = tt.DB.ExecContext(ctx, `INSERT INTO vector_dml (info, embedding) VALUES ('insert-during', STRING_TO_VECTOR('[0,0,0]'))`)
			_, _ = tt.DB.ExecContext(ctx, `INSERT INTO vector_dml (info, embedding) VALUES ('insert-during', STRING_TO_VECTOR('[0,1,0]'))`)
			// UPDATE to the all-zeros vector: the full after-image drags the
			// value back through the applier.
			_, _ = tt.DB.ExecContext(ctx, `UPDATE vector_dml SET embedding = STRING_TO_VECTOR('[0,0,0]') WHERE info = 'update-target-1'`)
			_, _ = tt.DB.ExecContext(ctx, `UPDATE vector_dml SET embedding = STRING_TO_VECTOR('[0,0,1e-5]') WHERE info = 'update-target-2'`)
		}
	}()

	migrationErr := m.Run(ctx)
	cancel()
	<-dmlDone
	require.NoError(t, m.Close())
	require.NoError(t, migrationErr, "VECTOR values written during the copy must replicate through the applier")

	// The UPDATE targets must hold their post-UPDATE values, proving the
	// binlog UPDATE path replayed rather than the copier's initial image
	// surviving.
	requireVectorEquals(t, tt.DB, "update-target-1", "[0,0,0]")
	requireVectorEquals(t, tt.DB, "update-target-2", "[0,0,1e-5]")

	// The DELETE must have replayed too.
	var deleted int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM vector_dml WHERE info = 'delete-target'`).Scan(&deleted))
	require.Zero(t, deleted, "the row deleted during the migration must not reappear on the target")

	// Rows inserted during the copy must be present and intact — otherwise
	// the binlog INSERT path was never exercised and the test is vacuous.
	var insertCount int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM vector_dml WHERE info = 'insert-during'`).Scan(&insertCount))
	require.Positive(t, insertCount, "no concurrent INSERTs reached the binlog path — test is vacuous")

	var badRows int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM vector_dml
		 WHERE info = 'insert-during' AND (VECTOR_DIM(embedding) <> 3 OR OCTET_LENGTH(embedding) <> 12)`).Scan(&badRows))
	require.Zero(t, badRows, "vectors inserted during the migration must keep their exact byte image")
}

// requireVectorEquals asserts the row identified by info holds exactly the
// vector described by the given STRING_TO_VECTOR literal, compared as raw
// bytes.
func requireVectorEquals(t *testing.T, db *sql.DB, info, want string) {
	t.Helper()
	var got, expected string
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT HEX(embedding), HEX(STRING_TO_VECTOR(?)) FROM vector_dml WHERE info = ? LIMIT 1`,
		want, info).Scan(&got, &expected))
	require.Equal(t, expected, got, "row %q must hold the vector %s", info, want)
}
