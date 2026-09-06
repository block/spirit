package applier

import (
	"context"
	"database/sql"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// TestSingleTargetApplierBasic tests basic functionality of SingleTargetApplier
func TestSingleTargetApplierBasic(t *testing.T) {
	// Setup test databases
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS single_source")
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS single_target")
	testutils.RunSQL(t, "CREATE DATABASE single_source")
	testutils.RunSQL(t, "CREATE DATABASE single_target")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	source := base.Clone()
	source.DBName = "single_source"
	sourceDB, err := sql.Open("block-mysql", source.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(sourceDB)

	target := base.Clone()
	target.DBName = "single_target"
	targetDB, err := sql.Open("block-mysql", target.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(targetDB)

	// Create test table
	createTableSQL := `
		CREATE TABLE test_table (
			id INT PRIMARY KEY,
			name VARCHAR(100),
			value INT,
			binaryvalue BLOB
		)
	`

	_, err = sourceDB.ExecContext(t.Context(), createTableSQL)
	require.NoError(t, err)
	_, err = targetDB.ExecContext(t.Context(), createTableSQL)
	require.NoError(t, err)

	// Create table info objects
	sourceTable := table.NewTableInfo(sourceDB, source.DBName, "test_table")
	err = sourceTable.SetInfo(t.Context())
	require.NoError(t, err)

	targetTable := table.NewTableInfo(targetDB, target.DBName, "test_table")
	err = targetTable.SetInfo(t.Context())
	require.NoError(t, err)

	// Create applier
	tar := Target{
		DB:       targetDB,
		Config:   target,
		KeyRange: "0",
	}
	applier, err := NewSingleTargetApplier(tar, NewApplierDefaultConfig())
	require.NoError(t, err)

	// Start the applier
	err = applier.Start(t.Context())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, applier.Stop())
	}()

	// Prepare test data
	testRows := [][]any{
		{int64(1), "Alice", int64(100), []byte{0x00}},
		{int64(2), "Bob", int64(200), nil},
		{int64(3), "Charlie", int64(300), []byte{0x62, 0x72, 0x6F, 0x77, 0x6E}},
		{int64(4), "Dan", int64(400), []byte{}},
	}

	// Create a chunk
	chunk := &table.Chunk{
		Table:         sourceTable,
		NewTable:      targetTable,
		ColumnMapping: table.NewColumnMapping(sourceTable, targetTable, nil),
	}

	// Apply the rows
	var callbackInvoked atomic.Bool
	var callbackAffectedRows atomic.Int64
	var callbackErrMu sync.Mutex
	var callbackErr error

	callback := func(affectedRows int64, err error) {
		callbackInvoked.Store(true)
		callbackAffectedRows.Store(affectedRows)
		callbackErrMu.Lock()
		callbackErr = err
		callbackErrMu.Unlock()
	}

	err = applier.Apply(t.Context(), chunk, testRows, callback)
	require.NoError(t, err)

	// Wait for processing
	err = applier.Wait(t.Context())
	require.NoError(t, err)

	// Verify callback was invoked
	require.True(t, callbackInvoked.Load(), "Callback should have been invoked")
	callbackErrMu.Lock()
	require.NoError(t, callbackErr, "Callback should not have an error")
	callbackErrMu.Unlock()
	require.Equal(t, int64(4), callbackAffectedRows.Load(), "Should have affected 4 rows")

	// Verify data in target
	var count int
	err = targetDB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM test_table").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 4, count, "Should have 4 rows in target")

	// Verify specific rows. The blob assertions check nullness and length
	// explicitly: bytes.Equal (which testify uses for []byte) treats nil and
	// []byte{} as equal, but NULL, the empty string, and a one-byte NUL are
	// three distinct values and the applier must preserve each exactly.
	// Regression: the empty value used to be written as 0x00 (a one-byte
	// NUL), silently corrupting empty blobs applied via the binlog feed.
	rows, err := targetDB.QueryContext(t.Context(),
		"SELECT id, name, value, binaryvalue, binaryvalue IS NULL, COALESCE(LENGTH(binaryvalue), -1) FROM test_table ORDER BY id")
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)

	expected := []struct {
		id          int64
		name        string
		value       int64
		binaryvalue []byte
		isNull      bool
		length      int64
	}{
		{1, "Alice", 100, []byte{0x00}, false, 1},
		{2, "Bob", 200, nil, true, -1},
		{3, "Charlie", 300, []byte{0x62, 0x72, 0x6F, 0x77, 0x6E}, false, 5},
		{4, "Dan", 400, []byte{}, false, 0},
	}

	i := 0
	for rows.Next() {
		var id, value, length int64
		var name string
		var binaryvalue []byte
		var isNull bool
		err = rows.Scan(&id, &name, &value, &binaryvalue, &isNull, &length)
		require.NoError(t, err)
		require.Equal(t, expected[i].id, id)
		require.Equal(t, expected[i].name, name)
		require.Equal(t, expected[i].value, value)
		require.Equal(t, expected[i].binaryvalue, binaryvalue)
		require.Equal(t, expected[i].isNull, isNull, "row id=%d nullness", id)
		require.Equal(t, expected[i].length, length, "row id=%d blob length", id)
		i++
	}
	require.NoError(t, rows.Err())
	require.Equal(t, 4, i, "Should have read 4 rows")
}

// TestSingleTargetApplierEmptyRows tests applying empty row set
func TestSingleTargetApplierEmptyRows(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS single_empty_test")
	testutils.RunSQL(t, "CREATE DATABASE single_empty_test")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	target := base.Clone()
	target.DBName = "single_empty_test"
	targetDB, err := sql.Open("block-mysql", target.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(targetDB)

	// Create test table
	createTableSQL := `CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(100))`
	_, err = targetDB.ExecContext(t.Context(), createTableSQL)
	require.NoError(t, err)

	targetTable := table.NewTableInfo(targetDB, target.DBName, "test_table")
	err = targetTable.SetInfo(t.Context())
	require.NoError(t, err)

	// Create applier
	tar := Target{
		DB:       targetDB,
		Config:   target,
		KeyRange: "0",
	}
	applier, err := NewSingleTargetApplier(tar, NewApplierDefaultConfig())
	require.NoError(t, err)

	err = applier.Start(t.Context())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, applier.Stop())
	}()

	// Apply empty rows
	chunk := &table.Chunk{
		Table:         targetTable,
		NewTable:      targetTable,
		ColumnMapping: table.NewColumnMapping(targetTable, targetTable, nil),
	}

	var callbackInvoked atomic.Bool
	callback := func(affectedRows int64, err error) {
		callbackInvoked.Store(true)
		require.NoError(t, err)
		require.Equal(t, int64(0), affectedRows)
	}

	err = applier.Apply(t.Context(), chunk, [][]any{}, callback)
	require.NoError(t, err)

	// Callback should be invoked immediately for empty rows
	require.True(t, callbackInvoked.Load(), "Callback should have been invoked immediately for empty rows")
}

// TestSingleTargetApplierLargeDataset tests applying a large dataset that spans multiple chunklets
func TestSingleTargetApplierLargeDataset(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS single_large_test")
	testutils.RunSQL(t, "CREATE DATABASE single_large_test")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	target := base.Clone()
	target.DBName = "single_large_test"
	targetDB, err := sql.Open("block-mysql", target.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(targetDB)

	// Create test table
	createTableSQL := `CREATE TABLE test_table (id INT PRIMARY KEY, value INT)`
	_, err = targetDB.ExecContext(t.Context(), createTableSQL)
	require.NoError(t, err)

	targetTable := table.NewTableInfo(targetDB, target.DBName, "test_table")
	err = targetTable.SetInfo(t.Context())
	require.NoError(t, err)

	// Create applier
	tar := Target{
		DB:       targetDB,
		Config:   target,
		KeyRange: "0",
	}
	applier, err := NewSingleTargetApplier(tar, NewApplierDefaultConfig())
	require.NoError(t, err)

	err = applier.Start(t.Context())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, applier.Stop())
	}()

	// Create 2500 rows (will span 3 chunklets of 1000 each)
	rowCount := 2500
	testRows := make([][]any, rowCount)
	for i := range rowCount {
		testRows[i] = []any{int64(i + 1), int64(i * 10)}
	}

	chunk := &table.Chunk{
		Table:         targetTable,
		NewTable:      targetTable,
		ColumnMapping: table.NewColumnMapping(targetTable, targetTable, nil),
	}

	var callbackInvoked atomic.Bool
	var callbackAffectedRows atomic.Int64

	callback := func(affectedRows int64, err error) {
		callbackInvoked.Store(true)
		callbackAffectedRows.Store(affectedRows)
		require.NoError(t, err)
	}

	err = applier.Apply(t.Context(), chunk, testRows, callback)
	require.NoError(t, err)

	err = applier.Wait(t.Context())
	require.NoError(t, err)

	require.True(t, callbackInvoked.Load())
	require.Equal(t, int64(rowCount), callbackAffectedRows.Load())

	// Verify count
	var count int
	err = targetDB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM test_table").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, rowCount, count)
}

// TestSingleTargetApplierConcurrentApplies tests multiple concurrent Apply calls
func TestSingleTargetApplierConcurrentApplies(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS single_concurrent_test")
	testutils.RunSQL(t, "CREATE DATABASE single_concurrent_test")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	target := base.Clone()
	target.DBName = "single_concurrent_test"
	targetDB, err := sql.Open("block-mysql", target.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(targetDB)

	// Create test table
	createTableSQL := `CREATE TABLE test_table (id INT PRIMARY KEY, batch INT, value INT)`
	_, err = targetDB.ExecContext(t.Context(), createTableSQL)
	require.NoError(t, err)

	targetTable := table.NewTableInfo(targetDB, target.DBName, "test_table")
	err = targetTable.SetInfo(t.Context())
	require.NoError(t, err)

	// Create applier
	tar := Target{
		DB:       targetDB,
		Config:   target,
		KeyRange: "0",
	}
	applier, err := NewSingleTargetApplier(tar, NewApplierDefaultConfig())
	require.NoError(t, err)

	err = applier.Start(t.Context())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, applier.Stop())
	}()

	// Launch multiple concurrent Apply operations
	numBatches := 10
	rowsPerBatch := 100
	var wg sync.WaitGroup
	var totalCallbacks atomic.Int32

	// require.* must not run in a non-test goroutine (testifylint go-require):
	// a failed require calls runtime.Goexit, which only unwinds that goroutine.
	// Each goroutine and callback sends its result over a channel and the
	// assertions run in the main test goroutine after wg.Wait(). The channels
	// are buffered larger than any possible number of sends so neither the
	// Apply goroutines nor the applier's callback goroutines can block.
	applyErrs := make(chan error, 2*numBatches)
	callbackRows := make(chan int64, numBatches*rowsPerBatch)

	for batch := range numBatches {
		wg.Add(1)
		go func(batchNum int) {
			defer wg.Done()

			// Create rows for this batch
			testRows := make([][]any, rowsPerBatch)
			for i := range rowsPerBatch {
				id := batchNum*rowsPerBatch + i + 1
				testRows[i] = []any{int64(id), int64(batchNum), int64(i)}
			}

			chunk := &table.Chunk{
				Table:         targetTable,
				NewTable:      targetTable,
				ColumnMapping: table.NewColumnMapping(targetTable, targetTable, nil),
			}

			callback := func(affectedRows int64, err error) {
				totalCallbacks.Add(1)
				if err != nil {
					applyErrs <- err
					return
				}
				callbackRows <- affectedRows
			}

			applyErrs <- applier.Apply(t.Context(), chunk, testRows, callback)
		}(batch)
	}

	wg.Wait()

	// Wait for all work to complete
	err = applier.Wait(t.Context())
	require.NoError(t, err)
	close(applyErrs)
	close(callbackRows)

	// Verify all callbacks were invoked
	require.Equal(t, int32(numBatches), totalCallbacks.Load())

	// All Apply calls and callbacks must have succeeded.
	for applyErr := range applyErrs {
		require.NoError(t, applyErr)
	}
	for affectedRows := range callbackRows {
		require.Equal(t, int64(rowsPerBatch), affectedRows)
	}

	// Verify total count
	var count int
	err = targetDB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM test_table").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, numBatches*rowsPerBatch, count)
}

// TestSingleTargetApplierDeleteKeys tests the DeleteKeys method
func TestSingleTargetApplierDeleteKeys(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS single_delete_test")
	testutils.RunSQL(t, "CREATE DATABASE single_delete_test")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	target := base.Clone()
	target.DBName = "single_delete_test"
	targetDB, err := sql.Open("block-mysql", target.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(targetDB)

	// Create test table
	createTableSQL := `CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(100))`
	_, err = targetDB.ExecContext(t.Context(), createTableSQL)
	require.NoError(t, err)

	// Insert test data
	_, err = targetDB.ExecContext(t.Context(), "INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie'), (4, 'Diana'), (5, 'Eve')")
	require.NoError(t, err)

	targetTable := table.NewTableInfo(targetDB, target.DBName, "test_table")
	err = targetTable.SetInfo(t.Context())
	require.NoError(t, err)

	// Create applier
	tar := Target{
		DB:       targetDB,
		Config:   target,
		KeyRange: "0",
	}
	applier, err := NewSingleTargetApplier(tar, NewApplierDefaultConfig())
	require.NoError(t, err)

	// Delete keys 2 and 4
	keysToDelete := [][]any{
		{int64(2)},
		{int64(4)},
	}

	affectedRows, err := applier.DeleteKeys(t.Context(), targetTable, targetTable, keysToDelete, nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), affectedRows)

	// Verify remaining rows
	var count int
	err = targetDB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM test_table").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 3, count)

	// Verify specific rows remain
	rows, err := targetDB.QueryContext(t.Context(), "SELECT id, name FROM test_table ORDER BY id")
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)

	expected := []struct {
		id   int64
		name string
	}{
		{1, "Alice"},
		{3, "Charlie"},
		{5, "Eve"},
	}

	i := 0
	for rows.Next() {
		var id int64
		var name string
		err = rows.Scan(&id, &name)
		require.NoError(t, err)
		require.Equal(t, expected[i].id, id)
		require.Equal(t, expected[i].name, name)
		i++
	}
	require.NoError(t, rows.Err())
}

// TestSingleTargetApplierDeleteKeysSeparatorInValues tests DeleteKeys with
// string PK values containing the hash-key separator "-#-". DeleteKeys now
// receives the original typed key tuples directly (not a hashed string),
// so the separator cannot corrupt the DELETE arity — but a single-column
// PK value containing "-#-" must still delete exactly one row, and a
// composite PK must delete exactly the intended tuple. (The hash-key
// escaping that keeps these distinct as *map keys* is covered in
// pkg/utils.)
func TestSingleTargetApplierDeleteKeysSeparatorInValues(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS single_delete_sep_test")
	testutils.RunSQL(t, "CREATE DATABASE single_delete_sep_test")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	target := base.Clone()
	target.DBName = "single_delete_sep_test"
	targetDB, err := sql.Open("block-mysql", target.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(targetDB)

	// Single-column VARCHAR PK whose values contain the separator.
	_, err = targetDB.ExecContext(t.Context(),
		`CREATE TABLE test_table (id VARCHAR(64) PRIMARY KEY, name VARCHAR(100))`)
	require.NoError(t, err)
	_, err = targetDB.ExecContext(t.Context(),
		`INSERT INTO test_table VALUES ('a-#-b', 'Alice'), ('plain', 'Bob'), ('-#-', 'Charlie')`)
	require.NoError(t, err)

	targetTable := table.NewTableInfo(targetDB, target.DBName, "test_table")
	require.NoError(t, targetTable.SetInfo(t.Context()))

	tar := Target{
		DB:       targetDB,
		Config:   target,
		KeyRange: "0",
	}
	applier, err := NewSingleTargetApplier(tar, NewApplierDefaultConfig())
	require.NoError(t, err)

	keysToDelete := [][]any{
		{"a-#-b"},
		{"-#-"},
	}
	affectedRows, err := applier.DeleteKeys(t.Context(), targetTable, targetTable, keysToDelete, nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), affectedRows)

	var id, name string
	require.NoError(t, targetDB.QueryRowContext(t.Context(),
		"SELECT id, name FROM test_table").Scan(&id, &name))
	require.Equal(t, "plain", id)
	require.Equal(t, "Bob", name)

	// Composite PK: values containing the separator must round-trip into a
	// tuple of the correct arity and delete exactly the intended row.
	_, err = targetDB.ExecContext(t.Context(),
		`CREATE TABLE test_composite (id1 VARCHAR(64), id2 VARCHAR(64), name VARCHAR(100), PRIMARY KEY (id1, id2))`)
	require.NoError(t, err)
	_, err = targetDB.ExecContext(t.Context(),
		`INSERT INTO test_composite VALUES ('a-#-b', 'c', 'collide1'), ('a', 'b-#-c', 'collide2'), ('keep', 'me', 'safe')`)
	require.NoError(t, err)

	compositeTable := table.NewTableInfo(targetDB, target.DBName, "test_composite")
	require.NoError(t, compositeTable.SetInfo(t.Context()))

	affectedRows, err = applier.DeleteKeys(t.Context(), compositeTable, compositeTable,
		[][]any{{"a-#-b", "c"}}, nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), affectedRows, "only ('a-#-b','c') must be deleted, not ('a','b-#-c')")

	var count int
	require.NoError(t, targetDB.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM test_composite").Scan(&count))
	require.Equal(t, 2, count)
	require.NoError(t, targetDB.QueryRowContext(t.Context(),
		"SELECT name FROM test_composite WHERE id1 = 'a' AND id2 = 'b-#-c'").Scan(&name))
	require.Equal(t, "collide2", name, "the colliding sibling row must survive the delete")
}

// TestSingleTargetApplierDeleteKeysEmpty tests DeleteKeys with empty key list
func TestSingleTargetApplierDeleteKeysEmpty(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS single_delete_empty_test")
	testutils.RunSQL(t, "CREATE DATABASE single_delete_empty_test")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	target := base.Clone()
	target.DBName = "single_delete_empty_test"
	targetDB, err := sql.Open("block-mysql", target.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(targetDB)

	createTableSQL := `CREATE TABLE test_table (id INT PRIMARY KEY)`
	_, err = targetDB.ExecContext(t.Context(), createTableSQL)
	require.NoError(t, err)

	targetTable := table.NewTableInfo(targetDB, target.DBName, "test_table")
	err = targetTable.SetInfo(t.Context())
	require.NoError(t, err)

	tar := Target{
		DB:       targetDB,
		Config:   target,
		KeyRange: "0",
	}
	applier, err := NewSingleTargetApplier(tar, NewApplierDefaultConfig())
	require.NoError(t, err)

	// Delete with empty keys
	affectedRows, err := applier.DeleteKeys(t.Context(), targetTable, targetTable, [][]any{}, nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), affectedRows)
}

// TestSingleTargetApplierUpsertRows tests the UpsertRows method
func TestSingleTargetApplierUpsertRows(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS single_upsert_test")
	testutils.RunSQL(t, "CREATE DATABASE single_upsert_test")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	target := base.Clone()
	target.DBName = "single_upsert_test"
	targetDB, err := sql.Open("block-mysql", target.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(targetDB)

	// Create test table
	createTableSQL := `CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(100), value INT)`
	_, err = targetDB.ExecContext(t.Context(), createTableSQL)
	require.NoError(t, err)

	// Insert initial data
	_, err = targetDB.ExecContext(t.Context(), "INSERT INTO test_table VALUES (1, 'Alice', 100), (2, 'Bob', 200)")
	require.NoError(t, err)

	targetTable := table.NewTableInfo(targetDB, target.DBName, "test_table")
	err = targetTable.SetInfo(t.Context())
	require.NoError(t, err)

	// Create applier
	tar := Target{
		DB:       targetDB,
		Config:   target,
		KeyRange: "0",
	}
	applier, err := NewSingleTargetApplier(tar, NewApplierDefaultConfig())
	require.NoError(t, err)

	// Upsert rows: update id=1, insert id=3
	upsertRows := []LogicalRow{
		{
			RowImage:  []any{int64(1), "Alice Updated", int64(150)},
			IsDeleted: false,
		},
		{
			RowImage:  []any{int64(3), "Charlie", int64(300)},
			IsDeleted: false,
		},
	}

	affectedRows, err := applier.UpsertRows(t.Context(), table.NewColumnMapping(targetTable, targetTable, nil), upsertRows, nil)
	require.NoError(t, err)
	// MySQL returns 1 for insert, 2 for update in ON DUPLICATE KEY UPDATE
	// So we expect 1 (update of id=1) + 1 (insert of id=3) = at least 2
	require.GreaterOrEqual(t, affectedRows, int64(2))

	// Verify data
	rows, err := targetDB.QueryContext(t.Context(), "SELECT id, name, value FROM test_table ORDER BY id")
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)

	expected := []struct {
		id    int64
		name  string
		value int64
	}{
		{1, "Alice Updated", 150},
		{2, "Bob", 200},
		{3, "Charlie", 300},
	}

	i := 0
	for rows.Next() {
		var id, value int64
		var name string
		err = rows.Scan(&id, &name, &value)
		require.NoError(t, err)
		require.Equal(t, expected[i].id, id)
		require.Equal(t, expected[i].name, name)
		require.Equal(t, expected[i].value, value)
		i++
	}
	require.NoError(t, rows.Err())
	require.Equal(t, 3, i)
}

// TestSingleTargetApplierUpsertRowsEmptyBlob is a regression test for the
// binlog-apply path corrupting empty (zero-length, non-NULL) binary values.
// Under row-based replication any update to a row produces a full row image,
// so UpsertRows rewrites every column — including blobs the application never
// touched. The empty value used to serialize as 0x00 — a one-byte NUL, a
// different value — so on tables that store empty strings (e.g. zero-length
// serialized protos) every feed-touched row produced a checksum mismatch,
// was recopied byte-exact, and then re-minted on the row's next update: a
// checksum that never converges, and real corruption for rows whose last
// update lands after the final checksum pass.
func TestSingleTargetApplierUpsertRowsEmptyBlob(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS single_upsert_emptyblob_test")
	testutils.RunSQL(t, "CREATE DATABASE single_upsert_emptyblob_test")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	target := base.Clone()
	target.DBName = "single_upsert_emptyblob_test"
	targetDB, err := sql.Open("block-mysql", target.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(targetDB)

	_, err = targetDB.ExecContext(t.Context(),
		"CREATE TABLE transfers (id INT PRIMARY KEY, state VARCHAR(20), instrument_proto BLOB)")
	require.NoError(t, err)

	// The copy phase wrote these rows byte-exact (server-side INSERT..SELECT).
	_, err = targetDB.ExecContext(t.Context(),
		"INSERT INTO transfers VALUES (1, 'pending', ''), (2, 'pending', NULL), (3, 'pending', 0x00)")
	require.NoError(t, err)

	targetTable := table.NewTableInfo(targetDB, target.DBName, "transfers")
	require.NoError(t, targetTable.SetInfo(t.Context()))

	applier, err := NewSingleTargetApplier(Target{DB: targetDB, Config: target, KeyRange: "0"}, NewApplierDefaultConfig())
	require.NoError(t, err)

	// The application updates only `state`; the binlog row images still carry
	// the full row, so the blobs are rewritten with their existing values.
	upsertRows := []LogicalRow{
		{RowImage: []any{int64(1), "settled", []byte{}}},
		{RowImage: []any{int64(2), "settled", nil}},
		{RowImage: []any{int64(3), "settled", []byte{0x00}}},
	}
	_, err = applier.UpsertRows(t.Context(), table.NewColumnMapping(targetTable, targetTable, nil), upsertRows, nil)
	require.NoError(t, err)

	// The REPLACE must preserve all three distinct values: empty, NULL, and a
	// one-byte NUL.
	rows, err := targetDB.QueryContext(t.Context(),
		"SELECT id, state, instrument_proto IS NULL, COALESCE(LENGTH(instrument_proto), -1) FROM transfers ORDER BY id")
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)

	expected := []struct {
		isNull bool
		length int64
	}{
		{false, 0}, // id=1: empty string stays zero-length, NOT 0x00
		{true, -1}, // id=2: NULL stays NULL
		{false, 1}, // id=3: a real one-byte NUL stays one byte
	}
	i := 0
	for rows.Next() {
		var id, length int64
		var state string
		var isNull bool
		require.NoError(t, rows.Scan(&id, &state, &isNull, &length))
		require.Equal(t, "settled", state)
		require.Equal(t, expected[i].isNull, isNull, "row id=%d nullness", id)
		require.Equal(t, expected[i].length, length, "row id=%d blob length", id)
		i++
	}
	require.NoError(t, rows.Err())
	require.Equal(t, 3, i)
}

// TestSingleTargetApplierUpsertRowsWithGeneratedColumns is a regression test
// for the bug where UpsertRows indexed RowImage via NonGeneratedColumns
// positions, causing values from the wrong source columns to be inserted when
// the source table has STORED generated columns earlier in column order.
//
// The reproduction places a STORED generated column between two non-generated
// columns, with a TIMESTAMP(3) further along. Pre-fix, the varchar that
// follows the generated column shifted into the TIMESTAMP slot, producing a
// "Data truncated for column" warning that bubbled up as a fatal upsert
// error.
func TestSingleTargetApplierUpsertRowsWithGeneratedColumns(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS single_upsert_genco_test")
	testutils.RunSQL(t, "CREATE DATABASE single_upsert_genco_test")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	target := base.Clone()
	target.DBName = "single_upsert_genco_test"
	targetDB, err := sql.Open("block-mysql", target.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(targetDB)

	// Schema with a STORED generated column sitting between two regular
	// columns, then a TIMESTAMP(3) further along. If the applier indexes
	// the row image via NonGeneratedColumns positions, the varchar after
	// the generated column shifts into the TIMESTAMP slot and MySQL
	// reports "Data truncated for column 'updated_at'".
	createTableSQL := `CREATE TABLE test_table (
		id INT PRIMARY KEY,
		amount BIGINT,
		is_set TINYINT(1) GENERATED ALWAYS AS (IF(amount IS NOT NULL, 1, NULL)) STORED,
		balance_change_id VARCHAR(255),
		updated_at TIMESTAMP(3) NULL DEFAULT NULL
	)`
	_, err = targetDB.ExecContext(t.Context(), createTableSQL)
	require.NoError(t, err)

	targetTable := table.NewTableInfo(targetDB, target.DBName, "test_table")
	err = targetTable.SetInfo(t.Context())
	require.NoError(t, err)

	tar := Target{
		DB:       targetDB,
		Config:   target,
		KeyRange: "0",
	}
	applier, err := NewSingleTargetApplier(tar, NewApplierDefaultConfig())
	require.NoError(t, err)

	// RowImage has all columns including the generated one (position 2),
	// mirroring what the binlog reader produces.
	upsertRows := []LogicalRow{
		{
			RowImage:  []any{int64(1), int64(100), int64(1), "abc-123", "2026-05-20 17:16:12.123"},
			IsDeleted: false,
		},
	}

	_, err = applier.UpsertRows(t.Context(), table.NewColumnMapping(targetTable, targetTable, nil), upsertRows, nil)
	require.NoError(t, err)

	var amount int64
	var balanceChangeID, updatedAt string
	err = targetDB.QueryRowContext(t.Context(), "SELECT amount, balance_change_id, updated_at FROM test_table WHERE id = 1").Scan(&amount, &balanceChangeID, &updatedAt)
	require.NoError(t, err)
	require.Equal(t, int64(100), amount)
	require.Equal(t, "abc-123", balanceChangeID)
	require.Equal(t, "2026-05-20 17:16:12.123", updatedAt)
}

// TestSingleTargetApplierUpsertRowsSkipDeleted tests that deleted rows are skipped
func TestSingleTargetApplierUpsertRowsSkipDeleted(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS single_upsert_deleted_test")
	testutils.RunSQL(t, "CREATE DATABASE single_upsert_deleted_test")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	target := base.Clone()
	target.DBName = "single_upsert_deleted_test"
	targetDB, err := sql.Open("block-mysql", target.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(targetDB)

	createTableSQL := `CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(100))`
	_, err = targetDB.ExecContext(t.Context(), createTableSQL)
	require.NoError(t, err)

	targetTable := table.NewTableInfo(targetDB, target.DBName, "test_table")
	err = targetTable.SetInfo(t.Context())
	require.NoError(t, err)

	tar := Target{
		DB:       targetDB,
		Config:   target,
		KeyRange: "0",
	}
	applier, err := NewSingleTargetApplier(tar, NewApplierDefaultConfig())
	require.NoError(t, err)

	// Upsert with deleted rows mixed in
	upsertRows := []LogicalRow{
		{
			RowImage:  []any{int64(1), "Alice"},
			IsDeleted: false,
		},
		{
			RowImage:  []any{int64(2), "Bob"},
			IsDeleted: true, // This should be skipped
		},
		{
			RowImage:  []any{int64(3), "Charlie"},
			IsDeleted: false,
		},
	}

	affectedRows, err := applier.UpsertRows(t.Context(), table.NewColumnMapping(targetTable, targetTable, nil), upsertRows, nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), affectedRows)

	// Verify only non-deleted rows were inserted
	var count int
	err = targetDB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM test_table").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 2, count)
}

// TestSingleTargetApplierUpsertRowsEmpty tests UpsertRows with empty row list
func TestSingleTargetApplierUpsertRowsEmpty(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS single_upsert_empty_test")
	testutils.RunSQL(t, "CREATE DATABASE single_upsert_empty_test")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	target := base.Clone()
	target.DBName = "single_upsert_empty_test"
	targetDB, err := sql.Open("block-mysql", target.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(targetDB)

	createTableSQL := `CREATE TABLE test_table (id INT PRIMARY KEY)`
	_, err = targetDB.ExecContext(t.Context(), createTableSQL)
	require.NoError(t, err)

	targetTable := table.NewTableInfo(targetDB, target.DBName, "test_table")
	err = targetTable.SetInfo(t.Context())
	require.NoError(t, err)

	tar := Target{
		DB:       targetDB,
		Config:   target,
		KeyRange: "0",
	}
	applier, err := NewSingleTargetApplier(tar, NewApplierDefaultConfig())
	require.NoError(t, err)

	// Upsert with empty rows
	affectedRows, err := applier.UpsertRows(t.Context(), table.NewColumnMapping(targetTable, targetTable, nil), []LogicalRow{}, nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), affectedRows)
}

// TestSingleTargetApplierContextCancellation tests that context cancellation stops processing
func TestSingleTargetApplierContextCancellation(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS single_cancel_test")
	testutils.RunSQL(t, "CREATE DATABASE single_cancel_test")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	target := base.Clone()
	target.DBName = "single_cancel_test"
	targetDB, err := sql.Open("block-mysql", target.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(targetDB)

	createTableSQL := `CREATE TABLE test_table (id INT PRIMARY KEY, value INT)`
	_, err = targetDB.ExecContext(t.Context(), createTableSQL)
	require.NoError(t, err)

	targetTable := table.NewTableInfo(targetDB, target.DBName, "test_table")
	err = targetTable.SetInfo(t.Context())
	require.NoError(t, err)

	tar := Target{
		DB:       targetDB,
		Config:   target,
		KeyRange: "0",
	}
	applier, err := NewSingleTargetApplier(tar, NewApplierDefaultConfig())
	require.NoError(t, err)

	// Create a cancellable context
	cancelCtx, cancel := context.WithCancel(t.Context())

	err = applier.Start(cancelCtx)
	require.NoError(t, err)
	defer func() {
		_ = applier.Stop() // Ignore error as context is cancelled
	}()

	// Create a large dataset
	testRows := make([][]any, 1000)
	for i := range 1000 {
		testRows[i] = []any{int64(i + 1), int64(i)}
	}

	chunk := &table.Chunk{
		Table:         targetTable,
		NewTable:      targetTable,
		ColumnMapping: table.NewColumnMapping(targetTable, targetTable, nil),
	}

	// Hold the callback open until Wait() has returned. Wait() counts an
	// executing callback as pending work (see callbacksInFlight), so this
	// guarantees Wait(cancelledCtx) observes pending work for the whole
	// duration of the call and must return ctx.Err(). Without this the test
	// is racy: the work can complete entirely before Wait's first poll, in
	// which case Wait correctly returns nil.
	callbackRelease := make(chan struct{})
	callback := func(affectedRows int64, err error) {
		<-callbackRelease
	}

	err = applier.Apply(cancelCtx, chunk, testRows, callback)
	require.NoError(t, err)
	// Cancel the context
	cancel()

	// Wait should return context cancelled error
	err = applier.Wait(cancelCtx)
	close(callbackRelease)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}

// TestSingleTargetApplierCancelCallbackRace stress-tests the interaction
// between the feedbackCoordinator's error path and Apply's ctx-cancel cleanup.
// Both paths can invoke the work's callback; the completion invariant (see the
// pendingWork field docs) requires that exactly one of them claims the work,
// so the callback runs exactly once.
//
// Historically (after the original #765 fix) the coordinator's error path
// released pendingMutex between invoking the callback and deleting the
// pendingWork entry. If Apply was concurrently blocked sending chunklets and
// its ctx was cancelled inside that window, Apply's cleanup found the entry
// still in the map, deleted it, and invoked the callback a second time. With
// real callbacks — e.g. the checksum recopier's, which sends to a capacity-1
// done channel and may return without draining it — the second invocation can
// block forever on the coordinator goroutine, wedging the whole applier.
//
// This test forces the window deterministically: every write fails fast (the
// target table is dropped; error 1146 is fatal and not retried), and the first
// callback invocation cancels Apply's context — while Apply is still blocked
// sending chunklets, because the work splits into more chunklets than the
// buffer holds — then parks until Apply has returned. Buggy code double-invokes
// the callback on effectively every iteration; correct code invokes it exactly
// once.
func TestSingleTargetApplierCancelCallbackRace(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS single_cbrace_test")
	testutils.RunSQL(t, "CREATE DATABASE single_cbrace_test")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	target := base.Clone()
	target.DBName = "single_cbrace_test"
	targetDB, err := sql.Open("block-mysql", target.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(targetDB)

	_, err = targetDB.ExecContext(t.Context(), `CREATE TABLE test_table (id INT PRIMARY KEY, value INT)`)
	require.NoError(t, err)

	targetTable := table.NewTableInfo(targetDB, target.DBName, "test_table")
	require.NoError(t, targetTable.SetInfo(t.Context()))

	// Drop the table so that every chunklet write fails immediately with a
	// fatal (non-retried) error, producing error completions while Apply is
	// still sending chunklets.
	_, err = targetDB.ExecContext(t.Context(), "DROP TABLE test_table")
	require.NoError(t, err)

	chunk := &table.Chunk{
		Table:         targetTable,
		NewTable:      targetTable,
		ColumnMapping: table.NewColumnMapping(targetTable, targetTable, nil),
	}

	// Enough rows that Apply splits the work into more chunklets than fit in
	// chunkletBuffer — Apply blocks mid-send, which is the precondition for
	// its ctx-cancel cleanup path to race the coordinator.
	rows := make([][]any, (defaultBufferSize+32)*chunkletMaxRows)
	for i := range rows {
		rows[i] = []any{int64(i), int64(i)}
	}

	for iteration := range 10 {
		ctx, cancel := context.WithCancel(t.Context())
		cfg := NewApplierDefaultConfig()
		cfg.Threads = 1 // slow consumption so Apply stays blocked mid-send
		applier, err := NewSingleTargetApplier(Target{DB: targetDB, Config: target}, cfg)
		require.NoError(t, err)
		require.NoError(t, applier.Start(ctx))

		var callbackCount atomic.Int32
		applyReturned := make(chan struct{})
		callback := func(_ int64, err error) {
			if callbackCount.Add(1) == 1 {
				// First invocation: cancel Apply's ctx while it is still
				// blocked sending chunklets, then park until Apply has
				// returned. This holds the completion path open across
				// Apply's cleanup, maximally exposing any window in which
				// the work has not yet been claimed.
				cancel()
				<-applyReturned
			}
		}

		applyErr := make(chan error, 1)
		go func() {
			applyErr <- applier.Apply(ctx, chunk, rows, callback)
		}()
		err = <-applyErr
		close(applyReturned)
		// Apply must have been interrupted by the cancel issued from the
		// callback (the buffer cannot drain 32+ chunklets of failed writes
		// before the first error completion is processed).
		require.ErrorIs(t, err, context.Canceled, "iteration %d", iteration)

		// Stop drains the remaining buffered chunklets (their completions
		// are dropped as unknown work) and joins the coordinator.
		require.NoError(t, applier.Stop())
		require.Equal(t, int32(1), callbackCount.Load(),
			"iteration %d: callback invoked %d times, want exactly 1 (a second invocation means the "+
				"coordinator error path and Apply's cancel cleanup both claimed the same work)",
			iteration, callbackCount.Load())
		cancel()
	}
}

// TestSingleTargetApplierWaitTimeout tests Wait with a timeout context
func TestSingleTargetApplierWaitTimeout(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS single_timeout_test")
	testutils.RunSQL(t, "CREATE DATABASE single_timeout_test")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	target := base.Clone()
	target.DBName = "single_timeout_test"
	targetDB, err := sql.Open("block-mysql", target.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(targetDB)

	createTableSQL := `CREATE TABLE test_table (id INT PRIMARY KEY)`
	_, err = targetDB.ExecContext(t.Context(), createTableSQL)
	require.NoError(t, err)

	targetTable := table.NewTableInfo(targetDB, target.DBName, "test_table")
	err = targetTable.SetInfo(t.Context())
	require.NoError(t, err)

	tar := Target{
		DB:       targetDB,
		Config:   target,
		KeyRange: "0",
	}
	applier, err := NewSingleTargetApplier(tar, NewApplierDefaultConfig())
	require.NoError(t, err)

	err = applier.Start(t.Context())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, applier.Stop())
	}()

	// Apply some work
	testRows := [][]any{{int64(1)}, {int64(2)}}
	chunk := &table.Chunk{
		Table:         targetTable,
		NewTable:      targetTable,
		ColumnMapping: table.NewColumnMapping(targetTable, targetTable, nil),
	}

	callback := func(affectedRows int64, err error) {}
	err = applier.Apply(t.Context(), chunk, testRows, callback)
	require.NoError(t, err)

	// Create a context with very short timeout
	timeoutCtx, cancel := context.WithTimeout(t.Context(), 1*time.Nanosecond)
	defer cancel()

	// Wait should timeout (or succeed if fast enough)
	err = applier.Wait(timeoutCtx)
	// Either succeeds or times out, both are acceptable
	if err != nil {
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}
}

// TestSingleTargetApplierStartClose tests the lifecycle of starting and closing
func TestSingleTargetApplierStartClose(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS single_lifecycle_test")
	testutils.RunSQL(t, "CREATE DATABASE single_lifecycle_test")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	target := base.Clone()
	target.DBName = "single_lifecycle_test"
	targetDB, err := sql.Open("block-mysql", target.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(targetDB)

	createTableSQL := `CREATE TABLE test_table (id INT PRIMARY KEY)`
	_, err = targetDB.ExecContext(t.Context(), createTableSQL)
	require.NoError(t, err)

	targetTable := table.NewTableInfo(targetDB, target.DBName, "test_table")
	err = targetTable.SetInfo(t.Context())
	require.NoError(t, err)

	tar := Target{
		DB:       targetDB,
		Config:   target,
		KeyRange: "0",
	}
	applier, err := NewSingleTargetApplier(tar, NewApplierDefaultConfig())
	require.NoError(t, err)

	// Start the applier
	err = applier.Start(t.Context())
	require.NoError(t, err)

	// Apply some work
	testRows := [][]any{{int64(1)}, {int64(2)}}
	chunk := &table.Chunk{
		Table:         targetTable,
		NewTable:      targetTable,
		ColumnMapping: table.NewColumnMapping(targetTable, targetTable, nil),
	}

	var callbackInvoked atomic.Bool
	callback := func(affectedRows int64, err error) {
		callbackInvoked.Store(true)
		require.NoError(t, err)
	}

	err = applier.Apply(t.Context(), chunk, testRows, callback)
	require.NoError(t, err)

	// Wait for work to complete
	err = applier.Wait(t.Context())
	require.NoError(t, err)
	require.True(t, callbackInvoked.Load())

	// Close the applier
	err = applier.Stop()
	require.NoError(t, err)

	// Verify data was written
	var count int
	err = targetDB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM test_table").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 2, count)
}

// TestSingleTargetApplierDynamicScaling exercises SetWriteWorkers: the live
// worker count moves up and down at runtime, data is applied correctly under
// both, the count clamps at a minimum of 1, and shutdown stays clean (goleak in
// TestMain catches any leaked worker).
func TestSingleTargetApplierDynamicScaling(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS single_scaling_test")
	testutils.RunSQL(t, "CREATE DATABASE single_scaling_test")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	target := base.Clone()
	target.DBName = "single_scaling_test"
	// The pool must cover the peak worker count we scale to.
	target.Params = map[string]string{}
	targetDB, err := sql.Open("block-mysql", target.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(targetDB)
	targetDB.SetMaxOpenConns(10)

	_, err = targetDB.ExecContext(t.Context(), `CREATE TABLE test_table (id INT PRIMARY KEY, val INT)`)
	require.NoError(t, err)

	targetTable := table.NewTableInfo(targetDB, target.DBName, "test_table")
	require.NoError(t, targetTable.SetInfo(t.Context()))

	tar := Target{DB: targetDB, Config: target, KeyRange: "0"}
	cfg := NewApplierDefaultConfig()
	cfg.Threads = 2 // start value
	appl, err := NewSingleTargetApplier(tar, cfg)
	require.NoError(t, err)

	require.NoError(t, appl.Start(t.Context()))
	defer func() { require.NoError(t, appl.Stop()) }()

	// Initial pool matches the start value (spawn increments synchronously).
	require.Equal(t, 2, appl.ActiveWriteWorkers())

	chunk := &table.Chunk{
		Table:         targetTable,
		NewTable:      targetTable,
		ColumnMapping: table.NewColumnMapping(targetTable, targetTable, nil),
	}

	// applyN applies rows [start, start+n) and waits for them to land.
	applyN := func(start, n int) {
		rows := make([][]any, n)
		for i := range n {
			rows[i] = []any{int64(start + i), int64(start + i)}
		}
		var cbErr error
		var done atomic.Bool
		require.NoError(t, appl.Apply(t.Context(), chunk, rows, func(_ int64, err error) {
			cbErr = err
			done.Store(true)
		}))
		require.NoError(t, appl.Wait(t.Context()))
		require.True(t, done.Load())
		require.NoError(t, cbErr)
	}

	// Scale up, then apply work under the larger pool.
	appl.SetWriteWorkers(6)
	require.Equal(t, 6, appl.ActiveWriteWorkers())
	applyN(0, 1500) // > chunkletMaxRows so it splits across workers

	// Scale down. Parked workers exit asynchronously (after finishing any
	// in-flight chunklet), so the count converges rather than dropping instantly.
	appl.SetWriteWorkers(1)
	require.Eventually(t, func() bool {
		return appl.ActiveWriteWorkers() == 1
	}, 5*time.Second, 5*time.Millisecond)
	applyN(1500, 500)

	// Clamp: requesting < 1 holds at 1.
	appl.SetWriteWorkers(0)
	require.Equal(t, 1, appl.ActiveWriteWorkers())

	// All rows from both batches must be present.
	var count int
	require.NoError(t, targetDB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM test_table").Scan(&count))
	require.Equal(t, 2000, count)
}

// TestSingleTargetApplierUnderLock verifies the under-lock write path:
// with one lock the statements execute on the lock's own transaction, and
// supplying more than one lock is rejected (the single-target applier
// writes through exactly one server, so multiple locks indicate a caller
// bug such as per-shard locks being passed to the wrong applier).
func TestSingleTargetApplierUnderLock(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS test_pr4_single_lock")
	testutils.RunSQL(t, "CREATE DATABASE test_pr4_single_lock")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	target := base.Clone()
	target.DBName = "test_pr4_single_lock"
	targetDB, err := sql.Open("block-mysql", target.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(targetDB)

	ctx := t.Context()
	_, err = targetDB.ExecContext(ctx, `CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(100))`)
	require.NoError(t, err)
	_, err = targetDB.ExecContext(ctx, "INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob')")
	require.NoError(t, err)

	targetTable := table.NewTableInfo(targetDB, target.DBName, "test_table")
	require.NoError(t, targetTable.SetInfo(ctx))

	applier, err := NewSingleTargetApplier(Target{DB: targetDB, Config: target, KeyRange: "0"}, NewApplierDefaultConfig())
	require.NoError(t, err)

	lock, err := dbconn.NewTableLock(ctx, targetDB, []*table.TableInfo{targetTable}, dbconn.NewDBConfig(), slog.Default())
	require.NoError(t, err)

	// More than one lock must be rejected before executing anything.
	_, err = applier.UpsertRows(ctx, table.NewColumnMapping(targetTable, targetTable, nil),
		[]LogicalRow{{RowImage: []any{int64(3), "Charlie"}}}, []*dbconn.TableLock{lock, lock})
	require.ErrorContains(t, err, "at most one table lock")
	_, err = applier.DeleteKeys(ctx, targetTable, targetTable,
		[][]any{{int64(1)}}, []*dbconn.TableLock{lock, lock})
	require.ErrorContains(t, err, "at most one table lock")

	// A single lock executes on the lock transaction.
	affected, err := applier.UpsertRows(ctx, table.NewColumnMapping(targetTable, targetTable, nil),
		[]LogicalRow{{RowImage: []any{int64(3), "Charlie"}}}, []*dbconn.TableLock{lock})
	require.NoError(t, err)
	require.Equal(t, int64(1), affected)
	_, err = applier.DeleteKeys(ctx, targetTable, targetTable,
		[][]any{{int64(2)}}, []*dbconn.TableLock{lock})
	require.NoError(t, err)

	require.NoError(t, lock.Close(ctx))

	var ids []int64
	rows, err := targetDB.QueryContext(ctx, "SELECT id FROM test_table ORDER BY id")
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)
	for rows.Next() {
		var id int64
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []int64{1, 3}, ids, "id=2 deleted and id=3 inserted under lock")
}

// TestSingleTargetApplierCallbackPanicDecrements verifies that a panicking
// callback still decrements callbacksInFlight, so a recovered panic upstream
// cannot wedge Wait() forever. invokeCallback must balance the counter on every
// exit path, not just normal return.
func TestSingleTargetApplierCallbackPanicDecrements(t *testing.T) {
	a := &SingleTargetApplier{
		logger:      slog.New(slog.DiscardHandler),
		pendingWork: make(map[int64]*pendingWork),
	}

	// Claim a unit of work the way the real claim paths do: increment
	// callbacksInFlight under pendingMutex before invoking the callback.
	a.pendingMutex.Lock()
	a.callbacksInFlight++
	a.pendingMutex.Unlock()

	panicking := func(affectedRows int64, err error) {
		panic("callback boom")
	}

	// invokeCallback must propagate the panic; recover it here the way an
	// upstream goroutine would.
	func() {
		defer func() {
			require.Equal(t, "callback boom", recover())
		}()
		a.invokeCallback(panicking, 0, nil)
	}()

	// Despite the panic, the deferred decrement must have run, so Wait()
	// observes a balanced counter and returns promptly.
	a.pendingMutex.Lock()
	require.Equal(t, 0, a.callbacksInFlight)
	a.pendingMutex.Unlock()

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	require.NoError(t, a.Wait(ctx))
}

// TestSingleTargetApplierSetWriteWorkersBeforeStart verifies the exported
// SetWriteWorkers is a safe no-op when called before Start(): workerCtx is
// still nil, so spawning a worker would panic on its first writeChunklet
// (ctx.Err()). It must spawn nothing rather than panic.
func TestSingleTargetApplierSetWriteWorkersBeforeStart(t *testing.T) {
	a := &SingleTargetApplier{
		logger: slog.New(slog.DiscardHandler),
	}
	require.NotPanics(t, func() { a.SetWriteWorkers(4) })
	require.Equal(t, 0, a.ActiveWriteWorkers(), "no workers should be spawned before Start")
}
