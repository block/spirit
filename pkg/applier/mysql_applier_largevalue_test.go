package applier

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"log/slog"
	"sync"
	"testing"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/contract"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// largeBinaryValue returns a binary value that fits in max_allowed_packet as
// stored bytes but not once it is rendered as a 0x-hex literal, which doubles
// it. The default max_allowed_packet is 64 MiB on both the server and the Go
// MySQL driver, so a 40 MiB value is one a table can hold and an INSERT built
// from hex literals cannot carry. The bytes are not valid UTF-8, so a value
// that went through a character set conversion would not compare equal.
func largeBinaryValue(t *testing.T) []byte {
	t.Helper()
	var maxAllowedPacket int
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT @@max_allowed_packet").Scan(&maxAllowedPacket))
	size := 40 << 20
	require.Less(t, size, maxAllowedPacket, "the value must fit in max_allowed_packet as stored bytes")
	require.Greater(t, 2*size+2, maxAllowedPacket, "the value must not fit in max_allowed_packet as a hex literal")

	value := make([]byte, size)
	for i := range value {
		value[i] = byte(0x80 + i%127)
	}
	return value
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// openLargeValueTarget creates a fresh database holding one table with a
// LONGBLOB column, and returns a connection to it with its table info.
func openLargeValueTarget(t *testing.T, dbName string) (*sql.DB, *mysql.Config, *table.TableInfo) {
	t.Helper()
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+dbName)
	testutils.RunSQL(t, "CREATE DATABASE "+dbName)

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	target := base.Clone()
	target.DBName = dbName
	targetDB, err := sql.Open("block-mysql", target.FormatDSN())
	require.NoError(t, err)
	t.Cleanup(func() { _ = targetDB.Close() })

	_, err = targetDB.ExecContext(t.Context(),
		"CREATE TABLE reports (id INT PRIMARY KEY, note VARCHAR(20), encrypted_data LONGBLOB)")
	require.NoError(t, err)
	targetTable := table.NewTableInfo(targetDB, dbName, "reports")
	require.NoError(t, targetTable.SetInfo(t.Context()))
	return targetDB, target, targetTable
}

// requireReportsRows checks the reports table holds exactly the expected
// encrypted_data per id, compared by length and SHA-256 on the server.
func requireReportsRows(t *testing.T, db *sql.DB, want map[int64][]byte) {
	t.Helper()
	rows, err := db.QueryContext(t.Context(),
		"SELECT id, LENGTH(encrypted_data), SHA2(encrypted_data, 256) FROM reports ORDER BY id")
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)
	seen := 0
	for rows.Next() {
		var id, length int64
		var digest string
		require.NoError(t, rows.Scan(&id, &length, &digest))
		expected, ok := want[id]
		require.True(t, ok, "unexpected row id=%d", id)
		require.Equal(t, int64(len(expected)), length, "row id=%d length", id)
		require.Equal(t, sha256Hex(expected), digest, "row id=%d contents", id)
		seen++
	}
	require.NoError(t, rows.Err())
	require.Equal(t, len(want), seen)
}

// TestSingleTargetApplierCopiesLargeBinaryValue copies a row whose binary
// value fits in max_allowed_packet but whose hex rendering does not. A table
// can hold such a row, so the copy must be able to move it: the row lands
// byte-exact alongside the ordinary rows of the same chunk.
func TestSingleTargetApplierCopiesLargeBinaryValue(t *testing.T) {
	large := largeBinaryValue(t)
	targetDB, target, targetTable := openLargeValueTarget(t, "single_large_value_copy_test")

	applier, err := New([]Target{{DB: targetDB, Config: target, KeyRange: "0"}}, NewApplierDefaultConfig())
	require.NoError(t, err)
	require.NoError(t, applier.Start(t.Context()))
	defer func() { require.NoError(t, applier.Stop()) }()

	small := []byte{0x00, 0xff, 0x27, 0x5c}
	chunk := &table.Chunk{
		Table:         targetTable,
		NewTable:      targetTable,
		ColumnMapping: table.NewColumnMapping(targetTable, targetTable, nil),
	}
	var (
		mu          sync.Mutex
		callbackErr error
		affected    int64
	)
	err = applier.Apply(t.Context(), chunk, [][]any{
		{int64(1), "before", small},
		{int64(2), "large", large},
		{int64(3), "after", []byte{}},
	}, func(n int64, err error) {
		mu.Lock()
		defer mu.Unlock()
		affected, callbackErr = n, err
	})
	require.NoError(t, err)
	require.NoError(t, applier.Wait(t.Context()))

	mu.Lock()
	require.NoError(t, callbackErr)
	require.Equal(t, int64(3), affected)
	mu.Unlock()

	requireReportsRows(t, targetDB, map[int64][]byte{1: small, 2: large, 3: {}})
}

// TestSingleTargetApplierUpsertsLargeBinaryValue replays a row image whose
// binary value fits in max_allowed_packet but whose hex rendering does not, as
// the binlog path does when the application updates such a row during the
// copy. The row is written byte-exact.
func TestSingleTargetApplierUpsertsLargeBinaryValue(t *testing.T) {
	large := largeBinaryValue(t)
	targetDB, target, targetTable := openLargeValueTarget(t, "single_large_value_upsert_test")

	_, err := targetDB.ExecContext(t.Context(), "INSERT INTO reports VALUES (1, 'old', 0x01), (2, 'old', 0x02)")
	require.NoError(t, err)

	applier, err := New([]Target{{DB: targetDB, Config: target, KeyRange: "0"}}, NewApplierDefaultConfig())
	require.NoError(t, err)

	small := []byte{0x00, 0xff}
	_, err = applier.UpsertRows(t.Context(), table.NewColumnMapping(targetTable, targetTable, nil), []contract.LogicalRow{
		{RowImage: []any{int64(1), "updated", large}},
		{RowImage: []any{int64(2), "updated", small}},
	}, nil)
	require.NoError(t, err)

	requireReportsRows(t, targetDB, map[int64][]byte{1: large, 2: small})
}

// TestSingleTargetApplierUpsertsLargeBinaryValueUnderLock replays the same
// large row image through the table lock, as the final flush does at cutover
// while LOCK TABLES is held. The statement is prepared on the locking session
// and the row is written byte-exact.
func TestSingleTargetApplierUpsertsLargeBinaryValueUnderLock(t *testing.T) {
	large := largeBinaryValue(t)
	targetDB, target, targetTable := openLargeValueTarget(t, "single_large_value_lock_test")

	_, err := targetDB.ExecContext(t.Context(), "INSERT INTO reports VALUES (1, 'old', 0x01)")
	require.NoError(t, err)

	applier, err := New([]Target{{DB: targetDB, Config: target, KeyRange: "0"}}, NewApplierDefaultConfig())
	require.NoError(t, err)
	lock, err := dbconn.NewTableLock(t.Context(), targetDB, []*table.TableInfo{targetTable}, dbconn.NewDBConfig(), slog.Default())
	require.NoError(t, err)

	affected, err := applier.UpsertRows(t.Context(), table.NewColumnMapping(targetTable, targetTable, nil), []contract.LogicalRow{
		{RowImage: []any{int64(1), "updated", large}},
	}, []contract.TableLock{lock})
	require.NoError(t, err)
	require.Equal(t, int64(1), affected)
	require.NoError(t, lock.Close(t.Context()))

	requireReportsRows(t, targetDB, map[int64][]byte{1: large})
}
