package dbconn

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

const slowStatementReport = "a statement under the table lock is still running; logging the metadata locks on its server"

// TestExecUnderLockReportsSlowStatement: a statement under the lock that runs
// past the threshold logs what the lock session is doing and the metadata
// locks on the server, granted and pending, and logs them again each time its
// running time doubles. Statements appear only as digests, without their
// literals. A fast statement logs nothing.
func TestExecUnderLockReportsSlowStatement(t *testing.T) {
	threshold := slowLockedStatementThreshold
	slowLockedStatementThreshold = 100 * time.Millisecond
	t.Cleanup(func() { slowLockedStatementThreshold = threshold })

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE slowlock (id INT NOT NULL PRIMARY KEY)")
	db, err := New(testutils.DSNForDatabase(dbName), NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	logs := &lockedBuffer{}
	logger := slog.New(slog.NewJSONHandler(logs, nil))
	tbl := &table.TableInfo{SchemaName: dbName, TableName: "slowlock", QuotedTableName: "`slowlock`"}
	lock, err := NewTableLock(ctx, db, []*table.TableInfo{tbl}, NewDBConfig(), logger)
	require.NoError(t, err)
	locked := true
	defer func() {
		if locked {
			utils.CloseAndLogWithContext(ctx, lock)
		}
	}()

	// A reader queued behind the lock shows as a pending metadata lock. The
	// statements' string literals must not reach the log; numbers would not
	// prove it, since timestamps, durations and addresses hold digits.
	readerDone := make(chan error, 1)
	go func() {
		_, err := db.ExecContext(ctx, "SELECT * FROM slowlock WHERE id = 1 OR 'reader-literal' = ''")
		readerDone <- err
	}()
	require.Eventually(t, func() bool {
		var pending int
		err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM performance_schema.metadata_locks
			WHERE object_schema = ? AND object_name = 'slowlock' AND lock_status = 'PENDING'`, dbName).Scan(&pending)
		return err == nil && pending > 0
	}, 10*time.Second, 10*time.Millisecond)

	logs.Reset()
	require.NoError(t, lock.ExecUnderLock(ctx, "DO SLEEP(0.7), 'locked-literal'"))
	out := logs.String()
	require.NotContains(t, out, "reader-literal", "a report must not log a statement's literals")
	require.NotContains(t, out, "locked-literal", "a report must not log a statement's literals")
	records := logRecords(t, out)

	// Reports at 100ms, 200ms and 400ms.
	var reports []map[string]any
	for _, r := range records {
		if r["msg"] == slowStatementReport {
			reports = append(reports, r)
		}
	}
	require.Len(t, reports, 3)
	require.Equal(t, "User sleep", reports[0]["lock_session_state"])
	require.NotEmpty(t, reports[0]["server"])
	require.Equal(t, "DO `SLEEP` (?) , ?", reports[0]["statement_digest"])
	require.InDelta(t, float64(lock.lockPID), reports[0]["lock_session"], 0)

	var granted, pending bool
	for _, r := range records {
		if r["msg"] != "metadata lock" {
			continue
		}
		require.Equal(t, reports[0]["server"], r["server"], "every lock record names its server")
		require.NotNil(t, r["owner_thread"], "every lock record names its owner thread")
		digest, _ := r["statement_digest"].(string)
		require.False(t, strings.HasPrefix(digest, "SELECT `ml` . `owner_thread_id`"), "the report must not list its own locks")
		if r["object_schema"] != dbName || r["object_name"] != "slowlock" {
			continue
		}
		switch {
		case r["lock_status"] == "GRANTED" && r["lock_type"] == "SHARED_NO_READ_WRITE":
			require.InDelta(t, float64(lock.lockPID), r["pid"], 0, "the table lock is held by the lock session")
			granted = true
		case r["lock_status"] == "PENDING":
			require.Equal(t, "SELECT * FROM `slowlock` WHERE `id` = ? OR ? = ?", r["statement_digest"])
			pending = true
		}
	}
	require.True(t, granted, "the report lists the lock session's table lock")
	require.True(t, pending, "the report lists the reader waiting for the table")

	logs.Reset()
	require.NoError(t, lock.ExecUnderLock(ctx, "DO 1"))
	require.NotContains(t, logs.String(), slowStatementReport)

	require.NoError(t, lock.Close(ctx))
	locked = false
	require.NoError(t, <-readerDone)
}

// TestSlowStatementReportKeepsTheLockSessionFirst: when more locks are held
// or awaited than a report logs, the report keeps the lock session's own
// locks ahead of the queries queued behind it, and says it was truncated. A
// zero threshold disables the reports.
func TestSlowStatementReportKeepsTheLockSessionFirst(t *testing.T) {
	threshold, limit := slowLockedStatementThreshold, metadataLockReportLimit
	slowLockedStatementThreshold = 100 * time.Millisecond
	t.Cleanup(func() { slowLockedStatementThreshold, metadataLockReportLimit = threshold, limit })

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE busy (id INT NOT NULL PRIMARY KEY)")
	// The readers' sessions exist before the lock session, so their
	// processlist ids sort ahead of its own.
	app, err := New(testutils.DSNForDatabase(dbName), NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(app)
	const readers = 20
	app.SetMaxOpenConns(readers)
	warm := make([]*sql.Conn, readers)
	for i := range warm {
		warm[i], err = app.Conn(ctx)
		require.NoError(t, err)
	}
	for _, c := range warm {
		require.NoError(t, c.Close())
	}

	db, err := New(testutils.DSNForDatabase(dbName), NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	logs := &lockedBuffer{}
	logger := slog.New(slog.NewJSONHandler(logs, nil))
	tbl := &table.TableInfo{SchemaName: dbName, TableName: "busy", QuotedTableName: "`busy`"}
	lock, err := NewTableLock(ctx, db, []*table.TableInfo{tbl}, NewDBConfig(), logger)
	require.NoError(t, err)
	locked := true
	defer func() {
		if locked {
			utils.CloseAndLogWithContext(ctx, lock)
		}
	}()
	var queued sync.WaitGroup
	for range readers {
		queued.Go(func() { _, _ = app.ExecContext(ctx, "SELECT * FROM busy") })
	}
	require.Eventually(t, func() bool {
		var pending int
		err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM performance_schema.metadata_locks
			WHERE object_schema = ? AND object_name = 'busy' AND lock_status = 'PENDING'`, dbName).Scan(&pending)
		return err == nil && pending == readers
	}, 10*time.Second, 10*time.Millisecond)

	logs.Reset()
	require.NoError(t, lock.ExecUnderLock(ctx, "DO SLEEP(0.15)"))
	records := logRecords(t, logs.String())
	require.Equal(t, slowStatementReport, records[0]["msg"])
	require.Equal(t, false, records[0]["truncated"])
	report := records[1:]
	ownRows := 0
	for ownRows < len(report) && report[ownRows]["pid"] == float64(lock.lockPID) {
		ownRows++
	}
	require.Positive(t, ownRows, "the lock session's own locks come first")
	for _, r := range report[ownRows:] {
		require.NotEqual(t, float64(lock.lockPID), r["pid"])
	}
	require.Equal(t, "PENDING", report[ownRows]["lock_status"], "the queued readers follow")

	// Truncated to the limit.
	metadataLockReportLimit = ownRows + 2
	logs.Reset()
	require.NoError(t, lock.ExecUnderLock(ctx, "DO SLEEP(0.15)"))
	records = logRecords(t, logs.String())
	require.Equal(t, slowStatementReport, records[0]["msg"])
	require.Equal(t, true, records[0]["truncated"])
	require.InDelta(t, float64(ownRows+2), records[0]["metadata_locks"], 0)
	require.Len(t, records, ownRows+3, "a header and the first locks up to the limit")
	require.Equal(t, "PENDING", records[len(records)-1]["lock_status"], "the queued readers follow")

	// Disabled.
	slowLockedStatementThreshold = 0
	logs.Reset()
	require.NoError(t, lock.ExecUnderLock(ctx, "DO SLEEP(0.15)"))
	require.NotContains(t, logs.String(), slowStatementReport)

	require.NoError(t, lock.Close(ctx))
	locked = false
	queued.Wait()
}

// lockedBuffer is an io.Writer safe for concurrent use, for capturing logs.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *lockedBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Reset()
}

// logRecords parses the JSON log lines in out.
func logRecords(t *testing.T, out string) []map[string]any {
	t.Helper()
	var records []map[string]any
	for line := range strings.SplitSeq(strings.TrimSpace(out), "\n") {
		if line == "" {
			continue
		}
		var r map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &r))
		records = append(records, r)
	}
	return records
}
