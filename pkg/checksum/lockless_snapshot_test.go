package checksum

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

func snapshotTestTables(t *testing.T, ddl string, keys []string) (*sql.DB, *table.Chunk) {
	t.Helper()
	schema, db := testutils.CreateUniqueTestDatabase(t)
	for _, name := range []string{"src", "dst"} {
		_, err := db.ExecContext(t.Context(), "CREATE TABLE "+name+" ("+ddl+")")
		require.NoError(t, err)
	}
	source, target := table.NewTableInfo(db, schema, "src"), table.NewTableInfo(db, schema, "dst")
	require.NoError(t, source.SetInfo(t.Context()))
	require.NoError(t, target.SetInfo(t.Context()))
	return db, &table.Chunk{Key: keys, Table: source, NewTable: target, ColumnMapping: table.NewColumnMapping(source, target, nil)}
}

func snapshotExec(t *testing.T, db *sql.DB, query string) {
	t.Helper()
	_, err := db.ExecContext(t.Context(), query)
	require.NoError(t, err)
}

func TestHotSnapshotFiniteTail(t *testing.T) {
	db, chunk := snapshotTestTables(t, "id INT PRIMARY KEY, value INT", []string{"id"})
	snapshotExec(t, db, "INSERT INTO src VALUES (1,10),(2,20)")
	snapshotExec(t, db, "INSERT INTO dst VALUES (1,10)")
	snapshot, err := captureHotSnapshot(t.Context(), db, db, chunk)
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	passed, err := snapshot.check(t.Context())
	require.NoError(t, err)
	require.False(t, passed)
	require.Len(t, snapshot.pending, 1)
	// The source continues appending, and even changes an already matched row.
	// Neither operation may change the remaining frozen obligation for id=2.
	snapshotExec(t, db, "INSERT INTO src VALUES (3,30),(4,40)")
	snapshotExec(t, db, "UPDATE src SET value=11 WHERE id=1")
	snapshotExec(t, db, "INSERT INTO dst VALUES (2,20),(3,30)")
	passed, err = snapshot.check(t.Context())
	require.NoError(t, err)
	require.True(t, passed)
	srcCRC, dstCRC, srcCount, dstCount, err := readChunkCRC(t.Context(), db, db, chunk)
	require.NoError(t, err)
	require.NotEqual(t, chunkSig{srcCRC, srcCount}, chunkSig{dstCRC, dstCount}, "aggregate still races the growing tail")
}

func TestHotSnapshotOrphansAndDeletes(t *testing.T) {
	db, chunk := snapshotTestTables(t, "id INT PRIMARY KEY, value INT", []string{"id"})
	snapshotExec(t, db, "INSERT INTO src VALUES (1,10)")
	snapshotExec(t, db, "INSERT INTO dst VALUES (1,10),(99,99)")
	snapshot, err := captureHotSnapshot(t.Context(), db, db, chunk)
	require.NoError(t, err)
	passed, err := snapshot.check(t.Context())
	require.NoError(t, err)
	require.False(t, passed, "a target-only tail row must prevent verification")
	snapshotExec(t, db, "DELETE FROM dst WHERE id=99")
	passed, err = snapshot.check(t.Context())
	require.NoError(t, err)
	require.True(t, passed)

	snapshotExec(t, db, "INSERT INTO src VALUES (2,20)")
	snapshot, err = captureHotSnapshot(t.Context(), db, db, chunk)
	require.NoError(t, err)
	snapshotExec(t, db, "DELETE FROM src WHERE id=2")
	passed, err = snapshot.check(t.Context())
	require.NoError(t, err)
	require.False(t, passed, "absence cannot stand in for a frozen source image that never matched")
}

func TestHotSnapshotKeyIdentity(t *testing.T) {
	cases := []struct {
		name, ddl, values string
		keys              []string
	}{
		{"composite", "a VARCHAR(30), b VARCHAR(30), value INT, PRIMARY KEY(a,b)", "('a,b','c',10),('a','b,c',20)", []string{"a", "b"}},
		{"binary", "id VARBINARY(10) PRIMARY KEY, value INT", "(X'0027FF',10),(X'00',20)", []string{"id"}},
		{"temporal", "id DATETIME(6) PRIMARY KEY, value INT", "('2026-09-18 01:02:03.123456',10),('2026-09-18 01:02:03.123457',20)", []string{"id"}},
		{"unsigned", "id BIGINT UNSIGNED PRIMARY KEY, value INT", "(18446744073709551615,10),(9223372036854775808,20)", []string{"id"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, chunk := snapshotTestTables(t, tc.ddl, tc.keys)
			snapshotExec(t, db, "INSERT INTO src VALUES "+tc.values)
			snapshot, err := captureHotSnapshot(t.Context(), db, db, chunk)
			require.NoError(t, err)
			require.Len(t, snapshot.pending, 2)
			snapshotExec(t, db, "INSERT INTO dst SELECT * FROM src")
			passed, err := snapshot.check(t.Context())
			require.NoError(t, err)
			require.True(t, passed)
		})
	}
}

func TestHotSnapshotBoundsAndBudget(t *testing.T) {
	db, chunk := snapshotTestTables(t, "id INT PRIMARY KEY, value INT", []string{"id"})
	snapshotExec(t, db, "INSERT INTO src VALUES (1,10),(2,20)")
	snapshotExec(t, db, "INSERT INTO dst VALUES (1,10),(2,20),(3,30)")
	chunk.AdditionalConditions = "id < 3"
	snapshot, err := captureHotSnapshot(t.Context(), db, db, chunk)
	require.NoError(t, err)
	passed, err := snapshot.check(t.Context())
	require.NoError(t, err)
	require.True(t, passed, "target rows outside the parent are not orphans")
	chunk.AdditionalConditions = ""
	var values []string
	for i := 4; i <= 132; i++ {
		values = append(values, fmt.Sprintf("(%d,%d)", i, i))
	}
	snapshotExec(t, db, "INSERT INTO src VALUES "+strings.Join(values, ","))
	snapshot, err = captureHotSnapshot(t.Context(), db, db, chunk)
	require.NoError(t, err)
	require.Nil(t, snapshot, "row budget overflow cannot truncate verification")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = captureHotSnapshot(ctx, db, db, chunk)
	require.ErrorIs(t, err, context.Canceled)
}

func TestLocklessHotSnapshotGate(t *testing.T) {
	for _, converge := range []bool{true, false} {
		t.Run(fmt.Sprint(converge), func(t *testing.T) {
			db, chunk := snapshotTestTables(t, "id INT PRIMARY KEY, value INT", []string{"id"})
			snapshotExec(t, db, "INSERT INTO src VALUES (1,10)")
			chunker := newTestChunker(1)
			chunker.chunks[0] = chunk
			cfg := fastConfig()
			cfg.RetryDelay = time.Millisecond
			cfg.MaxHotAttempts = 4
			cfg.MinPassInterval = time.Hour
			cfg.SnapshotHotChunks = true
			c := newTestChecker(t, chunker, cfg, func(_ context.Context, _ *table.Chunk, attempt int) (int64, int64, uint64, error) {
				return int64(attempt), 0, 1, nil // aggregate keeps changing forever
			})
			c.snapshotChunk = func(ctx context.Context, chunk *table.Chunk) (*hotSnapshot, error) {
				snapshot, err := captureHotSnapshot(ctx, db, db, chunk)
				if err == nil && converge {
					_, err = db.ExecContext(ctx, "INSERT INTO dst SELECT * FROM src")
				}
				return snapshot, err
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- c.RunUntilClean(ctx) }()
			if converge {
				require.NoError(t, <-done)
				require.False(t, c.Stats().FirstCleanPassAt.IsZero())
			} else {
				require.Eventually(t, func() bool { return c.Stats().PassesCompleted == 1 }, 2*time.Second, time.Millisecond)
				require.True(t, c.Stats().FirstCleanPassAt.IsZero())
				require.Equal(t, uint64(1), c.Stats().HotChunksDeferredThisPass)
				cancel()
				require.ErrorIs(t, <-done, context.Canceled)
			}
		})
	}
}

func TestHotSnapshotCollatedOrphanIsNotAbsent(t *testing.T) {
	db, chunk := snapshotTestTables(t, "id VARCHAR(20) COLLATE utf8mb4_unicode_ci PRIMARY KEY, value INT", []string{"id"})
	snapshotExec(t, db, "INSERT INTO dst VALUES ('UPPER',1)")
	snapshot, err := captureHotSnapshot(t.Context(), db, db, chunk)
	require.NoError(t, err)
	snapshotExec(t, db, "UPDATE dst SET id='upper'")
	passed, err := snapshot.check(t.Context())
	require.NoError(t, err)
	require.False(t, passed, "collation-equivalent orphan still exists")
	snapshotExec(t, db, "DELETE FROM dst")
	passed, err = snapshot.check(t.Context())
	require.NoError(t, err)
	require.True(t, passed)
}

func TestHotSnapshotByteBudget(t *testing.T) {
	db, chunk := snapshotTestTables(t, "id VARBINARY(1000) PRIMARY KEY, value INT", []string{"id"})
	for i := range 70 {
		_, err := db.ExecContext(t.Context(), "INSERT INTO src VALUES (?,?)", fmt.Sprintf("%03d%s", i, strings.Repeat("x", 997)), i)
		require.NoError(t, err)
	}
	snapshot, err := captureHotSnapshot(t.Context(), db, db, chunk)
	require.NoError(t, err)
	require.Nil(t, snapshot, "byte overflow cannot truncate verification")
}

func TestHotSnapshotColumnMapping(t *testing.T) {
	db, chunk := snapshotTestTables(t, "id INT PRIMARY KEY, value INT", []string{"id"})
	snapshotExec(t, db, "ALTER TABLE dst CHANGE COLUMN value renamed BIGINT")
	require.NoError(t, chunk.NewTable.SetInfo(t.Context()))
	chunk.ColumnMapping = table.NewColumnMapping(chunk.Table, chunk.NewTable, map[string]string{"value": "renamed"})
	snapshotExec(t, db, "INSERT INTO src VALUES (1,10),(2,NULL)")
	snapshot, err := captureHotSnapshot(t.Context(), db, db, chunk)
	require.NoError(t, err)
	snapshotExec(t, db, "INSERT INTO dst SELECT * FROM src")
	passed, err := snapshot.check(t.Context())
	require.NoError(t, err)
	require.True(t, passed)
}
