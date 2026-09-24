package checkpoint_test

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/checkpoint"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

func setup(t *testing.T) (*sql.DB, string) {
	t.Helper()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db, cfg.DBName
}

func countRows(t *testing.T, db *sql.DB, schema, name string) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`", schema, name)).Scan(&n))
	return n
}

func TestTableLifecycleTransient(t *testing.T) {
	db, schema := setup(t)
	tbl := checkpoint.NewTable(db, "_ckpt_test_transient", checkpoint.Transient)
	t.Cleanup(func() { _ = dbconn.Exec(t.Context(), db, "DROP TABLE IF EXISTS %n.%n", schema, "_ckpt_test_transient") })

	require.NoError(t, tbl.Create(t.Context()))

	// Empty table → ErrNotFound (a normal "nothing to resume" state).
	_, err := tbl.ReadLatest(t.Context())
	require.ErrorIs(t, err, checkpoint.ErrNotFound)

	// Write a row and read every field back.
	rec := checkpoint.Record{
		CopierWatermark:   "cw1",
		ChecksumWatermark: "sw1",
		Position:          "pos1",
		Statement:         "ALTER TABLE t ENGINE=InnoDB",
		OriginalTableName: "t1",
	}
	require.NoError(t, tbl.Write(t.Context(), rec))
	got, err := tbl.ReadLatest(t.Context())
	require.NoError(t, err)
	require.Equal(t, rec.CopierWatermark, got.CopierWatermark)
	require.Equal(t, rec.ChecksumWatermark, got.ChecksumWatermark)
	require.Equal(t, rec.Position, got.Position)
	require.Equal(t, rec.Statement, got.Statement)
	require.Equal(t, rec.OriginalTableName, got.OriginalTableName)
	require.False(t, got.CreatedAt.IsZero())
	require.Less(t, got.Age(), time.Hour, "a just-written checkpoint is fresh")

	// Single-owner: Write overwrites the one row in place rather than appending,
	// so the table stays capped at one row and ReadLatest returns it.
	require.NoError(t, tbl.Write(t.Context(), checkpoint.Record{CopierWatermark: "cw2"}))
	got, err = tbl.ReadLatest(t.Context())
	require.NoError(t, err)
	require.Equal(t, "cw2", got.CopierWatermark)
	require.Equal(t, 1, countRows(t, db, schema, "_ckpt_test_transient"), "Transient Write must keep exactly one row")

	// Drop removes the table entirely; a subsequent read errors (not ErrNotFound).
	require.NoError(t, tbl.Drop(t.Context()))
	_, err = tbl.ReadLatest(t.Context())
	require.Error(t, err)
	require.NotErrorIs(t, err, checkpoint.ErrNotFound)
}

// TestCreateTransientIsFresh verifies Transient Create drops any pre-existing table (so
// a stale row can't be mistaken for resumable progress).
func TestCreateTransientIsFresh(t *testing.T) {
	db, schema := setup(t)
	name := "_ckpt_test_fresh"
	t.Cleanup(func() { _ = dbconn.Exec(t.Context(), db, "DROP TABLE IF EXISTS %n.%n", schema, name) })
	tbl := checkpoint.NewTable(db, name, checkpoint.Transient)

	require.NoError(t, tbl.Create(t.Context()))
	require.NoError(t, tbl.Write(t.Context(), checkpoint.Record{CopierWatermark: "stale"}))
	// Re-create: the prior row must be gone.
	require.NoError(t, tbl.Create(t.Context()))
	_, err := tbl.ReadLatest(t.Context())
	require.ErrorIs(t, err, checkpoint.ErrNotFound)
}

// TestTablePersistent covers the datasync lifecycle: Create is idempotent and
// never clears (so a re-create preserves the row a resume needs), Write keeps a
// single row, and Exists is the resume signal — distinct from "has a row".
func TestTablePersistent(t *testing.T) {
	db, schema := setup(t)
	name := "_ckpt_test_persistent"
	t.Cleanup(func() { _ = dbconn.Exec(t.Context(), db, "DROP TABLE IF EXISTS %n.%n", schema, name) })
	tbl := checkpoint.NewTable(db, name, checkpoint.Persistent)

	// Exists is false before Create — datasync reads this as "fresh sync".
	exists, err := tbl.Exists(t.Context())
	require.NoError(t, err)
	require.False(t, exists)

	require.NoError(t, tbl.Create(t.Context()))

	// After Create the table exists (the resume signal) but has no row yet —
	// datasync resumes and re-copies from scratch in this state.
	exists, err = tbl.Exists(t.Context())
	require.NoError(t, err)
	require.True(t, exists)
	_, err = tbl.ReadLatest(t.Context())
	require.ErrorIs(t, err, checkpoint.ErrNotFound)

	require.NoError(t, tbl.Write(t.Context(), checkpoint.Record{CopierWatermark: "wm1", Position: "pos1"}))
	require.NoError(t, tbl.Write(t.Context(), checkpoint.Record{CopierWatermark: "wm2", Position: "pos2"}))
	// Single row, overwritten in place — a continuous sync can't grow it.
	require.Equal(t, 1, countRows(t, db, schema, name), "Persistent Write must keep exactly one row")

	// Re-create must be a no-op that preserves the existing row (unlike Transient).
	require.NoError(t, tbl.Create(t.Context()))
	got, err := tbl.ReadLatest(t.Context())
	require.NoError(t, err)
	require.Equal(t, "wm2", got.CopierWatermark, "re-create must not drop the table")
	require.Equal(t, "pos2", got.Position)

	// Drop removes the table; Exists is false again.
	require.NoError(t, tbl.Drop(t.Context()))
	exists, err = tbl.Exists(t.Context())
	require.NoError(t, err)
	require.False(t, exists)
}
