// Package checkpoint owns the checkpoint table shared by all three runners
// (migration, move, datasync): its schema, lifecycle (create / drop / exists),
// and row read/write. A checkpoint records where the row copy and (for
// migrate/move) the checksum got to, plus the change-feed position, so an
// interrupted run can resume instead of starting over.
//
// The table holds a single row, overwritten in place each dump (REPLACE on
// id=1), and resume reads it. Two Modes cover the runners' differing
// lifecycles — Transient (one finite run) and Persistent (a continuous run);
// see Mode. The package owns the table mechanics; callers own the
// interpretation of the watermarks and the resume policy (staleness max-age,
// statement match, table-name collision, and — for datasync — using Exists as
// the resume signal) via the returned Record, Age, and Exists.
package checkpoint

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/dbconn"
)

// erNoSuchTable is MySQL error 1146 (ER_NO_SUCH_TABLE); erBadFieldError is 1054
// (ER_BAD_FIELD_ERROR), returned when a column is missing.
const (
	erNoSuchTable   = 1146
	erBadFieldError = 1054
)

// IsIncompatible reports whether err means the checkpoint table can't be read
// with this spirit version's schema — a missing column (ER_BAD_FIELD_ERROR,
// what a table written by an incompatible version looks like) or the table
// having vanished (ER_NO_SUCH_TABLE). Callers use it to tell an unusable
// checkpoint (recover / start fresh) apart from a transient read failure
// (permission, server gone), which must not be mistaken for "no checkpoint".
func IsIncompatible(err error) bool {
	myErr, ok := errors.AsType[*mysql.MySQLError](err)
	return ok && (myErr.Number == erNoSuchTable || myErr.Number == erBadFieldError)
}

// ErrNotFound is returned by ReadLatest when the table holds no checkpoint row
// to resume from (it is empty). It is a normal state, distinct from a read
// failure.
var ErrNotFound = errors.New("checkpoint: no checkpoint found")

// Record is one checkpoint row. Watermarks and Position are opaque strings the
// caller produces and consumes; this package never interprets them.
type Record struct {
	// CopierWatermark is the row-copy resume point.
	CopierWatermark string
	// ChecksumWatermark is the checksum resume point ("" when not in / not past
	// the checksum phase, or deliberately blanked to force re-verification).
	ChecksumWatermark string
	// Position is the change-feed resume position: a single opaque position for
	// a single source (migration), or a JSON map of per-source positions for a
	// multi-source move. Stored in the binlog_position column.
	Position string
	// Statement is the migration's ALTER statement, which the runner uses to
	// match a resume to its checkpoint (ErrMismatchedAlter). Empty for move and
	// datasync.
	Statement string
	// OriginalTableName is the untruncated source table name, stored so a
	// single-table migration can detect the rare case where two long table
	// names truncate to the same checkpoint table name. Empty otherwise.
	OriginalTableName string
	// Phase is the move's reverse-window lifecycle: "" (copying — the default,
	// and the only value migration/datasync ever use), "reverse_window" (forward
	// cutover done, reverse feed live), or "reverting" (reverse cutover under
	// way). It gates resume so a restart neither re-copies nor re-cuts-over.
	// Stored in move_phase.
	Phase string
	// CutoverAt is when the forward cutover completed, used to compute the
	// reverse-window deadline across a resume. Zero when not past cutover; stored
	// in cutover_at as an RFC3339 string ("" when zero).
	CutoverAt time.Time
	// CreatedAt is when the row was written (UTC; spirit connections use
	// time_zone="+00:00").
	CreatedAt time.Time
}

// Age reports how long ago the checkpoint was written. Callers compare it to
// their configured max age to decide whether replaying from here is worthwhile.
func (r Record) Age() time.Duration { return time.Since(r.CreatedAt) }

// Mode selects how a Table is created, matching spirit's checkpoint lifecycles.
// Either way Write keeps a single row (REPLACE on id=1) and ReadLatest reads it.
type Mode int

const (
	// Transient is a checkpoint for one finite run: it lives only as long as the
	// migration/move that owns it. Create DROPs and recreates the table (so it
	// always matches this version's schema and carries no stale rows) and Drop
	// drops it. Used by single-table and atomic multi-table migrations (only one
	// of the latter runs per schema at a time, so the shared _spirit_checkpoint
	// has a single owner) and by move.
	Transient Mode = iota
	// Persistent is a checkpoint for a continuous, never-ending run (datasync):
	// the table outlives any single run, so Create is idempotent and never
	// clears, and the caller decides fresh-vs-resume from Exists and ReadLatest
	// (not by recreating the table) — Create must never destroy the rows a
	// resume needs. Drop drops it.
	Persistent
)

// Table manages a checkpoint table named name on db. Operations are unqualified
// and run against db's currently-selected schema (DATABASE()), so callers point
// the connection at the right schema rather than passing one — this is what lets
// it work under Vitess, and it matches pkg/sentinel. mode selects its
// create / clear / read behaviour; see Mode.
type Table struct {
	db   *sql.DB
	name string
	mode Mode
}

// NewTable returns a handle to the checkpoint table name on db (in db's selected
// schema), with the lifecycle selected by mode.
func NewTable(db *sql.DB, name string, mode Mode) *Table {
	return &Table{db: db, name: name, mode: mode}
}

// tableDDL is the column list for CREATE TABLE. The schema is the union of what
// the runners need; move and datasync leave statement and original_table_name
// empty. id is the primary key single-owner modes REPLACE on to keep one row.
const tableDDL = `(
	id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	copier_watermark TEXT,
	checksum_watermark TEXT,
	binlog_position TEXT,
	statement TEXT,
	original_table_name VARCHAR(64) NOT NULL DEFAULT '',
	move_phase VARCHAR(32) NOT NULL DEFAULT '',
	cutover_at TEXT,
	created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
)`

// Create prepares the checkpoint table for a run. Behaviour depends on Mode:
//
//   - Transient (single-table / atomic multi-table migration, move): DROP +
//     CREATE, so the table always matches this spirit version's schema and
//     carries no stale rows. Safe for the shared multi-table _spirit_checkpoint
//     because only one multi-table migration runs per schema at a time.
//   - Persistent (datasync): CREATE IF NOT EXISTS and nothing else. The table
//     is long-lived and its existence is the caller's resume signal, so it is
//     never dropped or cleared here.
func (t *Table) Create(ctx context.Context) error {
	switch t.mode {
	case Transient:
		if err := dbconn.Exec(ctx, t.db, "DROP TABLE IF EXISTS %n", t.name); err != nil {
			return err
		}
		return dbconn.Exec(ctx, t.db, "CREATE TABLE %n "+tableDDL, t.name)
	case Persistent:
		return dbconn.Exec(ctx, t.db, "CREATE TABLE IF NOT EXISTS %n "+tableDDL, t.name)
	default:
		return fmt.Errorf("checkpoint: unknown table mode %d", t.mode)
	}
}

// Drop removes this run's checkpoint (DROP TABLE IF EXISTS).
func (t *Table) Drop(ctx context.Context) error {
	return dbconn.Exec(ctx, t.db, "DROP TABLE IF EXISTS %n", t.name)
}

// Write records a checkpoint row, keeping a single row by overwriting it in
// place (REPLACE on the fixed primary key id=1). The table has one logical
// owner, so there is no value in accumulating history, and REPLACE is one atomic
// statement — a crash mid-write leaves either the old row or the new one, never
// none. created_at is (re)assigned by the server on each write.
func (t *Table) Write(ctx context.Context, rec Record) error {
	var cutoverAt string
	if !rec.CutoverAt.IsZero() {
		cutoverAt = rec.CutoverAt.UTC().Format(time.RFC3339Nano)
	}
	return dbconn.Exec(ctx, t.db,
		"REPLACE INTO %n (id, copier_watermark, checksum_watermark, binlog_position, statement, original_table_name, move_phase, cutover_at) VALUES (1, %?, %?, %?, %?, %?, %?, %?)",
		t.name,
		rec.CopierWatermark, rec.ChecksumWatermark, rec.Position, rec.Statement, rec.OriginalTableName,
		rec.Phase, cutoverAt,
	)
}

// ReadLatest returns the most recent checkpoint row, or ErrNotFound when there
// is none.
//
// Columns are selected explicitly (not SELECT *): a checkpoint table written by
// an incompatible spirit version that is missing a column surfaces as a read
// error, so resume fails safely rather than silently misreading.
func (t *Table) ReadLatest(ctx context.Context) (Record, error) {
	query := fmt.Sprintf(
		"SELECT copier_watermark, checksum_watermark, binlog_position, statement, original_table_name, move_phase, cutover_at, created_at FROM `%s` ORDER BY id DESC LIMIT 1",
		t.name)

	var rec Record
	var createdAt string
	var cutoverAt sql.NullString
	err := t.db.QueryRowContext(ctx, query).Scan(
		&rec.CopierWatermark, &rec.ChecksumWatermark, &rec.Position, &rec.Statement, &rec.OriginalTableName,
		&rec.Phase, &cutoverAt, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Record{}, ErrNotFound
	}
	if err != nil {
		return Record{}, err
	}
	if cutoverAt.Valid && cutoverAt.String != "" {
		rec.CutoverAt, err = time.Parse(time.RFC3339Nano, cutoverAt.String)
		if err != nil {
			return Record{}, fmt.Errorf("could not parse checkpoint cutover_at timestamp: %w", err)
		}
	}
	// Spirit connections use time_zone="+00:00", so created_at is UTC.
	rec.CreatedAt, err = time.Parse(time.DateTime, createdAt)
	if err != nil {
		return Record{}, fmt.Errorf("could not parse checkpoint created_at timestamp: %w", err)
	}
	return rec, nil
}

// Exists reports whether the checkpoint table exists in db's selected schema
// (DATABASE()). datasync uses this as its resume signal: the table is created
// before any rows are copied, so its presence means a prior run already owns
// the target — even one that died before writing its first checkpoint row.
// Because it keys on DATABASE(), the connection must have the target schema
// selected (it cannot be a no-database admin connection).
func (t *Table) Exists(ctx context.Context) (bool, error) {
	var one int
	err := t.db.QueryRowContext(ctx,
		"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?",
		t.name).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}
