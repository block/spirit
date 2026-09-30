package checkpoint

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"sync/atomic"
	"testing"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/stretchr/testify/require"
)

// fakeConnector is a database/sql driver that fails Write at a chosen step:
// opening the session, reading its ID, or running the REPLACE. It counts the
// REPLACEs it was asked to run, so a test can tell whether one was sent.
type fakeConnector struct {
	connectErr error
	queryErr   error
	execErr    error
	execs      atomic.Int32
}

func (c *fakeConnector) Connect(context.Context) (driver.Conn, error) {
	if c.connectErr != nil {
		return nil, c.connectErr
	}
	return &fakeConn{c: c}, nil
}

func (c *fakeConnector) Driver() driver.Driver { return fakeDriver{} }

type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) { return nil, errors.New("use the connector") }

type fakeConn struct{ c *fakeConnector }

func (*fakeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (*fakeConn) Close() error                        { return nil }
func (*fakeConn) Begin() (driver.Tx, error)           { return nil, errors.New("not supported") }

func (f *fakeConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	if f.c.queryErr != nil {
		return nil, f.c.queryErr
	}
	return &connectionIDRows{}, nil
}

func (f *fakeConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	f.c.execs.Add(1)
	if f.c.execErr != nil {
		return nil, f.c.execErr
	}
	return driver.RowsAffected(1), nil
}

// connectionIDRows answers SELECT CONNECTION_ID() with a single row.
type connectionIDRows struct{ done bool }

func (*connectionIDRows) Columns() []string { return []string{"CONNECTION_ID()"} }
func (*connectionIDRows) Close() error      { return nil }
func (r *connectionIDRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dest[0] = int64(42)
	return nil
}

// TestWriteMarksErrorsBeforeTheReplaceIsSent: a failure before the REPLACE
// reaches the server wraps ErrWriteNotSent, keeping the cause in the chain, so
// a caller can tell a write that cannot commit later from one whose outcome is
// unknown. A lost connection after the REPLACE was sent is not marked.
func TestWriteMarksErrorsBeforeTheReplaceIsSent(t *testing.T) {
	lockWait := &mysql.MySQLError{Number: 1205, Message: "Lock wait timeout exceeded"}
	for _, tc := range []struct {
		name        string
		fake        *fakeConnector
		wantNotSent bool
		wantCause   error
		wantExecs   int32
	}{
		{name: "session cannot be opened", fake: &fakeConnector{connectErr: mysql.ErrInvalidConn}, wantNotSent: true, wantCause: mysql.ErrInvalidConn},
		{name: "session ID cannot be read", fake: &fakeConnector{queryErr: mysql.ErrInvalidConn}, wantNotSent: true, wantCause: mysql.ErrInvalidConn},
		{name: "REPLACE refused as a bad connection", fake: &fakeConnector{execErr: driver.ErrBadConn}, wantNotSent: true, wantCause: driver.ErrBadConn, wantExecs: 1},
		{name: "connection lost after the REPLACE was sent", fake: &fakeConnector{execErr: mysql.ErrInvalidConn}, wantCause: mysql.ErrInvalidConn, wantExecs: 1},
		{name: "server error on the REPLACE", fake: &fakeConnector{execErr: lockWait}, wantCause: lockWait, wantExecs: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := sql.OpenDB(tc.fake)
			t.Cleanup(func() { _ = db.Close() })
			err := NewTable(db, "_ckpt_test_notsent", Transient).Write(t.Context(), Record{Position: "new"})
			require.ErrorIs(t, err, tc.wantCause)
			require.Equal(t, tc.wantNotSent, errors.Is(err, ErrWriteNotSent), "ErrWriteNotSent marking: %v", err)
			require.Equal(t, tc.wantExecs, tc.fake.execs.Load())
		})
	}

	t.Run("a connection loss before the REPLACE is still a connection loss", func(t *testing.T) {
		// Marking adds to the chain; it does not hide the cause from the
		// classifiers callers already use.
		db := sql.OpenDB(&fakeConnector{queryErr: mysql.ErrInvalidConn})
		t.Cleanup(func() { _ = db.Close() })
		err := NewTable(db, "_ckpt_test_notsent", Transient).Write(t.Context(), Record{Position: "new"})
		require.ErrorIs(t, err, ErrWriteNotSent)
		require.True(t, dbconn.IsOutcomeUnknown(err))
	})

	t.Run("closed pool", func(t *testing.T) {
		fake := &fakeConnector{}
		db := sql.OpenDB(fake)
		require.NoError(t, db.Close())
		err := NewTable(db, "_ckpt_test_notsent", Transient).Write(t.Context(), Record{Position: "new"})
		require.ErrorIs(t, err, ErrWriteNotSent)
		require.Zero(t, fake.execs.Load())
	})

	t.Run("context already done", func(t *testing.T) {
		fake := &fakeConnector{}
		db := sql.OpenDB(fake)
		t.Cleanup(func() { _ = db.Close() })
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		err := NewTable(db, "_ckpt_test_notsent", Transient).Write(ctx, Record{Position: "new"})
		require.ErrorIs(t, err, ErrWriteNotSent)
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, fake.execs.Load())
	})
}
