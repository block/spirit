package dbconn

import (
	"context"
	"database/sql"
	"errors"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
)

// ExecWithoutForeignKeyChecks is like Exec, but runs stmt in a session with
// foreign_key_checks turned off. It is for DDL, which a SET_VAR hint cannot
// apply to: with the checks off, MySQL adds a foreign key in place, as a
// metadata change, rather than copying the table to check its rows.
//
// The session is turned back to the setting it had before it is returned to
// the pool. When that fails, the session is discarded instead, so no other
// statement can run with the checks off. The setting is read rather than reset
// with DEFAULT: SET foreign_key_checks = DEFAULT turns the checks off.
func ExecWithoutForeignKeyChecks(ctx context.Context, db *sql.DB, stmt string, args ...any) error {
	stmt, err := sqlescape.EscapeSQL(stmt, args...)
	if err != nil {
		return err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	var checks int
	if err := conn.QueryRowContext(ctx, "SELECT @@session.foreign_key_checks").Scan(&checks); err != nil {
		return errors.Join(err, conn.Close())
	}
	if _, err := conn.ExecContext(ctx, "SET SESSION foreign_key_checks = 0"); err != nil {
		return errors.Join(err, discardConn(conn))
	}
	_, execErr := conn.ExecContext(ctx, stmt)
	// Restore on a context of its own: the statement may have used up ctx.
	restoreCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), tableUnlockTimeout)
	defer cancel()
	if _, err := conn.ExecContext(restoreCtx, "SET SESSION foreign_key_checks = ?", checks); err != nil {
		return errors.Join(execErr, err, discardConn(conn))
	}
	return errors.Join(execErr, conn.Close())
}
