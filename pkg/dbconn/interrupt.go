package dbconn

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"log/slog"

	"github.com/block/mysql"
)

// poolConnector is the connector New opens its pools with: block/mysql's
// connector, unchanged, plus the Interrupter that ends the pool's cancelled
// statements with KILL QUERY. database/sql closes the connector with the pool,
// and that closes the Interrupter.
type poolConnector struct {
	driver.Connector
	driver poolDriver
}

// poolDriver is what (*sql.DB).Driver returns for a pool New opened. It
// carries the pool's Interrupter: database/sql gives no other way back from a
// *sql.DB to what opened it.
type poolDriver struct {
	driver.Driver
	interrupter *mysql.Interrupter
}

func (c *poolConnector) Driver() driver.Driver {
	return c.driver
}

// Close closes the pool's Interrupter. database/sql calls it from
// (*sql.DB).Close.
func (c *poolConnector) Close() error {
	return c.driver.interrupter.Close()
}

// openPool opens a pool on dsn, as sql.Open(DriverName, dsn) does, with an
// Interrupter that dials its own connections with the same DSN. The
// Interrupter keeps at most a few connections, apart from the pool's, so a
// KILL QUERY never waits for a connection the pool has no room for.
func openPool(dsn string) (*sql.DB, error) {
	connector, err := mysql.MySQLDriver{}.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return openPoolWithInterrupter(connector, mysql.NewInterrupter(connector)), nil
}

// openPoolWithInterrupter opens a pool on connector whose cancelled
// statements are ended through interrupter. Tests use it to give a pool an
// Interrupter that cannot reach the server.
func openPoolWithInterrupter(connector driver.Connector, interrupter *mysql.Interrupter) *sql.DB {
	return sql.OpenDB(&poolConnector{
		Connector: connector,
		driver:    poolDriver{Driver: connector.Driver(), interrupter: interrupter},
	})
}

// poolInterrupter returns the Interrupter of a pool New opened, and nil for
// any other pool, whose cancelled statements are then only ended by killing
// their session.
func poolInterrupter(db *sql.DB) mysql.QueryKiller {
	if d, ok := db.Driver().(poolDriver); ok {
		return d.interrupter
	}
	return nil
}

// readKillTarget returns what names conn's session for a kill: the thread ID
// from the connection's handshake, and the server's @@server_uuid and
// @@hostname, read once per connection. It replaces SELECT CONNECTION_ID().
func readKillTarget(ctx context.Context, conn *sql.Conn) (mysql.KillTarget, error) {
	var target mysql.KillTarget
	err := conn.Raw(func(driverConn any) error {
		var err error
		target, err = mysql.ReadKillTarget(ctx, driverConn)
		return err
	})
	return target, err
}

// execEndingOnCancel runs stmt on conn, the session target names, so that a
// caller who cancels it does not leave it running on the server.
//
// When db was opened by New, the statement runs on a context that the
// caller's cancel does not end. On cancel, KILL QUERY goes to the statement's
// server on one of the Interrupter's own connections, after checking that the
// connection is on that server, and the statement's own reply is awaited. The
// reply says what happened: OK means the statement took effect before the
// kill landed, an error (1317 for the kill) that it did not. Only when KILL
// QUERY fails, or the statement has not ended cancelledStatementKillTimeout
// after the cancel, is the context ended, so that the driver closes the
// connection. For any other pool the driver does that at once.
//
// It returns what the interrupt did and the statement's error. The caller
// passes both to endCancelled once nothing else uses conn.
func execEndingOnCancel(ctx context.Context, db *sql.DB, conn *sql.Conn, target mysql.KillTarget, stmt string, logger *slog.Logger) (mysql.InterruptOutcome, error) {
	stmtCtx, armed := mysql.ArmInterrupt(ctx, poolInterrupter(db), target, mysql.InterruptOptions{
		Timeout: cancelledStatementKillTimeout,
		OnFallback: func(target mysql.KillTarget, err error) {
			logger.Warn("KILL QUERY did not end a cancelled statement; closing its connection and killing its session",
				"pid", target.ThreadID,
				"error", err,
			)
		},
	})
	_, err := conn.ExecContext(stmtCtx, stmt)
	return armed.Finish(), err
}

// endCancelled settles a statement that execEndingOnCancel ran on conn when
// the caller's ctx has ended, once nothing else uses conn. execErr and
// interrupt are what execEndingOnCancel returned. It reports whether conn has
// been discarded, and the error to return; a conn that has not been discarded
// is the caller's to close.
//
// When the server replied to the statement, it is over: its error is returned
// with the cancellation joined in, and nil when it succeeded. The connection
// is discarded only if a KILL QUERY may still arrive on it or the interrupt
// may have closed it. When no reply came, the statement may still run on the
// server, and endCancelledConn kills its session.
func endCancelled(ctx context.Context, db *sql.DB, conn *sql.Conn, connID int, execErr error, interrupt mysql.InterruptOutcome, logger *slog.Logger) (discarded bool, err error) {
	if !statementReplied(execErr) {
		return true, endCancelledConn(ctx, db, conn, connID, execErr, logger)
	}
	if execErr != nil {
		if cause := ctx.Err(); !errors.Is(execErr, cause) {
			err = errors.Join(execErr, cause)
		} else {
			err = execErr
		}
	}
	if interrupt.Unacknowledged || interrupt.FellBack {
		return true, errors.Join(err, discardConn(conn))
	}
	return false, err
}

// statementReplied reports whether err, a statement's error, means the server
// replied to the statement: it succeeded, or the server sent an error. A
// cancelled context or a lost connection means no reply came, so the
// statement may still be running.
func statementReplied(err error) bool {
	if err == nil {
		return true
	}
	if _, ok := errors.AsType[*mysql.MySQLError](err); !ok {
		return false
	}
	return !IsConnectionLossError(err)
}
