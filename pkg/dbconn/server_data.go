package dbconn

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/block/spirit/pkg/utils"
)

// NewHostDataLimit is the most user data a server may hold and still count as
// a new host: one that serves nothing yet, so a run copying into it may load
// it harder than it would a serving host. Below this size a server holds at
// most a few small tables, such as a sidecar's bookkeeping.
const NewHostDataLimit = 100 * 1024 * 1024

// UserDataBytes returns the data and index length of every base table on db's
// server outside the system schemas, less the tables exclude reports, such as
// the ones the run itself writes, so a resumed run measures the server the
// same way as the fresh run did.
//
// information_schema.TABLES lists only the tables the user holds a privilege
// on, so a user granted only some schemas measures only those.
//
// The sizes are read with information_schema_stats_expiry set to 0 on the
// connection: by default MySQL caches them for a day, so a table loaded since
// it was last read would still show its old size.
func UserDataBytes(ctx context.Context, db *sql.DB, exclude func(schema, table string) bool) (int64, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer utils.CloseAndLog(conn)
	if _, err := conn.ExecContext(ctx, "SET SESSION information_schema_stats_expiry = 0"); err != nil {
		return 0, fmt.Errorf("read current table sizes: %w", err)
	}
	defer func() {
		// The pool reuses the connection, so put the cache back. If that
		// fails, drop the connection instead. The reset is bounded: the
		// caller may hold locks, so a hung server must not keep it waiting.
		resetCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), tableUnlockTimeout)
		defer cancel()
		if _, err := conn.ExecContext(resetCtx, "SET SESSION information_schema_stats_expiry = DEFAULT"); err != nil {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
	}()
	rows, err := conn.QueryContext(ctx, `SELECT TABLE_SCHEMA, TABLE_NAME, COALESCE(DATA_LENGTH, 0) + COALESCE(INDEX_LENGTH, 0)
		FROM information_schema.TABLES
		WHERE TABLE_TYPE = 'BASE TABLE'
		AND TABLE_SCHEMA NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')`)
	if err != nil {
		return 0, fmt.Errorf("read table sizes: %w", err)
	}
	defer utils.CloseAndLog(rows)
	var total int64
	for rows.Next() {
		var schema, table string
		var size int64
		if err := rows.Scan(&schema, &table, &size); err != nil {
			return 0, err
		}
		if exclude != nil && exclude(schema, table) {
			continue
		}
		total += size
	}
	return total, rows.Err()
}
