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

// tableVisibilityPrivileges are the static privileges that, granted globally,
// make every table appear in information_schema.TABLES. information_schema
// shows a user only the tables it holds some privilege on.
var tableVisibilityPrivileges = []string{
	"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "REFERENCES",
	"INDEX", "ALTER", "CREATE VIEW", "SHOW VIEW", "TRIGGER",
}

// ServerUserData is how much user data a server holds, as one user sees it.
type ServerUserData struct {
	// Bytes is the data and index length of every base table outside the
	// system schemas, less the tables the caller excluded.
	Bytes int64
	// SeesEveryTable reports whether the user holds a global table privilege.
	// Without one, information_schema.TABLES lists only the schemas it was
	// granted, so Bytes misses the rest of the server.
	SeesEveryTable bool
}

// NewHost reports whether the server counts as a new host: the tables the
// user sees hold less than NewHostDataLimit bytes. Without SeesEveryTable,
// other schemas on the server may hold data the measurement cannot see.
func (d ServerUserData) NewHost() bool {
	return d.Bytes < NewHostDataLimit
}

// ReadServerUserData measures the user data on db's server. exclude reports
// the tables not to count, such as the ones the run itself writes, so a
// resumed run measures the server the same way as the fresh run did.
//
// The sizes are read with information_schema_stats_expiry set to 0 on the
// connection: by default MySQL caches them for a day, so a table loaded since
// it was last read would still show its old size.
func ReadServerUserData(ctx context.Context, db *sql.DB, exclude func(schema, table string) bool) (ServerUserData, error) {
	grants, err := readGrants(ctx, db)
	if err != nil {
		return ServerUserData{}, err
	}
	var data ServerUserData
	for _, grant := range grants {
		if utils.GlobalGrantHasAny(grant, tableVisibilityPrivileges...) {
			data.SeesEveryTable = true
			break
		}
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return ServerUserData{}, err
	}
	defer utils.CloseAndLog(conn)
	if _, err := conn.ExecContext(ctx, "SET SESSION information_schema_stats_expiry = 0"); err != nil {
		return ServerUserData{}, fmt.Errorf("read current table sizes: %w", err)
	}
	defer func() {
		// The pool reuses the connection, so put the cache back. If that
		// fails, drop the connection instead.
		if _, err := conn.ExecContext(context.WithoutCancel(ctx), "SET SESSION information_schema_stats_expiry = DEFAULT"); err != nil {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
	}()
	rows, err := conn.QueryContext(ctx, `SELECT TABLE_SCHEMA, TABLE_NAME, COALESCE(DATA_LENGTH, 0) + COALESCE(INDEX_LENGTH, 0)
		FROM information_schema.TABLES
		WHERE TABLE_TYPE = 'BASE TABLE'
		AND TABLE_SCHEMA NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')`)
	if err != nil {
		return ServerUserData{}, fmt.Errorf("read table sizes: %w", err)
	}
	defer utils.CloseAndLog(rows)
	for rows.Next() {
		var schema, table string
		var size int64
		if err := rows.Scan(&schema, &table, &size); err != nil {
			return ServerUserData{}, err
		}
		if exclude != nil && exclude(schema, table) {
			continue
		}
		data.Bytes += size
	}
	return data, rows.Err()
}

// readGrants returns the SHOW GRANTS lines of db's user, which include the
// privileges of its active roles.
func readGrants(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, "SHOW GRANTS")
	if err != nil {
		return nil, fmt.Errorf("read grants: %w", err)
	}
	defer utils.CloseAndLog(rows)
	var grants []string
	for rows.Next() {
		var grant string
		if err := rows.Scan(&grant); err != nil {
			return nil, err
		}
		grants = append(grants, grant)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read grants: %w", err)
	}
	return grants, nil
}
