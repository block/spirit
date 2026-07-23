package check

import (
	"context"
	"database/sql"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
)

// showCreateTable returns the SHOW CREATE TABLE statement for schema.table.
func showCreateTable(ctx context.Context, db *sql.DB, schema, table string) (string, error) {
	// Build the query with sqlescape's %n identifier verb so schema/table names
	// containing backticks (or other identifier characters) are quoted safely,
	// consistent with the rest of the codebase's identifier handling.
	query, err := sqlescape.EscapeSQL("SHOW CREATE TABLE %n.%n", schema, table)
	if err != nil {
		return "", err
	}
	var name, createStmt string
	row := db.QueryRowContext(ctx, query)
	if err := row.Scan(&name, &createStmt); err != nil {
		return "", err
	}
	return createStmt, nil
}
