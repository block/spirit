package dbconn

import (
	"context"
	"database/sql"
	"fmt"
)

// LowerCaseTableNames returns db's server's @@lower_case_table_names, the
// value statement.DiffOptions.LowerCaseTableNames expects. It says how the
// server stores and compares table names:
//
//   - 0: stored as given and compared case-sensitively, so `orders` and
//     `Orders` are different tables.
//   - 1: stored in lower case and compared case-insensitively.
//   - 2: stored as given but compared in lower case, so case-insensitively.
//
// Nonzero therefore means table names match case-insensitively.
func LowerCaseTableNames(ctx context.Context, db *sql.DB) (int, error) {
	var lowerCaseTableNames int
	if err := db.QueryRowContext(ctx, "SELECT @@lower_case_table_names").Scan(&lowerCaseTableNames); err != nil {
		return 0, fmt.Errorf("read lower_case_table_names: %w", err)
	}
	return lowerCaseTableNames, nil
}
