package check

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/table"
)

// TableRefusal reports whether the copy process refuses tableName in schema
// as it stands on db, and the reason it would give. It runs the PhaseTable
// checks under the default migration options: the table is either end of a
// foreign key, or it has triggers.
//
// It is the connected counterpart of StatementRefusal, for a caller that has a
// connection to the server and needs to know ahead of an apply that the copy
// process will not take the table, such as one that can run the statement
// some other way.
//
// refused is true only when the copy process refuses the table. It is not a
// promise that Spirit fails a statement on it: for a single-table change
// Spirit first attempts MySQL's native DDL, and a statement MySQL completes
// that way succeeds whatever these checks say (see PhaseTable). err reports a
// failure to read the server, and is never itself a refusal.
//
// logger may be nil, in which case the checks' own logging is discarded.
func TableRefusal(ctx context.Context, db *sql.DB, schema, tableName string, logger *slog.Logger) (reason string, refused bool, err error) {
	if db == nil {
		return "", false, errors.New("table refusal check requires a database connection")
	}
	resources := Resources{
		Runtime: Migration,
		Sources: []Endpoint{{DB: db, Config: &mysql.Config{DBName: schema}}},
		Change:  Change{Table: table.NewTableInfo(db, schema, tableName)},
	}
	if checkErr := RunChecks(ctx, resources, logger, PhaseTable); checkErr != nil {
		if errors.Is(checkErr, ErrRefused) {
			return checkErr.Error(), true, nil
		}
		return "", false, fmt.Errorf("check whether table %s.%s can be copied: %w", schema, tableName, checkErr)
	}
	return "", false, nil
}
