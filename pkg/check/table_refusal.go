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
// checks under opts, the options the run would use: by default the table is
// refused when it is either end of a foreign key or has triggers, and with
// opts.ExperimentalForeignKeys a table with foreign keys of its own is judged
// as a run with --enable-experimental-foreign-keys judges it.
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
// reason names the first refusal only. The checks run in name order and stop
// at the first that refuses, so a table that is both the end of a foreign key
// and has triggers reports the foreign key, and is refused for its triggers
// once the foreign key is gone.
//
// logger may be nil, in which case the checks' own logging is discarded.
func TableRefusal(ctx context.Context, db *sql.DB, schema, tableName string, opts Options, logger *slog.Logger) (reason string, refused bool, err error) {
	if db == nil {
		return "", false, errors.New("table refusal check requires a database connection")
	}
	resources := Resources{
		Runtime: Migration,
		Sources: []Endpoint{{DB: db, Config: &mysql.Config{DBName: schema}}},
		Change:  Change{Table: table.NewTableInfo(db, schema, tableName)},
		Options: opts,
	}
	if checkErr := RunChecks(ctx, resources, logger, PhaseTable); checkErr != nil {
		if errors.Is(checkErr, ErrRefused) {
			return checkErr.Error(), true, nil
		}
		return "", false, fmt.Errorf("check whether table %s.%s can be copied: %w", schema, tableName, checkErr)
	}
	return "", false, nil
}
