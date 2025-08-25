package check

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/siddontang/loggers"
)

func init() {
	registerCheck("version", versionCheck, ScopePreRun)
}

func versionCheck(_ context.Context, r Resources, _ loggers.Advanced) error {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/", r.Username, r.Password, r.Host)
	db, err := dbconn.New(dsn, dbconn.NewDBConfig())
	if err != nil {
		return err
	}
	defer db.Close()
	// This ensures that we first return an error like
	// connection refused if the host is unreachable,
	// rather than "MySQL 8.0 is required."
	if err := db.Ping(); err != nil {
		return err
	}
	if !isMySQL8(db) {
		return errors.New("MySQL 8.0 is required")
	}
	return nil
}

// isMySQL8 returns true if we can positively identify this as mysql 8
func isMySQL8(db *sql.DB) bool {
	var version string
	if err := db.QueryRow("select substr(version(), 1, 1)").Scan(&version); err != nil {
		return false // can't tell
	}
	return version == "8"
}
