package check

import (
	"context"
	"database/sql"
	"errors"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/go-sql-driver/mysql"
	"github.com/siddontang/loggers"
)

func init() {
	registerCheck("version", versionCheck, ScopePreRun)
}

func versionCheck(_ context.Context, r Resources, _ loggers.Advanced) error {
	// Use the go-sql-driver/mysql.Config to properly escape the DSN
	// For version check, we try to connect without specifying a database first,
	// but if that fails due to permissions, we can still check the version
	cfg := mysql.Config{
		User:   r.Username,
		Passwd: r.Password,
		Net:    "tcp",
		Addr:   r.Host,
		// Don't specify DBName - connect without selecting a database
	}
	dsn := cfg.FormatDSN()

	// Create DBConfig with TLS settings from the migration
	dbConfig := dbconn.NewDBConfig()
	dbConfig.TLSMode = r.TLSMode
	dbConfig.TLSCertificatePath = r.TLSCertificatePath

	db, err := dbconn.New(dsn, dbConfig)
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
