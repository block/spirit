package check

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/utils"
)

func init() {
	registerCheck("version", versionCheck, ScopePreRun)
}

func versionCheck(ctx context.Context, r Resources, _ *slog.Logger) error {
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
	defer utils.CloseAndLog(db)
	// This ensures that we first return an error like
	// connection refused if the host is unreachable,
	// rather than "MySQL 8.0 is required."
	if err := db.PingContext(ctx); err != nil {
		return err
	}
	if !isMySQLSupported(ctx, db) {
		return errors.New("MySQL 8.0 or later is required")
	}
	return nil
}

// isMySQLSupported returns true if the MySQL version is 8.0 or later.
// Spirit supports MySQL 8.0+ (including 8.4, 9.x, etc.)
func isMySQLSupported(ctx context.Context, db *sql.DB) bool {
	var majorVersion int
	if err := db.QueryRowContext(ctx, "SELECT SUBSTRING_INDEX(version(), '.', 1)").Scan(&majorVersion); err != nil {
		return false // can't tell
	}
	return majorVersion >= 8
}
