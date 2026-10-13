package check

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/utils"
)

func init() {
	// A migration checks the version at PhasePreRun, before it has a
	// connection of its own. A move and a sync have no PhasePreRun: they
	// check every source and target they connected to at PhasePreflight. A
	// migration runs the preflight registration too, against its source.
	register(check{name: "version", run: versionCheck, phases: PhasePreRun | PhasePreflight, runtimes: Migration | Move | Sync})
}

// versionCheck requires MySQL 8.0 or later on every server the run uses.
func versionCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	if r.phase == PhasePreRun {
		return preRunVersionCheck(ctx, r)
	}
	for i, src := range r.Sources {
		if src.DB == nil {
			return r.sourceError(i, errors.New("database connection is not initialized"))
		}
		if !isMySQLSupported(ctx, src.DB) {
			return r.sourceError(i, errMySQLVersion)
		}
	}
	for i, target := range r.Targets {
		if target.DB == nil {
			return fmt.Errorf("target %d: database connection is not initialized", i)
		}
		if !isMySQLSupported(ctx, target.DB) {
			return fmt.Errorf("target %d: %w", i, errMySQLVersion)
		}
	}
	return nil
}

var errMySQLVersion = errors.New("MySQL 8.0 or later is required")

// preRunVersionCheck connects with r.Credentials, because a migration has no
// connection yet at PhasePreRun.
func preRunVersionCheck(ctx context.Context, r Resources) error {
	// Use the go-sql-driver/mysql.Config to properly escape the DSN
	// For version check, we try to connect without specifying a database first,
	// but if that fails due to permissions, we can still check the version
	cfg := mysql.Config{
		User:   r.Credentials.Username,
		Passwd: r.Credentials.Password,
		Net:    "tcp",
		Addr:   r.Credentials.Host,
		// Don't specify DBName - connect without selecting a database
	}
	dsn := cfg.FormatDSN()

	// Create DBConfig with TLS settings from the migration
	dbConfig := dbconn.NewDBConfig()
	dbConfig.TLSMode = r.Credentials.TLSMode
	dbConfig.TLSCertificatePath = r.Credentials.TLSCertificatePath

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
		return errMySQLVersion
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
