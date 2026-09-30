package check

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/utils"
)

func init() {
	registerCheck("privileges", privilegesCheck, ScopePreflight)
}

// privilegesCheck checks the privileges of the user running the move operation.
// Move operations require:
// - REPLICATION CLIENT and REPLICATION SLAVE (or SUPER) for binlog reading
// - RELOAD for FLUSH TABLES
// - Table-level privileges (SELECT, INSERT, etc.) on the source database
// - LOCK TABLES for cutover
// - CONNECTION_ADMIN + PROCESS + performance_schema access for force-kill (enabled by default)
//
// On RDS, the opaque rds_superuser_role cannot be inspected for its underlying
// privileges. When activate_all_roles_on_login=ON, the role is automatically
// active on every connection, so we tolerate its presence as a substitute for
// CONNECTION_ADMIN and PROCESS.
func privilegesCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	for i, src := range r.Sources {
		if err := checkSourcePrivileges(ctx, src, r, logger); err != nil {
			return fmt.Errorf("source %d: %w", i, err)
		}
	}
	return nil
}

func checkSourcePrivileges(ctx context.Context, src SourceResource, r Resources, logger *slog.Logger) error {
	if src.DB == nil {
		return errors.New("database connection is not initialized")
	}

	var foundAll, foundSuper, foundReplicationClient, foundReplicationSlave, foundDBAll, foundReload bool

	schemaName := ""
	if src.Config != nil {
		schemaName = src.Config.DBName
	}

	rows, err := src.DB.QueryContext(ctx, `SHOW GRANTS`)
	if err != nil {
		return err
	}
	defer utils.CloseAndLog(rows)
	for rows.Next() {
		var grant string
		if err := rows.Scan(&grant); err != nil {
			return err
		}
		if strings.Contains(grant, `GRANT ALL PRIVILEGES ON *.*`) {
			foundAll = true
		}
		if strings.Contains(grant, `SUPER`) && strings.Contains(grant, ` ON *.*`) {
			foundSuper = true
		}
		if strings.Contains(grant, `REPLICATION CLIENT`) && strings.Contains(grant, ` ON *.*`) {
			foundReplicationClient = true
		}
		if strings.Contains(grant, `REPLICATION SLAVE`) && strings.Contains(grant, ` ON *.*`) {
			foundReplicationSlave = true
		}
		if strings.Contains(grant, `RELOAD`) && strings.Contains(grant, ` ON *.*`) {
			foundReload = true
		}
		if utils.StringContainsAll(grant, `ALTER`, `CREATE`, `DELETE`, `DROP`, `INDEX`, `INSERT`, `LOCK TABLES`, `SELECT`, `TRIGGER`, `UPDATE`, ` ON *.*`) {
			foundDBAll = true
		}
		// A database-level grant covers the schema if its database-name pattern
		// matches (including MySQL wildcards such as `strata_%`) and it confers
		// either ALL PRIVILEGES or the full set spirit requires.
		if schemaName != "" && utils.DBLevelGrantCoversSchema(grant, schemaName) {
			foundDBAll = true
		}
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	if foundAll {
		return nil
	}

	// Force-kill is enabled by default, so its privileges are required: the
	// performance_schema lock tables, PROCESS, and CONNECTION_ADMIN or SUPER.
	// The check reads at most one row and logs nothing; the lock detection
	// that does log runs during cutover.
	if err := dbconn.CheckForceKillPrivileges(ctx, src.DB); err != nil {
		return fmt.Errorf("insufficient privileges to run a move with force-kill enabled. Needed: CONNECTION_ADMIN/SUPER, PROCESS, and SELECT on performance_schema.*: %w", err)
	}

	if foundSuper && foundReplicationSlave && foundDBAll {
		return nil
	}
	if foundReplicationClient && foundReplicationSlave && foundDBAll && foundReload {
		return nil
	}

	return fmt.Errorf("insufficient privileges to run a move. Needed: SUPER|REPLICATION CLIENT, RELOAD, REPLICATION SLAVE and ALL on %s.*", schemaName)
}
