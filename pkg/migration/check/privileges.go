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

// Check the privileges of the user running the migration.
// Ensure there is LOCK TABLES etc so we don't find out and get errors
// at cutover time.
func privilegesCheck(ctx context.Context, r Resources, _ *slog.Logger) error {
	// This is a re-implementation of the gh-ost check
	// validateGrants() in gh-ost/go/logic/inspect.go
	var foundAll, foundSuper, foundReplicationClient, foundReplicationSlave, foundDBAll, foundReload bool
	rows, err := r.DB.QueryContext(ctx, `SHOW GRANTS`)
	if err != nil {
		return err
	}
	defer utils.CloseAndLog(rows)
	var grants []string
	for rows.Next() {
		var grant string
		if err := rows.Scan(&grant); err != nil {
			return err
		}
		grants = append(grants, grant)
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
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	// Database-level grants are evaluated together: MySQL applies one
	// database-level grant to the schema, not the union of every match, so
	// every matching name (exact or pattern) must carry the full set.
	if utils.DBLevelGrantsCoverSchema(grants, r.Table.SchemaName) {
		foundDBAll = true
	}
	if foundAll {
		return nil
	}

	// Force-kill is enabled by default, so its privileges are required: the
	// performance_schema lock tables, PROCESS, and CONNECTION_ADMIN or SUPER,
	// or EXECUTE on mysql.rds_kill (RDS and Aurora).
	// The check logs nothing; the lock detection that does log runs during
	// cutover.
	if err := dbconn.CheckForceKillPrivileges(ctx, r.DB); err != nil {
		if errors.Is(err, dbconn.ErrForceKillPrivilegeMissing) {
			return fmt.Errorf("insufficient privileges to run a migration with force-kill. Needed: CONNECTION_ADMIN/SUPER or EXECUTE on mysql.rds_kill, PROCESS, and SELECT on performance_schema.*: %w", err)
		}
		return fmt.Errorf("could not check the privileges force-kill needs: %w", err)
	}

	if foundSuper && foundReplicationSlave && foundDBAll {
		return nil
	}
	if foundReplicationClient && foundReplicationSlave && foundDBAll && foundReload {
		return nil
	}

	return fmt.Errorf("insufficient privileges to run a migration. Needed: SUPER|REPLICATION CLIENT, RELOAD, REPLICATION SLAVE and ALL on %s.*", r.Table.SchemaName)
}
