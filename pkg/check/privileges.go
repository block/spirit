package check

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/utils"
)

func init() {
	register(check{name: "privileges", run: privilegesCheck, phases: PhasePreflight, runtimes: Migration | Move})
}

// privilegesCheck checks the privileges of the user running the migration or
// move on every source, so a missing one is found before any data is copied
// rather than at cutover time. Each source requires:
//   - REPLICATION CLIENT and REPLICATION SLAVE (or SUPER) for binlog reading
//   - RELOAD for FLUSH TABLES
//   - ALL (or ALTER, CREATE, DELETE, DROP, INDEX, INSERT, LOCK TABLES,
//     SELECT, TRIGGER and UPDATE) on the schema
//   - CONNECTION_ADMIN or SUPER (or EXECUTE on mysql.rds_kill on RDS and
//     Aurora), PROCESS, and performance_schema access for force-kill
//     (enabled by default), checked by dbconn.CheckForceKillPrivileges
//
// A move additionally requires:
//   - Visibility of every view, trigger, event and stored routine in the
//     source schema in information_schema, so the source_schema_objects check
//     cannot pass just because they are hidden (see schemaGrants)
//   - Visibility of every trigger and event in each target schema (TRIGGER
//     and EVENT on the target schema), for the same reason for the
//     target_schema_objects check. Only visibility is checked on the target
//     here; the target-side write privileges are not.
//
// SHOW GRANTS by the current user lists the privileges of its active roles,
// including a default role and, with activate_all_roles_on_login=ON, every
// granted role, so privileges granted through a role are counted like direct
// grants. The visibility grants are always read this way: a role named
// rds_superuser_role counts for what SHOW GRANTS lists for it, and its name
// alone counts for nothing.
//
// The force-kill privileges are checked by dbconn.CheckForceKillPrivileges.
// It accepts CONNECTION_ADMIN or SUPER, or, on RDS and Aurora, EXECUTE on an
// existing mysql.rds_kill, which the kill falls back to when KILL is denied.
// Like the visibility grants, it counts what SHOW GRANTS lists, so a role
// named rds_superuser_role counts for its privileges, not its name. It proves
// PROCESS by reading an InnoDB information_schema table. Force-kill uses
// those privileges only during cutover, to find and kill other users'
// sessions that block the table lock. If a kill still fails, it is logged,
// and the cutover waits for the blocking sessions or times out with an error.
// It never goes ahead without the lock, and no object is missed.
//
// The base grant rules are a re-implementation of the gh-ost check
// validateGrants() in gh-ost/go/logic/inspect.go.
func privilegesCheck(ctx context.Context, r Resources, _ *slog.Logger) error {
	for i, src := range r.Sources {
		if err := checkSourcePrivileges(ctx, r.Runtime, src); err != nil {
			return r.sourceError(i, err)
		}
	}
	for i, target := range r.Targets {
		if target.DB == nil || target.Config == nil {
			return fmt.Errorf("target %d database connection or config is not initialized", i)
		}
		if err := targetObjectVisibility(ctx, r.Runtime, target.DB, target.Config.DBName); err != nil {
			return fmt.Errorf("target %d (%s): %w", i, target.Config.DBName, err)
		}
	}
	return nil
}

func checkSourcePrivileges(ctx context.Context, runtime Runtime, src Endpoint) error {
	if src.DB == nil {
		return errors.New("database connection is not initialized")
	}
	return sourcePrivileges(ctx, runtime, src.DB, src.schema(), func(ctx context.Context) error {
		return dbconn.CheckForceKillPrivileges(ctx, src.DB)
	})
}

// querier is the part of *sql.DB the grant checks use.
type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// sourcePrivileges checks one source's privileges for runtime (see
// privilegesCheck). forceKillProbe checks the privileges force-kill needs.
func sourcePrivileges(ctx context.Context, runtime Runtime, db querier, schemaName string, forceKillProbe func(context.Context) error) error {
	var foundAll, foundSuper, foundReplicationClient, foundReplicationSlave, foundDBAll, foundReload bool

	grants, err := readGrants(ctx, db)
	if err != nil {
		return err
	}
	for _, grant := range grants {
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
	// Database-level grants are evaluated together: MySQL applies one
	// database-level grant to the schema, not the union of every match, so
	// every matching name (exact or pattern) must carry the full set.
	if schemaName != "" && utils.DBLevelGrantsCoverSchema(grants, schemaName) {
		foundDBAll = true
	}
	visibilityKinds := sourceVisibilityKinds(runtime)
	if foundAll {
		return schemaObjectVisibilityFromGrants(grants, schemaName, visibilityKinds...)
	}

	// Force-kill is enabled by default, so its privileges are required. The
	// check logs nothing; the lock detection that does log runs during
	// cutover, not preflight.
	if err := forceKillProbe(ctx); err != nil {
		if errors.Is(err, dbconn.ErrForceKillPrivilegeMissing) {
			return fmt.Errorf("insufficient privileges to run a %s with force-kill enabled. Needed: CONNECTION_ADMIN/SUPER or EXECUTE on mysql.rds_kill, PROCESS, and SELECT on performance_schema.*: %w", runtime.noun(), err)
		}
		return fmt.Errorf("could not check the privileges force-kill needs: %w", err)
	}

	hasBasePrivileges := (foundSuper && foundReplicationSlave && foundDBAll) ||
		(foundReplicationClient && foundReplicationSlave && foundDBAll && foundReload)
	if !hasBasePrivileges {
		err := fmt.Errorf("insufficient privileges to run a %s. Needed: SUPER|REPLICATION CLIENT, RELOAD, REPLICATION SLAVE and ALL on %s.*", runtime.noun(), schemaName)
		if note := utils.ShadowingGrantsNote(grants, schemaName); note != "" && !foundDBAll && schemaName != "" {
			err = fmt.Errorf("%w (%s)", err, note)
		}
		return err
	}
	return schemaObjectVisibilityFromGrants(grants, schemaName, visibilityKinds...)
}

// sourceVisibilityKinds are the schema objects a source's user must be able
// to see. A move refuses a source schema with any of them
// (source_schema_objects), so it must see them all; a migration only looks at
// the migrated table's own triggers and foreign keys, and needs none.
func sourceVisibilityKinds(runtime Runtime) []schemaObject {
	if runtime == Move {
		return allSchemaObjects
	}
	return nil
}
