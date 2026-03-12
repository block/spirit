package check

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
	gmysql "github.com/go-sql-driver/mysql"
)

func init() {
	registerCheck("privileges", privilegesCheck, ScopePreflight)
}

// Check the privileges of the user running the migration.
// Ensure there is LOCK TABLES etc so we don't find out and get errors
// at cutover time.
func privilegesCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	// This is a re-implementation of the gh-ost check
	// validateGrants() in gh-ost/go/logic/inspect.go

	grants, err := showGrantsWithRoles(ctx, r.DB, logger)
	if err != nil {
		return err
	}

	var foundAll, foundSuper, foundReplicationClient, foundReplicationSlave, foundDBAll, foundReload, foundConnectionAdmin, foundProcess bool
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
		if strings.Contains(grant, fmt.Sprintf("GRANT ALL PRIVILEGES ON `%s`.*", r.Table.SchemaName)) {
			foundDBAll = true
		}
		if strings.Contains(grant, fmt.Sprintf("GRANT ALL PRIVILEGES ON `%s`.*", strings.ReplaceAll(r.Table.SchemaName, "_", "\\_"))) {
			foundDBAll = true
		}
		if stringContainsAll(grant, `ALTER`, `CREATE`, `DELETE`, `DROP`, `INDEX`, `INSERT`, `LOCK TABLES`, `SELECT`, `TRIGGER`, `UPDATE`, ` ON *.*`) {
			foundDBAll = true
		}
		if stringContainsAll(grant, `ALTER`, `CREATE`, `DELETE`, `DROP`, `INDEX`, `INSERT`, `LOCK TABLES`, `SELECT`, `TRIGGER`, `UPDATE`, fmt.Sprintf(" ON `%s`.*", r.Table.SchemaName)) {
			foundDBAll = true
		}
		if strings.Contains(grant, `CONNECTION_ADMIN`) && strings.Contains(grant, ` ON *.*`) {
			foundConnectionAdmin = true
		}
		if strings.Contains(grant, `PROCESS`) && strings.Contains(grant, ` ON *.*`) {
			foundProcess = true
		}
	}
	if foundAll {
		return nil
	}

	if r.ForceKill {
		var errs []error
		// Rather than parsing grants for specific privilege labels (which vary
		// across MySQL variants — e.g. managed services may grant the capability
		// through roles without exposing the label), we test the actual capabilities.
		if _, err := dbconn.GetTableLocks(ctx, r.DB, []*table.TableInfo{r.Table}, logger, nil); err != nil {
			errs = append(errs, err)
		}
		if _, err := dbconn.GetLockingTransactions(ctx, r.DB, []*table.TableInfo{r.Table}, nil, logger, nil); err != nil {
			errs = append(errs, err)
		}
		if !foundConnectionAdmin && !foundSuper && !foundAll {
			// CONNECTION_ADMIN not visible in grants (may be hidden behind roles
			// on managed services like RDS). Fall back to a direct capability probe.
			if err := canKillConnections(ctx, r.DB, r.Host); err != nil {
				errs = append(errs, fmt.Errorf("cannot kill connections (need CONNECTION_ADMIN, SUPER, or equivalent): %w", err))
			}
		}
		if !foundProcess && !foundAll {
			errs = append(errs, errors.New("missing PROCESS privilege"))
		}
		if len(errs) > 0 {
			return fmt.Errorf("insufficient privileges to run a migration with force-kill enabled (disable with --skip-force-kill). Needed: CONNECTION_ADMIN/SUPER, PROCESS, and SELECT on performance_schema.*: %w", errors.Join(errs...))
		}
	}

	if foundSuper && foundReplicationSlave && foundDBAll {
		return nil
	}
	if foundReplicationClient && foundReplicationSlave && foundDBAll && foundReload {
		return nil
	}

	return errors.New("insufficient privileges to run a migration. Needed: SUPER|REPLICATION CLIENT, RELOAD, REPLICATION SLAVE and ALL on %s.*")
}

// showGrantsWithRoles activates all granted roles and returns the SHOW GRANTS
// output on a single connection. SET ROLE ALL is session-scoped, so a pinned
// connection ensures SHOW GRANTS sees the expanded role privileges.
// This handles environments like Amazon RDS where privileges are assigned via
// roles (e.g. rds_superuser_role) that aren't set as DEFAULT ROLE.
func showGrantsWithRoles(ctx context.Context, db *sql.DB, logger *slog.Logger) ([]string, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire connection: %w", err)
	}
	defer utils.CloseAndLog(conn)

	// Activate all granted roles for this connection.
	if _, err := conn.ExecContext(ctx, `SET ROLE ALL`); err != nil {
		// Not fatal — user may have no granted roles.
		logger.Warn("SET ROLE ALL failed (roles may not be supported)", "error", err)
	}

	rows, err := conn.QueryContext(ctx, `SHOW GRANTS`)
	if err != nil {
		return nil, err
	}
	defer utils.CloseAndLog(rows)

	var grants []string
	for rows.Next() {
		var grant string
		if err := rows.Scan(&grant); err != nil {
			return nil, err
		}
		grants = append(grants, grant)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return grants, nil
}

// canKillConnections tests whether the current user can kill other users'
// connections by creating a temporary "victim" connection from a disposable
// MySQL user and attempting to KILL it.
//
// MySQL only checks kill privileges when the target thread belongs to a
// different user — same-user kills always succeed, and non-existent thread IDs
// skip the privilege check entirely (returning ER_NO_SUCH_THREAD regardless of
// privilege). So we must target a real connection from a different user.
//
// The victim connection is purpose-built for this test, so there is no risk
// to active workloads.
func canKillConnections(ctx context.Context, db *sql.DB, host string) error {
	const probeUser = "_spirit_kill_probe"

	// Create a temporary user to own the victim connection.
	if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP USER IF EXISTS %s", probeUser)); err != nil {
		return fmt.Errorf("cannot verify kill capability (DROP USER failed): %w", err)
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE USER %s", probeUser)); err != nil {
		return fmt.Errorf("cannot verify kill capability (CREATE USER failed): %w", err)
	}
	defer func() {
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP USER IF EXISTS %s", probeUser))
	}()

	// Connect as the probe user to create the victim connection.
	victimCfg := gmysql.NewConfig()
	victimCfg.User = probeUser
	victimCfg.Net = "tcp"
	victimCfg.Addr = host
	victimDB, err := sql.Open("mysql", victimCfg.FormatDSN())
	if err != nil {
		return fmt.Errorf("cannot verify kill capability (connect probe user): %w", err)
	}
	defer utils.CloseAndLog(victimDB)

	// Pin a connection and get its ID.
	victimConn, err := victimDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("cannot verify kill capability (pin probe connection): %w", err)
	}
	defer utils.CloseAndLog(victimConn)

	var victimID int64
	if err := victimConn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&victimID); err != nil {
		return fmt.Errorf("cannot verify kill capability (get connection ID): %w", err)
	}

	// Try to KILL the victim from the caller's connection.
	// ER_KILL_DENIED_ERROR (1095) = caller lacks the privilege.
	// Success or ER_NO_SUCH_THREAD (1094) = caller has the privilege.
	_, killErr := db.ExecContext(ctx, fmt.Sprintf("KILL %d", victimID))
	if killErr == nil {
		return nil
	}
	errStr := killErr.Error()
	if strings.Contains(errStr, "1094") || strings.Contains(errStr, "Unknown thread id") {
		return nil // Thread already gone, but we had permission to try.
	}
	return fmt.Errorf("KILL privilege test failed: %w", killErr)
}

// stringContainsAll returns true if `s` contains all non empty given `substrings`
// The function returns `false` if no non-empty arguments are given.
func stringContainsAll(s string, substrings ...string) bool {
	nonEmptyStringsFound := false
	for _, substring := range substrings {
		if substring == "" {
			continue
		}
		if strings.Contains(s, substring) {
			nonEmptyStringsFound = true
		} else {
			// Immediate failure
			return false
		}
	}
	return nonEmptyStringsFound
}
