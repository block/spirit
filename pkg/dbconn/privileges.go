package dbconn

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/block/spirit/pkg/utils"
)

// RowQuerier is the part of *sql.DB (or *sql.Conn) that reads one row.
type RowQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// grantsQuerier is the part of *sql.DB checkKillPrivilege reads SHOW GRANTS
// and information_schema.ROUTINES with.
type grantsQuerier interface {
	RowQuerier
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// errKillPrivilegeMissing is the text of the error checkKillPrivilege returns
// when the user can kill another user's session neither way.
const errKillPrivilegeMissing = "missing CONNECTION_ADMIN or SUPER privilege, or EXECUTE on mysql.rds_kill"

// checkKillPrivilege reports an error unless the connection's user can kill
// another user's session. It can with KILL when it holds CONNECTION_ADMIN or
// SUPER, or, on RDS and Aurora, through the mysql.rds_kill procedure when it
// may execute it (see killConnection). No query tests either without killing
// a session, so it reads SHOW GRANTS, which for the current user lists the
// privileges of its active roles alongside its own, and, for the
// procedure, information_schema.ROUTINES to check that it exists.
//
// A role's name confers nothing: on Aurora MySQL 3, rds_superuser_role often
// lacks CONNECTION_ADMIN, and a blue/green switchover can remove it, so only
// the privileges SHOW GRANTS lists count. When rds_superuser_role is active,
// SHOW GRANTS lists its EXECUTE on *.*, so its holder passes through
// mysql.rds_kill. A failed read is returned as it is, not as a missing
// privilege.
func checkKillPrivilege(ctx context.Context, db grantsQuerier) error {
	rows, err := db.QueryContext(ctx, "SHOW GRANTS")
	if err != nil {
		return fmt.Errorf("read grants to check for CONNECTION_ADMIN: %w", err)
	}
	defer utils.CloseAndLog(rows)
	var grants []string
	for rows.Next() {
		var grant string
		if err := rows.Scan(&grant); err != nil {
			return fmt.Errorf("read grants to check for CONNECTION_ADMIN: %w", err)
		}
		if utils.GlobalGrantHasAny(grant, "CONNECTION_ADMIN", "SUPER") {
			return nil
		}
		grants = append(grants, grant)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read grants to check for CONNECTION_ADMIN: %w", err)
	}
	if grantsAllowExecute(grants, rdsKillSchema, rdsKillName) {
		var found int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.ROUTINES
			WHERE ROUTINE_SCHEMA = ? AND ROUTINE_NAME = ? AND ROUTINE_TYPE = 'PROCEDURE'`,
			rdsKillSchema, rdsKillName).Scan(&found); err != nil {
			return fmt.Errorf("check whether %s exists: %w", rdsKillProcedure(), err)
		}
		if found > 0 {
			return nil
		}
	}
	return missingPrivilegeError{errors.New(errKillPrivilegeMissing)}
}

// grantsAllowExecute reports whether SHOW GRANTS lines grant EXECUTE on the
// procedure schema.proc: globally, on the procedure itself, or on its schema.
//
// MySQL applies one database-level grant (mysql.db row) to a schema, not the
// union of every row whose name pattern matches it, and SHOW GRANTS does not
// show which one, so a database-level grant counts only when every matching
// name has EXECUTE.
func grantsAllowExecute(grants []string, schema, proc string) bool {
	onSchema := map[string]bool{}
	for _, grant := range grants {
		if utils.GlobalGrantHasAny(grant, "EXECUTE") || utils.ProcedureGrantHasAny(grant, schema, proc, "EXECUTE") {
			return true
		}
		if name, ok := utils.DBLevelGrantName(grant, schema); ok {
			onSchema[name] = onSchema[name] || utils.DBLevelGrantHasAny(grant, schema, "EXECUTE")
		}
	}
	for _, ok := range onSchema {
		if !ok {
			return false
		}
	}
	return len(onSchema) > 0
}
