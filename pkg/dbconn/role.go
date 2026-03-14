package dbconn

import (
	"context"
	"database/sql"
	"log/slog"
	"time"
)

// SetRoleAllOnTxn executes SET ROLE ALL on a transaction to activate all granted roles.
// This is useful in RDS environments where privileges like LOCK TABLES, CONNECTION_ADMIN,
// or PROCESS may be granted via roles (e.g. rds_superuser_role) that are not enabled by default.
// It returns a cleanup function that must be deferred to reset the role to DEFAULT before
// the connection is returned to the pool (the Go MySQL driver does not reset session state
// on transaction commit/rollback).
// SET ROLE ALL must be used on a transaction (dedicated connection) to ensure it only
// affects the current session.
func SetRoleAllOnTxn(ctx context.Context, txn *sql.Tx, logger *slog.Logger) (func(), error) {
	if _, err := txn.ExecContext(ctx, "SET ROLE ALL"); err != nil {
		return func() {}, err
	}
	return func() {
		// Use a fresh context with a short timeout instead of the caller's ctx because the cleanup
		// may run after the original context is canceled (e.g. on shutdown/timeout).
		// SET ROLE DEFAULT must still succeed to prevent leaking elevated roles
		// into the connection pool, but this should not be able to block indefinitely.
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := txn.ExecContext(cleanupCtx, "SET ROLE DEFAULT"); err != nil {
			// If SET ROLE DEFAULT fails, the transaction is likely already closed.
			logger.Debug("SET ROLE DEFAULT failed", "error", err)
		}
	}, nil
}
