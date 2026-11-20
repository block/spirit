package contract

import (
	"context"
	"database/sql"
)

// TableLock is a held LOCK TABLES on one server: while it is held, writes to
// the locked tables can only go through the lock's own session. A change feed
// passes the locks it is given to its subscriptions' flush, and the flush
// writes through them. The feed itself never calls a lock's methods, so a
// source only forwards the locks it receives.
//
// The lock's owner (whoever took it) keeps the concrete type for everything
// else it does with the lock, such as DDL under the lock and releasing it.
type TableLock interface {
	// DB returns the connection pool the lock was acquired on. A writer
	// holding locks on several servers (one per shard) matches each lock to
	// its target by comparing this with the target's own pool.
	DB() *sql.DB

	// ExecUnderLockWithWarnings executes DML on the locking session, in
	// order, and fails if a statement raises a warning. A started statement
	// runs to completion even if ctx is cancelled.
	ExecUnderLockWithWarnings(ctx context.Context, stmts ...string) error

	// ExecUnderLockWithWarningsArgs is ExecUnderLockWithWarnings for one
	// statement with arguments bound to its placeholders.
	ExecUnderLockWithWarningsArgs(ctx context.Context, stmt string, args ...any) error
}
