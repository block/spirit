# Database Connections

The `dbconn` package provides MySQL database connection management and locking utilities for Spirit. It wraps `database/sql` with Spirit-specific concerns: retry logic, TLS auto-configuration, advisory locking, table locking, and the ability to kill blocking transactions.

## Connection Setup

When creating a new connection, Spirit appends standardized DSN parameters to ensure consistent behavior across all connections. These include setting `sql_mode=""` (to be able to copy legacy data like `0000-00-00`), `time_zone=+00:00`, `transaction_isolation=read-committed`, `charset=utf8mb4`, `collation=utf8mb4_bin`, and `rejectReadOnly=true` (for Aurora failover resilience). This means that regardless of the server's global configuration, Spirit connections behave predictably.

## Pool sizing

Use `SetPoolSize(db, n)` rather than `db.SetMaxOpenConns(n)` directly. It sets the open and idle limits together, and they must stay together: `database/sql` closes a connection returned to a pool whose free list already holds `MaxIdleConns` entries, so an idle limit below the open limit turns every release past that point into a close and every subsequent acquire into a fresh dial, TLS handshake and MySQL auth. The copy phase — hundreds of read and write workers cycling connections continuously — is exactly that workload, and the churn is invisible on the status block, which does not report pool internals at all.

Holding the connections idle instead costs nothing that was not already reserved: the open limit is the budget, and matching the idle limit to it only stops the pool from discarding what it is entitled to keep. Several call sites ratchet a pool's size after it was created (the migration runner once thread counts are final, the checksum, cutover), which is why this lives in a helper rather than at construction only.

Connections are still recycled on `maxConnLifetime` (3 minutes), which pool sizing does not affect. With a large pool in steady use, that lifetime — not the idle limit — is the dominant source of reconnects.

## TLS

Spirit supports five TLS modes: DISABLED, PREFERRED, REQUIRED, VERIFY_CA, and VERIFY_IDENTITY. The default is PREFERRED, which first attempts a TLS connection and falls back to plaintext if it fails. RDS hosts are auto-detected via hostname pattern matching (`*.rds.amazonaws.com`), and an embedded RDS CA bundle is used automatically.

## Retryable Transactions

`RetryableTransaction` is the primary mechanism for executing statements that may encounter transient errors. It classifies MySQL errors into retryable (deadlocks, lock wait timeouts, connection loss, read-only mode, killed queries) and fatal (everything else). On transient errors, the entire transaction is retried up to `MaxRetries` times.

An important subtlety is that `RetryableTransaction` inspects `SHOW WARNINGS` after every statement. This catches issues that MySQL does not surface as errors, such as `range_optimizer_max_mem_size` exceeded warnings. This particular warning is treated as fatal because it indicates a table scan will occur instead of an index range scan.

## Force Kill

Both `ForceExec` and `NewTableLock` kill blocking transactions after a delay. They wait for `DBConfig.ForceKillAfter` (zero defaults to 90% of `LockWaitTimeout`), then query `performance_schema` to identify and kill transactions that are blocking metadata lock acquisition. `ForceExec` always runs its kill worker; for `NewTableLock` the kill timer is gated on `DBConfig.ForceKill` (default true), which programmatic callers such as datasync's read-only source disable for connections that must never kill. `LOCK TABLES` returns as soon as it holds its locks, so `NewTableLock`'s timer only ever fires while the statement is waiting. A `ForceExec` statement can keep running after it holds its locks, as a table rebuild does, and the sessions holding locks on the table beside it are then concurrent traffic, not blockers. So while its statement runs, `ForceExec` checks `performance_schema.metadata_locks` every 100ms, and at the moment the delay would be reached, and kills only after the statement has been waiting for a table metadata lock for the delay. That also covers a statement that starts waiting part-way through, such as a rebuild upgrading its lock to finish: its blockers get the full delay from when the wait started. The checks run over connections from the same pool as the statement, so the pool must be able to supply a second connection. Each check is bounded to one second. A check that fails, including one that cannot get a connection, kills nothing. It leaves a wait already seen in progress, since a statement waiting for a lock keeps waiting until it gets it, and otherwise restarts the wait.

`ForceExec` retries a statement that hits a lock wait timeout after its kill worker saw it waiting for the delay and killed, up to `DBConfig.MaxRetries` attempts in total (the same budget cutover uses). Every attempt runs a fresh kill worker, so a blocker that rolls back slowly or a new blocker that arrives between attempts is killed as well, instead of the retry timing out and the migration falling into a table copy. Between attempts it waits (bounded by 30 seconds) for the killed sessions to leave `performance_schema.threads`, because `KILL` is asynchronous. Errors from the kill and cleanup steps are logged but never joined into the statement's error: callers inspect that error to detect ambiguous DDL. Any error other than a lock wait timeout, or a timeout on which no kill ran, is returned immediately. When no kill ran because a check failed, `ForceExec` logs that at the point of return, so the skipped retries are not mistaken for an exhausted budget. A timeout whose kill found an explicit table lock is also returned immediately: the kill never ends a `LOCK TABLES` session, so another attempt succeeds only if that session happens to unlock in time, and until then it holds up the table for another lock wait timeout.

`ForceExec` reserves a `sql.Conn` for the connection ID lookup, DDL, kill-worker join, and retries. Cancellation cannot return an idle session to the pool while its kill worker still runs. DDL is not wrapped in a transaction; MySQL implicitly commits `ALTER TABLE`. Use `sql.DB.BeginTx` when a real transaction is needed.

There are two important safety constraints:

1. **Transaction weight threshold**: Transactions with a weight above 1,000,000 (as reported by `information_schema.innodb_trx.trx_weight`) are never killed, because their rollback would be expensive and disruptive.
2. **Explicit table locks**: Connections holding `LOCK TABLES` are never killed. Instead, an `ErrTableLockFound` error is returned. This is because killing non-transactional locks is unsafe.

## Metadata Lock

`AdvisoryLock` provides an advisory locking mechanism using MySQL's `GET_LOCK()` function. It runs on a dedicated single-connection database pool with a background goroutine that periodically refreshes the lock. If the connection drops, it automatically reconnects and re-acquires locks.

Lock names are deterministic hashes of `schema.table`, truncated with a SHA1 suffix to fit MySQL's 64-character limit for lock names. This is used to prevent concurrent Spirit migrations on the same table.

## Table Lock

`TableLock` wraps MySQL's `LOCK TABLES ... WRITE` statement. It integrates with the force-kill mechanism to automatically kill blocking transactions if the lock cannot be acquired within the timeout. This is used during checksum setup and cutover.

Table locks are session-scoped, so the helper reserves a dedicated `sql.Conn` until `Close`. A transaction cannot provide that ownership: cancellation can automatically roll it back and return its connection to the pool while table locks remain held. Callers must defer `Close` after successful acquisition. It ignores the caller's cancellation and starts its own 30-second cleanup timeout when invoked. A successful unlock returns the connection to the pool; a failed unlock or failed acquisition discards the connection. This does not depend on driver session-reset support.

Discarding prevents reuse of the session, but does not guarantee that server-side locks have been released when `Close` returns. If a statement was interrupted, MySQL may retain the session and its locks until that statement detects the disconnected client or finishes.

## Transaction Pool

`TrxPool` pre-creates a pool of `REPEATABLE READ` transactions with `START TRANSACTION WITH CONSISTENT SNAPSHOT`. This ensures all worker threads see the same point-in-time data, which is essential for parallel checksum verification.

## See Also

- [pkg/dbconn/sqlescape](sqlescape/README.md) - Client-side SQL escaping
- [pkg/checksum](../checksum/README.md) - Uses `TrxPool` for consistent parallel checksumming
- [pkg/migration](../migration/README.md) - Uses force-kill and metadata locks during schema changes
