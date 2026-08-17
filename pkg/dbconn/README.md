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

Both `ForceExec` and `NewTableLock` implement a timer-based force-kill pattern. They wait for 90% of `LockWaitTimeout`, then query `performance_schema` to identify and kill transactions that are blocking metadata lock acquisition. `ForceExec` always arms the kill timer; for `NewTableLock` it is gated on `DBConfig.ForceKill` (default true), which programmatic callers such as datasync's read-only source disable for connections that must never kill.

There are two important safety constraints:

1. **Transaction weight threshold**: Transactions with a weight above 1,000,000 (as reported by `information_schema.innodb_trx.trx_weight`) are never killed, because their rollback would be expensive and disruptive.
2. **Explicit table locks**: Connections holding `LOCK TABLES` are never killed. Instead, an `ErrTableLockFound` error is returned. This is because killing non-transactional locks is unsafe.

## Metadata Lock

`AdvisoryLock` provides an advisory locking mechanism using MySQL's `GET_LOCK()` function. It runs on a dedicated single-connection database pool with a background goroutine that periodically refreshes the lock. If the connection drops, it automatically reconnects and re-acquires locks.

Lock names are deterministic hashes of `schema.table`, truncated with a SHA1 suffix to fit MySQL's 64-character limit for lock names. This is used to prevent concurrent Spirit migrations on the same table.

## Table Lock

`TableLock` wraps MySQL's `LOCK TABLES ... WRITE` statement. It integrates with the force-kill mechanism to automatically kill blocking transactions if the lock cannot be acquired within the timeout. This is used during the cutover phase.

## Transaction Pool

`TrxPool` pre-creates a pool of `REPEATABLE READ` transactions with `START TRANSACTION WITH CONSISTENT SNAPSHOT`. This ensures all worker threads see the same point-in-time data, which is essential for parallel checksum verification.

## See Also

- [pkg/dbconn/sqlescape](sqlescape/README.md) - Client-side SQL escaping
- [pkg/checksum](../checksum/README.md) - Uses `TrxPool` for consistent parallel checksumming
- [pkg/migration](../migration/README.md) - Uses force-kill and metadata locks during schema changes
