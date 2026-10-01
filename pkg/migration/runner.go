package migration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/block/mysql"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/buildinfo"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/checkpoint"
	"github.com/block/spirit/pkg/checksum"
	"github.com/block/spirit/pkg/concurrency"
	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/migration/check"
	"github.com/block/spirit/pkg/runstatus"
	"github.com/block/spirit/pkg/sentinel"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/throttler"
	"github.com/block/spirit/pkg/utils"
)

// postCutoverCleanupTimeout bounds the cleanup that runs after the cutover
// has committed (see run). Each statement's metadata lock wait is already
// bounded by the session's lock_wait_timeout; this also covers a server that
// stops responding.
const postCutoverCleanupTimeout = 2 * time.Minute

// These are really consts, but set to var for testing.
var (
	tableStatUpdateInterval = 5 * time.Minute
	checkpointTableName     = "_spirit_checkpoint" // const for multi-migration checkpoints.
	// Sentinel-wait timing lives in pkg/sentinel (sentinel.WaitLimit /
	// sentinel.CheckInterval / sentinel.TableName) and continuous-checksum
	// pacing in pkg/checksum (checksum.LocklessMinPassInterval /
	// checksum.DefaultLocklessRetryDelay), so they are shared with move/sync.
)

type Runner struct {
	migration *Migration
	db        *sql.DB
	dbConfig  *dbconn.DBConfig
	replicas  []*sql.DB
	// monitorDB is a small dedicated connection pool used by the Aurora
	// throttlers to poll perf-schema / global-status. Sharing the main
	// r.db pool let throttler polls queue behind chunk writes, which
	// delayed the very signal we wanted to react to (and counted the
	// throttler's own SELECT as an active query thread). nil unless Aurora
	// throttling is enabled.
	monitorDB       *sql.DB
	checkpointTable *table.TableInfo

	// aurora is the target's Aurora probe, built once by setupAutoscaling
	// before the copier exists. setupThrottler installs its throttlers, and
	// autoscale is sized from the same result, so the signal autoscaling scales
	// against is the one throttling the migration. Its MonitorDB is owned by
	// monitorDB from the moment it is built.
	aurora throttler.AuroraResult
	// autoscale is the outcome of concurrency.Engage. Zero (not Engaged)
	// unless autoscaling was requested and the target qualified.
	autoscale concurrency.Plan
	// buildAurora and auroraVCPUs are the Aurora probes setupAutoscaling runs.
	// NewRunner sets them to the throttler package's; tests replace them,
	// because CI has no Aurora to probe.
	buildAurora func(context.Context, throttler.AuroraSetup) (throttler.AuroraResult, error)
	auroraVCPUs func(context.Context, *sql.DB) (int, error)

	// Changes enccapsulates all changes
	// With a stmt, alter, table, newTable.
	changes []*tableChange

	status     status.Tracker // owns the current state and per-state timing.
	replClient change.Source  // feed contains all binlog subscription activity.
	throttler  throttler.Throttler

	// throttlerMu guards throttler. setupThrottler assigns it partway through
	// setup, while an API caller may already be polling Progress() — which reads
	// the throttler to report whether the migration is currently paused.
	throttlerMu sync.RWMutex

	copier      copier.Copier
	copyChunker table.Chunker // the chunker for copying
	// copyRowsAtResume is the settled row count the chunker restored from the
	// checkpoint, excluded from this invocation's copy aggregate.
	copyRowsAtResume uint64

	// applier is the shared write layer used by both the copier (buffered
	// copy) and the replication client (binlog deltas). Kept on the runner
	// so Status() can report its pipeline snapshot.
	applier applier.Applier

	checker         checksum.Checker
	checksumChunker table.Chunker // the chunker for checksum

	chunkerMu sync.RWMutex // protects copyChunker and checksumChunker from concurrent access

	// lastCheckpoint is when the checkpoint was last persisted and the binlog
	// position it saved, reported together on the ckpt row of the status
	// block. The checkpoint itself no longer logs at INFO on every dump
	// (#329).
	lastCheckpoint status.LastCheckpoint

	// checkpointMu serializes periodic dumps with clearing checksum evidence
	// before continuous verification, so an older dump cannot restore it.
	checkpointMu sync.Mutex

	// Used by the test-suite and some post-migration output.
	// Indicates if certain optimizations applied.
	usedInstantDDL bool
	usedInplaceDDL bool
	// usedResumeFromCheckpoint is atomic because it is also reported to API
	// callers as Progress().Resume, which they poll from their own goroutine
	// while setup is still writing it.
	usedResumeFromCheckpoint atomic.Bool

	// Attached logger
	logger *slog.Logger
	// cancelFunc cancels the migration context with a cause. Run returns that
	// cause (see status.AbortCause) instead of the context.Canceled error the
	// phases observe, so a fatal abort is reported as the failure it is and
	// not as an operator cancellation. Cancel passes a nil cause.
	cancelFunc context.CancelCauseFunc
	// cancelMu guards cancelFunc: Run assigns it while Cancel, Abort, Close
	// and fatalError may already be reading it from other goroutines.
	cancelMu sync.Mutex

	// fatalOnce makes fatalError idempotent. Without it a concurrent burst
	// of fatal events from the binlog goroutine and the migration loop
	// could double-drop the checkpoint and double-cancel the context. The
	// individual operations underneath are idempotent, but routing
	// everything through Once keeps the side-effect set small enough to
	// reason about and avoids racing with Close() teardown of r.db and
	// r.checkpointTable.
	fatalOnce sync.Once

	// watchTaskWait blocks until the WatchTask goroutines (status/checkpoint
	// dumpers) have exited. Set in startBackgroundRoutines and invoked from
	// Close() before tearing down the database connection so that no late
	// checkpoint INSERT can race with post-Close cleanup.
	watchTaskWait func()

	// MetricsSink
	metricsSink metrics.Sink

	// Correctness evidence from the most recent Run invocation.
	durableMutation   atomic.Bool
	terminalOwnership atomic.Uint32

	// testAfterCutover is a test-only seam that runs after the cutover has
	// committed and before the post-cutover cleanup.
	testAfterCutover func()
}

var (
	_ status.Task    = (*Runner)(nil)
	_ status.Aborter = (*Runner)(nil)
)

func NewRunner(m *Migration) (*Runner, error) {
	stmts, err := m.normalizeOptions()
	if err != nil {
		return nil, err
	}
	changes := make([]*tableChange, 0, len(stmts))
	for _, stmt := range stmts {
		changes = append(changes, &tableChange{
			stmt: stmt,
		})
	}
	runner := &Runner{
		migration:   m,
		logger:      slog.Default(),
		metricsSink: &metrics.NoopSink{},
		changes:     changes,
		buildAurora: func(ctx context.Context, setup throttler.AuroraSetup) (throttler.AuroraResult, error) {
			return setup.Build(ctx)
		},
		auroraVCPUs: throttler.AuroraVCPUs,
	}
	for _, change := range changes {
		change.runner = runner // link back.
	}
	return runner, nil
}

// checksumOffPoolConns is the connection headroom the checksum phase needs on
// top of its REPEATABLE READ transaction pool, for the two things it does that
// the pool does not cover. Both are serialized, so one connection each:
//
//   - Chunk repair. When a chunk mismatches, the recopier runs its DELETE and
//     then its re-read of the source on r.db rather than on the pooled read-view
//     transaction, and repairs are serialized under the checker's recopyLock —
//     so it is one connection at a time. (The rewrite itself goes through the
//     applier, whose write connections are already budgeted below as maxWrite;
//     the copy phase has finished by then, so that headroom is free.)
//   - Chunker prefetch. chunker.Next() runs a SELECT ... LIMIT 1 OFFSET n on
//     Ti.Db to find the next chunk boundary, also off-pool. Workers call it
//     concurrently but the chunker's own mutex serializes them, so only one
//     such query is ever in flight.
//
// This reserve matters more than it looks. The transaction pool is sized to the
// read ceiling and every one of its transactions pins a connection for the whole
// phase whether or not a worker has it checked out, so there is no incidental
// slack left to absorb either query — without the reserve, chunk dispatch would
// queue behind applier and control-plane connections.
const checksumOffPoolConns = 2

// controlPlaneConns is the connection headroom the main pool reserves above
// the copy hot path (Threads read workers + WriteThreads applier workers) for
// the periodic control-plane queries that also run on r.db:
//
//   - +1 checkpoint INSERT          (every CheckpointDumpInterval)
//   - +1 replication-flush poll     (every DefaultFlushInterval, reads gtid_executed)
//   - +len(changes) table-stats     (AutoUpdateStatistics runs one goroutine per change table)
//
// A single fixed spare (the historical "+1") could not cover these once the
// copier + applier saturated the budget: a saturated pool left checkpoint, the
// flush poll, and the stats updater serializing behind one connection — adding
// latency to checkpoints and stretching flush durations. The squeeze is worst
// on non-autoscaling instances, where maxWrite == WriteThreads leaves no
// incidental slack (an autoscaling pool sized for 2× WriteThreads happens to
// carry spare connections while the applier runs near its start size).
// Throttler polls are NOT counted here — they run on the dedicated monitorDB
// pool (see the monitorDB field).
func (r *Runner) controlPlaneConns() int {
	return len(r.changes) + controlPlaneFixedConns
}

// controlPlaneFixedConns is the part of controlPlaneConns that does not scale
// with the number of change tables: the checkpoint INSERT and the
// replication-flush poll.
const controlPlaneFixedConns = 2

// drainReserveConns is what the change-feed drain keeps while the checksum runs.
// One connection, not the flush concurrency: a narrow drain is a slow drain and
// that is a fine trade, but a drain with nothing to check out does not advance
// the binlog position at all, and that position has to keep moving or the
// migration runs out its retention window. Slow is a late migration; stopped is
// a failed one.
const drainReserveConns = 1

// checksumPhaseReserve is the number of connections that must stay checkout-able
// while the checksum holds its read transactions open.
//
// The checksum is the only phase that pins connections rather than borrowing
// them: every transaction in its pool holds one for the whole phase whether or
// not a worker has it checked out. So anything that has to keep running
// alongside it needs its share carved out of the pool up front — the checksum's
// own off-pool queries (checksumOffPoolConns), the control-plane queries, and
// the drain. The copier and applier are not in the reserve because they have
// finished by the time the checksum starts.
//
// Lockless verification pins nothing — it borrows a connection per read and
// gives it straight back — so the reserve is strictly conservative for it and
// is deliberately not narrowed: the flag is experimental, and an over-reserved
// pool costs read concurrency while an under-reserved one costs a stalled
// migration.
func (r *Runner) checksumPhaseReserve() int {
	return checksumOffPoolConns + r.controlPlaneConns() + drainReserveConns
}

// minChecksumPhaseReserve is checksumPhaseReserve for the smallest migration
// there is, one change table. Migration.Validate needs the number before the
// statement has been parsed into change tables, so it uses this lower bound; the
// runtime fit in setupCopierCheckerAndReplClient uses the real count.
const minChecksumPhaseReserve = checksumOffPoolConns + controlPlaneFixedConns + 1 + drainReserveConns

func (r *Runner) SetMetricsSink(sink metrics.Sink) {
	r.metricsSink = sink
}

func (r *Runner) SetLogger(logger *slog.Logger) {
	r.logger = logger
}

// dropStaleCopyTables drops the _new and checkpoint tables that an earlier,
// interrupted copy of the table may have left, after the ALTER has completed
// with MySQL's own DDL. That state describes the table before this ALTER
// changed it, so it can no longer be resumed from, and a later copy-based run
// would only discard it. Without this, it stays behind indefinitely.
//
// Auxiliary table names are truncated, so two long table names can share them
// (see utils.AuxTableName), and the state may belong to the other table. A
// _new table without a checkpoint table may not be Spirit's at all (other
// online schema change tools use the same name, and may still have triggers
// writing to it). So the tables are dropped only when the checkpoint table
// exists and its latest row names this table. Otherwise (no checkpoint table,
// or one that cannot be read, is empty, or names another or no table) nothing
// is dropped and the reason is logged.
//
// A failure is logged, not returned: the ALTER has already been applied.
func (r *Runner) dropStaleCopyTables(ctx context.Context) {
	if len(r.changes) != 1 {
		return // attemptMySQLDDL only supports single-table changes.
	}
	tableName := r.changes[0].table.TableName
	newName := utils.NewTableName(tableName)
	ckpt := r.checkpointTbl()
	ckptName := r.checkpointTableName()
	leaveInPlace := func(reason string, args ...any) {
		r.logger.Warn("not dropping tables from an earlier interrupted migration: "+reason,
			append([]any{"new-table", newName, "checkpoint-table", ckptName}, args...)...)
	}

	ckptExists, err := ckpt.Exists(ctx)
	if err != nil {
		leaveInPlace("could not check whether the checkpoint table exists", "error", err)
		return
	}
	if !ckptExists {
		// Only a _new table can be left: without a checkpoint there is no
		// evidence that Spirit created it, so leave it in place.
		var n int
		if err := r.db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
			r.changes[0].table.SchemaName, newName).Scan(&n); err != nil {
			leaveInPlace("could not check whether the new table exists", "error", err)
			return
		}
		if n > 0 {
			leaveInPlace("there is no checkpoint table to confirm the new table was created by Spirit for this table")
		}
		return
	}
	rec, err := ckpt.ReadLatest(ctx)
	if err != nil {
		leaveInPlace("could not read the checkpoint to confirm it belongs to this table", "error", err)
		return
	}
	if rec.OriginalTableName != tableName {
		leaveInPlace("the checkpoint does not belong to this table", "checkpoint-original-table", rec.OriginalTableName)
		return
	}
	// _new first: the checkpoint is the evidence of ownership, so it must
	// outlive _new. If a drop fails, stop and keep the checkpoint so a later
	// run can retry.
	for _, name := range []string{newName, ckptName} {
		if err := dbconn.Exec(ctx, r.db, "DROP TABLE IF EXISTS %n", name); err != nil {
			r.logger.Error("could not drop a stale table from an earlier interrupted migration", "table", name, "error", err)
			return
		}
		r.logger.Info("dropped a stale table from an earlier interrupted migration", "table", name)
	}
}

// attemptMySQLDDL tries to perform the DDL using MySQL's built-in
// either with INSTANT or known safe INPLACE operations.
func (r *Runner) attemptMySQLDDL(ctx context.Context) error {
	if len(r.changes) > 1 {
		return errors.New("attemptMySQLDDL only supports single-table changes")
	}
	return r.changes[0].attemptMySQLDDL(ctx)
}

// recordCopyCompleted reports the copy aggregate settled during this
// Runner.Run invocation. The chunker restores its settled row count from the
// checkpoint, while its chunk count starts afresh, so the restored rows are
// subtracted here to keep the two figures on the same invocation.
func (r *Runner) recordCopyCompleted() {
	chunker := r.copier.GetChunker()
	if chunker == nil {
		return
	}
	_, chunks, _ := chunker.Progress()
	r.status.RecordCopyCompleted(chunker.RowsCopied()-r.copyRowsAtResume, chunks)
}

func (r *Runner) runCopy(ctx context.Context) error {
	defer r.recordCopyCompleted()
	return r.status.DoContext(ctx, status.CopyRows, func() error {
		return r.copier.Run(ctx)
	})
}

func (r *Runner) Run(ctx context.Context) (retErr error) {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	r.cancelMu.Lock()
	r.cancelFunc = cancel
	r.cancelMu.Unlock()
	r.status.SetMetricsSink(r.metricsSink, r.logger)
	r.status.Begin()
	r.durableMutation.Store(false)
	r.terminalOwnership.Store(uint32(status.WorkflowTerminalOwnershipNone))
	defer func() {
		r.recordWorkflowError(retErr)
	}()
	// Registered after recordWorkflowError so it runs first, and before the
	// deferred cancelFunc(nil) so the cause read is the one that aborted us.
	defer func() {
		retErr = status.AbortCause(ctx, retErr)
	}()
	bi := buildinfo.Get()
	r.logger.Info("Starting spirit migration",
		"version", bi.Version,
		"commit", bi.Commit,
		"build-date", bi.Date,
		"go", bi.GoVer,
		"dirty", bi.Modified,
		"concurrency", r.migration.Threads,
		"target-chunk-size", r.migration.TargetChunkSize,
	)

	// Create a database connection
	// It will be closed in r.Close()
	var err error
	r.dbConfig = dbconn.NewDBConfig()
	// The pool size, TLS, interpolation and lock timeouts come from the shared
	// flags.
	r.migration.Common.ApplyTo(r.dbConfig)
	r.migration.Cutover.ApplyTo(r.dbConfig)
	// ForceKill is always enabled for migrations (true by default in NewDBConfig).
	// The pool is --max-connections, verbatim and once. Nothing recomputes it,
	// no phase ratchets it, and no ceiling derived later raises it — an operator
	// budgeting against max_user_connections needs a number they can subtract.
	//
	// The copier, applier, drain and control-plane queries all share it, and
	// their ceilings can add up to more than it holds. For the copier and the
	// applier that costs throughput and nothing else: a worker waiting on
	// checkout is a worker that will eventually run, and the work it was going
	// to do is still there when it does.
	//
	// Two paths are not like that:
	//
	//   - Read workers, because the checksum pins one connection per transaction
	//     for a whole phase — a ceiling the pool cannot hold blocks on checkout
	//     with a table lock held. Migration.Validate rejects a pool that cannot
	//     hold the configured count; readBoundsForPool handles the count
	//     autoscaling derives later.
	//   - The drain, because its work expires. A flush batch queueing behind a
	//     saturated copy is spending the binlog retention window, and running out
	//     of that window ends the migration rather than slowing it. It is why the
	//     drain has a reserve during the checksum (drainReserveConns) rather than
	//     being left to contend like the copier.
	r.dbConfig.MaxOpenConnections = r.migration.MaxConnections
	r.db, err = dbconn.New(r.dsn(), r.dbConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to main database (DSN: %s): %w", dbconn.RedactDSN(r.dsn()), err)
	}

	if len(r.changes) == 1 {
		// We only allow non-ALTERs (i.e. CREATE TABLE, DROP TABLE, RENAME TABLE)
		// in single table mode.
		if !r.changes[0].stmt.IsAlterTable() {
			// The statement is the user's own SQL and is spliced in with %r:
			// it may contain % characters in literals (e.g. COMMENT
			// '100%new') that must not be format-interpreted.
			err := dbconn.Exec(ctx, r.db, "%r", sqlescape.RawSQL(r.changes[0].stmt.Statement))
			if err != nil {
				if ambiguous := ambiguousDDLError(err); ambiguous != nil {
					return ambiguous
				}
				return err
			}
			r.logger.Info("apply complete")
			return nil
		}
	}
	// Reject ALTERs that contain unsupported clauses such as ALGORITHM=
	// or LOCK= *before* any DDL is attempted directly on MySQL.
	// attemptMySQLDDL below prepends its own ALGORITHM= (and LOCK=)
	// assertions, and MySQL resolves duplicate options last-one-wins: a
	// user-supplied "ALGORITHM=COPY, LOCK=SHARED" would override our
	// ALGORITHM=INSTANT and execute as a blocking table rebuild. The
	// preflight illegalClause check also rejects these clauses, but it
	// only runs after the direct DDL attempt has already failed, which is
	// too late to protect this path. This is a pure parse-level check, so
	// it runs before any table introspection or advisory locking.
	for _, change := range r.changes {
		if !change.stmt.IsAlterTable() {
			continue // the check only applies to ALTER TABLE statements
		}
		if err := change.stmt.AlterContainsUnsupportedClause(); err != nil {
			return err
		}
	}
	// Set info for all of the tables.
	tables := make([]*table.TableInfo, 0, len(r.changes))
	for _, change := range r.changes {
		change.table = table.NewTableInfo(r.db, change.stmt.Schema, change.stmt.Table)
		if err := change.table.SetInfo(ctx); err != nil {
			return err
		}
		tables = append(tables, change.table)
	}
	// Run the statement-scope checks before MySQL's native DDL is attempted.
	// A ScopeStatement failure is documented as a refusal a caller can report
	// as certain (see check.StatementRefusal), so the runner must refuse
	// exactly those statements, including ones the native DDL could complete.
	// Run later, as preflight is, they would only apply when the native
	// attempt fails: an INSTANT ADD COLUMN on a table with a FLOAT or BIT in
	// its primary key would succeed where a planning tool reported a refusal.
	if err := r.runChecks(ctx, check.ScopeStatement); err != nil {
		return err
	}

	// Take a single advisory lock for all tables to prevent concurrent DDL.
	// This uses a single DB connection instead of one per table.
	// We release the lock when this function finishes executing.
	//
	// A multi-table migration additionally takes a schema-scoped lock. (len > 1
	// always means several *distinct* tables — spirit rejects multiple
	// statements against the same table, see #487.) Such migrations all share
	// one _spirit_checkpoint/_spirit_sentinel per schema, so only one may run
	// per schema at a time. Single-table migrations skip it and may run
	// concurrently (serialized per-table by the table locks above).
	var lockOpts []func(*dbconn.AdvisoryLock)
	if len(r.changes) > 1 {
		lockOpts = append(lockOpts, dbconn.WithMultiTableSchemaLock(r.changes[0].table.SchemaName))
	}
	lock, err := dbconn.NewAdvisoryLock(ctx, r.dsn(), tables, r.dbConfig, r.logger, lockOpts...)
	if err != nil {
		if len(r.changes) > 1 {
			return fmt.Errorf("could not start atomic multi-table migration (another one may already be running in schema %q, or one of its tables is busy): %w", r.changes[0].table.SchemaName, err)
		}
		return err
	}

	// Release the lock
	defer func() {
		if err := lock.Close(); err != nil {
			r.logger.Error("failed to release advisory lock", "error", err)
		}
	}()
	// This step is technically optional, but first we attempt to
	// use MySQL's built-in DDL. This is because it's usually faster
	// when it is compatible. If it returns no error, that means it
	// has been successful and the DDL is complete.
	// Note: this function returns an error when in multi-table mode.
	err = r.attemptMySQLDDL(ctx)
	if err == nil {
		r.durableMutation.Store(true)
		r.dropStaleCopyTables(ctx)
		r.logger.Info("apply complete",
			"instant-ddl", r.usedInstantDDL,
			"inplace-ddl", r.usedInplaceDDL,
		)
		return nil // success!
	}
	// A direct-DDL failure is normally expected and ignored: we fall through
	// to the copy algorithm below. But if the DDL's outcome is unknown, the
	// source table may already carry the ALTER, and copying from it would
	// build the _new table from an unexpected schema. Abort instead.
	if errors.Is(err, status.ErrOwnershipAmbiguous) {
		return err
	}

	// Perform preflight basic checks.
	if err := r.runChecks(ctx, check.ScopePreflight); err != nil {
		return err
	}

	// Perform setup steps, including resuming from a checkpoint (if available)
	// and creating the new and checkpoint tables.
	// The replication client is also created here.
	if err := r.setup(ctx); err != nil {
		return err
	}

	// Run post-setup checks
	if err := r.runChecks(ctx, check.ScopePostSetup); err != nil {
		return err
	}

	// Perform the main copy rows task. This is where the majority
	// of migrations usually spend time. It is not strictly necessary,
	// but we always recopy the last-bit, even if we are resuming
	// partially through the checksum.
	if err := r.runCopy(ctx); err != nil {
		return err
	}
	r.logger.Info("copy rows complete")

	// Disable both watermark optimizations so that all changes can be flushed.
	// For non-memory-comparable PKs this also drains the buffered map and
	// switches the subscription into FIFO queue mode (see
	// pkg/change/subscription_buffered.go), so the call can return an error.
	if err := r.replClient.SetWatermarkOptimization(ctx, false); err != nil {
		return err
	}

	// Post-copy phase: catch up on replClient apply, run ANALYZE TABLE
	// so cutover stats are fresh, and run the initial checksum.
	if err := r.postCopyPhase(ctx); err != nil {
		return err
	}

	// Reuse the configured checker while waiting for a sentinel, including one
	// created manually. The completed initial checksum remains the cutover gate.
	if r.migration.WaitsOnSentinel() {
		if err := r.status.DoContext(ctx, status.WaitingOnSentinelTable, func() error {
			return sentinel.Wait(ctx, sentinel.WaitConfig{
				Exists: func(ctx context.Context) (bool, error) { return sentinel.Exists(ctx, r.db) },
				RunChecksum: func(ctx context.Context) error {
					// Clear evidence before background work, including on a hard crash.
					if err := r.invalidateChecksumWatermark(context.WithoutCancel(ctx)); err != nil {
						return err
					}
					return r.checker.RunContinuous(ctx)
				},
				InvalidateWatermark: r.invalidateChecksumWatermark,
				Logger:              r.logger,
			})
		}); err != nil {
			return err
		}
	}
	// Run any checks that need to be done pre-cutover.
	if err := r.runChecks(ctx, check.ScopeCutover); err != nil {
		return err
	}
	// It's time for the final cut-over, where
	// the tables are swapped under a lock.
	if err := r.status.DoContext(ctx, status.CutOver, func() error {
		cutoverCfg := []*cutoverConfig{}
		for _, change := range r.changes {
			cutoverCfg = append(cutoverCfg, &cutoverConfig{
				table:          change.table,
				newTable:       change.newTable,
				oldTableName:   change.oldTableName(),
				useTestCutover: r.migration.useTestCutover, // indicates we want the test cutover
				// An AUTO_INCREMENT=N in the ALTER is honored, as by a native ALTER.
				keepNewAutoIncrement: change.stmt.SetsAutoIncrement(),
			})
		}
		cutover, err := NewCutOver(r.db, cutoverCfg, r.replClient, r.dbConfig, r.logger)
		if err != nil {
			return err
		}
		// Schema-change notifications are not acted on from here on, so a
		// trigger or foreign key created while the cutover waits for its lock
		// is only caught by checking again once the lock is held.
		cutover.checksUnderLock = func(ctx context.Context) error {
			return r.runChecks(ctx, check.ScopeCutoverLocked)
		}
		// Drop the _old table if it exists. This ensures
		// that the rename will succeed (although there is a brief race)
		for _, change := range r.changes {
			if err := change.dropOldTable(ctx); err != nil {
				return err
			}
		}
		if err := cutover.Run(ctx); err != nil {
			return fmt.Errorf("cutover failed: %w", err)
		}
		r.durableMutation.Store(true)
		if r.testAfterCutover != nil {
			r.testAfterCutover()
		}
		return nil
	}); err != nil {
		return err
	}
	// The cutover has committed, so the migration has succeeded even if ctx
	// is cancelled from here on. The cleanup below that decides that outcome
	// runs on a detached, bounded context: with ctx, a cancel that arrived
	// during the cutover would fail it, report the committed migration as
	// failed, and leave a checkpoint that the next run tries to resume from
	// without a _new table (issue #1338). Dropping _old stays on ctx: it can
	// be slow on a large table, a cancel should not wait for it, and its
	// failure is only logged.
	cleanupCtx, cancelCleanup := context.WithTimeout(context.WithoutCancel(ctx), postCutoverCleanupTimeout)
	defer cancelCleanup()
	if !r.migration.SkipDropAfterCutover {
		for _, change := range r.changes {
			if err := change.dropOldTable(ctx); err != nil {
				// Don't return the error because our automation
				// will retry the migration (but it's already happened)
				r.logger.Error("migration successful but failed to drop old table",
					"table", change.oldTableName(),
					"error", err,
				)
			} else {
				r.logger.Info("successfully dropped old table",
					"table", change.oldTableName(),
				)
			}
		}
	} else {
		r.logger.Info("skipped dropping old table")
	}
	_, copiedChunks, _ := r.copyChunker.Progress()
	r.logger.Info("apply complete",
		"instant-ddl", r.usedInstantDDL,
		"inplace-ddl", r.usedInplaceDDL,
		"total-chunks", copiedChunks,
		"copy-rows-time", r.status.Duration(status.CopyRows).Round(time.Second).String(),
		"checksum-time", r.status.Duration(status.Checksum).Round(time.Second).String(),
		"total-time", r.status.TotalElapsed().Round(time.Second).String(),
	)
	// cleanup all the tables
	for _, change := range r.changes {
		if err := change.cleanup(cleanupCtx); err != nil {
			return err
		}
	}
	// drop the checkpoint table
	if r.checkpointTable != nil {
		if err := r.checkpointTbl().Drop(cleanupCtx); err != nil {
			return err
		}
	}
	return nil
}

// postCopyPhase runs the work that happens between copy-rows and the
// sentinel wait: drain the binlog backlog, run ANALYZE TABLE, and
// perform the initial checksum. When defer-cutover is not in use this
// is also the last phase before cutover.
func (r *Runner) postCopyPhase(ctx context.Context) error {
	// Disable the periodic flush and flush all pending events.
	// We want it disabled for ANALYZE TABLE and acquiring a table lock
	// *but* it will be started again briefly inside of the checksum
	// runner to ensure that the lag does not grow too long.
	if err := r.status.DoContext(ctx, status.ApplyChangeset, func() error {
		r.replClient.StopPeriodicFlush()
		return r.replClient.Flush(ctx)
	}); err != nil {
		return err
	}

	// Run ANALYZE TABLE to update the statistics on the new table.
	// This is required so on cutover plans don't go sideways, which
	// is at elevated risk because the batch loading can cause statistics
	// to be out of date.
	if err := r.status.DoContext(ctx, status.AnalyzeTable, func() error {
		r.logger.Info("Running ANALYZE TABLE")
		for _, change := range r.changes {
			if err := dbconn.Exec(ctx, r.db, "ANALYZE TABLE %n.%n", change.newTable.SchemaName, change.newTable.TableName); err != nil {
				return err
			}

			// Disable the auto-update statistics go routine. This is because the
			// checksum uses a consistent read and doesn't see any of the new rows in the
			// table anyway. Chunking in the space where the consistent reads may need
			// to read a lot of older versions is *much* slower.
			// In a previous migration:
			// - The checksum chunks were about 100K rows each
			// - When the checksum reached the point at which the copier had reached,
			//   the chunks slowed down to about 30 rows(!)
			// - The checksum task should have finished in the next 5 minutes, but instead
			//   the projected time was another 40 hours.
			// My understanding of MVCC in MySQL is that the consistent read threads may
			// have had to follow pointers to older versions of rows in UNDO, which is a
			// linked list to find the specific versions these transactions needed. It
			// appears that it is likely N^2 complexity, and we are better off to just
			// have the last chunk of the checksum be slow and do this once rather than
			// repeatedly chunking in this range.
			change.table.DisableAutoUpdateStatistics.Store(true)
		}
		return nil
	}); err != nil {
		return err
	}

	// The checksum is ONLINE after an initial lock
	// for consistency. It is the main way that we determine that
	// this program is safe to use even when immature.
	return r.checksum(ctx)
}

// runChecks wraps around check.RunChecks and adds the context of this migration
// We redundantly run checks, once per change.
func (r *Runner) runChecks(ctx context.Context, scope check.ScopeFlag) error {
	for _, change := range r.changes {
		if err := check.RunChecks(ctx, check.Resources{
			DB:            r.db,
			Replicas:      r.replicas,
			Table:         change.table,
			NewTable:      change.newTable,
			Statement:     change.stmt,
			Threads:       r.migration.Threads,
			ReplicaMaxLag: r.migration.ReplicaMaxLag,
			// For the pre-run checks we don't have a DB connection yet.
			// Instead we check the credentials provided.
			Host:                 r.migration.Host,
			Username:             r.migration.Username,
			Password:             *r.migration.Password,
			TLSMode:              r.migration.TLSMode,
			TLSCertificatePath:   r.migration.TLSCertificatePath,
			SkipDropAfterCutover: r.migration.SkipDropAfterCutover,
		}, r.logger, scope); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) dsn() string {
	cfg := mysql.NewConfig()
	cfg.User = r.migration.Username
	cfg.Passwd = *r.migration.Password
	cfg.Net = "tcp"
	cfg.Addr = r.migration.Host
	cfg.DBName = r.changes[0].stmt.Schema
	return cfg.FormatDSN()
}

func (r *Runner) checkpointTableName() string {
	// We also call the create functions for the sentinel
	// and checkpoint tables.
	if len(r.changes) > 1 {
		return checkpointTableName
	}
	return utils.CheckpointTableName(r.changes[0].table.TableName)
}

// checkpointTbl returns a handle to this migration's checkpoint table (shared
// machinery in pkg/checkpoint). Always Transient: single-table migrations get a
// per-table table, and atomic multi-table migrations share one _spirit_checkpoint
// per schema but only one runs per schema at a time (enforced by the schema lock
// in Run), so it has a single owner either way. Constructed on demand — cheap,
// and avoids an ordering dependency on when a cached handle would be set during
// setup. Callers must have r.db and r.changes[0].table initialized (true at
// every call site: resume, create, dump, drop).
func (r *Runner) checkpointTbl() *checkpoint.Table {
	// r.db's selected schema is the migrated schema (same connection sentinel
	// uses), so the checkpoint table lands there — no schema is threaded in.
	return checkpoint.NewTable(r.db, r.checkpointTableName(), checkpoint.Transient)
}

// setupCopierCheckerAndReplClient builds the copier, the checker, and the
// change source. resumePosition is the checkpointed source position when
// resuming ("" for a fresh migration); it decides the change source's
// coordinate scheme — see change.NewAutoClient.
func (r *Runner) setupCopierCheckerAndReplClient(ctx context.Context, resumePosition, checksumWatermark string) error {
	var err error

	copierAutoscale, checksumAutoscale := r.autoscaleConfigs()
	r.checkpointTable = table.NewTableInfo(r.db, r.changes[0].table.SchemaName, r.checkpointTableName())

	// We always create an applier — the replication client requires one to
	// apply row images directly from the binlog (the buffered subscription
	// path). This is what sidesteps the MySQL binlog/visibility race that
	// caused silent row loss under load (issue #746): there is no SELECT
	// FROM original ... after the row event arrives, the row image *is* the
	// applied state.
	//
	// The same applier is handed to the copier, so the copy and the binlog
	// replay share one write pipeline.
	appl, err := applier.New(
		[]applier.Target{{DB: r.db}},
		&applier.ApplierConfig{
			Logger:      r.logger,
			DBConfig:    r.dbConfig,
			Threads:     r.migration.WriteThreads,
			MetricsSink: r.metricsSink,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to create applier: %w", err)
	}
	r.applier = appl

	// Create copier with the prepared chunker
	r.copier, err = copier.NewCopier(r.copyChunker, &copier.CopierConfig{
		Concurrency: r.migration.Threads,
		Throttler:   &throttler.Noop{},
		Logger:      r.logger,
		MetricsSink: r.metricsSink,
		DBConfig:    r.dbConfig,
		Applier:     appl,
		Autoscale:   copierAutoscale,
	})
	if err != nil {
		return err
	}

	// Create the change source. The GTID vs binlog file+position choice is
	// automatic: a resumed migration stays in the coordinate scheme its
	// checkpoint was written in (resumePosition), and a fresh one uses GTIDs
	// whenever the server has them enabled.
	replConfig := r.replClientConfig(r.autoscale.FlushConcurrency, r.autoscale.FlushBatchSize)
	r.replClient, err = change.NewAutoClient(ctx, r.db, r.migration.Host, r.migration.Username, *r.migration.Password, appl, replConfig, resumePosition)
	if err != nil {
		return err
	}
	// For each of the changes, we know the new table exists now
	// So we should call SetInfo to populate the columns etc.
	for _, change := range r.changes {
		if err := change.newTable.SetInfo(ctx); err != nil {
			return err
		}
		if err := r.replClient.AddSubscription(change.table, change.newTable, change.chunker); err != nil {
			return err
		}
	}

	// Choosing the checker is the whole of what the flag does. Everything
	// downstream — the repair policy, the resume watermark, pacing, the pool
	// reserve, the status block, cutover — is written once against the Checker
	// contract and does not ask which one it got.
	if r.migration.EnableExperimentalLocklessChecksum {
		r.logger.Warn("experimental lockless checksum enabled; verification uses optimistic reads, cutover locking is unchanged")
	}
	// Repair policy is not configured: both checkers repair in Run (the initial
	// checksum) and report in RunContinuous (the sentinel wait).
	r.checker, err = checksum.NewChecker([]*sql.DB{r.db}, r.checksumChunker, []change.Source{r.replClient}, &checksum.CheckerConfig{
		Lockless:        r.migration.EnableExperimentalLocklessChecksum,
		Watermark:       checksumWatermark,
		Concurrency:     r.migration.Threads,
		TargetChunkTime: table.ChunkerDefaultTarget,
		DBConfig:        r.dbConfig,
		Logger:          r.logger,
		MaxRetries:      3,
		YieldTimeout:    r.migration.ChecksumYieldTimeout,
		MetricsSink:     r.metricsSink,
		// Repairing a mismatched chunk writes through the same applier the copy
		// and binlog-apply phases use, so a repair inherits the configured write
		// concurrency instead of standing up a second write path. The copier has
		// stopped it by the time the checksum runs; the checker starts and stops
		// it around each repair.
		Applier: appl,
		// The checksum reads with its own pool, so it shares the read side's
		// bounds: it starts at Threads and grows to maxRead, which is already in
		// the pool sizing above. The copier's readers have finished by the time the
		// checksum runs, so the checksum reuses that headroom rather than adding to
		// it. The only checksum-specific term is checksumOffPoolConns, for the
		// queries that run off-pool.
		Autoscale: checksumAutoscale,
	})

	return err
}

// newMigration is called when resumeFromCheckpoint has failed.
// It performs all the initial steps to prepare for a fresh migration.
func (r *Runner) newMigration(ctx context.Context) error {
	// A resume that got far enough to take a baseline and then failed
	// definitively lands here; the fresh chunker counts from zero.
	r.copyRowsAtResume = 0
	// This is the non-resume path, so we need to create each of the new tables
	// And apply the alters. This doesn't apply to resume.
	for _, change := range r.changes {
		if err := change.createNewTable(ctx); err != nil {
			return err
		}
		if err := change.alterNewTable(ctx); err != nil {
			return err
		}
	}
	if err := r.checkpointTbl().Create(ctx); err != nil {
		return err
	}
	if r.migration.DeferCutOver {
		// Idempotent (CREATE IF NOT EXISTS): the sentinel is shared by every
		// migration in the schema and must never pass through a "table absent"
		// state that a concurrent deferred cutover's poll could observe.
		if err := sentinel.Create(ctx, r.db); err != nil {
			return err
		}
	}
	// Now that new tables are created, we can initialize the chunker
	if err := r.initChunkers(); err != nil {
		return err
	}
	// Finally we open the chunker, since in the resume
	// path we call OpenAtWatermark instead.
	if err := r.copyChunker.Open(); err != nil {
		return err // could not open chunker
	}

	if err := r.checksumChunker.Open(); err != nil {
		return err
	}

	// This is setup the same way in both code-paths,
	// but we need to do it before we finish resumeFromCheckpoint
	// because we need to check that the binlog file exists.
	if err := r.setupCopierCheckerAndReplClient(ctx, "", ""); err != nil {
		return err
	}
	// Start the change feed now
	if err := r.replClient.Start(ctx); err != nil {
		return err
	}
	return nil
}

// closeReplicas closes all open replica database connections, aggregating
// errors with errors.Join so a failure on one replica doesn't leak the
// handles of the rest. Matches the cleanup discipline in Close().
func (r *Runner) closeReplicas() error {
	var errs []error
	for _, replica := range r.replicas {
		if err := replica.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	r.replicas = nil
	return errors.Join(errs...)
}

// setThrottler publishes the resolved throttler. It is written once during
// setup, but Progress() may be reading it concurrently — see throttlerMu.
func (r *Runner) setThrottler(t throttler.Throttler) {
	r.throttlerMu.Lock()
	defer r.throttlerMu.Unlock()
	r.throttler = t
}

// currentThrottler returns the resolved throttler, or nil if setup has not got
// that far (or found nothing to throttle on, in which case the copier keeps its
// own Noop).
func (r *Runner) currentThrottler() throttler.Throttler {
	r.throttlerMu.RLock()
	defer r.throttlerMu.RUnlock()
	return r.throttler
}

// autoscaleConfigs turns the autoscaling plan into the copier's and the
// checksum's scaling bounds, after fitting the read side to the connection
// pool. It may lower r.migration.Threads, which both the copier and the
// checksum take as their starting read concurrency. The result depends only
// on the plan and the flags, so calling it again (a failed resume falling
// back to a fresh migration) returns the same configs.
func (r *Runner) autoscaleConfigs() (copier.AutoscaleConfig, checksum.AutoscaleConfig) {
	// The thread counts and bounds were settled once, before resume or a fresh
	// migration started, by setupAutoscaling: when autoscaling engaged,
	// r.migration.Threads and WriteThreads already hold the instance-derived
	// starting sizes and r.autoscale holds the bounds.
	autoscaleEnabled := r.autoscale.Engaged
	// When autoscaling did not engage no pool can grow, so provision the
	// configured counts and no more. That matters most for the checksum, which
	// turns its read ceiling into transactions started serially under the
	// table lock whether or not scaling can ever reach it — capacity nothing
	// can use, paid for in lock time.
	maxRead, maxWrite := r.migration.Threads, r.migration.WriteThreads
	if autoscaleEnabled {
		maxRead, maxWrite = r.autoscale.MaxReadThreads, r.autoscale.MaxWriteThreads
	}
	// Fit both read bounds to the pool. The start matters as much as the ceiling
	// here: r.migration.Threads is what the checksum takes as its Concurrency and
	// what the copier takes as its starting read-worker count, and both of them
	// floor the ceiling back up to it (see readBoundsForPool). Under autoscaling
	// it is instance-derived and has never been checked against the operator's
	// pool.
	if fitStart, fitCeiling := dbconn.ReadBoundsForPool(r.migration.Threads, maxRead, r.migration.MaxConnections, r.checksumPhaseReserve()); fitStart != r.migration.Threads || fitCeiling != maxRead {
		r.logger.Warn("read thread bounds do not fit the connection pool; capping them",
			"threads", r.migration.Threads, "capped_threads", fitStart,
			"read_ceiling", maxRead, "capped_read_ceiling", fitCeiling,
			"max_connections", r.migration.MaxConnections,
			"reserved", r.checksumPhaseReserve())
		r.migration.Threads, maxRead = fitStart, fitCeiling
	}
	copierAutoscale := copier.AutoscaleConfig{
		Enabled:        autoscaleEnabled,
		StartThreads:   r.migration.WriteThreads,
		MaxThreads:     maxWrite,
		MaxReadThreads: maxRead,
	}
	checksumAutoscale := checksum.AutoscaleConfig{
		Enabled:    autoscaleEnabled,
		MaxThreads: maxRead,
	}
	return copierAutoscale, checksumAutoscale
}

// replClientConfig assembles the change client's configuration from the flush
// shape the caller derived. Extracted from setupCopierCheckerAndReplClient so
// the wiring can be asserted directly: every field here is a behaviour of the
// feed that is otherwise only reachable through a full migration, and a dropped
// assignment would silently disable the feature it carries rather than fail.
func (r *Runner) replClientConfig(flushConcurrency, flushBatchSize int) *change.ClientConfig {
	cfg := change.NewClientDefaultConfig()
	cfg.Logger = r.logger
	cfg.CancelFunc = r.fatalError
	cfg.DBConfig = r.dbConfig
	// Zero for either of these means the change package's own default, which is
	// what a non-Aurora or too-small instance gets.
	cfg.FlushConcurrency = flushConcurrency
	cfg.BatchSize = flushBatchSize
	cfg.UnderLoad = r.flushUnderLoad
	return cfg
}

// flushUnderLoad is the change feed's load signal (change.ClientConfig.UnderLoad):
// whether the target is loaded enough that the drain should narrow itself.
//
// It reads GradualOnly, the same restriction the write-thread autoscaler and the
// checksum use, so the drain reacts to the Aurora *load* signals and not to
// replica lag. Lag is an SLO budget rather than a load gauge, and the flush is
// the one path that cannot afford to be paced by a budget: narrowing it does not
// reduce the lag it would be reacting to, while the binlog position it stops
// advancing is a retention deadline. GradualOnly returns a Noop when there is no
// continuous signal — a non-Aurora target — so the drain keeps its configured
// width there, which is correct: without a load gauge there is nothing to shed
// against.
//
// Resolved per call rather than captured, because setup replaces the throttler
// (setThrottler) after the change client is built, and nil until it does. The
// nil check is belt-and-braces — GradualOnly already answers a nil throttler
// with a Noop — but a drain must not panic on a signal it consults for advice,
// and that should not rest on another package's nil handling.
func (r *Runner) flushUnderLoad() bool {
	t := r.currentThrottler()
	if t == nil {
		return false
	}
	return throttler.GradualOnly(t).IsThrottled()
}

// setThrottlerOnPhases hands the resolved throttler to every phase that paces
// itself against it. Both the copier and every finite checker accept one.
//
// Both phases get the same composite, but they do not react to the same parts of
// it: the copier writes and so honours every signal in it, while the checksum
// narrows it to the load signals (see checksum's loadOnlyThrottler — a read-only
// snapshot pass cannot cause replica lag, so pausing it on lag would only hold
// the snapshot open for longer). Progress().Throttle mirrors that split — see
// runstatus.Snapshot.ThrottleStatus.
func (r *Runner) setThrottlerOnPhases() {
	t := r.currentThrottler()
	r.copier.SetThrottler(t)
	r.checker.SetThrottler(t)
}

// setupAutoscaling builds the target's Aurora throttlers and sizes autoscaling
// from the same probe (concurrency.Engage). The throttlers are installed later,
// by setupThrottler, once the copier and checker exist to receive them.
//
// The two Aurora throttlers have independent gates: setting MaxCommitLatency=0
// disables only commit-latency; the Aurora threads throttler is always enabled
// when Aurora is detected (Build picks the redo-aware perf_schema signal or the
// Threads_running fallback via a privilege probe). Build returns a zero result
// on non-Aurora targets, so this is safe to call unconditionally.
//
// OpenMonitor is invoked lazily by Build only after IsAurora returns true, so
// non-Aurora users never pay the connect cost. MaxOpenConnections=2 lets both
// Aurora throttlers poll concurrently without serializing on a single conn,
// with a touch of headroom.
//
// A test throttler replaces the Aurora throttlers entirely, so there is no
// probe and autoscaling cannot engage.
func (r *Runner) setupAutoscaling(ctx context.Context) error {
	if r.migration.testThrottler == nil {
		result, err := r.buildAurora(ctx, throttler.AuroraSetup{
			Source: r.db,
			OpenMonitor: func() (*sql.DB, error) {
				monitorCfg := *r.dbConfig // shallow copy — MaxOpenConnections is value-typed
				monitorCfg.MaxOpenConnections = 2
				return dbconn.NewWithConnectionType(r.dsn(), &monitorCfg, "monitor database")
			},
			CommitLatencyThreshold: r.migration.MaxCommitLatency,
			Logger:                 r.logger,
		})
		if err != nil {
			return err
		}
		r.aurora = result
		r.monitorDB = result.MonitorDB
	}
	plan, err := concurrency.Engage(ctx, &r.migration.Common, concurrency.Request{
		Targets: []concurrency.Target{{DB: r.db, Aurora: r.aurora}},
		VCPUs:   r.auroraVCPUs,
		Logger:  r.logger,
	})
	if err != nil {
		return err
	}
	r.autoscale = plan
	return nil
}

// setupThrottler sets up the throttlers used to pace the copier and the
// checksum:
//   - one replication throttler per --replica-dsn (slowest wins)
//   - a commit-latency throttler if the source is detected as Aurora and
//     --max-commit-latency is positive (issue #468)
//   - an Aurora threads throttler whenever the source is detected as Aurora —
//     the redo-aware perf_schema signal when the user can read the perf-schema
//     tables it needs, else the Threads_running fallback (issue #831)
//
// Multiple replica DSNs can be specified as a comma-separated list.
// This is common logic shared between resume and new migration paths.
func (r *Runner) setupThrottler(ctx context.Context) error {
	if r.migration.testThrottler != nil {
		// We are in tests: use the test's throttler (a throttler.Mock).
		//
		// Deliberately wired to the copier only, not through
		// setThrottlerOnPhases. The mock exists to pace or stall the copy.
		// Handing it to the checksum as well would add a second per checksum
		// chunk to every test that paces with it — real wall-clock cost, no
		// extra coverage. Checksum throttling is covered directly in
		// pkg/checksum.
		r.setThrottler(r.migration.testThrottler)
		r.copier.SetThrottler(r.currentThrottler())
		return r.currentThrottler().Open(ctx)
	}

	var throttlers []throttler.Throttler

	if r.migration.ReplicaDSN != "" {
		replicaThrottlers, err := r.buildReplicaThrottlers()
		if err != nil {
			return err
		}
		throttlers = append(throttlers, replicaThrottlers...)
	}

	// The Aurora throttlers were built by setupAutoscaling (empty on a
	// non-Aurora target, or when a test throttler is set).
	throttlers = append(throttlers, r.aurora.Throttlers...)

	if len(throttlers) == 0 {
		return nil // use default Noop throttler
	}

	r.setThrottler(throttler.NewMultiThrottler(throttlers...))
	r.setThrottlerOnPhases()
	if err := r.currentThrottler().Open(ctx); err != nil {
		// multiThrottler already closes child throttlers on partial Open
		// failure, but the *sql.DB connections backing replica throttlers
		// are owned by r.replicas (and the Aurora monitor pool is owned
		// by r.monitorDB) — clean those up too rather than leaving them
		// dangling until Runner.Close() runs.
		if r.monitorDB != nil {
			_ = r.monitorDB.Close()
			r.monitorDB = nil
		}
		_ = r.closeReplicas()
		return fmt.Errorf("opening throttlers: %w", err)
	}
	return nil
}

// buildReplicaThrottlers opens the configured replica DSN(s) and returns a
// throttler per replica. Replica connections are tracked on the runner so
// they get closed alongside the main DB.
func (r *Runner) buildReplicaThrottlers() ([]throttler.Throttler, error) {
	dsns := dbconn.SplitDSNs(r.migration.ReplicaDSN)
	if len(dsns) == 0 {
		return nil, fmt.Errorf("--replica-dsn was specified but contains no valid DSNs: %q", r.migration.ReplicaDSN)
	}

	// Create a separate DB config for replica connections
	replicaDBConfig := dbconn.NewDBConfig()
	replicaDBConfig.LockWaitTimeout = r.dbConfig.LockWaitTimeout
	replicaDBConfig.InterpolateParams = r.dbConfig.InterpolateParams
	replicaDBConfig.MaxOpenConnections = r.dbConfig.MaxOpenConnections

	// Copy TLS settings from main DB config to replica config
	replicaDBConfig.TLSMode = r.dbConfig.TLSMode
	replicaDBConfig.TLSCertificatePath = r.dbConfig.TLSCertificatePath

	throttlers := make([]throttler.Throttler, 0, len(dsns))
	for _, dsn := range dsns {
		// Enhance replica DSN with TLS settings if not already present
		enhancedDSN, err := dbconn.EnhanceDSNWithTLS(dsn, replicaDBConfig)
		if err != nil {
			r.logger.Warn("could not enhance replica DSN with TLS settings",
				"dsn", dbconn.RedactDSN(dsn),
				"error", err,
			)
			enhancedDSN = dsn
		}

		replicaDB, err := dbconn.NewWithConnectionType(enhancedDSN, replicaDBConfig, "replica database")
		if err != nil {
			_ = r.closeReplicas()
			return nil, fmt.Errorf("failed to connect to replica database (DSN: %s): %w", dbconn.RedactDSN(dsn), err)
		}
		r.replicas = append(r.replicas, replicaDB)

		replicaThrottler, err := throttler.NewReplicationThrottler(replicaDB, r.migration.ReplicaMaxLag, r.logger)
		if err != nil {
			_ = r.closeReplicas()
			return nil, fmt.Errorf("could not create replication throttler (DSN: %s): %w", dbconn.RedactDSN(dsn), err)
		}
		throttlers = append(throttlers, replicaThrottler)
	}
	return throttlers, nil
}

// startBackgroundRoutines starts the background routines needed for migration monitoring.
// This includes table statistics updates and periodic binlog flushing.
func (r *Runner) startBackgroundRoutines(ctx context.Context) {
	// Start routines in table and replication packages to
	// Continuously update the min/max and estimated rows
	// and to flush the binary log position periodically.
	// These will both be stopped when the copier finishes
	// and checksum starts, although the PeriodicFlush
	// will be restarted again after.
	for _, change := range r.changes {
		go change.table.AutoUpdateStatistics(ctx, tableStatUpdateInterval, r.logger)
	}
	r.replClient.StartPeriodicFlush(ctx, change.DefaultFlushInterval)
	// Start go routines for checkpointing and dumping status. The returned
	// wait function is invoked from Close() so we can be sure no late
	// checkpoint INSERT lands after teardown begins.
	r.watchTaskWait = status.WatchTask(ctx, r, r.logger)
}

// setup performs all the initial steps to prepare for the migration,
// including:
// - creating copier chunker
// - opening the chunker (from a checkpoint if possible)
// - creating the new tables + performing ALTER TABLE on them.
// - creating the copier + replClient
// - starting the replication feed
// - starting the table statistics auto-update routines
// - starting the periodic flush routine
func (r *Runner) setup(ctx context.Context) error {
	var err error

	// Probe the target and settle the thread counts before either path builds
	// the copier, so both the fresh and the resumed migration size their pools
	// from the same probe the throttlers are built from.
	if err := r.setupAutoscaling(ctx); err != nil {
		return err
	}

	// We always attempt to resume from a checkpoint.
	if err = r.resumeFromCheckpoint(ctx); err != nil {
		if !resumeErrorIsDefinitive(err) {
			// The error does not prove "there is nothing to resume from" — it
			// only proves a step of the resume failed (e.g. a transient
			// connection error on the probe or checkpoint read). Falling
			// through to a fresh migration here would DROP the partially
			// populated _new table and the checkpoint, silently destroying
			// possibly days of copy progress over a blip. Fail instead: all
			// state is preserved, and re-running spirit retries the resume.
			return fmt.Errorf("resuming from checkpoint failed with a possibly-transient error; "+
				"refusing to start a fresh migration because that would discard resumable copy progress. "+
				"Re-run spirit to retry the resume, or drop the checkpoint table %q to force a fresh start: %w",
				r.checkpointTableName(), err)
		}
		// Resume is definitively not possible: no checkpoint exists (first
		// run, or it was invalidated), or it can't be used — a mismatched
		// alter, expired binlog, too-old checkpoint, truncation collision, or
		// unreadable content. Spirit logs the reason and falls back to a
		// fresh migration so it always makes forward progress.
		r.logger.Info("could not resume from checkpoint",
			"reason", err,
		) // explain why it failed.

		if err := r.newMigration(ctx); err != nil {
			return err
		}
	}

	// Setup replication throttler (common logic for both paths)
	if err := r.setupThrottler(ctx); err != nil {
		return err
	}

	// We can enable the key above watermark optimization
	if err := r.replClient.SetWatermarkOptimization(ctx, true); err != nil {
		return err
	}

	// Start background monitoring routines (common logic for both paths)
	r.startBackgroundRoutines(ctx)

	return nil
}

// fatalError is the callback provided to the replication client.
// It is called when a DDL change is detected on a subscribed table
// (change.FatalReasonSchemaChange), when a fatal stream error occurs
// (change.FatalReasonStreamError), or when the periodic flush fails to apply
// changes (change.FatalReasonFlushError). The replication client is
// responsible for any logging related to these errors.
// It returns true if the error was acted upon (migration cancelled),
// or false if it was ignored (e.g. because the migration is already
// past cutover, where Spirit's own RENAME TABLE DDL is expected).
//
// The reason decides what happens to the checkpoint: a schema change
// invalidates it (resuming against a changed table definition could corrupt
// data), while a stream error preserves it — a dead binlog stream is exactly
// the failure checkpoint resume exists to recover from, so a re-run picks up
// the copy and replays the binlog from the checkpointed position.
//
// fatalError is safe to call concurrently. fatalOnce makes the
// invalidate-and-cancel side effects idempotent and prevents racing
// with Close() teardown of r.db / r.checkpointTable / r.cancelFunc.
func (r *Runner) fatalError(reason change.FatalReason) bool {
	if r.status.Get() >= status.CutOver {
		return false
	}
	r.fatalOnce.Do(func() {
		r.status.Set(status.ErrCleanup)
		if advice := reason.Advice("migration"); advice != "" {
			r.logger.Error(advice)
		}
		// Unless the reason leaves the checkpoint resumable, invalidate it, so
		// we don't try to resume: the migration would otherwise be blocked from
		// proceeding permanently, and letting it start again is the better
		// choice. Use a background context since the migration context may
		// already be cancelled. checkpointTable can still be nil if fatalError
		// fires during early setup, before createCheckpointTable runs — skip
		// the drop in that case.
		if !reason.PreservesCheckpoint() && r.checkpointTable != nil && r.db != nil {
			if err := r.checkpointTbl().Drop(context.Background()); err != nil {
				r.logger.Error("could not remove checkpoint",
					"error", err,
				)
			}
		}
		r.cancel(status.FatalAbort(fmt.Errorf("migration aborted: fatal change feed condition (%s); see the preceding log lines for details", reason)))
	})
	return true
}

func (r *Runner) recordWorkflowError(err error) {
	if errors.Is(err, status.ErrDurableMutation) {
		r.durableMutation.Store(true)
	}
	if errors.Is(err, status.ErrOwnershipAmbiguous) {
		r.terminalOwnership.Store(uint32(status.WorkflowTerminalOwnershipAmbiguous))
	}
}

// Result returns correctness evidence retained from the most recent Run
// invocation. It is intentionally separate from phase metrics.
func (r *Runner) Result() status.WorkflowResult {
	return status.WorkflowResult{
		DurableMutation:   r.durableMutation.Load(),
		TerminalOwnership: status.WorkflowTerminalOwnership(r.terminalOwnership.Load()),
	}
}

// copyTables snapshots the copy chunker and returns the per-table progress.
// Progress and Status both derive their copy figures from it, so the API and
// the log block report one measure: settled rows against the tables'
// cardinality estimates, kept past the end of the copy. The copier's own
// progress is not used for either, because on an auto_increment key it
// measures keyspace distance, not rows. The chunker is read under chunkerMu
// to synchronize with initChunkers(), which may be assigning it concurrently
// during setup.
func (r *Runner) copyTables() []status.TableProgress {
	r.chunkerMu.RLock()
	copyChunker := r.copyChunker
	r.chunkerMu.RUnlock()
	return status.TablesFromChunker(copyChunker)
}

// snapshot captures what Status and Progress report on, for the given state.
func (r *Runner) snapshot(state status.State) *runstatus.Snapshot {
	return &runstatus.Snapshot{
		Noun:       "migration",
		State:      state,
		Tracker:    &r.status,
		Tables:     r.copyTables(),
		Checkpoint: &r.lastCheckpoint,
		Resumed:    r.usedResumeFromCheckpoint.Load(),
		Source:     statusSource{r},
	}
}

// statusSource hands runstatus the runner's subsystems. It reads each one only
// when the state being reported on needs it, which is after setup has assigned
// it (see runstatus.Source).
type statusSource struct{ r *Runner }

func (s statusSource) Copier() copier.Copier          { return s.r.copier }
func (s statusSource) Applier() applier.Applier       { return s.r.applier }
func (s statusSource) Checker() checksum.Checker      { return s.r.checker }
func (s statusSource) Feeds() []change.Source         { return []change.Source{s.r.replClient} }
func (s statusSource) Throttler() throttler.Throttler { return s.r.currentThrottler() }
func (s statusSource) SentinelSchema() string         { return s.r.changes[0].table.SchemaName }

func (r *Runner) Progress() status.Progress {
	return r.snapshot(r.status.Get()).Progress()
}

func (r *Runner) Close() error {
	r.status.Set(status.Close)
	// Cancel the migration context so background goroutines started in
	// startBackgroundRoutines (notably the status.WatchTask checkpoint
	// dumper) observe ctx.Done() and exit. This is normally already done
	// by Run's deferred cancel, but Close() may be called via paths that
	// don't run that defer; calling it here is idempotent and cheap.
	r.Cancel()
	// Wait for the status/checkpoint dumper goroutines to exit *before*
	// tearing down the database connection, so a late DumpCheckpoint INSERT
	// cannot land in the checkpoint table after the caller assumes Close()
	// has fully quiesced the runner.
	if r.watchTaskWait != nil {
		r.watchTaskWait()
	}
	// Run every cleanup step unconditionally and collect errors with
	// errors.Join. Previously the first failing step short-circuited the
	// rest, leaking the repl client's binlog reader goroutine, the
	// throttler, replica DB handles, and finally the primary DB pool. The
	// individual close calls are independent enough that running them all
	// out of order does no harm.
	var errs []error
	for _, change := range r.changes {
		if err := change.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if r.replClient != nil {
		r.replClient.Close()
	}
	// Stop the applier's async write workers. After a completed copy this is
	// a no-op (the copier's Run already stopped them; Stop is idempotent),
	// but paths that never reach or never finish the copy — early failures,
	// and tests stepping the copy incrementally via CopyChunk — would
	// otherwise leak the worker goroutines. The replication client's
	// synchronous applier methods are unaffected by Stop, and it has already
	// been closed above.
	if r.applier != nil {
		if err := r.applier.Stop(); err != nil {
			errs = append(errs, err)
		}
	}
	if t := r.currentThrottler(); t != nil {
		if err := t.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	// Close the Aurora monitor pool after the throttler so its background
	// pollers observe Close() / ctx cancellation before we yank the pool
	// out from under them. No-op when not Aurora.
	if r.monitorDB != nil {
		if err := r.monitorDB.Close(); err != nil {
			errs = append(errs, err)
		}
		r.monitorDB = nil
	}
	if err := r.closeReplicas(); err != nil {
		errs = append(errs, err)
	}
	if r.db != nil {
		if err := r.db.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (r *Runner) resumeFromCheckpoint(ctx context.Context) error {
	// Check that the new table(s) exists and are readable.
	for _, change := range r.changes {
		newName := utils.NewTableName(change.table.TableName)
		if err := dbconn.Exec(ctx, r.db, "SELECT 1 FROM %n.%n LIMIT 1", change.stmt.Schema, newName); err != nil {
			// Wrap the underlying error: resumeErrorIsDefinitive relies on it
			// to tell "the table does not exist" (ER_NO_SUCH_TABLE — start
			// fresh) apart from a transient probe failure (fail the run).
			return fmt.Errorf("could not read new table '%s' to resume from checkpoint: %w", newName, err)
		}
	}

	// Read the latest checkpoint row. The table has a single owner — single-table
	// migrations get a per-table table, and the shared multi-table
	// _spirit_checkpoint is guarded by the schema lock — so this is simply "the
	// newest row, if any"; the statement match below confirms it is ours.
	rec, err := r.checkpointTbl().ReadLatest(ctx)
	if err != nil {
		// Distinguish "no checkpoint to resume from" — a normal state — from a
		// real read failure (permission denied, server gone, or an incompatible
		// schema missing a column) so an operator doesn't mistake the absence of
		// a checkpoint for a permission issue. Both paths wrap the underlying
		// error: resumeErrorIsDefinitive classifies it in setup().
		if errors.Is(err, checkpoint.ErrNotFound) {
			return fmt.Errorf("checkpoint table '%s' has no checkpoint, nothing to resume from: %w", r.checkpointTableName(), err)
		}
		return fmt.Errorf("could not read from table '%s', err:%w", r.checkpointTableName(), err)
	}

	// Validate that the statement matches between the checkpoint and the
	// migration we are running — this catches a changed alter, and (for the
	// shared multi-table table) a stale checkpoint left by a different
	// multi-table migration that previously ran in this schema.
	if r.migration.Statement != rec.Statement {
		return status.ErrMismatchedAlter
	}

	// In single-table mode the checkpoint table name is built by deterministic
	// truncation, so two long table names that share a prefix can collide.
	// Cross-check the stored original table name to guard against resuming
	// from another table's checkpoint.
	if len(r.changes) == 1 && rec.OriginalTableName != "" && rec.OriginalTableName != r.changes[0].table.TableName {
		return fmt.Errorf("%w: stored=%q expected=%q", status.ErrCheckpointCollision, rec.OriginalTableName, r.changes[0].table.TableName)
	}

	// Check if the checkpoint is too old to safely resume.
	// Replaying many days of binary logs can be slower than starting fresh.
	if age := rec.Age(); age >= r.migration.CheckpointMaxAge {
		return fmt.Errorf("%w: checkpoint is %s old (max allowed: %s)",
			status.ErrCheckpointTooOld,
			age.Round(time.Second),
			r.migration.CheckpointMaxAge,
		)
	}

	copierWatermark := rec.CopierWatermark
	checksumWatermark := rec.ChecksumWatermark
	// The source position: a binlog file:offset coordinate or a GTID set,
	// depending on which change source the checkpointing run was using. Its
	// encoding decides which client the resume constructs (NewAutoClient).
	resumePosition := rec.Position

	// Initialize and call SetInfo on all the new tables, since we need the column info
	for _, change := range r.changes {
		// Initialize newTable with the expected new table name
		newName := utils.NewTableName(change.table.TableName)
		change.newTable = table.NewTableInfo(r.db, change.stmt.Schema, newName)
		if err := change.newTable.SetInfo(ctx); err != nil {
			return err
		}
	}

	// Initialize the chunker now that we have the new table info
	if err := r.initChunkers(); err != nil {
		return err
	}

	// Open chunker at the specified watermark
	if err := r.copyChunker.OpenAtWatermark(copierWatermark); err != nil {
		return err
	}

	// With saved evidence, the factory opens the chunker according to the
	// selected verification policy. Otherwise start at the beginning.
	if checksumWatermark == "" {
		if err = r.checksumChunker.Open(); err != nil {
			return err
		}
	}

	// Setup is the same shape as the fresh-start path; we do it here so
	// the replClient and its subscriptions exist before we hand them the
	// checkpointed position via StartFromPosition. Passing the position keeps
	// the resume in the coordinate scheme the checkpoint was written in; a
	// GTID checkpoint on a server that no longer has GTIDs enabled errors
	// here (not definitive, so the run fails with state preserved rather
	// than silently restarting).
	if err := r.setupCopierCheckerAndReplClient(ctx, resumePosition, checksumWatermark); err != nil {
		return err
	}

	// Open the change source at the checkpointed position. StartFromPosition
	// validates the position is still resumable (e.g. binlog file purged on
	// MySQL) and starts streaming. If the source can no longer reach the
	// position, surface it as status.ErrBinlogNotFound — a definitive
	// "cannot resume", so setup() falls back to a fresh migration; any other
	// error propagates as-is and fails the run (state preserved), because it
	// may be transient.
	if err := r.replClient.StartFromPosition(ctx, resumePosition); err != nil {
		r.logger.Warn("resuming from checkpoint failed because resuming from the previous source position failed",
			"position", resumePosition,
		)
		if errors.Is(err, change.ErrPositionNotFound) {
			return fmt.Errorf("%w: %w", status.ErrBinlogNotFound, err)
		}
		return err
	}
	r.logger.Warn("resuming from checkpoint",
		"copier-watermark", copierWatermark,
		"checksum-watermark", checksumWatermark,
		"position", resumePosition,
	)
	// The baseline is taken only here, past every step that can still send
	// setup down the fresh-copy path: the fresh chunker starts at zero, and a
	// baseline left over from an abandoned resume would underflow the
	// unsigned subtraction in recordCopyCompleted.
	r.copyRowsAtResume = r.copyChunker.RowsCopied()
	r.usedResumeFromCheckpoint.Store(true)
	return nil
}

// resumeErrorIsDefinitive reports whether an error returned by
// resumeFromCheckpoint definitively means "there is no usable checkpoint to
// resume from", making it safe for setup() to fall back to a fresh migration —
// whose first act is to DROP the _new and checkpoint tables. Anything not
// recognized here (connection failures, timeouts, unknown errors) is treated
// as possibly transient: setup() then fails the run and preserves all state,
// because guessing "start fresh" on a blip silently destroys the copy
// progress a multi-day migration has accumulated.
func resumeErrorIsDefinitive(err error) bool {
	// Sentinel classifications produced along the resume path.
	for _, definitive := range []error{
		checkpoint.ErrNotFound,        // checkpoint table exists but holds no row
		status.ErrMismatchedAlter,     // checkpoint belongs to a different statement
		status.ErrCheckpointCollision, // checkpoint belongs to a different table
		status.ErrCheckpointTooOld,    // replaying would be slower than restarting
		status.ErrBinlogNotFound,      // position purged from (or unparseable by) the source
	} {
		if errors.Is(err, definitive) {
			return true
		}
	}
	// ER_NO_SUCH_TABLE on the _new probe (nothing was ever copied — the
	// normal first-run path) or on the checkpoint read (a previous run
	// invalidated it), or ER_BAD_FIELD_ERROR (a checkpoint table layout this
	// version cannot read).
	if checkpoint.IsIncompatible(err) {
		return true
	}
	// Unusable checkpoint *content*: watermarks are JSON documents and
	// created_at must parse as a DATETIME. A decode failure means the stored
	// state can never be resumed from — retrying will not help — so a fresh
	// start is the only way forward.
	if _, ok := errors.AsType[*json.SyntaxError](err); ok {
		return true
	}
	if _, ok := errors.AsType[*json.UnmarshalTypeError](err); ok {
		return true
	}
	if _, ok := errors.AsType[*time.ParseError](err); ok {
		return true
	}
	return false
}

// initChunkers sets up the chunker(s) for the migration.
// It does not open them yet, and we need to either
// call Open() or OpenAtWatermark() later.
func (r *Runner) initChunkers() error {
	copyChunkers := make([]table.Chunker, 0, len(r.changes))
	checksumChunkers := make([]table.Chunker, 0, len(r.changes))
	for _, change := range r.changes {
		columnRenames := change.stmt.ColumnRenameMap()
		if len(columnRenames) > 0 {
			r.logger.Info("column renames detected",
				"table", change.table.TableName,
				"renames", columnRenames,
			)
		}
		columnMapping := table.NewColumnMapping(change.table, change.newTable, columnRenames)
		// TargetChunkTime is left unset: the time signal is a constant
		// (table.ChunkerDefaultTarget), not a per-run knob.
		chunkerCfg := table.ChunkerConfig{
			NewTable:      change.newTable,
			Logger:        r.logger,
			ColumnMapping: columnMapping,
		}
		// The copier sizes chunks by an in-memory byte budget rather than
		// copy time — it reads rows into client memory, and its time signal
		// collapses under backpressure. This applies to the copy chunker
		// only: the checksum runs server-side CRC and keeps the time signal.
		copyChunkerCfg := chunkerCfg
		copyChunkerCfg.TargetChunkBytes = r.migration.TargetChunkSize
		var err error
		change.chunker, err = table.NewChunker(change.table, copyChunkerCfg)
		if err != nil {
			return err
		}
		checksumChunker, err := table.NewChunker(change.table, chunkerCfg)
		if err != nil {
			return err
		}
		copyChunkers = append(copyChunkers, change.chunker)
		checksumChunkers = append(checksumChunkers, checksumChunker)
	}
	// We can wrap it the multi-chunker regardless.
	// It won't cause any harm.
	r.chunkerMu.Lock()
	r.copyChunker = table.NewMultiChunker(copyChunkers...)
	r.checksumChunker = table.NewMultiChunker(checksumChunkers...)
	r.chunkerMu.Unlock()
	return nil
}

// checksum runs the selected verification gate before the final binlog drain.
func (r *Runner) checksum(ctx context.Context) error {
	if err := r.status.DoContext(ctx, status.Checksum, func() error {
		// Run the checksum with internal retry logic.
		//
		// We do not invalidate the checkpoint on a checksum error. The dumper
		// already refuses to persist a checksum_watermark for any pass that
		// had to repair a chunk (see DumpCheckpoint), so on resume — whether
		// the failure here was retry exhaustion, operator cancellation, or
		// anything else — the persisted row either carries an empty watermark
		// (forcing full re-verification) or a watermark from a clean pass
		// (safe to resume from). Either way the silent-cutover hole is
		// closed without needing to special-case the error path.
		return r.checker.Run(ctx)
	}); err != nil {
		// A statement adding a UNIQUE index over data that is not unique is the
		// common cause of a checksum that keeps finding differences: the copier
		// drops the duplicate rows rather than refusing them, so the new table
		// is simply short. Say so, but keep the checker's error in the chain —
		// it carries whether the attempts kept finding differences (this cause,
		// and reproducible) or merely errored (nothing proven, and worth
		// another attempt), which a caller deciding whether to retry needs.
		if r.addsUniqueIndex() {
			return fmt.Errorf("checksum failed after several attempts. This is likely related to your statement adding a UNIQUE index on non-unique data: %w", err)
		}
		return fmt.Errorf("checksum failed: %w", err)
	}

	// A long checksum extends the binlog deltas
	// So if we've called this optional checksum, we need one more state
	// of applying the binlog deltas.
	return r.status.DoContext(ctx, status.PostChecksum, func() error {
		return r.replClient.Flush(ctx)
	})
}

func (r *Runner) addsUniqueIndex() bool {
	for _, change := range r.changes {
		if err := change.stmt.AlterContainsAddUnique(); err != nil {
			return true
		}
	}
	return false
}

// DumpCheckpoint is called approximately every minute.
// It writes the current state of the migration to the checkpoint table,
// which can be used in recovery. Previously resuming from checkpoint
// would always restart at the copier, but it can now also resume at
// the checksum phase.
func (r *Runner) DumpCheckpoint(ctx context.Context) error {
	// Serialize condition evaluation and persistence with watermark invalidation.
	r.checkpointMu.Lock()
	defer r.checkpointMu.Unlock()
	// Check if replication client and copier are initialized (nil if called before setup completes).
	// We hold chunkerMu to synchronize with initChunkers(), which
	// may be assigning r.copyChunker concurrently during setup.
	r.chunkerMu.RLock()
	copyChunker := r.copyChunker
	r.chunkerMu.RUnlock()
	if r.replClient == nil || copyChunker == nil {
		return status.ErrWatermarkNotReady
	}
	// Retrieve the safe-flushed position first.
	binlogPosition := r.replClient.Position()
	copierWatermark, err := copyChunker.GetLowWatermark()
	if err != nil {
		return status.ErrWatermarkNotReady // it might not be ready, we can try again.
	}
	// The checker owns verification evidence: traversal alone may include
	// unresolved retries or repairs. Sentinel waiting discards checksum evidence
	// before the background checker starts, including the gap before its call.
	var checksumWatermark string
	if state := r.status.Get(); state >= status.Checksum && state < status.WaitingOnSentinelTable && r.checker != nil {
		wm, wmErr := r.checker.ResumeWatermark()
		if wmErr != nil {
			return status.ErrWatermarkNotReady
		}
		checksumWatermark = wm
	}

	// Debug, not Info: the status block's ckpt row reports it instead, so
	// this no longer needs a line of its own on every dump (#329). The
	// watermark detail is still one -v away when a resume needs debugging.
	//
	// Note: when we dump the lowWatermark to the log, we are exposing the PK values,
	// when using the composite chunker are based on actual user-data.
	// We believe this is OK but may change it in the future. Please do not
	// add any other fields to this log line.
	r.logger.Debug("checkpoint",
		"low-watermark", copierWatermark,
		"position", binlogPosition,
	)
	originalTableName := ""
	if len(r.changes) == 1 {
		originalTableName = r.changes[0].table.TableName
	}
	if err := r.checkpointTbl().Write(ctx, checkpoint.Record{
		CopierWatermark:   copierWatermark,
		ChecksumWatermark: checksumWatermark,
		Position:          binlogPosition,
		Statement:         r.migration.Statement,
		OriginalTableName: originalTableName,
	}); err != nil {
		// Keep the cause: the WatchTask dumper distinguishes a benign
		// canceled-mid-write (it is being stopped) from a genuinely broken
		// checkpoint table, which is fatal.
		return fmt.Errorf("%w: %w", status.ErrCouldNotWriteCheckpoint, err)
	}
	r.lastCheckpoint.Record(binlogPosition)
	return nil
}

// Status returns the periodic report on the whole migration: a header line
// plus one indented row per subsystem (see status.Block). It deliberately
// absorbs what used to be separate periodic lines from the change feed
// (flushes, rotations) and the checkpoint dumper, which each ran on their own
// interval — see github.com/block/spirit/issues/329.
func (r *Runner) Status() string {
	return r.snapshot(r.status.Get()).Status()
}

// invalidateChecksumWatermark serializes with periodic dumps to clear previously
// persisted evidence when background verification has invalidated it.
func (r *Runner) invalidateChecksumWatermark(ctx context.Context) error {
	r.checkpointMu.Lock()
	defer r.checkpointMu.Unlock()
	return dbconn.Exec(ctx, r.db, "UPDATE %n.%n SET checksum_watermark = %? WHERE statement = %?",
		r.checkpointTable.SchemaName,
		r.checkpointTable.TableName,
		"",
		r.migration.Statement,
	)
}

// Cancel stops a running migration. It is an operator cancellation: Run
// returns context.Canceled.
func (r *Runner) Cancel() {
	r.cancel(nil)
}

// Abort stops a running migration with cause (see status.Aborter). The
// checkpoint dumper calls it when it cannot write a checkpoint, so Run returns
// the write error instead of context.Canceled.
func (r *Runner) Abort(cause error) {
	r.cancel(cause)
}

// cancel cancels the migration context with cause. A nil cause is a plain
// cancellation (context.Canceled).
func (r *Runner) cancel(cause error) {
	r.cancelMu.Lock()
	cancel := r.cancelFunc
	r.cancelMu.Unlock()
	if cancel != nil {
		cancel(cause)
	}
}
