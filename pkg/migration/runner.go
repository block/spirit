package migration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/buildinfo"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/checkpoint"
	"github.com/block/spirit/pkg/checksum"
	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/lint"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/migration/check"
	"github.com/block/spirit/pkg/sentinel"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/throttler"
	"github.com/block/spirit/pkg/utils"
)

// These are really consts, but set to var for testing.
var (
	tableStatUpdateInterval = 5 * time.Minute
	checkpointTableName     = "_spirit_checkpoint" // const for multi-migration checkpoints.
	// Sentinel-wait timing lives in pkg/sentinel (sentinel.WaitLimit /
	// sentinel.CheckInterval / sentinel.TableName) and continuous-checksum
	// pacing in pkg/checksum (checksum.ContinuousMinPassInterval /
	// checksum.DefaultContinuousRetryDelay), so they are shared with move/sync.
)

// continuousDivergenceReporter is the minimal view of the sentinel-wait
// continuous checker that the checkpoint machinery needs: "has this checker
// observed any divergence?". Both the production *checksum.ContinuousChecker
// and the test mockChecker satisfy it. Keeping the field this narrow lets the
// continuous checksum use checksum.ContinuousChecker (which is intentionally
// not a checksum.Checker) without changing DumpCheckpoint /
// invalidateChecksumWatermark, which only consult DifferencesFound().
type continuousDivergenceReporter interface {
	DifferencesFound() uint64
}

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

	// Changes enccapsulates all changes
	// With a stmt, alter, table, newTable.
	changes []*tableChange

	status     status.State  // must use atomic helpers to change.
	replClient change.Source // feed contains all binlog subscription activity.
	throttler  throttler.Throttler

	copier       copier.Copier
	copyChunker  table.Chunker // the chunker for copying
	copyDuration time.Duration // how long the copy took

	// applier is the shared write layer used by both the copier (buffered
	// copy) and the replication client (binlog deltas). Kept on the runner
	// so Status() can report its pipeline snapshot.
	applier applier.Applier

	checker         checksum.Checker
	checksumChunker table.Chunker // the chunker for checksum

	chunkerMu sync.RWMutex // protects copyChunker and checksumChunker from concurrent access

	// continuousChecker is the sentinel-wait re-verification checker built
	// by runContinuousChecksum. It is deliberately separate from r.checker
	// (fresh chunker, not wired into resume), but DumpCheckpoint must
	// consult it: once it has repaired any chunk, the initial checksum's
	// watermark no longer proves the table clean, so persisting it would
	// let a resumed run skip re-verifying the repaired range. Written once
	// by the continuous-checksum goroutine and read by the checkpoint
	// dumper goroutine — both under checkpointMu.
	continuousChecker continuousDivergenceReporter

	// checkpointMu serializes checkpoint persistence (DumpCheckpoint's
	// watermark-condition evaluation + INSERT) against the sentinel-abort
	// path that blanks the persisted checksum_watermark
	// (invalidateChecksumWatermark). Without it, a periodic dump that
	// evaluated its conditions just before the continuous checker recorded
	// a difference could INSERT a stale-watermark row *after* the abort
	// path's UPDATE, resurrecting the watermark on the latest row — the
	// row resume reads. It also guards continuousChecker (see above).
	checkpointMu sync.Mutex

	// Track some key statistics.
	startTime             time.Time
	sentinelWaitStartTime time.Time

	// Used by the test-suite and some post-migration output.
	// Indicates if certain optimizations applied.
	usedInstantDDL           bool
	usedInplaceDDL           bool
	usedResumeFromCheckpoint bool

	// Attached logger
	logger     *slog.Logger
	cancelFunc context.CancelFunc

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
}

var _ status.Task = (*Runner)(nil)

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
	}
	for _, change := range changes {
		change.runner = runner // link back.
	}
	return runner, nil
}

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
	return len(r.changes) + 2
}

func (r *Runner) SetMetricsSink(sink metrics.Sink) {
	r.metricsSink = sink
}

func (r *Runner) SetLogger(logger *slog.Logger) {
	r.logger = logger
}

// attemptMySQLDDL tries to perform the DDL using MySQL's built-in
// either with INSTANT or known safe INPLACE operations.
func (r *Runner) attemptMySQLDDL(ctx context.Context) error {
	if len(r.changes) > 1 {
		return errors.New("attemptMySQLDDL only supports single-table changes")
	}
	return r.changes[0].attemptMySQLDDL(ctx)
}

func (r *Runner) Run(ctx context.Context) error {
	ctx, r.cancelFunc = context.WithCancel(ctx)
	defer r.cancelFunc()
	r.startTime = time.Now()
	bi := buildinfo.Get()
	r.logger.Info("Starting spirit migration",
		"version", bi.Version,
		"commit", bi.Commit,
		"build-date", bi.Date,
		"go", bi.GoVer,
		"dirty", bi.Modified,
		"concurrency", r.migration.Threads,
		"target-chunk-size", r.migration.TargetChunkTime,
	)

	// Create a database connection
	// It will be closed in r.Close()
	var err error
	r.dbConfig = dbconn.NewDBConfig()
	if r.migration.LockWaitTimeout > 0 {
		r.dbConfig.LockWaitTimeout = int(r.migration.LockWaitTimeout.Seconds())
	}
	r.dbConfig.InterpolateParams = r.migration.InterpolateParams
	r.dbConfig.ForceKill = !r.migration.SkipForceKill
	// Map TLS configuration from migration to dbConfig
	r.dbConfig.TLSMode = r.migration.TLSMode
	r.dbConfig.TLSCertificatePath = r.migration.TLSCertificatePath
	// Size the connection pool the same way for both the buffered and
	// unbuffered paths:
	//
	//	pool = threads + write-threads + controlPlaneConns()
	//
	//	- threads             copier + checksum read concurrency
	//	- write-threads       replication-applier write concurrency
	//	- controlPlaneConns() headroom for the periodic control-plane queries
	//	                      that also run on the main pool (checkpoint,
	//	                      replication-flush poll, per-table stats), so they
	//	                      don't serialize behind a single spare connection
	//	                      once the copier + applier saturate the budget.
	//
	// WriteThreads may still be 0 here — that's the "auto-size on Aurora"
	// sentinel, which can only be resolved once we have a connection to probe
	// the server. So this seeds the pool with what's known now, and
	// setupCopierCheckerAndReplClient grows it to the final size after
	// resolving WriteThreads. The pool only ever grows (via SetMaxOpenConns);
	// later phases (checksum, cutover) ratchet it further but never shrink it.
	r.dbConfig.MaxOpenConnections = r.migration.Threads + r.migration.WriteThreads + r.controlPlaneConns()
	r.db, err = dbconn.New(r.dsn(), r.dbConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to main database (DSN: %s): %w", dbconn.RedactDSN(r.dsn()), err)
	}

	// Run linting if --lint or --lint-only is specified.
	// --lint-only implies lint.
	if r.migration.Lint || r.migration.LintOnly {
		if err := r.lint(ctx); err != nil {
			return err
		}
		if r.migration.LintOnly {
			r.logger.Info("--lint-only set; exiting after running linters")
			return nil
		}
	}

	if len(r.changes) == 1 {
		// We only allow non-ALTERs (i.e. CREATE TABLE, DROP TABLE, RENAME TABLE)
		// in single table mode.
		if !r.changes[0].stmt.IsAlterTable() {
			// The statement is the user's own SQL and must be executed
			// verbatim: it is not a sqlescape format string, and may contain
			// % characters in literals (e.g. COMMENT '100%new').
			err := dbconn.ExecRaw(ctx, r.db, r.changes[0].stmt.Statement)
			if err != nil {
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
		r.logger.Info("apply complete",
			"instant-ddl", r.usedInstantDDL,
			"inplace-ddl", r.usedInplaceDDL,
		)
		return nil // success!
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
	r.status.Set(status.CopyRows)
	if err := r.copier.Run(ctx); err != nil {
		return err
	}
	r.logger.Info("copy rows complete")
	r.copyDuration = time.Since(r.copier.StartTime())

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

	// Block on the sentinel table (if defer-cutover is in use). While we
	// wait, waitOnSentinelTable also runs a "continuous checksum" loop in
	// the background — see docs/migrate.md for the two-checksum model.
	// The initial checksum above is the correctness gate; the continuous
	// checksum opportunistically re-verifies data so that on sentinel drop
	// we have high confidence even if the wait lasted hours.
	//
	// This is invoked even if DeferCutOver is false because it's possible
	// that the sentinel table was created manually after the migration
	// started.
	if r.migration.RespectSentinel {
		r.sentinelWaitStartTime = time.Now()
		r.status.Set(status.WaitingOnSentinelTable)
		// Block on the sentinel via the shared sentinel.Wait (poll/timeout timing
		// lives in the sentinel package). The continuous-checksum lifecycle and
		// watermark invalidation are migration-specific — invalidateChecksumWatermark
		// scopes its UPDATE by statement because the checkpoint table is shared in
		// multi-table mode — so they are injected as callbacks. See pkg/sentinel.
		if err := sentinel.Wait(ctx, sentinel.WaitConfig{
			Exists:              func(ctx context.Context) (bool, error) { return sentinel.Exists(ctx, r.db) },
			RunChecksum:         r.runContinuousChecksum,
			InvalidateWatermark: r.invalidateChecksumWatermark,
			Logger:              r.logger,
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
	r.status.Set(status.CutOver)
	cutoverCfg := []*cutoverConfig{}
	for _, change := range r.changes {
		cutoverCfg = append(cutoverCfg, &cutoverConfig{
			table:          change.table,
			newTable:       change.newTable,
			oldTableName:   change.oldTableName(),
			useTestCutover: r.migration.useTestCutover, // indicates we want the test cutover
		})
	}
	cutover, err := NewCutOver(r.db, cutoverCfg, r.replClient, r.dbConfig, r.logger)
	if err != nil {
		return err
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
		"copy-rows-time", r.copyDuration.Round(time.Second).String(),
		"checksum-time", r.checker.ExecTime().Round(time.Second).String(),
		"total-time", time.Since(r.startTime).Round(time.Second).String(),
		"conns-in-use", r.db.Stats().InUse,
	)
	// cleanup all the tables
	for _, change := range r.changes {
		if err := change.cleanup(ctx); err != nil {
			return err
		}
	}
	// drop the checkpoint table
	if r.checkpointTable != nil {
		if err := r.checkpointTbl().Drop(ctx); err != nil {
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
	r.status.Set(status.ApplyChangeset)
	// Disable the periodic flush and flush all pending events.
	// We want it disabled for ANALYZE TABLE and acquiring a table lock
	// *but* it will be started again briefly inside of the checksum
	// runner to ensure that the lag does not grow too long.
	r.replClient.StopPeriodicFlush()
	if err := r.replClient.Flush(ctx); err != nil {
		return err
	}

	// Run ANALYZE TABLE to update the statistics on the new table.
	// This is required so on cutover plans don't go sideways, which
	// is at elevated risk because the batch loading can cause statistics
	// to be out of date.
	r.status.Set(status.AnalyzeTable)
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
			DB:              r.db,
			Replicas:        r.replicas,
			Table:           change.table,
			Statement:       change.stmt,
			TargetChunkTime: r.migration.TargetChunkTime,
			Threads:         r.migration.Threads,
			ReplicaMaxLag:   r.migration.ReplicaMaxLag,
			ForceKill:       !r.migration.SkipForceKill,
			// For the pre-run checks we don't have a DB connection yet.
			// Instead we check the credentials provided.
			Host:                 r.migration.Host,
			Username:             r.migration.Username,
			Password:             *r.migration.Password,
			TLSMode:              r.migration.TLSMode,
			TLSCertificatePath:   r.migration.TLSCertificatePath,
			SkipDropAfterCutover: r.migration.SkipDropAfterCutover,
			GTID:                 r.migration.EnableExperimentalGTID,
		}, r.logger, scope); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) lint(ctx context.Context) error {
	var createTables []*statement.CreateTable
	var alterTables []*statement.AbstractStatement
	config := lint.Config{
		Enabled:  make(map[string]bool),
		Settings: defaultLinterSettings,
	}

	if err := printLinters(config); err != nil {
		return err
	}

	for _, change := range r.changes {
		// Collect ALTER TABLE statements and the CREATE TABLEs for the tables they reference
		if change.stmt.IsAlterTable() {
			alterTables = append(alterTables, change.stmt)

			ct, err := r.getCreateTable(ctx, change.stmt.Schema, change.stmt.Table)
			if err != nil {
				return err
			}
			createTables = append(createTables, ct)
		}

		// If the migration creates a table, we need to collect that CREATE TABLE as well
		if change.stmt.IsCreateTable() {
			ct, err := change.stmt.ParseCreateTable()
			if err != nil {
				return err
			}
			createTables = append(createTables, ct)
		}
	}

	var errs []error

	violations, err := lint.RunLinters(createTables, alterTables, config)
	if err != nil {
		errs = append(errs, err)
	}

	for _, v := range violations {
		if v.Severity == lint.SeverityError {
			errs = append(errs, errors.New(v.String()))
		}
		fmt.Println(v)
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (r *Runner) getCreateTable(ctx context.Context, db string, tbl string) (*statement.CreateTable, error) {
	sql := fmt.Sprintf("show create table %s.%s", sqlescape.EscapeIdentifier(db), sqlescape.EscapeIdentifier(tbl))

	row := r.db.QueryRowContext(ctx, sql)
	var createTable string
	if err := row.Scan(&tbl, &createTable); err != nil {
		return nil, err
	}
	stmt, err := statement.ParseCreateTable(createTable)
	if err != nil {
		return nil, err
	}
	return stmt, nil
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

func (r *Runner) setupCopierCheckerAndReplClient(ctx context.Context) error {
	var err error

	// Resolve the number of apply (write) threads now that we have a
	// connection. WriteThreads==0 means "auto-size": on Aurora it becomes the
	// instance vCPU count; on non-Aurora there is no reliable vCPU signal to
	// size from, so it falls back to the default. Idempotent: a resolved
	// (non-zero) value passes through unchanged if this runs again.
	r.migration.WriteThreads, err = throttler.ResolveWriteThreads(ctx, r.db, r.migration.WriteThreads, r.logger)
	if err != nil {
		return err
	}
	// Autoscaling drives the buffered copier's applier worker pool; the legacy
	// unbuffered copier has no such pool, so the combination downgrades to a
	// fixed thread count with a warning rather than silently doing nothing.
	autoscale := r.migration.EnableExperimentalAutoscaling
	if autoscale && r.migration.Unbuffered {
		r.logger.Warn("--enable-experimental-autoscaling has no effect with --unbuffered; write threads stay fixed",
			"write_threads", r.migration.WriteThreads)
		autoscale = false
	}
	// redoAware tracks whether the Aurora threads throttler will run its
	// redo-aware perf_schema signal (which excludes redo-log waiters). It gates
	// the autoscaler's growth cap below: because that signal ignores redo-log
	// waiters it will not self-limit if extra write threads oversubscribe the
	// log, so growing above the start value needs the commit-latency throttler
	// as the backstop. AuroraSetup.Build makes the authoritative mode choice
	// (and logs it) when the throttler is built; this is an independent probe of
	// the same source in the same setup phase, used only to size maxWrite here.
	redoAware := false
	// On Aurora instances below MinAutoscaleVCPUs the utilization signal is too
	// coarse to control on — one thread is half or more of the dead band — so
	// the controller could only oscillate; run a fixed pool instead. Aurora is
	// the one place the autoscaler can engage at all (it needs the continuous
	// signal only the Aurora throttlers provide); an IsAurora probe failure is
	// benign here, matching AuroraSetup.Build, since without Aurora the
	// autoscaler stays dormant regardless and the copier logs its own downgrade.
	if autoscale {
		if isAurora, err := throttler.IsAurora(ctx, r.db); err == nil && isAurora {
			vCPUs, err := throttler.AuroraVCPUs(ctx, r.db)
			if err != nil {
				return err
			}
			if vCPUs < throttler.MinAutoscaleVCPUs {
				r.logger.Warn("autoscaling disabled: instance is too small for the utilization signal to guide scaling; write threads stay fixed",
					"vcpus", vCPUs, "min_vcpus", throttler.MinAutoscaleVCPUs,
					"write_threads", r.migration.WriteThreads)
				autoscale = false
			} else {
				redoAware = throttler.CanReadRedoAwareThreads(ctx, r.db) == nil
			}
		}
	}
	// Resolve the autoscaler's upper bound. When autoscaling is disabled this
	// equals WriteThreads (no movement). When enabled it is 2x the start value —
	// except in redo-aware mode with the commit-latency backstop disabled
	// (--max-commit-latency=0), where it stays at the start value so the
	// autoscaler can shed but not oversubscribe the redo log unguarded (see
	// ResolveMaxWriteThreads).
	commitLatencyEnabled := r.migration.MaxCommitLatency > 0
	maxWrite := throttler.ResolveMaxWriteThreads(r.migration.WriteThreads, autoscale, redoAware, commitLatencyEnabled)
	// Finalize the pool now that WriteThreads (and its autoscale ceiling) is
	// known: threads + maxWrite + controlPlaneConns() (see the MaxOpenConnections
	// doc in Run). Sizing for maxWrite ensures a scaled-up applier never starves
	// on connections. This is a no-op unless WriteThreads was auto-sized up from 0
	// or autoscaling raised the ceiling; the pool only ever grows.
	if poolSize := r.migration.Threads + maxWrite + r.controlPlaneConns(); poolSize > r.dbConfig.MaxOpenConnections {
		r.dbConfig.MaxOpenConnections = poolSize
		r.db.SetMaxOpenConns(poolSize)
	}

	r.checkpointTable = table.NewTableInfo(r.db, r.changes[0].table.SchemaName, r.checkpointTableName())

	// We always create an applier — the replication client requires one to
	// apply row images directly from the binlog (the buffered subscription
	// path). This is what sidesteps the MySQL binlog/visibility race that
	// caused silent row loss under load (issue #746): there is no SELECT
	// FROM original ... after the row event arrives, the row image *is* the
	// applied state.
	//
	// The same applier is handed to the copier, but the copier only uses it
	// for buffered copy (the default). Unbuffered copy (--unbuffered) issues
	// INSERT IGNORE INTO _new ... SELECT FROM original directly and ignores the
	// applier.
	appl, err := applier.NewSingleTargetApplier(
		applier.Target{DB: r.db},
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
	r.copier, err = copier.NewCopier(r.db, r.copyChunker, &copier.CopierConfig{
		Concurrency:     r.migration.Threads,
		TargetChunkTime: r.migration.TargetChunkTime,
		Throttler:       &throttler.Noop{},
		Logger:          r.logger,
		MetricsSink:     r.metricsSink,
		DBConfig:        r.dbConfig,
		Applier:         appl,
		Unbuffered:      r.migration.Unbuffered,
		Autoscale: copier.AutoscaleConfig{
			Enabled:      autoscale,
			StartThreads: r.migration.WriteThreads,
			MaxThreads:   maxWrite,
		},
	})
	if err != nil {
		return err
	}

	// Set the binlog position.
	// Create a binlog subscriber
	replConfig := change.NewClientDefaultConfig()
	replConfig.Logger = r.logger
	replConfig.CancelFunc = r.fatalError
	replConfig.DBConfig = r.dbConfig
	if r.migration.EnableExperimentalGTID {
		r.logger.Info("EXPERIMENTAL: using GTID-based change source")
		r.replClient = change.NewGTIDClient(r.db, r.migration.Host, r.migration.Username, *r.migration.Password, appl, replConfig)
	} else {
		r.replClient = change.NewBinlogClient(r.db, r.migration.Host, r.migration.Username, *r.migration.Password, appl, replConfig)
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

	r.checker, err = checksum.NewChecker([]*sql.DB{r.db}, r.checksumChunker, []change.Source{r.replClient}, &checksum.CheckerConfig{
		Concurrency:     r.migration.Threads,
		TargetChunkTime: r.migration.TargetChunkTime,
		DBConfig:        r.dbConfig,
		Logger:          r.logger,
		FixDifferences:  true,
		MaxRetries:      3,
		YieldTimeout:    r.migration.ChecksumYieldTimeout,
	})

	return err
}

// newMigration is called when resumeFromCheckpoint has failed.
// It performs all the initial steps to prepare for a fresh migration.
func (r *Runner) newMigration(ctx context.Context) error {
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
	if err := r.setupCopierCheckerAndReplClient(ctx); err != nil {
		return err
	}
	// Start the binary log feed now
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

// setupThrottler sets up the throttlers used to pace the copier:
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
	if r.migration.useTestThrottler {
		// We are in tests, add a throttler that always throttles.
		r.throttler = &throttler.Mock{}
		r.copier.SetThrottler(r.throttler)
		return r.throttler.Open(ctx)
	}

	var throttlers []throttler.Throttler

	if r.migration.ReplicaDSN != "" {
		replicaThrottlers, err := r.buildReplicaThrottlers()
		if err != nil {
			return err
		}
		throttlers = append(throttlers, replicaThrottlers...)
	}

	// Aurora throttlers — assembled by the shared throttler.AuroraSetup
	// helper so the move runner can use the same wiring. The two Aurora
	// throttlers have independent gates: setting MaxCommitLatency=0 disables
	// only commit-latency; the Aurora threads throttler is always enabled
	// when Aurora is detected (Build picks the redo-aware perf_schema signal or
	// the Threads_running fallback via a privilege probe). Build returns a zero
	// result on non-Aurora sources so this call is safe to make unconditionally.
	//
	// OpenMonitor is invoked lazily by the helper only after IsAurora
	// returns true (on Aurora the threads throttler is always built,
	// so a pool is always needed there), so non-Aurora users never pay the
	// connect cost. MaxOpenConnections=2 lets both Aurora throttlers poll
	// concurrently without serializing on a single conn, with a touch of
	// headroom.
	auroraRes, err := throttler.AuroraSetup{
		Source: r.db,
		OpenMonitor: func() (*sql.DB, error) {
			monitorCfg := *r.dbConfig // shallow copy — MaxOpenConnections is value-typed
			monitorCfg.MaxOpenConnections = 2
			return dbconn.NewWithConnectionType(r.dsn(), &monitorCfg, "monitor database")
		},
		CommitLatencyThreshold: r.migration.MaxCommitLatency,
		Logger:                 r.logger,
	}.Build(ctx)
	if err != nil {
		_ = r.closeReplicas()
		return err
	}
	if auroraRes.MonitorDB != nil {
		r.monitorDB = auroraRes.MonitorDB
	}
	throttlers = append(throttlers, auroraRes.Throttlers...)

	if len(throttlers) == 0 {
		return nil // use default Noop throttler
	}

	r.throttler = throttler.NewMultiThrottler(throttlers...)
	r.copier.SetThrottler(r.throttler)
	if err := r.throttler.Open(ctx); err != nil {
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
	replicaDBConfig.ForceKill = r.dbConfig.ForceKill
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
// (change.FatalReasonSchemaChange), or when a fatal stream error occurs
// (change.FatalReasonStreamError). The replication client is
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
		switch reason { //nolint: exhaustive // schema change intentionally handled by default: drop is the safe fallback for unknown reasons
		case change.FatalReasonStreamError:
			// The stream died but the subscribed tables are not known to have
			// changed, so the checkpoint remains valid. Keep it and tell the
			// operator how to recover.
			r.logger.Error("fatal replication stream error; the checkpoint has been preserved — re-run spirit to resume the migration from it")
		default:
			// Schema change — and, defensively, any future reason we don't
			// recognize (invalidating is the safe default: it costs a restart,
			// while wrongly resuming could corrupt data).
			// Invalidate the checkpoint, so we don't try to resume.
			// If we don't do this, the migration will permanently be blocked
			// from proceeding. Letting it start again is the better choice.
			// Use a background context since the migration context may
			// already be cancelled. checkpointTable can still be nil if
			// fatalError fires during early setup, before
			// createCheckpointTable runs — skip the drop in that case.
			if r.checkpointTable != nil && r.db != nil {
				if err := r.checkpointTbl().Drop(context.Background()); err != nil {
					r.logger.Error("could not remove checkpoint",
						"error", err,
					)
				}
			}
		}
		r.Cancel()
	})
	return true
}

func (r *Runner) Progress() status.Progress {
	var summary string
	var eta status.ETA
	var checksum status.ChecksumProgress
	switch r.status.Get() { //nolint: exhaustive
	case status.CopyRows:
		summary = fmt.Sprintf("%v %s ETA %v",
			r.copier.GetProgress(),
			r.status.Get().String(),
			r.copier.GetETA(),
		)
		eta = r.copier.GetETAState()
	case status.WaitingOnSentinelTable:
		summary = "Waiting on Sentinel Table"
	case status.ApplyChangeset, status.PostChecksum:
		summary = fmt.Sprintf("Applying Changeset Deltas=%v", r.replClient.GetDeltaLen())
	case status.Checksum:
		checksum = r.checker.GetProgress()
		summary = "Checksum Progress=" + checksum.String()
	}

	// Get per-table progress if available (multi-table migrations).
	// We hold chunkerMu to synchronize with initChunkers(), which
	// may be assigning r.copyChunker concurrently during setup.
	var tables []status.TableProgress
	r.chunkerMu.RLock()
	copyChunker := r.copyChunker
	r.chunkerMu.RUnlock()
	if mc, ok := copyChunker.(interface{ PerTableProgress() []table.TableProgress }); ok {
		for _, tp := range mc.PerTableProgress() {
			tables = append(tables, status.TableProgress{
				TableName:  tp.TableName,
				RowsCopied: tp.RowsCopied,
				RowsTotal:  tp.RowsTotal,
				IsComplete: tp.IsComplete,
			})
		}
	} else if copyChunker != nil {
		// Single table migration - get progress from chunker
		rowsCopied, _, rowsTotal := copyChunker.Progress()
		tableTables := copyChunker.Tables()
		tableName := ""
		if len(tableTables) > 0 {
			tableName = tableTables[0].TableName
		}
		tables = append(tables, status.TableProgress{
			TableName:  tableName,
			RowsCopied: rowsCopied,
			RowsTotal:  rowsTotal,
			IsComplete: copyChunker.IsRead(),
		})
	}
	return status.Progress{
		CurrentState: r.status.Get(),
		Summary:      summary,
		ETA:          eta,
		Checksum:     checksum,
		Tables:       tables,
	}
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
	if r.throttler != nil {
		if err := r.throttler.Close(); err != nil {
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
	binlogPosition := rec.Position

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

	if checksumWatermark != "" {
		if err := r.checksumChunker.OpenAtWatermark(checksumWatermark); err != nil {
			return err
		}
	} else {
		if err = r.checksumChunker.Open(); err != nil {
			return err
		}
	}

	// Setup is the same shape as the fresh-start path; we do it here so
	// the replClient and its subscriptions exist before we hand them the
	// checkpointed position via StartFromPosition.
	if err := r.setupCopierCheckerAndReplClient(ctx); err != nil {
		return err
	}

	// Open the change source at the checkpointed position. StartFromPosition
	// validates the position is still resumable (e.g. binlog file purged on
	// MySQL) and starts streaming. If the source can no longer reach the
	// position, surface it as status.ErrBinlogNotFound — a definitive
	// "cannot resume", so setup() falls back to a fresh migration; any other
	// error propagates as-is and fails the run (state preserved), because it
	// may be transient.
	if err := r.replClient.StartFromPosition(ctx, binlogPosition); err != nil {
		r.logger.Warn("resuming from checkpoint failed because resuming from the previous source position failed",
			"position", binlogPosition,
		)
		if errors.Is(err, change.ErrPositionNotFound) {
			return fmt.Errorf("%w: %w", status.ErrBinlogNotFound, err)
		}
		return err
	}
	r.logger.Warn("resuming from checkpoint",
		"copier-watermark", copierWatermark,
		"checksum-watermark", checksumWatermark,
		"position", binlogPosition,
	)
	r.usedResumeFromCheckpoint = true
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
		chunkerCfg := table.ChunkerConfig{
			NewTable:        change.newTable,
			TargetChunkTime: r.migration.TargetChunkTime,
			Logger:          r.logger,
			ColumnMapping:   columnMapping,
		}
		// The buffered copier (the default) sizes chunks by an in-memory byte
		// budget rather than copy time — the only path that reads rows into
		// client memory, and the one whose time signal collapses under
		// backpressure. This applies to the copy chunker only: the checksum
		// runs server-side CRC and keeps the time signal. The legacy
		// --unbuffered copier keeps the time signal (TargetChunkBytes == 0).
		copyChunkerCfg := chunkerCfg
		if !r.migration.Unbuffered {
			copyChunkerCfg.TargetChunkBytes = r.migration.TargetChunkSize
		}
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

// checksum creates the checksum which opens the read view
func (r *Runner) checksum(ctx context.Context) error {
	r.status.Set(status.Checksum)

	// The checksum keeps the pool threads open, so we need to extend
	// by more than +1 on threads as we did previously. We have:
	// - background flushing
	// - checkpoint thread
	// - checksum "replaceChunk" DB connections
	// Handle a case just in the tests not having a dbConfig.
	//
	// Not restored when checksum completes — by then we are past the copy
	// phase, so the +1 backpressure between copier and applier no longer
	// applies, and the only thing left is cutover, which itself wants at
	// least 5 connections. Pool size grows monotonically; see the
	// MaxOpenConnections doc in (*Runner).Run.
	r.db.SetMaxOpenConns(r.dbConfig.MaxOpenConnections + 2)

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
	if err := r.checker.Run(ctx); err != nil {
		if r.addsUniqueIndex() {
			// Overwrite the error if we think it's because of a unique index addition
			return errors.New("checksum failed after several attempts. This is likely related to your statement adding a UNIQUE index on non-unique data")
		}
		return fmt.Errorf("checksum failed: %w", err)
	}

	// A long checksum extends the binlog deltas
	// So if we've called this optional checksum, we need one more state
	// of applying the binlog deltas.
	r.status.Set(status.PostChecksum)
	return r.replClient.Flush(ctx)
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
	// Serialize the whole dump (condition evaluation + INSERT) against
	// invalidateChecksumWatermark, so the sentinel-abort path can never be
	// overtaken by an in-flight dump that read its conditions before the
	// continuous checker recorded a difference. See checkpointMu.
	r.checkpointMu.Lock()
	defer r.checkpointMu.Unlock()
	// Check if replication client and copier are initialized (nil if called before setup completes).
	// We hold chunkerMu to synchronize with initChunkers(), which
	// may be assigning r.copyChunker concurrently during setup.
	r.chunkerMu.RLock()
	copyChunker := r.copyChunker
	checksumChunker := r.checksumChunker
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
	// We only dump the checksumWatermark if we are in >= checksum state.
	// We require a mutex because the checker can be replaced during
	// operation, leaving a race condition.
	//
	// Safety invariant: the persisted checksum_watermark must only ever
	// describe chunks that have been *verified* clean (source == target on
	// a fresh read). A chunk that needed a recopy has not been verified —
	// only the recopy succeeded. The chunker, however, advances its
	// low-watermark past every chunk it sees Feedback() for, including
	// recopied ones. So in any pass where any chunk needed repair, the
	// chunker's low-watermark is *not* a valid resume point until a
	// subsequent pass re-checks those chunks clean.
	//
	// We enforce that here by reading DifferencesFound() *after* the
	// watermark and dropping the watermark to "" whenever the current
	// pass has had any repairs. Ordering matters: in single.go's
	// runChecksum, differencesFound is incremented before replaceChunk,
	// which is before chunker.Feedback advances the watermark. So any
	// failing chunk that has contributed to the watermark we just read
	// is guaranteed to be visible in DifferencesFound() by the time we
	// read it next.
	//
	// With this rule in place, a crash mid-pass (or retry exhaustion)
	// leaves a checkpoint whose checksum_watermark is "", forcing the
	// resumed run to re-verify the table from the start of the checksum
	// phase. That is the only safe recovery from a not-yet-completed
	// repair.
	//
	// The same invariant applies to the sentinel-wait continuous checker
	// (a separate object from r.checker — see continuousChecker): once it
	// has repaired any chunk, the watermark we would persist here is the
	// end-of-initial-checksum watermark, and resuming from it would let
	// the operator's re-run "pass" by verifying only the trailing chunks —
	// silently neutralizing the deliberate abort that the continuous
	// checksum triggers on divergence. So the watermark is persisted only
	// while BOTH checkers are clean (or the continuous one doesn't exist
	// yet).
	var checksumWatermark string
	if r.status.Get() >= status.Checksum {
		wm, wmErr := checksumChunker.GetLowWatermark()
		if wmErr != nil {
			return status.ErrWatermarkNotReady
		}
		if r.checker != nil && r.checker.DifferencesFound() == 0 &&
			(r.continuousChecker == nil || r.continuousChecker.DifferencesFound() == 0) {
			checksumWatermark = wm
		}
	}
	// Note: when we dump the lowWatermark to the log, we are exposing the PK values,
	// when using the composite chunker are based on actual user-data.
	// We believe this is OK but may change it in the future. Please do not
	// add any other fields to this log line.
	r.logger.Info("checkpoint",
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
		return status.ErrCouldNotWriteCheckpoint
	}
	return nil
}

func (r *Runner) Status() string {
	state := r.status.Get()
	if state > status.CutOver {
		return ""
	}
	switch state { //nolint: exhaustive
	case status.CopyRows:
		// Status for copy rows
		return fmt.Sprintf("migration status: state=%s copy-progress=%s binlog-deltas=%v total-time=%s copier-time=%s copier-remaining-time=%v copier-is-throttled=%v conns-in-use=%d%s",
			r.status.Get().String(),
			r.copier.GetProgress(),
			r.replClient.GetDeltaLen(),
			time.Since(r.startTime).Round(time.Second),
			time.Since(r.copier.StartTime()).Round(time.Second),
			r.copier.GetETA(),
			r.copier.GetThrottler().IsThrottled(),
			r.db.Stats().InUse,
			applier.StatusSuffix(r.applier),
		)
	case status.WaitingOnSentinelTable:
		return fmt.Sprintf("migration status: state=%s sentinel-table=%s.%s total-time=%s sentinel-wait-time=%s sentinel-max-wait-time=%s conns-in-use=%d",
			r.status.Get().String(),
			r.changes[0].table.SchemaName,
			sentinel.TableName,
			time.Since(r.startTime).Round(time.Second),
			time.Since(r.sentinelWaitStartTime).Round(time.Second),
			sentinel.WaitLimit,
			r.db.Stats().InUse,
		)
	case status.ApplyChangeset, status.PostChecksum:
		// We've finished copying rows, and we are now trying to reduce the number of binlog deltas before
		// proceeding to the checksum and then the final cutover.
		return fmt.Sprintf("migration status: state=%s binlog-deltas=%v total-time=%s conns-in-use=%d%s",
			r.status.Get().String(),
			r.replClient.GetDeltaLen(),
			time.Since(r.startTime).Round(time.Second),
			r.db.Stats().InUse,
			applier.StatusSuffix(r.applier),
		)
	case status.Checksum:
		return fmt.Sprintf("migration status: state=%s checksum-progress=%s binlog-deltas=%v total-time=%s checksum-time=%s conns-in-use=%d",
			r.status.Get().String(),
			r.checker.GetProgress().String(),
			r.replClient.GetDeltaLen(),
			time.Since(r.startTime).Round(time.Second),
			time.Since(r.checker.StartTime()).Round(time.Second),
			r.db.Stats().InUse,
		)
	}
	return ""
}

// invalidateChecksumWatermark blanks the checksum_watermark on this
// migration's persisted checkpoint rows if (and only if) the sentinel-wait
// continuous checker recorded any repaired chunks. Called from the
// sentinel-abort path: the periodic dumper already refuses to persist a
// watermark once the difference counter is non-zero, but the difference can
// be recorded between a dump's condition read and its INSERT — this UPDATE,
// serialized against the dumper via checkpointMu, runs strictly after any
// such in-flight INSERT and guarantees resume re-verifies from the start of
// the checksum phase. Scoped by statement because in multi-table mode the
// checkpoint table is shared with other concurrently-running migrations in
// the same schema (resume filters on statement the same way).
func (r *Runner) invalidateChecksumWatermark(ctx context.Context) error {
	r.checkpointMu.Lock()
	defer r.checkpointMu.Unlock()
	if r.continuousChecker == nil || r.continuousChecker.DifferencesFound() == 0 {
		return nil
	}
	r.logger.Warn("continuous checksum found differences; clearing persisted checksum watermark so the next run re-verifies from the start of the checksum phase")
	return dbconn.Exec(ctx, r.db, "UPDATE %n.%n SET checksum_watermark = %? WHERE statement = %?",
		r.checkpointTable.SchemaName,
		r.checkpointTable.TableName,
		"",
		r.migration.Statement,
	)
}

// runContinuousChecksum drives a checksum.ContinuousChecker over the source/new
// tables for as long as ctx is alive. It is the "continuous" half of the
// two-checksum model (see docs/migrate.md) and is only called while the
// migration is blocked in WaitingOnSentinelTable.
//
// It shares the implementation datasync uses (#979). No Recopier is configured:
// migration treats the continuous checksum as a cutover GATE, so a stable
// divergence surfaces as checksum.ErrPermanentDivergence and aborts the cutover.
// The previous SingleChecker did this via FixDifferences + MaxRetries=1, but it
// also aborted on rows that were merely mid-replication; the ContinuousChecker's
// retry / hot-chunk logic distinguishes transient lag from real divergence, so
// it is both safer and quieter. The checker reads the original table and the
// _new shadow table on the same connection (r.db for both source and target —
// the chunk carries the column mapping). It uses a fresh chunker so checkpoint
// state is unaffected. Single-threaded by design — checksum throttling is
// tracked separately in github.com/block/spirit/issues/831.
func (r *Runner) runContinuousChecksum(ctx context.Context) error {
	chunker, err := r.buildContinuousChunker()
	if err != nil {
		return fmt.Errorf("failed to build continuous-checksum chunker: %w", err)
	}
	if err := chunker.Open(); err != nil {
		return fmt.Errorf("failed to open continuous-checksum chunker: %w", err)
	}
	defer utils.CloseAndLog(chunker)

	checker, err := checksum.NewContinuousChecker(
		r.db, r.db, chunker, r.replClient,
		checksum.ContinuousCheckerConfig{
			// TODO(#831): once the throttler can size threads dynamically,
			// replace the hard-coded 1 with the migration's thread count.
			Concurrency:     1,
			TargetChunkTime: r.migration.TargetChunkTime,
			MinPassInterval: checksum.ContinuousMinPassInterval,
			// RetryDelay omitted: the constructor defaults it to
			// checksum.DefaultContinuousRetryDelay.
			Logger: r.logger,
			// Replication keeps _new in sync, so a confirmed divergence means a
			// real problem: abort the cutover (no Recopier, no self-heal).
			DivergenceIsFatal: true,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to create continuous checker: %w", err)
	}
	// Publish the checker so DumpCheckpoint (on the WatchTask goroutine) can
	// consult its DifferencesFound() when deciding whether the persisted
	// checksum_watermark is still trustworthy. Published before Run so there is
	// no window where a difference could be recorded while the dumper still
	// believes the table clean — the checker increments its mismatch counter
	// atomically as soon as a chunk's initial read mismatches.
	r.checkpointMu.Lock()
	r.continuousChecker = checker
	r.checkpointMu.Unlock()

	// Keep binlog deltas drained for the whole continuous phase (including the
	// inter-pass waits inside Run) so the cutover does not inherit a large
	// backlog that must be applied under the table lock.
	r.replClient.StartPeriodicFlush(ctx, change.DefaultFlushInterval)
	defer r.replClient.StopPeriodicFlush()

	runErr := checker.Run(ctx)
	// Suppress a clean cancellation (the sentinel was dropped, or the parent ctx
	// was cancelled, while a pass was in flight): ContinuousChecker.Run returns
	// ctx.Err() on cancel.
	//
	// We deliberately do NOT also gate this on DifferencesFound()==0 (as move's
	// older distributed-checker loop does), for two reasons:
	//  1. By the time runErr is context.Canceled, a *confirmed* divergence has
	//     provably not happened: Run returns ErrPermanentDivergence (a
	//     non-Canceled error, handled below) the instant it confirms a stable
	//     divergence, so a real problem aborts the cutover before we reach here.
	//  2. ContinuousChecker.DifferencesFound() is a LIFETIME count of
	//     first-attempt mismatches, which by design include the transient
	//     replication lag the retry loop reconciles. Under writes during the
	//     wait it is routinely >0, so gating on it would abort the cutover on
	//     nearly every sentinel-drop.
	// Whether a *confirmed* divergence aborts or self-heals is the explicit
	// DivergenceIsFatal policy: migration sets it true (replication-backed, so a
	// stable divergence becomes ErrPermanentDivergence and aborts here), while
	// datasync sets it false and heals via its Recopier.
	if ctx.Err() != nil && errors.Is(runErr, context.Canceled) {
		return nil
	}
	return runErr
}

// buildContinuousChunker builds a fresh chunker for the continuous-checksum
// loop. It is deliberately not wired into r.checksumChunker / checkpoint.
func (r *Runner) buildContinuousChunker() (table.Chunker, error) {
	chunkers := make([]table.Chunker, 0, len(r.changes))
	for _, change := range r.changes {
		columnRenames := change.stmt.ColumnRenameMap()
		columnMapping := table.NewColumnMapping(change.table, change.newTable, columnRenames)
		c, err := table.NewChunker(change.table, table.ChunkerConfig{
			NewTable:        change.newTable,
			TargetChunkTime: r.migration.TargetChunkTime,
			Logger:          r.logger,
			ColumnMapping:   columnMapping,
		})
		if err != nil {
			return nil, err
		}
		chunkers = append(chunkers, c)
	}
	return table.NewMultiChunker(chunkers...), nil
}

func (r *Runner) Cancel() {
	if r.cancelFunc != nil {
		r.cancelFunc()
	}
}
