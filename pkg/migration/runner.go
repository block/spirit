package migration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/check"
	"github.com/block/spirit/pkg/checksum"
	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/repl"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/throttler"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

// These are really consts, but set to var for testing.
var (
	tableStatUpdateInterval = 5 * time.Minute
	sentinelCheckInterval   = 1 * time.Second
	sentinelWaitLimit       = 48 * time.Hour
	sentinelTableName       = "_spirit_sentinel"   // this is now a const.
	checkpointTableName     = "_spirit_checkpoint" // const for multi-migration checkpoints.
)

type Runner struct {
	migration       *Migration
	poolDB          *sql.DB // Worker pool for copier/applier/repl (thread-limited)
	db              *sql.DB // Dedicated connection for checkpoints/sentinels/control queries
	dbConfig        *dbconn.DBConfig
	replica         *sql.DB
	checkpointTable *table.TableInfo

	// Changes enccapsulates all changes
	// With a stmt, alter, table, newTable.
	changes []*change

	status     status.State // must use atomic helpers to change.
	replClient *repl.Client // feed contains all binlog subscription activity.
	throttler  throttler.Throttler

	copier       copier.Copier
	copyChunker  table.Chunker // the chunker for copying
	copyDuration time.Duration // how long the copy took

	checker         checksum.Checker
	checksumChunker table.Chunker // the chunker for checksum

	ddlNotification chan string

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

	// MetricsSink
	metricsSink metrics.Sink
}

var _ status.Task = (*Runner)(nil)

func NewRunner(m *Migration) (*Runner, error) {
	stmts, err := m.normalizeOptions()
	if err != nil {
		return nil, err
	}
	changes := make([]*change, 0, len(stmts))
	for _, stmt := range stmts {
		changes = append(changes, &change{
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
	r.logger.Info("Starting spirit migration",
		"concurrency", r.migration.Threads,
		"target-chunk-size", r.migration.TargetChunkTime,
	)

	// Create a database connection
	// It will be closed in r.Close()
	var err error
	r.dbConfig = dbconn.NewDBConfig()
	r.dbConfig.LockWaitTimeout = int(r.migration.LockWaitTimeout.Seconds())
	r.dbConfig.InterpolateParams = r.migration.InterpolateParams
	r.dbConfig.ForceKill = r.migration.ForceKill
	// Map TLS configuration from migration to dbConfig
	r.dbConfig.TLSMode = r.migration.TLSMode
	r.dbConfig.TLSCertificatePath = r.migration.TLSCertificatePath
	// The copier and checker will use Threads to limit N tasks concurrently.
	// We always +1 on what the user asks for (legacy; to always make progress)
	r.dbConfig.MaxOpenConnections = r.migration.Threads + 1
	if r.migration.EnableExperimentalBufferedCopy {
		// Buffered has many more connections because it fans out read and write.
		// Currently in the migration runner we don't have a writeThreads config,
		// so we just need to make sure we add the default here.
		r.dbConfig.MaxOpenConnections += 2 /* applier.defaultWriteWorkers*/
	}
	r.poolDB, err = dbconn.New(r.dsn(), r.dbConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to main database (DSN: %s): %w", maskPasswordInDSN(r.dsn()), err)
	}

	// Create dedicated management connection for checkpoints, sentinels, and control queries
	// This connection is never blocked by worker pool exhaustion
	managementConfig := dbconn.NewDBConfig()
	managementConfig.TLSMode = r.migration.TLSMode
	managementConfig.TLSCertificatePath = r.migration.TLSCertificatePath
	managementConfig.LockWaitTimeout = int(r.migration.LockWaitTimeout.Seconds())
	managementConfig.ForceKill = r.migration.ForceKill
	managementConfig.MaxOpenConnections = 5
	r.db, err = dbconn.New(r.dsn(), managementConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to management database (DSN: %s): %w", maskPasswordInDSN(r.dsn()), err)
	}

	// Enable linting if any of the linting related options are given
	if r.migration.EnableExperimentalLinting || r.migration.ExperimentalLintOnly ||
		len(r.migration.EnableExperimentalLinters) > 0 || len(r.migration.ExperimentalLinterConfig) > 0 {
		if err := r.lint(ctx); err != nil {
			return err
		}
		if r.migration.ExperimentalLintOnly {
			fmt.Printf("Exiting after running linters.\n")
			return nil
		}
	}

	if len(r.changes) == 1 {
		// We only allow non-ALTERs (i.e. CREATE TABLE, DROP TABLE, RENAME TABLE)
		// in single table mode.
		if !r.changes[0].stmt.IsAlterTable() {
			err := dbconn.Exec(ctx, r.db, r.changes[0].stmt.Statement)
			if err != nil {
				return err
			}
			r.logger.Info("apply complete")
			return nil
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

	// Take a single metadata lock for all tables to prevent concurrent DDL.
	// This uses a single DB connection instead of one per table.
	// We release the lock when this function finishes executing.
	lock, err := dbconn.NewMetadataLock(ctx, r.dsn(), tables, r.dbConfig, r.logger)
	if err != nil {
		return err
	}

	// Release the lock
	defer func() {
		if err := lock.Close(); err != nil {
			r.logger.Error("failed to release metadata lock", "error", err)
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
	// The watermark optimizations can prevent some keys from being flushed,
	// which would cause flushedPos to not advance, leading to a mismatch
	// with bufferedPos and causing AllChangesFlushed() to return false.
	r.replClient.SetWatermarkOptimization(false)

	// r.waitOnSentinel may return an error if there is
	// some unexpected problem checking for the existence of
	// the sentinel table OR if sentinelWaitLimit is exceeded.
	// This function is invoked even if DeferCutOver is false
	// because it's possible that the sentinel table was created
	// manually after the migration started.
	if r.migration.RespectSentinel {
		r.sentinelWaitStartTime = time.Now()
		r.status.Set(status.WaitingOnSentinelTable)
		if err := r.waitOnSentinelTable(ctx); err != nil {
			return err
		}
	}
	// Perform steps to prepare for final cutover.
	// This includes computing a checksum,
	// catching up on replClient apply, running ANALYZE TABLE so
	// that the statistics will be up-to-date on cutover.
	if err := r.prepareForCutover(ctx); err != nil {
		return err
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
		return err
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
		"copy-rows-time", r.copyDuration.Round(time.Second),
		"checksum-time", r.checker.ExecTime().Round(time.Second),
		"total-time", time.Since(r.startTime).Round(time.Second),
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
		if err := r.dropCheckpoint(ctx); err != nil {
			return err
		}
	}
	return nil
}

// prepareForCutover performs steps to prepare for the final cutover.
// most of these steps are technically optional, but skipping them
// could for example cause a stall during the cutover if the replClient
// has too many pending updates.
func (r *Runner) prepareForCutover(ctx context.Context) error {
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
			Replica:         r.replica,
			Table:           change.table,
			Statement:       change.stmt,
			TargetChunkTime: r.migration.TargetChunkTime,
			Threads:         r.migration.Threads,
			ReplicaMaxLag:   r.migration.ReplicaMaxLag,
			ForceKill:       r.migration.ForceKill,
			// For the pre-run checks we don't have a DB connection yet.
			// Instead we check the credentials provided.
			Host:                     r.migration.Host,
			Username:                 r.migration.Username,
			Password:                 *r.migration.Password,
			TLSMode:                  r.migration.TLSMode,
			TLSCertificatePath:       r.migration.TLSCertificatePath,
			SkipDropAfterCutover:     r.migration.SkipDropAfterCutover,
			ExperimentalBufferedCopy: r.migration.EnableExperimentalBufferedCopy,
		}, r.logger, scope); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) dsn() string {
	return fmt.Sprintf("%s:%s@tcp(%s)/%s", r.migration.Username, *r.migration.Password, r.migration.Host, r.changes[0].stmt.Schema)
}

// maskPasswordInDSN masks the password in any DSN string for safe logging
func maskPasswordInDSN(dsn string) string {
	if dsn == "" {
		return dsn
	}

	// Use MySQL driver's ParseDSN for robust parsing
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		// If parsing fails, fall back to the original DSN
		// This preserves the original behavior for malformed DSNs
		return dsn
	}

	// Check if the original DSN had a password field by looking for `:` before `@`
	// This handles both empty passwords (user:@host) and non-empty passwords (user:pass@host)
	atIndex := strings.Index(dsn, "@")
	colonIndex := strings.Index(dsn, ":")
	hasPasswordField := colonIndex != -1 && atIndex != -1 && colonIndex < atIndex

	// Only mask if there was actually a password field in the original DSN
	if hasPasswordField {
		cfg.Passwd = "***"
	}

	return cfg.FormatDSN()
}

func (r *Runner) checkpointTableName() string {
	// We also call the create functions for the sentinel
	// and checkpoint tables.
	if len(r.changes) > 1 {
		return checkpointTableName
	}
	return fmt.Sprintf(check.NameFormatCheckpoint, r.changes[0].table.TableName)
}

func (r *Runner) setupCopierCheckerAndReplClient(ctx context.Context) error {
	var err error
	r.checkpointTable = table.NewTableInfo(r.db, r.changes[0].table.SchemaName, r.checkpointTableName())
	// Create an applier if using buffered copy or buffered replication
	var appl applier.Applier
	if r.migration.EnableExperimentalBufferedCopy {
		// For now, we only support single-table migrations with buffered copy
		if len(r.changes) > 1 {
			return errors.New("buffered copy is not yet supported for multi-table migrations")
		}
		// Create a SingleTargetApplier for the buffered copier
		appl, err = applier.NewSingleTargetApplier(
			applier.Target{DB: r.poolDB},
			&applier.ApplierConfig{
				Logger:   r.logger,
				DBConfig: r.dbConfig,
			},
		)
		if err != nil {
			return fmt.Errorf("failed to create applier for buffered copier: %w", err)
		}
	}
	// Create copier with the prepared chunker
	r.copier, err = copier.NewCopier(r.poolDB, r.copyChunker, &copier.CopierConfig{
		Concurrency:                   r.migration.Threads,
		TargetChunkTime:               r.migration.TargetChunkTime,
		Throttler:                     &throttler.Noop{},
		Logger:                        r.logger,
		MetricsSink:                   r.metricsSink,
		DBConfig:                      r.dbConfig,
		UseExperimentalBufferedCopier: r.migration.EnableExperimentalBufferedCopy,
		Applier:                       appl,
	})
	if err != nil {
		return err
	}

	// Set the binlog position.
	// Create a binlog subscriber with both worker pool and management connections
	r.replClient = repl.NewClient(r.poolDB, r.db, r.migration.Host, r.migration.Username, *r.migration.Password, &repl.ClientConfig{
		Logger:          r.logger,
		Concurrency:     r.migration.Threads,
		TargetBatchTime: r.migration.TargetChunkTime,
		OnDDL:           r.ddlNotification,
		ServerID:        repl.NewServerID(),
		DBConfig:        r.dbConfig, // Pass database configuration to replication client
		Applier:         appl,
	})
	// For each of the changes, we know the new table exists now
	// So we should call SetInfo to populate the columns etc.
	for _, change := range r.changes {
		if err := change.newTable.SetInfo(ctx); err != nil {
			return err
		}
		if err := r.replClient.AddSubscription(change.table, change.newTable, r.copyChunker); err != nil {
			return err
		}
	}

	r.checker, err = checksum.NewChecker(r.poolDB, r.checksumChunker, r.replClient, &checksum.CheckerConfig{
		Concurrency:     r.migration.Threads,
		TargetChunkTime: r.migration.TargetChunkTime,
		DBConfig:        r.dbConfig,
		Logger:          r.logger,
		FixDifferences:  true, // we want to repair the differences.
		MaxRetries:      3,
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
	if err := r.createCheckpointTable(ctx); err != nil {
		return err
	}
	if r.migration.DeferCutOver {
		if err := r.createSentinelTable(ctx); err != nil {
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
	if err := r.replClient.Run(ctx); err != nil {
		return err
	}
	return nil
}

// setupReplicationThrottler sets up the replication throttler if a replica DSN is configured.
// This is common logic shared between resume and new migration paths.
func (r *Runner) setupReplicationThrottler(ctx context.Context) error {
	if r.migration.ReplicaDSN == "" {
		return nil // No replica DSN specified, use default NOOP throttler
	}

	var err error
	// Create a separate DB config for replica connection
	replicaDBConfig := dbconn.NewDBConfig()
	replicaDBConfig.LockWaitTimeout = r.dbConfig.LockWaitTimeout
	replicaDBConfig.InterpolateParams = r.dbConfig.InterpolateParams
	replicaDBConfig.ForceKill = r.dbConfig.ForceKill
	replicaDBConfig.MaxOpenConnections = r.dbConfig.MaxOpenConnections

	// Copy TLS settings from main DB config to replica config
	// This allows the DSN enhancement to use the correct TLS settings
	replicaDBConfig.TLSMode = r.dbConfig.TLSMode
	replicaDBConfig.TLSCertificatePath = r.dbConfig.TLSCertificatePath

	// Enhance replica DSN with TLS settings if not already present
	// This preserves any explicit TLS configuration in the replica DSN
	// while providing sensible defaults from the main DB configuration
	enhancedReplicaDSN, err := dbconn.EnhanceDSNWithTLS(r.migration.ReplicaDSN, replicaDBConfig)
	if err != nil {
		r.logger.Warn("could not enhance replica DSN with TLS settings",
			"error", err,
		)
		// Continue with original DSN if enhancement fails
		enhancedReplicaDSN = r.migration.ReplicaDSN
	}

	r.replica, err = dbconn.NewWithConnectionType(enhancedReplicaDSN, replicaDBConfig, "replica database")
	if err != nil {
		return fmt.Errorf("failed to connect to replica database (DSN: %s): %w", maskPasswordInDSN(r.migration.ReplicaDSN), err)
	}

	// An error here means the connection to the replica is not valid, or it can't be detected
	// This is fatal because if a user specifies a replica throttler, and it can't be used,
	// we should not proceed.
	r.throttler, err = throttler.NewReplicationThrottler(r.replica, r.migration.ReplicaMaxLag, r.logger)
	if err != nil {
		r.logger.Warn("could not create replication throttler",
			"error", err,
		)
		return err
	}

	r.copier.SetThrottler(r.throttler)
	return r.throttler.Open(ctx)
}

// startBackgroundRoutines starts the background routines needed for migration monitoring.
// This includes table statistics updates, periodic binlog flushing, and DDL change notifications.
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
	go r.replClient.StartPeriodicFlush(ctx, repl.DefaultFlushInterval)
	go r.tableChangeNotification(ctx)
	// Start go routines for checkpointing and dumping status
	status.WatchTask(ctx, r, r.logger)
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
	r.ddlNotification = make(chan string, 1)

	// We always attempt to resume from a checkpoint.
	if err = r.resumeFromCheckpoint(ctx); err != nil {
		// Strict mode means if we have a mismatched alter,
		// we should not continue. This is to protect against
		// a user re-running a migration with a different alter
		// statement when a previous migration was incomplete,
		// and all progress is lost.
		if r.migration.Strict && err == status.ErrMismatchedAlter {
			return err
		}

		r.logger.Info("could not resume from checkpoint",
			"reason", err,
		) // explain why it failed.

		// Since we are not strict, we are allowed to
		// start a new migration.
		if err := r.newMigration(ctx); err != nil {
			return err
		}
	}

	// Setup replication throttler (common logic for both paths)
	if err := r.setupReplicationThrottler(ctx); err != nil {
		return err
	}

	// We can enable the key above watermark optimization
	r.replClient.SetWatermarkOptimization(true)

	// Start background monitoring routines (common logic for both paths)
	r.startBackgroundRoutines(ctx)

	return nil
}

// tableChangeNotification is called as a goroutine.
// Any schema changes to the source or new table will be sent to a channel
// that this function reads from.
func (r *Runner) tableChangeNotification(ctx context.Context) {
	defer r.replClient.SetDDLNotificationChannel(nil)
	for {
		select {
		case <-ctx.Done():
			return
		case tbl, ok := <-r.ddlNotification:
			if !ok {
				return // channel was closed
			}
			if r.status.Get() >= status.CutOver {
				return
			}

			// The table names are filtered from the replication stream
			// Before they are sent here, so we know it's one of our tables.
			// Because there has been an external change,
			// we now have to cancel our work :(
			r.status.Set(status.ErrCleanup)
			// Write this to the logger, so it can be captured by the initiator.
			r.logger.Error("table definition changed during migration",
				"table", tbl,
			)
			// Invalidate the checkpoint, so we don't try to resume.
			// If we don't do this, the migration will permanently be blocked from proceeding.
			// Letting it start again is the better choice.
			if err := r.dropCheckpoint(ctx); err != nil {
				r.logger.Error("could not remove checkpoint",
					"error", err,
				)
			}
			r.cancelFunc() // cancel the migration context
		}
	}
}

func (r *Runner) dropCheckpoint(ctx context.Context) error {
	return dbconn.Exec(ctx, r.db, "DROP TABLE IF EXISTS %n.%n", r.checkpointTable.SchemaName, r.checkpointTable.TableName)
}

func (r *Runner) createCheckpointTable(ctx context.Context) error {
	cpName := r.checkpointTableName()
	// drop both if we've decided to call this func.
	if err := dbconn.Exec(ctx, r.db, "DROP TABLE IF EXISTS %n.%n", r.changes[0].table.SchemaName, cpName); err != nil {
		return err
	}
	if err := dbconn.Exec(ctx, r.db, `CREATE TABLE %n.%n (
	id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	copier_watermark TEXT,
	checksum_watermark TEXT,
	binlog_name VARCHAR(255),
	binlog_pos INT,
	statement TEXT
	)`,
		r.changes[0].table.SchemaName, cpName); err != nil {
		return err
	}
	return nil
}

func (r *Runner) Progress() status.Progress {
	var summary string
	switch r.status.Get() { //nolint: exhaustive
	case status.CopyRows:
		summary = fmt.Sprintf("%v %s ETA %v",
			r.copier.GetProgress(),
			r.status.Get().String(),
			r.copier.GetETA(),
		)
	case status.WaitingOnSentinelTable:
		summary = "Waiting on Sentinel Table"
	case status.ApplyChangeset, status.PostChecksum:
		summary = fmt.Sprintf("Applying Changeset Deltas=%v", r.replClient.GetDeltaLen())
	case status.Checksum:
		summary = "Checksum Progress=" + r.checker.GetProgress()
	}
	return status.Progress{
		CurrentState: r.status.Get(),
		Summary:      summary,
	}
}

func (r *Runner) createSentinelTable(ctx context.Context) error {
	if err := dbconn.Exec(ctx, r.db, "DROP TABLE IF EXISTS %n.%n", r.changes[0].table.SchemaName, sentinelTableName); err != nil {
		return err
	}
	if err := dbconn.Exec(ctx, r.db, "CREATE TABLE %n.%n (id int NOT NULL PRIMARY KEY)", r.changes[0].table.SchemaName, sentinelTableName); err != nil {
		return err
	}
	return nil
}

func (r *Runner) Close() error {
	r.status.Set(status.Close)
	for _, change := range r.changes {
		err := change.Close()
		if err != nil {
			return err
		}
	}
	// Set the DDL notification channel to nil before closing it
	// to prevent race conditions where another goroutine might try to send to it
	if r.replClient != nil {
		r.replClient.SetDDLNotificationChannel(nil)
	}
	if r.ddlNotification != nil {
		close(r.ddlNotification)
	}
	if r.replClient != nil {
		r.replClient.Close()
	}
	if r.throttler != nil {
		err := r.throttler.Close()
		if err != nil {
			return err
		}
	}
	if r.replica != nil {
		err := r.replica.Close()
		if err != nil {
			return err
		}
	}
	if r.db != nil {
		err := r.db.Close()
		if err != nil {
			return err
		}
	}
	if r.poolDB != nil {
		err := r.poolDB.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) resumeFromCheckpoint(ctx context.Context) error {
	// Check that the new table(s) exists and are readable.
	for _, change := range r.changes {
		newName := fmt.Sprintf(check.NameFormatNew, change.table.TableName)
		if err := dbconn.Exec(ctx, r.db, "SELECT 1 FROM %n.%n LIMIT 1", change.stmt.Schema, newName); err != nil {
			return fmt.Errorf("could not find new table '%s' to resume from checkpoint", newName)
		}
	}

	// We intentionally SELECT * FROM the checkpoint table because if the structure
	// changes, we want this operation to fail. This will indicate that the checkpoint
	// was created by either an earlier or later version of spirit, in which case
	// we do not support recovery.
	query := fmt.Sprintf("SELECT * FROM `%s`.`%s` ORDER BY id DESC LIMIT 1",
		r.changes[0].stmt.Schema, r.checkpointTableName())
	var copierWatermark, binlogName, statement, checksumWatermark string
	var id, binlogPos int
	err := r.db.QueryRowContext(ctx, query).Scan(&id, &copierWatermark, &checksumWatermark, &binlogName, &binlogPos, &statement)
	if err != nil {
		return fmt.Errorf("could not read from table '%s', err:%v", r.checkpointTableName(), err)
	}

	// We need to validate that the statement matches between the checkpoint
	// and the new migration we are running. We can do this by string comparison
	// to r.migration.Statement since it is going to be populated for both
	// multi and non-multi table migrations.
	if r.migration.Statement != statement {
		return status.ErrMismatchedAlter
	}

	// Initialize and call SetInfo on all the new tables, since we need the column info
	for _, change := range r.changes {
		// Initialize newTable with the expected new table name
		newName := fmt.Sprintf(check.NameFormatNew, change.table.TableName)
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

	// This is setup the same way in both code-paths,
	// but we need to do it before we finish resumeFromCheckpoint
	// because we need to check that the binlog file exists.
	if err := r.setupCopierCheckerAndReplClient(ctx); err != nil {
		return err
	}

	r.replClient.SetFlushedPos(gomysql.Position{
		Name: binlogName,
		Pos:  uint32(binlogPos),
	})

	// Start the replClient now. This is because if the checkpoint is so old there
	// are no longer binary log files, we want to abandon resume-from-checkpoint
	// and still be able to start from scratch.
	// Start the binary log feed just before copy rows starts.
	if err := r.replClient.Run(ctx); err != nil {
		r.logger.Warn("resuming from checkpoint failed because resuming from the previous binlog position failed",
			"log-file", binlogName,
			"log-pos", binlogPos,
		)
		return err
	}
	r.logger.Warn("resuming from checkpoint",
		"copier-watermark", copierWatermark,
		"checksum-watermark", checksumWatermark,
		"log-file", binlogName,
		"log-pos", binlogPos,
	)
	r.usedResumeFromCheckpoint = true
	return nil
}

// initChunkers sets up the chunker(s) for the migration.
// It does not open them yet, and we need to either
// call Open() or OpenAtWatermark() later.
func (r *Runner) initChunkers() error {
	copyChunkers := make([]table.Chunker, 0, len(r.changes))
	checksumChunkers := make([]table.Chunker, 0, len(r.changes))
	for _, change := range r.changes {
		copyChunker, err := table.NewChunker(change.table, change.newTable, r.migration.TargetChunkTime, r.logger)
		if err != nil {
			return err
		}
		checksumChunker, err := table.NewChunker(change.table, change.newTable, r.migration.TargetChunkTime, r.logger)
		if err != nil {
			return err
		}
		copyChunkers = append(copyChunkers, copyChunker)
		checksumChunkers = append(checksumChunkers, checksumChunker)
	}
	// We can wrap it the multi-chunker regardless.
	// It won't cause any harm.
	r.copyChunker = table.NewMultiChunker(copyChunkers...)
	r.checksumChunker = table.NewMultiChunker(checksumChunkers...)
	return nil
}

// checksum creates the checksum which opens the read view
func (r *Runner) checksum(ctx context.Context) error {
	r.status.Set(status.Checksum)

	// Run the checksum with internal retry logic
	if err := r.checker.Run(ctx); err != nil {
		if r.addsUniqueIndex() {
			// Overwrite the error if we think it's because of a unique index addition
			return errors.New("checksum failed after several attempts. This is likely related to your statement adding a UNIQUE index on non-unique data")
		}
		return err
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
	// Retrieve the binlog position first and under a mutex.
	binlog := r.replClient.GetBinlogApplyPosition()
	copierWatermark, err := r.copyChunker.GetLowWatermark()
	if err != nil {
		return status.ErrWatermarkNotReady // it might not be ready, we can try again.
	}
	// We only dump the checksumWatermark if we are in >= checksum state.
	// We require a mutex because the checker can be replaced during
	// operation, leaving a race condition.
	var checksumWatermark string
	if r.status.Get() >= status.Checksum {
		checksumWatermark, err = r.checksumChunker.GetLowWatermark()
		if err != nil {
			return status.ErrWatermarkNotReady
		}
	}
	// Note: when we dump the lowWatermark to the log, we are exposing the PK values,
	// when using the composite chunker are based on actual user-data.
	// We believe this is OK but may change it in the future. Please do not
	// add any other fields to this log line.
	r.logger.Info("checkpoint",
		"low-watermark", copierWatermark,
		"log-file", binlog.Name,
		"log-pos", binlog.Pos,
	)
	err = dbconn.Exec(ctx, r.db, "INSERT INTO %n.%n (copier_watermark, checksum_watermark, binlog_name, binlog_pos, statement) VALUES (%?, %?, %?, %?, %?)",
		r.checkpointTable.SchemaName,
		r.checkpointTable.TableName,
		copierWatermark,
		checksumWatermark,
		binlog.Name,
		binlog.Pos,
		r.migration.Statement,
	)
	if err != nil {
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
		return fmt.Sprintf("migration status: state=%s copy-progress=%s binlog-deltas=%v total-time=%s copier-time=%s copier-remaining-time=%v copier-is-throttled=%v conns-in-use=%d",
			r.status.Get().String(),
			r.copier.GetProgress(),
			r.replClient.GetDeltaLen(),
			time.Since(r.startTime).Round(time.Second),
			time.Since(r.copier.StartTime()).Round(time.Second),
			r.copier.GetETA(),
			r.copier.GetThrottler().IsThrottled(),
			r.db.Stats().InUse,
		)
	case status.WaitingOnSentinelTable:
		return fmt.Sprintf("migration status: state=%s sentinel-table=%s.%s total-time=%s sentinel-wait-time=%s sentinel-max-wait-time=%s conns-in-use=%d",
			r.status.Get().String(),
			r.changes[0].table.SchemaName,
			sentinelTableName,
			time.Since(r.startTime).Round(time.Second),
			time.Since(r.sentinelWaitStartTime).Round(time.Second),
			sentinelWaitLimit,
			r.db.Stats().InUse,
		)
	case status.ApplyChangeset, status.PostChecksum:
		// We've finished copying rows, and we are now trying to reduce the number of binlog deltas before
		// proceeding to the checksum and then the final cutover.
		return fmt.Sprintf("migration status: state=%s binlog-deltas=%v total-time=%s conns-in-use=%d",
			r.status.Get().String(),
			r.replClient.GetDeltaLen(),
			time.Since(r.startTime).Round(time.Second),
			r.db.Stats().InUse,
		)
	case status.Checksum:
		return fmt.Sprintf("migration status: state=%s checksum-progress=%s binlog-deltas=%v total-time=%s checksum-time=%s conns-in-use=%d",
			r.status.Get().String(),
			r.checker.GetProgress(),
			r.replClient.GetDeltaLen(),
			time.Since(r.startTime).Round(time.Second),
			time.Since(r.checker.StartTime()).Round(time.Second),
			r.db.Stats().InUse,
		)
	}
	return ""
}

func (r *Runner) sentinelTableExists(ctx context.Context) (bool, error) {
	// Use managementDB for sentinel checks (infrastructure, not data)
	sql := "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
	var sentinelTableExists int
	err := r.db.QueryRowContext(ctx, sql, r.changes[0].table.SchemaName, sentinelTableName).Scan(&sentinelTableExists)
	if err != nil {
		return false, err
	}
	return sentinelTableExists > 0, nil
}

// Check every sentinelCheckInterval up to sentinelWaitLimit to see if sentinelTable has been dropped
func (r *Runner) waitOnSentinelTable(ctx context.Context) error {
	if sentinelExists, err := r.sentinelTableExists(ctx); err != nil {
		return err
	} else if !sentinelExists {
		// Sentinel table does not exist, we can proceed with cutover
		return nil
	}

	r.logger.Warn("cutover deferred while sentinel table exists; will wait",
		"sentinel-table", sentinelTableName,
		"wait-limit", sentinelWaitLimit,
	)

	timer := time.NewTimer(sentinelWaitLimit)
	defer timer.Stop() // Ensure timer is always stopped to prevent goroutine leak

	ticker := time.NewTicker(sentinelCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case t := <-ticker.C:
			sentinelExists, err := r.sentinelTableExists(ctx)
			if err != nil {
				return err
			}
			if !sentinelExists {
				// Sentinel table has been dropped, we can proceed with cutover
				r.logger.Info("sentinel table dropped",
					"time", t,
				)
				return nil
			}
		case <-timer.C:
			return errors.New("timed out waiting for sentinel table to be dropped")
		}
	}
}

func (r *Runner) Cancel() {
	if r.cancelFunc != nil {
		r.cancelFunc()
	}
}
