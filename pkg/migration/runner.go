package migration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

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
	"github.com/siddontang/go-log/loggers"
	"github.com/sirupsen/logrus"
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
	db              *sql.DB
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

	checker         *checksum.Checker
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
	logger     loggers.Advanced
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
		logger:      logrus.New(),
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

func (r *Runner) SetLogger(logger loggers.Advanced) {
	r.logger = logger
}

func (r *Runner) attemptMySQLDDL(ctx context.Context) error {
	if r.migration.EnableExperimentalMultiTableSupport {
		return errors.New("attemptMySQLDDL only supports single-table changes")
	}
	return r.changes[0].attemptMySQLDDL(ctx)
}

func (r *Runner) Run(ctx context.Context) error {
	ctx, r.cancelFunc = context.WithCancel(ctx)
	defer r.cancelFunc()
	r.startTime = time.Now()
	r.logger.Infof("Starting spirit migration: concurrency=%d target-chunk-size=%s",
		r.migration.Threads,
		r.migration.TargetChunkTime,
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
	// The copier and checker will use Threads to limit N tasks concurrently,
	// but we also set it at the DB pool level with +1. Because the copier and
	// the replication applier use the same pool, it allows for some natural throttling
	// of the copier if the replication applier is lagging. Because it's +1 it
	// means that the replication applier can always make progress immediately,
	// and does not need to wait for free slots from the copier *until* it needs
	// copy in more than 1 thread.
	r.dbConfig.MaxOpenConnections = r.migration.Threads + 1
	if r.migration.EnableExperimentalBufferedCopy {
		// Buffered has many more connections because it fans out x8 more write threads
		// Plus it has read threads. Set this high and figure it out later.
		r.dbConfig.MaxOpenConnections = 100
	}
	r.db, err = dbconn.New(r.dsn(), r.dbConfig)
	if err != nil {
		return err
	}

	if r.migration.EnableExperimentalMultiTableSupport {
		// We don't (yet) support a lot of features in multi-schema changes, and
		// we never attempt instant/inplace DDL. So for now all we need to do
		// is setup and call SetInfo on each of the tables.
		r.logger.Warn("Enabling the experimental option: enable-experimental-multi-table-support")
	} else {
		// We only allow non-ALTERs (i.e. CREATE TABLE, DROP TABLE, RENAME TABLE)
		// in single table mode.
		if !r.changes[0].stmt.IsAlterTable() {
			err := dbconn.Exec(ctx, r.db, r.changes[0].stmt.Statement)
			if err != nil {
				return err
			}
			r.logger.Infof("apply complete")
			return nil
		}
	}
	locks := make([]*dbconn.MetadataLock, 0, len(r.changes))
	// Set info for all of the tables.
	for _, change := range r.changes {
		change.table = table.NewTableInfo(r.db, change.stmt.Schema, change.stmt.Table)
		if err := change.table.SetInfo(ctx); err != nil {
			return err
		}
		// Take a metadata lock on the source table to prevent concurrent DDL.
		// We release the lock when this function finishes executing.
		lock, err := dbconn.NewMetadataLock(ctx, r.dsn(), change.table, r.dbConfig, r.logger)
		if err != nil {
			return err
		}
		locks = append(locks, lock)
	}
	// Release all our locks
	defer func() {
		for _, lock := range locks {
			if err := lock.Close(); err != nil {
				r.logger.Errorf("failed to release metadata lock: %v", err)
			}
		}
	}()
	// This step is technically optional, but first we attempt to
	// use MySQL's built-in DDL. This is because it's usually faster
	// when it is compatible. If it returns no error, that means it
	// has been successful and the DDL is complete.
	// Note: this function returns an error when in multi-table mode.
	err = r.attemptMySQLDDL(ctx)
	if err == nil {
		r.logger.Infof("apply complete: instant-ddl=%v inplace-ddl=%v", r.usedInstantDDL, r.usedInplaceDDL)
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
	r.replClient.SetKeyAboveWatermarkOptimization(false) // should no longer be used.

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
			table:        change.table,
			newTable:     change.newTable,
			oldTableName: change.oldTableName(),
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
				r.logger.Errorf("migration successful but failed to drop old table: %s - %v", change.oldTableName(), err)
			} else {
				r.logger.Info("successfully dropped old table: ", change.oldTableName())
			}
		}
	} else {
		r.logger.Info("skipped dropping old table")
	}
	_, copiedChunks, _ := r.copyChunker.Progress()
	r.logger.Infof("apply complete: instant-ddl=%v inplace-ddl=%v total-chunks=%v copy-rows-time=%s checksum-time=%s total-time=%s conns-in-use=%d",
		r.usedInstantDDL,
		r.usedInplaceDDL,
		copiedChunks,
		r.copyDuration.Round(time.Second),
		r.checker.ExecTime.Round(time.Second),
		time.Since(r.startTime).Round(time.Second),
		r.db.Stats().InUse,
	)
	// cleanup all the tables
	for _, change := range r.changes {
		if err := change.cleanup(ctx); err != nil {
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
	r.logger.Infof("Running ANALYZE TABLE")
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
			Password:                 r.migration.Password,
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
	return fmt.Sprintf("%s:%s@tcp(%s)/%s", r.migration.Username, r.migration.Password, r.migration.Host, r.changes[0].stmt.Schema)
}

func (r *Runner) checkpointTableName() string {
	// We also call the create functions for the sentinel
	// and checkpoint tables.
	cpName := fmt.Sprintf(check.NameFormatCheckpoint, r.changes[0].table.TableName)
	if r.migration.EnableExperimentalMultiTableSupport {
		// In multi-mode we always use a centralized checkpoint table.
		cpName = checkpointTableName
	}
	return cpName
}

func (r *Runner) setupCopierCheckerAndReplClient(ctx context.Context) error {
	var err error
	r.checkpointTable = table.NewTableInfo(r.db, r.changes[0].table.SchemaName, r.checkpointTableName())
	// Create copier with the prepared chunker
	r.copier, err = copier.NewCopier(r.db, r.copyChunker, &copier.CopierConfig{
		Concurrency:                   r.migration.Threads,
		TargetChunkTime:               r.migration.TargetChunkTime,
		Throttler:                     &throttler.Noop{},
		Logger:                        r.logger,
		MetricsSink:                   r.metricsSink,
		DBConfig:                      r.dbConfig,
		UseExperimentalBufferedCopier: r.migration.EnableExperimentalBufferedCopy,
	})
	if err != nil {
		return err
	}

	// Set the binlog position.
	// Create a binlog subscriber
	r.replClient = repl.NewClient(r.db, r.migration.Host, r.migration.Username, r.migration.Password, &repl.ClientConfig{
		Logger:          r.logger,
		Concurrency:     r.migration.Threads,
		TargetBatchTime: r.migration.TargetChunkTime,
		OnDDL:           r.ddlNotification,
		ServerID:        repl.NewServerID(),
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

	r.checker, err = checksum.NewChecker(r.db, r.checksumChunker, r.replClient, &checksum.CheckerConfig{
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
	// Create a separate DB config for replica connection without TLS overrides
	// The replica DSN should contain its own TLS configuration
	replicaDBConfig := dbconn.NewDBConfig()
	replicaDBConfig.LockWaitTimeout = r.dbConfig.LockWaitTimeout
	replicaDBConfig.InterpolateParams = r.dbConfig.InterpolateParams
	replicaDBConfig.ForceKill = r.dbConfig.ForceKill
	replicaDBConfig.MaxOpenConnections = r.dbConfig.MaxOpenConnections
	// Note: Deliberately NOT copying TLS settings (TLSMode, TLSCertificatePath)
	// TODO: Replica TLS configuration will be handled in a separate PR
	// See https://github.com/block/spirit/issues/175
	r.replica, err = dbconn.New(r.migration.ReplicaDSN, replicaDBConfig)
	if err != nil {
		return err
	}

	// An error here means the connection to the replica is not valid, or it can't be detected
	// This is fatal because if a user specifies a replica throttler, and it can't be used,
	// we should not proceed.
	r.throttler, err = throttler.NewReplicationThrottler(r.replica, r.migration.ReplicaMaxLag, r.logger)
	if err != nil {
		r.logger.Warnf("could not create replication throttler: %v", err)
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

		r.logger.Infof("could not resume from checkpoint: reason=%s", err) // explain why it failed.

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
	r.replClient.SetKeyAboveWatermarkOptimization(true)

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
			r.logger.Errorf("table definition of %s changed during migration", tbl)
			// Invalidate the checkpoint, so we don't try to resume.
			// If we don't do this, the migration will permanently be blocked from proceeding.
			// Letting it start again is the better choice.
			if err := r.dropCheckpoint(ctx); err != nil {
				r.logger.Errorf("could not remove checkpoint. err: %v", err)
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
	err := r.db.QueryRow(query).Scan(&id, &copierWatermark, &checksumWatermark, &binlogName, &binlogPos, &statement)
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
		r.logger.Warnf("resuming from checkpoint failed because resuming from the previous binlog position failed. log-file: %s log-pos: %d", binlogName, binlogPos)
		return err
	}
	r.logger.Warnf("resuming from checkpoint. copier-watermark: %s checksum-watermark: %s log-file: %s log-pos: %d", copierWatermark, checksumWatermark, binlogName, binlogPos)
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
	if !r.migration.EnableExperimentalMultiTableSupport {
		r.copyChunker = copyChunkers[0]
		r.checksumChunker = checksumChunkers[0]
	} else {
		r.copyChunker = table.NewMultiChunker(copyChunkers...)
		r.checksumChunker = table.NewMultiChunker(checksumChunkers...)
	}
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
	// Handle a case just in the tests not having a dbConfig
	r.db.SetMaxOpenConns(r.dbConfig.MaxOpenConnections + 2)

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
	r.logger.Infof("checkpoint: low-watermark=%s log-file=%s log-pos=%d", copierWatermark, binlog.Name, binlog.Pos)
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

	r.logger.Warnf("cutover deferred while sentinel table %s exists; will wait %s", sentinelTableName, sentinelWaitLimit)

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
				r.logger.Infof("sentinel table dropped at %s", t)
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
