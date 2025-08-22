package migration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cashapp/spirit/pkg/metrics"
	"github.com/cashapp/spirit/pkg/statement"

	"github.com/cashapp/spirit/pkg/check"
	"github.com/cashapp/spirit/pkg/checksum"
	"github.com/cashapp/spirit/pkg/dbconn"
	"github.com/cashapp/spirit/pkg/repl"
	"github.com/cashapp/spirit/pkg/row"
	"github.com/cashapp/spirit/pkg/table"
	"github.com/cashapp/spirit/pkg/throttler"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/siddontang/go-log/loggers"
	"github.com/sirupsen/logrus"
)

// These are really consts, but set to var for testing.
var (
	checkpointDumpInterval  = 50 * time.Second
	tableStatUpdateInterval = 5 * time.Minute
	statusInterval          = 30 * time.Second
	sentinelCheckInterval   = 1 * time.Second
	sentinelWaitLimit       = 48 * time.Hour
)

type Runner struct {
	migration       *Migration
	db              *sql.DB
	dbConfig        *dbconn.DBConfig
	replica         *sql.DB
	table           *table.TableInfo
	newTable        *table.TableInfo
	checkpointTable *table.TableInfo
	stmt            *statement.AbstractStatement

	currentState migrationState // must use atomic to get/set
	replClient   *repl.Client   // feed contains all binlog subscription activity.
	copier       *row.Copier
	throttler    throttler.Throttler
	checker      *checksum.Checker
	checkerLock  sync.Mutex

	// used to recover direct to checksum.
	checksumWatermark string

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
	logger loggers.Advanced

	// MetricsSink
	metricsSink metrics.Sink
}

// Progress is returned as a struct because we may add more to it later.
// It is designed for wrappers (like a GUI) to be able to summarize the
// current status without parsing log output.
type Progress struct {
	CurrentState string // string of current state, i.e. copyRows
	Summary      string // text based representation, i.e. "12.5% copyRows ETA 1h 30m"
}

func NewRunner(m *Migration) (*Runner, error) {
	stmt, err := m.normalizeOptions()
	if err != nil {
		return nil, err
	}
	return &Runner{
		migration:   m,
		logger:      logrus.New(),
		metricsSink: &metrics.NoopSink{},
		stmt:        stmt,
	}, nil
}

func (r *Runner) SetMetricsSink(sink metrics.Sink) {
	r.metricsSink = sink
}

func (r *Runner) SetLogger(logger loggers.Advanced) {
	r.logger = logger
}

func (r *Runner) Run(originalCtx context.Context) error {
	ctx, cancel := context.WithCancel(originalCtx)
	defer cancel()
	r.startTime = time.Now()
	r.logger.Infof("Starting spirit migration: concurrency=%d target-chunk-size=%s table='%s.%s' alter=%s ",
		r.migration.Threads, r.migration.TargetChunkTime, r.stmt.Schema, r.stmt.Table, r.stmt.Alter,
	)

	// Create a database connection
	// It will be closed in r.Close()
	var err error
	r.dbConfig = dbconn.NewDBConfig()
	r.dbConfig.LockWaitTimeout = int(r.migration.LockWaitTimeout.Seconds())
	r.dbConfig.InterpolateParams = r.migration.InterpolateParams
	r.dbConfig.ForceKill = r.migration.ForceKill
	// The copier and checker will use Threads to limit N tasks concurrently,
	// but we also set it at the DB pool level with +1. Because the copier and
	// the replication applier use the same pool, it allows for some natural throttling
	// of the copier if the replication applier is lagging. Because it's +1 it
	// means that the replication applier can always make progress immediately,
	// and does not need to wait for free slots from the copier *until* it needs
	// copy in more than 1 thread.
	// A MySQL 5.7 cutover also requires a minimum of 3 connections:
	// - The LOCK TABLES connection
	// - The Flush() connection(s)
	// - The blocking rename connection
	// We could extend the +1 to +2, but instead we increase the pool size
	// during the cutover procedure.
	r.dbConfig.MaxOpenConnections = r.migration.Threads + 1
	r.db, err = dbconn.New(r.dsn(), r.dbConfig)
	if err != nil {
		return err
	}

	// If it's not an alter table, that means it is a CREATE TABLE, DROP TABLE, or RENAME table.
	// We should execute it immediately before acquiring TableInfo.
	if !r.stmt.IsAlterTable() {
		err := dbconn.Exec(ctx, r.db, r.stmt.Statement)
		if err != nil {
			return err
		}
		r.logger.Infof("apply complete")
		return nil
	}

	// Get Table Info
	r.table = table.NewTableInfo(r.db, r.stmt.Schema, r.stmt.Table)
	if err := r.table.SetInfo(ctx); err != nil {
		return err
	}

	// Take a metadata lock to prevent other migrations from running concurrently.
	// We release the lock when this function finishes executing.
	// We need to call this after r.table is ready - otherwise we'd move this to
	// the start of the execution.
	metadataLock, err := dbconn.NewMetadataLock(ctx, r.dsn(), r.table, r.logger)
	if err != nil {
		return err
	}
	defer func() {
		if err := metadataLock.Close(); err != nil {
			r.logger.Errorf("failed to release metadata lock: %v", err)
		}
	}()
	// This step is technically optional, but first we attempt to
	// use MySQL's built-in DDL. This is because it's usually faster
	// when it is compatible. If it returns no error, that means it
	// has been successful and the DDL is complete.
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

	// Force enable the checksum if it's an ADD UNIQUE INDEX operation
	// https://github.com/cashapp/spirit/issues/266
	if !r.migration.Checksum {
		if err := r.stmt.AlterContainsAddUnique(); err != nil {
			r.logger.Warnf("force enabling checksum: %v", err)
			r.migration.Checksum = true
		}
	}

	// We don't want to allow visibility changes
	// This is because we've already attempted MySQL DDL as INPLACE, and it didn't work.
	// It likely means the user is combining this operation with other unsafe operations,
	// which is not a good idea. We need to protect them by not allowing it.
	// https://github.com/cashapp/spirit/issues/283
	if err := r.stmt.AlterContainsIndexVisibility(); err != nil {
		return err
	}

	// Run post-setup checks
	if err := r.runChecks(ctx, check.ScopePostSetup); err != nil {
		return err
	}

	go r.dumpStatus(ctx)                 // start periodically writing status
	go r.dumpCheckpointContinuously(ctx) // start periodically dumping the checkpoint.

	// Perform the main copy rows task. This is where the majority
	// of migrations usually spend time. It is not strictly necessary,
	// but we always recopy the last-bit, even if we are resuming
	// partially through the checksum.
	r.setCurrentState(stateCopyRows)
	if err := r.copier.Run(ctx); err != nil {
		return err
	}
	r.logger.Info("copy rows complete")
	r.replClient.SetKeyAboveWatermarkOptimization(false) // should no longer be used.

	// r.waitOnSentinel may return an error if there is
	// some unexpected problem checking for the existence of
	// the sentinel table OR if sentinelWaitLimit is exceeded.
	// This function is invoked even if DeferCutOver is false
	// because it's possible that the sentinel table was created
	// manually after the migration started.
	r.sentinelWaitStartTime = time.Now()
	r.setCurrentState(stateWaitingOnSentinelTable)
	if err := r.waitOnSentinelTable(ctx); err != nil {
		return err
	}

	// Perform steps to prepare for final cutover.
	// This includes computing an optional checksum,
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
	r.setCurrentState(stateCutOver)
	cutover, err := NewCutOver(r.db, r.table, r.newTable, r.oldTableName(), r.replClient, r.dbConfig, r.logger)
	if err != nil {
		return err
	}
	// Drop the _old table if it exists. This ensures
	// that the rename will succeed (although there is a brief race)
	if err := r.dropOldTable(ctx); err != nil {
		return err
	}
	if err := cutover.Run(ctx); err != nil {
		return err
	}
	if !r.migration.SkipDropAfterCutover {
		if err := r.dropOldTable(ctx); err != nil {
			// Don't return the error because our automation
			// will retry the migration (but it's already happened)
			r.logger.Errorf("migration successful but failed to drop old table: %s - %v", r.oldTableName(), err)
		} else {
			r.logger.Info("successfully dropped old table: ", r.oldTableName())
		}
	} else {
		r.logger.Info("skipped dropping old table: ", r.oldTableName())
	}
	checksumTime := time.Duration(0)
	if r.checker != nil {
		checksumTime = r.checker.ExecTime
	}
	_, copiedChunks, _ := r.copier.GetChunker().Progress()
	r.logger.Infof("apply complete: instant-ddl=%v inplace-ddl=%v total-chunks=%v copy-rows-time=%s checksum-time=%s total-time=%s conns-in-use=%d",
		r.usedInstantDDL,
		r.usedInplaceDDL,
		copiedChunks,
		r.copier.ExecTime.Round(time.Second),
		checksumTime.Round(time.Second),
		time.Since(r.startTime).Round(time.Second),
		r.db.Stats().InUse,
	)
	return r.cleanup(ctx)
}

// prepareForCutover performs steps to prepare for the final cutover.
// most of these steps are technically optional, but skipping them
// could for example cause a stall during the cutover if the replClient
// has too many pending updates.
func (r *Runner) prepareForCutover(ctx context.Context) error {
	r.setCurrentState(stateApplyChangeset)
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
	r.setCurrentState(stateAnalyzeTable)
	r.logger.Infof("Running ANALYZE TABLE")
	if err := dbconn.Exec(ctx, r.db, "ANALYZE TABLE %n.%n", r.newTable.SchemaName, r.newTable.TableName); err != nil {
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
	r.table.DisableAutoUpdateStatistics.Store(true)

	// The checksum is (usually) optional, but it is ONLINE after an initial lock
	// for consistency. It is the main way that we determine that
	// this program is safe to use even when immature. In the event that it is
	// a resume-from-checkpoint operation, the checksum is NOT optional.
	// This is because adding a unique index can not be differentiated from a
	// duplicate key error caused by retrying partial work.
	if r.migration.Checksum {
		if err := r.checksum(ctx); err != nil {
			return err
		}
	}
	return nil
}

// runChecks wraps around check.RunChecks and adds the context of this migration
func (r *Runner) runChecks(ctx context.Context, scope check.ScopeFlag) error {
	return check.RunChecks(ctx, check.Resources{
		DB:              r.db,
		Replica:         r.replica,
		Table:           r.table,
		Statement:       r.stmt,
		TargetChunkTime: r.migration.TargetChunkTime,
		Threads:         r.migration.Threads,
		ReplicaMaxLag:   r.migration.ReplicaMaxLag,
		ForceKill:       r.migration.ForceKill,
		// For the pre-run checks we don't have a DB connection yet.
		// Instead we check the credentials provided.
		Host:                 r.migration.Host,
		Username:             r.migration.Username,
		Password:             r.migration.Password,
		SkipDropAfterCutover: r.migration.SkipDropAfterCutover,
	}, r.logger, scope)
}

// attemptMySQLDDL "attempts" to use DDL directly on MySQL with an assertion
// such as ALGORITHM=INSTANT. If MySQL is able to use the INSTANT algorithm,
// it will perform the operation without error. If it can't, it will return
// an error. It is important to let MySQL decide if it can handle the DDL
// operation, because keeping track of which operations are "INSTANT"
// is incredibly difficult. It will depend on MySQL minor version,
// and could possibly be specific to the table.
func (r *Runner) attemptMySQLDDL(ctx context.Context) error {
	err := r.attemptInstantDDL(ctx)
	if err == nil {
		r.usedInstantDDL = true // success
		return nil
	}

	// Many "inplace" operations (such as adding an index)
	// are only online-safe to do in Aurora GLOBAL
	// because replicas do not use the binlog. Some, however,
	// only modify the table metadata and are safe.
	//
	// If the operator has specified that they want to attempt
	// an inplace add index, we will attempt inplace regardless
	// of the statement.
	err = r.stmt.AlgorithmInplaceConsideredSafe()
	if r.migration.ForceInplace || err == nil {
		err = r.attemptInplaceDDL(ctx)
		if err == nil {
			r.usedInplaceDDL = true // success
			return nil
		}
	}
	r.logger.Infof("unable to use INPLACE: %v", err)

	// Failure is expected, since MySQL DDL only applies in limited scenarios
	// Return the error, which will be ignored by the caller.
	// Proceed with regular copy algorithm.
	return err
}

func (r *Runner) dsn() string {
	return fmt.Sprintf("%s:%s@tcp(%s)/%s", r.migration.Username, r.migration.Password, r.migration.Host, r.stmt.Schema)
}

func (r *Runner) setup(ctx context.Context) error {
	// Drop the old table. It shouldn't exist, but it could.
	if err := r.dropOldTable(ctx); err != nil {
		return err
	}
	// Start subscribing to changes immediately.
	r.ddlNotification = make(chan string, 1)

	// First attempt to resume from a checkpoint.
	// It's OK if it fails, it just means it's a fresh migration.
	if err := r.resumeFromCheckpoint(ctx); err != nil {
		// Resume failed, do the initial steps.
		r.logger.Infof("could not resume from checkpoint: reason=%s", err)

		if r.migration.Strict && err == ErrMismatchedAlter {
			return err
		}

		if err := r.createNewTable(ctx); err != nil {
			return err
		}
		if err := r.alterNewTable(ctx); err != nil {
			return err
		}
		if err := r.createCheckpointTable(ctx); err != nil {
			return err
		}

		if r.migration.DeferCutOver {
			if err := r.createSentinelTable(ctx); err != nil {
				return err
			}
		}

		// Create chunker first with destination table info, then create copier with it
		chunker, err := table.NewChunker(r.table, r.newTable, r.migration.TargetChunkTime, r.logger)
		if err != nil {
			return err
		}
		// For now we always "open" the chunker, but that might become obsolete later.
		if err := chunker.Open(); err != nil {
			return err
		}
		r.copier, err = row.NewCopier(r.db, chunker, &row.CopierConfig{
			Concurrency:     r.migration.Threads,
			TargetChunkTime: r.migration.TargetChunkTime,
			FinalChecksum:   r.migration.Checksum,
			Throttler:       &throttler.Noop{},
			Logger:          r.logger,
			MetricsSink:     r.metricsSink,
			DBConfig:        r.dbConfig,
		})
		if err != nil {
			return err
		}
		r.replClient = repl.NewClient(r.db, r.migration.Host, r.migration.Username, r.migration.Password, &repl.ClientConfig{
			Logger:          r.logger,
			Concurrency:     r.migration.Threads,
			TargetBatchTime: r.migration.TargetChunkTime,
			OnDDL:           r.ddlNotification,
			ServerID:        repl.NewServerID(),
		})
		if err := r.replClient.AddSubscription(r.table, r.newTable, r.copier.KeyAboveHighWatermark); err != nil {
			return err
		}
		// Start the binary log feed now
		if err := r.replClient.Run(ctx); err != nil {
			return err
		}
	}

	// If the replica DSN was specified, attach a replication throttler.
	// Otherwise, it will default to the NOOP throttler.
	var err error
	if r.migration.ReplicaDSN != "" {
		r.replica, err = dbconn.New(r.migration.ReplicaDSN, r.dbConfig)
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
		if err := r.throttler.Open(); err != nil {
			return err
		}
	}

	// We can enable the key above watermark optimization
	r.replClient.SetKeyAboveWatermarkOptimization(true)

	// Start routines in table and replication packages to
	// Continuously update the min/max and estimated rows
	// and to flush the binary log position periodically.
	// These will both be stopped when the copier finishes
	// and checksum starts, although the PeriodicFlush
	// will be restarted again after.
	go r.table.AutoUpdateStatistics(ctx, tableStatUpdateInterval, r.logger)
	go r.replClient.StartPeriodicFlush(ctx, repl.DefaultFlushInterval)
	go r.tableChangeNotification(ctx)
	return nil
}

// tableChangeNotification is called as a goroutine.
// any schema changes will be sent here, and we need to determine
// if they are acceptable or not.
func (r *Runner) tableChangeNotification(ctx context.Context) {
	defer r.replClient.SetDDLNotificationChannel(nil)

	newTableEncoded := repl.EncodeSchemaTable(r.newTable.SchemaName, r.newTable.TableName)
	tableEncoded := repl.EncodeSchemaTable(r.table.SchemaName, r.table.TableName)

	for {
		select {
		case <-ctx.Done():
			return
		case tbl, ok := <-r.ddlNotification:
			if !ok {
				return // channel was closed
			}
			if r.getCurrentState() >= stateCutOver {
				return
			}
			if tbl == newTableEncoded || tbl == tableEncoded {
				r.setCurrentState(stateErrCleanup)
				// Write this to the logger, so it can be captured by the initiator.
				r.logger.Errorf("table definition of %s changed during migration", tbl)
				// Invalidate the checkpoint, so we don't try to resume.
				// If we don't do this, the migration will permanently be blocked from proceeding.
				// Letting it start again is the better choice.
				if err := r.dropCheckpoint(ctx); err != nil {
					r.logger.Errorf("could not remove checkpoint. err: %v", err)
				}
				// We can't do anything about it, just panic
				panic(fmt.Sprintf("table definition of %s changed during migration", tbl))
			}
		}
	}
}

func (r *Runner) dropCheckpoint(ctx context.Context) error {
	return dbconn.Exec(ctx, r.db, "DROP TABLE IF EXISTS %n.%n", r.checkpointTable.SchemaName, r.checkpointTable.TableName)
}

func (r *Runner) createNewTable(ctx context.Context) error {
	newName := fmt.Sprintf(check.NameFormatNew, r.table.TableName)
	// drop both if we've decided to call this func.
	if err := dbconn.Exec(ctx, r.db, "DROP TABLE IF EXISTS %n.%n", r.table.SchemaName, newName); err != nil {
		return err
	}
	if err := dbconn.Exec(ctx, r.db, "CREATE TABLE %n.%n LIKE %n.%n",
		r.table.SchemaName, newName, r.table.SchemaName, r.table.TableName); err != nil {
		return err
	}
	r.newTable = table.NewTableInfo(r.db, r.stmt.Schema, newName)
	if err := r.newTable.SetInfo(ctx); err != nil {
		return err
	}
	return nil
}

// alterNewTable applies the ALTER to the new table.
// It has been pre-checked it is not a rename, or modifying the PRIMARY KEY.
// We first attempt to do this using ALGORITHM=COPY so we don't burn
// an INSTANT version. But surprisingly this is not supported for all DDLs (issue #277)
func (r *Runner) alterNewTable(ctx context.Context) error {
	if err := dbconn.Exec(ctx, r.db, "ALTER TABLE %n.%n "+r.stmt.TrimAlter()+", ALGORITHM=COPY",
		r.newTable.SchemaName, r.newTable.TableName); err != nil {
		// Retry without the ALGORITHM=COPY. If there is a second error, then the DDL itself
		// is not supported. It could be a syntax error, in which case we return the second error,
		// which will probably be easier to read because it is unaltered.
		if err := dbconn.Exec(ctx, r.db, "ALTER TABLE %n.%n "+r.stmt.Alter, r.newTable.SchemaName, r.newTable.TableName); err != nil {
			return err
		}
	}
	// Call GetInfo on the table again, since the columns
	// might have changed and this will affect the row copiers intersect func.
	return r.newTable.SetInfo(ctx)
}

func (r *Runner) dropOldTable(ctx context.Context) error {
	return dbconn.Exec(ctx, r.db, "DROP TABLE IF EXISTS %n.%n", r.table.SchemaName, r.oldTableName())
}

func (r *Runner) oldTableName() string {
	// By default we just set the old table name to _<table>_old
	// but if they've enabled SkipDropAfterCutover, we add a timestamp
	if !r.migration.SkipDropAfterCutover {
		return fmt.Sprintf(check.NameFormatOld, r.table.TableName)
	}
	return fmt.Sprintf(check.NameFormatOldTimeStamp, r.table.TableName, r.startTime.UTC().Format(check.NameFormatTimestamp))
}

func (r *Runner) attemptInstantDDL(ctx context.Context) error {
	return dbconn.Exec(ctx, r.db, "ALTER TABLE %n.%n ALGORITHM=INSTANT, "+r.stmt.Alter, r.table.SchemaName, r.table.TableName)
}

func (r *Runner) attemptInplaceDDL(ctx context.Context) error {
	return dbconn.Exec(ctx, r.db, "ALTER TABLE %n.%n ALGORITHM=INPLACE, LOCK=NONE, "+r.stmt.Alter, r.table.SchemaName, r.table.TableName)
}

func (r *Runner) createCheckpointTable(ctx context.Context) error {
	cpName := fmt.Sprintf(check.NameFormatCheckpoint, r.table.TableName)
	// drop both if we've decided to call this func.
	if err := dbconn.Exec(ctx, r.db, "DROP TABLE IF EXISTS %n.%n", r.table.SchemaName, cpName); err != nil {
		return err
	}
	if err := dbconn.Exec(ctx, r.db, `CREATE TABLE %n.%n (
	id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	copier_watermark TEXT,
	checksum_watermark TEXT,
	binlog_name VARCHAR(255),
	binlog_pos INT,
	rows_copied BIGINT,
	alter_statement TEXT
	)`,
		r.table.SchemaName, cpName); err != nil {
		return err
	}
	r.checkpointTable = table.NewTableInfo(r.db, r.table.SchemaName, cpName)
	return nil
}

func (r *Runner) GetProgress() Progress {
	var summary string
	switch r.getCurrentState() { //nolint: exhaustive
	case stateCopyRows:
		summary = fmt.Sprintf("%v %s ETA %v",
			r.copier.GetProgress(),
			r.getCurrentState().String(),
			r.copier.GetETA(),
		)
	case stateWaitingOnSentinelTable:
		summary = "Waiting on Sentinel Table"
	case stateApplyChangeset, statePostChecksum:
		summary = fmt.Sprintf("Applying Changeset Deltas=%v", r.replClient.GetDeltaLen())
	case stateChecksum:
		r.checkerLock.Lock()
		summary = "Checksum Progress=" + r.checker.GetProgress()
		r.checkerLock.Unlock()
	}
	return Progress{
		CurrentState: r.getCurrentState().String(),
		Summary:      summary,
	}
}

func (r *Runner) sentinelTableName() string {
	return fmt.Sprintf(check.NameFormatSentinel, r.table.TableName)
}

func (r *Runner) createSentinelTable(ctx context.Context) error {
	if err := dbconn.Exec(ctx, r.db, "DROP TABLE IF EXISTS %n.%n", r.table.SchemaName, r.sentinelTableName()); err != nil {
		return err
	}
	if err := dbconn.Exec(ctx, r.db, "CREATE TABLE %n.%n (id int NOT NULL PRIMARY KEY)", r.table.SchemaName, r.sentinelTableName()); err != nil {
		return err
	}
	return nil
}

func (r *Runner) cleanup(ctx context.Context) error {
	if r.newTable != nil {
		if err := dbconn.Exec(ctx, r.db, "DROP TABLE IF EXISTS %n.%n", r.newTable.SchemaName, r.newTable.TableName); err != nil {
			return err
		}
	}
	if r.checkpointTable != nil {
		if err := r.dropCheckpoint(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) Close() error {
	r.setCurrentState(stateClose)
	if r.table != nil {
		err := r.table.Close()
		if err != nil {
			return err
		}
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
	if r.ddlNotification != nil {
		close(r.ddlNotification)
	}
	return nil
}

func (r *Runner) resumeFromCheckpoint(ctx context.Context) error {
	// Check that the new table exists and the checkpoint table
	// has at least one row in it.

	// The objects for these are not available until we confirm
	// tables exist and we
	newName := fmt.Sprintf(check.NameFormatNew, r.table.TableName)
	cpName := fmt.Sprintf(check.NameFormatCheckpoint, r.table.TableName)

	// Make sure we can read from the new table.
	if err := dbconn.Exec(ctx, r.db, "SELECT * FROM %n.%n LIMIT 1",
		r.stmt.Schema, newName); err != nil {
		return fmt.Errorf("could not find any checkpoints in table '%s'", newName)
	}

	// We intentionally SELECT * FROM the checkpoint table because if the structure
	// changes, we want this operation to fail. This will indicate that the checkpoint
	// was created by either an earlier or later version of spirit, in which case
	// we do not support recovery.
	query := fmt.Sprintf("SELECT * FROM `%s`.`%s` ORDER BY id DESC LIMIT 1",
		r.stmt.Schema, cpName)
	var copierWatermark, binlogName, alterStatement string
	var id, binlogPos int
	var rowsCopied uint64
	err := r.db.QueryRow(query).Scan(&id, &copierWatermark, &r.checksumWatermark, &binlogName, &binlogPos, &rowsCopied, &alterStatement)
	if err != nil {
		return fmt.Errorf("could not read from table '%s', err:%v", cpName, err)
	}
	if r.stmt.Alter != alterStatement {
		return ErrMismatchedAlter
	}
	// Populate the objects that would have been set in the other funcs.
	r.newTable = table.NewTableInfo(r.db, r.stmt.Schema, newName)
	if err := r.newTable.SetInfo(ctx); err != nil {
		return err
	}

	// In resume-from-checkpoint we need to ignore duplicate key errors when
	// applying copy-rows because we will partially re-apply some rows.
	// The problem with this is, we can't tell if it's not a re-apply but a new
	// row that's a duplicate and violating a new UNIQUE constraint we are trying
	// to add. The only way we can reconcile this fact is to make sure that
	// we checksum the table at the end. Thus, resume-from-checkpoint MUST
	// have the checksum enabled to apply all changes safely.
	r.migration.Checksum = true

	// Create chunker first and open at the checkpoint watermark
	chunker, err := table.NewChunker(r.table, r.newTable, r.migration.TargetChunkTime, r.logger)
	if err != nil {
		return err
	}

	// Open chunker at the specified watermark
	// For high watermark, use the type of old table, not the new one.
	highPtr := table.NewDatum(r.newTable.MaxValue().Val, r.table.MaxValue().Tp)
	if err := chunker.OpenAtWatermark(copierWatermark, highPtr, rowsCopied); err != nil {
		return err
	}

	// Create copier with the prepared chunker
	r.copier, err = row.NewCopier(r.db, chunker, &row.CopierConfig{
		Concurrency:     r.migration.Threads,
		TargetChunkTime: r.migration.TargetChunkTime,
		FinalChecksum:   r.migration.Checksum,
		Throttler:       &throttler.Noop{},
		Logger:          r.logger,
		MetricsSink:     r.metricsSink,
		DBConfig:        r.dbConfig,
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
	if err := r.replClient.AddSubscription(r.table, r.newTable, r.copier.KeyAboveHighWatermark); err != nil {
		return err
	}
	r.replClient.SetFlushedPos(mysql.Position{
		Name: binlogName,
		Pos:  uint32(binlogPos),
	})

	r.checkpointTable = table.NewTableInfo(r.db, r.table.SchemaName, cpName)

	// Start the replClient now. This is because if the checkpoint is so old there
	// are no longer binary log files, we want to abandon resume-from-checkpoint
	// and still be able to start from scratch.
	// Start the binary log feed just before copy rows starts.
	if err := r.replClient.Run(ctx); err != nil {
		r.logger.Warnf("resuming from checkpoint failed because resuming from the previous binlog position failed. log-file: %s log-pos: %d", binlogName, binlogPos)
		return err
	}
	r.logger.Warnf("resuming from checkpoint. copier-watermark: %s checksum-watermark: %s log-file: %s log-pos: %d copy-rows: %d", copierWatermark, r.checksumWatermark, binlogName, binlogPos, rowsCopied)
	r.usedResumeFromCheckpoint = true
	return nil
}

// checksum creates the checksum which opens the read view.
func (r *Runner) checksum(ctx context.Context) error {
	r.setCurrentState(stateChecksum)
	// The checksum keeps the pool threads open, so we need to extend
	// by more than +1 on threads as we did previously. We have:
	// - background flushing
	// - checkpoint thread
	// - checksum "replaceChunk" DB connections
	// Handle a case just in the tests not having a dbConfig
	if r.dbConfig == nil {
		r.dbConfig = dbconn.NewDBConfig()
	}
	r.db.SetMaxOpenConns(r.dbConfig.MaxOpenConnections + 2)
	var err error
	for i := range 3 { // try the checksum up to 3 times.
		if i > 0 {
			r.checksumWatermark = "" // reset the watermark if we are retrying.
		}
		r.checkerLock.Lock()
		r.checker, err = checksum.NewChecker(r.db, r.table, r.newTable, r.replClient, &checksum.CheckerConfig{
			Concurrency:     r.migration.Threads,
			TargetChunkTime: r.migration.TargetChunkTime,
			DBConfig:        r.dbConfig,
			Logger:          r.logger,
			FixDifferences:  true, // we want to repair the differences.
			Watermark:       r.checksumWatermark,
		})
		r.checkerLock.Unlock()
		if err != nil {
			return err
		}
		if err := r.checker.Run(ctx); err != nil {
			// This is really not expected to happen. The checksum should always pass.
			// If it doesn't, we have a resolver.
			return err
		}
		// If we are here, the checksum passed.
		// But we don't know if differences were found and chunks were recopied.
		// We want to know it passed without one.
		if r.checker.DifferencesFound() == 0 {
			break // success!
		}
		if i >= 2 {
			// This used to say "checksum failed, this should never happen" but that's not entirely true.
			// If the user attempts a lossy schema change such as adding a UNIQUE INDEX to non-unique data,
			// then the checksum will fail. This is entirely expected, and not considered a bug. We should
			// do our best-case to differentiate that we believe this ALTER statement is lossy, and
			// customize the returned error based on it.
			if err := r.stmt.AlterContainsAddUnique(); err != nil {
				return errors.New("checksum failed after 3 attempts. Check that the ALTER statement is not adding a UNIQUE INDEX to non-unique data")
			}
			return errors.New("checksum failed after 3 attempts. This likely indicates either a bug in Spirit, or a manual modification to the _new table outside of Spirit. Please report @ github.com/cashapp/spirit")
		}
		r.logger.Errorf("checksum failed, retrying %d/%d times", i+1, 3)
	}
	r.logger.Info("checksum passed")

	// A long checksum extends the binlog deltas
	// So if we've called this optional checksum, we need one more state
	// of applying the binlog deltas.
	r.setCurrentState(statePostChecksum)
	return r.replClient.Flush(ctx)
}

func (r *Runner) getCurrentState() migrationState {
	return migrationState(atomic.LoadInt32((*int32)(&r.currentState)))
}

func (r *Runner) setCurrentState(s migrationState) {
	atomic.StoreInt32((*int32)(&r.currentState), int32(s))
}

// dumpCheckpoint is called approximately every minute.
// It writes the current state of the migration to the checkpoint table,
// which can be used in recovery. Previously resuming from checkpoint
// would always restart at the copier, but it can now also resume at
// the checksum phase.
func (r *Runner) dumpCheckpoint(ctx context.Context) error {
	// Retrieve the binlog position first and under a mutex.
	binlog := r.replClient.GetBinlogApplyPosition()
	copierWatermark, err := r.copier.GetLowWatermark()
	if err != nil {
		return err // it might not be ready, we can try again.
	}
	// We only dump the checksumWatermark if we are in >= checksum state.
	// We require a mutex because the checker can be replaced during
	// operation, leaving a race condition.
	var checksumWatermark string
	if r.getCurrentState() >= stateChecksum {
		r.checkerLock.Lock()
		defer r.checkerLock.Unlock()
		if r.checker != nil {
			checksumWatermark, err = r.checker.GetLowWatermark()
			if err != nil {
				return err
			}
		}
	}
	copyRows, _, _ := r.copier.GetChunker().Progress()

	// Note: when we dump the lowWatermark to the log, we are exposing the PK values,
	// when using the composite chunker are based on actual user-data.
	// We believe this is OK but may change it in the future. Please do not
	// add any other fields to this log line.
	r.logger.Infof("checkpoint: low-watermark=%s log-file=%s log-pos=%d rows-copied=%d", copierWatermark, binlog.Name, binlog.Pos, copyRows)
	return dbconn.Exec(ctx, r.db, "INSERT INTO %n.%n (copier_watermark, checksum_watermark, binlog_name, binlog_pos, rows_copied, alter_statement) VALUES (%?, %?, %?, %?, %?, %?)",
		r.checkpointTable.SchemaName,
		r.checkpointTable.TableName,
		copierWatermark,
		checksumWatermark,
		binlog.Name,
		binlog.Pos,
		copyRows,
		r.stmt.Alter,
	)
}

func (r *Runner) dumpCheckpointContinuously(ctx context.Context) {
	ticker := time.NewTicker(checkpointDumpInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Continue to checkpoint until we exit the checksum.
			if r.getCurrentState() >= stateCutOver {
				return
			}
			if err := r.dumpCheckpoint(ctx); err != nil {
				r.logger.Errorf("error writing checkpoint: %v", err)
			}
		}
	}
}

func (r *Runner) dumpStatus(ctx context.Context) {
	ticker := time.NewTicker(statusInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			state := r.getCurrentState()
			if state > stateCutOver {
				return
			}

			switch state {
			case stateCopyRows:
				// Status for copy rows

				r.logger.Infof("migration status: state=%s copy-progress=%s binlog-deltas=%v total-time=%s copier-time=%s copier-remaining-time=%v copier-is-throttled=%v conns-in-use=%d",
					r.getCurrentState().String(),
					r.copier.GetProgress(),
					r.replClient.GetDeltaLen(),
					time.Since(r.startTime).Round(time.Second),
					time.Since(r.copier.StartTime()).Round(time.Second),
					r.copier.GetETA(),
					r.copier.Throttler.IsThrottled(),
					r.db.Stats().InUse,
				)
			case stateWaitingOnSentinelTable:
				r.logger.Infof("migration status: state=%s sentinel-table=%s.%s total-time=%s sentinel-wait-time=%s sentinel-max-wait-time=%s conns-in-use=%d",
					r.getCurrentState().String(),
					r.table.SchemaName,
					r.sentinelTableName(),
					time.Since(r.startTime).Round(time.Second),
					time.Since(r.sentinelWaitStartTime).Round(time.Second),
					sentinelWaitLimit,
					r.db.Stats().InUse,
				)
			case stateApplyChangeset, statePostChecksum:
				// We've finished copying rows, and we are now trying to reduce the number of binlog deltas before
				// proceeding to the checksum and then the final cutover.
				r.logger.Infof("migration status: state=%s binlog-deltas=%v total-time=%s conns-in-use=%d",
					r.getCurrentState().String(),
					r.replClient.GetDeltaLen(),
					time.Since(r.startTime).Round(time.Second),
					r.db.Stats().InUse,
				)
			case stateChecksum:
				// This could take a while if it's a large table.
				r.checkerLock.Lock()
				r.logger.Infof("migration status: state=%s checksum-progress=%s binlog-deltas=%v total-time=%s checksum-time=%s conns-in-use=%d",
					r.getCurrentState().String(),
					r.checker.GetProgress(),
					r.replClient.GetDeltaLen(),
					time.Since(r.startTime).Round(time.Second),
					time.Since(r.checker.StartTime()).Round(time.Second),
					r.db.Stats().InUse,
				)
				r.checkerLock.Unlock()
			default:
				// For the linter:
				// Status for all other states
			}
		}
	}
}

func (r *Runner) sentinelTableExists(ctx context.Context) (bool, error) {
	sql := "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
	var sentinelTableExists int
	err := r.db.QueryRowContext(ctx, sql, r.table.SchemaName, r.sentinelTableName()).Scan(&sentinelTableExists)
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

	r.logger.Warnf("cutover deferred while sentinel table %s.%s exists; will wait %s", r.table.SchemaName, r.sentinelTableName(), sentinelWaitLimit)

	timer := time.NewTimer(sentinelWaitLimit)

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
