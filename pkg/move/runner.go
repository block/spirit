package move

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/block/spirit/pkg/checksum"
	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/repl"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/throttler"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-sql-driver/mysql"
	"github.com/siddontang/loggers"
	"github.com/sirupsen/logrus"
)

var (
	sentinelCheckInterval   = 1 * time.Second
	tableStatUpdateInterval = 5 * time.Minute
	sentinelWaitLimit       = 48 * time.Hour
	sentinelTableName       = "_spirit_sentinel" // this is now a const.
	checkpointTableName     = "_spirit_checkpoint"
	checkpointDumpInterval  = 50 * time.Second
)

type Runner struct {
	move            *Move
	source          *sql.DB
	sourceConfig    *mysql.Config
	target          *sql.DB
	targetConfig    *mysql.Config
	status          status.State // must use atomic to get/set
	checkpointTable *table.TableInfo

	sourceTables []*table.TableInfo

	replClient        *repl.Client
	copyChunker       table.Chunker
	checksumChunker   table.Chunker
	copier            copier.Copier
	checker           *checksum.Checker
	checksumWatermark string

	// Track some key statistics.
	startTime                time.Time
	sentinelWaitStartTime    time.Time
	usedResumeFromCheckpoint bool

	cutoverFunc func(ctx context.Context) error

	logger     loggers.Advanced
	cancelFunc context.CancelFunc
	dbConfig   *dbconn.DBConfig
}

func NewRunner(m *Move) (*Runner, error) {
	r := &Runner{
		move:   m,
		logger: logrus.New(),
	}
	return r, nil
}

func (r *Runner) Close() error {
	if r.copyChunker != nil {
		r.copyChunker.Close()
	}
	if r.replClient != nil {
		r.replClient.Close()
	}
	return nil
}

// getTables connects to a DB and fetches the list of tables
// it can be run on either the source or the target.
func (r *Runner) getTables(ctx context.Context, db *sql.DB) ([]*table.TableInfo, error) {
	rows, err := db.QueryContext(ctx, "SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tableName string
	tables := make([]*table.TableInfo, 0)
	for rows.Next() {
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		tableInfo := table.NewTableInfo(r.source, r.sourceConfig.DBName, tableName)
		if err := tableInfo.SetInfo(ctx); err != nil {
			return nil, err
		}
		tables = append(tables, tableInfo)
	}
	return tables, rows.Err()
}

// checkTargetEmpty checks that the target database is empty.
// If any tables exist it returns an error and the move fails.
func (r *Runner) checkTargetEmpty() error {
	rows, err := r.target.Query("SHOW TABLES")
	if err != nil {
		return err
	}
	defer rows.Close()
	if rows.Next() {
		return errors.New("target database is not empty")
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}

// createTargetTables fetches the CREATE TABLE statement for each table
// in r.sourceTables and runs it on r.target. For now, we require that
// the target is identical to the source. In future, we may create
// the target with secondary indexes disabled, and re-add them after.
func (r *Runner) createTargetTables() error {
	for _, t := range r.sourceTables {
		var createStmt string
		row := r.source.QueryRow(fmt.Sprintf("SHOW CREATE TABLE `%s`", t.TableName))
		var tbl string
		if err := row.Scan(&tbl, &createStmt); err != nil {
			return err
		}
		// Execute the create statement on the target.
		if _, err := r.target.Exec(createStmt); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) resumeFromCheckpoint(ctx context.Context) error {
	copyChunkers := make([]table.Chunker, 0, len(r.sourceTables))
	checksumChunkers := make([]table.Chunker, 0, len(r.sourceTables))
	var err error
	// For each table create a chunker and add a subscription to the replication client.
	for _, src := range r.sourceTables {
		dest := table.NewTableInfo(r.target, r.targetConfig.DBName, src.TableName)
		if err := dest.SetInfo(ctx); err != nil {
			return err
		}
		copyChunker, err := table.NewChunker(src, dest, r.move.TargetChunkTime, r.logger)
		if err != nil {
			return err
		}
		if err := r.replClient.AddSubscription(src, dest, copyChunker.KeyAboveHighWatermark); err != nil {
			return err
		}
		checksumChunker, err := table.NewChunker(src, dest, r.move.TargetChunkTime, r.logger)
		if err != nil {
			return err
		}
		copyChunkers = append(copyChunkers, copyChunker)
		checksumChunkers = append(checksumChunkers, checksumChunker)
	}

	// Then create a multi chunker of all chunkers.
	r.copyChunker = table.NewMultiChunker(copyChunkers...)
	r.checksumChunker = table.NewMultiChunker(checksumChunkers...)

	// Create a copier that reads from the multi chunker and writes to the target.
	r.copier, err = copier.NewCopier(r.source, r.copyChunker, &copier.CopierConfig{
		Concurrency:                   r.move.Threads,
		TargetChunkTime:               r.move.TargetChunkTime,
		Logger:                        r.logger,
		Throttler:                     &throttler.Noop{},
		MetricsSink:                   &metrics.NoopSink{},
		DBConfig:                      r.dbConfig,
		UseExperimentalBufferedCopier: true,
		WriteDB:                       r.target,
	})
	if err != nil {
		return err
	}

	// We intentionally SELECT * FROM the checkpoint table because if the structure
	// changes, we want this operation to fail. This will indicate that the checkpoint
	// was created by either an earlier or later version of spirit, in which case
	// we do not support recovery.
	query := fmt.Sprintf("SELECT * FROM `%s`.`%s` ORDER BY id DESC LIMIT 1",
		r.targetConfig.DBName, checkpointTableName)
	var copierWatermark, binlogName, statement string
	var id, binlogPos int
	err = r.target.QueryRow(query).Scan(&id, &copierWatermark, &r.checksumWatermark, &binlogName, &binlogPos, &statement)
	if err != nil {
		return fmt.Errorf("could not read from table '%s', err:%v", checkpointTableName, err)
	}

	r.replClient.SetFlushedPos(gomysql.Position{
		Name: binlogName,
		Pos:  uint32(binlogPos),
	})

	// Open chunker at the specified watermark
	if err := r.copyChunker.OpenAtWatermark(copierWatermark); err != nil {
		return err
	}

	// Start the replication client.
	if err := r.replClient.Run(ctx); err != nil {
		return err
	}

	r.checkpointTable = table.NewTableInfo(r.target, r.targetConfig.DBName, checkpointTableName)
	r.usedResumeFromCheckpoint = true
	return nil
}

func (r *Runner) setup(ctx context.Context) error {
	var err error
	// Fetch a list of tables from the source.
	r.logger.Infof("Fetching source table list")
	if r.sourceTables, err = r.getTables(ctx, r.source); err != nil {
		return err
	}
	r.logger.Infof("Setting up repl client")

	r.replClient = repl.NewClient(r.source, r.sourceConfig.Addr, r.sourceConfig.User, r.sourceConfig.Passwd, &repl.ClientConfig{
		Logger:                     r.logger,
		Concurrency:                r.move.Threads,
		TargetBatchTime:            r.move.TargetChunkTime,
		ServerID:                   repl.NewServerID(),
		UseExperimentalBufferedMap: true,
		WriteDB:                    r.target,
	})

	r.logger.Infof("Checking target database state")

	if err := r.checkTargetEmpty(); err != nil {
		// There are existing tables there.
		// Optimistically try to resume from a checkpoint written
		// to the target database. If it fails, unlike schema changes,
		// the move fails because we don't want to overwrite existing data.
		if err := r.resumeFromCheckpoint(ctx); err != nil {
			return errors.New("target database is not empty and could not resume from checkpoint")
		}
		r.logger.Infof("Resumed move from existing checkpoint")
	} else {
		return r.newCopy(ctx)
	}
	return nil
}

func (r *Runner) newCopy(ctx context.Context) error {
	// We are starting fresh:
	// For each table, fetch the CREATE TABLE statement from the source and run it on the target.
	if err := r.createTargetTables(); err != nil {
		return err
	}

	// Create a sentinel.
	if r.move.CreateSentinel {
		if err := r.createSentinelTable(ctx); err != nil {
			return err
		}
	}

	if err := r.createCheckpointTable(ctx); err != nil {
		return err
	}

	copyChunkers := make([]table.Chunker, 0, len(r.sourceTables))
	checksumChunkers := make([]table.Chunker, 0, len(r.sourceTables))
	var err error
	// For each table create a chunker and add a subscription to the replication client.
	for _, src := range r.sourceTables {
		dest := table.NewTableInfo(r.target, r.targetConfig.DBName, src.TableName)
		if err := dest.SetInfo(ctx); err != nil {
			return err
		}
		copyChunker, err := table.NewChunker(src, dest, r.move.TargetChunkTime, r.logger)
		if err != nil {
			return err
		}
		if err := r.replClient.AddSubscription(src, dest, copyChunker.KeyAboveHighWatermark); err != nil {
			return err
		}
		checksumChunker, err := table.NewChunker(src, dest, r.move.TargetChunkTime, r.logger)
		if err != nil {
			return err
		}
		copyChunkers = append(copyChunkers, copyChunker)
		checksumChunkers = append(checksumChunkers, checksumChunker)
	}

	// Then create a multi chunker of all chunkers.
	r.copyChunker = table.NewMultiChunker(copyChunkers...)
	r.checksumChunker = table.NewMultiChunker(checksumChunkers...)

	// Create a copier that reads from the multi chunker and writes to the target.
	r.copier, err = copier.NewCopier(r.source, r.copyChunker, &copier.CopierConfig{
		Concurrency:                   r.move.Threads,
		TargetChunkTime:               r.move.TargetChunkTime,
		Logger:                        r.logger,
		Throttler:                     &throttler.Noop{},
		MetricsSink:                   &metrics.NoopSink{},
		DBConfig:                      r.dbConfig,
		UseExperimentalBufferedCopier: true,
		WriteDB:                       r.target,
	})
	if err != nil {
		return err
	}

	// Then open the multi chunker.
	if err := r.copyChunker.Open(); err != nil {
		return err
	}

	// Start the replication client.
	if err := r.replClient.Run(ctx); err != nil {
		return err
	}

	return nil
}

func (r *Runner) createCheckpointTable(ctx context.Context) error {
	// drop checkpoint if we've decided to call this func.
	if err := dbconn.Exec(ctx, r.target, "DROP TABLE IF EXISTS %n.%n", r.targetConfig.DBName, checkpointTableName); err != nil {
		return err
	}
	if err := dbconn.Exec(ctx, r.target, `CREATE TABLE %n.%n (
	id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	copier_watermark TEXT,
	checksum_watermark TEXT,
	binlog_name VARCHAR(255),
	binlog_pos INT,
	statement TEXT
	)`,
		r.targetConfig.DBName, checkpointTableName); err != nil {
		return err
	}
	r.checkpointTable = table.NewTableInfo(r.target, r.targetConfig.DBName, checkpointTableName)
	return nil
}

func (r *Runner) Run(ctx context.Context) error {
	ctx, r.cancelFunc = context.WithCancel(ctx)
	defer r.cancelFunc()
	r.startTime = time.Now()
	r.logger.Infof("Starting table move")

	var err error
	r.dbConfig = dbconn.NewDBConfig()
	r.logger.Warn("the move command is experimental and not yet safe for production use.")
	r.source, err = dbconn.New(r.move.SourceDSN, r.dbConfig)
	if err != nil {
		return err
	}
	defer r.source.Close()
	r.target, err = dbconn.New(r.move.TargetDSN, r.dbConfig)
	if err != nil {
		return err
	}
	defer r.target.Close()

	r.sourceConfig, err = mysql.ParseDSN(r.move.SourceDSN)
	if err != nil {
		return err
	}
	r.targetConfig, err = mysql.ParseDSN(r.move.TargetDSN)
	if err != nil {
		return err
	}

	if err := r.setup(ctx); err != nil {
		return err
	}

	// Start background monitoring routines
	r.startBackgroundRoutines(ctx)

	// Run the copier.
	r.status.Set(status.CopyRows)
	if err := r.copier.Run(ctx); err != nil {
		return err
	}

	// When the copier has finished, catch up the replication client
	// This is in a non-blocking way first.
	if err := r.replClient.Flush(ctx); err != nil {
		return err
	}

	r.logger.Infof("All tables copied successfully.")

	r.sentinelWaitStartTime = time.Now()
	r.status.Set(status.WaitingOnSentinelTable)
	if err := r.waitOnSentinelTable(ctx); err != nil {
		return err
	}

	// Perform a checksum operation + ANALYZE TABLEs
	// To make sure they are all in a ready state.
	if err := r.prepareForCutover(ctx); err != nil {
		return err
	}

	// Create a cutover.
	r.status.Set(status.CutOver)
	cutover, err := NewCutOver(r.source, r.sourceTables, r.cutoverFunc, r.replClient, r.dbConfig, r.logger)
	if err != nil {
		return err
	}
	if err = cutover.Run(ctx); err != nil {
		return err
	}
	r.logger.Infof("Move operation complete.")
	return nil
}

// startBackgroundRoutines starts the background routines needed for monitoring.
// This includes table statistics updates, periodic binlog flushing, and DDL change notifications.
func (r *Runner) startBackgroundRoutines(ctx context.Context) {
	// Start routines in table and replication packages to
	// Continuously update the min/max and estimated rows
	// and to flush the binary log position periodically.
	// These will both be stopped when the copier finishes
	// and checksum starts, although the PeriodicFlush
	// will be restarted again after.
	for _, tbl := range r.sourceTables {
		go tbl.AutoUpdateStatistics(ctx, tableStatUpdateInterval, r.logger)
	}
	go r.replClient.StartPeriodicFlush(ctx, repl.DefaultFlushInterval)
	//go r.tableChangeNotification(ctx)

	//go r.dumpStatus(ctx)                 // start periodically writing status
	go r.dumpCheckpointContinuously(ctx) // start periodically dumping the checkpoint.
}

func (r *Runner) SetLogger(logger loggers.Advanced) {
	r.logger = logger
}

func (r *Runner) prepareForCutover(ctx context.Context) error {
	// Disable the periodic flush and flush all pending events.
	// We want it disabled for ANALYZE TABLE and acquiring a table lock
	// *but* it will be started again briefly inside of the checksum
	// runner to ensure that the lag does not grow too long.
	r.replClient.StopPeriodicFlush()
	r.status.Set(status.ApplyChangeset)
	if err := r.replClient.Flush(ctx); err != nil {
		return err
	}

	// Run ANALYZE TABLE to update the statistics on the new table.
	// This is required so on cutover plans don't go sideways, which
	// is at elevated risk because the batch loading can cause statistics
	// to be out of date.
	r.status.Set(status.AnalyzeTable)
	r.logger.Infof("Running ANALYZE TABLE")
	for _, tbl := range r.sourceTables {
		if err := dbconn.Exec(ctx, r.target, "ANALYZE TABLE %n.%n", tbl.SchemaName, tbl.TableName); err != nil {
			return err
		}
	}

	r.status.Set(status.Checksum)
	if err := r.checksumChunker.Open(); err != nil {
		return err
	}
	defer r.checksumChunker.Close()

	// Perform a checksum operation
	// TODO: we currently don't support repairing in move
	// But if we do, we need to modify the checker so it can re-run on failure.
	var err error
	r.checker, err = checksum.NewChecker(r.source, r.checksumChunker, r.replClient, &checksum.CheckerConfig{
		Concurrency:     r.move.Threads,
		TargetChunkTime: r.move.TargetChunkTime,
		DBConfig:        r.dbConfig,
		Logger:          r.logger,
		WriteDB:         r.target,
	})
	if err != nil {
		return err
	}
	return r.checker.Run(ctx)
}

func (r *Runner) SetCutover(cutover func(ctx context.Context) error) {
	r.cutoverFunc = cutover
}

func (r *Runner) GetProgress() status.Progress {
	var summary string
	switch r.status.Get() { //nolint: exhaustive
	case status.CopyRows:
		summary = fmt.Sprintf("%v %s ETA %v",
			r.copier.GetProgress(),
			r.status.Get().String(),
			r.copier.GetETA(),
		)
	case status.WaitingOnSentinelTable:
		r.logger.Infof("migration status: state=%s sentinel-table=%s.%s total-time=%s sentinel-wait-time=%s sentinel-max-wait-time=%s",
			r.status.Get().String(),
			r.targetConfig.DBName,
			sentinelTableName,
			time.Since(r.startTime).Round(time.Second),
			time.Since(r.sentinelWaitStartTime).Round(time.Second),
			sentinelWaitLimit,
		)
	case status.ApplyChangeset, status.PostChecksum:
		summary = fmt.Sprintf("Applying Changeset Deltas=%v", r.replClient.GetDeltaLen())
	case status.Checksum:
		summary = fmt.Sprintf("%v %s",
			r.checker.GetProgress(),
			r.status.Get().String(),
		)
	}
	return status.Progress{
		CurrentState: r.status.Get().String(),
		Summary:      summary,
	}
}

func (r *Runner) createSentinelTable(ctx context.Context) error {
	if err := dbconn.Exec(ctx, r.target, "DROP TABLE IF EXISTS %n.%n", r.targetConfig.DBName, sentinelTableName); err != nil {
		return err
	}
	if err := dbconn.Exec(ctx, r.target, "CREATE TABLE %n.%n (id int NOT NULL PRIMARY KEY)", r.targetConfig.DBName, sentinelTableName); err != nil {
		return err
	}
	return nil
}

func (r *Runner) sentinelTableExists(ctx context.Context) (bool, error) {
	sql := "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
	var sentinelTableExists int
	err := r.target.QueryRowContext(ctx, sql, r.targetConfig.DBName, sentinelTableName).Scan(&sentinelTableExists)
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

// dumpCheckpoint is called approximately every minute.
// It writes the current state of the migration to the checkpoint table,
// which can be used in recovery. Previously resuming from checkpoint
// would always restart at the copier, but it can now also resume at
// the checksum phase.
func (r *Runner) dumpCheckpoint(ctx context.Context) error {
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
		if r.checker != nil {
			checksumWatermark, err = r.checksumChunker.GetLowWatermark()
			if err != nil {
				return status.ErrWatermarkNotReady
			}
		}
	}
	// Note: when we dump the lowWatermark to the log, we are exposing the PK values,
	// when using the composite chunker are based on actual user-data.
	// We believe this is OK but may change it in the future. Please do not
	// add any other fields to this log line.
	r.logger.Infof("checkpoint: low-watermark=%s log-file=%s log-pos=%d", copierWatermark, binlog.Name, binlog.Pos)
	err = dbconn.Exec(ctx, r.target, "INSERT INTO %n.%n (copier_watermark, checksum_watermark, binlog_name, binlog_pos, statement) VALUES (%?, %?, %?, %?, %?)",
		r.checkpointTable.SchemaName,
		r.checkpointTable.TableName,
		copierWatermark,
		checksumWatermark,
		binlog.Name,
		binlog.Pos,
		"",
	)
	if err != nil {
		return status.ErrCouldNotWriteCheckpoint
	}
	return nil
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
			if r.status.Get() >= status.CutOver {
				return
			}
			if err := r.dumpCheckpoint(ctx); err != nil {
				if errors.Is(err, status.ErrWatermarkNotReady) {
					// This is non fatal, we can try again later.
					r.logger.Warnf("could not write checkpoint yet, watermark not ready")
					continue
				}
				// Other errors such as not being able to write to the checkpoint
				// table are considered fatal. This is because if we can't record
				// our progress, we don't want to continue doing work.
				// We could get 10 days into a migration, and then fail, and then
				// discover this. It's better to fast fail now.
				r.logger.Errorf("error writing checkpoint: %v", err)
				r.cancelFunc()
				return
			}
		}
	}
}
