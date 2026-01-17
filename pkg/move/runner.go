package move

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/checksum"
	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/repl"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/throttler"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-sql-driver/mysql"
	"golang.org/x/sync/errgroup"
)

var (
	sentinelCheckInterval   = 1 * time.Second
	tableStatUpdateInterval = 5 * time.Minute
	sentinelWaitLimit       = 48 * time.Hour
	sentinelTableName       = "_spirit_sentinel" // this is now a const.
	checkpointTableName     = "_spirit_checkpoint"
)

type Runner struct {
	move            *Move
	source          *sql.DB
	sourceConfig    *mysql.Config
	targets         []applier.Target // Combined DB, Config, and KeyRange
	status          status.State     // must use atomic to get/set
	checkpointTable *table.TableInfo

	sourceTables   []*table.TableInfo
	sourceTableMap map[string]bool // used when only some tables are to be moved.

	applier           applier.Applier
	replClient        *repl.Client
	copyChunker       table.Chunker
	checksumChunker   table.Chunker
	copier            copier.Copier
	checker           checksum.Checker
	checksumWatermark string

	// Track some key statistics.
	startTime                time.Time
	sentinelWaitStartTime    time.Time
	usedResumeFromCheckpoint bool

	cutoverFunc func(ctx context.Context) error

	logger     *slog.Logger
	cancelFunc context.CancelFunc
	dbConfig   *dbconn.DBConfig
}

var _ status.Task = (*Runner)(nil)

func NewRunner(m *Move) (*Runner, error) {
	r := &Runner{
		move:   m,
		logger: slog.Default(),
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
	for _, target := range r.targets {
		target.DB.Close()
	}
	return nil
}

// getTables connects to a DB and fetches the list of tables
// it can be run on either the source or the target.
// If SourceTables is specified in the Move config, only those tables will be returned.
func (r *Runner) getTables(ctx context.Context, db *sql.DB) ([]*table.TableInfo, error) {
	rows, err := db.QueryContext(ctx, "SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Build a map of source tables if filtering is requested
	if len(r.move.SourceTables) > 0 {
		r.sourceTableMap = make(map[string]bool, len(r.move.SourceTables))
		for _, tbl := range r.move.SourceTables {
			r.sourceTableMap[tbl] = true
		}
	}

	var tableName string
	tables := make([]*table.TableInfo, 0)
	for rows.Next() {
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		if tableName == checkpointTableName || tableName == sentinelTableName {
			continue // Skip if the table name is the checkpoint or sentinel table
		}

		// If SourceTables is specified, only include tables in that list
		if r.sourceTableMap != nil && !r.sourceTableMap[tableName] {
			continue
		}

		tableInfo := table.NewTableInfo(r.source, r.sourceConfig.DBName, tableName)
		if err := tableInfo.SetInfo(ctx); err != nil {
			return nil, err
		}

		// If a ShardingProvider is configured, get sharding metadata for this table.
		// This is used for resharding operations where rows need to be distributed
		// across multiple target shards based on a sharding key.
		if r.move.ShardingProvider != nil {
			shardingColumn, hashFunc, err := r.move.ShardingProvider.GetShardingMetadata(r.sourceConfig.DBName, tableName)
			if err != nil {
				return nil, fmt.Errorf("failed to get sharding metadata for table %s: %w", tableName, err)
			}
			// Only set if sharding metadata is available (could be empty for some tables)
			if shardingColumn != "" && hashFunc != nil {
				tableInfo.ShardingColumn = shardingColumn
				tableInfo.HashFunc = hashFunc
				r.logger.Info("configured sharding for table",
					"table", tableName,
					"shardingColumn", shardingColumn)
			}
		}
		tables = append(tables, tableInfo)
	}

	// Validate that all source tables were found. We can do this
	// by just comparing the lengths of the r.move.SourceTables to tables
	// this should have been pre-validated by the caller.
	if r.sourceTableMap != nil && len(tables) != len(r.move.SourceTables) {
		return nil, errors.New("could not find all SourceTables in the source database")
	}
	return tables, rows.Err()
}

// checkTargetEmpty checks that the target database is ready for the move operation.
// If SourceTables is specified, it checks that those specific tables don't exist in the target,
// OR if they do exist, they must be empty (zero rows) and have matching schema to the source.
// If SourceTables is not specified, it checks that the target database is completely empty.
func (r *Runner) checkTargetEmpty(ctx context.Context) error {
	for i, target := range r.targets {
		rows, err := target.DB.QueryContext(ctx, "SHOW TABLES")
		if err != nil {
			return fmt.Errorf("failed to check target %d: %w", i, err)
		}
		defer rows.Close()
		var tableName string
		var existingTables []string
		for rows.Next() {
			if err := rows.Scan(&tableName); err != nil {
				return fmt.Errorf("failed to scan table name on target %d: %w", i, err)
			}
			if r.sourceTableMap != nil && !r.sourceTableMap[tableName] {
				// We are only copying specific tables, but this table
				// found is not one of them, so we can ignore and continue.
				continue
			}
			existingTables = append(existingTables, tableName)
		}
		if err := rows.Err(); err != nil {
			return err
		}

		// If there are existing tables, validate they are empty and have matching schema
		if len(existingTables) > 0 {
			for _, tableName := range existingTables {
				if err := r.validateExistingTargetTable(ctx, target, tableName, i); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// validateExistingTargetTable checks that an existing table in the target database
// is empty (zero rows) and has a schema that matches the source table.
// This allows move-tables to work with pre-created tables, which is necessary
// for declarative schema management workflows where tables must be tracked
// in the migrations directory before data is moved.
func (r *Runner) validateExistingTargetTable(ctx context.Context, target applier.Target, tableName string, targetIndex int) error {
	// Check 1: Table must have zero rows
	var rowCount int64
	err := target.DB.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`", target.Config.DBName, tableName)).Scan(&rowCount)
	if err != nil {
		return fmt.Errorf("failed to check row count for table '%s' on target %d: %w", tableName, targetIndex, err)
	}
	if rowCount > 0 {
		return fmt.Errorf("table '%s' already exists on target %d (%s) and contains %d rows; move-tables requires target tables to be empty to prevent data loss. Please drop the table or use a different target",
			tableName, targetIndex, target.Config.DBName, rowCount)
	}

	// Check 2: Schema must match source exactly
	// Find the source table info
	var sourceTable *table.TableInfo
	for _, src := range r.sourceTables {
		if src.TableName == tableName {
			sourceTable = src
			break
		}
	}
	if sourceTable == nil {
		return nil // exists on target only is fine.
	}

	// Get target table info and compare columns
	targetTable := table.NewTableInfo(target.DB, target.Config.DBName, tableName)
	if err := targetTable.SetInfo(ctx); err != nil {
		return fmt.Errorf("failed to get table info for target %d table '%s': %w", targetIndex, tableName, err)
	}

	if !slices.Equal(sourceTable.Columns, targetTable.Columns) {
		return fmt.Errorf("table '%s' exists on target %d (%s) but schema does not match source; column mismatch detected. Please ensure the table schema matches exactly or drop the table",
			tableName, targetIndex, target.Config.DBName)
	}

	r.logger.Info("validated existing target table",
		"table", tableName,
		"target", targetIndex,
		"database", target.Config.DBName,
		"rows", rowCount,
		"columns", len(targetTable.Columns))

	return nil
}

// createTargetTables creates tables on all targets.
// If DeferSecondaryIndexes is enabled, tables are created without secondary indexes.
// Secondary indexes will be added later by restoreSecondaryIndexes() before cutover.
// This function skips tables that already exist (they were validated by checkTargetEmpty).
func (r *Runner) createTargetTables(ctx context.Context) error {
	for _, t := range r.sourceTables {
		var createStmt string
		row := r.source.QueryRowContext(ctx, fmt.Sprintf("SHOW CREATE TABLE `%s`", t.TableName))
		var tbl string
		if err := row.Scan(&tbl, &createStmt); err != nil {
			return err
		}

		// If DeferSecondaryIndexes is enabled, remove secondary indexes from CREATE TABLE
		// We don't have to track what these indexes were: we'll just recreate them later
		// from the source table.
		if r.move.DeferSecondaryIndexes {
			var err error
			createStmt, err = statement.RemoveSecondaryIndexes(createStmt)
			if err != nil {
				return fmt.Errorf("failed to remove secondary indexes from CREATE TABLE for %s: %w", t.TableName, err)
			}
		}

		// Execute the create statement on all targets.
		for i, target := range r.targets {
			// Check if table already exists
			var tableExists int
			err := target.DB.QueryRowContext(ctx,
				"SELECT 1 FROM information_schema.TABLES WHERE table_schema = ? AND table_name = ?",
				target.Config.DBName, t.TableName).Scan(&tableExists)

			if err == nil {
				// Table already exists, skip creation (it was validated by checkTargetEmpty)
				r.logger.Info("skipping table creation, already exists",
					"table", t.TableName,
					"target", i,
					"database", target.Config.DBName)
				continue
			} else if err != sql.ErrNoRows {
				// Unexpected error
				return fmt.Errorf("failed to check if table exists on target %d: %w", i, err)
			}

			// Table doesn't exist, create it
			if _, err := target.DB.ExecContext(ctx, createStmt); err != nil {
				return fmt.Errorf("failed to create table on target %d: %w", i, err)
			}
			r.logger.Info("created table on target",
				"table", t.TableName,
				"target", i,
				"database", target.Config.DBName,
				"deferred_indexes", r.move.DeferSecondaryIndexes,
			)
		}
	}
	return nil
}

func (r *Runner) resumeFromCheckpoint(ctx context.Context) error {
	copyChunkers := make([]table.Chunker, 0, len(r.sourceTables))
	checksumChunkers := make([]table.Chunker, 0, len(r.sourceTables))
	var err error

	// For each table and each target, create a chunker and add a subscription
	// The destination is nil because this is used for table structure checking, which is unused in move
	// We also have the problem that the dest could be multiple destinations (sharded) which makes it
	// ambiguous.
	for _, src := range r.sourceTables {
		copyChunker, err := table.NewChunker(src, nil, r.move.TargetChunkTime, r.logger)
		if err != nil {
			return err
		}
		if err := r.replClient.AddSubscription(src, nil, copyChunker); err != nil {
			return err
		}
		checksumChunker, err := table.NewChunker(src, nil, r.move.TargetChunkTime, r.logger)
		if err != nil {
			return err
		}
		copyChunkers = append(copyChunkers, copyChunker)
		checksumChunkers = append(checksumChunkers, checksumChunker)

		// Perform an exhaustive check to ensure that the columns
		// match between source and target for all tables, on all targets.
		for i, target := range r.targets {
			targetTable := table.NewTableInfo(target.DB, target.Config.DBName, src.TableName)
			if err := targetTable.SetInfo(ctx); err != nil {
				return fmt.Errorf("failed to get table info for target %d table %s: %w", i, src.TableName, err)
			}
			if !slices.Equal(src.Columns, targetTable.Columns) {
				return fmt.Errorf("source and target table structures do not match for table '%s' on target %d", src.TableName, i)
			}
		}
	}

	// Then create a multi chunker of all chunkers.
	r.copyChunker = table.NewMultiChunker(copyChunkers...)
	r.checksumChunker = table.NewMultiChunker(checksumChunkers...)

	// Create a copier that reads from the multi chunker and uses the shared applier.
	r.copier, err = copier.NewCopier(r.source, r.copyChunker, &copier.CopierConfig{
		Concurrency:                   r.move.Threads,
		TargetChunkTime:               r.move.TargetChunkTime,
		Logger:                        r.logger,
		Throttler:                     &throttler.Noop{},
		MetricsSink:                   &metrics.NoopSink{},
		DBConfig:                      r.dbConfig,
		UseExperimentalBufferedCopier: true,
		Applier:                       r.applier, // Use the shared applier
	})
	if err != nil {
		return err
	}

	// We explicitly specify the columns we need from the checkpoint table.
	// If the structure changes, this will fail and indicate that the checkpoint
	// was created by either an earlier or later version of spirit, in which case
	// we do not support recovery.
	query := fmt.Sprintf("SELECT id, copier_watermark, checksum_watermark, binlog_name, binlog_pos, statement FROM `%s`.`%s` ORDER BY id DESC LIMIT 1",
		r.sourceConfig.DBName, checkpointTableName)
	var copierWatermark, binlogName, statement string
	var id, binlogPos int
	err = r.source.QueryRowContext(ctx, query).Scan(&id, &copierWatermark, &r.checksumWatermark, &binlogName, &binlogPos, &statement)
	if err != nil {
		return fmt.Errorf("could not read from checkpoint table '%s' on source: %v", checkpointTableName, err)
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

	r.checkpointTable = table.NewTableInfo(r.source, r.sourceConfig.DBName, checkpointTableName)
	r.usedResumeFromCheckpoint = true
	return nil
}

func (r *Runner) setup(ctx context.Context) error {
	var err error
	// Fetch a list of tables from the source.
	r.logger.Info("Fetching source table list")
	if r.sourceTables, err = r.getTables(ctx, r.source); err != nil {
		return err
	}

	if len(r.sourceTables) == 0 {
		r.logger.Info("No tables found in source database; nothing to move")
		return nil
	}

	// Create a single applier instance that will be shared by both
	// the replication client and the copier
	r.logger.Info("Creating shared applier")
	r.applier, err = r.createApplier()
	if err != nil {
		return err
	}

	r.logger.Info("Setting up repl client")
	r.replClient = repl.NewClient(r.source, r.source, r.sourceConfig.Addr, r.sourceConfig.User, r.sourceConfig.Passwd, &repl.ClientConfig{
		Logger:                     r.logger,
		Concurrency:                r.move.Threads,
		TargetBatchTime:            r.move.TargetChunkTime,
		ServerID:                   repl.NewServerID(),
		UseExperimentalBufferedMap: true,
		Applier:                    r.applier, // Use the shared applier
		DBConfig:                   r.dbConfig,
	})

	r.logger.Info("Checking target database state")

	err = r.checkTargetEmpty(ctx)
	if err != nil {
		// checkTargetEmpty returns an error if:
		// 1. Tables exist that don't match our criteria (non-empty or schema mismatch)
		// 2. There are tables we can't validate
		// In either case, try to resume from checkpoint as a fallback.
		// The checkpoint is on the source (not target) because reshards are 1:N and the
		// source is always guaranteed to be singular.
		if err := r.resumeFromCheckpoint(ctx); err != nil {
			return fmt.Errorf("target database validation failed and could not resume from checkpoint: %v", err)
		}
		r.logger.Info("Resumed move from existing checkpoint")
		return nil
	}

	// Target is ready: it is either empty or has valid
	// pre-existing empty tables with matching schema
	return r.newCopy(ctx)
}

func (r *Runner) newCopy(ctx context.Context) error {
	// We are starting fresh:
	// For each table, fetch the CREATE TABLE statement from the source and run it on the target.
	if err := r.createTargetTables(ctx); err != nil {
		return err
	}

	// Create sentinel on SOURCE
	if r.move.CreateSentinel {
		if err := r.createSentinelTable(ctx); err != nil {
			return err
		}
	}

	// Create checkpoint on SOURCE
	if err := r.createCheckpointTable(ctx); err != nil {
		return err
	}

	copyChunkers := make([]table.Chunker, 0, len(r.sourceTables))
	checksumChunkers := make([]table.Chunker, 0, len(r.sourceTables))

	// For each table and each target, create a chunker and add a subscription
	// The destination is nil because this is used for table structure checking, which is unused in move
	// We also have the problem that the dest could be multiple destinations (sharded) which makes it
	// ambiguous.
	for _, src := range r.sourceTables {
		copyChunker, err := table.NewChunker(src, nil, r.move.TargetChunkTime, r.logger)
		if err != nil {
			return err
		}
		if err := r.replClient.AddSubscription(src, nil, copyChunker); err != nil {
			return err
		}
		checksumChunker, err := table.NewChunker(src, nil, r.move.TargetChunkTime, r.logger)
		if err != nil {
			return err
		}
		copyChunkers = append(copyChunkers, copyChunker)
		checksumChunkers = append(checksumChunkers, checksumChunker)
	}

	// Then create a multi chunker of all chunkers.
	r.copyChunker = table.NewMultiChunker(copyChunkers...)
	r.checksumChunker = table.NewMultiChunker(checksumChunkers...)

	// Create a copier that reads from the multi chunker and uses the shared applier.
	var err error
	r.copier, err = copier.NewCopier(r.source, r.copyChunker, &copier.CopierConfig{
		Concurrency:                   r.move.Threads,
		TargetChunkTime:               r.move.TargetChunkTime,
		Logger:                        r.logger,
		Throttler:                     &throttler.Noop{},
		MetricsSink:                   &metrics.NoopSink{},
		DBConfig:                      r.dbConfig,
		UseExperimentalBufferedCopier: true,
		Applier:                       r.applier, // Use the shared applier
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

// createCheckpointTable creates checkpoint table on SOURCE (not target).
func (r *Runner) createCheckpointTable(ctx context.Context) error {
	// drop checkpoint if we've decided to call this func.
	if err := dbconn.Exec(ctx, r.source, "DROP TABLE IF EXISTS %n.%n", r.sourceConfig.DBName, checkpointTableName); err != nil {
		return err
	}
	if err := dbconn.Exec(ctx, r.source, `CREATE TABLE %n.%n (
	id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	copier_watermark TEXT,
	checksum_watermark TEXT,
	binlog_name VARCHAR(255),
	binlog_pos INT,
	statement TEXT
	)`,
		r.sourceConfig.DBName, checkpointTableName); err != nil {
		return err
	}
	r.checkpointTable = table.NewTableInfo(r.source, r.sourceConfig.DBName, checkpointTableName)
	return nil
}

func (r *Runner) Run(ctx context.Context) error {
	ctx, r.cancelFunc = context.WithCancel(ctx)
	defer r.cancelFunc()
	r.startTime = time.Now()
	r.logger.Info("Starting table move")

	var err error
	r.dbConfig = dbconn.NewDBConfig()
	r.dbConfig.ForceKill = true // in move we always use force kill; it's new code.
	// Buffered copier needs more connections due to parallel read/write workers
	r.dbConfig.MaxOpenConnections = r.move.Threads + r.move.WriteThreads + 2
	r.logger.Warn("the move command is experimental and not yet safe for production use.")
	r.source, err = dbconn.New(r.move.SourceDSN, r.dbConfig)
	if err != nil {
		return err
	}
	defer r.source.Close()

	r.sourceConfig, err = mysql.ParseDSN(r.move.SourceDSN)
	if err != nil {
		return err
	}

	// If targets are already configured (e.g., for resharding), use them.
	// Otherwise, create a single target from TargetDSN (for simple 1:1 moves).
	if len(r.move.Targets) > 0 {
		r.targets = r.move.Targets
		r.logger.Info("Using pre-configured targets", "count", len(r.targets))
	} else {
		db, err := dbconn.New(r.move.TargetDSN, r.dbConfig)
		if err != nil {
			return err
		}
		targetConfig, err := mysql.ParseDSN(r.move.TargetDSN)
		if err != nil {
			return err
		}
		r.targets = []applier.Target{{
			KeyRange: "0",
			DB:       db,
			Config:   targetConfig,
		}}
		r.logger.Info("Created single target from TargetDSN")
	}
	if err := r.setup(ctx); err != nil {
		return err
	}

	if len(r.sourceTables) == 0 {
		// Because this is called from orchestration, there might be a bug where
		// it is asked to move *no tables*. Since there are no tables,
		// there is no:
		// - copier, replication changes
		// - metadata lock
		// - cutover step
		//
		// But the caller will still want their cutoverFunc called. So we do that
		// and then exit.
		r.logger.Info("No tables to copy, proceeding directly to cutover")
		r.status.Set(status.CutOver)
		if r.cutoverFunc != nil {
			if err := r.cutoverFunc(ctx); err != nil {
				return err
			}
		}
		r.logger.Info("Move operation complete.")
		return nil
	}

	// Take a single metadata lock for all tables to prevent concurrent DDL.
	// This uses a single DB connection instead of one per table.
	// We release the lock when this function finishes executing.
	lock, err := dbconn.NewMetadataLock(ctx, r.move.SourceDSN, r.sourceTables, r.dbConfig, r.logger)
	if err != nil {
		return err
	}

	// Release the lock
	defer func() {
		if err := lock.Close(); err != nil {
			r.logger.Error("failed to release metadata lock", "error", err)
		}
	}()

	// Start background monitoring routines
	r.startBackgroundRoutines(ctx)

	r.replClient.SetWatermarkOptimization(true)

	// Run the copier.
	r.status.Set(status.CopyRows)
	if err := r.copier.Run(ctx); err != nil {
		return err
	}

	// Disable both watermark optimizations so that all changes can be flushed.
	// The watermark optimizations can prevent some keys from being flushed,
	// which would cause flushedPos to not advance, leading to a mismatch
	// with bufferedPos and causing AllChangesFlushed() to return false.
	r.replClient.SetWatermarkOptimization(false)

	// When the copier has finished, catch up the replication client
	// This is in a non-blocking way first.
	if err := r.replClient.Flush(ctx); err != nil {
		return err
	}

	r.logger.Info("All tables copied successfully.")

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

	r.logger.Info("Checksum completed successfully, starting cutover")
	// Create a cutover.
	r.status.Set(status.CutOver)
	cutover, err := NewCutOver(r.source, r.sourceTables, r.cutoverFunc, r.replClient, r.dbConfig, r.logger)
	if err != nil {
		return err
	}
	if err = cutover.Run(ctx); err != nil {
		return err
	}
	// Delete checkpoint table
	if err := dbconn.Exec(ctx, r.source, "DROP TABLE IF EXISTS %n.%n", r.sourceConfig.DBName, checkpointTableName); err != nil {
		return err
	}
	r.logger.Info("Move operation complete.")
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

	// Start go routines for checkpointing and dumping status
	status.WatchTask(ctx, r, r.logger)
}

func (r *Runner) Status() string {
	state := r.status.Get()
	if state > status.CutOver {
		return ""
	}
	switch state {
	case status.CopyRows:
		// Status for copy rows
		return fmt.Sprintf("migration status: state=%s copy-progress=%s binlog-deltas=%v total-time=%s copier-time=%s copier-remaining-time=%v copier-is-throttled=%v",
			r.status.Get().String(),
			r.copier.GetProgress(),
			r.replClient.GetDeltaLen(),
			time.Since(r.startTime).Round(time.Second),
			time.Since(r.copier.StartTime()).Round(time.Second),
			r.copier.GetETA(),
			r.copier.GetThrottler().IsThrottled(),
		)
	case status.WaitingOnSentinelTable:
		return fmt.Sprintf("migration status: state=%s total-time=%s sentinel-wait-time=%s sentinel-max-wait-time=%s",
			r.status.Get().String(),
			time.Since(r.startTime).Round(time.Second),
			time.Since(r.sentinelWaitStartTime).Round(time.Second),
			sentinelWaitLimit,
		)
	case status.ApplyChangeset, status.PostChecksum:
		// We've finished copying rows, and we are now trying to reduce the number of binlog deltas before
		// proceeding to the checksum and then the final cutover.
		return fmt.Sprintf("migration status: state=%s binlog-deltas=%v total-time=%s",
			r.status.Get().String(),
			r.replClient.GetDeltaLen(),
			time.Since(r.startTime).Round(time.Second),
		)
	case status.Checksum:
		// This could take a while if it's a large table.
		return fmt.Sprintf("migration status: state=%s checksum-progress=%s binlog-deltas=%v total-time=%s checksum-time=%s",
			r.status.Get().String(),
			r.checker.GetProgress(),
			r.replClient.GetDeltaLen(),
			time.Since(r.startTime).Round(time.Second),
			time.Since(r.checker.StartTime()).Round(time.Second),
		)
	case status.RestoreSecondaryIndexes:
		return fmt.Sprintf("migration status: state=%s total-time=%s",
			r.status.Get().String(),
			time.Since(r.startTime).Round(time.Second),
		)
	default:
		return ""
	}
}

func (r *Runner) SetLogger(logger *slog.Logger) {
	r.logger = logger
}

// restoreSecondaryIndexes restores any secondary indexes that were deferred during table creation.
// This function is always called (regardless of DeferSecondaryIndexes flag) to handle
// checkpoint resume scenarios. It uses statement.GetMissingSecondaryIndexes to compare source and target
// schemas and generate a single ALTER TABLE statement for all missing indexes per table.
// Targets are processed in parallel, grouped by hostname to avoid overloading any single MySQL instance.
func (r *Runner) restoreSecondaryIndexes(ctx context.Context) error {
	r.logger.Info("Checking for deferred secondary indexes to restore")

	// Group targets by hostname to enable parallel processing across different hosts
	// while avoiding overloading any single MySQL instance
	hostGroups := make(map[string][]int) // hostname -> []targetIdx
	for idx, target := range r.targets {
		host := target.Config.Addr // e.g., "host:3306"
		hostGroups[host] = append(hostGroups[host], idx)
	}

	r.logger.Info("Parallelizing index restoration across hosts",
		"hostCount", len(hostGroups),
		"targetCount", len(r.targets))

	// Process each host group in parallel using errgroup
	g, gctx := errgroup.WithContext(ctx)
	for host, targetIndices := range hostGroups {
		// Shadow loop variables to avoid closure capture issues.
		host, targetIndices := host, targetIndices //nolint: copyloopvar, modernize
		g.Go(func() error {
			return r.restoreIndexesForTargets(gctx, host, targetIndices)
		})
	}

	// Wait for all host groups to complete
	if err := g.Wait(); err != nil {
		return err
	}
	r.logger.Info("Completed restoring all secondary indexes")
	return nil
}

// restoreIndexesForTargets restores secondary indexes for a group of targets on the same host.
// This is called as part of parallel processing in restoreSecondaryIndexes.
func (r *Runner) restoreIndexesForTargets(ctx context.Context, host string, targetIndices []int) error {
	r.logger.Debug("Starting index restoration for host",
		"host", host,
		"targetCount", len(targetIndices))
	// For each source table, compare with targets on this host and restore missing indexes
	for _, tbl := range r.sourceTables {
		// Get CREATE TABLE statement from source
		var sourceCreateStmt string
		row := r.source.QueryRowContext(ctx, fmt.Sprintf("SHOW CREATE TABLE `%s`", tbl.TableName))
		var tableName string
		if err := row.Scan(&tableName, &sourceCreateStmt); err != nil {
			return fmt.Errorf("failed to get CREATE TABLE for source %s: %w", tbl.TableName, err)
		}

		// Process each target on this host sequentially
		// We re-evaluate what is missing per-target, because in resume scenarios we could have a failure
		// in this restore function. We need to be able to pick up where we left off,
		// which includes that some work may already be complete.
		for _, targetIdx := range targetIndices {
			target := r.targets[targetIdx]

			// Get CREATE TABLE statement from target
			var targetCreateStmt, targetTableName string
			targetRow := target.DB.QueryRowContext(ctx, fmt.Sprintf("SHOW CREATE TABLE `%s`", tbl.TableName))
			if err := targetRow.Scan(&targetTableName, &targetCreateStmt); err != nil {
				return fmt.Errorf("failed to get CREATE TABLE for target %d table %s: %w", targetIdx, tbl.TableName, err)
			}

			// Compare and get ALTER TABLE statement for missing indexes
			alterStmt, err := statement.GetMissingSecondaryIndexes(sourceCreateStmt, targetCreateStmt, tbl.TableName)
			if err != nil {
				return fmt.Errorf("failed to compare indexes for table %s (target %d): %w", tbl.TableName, targetIdx, err)
			}

			// If no missing indexes, skip this target
			// this is going to be typical for most cases.
			if alterStmt == "" {
				r.logger.Debug("no missing secondary indexes",
					"table", tbl.TableName,
					"target", targetIdx,
					"database", target.Config.DBName,
					"host", host)
				continue
			}

			r.logger.Info("restoring secondary indexes",
				"table", tbl.TableName,
				"target", targetIdx,
				"database", target.Config.DBName,
				"host", host,
				"stmt", alterStmt)

			// Execute the ALTER TABLE statement to add all missing indexes at once
			if _, err := target.DB.ExecContext(ctx, alterStmt); err != nil {
				return fmt.Errorf("failed to restore indexes on target %d (host %s): %w", targetIdx, host, err)
			}
		}
	}
	r.logger.Debug("Completed index restoration for host",
		"host", host,
		"targetCount", len(targetIndices))
	return nil
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

	// Restore secondary indexes if they were deferred during table creation.
	// This is always called (not conditional on DeferSecondaryIndexes) to handle
	// checkpoint resume scenarios where indexes may have been deferred in a previous run.
	r.status.Set(status.RestoreSecondaryIndexes)
	if err := r.restoreSecondaryIndexes(ctx); err != nil {
		return err
	}

	// Run ANALYZE TABLE to update the statistics on the new table.
	// This is required so on cutover plans don't go sideways, which
	// is at elevated risk because the batch loading can cause statistics
	// to be out of date.
	r.status.Set(status.AnalyzeTable)
	r.logger.Info("Running ANALYZE TABLE")
	for _, target := range r.targets {
		for _, tbl := range r.sourceTables {
			if err := dbconn.Exec(ctx, target.DB, "ANALYZE TABLE %n.%n", tbl.SchemaName, tbl.TableName); err != nil {
				return err
			}
		}
	}

	if err := r.checksumChunker.Open(); err != nil {
		return err
	}
	defer r.checksumChunker.Close()

	// Perform a checksum operation
	var err error
	r.checker, err = checksum.NewChecker(r.source, r.checksumChunker, r.replClient, &checksum.CheckerConfig{
		Concurrency:     r.move.Threads,
		TargetChunkTime: r.move.TargetChunkTime,
		DBConfig:        r.dbConfig,
		Logger:          r.logger,
		Applier:         r.applier,
		FixDifferences:  true,
	})
	if err != nil {
		return err
	}
	r.status.Set(status.Checksum)
	return r.checker.Run(ctx)
}

func (r *Runner) SetCutover(cutover func(ctx context.Context) error) {
	r.cutoverFunc = cutover
}

func (r *Runner) Progress() status.Progress {
	var summary string
	switch r.status.Get() {
	case status.CopyRows:
		summary = fmt.Sprintf("%v %s ETA %v",
			r.copier.GetProgress(),
			r.status.Get().String(),
			r.copier.GetETA(),
		)
	case status.WaitingOnSentinelTable:
		r.logger.Info("migration status",
			"state", r.status.Get().String(),
			"sentinel-table", fmt.Sprintf("%s.%s", r.sourceConfig.DBName, sentinelTableName),
			"total-time", time.Since(r.startTime).Round(time.Second),
			"sentinel-wait-time", time.Since(r.sentinelWaitStartTime).Round(time.Second),
			"sentinel-max-wait-time", sentinelWaitLimit,
		)
	case status.ApplyChangeset, status.PostChecksum:
		summary = fmt.Sprintf("Applying Changeset Deltas=%v", r.replClient.GetDeltaLen())
	case status.Checksum:
		summary = "Checksum Progress=" + r.checker.GetProgress()
	default:
		summary = ""
	}
	return status.Progress{
		CurrentState: r.status.Get(),
		Summary:      summary,
	}
}

// createSentinelTable creates sentinel table on SOURCE (not target).
func (r *Runner) createSentinelTable(ctx context.Context) error {
	if err := dbconn.Exec(ctx, r.source, "DROP TABLE IF EXISTS %n.%n", r.sourceConfig.DBName, sentinelTableName); err != nil {
		return err
	}
	if err := dbconn.Exec(ctx, r.source, "CREATE TABLE %n.%n (id int NOT NULL PRIMARY KEY)", r.sourceConfig.DBName, sentinelTableName); err != nil {
		return err
	}
	return nil
}

// sentinelTableExists checks if sentinel table exists on SOURCE (not target).
func (r *Runner) sentinelTableExists(ctx context.Context) (bool, error) {
	sql := "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
	var sentinelTableExists int
	err := r.source.QueryRowContext(ctx, sql, r.sourceConfig.DBName, sentinelTableName).Scan(&sentinelTableExists)
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
		"wait-limit", sentinelWaitLimit)

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
				r.logger.Info("sentinel table dropped", "time", t)
				return nil
			}
		case <-timer.C:
			return errors.New("timed out waiting for sentinel table to be dropped")
		}
	}
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
	r.logger.Info("checkpoint",
		"low-watermark", copierWatermark,
		"log-file", binlog.Name,
		"log-pos", binlog.Pos)
	err = dbconn.Exec(ctx, r.source, "INSERT INTO %n.%n (copier_watermark, checksum_watermark, binlog_name, binlog_pos, statement) VALUES (%?, %?, %?, %?, %?)",
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

func (r *Runner) Cancel() {
	r.cancelFunc()
}

// createApplier creates the appropriate applier based on the number of targets.
// Note: The applier is NOT started here. The copier will start it when it begins copying.
func (r *Runner) createApplier() (applier.Applier, error) {
	if len(r.targets) == 1 && r.targets[0].KeyRange == "0" {
		// Single target - use SingleTargetApplier
		appl, err := applier.NewSingleTargetApplier(r.targets[0], &applier.ApplierConfig{
			DBConfig: r.dbConfig,
			Logger:   r.logger,
			Threads:  r.move.WriteThreads,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create SingleTargetApplier: %w", err)
		}
		r.logger.Info("Created SingleTargetApplier")
		return appl, nil
	}

	// Multiple targets - use ShardedApplier
	r.logger.Info("Creating ShardedApplier", "targetCount", len(r.targets))

	// Create the ShardedApplier
	appl, err := applier.NewShardedApplier(
		r.targets,
		&applier.ApplierConfig{
			DBConfig: r.dbConfig,
			Logger:   r.logger,
			Threads:  r.move.WriteThreads,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create ShardedApplier: %w", err)
	}
	r.logger.Info("ShardedApplier created successfully")
	return appl, nil
}
