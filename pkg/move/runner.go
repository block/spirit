package move

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/buildinfo"
	"github.com/block/spirit/pkg/checksum"
	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/move/check"
	"github.com/block/spirit/pkg/repl"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/throttler"
	"github.com/block/spirit/pkg/utils"
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

// sourceInfo holds per-source connection state for N:M moves.
type sourceInfo struct {
	db         *sql.DB
	config     *mysql.Config
	dsn        string
	replClient *repl.Client
	tables     []*table.TableInfo // this source's TableInfo objects (bound to this source's db)
}

// sourceKey returns a stable identifier for a source, used for checkpoint
// map keys and deterministic ordering. It is based on the network address
// and database name only, so it remains stable across credential rotations
// or DSN parameter reordering.
func (s *sourceInfo) sourceKey() string {
	return s.config.Addr + "/" + s.config.DBName
}

// binlogPosition is used for JSON serialization of per-source binlog positions in checkpoints.
type binlogPosition struct {
	Name string `json:"name"`
	Pos  uint32 `json:"pos"`
}

type Runner struct {
	move            *Move
	sources         []sourceInfo     // one per source database
	targets         []applier.Target // Combined DB, Config, and KeyRange
	status          status.State     // must use atomic to get/set
	checkpointTable *table.TableInfo

	sourceTables   []*table.TableInfo // canonical table list (from sources[0])
	sourceTableMap map[string]bool    // used when only some tables are to be moved.

	applier           applier.Applier
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

	// watchTaskWait blocks until the WatchTask goroutines have exited.
	// Set in startBackgroundRoutines and invoked from Close() so that
	// late status/checkpoint goroutine activity cannot race with teardown.
	watchTaskWait func()
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
	// Cancel the runner context so background goroutines (status.WatchTask)
	// observe ctx.Done() and exit. Idempotent.
	if r.cancelFunc != nil {
		r.cancelFunc()
	}
	// Wait for the status/checkpoint dumper goroutines to exit before
	// tearing down connections, so a late DumpCheckpoint cannot race with
	// post-Close cleanup.
	if r.watchTaskWait != nil {
		r.watchTaskWait()
	}
	if r.copyChunker != nil {
		if err := r.copyChunker.Close(); err != nil {
			return err
		}
	}
	for i := range r.sources {
		if r.sources[i].replClient != nil {
			r.sources[i].replClient.Close()
		}
	}
	for _, target := range r.targets {
		if err := target.DB.Close(); err != nil {
			return err
		}
	}
	return nil
}

// getTables connects to a source DB and fetches the list of tables.
// If SourceTables is specified in the Move config, only those tables will be returned.
func (r *Runner) getTables(ctx context.Context, src *sourceInfo) ([]*table.TableInfo, error) {
	rows, err := src.db.QueryContext(ctx, "SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer utils.CloseAndLog(rows)

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

		tableInfo := table.NewTableInfo(src.db, src.config.DBName, tableName)
		tableInfo.Host = src.config.Addr // Set the Host field for disambiguation in multi-chunker
		if err := tableInfo.SetInfo(ctx); err != nil {
			return nil, err
		}

		// If a ShardingProvider is configured, get sharding metadata for this table.
		if r.move.ShardingProvider != nil {
			shardingColumn, hashFunc, err := r.move.ShardingProvider.GetShardingMetadata(src.config.DBName, tableName)
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

// createTargetTables creates tables on all targets.
// If DeferSecondaryIndexes is enabled, tables are created without secondary indexes.
// Secondary indexes will be added later by restoreSecondaryIndexes() before cutover.
// This function skips tables that already exist (they were validated by checkTargetEmpty).
func (r *Runner) createTargetTables(ctx context.Context) error {
	// All sources have identical schemas, so use sources[0] for SHOW CREATE TABLE.
	for _, t := range r.sourceTables {
		var createStmt string
		row := r.sources[0].db.QueryRowContext(ctx, fmt.Sprintf("SHOW CREATE TABLE %s", t.QuotedTableName))
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
	copyChunkers := make([]table.Chunker, 0, len(r.sources)*len(r.sourceTables))
	checksumChunkers := make([]table.Chunker, 0, len(r.sources)*len(r.sourceTables))
	var err error

	// For each source and each table, create a chunker and add a subscription
	// to that source's repl client.
	for i := range r.sources {
		for _, tbl := range r.sources[i].tables {
			chunkerCfg := table.ChunkerConfig{
				TargetChunkTime: r.move.TargetChunkTime,
				Logger:          r.logger,
			}
			copyChunker, err := table.NewChunker(tbl, chunkerCfg)
			if err != nil {
				return err
			}
			if err := r.sources[i].replClient.AddSubscription(tbl, nil, copyChunker); err != nil {
				return err
			}
			checksumChunker, err := table.NewChunker(tbl, chunkerCfg)
			if err != nil {
				return err
			}
			copyChunkers = append(copyChunkers, copyChunker)
			checksumChunkers = append(checksumChunkers, checksumChunker)
		}
	}

	// Verify columns match between source and target for all tables.
	for _, src := range r.sourceTables {
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
	r.copier, err = copier.NewCopier(r.sources[0].db, r.copyChunker, &copier.CopierConfig{
		Concurrency:     r.move.Threads,
		TargetChunkTime: r.move.TargetChunkTime,
		Logger:          r.logger,
		Throttler:       &throttler.Noop{},
		MetricsSink:     &metrics.NoopSink{},
		DBConfig:        r.dbConfig,
		Applier:         r.applier, // Use the shared applier
		Buffered:        true,      // move always uses the buffered copier
	})
	if err != nil {
		return err
	}

	// Read checkpoint from sources[0] by convention.
	src0 := &r.sources[0]
	query := fmt.Sprintf("SELECT id, copier_watermark, checksum_watermark, binlog_positions, statement FROM `%s`.`%s` ORDER BY id DESC LIMIT 1",
		src0.config.DBName, checkpointTableName)
	var copierWatermark, binlogPositionsJSON, stmt string
	var id int
	err = src0.db.QueryRowContext(ctx, query).Scan(&id, &copierWatermark, &r.checksumWatermark, &binlogPositionsJSON, &stmt)
	if err != nil {
		return fmt.Errorf("could not read from checkpoint table '%s' on source: %w", checkpointTableName, err)
	}

	// Restore per-source binlog positions, keyed by sourceKey (addr/dbname).
	var positions map[string]binlogPosition
	if err := json.Unmarshal([]byte(binlogPositionsJSON), &positions); err != nil {
		return fmt.Errorf("could not parse binlog positions from checkpoint: %w", err)
	}
	for i := range r.sources {
		key := r.sources[i].sourceKey()
		pos, ok := positions[key]
		if !ok {
			return fmt.Errorf("checkpoint missing binlog position for source %s", key)
		}
		r.sources[i].replClient.SetFlushedPos(gomysql.Position{
			Name: pos.Name,
			Pos:  pos.Pos,
		})
	}

	// Delete rows above the watermark from all target tables before resuming.
	// When resuming from a checkpoint, the keyAboveWatermark optimization
	// needs to know the highest key in the target table to avoid discarding
	// binlog events for keys that were already copied. In the move path the
	// target is on a different server, so we can't (easily) read its max value.
	// Instead, we delete everything above the watermark from the targets,
	// guaranteeing that no rows exist above the copier's resume position.
	// The copier will re-copy these rows, and the checksum will verify.
	if err := r.deleteAboveWatermark(ctx, copierWatermark); err != nil {
		return err
	}

	if err := r.copyChunker.OpenAtWatermark(copierWatermark); err != nil {
		return err
	}

	// Start all replication clients.
	for i := range r.sources {
		if err := r.sources[i].replClient.Run(ctx); err != nil {
			return fmt.Errorf("failed to start repl client for source %d: %w", i, err)
		}
	}

	r.checkpointTable = table.NewTableInfo(src0.db, src0.config.DBName, checkpointTableName)
	r.usedResumeFromCheckpoint = true
	return nil
}

func (r *Runner) setup(ctx context.Context) error {
	var err error

	// Run preflight checks on the source database
	r.logger.Info("Running preflight checks")
	if err := r.runChecks(ctx, check.ScopePreflight); err != nil {
		return err
	}

	// Fetch the canonical table list from sources[0].
	// All sources have identical schemas (validated by source_schema_consistency check).
	r.logger.Info("Fetching source table list")
	if r.sourceTables, err = r.getTables(ctx, &r.sources[0]); err != nil {
		return err
	}
	r.sources[0].tables = r.sourceTables

	// Create per-source TableInfo objects for additional sources.
	for i := 1; i < len(r.sources); i++ {
		tables, err := r.getTables(ctx, &r.sources[i])
		if err != nil {
			return fmt.Errorf("failed to get tables for source %d: %w", i, err)
		}
		r.sources[i].tables = tables
	}

	if len(r.sourceTables) == 0 {
		r.logger.Info("No tables found in source database; nothing to move")
		return nil
	}

	// Create a single applier instance shared by all repl clients and the copier.
	r.logger.Info("Creating shared applier")
	r.applier, err = r.createApplier()
	if err != nil {
		return err
	}

	// Create one repl client per source, all sharing the same applier.
	r.logger.Info("Setting up repl clients", "sourceCount", len(r.sources))
	for i := range r.sources {
		src := &r.sources[i]
		src.replClient = repl.NewClient(src.db, src.config.Addr, src.config.User, src.config.Passwd, r.applier, &repl.ClientConfig{
			Logger:          r.logger,
			Concurrency:     r.move.Threads,
			TargetBatchTime: r.move.TargetChunkTime,
			CancelFunc:      r.fatalError,
			DDLFilterSchema: src.config.DBName,
			DDLFilterTables: r.move.SourceTables,
			ServerID:        repl.NewServerID(),
			DBConfig:        r.dbConfig,
		})
	}

	// Run post-setup checks
	if err = r.runChecks(ctx, check.ScopePostSetup); err != nil {
		// The checks returned an error, which could just mean that tables exist on the target.
		// So we can switch tactics and check if these artifacts pass the tests
		// to resume from checkpoint instead.
		if resumeErr := r.runChecks(ctx, check.ScopeResume); resumeErr != nil {
			return fmt.Errorf("target state is invalid for both new copy and resume: new_copy_error=%w, resume_error=%w", err, resumeErr)
		}
		// We pass the pre-check for resume, so attempt it
		if err := r.resumeFromCheckpoint(ctx); err != nil {
			return fmt.Errorf("resume validation passed but checkpoint resume failed: %w", err)
		}
		r.logger.Info("Successfully resumed move from existing checkpoint")
		return nil
	}
	// The post-setup checks returned no errors so we can proceed with new copy
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

	copyChunkers := make([]table.Chunker, 0, len(r.sources)*len(r.sourceTables))
	checksumChunkers := make([]table.Chunker, 0, len(r.sources)*len(r.sourceTables))

	// For each source and each table, create a chunker and add a subscription
	// to that source's repl client.
	for i := range r.sources {
		for _, tbl := range r.sources[i].tables {
			chunkerCfg := table.ChunkerConfig{
				TargetChunkTime: r.move.TargetChunkTime,
				Logger:          r.logger,
			}
			copyChunker, err := table.NewChunker(tbl, chunkerCfg)
			if err != nil {
				return err
			}
			if err := r.sources[i].replClient.AddSubscription(tbl, nil, copyChunker); err != nil {
				return err
			}
			checksumChunker, err := table.NewChunker(tbl, chunkerCfg)
			if err != nil {
				return err
			}
			copyChunkers = append(copyChunkers, copyChunker)
			checksumChunkers = append(checksumChunkers, checksumChunker)
		}
	}

	r.copyChunker = table.NewMultiChunker(copyChunkers...)
	r.checksumChunker = table.NewMultiChunker(checksumChunkers...)

	// Create a copier that reads from the multi chunker and uses the shared applier.
	var err error
	r.copier, err = copier.NewCopier(r.sources[0].db, r.copyChunker, &copier.CopierConfig{
		Concurrency:     r.move.Threads,
		TargetChunkTime: r.move.TargetChunkTime,
		Logger:          r.logger,
		Throttler:       &throttler.Noop{},
		MetricsSink:     &metrics.NoopSink{},
		DBConfig:        r.dbConfig,
		Applier:         r.applier, // Use the shared applier
		Buffered:        true,      // move always uses the buffered copier
	})
	if err != nil {
		return err
	}

	// Then open the multi chunker.
	if err := r.copyChunker.Open(); err != nil {
		return err
	}

	// Start all replication clients.
	for i := range r.sources {
		if err := r.sources[i].replClient.Run(ctx); err != nil {
			return fmt.Errorf("failed to start repl client for source %d: %w", i, err)
		}
	}

	return nil
}

// createCheckpointTable creates checkpoint table on SOURCE (not target).
// createCheckpointTable creates checkpoint table on sources[0] by convention.
func (r *Runner) createCheckpointTable(ctx context.Context) error {
	src0 := &r.sources[0]
	if err := dbconn.Exec(ctx, src0.db, "DROP TABLE IF EXISTS %n.%n", src0.config.DBName, checkpointTableName); err != nil {
		return err
	}
	if err := dbconn.Exec(ctx, src0.db, `CREATE TABLE %n.%n (
	id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	copier_watermark TEXT,
	checksum_watermark TEXT,
	binlog_positions TEXT,
	statement TEXT
	)`,
		src0.config.DBName, checkpointTableName); err != nil {
		return err
	}
	r.checkpointTable = table.NewTableInfo(src0.db, src0.config.DBName, checkpointTableName)
	return nil
}

func (r *Runner) Run(ctx context.Context) error {
	ctx, r.cancelFunc = context.WithCancel(ctx)
	defer r.cancelFunc()
	r.startTime = time.Now()
	bi := buildinfo.Get()
	r.logger.Info("Starting table move",
		"version", bi.Version,
		"commit", bi.Commit,
		"build-date", bi.Date,
		"go", bi.GoVer,
		"dirty", bi.Modified,
	)

	var err error
	r.dbConfig = dbconn.NewDBConfig()
	// ForceKill is now true by default in NewDBConfig(), no need to set explicitly.
	// Buffered copier needs more connections due to parallel read/write workers
	r.dbConfig.MaxOpenConnections = r.move.Threads + r.move.WriteThreads + 2

	// Build the list of source DSNs. If SourceDSNs is set (N:M), use it.
	// Otherwise, use SourceDSN as the single source (backward compat).
	sourceDSNs := r.move.SourceDSNs
	if len(sourceDSNs) == 0 {
		sourceDSNs = []string{r.move.SourceDSN}
	}

	// Open connections to all sources.
	r.sources = make([]sourceInfo, len(sourceDSNs))
	for i, dsn := range sourceDSNs {
		db, err := dbconn.New(dsn, r.dbConfig)
		if err != nil {
			return fmt.Errorf("failed to connect to source %d: %w", i, err)
		}
		cfg, err := mysql.ParseDSN(dsn)
		if err != nil {
			return fmt.Errorf("failed to parse source DSN %d: %w", i, err)
		}
		r.sources[i] = sourceInfo{db: db, config: cfg, dsn: dsn}
	}
	// Sort sources by sourceKey (addr/dbname) for deterministic ordering.
	// The checkpoint is always written to sources[0], so the order must be
	// stable across runs even if the caller constructed SourceDSNs from a map.
	// Sorting by addr/dbname rather than raw DSN ensures stability across
	// credential rotations or DSN parameter reordering.
	slices.SortFunc(r.sources, func(a, b sourceInfo) int {
		return strings.Compare(a.sourceKey(), b.sourceKey())
	})
	defer func() {
		for i := range r.sources {
			utils.CloseAndLog(r.sources[i].db)
		}
	}()

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

	// Take a metadata lock on each source to prevent concurrent DDL.
	var metadataLocks []*dbconn.MetadataLock
	for i := range r.sources {
		lock, err := dbconn.NewMetadataLock(ctx, r.sources[i].dsn, r.sources[i].tables, r.dbConfig, r.logger)
		if err != nil {
			for _, acquiredLock := range metadataLocks {
				if closeErr := acquiredLock.Close(); closeErr != nil {
					r.logger.Error("failed to release metadata lock after acquisition failure", "error", closeErr)
				}
			}
			return fmt.Errorf("failed to acquire metadata lock on source %d: %w", i, err)
		}
		metadataLocks = append(metadataLocks, lock)
	}
	defer func() {
		for _, lock := range metadataLocks {
			if err := lock.Close(); err != nil {
				r.logger.Error("failed to release metadata lock", "error", err)
			}
		}
	}()

	r.startBackgroundRoutines(ctx)
	r.setWatermarkOptimizationAll(true)

	r.status.Set(status.CopyRows)
	if err := r.copier.Run(ctx); err != nil {
		return err
	}

	// Disable both watermark optimizations so that all changes can be flushed.
	// The watermark optimizations can prevent some keys from being flushed,
	// which would cause flushedPos to not advance, leading to a mismatch
	// with bufferedPos and causing AllChangesFlushed() to return false.
	r.setWatermarkOptimizationAll(false)
	if err := r.flushAllReplClients(ctx); err != nil {
		return err
	}

	r.logger.Info("All tables copied successfully.")

	r.sentinelWaitStartTime = time.Now()
	r.status.Set(status.WaitingOnSentinelTable)
	if err := r.waitOnSentinelTable(ctx); err != nil {
		return err
	}

	if err := r.prepareForCutover(ctx); err != nil {
		return err
	}
	r.logger.Info("Checksum completed successfully, starting cutover")
	// Create a cutover.
	r.status.Set(status.CutOver)
	cutoverSources := make([]CutOverSource, len(r.sources))
	for i := range r.sources {
		cutoverSources[i] = CutOverSource{
			DB:         r.sources[i].db,
			ReplClient: r.sources[i].replClient,
			Tables:     r.sources[i].tables,
		}
	}
	cutover, err := NewCutOver(cutoverSources, r.cutoverFunc, r.dbConfig, r.logger)
	if err != nil {
		return err
	}
	if err = cutover.Run(ctx); err != nil {
		return err
	}
	// Delete checkpoint table from sources[0].
	src0 := &r.sources[0]
	if err := dbconn.Exec(ctx, src0.db, "DROP TABLE IF EXISTS %n.%n", src0.config.DBName, checkpointTableName); err != nil {
		return err
	}
	r.logger.Info("Move operation complete.")
	return nil
}

// startBackgroundRoutines starts the background routines needed for monitoring.
// This includes table statistics updates and periodic binlog flushing.
func (r *Runner) startBackgroundRoutines(ctx context.Context) {
	// Start routines in table and replication packages to
	// Continuously update the min/max and estimated rows
	// and to flush the binary log position periodically.
	// These will both be stopped when the copier finishes
	// and checksum starts, although the PeriodicFlush
	// will be restarted again after.
	for i := range r.sources {
		for _, tbl := range r.sources[i].tables {
			go tbl.AutoUpdateStatistics(ctx, tableStatUpdateInterval, r.logger)
		}
		go r.sources[i].replClient.StartPeriodicFlush(ctx, repl.DefaultFlushInterval)
	}

	// Start go routines for checkpointing and dumping status. The returned
	// wait function is invoked from Close() so we can be sure no late
	// checkpoint INSERT lands after teardown begins.
	r.watchTaskWait = status.WatchTask(ctx, r, r.logger)
}

// fatalError is the callback provided to the replication client.
// It is called when a DDL change is detected on a subscribed table,
// or when a fatal stream error occurs. The replication client may perform
// its own logging either before or after invoking this callback, and DDL
// logging may be skipped entirely if this callback returns false.
//
// The return value indicates whether the replication client should treat
// the condition as fatal and stop the replication stream. It returns true
// when the error should be treated as fatal (and replication should be
// terminated and cleaned up), and false when the error should not be treated
// as fatal (in which case the client may continue without logging the DDL).
func (r *Runner) fatalError() bool {
	if r.status.Get() >= status.CutOver {
		return false
	}
	r.status.Set(status.ErrCleanup)
	// Invalidate the checkpoint, so we don't try to resume.
	// If we don't do this, the move will permanently be blocked from proceeding.
	// Letting it start again is the better choice.
	// Use a background context since the move context may already be cancelled.
	if r.checkpointTable != nil && len(r.sources) > 0 {
		if err := dbconn.Exec(context.Background(), r.sources[0].db, "DROP TABLE IF EXISTS %n.%n", r.checkpointTable.SchemaName, r.checkpointTable.TableName); err != nil {
			r.logger.Error("could not remove checkpoint",
				"error", err,
			)
		}
	}
	r.cancelFunc() // cancel the move context
	return true
}

func (r *Runner) Status() string {
	state := r.status.Get()
	if state > status.CutOver {
		return ""
	}
	switch state { //nolint:exhaustive
	case status.CopyRows:
		// Status for copy rows
		return fmt.Sprintf("migration status: state=%s copy-progress=%s binlog-deltas=%v total-time=%s copier-time=%s copier-remaining-time=%v copier-is-throttled=%v",
			r.status.Get().String(),
			r.copier.GetProgress(),
			r.getDeltaLenAll(),
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
			r.getDeltaLenAll(),
			time.Since(r.startTime).Round(time.Second),
		)
	case status.Checksum:
		// This could take a while if it's a large table.
		return fmt.Sprintf("migration status: state=%s checksum-progress=%s binlog-deltas=%v total-time=%s checksum-time=%s",
			r.status.Get().String(),
			r.checker.GetProgress(),
			r.getDeltaLenAll(),
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

// runChecks wraps around check.RunChecks and adds the context of this move operation
func (r *Runner) runChecks(ctx context.Context, scope check.ScopeFlag) error {
	sources := make([]check.SourceResource, len(r.sources))
	for i := range r.sources {
		sources[i] = check.SourceResource{
			DB:     r.sources[i].db,
			Config: r.sources[i].config,
			DSN:    r.sources[i].dsn,
		}
	}
	return check.RunChecks(ctx, check.Resources{
		Sources:        sources,
		Targets:        r.targets,
		SourceTables:   r.sourceTables,
		CreateSentinel: r.move.CreateSentinel,
	}, r.logger, scope)
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
		// All sources have identical schemas, so use sources[0].
		row := r.sources[0].db.QueryRowContext(ctx, fmt.Sprintf("SHOW CREATE TABLE %s", tbl.QuotedTableName))
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
			targetRow := target.DB.QueryRowContext(ctx, fmt.Sprintf("SHOW CREATE TABLE %s", tbl.QuotedTableName))
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
	r.stopPeriodicFlushAll()
	r.status.Set(status.ApplyChangeset)
	if err := r.flushAllReplClients(ctx); err != nil {
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
	defer utils.CloseAndLog(r.checksumChunker)

	// Perform a checksum operation
	// Collect all source DBs and repl clients for the checksum.
	sourceDBs := make([]*sql.DB, len(r.sources))
	feeds := make([]*repl.Client, len(r.sources))
	for i := range r.sources {
		sourceDBs[i] = r.sources[i].db
		feeds[i] = r.sources[i].replClient
	}
	var err error
	r.checker, err = checksum.NewChecker(sourceDBs, r.checksumChunker, feeds, &checksum.CheckerConfig{
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
	switch r.status.Get() { //nolint:exhaustive
	case status.CopyRows:
		summary = fmt.Sprintf("%v %s ETA %v",
			r.copier.GetProgress(),
			r.status.Get().String(),
			r.copier.GetETA(),
		)
	case status.WaitingOnSentinelTable:
		r.logger.Info("migration status",
			"state", r.status.Get().String(),
			"sentinel-table", fmt.Sprintf("%s.%s", r.sources[0].config.DBName, sentinelTableName),
			"total-time", time.Since(r.startTime).Round(time.Second),
			"sentinel-wait-time", time.Since(r.sentinelWaitStartTime).Round(time.Second),
			"sentinel-max-wait-time", sentinelWaitLimit,
		)
	case status.ApplyChangeset, status.PostChecksum:
		summary = fmt.Sprintf("Applying Changeset Deltas=%v", r.getDeltaLenAll())
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
	if err := dbconn.Exec(ctx, r.sources[0].db, "DROP TABLE IF EXISTS %n.%n", r.sources[0].config.DBName, sentinelTableName); err != nil {
		return err
	}
	if err := dbconn.Exec(ctx, r.sources[0].db, "CREATE TABLE %n.%n (id int NOT NULL PRIMARY KEY)", r.sources[0].config.DBName, sentinelTableName); err != nil {
		return err
	}
	return nil
}

// sentinelTableExists checks if sentinel table exists on SOURCE (not target).
func (r *Runner) sentinelTableExists(ctx context.Context) (bool, error) {
	sql := "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
	var sentinelTableExists int
	err := r.sources[0].db.QueryRowContext(ctx, sql, r.sources[0].config.DBName, sentinelTableName).Scan(&sentinelTableExists)
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
	// Collect per-source binlog positions, keyed by sourceKey (addr/dbname).
	positions := make(map[string]binlogPosition)
	for i := range r.sources {
		pos := r.sources[i].replClient.GetBinlogApplyPosition()
		positions[r.sources[i].sourceKey()] = binlogPosition{Name: pos.Name, Pos: pos.Pos}
	}
	positionsJSON, err := json.Marshal(positions)
	if err != nil {
		return fmt.Errorf("failed to marshal binlog positions: %w", err)
	}

	copierWatermark, err := r.copyChunker.GetLowWatermark()
	if err != nil {
		return status.ErrWatermarkNotReady // it might not be ready, we can try again.
	}
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
		"binlog-positions", string(positionsJSON))
	err = dbconn.Exec(ctx, r.sources[0].db, "INSERT INTO %n.%n (copier_watermark, checksum_watermark, binlog_positions, statement) VALUES (%?, %?, %?, %?)",
		r.checkpointTable.SchemaName,
		r.checkpointTable.TableName,
		copierWatermark,
		checksumWatermark,
		string(positionsJSON),
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

// flushAllReplClients flushes all replication clients.
func (r *Runner) flushAllReplClients(ctx context.Context) error {
	for i := range r.sources {
		if err := r.sources[i].replClient.Flush(ctx); err != nil {
			return fmt.Errorf("failed to flush repl client for source %d: %w", i, err)
		}
	}
	return nil
}

// deleteAboveWatermark deletes rows above the copier watermark from all target
// tables. This is called during resume-from-checkpoint because of a race
// condition with the keyAboveWatermark optimization:
//
// During normal copying, the keyAboveWatermark optimization discards binlog
// events for keys the copier "hasn't reached yet" — these rows don't exist
// in the target, so deletes/updates for them can be safely ignored. But after
// a resume, some rows above the watermark may have already been copied to the
// target before the interruption. If a DELETE event arrives for one of these
// rows and keyAboveWatermark discards it, the row remains in the target as
// a phantom row that no longer exists in the source.
//
// In the migration path, this is solved by reading the target table's max
// value and temporarily disabling the optimization up to that point. In the
// move path, the target is on a different server, so we can't cheaply read
// its max value. Instead, we delete everything above the watermark from the
// targets before resuming. This guarantees no rows exist above the copier's
// resume position, so the optimization is safe.
//
// tl;dr: this is required to prevent a race where:
//   - watermark is at key=100, but a row at key=105 was inserted and copied.
//   - immediately after resume there is a delete for key=105 but we incorrectly
//     skip it because it is above the watermark.
func (r *Runner) deleteAboveWatermark(ctx context.Context, copierWatermark string) error {
	for _, src := range r.sourceTables {
		aboveClause, err := table.WatermarkAboveClause(src, copierWatermark)
		if err != nil {
			return fmt.Errorf("failed to parse watermark for table %s: %w", src.TableName, err)
		}
		for i, target := range r.targets {
			deleteStmt := fmt.Sprintf("DELETE FROM %s WHERE %s",
				src.QuotedTableName, aboveClause)
			result, err := target.DB.ExecContext(ctx, deleteStmt)
			if err != nil {
				return fmt.Errorf("failed to delete above watermark on target %d table %s: %w", i, src.TableName, err)
			}
			rowsDeleted, _ := result.RowsAffected()
			if rowsDeleted > 0 {
				r.logger.Info("deleted rows above watermark from target",
					"target", i,
					"table", src.TableName,
					"rowsDeleted", rowsDeleted,
					"watermark", copierWatermark,
				)
			}
		}
	}
	return nil
}

// setWatermarkOptimizationAll sets watermark optimization on all replication clients.
func (r *Runner) setWatermarkOptimizationAll(enabled bool) {
	for i := range r.sources {
		r.sources[i].replClient.SetWatermarkOptimization(enabled)
	}
}

// getDeltaLenAll returns the total number of pending changes across all replication clients.
func (r *Runner) getDeltaLenAll() int {
	total := 0
	for i := range r.sources {
		total += r.sources[i].replClient.GetDeltaLen()
	}
	return total
}

// stopPeriodicFlushAll stops periodic flushing on all replication clients.
func (r *Runner) stopPeriodicFlushAll() {
	for i := range r.sources {
		r.sources[i].replClient.StopPeriodicFlush()
	}
}
