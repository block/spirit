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
	"sync"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/buildinfo"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/checkpoint"
	"github.com/block/spirit/pkg/checksum"
	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/move/check"
	"github.com/block/spirit/pkg/sentinel"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/throttler"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"golang.org/x/sync/errgroup"
)

var (
	tableStatUpdateInterval = 5 * time.Minute
	// checkpointTableName is deliberately distinct from migration's shared
	// _spirit_checkpoint and datasync's _spirit_sync_checkpoint. A table with
	// this name can only have been created by a move, which is what lets
	// decideResume treat an empty one as a dead move's leavings and recover
	// without --force. Keep it in sync with the literal in
	// pkg/move/check/resume_state.go (that package cannot import this one).
	checkpointTableName = "_spirit_move_checkpoint"
	// Sentinel-wait timing lives in pkg/sentinel (sentinel.WaitLimit /
	// sentinel.CheckInterval / sentinel.TableName) so it is shared with migrate.
	//
	// continuousChecksumMinInterval is the minimum amount of time between
	// continuous-checksum iterations during the sentinel wait. Without it,
	// small tables would re-acquire the table lock back-to-back since each
	// pass finishes in seconds. (Move still drives its own checksum loop; this
	// stays local until move adopts checksum.ContinuousChecker.)
	continuousChecksumMinInterval = 1 * time.Hour
)

// sourceInfo holds per-source connection state for N:M moves.
type sourceInfo struct {
	db         *sql.DB
	config     *mysql.Config
	dsn        string
	replClient change.Source
	tables     []*table.TableInfo // this source's TableInfo objects (bound to this source's db)
}

// sourceKey returns a stable identifier for a source, used for checkpoint
// map keys and deterministic ordering. It is based on the network address
// and database name only, so it remains stable across credential rotations
// or DSN parameter reordering.
func (s *sourceInfo) sourceKey() string {
	return s.config.Addr + "/" + s.config.DBName
}

// targetKey returns a stable identifier for a target, used to sort targets
// deterministically so the checkpoint always lands on the same targets[0]
// across a stop and a later resume, even if the caller supplies the targets
// in a different order. The key range is included because two shards may live
// in the same database on the same host and are only distinguished by range.
func targetKey(t applier.Target) string {
	return t.Config.Addr + "/" + t.Config.DBName + "/" + t.KeyRange
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

	// continuousChecker is the sentinel-wait re-verification checker built
	// by runContinuousChecksum. It is deliberately separate from r.checker
	// (fresh chunker, not wired into resume), but DumpCheckpoint must
	// consult it: once it has repaired any chunk, the initial checksum's
	// watermark no longer proves the tables clean, so persisting it would
	// let a resumed run skip re-verifying the repaired range. Written once
	// by the continuous-checksum goroutine and read by the checkpoint
	// dumper goroutine — both under checkpointMu. Mirrors pkg/migration.
	continuousChecker checksum.Checker

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
	startTime                time.Time
	sentinelWaitStartTime    time.Time
	usedResumeFromCheckpoint bool

	cutoverFunc func(ctx context.Context) error
	// reverseCutoverFunc is the reverse-window rollback's traffic switch (route
	// back to the source). Set via SetReverseCutover; used only when
	// move.ReverseWindow > 0 and a revert is requested during the window.
	reverseCutoverFunc func(ctx context.Context) error
	// reversePositions holds each target's binlog position captured at cutover
	// (keyed by targetKey) — the start points for the reverse feeds. cutoverAt
	// is when the forward cutover completed (the reverse-window deadline is
	// measured from it). Both are set by the cutover postSwitch hook when
	// move.ReverseWindow > 0.
	reversePositions map[string]string
	cutoverAt        time.Time

	logger     *slog.Logger
	cancelFunc context.CancelFunc
	dbConfig   *dbconn.DBConfig

	// fatalOnce makes fatalError idempotent. Move wires N repl clients
	// (one per source) to the same fatalError callback, so a concurrent
	// burst of fatal events is realistic and without Once could
	// double-drop the checkpoint and double-cancel the context. The
	// individual operations underneath are idempotent, but routing
	// everything through Once keeps the side-effect set small enough to
	// reason about and avoids racing with Close() teardown.
	fatalOnce sync.Once

	// watchTaskWait blocks until the WatchTask goroutines have exited.
	// Set in startBackgroundRoutines and invoked from Close() so that
	// late status/checkpoint goroutine activity cannot race with teardown.
	watchTaskWait func()
	// bgCancel cancels the context the WatchTask (status + checkpoint dumper)
	// goroutines run under. Reverse-window mode stops the dumper before cutover
	// (stopWatchTask) so the reverse window is the sole writer of the checkpoint
	// row.
	bgCancel context.CancelFunc
}

var _ status.Task = (*Runner)(nil)

func NewRunner(m *Move) (*Runner, error) {
	// Normalize CheckpointMaxAge here rather than in a Validate hook:
	// orchestration callers construct Move programmatically (bypassing the
	// Kong default of 168h), so a zero value means "use the default". This
	// mirrors Migration.normalizeOptions in pkg/migration.
	if m.CheckpointMaxAge < 0 {
		return nil, fmt.Errorf("checkpoint-max-age must be non-negative, got %s", m.CheckpointMaxAge)
	}
	if m.CheckpointMaxAge == 0 {
		m.CheckpointMaxAge = 7 * 24 * time.Hour // 7 days, same as migrate
	}
	if m.TargetChunkSize == 0 {
		m.TargetChunkSize = table.DefaultTargetChunkBytes
	}
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
	// Run every cleanup step unconditionally and collect errors with
	// errors.Join. Previously the first failing step short-circuited the
	// rest, leaking the remaining repl clients' binlog reader goroutines
	// and the target DB handles.
	var errs []error
	if r.copyChunker != nil {
		if err := r.copyChunker.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	for i := range r.sources {
		if r.sources[i].replClient != nil {
			r.sources[i].replClient.Close()
		}
	}
	for _, target := range r.targets {
		if target.DB == nil {
			continue
		}
		if err := target.DB.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
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
		if strings.HasPrefix(tableName, "_spirit_") {
			// Skip Spirit-internal artifacts: this move's checkpoint
			// (checkpointTableName) and sentinel (sentinel.TableName), and also
			// any _spirit_-prefixed table left behind by another operation — e.g.
			// a migration's _spirit_checkpoint — so it is never copied as data.
			continue
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
	// All sources have identical schemas (enforced by the source_schema_consistency
	// check at ScopePostSetup), so use sources[0] for SHOW CREATE TABLE.
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
			// Move always uses the buffered copier, which reads rows into client
			// memory; size the copy chunker by an in-memory byte budget rather than
			// copy time, whose signal collapses under write-side backpressure. The
			// checksum runs server-side and keeps the time signal.
			copyChunkerCfg := chunkerCfg
			copyChunkerCfg.TargetChunkBytes = r.move.TargetChunkSize
			copyChunker, err := table.NewChunker(tbl, copyChunkerCfg)
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
		Unbuffered:      false,     // move always uses the buffered copier
	})
	if err != nil {
		return err
	}

	// Read checkpoint from targets[0] by convention. A checkpoint table written
	// by an incompatible spirit version (e.g. missing or renamed a column) fails
	// the read and aborts the move; we do not support cross-version resume.
	tgt0 := &r.targets[0]
	rec, err := r.checkpointTbl().ReadLatest(ctx)
	if err != nil {
		return fmt.Errorf("could not read from checkpoint table '%s' on target: %w", checkpointTableName, err)
	}
	// A checkpoint past the forward cutover (a reverse-window phase) is resumed
	// earlier by maybeResumeReverseWindow, before discovery/locks — it should
	// never reach this copy-path resume. This is a defensive backstop: getting
	// here with a non-empty phase means that earlier handoff was bypassed, so
	// fail loudly rather than re-run the copy and cutover (which would re-invoke
	// the traffic switch and rename the already-retired tables).
	if rec.Phase != "" {
		return fmt.Errorf("cannot resume move down the copy path: checkpoint is past cutover (phase=%q); a reverse-window resume is handled earlier (see maybeResumeReverseWindow)", rec.Phase)
	}
	copierWatermark := rec.CopierWatermark
	r.checksumWatermark = rec.ChecksumWatermark
	binlogPositionsJSON := rec.Position

	// Check if the checkpoint is too old to safely resume — replaying many
	// days of binary logs can be slower than re-copying, and the binlogs may
	// have been purged anyway. This must happen before any destructive step
	// (deleteRecopyRange below modifies the targets). Unlike migrate,
	// move cannot silently fall back to a fresh copy: the target tables are
	// non-empty (that is exactly why setupUnderLocks() chose the resume path), so we
	// fail loudly and leave the decision to the operator.
	if checkpointAge := rec.Age(); checkpointAge >= r.move.CheckpointMaxAge {
		return fmt.Errorf("%w: checkpoint is %s old (max allowed: %s). To proceed, either re-run with a larger --checkpoint-max-age, or wipe the target tables (including '%s') and restart the move from scratch",
			status.ErrCheckpointTooOld,
			checkpointAge.Round(time.Second),
			r.move.CheckpointMaxAge,
			checkpointTableName,
		)
	}

	// With multiple sources, a persisted checksum watermark cannot be trusted.
	// deleteRecopyRange (below) runs every (source, table) DELETE against
	// every target, and same-named tables from different sources interleave in
	// the target tables — so one source's DELETE also removes OTHER sources'
	// rows below their own watermarks. Those rows are not recopied (each
	// source's chunker resumes from its own watermark); only a checksum pass
	// that runs from the very beginning detects and repairs the hole. Resuming
	// the checksum at a watermark would skip re-verifying exactly the range
	// where the hole sits, so discard it and force a full pass.
	//
	// With a single source the watermark is kept: deletes are per-table, and
	// each table's delete range coincides exactly with its recopy range (both
	// start at the watermark chunk's lower bound), so nothing below the
	// checksum watermark can have been deleted without being recopied.
	if len(r.sources) > 1 && r.checksumWatermark != "" {
		r.logger.Info("discarding persisted checksum watermark: multi-source resume requires a full checksum pass",
			"reason", "deleteRecopyRange may remove rows below other sources' watermarks; only a from-scratch checksum re-verifies and repairs them",
			"sources", len(r.sources))
		r.checksumWatermark = ""
	}

	// Parse per-source positions (opaque strings owned by the source impl),
	// keyed by sourceKey (addr/dbname).
	var positions map[string]string
	if err := json.Unmarshal([]byte(binlogPositionsJSON), &positions); err != nil {
		return fmt.Errorf("could not parse binlog positions from checkpoint: %w", err)
	}
	for i := range r.sources {
		if _, ok := positions[r.sources[i].sourceKey()]; !ok {
			return fmt.Errorf("checkpoint missing binlog position for source %s", r.sources[i].sourceKey())
		}
	}

	// Delete rows at/above the copier's resume position (the watermark
	// chunk's lower bound) from all target tables before resuming, so that
	// the deleted range coincides exactly with the range the copier is about
	// to re-copy. When resuming from a checkpoint, the keyAboveWatermark
	// optimization needs to know the highest key in the target table to avoid
	// discarding binlog events for keys that were already copied. In the move
	// path the target is on a different server, so we can't (easily) read its
	// max value. Instead, we guarantee no rows exist at/above the copier's
	// resume position: the copier re-copies the deleted range from the
	// current source snapshot (so rows deleted on the source stay gone), and
	// the checksum will verify. See deleteRecopyRange for the races.
	if err := r.deleteRecopyRange(ctx, copierWatermark); err != nil {
		return err
	}

	if err := r.copyChunker.OpenAtWatermark(copierWatermark); err != nil {
		return err
	}

	// Open each source's change feed at its checkpointed position.
	// OpenFromPosition primes the position and starts streaming in one call.
	for i := range r.sources {
		key := r.sources[i].sourceKey()
		if err := r.sources[i].replClient.StartFromPosition(ctx, positions[key]); err != nil {
			return fmt.Errorf("failed to start change feed for source %d at %q: %w", i, positions[key], err)
		}
	}

	r.checkpointTable = table.NewTableInfo(tgt0.DB, tgt0.Config.DBName, checkpointTableName)
	r.usedResumeFromCheckpoint = true
	return nil
}

// setupDiscovery is the read-only first phase of setup: it runs the
// preflight checks and populates the table lists (r.sourceTables and each
// r.sources[i].tables). Run() calls it before acquiring the per-source
// advisory locks — the lock names are derived from the table lists, so
// discovery has to happen first — and nothing in here may modify the source
// or the target. All potentially destructive setup lives in setupUnderLocks.
func (r *Runner) setupDiscovery(ctx context.Context) error {
	var err error

	// Run preflight checks on the source database
	r.logger.Debug("Running preflight checks")
	if err := r.runChecks(ctx, check.ScopePreflight); err != nil {
		return err
	}

	// Fetch the canonical table list from sources[0].
	// All sources have identical schemas (validated by source_schema_consistency check).
	r.logger.Debug("Fetching source table list")
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
	}
	return nil
}

// setupUnderLocks is the second phase of setup. It contains every step that
// can modify the source or the target — the resume path's
// delete-above-watermark, --force's target wipe, target table and checkpoint
// creation, and starting the replication clients — so Run() only calls it
// after the per-source advisory locks are held. That ordering guarantees a
// concurrent second spirit invocation against the same sources fails on the
// lock before it can destroy the first run's target data.
func (r *Runner) setupUnderLocks(ctx context.Context) error {
	var err error

	// Resolve the number of apply (write) threads against the target now that
	// it is connected. WriteThreads==0 means "auto-size": on Aurora it becomes
	// the instance vCPU count; on non-Aurora there is no reliable vCPU signal
	// to size from, so it falls back to the default.
	r.move.WriteThreads, err = throttler.ResolveWriteThreads(ctx, r.targets[0].DB, r.move.WriteThreads, r.logger)
	if err != nil {
		return err
	}
	// Now that write threads are known, grow connection pools to cover both the
	// copy (read) threads and the apply (write) threads. The initial pool (set
	// before connecting) used the requested value, which may have been 0.
	if poolSize := r.move.Threads + r.move.WriteThreads + 2; poolSize > r.dbConfig.MaxOpenConnections {
		r.dbConfig.MaxOpenConnections = poolSize
		for i := range r.sources {
			r.sources[i].db.SetMaxOpenConns(poolSize)
		}
		for i := range r.targets {
			r.targets[i].DB.SetMaxOpenConns(poolSize)
		}
	}

	// Create a single applier instance shared by all repl clients and the copier.
	r.logger.Debug("Creating shared applier")
	r.applier, err = r.createApplier()
	if err != nil {
		return err
	}

	// Create one repl client per source, all sharing the same applier.
	r.logger.Debug("Setting up repl clients", "sourceCount", len(r.sources))
	if r.move.EnableExperimentalGTID {
		r.logger.Info("EXPERIMENTAL: using GTID-based change source")
	}
	for i := range r.sources {
		src := &r.sources[i]
		replConfig := change.NewClientDefaultConfig()
		replConfig.Logger = r.logger
		replConfig.CancelFunc = r.fatalError
		replConfig.DDLFilterSchema = src.config.DBName
		replConfig.DDLFilterTables = r.move.SourceTables
		replConfig.DBConfig = r.dbConfig
		if r.move.EnableExperimentalGTID {
			src.replClient = change.NewGTIDClient(src.db, src.config.Addr, src.config.User, src.config.Passwd, r.applier, replConfig)
		} else {
			src.replClient = change.NewBinlogClient(src.db, src.config.Addr, src.config.User, src.config.Passwd, r.applier, replConfig)
		}
	}

	// Run post-setup checks
	if err = r.runChecks(ctx, check.ScopePostSetup); err != nil {
		// New-copy checks failed — usually because tables already exist on the
		// target. Resume if we can; recover automatically when the target is
		// provably a dead attempt's partial copy; otherwise --force wipes the
		// target and starts fresh, and without it this is a hard error.
		decision, probeErr := r.decideResume(ctx)
		if probeErr != nil {
			return probeErr // the probe itself failed transiently — don't wipe on a blip
		}
		switch decision {
		case resumeCheckpoint:
			if resumeErr := r.resumeFromCheckpoint(ctx); resumeErr != nil {
				return fmt.Errorf("resume validation passed but checkpoint resume failed: %w", resumeErr)
			}
			r.logger.Info("Successfully resumed move from existing checkpoint")
			return nil
		case resumeFreshOwned:
			r.logger.Warn("target holds an empty checkpoint table: a prior move attempt stopped before writing its first checkpoint; wiping target tables and starting fresh")
		case resumeNone:
			if !r.move.Force {
				return fmt.Errorf("target state is invalid for both new copy and resume (re-run with --force to wipe the target and start fresh): %w", err)
			}
			r.logger.Warn("force set and the target cannot resume; wiping target tables and starting fresh")
		}
		if werr := r.wipeTargets(ctx); werr != nil {
			return fmt.Errorf("failed to wipe target before a fresh copy: %w", werr)
		}
		// Wiping only clears the target-not-empty failure — it must not bypass
		// the other post-setup safety checks (rename_safety,
		// source_schema_consistency, ...). Re-run them against the now-wiped
		// target (target_state passes for the absent tables, exactly as a fresh
		// move); abort if anything still fails rather than copying into a state
		// that would only fail at cutover. RunChecks iterates a map, so the
		// original failure was not necessarily target_state.
		if err := r.runChecks(ctx, check.ScopePostSetup); err != nil {
			return fmt.Errorf("target still fails post-setup checks after wiping: %w", err)
		}
		return r.newCopy(ctx)
	}
	// The post-setup checks returned no errors so we can proceed with new copy
	return r.newCopy(ctx)
}

// resumeDecision is decideResume's verdict on a target that failed the
// new-copy (post-setup) checks.
type resumeDecision int

const (
	// resumeCheckpoint: a checkpoint row exists and the resume pre-checks
	// pass — continue the interrupted move from it.
	resumeCheckpoint resumeDecision = iota
	// resumeFreshOwned: the checkpoint table exists but holds no row. Its name
	// (checkpointTableName) is unique to move, so only a move creates it —
	// transiently, after the target tables passed the empty-target validation
	// and before any row is copied. An empty one therefore means everything on
	// the target is a prior move attempt's partial copy that died before its
	// first checkpoint dump: safe to wipe and start fresh without --force. (A
	// migration's shared _spirit_checkpoint has a different name and never lands
	// here, so it can't trick move into wiping unrelated data.)
	resumeFreshOwned
	// resumeNone: nothing proves spirit owns the target rows — no checkpoint
	// table at all, one written by an incompatible version, or failing resume
	// pre-checks. The operator decides via --force.
	resumeNone
)

// decideResume reports how setupUnderLocks should treat a target that failed
// the new-copy checks: resume from its checkpoint, wipe it and start fresh
// (the empty-checkpoint state a run canceled before its first checkpoint dump
// leaves behind), or hand the decision to the operator (--force). A non-nil
// error means the probe itself failed transiently (don't act on it).
//
// The checkpoint is read before the resume pre-checks so that the
// empty-checkpoint recovery also fires when the pre-checks would fail (e.g.
// the source schema changed since the dead attempt): the target tables are
// about to be dropped and recreated, so their state cannot matter. This
// mirrors datasync, whose checkpoint table is treated as spirit-owned by its
// mere existence rather than a written row. The unique table name is
// load-bearing for both: migration's shared _spirit_checkpoint is NOT proof of
// a move, so move keys ownership on its own name (see checkpointTableName).
func (r *Runner) decideResume(ctx context.Context) (resumeDecision, error) {
	if _, err := r.checkpointTbl().ReadLatest(ctx); err != nil {
		switch {
		case errors.Is(err, checkpoint.ErrNotFound):
			return resumeFreshOwned, nil
		case checkpoint.IsIncompatible(err):
			// No checkpoint table, or a layout this version can't read.
			return resumeNone, nil
		default:
			return resumeNone, fmt.Errorf("could not read checkpoint to decide resume: %w", err)
		}
	}
	if err := r.runChecks(ctx, check.ScopeResume); err != nil {
		r.logger.Info("resume pre-checks failed", "reason", err)
		return resumeNone, nil
	}
	return resumeCheckpoint, nil
}

// wipeTargets drops the move's target tables (on every target) and the
// checkpoint table, so a fresh copy can proceed. Used by --force when the target
// cannot resume, and by the empty-checkpoint fresh-start recovery
// (resumeFreshOwned). The source is left untouched; so is the sentinel on
// targets[0] (a fresh copy recreates it idempotently).
func (r *Runner) wipeTargets(ctx context.Context) error {
	for i := range r.targets {
		for _, t := range r.sourceTables {
			if err := dbconn.Exec(ctx, r.targets[i].DB, "DROP TABLE IF EXISTS %n", t.TableName); err != nil {
				return fmt.Errorf("drop target table %q on target %d: %w", t.TableName, i, err)
			}
		}
	}
	return r.checkpointTbl().Drop(ctx) // Transient → DROP TABLE IF EXISTS
}

// dropStaleRevertTables drops any leftover <table>_revert tables on every
// target. A reverse-window rollback retires the former targets to their
// _revert form (check.RevertRetiredName); a later fresh move — and the reverse
// cutover itself — must clear those first so the retire RENAME cannot collide
// with a leftover. Safe because only a revert ever creates a _revert table.
func (r *Runner) dropStaleRevertTables(ctx context.Context) error {
	for i := range r.targets {
		for _, t := range r.sourceTables {
			revertName := check.RevertRetiredName(t.TableName)
			if err := dbconn.Exec(ctx, r.targets[i].DB, "DROP TABLE IF EXISTS %n", revertName); err != nil {
				return fmt.Errorf("drop stale revert table %q on target %d: %w", revertName, i, err)
			}
		}
	}
	return nil
}

// maybeResumeReverseWindow takes over the run when a prior attempt reached the
// reverse window, rather than falling through to discovery/copy (which would
// re-copy the source's now-_old tables — the reported failure mode). It reads
// the checkpoint before any locks or discovery. Returns handled=true when it
// owns the run.
//
// Not yet handled: an interrupted reverse *cutover* (phase "reverting") leaves
// an ambiguous half-renamed state, so that surfaces as an error for manual
// completion rather than an unsafe auto-resume.
func (r *Runner) maybeResumeReverseWindow(ctx context.Context) (bool, error) {
	rec, err := r.checkpointTbl().ReadLatest(ctx)
	if err != nil {
		// No usable checkpoint (fresh move, empty, or a layout this version
		// can't read) — not a reverse resume; let the normal flow decide
		// copy-resume vs fresh.
		return false, nil //nolint:nilerr
	}
	switch rec.Phase {
	case phaseReverseWindow:
		r.logger.Warn("resuming an interrupted reverse window from checkpoint", "cutover_at", rec.CutoverAt)
		return true, r.resumeReverseWindow(ctx, rec)
	case phaseReverting:
		return true, fmt.Errorf("a reverse cutover was interrupted (checkpoint phase=%q); resuming a partial rollback is not yet supported — complete it manually", rec.Phase)
	default:
		return false, nil // phase "" (copy) — the normal flow handles resume/fresh
	}
}

// resumeReverseWindow rebuilds the reverse-window state from the checkpoint and
// re-enters the window. The reverse feeds restart from the checkpointed
// positions and catch up whatever the source missed while the process was down.
//
// v1 note: no advisory locks are re-acquired here (a concurrent second restart
// of the same move is an operator error), and the feed always resumes from the
// original cutover position — correct via idempotent apply, at the cost of
// re-reading the window's binlog.
func (r *Runner) resumeReverseWindow(ctx context.Context, rec checkpoint.Record) error {
	var positions map[string]string
	if err := json.Unmarshal([]byte(rec.Position), &positions); err != nil {
		return fmt.Errorf("resume reverse window: parse stored positions: %w", err)
	}
	r.reversePositions = positions
	r.cutoverAt = rec.CutoverAt

	logical, err := r.reverseWindowLogicalTables(ctx)
	if err != nil {
		return err
	}
	if len(logical) == 0 {
		return fmt.Errorf("resume reverse window: found no retired (_old) source tables to resume from")
	}
	src := &r.sources[0] // reverse-window is single-source (guarded at cutover)
	src.tables = make([]*table.TableInfo, 0, len(logical))
	for _, name := range logical {
		// Only the name is read downstream (buildFeed derives the _old source
		// and real target TableInfos itself), so no SetInfo — the logical table
		// no longer exists on the source under this name.
		src.tables = append(src.tables, table.NewTableInfo(src.db, src.config.DBName, name))
	}
	r.sourceTables = src.tables

	return newReverseWindow(r).run(ctx)
}

// reverseWindowLogicalTables recovers the logical names of the moved tables when
// resuming a reverse window: the forward cutover renamed each to <name>_old on
// the source, so it lists those and strips the suffix. When an explicit table
// list was supplied it is used to filter (ignoring unrelated _old tables).
func (r *Runner) reverseWindowLogicalTables(ctx context.Context) ([]string, error) {
	src := &r.sources[0]
	rows, err := src.db.QueryContext(ctx,
		"SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name LIKE '%\\_old'",
		src.config.DBName)
	if err != nil {
		return nil, fmt.Errorf("resume reverse window: list retired source tables: %w", err)
	}
	defer utils.CloseAndLog(rows)
	wanted := make(map[string]bool, len(r.move.SourceTables))
	for _, t := range r.move.SourceTables {
		wanted[t] = true
	}
	var logical []string
	for rows.Next() {
		var oldName string
		if err := rows.Scan(&oldName); err != nil {
			return nil, err
		}
		name := strings.TrimSuffix(oldName, "_old")
		if len(wanted) > 0 && !wanted[name] {
			continue // an unrelated _old table, not part of this move
		}
		logical = append(logical, name)
	}
	return logical, rows.Err()
}

func (r *Runner) newCopy(ctx context.Context) error {
	// Starting fresh. Clear any leftover _revert tables from an earlier
	// reverse-window rollback on these targets, so this run's own reverse
	// cutover can retire the targets without colliding (and so they don't
	// linger). Only on the fresh path — a resume must not drop tables.
	if err := r.dropStaleRevertTables(ctx); err != nil {
		return err
	}
	// For each table, fetch the CREATE TABLE statement from the source and run it on the target.
	if err := r.createTargetTables(ctx); err != nil {
		return err
	}

	// Create the sentinel on targets[0], alongside the checkpoint, so all of
	// move's coordination tables live in one place (the source tables are
	// renamed out of the way at cutover). Idempotent (CREATE IF NOT EXISTS) so a
	// resume recreates it and a concurrent existence probe never sees it absent.
	if r.move.CreateSentinel {
		if err := sentinel.Create(ctx, r.targets[0].DB); err != nil {
			return err
		}
	}

	// Create checkpoint on the first target
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
			// Move always uses the buffered copier, which reads rows into client
			// memory; size the copy chunker by an in-memory byte budget rather than
			// copy time, whose signal collapses under write-side backpressure. The
			// checksum runs server-side and keeps the time signal.
			copyChunkerCfg := chunkerCfg
			copyChunkerCfg.TargetChunkBytes = r.move.TargetChunkSize
			copyChunker, err := table.NewChunker(tbl, copyChunkerCfg)
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
		Unbuffered:      false,     // move always uses the buffered copier
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
		if err := r.sources[i].replClient.Start(ctx); err != nil {
			return fmt.Errorf("failed to start repl client for source %d: %w", i, err)
		}
	}

	return nil
}

// checkpointTbl returns a handle to the checkpoint table on the first target
// (targets[0]) by convention. Targets are sorted in Run() so targets[0] is
// stable across a stop and a later resume. Move uses a per-run (non-shared)
// table that it owns outright.
func (r *Runner) checkpointTbl() *checkpoint.Table {
	// targets[0].DB is connected to that target's schema, so the checkpoint
	// table lands there — no schema is threaded in.
	return checkpoint.NewTable(r.targets[0].DB, checkpointTableName, checkpoint.Transient)
}

// createCheckpointTable creates the checkpoint table on the first target
// (targets[0]) by convention. Targets are sorted in Run() so targets[0] is
// stable across a stop and a later resume.
func (r *Runner) createCheckpointTable(ctx context.Context) error {
	if err := r.checkpointTbl().Create(ctx); err != nil {
		return err
	}
	tgt0 := &r.targets[0]
	r.checkpointTable = table.NewTableInfo(tgt0.DB, tgt0.Config.DBName, checkpointTableName)
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

	if r.move.ReverseWindow > 0 && len(r.move.SourceDSNs) > 1 {
		// Reverse-window is only defined for an unsharded source (1 source →
		// M targets ⇒ an M:1 reverse). A sharded source would need an M:N
		// reverse with a reverse sharding provider, which is out of scope.
		return fmt.Errorf("--reverse-window requires an unsharded (single) source, got %d source shards", len(r.move.SourceDSNs))
	}

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
	// sources[0] is the canonical source we read the table list and SHOW
	// CREATE TABLE from and the db handle the copier is constructed with, so
	// its identity must be stable across runs even if the caller constructed
	// SourceDSNs from a map. Sorting by addr/dbname rather than raw DSN
	// ensures stability across credential rotations or DSN parameter
	// reordering. (The checkpoint itself now lives on targets[0].)
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
		// Clone so the sort below does not reorder the caller's slice.
		r.targets = slices.Clone(r.move.Targets)
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
		r.logger.Debug("Created single target from TargetDSN")
	}
	// Sort targets by targetKey (addr/dbname/keyrange) for deterministic
	// ordering. The checkpoint is written to targets[0], so the order must be
	// stable across runs even if the caller constructed Targets from a map.
	// The ShardedApplier routes by key range, so slice order is otherwise
	// irrelevant to which target a row lands on.
	slices.SortFunc(r.targets, func(a, b applier.Target) int {
		return strings.Compare(targetKey(a), targetKey(b))
	})

	// If a prior run reached the reverse window (or a reverse cutover), resume
	// that here instead of the normal discovery/copy path. The forward cutover
	// renamed the source tables to _old, so the normal path would otherwise
	// treat them as fresh data and re-copy them. This must run before the
	// revert-marker pre-flight guard, since a resumed window legitimately honors
	// a marker created before the crash.
	if handled, err := r.maybeResumeReverseWindow(ctx); err != nil {
		return err
	} else if handled {
		return nil
	}

	// Pre-flight: refuse to start if a revert marker is already present on
	// targets[0]. It means a prior reverse-window move on this target requested a
	// rollback and did not complete cleanly, so the target is in an unknown state
	// — starting a new move on top of it is unsafe. (The marker is otherwise
	// created only during a reverse window and dropped by its terminal action.)
	if err := r.assertNoRevertMarker(ctx, "pre-flight"); err != nil {
		return err
	}
	// Discovery is read-only: preflight checks plus building the per-source
	// table lists (which the advisory lock names below are derived from).
	if err := r.setupDiscovery(ctx); err != nil {
		return err
	}

	if len(r.sourceTables) == 0 {
		// Because this is called from orchestration, there might be a bug where
		// it is asked to move *no tables*. Since there are no tables,
		// there is no:
		// - copier, replication changes
		// - advisory lock
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

	// Take an advisory lock on each source to prevent concurrent DDL.
	var advisoryLocks []*dbconn.AdvisoryLock
	for i := range r.sources {
		lock, err := dbconn.NewAdvisoryLock(ctx, r.sources[i].dsn, r.sources[i].tables, r.dbConfig, r.logger)
		if err != nil {
			for _, acquiredLock := range advisoryLocks {
				if closeErr := acquiredLock.Close(); closeErr != nil {
					r.logger.Error("failed to release advisory lock after acquisition failure", "error", closeErr)
				}
			}
			return fmt.Errorf("failed to acquire advisory lock on source %d: %w", i, err)
		}
		advisoryLocks = append(advisoryLocks, lock)
	}
	defer func() {
		for _, lock := range advisoryLocks {
			if err := lock.Close(); err != nil {
				r.logger.Error("failed to release advisory lock", "error", err)
			}
		}
	}()

	// Now that the locks are held, run the rest of setup. It includes steps
	// that modify the target (the resume path deletes rows above the
	// checkpointed watermark; --force drops the target tables), which must
	// never race with another spirit process moving the same sources: a
	// concurrent invocation has to die on the lock above before it can touch
	// the first run's target.
	if err := r.setupUnderLocks(ctx); err != nil {
		return err
	}

	r.startBackgroundRoutines(ctx)
	if err := r.setWatermarkOptimizationAll(ctx, true); err != nil {
		return err
	}

	r.status.Set(status.CopyRows)
	if err := r.copier.Run(ctx); err != nil {
		return err
	}

	// Disable both watermark optimizations so that all changes can be flushed.
	// For non-memory-comparable PKs this also drains the buffered map and
	// switches the subscription into FIFO queue mode (see
	// pkg/change/subscription_buffered.go), so the call can return an error.
	if err := r.setWatermarkOptimizationAll(ctx, false); err != nil {
		return err
	}
	if err := r.flushAllReplClients(ctx); err != nil {
		return err
	}

	r.logger.Info("All tables copied successfully")

	// Post-copy phase: drain the binlog, restore secondary indexes,
	// ANALYZE TABLE, run the initial checksum. While the sentinel blocks
	// cutover, waitOnSentinelTable runs a continuous checksum loop in the
	// background — see docs/move.md for the two-checksum model.
	if err := r.postCopyPhase(ctx); err != nil {
		return err
	}
	r.logger.Info("Initial checksum completed successfully")

	r.sentinelWaitStartTime = time.Now()
	r.status.Set(status.WaitingOnSentinelTable)
	// Block on the sentinel via the shared sentinel.Wait (poll/timeout timing
	// lives in the sentinel package). The continuous-checksum lifecycle and
	// watermark invalidation are move-specific (multi-source feeds;
	// invalidateChecksumWatermark blanks the whole per-move checkpoint table),
	// so they are injected as callbacks. See pkg/sentinel.
	if err := sentinel.Wait(ctx, sentinel.WaitConfig{
		Exists:              func(ctx context.Context) (bool, error) { return sentinel.Exists(ctx, r.targets[0].DB) },
		RunChecksum:         r.runContinuousChecksum,
		InvalidateWatermark: r.invalidateChecksumWatermark,
		Logger:              r.logger,
	}); err != nil {
		return err
	}

	if r.move.ReverseWindow > 0 {
		// From here the reverse window is the sole writer of the checkpoint row,
		// so stop the periodic checkpoint dumper first — otherwise it would
		// clobber the reverse-window phase / revert flag with a copy-phase row.
		r.stopWatchTask()
	}
	r.logger.Info("Sentinel released, starting cutover")
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
	if r.move.ReverseWindow > 0 {
		// Under the cutover lock, right after the traffic switch, capture the
		// reverse-feed start positions and record that the move has entered its
		// reverse window (see captureReverseWindow). The source rename still runs.
		cutover.SetPostSwitch(func(ctx context.Context) error { return captureReverseWindow(ctx, r) })
	}
	// Pre-cutover: refuse to switch traffic if a revert has been requested (a
	// marker appeared on targets[0] during the copy). Cutting over only to
	// immediately roll back is pointless — abort while the source is still live.
	if err := r.assertNoRevertMarker(ctx, "pre-cutover"); err != nil {
		return err
	}
	if err = cutover.Run(ctx); err != nil {
		return err
	}

	if r.move.ReverseWindow > 0 {
		// Hold the reverse window keeping the source current, then complete
		// forward (retire the source) or roll back. The checkpoint is dropped by
		// the terminal action, not here.
		return newReverseWindow(r).run(ctx)
	}

	// Delete checkpoint table from targets[0].
	tgt0 := &r.targets[0]
	if err := dbconn.Exec(ctx, tgt0.DB, "DROP TABLE IF EXISTS %n", checkpointTableName); err != nil {
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
		r.sources[i].replClient.StartPeriodicFlush(ctx, change.DefaultFlushInterval)
	}

	// Start go routines for checkpointing and dumping status. The returned
	// wait function is invoked from Close() so we can be sure no late
	// checkpoint INSERT lands after teardown begins. They run under a child
	// context so reverse-window mode can stop them independently of Run's ctx
	// (see stopWatchTask).
	bgCtx, bgCancel := context.WithCancel(ctx)
	r.bgCancel = bgCancel
	r.watchTaskWait = status.WatchTask(bgCtx, r, r.logger)
}

// stopWatchTask stops the background status/checkpoint dumper and waits for it
// to exit. Idempotent. Reverse-window mode calls it before cutover so the
// reverse window owns the checkpoint row exclusively — no periodic dump can
// clobber the reverse-window phase or the revert flag. Close() then skips the
// dumper because watchTaskWait is cleared.
func (r *Runner) stopWatchTask() {
	if r.bgCancel != nil {
		r.bgCancel()
		r.bgCancel = nil
	}
	if r.watchTaskWait != nil {
		r.watchTaskWait()
		r.watchTaskWait = nil
	}
}

// assertNoRevertMarker fails if the revert marker is present on targets[0].
// Called at pre-flight and pre-cutover (phase names the caller): a marker there
// means a reverse-window rollback is pending, or a prior reverse-window move did
// not complete, so neither starting a move nor cutting over makes sense.
func (r *Runner) assertNoRevertMarker(ctx context.Context, phase string) error {
	present, err := revertMarkerExists(ctx, r.targets[0].DB)
	if err != nil {
		return fmt.Errorf("%s: could not check for revert marker on targets[0]: %w", phase, err)
	}
	if present {
		return fmt.Errorf("%s: revert marker %q is present on targets[0]; a reverse-window rollback is pending or a prior move did not complete — investigate and drop it before proceeding", phase, revertMarkerName)
	}
	return nil
}

// fatalError is the callback provided to the replication client.
// It is called when a DDL change is detected on a subscribed table
// (change.FatalReasonSchemaChange), or when a fatal stream error occurs
// (change.FatalReasonStreamError). The replication client may perform
// its own logging either before or after invoking this callback, and DDL
// logging may be skipped entirely if this callback returns false.
//
// The return value indicates whether the replication client should treat
// the condition as fatal and stop the replication stream. It returns true
// when the error should be treated as fatal (and replication should be
// terminated and cleaned up), and false when the error should not be treated
// as fatal (in which case the client may continue without logging the DDL).
//
// The reason decides what happens to the checkpoint: a schema change
// invalidates it (resuming against a changed table definition could corrupt
// data), while a stream error preserves it — a dead binlog stream is exactly
// the failure checkpoint resume exists to recover from, so a re-run picks up
// the copy and replays the change stream from the checkpointed positions.
//
// fatalError is safe to call concurrently — every source's repl client is
// wired to this same callback, so a burst of fatal events from multiple
// binlog goroutines is realistic. fatalOnce makes the invalidate-and-cancel
// side effects idempotent and prevents racing with Close() teardown of
// r.checkpointTable / r.targets / r.cancelFunc.
func (r *Runner) fatalError(reason change.FatalReason) bool {
	if r.status.Get() >= status.CutOver {
		return false
	}
	r.fatalOnce.Do(func() {
		r.status.Set(status.ErrCleanup)
		switch reason { //nolint: exhaustive // schema change intentionally handled by default: drop is the safe fallback for unknown reasons
		case change.FatalReasonStreamError:
			// The stream died but the source tables are not known to have
			// changed, so the checkpoint remains valid. Keep it and tell the
			// operator how to recover.
			r.logger.Error("fatal replication stream error; the checkpoint has been preserved — re-run spirit to resume the move from it")
		default:
			// Schema change — and, defensively, any future reason we don't
			// recognize (invalidating is the safe default: it costs a restart,
			// while wrongly resuming could corrupt data).
			// Invalidate the checkpoint, so we don't try to resume.
			// If we don't do this, the move will permanently be blocked from proceeding.
			// Letting it start again is the better choice.
			// Use a background context since the move context may already be
			// cancelled. checkpointTable can still be nil if fatalError fires
			// during early setup, before createCheckpointTable runs — skip the
			// drop in that case.
			if r.checkpointTable != nil && len(r.targets) > 0 && r.targets[0].DB != nil {
				if err := dbconn.Exec(context.Background(), r.targets[0].DB, "DROP TABLE IF EXISTS %n", r.checkpointTable.TableName); err != nil {
					r.logger.Error("could not remove checkpoint",
						"error", err,
					)
				}
			}
		}
		// cancelFunc can be nil during early setup or in test paths that
		// bypass Run; nil-check before calling.
		if r.cancelFunc != nil {
			r.cancelFunc() // cancel the move context
		}
	})
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
		return fmt.Sprintf("migration status: state=%s copy-progress=%s binlog-deltas=%v total-time=%s copier-time=%s copier-remaining-time=%v copier-is-throttled=%v%s",
			r.status.Get().String(),
			r.copier.GetProgress(),
			r.getDeltaLenAll(),
			time.Since(r.startTime).Round(time.Second),
			time.Since(r.copier.StartTime()).Round(time.Second),
			r.copier.GetETA(),
			r.copier.GetThrottler().IsThrottled(),
			applier.StatusSuffix(r.applier),
		)
	case status.WaitingOnSentinelTable:
		return fmt.Sprintf("migration status: state=%s total-time=%s sentinel-wait-time=%s sentinel-max-wait-time=%s",
			r.status.Get().String(),
			time.Since(r.startTime).Round(time.Second),
			time.Since(r.sentinelWaitStartTime).Round(time.Second),
			sentinel.WaitLimit,
		)
	case status.ApplyChangeset, status.PostChecksum:
		// We've finished copying rows, and we are now trying to reduce the number of binlog deltas before
		// proceeding to the checksum and then the final cutover.
		return fmt.Sprintf("migration status: state=%s binlog-deltas=%v total-time=%s%s",
			r.status.Get().String(),
			r.getDeltaLenAll(),
			time.Since(r.startTime).Round(time.Second),
			applier.StatusSuffix(r.applier),
		)
	case status.Checksum:
		// This could take a while if it's a large table.
		return fmt.Sprintf("migration status: state=%s checksum-progress=%s binlog-deltas=%v total-time=%s checksum-time=%s",
			r.status.Get().String(),
			r.checker.GetProgress().String(),
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
		GTID:           r.move.EnableExperimentalGTID,
		MoveEverything: len(r.move.SourceTables) == 0,
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
		// All sources have identical schemas (enforced by the source_schema_consistency
		// check at ScopePostSetup), so use sources[0].
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

// postCopyPhase runs the work that happens between copy-rows and the
// sentinel wait: drain the binlog backlog, restore secondary indexes
// (if deferred), run ANALYZE TABLE, and perform the initial checksum.
// When create-sentinel is not in use this is also the last phase
// before cutover.
func (r *Runner) postCopyPhase(ctx context.Context) error {
	// Flush all pending events, but leave the periodic flush running until
	// just before the checksum. With --defer-secondary-indexes,
	// restoreSecondaryIndexes issues one giant ALTER per table that can run
	// for hours, and both it and ANALYZE TABLE permit concurrent DML on the
	// target: adding a regular secondary index is online (INPLACE) DDL in
	// InnoDB, so the applier can keep draining deltas while they run
	// (mirroring pkg/datasync's restoreSecondaryIndexes). If flushing
	// stopped here instead, deltas would accumulate until the buffered
	// subscription parks the binlog reader on its soft memory limit (see
	// pkg/change/subscription_buffered.go), unread binlog would pile up
	// server-side, and a source purging binlogs past the reader's position
	// during an hours-long index build would fail the move fatally.
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
			// Unqualified on target.DB: the old code qualified with the source
			// schema, which doesn't exist on a cross-cluster target (and breaks
			// through a vtgate). See analyzeTable.
			if err := r.analyzeTable(ctx, target.DB, tbl.TableName); err != nil {
				return err
			}
		}
	}

	// Now stop the periodic flush: the checksum requires it. The checker
	// must be the only flusher — its initConnPool acquires
	// LOCK TABLES ... WRITE on sources and targets and flushes the
	// remaining deltas on the lock connections, and a periodic flush
	// executing DML against those locked tables from regular connections
	// would deadlock with the lock holder (see the same rule in
	// SingleChecker.runChecksum). There is no flush gap of concern here:
	// the checker flushes again before locking, restarts the periodic
	// flush itself for the long chunk-verification phase, and stops it
	// when it finishes.
	r.stopPeriodicFlushAll()

	// On resume from checkpoint, r.checksumWatermark carries the high-water
	// mark from a previous run. With the sentinel wait now after the initial
	// checksum, a crash during that wait should resume from the completed
	// checksum watermark rather than re-running the entire initial pass.
	if r.checksumWatermark != "" {
		if err := r.checksumChunker.OpenAtWatermark(r.checksumWatermark); err != nil {
			return err
		}
	} else {
		if err := r.checksumChunker.Open(); err != nil {
			return err
		}
	}
	defer utils.CloseAndLog(r.checksumChunker)

	// Perform a checksum operation
	// Collect all source DBs and repl clients for the checksum.
	sourceDBs := make([]*sql.DB, len(r.sources))
	feeds := make([]change.Source, len(r.sources))
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
	// On a checker error we just propagate. The DumpCheckpoint invariant
	// guarantees that any persisted checksum_watermark describes only
	// verified-clean chunks, so a resumed run either replays the checksum
	// from scratch (empty watermark, set whenever the failing pass had any
	// repair) or resumes safely from a watermark that came from a clean
	// pass. See pkg/migration/runner.go DumpCheckpoint for the full
	// rationale.
	return r.checker.Run(ctx)
}

// analyzeTable runs ANALYZE TABLE for tableName on db, unqualified: db is
// already connected to the target schema, and qualifying would be wrong
// through a Vitess vtgate. It reads the result set rather than using Exec
// because ANALYZE reports a missing table as a Msg_type="Error" row (not a
// statement error), which would otherwise be a silent no-op.
func (r *Runner) analyzeTable(ctx context.Context, db *sql.DB, tableName string) error {
	stmt, err := sqlescape.EscapeSQL("ANALYZE TABLE %n", tableName)
	if err != nil {
		return err
	}
	rows, err := db.QueryContext(ctx, stmt)
	if err != nil {
		return err
	}
	defer utils.CloseAndLog(rows)
	for rows.Next() {
		// ANALYZE TABLE returns: Table, Op, Msg_type, Msg_text.
		var tbl, op, msgType, msgText string
		if err := rows.Scan(&tbl, &op, &msgType, &msgText); err != nil {
			return err
		}
		// Only Msg_type = "Error" indicates the statistics were not refreshed.
		// Other rows ("status", "note", "warning") are not failures even when
		// Msg_text is not "OK" (e.g. "Table is already up to date"); accept
		// them, logging anything non-OK as a warning for visibility.
		if strings.EqualFold(msgType, "error") {
			return fmt.Errorf("ANALYZE TABLE %s failed: %s: %s", tableName, msgType, msgText)
		}
		if !strings.EqualFold(msgText, "OK") && r.logger != nil {
			r.logger.Warn("ANALYZE TABLE reported a non-OK message",
				"table", tableName,
				"msg_type", msgType,
				"msg_text", msgText,
			)
		}
	}
	return rows.Err()
}

func (r *Runner) SetCutover(cutover func(ctx context.Context) error) {
	r.cutoverFunc = cutover
}

// SetReverseCutover registers the rollback traffic switch used if a revert is
// requested during the reverse window (route back to the source). It mirrors
// SetCutover and is only consulted when move.ReverseWindow > 0.
func (r *Runner) SetReverseCutover(fn func(ctx context.Context) error) {
	r.reverseCutoverFunc = fn
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
			"sentinel-table", fmt.Sprintf("%s.%s", r.targets[0].Config.DBName, sentinel.TableName),
			"total-time", time.Since(r.startTime).Round(time.Second).String(),
			"sentinel-wait-time", time.Since(r.sentinelWaitStartTime).Round(time.Second).String(),
			"sentinel-max-wait-time", sentinel.WaitLimit.String(),
		)
	case status.ApplyChangeset, status.PostChecksum:
		summary = fmt.Sprintf("Applying Changeset Deltas=%v", r.getDeltaLenAll())
	case status.Checksum:
		summary = "Checksum Progress=" + r.checker.GetProgress().String()
	default:
		summary = ""
	}
	return status.Progress{
		CurrentState: r.status.Get(),
		Summary:      summary,
	}
}

// invalidateChecksumWatermark blanks the checksum_watermark on the persisted
// checkpoint rows if (and only if) the sentinel-wait continuous checker
// recorded any repaired chunks. Called from the sentinel-abort path: the
// periodic dumper already refuses to persist a watermark once the difference
// counter is non-zero, but the difference can be recorded between a dump's
// condition read and its INSERT — this UPDATE, serialized against the dumper
// via checkpointMu, runs strictly after any such in-flight INSERT and
// guarantees resume re-verifies from the start of the checksum phase.
// Unlike pkg/migration there is no statement scoping: a move owns its
// target's checkpoint table outright (resume reads the latest row
// unfiltered). Mirrors pkg/migration/runner.go.
func (r *Runner) invalidateChecksumWatermark(ctx context.Context) error {
	r.checkpointMu.Lock()
	defer r.checkpointMu.Unlock()
	if r.continuousChecker == nil || r.continuousChecker.DifferencesFound() == 0 {
		return nil
	}
	r.logger.Warn("continuous checksum found differences; clearing persisted checksum watermark so the next run re-verifies from the start of the checksum phase")
	return dbconn.Exec(ctx, r.targets[0].DB, "UPDATE %n SET checksum_watermark = %?",
		r.checkpointTable.TableName,
		"",
	)
}

// runContinuousChecksum loops calling a fresh distributed checker over the
// source/target tables for as long as ctx is alive. It is the "continuous"
// half of the two-checksum model (see docs/move.md) and is only called while
// the move is blocked in WaitingOnSentinelTable.
//
// The checker used here is separate from r.checker and uses a fresh chunker
// so checkpoint state is unaffected. Single-threaded by design — checksum
// throttling is tracked separately in github.com/block/spirit/issues/831.
func (r *Runner) runContinuousChecksum(ctx context.Context) error {
	chunker, err := r.buildContinuousChunker()
	if err != nil {
		return fmt.Errorf("failed to build continuous-checksum chunker: %w", err)
	}
	if err := chunker.Open(); err != nil {
		return fmt.Errorf("failed to open continuous-checksum chunker: %w", err)
	}
	defer utils.CloseAndLog(chunker)

	sourceDBs := make([]*sql.DB, len(r.sources))
	feeds := make([]change.Source, len(r.sources))
	for i := range r.sources {
		sourceDBs[i] = r.sources[i].db
		feeds[i] = r.sources[i].replClient
	}
	checker, err := checksum.NewChecker(sourceDBs, chunker, feeds, &checksum.CheckerConfig{
		// TODO(#831): once the throttler can size threads dynamically,
		// replace the hard-coded 1 with the move's thread count.
		Concurrency:     1,
		TargetChunkTime: r.move.TargetChunkTime,
		DBConfig:        r.dbConfig,
		Logger:          r.logger,
		Applier:         r.applier,
		FixDifferences:  true,
		// One pass per outer-loop iteration; the continuous-checksum
		// loop itself supplies the retry, so we don't nest a second
		// retry loop inside each iteration.
		MaxRetries: 1,
	})
	if err != nil {
		return fmt.Errorf("failed to create continuous checker: %w", err)
	}
	// Publish the checker so DumpCheckpoint (on the WatchTask goroutine)
	// can consult its DifferencesFound() when deciding whether the
	// persisted checksum_watermark is still trustworthy. Published before
	// the first pass starts, so there is no window where a difference
	// could be recorded while the dumper still believes the tables clean —
	// the checker increments its counter atomically *before* repairing.
	r.checkpointMu.Lock()
	r.continuousChecker = checker
	r.checkpointMu.Unlock()

	iteration := 0
	var lastDuration time.Duration // zero before the first iteration → full interval wait
	for {
		// Wait the minimum interval (less time already spent in the prior
		// iteration). On the first iteration this is the full interval, so
		// we don't kick off a checksum immediately after the initial pass.
		// Run StartPeriodicFlush on every source during the wait — the
		// checker.Run inside each iteration starts its own per-source, but
		// the inter-iteration gap (up to continuousChecksumMinInterval)
		// sits outside that lifetime. Without flushing here, binlog deltas
		// accumulate during the wait and have to be drained under the
		// cutover's table lock.
		if remaining := continuousChecksumMinInterval - lastDuration; remaining > 0 {
			r.logger.Info("continuous checksum waiting before next iteration", "wait", remaining.Round(time.Second).String())
			for i := range r.sources {
				r.sources[i].replClient.StartPeriodicFlush(ctx, change.DefaultFlushInterval)
			}
			timer := time.NewTimer(remaining)
			select {
			case <-ctx.Done():
				timer.Stop()
				for i := range r.sources {
					r.sources[i].replClient.StopPeriodicFlush()
				}
				return nil
			case <-timer.C:
			}
			for i := range r.sources {
				r.sources[i].replClient.StopPeriodicFlush()
			}
		}
		if err := ctx.Err(); err != nil {
			return nil
		}
		// Reset the chunker so this iteration scans the table from the start.
		// (Skipped on the very first pass — the chunker is already freshly Open'd.)
		if iteration > 0 {
			if err := chunker.Reset(); err != nil {
				return fmt.Errorf("failed to reset continuous-checksum chunker: %w", err)
			}
		}
		iteration++
		iterationStart := time.Now()
		r.logger.Info("continuous checksum iteration starting", "iteration", iteration)
		runErr := checker.Run(ctx)
		if runErr != nil {
			// Only suppress a `context.Canceled` that came from OUR ctx
			// being cancelled (the sentinel was dropped while a pass was
			// in flight) AND no mismatch was detected during the pass.
			// If `DifferencesFound() > 0`, the fix path may have run only
			// partway through — the distributed Apply step's workers run
			// with the parent context and can be cancelled mid-write — so
			// propagate the failure to abort cutover. A nested
			// `DeadlineExceeded` from fixChunkTimeout always propagates
			// (stuck recopy).
			if ctx.Err() != nil && errors.Is(runErr, context.Canceled) && checker.DifferencesFound() == 0 {
				return nil
			}
			return runErr
		}
		lastDuration = time.Since(iterationStart)
		r.logger.Info("continuous checksum iteration complete",
			"iteration", iteration,
			"duration", lastDuration.Round(time.Second).String(),
		)
	}
}

// buildContinuousChunker builds a fresh chunker for the continuous-checksum
// loop. It is deliberately not wired into r.checksumChunker / checkpoint.
func (r *Runner) buildContinuousChunker() (table.Chunker, error) {
	chunkers := make([]table.Chunker, 0)
	for i := range r.sources {
		for _, tbl := range r.sources[i].tables {
			chunkerCfg := table.ChunkerConfig{
				TargetChunkTime: r.move.TargetChunkTime,
				Logger:          r.logger,
			}
			c, err := table.NewChunker(tbl, chunkerCfg)
			if err != nil {
				return nil, err
			}
			chunkers = append(chunkers, c)
		}
	}
	return table.NewMultiChunker(chunkers...), nil
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
	// Collect per-source positions (opaque strings owned by the source
	// implementation), keyed by sourceKey (addr/dbname).
	positions := make(map[string]string)
	for i := range r.sources {
		positions[r.sources[i].sourceKey()] = r.sources[i].replClient.Position()
	}
	positionsJSON, err := json.Marshal(positions)
	if err != nil {
		return fmt.Errorf("failed to marshal binlog positions: %w", err)
	}

	copierWatermark, err := r.copyChunker.GetLowWatermark()
	if err != nil {
		return status.ErrWatermarkNotReady // it might not be ready, we can try again.
	}
	// Safety invariant: only persist the checksum_watermark if the current
	// checksum pass has had zero differences. The chunker advances its
	// low-watermark past every chunk it sees Feedback() for, including
	// chunks that needed a recopy — but a recopy isn't a verification.
	// Reading DifferencesFound() *after* the watermark catches any chunk
	// in the watermark that was repaired (the per-chunk path increments
	// differencesFound strictly before chunker.Feedback). When set,
	// suppress the watermark so a restart re-validates from the start of
	// the checksum phase. See pkg/migration/runner.go DumpCheckpoint for
	// the full rationale.
	//
	// The same invariant applies to the sentinel-wait continuous checker
	// (a separate object from r.checker — see continuousChecker): once it
	// has repaired any chunk, the watermark here is the stale
	// end-of-initial-checksum one, and resuming from it would verify only
	// the trailing chunks — silently neutralizing the deliberate abort the
	// continuous checksum triggers on divergence. So the watermark is
	// persisted only while BOTH checkers are clean (or the continuous one
	// doesn't exist yet).
	var checksumWatermark string
	if r.status.Get() >= status.Checksum && r.checker != nil {
		wm, wmErr := r.checksumChunker.GetLowWatermark()
		if wmErr != nil {
			return status.ErrWatermarkNotReady
		}
		if r.checker.DifferencesFound() == 0 &&
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
		"binlog-positions", string(positionsJSON))
	// Statement and OriginalTableName are unused by move; Position carries the
	// JSON map of per-source change-feed positions.
	if err := r.checkpointTbl().Write(ctx, checkpoint.Record{
		CopierWatermark:   copierWatermark,
		ChecksumWatermark: checksumWatermark,
		Position:          string(positionsJSON),
	}); err != nil {
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
		r.logger.Debug("Created SingleTargetApplier")
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

// deleteRecopyRange deletes rows at or above the copier's resume position
// — the watermark chunk's LOWER bound — from all target tables. The deleted
// range deliberately coincides with the range the resumed copier re-copies
// (the chunkers restart from the watermark chunk's lower bound, inclusive),
// which closes two races with the keyAboveWatermark optimization:
//
// During normal copying, the keyAboveWatermark optimization discards binlog
// events for keys the copier "hasn't reached yet" — these rows don't exist
// in the target, so deletes/updates for them can be safely ignored. But after
// a crash, rows at or above the resume position may have already been copied
// to the target:
//
//   - a row above the watermark chunk's upper bound was copied before the
//     crash; a DELETE for it arrives after resume and is discarded, leaving a
//     phantom row on the target (the classic race).
//   - subtler: a row INSIDE the watermark chunk was copied, and its source
//     DELETE committed in the unflushed window just before the crash. After
//     resume the row no longer exists on the source, so the recopy (which
//     reads the current source snapshot) never touches it, and its replayed
//     DELETE can be discarded — the key sits above the post-crash source max
//     that seeds checkpointHighPtr (see OpenAtWatermark). Deleting only rows
//     above the chunk's upper bound resurrects such rows at cutover.
//
// In the migration path, this is solved by reading the target table's max
// value and temporarily disabling the optimization up to that point. In the
// move path, the target is on a different server, so we can't cheaply read
// its max value. Instead, we delete exactly what the resumed copy is about to
// re-copy: afterwards a target row exists iff it is below the resume position,
// and every deleted row is re-created iff it still exists on the source.
// Deleting already-copied rows inside the watermark chunk is safe — the
// copier re-copies that range from the current source snapshot before the
// checksum and cutover, and binlog replay converges concurrent changes.
func (r *Runner) deleteRecopyRange(ctx context.Context, copierWatermark string) error {
	// The checkpoint watermark format depends on how many chunkers the copy
	// chunker wraps: a single (source, table) pair stores that chunker's own
	// watermark (raw chunk JSON for auto-inc PKs, or the composite chunker's
	// envelope), while multiple pairs store a JSON map keyed by
	// table.QualifiedName(). WatermarkPerTable normalizes every format into
	// a per-table map of raw chunk JSON.
	allTables := make([]*table.TableInfo, 0, len(r.sources)*len(r.sourceTables))
	for i := range r.sources {
		allTables = append(allTables, r.sources[i].tables...)
	}
	watermarks, err := table.WatermarkPerTable(copierWatermark, allTables...)
	if err != nil {
		return fmt.Errorf("failed to parse copier watermark: %w", err)
	}
	for _, src := range allTables {
		// A table without a watermark entry was not ready when the
		// checkpoint was written. On resume the chunker restarts it from
		// scratch (multiChunker.OpenAtWatermark falls back to Open()), so
		// the recopy range is the whole table and every row already copied
		// to the target must be deleted before the recopy.
		recopyClause := "1=1"
		watermark, hasWatermark := watermarks[src.QualifiedName()]
		if hasWatermark {
			recopyClause, err = table.WatermarkRecopyClause(src, watermark)
			if err != nil {
				return fmt.Errorf("failed to parse watermark for table %s: %w", src.TableName, err)
			}
		}
		// With multiple sources, tables with the same name from different
		// sources interleave in the same target table, so each source's
		// DELETE may also remove another source's rows below that source's
		// own watermark. Those rows are NOT recopied (each source's chunker
		// resumes from its own watermark), so this is only safe because the
		// initial checksum (FixDifferences) then runs a FULL pass that
		// detects and repairs the missing rows before cutover — enforced by
		// resumeFromCheckpoint discarding any persisted checksum watermark
		// when there is more than one source. Deleting too little would
		// leave phantom rows, which is the race this function exists to
		// prevent.
		for i, target := range r.targets {
			deleteStmt := fmt.Sprintf("DELETE FROM %s WHERE %s",
				src.QuotedTableName, recopyClause)
			result, err := target.DB.ExecContext(ctx, deleteStmt)
			if err != nil {
				return fmt.Errorf("failed to delete the recopy range on target %d table %s: %w", i, src.TableName, err)
			}
			rowsDeleted, _ := result.RowsAffected()
			if rowsDeleted > 0 {
				r.logger.Info("deleted rows at/above the copier resume position from target",
					"target", i,
					"table", src.TableName,
					"rowsDeleted", rowsDeleted,
					"watermark", watermark,
				)
			}
		}
	}
	return nil
}

// setWatermarkOptimizationAll sets watermark optimization on all replication
// clients. Each subscription may drain its outgoing store on the toggle (see
// pkg/change/subscription_buffered.go), so this can return the drain error.
func (r *Runner) setWatermarkOptimizationAll(ctx context.Context, enabled bool) error {
	for i := range r.sources {
		if err := r.sources[i].replClient.SetWatermarkOptimization(ctx, enabled); err != nil {
			return err
		}
	}
	return nil
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
