package checksum

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
)

// chunkRepairer rewrites a diverged chunk on the target from the source, on a
// single MySQL server. It is the repair path for BOTH single-server checkers:
// SingleChecker calls it when a chunk mismatches under its snapshot, and the
// lockless checker is handed one as its Recopier when the caller asked for
// repairs (CheckerConfig.FixDifferences). Sharing the implementation is what
// makes the two algorithms repair identically rather than nearly-identically —
// in particular both go through the caller's ColumnMapping, so a repair is
// correct for an ALTER that renames or drops columns. (MySQLRecopier, the
// cross-server implementation used by `spirit sync`, reads
// NonGeneratedColumns instead: there is no mapping between two copies of the
// same logical table.)
//
// The operation is:
//
//  1. DELETE the chunk's key range on the target — this is what removes rows
//     the source no longer has, which a pure upsert could never do.
//  2. SELECT the chunk's rows from the source into Spirit.
//  3. Write them back through the applier, the same buffered write path the
//     copier and the binlog apply use.
//
// Repairs are serialized on recopyLock, one chunk at a time. Historically,
// concurrent DELETE + REPLACE on overlapping chunks deadlocked on a UNIQUE
// secondary index (an idempotence key), not on the PRIMARY:
//
//	*** (1) TRANSACTION: ... updating or deleting
//	DELETE FROM `<snip>`.`_<snip>_new` WHERE `id` >= 1108588365 AND `id` < 1108688365
//	*** (1) HOLDS THE LOCK(S): ... index idempotence_key_idx ... lock_mode X locks rec but not gap
//	*** (2) TRANSACTION: ... inserting
//	REPLACE INTO `<snip>`.`_<snip>_new` ... WHERE `id` >= 1106488365 AND `id` < 1106588365
//	*** WE ROLL BACK TRANSACTION (1)
//
// The DELETE and the rewrite are deliberately not one transaction: the DELETE
// must land first so since-deleted rows are removed, and splitting them shrinks
// the lock footprint. They do run under a context derived from
// context.WithoutCancel, so a cancellation between the two cannot leave the
// chunk deleted-but-not-rewritten; the bounded fixChunkTimeout still catches a
// hung repair. The applier's workers are started under that same context, so a
// cancelled parent cannot strand queued inserts either.
//
// Two behaviours are inherited from the applier and worth knowing about:
//
//   - It writes with INSERT IGNORE, not REPLACE. Rows inside the key range were
//     just deleted so nothing there conflicts; a row that collides on a UNIQUE
//     secondary key with a row *outside* the range is skipped rather than
//     clobbering it. The chunk then stays diverged, the next attempt re-flags
//     it, and attempts exhaust into a hard error — the correct outcome for a
//     lossy ALTER such as adding a UNIQUE index to non-unique data. The skipped
//     count is logged.
//   - JSON columns are read bare, with no round-trip cast. The read/write pair
//     is already text-mediated, so a repaired row lands as exactly the
//     one-text-round-trip image the checksum's source side predicts. Casting on
//     top would apply parse∘render twice, which does not converge for the
//     doubles MySQL's JSON text parser misrounds — see castExpr in pkg/table.
//
// The read is not synchronized with the change feed: a row deleted on the
// source after the repair reads it is written back if the feed has already
// applied that DELETE to the target. The chunk stays diverged, the next attempt
// repairs it again, and it converges once churn on that key range stops.
// Cut-over requires a pass that finds no differences at all, so sustained
// delete churn costs attempts, never a bad cut-over.
type chunkRepairer struct {
	db       *sql.DB
	applier  applier.Applier
	dbConfig *dbconn.DBConfig
	logger   *slog.Logger

	// recopyLock serializes Recopy calls. See the type doc for rationale.
	recopyLock sync.Mutex
}

var _ Recopier = (*chunkRepairer)(nil)

// newChunkRepairer builds the single-server repair path. app is the write path
// repairs go through; it is started and stopped around each repair rather than
// held for the repairer's lifetime, because repairs are rare and serialized.
func newChunkRepairer(db *sql.DB, app applier.Applier, dbConfig *dbconn.DBConfig, logger *slog.Logger) *chunkRepairer {
	if dbConfig == nil {
		dbConfig = dbconn.NewDBConfig()
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &chunkRepairer{db: db, applier: app, dbConfig: dbConfig, logger: logger}
}

// Recopy rewrites the chunk's rows on the target from the source. See the type
// doc for the operation's shape, its locking, and its two applier-inherited
// behaviours.
//
// Note that the chunk is dynamically sized based on the target-time it took to
// *read* the data in the checksum, which can be substantially longer than the
// time it takes to copy it.
func (r *chunkRepairer) Recopy(ctx context.Context, chunk *table.Chunk) error {
	start := time.Now()
	r.logger.Warn("recopying chunk via DELETE + Apply", "chunk", chunk.String())

	r.recopyLock.Lock()
	defer r.recopyLock.Unlock()

	deleteStmt := "DELETE FROM " + chunk.NewTable.QuotedTableName + " WHERE " + chunk.String()

	fixCtx, fixCancel := context.WithTimeout(context.WithoutCancel(ctx), fixChunkTimeout)
	defer fixCancel()
	if _, err := dbconn.RetryableTransaction(fixCtx, r.db, dbconn.ErrorOnDupKey, r.dbConfig, deleteStmt); err != nil {
		return fmt.Errorf("failed to delete existing rows: %w", err)
	}

	// The applier is started per repair rather than for the repairer's
	// lifetime. Repairs are rare and serialized by recopyLock, so the goroutine
	// churn is irrelevant next to the work itself, and in exchange the repair is
	// fully self-contained: the workers live under fixCtx, and the Stop() below
	// joins them, so no write can outlive (or be dropped by) the repair that
	// queued it. The start is deferred until there is actually something to
	// write — a source range that is entirely empty is repaired by the DELETE
	// alone.
	applierStarted := false
	defer func() {
		if !applierStarted {
			return
		}
		if err := r.applier.Stop(); err != nil {
			r.logger.Warn("failed to stop repair applier", "error", err)
		}
	}()

	// Read the source rows for the chunk. This is deliberately a plain,
	// non-locking consistent read of *current* data — not a read inside any
	// checksum snapshot: by the time we repair, that snapshot is stale and what
	// the target needs is the source as it is now.
	//
	// The column list is the source/target intersection (with renames applied on
	// the target side), which is exactly what the applier expects: row values are
	// positional, values[i] belongs to sourceColumns[i].
	//
	// FORCE INDEX (PRIMARY) for the same reason the copier's read of this shape
	// does (pkg/copier/buffered.go): the predicate is a range over the primary
	// key, and misleading statistics on a wide table can otherwise talk the
	// optimizer into a scan.
	sourceColumns, _ := chunk.ColumnMapping.ColumnsSlice()
	sourceColumnList, _ := chunk.ColumnMapping.Columns()
	query := fmt.Sprintf("SELECT %s FROM %s FORCE INDEX (PRIMARY) WHERE %s",
		sourceColumnList,
		chunk.Table.QuotedTableName,
		chunk.String(),
	)
	rows, err := r.db.QueryContext(fixCtx, query)
	if err != nil {
		return fmt.Errorf("failed to read source chunk: %w", err)
	}
	// Closed early on the success path (see below) to hand the connection back
	// before waiting on the writers; this covers the early returns until then.
	defer func() {
		if rows != nil {
			utils.CloseAndLog(rows)
		}
	}()

	// Apply() is asynchronous: it hands the batch to the write workers and
	// returns, so reads and writes pipeline. Callbacks run on the applier's
	// coordinator goroutine, hence the mutex; they must never block, so results
	// are accumulated here and inspected after Wait().
	var (
		resultMu      sync.Mutex
		firstApplyErr error
		appliedRows   int64
	)
	callback := func(affectedRows int64, applyErr error) {
		resultMu.Lock()
		defer resultMu.Unlock()
		appliedRows += affectedRows
		if applyErr != nil && firstApplyErr == nil {
			firstApplyErr = applyErr
		}
	}

	var (
		batch      [][]any
		batchBytes int
		sourceRows int64
	)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if !applierStarted {
			if err := r.applier.Start(fixCtx); err != nil {
				return fmt.Errorf("failed to start repair applier: %w", err)
			}
			applierStarted = true
		}
		if err := r.applier.Apply(fixCtx, chunk, batch, callback); err != nil {
			return fmt.Errorf("failed to submit rows for rewrite: %w", err)
		}
		// The applier owns the batch now (it keeps the slices), so start a fresh
		// one rather than reusing the backing array.
		batch, batchBytes = nil, 0
		return nil
	}
	for rows.Next() {
		values := make([]any, len(sourceColumns))
		valuePtrs := make([]any, len(sourceColumns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("failed to scan source row: %w", err)
		}
		batch = append(batch, values)
		batchBytes += applier.EstimateRowSize(values)
		sourceRows++
		if len(batch) >= repairBatchRows || batchBytes >= repairBatchBytes {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to read source chunk: %w", err)
	}
	if err := flush(); err != nil {
		return err
	}
	// Return the read connection before waiting on the writers. They take their
	// connections from the same pool, so holding this one across the wait would
	// be one connection of headroom given up for nothing. Nil it out so the
	// deferred close does not close it a second time.
	utils.CloseAndLog(rows)
	rows = nil

	// Wait for every submitted batch to be written and its callback to have run.
	// Repairs are serialized on recopyLock, so on a private applier there is no
	// other pending work this could be waiting on; on a shared one (the migration
	// runner passes the applier the copy phase used) the copy has long finished
	// and the binlog feed only uses the synchronous write methods, which do not
	// register pending work.
	if applierStarted {
		if err := r.applier.Wait(fixCtx); err != nil {
			return fmt.Errorf("failed waiting for chunk rewrite: %w", err)
		}
	}
	resultMu.Lock()
	defer resultMu.Unlock()
	if firstApplyErr != nil {
		return fmt.Errorf("failed to rewrite chunk data: %w", firstApplyErr)
	}
	if appliedRows < sourceRows {
		// INSERT IGNORE skipped rows: see the note on UNIQUE secondary keys in
		// the type doc. The chunk is still diverged, so this is reported rather
		// than swallowed — the next attempt re-flags it.
		//
		// One-directional: appliedRows is a sum of SQL rows-affected, and
		// dbconn.RetryableTransaction accumulates that across its retry attempts
		// (a statement that succeeded but whose COMMIT was then retried counts
		// twice), so an inflated count can hide this warning. That costs a log
		// line, not correctness — the divergence itself is caught by the next
		// attempt's checksum either way.
		r.logger.Warn("recopying chunk did not rewrite every source row; a UNIQUE key conflict outside the chunk range is the usual cause",
			"chunk", chunk.String(),
			"sourceRows", sourceRows,
			"writtenRows", appliedRows,
		)
	}
	r.logger.Info("chunk recopied",
		"chunk", chunk.String(),
		"rowCount", sourceRows,
		"elapsed", time.Since(start).Round(time.Millisecond).String(),
	)
	return nil
}
