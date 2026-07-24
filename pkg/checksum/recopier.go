package checksum

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
)

// MySQLRecopier is the production Recopier used by `spirit sync`. Given a
// chunk that the continuous checker has identified as stably diverged
// (source CRC unchanged across the retry window, target still wrong), it
// rewrites the chunk's rows on the target from the source.
//
// The operation is the cross-DB analog of SingleChecker.replaceChunk:
//
//  1. DELETE the chunk's key range on the target.
//  2. SELECT the chunk's rows from the source.
//  3. Hand the rows to the applier's Apply method, which upserts them on
//     the target through the same write path the change feed uses.
//
// Recopies are serialized by an internal mutex. Concurrent DELETE +
// INSERT on overlapping chunks can deadlock on secondary indexes (see the
// transcript in single.go around line 211); serialization keeps the
// fix-up path safe at the cost of a small stall if multiple chunks need
// recopy at once. Since stable divergence is rare, this is acceptable.
//
// The DELETE and Apply run under a context derived from
// context.WithoutCancel(ctx) so a parent cancellation between them does
// not leave the target with rows deleted but not yet rewritten. A bounded
// timeout (10 minutes) still protects against a hung Apply.
type MySQLRecopier struct {
	sourceDB *sql.DB
	targetDB *sql.DB
	applier  applier.Applier
	dbConfig *dbconn.DBConfig
	logger   *slog.Logger

	// recopyLock serializes Recopy calls. See struct doc for rationale.
	recopyLock sync.Mutex
}

// Compile-time interface assertion.
var _ Recopier = (*MySQLRecopier)(nil)

// NewMySQLRecopier constructs a recopier for the source/target pair. The
// applier must be Started before Recopy is called (the production wiring
// in datasync.Runner starts the applier during the copy phase and leaves
// it running through continuous sync, so this is satisfied naturally).
func NewMySQLRecopier(sourceDB, targetDB *sql.DB, app applier.Applier, dbConfig *dbconn.DBConfig, logger *slog.Logger) (*MySQLRecopier, error) {
	if sourceDB == nil {
		return nil, errors.New("sourceDB must be non-nil")
	}
	if targetDB == nil {
		return nil, errors.New("targetDB must be non-nil")
	}
	if app == nil {
		return nil, errors.New("applier must be non-nil")
	}
	if dbConfig == nil {
		dbConfig = dbconn.NewDBConfig()
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &MySQLRecopier{
		sourceDB: sourceDB,
		targetDB: targetDB,
		applier:  app,
		dbConfig: dbConfig,
		logger:   logger,
	}, nil
}

// Recopy rewrites the chunk's rows on the target from the source. See
// MySQLRecopier's struct doc for the operation's shape and concurrency
// rules.
func (r *MySQLRecopier) Recopy(ctx context.Context, chunk *table.Chunk) error {
	r.recopyLock.Lock()
	defer r.recopyLock.Unlock()

	start := time.Now()
	r.logger.Warn("continuous checksum: recopying chunk via DELETE + Apply", "chunk", chunk.String())

	// Detach from parent cancellation between DELETE and Apply (see
	// struct doc). The 10m timeout matches fixChunkTimeout used by the
	// migration-side recopy.
	fixCtx, fixCancel := context.WithTimeout(context.WithoutCancel(ctx), fixChunkTimeout)
	defer fixCancel()

	// Step 1: Delete the chunk range on the target. The chunk's
	// NewTable holds the target-side table info; for sync (same logical
	// table on both sides) NewTable.QuotedTableName == Table.QuotedTableName.
	deleteStmt := fmt.Sprintf("DELETE FROM %s WHERE %s", chunk.NewTable.QuotedTableName, chunk.String())
	if _, err := dbconn.RetryableTransaction(fixCtx, r.targetDB, dbconn.ErrorOnDupKey, r.dbConfig, deleteStmt); err != nil {
		return fmt.Errorf("delete target chunk range: %w", err)
	}

	// Step 2: Read source rows for the chunk. We use NonGeneratedColumns
	// because the applier's write path expects exactly those (it can't
	// write generated columns). The column ordering matches the applier's
	// expectation as long as we use the same column-list helper the
	// distributed checker's recopy uses. JSON columns are deliberately read
	// bare — the SELECT+applier pair already constitutes the one text
	// round-trip the checksum's JSON contract expects; see the matching
	// comment in DistributedChecker.replaceChunk.
	columnList := table.QuoteColumns(chunk.Table.NonGeneratedColumns)
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s",
		columnList,
		chunk.Table.QuotedTableName,
		chunk.String(),
	)
	rows, err := r.sourceDB.QueryContext(fixCtx, query)
	if err != nil {
		return fmt.Errorf("read source chunk: %w", err)
	}
	defer utils.CloseAndLog(rows)

	var rowData [][]any
	for rows.Next() {
		values := make([]any, len(chunk.Table.NonGeneratedColumns))
		valuePtrs := make([]any, len(chunk.Table.NonGeneratedColumns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("scan source row: %w", err)
		}
		rowData = append(rowData, values)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate source rows: %w", err)
	}

	// Step 3: Apply the source rows to the target via the applier. If
	// the source range is empty (every source row was deleted) the
	// DELETE on its own is sufficient — Apply with zero rows would still
	// fire callbacks but does no real work, so we just skip it.
	if len(rowData) == 0 {
		r.logger.Info("continuous checksum: recopy deleted target chunk; source range is empty", "chunk", chunk.String())
		return nil
	}

	done := make(chan error, 1)
	if err := r.applier.Apply(fixCtx, chunk, rowData, func(affectedRows int64, applyErr error) {
		if applyErr != nil {
			done <- applyErr
			return
		}
		r.logger.Debug("continuous checksum: recopy applier write complete",
			"chunk", chunk.String(),
			"affected_rows", affectedRows,
		)
		done <- nil
	}); err != nil {
		return fmt.Errorf("submit applier write: %w", err)
	}
	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("applier write: %w", err)
		}
	case <-fixCtx.Done():
		return fixCtx.Err()
	}

	r.logger.Info("continuous checksum: chunk recopied",
		"chunk", chunk.String(),
		"row_count", len(rowData),
		"elapsed", time.Since(start).Round(time.Millisecond).String(),
	)
	return nil
}
