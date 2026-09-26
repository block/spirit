package checksum

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
)

// rowQuerier is the subset of *sql.DB / *sql.Tx that inspectDifferences needs.
// The snapshot checker hands it the REPEATABLE READ transaction the chunk
// mismatched under; the lockless checker has no snapshot to hand over and uses
// the pool directly.
type rowQuerier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// inspectDifferences logs a line per diverged row in the chunk: mismatched
// checksum, missing on the target, or missing on the source. It is diagnostic
// only — the caller has already decided the chunk is diverged, and nothing here
// changes that verdict.
//
// The reads are per-row, so this is materially more expensive than the
// aggregate the checksum itself runs. It is only called on a chunk already
// known to be diverged, which is rare by construction.
//
// A chunk read outside a snapshot (the lockless path) can report rows that are
// concurrently changing rather than truly diverged. That costs log noise, not a
// wrong verdict.
func inspectDifferences(ctx context.Context, q rowQuerier, chunk *table.Chunk, logger *slog.Logger) error {
	sourceChecksumCols, targetChecksumCols, err := chunk.ColumnMapping.ChecksumExprs()
	if err != nil {
		return err
	}
	sourceRows, err := q.QueryContext(ctx, fmt.Sprintf(queryTemplate,
		sourceChecksumCols,
		table.QuoteColumns(chunk.Table.KeyColumns),
		chunk.Table.QuotedTableName,
		chunk.String(),
	))
	if err != nil {
		return fmt.Errorf("failed to query source rows: %w", err)
	}
	defer utils.CloseAndLog(sourceRows)

	// Build map of source checksums
	sourceChecksums := make(map[string]string) // pk -> checksum
	for sourceRows.Next() {
		var checksum, pk string
		if err := sourceRows.Scan(&checksum, &pk); err != nil {
			return fmt.Errorf("failed to scan source row: %w", err)
		}
		sourceChecksums[pk] = checksum
	}
	if err := sourceRows.Err(); err != nil {
		return fmt.Errorf("error iterating source rows: %w", err)
	}

	targetRows, err := q.QueryContext(ctx, fmt.Sprintf(queryTemplate,
		targetChecksumCols,
		table.QuoteColumns(chunk.NewTable.KeyColumns),
		chunk.NewTable.QuotedTableName,
		chunk.String(),
	))
	if err != nil {
		return fmt.Errorf("failed to query target rows: %w", err)
	}
	defer utils.CloseAndLog(targetRows)

	// Build map of target checksums and compare
	targetChecksums := make(map[string]string) // pk -> checksum
	for targetRows.Next() {
		var checksum, pk string
		if err := targetRows.Scan(&checksum, &pk); err != nil {
			return fmt.Errorf("failed to scan target row: %w", err)
		}
		targetChecksums[pk] = checksum

		// Check if this row exists in source and has different checksum
		if sourceChecksum, exists := sourceChecksums[pk]; exists {
			if sourceChecksum != checksum {
				logger.Warn("inspection revealed row checksum mismatch", "pk", pk, "sourceChecksum", sourceChecksum, "targetChecksum", checksum)
			}
		} else {
			logger.Warn("inspection revealed row does not exist in source", "pk", pk)
		}
	}
	if err := targetRows.Err(); err != nil {
		return fmt.Errorf("error iterating target rows: %w", err)
	}

	// Check for rows that exist in source but not in target
	for pk, sourceChecksum := range sourceChecksums {
		if _, exists := targetChecksums[pk]; !exists {
			logger.Warn("inspection revealed row does not exist in target", "pk", pk, "sourceChecksum", sourceChecksum)
		}
	}

	return nil // managed to inspect differences
}
