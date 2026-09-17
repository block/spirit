package checksum

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/block/spirit/pkg/table"
)

const (
	hotSplitDepthLimit   = 32
	hotSplitPassLimit    = 1024
	hotSplitQueryTimeout = 30 * time.Second
)

// trySplitHot only runs after two successive source changes. Splits are bounded
// independently of retries, so resetting child evidence cannot make a pass
// unbounded. A failed SQL query fails verification, never marks a range clean.
func (c *ContinuousChecker) trySplitHot(ctx context.Context, res *workResult) bool {
	item := res.item
	if !c.cfg.SplitHotChunks || item.point || item.splitDepth >= hotSplitDepthLimit || item.consecutiveSrcChanged < 1 {
		return false
	}
	if c.splitAttempts.Add(1) > hotSplitPassLimit {
		return false
	}
	res.children, res.err = c.splitChunk(ctx, item.chunk, res.newSrc.count)
	return res.err != nil || len(res.children) != 0
}

// splitHotChunk partitions the complete parent predicate, not just the rows
// currently present. Actual SQL key ordering supports composite, textual, and
// binary keys without inventing a numeric midpoint or comparing keys in Go.
func splitHotChunk(ctx context.Context, db *sql.DB, parent *table.Chunk, rows uint64) ([]*table.Chunk, error) {
	ctx, cancel := context.WithTimeout(ctx, hotSplitQueryTimeout)
	defer cancel()
	if len(parent.Key) == 0 || parent.Table == nil {
		return nil, errors.New("hot range has no source key metadata")
	}
	keys := table.QuoteColumns(parent.Key)
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s ORDER BY %s LIMIT 1 OFFSET ?", keys, parent.Table.QuotedTableName, parent.String(), keys)
	values := make([]any, len(parent.Key))
	pointers := make([]any, len(values))
	for i := range values {
		pointers[i] = &values[i]
	}
	err := db.QueryRowContext(ctx, query, rows/2).Scan(pointers...)
	// The count came from a prior read: deletes may have removed the median.
	// Retry at the first existing key; an empty source is left to normal retry.
	if errors.Is(err, sql.ErrNoRows) && rows/2 > 0 {
		err = db.QueryRowContext(ctx, query, 0).Scan(pointers...)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("select hot range split key: %w", err)
	}
	pivot := make([]table.Datum, len(values))
	for i, name := range parent.Key {
		tp, ok := parent.Table.GetColumnMySQLType(name)
		if !ok {
			return nil, fmt.Errorf("missing split key type for %s", name)
		}
		pivot[i], err = table.NewDatumFromValue(values[i], tp)
		if err != nil {
			return nil, fmt.Errorf("decode split key %s: %w", name, err)
		}
	}
	left, point, right := *parent, *parent, *parent
	left.UpperBound = &table.Boundary{Value: pivot, Inclusive: false}
	point.LowerBound = &table.Boundary{Value: pivot, Inclusive: true}
	point.UpperBound = &table.Boundary{Value: pivot, Inclusive: true}
	right.LowerBound = &table.Boundary{Value: pivot, Inclusive: false}
	left.ChunkSize = max(uint64(1), rows/2)
	point.ChunkSize = 1
	right.ChunkSize = max(uint64(1), rows/2)
	return []*table.Chunk{&left, &point, &right}, nil
}
