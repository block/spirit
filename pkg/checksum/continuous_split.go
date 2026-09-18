package checksum

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/block/spirit/pkg/table"
)

const hotSplitQueryTimeout = 30 * time.Second

// splitHotChunk partitions the complete parent predicate, not just the rows
// currently present. Actual SQL key ordering supports composite, textual, and
// binary keys without inventing a numeric midpoint or comparing keys in Go.
func splitHotChunk(ctx context.Context, db *sql.DB, parent *table.Chunk, rows uint64) ([]*table.Chunk, error) {
	// Counts come from the latest checksum read. Singleton and empty ranges
	// retain their retry evidence rather than creating empty siblings.
	if rows <= 1 {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(ctx, hotSplitQueryTimeout)
	defer cancel()
	if len(parent.Key) == 0 || parent.Table == nil {
		return nil, errors.New("hot range has no source key metadata")
	}
	keys := table.QuoteColumns(parent.Key)
	// Preserve the server's temporal representation even with parseTime=true,
	// including fractional seconds and zero dates. Ordering remains on native keys.
	projections := make([]string, len(parent.Key))
	for i, name := range parent.Key {
		projections[i] = table.QuoteColumns([]string{name})
		tp, ok := parent.Table.GetColumnMySQLType(name)
		if !ok {
			return nil, fmt.Errorf("missing split key type for %s", name)
		}
		base, _, _ := strings.Cut(strings.ToUpper(tp), "(")
		switch base {
		case "DATE", "DATETIME", "TIMESTAMP", "TIME":
			projections[i] = "CAST(" + projections[i] + " AS CHAR)"
		}
	}
	queryPrefix := fmt.Sprintf("SELECT %s FROM %s WHERE %s ORDER BY %s LIMIT 1 OFFSET ", strings.Join(projections, ","), parent.Table.QuotedTableName, parent.String(), keys)
	values := make([]any, len(parent.Key))
	pointers := make([]any, len(values))
	for i := range values {
		pointers[i] = &values[i]
	}
	// Vitess can lose a prepared LIMIT parameter and send NULL
	// to MySQL. The offset is an internal uint64, so a decimal literal avoids
	// that path without interpolating any untrusted SQL.
	err := db.QueryRowContext(ctx, queryPrefix+strconv.FormatUint(rows/2, 10)).Scan(pointers...)
	// The count came from a prior read: deletes may have removed the median.
	// Retry at the first existing key; an empty source is left to normal retry.
	if errors.Is(err, sql.ErrNoRows) {
		err = db.QueryRowContext(ctx, queryPrefix+"0").Scan(pointers...)
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
