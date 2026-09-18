package checksum

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
)

const hotSnapshotMaxBytes = 64 * 1024

// hotSnapshot is finite evidence, not a long-lived database transaction. Target
// membership is read BEFORE the source images so an already-present orphan is
// not hidden by checking only source rows. Later changes remain replication's
// responsibility, just as they do after an ordinary chunk has passed.
// One worker owns the snapshot at a time; retries carry it through the queue.
type hotSnapshot struct {
	pending       map[string]hotSnapshotRow
	targetDB      *sql.DB
	chunk         *table.Chunk
	targetColumns string
	attempts      int
}

type hotSnapshotRow struct {
	key     []table.Datum
	crc     uint64
	present bool
}

// captureHotSnapshot reads at most 128 rows from each side. A nil snapshot means
// the range outgrew the fallback's row/byte budget; ordinary retries still apply.
func captureHotSnapshot(ctx context.Context, sourceDB, targetDB *sql.DB, chunk *table.Chunk) (*hotSnapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	sourceColumns, targetColumns, err := chunk.ColumnMapping.ChecksumExprs()
	if err != nil {
		return nil, err
	}
	target, size, oversized, err := readHotSnapshotRows(ctx, targetDB, chunk, chunk.NewTable, targetColumns, chunk.String(), int(hotSplitTargetRows))
	if err != nil || oversized {
		return nil, err
	}
	source, sourceSize, oversized, err := readHotSnapshotRows(ctx, sourceDB, chunk, chunk.Table, sourceColumns, chunk.String(), int(hotSplitTargetRows))
	if err != nil || oversized {
		return nil, err
	}
	if size+sourceSize > hotSnapshotMaxBytes {
		return nil, nil
	}
	pending := make(map[string]hotSnapshotRow, len(target)+len(source))
	for key, row := range target {
		row.present = false // not in the source snapshot unless overwritten below
		pending[key] = row
	}
	maps.Copy(pending, source)
	return &hotSnapshot{pending: pending, targetDB: targetDB, chunk: chunk, targetColumns: targetColumns}, nil
}

// check removes only obligations actually observed satisfied on the target.
// It neither refreshes source images nor adds new rows to the frozen work set.
func (s *hotSnapshot) check(ctx context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	s.attempts++
	if len(s.pending) == 0 {
		return true, nil
	}
	predicates := make([]string, 0, len(s.pending))
	for _, row := range s.pending {
		point := *s.chunk
		point.LowerBound = &table.Boundary{Value: row.key, Inclusive: true}
		point.UpperBound = &table.Boundary{Value: row.key, Inclusive: true}
		predicates = append(predicates, "("+point.String()+")")
	}
	rows, _, oversized, err := readHotSnapshotRows(ctx, s.targetDB, s.chunk, s.chunk.NewTable, s.targetColumns, strings.Join(predicates, " OR "), 2*int(hotSplitTargetRows))
	if err != nil {
		return false, err
	}
	if oversized {
		return false, nil
	}
	// MySQL collation can match a predicate while returning a different byte
	// representation (e.g. a case-only key change). Never interpret that as
	// absence of a target-only key. Leave the snapshot unresolved instead.
	for key := range rows {
		if _, known := s.pending[key]; !known {
			return false, nil
		}
	}
	for key, expected := range s.pending {
		actual, exists := rows[key]
		if (!expected.present && !exists) || (expected.present && exists && actual.crc == expected.crc) {
			delete(s.pending, key)
		}
	}
	return len(s.pending) == 0, nil
}

// readHotSnapshotRows preserves tuple identity without delimiter collisions.
// Temporal keys are cast to their server representation to preserve fractional
// seconds/zero dates with parseTime=true; predicates still use native key types.
func readHotSnapshotRows(ctx context.Context, db *sql.DB, chunk *table.Chunk, info *table.TableInfo, columns, predicate string, limit int) (map[string]hotSnapshotRow, int, bool, error) {
	if len(chunk.Key) == 0 {
		return nil, 0, false, fmt.Errorf("snapshot range has no key")
	}
	projections := make([]string, len(chunk.Key))
	types := make([]string, len(chunk.Key))
	for i, key := range chunk.Key {
		tp, ok := info.GetColumnMySQLType(key)
		if !ok {
			return nil, 0, false, fmt.Errorf("missing snapshot key type for %s", key)
		}
		types[i] = tp
		projections[i] = table.QuoteColumns([]string{key})
		base, _, _ := strings.Cut(strings.ToUpper(tp), "(")
		switch base {
		case "DATE", "DATETIME", "TIMESTAMP", "TIME":
			projections[i] = "CAST(" + projections[i] + " AS CHAR)"
		}
	}
	query := fmt.Sprintf("SELECT %s, CRC32(CONCAT(%s)) FROM %s WHERE %s LIMIT %d", strings.Join(projections, ","), columns, info.QuotedTableName, predicate, limit+1)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, 0, false, err
	}
	defer utils.CloseAndLog(rows)
	result := make(map[string]hotSnapshotRow)
	size, count := 0, 0
	for rows.Next() {
		count++
		if count > limit {
			return nil, size, true, nil
		}
		values := make([][]byte, len(chunk.Key))
		dest := make([]any, len(chunk.Key)+1)
		for i := range values {
			dest[i] = &values[i]
		}
		var crc uint64
		dest[len(values)] = &crc
		if err := rows.Scan(dest...); err != nil {
			return nil, size, false, err
		}
		key := make([]table.Datum, len(values))
		var identity []byte
		for i, value := range values {
			size += len(value) + 8
			if size > hotSnapshotMaxBytes {
				return nil, size, true, nil
			}
			identity = binary.BigEndian.AppendUint64(identity, uint64(len(value)))
			identity = append(identity, value...)
			key[i], err = table.NewDatumFromValue(value, types[i])
			if err != nil {
				return nil, size, false, err
			}
		}
		if _, exists := result[string(identity)]; exists {
			return nil, size, false, fmt.Errorf("duplicate snapshot key")
		}
		result[string(identity)] = hotSnapshotRow{key: key, crc: crc, present: true}
	}
	return result, size, false, rows.Err()
}
