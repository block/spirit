package checksum

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
)

// A hot range is one whose source keeps changing under the reader. The snapshot
// fallback (lockless_snapshot.go) freezes the source row images for such a range
// and waits for the target to show them, which converges for a range that is
// merely busy — but not for a row that is written continuously. There, the
// frozen image is stale before the first poll, every attempt observes a
// different source, and the range is deferred to the next pass with no verdict
// at all. It is deferred again on the next pass for the same reason. A genuinely
// diverged hot row and a merely busy one are indistinguishable, forever.
//
// Settling is the terminal step for exactly that case, and it works by giving up
// on reading the source at all.
//
// Any comparison between a SELECT of the source and a read of the target is
// between a source image at one position and a target state at a later one.
// Closing that window means stopping the writes, and for a row that is written
// continuously the window is never empty. But the change stream already carries
// the answer: with binlog_row_image=FULL, an event's after-image *is* the
// source's value for that row at that position. MySQL says so; no read is
// needed, and nothing has to hold still.
//
// So for each row still outstanding:
//
//  1. Ask the feed to wait for the next change to that row and park there.
//  2. Flush, so the target holds exactly that image and nothing past it.
//  3. Compare the target row to the event's image.
//
// A mismatch at step 3 is a real inconsistency: the feed delivered that image
// and the flush applied it, so apply lag cannot explain a difference.
//
// This terminates in the opposite direction from a lock: the more often the row
// is written, the sooner its next event arrives. The rows that defeat every
// read-and-compare strategy are exactly the ones this settles fastest, and a row
// quiet enough that no event arrives inside the budget is one the ordinary poll
// was already converging on.
//
// Rows are settled one at a time. The escalation is rare by construction — a
// range reaches it only after exhausting MaxHotAttempts — and serializing keeps
// the feed's park a single piece of state rather than a set of overlapping holds.
//
// With several sources (a move), each row is settled on the feed of the source
// that owns it: the hot snapshot records which source each row was read from,
// and a key read from two sources never becomes a snapshot at all (see
// readHotSnapshotRowsAcross). The other feeds keep applying their own changes,
// which is safe because none of them writes that key. A row the target holds
// and no source does has no owner, so with several sources it defers.
const (
	// settleBudget bounds the whole escalation: every row's wait for its next
	// change, each flush, and each comparison. Overrunning it is not an error,
	// it defers, exactly as before this path existed.
	settleBudget = 5 * time.Second

	// settleRowBudget bounds one row's wait. A genuinely hot row produces a
	// change in milliseconds, so a row that does not is not the case this
	// exists for — and spending the whole budget on it would starve the rows
	// behind it.
	settleRowBudget = time.Second
)

// settleVerdict is the outcome of trying to settle a hot range.
type settleVerdict int

const (
	// settleUnavailable means no verdict was reached: there is no feed, no
	// change arrived inside the budget, the flush could not land it, or a
	// change landed in the middle of the comparison. The caller defers the
	// range exactly as it did before this path existed.
	settleUnavailable settleVerdict = iota

	// settleClean means every outstanding row was observed holding the image
	// the stream had just delivered for it. The range is verified.
	settleClean

	// settleDiverged means at least one was not. The feed had just applied that
	// row's image, so nothing could still be in flight for it.
	settleDiverged
)

func (v settleVerdict) String() string {
	switch v {
	case settleUnavailable:
		return "unavailable"
	case settleClean:
		return "clean"
	case settleDiverged:
		return "diverged"
	default:
		return fmt.Sprintf("settleVerdict(%d)", int(v))
	}
}

// rowSettler performs the escalation described at the top of this file. It is
// deliberately a type of its own rather than more methods on LocklessChecker:
// settling needs the sources, their feeds, and somewhere to log, and nothing
// else about a running check. feeds[i] is the feed of sourceDBs[i].
type rowSettler struct {
	sourceDBs []*sql.DB
	feeds     []change.Source
	logger    *slog.Logger
}

// newRowSettler returns nil when there is no feed. That is not a failure — the
// feed parking at the watched change is the whole mechanism, and library
// callers may have none — so a nil settler answers settleUnavailable to
// everything and the caller defers exactly as it did before settling existed.
func newRowSettler(sourceDBs []*sql.DB, feeds []change.Source, logger *slog.Logger) *rowSettler {
	if len(feeds) == 0 || len(feeds) != len(sourceDBs) {
		return nil
	}
	return &rowSettler{sourceDBs: sourceDBs, feeds: feeds, logger: logger}
}

// owner returns the source a row is settled against, and its feed: the source
// the row was read from. A row only the target holds has no owner; with one
// source that is still the only feed its next change can arrive on, and with
// several there is no way to know which, so it gets a nil feed (defer).
func (s *rowSettler) owner(row hotSnapshotRow) (*sql.DB, change.Source) {
	i := row.source
	if i < 0 && len(s.feeds) == 1 {
		i = 0
	}
	if i < 0 || i >= len(s.feeds) {
		return nil, nil
	}
	return s.sourceDBs[i], s.feeds[i]
}

// settle returns settleUnavailable rather than an error for every condition
// that only means "not this time" — a row that went quiet, a flush that could
// not land, a change that arrived mid-comparison — because the caller's
// response to all of them is the deferral it would have done anyway. An error
// is reserved for a read that failed in a way worth surfacing.
func (s *rowSettler) settle(ctx context.Context, snapshot *hotSnapshot) (settleVerdict, error) {
	if s == nil || len(snapshot.pending) == 0 {
		return settleUnavailable, nil
	}

	parent := ctx
	ctx, cancel := context.WithTimeout(ctx, settleBudget)
	defer cancel()

	// Settled rows leave snapshot.pending as they are verified, so a run that
	// gives up part way still banks the progress: the next attempt waits only
	// for what is left.
	for key, row := range snapshot.pending {
		verdict, err := s.settleRow(ctx, parent, snapshot, row)
		if err != nil {
			return settleUnavailable, err
		}
		switch verdict {
		case settleClean:
			delete(snapshot.pending, key)
		case settleDiverged:
			return settleDiverged, nil
		case settleUnavailable:
			return settleUnavailable, nil
		}
	}
	return settleClean, nil
}

// settleRow waits for one row's next change and verifies the target against it.
// parent is the caller's context, used only to tell our own budget expiring
// (defer) from the run being cancelled (propagate).
func (s *rowSettler) settleRow(ctx, parent context.Context, snapshot *hotSnapshot, row hotSnapshotRow) (settleVerdict, error) {
	chunk := snapshot.chunk
	db, feed := s.owner(row)
	if feed == nil {
		s.logger.Debug("lockless checksum: watched row has no single owning source; deferring",
			"chunk", chunk.String())
		return settleUnavailable, nil
	}
	want, err := binlogKey(ctx, db, chunk.Table, chunk.Key, row.key)
	if err != nil {
		return settleUnavailable, err
	}
	matcher, err := keyMatcher(chunk.Table, chunk.Key, want)
	if err != nil {
		return settleUnavailable, err
	}
	rowCtx, cancel := context.WithTimeout(ctx, settleRowBudget)
	defer cancel()

	var verdict settleVerdict
	err = feed.VerifyRowAtNextChange(rowCtx, change.RowWatch{
		Schema: chunk.Table.SchemaName,
		Table:  chunk.Table.TableName,
		Match:  matcher,
	}, func(ctx context.Context, _, image []any, deleted bool) error {
		var err error
		verdict, err = compareRowToImage(ctx, db, snapshot, row, image, deleted)
		return err
	})
	switch {
	case parent.Err() != nil:
		// The run itself is going away; that is not a verdict and not ours to
		// swallow.
		return settleUnavailable, parent.Err()
	case errors.Is(err, change.ErrRowRewritten):
		s.logger.Debug("lockless checksum: watched row was rewritten mid-verification; deferring",
			"chunk", chunk.String())
		return settleUnavailable, nil
	case errors.Is(err, change.ErrFlushIncomplete):
		// The image may not have reached the target, so any difference we
		// found would be apply lag wearing a divergence's clothes.
		s.logger.Debug("lockless checksum: feed could not drain while parked; deferring",
			"chunk", chunk.String())
		return settleUnavailable, nil
	case errors.Is(err, context.DeadlineExceeded):
		s.logger.Debug("lockless checksum: no change arrived for the watched row inside its budget; deferring",
			"chunk", chunk.String())
		return settleUnavailable, nil
	case err != nil:
		return settleUnavailable, err
	}
	return verdict, nil
}

// compareRowToImage answers the only question the parked feed leaves open: does
// the target hold what the stream just delivered?
//
// A delete is the simple half — the row must be gone. For an image, the expected
// value is computed by evaluating the *same* checksum expressions the rest of the
// checker uses against the image itself (see expectedImageCRC), so a type change
// or a column rename is normalised exactly as it is everywhere else rather than
// by a second, parallel notion of equality written in Go.
//
// sourceDB is the owning source, used only to evaluate the checksum expressions.
func compareRowToImage(ctx context.Context, sourceDB *sql.DB, snapshot *hotSnapshot, row hotSnapshotRow, image []any, deleted bool) (settleVerdict, error) {
	chunk := snapshot.chunk
	predicate, err := pointPredicate(chunk, row.key)
	if err != nil {
		return settleUnavailable, err
	}
	actual, _, oversized, err := readHotSnapshotRowsAcross(ctx, snapshot.targetDBs, chunk, chunk.NewTable, snapshot.targetColumns, predicate, 2)
	if err != nil {
		return settleUnavailable, fmt.Errorf("read target row while settling %s: %w", chunk.String(), err)
	}
	if oversized || len(actual) > 1 {
		// A key representation that changed under collation can match the
		// predicate and come back different. Never read that as a verdict.
		return settleUnavailable, nil
	}
	if deleted {
		if len(actual) == 0 {
			return settleClean, nil
		}
		return settleDiverged, nil
	}
	if len(actual) == 0 {
		return settleDiverged, nil // the stream just wrote it; the target has no row
	}
	expected, err := expectedImageCRC(ctx, sourceDB, chunk, image)
	if err != nil {
		return settleUnavailable, err
	}
	for _, got := range actual {
		if got.crc != expected {
			return settleDiverged, nil
		}
	}
	return settleClean, nil
}

// expectedImageCRC evaluates the source side of the checksum expressions against
// a binlog row image, giving the CRC the target must hold for that row.
//
// The image is rendered as a one-row derived table whose column *types* come
// from the real table: the first UNION branch selects the mapped columns from it
// with a false predicate, so it contributes names and types and no rows, and the
// second supplies the values. That matters because ChecksumExprs casts every
// column to the target's type before hashing — the same normalisation an
// ordinary chunk read gets — and those casts have to be applied to a column of
// the right type, not to whatever a bare parameter would be typed as.
//
// The merge is not a substitute for binding the right value, though: see
// imageValueExpr for the two types where it produces the wrong rendering.
func expectedImageCRC(ctx context.Context, sourceDB *sql.DB, chunk *table.Chunk, image []any) (uint64, error) {
	sourceExprs, _, err := chunk.ColumnMapping.ChecksumExprs()
	if err != nil {
		return 0, err
	}
	castTps, err := chunk.ColumnMapping.ChecksumCastTypes()
	if err != nil {
		return 0, err
	}
	columns, _ := chunk.ColumnMapping.ColumnsSlice()
	ordinals := chunk.ColumnMapping.SourceOrdinalIndices()
	values := make([]any, len(ordinals))
	placeholders := make([]string, len(ordinals))
	for i, ordinal := range ordinals {
		if ordinal >= len(image) {
			return 0, fmt.Errorf("binlog row image has %d columns, need ordinal %d", len(image), ordinal)
		}
		if expr, val, ok := charsetImageValueExpr(chunk.Table, columns[i], image[ordinal]); ok {
			placeholders[i], values[i] = expr, val
			continue
		}
		tp, _ := chunk.Table.GetColumnMySQLType(columns[i])
		placeholders[i], values[i], err = imageValueExpr(tp, castTps[i], image[ordinal])
		if err != nil {
			return 0, fmt.Errorf("render column %s of the binlog row image: %w", columns[i], err)
		}
	}
	query := fmt.Sprintf("SELECT CRC32(CONCAT(%s)) FROM (SELECT %s FROM %s WHERE 1=0 UNION ALL SELECT %s) AS img",
		sourceExprs, sqlescape.EscapeIdentifierList(columns), chunk.Table.QuotedTableName,
		strings.Join(placeholders, ","))
	var crc uint64
	if err := sourceDB.QueryRowContext(ctx, query, values...).Scan(&crc); err != nil {
		return 0, fmt.Errorf("evaluate checksum over binlog row image: %w", err)
	}
	return crc, nil
}

// imageValueExpr renders one column of a binlog row image into the value branch
// of expectedImageCRC's derived table, as an expression and the value to bind.
// tp is the source column's type; castTp is the type the checksum casts the
// column to (see ColumnMapping.ChecksumCastTypes), which comes from the target
// and so can be a different type family from tp.
//
// A bare "?" is right for most types and wrong for two, because UNION type
// merging *widens*: a value whose Go type is wider than the column takes the
// wider type into the merge, and the checksum's cast then renders it the way
// that wider type would be rendered rather than the way the column is.
//
//   - FLOAT is bound as a float64 (DecodeBinlogRow widens the binlog's
//     float32, and database/sql widens every float before binding). Merged
//     with the column that is a DOUBLE, and a char cast (a FLOAT -> VARCHAR
//     change) renders 0.1 as "0.10000000149011612" where the real row gives
//     "0.1". Casting the parameter back to FLOAT restores the column's
//     precision, so the merge is FLOAT with FLOAT.
//   - BIT is decoded as an int64, and what the real column renders depends on
//     the cast. A numeric cast (BIT -> BIT is cast to unsigned, BIT -> INT to
//     signed) renders its value, so the image binds it as a uint64: a BIT(64)
//     with the top bit set decodes negative, and binding the int64 as-is would
//     render -1 where the real row renders 18446744073709551615. Any other
//     cast (BIT -> VARCHAR is cast to char, BIT -> VARBINARY to binary)
//     renders the column's raw big-endian bytes — ceil(N/8) of them, at the
//     source column's width, so 0x05 for BIT(8) and 0x0000000000000005 for
//     BIT(64) — and the image binds those bytes. Either binding under the
//     other cast is wrong: CAST(x'05' AS signed) is 0, and a uint64 cast to
//     char renders "5".
//
// Both are silent: the query succeeds and returns a CRC that simply is not the
// row's, so every hot row in a table with a FLOAT or BIT column settles to a
// false divergence. Every other type the checksum handles renders the same
// either way — TestExpectedImageCRCMatchesRealRow pins that over the ones where
// storage and text differ.
func imageValueExpr(tp, castTp string, v any) (string, any, error) {
	if v == nil {
		return "?", nil, nil // NULL renders as NULL under every cast
	}
	switch baseColumnType(tp) {
	case "float", "float unsigned":
		return "CAST(? AS FLOAT)", v, nil
	case "bit":
		var u uint64
		switch n := v.(type) {
		case int64:
			u = uint64(n)
		case uint64:
			u = n
		default:
			return "", nil, fmt.Errorf("binlog decoded a %s column as %T, want an integer", tp, v)
		}
		if isNumericCast(castTp) {
			return "?", u, nil
		}
		bits, err := bitWidth(tp)
		if err != nil {
			return "", nil, err
		}
		raw := make([]byte, (bits+7)/8)
		for i := len(raw) - 1; i >= 0; i-- {
			raw[i] = byte(u)
			u >>= 8
		}
		return "?", raw, nil
	}
	return "?", v, nil
}

// charsetImageValueExpr renders a string value of a binlog row image for a
// column in a charset other than utf8mb4 or utf8mb3. ok is false for any other
// column or value, which imageValueExpr renders.
//
// A plain parameter is utf8mb4 text. Merged with such a column it is either
// the wrong value or an error: MySQL converts it to the column's charset, and
// refuses to (1267, illegal mix of collations) when it is not ASCII.
//
//   - A CHAR, VARCHAR or TEXT value is the column's own bytes (see
//     table.TableInfo.BinlogCharset). Read as utf8mb4, latin1 C3 A9 ('Ã©')
//     would be 'é', and a utf16 value would not be text at all. The bytes are
//     bound as hex and relabelled with their charset, which UNHEX's binary
//     result takes without conversion.
//   - An ENUM or SET value is the element text DecodeBinlogRow took from the
//     column definition, which is utf8mb4. It is converted to the column's
//     charset.
//
// Either way the merge is the column's charset with the column's charset, as
// in the real row.
func charsetImageValueExpr(info *table.TableInfo, column string, v any) (string, any, bool) {
	var b []byte
	switch s := v.(type) {
	case string:
		b = []byte(s)
	case []byte:
		b = s
	default:
		return "", nil, false
	}
	if charset := info.BinlogCharset(column); charset != "" {
		return "CONVERT(UNHEX(?) USING " + charset + ")", hex.EncodeToString(b), true
	}
	tp, _ := info.GetColumnMySQLType(column)
	charset, _ := info.GetColumnCharset(column)
	if !utils.IsEnumOrSetType(tp) || charset == "" || charset == "utf8mb4" || charset == "utf8mb3" {
		return "", nil, false
	}
	return "CONVERT(? USING " + charset + ")", string(b), true
}

// baseColumnType strips a declared type's width, so "bit(8)" and
// "float(10,2) unsigned" reduce to what imageValueExpr switches on.
func baseColumnType(tp string) string {
	tp = strings.ToLower(strings.TrimSpace(tp))
	open := strings.IndexByte(tp, '(')
	closing := strings.IndexByte(tp, ')')
	if open < 0 || closing < open {
		return tp
	}
	return strings.TrimSpace(tp[:open] + " " + strings.TrimSpace(tp[closing+1:]))
}

// isNumericCast reports whether a checksum cast type (see
// table.ColumnMapping.ChecksumCastTypes) renders a number rather than bytes.
func isNumericCast(castTp string) bool {
	return castTp == "signed" || castTp == "unsigned" || castTp == "double" || castTp == "float" || strings.HasPrefix(castTp, "decimal")
}

// bitWidth reads N out of "bit(N)". A BIT column with no width is BIT(1).
func bitWidth(tp string) (int, error) {
	tp = strings.ToLower(strings.TrimSpace(tp))
	open := strings.IndexByte(tp, '(')
	closing := strings.IndexByte(tp, ')')
	if open < 0 {
		return 1, nil
	}
	if closing < open {
		return 0, fmt.Errorf("malformed bit type %q", tp)
	}
	bits, err := strconv.Atoi(strings.TrimSpace(tp[open+1 : closing]))
	if err != nil || bits < 1 || bits > 64 {
		return 0, fmt.Errorf("malformed bit type %q", tp)
	}
	return bits, nil
}

// keyMatcher decides whether a binlog event's key is the row being waited for.
// The two sides arrive differently — the stream decodes Go values, the snapshot
// holds Datums read back from MySQL — so both go through the key column's
// declared type before being compared.
//
// It is only a matcher: a false negative costs another wait, which is why it
// fails closed by returning false on a value it cannot convert rather than
// guessing.
func keyMatcher(info *table.TableInfo, keyColumns []string, want []table.Datum) (func([]any) bool, error) {
	if len(keyColumns) != len(want) {
		return nil, fmt.Errorf("key has %d columns but snapshot holds %d", len(keyColumns), len(want))
	}
	types := make([]string, len(keyColumns))
	for i, column := range keyColumns {
		tp, ok := info.GetColumnMySQLType(column)
		if !ok {
			return nil, fmt.Errorf("missing key column type for %s", column)
		}
		types[i] = tp
	}
	return func(key []any) bool {
		if len(key) != len(want) {
			return false
		}
		for i, value := range key {
			got, err := table.NewDatumFromValue(value, types[i])
			if err != nil {
				return false
			}
			below, err := got.LessThan(want[i])
			if err != nil || below {
				return false
			}
			above, err := got.GreaterThan(want[i])
			if err != nil || above {
				return false
			}
		}
		return true
	}, nil
}

// binlogKey returns key, a snapshot key read back from MySQL, as the stream
// decodes it, for keyMatcher. They differ only for a string key column in a
// charset other than utf8mb4 (see table.TableInfo.BinlogCharset): the read
// returns it converted to utf8mb4, and a binlog row image carries the column's
// own bytes, so latin1 "é" is C3 A9 on one side and E9 on the other and would
// never match. Those values are converted to the column's charset by the
// server, the only party that knows every charset. key itself is not changed:
// it is still what pointPredicate needs.
func binlogKey(ctx context.Context, db *sql.DB, info *table.TableInfo, keyColumns []string, key []table.Datum) ([]table.Datum, error) {
	var converted []table.Datum
	for i, column := range keyColumns {
		charset := info.BinlogCharset(column)
		if charset == "" || i >= len(key) {
			continue
		}
		s, ok := key[i].Val.(string)
		if !ok {
			continue
		}
		var hexBytes string
		if err := db.QueryRowContext(ctx, "SELECT HEX(CONVERT(? USING "+charset+"))", s).Scan(&hexBytes); err != nil {
			return nil, fmt.Errorf("convert key column %s to %s: %w", column, charset, err)
		}
		raw, err := hex.DecodeString(hexBytes)
		if err != nil {
			return nil, fmt.Errorf("convert key column %s to %s: %w", column, charset, err)
		}
		if converted == nil {
			converted = slices.Clone(key)
		}
		converted[i].Val = string(raw)
	}
	if converted == nil {
		return key, nil
	}
	return converted, nil
}

// pointPredicate renders a single key as a chunk predicate, which is how every
// other read in this package addresses one row.
func pointPredicate(chunk *table.Chunk, key []table.Datum) (string, error) {
	if len(key) == 0 {
		return "", errors.New("cannot address a row without a key")
	}
	point := *chunk
	point.LowerBound = &table.Boundary{Value: key, Inclusive: true}
	point.UpperBound = &table.Boundary{Value: key, Inclusive: true}
	return point.String(), nil
}
