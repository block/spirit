package checksum

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// parkedEvent is one scripted change: the event the reader would have parked
// at. err, if set, is delivered instead, which is how a rewrite is expressed.
type parkedEvent struct {
	key     []any
	image   []any
	deleted bool
	err     error
}

// parkingFeed scripts the change.Source half of a verification, standing in for
// the real clients. Each call to VerifyRowAtNextChange consumes the scripted event whose
// key the watch recognises — order-independent, because which row the settler
// asks about first is map iteration order — and runs the feed's flush before
// handing it to the verifier, in the order change.RowParker.Verify does.
//
// Scripting no event at all is the row that went quiet: nothing arrives and the
// caller's budget ends the wait, which is the real shape of that case rather
// than a shortcut around it. Scripting one the watch does *not* recognise is a
// harness bug and says so, rather than looking like a quiet row.
type parkingFeed struct {
	change.MockSource

	mu     sync.Mutex
	events []parkedEvent
	// rewriteAfter, when positive, makes every verification past that many
	// deliveries report the row as rewritten.
	rewriteAfter int
	watches      []change.RowWatch
}

func (f *parkingFeed) VerifyRowAtNextChange(ctx context.Context, watch change.RowWatch, verify change.RowVerifier) error {
	f.mu.Lock()
	f.watches = append(f.watches, watch)
	switch {
	case f.rewriteAfter > 0 && len(f.watches) > f.rewriteAfter:
		f.mu.Unlock()
		return change.ErrRowRewritten
	case len(f.events) == 0:
		f.mu.Unlock()
		<-ctx.Done()
		return ctx.Err()
	}
	index := -1
	for i, ev := range f.events {
		if ev.err != nil || watch.Match(ev.key) {
			index = i
			break
		}
	}
	if index < 0 {
		f.mu.Unlock()
		return fmt.Errorf("no scripted event matches the watch on %s.%s", watch.Schema, watch.Table)
	}
	ev := f.events[index]
	f.events = append(f.events[:index], f.events[index+1:]...)
	f.mu.Unlock()

	if ev.err != nil {
		return ev.err
	}
	if err := f.Flush(ctx); err != nil {
		return err
	}
	return verify(ctx, ev.key, ev.image, ev.deleted)
}

func (f *parkingFeed) watchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.watches)
}

// settleTestSettler builds the settler the checker would build: the source, the
// feed, and a logger. Everything else about a running check is irrelevant to it,
// which is the point of it being its own type.
func settleTestSettler(t *testing.T, db *sql.DB, feed change.Source) *rowSettler {
	t.Helper()
	cfg := CheckerConfig{}
	applySharedDefaults(&cfg)
	return newRowSettler(db, feed, cfg.Logger)
}

// pendingSnapshot captures a snapshot and asserts the fallback really did leave
// obligations outstanding, so a settle verdict is about the settle path rather
// than about an empty work set.
func pendingSnapshot(t *testing.T, db *sql.DB, chunk *table.Chunk, wantPending int) *hotSnapshot {
	t.Helper()
	snapshot, err := captureHotSnapshot(t.Context(), db, db, chunk)
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	passed, err := snapshot.check(t.Context())
	require.NoError(t, err)
	require.False(t, passed)
	require.Len(t, snapshot.pending, wantPending)
	return snapshot
}

// TestSettleHotRowCleanAgainstStreamImage is the case the escalation exists
// for: the target was behind when the snapshot froze, and the change that
// brings it level is the very event the feed parks at. Polling could have found
// this too, eventually and only by luck — what matters is that comparing
// against the stream's image reaches the verdict without the source ever having
// to hold still.
func TestSettleHotRowCleanAgainstStreamImage(t *testing.T) {
	db, chunk := snapshotTestTables(t, "id INT PRIMARY KEY, value INT", []string{"id"})
	snapshotExec(t, db, "INSERT INTO src VALUES (1,10),(2,20)")
	snapshotExec(t, db, "INSERT INTO dst VALUES (1,10)")
	snapshot := pendingSnapshot(t, db, chunk, 1)

	feed := &parkingFeed{events: []parkedEvent{{key: []any{int64(2)}, image: []any{int64(2), int64(20)}}}}
	feed.FlushFn = func(ctx context.Context) error {
		_, err := db.ExecContext(ctx, "REPLACE INTO dst VALUES (2,20)")
		return err
	}

	verdict, err := settleTestSettler(t, db, feed).settle(t.Context(), snapshot)
	require.NoError(t, err)
	require.Equal(t, settleClean, verdict)
	require.Empty(t, snapshot.pending, "a verified row must stop being an obligation")
}

// TestSettleHotRowDivergedAgainstStreamImage is the verdict polling can never
// reach. The feed delivered the source's value for the row and the flush
// applied everything up to it, so a target that still holds something else is
// wrong — there is no in-flight change left that could explain it.
func TestSettleHotRowDivergedAgainstStreamImage(t *testing.T) {
	db, chunk := snapshotTestTables(t, "id INT PRIMARY KEY, value INT", []string{"id"})
	snapshotExec(t, db, "INSERT INTO src VALUES (1,10),(2,20)")
	snapshotExec(t, db, "INSERT INTO dst VALUES (1,10),(2,99)")
	snapshot := pendingSnapshot(t, db, chunk, 1)

	feed := &parkingFeed{events: []parkedEvent{{key: []any{int64(2)}, image: []any{int64(2), int64(20)}}}}
	verdict, err := settleTestSettler(t, db, feed).settle(t.Context(), snapshot)
	require.NoError(t, err)
	require.Equal(t, settleDiverged, verdict)
}

// TestSettleHotRowMissingTargetRowDiverges covers the other divergence shape:
// the stream just wrote the row and the target has none.
func TestSettleHotRowMissingTargetRowDiverges(t *testing.T) {
	db, chunk := snapshotTestTables(t, "id INT PRIMARY KEY, value INT", []string{"id"})
	snapshotExec(t, db, "INSERT INTO src VALUES (1,10),(2,20)")
	snapshotExec(t, db, "INSERT INTO dst VALUES (1,10)")
	snapshot := pendingSnapshot(t, db, chunk, 1)

	feed := &parkingFeed{events: []parkedEvent{{key: []any{int64(2)}, image: []any{int64(2), int64(20)}}}}
	verdict, err := settleTestSettler(t, db, feed).settle(t.Context(), snapshot)
	require.NoError(t, err)
	require.Equal(t, settleDiverged, verdict)
}

// TestSettleHotRowDeleteIsAVerdict is the case a source-side read cannot
// resolve at all. The obligation is an *absence* — a row the target holds and
// the source does not — and no SELECT can prove a row will stay absent, because
// there is nothing to hold. A delete event proves it directly: the stream said
// the row is gone and the flush carried that through, so the target must not
// have it.
func TestSettleHotRowDeleteIsAVerdict(t *testing.T) {
	for _, targetKeepsRow := range []bool{false, true} {
		t.Run(fmt.Sprint(targetKeepsRow), func(t *testing.T) {
			db, chunk := snapshotTestTables(t, "id INT PRIMARY KEY, value INT", []string{"id"})
			snapshotExec(t, db, "INSERT INTO src VALUES (1,10)")
			snapshotExec(t, db, "INSERT INTO dst VALUES (1,10),(2,99)")
			snapshot := pendingSnapshot(t, db, chunk, 1)
			for _, row := range snapshot.pending {
				require.False(t, row.present, "the outstanding obligation should be an absence")
			}

			feed := &parkingFeed{events: []parkedEvent{{key: []any{int64(2)}, deleted: true}}}
			if !targetKeepsRow {
				feed.FlushFn = func(ctx context.Context) error {
					_, err := db.ExecContext(ctx, "DELETE FROM dst WHERE id=2")
					return err
				}
			}

			verdict, err := settleTestSettler(t, db, feed).settle(t.Context(), snapshot)
			require.NoError(t, err)
			if targetKeepsRow {
				require.Equal(t, settleDiverged, verdict)
				return
			}
			require.Equal(t, settleClean, verdict)
		})
	}
}

// TestSettleHotRowRewrittenDefers: if the row changed again before the target
// could be read, the image handed over is no longer what the flush left behind,
// and the comparison would be against a value the target was never meant to
// hold. That is not a divergence and must not be reported as one.
func TestSettleHotRowRewrittenDefers(t *testing.T) {
	db, chunk := snapshotTestTables(t, "id INT PRIMARY KEY, value INT", []string{"id"})
	snapshotExec(t, db, "INSERT INTO src VALUES (1,10),(2,20)")
	snapshotExec(t, db, "INSERT INTO dst VALUES (1,10),(2,99)")
	snapshot := pendingSnapshot(t, db, chunk, 1)

	feed := &parkingFeed{events: []parkedEvent{{err: change.ErrRowRewritten}}}
	verdict, err := settleTestSettler(t, db, feed).settle(t.Context(), snapshot)
	require.NoError(t, err)
	require.Equal(t, settleUnavailable, verdict)
}

// TestSettleHotRowQuietRowDefers: a row that produces no change inside its
// budget is not the case this exists for — the ordinary poll was already
// converging on it — so the range defers exactly as it did before settling
// existed.
func TestSettleHotRowQuietRowDefers(t *testing.T) {
	db, chunk := snapshotTestTables(t, "id INT PRIMARY KEY, value INT", []string{"id"})
	snapshotExec(t, db, "INSERT INTO src VALUES (1,10),(2,20)")
	snapshotExec(t, db, "INSERT INTO dst VALUES (1,10),(2,99)")
	snapshot := pendingSnapshot(t, db, chunk, 1)

	feed := &parkingFeed{} // no scripted change ever arrives
	start := time.Now()
	verdict, err := settleTestSettler(t, db, feed).settle(t.Context(), snapshot)
	require.NoError(t, err)
	require.Equal(t, settleUnavailable, verdict)
	require.Less(t, time.Since(start), settleBudget,
		"one quiet row must not be able to spend the whole range's budget")
}

// TestSettleHotSnapshotBanksProgress: rows are settled one at a time, so a run
// that gives up part way must keep what it proved. Otherwise a range with one
// stubborn row would re-verify every other row on every escalation.
func TestSettleHotSnapshotBanksProgress(t *testing.T) {
	db, chunk := snapshotTestTables(t, "id INT PRIMARY KEY, value INT", []string{"id"})
	snapshotExec(t, db, "INSERT INTO src VALUES (1,10),(2,20),(3,30)")
	snapshotExec(t, db, "INSERT INTO dst VALUES (1,10)")
	snapshot := pendingSnapshot(t, db, chunk, 2)
	snapshotExec(t, db, "INSERT INTO dst VALUES (2,20),(3,30)")

	// Whichever row is settled first is verified; the second is rewritten
	// underneath us. Map iteration order picks which, and the assertion holds
	// either way.
	feed := &parkingFeed{
		events: []parkedEvent{
			{key: []any{int64(2)}, image: []any{int64(2), int64(20)}},
			{key: []any{int64(3)}, image: []any{int64(3), int64(30)}},
		},
		rewriteAfter: 1,
	}

	verdict, err := settleTestSettler(t, db, feed).settle(t.Context(), snapshot)
	require.NoError(t, err)
	require.Equal(t, settleUnavailable, verdict)
	require.Len(t, snapshot.pending, 1, "the row that was verified must not be waited for again")
}

// TestSettleHotSnapshotWithoutFeedIsUnavailable: the feed parking at the
// watched change is the whole mechanism, and library callers may have no feed.
// Without one the range stays exactly where it was before settling existed,
// rather than getting a verdict from a comparison nothing was holding still.
func TestSettleHotSnapshotWithoutFeedIsUnavailable(t *testing.T) {
	db, chunk := snapshotTestTables(t, "id INT PRIMARY KEY, value INT", []string{"id"})
	snapshotExec(t, db, "INSERT INTO src VALUES (1,10),(2,20)")
	snapshotExec(t, db, "INSERT INTO dst VALUES (1,10),(2,99)")
	snapshot := pendingSnapshot(t, db, chunk, 1)

	verdict, err := settleTestSettler(t, db, nil).settle(t.Context(), snapshot)
	require.NoError(t, err)
	require.Equal(t, settleUnavailable, verdict)
}

// TestSettleHotSnapshotPropagatesCancellation: the settle path swallows every
// timeout, because deferring is the right answer to all of them. It must not
// swallow the run being cancelled — that is not a verdict about the data.
func TestSettleHotSnapshotPropagatesCancellation(t *testing.T) {
	db, chunk := snapshotTestTables(t, "id INT PRIMARY KEY, value INT", []string{"id"})
	snapshotExec(t, db, "INSERT INTO src VALUES (1,10),(2,20)")
	snapshotExec(t, db, "INSERT INTO dst VALUES (1,10),(2,99)")
	snapshot := pendingSnapshot(t, db, chunk, 1)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := settleTestSettler(t, db, &parkingFeed{}).settle(ctx, snapshot)
	require.ErrorIs(t, err, context.Canceled)
}

// TestExpectedImageCRCMatchesRealRow is the load-bearing claim of the whole
// design: evaluating the checksum expressions over a row *image* gives the same
// answer as evaluating them over the row. If it did not, every settle verdict
// would be a comparison between two differently-normalised values, and the
// clean ones would be luck.
//
// It is checked over types whose text rendering differs from their storage —
// fractional-second temporals, decimals, binary, NULL — because those are where
// a mis-typed parameter would show up.
func TestExpectedImageCRCMatchesRealRow(t *testing.T) {
	for _, ddl := range []string{
		"id INT PRIMARY KEY, v INT, s VARCHAR(50), t DATETIME(6), b VARBINARY(20), d DECIMAL(10,3), n INT NULL",
		"id BIGINT PRIMARY KEY, v TINYINT, s TEXT, t TIMESTAMP(3) NULL, b BLOB, d DOUBLE, n DATE NULL",
	} {
		db, chunk := snapshotTestTables(t, ddl, []string{"id"})
		snapshotExec(t, db, `INSERT INTO src VALUES (1,-7,'héllo ''x''','2024-02-29 12:34:56.789012',x'00ff00',12.345,NULL)`)

		sourceExprs, _, err := chunk.ColumnMapping.ChecksumExprs()
		require.NoError(t, err)
		var want uint64
		require.NoError(t, db.QueryRowContext(t.Context(),
			"SELECT CRC32(CONCAT("+sourceExprs+")) FROM src WHERE id=1").Scan(&want))

		s := &rowSettler{sourceDB: db}
		got, err := s.expectedImageCRC(t.Context(), chunk, readRowImage(t, db, "SELECT * FROM src WHERE id=1"))
		require.NoError(t, err)
		require.Equal(t, want, got, "ddl=%s", ddl)
	}
}

// TestExpectedImageCRCMatchesRealRowForBinlogTypes is the same equivalence over
// the two types where it is not free, with the image built from the Go types
// go-mysql decodes a row event into rather than from a SELECT.
//
// That distinction is the whole test. Reading the image back with SELECT hands
// back a FLOAT as a float64 already rounded to the column's precision and a BIT
// as []byte, so both happen to render correctly — and the real decoding, float32
// and int64, does not. UNION type merging widens, so the float32 (widened again
// to float64 by database/sql) merges to DOUBLE and renders every digit of its
// binary expansion, and the int64 merges to an integer and renders in decimal
// where the column renders raw bytes. Neither fails the query: they return a CRC
// that is simply not the row's, so every hot row in such a table would settle to
// a false divergence.
func TestExpectedImageCRCMatchesRealRowForBinlogTypes(t *testing.T) {
	for _, tc := range []struct {
		ddl    string
		insert string
		image  []any
	}{
		// BIT(8)=5 renders as the single byte 0x05, BIT(64)=5 as eight bytes.
		{
			"id INT PRIMARY KEY, f FLOAT, b8 BIT(8), b64 BIT(64)",
			"INSERT INTO src VALUES (1, 0.1, b'00000101', 5)",
			[]any{int32(1), float32(0.1), int64(5), int64(5)},
		},
		// A BIT with no declared width is BIT(1), and a negative float has to
		// survive the round trip back through the column's precision.
		{
			"id INT PRIMARY KEY, f FLOAT, b1 BIT, b17 BIT(17)",
			"INSERT INTO src VALUES (1, -3.4028234e+38, b'1', 131071)",
			[]any{int32(1), float32(-3.4028234e+38), int64(1), int64(131071)},
		},
		// NULL has no Go type to widen, and must stay NULL through the cast.
		{
			"id INT PRIMARY KEY, f FLOAT NULL, b8 BIT(8) NULL",
			"INSERT INTO src VALUES (1, NULL, NULL)",
			[]any{int32(1), nil, nil},
		},
	} {
		t.Run(tc.ddl, func(t *testing.T) {
			db, chunk := snapshotTestTables(t, tc.ddl, []string{"id"})
			snapshotExec(t, db, tc.insert)

			sourceExprs, _, err := chunk.ColumnMapping.ChecksumExprs()
			require.NoError(t, err)
			var want uint64
			require.NoError(t, db.QueryRowContext(t.Context(),
				"SELECT CRC32(CONCAT("+sourceExprs+")) FROM src WHERE id=1").Scan(&want))

			got, err := (&rowSettler{sourceDB: db}).expectedImageCRC(t.Context(), chunk, tc.image)
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
}

// TestImageValueExpr covers the parts of the rendering that do not need a
// server: which types are special-cased, and the big-endian byte layout a BIT
// column's cast produces.
func TestImageValueExpr(t *testing.T) {
	for name, tc := range map[string]struct {
		tp       string
		in       any
		wantExpr string
		wantVal  any
	}{
		"int is bound as-is":     {"int", int32(7), "?", int32(7)},
		"varchar is bound as-is": {"varchar(20)", "x", "?", "x"},
		"null short-circuits":    {"bit(8)", nil, "?", nil},
		"float is narrowed":      {"float", float32(0.1), "CAST(? AS FLOAT)", float32(0.1)},
		"float with width":       {"float(10,2)", float32(1), "CAST(? AS FLOAT)", float32(1)},
		"float unsigned":         {"float unsigned", float32(1), "CAST(? AS FLOAT)", float32(1)},
		"double is not":          {"double", float64(0.1), "?", float64(0.1)},
		"bit(1)":                 {"bit(1)", int64(1), "?", []byte{0x01}},
		"bit with no width":      {"bit", int64(1), "?", []byte{0x01}},
		"bit(8)":                 {"bit(8)", int64(5), "?", []byte{0x05}},
		"bit(9) rounds up":       {"bit(9)", int64(257), "?", []byte{0x01, 0x01}},
		"bit(64) is big-endian":  {"bit(64)", int64(5), "?", []byte{0, 0, 0, 0, 0, 0, 0, 5}},
		"bit(64) all ones":       {"bit(64)", int64(-1), "?", []byte{255, 255, 255, 255, 255, 255, 255, 255}},
	} {
		t.Run(name, func(t *testing.T) {
			expr, val, err := imageValueExpr(tc.tp, tc.in)
			require.NoError(t, err)
			require.Equal(t, tc.wantExpr, expr)
			require.Equal(t, tc.wantVal, val)
		})
	}

	// A BIT column the stream did not decode to an integer is a mismatch
	// between what we believe the schema is and what arrived. Guessing would
	// mint a false divergence, so it is an error.
	_, _, err := imageValueExpr("bit(8)", "5")
	require.ErrorContains(t, err, "want an integer")

	_, _, err = imageValueExpr("bit(0)", int64(1))
	require.ErrorContains(t, err, "malformed bit type")
}

// TestExpectedImageCRCRejectsShortImage: an image with fewer columns than the
// mapping expects would otherwise index out of range.
func TestExpectedImageCRCRejectsShortImage(t *testing.T) {
	db, chunk := snapshotTestTables(t, "id INT PRIMARY KEY, value INT", []string{"id"})
	s := &rowSettler{sourceDB: db}
	_, err := s.expectedImageCRC(t.Context(), chunk, []any{int64(1)})
	require.ErrorContains(t, err, "binlog row image has 1 columns")
}

// readRowImage reads a row back as raw driver values, which is the shape a
// binlog after-image arrives in.
func readRowImage(t *testing.T, db *sql.DB, query string) []any {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), query)
	require.NoError(t, err)
	defer rows.Close() //nolint:errcheck // test cleanup
	columns, err := rows.Columns()
	require.NoError(t, err)
	image := make([]any, len(columns))
	dest := make([]any, len(columns))
	for i := range dest {
		dest[i] = &image[i]
	}
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(dest...))
	require.NoError(t, rows.Err())
	return image
}

// TestKeyMatcher: the two sides of the comparison arrive by different routes —
// the stream decodes Go values, the snapshot holds Datums read back from MySQL
// — so the matcher has to put both through the column's declared type. It is
// only a matcher, so a value it cannot convert costs another wait rather than a
// wrong answer.
func TestKeyMatcher(t *testing.T) {
	db, chunk := snapshotTestTables(t, "id INT PRIMARY KEY, name VARCHAR(20), value INT", []string{"id", "name"})
	snapshotExec(t, db, "INSERT INTO src VALUES (7,'abc',1)")

	// Build the snapshot's side through the production read, so this tests the
	// two real representations rather than two hand-built Datums.
	rows, _, _, err := readHotSnapshotRows(t.Context(), db, chunk, chunk.Table, "value", chunk.String(), 10)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	var want []table.Datum
	for _, row := range rows {
		want = row.key
	}

	match, err := keyMatcher(chunk.Table, chunk.Key, want)
	require.NoError(t, err)

	require.True(t, match([]any{int64(7), "abc"}), "the ordinary decoding must match")
	require.True(t, match([]any{int32(7), []byte("abc")}), "a narrower int and a byte-slice string are the same key")
	require.False(t, match([]any{int64(8), "abc"}))
	require.False(t, match([]any{int64(7), "abd"}))
	require.False(t, match([]any{int64(7)}), "a key of the wrong arity is not a match")
	require.False(t, match([]any{"not a number", "abc"}), "an unconvertible value must fail closed")

	_, err = keyMatcher(chunk.Table, chunk.Key, want[:1])
	require.Error(t, err, "a snapshot key that does not fit the chunk key is a bug, not a miss")
	_, err = keyMatcher(chunk.Table, []string{"nosuch"}, want[:1])
	require.Error(t, err)
}

// TestCheckHotSnapshotEscalatesOnlyWhenExhausted: settling parks the change
// stream, which stops every other subscriber's progress for as long as it
// holds. It must therefore be the terminal step and not something every poll
// does. Below MaxHotAttempts the range keeps polling; at the limit it settles
// instead of deferring.
func TestCheckHotSnapshotEscalatesOnlyWhenExhausted(t *testing.T) {
	db, chunk := snapshotTestTables(t, "id INT PRIMARY KEY, value INT", []string{"id"})
	snapshotExec(t, db, "INSERT INTO src VALUES (1,10),(2,20)")
	snapshotExec(t, db, "INSERT INTO dst VALUES (1,10),(2,99)")
	snapshot := pendingSnapshot(t, db, chunk, 1)

	feed := &parkingFeed{events: []parkedEvent{{key: []any{int64(2)}, image: []any{int64(2), int64(20)}}}}
	cfg := CheckerConfig{MaxHotAttempts: 3}
	applySharedDefaults(&cfg)
	c := newLocklessChecker(db, db, nil, feed, nil, &cfg)

	res := &workResult{item: &workItem{chunk: chunk}}
	// snapshot.check already consumed one attempt in pendingSnapshot.
	for snapshot.attempts < cfg.MaxHotAttempts-1 {
		c.checkHotSnapshot(t.Context(), res, snapshot)
		require.NoError(t, res.err)
		require.False(t, res.passed)
		require.False(t, res.deferHot)
		require.Zero(t, feed.watchCount(), "must not park the stream while ordinary polling is still allowed")
	}

	c.checkHotSnapshot(t.Context(), res, snapshot)
	require.NoError(t, res.err)
	require.Equal(t, 1, feed.watchCount())
	require.True(t, snapshot.settled)
	require.False(t, res.deferHot, "a settled divergence is a verdict, not a deferral")
	require.True(t, res.permanent, "no recopier configured, so a settled divergence is fatal")
	require.Equal(t, uint64(1), c.hotChunksSettledThisPass.Load())
}

// TestLocklessSettlesHotChunkEndToEnd is the whole point of the escalation,
// driven through RunUntilClean rather than by calling settleHotSnapshot.
//
// The chunk's aggregate never matches — the source CRC changes on every read,
// which is what a continuously written range looks like — so the checker falls
// back to the row snapshot, and the snapshot never converges by polling either,
// because polling is passive: it reads the target and waits. Before settling,
// that combination had exactly one outcome regardless of whether the data was
// actually wrong. Now it reaches a verdict, and which verdict depends on the
// data:
//
//   - converge: the change the feed parks at is the one the target was missing,
//     so the flush lands it and the range verifies.
//   - diverge: the target holds a value the stream's image contradicts, so the
//     range is reported rather than deferred for the rest of the run.
func TestLocklessSettlesHotChunkEndToEnd(t *testing.T) {
	for _, converge := range []bool{true, false} {
		t.Run(fmt.Sprint(converge), func(t *testing.T) {
			db, chunk := snapshotTestTables(t, "id INT PRIMARY KEY, value INT", []string{"id"})
			snapshotExec(t, db, "INSERT INTO src VALUES (1,10),(2,20)")
			snapshotExec(t, db, "INSERT INTO dst VALUES (1,10)")
			if !converge {
				// Present but wrong, which the stream's image contradicts.
				snapshotExec(t, db, "INSERT INTO dst VALUES (2,99)")
			}

			chunker := newTestChunker(1)
			chunker.chunks[0] = chunk
			cfg := fastConfig()
			cfg.RetryDelay = time.Millisecond
			cfg.MaxHotAttempts = 3
			cfg.MinPassInterval = time.Hour
			// No recopier: a settled divergence must be reported, which is the
			// clearest way to see that a verdict was reached at all.
			c := newTestChecker(t, chunker, cfg, func(_ context.Context, _ *table.Chunk, attempt int) (int64, int64, uint64, error) {
				return int64(attempt), 0, 1, nil // the source never stops moving
			})
			c.sourceDB = db
			c.snapshotChunk = func(ctx context.Context, chunk *table.Chunk) (*hotSnapshot, error) {
				return captureHotSnapshot(ctx, db, db, chunk)
			}
			// The feed parks at the next change to the outstanding row and
			// hands over its after-image; flushing carries every change up to
			// that event — including, when converging, the one the target was
			// missing.
			feed := &parkingFeed{events: []parkedEvent{{key: []any{int64(2)}, image: []any{int64(2), int64(20)}}}}
			if converge {
				feed.FlushFn = func(ctx context.Context) error {
					_, err := db.ExecContext(ctx, "REPLACE INTO dst SELECT * FROM src")
					return err
				}
			}
			c.feed = feed

			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			err := c.RunUntilClean(ctx)

			stats := c.Stats()
			require.Equal(t, uint64(1), stats.HotChunksSettledThisPass, "the range must reach a verdict, not defer")
			require.Zero(t, stats.HotChunksDeferredThisPass)
			if converge {
				require.NoError(t, err)
				require.False(t, stats.FirstCleanPassAt.IsZero())
				var diffs int
				require.NoError(t, db.QueryRowContext(t.Context(),
					"SELECT COUNT(*) FROM src LEFT JOIN dst USING (id, value) WHERE dst.id IS NULL").Scan(&diffs))
				require.Zero(t, diffs, "the flush the settle path drove must have landed the missing row")
				return
			}
			require.ErrorIs(t, err, ErrPermanentDivergence,
				"a settled divergence is reported; before settling it was invisible")
			require.ErrorContains(t, err, "settled against the change stream",
				"a settled divergence names its own evidence, not aggregate CRCs it never read")
		})
	}
}

// TestCompareRowToImageRefusesAmbiguousTargetRead pins the guard that a point
// predicate must address exactly one target row before its value is read as a
// verdict.
//
// The predicate is rendered from the *source's* key, but it is evaluated by the
// target, under the target's collation. When those differ, one source key can
// address two target rows — here a case-sensitive source holding 'a' against a
// case-insensitive target holding both 'a' and 'A'. Neither of the two is "the"
// row, so whichever came back first would decide a verdict by accident. The
// range defers instead, and the ordinary retries carry it.
func TestCompareRowToImageRefusesAmbiguousTargetRead(t *testing.T) {
	schema, db := testutils.CreateUniqueTestDatabase(t)
	snapshotExec(t, db, "CREATE TABLE src (k VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs PRIMARY KEY, v INT)")
	// k alone is not unique on the target, which is what lets it hold both rows.
	snapshotExec(t, db, "CREATE TABLE dst (k VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci, v INT, PRIMARY KEY (k, v))")
	snapshotExec(t, db, "INSERT INTO src VALUES ('a',1)")
	snapshotExec(t, db, "INSERT INTO dst VALUES ('a',1),('A',2)")

	source, target := table.NewTableInfo(db, schema, "src"), table.NewTableInfo(db, schema, "dst")
	require.NoError(t, source.SetInfo(t.Context()))
	require.NoError(t, target.SetInfo(t.Context()))
	chunk := &table.Chunk{Key: []string{"k"}, Table: source, NewTable: target, ColumnMapping: table.NewColumnMapping(source, target, nil)}

	snapshot, err := captureHotSnapshot(t.Context(), db, db, chunk)
	require.NoError(t, err)
	require.NotNil(t, snapshot)

	var row hotSnapshotRow
	var found bool
	for _, pending := range snapshot.pending {
		if pending.present {
			row, found = pending, true
		}
	}
	require.True(t, found, "the source row should be an outstanding obligation")

	settler := newRowSettler(db, &change.MockSource{}, slog.Default())
	verdict, err := settler.compareRowToImage(t.Context(), snapshot, row, []any{[]byte("a"), int32(1)}, false)
	require.NoError(t, err)
	require.Equal(t, settleUnavailable, verdict, "two target rows for one key is not a verdict")
}
