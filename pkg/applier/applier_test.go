package applier

import (
	"context"
	"database/sql"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// A ~900 KB row must still estimate under MaxStatementSizeBytes, so it fits
// in a single chunklet.
func TestEstimateRenderedRowSizeUnderStatementBudget(t *testing.T) {
	// Create a row that's close to 1MB
	largeData := make([]byte, 900000) // 900KB
	for i := range largeData {
		largeData[i] = 'x'
	}
	values := []any{
		int64(1),
		string(largeData),
		"metadata",
	}
	size := utils.EstimateRenderedRowSize(values)
	// Should be close to but not exceed our threshold
	require.Greater(t, size, 900000, "should account for large data")
	require.Less(t, size, MaxStatementSizeBytes, "single row should fit in a chunklet")
	t.Logf("Large row size: %d bytes (threshold: %d)", size, MaxStatementSizeBytes)
}

func TestSplitRowsIntoChunklets(t *testing.T) {
	t.Run("empty rows", func(t *testing.T) {
		rows := []rowData{}
		chunklets := splitRowsIntoChunklets(rows)
		require.Nil(t, chunklets, "should return nil for empty input")
	})

	t.Run("single small row", func(t *testing.T) {
		rows := []rowData{
			{values: []any{int64(1), "test"}},
		}
		chunklets := splitRowsIntoChunklets(rows)
		require.Len(t, chunklets, 1, "should create one chunklet")
		require.Len(t, chunklets[0], 1, "chunklet should have one row")
	})

	t.Run("rows under max count threshold", func(t *testing.T) {
		// Half the row cap, in rows small enough that the byte cap cannot be
		// what splits them. Derived from the constant so the case stays
		// meaningful if the cap is retuned.
		n := chunkletMaxRows / 2
		rows := make([]rowData, n)
		for i := range rows {
			rows[i] = rowData{values: []any{int64(i), "test"}}
		}
		chunklets := splitRowsIntoChunklets(rows)
		require.Len(t, chunklets, 1, "should create one chunklet")
		require.Len(t, chunklets[0], n, "chunklet should have all %d rows", n)
	})

	t.Run("rows exceeding max count threshold", func(t *testing.T) {
		// Two full chunklets plus a half remainder, again in rows too small for
		// the byte cap to reach. This pins the row cap specifically: with
		// ~24-byte rows, chunkletMaxRows would have to exceed ~40k before
		// MaxStatementSizeBytes could bind first and invalidate the case.
		remainder := chunkletMaxRows / 2
		rows := make([]rowData, 2*chunkletMaxRows+remainder)
		for i := range rows {
			rows[i] = rowData{values: []any{int64(i), "test"}}
		}
		chunklets := splitRowsIntoChunklets(rows)
		require.Len(t, chunklets, 3, "should create 3 chunklets")
		require.Len(t, chunklets[0], chunkletMaxRows, "first chunklet should be full")
		require.Len(t, chunklets[1], chunkletMaxRows, "second chunklet should be full")
		require.Len(t, chunklets[2], remainder, "third chunklet should hold the remainder")
	})

	t.Run("rows exceeding max size threshold", func(t *testing.T) {
		// Rows large enough that the byte cap splits them well before the row
		// cap does: each row is a tenth of MaxStatementSizeBytes, and we supply
		// enough for a little over two chunklets' worth.
		largeData := make([]byte, MaxStatementSizeBytes/10)
		for i := range largeData {
			largeData[i] = 'x'
		}

		rows := make([]rowData, 22)
		for i := range rows {
			rows[i] = rowData{values: []any{int64(i), string(largeData)}}
		}

		chunklets := splitRowsIntoChunklets(rows)
		// Should split based on size, not row count
		require.GreaterOrEqual(t, len(chunklets), 2, "should create at least 2 chunklets due to size")

		// Verify each chunklet is under the size limit
		for i, chunklet := range chunklets {
			totalSize := 0
			for _, row := range chunklet {
				totalSize += utils.EstimateRenderedRowSize(row.values)
			}
			// Allow some overhead, but should be reasonably close to limit
			require.LessOrEqual(t, totalSize, MaxStatementSizeBytes+10000,
				"chunklet %d should be under size limit (with small overhead)", i)
			t.Logf("Chunklet %d: %d rows, ~%d bytes", i, len(chunklet), totalSize)
		}
	})

	t.Run("mixed row sizes", func(t *testing.T) {
		// Mix of small and large rows
		rows := make([]rowData, 100)
		for i := range rows {
			if i%10 == 0 {
				// Every 10th row is large (10KB)
				largeData := make([]byte, 10000)
				for j := range largeData {
					largeData[j] = 'y'
				}
				rows[i] = rowData{values: []any{int64(i), string(largeData)}}
			} else {
				// Small rows
				rows[i] = rowData{values: []any{int64(i), "small"}}
			}
		}

		chunklets := splitRowsIntoChunklets(rows)
		require.NotEmpty(t, chunklets, "should create at least one chunklet")

		// Verify all rows are accounted for
		totalRows := 0
		for _, chunklet := range chunklets {
			totalRows += len(chunklet)
		}
		require.Equal(t, 100, totalRows, "all rows should be in chunklets")
		t.Logf("Created %d chunklets for 100 mixed-size rows", len(chunklets))
	})

	t.Run("exactly at row threshold", func(t *testing.T) {
		// Create exactly chunkletMaxRows rows
		rows := make([]rowData, chunkletMaxRows)
		for i := range rows {
			rows[i] = rowData{values: []any{int64(i), "test"}}
		}
		chunklets := splitRowsIntoChunklets(rows)
		require.Len(t, chunklets, 1, "should create one chunklet for exactly max rows")
		require.Len(t, chunklets[0], chunkletMaxRows, "chunklet should have all rows")
	})

	t.Run("one row over threshold", func(t *testing.T) {
		// Create chunkletMaxRows + 1 rows
		rows := make([]rowData, chunkletMaxRows+1)
		for i := range rows {
			rows[i] = rowData{values: []any{int64(i), "test"}}
		}
		chunklets := splitRowsIntoChunklets(rows)
		require.Len(t, chunklets, 2, "should create two chunklets")
		require.Len(t, chunklets[0], chunkletMaxRows, "first chunklet should have max rows")
		require.Len(t, chunklets[1], 1, "second chunklet should have 1 row")
	})

	t.Run("single very large row under limit", func(t *testing.T) {
		// Single row that's close to but under the size limit
		largeData := make([]byte, 900000) // 900KB
		for i := range largeData {
			largeData[i] = 'z'
		}

		rows := []rowData{
			{values: []any{int64(1), string(largeData)}},
		}

		chunklets := splitRowsIntoChunklets(rows)
		require.Len(t, chunklets, 1, "should create one chunklet for single large row")
		require.Len(t, chunklets[0], 1, "chunklet should have the one row")
	})

	t.Run("single row exceeding size limit", func(t *testing.T) {
		// Single row that exceeds MaxStatementSizeBytes (1 MiB)
		// This is an edge case - the row will be placed in its own chunklet
		// and we rely on max_allowed_packet being large enough (typically 64 MiB)
		veryLargeData := make([]byte, 2*1024*1024) // 2 MiB - exceeds our 1 MiB threshold
		for i := range veryLargeData {
			veryLargeData[i] = 'x'
		}

		rows := []rowData{
			{values: []any{int64(1), string(veryLargeData)}},
		}

		chunklets := splitRowsIntoChunklets(rows)
		require.Len(t, chunklets, 1, "should create one chunklet even though row exceeds size limit")
		require.Len(t, chunklets[0], 1, "chunklet should have the one oversized row")

		// Verify the row size does exceed our threshold
		rowSize := utils.EstimateRenderedRowSize(rows[0].values)
		require.Greater(t, rowSize, MaxStatementSizeBytes, "row should exceed MaxStatementSizeBytes")
		t.Logf("Single row size: %d bytes (exceeds threshold of %d bytes)", rowSize, MaxStatementSizeBytes)
		t.Logf("Note: This relies on max_allowed_packet being large enough (typically 64 MiB)")
	})

	t.Run("multiple rows with one exceeding limit", func(t *testing.T) {
		// Mix of normal rows and one that exceeds the limit
		veryLargeData := make([]byte, 2*1024*1024) // 2 MiB
		for i := range veryLargeData {
			veryLargeData[i] = 'y'
		}

		rows := []rowData{
			{values: []any{int64(1), "small"}},
			{values: []any{int64(2), "small"}},
			{values: []any{int64(3), string(veryLargeData)}}, // Oversized row
			{values: []any{int64(4), "small"}},
			{values: []any{int64(5), "small"}},
		}

		chunklets := splitRowsIntoChunklets(rows)
		// Should create at least 3 chunklets: small rows before, oversized row alone, small rows after
		require.GreaterOrEqual(t, len(chunklets), 3, "should create multiple chunklets")

		// Verify all rows are accounted for
		totalRows := 0
		for _, chunklet := range chunklets {
			totalRows += len(chunklet)
		}
		require.Equal(t, 5, totalRows, "all rows should be in chunklets")
		t.Logf("Created %d chunklets for 5 rows (including one 2 MiB row)", len(chunklets))
	})
}

// TestEstimateRenderedRowSizeTracksRenderedSize is the property that matters: the
// estimate feeds MaxStatementSizeBytes, so it has to stay in the same
// ballpark as what datum.String() actually emits into the VALUES clause.
//
// The previous implementation measured len(fmt.Sprintf("%v", v)), which drifted
// badly once you account for how values actually arrive: a text-protocol Scan
// into *any returns []byte for every column, and %v renders a []byte as
// "[49 50 51 …]" — about four characters per byte. That over-estimated by
// ~2.7x, so chunklets were cut well short of the budget they were sized for,
// and nothing failed because an over-estimate is safe. This pins the direction
// as well as the magnitude.
func TestEstimateRenderedRowSizeTracksRenderedSize(t *testing.T) {
	// Exactly what the driver hands back for a text-protocol row.
	values := []any{
		[]byte("298801139"), []byte("4211"), []byte("settled"),
		[]byte("2026-07-30 15:12:27"), []byte("1234.560000"),
		[]byte("405b6747-605e-3aa4-909d-69e049a6ed19"), nil,
	}
	types := []string{
		"bigint", "int", "varchar(64)", "timestamp", "decimal(20,6)",
		"varchar(36)", "int",
	}

	// Render the tuple exactly as writeChunklet would — join, not terminate,
	// so the baseline carries no trailing separator that would slacken the
	// ratio assertion.
	literals := make([]string, len(values))
	for i, v := range values {
		datum, err := table.NewDatumFromValue(v, types[i])
		require.NoError(t, err)
		literals[i] = datum.String()
	}
	rendered := len("(" + strings.Join(literals, ", ") + ")")

	estimated := utils.EstimateRenderedRowSize(values)
	ratio := float64(estimated) / float64(rendered)
	assert.InDelta(t, 1.0, ratio, 0.5,
		"estimate %d vs rendered %d (%.2fx) — the estimate has drifted from what is actually emitted",
		estimated, rendered, ratio)

	// And it must not allocate: this runs on every value of every copied row,
	// on top of the rendering writeChunklet does anyway.
	assert.Zero(t, testing.AllocsPerRun(100, func() { _ = utils.EstimateRenderedRowSize(values) }),
		"EstimateRenderedRowSize should not allocate")
}

// TestEstimateRenderedRowSizeUnderestimateStaysSafe pins the safety argument behind
// three deliberate under-estimates: a []byte bound to a binary column renders
// as 0x-hex (2 chars/byte), a string grows under escaping, and an integer is
// assumed to be 10 digits when an int64 can render 20.
//
// Each is preferred to padding, because an over-estimate is not free — it
// shrinks every chunklet, which is the bug this replaced. The reason it is
// safe is headroom: MaxStatementSizeBytes sits ~64x below a typical
// max_allowed_packet, so even all three compounding on one pathological row
// leaves a wide margin.
func TestEstimateRenderedRowSizeUnderestimateStaysSafe(t *testing.T) {
	// A row built to hit every under-estimating branch at once.
	worst := []any{
		int64(math.MaxInt64),                       // 19 rendered, 10 estimated
		int64(math.MinInt64),                       // 20 rendered, 10 estimated
		[]byte("\x00\x01\x02\x03\x04\x05\x06\x07"), // hex-renders at 2x
		`a string with "quotes" and \backslashes\ that escaping will grow`,
	}
	estimated := utils.EstimateRenderedRowSize(worst)
	require.Positive(t, estimated)

	// Worst-case compounding is bounded by ~2x per value, so a full statement
	// built at the budget cannot approach a 64 MiB max_allowed_packet.
	const compoundingFactor = 4 // generous: 2x is the real per-value bound
	const typicalMaxAllowedPacket = 64 * 1024 * 1024
	assert.Less(t, MaxStatementSizeBytes*compoundingFactor, typicalMaxAllowedPacket,
		"the byte budget no longer leaves room for the estimate to under-measure")
}

// TestApplierTimeoutScope pins which writes chunkTaskTimeout bounds, for both
// appliers. Every write blocks on a row lock held by another transaction.
//
//   - Apply (the copy path) must end with context.DeadlineExceeded.
//   - DeleteKeys and UpsertRows (the change feed's flushes) must not: they must
//     wait out innodb_lock_wait_timeout and fail with 1205, because pkg/change
//     defers a contended batch only when it sees a lock-contention error. A
//     spirit-owned deadline there would fail the whole drain instead.
func TestApplierTimeoutScope(t *testing.T) {
	defer func(d time.Duration) { chunkTaskTimeout = d }(chunkTaskTimeout)
	chunkTaskTimeout = 500 * time.Millisecond

	tt := testutils.NewTestTable(t, "applier_timeout_scope",
		`CREATE TABLE applier_timeout_scope (id INT PRIMARY KEY, name VARCHAR(100))`)
	ctx := t.Context()
	_, err := tt.DB.ExecContext(ctx, "INSERT INTO applier_timeout_scope VALUES (1, 'Alice')")
	require.NoError(t, err)
	tbl := table.NewTableInfo(tt.DB, "test", "applier_timeout_scope")
	require.NoError(t, tbl.SetInfo(ctx))
	// Sharded routing needs these; the single-target applier ignores them.
	tbl.ShardingColumn = "id"
	tbl.HashFunc = testutils.EvenOddHasher
	mapping := table.NewColumnMapping(tbl, tbl, nil)

	// The appliers write through a 1s innodb_lock_wait_timeout, so lock
	// contention surfaces as 1205 after RetryableTransaction's few attempts.
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	cfg.Params["innodb_lock_wait_timeout"] = "1"
	targetDB, err := sql.Open("block-mysql", cfg.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(targetDB)

	// Hold an exclusive lock on id=1 for the duration of the test.
	blocker, err := tt.DB.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, blocker.Rollback()) }()
	_, err = blocker.ExecContext(ctx, "SELECT * FROM applier_timeout_scope WHERE id = 1 FOR UPDATE")
	require.NoError(t, err)

	// within runs write and returns its error, failing the test if it takes
	// longer than bound. The bound stops a regression from hanging the test.
	within := func(t *testing.T, bound time.Duration, write func() error) error {
		t.Helper()
		done := make(chan error, 1)
		go func() { done <- write() }()
		select {
		case err := <-done:
			return err
		case <-time.After(bound):
			t.Fatalf("write did not return within %s", bound)
			return nil
		}
	}

	appliers := []struct {
		name string
		new  func() (Applier, error)
	}{
		{"single", func() (Applier, error) {
			return New([]Target{{DB: targetDB}}, NewApplierDefaultConfig())
		}},
		{"sharded", func() (Applier, error) {
			return New([]Target{{DB: targetDB, KeyRange: "-"}}, NewApplierDefaultConfig())
		}},
	}
	for _, tc := range appliers {
		t.Run(tc.name, func(t *testing.T) {
			applier, err := tc.new()
			require.NoError(t, err)
			require.NoError(t, applier.Start(ctx))
			defer func() { require.NoError(t, applier.Stop()) }()

			t.Run("Apply is bounded", func(t *testing.T) {
				chunk := &table.Chunk{Table: tbl, NewTable: tbl, ColumnMapping: mapping}
				err := within(t, 10*time.Second, func() error {
					callbackErr := make(chan error, 1)
					if err := applier.Apply(ctx, chunk, [][]any{{int64(1), "Bob"}}, func(_ int64, err error) {
						callbackErr <- err
					}); err != nil {
						return err
					}
					return <-callbackErr
				})
				require.ErrorIs(t, err, context.DeadlineExceeded)
			})

			t.Run("UpsertRows is not bounded", func(t *testing.T) {
				err := within(t, 30*time.Second, func() error {
					_, err := applier.UpsertRows(ctx, mapping, []LogicalRow{{RowImage: []any{int64(1), "Bob"}}}, nil)
					return err
				})
				require.NotErrorIs(t, err, context.DeadlineExceeded)
				require.True(t, dbconn.IsLockContentionError(err), "want lock contention (1205), got: %v", err)
			})

			t.Run("DeleteKeys is not bounded", func(t *testing.T) {
				err := within(t, 30*time.Second, func() error {
					_, err := applier.DeleteKeys(ctx, tbl, tbl, [][]any{{int64(1)}}, nil)
					return err
				})
				require.NotErrorIs(t, err, context.DeadlineExceeded)
				require.True(t, dbconn.IsLockContentionError(err), "want lock contention (1205), got: %v", err)
			})
		})
	}
}
