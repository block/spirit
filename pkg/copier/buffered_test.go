package copier

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestBufferedCopier(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS bufferedt1, bufferedt2")
	testutils.RunSQL(t, "CREATE TABLE bufferedt1 (a INT NOT NULL, b INT, c VARCHAR(255), d VARBINARY(255), e JSON, f DATETIME, g TIMESTAMP, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE bufferedt2 (a INT NOT NULL, b INT, c VARCHAR(255), d VARBINARY(255), e JSON, f DATETIME, g TIMESTAMP, PRIMARY KEY (a))")

	// Insert all sorts of evil data.
	testutils.RunSQL(t, "INSERT INTO bufferedt1 VALUES (1, NULL, 'normal'' string', RANDOM_BYTES(10), JSON_ARRAY(1,2,3), NOW(), NOW())")
	testutils.RunSQL(t, `INSERT INTO bufferedt1 VALUES (2, 42, 'string with \\ backslash', RANDOM_BYTES(10), JSON_OBJECT('key', 'value\\ \''), NOW(), NOW())`)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.Equal(t, 0, db.Stats().InUse) // no connections in use.

	t1 := table.NewTableInfo(db, "test", "bufferedt1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "bufferedt2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg := NewCopierDefaultConfig()
	cfg.Buffered = true
	target := applier.Target{
		DB:       db,
		KeyRange: "0",
	}
	cfg.Applier, err = applier.NewSingleTargetApplier(target, applier.NewApplierDefaultConfig())
	require.NoError(t, err)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2, TargetChunkTime: cfg.TargetChunkTime, Logger: cfg.Logger})
	require.NoError(t, err)

	require.NoError(t, chunker.Open())

	copier, err := NewCopier(db, chunker, cfg)
	require.NoError(t, err)
	require.NoError(t, copier.Run(t.Context())) // works.

	// We should expect to have the same number of rows
	// and a basic checksum confirms a match.
	var checksumSrc, checksumDst string
	err = db.QueryRowContext(t.Context(), "SELECT BIT_XOR(CRC32(CONCAT(a, IFNULL(b, ''), c, d, e, f, g))) as checksum FROM bufferedt1").Scan(&checksumSrc)
	require.NoError(t, err)

	err = db.QueryRowContext(t.Context(), "SELECT BIT_XOR(CRC32(CONCAT(a, IFNULL(b, ''), c, d, e, f, g))) as checksum FROM bufferedt2").Scan(&checksumDst)
	require.NoError(t, err)
	require.Equal(t, checksumSrc, checksumDst, "Checksums do not match between source and destination tables")

	require.Equal(t, 0, db.Stats().InUse) // no connections in use.
}

// TestBufferedCopierCharsetConversion tests that the buffered copier
// handles charset conversions correctly.
//
// In the unbuffered copier, we don't really have to worry about this because
// MySQL can infer source and dest charset from the INSERT.. SELECT
// and do any conversion that is required.
//
// In the buffered copier, we need to set the connection charset to utf8mb4.
// For this test, what this means is that on *read* of charsetsrc, the characters
// will be converted from latin1 to utf8mb4 by the MySQL server. We then insert
// into charsetdst as utf8mb4 characters.
//
// In the reverse direction (utf8mb4 -> latin1) the server knows that the client
// is in utf8mb4 and is able to convert on insert to match the tables requirements.
func TestBufferedCopierCharsetConversion(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS charsetsrc, charsetdst")
	testutils.RunSQL(t, "CREATE TABLE charsetsrc (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, b VARCHAR(100) NOT NULL) CHARSET=latin1")
	testutils.RunSQL(t, "CREATE TABLE charsetdst (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, b VARCHAR(100) NOT NULL) CHARSET=utf8mb4")

	// Insert rows with special characters that exist in latin1
	// 'à' (U+00E0) and '€' (U+20AC) are both valid in latin1
	testutils.RunSQL(t, "INSERT INTO charsetsrc VALUES (NULL, 'à'), (NULL, '€')")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "charsetsrc")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "charsetdst")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg := NewCopierDefaultConfig()
	cfg.Buffered = true
	cfg.Applier, err = applier.NewSingleTargetApplier(applier.Target{DB: db}, applier.NewApplierDefaultConfig())
	require.NoError(t, err)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2, TargetChunkTime: cfg.TargetChunkTime, Logger: cfg.Logger})
	require.NoError(t, err)

	require.NoError(t, chunker.Open())

	copier, err := NewCopier(db, chunker, cfg)
	require.NoError(t, err)

	// The copy should succeed because we set the connection charset to utf8mb4
	// On read from the src it will be converted from latin1 to utf8mb4
	err = copier.Run(t.Context())
	require.NoError(t, err, "Charset conversion from latin1 to utf8mb4 should succeed")

	// Reverse the copy to show the other direction works too
	// Start by emptying the "src" table, which is our intended destination.
	testutils.RunSQL(t, "TRUNCATE TABLE charsetsrc")
	chunker, err = table.NewChunker(t2, table.ChunkerConfig{NewTable: t1, TargetChunkTime: cfg.TargetChunkTime, Logger: cfg.Logger})
	require.NoError(t, err)
	copier, err = NewCopier(db, chunker, cfg)
	require.NoError(t, err)
	require.NoError(t, chunker.Open())
	err = copier.Run(t.Context())
	require.NoError(t, err, "Charset conversion from utf8mb4 to latin1 should succeed")
}

// TestBufferedCopierDataTypeConversionError tests that the buffered copier
// returns an error when data cannot be converted to the target column type,
// and that it stops processing additional chunks after encountering the error.
// This test reproduces the issue from TestChangeDatatypeDataLoss where the
// buffered copier continues despite conversion errors in the applier callback.
func TestBufferedCopierDataTypeConversionError(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS datatypesrc, datatypedst")
	testutils.RunSQL(t, "CREATE TABLE datatypesrc (id INT NOT NULL PRIMARY KEY auto_increment, b VARCHAR(255))")
	testutils.RunSQL(t, "CREATE TABLE datatypedst (id INT NOT NULL PRIMARY KEY auto_increment, b INT)")

	// Insert enough rows to create multiple chunks
	// The first row has an error, so the copier should fail early
	// and not process all the remaining chunks
	testutils.RunSQL(t, "INSERT INTO datatypesrc (id, b) VALUES (NULL, 'not_a_number')")
	testutils.RunSQL(t, "INSERT INTO datatypesrc (id, b) SELECT NULL, 'not_a_number' FROM datatypesrc a JOIN datatypesrc b JOIN datatypesrc c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO datatypesrc (id, b) SELECT NULL, 'not_a_number' FROM datatypesrc a JOIN datatypesrc b JOIN datatypesrc c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO datatypesrc (id, b) SELECT NULL, 'not_a_number' FROM datatypesrc a JOIN datatypesrc b JOIN datatypesrc c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO datatypesrc (id, b) SELECT NULL, 'not_a_number' FROM datatypesrc a JOIN datatypesrc b JOIN datatypesrc c LIMIT 100000")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "datatypesrc")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "datatypedst")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg := NewCopierDefaultConfig()
	cfg.Buffered = true
	cfg.TargetChunkTime = 10 // Small chunk time to create more chunks
	cfg.Applier, err = applier.NewSingleTargetApplier(applier.Target{DB: db}, applier.NewApplierDefaultConfig())
	require.NoError(t, err)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2, TargetChunkTime: cfg.TargetChunkTime, Logger: cfg.Logger})
	require.NoError(t, err)

	require.NoError(t, chunker.Open())

	copier, err := NewCopier(db, chunker, cfg)
	require.NoError(t, err)

	// Run the copier - should fail with conversion error
	err = copier.Run(t.Context())
	require.Error(t, err, "Copier should return an error when data conversion fails")

	// Verify early exit by checking how many chunks were processed
	_, chunksCopied, _ := copier.GetChunker().Progress()
	require.Less(t, chunksCopied, uint64(10))

	// Also check destination table is zero
	var copiedRows int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM datatypedst").Scan(&copiedRows)
	require.NoError(t, err)
	require.Equal(t, 0, copiedRows, "No rows should have been copied to destination table due to conversion error")
}

// TestBufferedCopierChunkTimingIncludesCallbackDelay verifies that the chunk timing
// reported to chunker.Feedback() and sendMetrics includes the async write phase
// (via the applier callback) instead of only the read time.
//
// This test addresses the behavioral change where chunk timing now includes both:
// 1. The time to read the chunk data from the source
// 2. The time for the applier to flush the data (measured via callback invocation)
//
// The test uses a stub applier that introduces a controlled delay before invoking
// the callback, and a mock chunker to capture the duration passed to Feedback().
func TestBufferedCopierChunkTimingIncludesCallbackDelay(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	// Create test tables
	testutils.RunSQL(t, "DROP TABLE IF EXISTS timing_test_src, timing_test_dst")
	testutils.RunSQL(t, "CREATE TABLE timing_test_src (id INT NOT NULL PRIMARY KEY, data VARCHAR(100))")
	testutils.RunSQL(t, "CREATE TABLE timing_test_dst (id INT NOT NULL PRIMARY KEY, data VARCHAR(100))")

	// Insert test data - enough for one chunk
	testutils.RunSQL(t, "INSERT INTO timing_test_src VALUES (1, 'test1'), (2, 'test2'), (3, 'test3')")

	t1 := table.NewTableInfo(db, "test", "timing_test_src")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "timing_test_dst")
	require.NoError(t, t2.SetInfo(t.Context()))

	// Create copier config first so we can use its logger
	cfg := NewCopierDefaultConfig()
	cfg.Buffered = true

	// Create a real chunker (we need real chunk metadata for the copier)
	realChunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2, TargetChunkTime: 1000 * time.Millisecond, Logger: cfg.Logger})
	require.NoError(t, err)
	require.NoError(t, realChunker.Open())

	// Wrap it to capture feedback calls
	wrappedChunker := &feedbackCapturingChunker{
		Chunker:       realChunker,
		feedbackCalls: make([]feedbackCall, 0),
	}

	// Create a stub applier that introduces a controlled delay
	// Use a large delay (500ms) to ensure it's significantly larger than any expected
	// read time, even on slow CI runners. This prevents false positives where a slow
	// read could satisfy the timing assertion even if the copier reverted to reporting
	// read-only time.
	callbackDelay := 500 * time.Millisecond
	stubApplier := &delayedCallbackApplier{
		realApplier: nil, // We'll set this after creating it
		delay:       callbackDelay,
	}

	// Create the real applier
	realApplier, err := applier.NewSingleTargetApplier(
		applier.Target{DB: db, KeyRange: "0"},
		applier.NewApplierDefaultConfig(),
	)
	require.NoError(t, err)
	stubApplier.realApplier = realApplier

	// Set our stub applier and concurrency in the config
	cfg.Applier = stubApplier
	cfg.Concurrency = 1 // Single worker for predictable behavior

	// Create the copier via the public constructor to match production configuration
	copier, err := NewCopier(db, wrappedChunker, cfg)
	require.NoError(t, err)

	// Run the copier with a context that won't timeout during the delay
	// We need to ensure the context lives long enough for the callback delay
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = copier.Run(ctx)
	require.NoError(t, err)

	// Get feedback calls from the wrapped chunker
	feedbackCalls := wrappedChunker.GetFeedbackCalls()
	require.Len(t, feedbackCalls, 1, "Expected exactly one feedback call for one chunk")

	// Verify the duration includes the callback delay
	// The total time should be: read time + callback delay
	// We can't precisely measure read time, but we know it should be much less than the delay
	// So the total should be at least the callback delay
	actualDuration := feedbackCalls[0].duration
	require.GreaterOrEqual(t, actualDuration, callbackDelay,
		"Chunk timing should include the callback delay (read + write time)")

	// Verify the rows were actually copied
	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM timing_test_dst").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 3, count, "All rows should be copied")
}

// feedbackCall captures the parameters passed to chunker.Feedback()
type feedbackCall struct {
	chunk      *table.Chunk
	duration   time.Duration
	actualRows uint64
	timestamp  time.Time
}

// feedbackCapturingChunker wraps a real chunker to capture Feedback() calls
type feedbackCapturingChunker struct {
	table.Chunker
	feedbackCalls []feedbackCall
	mu            sync.Mutex
}

func (f *feedbackCapturingChunker) Feedback(chunk *table.Chunk, duration time.Duration, actualRows uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.feedbackCalls = append(f.feedbackCalls, feedbackCall{
		chunk:      chunk,
		duration:   duration,
		actualRows: actualRows,
		timestamp:  time.Now(),
	})

	// Call the underlying chunker's Feedback
	f.Chunker.Feedback(chunk, duration, actualRows)
}

func (f *feedbackCapturingChunker) GetFeedbackCalls() []feedbackCall {
	f.mu.Lock()
	defer f.mu.Unlock()

	result := make([]feedbackCall, len(f.feedbackCalls))
	copy(result, f.feedbackCalls)
	return result
}

// delayedCallbackApplier is a stub applier that wraps a real applier
// and introduces a controlled delay before invoking the callback.
// This simulates the async write phase taking time.
type delayedCallbackApplier struct {
	realApplier applier.Applier
	delay       time.Duration
}

func (d *delayedCallbackApplier) Start(ctx context.Context) error {
	return d.realApplier.Start(ctx)
}

func (d *delayedCallbackApplier) Apply(ctx context.Context, chunk *table.Chunk, rows [][]any, callback applier.ApplyCallback) error {
	// Wrap the callback to add delay
	wrappedCallback := func(affectedRows int64, err error) {
		// Introduce delay to simulate write time.
		// We use time.Sleep instead of a timer with context cancellation because
		// we want to simulate a real write operation that takes time to complete,
		// not one that can be canceled mid-flight. This ensures the test accurately
		// measures the full duration including the simulated write time.
		time.Sleep(d.delay)
		callback(affectedRows, err)
	}

	// Call the real applier with the wrapped callback
	return d.realApplier.Apply(ctx, chunk, rows, wrappedCallback)
}

func (d *delayedCallbackApplier) DeleteKeys(ctx context.Context, sourceTable, targetTable *table.TableInfo, keys []string, lock *dbconn.TableLock) (int64, error) {
	return d.realApplier.DeleteKeys(ctx, sourceTable, targetTable, keys, lock)
}

func (d *delayedCallbackApplier) UpsertRows(ctx context.Context, mapping *table.ColumnMapping, rows []applier.LogicalRow, lock *dbconn.TableLock) (int64, error) {
	return d.realApplier.UpsertRows(ctx, mapping, rows, lock)
}

func (d *delayedCallbackApplier) Wait(ctx context.Context) error {
	return d.realApplier.Wait(ctx)
}

func (d *delayedCallbackApplier) Stop() error {
	return d.realApplier.Stop()
}

func (d *delayedCallbackApplier) GetTargets() []applier.Target {
	return d.realApplier.GetTargets()
}
