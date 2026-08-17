package checksum

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// testChunker is a deterministic Chunker for unit-testing the dispatcher
// without standing up real databases. It yields a fixed slice of chunks
// on each pass; Reset() rewinds. Safe for concurrent calls from the
// walker goroutine.
type testChunker struct {
	mu       sync.Mutex
	chunks   []*table.Chunk
	cursor   int
	resets   int
	feedback []table.FeedbackCall
	closed   bool
}

func newTestChunker(n int) *testChunker {
	chunks := make([]*table.Chunk, n)
	for i := range n {
		chunks[i] = newTestChunk(uint64(i*1000), uint64((i+1)*1000))
	}
	return &testChunker{chunks: chunks}
}

func newTestChunk(lo, hi uint64) *table.Chunk {
	loDatum, err := table.NewDatumFromValue(lo, "bigint unsigned")
	if err != nil {
		panic(err)
	}
	hiDatum, err := table.NewDatumFromValue(hi, "bigint unsigned")
	if err != nil {
		panic(err)
	}
	return &table.Chunk{
		Key:        []string{"id"},
		ChunkSize:  hi - lo,
		LowerBound: &table.Boundary{Value: []table.Datum{loDatum}, Inclusive: true},
		UpperBound: &table.Boundary{Value: []table.Datum{hiDatum}, Inclusive: false},
		Table:      &table.TableInfo{SchemaName: "test", TableName: "t"},
		NewTable:   &table.TableInfo{SchemaName: "test", TableName: "t"},
	}
}

func (c *testChunker) Open() error  { return nil }
func (c *testChunker) Close() error { c.mu.Lock(); defer c.mu.Unlock(); c.closed = true; return nil }
func (c *testChunker) IsRead() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cursor >= len(c.chunks)
}
func (c *testChunker) Next() (*table.Chunk, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cursor >= len(c.chunks) {
		return nil, table.ErrTableIsRead
	}
	ch := c.chunks[c.cursor]
	c.cursor++
	return ch, nil
}
func (c *testChunker) Feedback(chunk *table.Chunk, duration time.Duration, actualRows uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.feedback = append(c.feedback, table.FeedbackCall{Chunk: chunk, Duration: duration, ActualRows: actualRows, Timestamp: time.Now()})
}
func (c *testChunker) RowsCopied() uint64 { return 0 }

func (c *testChunker) Progress() (uint64, uint64, uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return uint64(c.cursor), uint64(c.cursor), uint64(len(c.chunks))
}
func (c *testChunker) OpenAtWatermark(watermark string) error { return nil }
func (c *testChunker) GetLowWatermark() (string, error)       { return "", nil }
func (c *testChunker) Reset() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cursor = 0
	c.resets++
	return nil
}
func (c *testChunker) Tables() []*table.TableInfo { return nil }

// newTestChecker builds a checker with a swapped readChunk hook. The hook
// receives the chunk and an attempt counter (incremented each call for the
// same chunk pointer) so tests can express "fail twice, then pass" etc.
//
// We pass nil DB pointers (allowed because readChunk is swapped) but the
// constructor requires non-nil, so use minimal sentinel values.
func newTestChecker(t *testing.T, chunker table.Chunker, cfg ContinuousCheckerConfig,
	read func(ctx context.Context, chunk *table.Chunk, attempt int) (srcCRC, tgtCRC int64, tgtCount uint64, err error),
) *ContinuousChecker {
	t.Helper()
	// Constructor demands non-nil DBs; we pass empty *sql.DB pointers — they
	// are never used because readChunk is swapped before Run.
	srcDB, tgtDB := &sql.DB{}, &sql.DB{}
	c, err := NewContinuousChecker(srcDB, tgtDB, chunker, nil, cfg)
	require.NoError(t, err)

	attempts := sync.Map{}
	c.readChunk = func(ctx context.Context, chunk *table.Chunk) (int64, int64, uint64, uint64, error) {
		var n int
		if v, ok := attempts.Load(chunk); ok {
			n = v.(int) + 1
		} else {
			n = 1
		}
		attempts.Store(chunk, n)
		srcCRC, tgtCRC, count, err := read(ctx, chunk, n)
		// The legacy 3-value read hook supplies a single row count that
		// applies to BOTH source and target — these tests exercise CRC-only
		// divergence with matching counts. Tests that need divergent counts
		// use newTestCheckerSig below.
		return srcCRC, tgtCRC, count, count, err
	}
	return c
}

// newTestCheckerSig is like newTestChecker but the read hook returns full
// signatures (CRC + count) for source and target independently, so tests can
// exercise row-count divergence with matching CRCs (the defense-in-depth gap
// this comparison closes).
func newTestCheckerSig(t *testing.T, chunker table.Chunker, cfg ContinuousCheckerConfig,
	read func(ctx context.Context, chunk *table.Chunk, attempt int) (srcCRC, tgtCRC int64, srcCount, tgtCount uint64, err error),
) *ContinuousChecker {
	t.Helper()
	srcDB, tgtDB := &sql.DB{}, &sql.DB{}
	c, err := NewContinuousChecker(srcDB, tgtDB, chunker, nil, cfg)
	require.NoError(t, err)

	attempts := sync.Map{}
	c.readChunk = func(ctx context.Context, chunk *table.Chunk) (int64, int64, uint64, uint64, error) {
		var n int
		if v, ok := attempts.Load(chunk); ok {
			n = v.(int) + 1
		} else {
			n = 1
		}
		attempts.Store(chunk, n)
		return read(ctx, chunk, n)
	}
	return c
}

// runUntil starts ctr.Run in a goroutine and returns:
//   - a stop function that cancels and waits for Run to exit
//   - a channel that receives Run's return value
func runUntil(t *testing.T, c *ContinuousChecker) (stop func() error, errCh <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan error, 1)
	go func() {
		out <- c.Run(ctx)
	}()
	return func() error {
		cancel()
		select {
		case err := <-out:
			return err
		case <-time.After(5 * time.Second):
			return errors.New("Run did not return within 5s of cancel")
		}
	}, out
}

// fastConfig is a default config tuned for fast tests: 50ms retry delay,
// silent logger.
func fastConfig() ContinuousCheckerConfig {
	return ContinuousCheckerConfig{
		Concurrency:     4,
		RetryDelay:      50 * time.Millisecond,
		MaxQueueSize:    16,
		TargetChunkTime: time.Second,
		Logger:          slog.New(slog.NewTextHandler(testWriter{}, &slog.HandlerOptions{Level: slog.LevelError})),
	}
}

// testWriter discards log output unless a test wants to read it.
type testWriter struct{}

func (testWriter) Write(p []byte) (int, error) { return len(p), nil }

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestContinuousMinPassIntervalPacesPasses verifies MinPassInterval throttles
// the gap between passes: the first pass runs immediately, then each subsequent
// pass waits until MinPassInterval has elapsed since the previous pass started.
// With an always-clean table, reaching 3 passes therefore cannot happen before
// 2*MinPassInterval. Only the lower bound is asserted (the upper bound would be
// timing-flaky).
func TestContinuousMinPassIntervalPacesPasses(t *testing.T) {
	const interval = 100 * time.Millisecond
	cfg := fastConfig()
	cfg.MinPassInterval = interval
	chunker := newTestChunker(1)
	c := newTestChecker(t, chunker, cfg,
		func(ctx context.Context, chunk *table.Chunk, attempt int) (int64, int64, uint64, error) {
			return 7, 7, 100, nil // always match
		},
	)

	start := time.Now()
	stop, _ := runUntil(t, c)
	t.Cleanup(func() { _ = stop() })

	require.Eventually(t, func() bool { return c.Stats().PassesCompleted >= 3 },
		5*time.Second, 5*time.Millisecond, "expected at least 3 passes")
	require.GreaterOrEqual(t, time.Since(start), 2*interval,
		"three passes must span at least two inter-pass intervals; pacing not applied")
}

// TestCleanPassQuietTable: every chunk matches on first read; first clean
// pass should fire promptly and counters should reflect the pass.
func TestCleanPassQuietTable(t *testing.T) {
	chunker := newTestChunker(10)
	c := newTestChecker(t, chunker, fastConfig(),
		func(ctx context.Context, chunk *table.Chunk, attempt int) (int64, int64, uint64, error) {
			return 42, 42, 1000, nil
		},
	)

	stop, _ := runUntil(t, c)

	select {
	case <-c.FirstCleanPass():
		// good — fired
	case <-time.After(2 * time.Second):
		t.Fatal("FirstCleanPass did not fire within 2s on quiet table")
	}

	stats := c.Stats()
	require.GreaterOrEqual(t, stats.PassesCompleted, uint64(1))
	require.Equal(t, uint64(0), stats.MismatchesDetected)
	require.Equal(t, 0, stats.RetryQueueDepth)
	require.Equal(t, uint64(0), stats.PermanentFailures)
	require.False(t, stats.FirstCleanPassAt.IsZero())

	err := stop()
	require.True(t, errors.Is(err, context.Canceled) || err == nil, "expected cancellation, got %v", err)
}

// TestTargetCatchesUpToOriginal: chunk mismatches on first read; on retry,
// the target CRC matches the *original* source CRC (i.e. target caught up
// to the version we first saw on the source). Counts as a pass.
func TestTargetCatchesUpToOriginal(t *testing.T) {
	chunker := newTestChunker(3)
	c := newTestChecker(t, chunker, fastConfig(),
		func(ctx context.Context, chunk *table.Chunk, attempt int) (int64, int64, uint64, error) {
			if attempt == 1 {
				// First read: target lags; src=100, target=99
				return 100, 99, 1000, nil
			}
			// Retry: target has caught up to original src=100; src may
			// also still be 100. tgtCRC == originalSrcCRC ⇒ pass.
			return 100, 100, 1000, nil
		},
	)

	stop, _ := runUntil(t, c)
	select {
	case <-c.FirstCleanPass():
	case <-time.After(2 * time.Second):
		t.Fatal("FirstCleanPass did not fire")
	}
	stats := c.Stats()
	require.Equal(t, uint64(3), stats.MismatchesDetected, "every chunk first-read mismatched")
	require.Equal(t, uint64(0), stats.PermanentFailures)
	require.Equal(t, 0, stats.RetryQueueDepth)
	err := stop()
	require.True(t, errors.Is(err, context.Canceled) || err == nil)
}

// TestTargetCatchesUpToNewSource: source ALSO advanced during retry
// window, but target matches the new source CRC. tgtCRC == newSrcCRC ⇒
// pass without entering hot-chunk path.
func TestTargetCatchesUpToNewSource(t *testing.T) {
	chunker := newTestChunker(2)
	c := newTestChecker(t, chunker, fastConfig(),
		func(ctx context.Context, chunk *table.Chunk, attempt int) (int64, int64, uint64, error) {
			if attempt == 1 {
				return 100, 99, 1000, nil // src=100, tgt=99
			}
			// On retry: src moved to 200, target also at 200 (target
			// applied the same change). tgtCRC == newSrcCRC ⇒ pass.
			return 200, 200, 1000, nil
		},
	)
	stop, _ := runUntil(t, c)
	select {
	case <-c.FirstCleanPass():
	case <-time.After(2 * time.Second):
		t.Fatal("FirstCleanPass did not fire")
	}
	stats := c.Stats()
	require.Equal(t, uint64(2), stats.MismatchesDetected)
	require.Equal(t, uint64(0), stats.PermanentFailures)
	err := stop()
	require.True(t, errors.Is(err, context.Canceled) || err == nil)
}

// TestHotChunkConverges: source CRC keeps changing for a few retries, then
// settles. Chunk eventually passes; pass completes; FirstCleanPass fires.
// HotChunkCount briefly observable mid-test.
func TestHotChunkConverges(t *testing.T) {
	chunker := newTestChunker(1)
	c := newTestChecker(t, chunker, fastConfig(),
		func(ctx context.Context, chunk *table.Chunk, attempt int) (int64, int64, uint64, error) {
			switch attempt {
			case 1:
				return 100, 99, 1000, nil // mismatch
			case 2:
				return 200, 99, 1000, nil // src changed, tgt unchanged ⇒ hot, re-queue
			case 3:
				return 300, 200, 1000, nil // src changed AGAIN, tgt matches PREVIOUS src ⇒ hot per current code (tgt != originalSrc which is now 200, tgt != newSrc=300, src != original=200 ⇒ hot, replace original with 300)
			case 4:
				return 300, 300, 1000, nil // pass
			default:
				return 300, 300, 1000, nil
			}
		},
	)
	stop, _ := runUntil(t, c)
	select {
	case <-c.FirstCleanPass():
	case <-time.After(2 * time.Second):
		t.Fatalf("FirstCleanPass did not fire; stats=%+v", c.Stats())
	}
	stats := c.Stats()
	require.Equal(t, uint64(1), stats.MismatchesDetected)
	require.Equal(t, uint64(0), stats.PermanentFailures)
	require.Equal(t, 0, stats.RetryQueueDepth, "queue should drain to empty after convergence")
	err := stop()
	require.True(t, errors.Is(err, context.Canceled) || err == nil)
}

// TestPermanentDivergence: chunk mismatches initially; on retry, source CRC
// unchanged but target CRC still wrong ⇒ ErrPermanentDivergence.
func TestPermanentDivergence(t *testing.T) {
	chunker := newTestChunker(1)
	c := newTestChecker(t, chunker, fastConfig(),
		func(ctx context.Context, chunk *table.Chunk, attempt int) (int64, int64, uint64, error) {
			// src always 100, tgt always 99 — target diverged for real.
			return 100, 99, 1000, nil
		},
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := c.Run(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrPermanentDivergence, "expected ErrPermanentDivergence, got %v", err)
	stats := c.Stats()
	require.Equal(t, uint64(1), stats.PermanentFailures)
	require.Equal(t, uint64(0), stats.PassesCompleted, "no pass should have completed")
	// FirstCleanPass should NOT have fired.
	select {
	case <-c.FirstCleanPass():
		t.Fatal("FirstCleanPass fired despite permanent divergence")
	default:
	}
}

// TestQueueCapBackpressure: many chunks mismatch, retry delay is long,
// queue fills up to MaxQueueSize. The walker should be back-pressured —
// not aborted — so Run blocks until ctx cancels, with WalkerStalls > 0
// in the stats snapshot and RetryQueueDepth capped at MaxQueueSize.
func TestQueueCapBackpressure(t *testing.T) {
	chunker := newTestChunker(20)
	cfg := fastConfig()
	cfg.MaxQueueSize = 4
	cfg.RetryDelay = 10 * time.Second // retries won't fire during test
	cfg.Concurrency = 1               // serialize so the walker outpaces retry drain

	c := newTestChecker(t, chunker, cfg,
		func(ctx context.Context, chunk *table.Chunk, attempt int) (int64, int64, uint64, error) {
			return 100, 99, 1000, nil // every chunk mismatches
		},
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := c.Run(ctx)
	// Clean back-pressure now: ctx cancellation is the only exit.
	require.ErrorIs(t, err, context.DeadlineExceeded,
		"expected context.DeadlineExceeded, got %v", err)
	stats := c.Stats()
	require.GreaterOrEqual(t, stats.WalkerStalls, uint64(1),
		"expected at least one walker stall while queue was at MaxQueueSize")
	// Queue can briefly exceed MaxQueueSize. Three sources of work have
	// already passed the back-pressure gate (which only throttles fresh
	// walker intake) by the time the queue reaches MaxQueueSize, and each
	// becomes a retry when its result is serviced:
	//   - in-flight workers              (up to Concurrency)
	//   - results buffered in resultCh   (up to Concurrency)
	//   - the single-slot pendingFresh prefetch the dispatcher stages
	//     one-ahead of itself             (1)
	// so the worst-case overshoot is 2*Concurrency+1, not 2*Concurrency.
	// (Concurrency=1 here => bound 7; the +1 slot is what made the old
	// 2*Concurrency bound flake at depth 7.) The test cares that the queue
	// stays bounded near MaxQueueSize, not unbounded — anything dramatically
	// larger would be the symptom of a real back-pressure failure. See
	// enqueueRetry's doc comment in continuous.go.
	require.LessOrEqual(t, stats.RetryQueueDepth, cfg.MaxQueueSize+2*cfg.Concurrency+1,
		"queue depth should stay bounded under back-pressure")
}

// TestFirstCleanPassMonotonic: after first clean pass, subsequent drift
// MUST NOT close/re-open the signal. We verify by stopping after observing
// the close and checking again post-stop.
func TestFirstCleanPassMonotonic(t *testing.T) {
	chunker := newTestChunker(2)
	c := newTestChecker(t, chunker, fastConfig(),
		func(ctx context.Context, chunk *table.Chunk, attempt int) (int64, int64, uint64, error) {
			return 42, 42, 1000, nil
		},
	)
	stop, _ := runUntil(t, c)
	select {
	case <-c.FirstCleanPass():
	case <-time.After(2 * time.Second):
		t.Fatal("FirstCleanPass did not fire")
	}
	// Channel must remain closed for the lifetime of the checker.
	for range 5 {
		select {
		case _, ok := <-c.FirstCleanPass():
			require.False(t, ok, "FirstCleanPass channel re-opened — signal not monotonic")
		default:
			t.Fatal("FirstCleanPass channel reverted to open")
		}
	}
	err := stop()
	require.True(t, errors.Is(err, context.Canceled) || err == nil)
}

// TestReadError: a read error from the source/target read function should
// propagate out of Run.
func TestReadError(t *testing.T) {
	chunker := newTestChunker(1)
	readErr := errors.New("connection refused")
	c := newTestChecker(t, chunker, fastConfig(),
		func(ctx context.Context, chunk *table.Chunk, attempt int) (int64, int64, uint64, error) {
			return 0, 0, 0, readErr
		},
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := c.Run(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, readErr, "expected wrapped read error, got %v", err)
}

// ---------------------------------------------------------------------------
// Recopier tests
// ---------------------------------------------------------------------------

// fakeRecopier is a Recopier used by tests. recopyFn is what fires when
// the checker decides to recopy; calls increments per invocation so tests
// can assert "the recopier was called N times".
type fakeRecopier struct {
	mu       sync.Mutex
	calls    int
	chunks   []*table.Chunk
	recopyFn func(ctx context.Context, chunk *table.Chunk) error
}

func (r *fakeRecopier) Recopy(ctx context.Context, chunk *table.Chunk) error {
	r.mu.Lock()
	r.calls++
	r.chunks = append(r.chunks, chunk)
	fn := r.recopyFn
	r.mu.Unlock()
	if fn != nil {
		return fn(ctx, chunk)
	}
	return nil
}

func (r *fakeRecopier) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

// TestRecopyOnStableDivergence: a chunk mismatches twice with the source
// CRC unchanged. With a Recopier configured, the checker calls Recopy
// instead of returning ErrPermanentDivergence; the pass completes with
// the chunk counted in the recopies bucket, and FirstCleanPass fires on
// the follow-up pass that re-verifies the repaired chunks.
func TestRecopyOnStableDivergence(t *testing.T) {
	chunker := newTestChunker(2)
	// The fake recopier "fixes" the chunk so subsequent reads pass. We
	// gate behavior on whether the chunk has been recopied: after recopy,
	// the readChunk hook returns (42, 42); before recopy it returns (100, 99).
	var recopied sync.Map // chunk pointer → recopied? (bool)
	recopier := &fakeRecopier{
		recopyFn: func(ctx context.Context, chunk *table.Chunk) error {
			recopied.Store(chunk, true)
			return nil
		},
	}
	cfg := fastConfig()
	cfg.Recopier = recopier

	c := newTestChecker(t, chunker, cfg,
		func(ctx context.Context, chunk *table.Chunk, attempt int) (int64, int64, uint64, error) {
			if _, ok := recopied.Load(chunk); ok {
				return 42, 42, 1000, nil // post-recopy reads match
			}
			return 100, 99, 1000, nil // pre-recopy mismatch (stable: src always 100)
		},
	)

	stop, _ := runUntil(t, c)
	select {
	case <-c.FirstCleanPass():
	case <-time.After(2 * time.Second):
		t.Fatalf("FirstCleanPass did not fire; stats=%+v calls=%d", c.Stats(), recopier.callCount())
	}
	stats := c.Stats()
	require.Equal(t, uint64(0), stats.PermanentFailures, "with a Recopier, no permanent failures")
	require.GreaterOrEqual(t, recopier.callCount(), 2, "both chunks should have been recopied")

	err := stop()
	require.True(t, errors.Is(err, context.Canceled) || err == nil)
}

// TestDivergenceIsFatalAbortsDespiteRecopier: with DivergenceIsFatal set, a
// confirmed stable divergence returns ErrPermanentDivergence and the Recopier
// is NOT invoked, even though one is configured. This is the migration cutover
// gate's policy made explicit (vs. datasync, which leaves it false and heals).
func TestDivergenceIsFatalAbortsDespiteRecopier(t *testing.T) {
	chunker := newTestChunker(1)
	recopier := &fakeRecopier{} // must never be called
	cfg := fastConfig()
	cfg.Recopier = recopier
	cfg.DivergenceIsFatal = true

	c := newTestChecker(t, chunker, cfg,
		func(ctx context.Context, chunk *table.Chunk, attempt int) (int64, int64, uint64, error) {
			return 100, 99, 1000, nil // stable divergence: src always 100, tgt 99
		},
	)

	// Run returns ErrPermanentDivergence on its own; t.Context() is cancelled at
	// test cleanup, which tears down any remaining workers.
	err := c.Run(t.Context())
	require.ErrorIs(t, err, ErrPermanentDivergence,
		"DivergenceIsFatal must abort with ErrPermanentDivergence even with a Recopier set")
	require.Equal(t, 0, recopier.callCount(), "the Recopier must not be called when DivergenceIsFatal")
	require.Positive(t, c.Stats().PermanentFailures)
}

// fakeFeed is a minimal change.Source double for continuous-checksum tests.
// Only Flush carries behaviour — it runs flushFn so a test can simulate the
// target catching up as buffered changes are applied (apply lag draining).
// Every other method is an inert no-op.
type fakeFeed struct {
	flushFn func(ctx context.Context) error
	flushes atomic.Int64
}

var _ change.Source = (*fakeFeed)(nil)

func (f *fakeFeed) Flush(ctx context.Context) error {
	f.flushes.Add(1)
	if f.flushFn != nil {
		return f.flushFn(ctx)
	}
	return nil
}
func (f *fakeFeed) AddSubscription(_, _ *table.TableInfo, _ table.MappedChunker) error { return nil }
func (f *fakeFeed) Start(context.Context) error                                        { return nil }
func (f *fakeFeed) StartFromPosition(context.Context, string) error                    { return nil }
func (f *fakeFeed) Position() string                                                   { return "" }
func (f *fakeFeed) CurrentPosition(context.Context) (string, error)                    { return "", nil }
func (f *fakeFeed) FlushUnderTableLock(context.Context, []*dbconn.TableLock) error     { return nil }
func (f *fakeFeed) BlockWait(context.Context) error                                    { return nil }
func (f *fakeFeed) GetDeltaLen() int                                                   { return 0 }

func (f *fakeFeed) FlushResidual() (int, int)                            { return 0, 0 }
func (f *fakeFeed) SetWatermarkOptimization(context.Context, bool) error { return nil }
func (f *fakeFeed) StartPeriodicFlush(context.Context, time.Duration)    {}
func (f *fakeFeed) StopPeriodicFlush()                                   {}
func (f *fakeFeed) AllChangesFlushed() bool                              { return true }
func (f *fakeFeed) Stop()                                                {}
func (f *fakeFeed) Close()                                               {}

// TestDivergenceIsFatalReconcilesApplyLag is the regression test for the
// false-positive cutover abort: a chunk that is merely behind on applying
// buffered changes (apply lag) must NOT be reported as a fatal divergence.
// Before declaring a stable divergence on the fatal path, the checker drains
// the change feed and re-reads; once the feed flushes, the target catches up
// and the chunk verifies clean — the same reconciliation the cutover performs
// under its table lock.
func TestDivergenceIsFatalReconcilesApplyLag(t *testing.T) {
	chunker := newTestChunker(1)
	var drained atomic.Bool
	feed := &fakeFeed{
		flushFn: func(context.Context) error { drained.Store(true); return nil },
	}
	cfg := fastConfig()
	cfg.DivergenceIsFatal = true // migration cutover-gate policy; no Recopier

	c := newTestChecker(t, chunker, cfg,
		func(ctx context.Context, chunk *table.Chunk, attempt int) (int64, int64, uint64, error) {
			// Source is stable at 100. The target lags at 99 (a buffered change
			// not yet flushed) until the feed drains, after which it matches.
			if drained.Load() {
				return 100, 100, 1000, nil
			}
			return 100, 99, 1000, nil
		},
	)
	c.feed = feed // attach a feed so the drain-and-confirm path engages

	stop, _ := runUntil(t, c)
	select {
	case <-c.FirstCleanPass():
	case <-time.After(2 * time.Second):
		t.Fatalf("FirstCleanPass did not fire — apply lag misclassified as divergence; stats=%+v", c.Stats())
	}
	require.Equal(t, uint64(0), c.Stats().PermanentFailures, "apply lag must not count as permanent divergence")
	require.True(t, drained.Load(), "the checker must drain the feed before judging divergence")
	require.GreaterOrEqual(t, feed.flushes.Load(), int64(1), "feed.Flush must be called to confirm divergence")

	err := stop()
	require.True(t, errors.Is(err, context.Canceled) || err == nil)
}

// TestDivergenceIsFatalStillAbortsAfterDrain guards the fix above: a genuine
// divergence — one the feed drain does NOT reconcile — must still return
// ErrPermanentDivergence. The drain rules out apply lag; it must not mask real
// corruption.
func TestDivergenceIsFatalStillAbortsAfterDrain(t *testing.T) {
	chunker := newTestChunker(1)
	feed := &fakeFeed{} // Flush is a no-op: draining changes nothing
	cfg := fastConfig()
	cfg.DivergenceIsFatal = true

	c := newTestChecker(t, chunker, cfg,
		func(ctx context.Context, chunk *table.Chunk, attempt int) (int64, int64, uint64, error) {
			return 100, 99, 1000, nil // stable, real divergence; a drain won't fix it
		},
	)
	c.feed = feed

	err := c.Run(t.Context())
	require.ErrorIs(t, err, ErrPermanentDivergence,
		"a divergence that survives a feed drain must still be fatal")
	require.GreaterOrEqual(t, feed.flushes.Load(), int64(1), "the checker must attempt a drain before the fatal verdict")
	require.Positive(t, c.Stats().PermanentFailures)
}

// TestRecopyPassDoesNotFireFirstCleanPass: a chunk stably diverges and is
// recopied. The pass containing the recopy must NOT fire FirstCleanPass —
// a recopy is a repair, not a verification (the rewritten rows were never
// observed equal, and the recopy itself can race the live replication
// feed). The signal must fire only after the following pass re-reads
// every chunk clean with zero recopies.
func TestRecopyPassDoesNotFireFirstCleanPass(t *testing.T) {
	chunker := newTestChunker(1)
	var recopied sync.Map
	recopier := &fakeRecopier{
		recopyFn: func(ctx context.Context, chunk *table.Chunk) error {
			recopied.Store(chunk, true)
			return nil
		},
	}
	cfg := fastConfig()
	cfg.Recopier = recopier

	// gate blocks the first post-recopy read (pass 2's fresh read) until
	// the test has asserted that pass 1 completed without firing the
	// signal. Without it there would be a race between "pass 1 done" and
	// "pass 2 instantly completes and legitimately fires".
	gate := make(chan struct{})
	c := newTestChecker(t, chunker, cfg,
		func(ctx context.Context, chunk *table.Chunk, attempt int) (int64, int64, uint64, error) {
			if _, ok := recopied.Load(chunk); ok {
				select {
				case <-gate:
				case <-ctx.Done():
					return 0, 0, 0, ctx.Err()
				}
				return 42, 42, 1000, nil // post-recopy reads verify clean
			}
			return 100, 99, 1000, nil // stable divergence: src constant, tgt wrong
		},
	)

	stop, _ := runUntil(t, c)

	// Wait for pass 1 — the pass containing the recopy — to complete.
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		stats := c.Stats()
		assert.GreaterOrEqualf(collect, stats.PassesCompleted, uint64(1), "pass 1 did not complete in time; stats=%+v", stats)
	}, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, 1, recopier.callCount(), "chunk should have been recopied in pass 1")

	// Pass 1 contained a recopy, so it must not satisfy the
	// first-clean-pass criterion. Pass 2's read is parked on the gate, so
	// this check cannot race a legitimate later signal.
	select {
	case <-c.FirstCleanPass():
		t.Fatal("FirstCleanPass fired in the pass containing the recopy — recopied data was never read-verified")
	default:
	}

	// Release pass 2's read: the recopied chunk re-verifies equal, the
	// pass completes with zero recopies, and the signal fires.
	close(gate)
	select {
	case <-c.FirstCleanPass():
	case <-time.After(2 * time.Second):
		t.Fatalf("FirstCleanPass did not fire on the follow-up clean pass; stats=%+v", c.Stats())
	}
	stats := c.Stats()
	require.GreaterOrEqual(t, stats.PassesCompleted, uint64(2),
		"signal requires the follow-up pass, so at least 2 passes must have completed")
	require.Equal(t, uint64(0), stats.PermanentFailures)

	err := stop()
	require.True(t, errors.Is(err, context.Canceled) || err == nil)
}

// TestRecopiedChunkReverifiedBeforeCleanPass: by the time FirstCleanPass
// fires, the recopied chunk's range must have been re-read and observed
// equal. The re-read happens on the next pass's fresh walk; the signal
// cannot fire before that pass completes, so observing the signal
// guarantees the re-read happened (happens-before via the pass barrier).
func TestRecopiedChunkReverifiedBeforeCleanPass(t *testing.T) {
	chunker := newTestChunker(2)
	divergent := chunker.chunks[0] // only this chunk diverges
	var recopied sync.Map
	var readAfterRecopy atomic.Bool
	recopier := &fakeRecopier{
		recopyFn: func(ctx context.Context, chunk *table.Chunk) error {
			recopied.Store(chunk, true)
			return nil
		},
	}
	cfg := fastConfig()
	cfg.Recopier = recopier

	c := newTestChecker(t, chunker, cfg,
		func(ctx context.Context, chunk *table.Chunk, attempt int) (int64, int64, uint64, error) {
			if _, ok := recopied.Load(chunk); ok {
				readAfterRecopy.Store(true)
				return 42, 42, 1000, nil // post-recopy read verifies clean
			}
			if chunk == divergent {
				return 100, 99, 1000, nil // stable divergence until recopied
			}
			return 42, 42, 1000, nil
		},
	)

	stop, _ := runUntil(t, c)
	select {
	case <-c.FirstCleanPass():
	case <-time.After(2 * time.Second):
		t.Fatalf("FirstCleanPass did not fire; stats=%+v calls=%d", c.Stats(), recopier.callCount())
	}
	require.Equal(t, 1, recopier.callCount(), "exactly one chunk should have been recopied")
	require.True(t, readAfterRecopy.Load(),
		"recopied chunk was never re-read before FirstCleanPass fired")

	err := stop()
	require.True(t, errors.Is(err, context.Canceled) || err == nil)
}

// TestRecopyFailurePropagates: when the Recopier returns an error, the
// checker propagates it out of Run rather than retrying or silently
// continuing.
func TestRecopyFailurePropagates(t *testing.T) {
	chunker := newTestChunker(1)
	recopyErr := errors.New("simulated recopy failure")
	recopier := &fakeRecopier{
		recopyFn: func(ctx context.Context, chunk *table.Chunk) error {
			return recopyErr
		},
	}
	cfg := fastConfig()
	cfg.Recopier = recopier

	c := newTestChecker(t, chunker, cfg,
		func(ctx context.Context, chunk *table.Chunk, attempt int) (int64, int64, uint64, error) {
			return 100, 99, 1000, nil // stable mismatch
		},
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := c.Run(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, recopyErr, "expected wrapped recopy error, got %v", err)
	// PermanentFailures must NOT be bumped — the recopy attempt is the
	// alternative to permanent failure, not an additional outcome.
	require.Equal(t, uint64(0), c.Stats().PermanentFailures)
}

// TestRecopyNotCalledForHotChunk: a hot chunk (source CRC keeps changing
// across retries) must NOT trigger recopy. Recopy only fires when the
// source CRC is stable across the retry window.
func TestRecopyNotCalledForHotChunk(t *testing.T) {
	chunker := newTestChunker(1)
	recopier := &fakeRecopier{
		recopyFn: func(ctx context.Context, chunk *table.Chunk) error {
			return nil
		},
	}
	cfg := fastConfig()
	cfg.Recopier = recopier

	// Source CRC keeps changing on each read; target lags. Eventually
	// (on attempt 4) the target catches up and the chunk passes via the
	// retry path — never a recopy.
	c := newTestChecker(t, chunker, cfg,
		func(ctx context.Context, chunk *table.Chunk, attempt int) (int64, int64, uint64, error) {
			switch attempt {
			case 1:
				return 100, 99, 1000, nil
			case 2:
				return 200, 99, 1000, nil // src changed, tgt unchanged → hot
			case 3:
				return 300, 200, 1000, nil // src changed, tgt matches *previous* src
				// On the dispatcher side this is: tgt(200) != originalSrc(200)? wait
				// originalSrc was updated to 200 last time. tgt==originalSrc → pass.
			default:
				return 300, 300, 1000, nil
			}
		},
	)
	stop, _ := runUntil(t, c)
	select {
	case <-c.FirstCleanPass():
	case <-time.After(2 * time.Second):
		t.Fatalf("FirstCleanPass did not fire; stats=%+v calls=%d", c.Stats(), recopier.callCount())
	}
	require.Equal(t, 0, recopier.callCount(), "hot chunks must not trigger recopy")
	require.Equal(t, uint64(0), c.Stats().PermanentFailures)
	err := stop()
	require.True(t, errors.Is(err, context.Canceled) || err == nil)
}

// TestRecopyOnRowCountMismatch is the continuous-checker analog of the
// defense-in-depth fix: the source and target CRCs MATCH on every read, but
// the row counts differ and stay stable. Before the fix this passed silently
// (CRC equality alone). Now the count divergence is treated like a checksum
// mismatch: it enqueues a retry, the retry sees stable divergence (source
// signature unchanged, target signature still wrong), and the configured
// Recopier is invoked. After recopy the signatures match and the pass
// completes cleanly.
func TestRecopyOnRowCountMismatch(t *testing.T) {
	chunker := newTestChunker(2)
	var recopied sync.Map // chunk pointer → recopied? (bool)
	recopier := &fakeRecopier{
		recopyFn: func(ctx context.Context, chunk *table.Chunk) error {
			recopied.Store(chunk, true)
			return nil
		},
	}
	cfg := fastConfig()
	cfg.Recopier = recopier

	c := newTestCheckerSig(t, chunker, cfg,
		func(ctx context.Context, chunk *table.Chunk, attempt int) (int64, int64, uint64, uint64, error) {
			if _, ok := recopied.Load(chunk); ok {
				// Post-recopy: CRCs AND counts match.
				return 42, 42, 10, 10, nil
			}
			// Pre-recopy: CRCs are IDENTICAL (the checksum-only check would
			// pass!) but the source has one more row than the target. Stable
			// across the retry window so it becomes a recopy, not a hot chunk.
			return 42, 42, 11, 10, nil
		},
	)

	stop, _ := runUntil(t, c)
	select {
	case <-c.FirstCleanPass():
	case <-time.After(2 * time.Second):
		t.Fatalf("FirstCleanPass did not fire; stats=%+v calls=%d", c.Stats(), recopier.callCount())
	}
	stats := c.Stats()
	require.Equal(t, uint64(0), stats.PermanentFailures)
	require.Equal(t, uint64(2), stats.MismatchesDetected, "both chunks mismatch on row count despite equal CRC")
	require.GreaterOrEqual(t, recopier.callCount(), 2, "both row-count-divergent chunks should have been recopied")

	err := stop()
	require.True(t, errors.Is(err, context.Canceled) || err == nil)
}

// TestPermanentDivergenceOnRowCount: with matching CRCs but a stable row-count
// difference and NO Recopier, the continuous checker must surface
// ErrPermanentDivergence — the count mismatch is a real divergence, not a
// silent pass.
func TestPermanentDivergenceOnRowCount(t *testing.T) {
	chunker := newTestChunker(1)
	c := newTestCheckerSig(t, chunker, fastConfig(),
		func(ctx context.Context, chunk *table.Chunk, attempt int) (int64, int64, uint64, uint64, error) {
			// CRCs always match; source always has one extra row.
			return 100, 100, 6, 5, nil
		},
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := c.Run(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrPermanentDivergence, "expected ErrPermanentDivergence, got %v", err)
	require.Equal(t, uint64(1), c.Stats().PermanentFailures)
}

// TestMultiplePassesResetCounters: after a clean pass, counters reset for
// the next pass (ChunksThisPass, ChunksPassedThisPass) while lifetime
// counters (PassesCompleted) accumulate.
func TestMultiplePassesResetCounters(t *testing.T) {
	chunker := newTestChunker(3)
	c := newTestChecker(t, chunker, fastConfig(),
		func(ctx context.Context, chunk *table.Chunk, attempt int) (int64, int64, uint64, error) {
			return 42, 42, 1000, nil
		},
	)
	stop, _ := runUntil(t, c)

	// Wait for at least 2 passes
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		stats := c.Stats()
		assert.GreaterOrEqualf(collect, stats.PassesCompleted, uint64(2), "did not reach 2 passes in time; stats=%+v", stats)
	}, 3*time.Second, 10*time.Millisecond)
	err := stop()
	require.True(t, errors.Is(err, context.Canceled) || err == nil)

	// Chunker should have been Reset() at least once.
	chunker.mu.Lock()
	resets := chunker.resets
	chunker.mu.Unlock()
	require.GreaterOrEqual(t, resets, 1, "chunker should have been reset between passes")
}
