package change

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	mysql2 "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// gatedApplier wraps countingApplier so tests can hold a flush mid-batch
// deterministically: when a gate channel is set, every UpsertRows /
// DeleteKeys call first announces itself on entered (when set), then
// receives one token from its gate before proceeding. Waiting on
// entered is how a test knows the flush has swapped the buffer out and
// is now blocked inside an applier round trip. The fail toggles make
// the call error out (after the gate, without recording), exercising
// the reattach paths.
type gatedApplier struct {
	countingApplier
	entered     chan struct{}
	upsertGate  chan struct{}
	deleteGate  chan struct{}
	failUpserts atomic.Bool
	failDeletes atomic.Bool
	// failOneUpsert makes exactly one UpsertRows call fail (whichever
	// concurrent call consumes the flag first), for exercising the
	// partial-failure path of a parallel drain.
	failOneUpsert atomic.Bool
}

var errInjected = errors.New("gatedApplier: injected failure")

func (g *gatedApplier) announce() {
	if g.entered != nil {
		select {
		case g.entered <- struct{}{}:
		default:
		}
	}
}

func (g *gatedApplier) UpsertRows(ctx context.Context, mapping *table.ColumnMapping, rows []applier.LogicalRow, locks []*dbconn.TableLock) (int64, error) {
	g.announce()
	if g.upsertGate != nil {
		<-g.upsertGate
	}
	if g.failUpserts.Load() {
		return 0, errInjected
	}
	if g.failOneUpsert.CompareAndSwap(true, false) {
		return 0, errInjected
	}
	return g.countingApplier.UpsertRows(ctx, mapping, rows, locks)
}

func (g *gatedApplier) DeleteKeys(ctx context.Context, sourceTable, targetTable *table.TableInfo, keys [][]any, locks []*dbconn.TableLock) (int64, error) {
	g.announce()
	if g.deleteGate != nil {
		<-g.deleteGate
	}
	if g.failDeletes.Load() {
		return 0, errInjected
	}
	return g.countingApplier.DeleteKeys(ctx, sourceTable, targetTable, keys, locks)
}

// awaitEntered blocks until the applier reports a gated call in
// progress, i.e. the flush snapshot has been swapped out.
func awaitEntered(t *testing.T, fake *gatedApplier) {
	t.Helper()
	select {
	case <-fake.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("flush did not reach the applier in time")
	}
}

// newGatedBufferedMap builds a map-or-queue-mode bufferedMap wired to a
// gatedApplier, mirroring newByteCapBufferedMap.
func newGatedBufferedMap(fake *gatedApplier, queueMode bool) *bufferedMap {
	sub := newByteCapBufferedMap(&fake.countingApplier, queueMode)
	sub.applier = fake
	return sub
}

// releaseGate closes a gate exactly once, releasing all current and
// future receives. Registered via t.Cleanup so a failed assertion can't
// leave a flush goroutine blocked forever.
func releaseGate(gate chan struct{}) func() {
	var once sync.Once
	return func() { once.Do(func() { close(gate) }) }
}

// recomputeSizeBytes re-derives the byte accounting from the live
// stores, for asserting sizeBytes stays balanced across swap flushes,
// reattaches, and dedup overwrites. Takes s.Lock.
func recomputeSizeBytes(s *bufferedMap) int64 {
	s.Lock()
	defer s.Unlock()
	var n int64
	for k, c := range s.changes {
		n += sizeOfBufferedChange(k, c)
	}
	for _, qc := range s.queue {
		n += sizeOfQueuedChange(qc)
	}
	return n
}

// TestBufferedMapHasChangedDuringFlush pins the core concurrency fix:
// while a Flush is blocked inside an applier round trip, HasChanged must
// keep admitting events instead of blocking on the subscription mutex
// for the entire drain. Before the snapshot-swap flush, the binlog
// reader was stalled for the full flush duration — for a saturated
// applier that meant ~99% reader downtime and a dedup window frozen at
// whatever fit under the soft limit.
func TestBufferedMapHasChangedDuringFlush(t *testing.T) {
	fake := &gatedApplier{upsertGate: make(chan struct{}), entered: make(chan struct{}, 8)}
	release := releaseGate(fake.upsertGate)
	t.Cleanup(release)
	sub := newGatedBufferedMap(fake, false)

	for i := range 10 {
		sub.HasChanged([]any{int32(i)}, []any{int32(i), "seed"}, false)
	}
	require.Equal(t, 10, sub.Length())

	flushDone := make(chan error, 1)
	var allFlushed bool
	go func() {
		var err error
		allFlushed, err = sub.Flush(t.Context(), false, nil)
		flushDone <- err
	}()
	awaitEntered(t, fake)

	// The flush is now parked inside UpsertRows holding NO subscription
	// mutex. Both a brand-new key and a newer image for a key that is
	// mid-drain must be admitted promptly.
	admitted := make(chan struct{})
	go func() {
		sub.HasChanged([]any{int32(100)}, []any{int32(100), "new-during-flush"}, false)
		sub.HasChanged([]any{int32(3)}, []any{int32(3), "newer-image"}, false)
		close(admitted)
	}()
	select {
	case <-admitted:
	case <-time.After(5 * time.Second):
		t.Fatal("HasChanged blocked while a flush was draining — the reader is starved for the whole flush again")
	}

	// The 10 swapped-out entries are still pending (mid-air), plus the
	// two just admitted.
	require.Equal(t, 12, sub.Length(),
		"Length must count both the in-flight snapshot and newly-admitted entries")
	select {
	case err := <-flushDone:
		t.Fatalf("flush finished before the gate was released: %v", err)
	default:
	}

	release()
	require.NoError(t, <-flushDone)
	require.True(t, allFlushed)

	// The snapshot drained; only the two events admitted during the
	// drain remain, with the newer image for key 3 preserved.
	require.Equal(t, 2, sub.Length())
	sub.Lock()
	c, ok := sub.changes[utils.HashKey([]any{int32(3)})]
	sub.Unlock()
	require.True(t, ok, "newer image admitted during the drain must survive the flush")
	require.Equal(t, "newer-image", c.logicalRow.RowImage[1])
	require.Equal(t, recomputeSizeBytes(sub), func() int64 { sub.Lock(); defer sub.Unlock(); return sub.sizeBytes }(),
		"sizeBytes must balance after a swap flush with concurrent admissions")

	// A follow-up flush applies the two live entries: key 3's stale
	// image was applied by flush #1 and its newer image by flush #2 —
	// per-key binlog order.
	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.True(t, allFlushed)
	require.Equal(t, 0, sub.Length())
	require.Equal(t, int64(0), func() int64 { sub.Lock(); defer sub.Unlock(); return sub.sizeBytes }())
}

// TestBufferedMapParkedReaderWakesPerBatch verifies that a parked
// HasChanged caller resumes as soon as enough batches have drained to
// bring sizeBytes under the limit — not only when the entire flush
// completes. With 1100 uniform rows the drain is two batches (1000 +
// 100); the limit is set between the two so the parker must wake while
// batch 2 is still gated.
func TestBufferedMapParkedReaderWakesPerBatch(t *testing.T) {
	fake := &gatedApplier{upsertGate: make(chan struct{})}
	release := releaseGate(fake.upsertGate)
	t.Cleanup(release)
	sub := newGatedBufferedMap(fake, false)

	for i := range 1100 {
		sub.HasChanged([]any{int32(i)}, []any{int32(i), "0123456789012345678901234567890123456789"}, false)
	}
	sub.Lock()
	total := sub.sizeBytes
	sub.softLimitBytes = total / 2 // above the post-batch-1 residue, below the pre-drain total
	sub.Unlock()

	flushDone := make(chan error, 1)
	go func() {
		_, err := sub.Flush(t.Context(), false, nil)
		flushDone <- err
	}()

	// Park a new-key caller while batch 1 is gated.
	parked := make(chan struct{})
	go func() {
		sub.HasChanged([]any{int32(9999)}, []any{int32(9999), "parked"}, false)
		close(parked)
	}()
	require.Eventually(t, func() bool {
		return sub.timesParked.Load() >= 1
	}, 5*time.Second, 5*time.Millisecond, "caller should park: buffer is over the soft limit")

	// Release batch 1 only (1000 of 1100 rows ≈ 91% of the bytes). The
	// per-batch release must wake the parker even though batch 2 is
	// still gated.
	fake.upsertGate <- struct{}{}
	select {
	case <-parked:
	case <-time.After(5 * time.Second):
		t.Fatal("parked HasChanged did not wake after the first batch drained — capacity is only released at end of flush")
	}
	select {
	case err := <-flushDone:
		t.Fatalf("flush finished with only one batch released: %v", err)
	default:
	}

	release() // release batch 2
	require.NoError(t, <-flushDone)
	require.Len(t, fake.upserts(), 2, "1100 rows must drain as two batches (1000 + 100)")
	require.Equal(t, 1, sub.Length(), "only the parker's row should remain buffered")
}

// TestBufferedMapOverwriteBypassesSoftLimit pins the dedup-bypass rule:
// at or over the soft limit, a map-mode overwrite of an already-buffered
// key is ~memory-neutral and must be admitted without parking — hot-row
// churn is exactly the traffic dedup absorbs for free, and parking it
// clamps the effective apply rate to the applier's raw drain rate. A
// genuinely new key must still park.
func TestBufferedMapOverwriteBypassesSoftLimit(t *testing.T) {
	sub := newBareBufferedMap(1024)

	sub.HasChanged([]any{int32(1)}, []any{int32(1), "v1"}, false)
	sub.Lock()
	sub.sizeBytes = 2048 // force over-limit
	sub.Unlock()

	// Overwrite of key 1: must complete without parking.
	overwritten := make(chan struct{})
	go func() {
		sub.HasChanged([]any{int32(1)}, []any{int32(1), "v2"}, false)
		close(overwritten)
	}()
	select {
	case <-overwritten:
	case <-time.After(2 * time.Second):
		t.Fatal("overwrite of an already-buffered key parked on the soft limit")
	}
	require.Equal(t, int64(0), sub.timesParked.Load(),
		"dedup overwrite must not count as a park")
	sub.Lock()
	require.Equal(t, "v2", sub.changes[utils.HashKey([]any{int32(1)})].logicalRow.RowImage[1],
		"the newer image must have replaced the old one")
	require.Len(t, sub.changes, 1)
	sub.Unlock()

	// A new key must still park.
	done := make(chan struct{})
	t.Cleanup(func() {
		sub.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("parked goroutine did not exit after t.Cleanup Close")
		}
	})
	go func() {
		sub.HasChanged([]any{int32(2)}, []any{int32(2), "new"}, false)
		close(done)
	}()
	require.Eventually(t, func() bool {
		return sub.timesParked.Load() >= 1
	}, 2*time.Second, 5*time.Millisecond, "a new key must still park at the limit")
}

// TestBufferedMapParkRequestsFlush verifies that parking performs a
// non-blocking send of the parked subscription on the flush-request
// channel, so the owning client can flush that subscription first
// instead of leaving the reader parked until the next periodic-flush
// tick (or behind another subscription's drain).
func TestBufferedMapParkRequestsFlush(t *testing.T) {
	req := make(chan Subscription, 1)
	sub := newBareBufferedMap(1024)
	sub.flushRequest = req

	sub.Lock()
	sub.sizeBytes = 2048
	sub.Unlock()

	done := make(chan struct{})
	t.Cleanup(func() {
		sub.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("parked goroutine did not exit after t.Cleanup Close")
		}
	})
	go func() {
		sub.HasChanged([]any{int32(1)}, []any{int32(1), "x"}, false)
		close(done)
	}()

	select {
	case parked := <-req:
		require.Same(t, sub, parked, "the flush request must carry the subscription that parked")
	case <-time.After(2 * time.Second):
		t.Fatal("parking did not request a flush")
	}
}

// TestBufferedMapFlushErrorReattachesRemainder exercises the failure
// path of the swap flush: an applier error must merge the unapplied
// snapshot back into the active map — with a newer image admitted
// during the drain winning over the snapshot's stale one — leaving the
// accounting balanced, and a retry must succeed.
func TestBufferedMapFlushErrorReattachesRemainder(t *testing.T) {
	fake := &gatedApplier{upsertGate: make(chan struct{}), entered: make(chan struct{}, 8)}
	release := releaseGate(fake.upsertGate)
	t.Cleanup(release)
	sub := newGatedBufferedMap(fake, false)

	for i := range 5 {
		sub.HasChanged([]any{int32(i)}, []any{int32(i), "seed"}, false)
	}
	fake.failUpserts.Store(true)

	flushDone := make(chan error, 1)
	go func() {
		_, err := sub.Flush(t.Context(), false, nil)
		flushDone <- err
	}()
	awaitEntered(t, fake)

	// While the (single) batch is gated, admit a newer image for key 3.
	sub.HasChanged([]any{int32(3)}, []any{int32(3), "newer-image"}, false)

	release()
	require.ErrorIs(t, <-flushDone, errInjected)

	// All five entries are live again, key 3 holding the newer image,
	// and nothing leaked from the accounting.
	require.Equal(t, 5, sub.Length())
	sub.Lock()
	require.Len(t, sub.changes, 5)
	require.Equal(t, "newer-image", sub.changes[utils.HashKey([]any{int32(3)})].logicalRow.RowImage[1],
		"reattach must not clobber a newer image admitted during the drain")
	require.Equal(t, 0, sub.flushingCount, "no in-flight entries after reattach")
	sub.Unlock()
	require.Equal(t, recomputeSizeBytes(sub), func() int64 { sub.Lock(); defer sub.Unlock(); return sub.sizeBytes }(),
		"sizeBytes must balance after an error reattach with a dedup overwrite")

	// Clear the fault: the retry flushes everything.
	fake.failUpserts.Store(false)
	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.True(t, allFlushed)
	require.Equal(t, 0, sub.Length())
	require.Equal(t, int64(0), func() int64 { sub.Lock(); defer sub.Unlock(); return sub.sizeBytes }())
}

// TestBufferedMapQueueModeFlushOrderAcrossSwapAndError verifies queue
// mode (non-memory-comparable PKs, post-copy) through the swap flush: a
// failed segment is prepended back ahead of events appended during the
// drain, and the retry applies everything in original binlog order.
func TestBufferedMapQueueModeFlushOrderAcrossSwapAndError(t *testing.T) {
	fake := &gatedApplier{upsertGate: make(chan struct{}), entered: make(chan struct{}, 8)}
	release := releaseGate(fake.upsertGate)
	t.Cleanup(release)
	sub := newGatedBufferedMap(fake, true)

	// Segments: [U1, U2], [D3], [U4].
	sub.HasChanged([]any{"k1"}, []any{"k1", "v1"}, false)
	sub.HasChanged([]any{"k2"}, []any{"k2", "v2"}, false)
	sub.HasChanged([]any{"k3"}, nil, true)
	sub.HasChanged([]any{"k4"}, []any{"k4", "v4"}, false)

	// The delete segment will fail; the upsert segment before it succeeds.
	fake.failDeletes.Store(true)

	flushDone := make(chan error, 1)
	go func() {
		_, err := sub.Flush(t.Context(), false, nil)
		flushDone <- err
	}()
	awaitEntered(t, fake)

	// Append while the first segment is gated: must not block (well
	// under the limit) and must stay ordered after the failed remainder.
	appended := make(chan struct{})
	go func() {
		sub.HasChanged([]any{"k5"}, []any{"k5", "v5"}, false)
		close(appended)
	}()
	select {
	case <-appended:
	case <-time.After(5 * time.Second):
		t.Fatal("queue-mode HasChanged blocked while a flush was draining")
	}

	release()
	require.ErrorIs(t, <-flushDone, errInjected)

	// [U1, U2] applied; remainder [D3, U4] prepended ahead of the
	// concurrent append [U5].
	require.Len(t, fake.upserts(), 1)
	sub.Lock()
	var keys []string
	for _, qc := range sub.queue {
		keys = append(keys, qc.originalKey[0].(string))
	}
	sub.Unlock()
	require.Equal(t, []string{"k3", "k4", "k5"}, keys,
		"failed remainder must be prepended ahead of events appended during the drain")
	require.Equal(t, recomputeSizeBytes(sub), func() int64 { sub.Lock(); defer sub.Unlock(); return sub.sizeBytes }())

	// Retry drains FIFO: DeleteKeys([k3]) then UpsertRows([k4, k5]).
	fake.failDeletes.Store(false)
	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.True(t, allFlushed)
	require.Equal(t, 0, sub.Length())

	deletes := fake.deletes()
	require.Len(t, deletes, 1)
	require.Equal(t, "k3", deletes[0][0][0])
	upserts := fake.upserts()
	require.Len(t, upserts, 2)
	require.Equal(t, "k4", upserts[1][0].RowImage[0])
	require.Equal(t, "k5", upserts[1][1].RowImage[0])
}

// TestBufferedMapFlushEmpty pins the trivial path: flushing an empty
// subscription makes no applier calls and reports all-flushed.
func TestBufferedMapFlushEmpty(t *testing.T) {
	fake := &gatedApplier{}
	sub := newGatedBufferedMap(fake, false)
	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.True(t, allFlushed)
	require.Empty(t, fake.upserts())
	require.Empty(t, fake.deletes())
}

// TestPeriodicFlushRespondsToParkRequest is the end-to-end signal test:
// with the periodic flush interval cranked to an hour, a binlog-driven
// park must still be flushed within seconds via the flush-request
// channel. Before the signal existed, a parked reader sat idle for the
// remainder of the interval on every fill cycle.
func TestPeriodicFlushRespondsToParkRequest(t *testing.T) {
	db, client, srcTable, dstTable := setupBufferedTest(t)
	defer client.Close()
	defer utils.CloseAndLog(db)

	sub := getBufferedMap(t, client, srcTable.SchemaName+"."+srcTable.TableName)

	// Only the park signal can plausibly trigger a flush at this interval.
	client.StartPeriodicFlush(t.Context(), time.Hour)
	defer client.StopPeriodicFlush()

	requestBinlogPark(t, sub, srcTable)
	// The flusher may already have released it, so observe the park counter.
	require.Eventually(t, func() bool {
		return sub.timesParked.Load() >= 1
	}, parkWaitTimeout, 10*time.Millisecond, "binlog-driven HasChanged should park on soft limit")

	// The park requested a flush; the periodic goroutine must drain the
	// seed row promptly — not in an hour — which also unparks the reader.
	var count int
	require.Eventually(t, func() bool {
		require.NoError(t, db.QueryRowContext(t.Context(),
			fmt.Sprintf("SELECT COUNT(*) FROM %s", dstTable.QuotedTableName)).Scan(&count))
		return count >= 1
	}, 15*time.Second, 50*time.Millisecond,
		"park-requested flush did not run; reader would stay parked until the next interval tick")
}

// orderRecordingApplier wraps a real applier and records the source
// table name of every UpsertRows call, in call order. Used to assert
// which subscription a multi-subscription client flushed first.
type orderRecordingApplier struct {
	applier.Applier
	mu    sync.Mutex
	order []string
}

func (a *orderRecordingApplier) UpsertRows(ctx context.Context, mapping *table.ColumnMapping, rows []applier.LogicalRow, locks []*dbconn.TableLock) (int64, error) {
	a.mu.Lock()
	a.order = append(a.order, mapping.SourceTable().TableName)
	a.mu.Unlock()
	return a.Applier.UpsertRows(ctx, mapping, rows, locks)
}

func (a *orderRecordingApplier) recorded() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.order...)
}

// TestPeriodicFlushPrioritizesParkedSubscription covers the
// multi-subscription client: the park signal carries the subscription
// that parked, and runPeriodicFlush must drain it before the
// all-subscription pass. Without the priority, the pass visits the
// registry in nondeterministic map order, and the parked reader can sit
// behind another saturated subscription's entire drain — table B here
// has pending changes when table A parks, so an unprioritized flush
// would drain B first about half the time.
func TestPeriodicFlushPrioritizesParkedSubscription(t *testing.T) {
	makePair := func(suffix string) (*table.TableInfo, *table.TableInfo) {
		srcBase, _ := uniqueTableNames(t)
		srcName := srcBase + suffix
		dstName := fmt.Sprintf("_%s_new", srcName)
		testutils.RunSQL(t, fmt.Sprintf("DROP TABLE IF EXISTS `%s`, `%s`", srcName, dstName))
		testutils.RunSQL(t, fmt.Sprintf("CREATE TABLE `%s` (id INT NOT NULL, name VARCHAR(255) NOT NULL, PRIMARY KEY (id))", srcName))
		testutils.RunSQL(t, fmt.Sprintf("CREATE TABLE `%s` (id INT NOT NULL, name VARCHAR(255) NOT NULL, PRIMARY KEY (id))", dstName))
		db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
		require.NoError(t, err)
		defer utils.CloseAndLog(db)
		src := table.NewTableInfo(db, "test", srcName)
		dst := table.NewTableInfo(db, "test", dstName)
		require.NoError(t, src.SetInfo(t.Context()))
		require.NoError(t, dst.SetInfo(t.Context()))
		return src, dst
	}
	srcA, dstA := makePair("_a")
	srcB, dstB := makePair("_b")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	realApplier, err := applier.NewSingleTargetApplier(applier.Target{DB: db, KeyRange: "0", Config: cfg}, applier.NewApplierDefaultConfig())
	require.NoError(t, err)
	rec := &orderRecordingApplier{Applier: realApplier}
	client := NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, rec, NewClientDefaultConfig()).(*binlogClient)
	for _, pair := range []struct{ src, dst *table.TableInfo }{{srcA, dstA}, {srcB, dstB}} {
		chunker, err := table.NewChunker(pair.src, table.ChunkerConfig{NewTable: pair.dst})
		require.NoError(t, err)
		require.NoError(t, client.AddSubscription(pair.src, pair.dst, chunker))
	}
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()
	subA := getBufferedMap(t, client, srcA.SchemaName+"."+srcA.TableName)

	// Only the park signal can plausibly trigger a flush at this interval.
	client.StartPeriodicFlush(t.Context(), time.Hour)
	defer client.StopPeriodicFlush()

	// B accumulates pending changes first, so it is a candidate to be
	// drained ahead of A in an unprioritized all-subscription pass.
	testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'b'), (2, 'b'), (3, 'b')", srcB.QuotedTableName))
	requestBinlogPark(t, subA, srcA)
	// The flusher may already have released it, so observe the park counter.
	require.Eventually(t, func() bool {
		return subA.timesParked.Load() >= 1
	}, parkWaitTimeout, 10*time.Millisecond, "binlog-driven HasChanged should park on soft limit")

	// The priority flush drains A; the follow-up pass drains B. Gate on B
	// specifically rather than on a bare count: the all-subscription pass
	// walks the registry in map order, so it can drain A a second time
	// (the 'parked' insert buffers once the park releases) before it ever
	// reaches B, and a count of two would then snapshot [A, A].
	require.Eventually(t, func() bool {
		return slices.Contains(rec.recorded(), srcB.TableName)
	}, 15*time.Second, 50*time.Millisecond, "the all-subscription pass must still drain the other subscription")
	order := rec.recorded()
	require.Equal(t, srcA.TableName, order[0], "the parked subscription must be flushed before the all-subscription pass")
}

// totalUpsertedRows sums the rows across all recorded UpsertRows calls.
func totalUpsertedRows(fake *gatedApplier) int {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	total := 0
	for _, call := range fake.upsertCalls {
		total += len(call)
	}
	return total
}

// TestBufferedMapParallelFlushAppliesConcurrently pins the parallel
// map-mode drain: with flushConcurrency=3 and three batches, all three
// applier calls must be in flight at the same time. Under a serial
// drain the second `entered` announcement can never arrive while the
// first call is still gated, so this test times out if the drain
// regresses to serial.
func TestBufferedMapParallelFlushAppliesConcurrently(t *testing.T) {
	fake := &gatedApplier{upsertGate: make(chan struct{}), entered: make(chan struct{}, 8)}
	release := releaseGate(fake.upsertGate)
	t.Cleanup(release)
	sub := newGatedBufferedMap(fake, false)
	sub.flushConcurrency = 3

	const totalRows = 2*DefaultBatchSize + DefaultBatchSize/2 // 3 batches by construction
	for i := range totalRows {
		sub.HasChanged([]any{int32(i)}, []any{int32(i), "seed"}, false)
	}
	require.Equal(t, totalRows, sub.Length())

	flushDone := make(chan error, 1)
	var allFlushed bool
	go func() {
		var err error
		allFlushed, err = sub.Flush(t.Context(), false, nil)
		flushDone <- err
	}()

	// Three concurrent applier calls announce before any is released.
	for range 3 {
		awaitEntered(t, fake)
	}
	// All entries are still in flight: nothing has been released yet.
	require.Equal(t, totalRows, sub.Length(), "in-flight snapshot entries must still be counted")

	release()
	require.NoError(t, <-flushDone)
	require.True(t, allFlushed)
	require.Zero(t, sub.Length())
	require.Equal(t, totalRows, totalUpsertedRows(fake))
	require.Zero(t, recomputeSizeBytes(sub))
	sub.Lock()
	require.Zero(t, sub.sizeBytes, "accounting must balance after a parallel drain")
	sub.Unlock()
}

// TestBufferedMapParallelFlushErrorReattachesRemainder exercises the
// partial-failure path of the parallel drain: one batch fails, sibling
// batches either land (and are released) or are cancelled (and stay in
// the snapshot), and everything unapplied is merged back with balanced
// accounting. A retry drains the remainder; nothing is lost or applied
// from a stale snapshot twice.
func TestBufferedMapParallelFlushErrorReattachesRemainder(t *testing.T) {
	fake := &gatedApplier{}
	sub := newGatedBufferedMap(fake, false)
	sub.flushConcurrency = 2

	const totalRows = 2*DefaultBatchSize + DefaultBatchSize/2 // 3 batches by construction
	for i := range totalRows {
		sub.HasChanged([]any{int32(i)}, []any{int32(i), "seed"}, false)
	}
	fake.failOneUpsert.Store(true)

	_, err := sub.Flush(t.Context(), false, nil)
	require.ErrorIs(t, err, errInjected)
	require.False(t, fake.failOneUpsert.Load(), "the injected failure must have been consumed")

	// The failed batch (and any cancelled ones) must be back in the
	// active map with balanced accounting; applied batches must not be.
	applied := totalUpsertedRows(fake)
	require.Less(t, applied, totalRows, "the failed batch cannot have been recorded as applied")
	require.Equal(t, totalRows-applied, sub.Length(), "unapplied entries must be reattached")
	sub.Lock()
	flushing := sub.flushingCount
	sub.Unlock()
	require.Zero(t, flushing, "no snapshot entries may remain in flight after Flush returns")
	require.Equal(t, recomputeSizeBytes(sub), func() int64 { sub.Lock(); defer sub.Unlock(); return sub.sizeBytes }(),
		"sizeBytes must match the live stores after reattach")

	// A retry drains the remainder exactly once.
	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.True(t, allFlushed)
	require.Zero(t, sub.Length())
	require.Equal(t, totalRows, totalUpsertedRows(fake), "every row must land exactly once across both flushes")
	require.Zero(t, recomputeSizeBytes(sub))
}

// TestBufferedMapFlushCanceledContextIsAnError pins the outcome of a
// Flush whose context is already canceled: the drain schedules nothing,
// applies nothing, and must report the context error — not success.
// Without the skipped-batches check the errgroup joins cleanly over
// zero scheduled batches and the flush reports allChangesFlushed=true,
// which the clients would record as a successfully flushed position
// while every entry had merely been reattached to the buffer.
func TestBufferedMapFlushCanceledContextIsAnError(t *testing.T) {
	fake := &gatedApplier{}
	sub := newGatedBufferedMap(fake, false)
	sub.flushConcurrency = 2

	const totalRows = 2*DefaultBatchSize + DefaultBatchSize/2 // 3 batches by construction
	for i := range totalRows {
		sub.HasChanged([]any{int32(i)}, []any{int32(i), "seed"}, false)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	allFlushed, err := sub.Flush(ctx, false, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, allFlushed)
	require.Zero(t, totalUpsertedRows(fake), "no batch may reach the applier on a canceled context")

	// Everything is reattached with balanced accounting...
	require.Equal(t, totalRows, sub.Length(), "all entries must be reattached")
	sub.Lock()
	flushing := sub.flushingCount
	sub.Unlock()
	require.Zero(t, flushing, "no snapshot entries may remain in flight after Flush returns")
	require.Equal(t, recomputeSizeBytes(sub), func() int64 { sub.Lock(); defer sub.Unlock(); return sub.sizeBytes }(),
		"sizeBytes must match the live stores after reattach")

	// ...and a live retry drains it all.
	allFlushed, err = sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.True(t, allFlushed)
	require.Zero(t, sub.Length())
	require.Equal(t, totalRows, totalUpsertedRows(fake), "every row must land exactly once on the retry")
}

// TestBufferedMapFlushMidDrainCancelReportsError pins the "or while
// batches are being scheduled" half of the cancellation contract: when
// ctx is canceled after some batches were scheduled (and land
// successfully — the fake ignores ctx, like a statement already on the
// wire), the remainder is skipped and g.Wait() returns nil, yet Flush
// must report the context error. If the skippedBatches bookkeeping is
// reordered away, a mid-drain cancel returns (true, nil) and the
// client publishes the pre-flush position over reattached-only
// entries.
func TestBufferedMapFlushMidDrainCancelReportsError(t *testing.T) {
	fake := &gatedApplier{upsertGate: make(chan struct{}), entered: make(chan struct{}, 8)}
	release := releaseGate(fake.upsertGate)
	t.Cleanup(release)
	sub := newGatedBufferedMap(fake, false)
	sub.flushConcurrency = 1 // serialize scheduling so the cancel lands between batches

	const totalRows = 2*DefaultBatchSize + DefaultBatchSize/2 // 3 batches by construction
	for i := range totalRows {
		sub.HasChanged([]any{int32(i)}, []any{int32(i), "seed"}, false)
	}

	ctx, cancel := context.WithCancel(t.Context())
	flushDone := make(chan error, 1)
	var allFlushed bool
	go func() {
		var err error
		allFlushed, err = sub.Flush(ctx, false, nil)
		flushDone <- err
	}()

	// The first batch is inside the applier: cancel now, then release
	// the gate so the in-flight batch completes successfully.
	awaitEntered(t, fake)
	cancel()
	release()

	require.ErrorIs(t, <-flushDone, context.Canceled)
	require.False(t, allFlushed)

	// At least the gated batch landed; at least the tail batch was
	// skipped — with SetLimit(1) the scheduler cannot reach the tail's
	// gctx check until the in-flight batch (and therefore the cancel,
	// which precedes the gate release) has completed.
	applied := totalUpsertedRows(fake)
	require.GreaterOrEqual(t, applied, DefaultBatchSize, "the in-flight batch must have landed")
	require.Less(t, applied, totalRows, "the tail batch must have been skipped")
	require.Equal(t, totalRows-applied, sub.Length(), "skipped entries must be reattached")
	sub.Lock()
	flushing := sub.flushingCount
	sub.Unlock()
	require.Zero(t, flushing, "no snapshot entries may remain in flight after Flush returns")
	require.Equal(t, recomputeSizeBytes(sub), func() int64 { sub.Lock(); defer sub.Unlock(); return sub.sizeBytes }(),
		"sizeBytes must match the live stores after reattach")

	// A live retry drains the remainder exactly once.
	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.True(t, allFlushed)
	require.Zero(t, sub.Length())
	require.Equal(t, totalRows, totalUpsertedRows(fake), "every row must land exactly once across both flushes")
}

// TestFlushConcurrencyClientPlumbing pins the ClientConfig plumbing
// end-to-end for both clients: the zero value must resolve to
// DefaultFlushConcurrency (a refactor that drops the default would
// silently revert production to the serial drain this knob exists to
// escape), negative must resolve to the explicit serial opt-out, and
// the resolved value must reach the bufferedMap that AddSubscription
// builds.
func TestFlushConcurrencyClientPlumbing(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  int
		want int
	}{
		{"zero-defaults-parallel", 0, DefaultFlushConcurrency},
		{"negative-serial", -4, 1},
		{"explicit-value", 3, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			current := &table.TableInfo{SchemaName: "test", TableName: "plumbing"}

			cfg := NewClientDefaultConfig()
			cfg.FlushConcurrency = tc.cfg
			bc := NewBinlogClient(nil, "localhost", "user", "pass", &countingApplier{}, cfg).(*binlogClient)
			require.Equal(t, tc.want, bc.flushConcurrency)
			require.NoError(t, bc.AddSubscription(current, nil, table.NewMockChunker("plumbing", 1000)))
			bsubs := bc.subs.Snapshot()
			require.Len(t, bsubs, 1)
			require.Equal(t, tc.want, bsubs[0].(*bufferedMap).flushConcurrency)

			gcfg := NewClientDefaultConfig()
			gcfg.FlushConcurrency = tc.cfg
			gc := NewGTIDClient(nil, "localhost", "user", "pass", &countingApplier{}, gcfg).(*gtidClient)
			require.Equal(t, tc.want, gc.flushConcurrency)
			require.NoError(t, gc.AddSubscription(current, nil, table.NewMockChunker("plumbing", 1000)))
			gsubs := gc.subs.Snapshot()
			require.Len(t, gsubs, 1)
			require.Equal(t, tc.want, gsubs[0].(*bufferedMap).flushConcurrency)
		})
	}
}

// TestBufferedSubscriptionZeroFlushConcurrencyStaysSerial pins the
// compat promise on BufferedSubscriptionConfig: out-of-tree callers
// (e.g. vstream sources) that do not set FlushConcurrency keep the
// serial drain until they opt in.
func TestBufferedSubscriptionZeroFlushConcurrencyStaysSerial(t *testing.T) {
	sub, err := NewBufferedSubscription(BufferedSubscriptionConfig{
		CurrentTable: &table.TableInfo{SchemaName: "test", TableName: "compat"},
		Applier:      &countingApplier{},
		Chunker:      table.NewMockChunker("compat", 1000),
	})
	require.NoError(t, err)
	require.Equal(t, 1, sub.(*bufferedMap).effectiveFlushConcurrency())
}
