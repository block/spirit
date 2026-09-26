package change

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mysql2 "github.com/block/mysql"
	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// TestParkGate pins the gate's three obligations: an unparked gate costs a
// reader nothing, a parked one holds it, and a context ending releases it.
func TestParkGate(t *testing.T) {
	var g parkGate
	require.NoError(t, g.wait(t.Context()), "an unarmed gate must not block")
	g.unpark() // releasing a gate nobody armed is a no-op, not a panic
	require.NoError(t, g.wait(t.Context()))

	g.park()
	g.park() // arming twice must not replace the channel out from under a waiter
	released := make(chan error, 1)
	go func() { released <- g.wait(t.Context()) }()
	select {
	case <-released:
		t.Fatal("wait returned while the gate was armed")
	case <-time.After(50 * time.Millisecond):
	}
	g.unpark()
	require.NoError(t, <-released)

	// A gate re-armed between the release and the waiter waking must hold it
	// again rather than let one event through. wait re-checks for that reason.
	g.park()
	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	require.ErrorIs(t, g.wait(ctx), context.Canceled)
}

// TestParkedRowObserve: the parked row is what decides an event is *the* event,
// and it has to keep counting after it fires. A multi-row event goes on dispatching
// after the gate is armed, so a second change to the same key can still land —
// and the flush that follows would carry it, making the captured image stale.
func TestParkedRowObserve(t *testing.T) {
	matches := func(key []any) bool { return key[0] == int64(1) }
	var g parkGate
	w := newParkedRow(RowWatch{Schema: "test", Table: "t1", Match: matches}, &g)

	require.False(t, w.observe("test", "t2", []any{int64(1)}, []any{int64(1)}, false), "wrong table")
	require.False(t, w.observe("other", "t1", []any{int64(1)}, []any{int64(1)}, false), "wrong schema")
	require.False(t, w.observe("test", "t1", []any{int64(2)}, []any{int64(2)}, false), "wrong key")
	select {
	case <-w.ch:
		t.Fatal("a non-matching change must not fire the watch")
	default:
	}

	require.True(t, w.observe("test", "t1", []any{int64(1)}, []any{int64(1), "first"}, false))
	select {
	case <-w.ch:
		t.Fatal("observe must not wake the verification: the change is not buffered yet")
	default:
	}
	w.Release()
	w.Release() // the caller cannot know it is the first; twice is safe
	require.True(t, gateIsParked(&g), "the firing change must hold the reader")
	(*ParkedRow)(nil).Release() // nil is the common case, and must be a no-op
	<-w.ch
	key, image, deleted, rewritten := w.result()
	require.Equal(t, []any{int64(1)}, key)
	require.Equal(t, []any{int64(1), "first"}, image)
	require.False(t, deleted)
	require.False(t, rewritten)

	// A further change to the same key keeps the first image but marks it.
	require.True(t, w.observe("test", "t1", []any{int64(1)}, []any{int64(1), "second"}, false))
	_, image, _, rewritten = w.result()
	require.Equal(t, "first", image[1], "the captured image must not be replaced")
	require.True(t, rewritten, "the caller must be told the image it holds is stale")

	// A watch with no matcher matches nothing rather than everything.
	require.False(t, newParkedRow(RowWatch{Schema: "test", Table: "t1"}, &g).
		observe("test", "t1", []any{int64(1)}, nil, false))
}

// verifyHarness drives RowParker.Verify without a server: it stands in for the
// reader goroutine, so the test controls exactly when the watched change is
// dispatched and can see whether the gate held afterwards.
type verifyHarness struct {
	parker RowParker
	// armed closes once the watch is live. Dispatching before that would
	// deliver the change with nobody listening, which is the ordering bug the
	// arm-before-unpark step exists to prevent — so the harness must not
	// reproduce it by accident.
	armed chan struct{}
	once  sync.Once
	// blockBuffer, if set, runs where a subscription's HasChanged blocks on its
	// soft limit: after the watch has seen the change, before it is buffered and
	// the reader parked. A dispatch can sit there for as long as the
	// backpressure lasts, which is long enough for the verification waiting on
	// it to give up.
	blockBuffer func()
	buffered    [][]any
	flushes     atomic.Int64
	flushFn     func(context.Context) error
}

func newVerifyHarness() *verifyHarness {
	return &verifyHarness{armed: make(chan struct{})}
}

func (h *verifyHarness) run(ctx context.Context, watch RowWatch, verify RowVerifier) error {
	// Arming is internal to Verify, so the harness watches for it rather than
	// hooking it. Bounded, because a Verify that returns before the watcher
	// observes the slot would otherwise leave this goroutine spinning for the
	// rest of the run; closing armed unarmed just makes the test fail on its
	// own assertion instead.
	go func() {
		deadline := time.Now().Add(10 * time.Second)
		for h.parker.slot.Load() == nil && time.Now().Before(deadline) {
			runtime.Gosched()
		}
		h.once.Do(func() { close(h.armed) })
	}()
	return h.parker.Verify(ctx, watch, verify,
		func(ctx context.Context) error {
			h.flushes.Add(1)
			if h.flushFn != nil {
				return h.flushFn(ctx)
			}
			return nil
		},
		func() bool { return true })
}

// dispatch is what a client's dispatchRow does, in the same order.
func (h *verifyHarness) dispatch(key, image []any, deleted bool) {
	watched := h.parker.Watch(&table.TableInfo{SchemaName: "test", TableName: "t1"}, key, image, deleted)
	if h.blockBuffer != nil {
		h.blockBuffer()
	}
	h.buffered = append(h.buffered, key)
	watched.Release()
}

// readerParked reports whether a reader arriving at the gate right now would be
// held, which is the property "nothing past the watched event is admitted"
// reduces to.
func (h *verifyHarness) readerParked() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	return h.parker.Wait(ctx) != nil
}

func gateIsParked(g *parkGate) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.parked != nil
}

var anyRow = RowWatch{Schema: "test", Table: "t1", Match: func([]any) bool { return true }}

// TestVerifyRowAtNextChange pins the ordering that is the whole contract: the
// watch is armed before the reader is released, the reader is parked when the
// change fires, the flush happens only after that, and the verifier sees the
// image with the gate still shut.
func TestVerifyRowAtNextChange(t *testing.T) {
	h := newVerifyHarness()
	dispatched := make(chan struct{})
	go func() {
		<-h.armed
		h.dispatch([]any{int64(1)}, []any{int64(1), "image"}, false)
		close(dispatched)
	}()

	var sawImage []any
	err := h.run(t.Context(), anyRow, func(_ context.Context, key, image []any, deleted bool) error {
		<-dispatched
		require.Equal(t, int64(1), key[0].(int64))
		require.False(t, deleted)
		require.Equal(t, int64(1), h.flushes.Load(), "the flush must run before the verifier, exactly once")
		require.True(t, h.readerParked(), "the reader must still be held while the target is read")
		sawImage = image
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []any{int64(1), "image"}, sawImage)
	require.False(t, h.readerParked(), "the reader must be released once the verification is done")
	require.Nil(t, h.parker.slot.Load(), "the watch must be disarmed")
}

// TestVerifyRowAtNextChangeFailures: every way this can fail has to leave the
// reader running. A verification that parked the stream and then returned an
// error without unparking would stop replication for the rest of the run.
func TestVerifyRowAtNextChangeFailures(t *testing.T) {
	boom := errors.New("boom")
	for name, tc := range map[string]struct {
		dispatch []func(h *verifyHarness)
		flushErr error
		verifyFn RowVerifier
		wantErr  error
	}{
		"no change arrives": {
			wantErr: context.DeadlineExceeded,
		},
		"rewritten before the target is read": {
			// One event, two rows for the same key: the gate arms on the
			// first and the second still dispatches behind it.
			dispatch: []func(h *verifyHarness){
				func(h *verifyHarness) { h.dispatch([]any{int64(1)}, []any{int64(1), "first"}, false) },
				func(h *verifyHarness) { h.dispatch([]any{int64(1)}, []any{int64(1), "second"}, false) },
			},
			wantErr: ErrRowRewritten,
		},
		"flush fails": {
			dispatch: []func(h *verifyHarness){
				func(h *verifyHarness) { h.dispatch([]any{int64(1)}, []any{int64(1)}, false) },
			},
			flushErr: boom,
			wantErr:  boom,
		},
		"verifier fails": {
			dispatch: []func(h *verifyHarness){
				func(h *verifyHarness) { h.dispatch([]any{int64(1)}, []any{int64(1)}, false) },
			},
			verifyFn: func(context.Context, []any, []any, bool) error { return boom },
			wantErr:  boom,
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := newVerifyHarness()
			if tc.flushErr != nil {
				h.flushFn = func(context.Context) error { return tc.flushErr }
			}
			var wg sync.WaitGroup
			wg.Go(func() {
				<-h.armed
				for _, d := range tc.dispatch {
					d(h)
				}
			})

			ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
			defer cancel()
			verify := tc.verifyFn
			if verify == nil {
				verify = func(context.Context, []any, []any, bool) error { return nil }
			}
			require.ErrorIs(t, h.run(ctx, anyRow, verify), tc.wantErr)
			wg.Wait()
			require.False(t, h.readerParked(), "the reader must be released on every failure path")
			require.Nil(t, h.parker.slot.Load())
		})
	}
}

// TestParkedRowAbandon pins the rule that makes the park safe to arm from
// inside a dispatch: only a live verification's waiter may hold the reader.
//
// A dispatch and the verification that armed it run on different goroutines,
// and the verification can give up — its budget ends, or it already returned —
// while a dispatch is still on its way to parking. Parking for a verification
// that is gone stops replication for the rest of the run, because the only
// thing that would have unparked it is the verification itself.
func TestParkedRowAbandon(t *testing.T) {
	var g parkGate
	w := newParkedRow(anyRow, &g)

	// A dispatch that lands while the verification is live parks the reader.
	require.True(t, w.observe("test", "t1", []any{int64(1)}, nil, false))
	w.Release()
	require.True(t, gateIsParked(&g))
	<-w.ch

	// Abandoning releases whatever that dispatch parked: the verification is
	// the only thing that can, and it is leaving.
	w.abandon()
	require.False(t, gateIsParked(&g))

	// And no dispatch still in flight on the same row can park it again.
	w.Release()
	require.False(t, gateIsParked(&g), "an abandoned row must not park the reader")
}

// TestVerifyRowAtNextChangeAbandonsInFlightDispatch is the same rule end to
// end, in the window that actually produces it: a dispatch blocked in
// HasChanged on the subscription's soft limit while the verification's budget
// runs out. The change is buffered and the waiter parked long after the
// verification has returned.
func TestVerifyRowAtNextChangeAbandonsInFlightDispatch(t *testing.T) {
	h := newVerifyHarness()
	gaveUp := make(chan struct{})
	h.blockBuffer = func() { <-gaveUp }

	var wg sync.WaitGroup
	wg.Go(func() {
		<-h.armed
		h.dispatch([]any{int64(1)}, []any{int64(1)}, false)
	})

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	err := h.run(ctx, anyRow, func(context.Context, []any, []any, bool) error {
		t.Error("the verifier must not run: the change never reached the buffer in time")
		return nil
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// Only now does the dispatch finish, with nothing left to park for.
	close(gaveUp)
	wg.Wait()
	require.False(t, h.readerParked(),
		"a dispatch that finishes after the verification gave up must not park the reader")
	require.Nil(t, h.parker.slot.Load())
}

// TestFlushParked: the parked drain answers "did everything land", because that
// is what makes the comparison honest. A buffer that will not empty may still
// be holding the watched change, and the caller must retry rather than read the
// target and call the difference a divergence.
func TestFlushParked(t *testing.T) {
	var drains int
	drain := func(context.Context) error { drains++; return nil }

	require.NoError(t, flushParked(t.Context(), drain, func() bool { return true }))
	require.Equal(t, 1, drains, "a buffer that empties needs exactly one drain")

	// A change the reader buffered between a drain's swap and the park needs a
	// second pass, which must not be mistaken for a stuck buffer.
	drains = 0
	require.NoError(t, flushParked(t.Context(), drain, func() bool { return drains > 1 }))
	require.Equal(t, 2, drains)

	drains = 0
	require.ErrorIs(t, flushParked(t.Context(), drain, func() bool { return false }), ErrFlushIncomplete)
	require.Equal(t, 3, drains, "giving up must be bounded, not a spin")

	boom := errors.New("boom")
	require.ErrorIs(t, flushParked(t.Context(), func(context.Context) error { return boom },
		func() bool { return true }), boom)
}

// TestDispatchRowOrdering pins the order both clients dispatch in, because each
// step being elsewhere is a different wrong answer.
//
// The subtle one is the rewrite: a verification reads the rewrite count *after*
// its flush, so a second change to the watched row must be counted before it
// can be buffered. Counting it afterwards leaves a window where the flush
// carries the newer image to the target while the count still reads zero — and
// the verification then compares the target against an image it has already
// moved past and calls it a divergence.
func TestDispatchRowOrdering(t *testing.T) {
	tbl := &table.TableInfo{SchemaName: "test", TableName: "t1"}
	for name, newClient := range map[string]func() (*RowParker, func(Subscription)){
		"binlog": func() (*RowParker, func(Subscription)) {
			c := &binlogClient{subs: newSubscriptionRegistry()}
			return &c.parker, func(sub Subscription) {
				c.dispatchRow(sub, tbl, []any{int64(1)}, []any{int64(1)}, false)
			}
		},
		"gtid": func() (*RowParker, func(Subscription)) {
			c := &gtidClient{subs: newSubscriptionRegistry()}
			return &c.parker, func(sub Subscription) {
				c.dispatchRow(sub, tbl, []any{int64(1)}, []any{int64(1)}, false)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			parker, dispatch := newClient()
			watch := newParkedRow(anyRow, &parker.gate)
			parker.slot.Store(watch)

			// The firing change: it must be buffered before anything wakes the
			// verification, or the flush would run without it.
			first := &recordingSubscription{onChange: func() {
				select {
				case <-watch.ch:
					t.Error("the verification was released before the change was buffered")
				default:
				}
			}}
			dispatch(first)
			require.Equal(t, 1, first.changes, "the change must reach the subscription")
			require.True(t, gateIsParked(&parker.gate), "the matching change must park the reader")
			<-watch.ch

			// A rewrite: counted before it is buffered, so a flush cannot carry
			// it while the count still reads zero.
			second := &recordingSubscription{onChange: func() {
				_, _, _, rewritten := watch.result()
				require.True(t, rewritten, "a rewrite must be counted before it can be buffered")
			}}
			dispatch(second)
			require.Equal(t, 1, second.changes)
		})
	}
}

type recordingSubscription struct {
	Subscription
	changes  int
	onChange func()
}

func (s *recordingSubscription) HasChanged([]any, []any, bool) {
	s.changes++
	if s.onChange != nil {
		s.onChange()
	}
}

// TestVerifyRowAtNextChangeLive runs the whole mechanism against a real binlog
// stream and a row that is being written continuously — the case it exists for,
// and the only way to see that the pieces fit together.
//
// Two things here are not visible to any fake. The flush a parked verification
// runs cannot be the exported Flush, because that ends in BlockWait, which
// waits for the reader to reach the source's current position — and the reader
// is parked, by us. And the target must hold the delivered image and nothing
// past it, which is only meaningful while writes are still arriving.
func TestVerifyRowAtNextChangeLive(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS parkt1, parkt2")
	testutils.RunSQL(t, "CREATE TABLE parkt1 (a INT NOT NULL, b INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE parkt2 (a INT NOT NULL, b INT, PRIMARY KEY (a))")

	t1 := table.NewTableInfo(db, "test", "parkt1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "parkt2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	client := NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd,
		applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*binlogClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()

	testutils.RunSQL(t, "INSERT INTO parkt1 VALUES (1, 0)")

	// The row is written continuously, which is what defeats every
	// read-and-compare strategy and what makes the next event arrive quickly.
	writerCtx, stopWriter := context.WithCancel(t.Context())
	var writer sync.WaitGroup
	writer.Go(func() {
		for writerCtx.Err() == nil {
			_, _ = db.ExecContext(writerCtx, "UPDATE parkt1 SET b = b + 1 WHERE a = 1")
			time.Sleep(time.Millisecond)
		}
	})
	defer func() {
		stopWriter()
		writer.Wait()
	}()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	var verified bool
	err = client.VerifyRowAtNextChange(ctx, RowWatch{
		Schema: "test", Table: "parkt1",
		Match: func(key []any) bool { return fmt.Sprint(key[0]) == "1" },
	}, func(ctx context.Context, _, image []any, deleted bool) error {
		verified = true
		require.False(t, deleted)
		want := fmt.Sprint(image[1])

		// The delivered image is the source's value at that position, and the
		// flush has applied everything up to it. So the target holds exactly
		// this — and, because the reader is parked, goes on holding it even
		// though the writer is still committing.
		for range 3 {
			var got string
			require.NoError(t, db.QueryRowContext(ctx, "SELECT b FROM parkt2 WHERE a = 1").Scan(&got))
			require.Equal(t, want, got, "the target must hold the delivered image and nothing past it")
			time.Sleep(50 * time.Millisecond)
		}
		return nil
	})
	require.NoError(t, err)
	require.True(t, verified, "the verifier must have run")

	// And the stream keeps moving afterwards: a verification releases the
	// reader whatever happened.
	require.False(t, gateIsParked(&client.parker.gate), "a verification must release the reader")
}

// TestVerifyRowAtNextChangeRewrittenDuringVerify pins the second read of the
// rewrite count, after the verifier returns.
//
// The gate stops the *next* event, not the rest of the current one, so a
// multi-row event — an ODKU listing the key twice, a PK-shifting UPDATE — goes
// on dispatching while the verifier reads the target. A periodic flush is not
// serialized with the verification, so it can carry that second change through
// while the read is in flight. The verifier's verdict is then about a target
// that moved, which is not a verdict at all.
func TestVerifyRowAtNextChangeRewrittenDuringVerify(t *testing.T) {
	h := newVerifyHarness()
	go func() {
		<-h.armed
		h.dispatch([]any{int64(1)}, []any{int64(1), "first"}, false)
	}()

	verified := 0
	err := h.run(t.Context(), anyRow, func(_ context.Context, _, image []any, _ bool) error {
		verified++
		require.Equal(t, "first", image[1], "the verifier is handed the image the feed parked at")
		// The rest of the event, arriving while the target is being read.
		h.dispatch([]any{int64(1)}, []any{int64(1), "second"}, false)
		return nil
	})
	require.ErrorIs(t, err, ErrRowRewritten, "a verdict reached against a target that moved must be discarded")
	require.Equal(t, 1, verified, "the verifier ran; it is its verdict that is thrown away")
	require.False(t, h.readerParked(), "the reader must be released even so")
}
