package change

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConstructorsConfigureSharedCore pins that both constructors hand their
// config to the shared core the same way: soft-limit defaulting (0 means
// default, negative disables) and every field the subscriptions inherit. The
// constructors do not connect, so no server is needed.
func TestConstructorsConfigureSharedCore(t *testing.T) {
	constructors := map[string]func(*ClientConfig) *feedCore{
		"binlog": func(cfg *ClientConfig) *feedCore {
			return &NewBinlogClient(nil, "127.0.0.1:3306", "u", "p", cfg).(*binlogClient).feedCore
		},
		"gtid": func(cfg *ClientConfig) *feedCore {
			return &NewGTIDClient(nil, "127.0.0.1:3306", "u", "p", cfg).(*gtidClient).feedCore
		},
	}
	for name, newCore := range constructors {
		t.Run(name, func(t *testing.T) {
			underLoadCalled := false
			cfg := NewClientDefaultConfig()
			cfg.DDLFilterSchema = "test"
			cfg.DDLFilterTables = []string{"t1"}
			cfg.UnderLoad = func() bool { underLoadCalled = true; return false }
			c := newCore(cfg)
			assert.Equal(t, int64(DefaultSubscriptionSoftLimitBytes), c.subscriptionSoftLimitBytes)
			assert.Equal(t, DefaultSubscriptionSoftLimitChanges, c.subscriptionSoftLimitChanges)
			assert.Equal(t, "test", c.ddlFilterSchema)
			assert.Equal(t, map[string]struct{}{"t1": {}}, c.ddlFilterTables)
			assert.Equal(t, cfg.ServerID, c.serverID)
			assert.NotNil(t, c.subs)
			assert.Equal(t, 1, cap(c.flushRequests))
			if assert.NotNil(t, c.underLoad) {
				c.underLoad()
				assert.True(t, underLoadCalled)
			}

			cfg = NewClientDefaultConfig()
			cfg.SubscriptionSoftLimitBytes, cfg.SubscriptionSoftLimitChanges = -1, -1
			c = newCore(cfg)
			assert.Zero(t, c.subscriptionSoftLimitBytes, "negative disables the byte cap")
			assert.Zero(t, c.subscriptionSoftLimitChanges, "negative disables the change cap")
		})
	}
}

// TestSharedFlushSequencing pins the order the shared flush helpers drive the
// client hooks in. The under-lock flush must flush, wait for the reader to
// ingest what that flush generated, then flush again; a failed wait must stop
// before the second flush. Flush must wait for the reader before its final
// flush, and give up on a cancelled context.
func TestSharedFlushSequencing(t *testing.T) {
	var calls []string
	flush := func(_ context.Context, underLock bool, locks []*dbconn.TableLock) error {
		calls = append(calls, fmt.Sprintf("flush(underLock=%v,locks=%d)", underLock, len(locks)))
		return nil
	}
	waitErr := errors.New("reader stalled")
	blockWait := func(err error) func(context.Context) error {
		return func(context.Context) error { calls = append(calls, "blockWait"); return err }
	}
	c := &feedCore{logger: slog.Default(), subs: newSubscriptionRegistry()}
	locks := []*dbconn.TableLock{nil}

	require.NoError(t, c.flushUnderTableLock(t.Context(), locks, flush, blockWait(nil)))
	assert.Equal(t, []string{"flush(underLock=true,locks=1)", "blockWait", "flush(underLock=true,locks=1)"}, calls)

	calls = nil
	require.ErrorIs(t, c.flushUnderTableLock(t.Context(), locks, flush, blockWait(waitErr)), waitErr)
	assert.Equal(t, []string{"flush(underLock=true,locks=1)", "blockWait"}, calls)

	calls = nil
	require.NoError(t, c.flushUntilTrivial(t.Context(), flush, blockWait(nil), "test"))
	assert.Equal(t, []string{"flush(underLock=false,locks=0)", "blockWait", "flush(underLock=false,locks=0)"}, calls)

	calls = nil
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, c.flushUntilTrivial(ctx, flush, blockWait(context.Canceled), "test"), context.Canceled)
	assert.Equal(t, []string{"flush(underLock=false,locks=0)", "blockWait"}, calls)
}

// closeCountingSub wraps a subscription and counts Close calls, so a test can
// see whether AddSubscription released a subscription it refused.
type closeCountingSub struct {
	binlogSubscription
	closed int
}

func (s *closeCountingSub) Close() { s.closed++ }

// foreignSub is a Subscription that carries no MySQL table metadata, as an
// out-of-tree subscription would be.
type foreignSub struct {
	stubSubscription
	closed int
}

func (s *foreignSub) Close() { s.closed++ }

// TestAddSubscriptionFactory pins the binlog clients' side of the
// AddSubscription contract: the factory is called with the client's own
// settings, the subscription is registered under its source table, and a
// subscription the client refuses (one without MySQL table metadata, or a
// second one for a table) is closed rather than leaked.
func TestAddSubscriptionFactory(t *testing.T) {
	current := &table.TableInfo{SchemaName: "test", TableName: "addsub", Columns: []string{"id", "v"}, KeyColumns: []string{"id"}}
	newTbl := &table.TableInfo{SchemaName: "test", TableName: "_addsub_new", Columns: []string{"id", "v"}, KeyColumns: []string{"id"}}
	appl := &applier.MockApplier{}
	buffered := BufferedSubscriptionFactory(current, newTbl, table.NewMockChunker("addsub", 1000), appl)

	cfg := NewClientDefaultConfig()
	cfg.SubscriptionSoftLimitBytes = 12345
	cfg.FlushConcurrency = 3
	c := NewBinlogClient(nil, "127.0.0.1:3306", "u", "p", cfg).(*binlogClient)

	var got SubscriptionSettings
	require.NoError(t, c.AddSubscription(func(s SubscriptionSettings) (Subscription, error) {
		got = s
		return buffered(s)
	}))
	assert.Equal(t, int64(12345), got.SoftLimitBytes)
	assert.Equal(t, 3, got.FlushConcurrency)
	assert.NotNil(t, got.FlushRequest)
	assert.Same(t, c.logger, got.Logger)
	sub, ok := c.subs.Get(encodeSchemaTable("test", "addsub"))
	require.True(t, ok, "the subscription must be registered under its source table")
	assert.Equal(t, []TableDescriptor{
		{SchemaName: "test", TableName: "addsub", Columns: []string{"id", "v"}, KeyColumns: []string{"id"}},
		{SchemaName: "test", TableName: "_addsub_new", Columns: []string{"id", "v"}, KeyColumns: []string{"id"}},
	}, sub.Tables())

	t.Run("duplicate table is closed", func(t *testing.T) {
		var dup *closeCountingSub
		err := c.AddSubscription(func(s SubscriptionSettings) (Subscription, error) {
			inner, err := buffered(s)
			if err != nil {
				return nil, err
			}
			dup = &closeCountingSub{binlogSubscription: inner.(binlogSubscription)}
			return dup, nil
		})
		require.ErrorContains(t, err, "subscription already exists for table test.addsub")
		assert.Equal(t, 1, dup.closed)
		still, _ := c.subs.Get(encodeSchemaTable("test", "addsub"))
		assert.Same(t, sub, still, "the first subscription must stay registered")
	})

	t.Run("subscription without MySQL metadata is refused and closed", func(t *testing.T) {
		foreign := &foreignSub{}
		err := c.AddSubscription(func(SubscriptionSettings) (Subscription, error) { return foreign, nil })
		require.ErrorContains(t, err, "needs a subscription that carries its source table's MySQL metadata")
		assert.Equal(t, 1, foreign.closed)
		assert.Len(t, c.subs.Snapshot(), 1)
	})

	t.Run("factory error is returned", func(t *testing.T) {
		boom := errors.New("boom")
		err := c.AddSubscription(func(SubscriptionSettings) (Subscription, error) { return nil, boom })
		require.ErrorIs(t, err, boom)
		assert.Len(t, c.subs.Snapshot(), 1)
	})
}
