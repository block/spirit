package change

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/contract"
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
			return &NewBinlogClient(nil, "127.0.0.1:3306", "u", "p", nil, cfg).(*binlogClient).feedCore
		},
		"gtid": func(cfg *ClientConfig) *feedCore {
			return &NewGTIDClient(nil, "127.0.0.1:3306", "u", "p", nil, cfg).(*gtidClient).feedCore
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
	flush := func(_ context.Context, underLock bool, locks []contract.TableLock) error {
		calls = append(calls, fmt.Sprintf("flush(underLock=%v,locks=%d)", underLock, len(locks)))
		return nil
	}
	waitErr := errors.New("reader stalled")
	blockWait := func(err error) func(context.Context) error {
		return func(context.Context) error { calls = append(calls, "blockWait"); return err }
	}
	c := &feedCore{logger: slog.Default(), subs: newSubscriptionRegistry()}
	locks := []contract.TableLock{nil}

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

// foreignTable is a contract.Table that is not a *table.TableInfo, as a
// source outside spirit would supply.
type foreignTable struct{}

func (foreignTable) ID() contract.TableID {
	return contract.TableID{Schema: "test", Name: "foreign"}
}
func (foreignTable) ColumnNames() []string    { return []string{"id"} }
func (foreignTable) KeyColumnNames() []string { return []string{"id"} }

// TestAddSubscriptionTableChecks pins how the built-in sources take a
// contract.Table: they need a *table.TableInfo, so any other implementation
// is refused when the subscription is added, not when its first event
// arrives. A nil newTable — untyped or a nil *table.TableInfo — is a
// move-flow subscription.
func TestAddSubscriptionTableChecks(t *testing.T) {
	newCore := func() *feedCore {
		return &feedCore{logger: slog.Default(), subs: newSubscriptionRegistry(), applier: &applier.MockApplier{}}
	}
	t1 := &table.TableInfo{SchemaName: "test", TableName: "t1", Columns: []string{"id"}, KeyColumns: []string{"id"}}
	chunker := table.NewMockChunker("t1", 0)

	err := newCore().AddSubscription(foreignTable{}, nil, chunker)
	require.ErrorContains(t, err, "change.foreignTable, not a *table.TableInfo")

	err = newCore().AddSubscription(t1, foreignTable{}, chunker)
	require.ErrorContains(t, err, "change.foreignTable, not a *table.TableInfo")

	require.ErrorContains(t, newCore().AddSubscription(nil, nil, chunker), "currentTable is nil")
	var nilTable *table.TableInfo
	require.ErrorContains(t, newCore().AddSubscription(nilTable, nil, chunker), "currentTable is nil")

	c := newCore()
	require.NoError(t, c.AddSubscription(t1, nilTable, chunker))
	sub, ok := c.subs.Get(encodeSchemaTable("test", "t1"))
	require.True(t, ok)
	require.Equal(t, []contract.Table{t1}, sub.Tables(), "a nil *table.TableInfo newTable must not become a Tables() entry")
}
