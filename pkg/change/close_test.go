package change

import (
	"context"
	"testing"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/stretchr/testify/require"
)

// Hold the flush after it observes cancellation so the test distinguishes
// cancelling the worker from actually joining it.
type closingSubscription struct {
	stubSubscription
	entered   chan struct{}
	cancelled chan struct{}
	release   chan struct{}
}

func (s *closingSubscription) Flush(ctx context.Context, _ bool, _ []*dbconn.TableLock) (bool, error) {
	close(s.entered)
	<-ctx.Done()
	close(s.cancelled)
	<-s.release
	return false, ctx.Err()
}

func TestCloseJoinsPeriodicFlush(t *testing.T) {
	for _, name := range []string{"binlog", "gtid"} {
		t.Run(name, func(t *testing.T) {
			var client Source
			var requests chan Subscription
			var flushDone func() chan struct{}
			if name == "binlog" {
				c := NewBinlogClient(nil, "", "", "", nil, NewClientDefaultConfig()).(*binlogClient)
				client, requests = c, c.flushRequests
				flushDone = func() chan struct{} { return c.periodicFlushDone }
			} else {
				c := NewGTIDClient(nil, "", "", "", nil, NewClientDefaultConfig()).(*gtidClient)
				client, requests = c, c.flushRequests
				flushDone = func() chan struct{} { return c.periodicFlushDone }
			}
			sub := &closingSubscription{
				entered: make(chan struct{}), cancelled: make(chan struct{}), release: make(chan struct{}),
			}
			ctx, cancel := context.WithCancel(t.Context())
			closed := make(chan struct{})
			t.Cleanup(func() {
				cancel()
				close(sub.release)
				client.StopPeriodicFlush()
				select {
				case <-closed:
				case <-time.After(5 * time.Second):
					t.Error("Close did not finish during cleanup")
				}
			})
			client.StartPeriodicFlush(ctx, time.Hour)
			done := flushDone()
			requests <- sub
			<-sub.entered
			go func() {
				client.Close()
				close(closed)
			}()
			select {
			case <-sub.cancelled:
			case <-time.After(5 * time.Second):
				t.Fatal("Close did not cancel the in-flight flush")
			}
			select {
			case <-closed:
				t.Fatal("Close returned before the in-flight flush finished")
			default:
			}
			// Unblock without closing: cleanup owns the channel close.
			sub.release <- struct{}{}
			select {
			case <-closed:
			case <-time.After(5 * time.Second):
				t.Fatal("Close did not join the completed flush")
			}
			select {
			case <-done:
			default:
				t.Fatal("periodic flush is still running after Close")
			}
			client.Close() // idempotent
			client.StartPeriodicFlush(ctx, time.Hour)
			require.Nil(t, flushDone(), "a closed client must not restart flushing")
		})
	}
}
