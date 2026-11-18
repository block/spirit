package change

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/contract"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReconnectRetryBudget drives both readers' consecutive-error path
// without a server: every streamer a reconnect installs delivers a scripted
// list of events and then fails. It pins when the reconnect budget refills.
//
//   - A reconnect that delivers nothing, or only the events every dump opens
//     with, does not refill it, so the reader reaches FatalReasonStreamError
//     after maxRecreateAttempts-1 reconnects (the attempt that reaches the
//     limit goes fatal before reconnecting).
//   - A reconnect that delivers an event from the binlog's body refills it,
//     so the failures after it get the full budget again.
func TestReconnectRetryBudget(t *testing.T) {
	// Each reconnect waits out the 1s initial backoff, so a smaller budget
	// keeps the test short. Package-level state: these subtests must not run
	// in parallel with anything else that reads it.
	orig := maxRecreateAttempts
	maxRecreateAttempts = 3
	t.Cleanup(func() { maxRecreateAttempts = orig })

	// The events a dump opens with: the artificial rotate (LogPos 0) naming
	// the file, then the file's format description and previous GTIDs.
	preamble := []*replication.BinlogEvent{
		{
			Header: &replication.EventHeader{EventType: replication.ROTATE_EVENT, Flags: replication.LOG_EVENT_ARTIFICIAL_F},
			Event:  &replication.RotateEvent{Position: 4, NextLogName: []byte("binlog.000001")},
		},
		{
			Header: &replication.EventHeader{EventType: replication.FORMAT_DESCRIPTION_EVENT, LogPos: 126},
			Event:  &replication.FormatDescriptionEvent{},
		},
		{
			Header: &replication.EventHeader{EventType: replication.PREVIOUS_GTIDS_EVENT, LogPos: 157},
			Event:  &replication.PreviousGTIDsEvent{},
		},
	}
	// An event from the binlog's body: a transaction's commit.
	commit := &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.XID_EVENT, LogPos: 500},
		Event:  &replication.XIDEvent{},
	}

	tests := []struct {
		name string
		// script returns what the n-th reconnect's streamer (n from 1)
		// delivers before it fails. The initial streamer delivers nothing.
		script func(n int) []*replication.BinlogEvent
		want   int // reconnects before the fatal error
	}{
		{
			name:   "no events",
			script: func(int) []*replication.BinlogEvent { return nil },
			want:   maxRecreateAttempts - 1,
		},
		{
			name:   "dump preamble only",
			script: func(int) []*replication.BinlogEvent { return preamble },
			want:   maxRecreateAttempts - 1,
		},
		{
			// Reconnect 1 delivers nothing. Reconnect 2 delivers a commit,
			// which refills the budget, so maxRecreateAttempts-1 more
			// reconnects follow before the fatal error.
			name: "commit refills the budget",
			script: func(n int) []*replication.BinlogEvent {
				if n == 2 {
					return append(append([]*replication.BinlogEvent{}, preamble...), commit)
				}
				return nil
			},
			want: 2 + maxRecreateAttempts - 1,
		},
	}

	readers := []struct {
		name string
		new  func(t *testing.T) (interface{ readStream(context.Context) }, *feedCore)
	}{
		{"binlog", func(*testing.T) (interface{ readStream(context.Context) }, *feedCore) {
			c := &binlogClient{}
			return c, &c.feedCore
		}},
		{"gtid", func(t *testing.T) (interface{ readStream(context.Context) }, *feedCore) {
			empty, err := mysql.ParseMysqlGTIDSet("")
			require.NoError(t, err)
			c := &gtidClient{bufferedGTID: empty, flushedGTID: empty.Clone()}
			return c, &c.feedCore
		}},
	}

	for _, r := range readers {
		for _, tc := range tests {
			t.Run(r.name+"/"+tc.name, func(t *testing.T) {
				reader, core := r.new(t)
				ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
				defer cancel()

				var feeders sync.WaitGroup
				defer feeders.Wait()
				defer cancel()

				// scripted returns a streamer that delivers evs and then
				// fails. GetEvent picks at random between a queued event and
				// a queued error, so each event is queued only once the
				// reader has taken the previous one (it stamps lastReceived
				// for every event), and the error only after the last.
				scripted := func(evs []*replication.BinlogEvent) *replication.BinlogStreamer {
					s := replication.NewBinlogStreamer()
					feeders.Go(func() {
						for _, ev := range evs {
							before := core.lastReceived.Load()
							if err := s.AddEventToStreamer(ev); err != nil {
								return
							}
							for core.lastReceived.Load() == before {
								select {
								case <-ctx.Done():
									return
								case <-time.After(time.Millisecond):
								}
							}
						}
						s.AddErrorToStreamer(errors.New("simulated read error"))
					})
					return s
				}

				fatal := make(chan contract.FatalReason, 1)
				core.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
				core.callerCancelFunc = func(reason contract.FatalReason) bool {
					fatal <- reason
					return true
				}
				core.streamer = scripted(nil)
				reconnects := 0 // read and written only on the reader goroutine
				core.recreate = func() error {
					reconnects++
					core.streamer = scripted(tc.script(reconnects))
					return nil
				}

				done := make(chan struct{})
				go func() {
					defer close(done)
					reader.readStream(ctx)
				}()
				select {
				case <-done:
				case <-ctx.Done():
					<-done
					t.Fatal("reader did not exhaust its reconnect budget")
				}

				select {
				case reason := <-fatal:
					assert.Equal(t, contract.FatalReasonStreamError, reason)
				default:
					t.Fatal("expected FatalReasonStreamError")
				}
				assert.Equal(t, tc.want, reconnects)
			})
		}
	}
}
