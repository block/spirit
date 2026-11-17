package change

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/contract"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

func TestReconnectRetryBudgetSurvivesSuccessfulReconnect(t *testing.T) {
	if maxRecreateAttempts < 2 {
		t.Fatal("test requires at least two reconnect attempts")
	}

	brokenStreamer := func() *replication.BinlogStreamer {
		s := replication.NewBinlogStreamer()
		if !s.AddErrorToStreamer(errors.New("simulated permanent read error")) {
			t.Fatal("could not inject stream error")
		}
		return s
	}

	run := func(t *testing.T, reader interface {
		readStream(context.Context)
	}, core *feedCore) {
		t.Helper()

		fatal := make(chan contract.FatalReason, 1)
		attempts := 0

		core.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
		core.streamer = brokenStreamer()
		core.callerCancelFunc = func(reason contract.FatalReason) bool {
			fatal <- reason
			return true
		}

		core.recreateStreamerForTest = func() error {
			attempts++
			core.streamer = brokenStreamer()
			return nil
		}

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		done := make(chan struct{})
		go func() {
			defer close(done)
			reader.readStream(ctx)
		}()

		select {
		case <-done:
		case <-ctx.Done():
			<-done
			t.Fatal("reader did not exhaust retry budget")
		}

		select {
		case reason := <-fatal:
			if reason != contract.FatalReasonStreamError {
				t.Fatalf("unexpected fatal reason: %v", reason)
			}
		default:
			t.Fatal("expected FatalReasonStreamError")
		}

		if attempts != maxRecreateAttempts-1 {
			t.Fatalf("got %d reconnects; want %d",
				attempts, maxRecreateAttempts-1)
		}
	}

	t.Run("binlog", func(t *testing.T) {
		c := &binlogClient{}
		run(t, c, &c.feedCore)
	})

	t.Run("gtid", func(t *testing.T) {
		empty, err := mysql.ParseMysqlGTIDSet("")
		if err != nil {
			t.Fatal(err)
		}

		c := &gtidClient{
			bufferedGTID: empty,
			flushedGTID:  empty.Clone(),
		}
		run(t, c, &c.feedCore)
	})
}
