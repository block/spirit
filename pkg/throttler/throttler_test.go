package throttler

import (
	"database/sql"
	"log/slog"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
	os.Exit(m.Run())
}

func TestThrottlerInterface(t *testing.T) {
	replicaDSN := os.Getenv("REPLICA_DSN")
	if replicaDSN == "" {
		t.Skip("skipping test because REPLICA_DSN not set")
	}
	db, err := sql.Open("mysql", replicaDSN)
	assert.NoError(t, err)
	defer db.Close()

	//	NewReplicationThrottler will attach either MySQL 8.0 or MySQL 5.7 throttler
	loopInterval = 1 * time.Millisecond
	throttler, err := NewReplicationThrottler(db, 60*time.Second, slog.Default())
	assert.NoError(t, err)
	assert.NoError(t, throttler.Open(t.Context()))

	time.Sleep(50 * time.Millisecond)        // make sure the throttler loop can calculate.
	throttler.BlockWait()                    // wait for catch up (there's no activity)
	assert.False(t, throttler.IsThrottled()) // there's a race, but its unlikely to be throttled

	assert.NoError(t, throttler.Close())

	time.Sleep(50 * time.Millisecond) // give it time to shutdown.
}

func TestNoopThrottler(t *testing.T) {
	throttler := &Noop{}
	assert.NoError(t, throttler.Open(t.Context()))
	throttler.currentLag = 1 * time.Second
	throttler.lagTolerance = 2 * time.Second
	assert.False(t, throttler.IsThrottled())
	assert.NoError(t, throttler.UpdateLag())
	throttler.BlockWait()
	throttler.lagTolerance = 100 * time.Millisecond
	assert.True(t, throttler.IsThrottled())
	assert.NoError(t, throttler.Close())
}
