package migration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/autoscale"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/throttler"
	"github.com/stretchr/testify/require"
)

// copyThreadSampleQuery counts the threads one migration is running in Query
// state, which is what the redo-aware signal counts: PROCESSLIST_COMMAND =
// 'Query', excluding the sampler's own connection. It adds a database filter so
// the sample is that migration's own work rather than the rest of the suite's,
// and it leaves out the redo-log subtraction, which matches nothing off Aurora.
const copyThreadSampleQuery = `SELECT COUNT(*)
	FROM performance_schema.threads
	WHERE PROCESSLIST_ID IS NOT NULL
	  AND PROCESSLIST_ID <> CONNECTION_ID()
	  AND PROCESSLIST_DB = ?
	  AND PROCESSLIST_COMMAND = 'Query'`

const (
	// copyThreadSampleInterval is far shorter than the 5s the real throttler
	// polls on, so a copy measured in seconds is observed about as many times
	// as a real one is over minutes.
	copyThreadSampleInterval = 50 * time.Millisecond
	// copyThreadBackoff and copyThreadBackoffPolls mirror the real BlockWait:
	// re-check every second, for up to a minute, while the signal stays over.
	copyThreadBackoff      = time.Second
	copyThreadBackoffPolls = 60
)

// copyThreadThrottler stands in for the Aurora threads hard-stop on a target
// that cannot produce one. It throttles while the copy's own running threads
// exceed the threshold an instance of a given vCPU count sets, and parks a
// throttled caller the way the real one does.
//
// The instance is what is simulated, not the mechanism. A test server is not
// Aurora, so there is no @@innodb_buffer_pool_instances to read a vCPU count
// from and no redo-log wait event to subtract. The threshold is production's
// own function of that count (throttler.MinThreadsThrottleThreshold), the count
// is a live sample of real worker threads, and the backoff loop is shaped like
// production's.
type copyThreadThrottler struct {
	db        *sql.DB
	database  string
	threshold int

	throttled atomic.Bool
	// entries counts transitions into the throttled state — the repeats an
	// operator counts in the logs, since the real throttler warns on the
	// transition rather than on every sample.
	entries atomic.Int64
	// peak is the highest sample seen, so a test can say how far over the
	// threshold the copy's own pool reached.
	peak atomic.Int64
	// blockedNanos is the total time callers spent parked in BlockWait: the
	// copy time the hard-stop took away.
	blockedNanos atomic.Int64
	// samples counts the observations behind peak, so a run can say how
	// thoroughly the copy was watched.
	samples atomic.Int64

	stop context.CancelFunc
	done chan struct{}
}

func (c *copyThreadThrottler) Open(ctx context.Context) error {
	if err := c.UpdateLag(ctx); err != nil {
		return err
	}
	// Outlives the request context the way the real poller does, and is stopped
	// by Close.
	loopCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	c.stop = cancel
	c.done = make(chan struct{})
	go func() {
		defer close(c.done)
		ticker := time.NewTicker(copyThreadSampleInterval)
		defer ticker.Stop()
		for {
			select {
			case <-loopCtx.Done():
				return
			case <-ticker.C:
				// A sample that fails as the server or the pool goes away is
				// teardown, not a signal: hold the last reading, as the real
				// poller does.
				_ = c.UpdateLag(loopCtx)
			}
		}
	}()
	return nil
}

func (c *copyThreadThrottler) Close() error {
	if c.stop != nil {
		c.stop()
		<-c.done
	}
	return nil
}

func (c *copyThreadThrottler) UpdateLag(ctx context.Context) error {
	var count int64
	c.samples.Add(1)
	if err := c.db.QueryRowContext(ctx, copyThreadSampleQuery, c.database).Scan(&count); err != nil {
		return fmt.Errorf("sampling running threads for %s: %w", c.database, err)
	}
	for peak := c.peak.Load(); count > peak; peak = c.peak.Load() {
		if c.peak.CompareAndSwap(peak, count) {
			break
		}
	}
	over := count > int64(c.threshold)
	switch {
	case over && !c.throttled.Swap(true):
		c.entries.Add(1)
	case !over:
		c.throttled.Store(false)
	}
	return nil
}

func (c *copyThreadThrottler) IsThrottled() bool { return c.throttled.Load() }

func (c *copyThreadThrottler) BlockWait(ctx context.Context) {
	timer := time.NewTimer(copyThreadBackoff)
	defer timer.Stop()
	if !c.throttled.Load() {
		return // not parked at all, so it costs the copy nothing
	}
	parked := time.Now()
	defer func() { c.blockedNanos.Add(int64(time.Since(parked))) }()

	for range copyThreadBackoffPolls {
		if !c.throttled.Load() {
			return
		}
		timer.Reset(copyThreadBackoff)
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
	}
}

// TestSmallInstanceCopyDoesNotTripItsOwnThrottle builds an index on a table of
// a hundred thousand or so narrow rows — work a native index build finishes in
// well under a second — behind the threads hard-stop an instance of the
// smallest size spirit meets would set.
//
// Both runs copy the same rows under the same gate, and the thread counts are
// the only difference between them. Pools sized from the instance total three
// against a threshold of three, so the copy cannot carry a sample over on its
// own: it runs to the end, and the gate trips at most once, when a spirit
// connection outside the copy pool is caught mid-query. Such a trip clears at
// the next sample, because the copy's own threads are under the threshold
// whatever the other connection is doing. Pools left at the thread flags'
// defaults total eight, and that copy throttles on its own workers over and
// over — once per chunk, in the limit — spending seconds parked per run with
// no production load anywhere.
//
// Neither run's wall-clock time is the assertion. A host with cores to spare
// copies a chunk faster than the throttler samples and refills the pool as soon
// as a backoff ends, so eight throttled threads still finish — sooner, here,
// than three unthrottled ones. On an instance the size the threshold is derived
// from that trade reverses: the copy's own appliers stay busy through the
// backoff, the sample never falls back under the threshold, and the copy
// advances one chunk per BlockWait. Whether the gate is tripped is what a test
// on any host can pin; what tripping it costs belongs to the target.
func TestSmallInstanceCopyDoesNotTripItsOwnThrottle(t *testing.T) {
	const (
		smallestInstanceVCPUs = 2
		rows                  = 131072
		// Chunks large enough that a worker is still executing one when the
		// next sample lands, which is what puts the copy's own threads in the
		// count, and few enough that the whole copy is a couple of seconds of
		// work on a busy runner rather than hundreds of round trips through a
		// single write thread.
		chunkBytes = 256 << 10
	)
	threshold := throttler.MinThreadsThrottleThreshold(smallestInstanceVCPUs)
	pools, _ := sizePoolsFromInstance(smallestInstanceVCPUs, autoscale.ClientCeiling())

	for _, tc := range []struct {
		name        string
		read, write int
		fits        bool
		// deadline is what the case needs to show what it is for, not a
		// uniform budget. The sized case has to run to completion, so it gets
		// room for a loaded runner to be slow. The unsized case does not: what
		// it demonstrates is the gate tripping, which happens in its first
		// seconds, and letting it run on would only spend the backoff it is
		// there to show.
		deadline time.Duration
	}{
		{"pools sized from the instance", pools.read, pools.write, true, 30 * time.Second},
		{"pools left at the thread flag defaults", defaultThreads, defaultWriteThreads, false, 15 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dbName, db := testutils.CreateUniqueTestDatabase(t)
			testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE copy_gate (
				id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
				pad VARBINARY(64) NOT NULL
			)`)
			testutils.RunSQLInDatabase(t, dbName, `INSERT INTO copy_gate (pad) SELECT RANDOM_BYTES(64) FROM dual`)
			for seeded := 1; seeded < rows; seeded *= 2 {
				testutils.RunSQLInDatabase(t, dbName, `INSERT INTO copy_gate (pad) SELECT RANDOM_BYTES(64) FROM copy_gate`)
			}

			hardStop := &copyThreadThrottler{db: db, database: dbName, threshold: threshold}
			m := NewTestRunner(t, "copy_gate", "ADD INDEX idx_pad (pad)",
				WithDBName(dbName),
				WithThreads(tc.read), WithWriteThreads(tc.write),
				WithTargetChunkSize(chunkBytes),
				WithThrottler(hardStop))
			t.Cleanup(func() { require.NoError(t, m.Close()) })

			ctx, cancel := context.WithTimeout(t.Context(), tc.deadline)
			defer cancel()
			started := time.Now()
			err := m.Run(ctx)
			blocked := time.Duration(hardStop.blockedNanos.Load())
			t.Logf("%d read + %d write threads against a hard-stop that trips above %d: elapsed=%s blocked=%s (summed across workers) peak_sample=%d throttle_entries=%d samples=%d err=%v",
				tc.read, tc.write, threshold, time.Since(started).Round(time.Millisecond),
				blocked.Round(time.Millisecond), hardStop.peak.Load(), hardStop.entries.Load(),
				hardStop.samples.Load(), err)

			if tc.fits {
				require.NoError(t, err, "a copy that fits under the hard-stop has no reason to miss its deadline")
				// The copy's own three threads cannot reach the threshold, so a
				// sample only goes over when a connection outside the copy pool
				// is caught mid-query, and only until the next sample. What must
				// not happen is the copy throttling on itself, chunk after chunk.
				require.LessOrEqual(t, hardStop.entries.Load(), int64(1),
					"a copy that fits must not throttle on itself, whatever else is briefly running")
				require.Less(t, blocked, 5*copyThreadBackoff,
					"a copy that fits must not spend its time parked")
				return
			}
			// Either it paid the backoff and finished, or it ran out of clock
			// doing so. Both are the gate firing on spirit's own copy.
			require.True(t, err == nil || errors.Is(err, context.DeadlineExceeded),
				"unexpected failure from a throttled copy: %v", err)
			require.Greater(t, hardStop.peak.Load(), int64(threshold),
				"the copy's own threads must be what carried the sample over")
			require.NotZero(t, hardStop.entries.Load(), "the copy must trip the gate its own threads exceed")
			// Parked time, not the number of trips, is what says the gate is
			// firing on the copy rather than grazing it. The two measure the
			// same throttling, but a slower target pays for it with longer
			// parks instead of more of them: its appliers are still busy when
			// a backoff ends, so BlockWait loops inside one entry rather than
			// taking a new one. Time spent parked only grows as the target
			// gets slower; the count of entries can fall to one.
			require.GreaterOrEqual(t, blocked, 2*copyThreadBackoff,
				"tripping the gate must cost the copy more than a single backoff")
		})
	}
}
