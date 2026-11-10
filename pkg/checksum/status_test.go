package checksum

import (
	"testing"
	"time"

	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/require"
)

func TestLocklessProgressSummary(t *testing.T) {
	stats := LocklessCheckerStats{CurrentPass: 1, ProgressBasisPoints: 1780, ChunksPassedThisPass: 40, InFlight: 8}
	require.Contains(t, (ChecksumStatus{Optimistic: &stats}).String(), "scanning scan≈17.8% passed=40")
	stats.ProgressBasisPoints = 10000
	stats.ScanComplete = true
	stats.InFlight = 0
	stats.RetryQueueDepth = 1
	stats.ChunksPassedThisPass = 263
	summary := (ChecksumStatus{Optimistic: &stats}).String()
	require.Contains(t, summary, "waiting for verification")
	require.Contains(t, summary, "scan≈100.0% passed=263 retrying=1")
	require.NotContains(t, summary, "verified")
	stats.RetryQueueDepth = 0
	stats.HotChunksDeferredThisPass = 1
	require.Contains(t, (ChecksumStatus{Optimistic: &stats}).String(), "deferred=1")
	// The pair has to be readable together: deferred falling on its own does
	// not say whether those ranges were verified or merely stopped being hot.
	stats.HotChunksDeferredThisPass, stats.HotChunksSettledThisPass = 0, 3
	require.Contains(t, (ChecksumStatus{Optimistic: &stats}).String(), "deferred=0 settled=3")
	stats.HotChunksSettledThisPass = 0
	stats.FirstCleanPassAt = time.Now()
	require.Contains(t, (ChecksumStatus{Optimistic: &stats}).String(), "checksum: verified")
}

// The status line names the phase, not the algorithm: an operator polling a
// move reads "checksum", and the line carries the pass ETA.
func TestLocklessStatusReportsETA(t *testing.T) {
	stats := LocklessCheckerStats{ProgressBasisPoints: 2500}
	ready := ChecksumStatus{Optimistic: &stats, ETA: status.ETA{State: status.ETAReady, Duration: 90 * time.Second}}
	require.Equal(t, "checksum: scanning scan≈25.0% eta=1m30s passed=0 retrying=0 in-flight=0 deferred=0 settled=0", ready.String())
	require.NotContains(t, ready.String(), "lockless")

	due := ChecksumStatus{Optimistic: &stats, ETA: status.ETA{State: status.ETADue}}
	require.Contains(t, due.String(), "eta=DUE")
	measuring := ChecksumStatus{Optimistic: &stats, ETA: status.ETA{State: status.ETAMeasuring}}
	require.Contains(t, measuring.String(), "eta=TBD")
	require.NotContains(t, (ChecksumStatus{Optimistic: &stats}).String(), "eta=", "a zero-value ETA is omitted")
}

func TestLocklessStatusPhase(t *testing.T) {
	for _, scanComplete := range []bool{false, true} {
		for _, clean := range []bool{false, true} {
			stats := LocklessCheckerStats{ScanComplete: scanComplete}
			phase := "scanning"
			if scanComplete {
				phase = "waiting for verification"
			}
			if clean {
				stats.FirstCleanPassAt = time.Now()
				phase = "verified"
			}
			require.Contains(t, (ChecksumStatus{Optimistic: &stats}).String(), "checksum: "+phase+" scan")
		}
	}
}

// TestLocklessChecksumStatusETA pins the checker's ETA: TBD for the first
// minute of a run, then the rest of the walk at the sampled rate, DUE at the
// end of the walk.
func TestLocklessChecksumStatusETA(t *testing.T) {
	chunker := table.NewMockChunker("t1", 10000)
	c := &LocklessChecker{chunker: chunker}
	c.started = time.Now()
	chunker.SimulateProgress(0.25)
	c.rate.Observe(1000) // 100 rows/s over one 10s sample
	require.Equal(t, status.ETAMeasuring, c.ChecksumStatus().ETA.State, "the first minute of a run is still measuring")

	c.started = time.Now().Add(-2 * time.Minute)
	st := c.ChecksumStatus()
	require.Equal(t, status.ETA{State: status.ETAReady, Duration: 75 * time.Second}, st.ETA, "7500 rows left at 100 rows/s")
	require.Equal(t, status.ChecksumProgress{RowsChecked: 2500, RowsTotal: 10000}, st.Progress)
	require.Contains(t, st.String(), "checksum: scanning scan≈25.0% eta=1m15s ")

	chunker.SimulateProgress(1)
	require.Equal(t, status.ETADue, c.ChecksumStatus().ETA.State)
}
