package checksum

import (
	"testing"
	"time"

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
	stats.HotChunksDeferredThisPass = 0
	stats.FirstCleanPassAt = time.Now()
	require.Contains(t, (ChecksumStatus{Optimistic: &stats}).String(), "lockless: verified")
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
			require.Contains(t, (ChecksumStatus{Optimistic: &stats}).String(), "lockless: "+phase+" scan")
		}
	}
}
