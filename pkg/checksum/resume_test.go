package checksum

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/require"
)

type pausedWatermarkChunker struct {
	table.Chunker
	watermark     string
	read, release chan struct{}
	once          sync.Once
}

func (c *pausedWatermarkChunker) GetLowWatermark() (string, error) {
	watermark := c.watermark
	c.once.Do(func() { close(c.read); <-c.release })
	return watermark, nil
}

func TestResumeWatermarkCannotCrossRetryReset(t *testing.T) {
	for _, distributed := range []bool{false, true} {
		name := "single"
		if distributed {
			name = "distributed"
		}
		t.Run(name, func(t *testing.T) {
			func() {
				chunker := &pausedWatermarkChunker{watermark: "repaired-prefix", read: make(chan struct{}), release: make(chan struct{})}
				var checker Checker
				var guard *snapshotResume
				var differences *atomic.Uint64
				if distributed {
					c := &DistributedChecker{chunker: chunker}
					checker, guard, differences = c, &c.resume, &c.differencesFound
				} else {
					c := &SingleChecker{chunker: chunker}
					checker, guard, differences = c, &c.resume, &c.differencesFound
				}
				differences.Store(1)
				captured := make(chan string, 1)
				go func() {
					wm, err := checker.ResumeWatermark()
					if err != nil {
						t.Error(err)
					}
					captured <- wm
				}()
				<-chunker.read // Capture holds the previous attempt's watermark.
				reset := make(chan error, 1)
				var resetStarted atomic.Bool
				go func() {
					reset <- guard.restart(differences, func() error {
						resetStarted.Store(true)
						chunker.watermark = "new-verified-prefix"
						return nil
					})
				}()
				locked := guard.mu.TryLock()
				if locked {
					guard.mu.Unlock()
				}
				require.False(t, locked, "capture must hold the reset guard while reading evidence")
				require.False(t, resetStarted.Load(), "retry must not reset between watermark and counter reads")
				require.Equal(t, uint64(1), differences.Load())
				close(chunker.release)
				require.Empty(t, <-captured, "a repairing attempt cannot publish resume evidence")
				require.NoError(t, <-reset)
				wm, err := checker.ResumeWatermark()
				require.NoError(t, err)
				require.Equal(t, "new-verified-prefix", wm, "a fresh attempt can publish its own verified prefix")
			}()
		})
	}
}

func TestResumeFailedResetKeepsEvidenceInvalid(t *testing.T) {
	var guard snapshotResume
	var differences atomic.Uint64
	differences.Store(1)
	resetErr := errors.New("reset failed")
	require.ErrorIs(t, guard.restart(&differences, func() error { return resetErr }), resetErr)
	require.Equal(t, uint64(1), differences.Load())
	chunker := &resumeChunker{testChunker: newTestChunker(0), watermark: "repaired-prefix"}
	wm, err := guard.capture(chunker, &differences)
	require.NoError(t, err)
	require.Empty(t, wm)
}
