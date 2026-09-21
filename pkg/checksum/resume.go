package checksum

import (
	"sync"
	"sync/atomic"

	"github.com/block/spirit/pkg/table"
)

// snapshotResume prevents evidence from one attempt being combined with the
// mismatch counter from another. Repair increments the counter before advancing
// the chunker's watermark; retries reset the chunker before clearing the counter.
// Capture must be atomic with respect to that reset, not just read in that order.
type snapshotResume struct {
	mu         sync.Mutex
	continuous atomic.Bool
	active     atomic.Bool
	// Never reset: cancellation must see mismatches across retries.
	observed atomic.Uint64
}

func (s *snapshotResume) capture(chunker table.Chunker, differences *atomic.Uint64) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.continuous.Load() {
		return "", nil
	}
	wm, err := chunker.GetLowWatermark()
	if differences.Load() != 0 {
		return "", nil
	}
	return wm, err
}

// restart runs after all workers from the previous attempt have joined. An
// unsuccessful reset leaves the mismatch count intact and evidence invalid.
func (s *snapshotResume) restart(differences *atomic.Uint64, reset func() error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := reset(); err != nil {
		return err
	}
	differences.Store(0)
	return nil
}
