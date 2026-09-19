package checksum

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/throttler"
)

// MockChecker is a shared test double for runners. Configure Chunker and RunError
// before use; SetDifferencesFound may be called concurrently with runner work.
// It models snapshot resume eligibility without performing verification.
type MockChecker struct {
	Chunker          table.Chunker
	RunError         error
	differencesFound atomic.Uint64
	continuous       atomic.Bool
}

var _ Checker = (*MockChecker)(nil)

func (*MockChecker) ContinuousActive() bool { return false }

func (m *MockChecker) RunContinuous(ctx context.Context) error {
	m.continuous.Store(true)
	if m.RunError != nil {
		return m.RunError
	}
	<-ctx.Done()
	return nil
}

func (m *MockChecker) Run(context.Context) error          { return m.RunError }
func (*MockChecker) SetThrottler(throttler.Throttler)     {}
func (*MockChecker) GetProgress() status.ChecksumProgress { return status.ChecksumProgress{} }
func (*MockChecker) StartTime() time.Time                 { return time.Time{} }
func (*MockChecker) ExecTime() time.Duration              { return 0 }
func (m *MockChecker) DifferencesFound() uint64           { return m.differencesFound.Load() }
func (m *MockChecker) SetDifferencesFound(n uint64)       { m.differencesFound.Store(n) }

func (m *MockChecker) ResumeWatermark() (string, error) {
	if m.continuous.Load() || m.Chunker == nil {
		return "", nil
	}
	wm, err := m.Chunker.GetLowWatermark()
	if m.DifferencesFound() != 0 {
		return "", nil
	}
	return wm, err
}
