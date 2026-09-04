package change

import "github.com/go-mysql-org/go-mysql/mysql"

// blockWaitStalls decides when the file-position reader needs a rotation to
// nudge it forward. The first observation establishes a baseline; progress
// resets the consecutive-stall count. getCurrentBinlogPosition rotates the log
// before sampling, so a brief initial stall while the reader opens it is normal.
// Keep this independent of wall time so tests can exercise the policy without
// assuming how CI schedules the reader.
type blockWaitStalls struct {
	observed    bool
	consecutive int
}

func (s *blockWaitStalls) observe(prev, curr mysql.Position) bool {
	if !s.observed || curr.Compare(prev) > 0 {
		s.observed = true
		s.consecutive = 0
		return false
	}
	s.consecutive++
	if s.consecutive < blockWaitStallThreshold {
		return false
	}
	s.consecutive = 0
	return true
}
