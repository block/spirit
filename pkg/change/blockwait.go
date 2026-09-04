package change

// blockWaitStalls decides when the file-position reader needs a rotation to
// nudge it forward. The first observation establishes a baseline; progress
// resets the consecutive-stall count. Keep this independent of wall time so
// tests can exercise the policy without assuming how CI schedules the reader.
type blockWaitStalls struct {
	observed    bool
	consecutive int
}

func (s *blockWaitStalls) observe(advanced bool) bool {
	if !s.observed || advanced {
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
