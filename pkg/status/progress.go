package status

// Progress is returned as a struct because we may add more to it later.
// It is designed for wrappers (like a GUI) to be able to summarize the
// current status without parsing log output.
type Progress struct {
	CurrentState State  // current state, i.e. CopyRows
	Summary      string // text based representation, i.e. "12.5% copyRows ETA 1h 30m"
}
