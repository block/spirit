package status

import (
	"fmt"
	"time"
)

// Progress is returned as a struct because we may add more to it later.
// It is designed for wrappers (like a GUI) to be able to summarize the
// current status without parsing log output.
// ETAState describes the availability of the row-copy ETA estimate, so callers
// can distinguish "still measuring" from a real estimate without parsing the
// Summary string. It mirrors the cases GetETA renders as text.
type ETAState string

const (
	// ETANone means there is no copy ETA because the migration is not in the
	// row-copy phase. Duration is 0.
	ETANone ETAState = ""
	// ETAMeasuring means a copy is in progress but no copy rate has been measured
	// yet, so no estimate is available (Summary shows "ETA TBD"). Duration is 0.
	ETAMeasuring ETAState = "measuring"
	// ETAReady means Duration holds a current remaining-time estimate.
	ETAReady ETAState = "ready"
	// ETADue means the copy is essentially complete (Summary shows "ETA DUE").
	// Duration is 0.
	ETADue ETAState = "due"
)

// ETA is the structured form of the ETA embedded in Summary. State reports
// whether Duration is available yet — e.g. ETAMeasuring during the initial
// window before a copy rate is known — so callers can show "calculating" rather
// than a misleading 0. Duration is the estimated remaining row-copy time, valid
// only when State is ETAReady and 0 otherwise.
type ETA struct {
	State    ETAState
	Duration time.Duration
}

type Progress struct {
	CurrentState State  // current state, i.e. CopyRows
	Summary      string // text based representation, i.e. "12.5% copyRows ETA 1h 30m"

	// ETA is the structured remaining row-copy estimate and its availability.
	ETA ETA

	// Checksum is the structured progress of the post-copy checksum phase,
	// populated while CurrentState is Checksum and zero otherwise. It is the
	// structured form of the checksum progress embedded in Summary.
	Checksum ChecksumProgress

	// Tables contains per-table progress for multi-table migrations.
	// For single-table migrations, this will have one entry.
	Tables []TableProgress
}

// ChecksumProgress tracks progress of the checksum phase, where Spirit verifies
// the copied data against the source before cutover. RowsChecked and RowsTotal
// are 0 outside the checksum phase.
type ChecksumProgress struct {
	RowsChecked uint64 // rows verified so far
	RowsTotal   uint64 // total rows to verify
}

// String renders the checksum progress for the human-readable summary line,
// e.g. "71436/221193 32.30%".
func (c ChecksumProgress) String() string {
	pct := float64(0)
	if c.RowsTotal > 0 {
		pct = float64(c.RowsChecked) / float64(c.RowsTotal) * 100
	}
	return fmt.Sprintf("%d/%d %.2f%%", c.RowsChecked, c.RowsTotal, pct)
}

// TableProgress tracks progress for a single table in the migration.
type TableProgress struct {
	TableName  string // name of the table being migrated
	RowsCopied uint64 // rows copied so far
	RowsTotal  uint64 // total rows expected
	IsComplete bool   // true if this table's copy is complete
}
