package status

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// ETA.String renders the estimate the way the copy summary and the copier's
// GetETA show it: the availability states as fixed words, otherwise the
// remaining duration.
func TestETAString(t *testing.T) {
	tests := []struct {
		name string
		eta  ETA
		want string
	}{
		{"none", ETA{}, "0s"},
		{"measuring", ETA{State: ETAMeasuring}, "TBD"},
		{"due", ETA{State: ETADue}, "DUE"},
		{"due ignores a leftover duration", ETA{State: ETADue, Duration: time.Minute}, "DUE"},
		{"ready", ETA{State: ETAReady, Duration: 90 * time.Second}, "1m30s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.eta.String())
		})
	}
}

// ChecksumProgress.String renders the verified/total rows and percentage shown
// in the checksum summary line, e.g. "71436/221193 32.30%".
func TestChecksumProgressString(t *testing.T) {
	tests := []struct {
		name     string
		progress ChecksumProgress
		want     string
	}{
		{"no rows yet", ChecksumProgress{}, "0/0 0.00%"},
		{"half way", ChecksumProgress{RowsChecked: 500, RowsTotal: 1000}, "500/1000 50.00%"},
		{"complete", ChecksumProgress{RowsChecked: 1000, RowsTotal: 1000}, "1000/1000 100.00%"},
		{"partial percent", ChecksumProgress{RowsChecked: 71436, RowsTotal: 221193}, "71436/221193 32.30%"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.progress.String())
		})
	}
}

// EstimateETA reports a remaining-time estimate only once a copy rate has been
// measured and while the copy is still meaningfully in flight; the not-ready
// cases are distinguished as ETAMeasuring (no rate yet) vs ETADue (near done),
// which the GUI/summary callers turn into "TBD"/"DUE"/0.
func TestEstimateETA(t *testing.T) {
	measured := time.Now().Add(-2 * etaInitialWaitTime)

	tests := []struct {
		name       string
		copiedRows uint64
		totalRows  uint64
		pct        float64
		rowsPerSec uint64
		startTime  time.Time
		want       ETA
	}{
		{
			name:       "due once the copy is essentially complete",
			copiedRows: 999, totalRows: 1000, pct: 99.999, rowsPerSec: 10, startTime: measured,
			want: ETA{State: ETADue},
		},
		{
			name:       "measuring before a copy rate is known",
			copiedRows: 100, totalRows: 1000, pct: 10, rowsPerSec: 0, startTime: measured,
			want: ETA{State: ETAMeasuring},
		},
		{
			name:       "measuring during the initial wait window",
			copiedRows: 100, totalRows: 1000, pct: 10, rowsPerSec: 50, startTime: time.Now(),
			want: ETA{State: ETAMeasuring},
		},
		{
			name:       "ready: estimate from remaining rows and rate",
			copiedRows: 500, totalRows: 1000, pct: 50, rowsPerSec: 10, startTime: measured,
			want: ETA{State: ETAReady, Duration: 50 * time.Second},
		},
		{
			name:       "ready: floors fractional seconds",
			copiedRows: 0, totalRows: 1000, pct: 0, rowsPerSec: 3, startTime: measured,
			want: ETA{State: ETAReady, Duration: 333 * time.Second},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, EstimateETA(tt.copiedRows, tt.totalRows, tt.pct, tt.rowsPerSec, tt.startTime))
		})
	}
}
