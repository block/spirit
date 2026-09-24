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
