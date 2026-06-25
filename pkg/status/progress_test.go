package status

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
