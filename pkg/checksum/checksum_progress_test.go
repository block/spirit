package checksum

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// formatChecksumProgress renders the verified/total rows and percentage shown in
// the checksum summary line, backing both GetProgress and GetProgressRows so the
// display string and structured data stay in sync.
func TestFormatChecksumProgress(t *testing.T) {
	tests := []struct {
		name        string
		rowsChecked uint64
		rowsTotal   uint64
		want        string
	}{
		{"no rows yet", 0, 0, "0/0 0.00%"},
		{"half way", 500, 1000, "500/1000 50.00%"},
		{"complete", 1000, 1000, "1000/1000 100.00%"},
		{"partial percent", 71436, 221193, "71436/221193 32.30%"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, formatChecksumProgress(tt.rowsChecked, tt.rowsTotal))
		})
	}
}
