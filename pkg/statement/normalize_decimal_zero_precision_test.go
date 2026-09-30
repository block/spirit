package statement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecimalZeroPrecision(t *testing.T) {
	tests := []struct {
		colDef        string
		wantPrecision *int
		wantScale     *int
	}{
		{"decimal(0)", new(10), nil},
		{"decimal(0,0)", new(10), nil},
		{"numeric(0)", new(10), nil},
		{"decimal(0) unsigned", new(10), nil},
		{"decimal", new(10), nil}, // the parser already expands a bare DECIMAL
		{"decimal(5,2)", new(5), new(2)},
	}
	for _, tc := range tests {
		t.Run(tc.colDef, func(t *testing.T) {
			ct, err := ParseCreateTable("CREATE TABLE t (d " + tc.colDef + ")")
			require.NoError(t, err)
			require.Len(t, ct.Columns, 1)
			c := ct.Columns[0]
			assert.Equal(t, tc.wantPrecision, c.Precision)
			assert.Equal(t, tc.wantPrecision, c.Length)
			assert.Equal(t, tc.wantScale, c.Scale)
		})
	}
}

// TestDecimalZeroPrecisionConverges verifies that a declared decimal(0) does
// not diff against the decimal(10,0) MySQL stores for it.
func TestDecimalZeroPrecisionConverges(t *testing.T) {
	declared, err := ParseCreateTable("CREATE TABLE t (d decimal(0), u decimal(0,0) unsigned)")
	require.NoError(t, err)
	live, err := ParseCreateTable("CREATE TABLE t (d decimal(10,0), u decimal(10,0) unsigned)")
	require.NoError(t, err)
	stmts, err := live.Diff(declared, nil)
	require.NoError(t, err)
	assert.Nil(t, stmts)
}
