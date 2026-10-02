package utils

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEstimateRenderedValueSize pins the per-value estimate for each concrete
// type database/sql and the binlog reader yield, plus the nil and fallback
// cases.
func TestEstimateRenderedValueSize(t *testing.T) {
	require.Equal(t, 4, estimateRenderedValueSize(nil), "nil renders as NULL")
	require.Equal(t, 7, estimateRenderedValueSize([]byte("hello")), "[]byte sized by length plus quotes")
	require.Equal(t, 2, estimateRenderedValueSize([]byte{}), "empty []byte is just its quotes")
	require.Equal(t, 2, estimateRenderedValueSize(""), "empty string is just its quotes")
	require.Equal(t, 5, estimateRenderedValueSize("abc"), "string sized by length plus quotes")
	require.Equal(t, 28, estimateRenderedValueSize(time.Now()), "time.Time is a fixed quoted width")
	require.Equal(t, 10, estimateRenderedValueSize(int64(42)), "integers assume 10 digits")
	require.Equal(t, 8, estimateRenderedValueSize(float64(1.5)), "floats assume a typical width")
	require.Equal(t, 1, estimateRenderedValueSize(true), "bool renders as one digit")
	require.Equal(t, 32, estimateRenderedValueSize(struct{}{}), "unknown types fall back to 32")
}

// TestEstimateRenderedChunkSize covers the whole-chunk sum, including the
// empty-chunk case (which the buffered copier reports as zero bytes for a gap
// chunk).
func TestEstimateRenderedChunkSize(t *testing.T) {
	require.Equal(t, uint64(0), EstimateRenderedChunkSize(nil), "no rows is zero bytes")
	require.Equal(t, uint64(0), EstimateRenderedChunkSize([][]any{}), "empty rows slice is zero bytes")

	// Each row is 2 (parentheses) plus, per value, its size and 2 (separator).
	rows := [][]any{
		{int64(1), []byte("alice"), nil},   // 2 + 12 + 9 + 6 = 29
		{int64(2), []byte("bob"), "extra"}, // 2 + 12 + 7 + 9 = 30
	}
	require.Equal(t, uint64(59), EstimateRenderedChunkSize(rows), "sum across all rows")

	// A non-empty chunk whose values are all empty var-length still sizes > 0,
	// so it is never mistaken for a gap chunk by the byte sizer.
	allEmpty := [][]any{
		{[]byte{}, ""},
		{"", []byte{}},
	}
	require.Equal(t, uint64(20), EstimateRenderedChunkSize(allEmpty),
		"rows of all-empty values still count their quotes and parentheses, so the chunk is not a gap")
}

func TestEstimateRenderedRowSize(t *testing.T) {
	tests := []struct {
		name    string
		values  []any
		minSize int // minimum expected size
		maxSize int // maximum expected size (for flexibility)
	}{
		{
			name:    "empty row",
			values:  []any{},
			minSize: 2, // just parentheses
			maxSize: 2,
		},
		{
			name:    "single integer",
			values:  []any{int64(123)},
			minSize: 6,
			maxSize: 20, // flat 10-digit assumption + overhead, not the rendered "123"
		},
		{
			name:    "single string",
			values:  []any{"hello"},
			minSize: 7, // "hello" + overhead
			maxSize: 15,
		},
		{
			name:    "nil value",
			values:  []any{nil},
			minSize: 6, // "<nil>" + overhead
			maxSize: 12,
		},
		{
			name:    "mixed types",
			values:  []any{int64(42), "test", nil, true, 3.14},
			minSize: 20, // sum of all values + overhead
			maxSize: 60,
		},
		{
			name:    "large string",
			values:  []any{"this is a very long string that represents a TEXT column with lots of data"},
			minSize: 75,
			maxSize: 100,
		},
		{
			name:    "byte slice",
			values:  []any{[]byte("binary data")},
			minSize: 11,
			maxSize: 50,
		},
		{
			name:    "multiple columns",
			values:  []any{int64(1), "Alice", "alice@example.com", int64(25), true},
			minSize: 30,
			maxSize: 80,
		},
		{
			// A full-width int64 is the case the flat integer estimate
			// deliberately under-measures: ~20 rendered characters estimated as
			// 10. See TestEstimateRenderedRowSizeUnderestimateStaysSafe (pkg/applier) for
			// why that is acceptable, and estimateRenderedValueSize for why it is preferred to
			// over-estimating every ordinary ID.
			name:    "large integers",
			values:  []any{int64(9223372036854775807), int64(-9223372036854775808)},
			minSize: 20,
			maxSize: 60,
		},
		{
			name:    "floating point numbers",
			values:  []any{3.14159, -2.71828, 0.0},
			minSize: 15,
			maxSize: 40,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size := EstimateRenderedRowSize(tt.values)
			assert.GreaterOrEqual(t, size, tt.minSize, "size should be at least minSize")
			assert.LessOrEqual(t, size, tt.maxSize, "size should not exceed maxSize")
			t.Logf("Estimated size for %s: %d bytes", tt.name, size)
		})
	}
}

func TestEstimateRenderedRowSizeRealistic(t *testing.T) {
	// Test with realistic table data
	t.Run("users table row", func(t *testing.T) {
		// id, username, email, created_at, is_active
		values := []any{
			int64(12345),
			"john_doe_2024",
			"john.doe@example.com",
			"2024-01-15 10:30:00",
			true,
		}
		size := EstimateRenderedRowSize(values)
		// Should be reasonable size, not too large
		require.Greater(t, size, 40, "should account for all fields")
		require.Less(t, size, 150, "should not be excessively large")
		t.Logf("Users table row size: %d bytes", size)
	})

	t.Run("blog posts with TEXT column", func(t *testing.T) {
		// id, title, content (large TEXT), author_id
		largeContent := make([]byte, 10000) // 10KB of content
		for i := range largeContent {
			largeContent[i] = 'a'
		}
		values := []any{
			int64(1),
			"My Blog Post Title",
			string(largeContent),
			int64(42),
		}
		size := EstimateRenderedRowSize(values)
		// Should be roughly 10KB + overhead
		require.Greater(t, size, 10000, "should account for large content")
		require.Less(t, size, 11000, "overhead should be reasonable")
		t.Logf("Blog post row size: %d bytes", size)
	})
}

func TestEstimateRenderedRowSizeConsistency(t *testing.T) {
	// Test that the same input produces the same output
	values := []any{int64(123), "test", true, 3.14}

	size1 := EstimateRenderedRowSize(values)
	size2 := EstimateRenderedRowSize(values)
	size3 := EstimateRenderedRowSize(values)

	require.Equal(t, size1, size2, "should be consistent")
	require.Equal(t, size2, size3, "should be consistent")
}

func TestEstimateRenderedRowSizeZeroValues(t *testing.T) {
	// Test with zero/empty values
	tests := []struct {
		name   string
		values []any
	}{
		{
			name:   "zero integer",
			values: []any{int64(0)},
		},
		{
			name:   "empty string",
			values: []any{""},
		},
		{
			name:   "zero float",
			values: []any{0.0},
		},
		{
			name:   "false boolean",
			values: []any{false},
		},
		{
			name:   "empty byte slice",
			values: []any{[]byte{}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size := EstimateRenderedRowSize(tt.values)
			// Should have some size even for zero values
			require.Positive(t, size, "should have non-zero size")
			t.Logf("%s size: %d bytes", tt.name, size)
		})
	}
}

// estimateRenderedValueSize must never return zero or negative, including for
// the deliberately under-estimated cases (full-width integers, binary data,
// strings that grow under escaping): a non-positive size would let the
// applier's chunklet splitter build an unbounded statement.
func TestEstimateRenderedValueSizePositive(t *testing.T) {
	values := []any{
		nil,
		int64(math.MaxInt64),
		int64(math.MinInt64),
		[]byte{},
		[]byte("\x00\x01\x02\x03\x04\x05\x06\x07"),
		"",
		`a string with "quotes" and \backslashes\ that escaping will grow`,
		time.Now(),
		0.0,
		false,
		struct{}{},
	}
	for _, v := range values {
		assert.Positive(t, estimateRenderedValueSize(v), "value %v estimated non-positively", v)
	}
}

func TestParseSizeNumber(t *testing.T) {
	for _, tc := range []struct {
		in      string
		want    uint64
		wantErr bool
	}{
		{in: "0", want: 0},
		{in: "4194304", want: 4194304},
		{in: "4M", want: 4 << 20},
		{in: "4m", want: 4 << 20},
		{in: "1K", want: 1 << 10},
		{in: "2G", want: 2 << 30},
		{in: "", wantErr: true},
		{in: "4T", wantErr: true},
		{in: "M", wantErr: true},
		{in: "-1", wantErr: true},
		{in: "4 M", wantErr: true},
		{in: "18446744073709551615K", wantErr: true},
	} {
		got, err := ParseSizeNumber(tc.in)
		if tc.wantErr {
			require.Error(t, err, tc.in)
			continue
		}
		require.NoError(t, err, tc.in)
		assert.Equal(t, tc.want, got, tc.in)
	}
}
