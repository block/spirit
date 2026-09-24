package applier

import (
	"testing"
)

func TestParseKeyRange(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		wantErr       bool
		wantStart     uint64
		wantEnd       uint64
		wantUnbounded bool
	}{
		{
			name:      "valid range -80",
			input:     "-80",
			wantErr:   false,
			wantStart: 0x0000000000000000,
			wantEnd:   0x8000000000000000,
		},
		{
			name:          "valid range 80-",
			input:         "80-",
			wantErr:       false,
			wantStart:     0x8000000000000000,
			wantUnbounded: true,
		},
		{
			name:      "valid range 40-80",
			input:     "40-80",
			wantErr:   false,
			wantStart: 0x4000000000000000,
			wantEnd:   0x8000000000000000,
		},
		{
			name:    "invalid start - uppercase hex",
			input:   "8A-",
			wantErr: true,
		},
		{
			name:    "invalid end - uppercase hex",
			input:   "-8F",
			wantErr: true,
		},
		{
			name:    "invalid start - contains g",
			input:   "8g-",
			wantErr: true,
		},
		{
			name:    "invalid end - contains z",
			input:   "-8z",
			wantErr: true,
		},
		{
			name:    "invalid start - contains special char",
			input:   "8!-",
			wantErr: true,
		},
		{
			name:    "invalid end - contains space",
			input:   "-8 0",
			wantErr: true,
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
		},
		{
			name:    "missing dash",
			input:   "80",
			wantErr: true,
		},
		{
			name:      "full 16-hex-digit bounds",
			input:     "4000000000000000-8000000000000000",
			wantErr:   false,
			wantStart: 0x4000000000000000,
			wantEnd:   0x8000000000000000,
		},
		{
			// A bound is a prefix of a 64-bit keyspace id; anything longer used
			// to panic in the zero-padding (strings.Repeat with a negative count).
			name:    "start longer than 16 hex digits",
			input:   "80000000000000000-",
			wantErr: true,
		},
		{
			name:    "end longer than 16 hex digits",
			input:   "-80000000000000000",
			wantErr: true,
		},
		{
			// [0xc0.., 0x80..) is empty: rows hashing there would have no shard.
			name:    "end before start",
			input:   "c0-80",
			wantErr: true,
		},
		{
			name:    "end equal to start",
			input:   "80-80",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseKeyRange(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseKeyRange(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if got.start != tt.wantStart {
					t.Errorf("parseKeyRange(%q).start = 0x%016x, want 0x%016x", tt.input, got.start, tt.wantStart)
				}
				if got.end != tt.wantEnd {
					t.Errorf("parseKeyRange(%q).end = 0x%016x, want 0x%016x", tt.input, got.end, tt.wantEnd)
				}
				if got.unbounded != tt.wantUnbounded {
					t.Errorf("parseKeyRange(%q).unbounded = %v, want %v", tt.input, got.unbounded, tt.wantUnbounded)
				}
			}
		})
	}
}

func TestKeyRangeContains(t *testing.T) {
	tests := []struct {
		name     string
		keyRange string
		hash     uint64
		want     bool
	}{
		{
			name:     "hash in lower half",
			keyRange: "-80",
			hash:     0x4000000000000000,
			want:     true,
		},
		{
			name:     "hash in upper half",
			keyRange: "80-",
			hash:     0xc000000000000000,
			want:     true,
		},
		{
			name:     "hash at start boundary (inclusive)",
			keyRange: "80-",
			hash:     0x8000000000000000,
			want:     true,
		},
		{
			name:     "hash at end boundary (exclusive)",
			keyRange: "-80",
			hash:     0x8000000000000000,
			want:     false,
		},
		{
			// Regression: an open-ended range has NO upper bound. Vitess's
			// byte-wise KeyRangeContains places keyspace id ff...ff in "80-";
			// modeling the open end as end=MaxUint64 with an exclusive
			// compare wrongly left this hash with no shard at all.
			name:     "max hash belongs to the open-ended range",
			keyRange: "80-",
			hash:     0xffffffffffffffff,
			want:     true,
		},
		{
			name:     "max hash belongs to the full range",
			keyRange: "-",
			hash:     0xffffffffffffffff,
			want:     true,
		},
		{
			name:     "zero hash belongs to the full range",
			keyRange: "-",
			hash:     0x0000000000000000,
			want:     true,
		},
		{
			name:     "max hash not in a bounded range",
			keyRange: "80-c0",
			hash:     0xffffffffffffffff,
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kr, err := parseKeyRange(tt.keyRange)
			if err != nil {
				t.Fatalf("parseKeyRange(%q) failed: %v", tt.keyRange, err)
			}
			got := kr.contains(tt.hash)
			if got != tt.want {
				t.Errorf("keyRange(%q).contains(0x%016x) = %v, want %v", tt.keyRange, tt.hash, got, tt.want)
			}
		})
	}
}

func TestKeyRangeOverlaps(t *testing.T) {
	tests := []struct {
		name   string
		range1 string
		range2 string
		want   bool
	}{
		{
			name:   "adjacent ranges do not overlap",
			range1: "-80",
			range2: "80-",
			want:   false,
		},
		{
			name:   "overlapping ranges",
			range1: "-80",
			range2: "40-c0",
			want:   true,
		},
		{
			name:   "contained range",
			range1: "40-c0",
			range2: "60-80",
			want:   true,
		},
		{
			name:   "two open-ended ranges overlap",
			range1: "80-",
			range2: "c0-",
			want:   true,
		},
		{
			name:   "bounded range below an open-ended range does not overlap",
			range1: "-80",
			range2: "80-",
			want:   false,
		},
		{
			name:   "bounded range crossing into an open-ended range overlaps",
			range1: "40-c0",
			range2: "80-",
			want:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kr1, err := parseKeyRange(tt.range1)
			if err != nil {
				t.Fatalf("parseKeyRange(%q) failed: %v", tt.range1, err)
			}
			kr2, err := parseKeyRange(tt.range2)
			if err != nil {
				t.Fatalf("parseKeyRange(%q) failed: %v", tt.range2, err)
			}
			got := kr1.overlaps(kr2)
			if got != tt.want {
				t.Errorf("keyRange(%q).overlaps(%q) = %v, want %v", tt.range1, tt.range2, got, tt.want)
			}
		})
	}
}
