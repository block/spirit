package applier

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// keyRange represents a parsed Vitess-style key range
type keyRange struct {
	start uint64 // inclusive
	end   uint64 // exclusive
}

// parseKeyRange parses a Vitess-style key range string into a keyRange struct.
// Examples: "-80" -> [0, 0x80...], "80-" -> [0x80..., 0xff...], "80-c0" -> [0x80..., 0xc0...]
func parseKeyRange(kr string) (keyRange, error) {
	if kr == "" {
		// We don't support empty key ranges right now to simplify testing.
		// Although this could be interpreted as an unsharded/full key range.
		return keyRange{}, errors.New("key range cannot be empty string")
	}
	parts := strings.Split(kr, "-")
	if len(parts) != 2 {
		return keyRange{}, fmt.Errorf("invalid key range format: %s (expected format: 'start-end', '-end', or 'start-')", kr)
	}

	var start, end uint64
	var err error

	// Parse start
	if parts[0] == "" {
		start = 0
	} else {
		// Pad to 16 hex chars (64 bits) and parse
		padded := parts[0] + strings.Repeat("0", 16-len(parts[0]))
		start, err = strconv.ParseUint(padded, 16, 64)
		if err != nil {
			return keyRange{}, fmt.Errorf("invalid start key range: %s: %w", parts[0], err)
		}
	}

	// Parse end
	if parts[1] == "" {
		end = ^uint64(0) // max uint64
	} else {
		// Pad to 16 hex chars (64 bits) and parse
		padded := parts[1] + strings.Repeat("0", 16-len(parts[1]))
		end, err = strconv.ParseUint(padded, 16, 64)
		if err != nil {
			return keyRange{}, fmt.Errorf("invalid end key range: %s: %w", parts[1], err)
		}
	}
	return keyRange{start: start, end: end}, nil
}

// contains checks if a hash value falls within this key range
func (kr keyRange) contains(hash uint64) bool {
	return hash >= kr.start && hash < kr.end
}

// overlaps checks if two key ranges overlap
func (kr keyRange) overlaps(other keyRange) bool {
	// Two ranges [a, b) and [c, d) overlap if:
	// - a < d AND c < b
	// This is because:
	// - If a >= d, then the first range starts at or after the second range ends (no overlap)
	// - If c >= b, then the second range starts at or after the first range ends (no overlap)
	return kr.start < other.end && other.start < kr.end
}
