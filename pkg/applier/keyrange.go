package applier

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// keyRange represents a parsed Vitess-style key range
type keyRange struct {
	start     uint64 // inclusive
	end       uint64 // exclusive; only meaningful when !unbounded
	unbounded bool   // open upper range ("80-"): contains everything >= start
}

// keyRangeHexDigits is the number of hex digits in a 64-bit keyspace id. A
// bound is a PREFIX of that id, so it may be shorter (right-padded with zeros)
// but never longer.
const keyRangeHexDigits = 16

var keyRangeHexRe = regexp.MustCompile(`^[0-9a-f]+$`)

// parseKeyRangeBound parses one side of a key range: a hex prefix of the 64-bit
// keyspace id, right-padded with zeros to a full id. side is "start" or "end",
// used only for the error message.
func parseKeyRangeBound(side, bound string) (uint64, error) {
	if !keyRangeHexRe.MatchString(bound) {
		return 0, fmt.Errorf("invalid %s key range: %s (expected hex characters [0-9a-f])", side, bound)
	}
	if len(bound) > keyRangeHexDigits {
		return 0, fmt.Errorf("invalid %s key range: %s (at most %d hex characters, it is a prefix of a 64-bit keyspace id)", side, bound, keyRangeHexDigits)
	}
	padded := bound + strings.Repeat("0", keyRangeHexDigits-len(bound))
	v, err := strconv.ParseUint(padded, 16, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s key range: %s: %w", side, bound, err)
	}
	return v, nil
}

// parseKeyRange parses a Vitess-style key range string into a keyRange struct.
// Examples: "-80" -> [0, 0x80...], "80-" -> [0x80..., 0xff...], "80-c0" -> [0x80..., 0xc0...]
//
// "", "0" and "-" all mean the whole key space. "0" is the Vitess name of the
// only shard of an unsharded keyspace; "" is what a caller with a single,
// unsharded target leaves unset.
func parseKeyRange(kr string) (keyRange, error) {
	if kr == "" || kr == "0" {
		return keyRange{unbounded: true}, nil
	}
	parts := strings.Split(kr, "-")
	if len(parts) != 2 {
		return keyRange{}, fmt.Errorf("invalid key range format: %s (expected format: 'start-end', '-end', or 'start-')", kr)
	}

	var start, end uint64
	var err error

	// Parse start
	if parts[0] != "" {
		if start, err = parseKeyRangeBound("start", parts[0]); err != nil {
			return keyRange{}, err
		}
	}

	// Parse end. An empty end means the range has NO upper bound — Vitess
	// key ranges are byte prefixes, so "80-" contains every keyspace id
	// >= 0x80..., INCLUDING 0xffffffffffffffff. Representing it as
	// end=MaxUint64 with an exclusive compare would wrongly exclude a hash
	// of exactly MaxUint64, leaving that row with no shard at all.
	if parts[1] == "" {
		return keyRange{start: start, unbounded: true}, nil
	}
	if end, err = parseKeyRangeBound("end", parts[1]); err != nil {
		return keyRange{}, err
	}
	// A bounded range is [start, end): end <= start describes no keyspace id at
	// all, so every row hashing into it would be stranded with "no shard found"
	// at apply time. Reject it here instead.
	if end <= start {
		return keyRange{}, fmt.Errorf("invalid key range: %s (end must be greater than start; use %q for an open-ended range)", kr, parts[0]+"-")
	}
	return keyRange{start: start, end: end}, nil
}

// contains checks if a hash value falls within this key range
func (kr keyRange) contains(hash uint64) bool {
	return hash >= kr.start && (kr.unbounded || hash < kr.end)
}

// coversAll reports whether the range is the whole key space.
func (kr keyRange) coversAll() bool {
	return kr.start == 0 && kr.unbounded
}

// String renders the parsed range for logs and errors.
func (kr keyRange) String() string {
	if kr.unbounded {
		return fmt.Sprintf("[0x%016x, unbounded)", kr.start)
	}
	return fmt.Sprintf("[0x%016x, 0x%016x)", kr.start, kr.end)
}

// IsFullKeyRange reports whether kr is a valid Vitess-style key range that
// covers the whole key space ("", "0", "-", "0-"). A one-target applier with
// such a range writes every row without routing; any other range routes, so
// rows outside it are refused. Callers that write one logical target use this
// to reject a partial range before setup.
func IsFullKeyRange(kr string) bool {
	parsed, err := parseKeyRange(kr)
	return err == nil && parsed.coversAll()
}

// ValidateKeyRanges parses each Vitess-style key range and checks that no two
// overlap — the same rules New enforces at construction. It
// exists so callers can fail fast on a bad shard layout before doing any work
// (e.g. move validates reverse-window source key ranges before the copy, since
// the sharded reverse applier is only constructed after the forward cutover).
func ValidateKeyRanges(ranges []string) error {
	parsed := make([]keyRange, len(ranges))
	for i, r := range ranges {
		kr, err := parseKeyRange(r)
		if err != nil {
			return fmt.Errorf("key range %d (%q): %w", i, r, err)
		}
		parsed[i] = kr
	}
	for i := range parsed {
		for j := i + 1; j < len(parsed); j++ {
			if parsed[i].overlaps(parsed[j]) {
				return fmt.Errorf("key ranges overlap: %q and %q", ranges[i], ranges[j])
			}
		}
	}
	return nil
}

// overlaps checks if two key ranges overlap
func (kr keyRange) overlaps(other keyRange) bool {
	// Two ranges [a, b) and [c, d) overlap if a < d AND c < b:
	// - If a >= d, then the first range starts at or after the second range ends (no overlap)
	// - If c >= b, then the second range starts at or after the first range ends (no overlap)
	// An unbounded range has no end, so the "< end" side of its check is
	// vacuously true — two unbounded ranges always overlap.
	return (other.unbounded || kr.start < other.end) &&
		(kr.unbounded || other.start < kr.end)
}
