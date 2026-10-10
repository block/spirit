package utils

import (
	"cmp"
	"strconv"
	"strings"
)

// CompareMySQLVersions compares two dotted MySQL versions numerically, returning
// -1, 0 or 1. Anything after the numeric part (8.0.28-log) is ignored, and a
// missing component counts as 0.
func CompareMySQLVersions(a, b string) int {
	pa, pb := versionParts(a), versionParts(b)
	for i := range max(len(pa), len(pb)) {
		var x, y int
		if i < len(pa) {
			x = pa[i]
		}
		if i < len(pb) {
			y = pb[i]
		}
		if x != y {
			return cmp.Compare(x, y)
		}
	}
	return 0
}

// versionParts returns the leading numeric components of a MySQL version.
func versionParts(version string) []int {
	var parts []int
	for field := range strings.SplitSeq(version, ".") {
		end := strings.IndexFunc(field, func(r rune) bool { return r < '0' || r > '9' })
		if end == 0 {
			break
		}
		if end > 0 {
			field = field[:end]
		}
		n, err := strconv.Atoi(field)
		if err != nil {
			break
		}
		parts = append(parts, n)
		if end > 0 {
			break // a suffix such as -log ends the numeric part
		}
	}
	return parts
}
