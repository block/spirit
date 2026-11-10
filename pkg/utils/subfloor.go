package utils

// SubFloor returns a - b for unsigned integers, or 0 when b is larger, where
// plain subtraction would wrap around to a huge value. Use it for the
// distance between two readings of a counter that can be reset in between.
func SubFloor[T ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64](a, b T) T {
	if a < b {
		return 0
	}
	return a - b
}
