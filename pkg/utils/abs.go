package utils

// Signed is the set of signed integer types Abs accepts.
type Signed interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64
}

// Abs returns the absolute value of n. The standard library has no integer
// abs (math.Abs is float64-only), so this avoids a float round trip.
//
// Like any two's-complement abs, it overflows for the most negative value of
// T: Abs(math.MinInt64) is math.MinInt64.
func Abs[T Signed](n T) T {
	if n < 0 {
		return -n
	}
	return n
}
