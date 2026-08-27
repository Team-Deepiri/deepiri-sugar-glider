package service

import "math"

// clampInt64ToInt converts value to int after clamping into the int domain.
// The MinInt/MaxInt guards are required so CodeQL treats the cast as safe.
func clampInt64ToInt(value int64) int {
	if value > math.MaxInt {
		return math.MaxInt
	}
	if value < math.MinInt {
		return math.MinInt
	}
	return int(value)
}
