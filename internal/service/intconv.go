package service

import (
	"fmt"
	"math"
)

func intFromInt64(name string, value int64) (int, error) {
	if value < 0 {
		return 0, fmt.Errorf("%s must be >= 0", name)
	}
	if value > int64(math.MaxInt) {
		return 0, fmt.Errorf("%s exceeds platform int max (%d)", name, value)
	}
	return int(value), nil
}
