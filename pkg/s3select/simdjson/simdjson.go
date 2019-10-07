package simdjson

import (
	"math"
)

func float64_2_uint64(f float64) uint64 {
	return math.Float64bits(f)
}
