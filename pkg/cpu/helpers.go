package cpu

import (
	"math"
)

func round(num float64) int {
	return int(num + math.Copysign(0.5, num))
}

func toFixed4(num float64) float64 {
	output := math.Pow(10, float64(4))
	return float64(round(num*output)) / output
}
