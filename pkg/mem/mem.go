package mem

import (
	"runtime"
)

type Performance struct {
	Mem   string `json:"mem"`
	Error string `json:"error,omitempty"`
}

func GetPerformance() Performance {
	memStats := new(runtime.MemStats)
	runtime.ReadMemStats(memStats)
	return Performance{
		Mem: fmtBytes(memStats.Sys),
	}
}
