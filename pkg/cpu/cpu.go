package cpu

import (
	"fmt"
	"sync"
	"time"
)

type Performance struct {
	Avg   string `json:"avg"`
	Max   string `json:"max"`
	Min   string `json:"min"`
	Error string `json:"error,omitempty"`
}

type counter struct{}

func GetPerformance() Performance {
	vals := make(chan time.Duration, 3)
	wg := sync.WaitGroup{}
	for i := 0; i < 3; i++ {
		cpuCounter, err := newCounter()
		if err != nil {
			return Performance{
				Error: err.Error(),
			}
		}
		go func() {
			wg.Add(1)
			start := cpuCounter.now()
			time.Sleep(200 * time.Millisecond)
			end := cpuCounter.now()
			vals <- end.Sub(start)
			wg.Done()
		}()
	}
	wg.Wait()

	sum := time.Duration(0)
	max := time.Duration(0)
	min := (999 * time.Minute)
	for i := 0; i < 3; i++ {
		val := <-vals
		sum = sum + val
		if val > max {
			max = val
		}
		if val < min {
			min = val
		}
	}
	close(vals)
	avg := sum / 3
	return Performance{
		Avg:   fmt.Sprintf("%.2f%%", toFixed4(float64(avg)/float64(200*time.Millisecond))*100),
		Max:   fmt.Sprintf("%.2f%%", toFixed4(float64(max)/float64(200*time.Millisecond))*100),
		Min:   fmt.Sprintf("%.2f%%", toFixed4(float64(min)/float64(200*time.Millisecond))*100),
		Error: "",
	}
}
