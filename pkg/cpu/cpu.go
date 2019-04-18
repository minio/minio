/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cpu

import (
	"sync"
	"time"
)

// rollingAvg holds the rolling average of the cpu load on the minio
// server over its lifetime
var rollingAvg *Load

// cpuMeasureInterval is the interval of time between two
// measurements of CPU load
const cpuLoadMeasureInterval = 5 * time.Second

// triggers the average load computation at server spawn
func init() {
	rollingAvg = &Load{
		Min: float64(0),
		Max: float64(0),
		Avg: float64(0),
	}
	var rollingSum float64
	var cycles float64
	go func() {
		for {
			time.Sleep(cpuLoadMeasureInterval)
			cycles = cycles + 1
			currLoad := GetLoad()
			if rollingAvg.Max < currLoad.Max || rollingAvg.Max == 0 {
				rollingAvg.Max = currLoad.Max
			}
			if rollingAvg.Min > currLoad.Min || rollingAvg.Min == 0 {
				rollingAvg.Min = currLoad.Min
			}
			rollingSum = rollingSum + currLoad.Avg
			rollingAvg.Avg = rollingSum / cycles
		}
	}()
}

const (
	// cpuLoadWindow is the interval of time for which the
	// cpu utilization is measured
	cpuLoadWindow = 200 * time.Millisecond

	// cpuLoadSampleSize is the number of samples measured
	// for calculating cpu utilization
	cpuLoadSampleSize = 3

	// endOfTime represents the end of time
	endOfTime = time.Duration(1<<63 - 1)
)

// Load holds CPU utilization % measured in three intervals of 200ms each
type Load struct {
	Avg   float64 `json:"avg"`
	Max   float64 `json:"max"`
	Min   float64 `json:"min"`
	Error string  `json:"error,omitempty"`
}

type counter struct{}

// GetHistoricLoad returns the historic CPU utilization of the current process
func GetHistoricLoad() Load {
	return *rollingAvg
}

// GetLoad returns the CPU utilization of the current process
// This function works by calcualating the amount of cpu clock
// cycles the current process used in a given time window
//
// This corresponds to the CPU utilization calculation done by
// tools like top. Here, we use the getclocktime with the
// CLOCK_PROCESS_CPUTIME_ID parameter to obtain the total number of
// clock ticks used by the process so far. Then we sleep for
// 200ms and obtain the the total number of clock ticks again. The
// difference between the two counts provides us the number of
// clock ticks used by the process in the 200ms interval.
//
// The ratio of clock ticks used (measured in nanoseconds) to number
// of nanoseconds in 200 milliseconds provides us the CPU usage
// for the process currently
func GetLoad() Load {
	vals := make(chan time.Duration, 3)
	wg := sync.WaitGroup{}
	for i := 0; i < cpuLoadSampleSize; i++ {
		cpuCounter, err := newCounter()
		if err != nil {
			return Load{
				Error: err.Error(),
			}
		}
		wg.Add(1)
		go func() {
			start := cpuCounter.now()
			time.Sleep(cpuLoadWindow)
			end := cpuCounter.now()
			vals <- end.Sub(start)
			wg.Done()
		}()
	}
	wg.Wait()

	sum := time.Duration(0)
	max := time.Duration(0)
	min := (endOfTime)
	for i := 0; i < cpuLoadSampleSize; i++ {
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
	return Load{
		Avg:   toFixed4(float64(avg)/float64(200*time.Millisecond)) * 100,
		Max:   toFixed4(float64(max)/float64(200*time.Millisecond)) * 100,
		Min:   toFixed4(float64(min)/float64(200*time.Millisecond)) * 100,
		Error: "",
	}
}
