/*
 * Minio Cloud Storage, (C) 2019 Minio, Inc.
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
	"fmt"
	"sync"
	"time"
)

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
	Avg   string `json:"avg"`
	Max   string `json:"max"`
	Min   string `json:"min"`
	Error string `json:"error,omitempty"`
}

type counter struct{}

// GetLoad returns the CPU utilization % of the current process
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
		Avg:   fmt.Sprintf("%.2f%%", toFixed4(float64(avg)/float64(200*time.Millisecond))*100),
		Max:   fmt.Sprintf("%.2f%%", toFixed4(float64(max)/float64(200*time.Millisecond))*100),
		Min:   fmt.Sprintf("%.2f%%", toFixed4(float64(min)/float64(200*time.Millisecond))*100),
		Error: "",
	}
}
