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

package mem

import (
	"runtime"
	"time"
)

// historicUsage holds the rolling average of memory used by
// minio server
var historicUsage *Usage

// memUsageMeasureInterval is the window of time between
// two measurements of memory usage
const memUsageMeasureInterval = 5 * time.Second

// triggers the collection of historic stats about the memory
// utilized by minio server
func init() {
	historicUsage = &Usage{}
	var cycles uint64
	go func() {
		for {
			time.Sleep(memUsageMeasureInterval)
			currUsage := GetUsage()
			currSum := cycles * historicUsage.Mem
			cycles = cycles + 1
			historicUsage.Mem = (currSum + currUsage.Mem) / cycles
		}
	}()
}

// Usage holds memory utilization information in human readable format
type Usage struct {
	Mem   uint64 `json:"mem"`
	Error string `json:"error,omitempty"`
}

// GetHistoricUsage measures the historic average of memory utilized by
// current process
func GetHistoricUsage() Usage {
	return *historicUsage
}

// GetUsage measures the total memory provisioned for the current process
// from the OS
func GetUsage() Usage {
	memStats := new(runtime.MemStats)
	runtime.ReadMemStats(memStats)
	return Usage{
		Mem: memStats.Sys,
	}
}
