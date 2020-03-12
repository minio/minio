/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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
 *
 */

package disk

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/montanaflynn/stats"
)

const (
	kb = uint64(1 << 10)
	mb = uint64(kb << 10)
	gb = uint64(mb << 10)
)

var globalLatency = map[string]Latency{}
var globalThroughput = map[string]Throughput{}

// Latency holds latency information for write operations to the drive
type Latency struct {
	Avg          float64 `json:"avg_secs,omitempty"`
	Percentile50 float64 `json:"percentile50_secs,omitempty"`
	Percentile90 float64 `json:"percentile90_secs,omitempty"`
	Percentile99 float64 `json:"percentile99_secs,omitempty"`
	Min          float64 `json:"min_secs,omitempty"`
	Max          float64 `json:"max_secs,omitempty"`
}

// Throughput holds throughput information for write operations to the drive
type Throughput struct {
	Avg          float64 `json:"avg_bytes_per_sec,omitempty"`
	Percentile50 float64 `json:"percentile50_bytes_per_sec,omitempty"`
	Percentile90 float64 `json:"percentile90_bytes_per_sec,omitempty"`
	Percentile99 float64 `json:"percentile99_bytes_per_sec,omitempty"`
	Min          float64 `json:"min_bytes_per_sec,omitempty"`
	Max          float64 `json:"max_bytes_per_sec,omitempty"`
}

// GetOBDInfo about the drive
func GetOBDInfo(ctx context.Context, endpoint string) (Latency, Throughput, error) {
	runtime.LockOSThread()

	f, err := OpenFileDirectIO(endpoint, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0755)
	if err != nil {
		return Latency{}, Throughput{}, err
	}
	defer func() {
		f.Close()
		os.Remove(f.Name())
	}()

	drive := filepath.Dir(endpoint)

	// going to leave this here incase we decide to go back to caching again
	// if gl, ok := globalLatency[drive]; ok {
	// 	if gt, ok := globalThroughput[drive]; ok {
	// 		return gl, gt, nil
	// 	}
	// }

	blockSize := 1 * mb
	fileSize := 256 * mb

	latencies := make([]float64, fileSize/blockSize)
	throughputs := make([]float64, fileSize/blockSize)

	dioFile := os.NewFile(uintptr(f.Fd()), endpoint)
	data := make([]byte, blockSize)

	for i := uint64(0); i < (fileSize / blockSize); i++ {
		if ctx.Err() != nil {
			return Latency{}, Throughput{}, ctx.Err()
		}
		startTime := time.Now()
		if n, err := dioFile.Write(data); err != nil {
			return Latency{}, Throughput{}, err
		} else if uint64(n) != blockSize {
			return Latency{}, Throughput{}, fmt.Errorf("Expected to write %d, but only wrote %d", blockSize, n)
		}
		latency := time.Since(startTime)
		latencies[i] = float64(latency.Seconds())
	}

	runtime.UnlockOSThread()

	for i := range latencies {
		throughput := float64(blockSize) / latencies[i]
		throughputs[i] = throughput
	}

	var avgLatency float64
	var percentile50Latency float64
	var percentile90Latency float64
	var percentile99Latency float64
	var minLatency float64
	var maxLatency float64

	var avgThroughput float64
	var percentile50Throughput float64
	var percentile90Throughput float64
	var percentile99Throughput float64
	var minThroughput float64
	var maxThroughput float64

	if avgLatency, err = stats.Mean(latencies); err != nil {
		return Latency{}, Throughput{}, err
	}
	if percentile50Latency, err = stats.Percentile(latencies, 50); err != nil {
		return Latency{}, Throughput{}, err
	}
	if percentile90Latency, err = stats.Percentile(latencies, 90); err != nil {
		return Latency{}, Throughput{}, err
	}
	if percentile99Latency, err = stats.Percentile(latencies, 99); err != nil {
		return Latency{}, Throughput{}, err
	}
	if maxLatency, err = stats.Max(latencies); err != nil {
		return Latency{}, Throughput{}, err
	}
	if minLatency, err = stats.Min(latencies); err != nil {
		return Latency{}, Throughput{}, err
	}
	l := Latency{
		Avg:          avgLatency,
		Percentile50: percentile50Latency,
		Percentile90: percentile90Latency,
		Percentile99: percentile99Latency,
		Min:          minLatency,
		Max:          maxLatency,
	}

	if avgThroughput, err = stats.Mean(throughputs); err != nil {
		return Latency{}, Throughput{}, err
	}
	if percentile50Throughput, err = stats.Percentile(throughputs, 50); err != nil {
		return Latency{}, Throughput{}, err
	}
	if percentile90Throughput, err = stats.Percentile(throughputs, 90); err != nil {
		return Latency{}, Throughput{}, err
	}
	if percentile99Throughput, err = stats.Percentile(throughputs, 99); err != nil {
		return Latency{}, Throughput{}, err
	}
	if maxThroughput, err = stats.Max(throughputs); err != nil {
		return Latency{}, Throughput{}, err
	}
	if minThroughput, err = stats.Min(throughputs); err != nil {
		return Latency{}, Throughput{}, err
	}
	t := Throughput{
		Avg:          avgThroughput,
		Percentile50: percentile50Throughput,
		Percentile90: percentile90Throughput,
		Percentile99: percentile99Throughput,
		Min:          minThroughput,
		Max:          maxThroughput,
	}

	globalLatency[drive] = l
	globalThroughput[drive] = t

	return l, t, nil
}
