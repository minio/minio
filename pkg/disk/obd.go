package disk

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"
	"syscall"
	"context"
	
	"github.com/montanaflynn/stats"
)


const kb = uint64(1 << 10)
const mb = uint64(kb << 10)
const gb = uint64(mb << 10)

var globalLatency = map[string]Latency{}
var globalThroughput = map[string]Throughput{}

// Latency holds latency information for write operations to the drive
type Latency struct {
	Avg          float64 `json:"avg,omitempty"`
	Percentile50 float64 `json:"percentile50,omitempty"`
	Percentile90 float64 `json:"percentile90,omitempty"`
	Percentile99 float64 `json:"percentile99,omitempty"`
	Min          float64 `json:"min,omitempty"`
	Max          float64 `json:"max,omitempty"`
}

// Throughput holds throughput information for write operations to the drive
type Throughput struct {
	Avg          float64 `json:"avg_bps,omitempty"`
	Percentile50 float64 `json:"percentile50_bps,omitempty"`
	Percentile90 float64 `json:"percentile90_bps,omitempty"`
	Percentile99 float64 `json:"percentile99_bps,omitempty"`
	Min          float64 `json:"min_bps,omitempty"`
	Max          float64 `json:"max_bps,omitempty"`
}

// GetOBDInfo about the drive
func GetOBDInfo(ctx context.Context, endpoint string) (Latency, Throughput, error) {
	runtime.LockOSThread()
	
	f, err := OpenFileDirectIO(endpoint, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0755)
	if err != nil {
		return Latency{}, Throughput{}, err
	}
	defer func() {
		if err := f.Close(); err != nil {
			// Ideal behavior should be to panic. But it might
			// lead to availability issues for the server. Given
			// that this code is not on the critical path, it's ok
			// to silently fail here
			// panic(err)
		}
			if err := os.Remove(f.Name()); err != nil {
			// Ideal behavior should be to panic. But it might
			// lead to availability issues for the server. Given
			// that this code is not on the critical path, it's ok
			// to silently fail here
			// panic(err)
		}
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

	data := make([]byte, blockSize)
	for i := uint64(0); i < (fileSize / blockSize); i++ {
		if ctx.Err() != nil {
			return Latency{}, Throughput{}, ctx.Err()
		}
		startTime := time.Now()
		if n, err := syscall.Write(int(f.Fd()), data); err != nil {
			return Latency{}, Throughput{}, err
		} else if uint64(n) != blockSize {
			return Latency{}, Throughput{}, fmt.Errorf("Expected to write %d, but only wrote %d", blockSize, n)
		}
		latency := time.Now().Sub(startTime)
		latencies[i] = float64(latency.Seconds())
	}

	runtime.UnlockOSThread()
	
	for i := range latencies {
		throughput := float64(blockSize)/latencies[i]
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
