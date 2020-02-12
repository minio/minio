package disk

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/montanaflynn/stats"
)

const KB = uint64(1 << 10)
const MB = uint64(KB << 10)
const GB = uint64(MB << 10)

type Latency struct {
	Avg          float64 `json:"avg,omitempty"`
	Percentile50 float64 `json:"percentile50,omitempty"`
	Percentile90 float64 `json:"percentile90,omitempty"`
	Percentile99 float64 `json:"percentile99,omitempty"`
	Min          float64 `json:"min,omitempty"`
	Max          float64 `json:"max,omitempty"`
}

type Throughput struct {
	Avg          float64 `json:"avg_bps,omitempty"`
	Percentile50 float64 `json:"percentile50_bps,omitempty"`
	Percentile90 float64 `json:"percentile90_bps,omitempty"`
	Percentile99 float64 `json:"percentile99_bps,omitempty"`
	Min          float64 `json:"min_bps,omitempty"`
	Max          float64 `json:"max_bps,omitempty"`
}

func GetOBDInfo(endpoint string) (Latency, Throughput, error) {
	f, err := OpenFileDirectIO(endpoint, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0755)
	if err != nil {
		return Latency{}, Throughput{}, err
	}
	defer f.Close()

	blockSize := 4 * KB
	fileSize := 4 * GB

	latencies := []float64{}
	throughputs := []float64{}

	data := make([]byte, blockSize)
	for i := uint64(0); i < (fileSize / blockSize); i++ {
		startTime := time.Now()
		if n, err := f.Write(data); err != nil {
			return Latency{}, Throughput{}, err
		} else if uint64(n) != blockSize {
			return Latency{}, Throughput{}, errors.New(fmt.Sprintf("Expected to write %d, but only wrote %d", blockSize, n))
		}
		latency := time.Now().Sub(startTime)
		throughput := float64(blockSize) / float64(latency.Seconds())

		latencies = append(latencies, float64(latency.Seconds()))
		throughputs = append(throughputs, throughput)
	}

	var avg_latency float64
	var percentile50_latency float64
	var percentile90_latency float64
	var percentile99_latency float64
	var min_latency float64
	var max_latency float64

	var avg_throughput float64
	var percentile50_throughput float64
	var percentile90_throughput float64
	var percentile99_throughput float64
	var min_throughput float64
	var max_throughput float64

	if avg_latency, err = stats.Mean(latencies); err != nil {
		return Latency{}, Throughput{}, err
	}
	if percentile50_latency, err = stats.Percentile(latencies, 50); err != nil {
		return Latency{}, Throughput{}, err
	}
	if percentile90_latency, err = stats.Percentile(latencies, 90); err != nil {
		return Latency{}, Throughput{}, err
	}
	if percentile99_latency, err = stats.Percentile(latencies, 99); err != nil {
		return Latency{}, Throughput{}, err
	}
	if max_latency, err = stats.Max(latencies); err != nil {
		return Latency{}, Throughput{}, err
	}
	if min_latency, err = stats.Min(latencies); err != nil {
		return Latency{}, Throughput{}, err
	}
	l := Latency{
		Avg:          avg_latency,
		Percentile50: percentile50_latency,
		Percentile90: percentile90_latency,
		Percentile99: percentile99_latency,
		Min:          min_latency,
		Max:          max_latency,
	}

	if avg_throughput, err = stats.Mean(throughputs); err != nil {
		return Latency{}, Throughput{}, err
	}
	if percentile50_throughput, err = stats.Percentile(throughputs, 50); err != nil {
		return Latency{}, Throughput{}, err
	}
	if percentile90_throughput, err = stats.Percentile(throughputs, 90); err != nil {
		return Latency{}, Throughput{}, err
	}
	if percentile99_throughput, err = stats.Percentile(throughputs, 99); err != nil {
		return Latency{}, Throughput{}, err
	}
	if max_throughput, err = stats.Max(throughputs); err != nil {
		return Latency{}, Throughput{}, err
	}
	if min_throughput, err = stats.Min(throughputs); err != nil {
		return Latency{}, Throughput{}, err
	}
	t := Throughput{
		Avg:          avg_throughput,
		Percentile50: percentile50_throughput,
		Percentile90: percentile90_throughput,
		Percentile99: percentile99_throughput,
		Min:          min_throughput,
		Max:          max_throughput,
	}

	return l, t, nil
}
