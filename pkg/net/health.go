// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package net

import (
	"github.com/montanaflynn/stats"
)

// Latency holds latency information for read/write operations to the drive
type Latency struct {
	Avg          float64 `json:"avg_secs,omitempty"`
	Percentile50 float64 `json:"percentile50_secs,omitempty"`
	Percentile90 float64 `json:"percentile90_secs,omitempty"`
	Percentile99 float64 `json:"percentile99_secs,omitempty"`
	Min          float64 `json:"min_secs,omitempty"`
	Max          float64 `json:"max_secs,omitempty"`
}

// Throughput holds throughput information for read/write operations to the drive
type Throughput struct {
	Avg          float64 `json:"avg_bytes_per_sec,omitempty"`
	Percentile50 float64 `json:"percentile50_bytes_per_sec,omitempty"`
	Percentile90 float64 `json:"percentile90_bytes_per_sec,omitempty"`
	Percentile99 float64 `json:"percentile99_bytes_per_sec,omitempty"`
	Min          float64 `json:"min_bytes_per_sec,omitempty"`
	Max          float64 `json:"max_bytes_per_sec,omitempty"`
}

// ComputePerfStats takes arrays of Latency & Throughput to compute Statistics
func ComputePerfStats(latencies, throughputs []float64) (Latency, Throughput, error) {
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
	var err error

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

	return l, t, nil
}
