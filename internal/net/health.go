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
	"github.com/minio/madmin-go"
	"github.com/montanaflynn/stats"
)

// ComputePerfStats takes arrays of Latency & Throughput to compute Statistics
func ComputePerfStats(latencies, throughputs []float64) (madmin.NetLatency, madmin.NetThroughput, error) {
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
		return madmin.NetLatency{}, madmin.NetThroughput{}, err
	}
	if percentile50Latency, err = stats.Percentile(latencies, 50); err != nil {
		return madmin.NetLatency{}, madmin.NetThroughput{}, err
	}
	if percentile90Latency, err = stats.Percentile(latencies, 90); err != nil {
		return madmin.NetLatency{}, madmin.NetThroughput{}, err
	}
	if percentile99Latency, err = stats.Percentile(latencies, 99); err != nil {
		return madmin.NetLatency{}, madmin.NetThroughput{}, err
	}
	if maxLatency, err = stats.Max(latencies); err != nil {
		return madmin.NetLatency{}, madmin.NetThroughput{}, err
	}
	if minLatency, err = stats.Min(latencies); err != nil {
		return madmin.NetLatency{}, madmin.NetThroughput{}, err
	}
	l := madmin.NetLatency{
		Avg:          avgLatency,
		Percentile50: percentile50Latency,
		Percentile90: percentile90Latency,
		Percentile99: percentile99Latency,
		Min:          minLatency,
		Max:          maxLatency,
	}

	if avgThroughput, err = stats.Mean(throughputs); err != nil {
		return madmin.NetLatency{}, madmin.NetThroughput{}, err
	}
	if percentile50Throughput, err = stats.Percentile(throughputs, 50); err != nil {
		return madmin.NetLatency{}, madmin.NetThroughput{}, err
	}
	if percentile90Throughput, err = stats.Percentile(throughputs, 90); err != nil {
		return madmin.NetLatency{}, madmin.NetThroughput{}, err
	}
	if percentile99Throughput, err = stats.Percentile(throughputs, 99); err != nil {
		return madmin.NetLatency{}, madmin.NetThroughput{}, err
	}
	if maxThroughput, err = stats.Max(throughputs); err != nil {
		return madmin.NetLatency{}, madmin.NetThroughput{}, err
	}
	if minThroughput, err = stats.Min(throughputs); err != nil {
		return madmin.NetLatency{}, madmin.NetThroughput{}, err
	}
	t := madmin.NetThroughput{
		Avg:          avgThroughput,
		Percentile50: percentile50Throughput,
		Percentile90: percentile90Throughput,
		Percentile99: percentile99Throughput,
		Min:          minThroughput,
		Max:          maxThroughput,
	}

	return l, t, nil
}
