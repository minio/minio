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

package cmd

import (
	"context"
	"math"
	"os"
	"sync"

	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/disk"
)

// round returns value rounding to specified decimal places.
func round(f float64, n int) float64 {
	if n <= 0 {
		return math.Round(f)
	}

	p := math.Pow10(n)
	return math.Round(f*p) / p
}

func getDrivePerfInfo(ctx context.Context, parallel bool) []madmin.DrivePerfInfo {
	pools := globalEndpoints
	info := []madmin.DrivePerfInfo{}
	var wg sync.WaitGroup
	for _, pool := range pools {
		for _, endpoint := range pool.Endpoints {
			if !endpoint.IsLocal {
				continue
			}

			if _, err := os.Stat(endpoint.Path); err != nil {
				info = append(info, madmin.DrivePerfInfo{
					Path:  endpoint.Path,
					Error: err.Error(),
				})
				continue
			}

			getHealthInfo := func(path string) {
				defer wg.Done()

				latency, throughput, err := disk.GetHealthInfo(
					ctx, path, pathJoin(path, minioMetaTmpBucket, mustGetUUID()),
				)
				if err != nil {
					info = append(info, madmin.DrivePerfInfo{
						Path:  path,
						Error: err.Error(),
					})
				} else {
					info = append(info, madmin.DrivePerfInfo{
						Path: path,
						Latency: madmin.Latency{
							Avg:          round(latency.Avg, 3),
							Max:          round(latency.Max, 3),
							Min:          round(latency.Min, 3),
							Percentile50: round(latency.Percentile50, 3),
							Percentile90: round(latency.Percentile90, 3),
							Percentile99: round(latency.Percentile99, 3),
						},
						Throughput: madmin.Throughput{
							Avg:          uint64(round(throughput.Avg, 0)),
							Max:          uint64(round(throughput.Max, 0)),
							Min:          uint64(round(throughput.Min, 0)),
							Percentile50: uint64(round(throughput.Percentile50, 0)),
							Percentile90: uint64(round(throughput.Percentile90, 0)),
							Percentile99: uint64(round(throughput.Percentile99, 0)),
						},
					})
				}
			}

			wg.Add(1)
			if parallel {
				go getHealthInfo(endpoint.Path)
			} else {
				getHealthInfo(endpoint.Path)
			}
		}
	}

	wg.Wait()
	return info
}

func getDrivePerfInfos(ctx context.Context, addr string) madmin.DrivePerfInfos {
	serialPerf := getDrivePerfInfo(ctx, false)
	parallelPerf := getDrivePerfInfo(ctx, true)
	return madmin.DrivePerfInfos{
		Addr:         addr,
		SerialPerf:   serialPerf,
		ParallelPerf: parallelPerf,
	}
}
