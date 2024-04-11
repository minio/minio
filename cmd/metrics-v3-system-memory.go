// Copyright (c) 2015-2024 MinIO, Inc.
//
// # This file is part of MinIO Object Storage stack
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
)

const (
	memTotal     = "total"
	memUsed      = "used"
	memFree      = "free"
	memBuffers   = "buffers"
	memCache     = "cache"
	memUsedPerc  = "used_perc"
	memShared    = "shared"
	memAvailable = "available"
)

var (
	memTotalMD     = NewGaugeMD(memTotal, "Total memory on the node")
	memUsedMD      = NewGaugeMD(memUsed, "Used memory on the node")
	memUsedPercMD  = NewGaugeMD(memUsedPerc, "Used memory percentage on the node")
	memFreeMD      = NewGaugeMD(memFree, "Free memory on the node")
	memBuffersMD   = NewGaugeMD(memBuffers, "Buffers memory on the node")
	memCacheMD     = NewGaugeMD(memCache, "Cache memory on the node")
	memSharedMD    = NewGaugeMD(memShared, "Shared memory on the node")
	memAvailableMD = NewGaugeMD(memAvailable, "Available memory on the node")
)

// loadMemoryMetrics - `MetricsLoaderFn` for node memory metrics.
func loadMemoryMetrics(ctx context.Context, m MetricValues, c *metricsCache) error {
	memMetrics, err := c.memoryMetrics.Get()
	if err != nil {
		metricsLogIf(ctx, err)
		return err
	}

	m.Set(memTotal, float64(memMetrics.Total))
	m.Set(memUsed, float64(memMetrics.Used))
	usedPerc := float64(memMetrics.Used) * 100 / float64(memMetrics.Total)
	m.Set(memUsedPerc, usedPerc)
	m.Set(memFree, float64(memMetrics.Free))
	m.Set(memBuffers, float64(memMetrics.Buffers))
	m.Set(memCache, float64(memMetrics.Cache))
	m.Set(memShared, float64(memMetrics.Shared))
	m.Set(memAvailable, float64(memMetrics.Available))

	return nil
}
