// Copyright (c) 2024 MinIO, Inc.
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
	sizeDistribution    = "size_distribution"
	versionDistribution = "version_distribution"
	usageObjectTotal    = "usage_object_total"
)

var (
	objectSizeDistributionMD    = NewCounterMD(sizeDistribution, "Distribution of object sizes across a cluster")
	objectVersionDistributionMD = NewCounterMD(versionDistribution, "Distribution of object versions across a cluster")
	objectUsageObjectTotalMD    = NewCounterMD(usageObjectTotal, "Total number of objects in a cluster")
)

// loadClusterObjectMetrics - `MetricsLoaderFn` for cluster object metrics.
func loadClusterObjectMetrics(ctx context.Context, m MetricValues, _ *metricsCache) error {
	objLayer := newObjectLayerFn()
	if objLayer == nil {
		return nil
	}

	dataUsageInfo, err := loadDataUsageFromBackend(ctx, objLayer)
	if err != nil {
		metricsLogIf(ctx, err)
		return nil
	}

	// data usage has not captured any data yet.
	if dataUsageInfo.LastUpdate.IsZero() {
		return nil
	}

	clusterObjectSizesHistogram := map[string]uint64{}
	clusterVersionsHistogram := map[string]uint64{}
	clusterObjectsCount := uint64(0)
	for _, usage := range dataUsageInfo.BucketsUsage {
		clusterObjectsCount += usage.ObjectsCount
		for k, v := range usage.ObjectSizesHistogram {
			clusterObjectSizesHistogram[k] += v
		}
		for k, v := range usage.ObjectVersionsHistogram {
			clusterVersionsHistogram[k] += v
		}
	}

	for k, v := range clusterObjectSizesHistogram {
		m.Set(sizeDistribution, float64(v), "range", k)
	}
	for k, v := range clusterVersionsHistogram {
		m.Set(usageObjectTotal, float64(v), "range", k)
	}
	m.Set(usageObjectTotal, float64(clusterObjectsCount))

	return nil
}
