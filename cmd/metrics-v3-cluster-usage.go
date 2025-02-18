// Copyright (c) 2015-2024 MinIO, Inc.
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
	"time"
)

const (
	usageSinceLastUpdateSeconds   = "since_last_update_seconds"
	usageTotalBytes               = "total_bytes"
	usageObjectsCount             = "count"
	usageVersionsCount            = "versions_count"
	usageDeleteMarkersCount       = "delete_markers_count"
	usageBucketsCount             = "buckets_count"
	usageSizeDistribution         = "size_distribution"
	usageVersionCountDistribution = "version_count_distribution"
)

var (
	usageSinceLastUpdateSecondsMD = NewGaugeMD(usageSinceLastUpdateSeconds,
		"Time since last update of usage metrics in seconds")
	usageTotalBytesMD = NewGaugeMD(usageTotalBytes,
		"Total cluster usage in bytes")
	usageObjectsCountMD = NewGaugeMD(usageObjectsCount,
		"Total cluster objects count")
	usageVersionsCountMD = NewGaugeMD(usageVersionsCount,
		"Total cluster object versions (including delete markers) count")
	usageDeleteMarkersCountMD = NewGaugeMD(usageDeleteMarkersCount,
		"Total cluster delete markers count")
	usageBucketsCountMD = NewGaugeMD(usageBucketsCount,
		"Total cluster buckets count")
	usageObjectsDistributionMD = NewGaugeMD(usageSizeDistribution,
		"Cluster object size distribution", "range")
	usageVersionsDistributionMD = NewGaugeMD(usageVersionCountDistribution,
		"Cluster object version count distribution", "range")
)

// loadClusterUsageObjectMetrics - reads cluster usage metrics.
//
// This is a `MetricsLoaderFn`.
func loadClusterUsageObjectMetrics(ctx context.Context, m MetricValues, c *metricsCache) error {
	dataUsageInfo, err := c.dataUsageInfo.Get()
	if err != nil {
		metricsLogIf(ctx, err)
		return nil
	}

	// data usage has not captured any data yet.
	if dataUsageInfo.LastUpdate.IsZero() {
		return nil
	}

	var (
		clusterSize               uint64
		clusterBuckets            uint64
		clusterObjectsCount       uint64
		clusterVersionsCount      uint64
		clusterDeleteMarkersCount uint64
	)

	clusterObjectSizesHistogram := map[string]uint64{}
	clusterVersionsHistogram := map[string]uint64{}
	for _, usage := range dataUsageInfo.BucketsUsage {
		clusterBuckets++
		clusterSize += usage.Size
		clusterObjectsCount += usage.ObjectsCount
		clusterVersionsCount += usage.VersionsCount
		clusterDeleteMarkersCount += usage.DeleteMarkersCount
		for k, v := range usage.ObjectSizesHistogram {
			clusterObjectSizesHistogram[k] += v
		}
		for k, v := range usage.ObjectVersionsHistogram {
			clusterVersionsHistogram[k] += v
		}
	}

	m.Set(usageSinceLastUpdateSeconds, time.Since(dataUsageInfo.LastUpdate).Seconds())
	m.Set(usageTotalBytes, float64(clusterSize))
	m.Set(usageObjectsCount, float64(clusterObjectsCount))
	m.Set(usageVersionsCount, float64(clusterVersionsCount))
	m.Set(usageDeleteMarkersCount, float64(clusterDeleteMarkersCount))
	m.Set(usageBucketsCount, float64(clusterBuckets))
	for k, v := range clusterObjectSizesHistogram {
		m.Set(usageSizeDistribution, float64(v), "range", k)
	}
	for k, v := range clusterVersionsHistogram {
		m.Set(usageVersionCountDistribution, float64(v), "range", k)
	}

	return nil
}

const (
	usageBucketQuotaTotalBytes = "quota_total_bytes"

	usageBucketTotalBytes                     = "total_bytes"
	usageBucketObjectsCount                   = "objects_count"
	usageBucketVersionsCount                  = "versions_count"
	usageBucketDeleteMarkersCount             = "delete_markers_count"
	usageBucketObjectSizeDistribution         = "object_size_distribution"
	usageBucketObjectVersionCountDistribution = "object_version_count_distribution"
)

var (
	usageBucketTotalBytesMD = NewGaugeMD(usageBucketTotalBytes,
		"Total bucket size in bytes", "bucket")
	usageBucketObjectsTotalMD = NewGaugeMD(usageBucketObjectsCount,
		"Total objects count in bucket", "bucket")
	usageBucketVersionsCountMD = NewGaugeMD(usageBucketVersionsCount,
		"Total object versions (including delete markers) count in bucket", "bucket")
	usageBucketDeleteMarkersCountMD = NewGaugeMD(usageBucketDeleteMarkersCount,
		"Total delete markers count in bucket", "bucket")

	usageBucketQuotaTotalBytesMD = NewGaugeMD(usageBucketQuotaTotalBytes,
		"Total bucket quota in bytes", "bucket")

	usageBucketObjectSizeDistributionMD = NewGaugeMD(usageBucketObjectSizeDistribution,
		"Bucket object size distribution", "range", "bucket")
	usageBucketObjectVersionCountDistributionMD = NewGaugeMD(
		usageBucketObjectVersionCountDistribution,
		"Bucket object version count distribution", "range", "bucket")
)

// loadClusterUsageBucketMetrics - `MetricsLoaderFn` to load bucket usage metrics.
func loadClusterUsageBucketMetrics(ctx context.Context, m MetricValues, c *metricsCache) error {
	dataUsageInfo, err := c.dataUsageInfo.Get()
	if err != nil {
		metricsLogIf(ctx, err)
		return nil
	}

	// data usage has not been captured yet.
	if dataUsageInfo.LastUpdate.IsZero() {
		return nil
	}

	m.Set(usageSinceLastUpdateSeconds, float64(time.Since(dataUsageInfo.LastUpdate)))

	for bucket, usage := range dataUsageInfo.BucketsUsage {
		quota, err := globalBucketQuotaSys.Get(ctx, bucket)
		if err != nil {
			// Log and continue if we are unable to retrieve metrics for this
			// bucket.
			metricsLogIf(ctx, err)
			continue
		}

		m.Set(usageBucketTotalBytes, float64(usage.Size), "bucket", bucket)
		m.Set(usageBucketObjectsCount, float64(usage.ObjectsCount), "bucket", bucket)
		m.Set(usageBucketVersionsCount, float64(usage.VersionsCount), "bucket", bucket)
		m.Set(usageBucketDeleteMarkersCount, float64(usage.DeleteMarkersCount), "bucket", bucket)

		if quota != nil && quota.Quota > 0 {
			m.Set(usageBucketQuotaTotalBytes, float64(quota.Quota), "bucket", bucket)
		}

		for k, v := range usage.ObjectSizesHistogram {
			m.Set(usageBucketObjectSizeDistribution, float64(v), "range", k, "bucket", bucket)
		}
		for k, v := range usage.ObjectVersionsHistogram {
			m.Set(usageBucketObjectVersionCountDistribution, float64(v), "range", k, "bucket", bucket)
		}
	}
	return nil
}
