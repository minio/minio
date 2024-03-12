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

import "context"

const (
	healthDrivesOfflineCount = "drives_offline_count"
	healthDrivesOnlineCount  = "drives_online_count"
	healthDrivesCount        = "drives_count"
)

var (
	healthDrivesOfflineCountMD = NewGaugeMD(healthDrivesOfflineCount,
		"Count of offline drives in the cluster")
	healthDrivesOnlineCountMD = NewGaugeMD(healthDrivesOnlineCount,
		"Count of online drives in the cluster")
	healthDrivesCountMD = NewGaugeMD(healthDrivesCount,
		"Count of all drives in the cluster")
)

// loadClusterHealthDriveMetrics - `MetricsLoaderFn` for cluster storage drive metrics
// such as online, offline and total drives.
func loadClusterHealthDriveMetrics(ctx context.Context, m MetricValues,
	c *metricsCache,
) error {
	clusterDriveMetrics, _ := c.clusterDriveMetrics.Get()

	m.Set(healthDrivesOfflineCount, float64(clusterDriveMetrics.offlineDrives))
	m.Set(healthDrivesOnlineCount, float64(clusterDriveMetrics.onlineDrives))
	m.Set(healthDrivesCount, float64(clusterDriveMetrics.totalDrives))

	return nil
}

const (
	healthNodesOfflineCount = "nodes_offline_count"
	healthNodesOnlineCount  = "nodes_online_count"
)

var (
	healthNodesOfflineCountMD = NewGaugeMD(healthNodesOfflineCount,
		"Count of offline nodes in the cluster")
	healthNodesOnlineCountMD = NewGaugeMD(healthNodesOnlineCount,
		"Count of online nodes in the cluster")
)

// loadClusterHealthNodeMetrics - `MetricsLoaderFn` for cluster health node
// metrics.
func loadClusterHealthNodeMetrics(ctx context.Context, m MetricValues,
	c *metricsCache,
) error {
	nodesUpDown, _ := c.nodesUpDown.Get()

	m.Set(healthNodesOfflineCount, float64(nodesUpDown.Offline))
	m.Set(healthNodesOnlineCount, float64(nodesUpDown.Online))

	return nil
}

const (
	healthCapacityRawTotalBytes    = "capacity_raw_total_bytes"
	healthCapacityRawFreeBytes     = "capacity_raw_free_bytes"
	healthCapacityUsableTotalBytes = "capacity_usable_total_bytes"
	healthCapacityUsableFreeBytes  = "capacity_usable_free_bytes"
)

var (
	healthCapacityRawTotalBytesMD = NewGaugeMD(healthCapacityRawTotalBytes,
		"Total cluster raw storage capacity in bytes")
	healthCapacityRawFreeBytesMD = NewGaugeMD(healthCapacityRawFreeBytes,
		"Total cluster raw storage free in bytes")
	healthCapacityUsableTotalBytesMD = NewGaugeMD(healthCapacityUsableTotalBytes,
		"Total cluster usable storage capacity in bytes")
	healthCapacityUsableFreeBytesMD = NewGaugeMD(healthCapacityUsableFreeBytes,
		"Total cluster usable storage free in bytes")
)

// loadClusterHealthCapacityMetrics - `MetricsLoaderFn` for cluster storage
// capacity metrics.
func loadClusterHealthCapacityMetrics(ctx context.Context, m MetricValues,
	c *metricsCache,
) error {
	clusterDriveMetrics, _ := c.clusterDriveMetrics.Get()

	storageInfo := clusterDriveMetrics.storageInfo

	m.Set(healthCapacityRawTotalBytes, float64(GetTotalCapacity(storageInfo.Disks)))
	m.Set(healthCapacityRawFreeBytes, float64(GetTotalCapacityFree(storageInfo.Disks)))
	m.Set(healthCapacityUsableTotalBytes, float64(GetTotalUsableCapacity(storageInfo.Disks, storageInfo)))
	m.Set(healthCapacityUsableFreeBytes, float64(GetTotalUsableCapacityFree(storageInfo.Disks, storageInfo)))

	return nil
}
