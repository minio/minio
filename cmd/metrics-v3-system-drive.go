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
	"strconv"
)

// label constants
const (
	driveL      = "drive"
	poolIndexL  = "pool_index"
	setIndexL   = "set_index"
	driveIndexL = "drive_index"

	apiL = "api"
)

var allDriveLabels = []string{driveL, poolIndexL, setIndexL, driveIndexL}

const (
	driveUsedBytes               = "used_bytes"
	driveFreeBytes               = "free_bytes"
	driveTotalBytes              = "total_bytes"
	driveFreeInodes              = "free_inodes"
	driveTimeoutErrorsTotal      = "timeout_errors_total"
	driveAvailabilityErrorsTotal = "availability_errors_total"
	driveWaitingIO               = "waiting_io"
	driveAPILatencyMicros        = "api_latency_micros"

	driveOfflineCount = "offline_count"
	driveOnlineCount  = "online_count"
	driveCount        = "count"
)

var (
	driveUsedBytesMD = NewGaugeMD(driveUsedBytes,
		"Total storage used on a drive in bytes", allDriveLabels...)
	driveFreeBytesMD = NewGaugeMD(driveFreeBytes,
		"Total storage free on a drive in bytes", allDriveLabels...)
	driveTotalBytesMD = NewGaugeMD(driveTotalBytes,
		"Total storage available on a drive in bytes", allDriveLabels...)
	driveFreeInodesMD = NewGaugeMD(driveFreeInodes,
		"Total free inodes on a drive", allDriveLabels...)
	driveTimeoutErrorsMD = NewCounterMD(driveTimeoutErrorsTotal,
		"Total timeout errors on a drive", allDriveLabels...)
	driveAvailabilityErrorsMD = NewCounterMD(driveAvailabilityErrorsTotal,
		"Total availability errors (I/O errors, permission denied and timeouts) on a drive",
		allDriveLabels...)
	driveWaitingIOMD = NewGaugeMD(driveWaitingIO,
		"Total waiting I/O operations on a drive", allDriveLabels...)
	driveAPILatencyMD = NewGaugeMD(driveAPILatencyMicros,
		"Average last minute latency in µs for drive API storage operations",
		append(allDriveLabels, apiL)...)

	driveOfflineCountMD = NewGaugeMD(driveOfflineCount,
		"Count of offline drives")
	driveOnlineCountMD = NewGaugeMD(driveOnlineCount,
		"Count of online drives")
	driveCountMD = NewGaugeMD(driveCount,
		"Count of all drives")
)

// loadDriveMetrics - `MetricsLoaderFn` for node drive metrics.
func loadDriveMetrics(ctx context.Context, m MetricValues, c *metricsCache) error {
	driveMetrics, err := c.driveMetrics.Get()
	if err != nil {
		metricsLogIf(ctx, err)
		return nil
	}

	storageInfo := driveMetrics.storageInfo

	for _, disk := range storageInfo.Disks {
		labels := []string{
			driveL, disk.DrivePath,
			poolIndexL, strconv.Itoa(disk.PoolIndex),
			setIndexL, strconv.Itoa(disk.SetIndex),
			driveIndexL, strconv.Itoa(disk.DiskIndex),
		}

		m.Set(driveUsedBytes, float64(disk.UsedSpace), labels...)
		m.Set(driveFreeBytes, float64(disk.AvailableSpace), labels...)
		m.Set(driveTotalBytes, float64(disk.TotalSpace), labels...)
		m.Set(driveFreeInodes, float64(disk.FreeInodes), labels...)

		if disk.Metrics != nil {
			m.Set(driveTimeoutErrorsTotal, float64(disk.Metrics.TotalErrorsTimeout), labels...)
			m.Set(driveAvailabilityErrorsTotal, float64(disk.Metrics.TotalErrorsAvailability), labels...)
			m.Set(driveWaitingIO, float64(disk.Metrics.TotalWaiting), labels...)

			// Append the api label for the drive API latencies.
			labels = append(labels, "api", "")
			lastIdx := len(labels) - 1
			for apiName, latency := range disk.Metrics.LastMinute {
				labels[lastIdx] = "storage." + apiName
				m.Set(driveAPILatencyMicros, float64(latency.Avg().Microseconds()),
					labels...)
			}
		}
	}

	m.Set(driveOfflineCount, float64(driveMetrics.offlineDrives))
	m.Set(driveOnlineCount, float64(driveMetrics.onlineDrives))
	m.Set(driveCount, float64(driveMetrics.totalDrives))

	return nil
}
