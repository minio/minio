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

	"github.com/minio/madmin-go/v3"
)

// label constants
const (
	driveL      = "drive"
	poolIndexL  = "pool_index"
	setIndexL   = "set_index"
	driveIndexL = "drive_index"

	apiL = "api"

	sectorSize = uint64(512)
	kib        = float64(1 << 10)

	driveHealthOffline = float64(0)
	driveHealthOnline  = float64(1)
	driveHealthHealing = float64(2)
)

var allDriveLabels = []string{driveL, poolIndexL, setIndexL, driveIndexL}

const (
	driveUsedBytes               = "used_bytes"
	driveFreeBytes               = "free_bytes"
	driveTotalBytes              = "total_bytes"
	driveUsedInodes              = "used_inodes"
	driveFreeInodes              = "free_inodes"
	driveTotalInodes             = "total_inodes"
	driveTimeoutErrorsTotal      = "timeout_errors_total"
	driveIOErrorsTotal           = "io_errors_total"
	driveAvailabilityErrorsTotal = "availability_errors_total"
	driveWaitingIO               = "waiting_io"
	driveAPILatencyMicros        = "api_latency_micros"
	driveHealth                  = "health"

	driveOfflineCount = "offline_count"
	driveOnlineCount  = "online_count"
	driveCount        = "count"

	// iostat related
	driveReadsPerSec    = "reads_per_sec"
	driveReadsKBPerSec  = "reads_kb_per_sec"
	driveReadsAwait     = "reads_await"
	driveWritesPerSec   = "writes_per_sec"
	driveWritesKBPerSec = "writes_kb_per_sec"
	driveWritesAwait    = "writes_await"
	drivePercUtil       = "perc_util"
)

var (
	driveUsedBytesMD = NewGaugeMD(driveUsedBytes,
		"Total storage used on a drive in bytes", allDriveLabels...)
	driveFreeBytesMD = NewGaugeMD(driveFreeBytes,
		"Total storage free on a drive in bytes", allDriveLabels...)
	driveTotalBytesMD = NewGaugeMD(driveTotalBytes,
		"Total storage available on a drive in bytes", allDriveLabels...)
	driveUsedInodesMD = NewGaugeMD(driveUsedInodes,
		"Total used inodes on a drive", allDriveLabels...)
	driveFreeInodesMD = NewGaugeMD(driveFreeInodes,
		"Total free inodes on a drive", allDriveLabels...)
	driveTotalInodesMD = NewGaugeMD(driveTotalInodes,
		"Total inodes available on a drive", allDriveLabels...)
	driveTimeoutErrorsMD = NewCounterMD(driveTimeoutErrorsTotal,
		"Total timeout errors on a drive", allDriveLabels...)
	driveIOErrorsMD = NewCounterMD(driveIOErrorsTotal,
		"Total I/O errors on a drive", allDriveLabels...)
	driveAvailabilityErrorsMD = NewCounterMD(driveAvailabilityErrorsTotal,
		"Total availability errors (I/O errors, timeouts) on a drive",
		allDriveLabels...)
	driveWaitingIOMD = NewGaugeMD(driveWaitingIO,
		"Total waiting I/O operations on a drive", allDriveLabels...)
	driveAPILatencyMD = NewGaugeMD(driveAPILatencyMicros,
		"Average last minute latency in µs for drive API storage operations",
		append(allDriveLabels, apiL)...)
	driveHealthMD = NewGaugeMD(driveHealth,
		"Drive health (0 = offline, 1 = healthy, 2 = healing)", allDriveLabels...)

	driveOfflineCountMD = NewGaugeMD(driveOfflineCount,
		"Count of offline drives")
	driveOnlineCountMD = NewGaugeMD(driveOnlineCount,
		"Count of online drives")
	driveCountMD = NewGaugeMD(driveCount,
		"Count of all drives")

	// iostat related
	driveReadsPerSecMD = NewGaugeMD(driveReadsPerSec,
		"Reads per second on a drive",
		allDriveLabels...)
	driveReadsKBPerSecMD = NewGaugeMD(driveReadsKBPerSec,
		"Kilobytes read per second on a drive",
		allDriveLabels...)
	driveReadsAwaitMD = NewGaugeMD(driveReadsAwait,
		"Average time for read requests served on a drive",
		allDriveLabels...)
	driveWritesPerSecMD = NewGaugeMD(driveWritesPerSec,
		"Writes per second on a drive",
		allDriveLabels...)
	driveWritesKBPerSecMD = NewGaugeMD(driveWritesKBPerSec,
		"Kilobytes written per second on a drive",
		allDriveLabels...)
	driveWritesAwaitMD = NewGaugeMD(driveWritesAwait,
		"Average time for write requests served on a drive",
		allDriveLabels...)
	drivePercUtilMD = NewGaugeMD(drivePercUtil,
		"Percentage of time the disk was busy",
		allDriveLabels...)
)

func getCurrentDriveIOStats() map[string]madmin.DiskIOStats {
	types := madmin.MetricsDisk
	driveRealtimeMetrics := collectLocalMetrics(types, collectMetricsOpts{
		hosts: map[string]struct{}{
			globalLocalNodeName: {},
		},
	})

	stats := map[string]madmin.DiskIOStats{}
	for d, m := range driveRealtimeMetrics.ByDisk {
		stats[d] = m.IOStats
	}
	return stats
}

func (m *MetricValues) setDriveBasicMetrics(drive madmin.Disk, labels []string) {
	m.Set(driveUsedBytes, float64(drive.UsedSpace), labels...)
	m.Set(driveFreeBytes, float64(drive.AvailableSpace), labels...)
	m.Set(driveTotalBytes, float64(drive.TotalSpace), labels...)
	m.Set(driveUsedInodes, float64(drive.UsedInodes), labels...)
	m.Set(driveFreeInodes, float64(drive.FreeInodes), labels...)
	m.Set(driveTotalInodes, float64(drive.UsedInodes+drive.FreeInodes), labels...)

	var health float64
	switch drive.Healing {
	case true:
		health = driveHealthHealing
	case false:
		if drive.State == "ok" {
			health = driveHealthOnline
		} else {
			health = driveHealthOffline
		}
	}
	m.Set(driveHealth, health, labels...)
}

func (m *MetricValues) setDriveAPIMetrics(disk madmin.Disk, labels []string) {
	if disk.Metrics == nil {
		return
	}

	m.Set(driveTimeoutErrorsTotal, float64(disk.Metrics.TotalErrorsTimeout), labels...)
	m.Set(driveIOErrorsTotal, float64(disk.Metrics.TotalErrorsAvailability-disk.Metrics.TotalErrorsTimeout), labels...)
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

func (m *MetricValues) setDriveIOStatMetrics(ioStats driveIOStatMetrics, labels []string) {
	m.Set(driveReadsPerSec, ioStats.readsPerSec, labels...)
	m.Set(driveReadsKBPerSec, ioStats.readsKBPerSec, labels...)
	if ioStats.readsPerSec > 0 {
		m.Set(driveReadsAwait, ioStats.readsAwait, labels...)
	}

	m.Set(driveWritesPerSec, ioStats.writesPerSec, labels...)
	m.Set(driveWritesKBPerSec, ioStats.writesKBPerSec, labels...)
	if ioStats.writesPerSec > 0 {
		m.Set(driveWritesAwait, ioStats.writesAwait, labels...)
	}

	m.Set(drivePercUtil, ioStats.percUtil, labels...)
}

// loadDriveMetrics - `MetricsLoaderFn` for node drive metrics.
func loadDriveMetrics(ctx context.Context, m MetricValues, c *metricsCache) error {
	driveMetrics, err := c.driveMetrics.Get()
	if err != nil {
		metricsLogIf(ctx, err)
		return nil
	}

	for _, disk := range driveMetrics.storageInfo.Disks {
		labels := []string{
			driveL, disk.DrivePath,
			poolIndexL, strconv.Itoa(disk.PoolIndex),
			setIndexL, strconv.Itoa(disk.SetIndex),
			driveIndexL, strconv.Itoa(disk.DiskIndex),
		}

		m.setDriveBasicMetrics(disk, labels)
		if dm, found := driveMetrics.ioStats[disk.DrivePath]; found {
			m.setDriveIOStatMetrics(dm, labels)
		}
		m.setDriveAPIMetrics(disk, labels)
	}

	m.Set(driveOfflineCount, float64(driveMetrics.offlineDrives))
	m.Set(driveOnlineCount, float64(driveMetrics.onlineDrives))
	m.Set(driveCount, float64(driveMetrics.totalDrives))

	return nil
}
