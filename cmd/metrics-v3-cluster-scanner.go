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
	scannerBucketScansFinished = "bucket_scans_finished"
	scannerBucketScansStarted  = "bucket_scans_started"
	scannerDirectoriesScanned  = "directories_scanned"
	scannerObjectsScanned      = "objects_scanned"
	scannerVersionsScanned     = "versions_scanned"
	scannerLastActivitySeconds = "last_activity_seconds"
)

var (
	scannerBucketScansFinishedMD = NewCounterMD(scannerBucketScansFinished,
		"Total number of bucket scans finished since server start")
	scannerBucketScansStartedMD = NewCounterMD(scannerBucketScansStarted,
		"Total number of bucket scans started since server start")
	scannerDirectoriesScannedMD = NewCounterMD(scannerDirectoriesScanned,
		"Total number of directories scanned since server start")
	scannerObjectsScannedMD = NewCounterMD(scannerObjectsScanned,
		"Total number of unique objects scanned since server start")
	scannerVersionsScannedMD = NewCounterMD(scannerVersionsScanned,
		"Total number of object versions scanned since server start")
	scannerLastActivitySecondsMD = NewGaugeMD(scannerLastActivitySeconds,
		"Time elapsed (in seconds) since last scan activity.")
)

// loadClusterScannerMetrics - `MetricsLoaderFn` for cluster webhook
// such as failed objects and directories scanned.
func loadClusterScannerMetrics(ctx context.Context, m MetricValues, c *metricsCache) error {
	m.Set(scannerBucketScansFinished, float64(globalScannerMetrics.lifetime(scannerMetricScanBucketDrive)))
	m.Set(scannerBucketScansStarted, float64(globalScannerMetrics.lifetime(scannerMetricScanBucketDrive)+uint64(globalScannerMetrics.activeDrives())))
	m.Set(scannerDirectoriesScanned, float64(globalScannerMetrics.lifetime(scannerMetricScanFolder)))
	m.Set(scannerObjectsScanned, float64(globalScannerMetrics.lifetime(scannerMetricScanObject)))
	m.Set(scannerVersionsScanned, float64(globalScannerMetrics.lifetime(scannerMetricApplyVersion)))

	dui, err := c.dataUsageInfo.Get()
	if err != nil {
		metricsLogIf(ctx, err)
	} else {
		m.Set(scannerLastActivitySeconds, time.Since(dui.LastUpdate).Seconds())
	}

	return nil
}
