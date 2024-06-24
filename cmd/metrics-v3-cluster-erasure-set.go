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

const (
	erasureSetOverallWriteQuorum = "overall_write_quorum"
	erasureSetOverallHealth      = "overall_health"
	erasureSetReadQuorum         = "read_quorum"
	erasureSetWriteQuorum        = "write_quorum"
	erasureSetOnlineDrivesCount  = "online_drives_count"
	erasureSetHealingDrivesCount = "healing_drives_count"
	erasureSetHealth             = "health"
	erasureSetReadTolerance      = "read_tolerance"
	erasureSetWriteTolerance     = "write_tolerance"
	erasureSetReadHealth         = "read_health"
	erasureSetWriteHealth        = "write_health"
)

const (
	poolIDL = "pool_id"
	setIDL  = "set_id"
)

var (
	erasureSetOverallWriteQuorumMD = NewGaugeMD(erasureSetOverallWriteQuorum,
		"Overall write quorum across pools and sets")
	erasureSetOverallHealthMD = NewGaugeMD(erasureSetOverallHealth,
		"Overall health across pools and sets (1=healthy, 0=unhealthy)")
	erasureSetReadQuorumMD = NewGaugeMD(erasureSetReadQuorum,
		"Read quorum for the erasure set in a pool", poolIDL, setIDL)
	erasureSetWriteQuorumMD = NewGaugeMD(erasureSetWriteQuorum,
		"Write quorum for the erasure set in a pool", poolIDL, setIDL)
	erasureSetOnlineDrivesCountMD = NewGaugeMD(erasureSetOnlineDrivesCount,
		"Count of online drives in the erasure set in a pool", poolIDL, setIDL)
	erasureSetHealingDrivesCountMD = NewGaugeMD(erasureSetHealingDrivesCount,
		"Count of healing drives in the erasure set in a pool", poolIDL, setIDL)
	erasureSetHealthMD = NewGaugeMD(erasureSetHealth,
		"Health of the erasure set in a pool (1=healthy, 0=unhealthy)",
		poolIDL, setIDL)
	erasureSetReadToleranceMD = NewGaugeMD(erasureSetReadTolerance,
		"No of drive failures that can be tolerated without disrupting read operations",
		poolIDL, setIDL)
	erasureSetWriteToleranceMD = NewGaugeMD(erasureSetWriteTolerance,
		"No of drive failures that can be tolerated without disrupting write operations",
		poolIDL, setIDL)
	erasureSetReadHealthMD = NewGaugeMD(erasureSetReadHealth,
		"Health of the erasure set in a pool for read operations (1=healthy, 0=unhealthy)",
		poolIDL, setIDL)
	erasureSetWriteHealthMD = NewGaugeMD(erasureSetWriteHealth,
		"Health of the erasure set in a pool for write operations (1=healthy, 0=unhealthy)",
		poolIDL, setIDL)
)

func b2f(v bool) float64 {
	if v {
		return 1
	}
	return 0
}

// loadClusterErasureSetMetrics - `MetricsLoaderFn` for cluster storage erasure
// set metrics.
func loadClusterErasureSetMetrics(ctx context.Context, m MetricValues, c *metricsCache) error {
	result, _ := c.esetHealthResult.Get()

	m.Set(erasureSetOverallWriteQuorum, float64(result.WriteQuorum))
	m.Set(erasureSetOverallHealth, b2f(result.Healthy))

	for _, h := range result.ESHealth {
		poolLV := strconv.Itoa(h.PoolID)
		setLV := strconv.Itoa(h.SetID)
		labels := []string{poolIDL, poolLV, setIDL, setLV}
		m.Set(erasureSetReadQuorum, float64(h.ReadQuorum), labels...)
		m.Set(erasureSetWriteQuorum, float64(h.WriteQuorum), labels...)
		m.Set(erasureSetOnlineDrivesCount, float64(h.HealthyDrives), labels...)
		m.Set(erasureSetHealingDrivesCount, float64(h.HealingDrives), labels...)
		m.Set(erasureSetHealth, b2f(h.Healthy), labels...)

		readHealthy := true
		readTolerance := float64(h.HealthyDrives - h.ReadQuorum)
		if readTolerance < 0 {
			readHealthy = false
		}
		m.Set(erasureSetReadTolerance, readTolerance, labels...)
		m.Set(erasureSetReadHealth, b2f(readHealthy), labels...)

		writeHealthy := true
		writeTolerance := float64(h.HealthyDrives + h.HealingDrives - h.WriteQuorum)
		if writeTolerance < 0 {
			writeHealthy = false
		}
		m.Set(erasureSetWriteTolerance, writeTolerance, labels...)
		m.Set(erasureSetWriteHealth, b2f(writeHealthy), labels...)
	}

	return nil
}
