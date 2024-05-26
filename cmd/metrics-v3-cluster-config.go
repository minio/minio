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
	configRRSParity      = "rrs_parity"
	configStandardParity = "standard_parity"
)

var (
	configRRSParityMD = NewGaugeMD(configRRSParity,
		"Reduced redundancy storage class parity")
	configStandardParityMD = NewGaugeMD(configStandardParity,
		"Standard storage class parity")
)

// loadClusterConfigMetrics - `MetricsLoaderFn` for cluster config
// such as standard and RRS parity.
func loadClusterConfigMetrics(ctx context.Context, m MetricValues, c *metricsCache) error {
	clusterDriveMetrics, err := c.clusterDriveMetrics.Get()
	if err != nil {
		metricsLogIf(ctx, err)
	} else {
		m.Set(configStandardParity, float64(clusterDriveMetrics.storageInfo.Backend.StandardSCParity))
		m.Set(configRRSParity, float64(clusterDriveMetrics.storageInfo.Backend.RRSCParity))
	}

	return nil
}
