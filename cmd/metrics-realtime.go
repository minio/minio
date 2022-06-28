// Copyright (c) 2015-2022 MinIO, Inc.
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

	"github.com/minio/madmin-go"
)

func collectLocalMetrics(types madmin.MetricType, hosts map[string]struct{}) (m madmin.RealtimeMetrics) {
	if types == madmin.MetricsNone {
		return
	}
	if len(hosts) > 0 {
		if _, ok := hosts[globalMinioAddr]; !ok {
			return
		}
	}
	if types.Contains(madmin.MetricsScanner) {
		metrics := globalScannerMetrics.report()
		m.Aggregated.Scanner = &metrics
	}
	// Add types...

	// ByHost is a shallow reference, so careful about sharing.
	m.ByHost = map[string]madmin.Metrics{globalMinioAddr: m.Aggregated}
	m.Hosts = append(m.Hosts, globalMinioAddr)

	return m
}

func collectRemoteMetrics(ctx context.Context, types madmin.MetricType, hosts map[string]struct{}) (m madmin.RealtimeMetrics) {
	if !globalIsDistErasure {
		return
	}
	all := globalNotificationSys.GetMetrics(ctx, types, hosts)
	for _, remote := range all {
		m.Merge(&remote)
	}
	return m
}
