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
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/disk"
	"github.com/minio/minio/internal/net"
	c "github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/load"
)

type collectMetricsOpts struct {
	hosts map[string]struct{}
	disks map[string]struct{}
	jobID string
	depID string
}

func collectLocalMetrics(types madmin.MetricType, opts collectMetricsOpts) (m madmin.RealtimeMetrics) {
	if types == madmin.MetricsNone {
		return m
	}

	byHostName := globalMinioAddr
	if len(opts.hosts) > 0 {
		server := getLocalServerProperty(globalEndpoints, &http.Request{
			Host: globalLocalNodeName,
		}, false)
		if _, ok := opts.hosts[server.Endpoint]; ok {
			byHostName = server.Endpoint
		} else {
			return m
		}
	}

	if strings.HasPrefix(byHostName, ":") && !strings.HasPrefix(globalLocalNodeName, ":") {
		byHostName = globalLocalNodeName
	}

	if types.Contains(madmin.MetricsDisk) {
		m.ByDisk = make(map[string]madmin.DiskMetric)
		aggr := madmin.DiskMetric{
			CollectedAt: time.Now(),
		}
		for name, disk := range collectLocalDisksMetrics(opts.disks) {
			m.ByDisk[name] = disk
			aggr.Merge(&disk)
		}
		m.Aggregated.Disk = &aggr
	}

	if types.Contains(madmin.MetricsScanner) {
		metrics := globalScannerMetrics.report()
		m.Aggregated.Scanner = &metrics
	}
	if types.Contains(madmin.MetricsOS) {
		metrics := globalOSMetrics.report()
		m.Aggregated.OS = &metrics
	}
	if types.Contains(madmin.MetricsBatchJobs) {
		m.Aggregated.BatchJobs = globalBatchJobsMetrics.report(opts.jobID)
	}
	if types.Contains(madmin.MetricsSiteResync) {
		m.Aggregated.SiteResync = globalSiteResyncMetrics.report(opts.depID)
	}
	if types.Contains(madmin.MetricNet) {
		m.Aggregated.Net = &madmin.NetMetrics{
			CollectedAt:   UTCNow(),
			InterfaceName: globalInternodeInterface,
		}
		netStats, err := net.GetInterfaceNetStats(globalInternodeInterface)
		if err != nil {
			m.Errors = append(m.Errors, fmt.Sprintf("%s: %v  (nicstats)", byHostName, err.Error()))
		} else {
			m.Aggregated.Net.NetStats = netStats
		}
	}
	if types.Contains(madmin.MetricsMem) {
		m.Aggregated.Mem = &madmin.MemMetrics{
			CollectedAt: UTCNow(),
		}
		m.Aggregated.Mem.Info = madmin.GetMemInfo(GlobalContext, byHostName)
	}
	if types.Contains(madmin.MetricsCPU) {
		m.Aggregated.CPU = &madmin.CPUMetrics{
			CollectedAt: UTCNow(),
		}
		cm, err := c.Times(false)
		if err != nil {
			m.Errors = append(m.Errors, fmt.Sprintf("%s: %v (cpuTimes)", byHostName, err.Error()))
		} else {
			// not collecting per-cpu stats, so there will be only one element
			if len(cm) == 1 {
				m.Aggregated.CPU.TimesStat = &cm[0]
			} else {
				m.Errors = append(m.Errors, fmt.Sprintf("%s: Expected one CPU stat, got %d", byHostName, len(cm)))
			}
		}
		cpuCount, err := c.Counts(true)
		if err != nil {
			m.Errors = append(m.Errors, fmt.Sprintf("%s: %v (cpuCount)", byHostName, err.Error()))
		} else {
			m.Aggregated.CPU.CPUCount = cpuCount
		}

		loadStat, err := load.Avg()
		if err != nil {
			m.Errors = append(m.Errors, fmt.Sprintf("%s: %v (loadStat)", byHostName, err.Error()))
		} else {
			m.Aggregated.CPU.LoadStat = loadStat
		}
	}
	if types.Contains(madmin.MetricsRPC) {
		gr := globalGrid.Load()
		if gr == nil {
			m.Errors = append(m.Errors, fmt.Sprintf("%s: Grid not initialized", byHostName))
		} else {
			stats := gr.ConnStats()
			m.Aggregated.RPC = &stats
		}
	}
	// Add types...

	// ByHost is a shallow reference, so careful about sharing.
	m.ByHost = map[string]madmin.Metrics{byHostName: m.Aggregated}
	m.Hosts = append(m.Hosts, byHostName)

	return m
}

func collectLocalDisksMetrics(disks map[string]struct{}) map[string]madmin.DiskMetric {
	objLayer := newObjectLayerFn()
	if objLayer == nil {
		return nil
	}

	metrics := make(map[string]madmin.DiskMetric)
	storageInfo := objLayer.LocalStorageInfo(GlobalContext, true)
	for _, d := range storageInfo.Disks {
		if len(disks) != 0 {
			_, ok := disks[d.Endpoint]
			if !ok {
				continue
			}
		}

		if d.State != madmin.DriveStateOk && d.State != madmin.DriveStateUnformatted {
			metrics[d.Endpoint] = madmin.DiskMetric{NDisks: 1, Offline: 1}
			continue
		}

		var dm madmin.DiskMetric
		dm.NDisks = 1
		if d.Healing {
			dm.Healing++
		}
		if d.Metrics != nil {
			dm.LifeTimeOps = make(map[string]uint64, len(d.Metrics.APICalls))
			for k, v := range d.Metrics.APICalls {
				if v != 0 {
					dm.LifeTimeOps[k] = v
				}
			}
			dm.LastMinute.Operations = make(map[string]madmin.TimedAction, len(d.Metrics.APICalls))
			for k, v := range d.Metrics.LastMinute {
				if v.Count != 0 {
					dm.LastMinute.Operations[k] = v
				}
			}
		}

		st, err := disk.GetDriveStats(d.Major, d.Minor)
		if err == nil {
			dm.IOStats = madmin.DiskIOStats{
				ReadIOs:        st.ReadIOs,
				ReadMerges:     st.ReadMerges,
				ReadSectors:    st.ReadSectors,
				ReadTicks:      st.ReadTicks,
				WriteIOs:       st.WriteIOs,
				WriteMerges:    st.WriteMerges,
				WriteSectors:   st.WriteSectors,
				WriteTicks:     st.WriteTicks,
				CurrentIOs:     st.CurrentIOs,
				TotalTicks:     st.TotalTicks,
				ReqTicks:       st.ReqTicks,
				DiscardIOs:     st.DiscardIOs,
				DiscardMerges:  st.DiscardMerges,
				DiscardSectors: st.DiscardSectors,
				DiscardTicks:   st.DiscardTicks,
				FlushIOs:       st.FlushIOs,
				FlushTicks:     st.FlushTicks,
			}
		}

		metrics[d.Endpoint] = dm
	}
	return metrics
}

func collectRemoteMetrics(ctx context.Context, types madmin.MetricType, opts collectMetricsOpts) (m madmin.RealtimeMetrics) {
	if !globalIsDistErasure {
		return m
	}
	all := globalNotificationSys.GetMetrics(ctx, types, opts)
	for _, remote := range all {
		m.Merge(&remote)
	}
	return m
}
