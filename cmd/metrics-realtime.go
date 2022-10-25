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
	"time"

	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/disk"
)

type collectMetricsOpts struct {
	hosts map[string]struct{}
	disks map[string]struct{}
	jobID string
}

func collectLocalMetrics(types madmin.MetricType, opts collectMetricsOpts) (m madmin.RealtimeMetrics) {
	if types == madmin.MetricsNone {
		return
	}

	if len(opts.hosts) > 0 {
		if _, ok := opts.hosts[globalMinioAddr]; !ok {
			return
		}
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

	// Add types...

	// ByHost is a shallow reference, so careful about sharing.
	m.ByHost = map[string]madmin.Metrics{globalMinioAddr: m.Aggregated}
	m.Hosts = append(m.Hosts, globalMinioAddr)

	return m
}

func collectLocalDisksMetrics(disks map[string]struct{}) map[string]madmin.DiskMetric {
	objLayer := newObjectLayerFn()
	if objLayer == nil {
		return nil
	}

	metrics := make(map[string]madmin.DiskMetric)

	procStats, procErr := disk.GetAllDrivesIOStats()
	if procErr != nil {
		return metrics
	}

	// only need Disks information in server mode.
	storageInfo, errs := objLayer.LocalStorageInfo(GlobalContext)

	for i, d := range storageInfo.Disks {
		if len(disks) != 0 {
			_, ok := disks[d.Endpoint]
			if !ok {
				continue
			}
		}

		if errs[i] != nil {
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

		// get disk
		if procErr == nil {
			st := procStats[disk.DevID{Major: d.Major, Minor: d.Minor}]
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
		return
	}
	all := globalNotificationSys.GetMetrics(ctx, types, opts)
	for _, remote := range all {
		m.Merge(&remote)
	}
	return m
}
