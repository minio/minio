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
	"sync"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/cachevalue"
)

// metricsCache - cache for metrics.
//
// When serving metrics, this cache is passed to the MetricsLoaderFn.
//
// This cache is used for metrics that would result in network/storage calls.
type metricsCache struct {
	dataUsageInfo       *cachevalue.Cache[DataUsageInfo]
	esetHealthResult    *cachevalue.Cache[HealthResult]
	driveMetrics        *cachevalue.Cache[storageMetrics]
	memoryMetrics       *cachevalue.Cache[madmin.MemInfo]
	cpuMetrics          *cachevalue.Cache[madmin.CPUMetrics]
	clusterDriveMetrics *cachevalue.Cache[storageMetrics]
	nodesUpDown         *cachevalue.Cache[nodesOnline]
}

func newMetricsCache() *metricsCache {
	return &metricsCache{
		dataUsageInfo:       newDataUsageInfoCache(),
		esetHealthResult:    newESetHealthResultCache(),
		driveMetrics:        newDriveMetricsCache(),
		memoryMetrics:       newMemoryMetricsCache(),
		cpuMetrics:          newCPUMetricsCache(),
		clusterDriveMetrics: newClusterStorageInfoCache(),
		nodesUpDown:         newNodesUpDownCache(),
	}
}

type nodesOnline struct {
	Online, Offline int
}

func newNodesUpDownCache() *cachevalue.Cache[nodesOnline] {
	loadNodesUpDown := func(ctx context.Context) (v nodesOnline, err error) {
		v.Online, v.Offline = globalNotificationSys.GetPeerOnlineCount()
		return v, err
	}
	return cachevalue.NewFromFunc(1*time.Minute,
		cachevalue.Opts{ReturnLastGood: true},
		loadNodesUpDown)
}

type driveIOStatMetrics struct {
	readsPerSec    float64
	readsKBPerSec  float64
	readsAwait     float64
	writesPerSec   float64
	writesKBPerSec float64
	writesAwait    float64
	percUtil       float64
}

// storageMetrics - cached storage metrics.
type storageMetrics struct {
	storageInfo                              madmin.StorageInfo
	ioStats                                  map[string]driveIOStatMetrics
	onlineDrives, offlineDrives, totalDrives int
}

func newDataUsageInfoCache() *cachevalue.Cache[DataUsageInfo] {
	loadDataUsage := func(ctx context.Context) (u DataUsageInfo, err error) {
		objLayer := newObjectLayerFn()
		if objLayer == nil {
			return u, err
		}

		// Collect cluster level object metrics.
		u, err = loadDataUsageFromBackend(GlobalContext, objLayer)
		return u, err
	}
	return cachevalue.NewFromFunc(1*time.Minute,
		cachevalue.Opts{ReturnLastGood: true},
		loadDataUsage)
}

func newESetHealthResultCache() *cachevalue.Cache[HealthResult] {
	loadHealth := func(ctx context.Context) (r HealthResult, err error) {
		objLayer := newObjectLayerFn()
		if objLayer == nil {
			return r, err
		}

		r = objLayer.Health(GlobalContext, HealthOptions{})
		return r, err
	}
	return cachevalue.NewFromFunc(1*time.Minute,
		cachevalue.Opts{ReturnLastGood: true},
		loadHealth,
	)
}

func getDiffStats(initialStats, currentStats madmin.DiskIOStats) madmin.DiskIOStats {
	return madmin.DiskIOStats{
		ReadIOs:      currentStats.ReadIOs - initialStats.ReadIOs,
		WriteIOs:     currentStats.WriteIOs - initialStats.WriteIOs,
		ReadSectors:  currentStats.ReadSectors - initialStats.ReadSectors,
		WriteSectors: currentStats.WriteSectors - initialStats.WriteSectors,
		ReadTicks:    currentStats.ReadTicks - initialStats.ReadTicks,
		WriteTicks:   currentStats.WriteTicks - initialStats.WriteTicks,
		TotalTicks:   currentStats.TotalTicks - initialStats.TotalTicks,
	}
}

func getDriveIOStatMetrics(ioStats madmin.DiskIOStats, duration time.Duration) (m driveIOStatMetrics) {
	durationSecs := duration.Seconds()

	m.readsPerSec = float64(ioStats.ReadIOs) / durationSecs
	m.readsKBPerSec = float64(ioStats.ReadSectors) * float64(sectorSize) / kib / durationSecs
	if ioStats.ReadIOs > 0 {
		m.readsAwait = float64(ioStats.ReadTicks) / float64(ioStats.ReadIOs)
	}

	m.writesPerSec = float64(ioStats.WriteIOs) / durationSecs
	m.writesKBPerSec = float64(ioStats.WriteSectors) * float64(sectorSize) / kib / durationSecs
	if ioStats.WriteIOs > 0 {
		m.writesAwait = float64(ioStats.WriteTicks) / float64(ioStats.WriteIOs)
	}

	// TotalTicks is in milliseconds
	m.percUtil = float64(ioStats.TotalTicks) * 100 / (durationSecs * 1000)

	return m
}

func newDriveMetricsCache() *cachevalue.Cache[storageMetrics] {
	var (
		// prevDriveIOStats is used to calculate "per second"
		// values for IOStat related disk metrics e.g. reads/sec.
		prevDriveIOStats            map[string]madmin.DiskIOStats
		prevDriveIOStatsMu          sync.RWMutex
		prevDriveIOStatsRefreshedAt time.Time
	)

	loadDriveMetrics := func(ctx context.Context) (v storageMetrics, err error) {
		objLayer := newObjectLayerFn()
		if objLayer == nil {
			return v, err
		}

		storageInfo := objLayer.LocalStorageInfo(GlobalContext, true)
		onlineDrives, offlineDrives := getOnlineOfflineDisksStats(storageInfo.Disks)
		totalDrives := onlineDrives.Merge(offlineDrives)

		v = storageMetrics{
			storageInfo:   storageInfo,
			onlineDrives:  onlineDrives.Sum(),
			offlineDrives: offlineDrives.Sum(),
			totalDrives:   totalDrives.Sum(),
			ioStats:       map[string]driveIOStatMetrics{},
		}

		currentStats := getCurrentDriveIOStats()
		now := time.Now().UTC()

		prevDriveIOStatsMu.Lock()
		if prevDriveIOStats != nil {
			duration := now.Sub(prevDriveIOStatsRefreshedAt)
			if duration.Seconds() > 1 {
				for d, cs := range currentStats {
					if ps, found := prevDriveIOStats[d]; found {
						v.ioStats[d] = getDriveIOStatMetrics(getDiffStats(ps, cs), duration)
					}
				}
			}
		}

		prevDriveIOStats = currentStats
		prevDriveIOStatsRefreshedAt = now
		prevDriveIOStatsMu.Unlock()

		return v, err
	}

	return cachevalue.NewFromFunc(1*time.Minute,
		cachevalue.Opts{ReturnLastGood: true},
		loadDriveMetrics)
}

func newCPUMetricsCache() *cachevalue.Cache[madmin.CPUMetrics] {
	loadCPUMetrics := func(ctx context.Context) (v madmin.CPUMetrics, err error) {
		types := madmin.MetricsCPU

		m := collectLocalMetrics(types, collectMetricsOpts{
			hosts: map[string]struct{}{
				globalLocalNodeName: {},
			},
		})

		for _, hm := range m.ByHost {
			if hm.CPU != nil {
				v = *hm.CPU
				break
			}
		}

		return v, err
	}

	return cachevalue.NewFromFunc(1*time.Minute,
		cachevalue.Opts{ReturnLastGood: true},
		loadCPUMetrics)
}

func newMemoryMetricsCache() *cachevalue.Cache[madmin.MemInfo] {
	loadMemoryMetrics := func(ctx context.Context) (v madmin.MemInfo, err error) {
		types := madmin.MetricsMem

		m := collectLocalMetrics(types, collectMetricsOpts{
			hosts: map[string]struct{}{
				globalLocalNodeName: {},
			},
		})

		for _, hm := range m.ByHost {
			if hm.Mem != nil && len(hm.Mem.Info.Addr) > 0 {
				v = hm.Mem.Info
				break
			}
		}

		return v, err
	}

	return cachevalue.NewFromFunc(1*time.Minute,
		cachevalue.Opts{ReturnLastGood: true},
		loadMemoryMetrics)
}

func newClusterStorageInfoCache() *cachevalue.Cache[storageMetrics] {
	loadStorageInfo := func(ctx context.Context) (v storageMetrics, err error) {
		objLayer := newObjectLayerFn()
		if objLayer == nil {
			return storageMetrics{}, nil
		}
		storageInfo := objLayer.StorageInfo(GlobalContext, true)
		onlineDrives, offlineDrives := getOnlineOfflineDisksStats(storageInfo.Disks)
		totalDrives := onlineDrives.Merge(offlineDrives)
		v = storageMetrics{
			storageInfo:   storageInfo,
			onlineDrives:  onlineDrives.Sum(),
			offlineDrives: offlineDrives.Sum(),
			totalDrives:   totalDrives.Sum(),
		}
		return v, err
	}
	return cachevalue.NewFromFunc(1*time.Minute,
		cachevalue.Opts{ReturnLastGood: true},
		loadStorageInfo,
	)
}
