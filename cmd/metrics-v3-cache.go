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
	clusterDriveMetrics *cachevalue.Cache[storageMetrics]
	nodesUpDown         *cachevalue.Cache[nodesOnline]
}

func newMetricsCache() *metricsCache {
	return &metricsCache{
		dataUsageInfo:       newDataUsageInfoCache(),
		esetHealthResult:    newESetHealthResultCache(),
		driveMetrics:        newDriveMetricsCache(),
		clusterDriveMetrics: newClusterStorageInfoCache(),
		nodesUpDown:         newNodesUpDownCache(),
	}
}

type nodesOnline struct {
	Online, Offline int
}

func newNodesUpDownCache() *cachevalue.Cache[nodesOnline] {
	loadNodesUpDown := func() (v nodesOnline, err error) {
		v.Online, v.Offline = globalNotificationSys.GetPeerOnlineCount()
		return
	}
	return cachevalue.NewFromFunc(1*time.Minute,
		cachevalue.Opts{ReturnLastGood: true},
		loadNodesUpDown)
}

type storageMetrics struct {
	storageInfo                              madmin.StorageInfo
	onlineDrives, offlineDrives, totalDrives int
}

func newDataUsageInfoCache() *cachevalue.Cache[DataUsageInfo] {
	loadDataUsage := func() (u DataUsageInfo, err error) {
		objLayer := newObjectLayerFn()
		if objLayer == nil {
			return
		}

		// Collect cluster level object metrics.
		u, err = loadDataUsageFromBackend(GlobalContext, objLayer)
		return
	}
	return cachevalue.NewFromFunc(1*time.Minute,
		cachevalue.Opts{ReturnLastGood: true},
		loadDataUsage)
}

func newESetHealthResultCache() *cachevalue.Cache[HealthResult] {
	loadHealth := func() (r HealthResult, err error) {
		objLayer := newObjectLayerFn()
		if objLayer == nil {
			return
		}

		r = objLayer.Health(GlobalContext, HealthOptions{})
		return
	}
	return cachevalue.NewFromFunc(1*time.Minute,
		cachevalue.Opts{ReturnLastGood: true},
		loadHealth,
	)
}

func newDriveMetricsCache() *cachevalue.Cache[storageMetrics] {
	loadDriveMetrics := func() (v storageMetrics, err error) {
		objLayer := newObjectLayerFn()
		if objLayer == nil {
			return
		}

		storageInfo := objLayer.LocalStorageInfo(GlobalContext, true)
		onlineDrives, offlineDrives := getOnlineOfflineDisksStats(storageInfo.Disks)
		totalDrives := onlineDrives.Merge(offlineDrives)
		v = storageMetrics{
			storageInfo:   storageInfo,
			onlineDrives:  onlineDrives.Sum(),
			offlineDrives: offlineDrives.Sum(),
			totalDrives:   totalDrives.Sum(),
		}
		return
	}
	return cachevalue.NewFromFunc(1*time.Minute,
		cachevalue.Opts{ReturnLastGood: true},
		loadDriveMetrics)
}

func newClusterStorageInfoCache() *cachevalue.Cache[storageMetrics] {
	loadStorageInfo := func() (v storageMetrics, err error) {
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
		return
	}
	return cachevalue.NewFromFunc(1*time.Minute,
		cachevalue.Opts{ReturnLastGood: true},
		loadStorageInfo,
	)
}
