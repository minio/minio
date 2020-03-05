/*
 * MinIO Cloud Storage, (C) 2016-2020 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"context"
	"sort"
	"sync"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/bpool"
	"github.com/minio/minio/pkg/dsync"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/minio/pkg/sync/errgroup"
)

// OfflineDisk represents an unavailable disk.
var OfflineDisk StorageAPI // zero value is nil

// partialUpload is a successful upload of an object
// but not written in all disks (having quorum)
type partialUpload struct {
	bucket    string
	object    string
	failedSet int
}

// erasureObjects - Implements ER object layer.
type erasureObjects struct {
	// getDisks returns list of storageAPIs.
	getDisks func() []StorageAPI

	// getLockers returns list of remote and local lockers.
	getLockers func() []dsync.NetLocker

	// Locker mutex map.
	nsMutex *nsLockMap

	// Byte pools used for temporary i/o buffers.
	bp *bpool.BytePoolCap

	// TODO: ListObjects pool management, should be removed in future.
	listPool *TreeWalkPool

	mrfUploadCh chan partialUpload

	endpoints Endpoints
}

// NewNSLock - initialize a new namespace RWLocker instance.
func (er erasureObjects) NewNSLock(ctx context.Context, bucket string, objects ...string) RWLocker {
	return er.nsMutex.NewNSLock(ctx, er.getLockers, bucket, objects...)
}

// Shutdown function for object storage interface.
func (er erasureObjects) Shutdown(ctx context.Context) error {
	// Add any object layer shutdown activities here.
	closeStorageDisks(er.getDisks())
	return nil
}

// byDiskTotal is a collection satisfying sort.Interface.
type byDiskTotal []DiskInfo

func (d byDiskTotal) Len() int      { return len(d) }
func (d byDiskTotal) Swap(i, j int) { d[i], d[j] = d[j], d[i] }
func (d byDiskTotal) Less(i, j int) bool {
	return d[i].Total < d[j].Total
}

// getDisksInfo - fetch disks info across all other storage API.
func getDisksInfo(disks []StorageAPI, endpoints Endpoints) (disksInfo []DiskInfo, onlineDisks, offlineDisks madmin.BackendDisks) {
	disksInfo = make([]DiskInfo, len(disks))

	g := errgroup.WithNErrs(len(disks))
	for index := range disks {
		index := index
		g.Go(func() error {
			if disks[index] == nil {
				// Storage disk is empty, perhaps ignored disk or not available.
				return errDiskNotFound
			}
			info, err := disks[index].DiskInfo()
			if err != nil {
				if IsErr(err, baseErrs...) {
					return err
				}
				reqInfo := (&logger.ReqInfo{}).AppendTags("disk", disks[index].String())
				ctx := logger.SetReqInfo(context.Background(), reqInfo)
				logger.LogIf(ctx, err)
			}
			disksInfo[index] = info
			return nil
		}, index)
	}

	onlineDisks = make(madmin.BackendDisks)
	offlineDisks = make(madmin.BackendDisks)

	// Wait for the routines.
	for i, diskInfoErr := range g.Wait() {
		peerAddr := endpoints[i].Host
		if _, ok := offlineDisks[peerAddr]; !ok {
			offlineDisks[peerAddr] = 0
		}
		if _, ok := onlineDisks[peerAddr]; !ok {
			onlineDisks[peerAddr] = 0
		}
		if disks[i] == nil || diskInfoErr != nil {
			// Upon error update offline disks.
			offlineDisks[peerAddr]++
			continue
		}
		onlineDisks[peerAddr]++
	}

	// Success.
	return disksInfo, onlineDisks, offlineDisks
}

// Get an aggregated storage info across all disks.
func getStorageInfo(disks []StorageAPI, endpoints Endpoints) StorageInfo {
	disksInfo, onlineDisks, offlineDisks := getDisksInfo(disks, endpoints)

	// Sort so that the first element is the smallest.
	sort.Sort(byDiskTotal(disksInfo))

	// Combine all disks to get total usage
	usedList := make([]uint64, len(disksInfo))
	totalList := make([]uint64, len(disksInfo))
	availableList := make([]uint64, len(disksInfo))
	mountPaths := make([]string, len(disksInfo))

	for i, di := range disksInfo {
		usedList[i] = di.Used
		totalList[i] = di.Total
		availableList[i] = di.Free
		mountPaths[i] = di.MountPath
	}

	storageInfo := StorageInfo{
		Used:       usedList,
		Total:      totalList,
		Available:  availableList,
		MountPaths: mountPaths,
	}

	storageInfo.Backend.Type = BackendErasure
	storageInfo.Backend.OnlineDisks = onlineDisks
	storageInfo.Backend.OfflineDisks = offlineDisks

	return storageInfo
}

// StorageInfo - returns underlying storage statistics.
func (er erasureObjects) StorageInfo(ctx context.Context, local bool) StorageInfo {
	var endpoints = er.endpoints
	var disks []StorageAPI

	if !local {
		disks = er.getDisks()
	} else {
		for i, d := range er.getDisks() {
			if endpoints[i].IsLocal {
				// Append this local disk since local flag is true
				disks = append(disks, d)
			}
		}
	}
	return getStorageInfo(disks, endpoints)
}

// GetMetrics - is not implemented and shouldn't be called.
func (er erasureObjects) GetMetrics(ctx context.Context) (*Metrics, error) {
	logger.LogIf(ctx, NotImplemented{})
	return &Metrics{}, NotImplemented{}
}

// CrawlAndGetDataUsage picks three random disks to crawl and get data usage
func (er erasureObjects) CrawlAndGetDataUsage(ctx context.Context, endCh <-chan struct{}) DataUsageInfo {
	var randomDisks []StorageAPI
	for _, d := range er.getLoadBalancedDisks() {
		if d == nil || !d.IsOnline() {
			continue
		}
		randomDisks = append(randomDisks, d)
		if len(randomDisks) >= 3 {
			break
		}
	}

	var dataUsageResults = make([]DataUsageInfo, len(randomDisks))

	var wg sync.WaitGroup
	for i := 0; i < len(randomDisks); i++ {
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			var err error
			dataUsageResults[index], err = disk.CrawlAndGetDataUsage(endCh)
			if err != nil {
				logger.LogIf(ctx, err)
			}
		}(i, randomDisks[i])
	}
	wg.Wait()

	var dataUsageInfo DataUsageInfo
	for i := 0; i < len(dataUsageResults); i++ {
		if dataUsageResults[i].ObjectsCount > dataUsageInfo.ObjectsCount {
			dataUsageInfo = dataUsageResults[i]
		}
	}

	return dataUsageInfo
}

// IsReady - No Op.
func (er erasureObjects) IsReady(ctx context.Context) bool {
	logger.CriticalIf(ctx, NotImplemented{})
	return true
}
