/*
 * MinIO Cloud Storage, (C) 2016, 2017, 2018 MinIO, Inc.
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
	"strings"
	"sync"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/bpool"
	"github.com/minio/minio/pkg/dsync"
	"github.com/minio/minio/pkg/madmin"
	xnet "github.com/minio/minio/pkg/net"
	"github.com/minio/minio/pkg/sync/errgroup"
)

// XL constants.
const (
	// XL metadata file carries per object metadata.
	xlMetaJSONFile = "xl.json"
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

// xlObjects - Implements XL object layer.
type xlObjects struct {
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
}

// NewNSLock - initialize a new namespace RWLocker instance.
func (xl xlObjects) NewNSLock(ctx context.Context, bucket string, object string) RWLocker {
	return xl.nsMutex.NewNSLock(ctx, xl.getLockers, bucket, object)
}

// Shutdown function for object storage interface.
func (xl xlObjects) Shutdown(ctx context.Context) error {
	// Add any object layer shutdown activities here.
	closeStorageDisks(xl.getDisks())
	closeLockers(xl.getLockers())
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
func getDisksInfo(disks []StorageAPI) (disksInfo []DiskInfo, onlineDisks, offlineDisks madmin.BackendDisks) {
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

	getPeerAddress := func(diskPath string) (string, error) {
		hostPort := strings.Split(diskPath, SlashSeparator)[0]
		// Host will be empty for xl/fs disk paths.
		if hostPort == "" {
			return "", nil
		}
		thisAddr, err := xnet.ParseHost(hostPort)
		if err != nil {
			return "", err
		}
		return thisAddr.String(), nil
	}

	onlineDisks = make(madmin.BackendDisks)
	offlineDisks = make(madmin.BackendDisks)
	// Wait for the routines.
	for i, err := range g.Wait() {
		peerAddr, pErr := getPeerAddress(disksInfo[i].RelativePath)
		if pErr != nil {
			continue
		}
		if _, ok := offlineDisks[peerAddr]; !ok {
			offlineDisks[peerAddr] = 0
		}
		if _, ok := onlineDisks[peerAddr]; !ok {
			onlineDisks[peerAddr] = 0
		}
		if err != nil {
			offlineDisks[peerAddr]++
			continue
		}
		onlineDisks[peerAddr]++
	}

	// Success.
	return disksInfo, onlineDisks, offlineDisks
}

// Get an aggregated storage info across all disks.
func getStorageInfo(disks []StorageAPI) StorageInfo {
	disksInfo, onlineDisks, offlineDisks := getDisksInfo(disks)

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
		mountPaths[i] = di.RelativePath
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
func (xl xlObjects) StorageInfo(ctx context.Context) StorageInfo {
	return getStorageInfo(xl.getDisks())
}

// GetMetrics - is not implemented and shouldn't be called.
func (xl xlObjects) GetMetrics(ctx context.Context) (*Metrics, error) {
	logger.LogIf(ctx, NotImplemented{})
	return &Metrics{}, NotImplemented{}
}

// CrawlAndGetDataUsage picks three random disks to crawl and get data usage
func (xl xlObjects) CrawlAndGetDataUsage(ctx context.Context, endCh <-chan struct{}) DataUsageInfo {
	var randomDisks []StorageAPI
	for _, d := range xl.getLoadBalancedDisks() {
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

	var dataUsageInfo = dataUsageResults[0]
	// Pick the crawling result of the disk which has the most
	// number of objects in it.
	for i := 1; i < len(dataUsageResults); i++ {
		if dataUsageResults[i].ObjectsCount > dataUsageInfo.ObjectsCount {
			dataUsageInfo = dataUsageResults[i]
		}
	}

	return dataUsageInfo
}

// IsReady - No Op.
func (xl xlObjects) IsReady(ctx context.Context) bool {
	logger.CriticalIf(ctx, NotImplemented{})
	return true
}
