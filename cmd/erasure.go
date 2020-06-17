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
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/bpool"
	"github.com/minio/minio/pkg/color"
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
	GatewayUnsupported

	// getDisks returns list of storageAPIs.
	getDisks func() []StorageAPI

	// getLockers returns list of remote and local lockers.
	getLockers func() []dsync.NetLocker

	// getEndpoints returns list of endpoint strings belonging this set.
	// some may be local and some remote.
	getEndpoints func() []string

	// Locker mutex map.
	nsMutex *nsLockMap

	// Byte pools used for temporary i/o buffers.
	bp *bpool.BytePoolCap

	mrfUploadCh chan partialUpload
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
func getDisksInfo(disks []StorageAPI, local bool) (disksInfo []DiskInfo, errs []error, onlineDisks, offlineDisks madmin.BackendDisks) {
	disksInfo = make([]DiskInfo, len(disks))
	onlineDisks = make(madmin.BackendDisks)
	offlineDisks = make(madmin.BackendDisks)

	for _, disk := range disks {
		if disk == OfflineDisk {
			continue
		}
		peerAddr := disk.Hostname()
		if _, ok := offlineDisks[peerAddr]; !ok {
			offlineDisks[peerAddr] = 0
		}
		if _, ok := onlineDisks[peerAddr]; !ok {
			onlineDisks[peerAddr] = 0
		}
	}

	g := errgroup.WithNErrs(len(disks))
	for index := range disks {
		index := index
		g.Go(func() error {
			if disks[index] == OfflineDisk {
				// Storage disk is empty, perhaps ignored disk or not available.
				return errDiskNotFound
			}
			info, err := disks[index].DiskInfo()
			if err != nil {
				if !IsErr(err, baseErrs...) {
					reqInfo := (&logger.ReqInfo{}).AppendTags("disk", disks[index].String())
					ctx := logger.SetReqInfo(GlobalContext, reqInfo)
					logger.LogIf(ctx, err)
				}
				return err
			}
			disksInfo[index] = info
			return nil
		}, index)
	}

	errs = g.Wait()
	// Wait for the routines.
	for i, diskInfoErr := range errs {
		if disks[i] == OfflineDisk {
			continue
		}
		if diskInfoErr != nil {
			offlineDisks[disks[i].Hostname()]++
			continue
		}
		onlineDisks[disks[i].Hostname()]++
	}

	// Iterate over the passed endpoints arguments and check
	// if there are still disks missing from the offline/online lists
	// and update them accordingly.
	missingOfflineDisks := make(map[string]int)
	for _, zone := range globalEndpoints {
		for _, endpoint := range zone.Endpoints {
			// if local is set and endpoint is not local
			// we are not interested in remote disks.
			if local && !endpoint.IsLocal {
				continue
			}

			if _, ok := offlineDisks[endpoint.Host]; !ok {
				missingOfflineDisks[endpoint.Host]++
			}
		}
	}
	for missingDisk, n := range missingOfflineDisks {
		onlineDisks[missingDisk] = 0
		offlineDisks[missingDisk] = n
	}

	// Success.
	return disksInfo, errs, onlineDisks, offlineDisks
}

// Get an aggregated storage info across all disks.
func getStorageInfo(disks []StorageAPI, local bool) (StorageInfo, []error) {
	disksInfo, errs, onlineDisks, offlineDisks := getDisksInfo(disks, local)

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

	return storageInfo, errs
}

// StorageInfo - returns underlying storage statistics.
func (er erasureObjects) StorageInfo(ctx context.Context, local bool) (StorageInfo, []error) {
	disks := er.getDisks()
	if local {
		var localDisks []StorageAPI
		for _, disk := range disks {
			if disk != nil {
				if disk.IsLocal() {
					// Append this local disk since local flag is true
					localDisks = append(localDisks, disk)
				}
			}
		}
		disks = localDisks
	}
	return getStorageInfo(disks, local)
}

// GetMetrics - is not implemented and shouldn't be called.
func (er erasureObjects) GetMetrics(ctx context.Context) (*Metrics, error) {
	logger.LogIf(ctx, NotImplemented{})
	return &Metrics{}, NotImplemented{}
}

// CrawlAndGetDataUsage collects usage from all buckets.
// updates are sent as different parts of the underlying
// structure has been traversed.
func (er erasureObjects) CrawlAndGetDataUsage(ctx context.Context, bf *bloomFilter, updates chan<- DataUsageInfo) error {
	return NotImplemented{API: "CrawlAndGetDataUsage"}
}

// CrawlAndGetDataUsage will start crawling buckets and send updated totals as they are traversed.
// Updates are sent on a regular basis and the caller *must* consume them.
func (er erasureObjects) crawlAndGetDataUsage(ctx context.Context, buckets []BucketInfo, bf *bloomFilter, updates chan<- dataUsageCache) error {
	var disks []StorageAPI

	for _, d := range er.getLoadBalancedDisks() {
		if d == nil || !d.IsOnline() {
			continue
		}
		disks = append(disks, d)
	}
	if len(disks) == 0 || len(buckets) == 0 {
		return nil
	}

	// Load bucket totals
	oldCache := dataUsageCache{}
	err := oldCache.load(ctx, er, dataUsageCacheName)
	if err != nil {
		return err
	}

	// New cache..
	cache := dataUsageCache{
		Info: dataUsageCacheInfo{
			Name:      dataUsageRoot,
			NextCycle: oldCache.Info.NextCycle,
		},
		Cache: make(map[string]dataUsageEntry, len(oldCache.Cache)),
	}
	bloom := bf.bytes()

	// Put all buckets into channel.
	bucketCh := make(chan BucketInfo, len(buckets))
	// Add new buckets first
	for _, b := range buckets {
		if oldCache.find(b.Name) == nil {
			bucketCh <- b
		}
	}

	// Add existing buckets if changes or lifecycles.
	for _, b := range buckets {
		e := oldCache.find(b.Name)
		if e != nil {
			cache.replace(b.Name, dataUsageRoot, *e)
			lc, err := globalLifecycleSys.Get(b.Name)
			activeLC := err == nil && lc.HasActiveRules("", true)
			if activeLC || bf == nil || bf.containsDir(b.Name) {
				bucketCh <- b
			} else {
				if intDataUpdateTracker.debug {
					logger.Info(color.Green("crawlAndGetDataUsage:")+" Skipping bucket %v, not updated", b.Name)
				}
			}
		}
	}

	close(bucketCh)
	bucketResults := make(chan dataUsageEntryInfo, len(disks))

	// Start async collector/saver.
	// This goroutine owns the cache.
	var saverWg sync.WaitGroup
	saverWg.Add(1)
	go func() {
		const updateTime = 30 * time.Second
		t := time.NewTicker(updateTime)
		defer t.Stop()
		defer saverWg.Done()
		var lastSave time.Time

	saveLoop:
		for {
			select {
			case <-ctx.Done():
				// Return without saving.
				return
			case <-t.C:
				if cache.Info.LastUpdate.Equal(lastSave) {
					continue
				}
				logger.LogIf(ctx, cache.save(ctx, er, dataUsageCacheName))
				updates <- cache.clone()
				lastSave = cache.Info.LastUpdate
			case v, ok := <-bucketResults:
				if !ok {
					break saveLoop
				}
				cache.replace(v.Name, v.Parent, v.Entry)
				cache.Info.LastUpdate = time.Now()
			}
		}
		// Save final state...
		cache.Info.NextCycle++
		cache.Info.LastUpdate = time.Now()
		logger.LogIf(ctx, cache.save(ctx, er, dataUsageCacheName))
		updates <- cache
	}()

	// Start one crawler per disk
	var wg sync.WaitGroup
	wg.Add(len(disks))
	for i := range disks {
		go func(i int) {
			defer wg.Done()
			disk := disks[i]

			for bucket := range bucketCh {
				select {
				case <-ctx.Done():
					return
				default:
				}

				// Load cache for bucket
				cacheName := pathJoin(bucket.Name, dataUsageCacheName)
				cache := dataUsageCache{}
				logger.LogIf(ctx, cache.load(ctx, er, cacheName))
				if cache.Info.Name == "" {
					cache.Info.Name = bucket.Name
				}
				cache.Info.BloomFilter = bloom
				if cache.Info.Name != bucket.Name {
					logger.LogIf(ctx, fmt.Errorf("cache name mismatch: %s != %s", cache.Info.Name, bucket.Name))
					cache.Info = dataUsageCacheInfo{
						Name:       bucket.Name,
						LastUpdate: time.Time{},
						NextCycle:  0,
					}
				}

				// Calc usage
				before := cache.Info.LastUpdate
				cache, err = disk.CrawlAndGetDataUsage(ctx, cache)
				cache.Info.BloomFilter = nil
				if err != nil {
					logger.LogIf(ctx, err)
					if cache.Info.LastUpdate.After(before) {
						logger.LogIf(ctx, cache.save(ctx, er, cacheName))
					}
					continue
				}

				var root dataUsageEntry
				if r := cache.root(); r != nil {
					root = cache.flatten(*r)
				}
				bucketResults <- dataUsageEntryInfo{
					Name:   cache.Info.Name,
					Parent: dataUsageRoot,
					Entry:  root,
				}
				// Save cache
				logger.LogIf(ctx, cache.save(ctx, er, cacheName))
			}
		}(i)
	}
	wg.Wait()
	close(bucketResults)
	saverWg.Wait()

	return nil
}

// IsReady - shouldn't be called will panic.
func (er erasureObjects) IsReady(ctx context.Context) bool {
	logger.CriticalIf(ctx, NotImplemented{})
	return true
}
