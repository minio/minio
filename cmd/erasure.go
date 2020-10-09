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
	"errors"
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

// partialOperation is a successful upload/delete of an object
// but not written in all disks (having quorum)
type partialOperation struct {
	bucket    string
	object    string
	versionID string
	failedSet int
}

// erasureObjects - Implements ER object layer.
type erasureObjects struct {
	GatewayUnsupported

	// getDisks returns list of storageAPIs.
	getDisks func() []StorageAPI

	// getLockers returns list of remote and local lockers.
	getLockers func() ([]dsync.NetLocker, string)

	// getEndpoints returns list of endpoint strings belonging this set.
	// some may be local and some remote.
	getEndpoints func() []string

	// Locker mutex map.
	nsMutex *nsLockMap

	// Byte pools used for temporary i/o buffers.
	bp *bpool.BytePoolCap

	mrfOpCh chan partialOperation
}

// NewNSLock - initialize a new namespace RWLocker instance.
func (er erasureObjects) NewNSLock(ctx context.Context, bucket string, objects ...string) RWLocker {
	return er.nsMutex.NewNSLock(ctx, er.getLockers, bucket, objects...)
}

// SetDriveCount returns the current drives per set.
func (er erasureObjects) SetDriveCount() int {
	return len(er.getDisks())
}

// Shutdown function for object storage interface.
func (er erasureObjects) Shutdown(ctx context.Context) error {
	// Add any object layer shutdown activities here.
	closeStorageDisks(er.getDisks())
	select {
	case _, ok := <-er.mrfOpCh:
		if ok {
			close(er.mrfOpCh)
		}
	default:
		close(er.mrfOpCh)
	}
	return nil
}

// byDiskTotal is a collection satisfying sort.Interface.
type byDiskTotal []madmin.Disk

func (d byDiskTotal) Len() int      { return len(d) }
func (d byDiskTotal) Swap(i, j int) { d[i], d[j] = d[j], d[i] }
func (d byDiskTotal) Less(i, j int) bool {
	return d[i].TotalSpace < d[j].TotalSpace
}

func diskErrToDriveState(err error) (state string) {
	state = madmin.DriveStateUnknown
	switch {
	case errors.Is(err, errDiskNotFound):
		state = madmin.DriveStateOffline
	case errors.Is(err, errCorruptedFormat):
		state = madmin.DriveStateCorrupt
	case errors.Is(err, errUnformattedDisk):
		state = madmin.DriveStateUnformatted
	case errors.Is(err, errDiskAccessDenied):
		state = madmin.DriveStatePermission
	case errors.Is(err, errFaultyDisk):
		state = madmin.DriveStateFaulty
	case err == nil:
		state = madmin.DriveStateOk
	}
	return
}

// getDisksInfo - fetch disks info across all other storage API.
func getDisksInfo(disks []StorageAPI, endpoints []string) (disksInfo []madmin.Disk, errs []error, onlineDisks, offlineDisks madmin.BackendDisks) {
	disksInfo = make([]madmin.Disk, len(disks))
	onlineDisks = make(madmin.BackendDisks)
	offlineDisks = make(madmin.BackendDisks)

	for _, ep := range endpoints {
		if _, ok := offlineDisks[ep]; !ok {
			offlineDisks[ep] = 0
		}
		if _, ok := onlineDisks[ep]; !ok {
			onlineDisks[ep] = 0
		}
	}

	g := errgroup.WithNErrs(len(disks))
	for index := range disks {
		index := index
		g.Go(func() error {
			if disks[index] == OfflineDisk {
				logger.LogIf(GlobalContext, fmt.Errorf("%s: %s", errDiskNotFound, endpoints[index]))
				disksInfo[index] = madmin.Disk{
					State:    diskErrToDriveState(errDiskNotFound),
					Endpoint: endpoints[index],
				}
				// Storage disk is empty, perhaps ignored disk or not available.
				return errDiskNotFound
			}
			info, err := disks[index].DiskInfo(context.TODO())
			di := madmin.Disk{
				Endpoint:       endpoints[index],
				DrivePath:      info.MountPath,
				TotalSpace:     info.Total,
				UsedSpace:      info.Used,
				AvailableSpace: info.Free,
				UUID:           info.ID,
				RootDisk:       info.RootDisk,
				Healing:        info.Healing,
				State:          diskErrToDriveState(err),
			}
			if info.Total > 0 {
				di.Utilization = float64(info.Used / info.Total * 100)
			}
			disksInfo[index] = di
			return err
		}, index)
	}

	errs = g.Wait()
	// Wait for the routines.
	for i, diskInfoErr := range errs {
		ep := disksInfo[i].Endpoint
		if diskInfoErr != nil && !errors.Is(diskInfoErr, errUnformattedDisk) {
			offlineDisks[ep]++
			continue
		}
		onlineDisks[ep]++
	}

	rootDiskCount := 0
	for _, di := range disksInfo {
		if di.RootDisk {
			rootDiskCount++
		}
	}

	if len(disksInfo) == rootDiskCount {
		// Success.
		return disksInfo, errs, onlineDisks, offlineDisks
	}

	// Root disk should be considered offline
	for i := range disksInfo {
		ep := disksInfo[i].Endpoint
		if disksInfo[i].RootDisk {
			offlineDisks[ep]++
			onlineDisks[ep]--
		}
	}

	return disksInfo, errs, onlineDisks, offlineDisks
}

// Get an aggregated storage info across all disks.
func getStorageInfo(disks []StorageAPI, endpoints []string) (StorageInfo, []error) {
	disksInfo, errs, onlineDisks, offlineDisks := getDisksInfo(disks, endpoints)

	// Sort so that the first element is the smallest.
	sort.Sort(byDiskTotal(disksInfo))

	storageInfo := StorageInfo{
		Disks: disksInfo,
	}

	storageInfo.Backend.Type = BackendErasure
	storageInfo.Backend.OnlineDisks = onlineDisks
	storageInfo.Backend.OfflineDisks = offlineDisks

	return storageInfo, errs
}

// StorageInfo - returns underlying storage statistics.
func (er erasureObjects) StorageInfo(ctx context.Context, local bool) (StorageInfo, []error) {
	disks := er.getDisks()
	endpoints := er.getEndpoints()
	if local {
		var localDisks []StorageAPI
		var localEndpoints []string
		for i, disk := range disks {
			if disk != nil {
				if disk.IsLocal() {
					// Append this local disk since local flag is true
					localDisks = append(localDisks, disk)
					localEndpoints = append(localEndpoints, endpoints[i])
				}
			}
		}
		disks = localDisks
		endpoints = localEndpoints
	}
	return getStorageInfo(disks, endpoints)
}

// CrawlAndGetDataUsage will start crawling buckets and send updated totals as they are traversed.
// Updates are sent on a regular basis and the caller *must* consume them.
func (er erasureObjects) crawlAndGetDataUsage(ctx context.Context, buckets []BucketInfo, bf *bloomFilter, updates chan<- dataUsageCache) error {
	if len(buckets) == 0 {
		logger.Info(color.Green("data-crawl:") + " No buckets found, skipping crawl")
		return nil
	}

	// Collect disks we can use.
	disks := er.getLoadBalancedDisks(true)
	if len(disks) == 0 {
		logger.Info(color.Green("data-crawl:") + " all disks are offline or being healed, skipping crawl")
		return nil
	}

	// Load bucket totals
	oldCache := dataUsageCache{}
	if err := oldCache.load(ctx, er, dataUsageCacheName); err != nil {
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

	// Add existing buckets.
	for _, b := range buckets {
		e := oldCache.find(b.Name)
		if e != nil {
			cache.replace(b.Name, dataUsageRoot, *e)
			bucketCh <- b
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
				var err error
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
