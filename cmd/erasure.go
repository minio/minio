// Copyright (c) 2015-2021 MinIO, Inc.
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
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/bpool"
	"github.com/minio/minio/internal/color"
	"github.com/minio/minio/internal/dsync"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/sync/errgroup"
	"github.com/minio/pkg/console"
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

	setDriveCount      int
	defaultParityCount int

	setIndex  int
	poolIndex int

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

	// Byte pools used for temporary i/o buffers,
	// legacy objects.
	bpOld *bpool.BytePoolCap

	mrfOpCh chan partialOperation

	deletedCleanupSleeper *dynamicSleeper
}

// NewNSLock - initialize a new namespace RWLocker instance.
func (er erasureObjects) NewNSLock(bucket string, objects ...string) RWLocker {
	return er.nsMutex.NewNSLock(er.getLockers, bucket, objects...)
}

// Shutdown function for object storage interface.
func (er erasureObjects) Shutdown(ctx context.Context) error {
	// Add any object layer shutdown activities here.
	closeStorageDisks(er.getDisks())
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

func getOnlineOfflineDisksStats(disksInfo []madmin.Disk) (onlineDisks, offlineDisks madmin.BackendDisks) {
	onlineDisks = make(madmin.BackendDisks)
	offlineDisks = make(madmin.BackendDisks)

	for _, disk := range disksInfo {
		ep := disk.Endpoint
		if _, ok := offlineDisks[ep]; !ok {
			offlineDisks[ep] = 0
		}
		if _, ok := onlineDisks[ep]; !ok {
			onlineDisks[ep] = 0
		}
	}

	// Wait for the routines.
	for _, disk := range disksInfo {
		ep := disk.Endpoint
		state := disk.State
		if state != madmin.DriveStateOk && state != madmin.DriveStateUnformatted {
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

	// Count offline disks as well to ensure consistent
	// reportability of offline drives on local setups.
	if len(disksInfo) == (rootDiskCount + offlineDisks.Sum()) {
		// Success.
		return onlineDisks, offlineDisks
	}

	// Root disk should be considered offline
	for i := range disksInfo {
		ep := disksInfo[i].Endpoint
		if disksInfo[i].RootDisk {
			offlineDisks[ep]++
			onlineDisks[ep]--
		}
	}

	return onlineDisks, offlineDisks
}

// getDisksInfo - fetch disks info across all other storage API.
func getDisksInfo(disks []StorageAPI, endpoints []string) (disksInfo []madmin.Disk, errs []error) {
	disksInfo = make([]madmin.Disk, len(disks))

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
				Endpoint:       info.Endpoint,
				DrivePath:      info.MountPath,
				TotalSpace:     info.Total,
				UsedSpace:      info.Used,
				AvailableSpace: info.Free,
				UUID:           info.ID,
				RootDisk:       info.RootDisk,
				Healing:        info.Healing,
				State:          diskErrToDriveState(err),
				FreeInodes:     info.FreeInodes,
			}
			di.PoolIndex, di.SetIndex, di.DiskIndex = disks[index].GetDiskLoc()
			if info.Healing {
				if hi := disks[index].Healing(); hi != nil {
					hd := hi.toHealingDisk()
					di.HealInfo = &hd
				}
			}
			di.Metrics = &madmin.DiskMetrics{
				APILatencies: make(map[string]string),
				APICalls:     make(map[string]uint64),
			}
			for k, v := range info.Metrics.APILatencies {
				di.Metrics.APILatencies[k] = v
			}
			for k, v := range info.Metrics.APICalls {
				di.Metrics.APICalls[k] = v
			}
			if info.Total > 0 {
				di.Utilization = float64(info.Used / info.Total * 100)
			}
			disksInfo[index] = di
			return err
		}, index)
	}

	return disksInfo, g.Wait()
}

// Get an aggregated storage info across all disks.
func getStorageInfo(disks []StorageAPI, endpoints []string) (StorageInfo, []error) {
	disksInfo, errs := getDisksInfo(disks, endpoints)

	// Sort so that the first element is the smallest.
	sort.Sort(byDiskTotal(disksInfo))

	storageInfo := StorageInfo{
		Disks: disksInfo,
	}

	storageInfo.Backend.Type = madmin.Erasure
	return storageInfo, errs
}

// StorageInfo - returns underlying storage statistics.
func (er erasureObjects) StorageInfo(ctx context.Context) (StorageInfo, []error) {
	disks := er.getDisks()
	endpoints := er.getEndpoints()
	return getStorageInfo(disks, endpoints)
}

// LocalStorageInfo - returns underlying local storage statistics.
func (er erasureObjects) LocalStorageInfo(ctx context.Context) (StorageInfo, []error) {
	disks := er.getLocalDisks()
	endpoints := make([]string, len(disks))
	for i, disk := range disks {
		if disk != nil {
			endpoints[i] = disk.String()
		}
	}
	return getStorageInfo(disks, endpoints)
}

func (er erasureObjects) getOnlineDisksWithHealing() (newDisks []StorageAPI, healing bool) {
	var wg sync.WaitGroup
	disks := er.getDisks()
	infos := make([]DiskInfo, len(disks))
	for _, i := range hashOrder(UTCNow().String(), len(disks)) {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()

			disk := disks[i-1]

			if disk == nil {
				infos[i-1].Error = "nil disk"
				return
			}

			di, err := disk.DiskInfo(context.Background())
			if err != nil {
				// - Do not consume disks which are not reachable
				//   unformatted or simply not accessible for some reason.
				//
				//
				// - Future: skip busy disks
				infos[i-1].Error = err.Error()
				return
			}

			infos[i-1] = di
		}()
	}
	wg.Wait()

	for i, info := range infos {
		// Check if one of the drives in the set is being healed.
		// this information is used by scanner to skip healing
		// this erasure set while it calculates the usage.
		if info.Healing || info.Error != "" {
			healing = true
			continue
		}
		newDisks = append(newDisks, disks[i])
	}

	return newDisks, healing
}

// Clean-up previously deleted objects. from .minio.sys/tmp/.trash/
func (er erasureObjects) cleanupDeletedObjects(ctx context.Context) {
	// run multiple cleanup's local to this server.
	var wg sync.WaitGroup
	for _, disk := range er.getLoadBalancedLocalDisks() {
		if disk != nil {
			wg.Add(1)
			go func(disk StorageAPI) {
				defer wg.Done()
				diskPath := disk.Endpoint().Path
				readDirFn(pathJoin(diskPath, minioMetaTmpDeletedBucket), func(ddir string, typ os.FileMode) error {
					wait := er.deletedCleanupSleeper.Timer(ctx)
					removeAll(pathJoin(diskPath, minioMetaTmpDeletedBucket, ddir))
					wait()
					return nil
				})
			}(disk)
		}
	}
	wg.Wait()
}

// nsScanner will start scanning buckets and send updated totals as they are traversed.
// Updates are sent on a regular basis and the caller *must* consume them.
func (er erasureObjects) nsScanner(ctx context.Context, buckets []BucketInfo, bf *bloomFilter, updates chan<- dataUsageCache) error {
	if len(buckets) == 0 {
		return nil
	}

	// Collect disks we can use.
	disks, healing := er.getOnlineDisksWithHealing()
	if len(disks) == 0 {
		logger.Info(color.Green("data-scanner:") + " all disks are offline or being healed, skipping scanner")
		return nil
	}

	// Collect disks for healing.
	allDisks := er.getDisks()
	allDiskIDs := make([]string, 0, len(allDisks))
	for _, disk := range allDisks {
		if disk == OfflineDisk {
			// its possible that disk is OfflineDisk
			continue
		}
		id, _ := disk.GetDiskID()
		if id == "" {
			// its possible that disk is unformatted
			// or just went offline
			continue
		}
		allDiskIDs = append(allDiskIDs, id)
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
		// Add jitter to the update time so multiple sets don't sync up.
		var updateTime = 30*time.Second + time.Duration(float64(10*time.Second)*rand.Float64())
		t := time.NewTicker(updateTime)
		defer t.Stop()
		defer saverWg.Done()
		var lastSave time.Time

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
					// Save final state...
					cache.Info.NextCycle++
					cache.Info.LastUpdate = time.Now()
					logger.LogIf(ctx, cache.save(ctx, er, dataUsageCacheName))
					updates <- cache
					return
				}
				cache.replace(v.Name, v.Parent, v.Entry)
				cache.Info.LastUpdate = time.Now()
			}
		}
	}()

	// Shuffle disks to ensure a total randomness of bucket/disk association to ensure
	// that objects that are not present in all disks are accounted and ILM applied.
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.Shuffle(len(disks), func(i, j int) { disks[i], disks[j] = disks[j], disks[i] })

	// Start one scanner per disk
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
				cache.Info.SkipHealing = healing
				cache.Disks = allDiskIDs
				if cache.Info.Name != bucket.Name {
					logger.LogIf(ctx, fmt.Errorf("cache name mismatch: %s != %s", cache.Info.Name, bucket.Name))
					cache.Info = dataUsageCacheInfo{
						Name:       bucket.Name,
						LastUpdate: time.Time{},
						NextCycle:  0,
					}
				}
				// Collect updates.
				updates := make(chan dataUsageEntry, 1)
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					for update := range updates {
						bucketResults <- dataUsageEntryInfo{
							Name:   cache.Info.Name,
							Parent: dataUsageRoot,
							Entry:  update,
						}
						if intDataUpdateTracker.debug {
							console.Debugln("bucket", bucket.Name, "got update", update)
						}
					}
				}()

				// Calc usage
				before := cache.Info.LastUpdate
				var err error
				cache, err = disk.NSScanner(ctx, cache, updates)
				cache.Info.BloomFilter = nil
				if err != nil {
					if !cache.Info.LastUpdate.IsZero() && cache.Info.LastUpdate.After(before) {
						logger.LogIf(ctx, cache.save(ctx, er, cacheName))
					} else {
						logger.LogIf(ctx, err)
					}
					continue
				}

				wg.Wait()
				var root dataUsageEntry
				if r := cache.root(); r != nil {
					root = cache.flatten(*r)
				}
				t := time.Now()
				bucketResults <- dataUsageEntryInfo{
					Name:   cache.Info.Name,
					Parent: dataUsageRoot,
					Entry:  root,
				}
				// We want to avoid synchronizing up all writes in case
				// the results are piled up.
				time.Sleep(time.Duration(float64(time.Since(t)) * rand.Float64()))
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
