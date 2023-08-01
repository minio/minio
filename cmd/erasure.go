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
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/dsync"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/pkg/v3/sync/errgroup"
)

// list all errors that can be ignore in a bucket operation.
var bucketOpIgnoredErrs = append(baseIgnoredErrs, errDiskAccessDenied, errUnformattedDisk)

// list all errors that can be ignored in a bucket metadata operation.
var bucketMetadataOpIgnoredErrs = append(bucketOpIgnoredErrs, errVolumeNotFound)

// OfflineDisk represents an unavailable disk.
var OfflineDisk StorageAPI // zero value is nil

// erasureObjects - Implements ER object layer.
type erasureObjects struct {
	setDriveCount      int
	defaultParityCount int

	setIndex  int
	poolIndex int

	// getDisks returns list of storageAPIs.
	getDisks func() []StorageAPI

	// getLockers returns list of remote and local lockers.
	getLockers func() ([]dsync.NetLocker, string)

	// getEndpoints returns list of endpoint belonging this set.
	// some may be local and some remote.
	getEndpoints func() []Endpoint

	// getEndpoints returns list of endpoint strings belonging this set.
	// some may be local and some remote.
	getEndpointStrings func() []string

	// Locker mutex map.
	nsMutex *nsLockMap
}

// NewNSLock - initialize a new namespace RWLocker instance.
func (er erasureObjects) NewNSLock(bucket string, objects ...string) RWLocker {
	return er.nsMutex.NewNSLock(er.getLockers, bucket, objects...)
}

// Shutdown function for object storage interface.
func (er erasureObjects) Shutdown(ctx context.Context) error {
	// Add any object layer shutdown activities here.
	closeStorageDisks(er.getDisks()...)
	return nil
}

// defaultWQuorum write quorum based on setDriveCount and defaultParityCount
func (er erasureObjects) defaultWQuorum() int {
	dataCount := er.setDriveCount - er.defaultParityCount
	if dataCount == er.defaultParityCount {
		return dataCount + 1
	}
	return dataCount
}

func diskErrToDriveState(err error) (state string) {
	switch {
	case errors.Is(err, errDiskNotFound) || errors.Is(err, context.DeadlineExceeded):
		state = madmin.DriveStateOffline
	case errors.Is(err, errCorruptedFormat) || errors.Is(err, errCorruptedBackend):
		state = madmin.DriveStateCorrupt
	case errors.Is(err, errUnformattedDisk):
		state = madmin.DriveStateUnformatted
	case errors.Is(err, errDiskAccessDenied):
		state = madmin.DriveStatePermission
	case errors.Is(err, errFaultyDisk):
		state = madmin.DriveStateFaulty
	case errors.Is(err, errDriveIsRoot):
		state = madmin.DriveStateRootMount
	case err == nil:
		state = madmin.DriveStateOk
	default:
		state = fmt.Sprintf("%s (cause: %s)", madmin.DriveStateUnknown, err)
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
func getDisksInfo(disks []StorageAPI, endpoints []Endpoint, metrics bool) (disksInfo []madmin.Disk) {
	disksInfo = make([]madmin.Disk, len(disks))

	g := errgroup.WithNErrs(len(disks))
	for index := range disks {
		index := index
		g.Go(func() error {
			di := madmin.Disk{
				Endpoint:  endpoints[index].String(),
				PoolIndex: endpoints[index].PoolIdx,
				SetIndex:  endpoints[index].SetIdx,
				DiskIndex: endpoints[index].DiskIdx,
				Local:     endpoints[index].IsLocal,
			}
			if disks[index] == OfflineDisk {
				di.State = diskErrToDriveState(errDiskNotFound)
				disksInfo[index] = di
				return nil
			}
			info, err := disks[index].DiskInfo(context.TODO(), DiskInfoOptions{Metrics: metrics})
			di.DrivePath = info.MountPath
			di.TotalSpace = info.Total
			di.UsedSpace = info.Used
			di.AvailableSpace = info.Free
			di.UUID = info.ID
			di.Major = info.Major
			di.Minor = info.Minor
			di.RootDisk = info.RootDisk
			di.Healing = info.Healing
			di.Scanning = info.Scanning
			di.State = diskErrToDriveState(err)
			di.FreeInodes = info.FreeInodes
			di.UsedInodes = info.UsedInodes
			if info.Healing {
				if hi := disks[index].Healing(); hi != nil {
					hd := hi.toHealingDisk()
					di.HealInfo = &hd
				}
			}
			di.Metrics = &madmin.DiskMetrics{
				LastMinute:              make(map[string]madmin.TimedAction, len(info.Metrics.LastMinute)),
				APICalls:                make(map[string]uint64, len(info.Metrics.APICalls)),
				TotalErrorsAvailability: info.Metrics.TotalErrorsAvailability,
				TotalErrorsTimeout:      info.Metrics.TotalErrorsTimeout,
				TotalWaiting:            info.Metrics.TotalWaiting,
			}
			for k, v := range info.Metrics.LastMinute {
				if v.N > 0 {
					di.Metrics.LastMinute[k] = v.asTimedAction()
				}
			}
			for k, v := range info.Metrics.APICalls {
				di.Metrics.APICalls[k] = v
			}
			if info.Total > 0 {
				di.Utilization = float64(info.Used / info.Total * 100)
			}
			disksInfo[index] = di
			return nil
		}, index)
	}

	g.Wait()
	return disksInfo
}

// Get an aggregated storage info across all disks.
func getStorageInfo(disks []StorageAPI, endpoints []Endpoint, metrics bool) StorageInfo {
	disksInfo := getDisksInfo(disks, endpoints, metrics)

	// Sort so that the first element is the smallest.
	sort.Slice(disksInfo, func(i, j int) bool {
		return disksInfo[i].TotalSpace < disksInfo[j].TotalSpace
	})

	storageInfo := StorageInfo{
		Disks: disksInfo,
	}

	storageInfo.Backend.Type = madmin.Erasure
	return storageInfo
}

// StorageInfo - returns underlying storage statistics.
func (er erasureObjects) StorageInfo(ctx context.Context) StorageInfo {
	disks := er.getDisks()
	endpoints := er.getEndpoints()
	return getStorageInfo(disks, endpoints, true)
}

// LocalStorageInfo - returns underlying local storage statistics.
func (er erasureObjects) LocalStorageInfo(ctx context.Context, metrics bool) StorageInfo {
	disks := er.getDisks()
	endpoints := er.getEndpoints()

	var localDisks []StorageAPI
	var localEndpoints []Endpoint

	for i, endpoint := range endpoints {
		if endpoint.IsLocal {
			localDisks = append(localDisks, disks[i])
			localEndpoints = append(localEndpoints, endpoint)
		}
	}

	return getStorageInfo(localDisks, localEndpoints, metrics)
}

// getOnlineDisksWithHealingAndInfo - returns online disks and overall healing status.
// Disks are ordered in the following groups:
// - Non-scanning disks
// - Non-healing disks
// - Healing disks (if inclHealing is true)
func (er erasureObjects) getOnlineDisksWithHealingAndInfo(inclHealing bool) (newDisks []StorageAPI, newInfos []DiskInfo, healing int) {
	var wg sync.WaitGroup
	disks := er.getDisks()
	infos := make([]DiskInfo, len(disks))
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for _, i := range r.Perm(len(disks)) {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()

			disk := disks[i]
			if disk == nil {
				infos[i].Error = errDiskNotFound.Error()
				return
			}

			di, err := disk.DiskInfo(context.Background(), DiskInfoOptions{})
			infos[i] = di
			if err != nil {
				// - Do not consume disks which are not reachable
				//   unformatted or simply not accessible for some reason.
				infos[i].Error = err.Error()
			}
		}()
	}
	wg.Wait()

	var scanningDisks, healingDisks []StorageAPI
	var scanningInfos, healingInfos []DiskInfo

	for i, info := range infos {
		// Check if one of the drives in the set is being healed.
		// this information is used by scanner to skip healing
		// this erasure set while it calculates the usage.
		if info.Error != "" || disks[i] == nil {
			continue
		}
		if info.Healing {
			healing++
			if inclHealing {
				healingDisks = append(healingDisks, disks[i])
				healingInfos = append(healingInfos, infos[i])
			}
			continue
		}

		if !info.Scanning {
			newDisks = append(newDisks, disks[i])
			newInfos = append(newInfos, infos[i])
		} else {
			scanningDisks = append(scanningDisks, disks[i])
			scanningInfos = append(scanningInfos, infos[i])
		}
	}

	// Prefer non-scanning disks over disks which are currently being scanned.
	newDisks = append(newDisks, scanningDisks...)
	newInfos = append(newInfos, scanningInfos...)

	/// Then add healing disks.
	newDisks = append(newDisks, healingDisks...)
	newInfos = append(newInfos, healingInfos...)

	return newDisks, newInfos, healing
}

func (er erasureObjects) getOnlineDisksWithHealing(inclHealing bool) ([]StorageAPI, bool) {
	newDisks, _, healing := er.getOnlineDisksWithHealingAndInfo(inclHealing)
	return newDisks, healing > 0
}

// Clean-up previously deleted objects. from .minio.sys/tmp/.trash/
func (er erasureObjects) cleanupDeletedObjects(ctx context.Context) {
	var wg sync.WaitGroup
	for _, disk := range er.getLocalDisks() {
		if disk == nil {
			continue
		}
		wg.Add(1)
		go func(disk StorageAPI) {
			defer wg.Done()
			drivePath := disk.Endpoint().Path
			readDirFn(pathJoin(drivePath, minioMetaTmpDeletedBucket), func(ddir string, typ os.FileMode) error {
				w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
				return w.Run(func() error {
					wait := deleteCleanupSleeper.Timer(ctx)
					removeAll(pathJoin(drivePath, minioMetaTmpDeletedBucket, ddir))
					wait()
					return nil
				})
			})
		}(disk)
	}
	wg.Wait()
}

// nsScanner will start scanning buckets and send updated totals as they are traversed.
// Updates are sent on a regular basis and the caller *must* consume them.
func (er erasureObjects) nsScanner(ctx context.Context, scanMgr *bucketsScanMgr, updates chan<- dataUsageCache) error {
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

	for k := range oldCache.Cache {
		if k == dataUsageRoot {
			continue
		}
		if e := oldCache.find(k); e != nil {
			cache.replace(k, dataUsageRoot, *e)
		}
	}

	bucketResults := make(chan dataUsageEntryInfo)

	// Start async collector/saver.
	// This goroutine owns the cache.
	var saverWg sync.WaitGroup
	saverWg.Add(1)
	go func() {
		// Add jitter to the update time so multiple sets don't sync up.
		updateTime := 30*time.Second + time.Duration(float64(10*time.Second)*rand.Float64())
		updateTimer := time.NewTicker(updateTime)
		defer updateTimer.Stop()

		cleanTimer := time.NewTicker(time.Minute)
		defer cleanTimer.Stop()

		defer saverWg.Done()
		var lastSave time.Time

		for {
			select {
			case <-cleanTimer.C:
				for k := range cache.Cache {
					if k != dataUsageRoot && !scanMgr.isKnownBucket(k) {
						delete(cache.Cache, k)
						delete(cache.Cache[dataUsageRoot].Children, k)
						cache.Info.LastUpdate = time.Now()
					}
				}
			case <-updateTimer.C:
				if cache.Info.LastUpdate.Equal(lastSave) {
					continue
				}
				scannerLogOnceIf(ctx, cache.save(ctx, er, dataUsageCacheName), "nsscanner-cache-update")
				updates <- cache.clone()

				lastSave = cache.Info.LastUpdate
			case v, ok := <-bucketResults:
				if !ok {
					// Save final state...
					cache.Info.LastUpdate = time.Now()
					scannerLogOnceIf(ctx, cache.save(ctx, er, dataUsageCacheName), "nsscanner-channel-closed")
					updates <- cache.clone()
					return
				}
				cache.replace(v.Name, v.Parent, v.Entry)
				cache.Info.LastUpdate = time.Now()
			}
		}
	}()

	var (
		// Restrict parallelism for disk usage scanner
		// upto GOMAXPROCS if GOMAXPROCS is < len(disks)
		cpuGuard = make(chan struct{}, runtime.GOMAXPROCS(0))

		scanWg sync.WaitGroup
		id     = setID{pool: er.poolIndex, set: er.setIndex}
	)

	for bucket := range scanMgr.getBucketCh(id) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case cpuGuard <- struct{}{}:
		}

		var (
			disk    StorageAPI
			healing bool
		)
		// Check for an available disk, not scanning or healing
		for {
			disks := er.getDisks()
			for i, d := range getDiskInfos(ctx, disks...) {
				if d == nil || disks[i] == nil {
					continue
				}
				if d.Healing {
					healing = true
				}
				if !d.Scanning && !d.Healing {
					disk = disks[i]
					goto scan
				}
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
				continue
			}
		}

	scan:
		scanWg.Add(1)
		go func(disk StorageAPI, bucket string) {
			defer func() {
				scanWg.Done()
				<-cpuGuard
			}()

			select {
			case <-ctx.Done():
				return
			default:
			}

			// Load cache for bucket
			cacheName := pathJoin(bucket, dataUsageCacheName)
			cache := dataUsageCache{}
			scannerLogIf(ctx, cache.load(ctx, er, cacheName))
			if cache.Info.Name == "" {
				cache.Info.Name = bucket
			}
			cache.Info.SkipHealing = healing
			cache.Info.NextCycle++
			if cache.Info.Name != bucket {
				scannerLogIf(ctx, fmt.Errorf("cache name mismatch: %s != %s", cache.Info.Name, bucket))
				cache.Info = dataUsageCacheInfo{
					Name:       bucket,
					LastUpdate: time.Time{},
					NextCycle:  0,
				}
			}

			scanMgr.markBucketScanStarted(id, bucket, cache.Info.NextCycle, cache.Info.LastUpdate)
			globalScannerMetrics.bucketScanStarted(id, bucket, cache.Info.NextCycle, cache.Info.LastUpdate)

			// Calculate healing mode in the next bucket scan
			bgHealInfo := readBackgroundHealInfo(ctx, er, bucket)
			healScanMode := getCycleScanMode(uint64(cache.Info.NextCycle), bgHealInfo.BitrotStartCycle, bgHealInfo.BitrotStartTime)
			if bgHealInfo.CurrentScanMode != healScanMode {
				newHealInfo := bgHealInfo
				newHealInfo.CurrentScanMode = healScanMode
				if healScanMode == madmin.HealDeepScan {
					newHealInfo.BitrotStartTime = time.Now().UTC()
					newHealInfo.BitrotStartCycle = uint64(cache.Info.NextCycle)
				}
				saveBackgroundHealInfo(ctx, er, bucket, newHealInfo)
			}

			// Collect updates.
			updates := make(chan dataUsageEntry, 1)
			var wg sync.WaitGroup
			wg.Add(1)
			go func(name string) {
				defer wg.Done()
				for update := range updates {
					select {
					case <-ctx.Done():
					case bucketResults <- dataUsageEntryInfo{
						Name:   name,
						Parent: dataUsageRoot,
						Entry:  update,
					}:
						globalScannerMetrics.bucketScanUpdate(id, bucket)
					}
				}
			}(cache.Info.Name)

			// Calc usage
			before := cache.Info.LastUpdate
			var err error
			cache, err = disk.NSScanner(ctx, cache, updates, healScanMode, nil)
			if err != nil {
				if !cache.Info.LastUpdate.IsZero() && cache.Info.LastUpdate.After(before) {
					scannerLogIf(ctx, cache.save(ctx, er, cacheName))
				} else {
					scannerLogIf(ctx, err)
				}
				// This ensures that we don't close
				// bucketResults channel while the
				// updates-collector goroutine still
				// holds a reference to this.
				wg.Wait()
				return
			}

			globalScannerMetrics.bucketScanFinished(id, bucket)
			scanMgr.markBucketScanDone(id, bucket)

			wg.Wait()
			var root dataUsageEntry
			if r := cache.root(); r != nil {
				root = cache.flatten(*r)
			}
			select {
			case <-ctx.Done():
				return
			case bucketResults <- dataUsageEntryInfo{
				Name:   cache.Info.Name,
				Parent: dataUsageRoot,
				Entry:  root,
			}:
			}

			// Save cache
			scannerLogIf(ctx, cache.save(ctx, er, cacheName))
		}(disk, bucket)
	}

	scanWg.Wait()
	xioutil.SafeClose(bucketResults)
	saverWg.Wait()

	return nil
}
