/*
 * Minio Cloud Storage, (C) 2016, 2017, 2018 Minio, Inc.
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
	"sort"
	"time"

	"github.com/minio/minio/pkg/bpool"
	"github.com/minio/minio/pkg/disk"
	"github.com/minio/minio/pkg/errors"
)

// XL constants.
const (
	// XL metadata file carries per object metadata.
	xlMetaJSONFile = "xl.json"

	// Uploads metadata file carries per multipart object metadata.
	uploadsJSONFile = "uploads.json"
)

// xlObjects - Implements XL object layer.
type xlObjects struct {
	// name space mutex for object layer.
	nsMutex *nsLockMap

	// getDisks returns list of storageAPIs.
	getDisks func() []StorageAPI

	// Byte pools used for temporary i/o buffers.
	bp *bpool.BytePoolCap

	// Variable represents bucket policies in memory.
	bucketPolicies *bucketPolicies

	// TODO: Deprecated only kept here for tests, should be removed in future.
	storageDisks []StorageAPI

	// TODO: ListObjects pool management, should be removed in future.
	listPool *treeWalkPool
}

// list of all errors that can be ignored in tree walk operation in XL
var xlTreeWalkIgnoredErrs = append(baseIgnoredErrs, errDiskAccessDenied, errVolumeNotFound, errFileNotFound)

// Shutdown function for object storage interface.
func (xl xlObjects) Shutdown() error {
	// Add any object layer shutdown activities here.
	for _, disk := range xl.getDisks() {
		// This closes storage rpc client connections if any.
		// Otherwise this is a no-op.
		if disk == nil {
			continue
		}
		disk.Close()
	}
	return nil
}

// Locking operations

// List namespace locks held in object layer
func (xl xlObjects) ListLocks(bucket, prefix string, duration time.Duration) ([]VolumeLockInfo, error) {
	xl.nsMutex.lockMapMutex.Lock()
	defer xl.nsMutex.lockMapMutex.Unlock()
	// Fetch current time once instead of fetching system time for every lock.
	timeNow := UTCNow()
	volumeLocks := []VolumeLockInfo{}

	for param, debugLock := range xl.nsMutex.debugLockMap {
		if param.volume != bucket {
			continue
		}
		// N B empty prefix matches all param.path.
		if !hasPrefix(param.path, prefix) {
			continue
		}

		volLockInfo := VolumeLockInfo{
			Bucket:                param.volume,
			Object:                param.path,
			LocksOnObject:         debugLock.counters.total,
			TotalBlockedLocks:     debugLock.counters.blocked,
			LocksAcquiredOnObject: debugLock.counters.granted,
		}
		// Filter locks that are held on bucket, prefix.
		for opsID, lockInfo := range debugLock.lockInfo {
			// filter locks that were held for longer than duration.
			elapsed := timeNow.Sub(lockInfo.since)
			if elapsed < duration {
				continue
			}
			// Add locks that are held for longer than duration.
			volLockInfo.LockDetailsOnObject = append(volLockInfo.LockDetailsOnObject,
				OpsLockState{
					OperationID: opsID,
					LockSource:  lockInfo.lockSource,
					LockType:    lockInfo.lType,
					Status:      lockInfo.status,
					Since:       lockInfo.since,
				})
			volumeLocks = append(volumeLocks, volLockInfo)
		}
	}
	return volumeLocks, nil
}

// Clear namespace locks held in object layer
func (xl xlObjects) ClearLocks(volLocks []VolumeLockInfo) error {
	// Remove lock matching bucket/prefix held longer than duration.
	for _, volLock := range volLocks {
		xl.nsMutex.ForceUnlock(volLock.Bucket, volLock.Object)
	}
	return nil
}

// byDiskTotal is a collection satisfying sort.Interface.
type byDiskTotal []disk.Info

func (d byDiskTotal) Len() int      { return len(d) }
func (d byDiskTotal) Swap(i, j int) { d[i], d[j] = d[j], d[i] }
func (d byDiskTotal) Less(i, j int) bool {
	return d[i].Total < d[j].Total
}

// getDisksInfo - fetch disks info across all other storage API.
func getDisksInfo(disks []StorageAPI) (disksInfo []disk.Info, onlineDisks int, offlineDisks int) {
	disksInfo = make([]disk.Info, len(disks))
	for i, storageDisk := range disks {
		if storageDisk == nil {
			// Storage disk is empty, perhaps ignored disk or not available.
			offlineDisks++
			continue
		}
		info, err := storageDisk.DiskInfo()
		if err != nil {
			errorIf(err, "Unable to fetch disk info for %#v", storageDisk)
			if errors.IsErr(err, baseErrs...) {
				offlineDisks++
				continue
			}
		}
		onlineDisks++
		disksInfo[i] = info
	}

	// Success.
	return disksInfo, onlineDisks, offlineDisks
}

// returns sorted disksInfo slice which has only valid entries.
// i.e the entries where the total size of the disk is not stated
// as 0Bytes, this means that the disk is not online or ignored.
func sortValidDisksInfo(disksInfo []disk.Info) []disk.Info {
	var validDisksInfo []disk.Info
	for _, diskInfo := range disksInfo {
		if diskInfo.Total == 0 {
			continue
		}
		validDisksInfo = append(validDisksInfo, diskInfo)
	}
	sort.Sort(byDiskTotal(validDisksInfo))
	return validDisksInfo
}

// Get an aggregated storage info across all disks.
func getStorageInfo(disks []StorageAPI) StorageInfo {
	disksInfo, onlineDisks, offlineDisks := getDisksInfo(disks)

	// Sort so that the first element is the smallest.
	validDisksInfo := sortValidDisksInfo(disksInfo)
	// If there are no valid disks, set total and free disks to 0
	if len(validDisksInfo) == 0 {
		return StorageInfo{
			Total: 0,
			Free:  0,
		}
	}

	_, sscParity := getRedundancyCount(standardStorageClass, len(disks))
	_, rrscparity := getRedundancyCount(reducedRedundancyStorageClass, len(disks))

	// Total number of online data drives available
	// This is the number of drives we report free and total space for
	availableDataDisks := uint64(onlineDisks - sscParity)

	// Return calculated storage info, choose the lowest Total and
	// Free as the total aggregated values. Total capacity is always
	// the multiple of smallest disk among the disk list.
	storageInfo := StorageInfo{
		Total: validDisksInfo[0].Total * availableDataDisks,
		Free:  validDisksInfo[0].Free * availableDataDisks,
	}

	storageInfo.Backend.Type = Erasure
	storageInfo.Backend.OnlineDisks = onlineDisks
	storageInfo.Backend.OfflineDisks = offlineDisks

	storageInfo.Backend.StandardSCParity = sscParity
	storageInfo.Backend.RRSCParity = rrscparity

	return storageInfo
}

// StorageInfo - returns underlying storage statistics.
func (xl xlObjects) StorageInfo() StorageInfo {
	return getStorageInfo(xl.getDisks())
}
