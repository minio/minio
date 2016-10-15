/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"fmt"
	"sort"

	"github.com/minio/minio/pkg/disk"
	"github.com/minio/minio/pkg/objcache"
)

// XL constants.
const (
	// Format config file carries backend format specific details.
	formatConfigFile = "format.json"

	// Format config tmp file carries backend format.
	formatConfigFileTmp = "format.json.tmp"

	// XL metadata file carries per object metadata.
	xlMetaJSONFile = "xl.json"

	// Uploads metadata file carries per multipart object metadata.
	uploadsJSONFile = "uploads.json"

	// 8GiB cache by default.
	maxCacheSize = 8 * 1024 * 1024 * 1024

	// Maximum erasure blocks.
	maxErasureBlocks = 16

	// Minimum erasure blocks.
	minErasureBlocks = 4
)

// xlObjects - Implements XL object layer.
type xlObjects struct {
	storageDisks []StorageAPI // Collection of initialized backend disks.
	dataBlocks   int          // dataBlocks count caculated for erasure.
	parityBlocks int          // parityBlocks count calculated for erasure.
	readQuorum   int          // readQuorum minimum required disks to read data.
	writeQuorum  int          // writeQuorum minimum required disks to write data.

	// ListObjects pool management.
	listPool *treeWalkPool

	// Object cache for caching objects.
	objCache *objcache.Cache

	// Object cache enabled.
	objCacheEnabled bool
}

// list of all errors that can be ignored in tree walk operation in XL
var xlTreeWalkIgnoredErrs = []error{
	errFileNotFound,
	errVolumeNotFound,
	errDiskNotFound,
	errDiskAccessDenied,
	errFaultyDisk,
}

func healFormatXL(storageDisks []StorageAPI) error {
	// Attempt to load all `format.json`.
	formatConfigs, sErrs := loadAllFormats(storageDisks)

	// Generic format check validates
	// if (no quorum) return error
	// if (disks not recognized) // Always error.
	if err := genericFormatCheck(formatConfigs, sErrs); err != nil {
		return err
	}

	// Handles different cases properly.
	switch reduceFormatErrs(sErrs, len(storageDisks)) {
	case errCorruptedFormat:
		if err := healFormatXLCorruptedDisks(storageDisks); err != nil {
			return fmt.Errorf("Unable to repair corrupted format, %s", err)
		}
	case errSomeDiskUnformatted:
		// All drives online but some report missing format.json.
		if err := healFormatXLFreshDisks(storageDisks); err != nil {
			// There was an unexpected unrecoverable error during healing.
			return fmt.Errorf("Unable to heal backend %s", err)
		}
	case errSomeDiskOffline:
		// FIXME: in future.
		return fmt.Errorf("Unable to initialize format %s and %s", errSomeDiskOffline, errSomeDiskUnformatted)
	}
	return nil
}

// newXLObjects - initialize new xl object layer.
func newXLObjects(storageDisks []StorageAPI) (ObjectLayer, error) {
	if storageDisks == nil {
		return nil, errInvalidArgument
	}

	// Runs house keeping code, like t, cleaning up tmp files etc.
	if err := xlHouseKeeping(storageDisks); err != nil {
		return nil, err
	}

	readQuorum := len(storageDisks) / 2
	writeQuorum := len(storageDisks)/2 + 1

	// Load saved XL format.json and validate.
	newStorageDisks, err := loadFormatXL(storageDisks, readQuorum)
	if err != nil {
		return nil, fmt.Errorf("Unable to recognize backend format, %s", err)
	}

	// Calculate data and parity blocks.
	dataBlocks, parityBlocks := len(newStorageDisks)/2, len(newStorageDisks)/2

	// Initialize object cache.
	objCache := objcache.New(globalMaxCacheSize, globalCacheExpiry)

	// Initialize list pool.
	listPool := newTreeWalkPool(globalLookupTimeout)

	// Initialize xl objects.
	xl := xlObjects{
		storageDisks:    newStorageDisks,
		dataBlocks:      dataBlocks,
		parityBlocks:    parityBlocks,
		listPool:        listPool,
		objCache:        objCache,
		objCacheEnabled: globalMaxCacheSize > 0,
	}

	// Figure out read and write quorum based on number of storage disks.
	// READ and WRITE quorum is always set to (N/2) number of disks.
	xl.readQuorum = readQuorum
	xl.writeQuorum = writeQuorum

	// Return successfully initialized object layer.
	return xl, nil
}

// Shutdown function for object storage interface.
func (xl xlObjects) Shutdown() error {
	// Add any object layer shutdown activities here.
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
	for _, storageDisk := range disks {
		if storageDisk == nil {
			// Storage disk is empty, perhaps ignored disk or not available.
			offlineDisks++
			continue
		}
		info, err := storageDisk.DiskInfo()
		if err != nil {
			errorIf(err, "Unable to fetch disk info for %#v", storageDisk)
			if err == errDiskNotFound {
				offlineDisks++
			}
			continue
		}
		onlineDisks++
		disksInfo = append(disksInfo, info)
	}

	// Sort so that the first element is the smallest.
	sort.Sort(byDiskTotal(disksInfo))

	// Success.
	return disksInfo, onlineDisks, offlineDisks
}

// Get an aggregated storage info across all disks.
func getStorageInfo(disks []StorageAPI) StorageInfo {
	disksInfo, onlineDisks, offlineDisks := getDisksInfo(disks)
	if len(disksInfo) == 0 {
		return StorageInfo{
			Total: -1,
			Free:  -1,
		}
	}
	// Return calculated storage info, choose the lowest Total and
	// Free as the total aggregated values. Total capacity is always
	// the multiple of smallest disk among the disk list.
	storageInfo := StorageInfo{
		Total: disksInfo[0].Total * int64(onlineDisks),
		Free:  disksInfo[0].Free * int64(onlineDisks),
	}
	storageInfo.Backend.Type = XL
	storageInfo.Backend.OnlineDisks = onlineDisks
	storageInfo.Backend.OfflineDisks = offlineDisks
	return storageInfo
}

// StorageInfo - returns underlying storage statistics.
func (xl xlObjects) StorageInfo() StorageInfo {
	storageInfo := getStorageInfo(xl.storageDisks)
	storageInfo.Backend.ReadQuorum = xl.readQuorum
	storageInfo.Backend.WriteQuorum = xl.writeQuorum
	return storageInfo
}
