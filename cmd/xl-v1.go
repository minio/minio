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

	"github.com/minio/minio-go/pkg/set"
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

func repairDiskMetadata(storageDisks []StorageAPI) error {
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
func newXLObjects(disks, ignoredDisks []string) (ObjectLayer, error) {
	if disks == nil {
		return nil, errInvalidArgument
	}
	disksSet := set.NewStringSet()
	if len(ignoredDisks) > 0 {
		disksSet = set.CreateStringSet(ignoredDisks...)
	}
	// Bootstrap disks.
	storageDisks := make([]StorageAPI, len(disks))
	for index, disk := range disks {
		// Check if disk is ignored.
		if disksSet.Contains(disk) {
			storageDisks[index] = nil
			continue
		}
		var err error
		// Intentionally ignore disk not found errors. XL is designed
		// to handle these errors internally.
		storageDisks[index], err = newStorageAPI(disk)
		if err != nil && err != errDiskNotFound {
			switch diskType := storageDisks[index].(type) {
			case networkStorage:
				diskType.rpcClient.Close()
			}
			return nil, err
		}
	}

	// Fix format files in case of fresh or corrupted disks
	repairDiskMetadata(storageDisks)

	// Runs house keeping code, like t, cleaning up tmp files etc.
	if err := xlHouseKeeping(storageDisks); err != nil {
		return nil, err
	}

	// Load saved XL format.json and validate.
	newPosixDisks, err := loadFormatXL(storageDisks)
	if err != nil {
		// errCorruptedDisk - healing failed
		return nil, fmt.Errorf("Unable to recognize backend format, %s", err)
	}

	// Calculate data and parity blocks.
	dataBlocks, parityBlocks := len(newPosixDisks)/2, len(newPosixDisks)/2

	// Initialize object cache.
	objCache := objcache.New(globalMaxCacheSize, globalCacheExpiry)

	// Initialize list pool.
	listPool := newTreeWalkPool(globalLookupTimeout)

	// Initialize xl objects.
	xl := xlObjects{
		storageDisks:    newPosixDisks,
		dataBlocks:      dataBlocks,
		parityBlocks:    parityBlocks,
		listPool:        listPool,
		objCache:        objCache,
		objCacheEnabled: globalMaxCacheSize > 0,
	}

	// Figure out read and write quorum based on number of storage disks.
	// READ and WRITE quorum is always set to (N/2) number of disks.
	xl.readQuorum = len(xl.storageDisks) / 2
	xl.writeQuorum = len(xl.storageDisks)/2 + 1

	// Return successfully initialized object layer.
	return xl, nil
}

// Shutdown function for object storage interface.
func (xl xlObjects) Shutdown() error {
	// Add any object layer shutdown activities here.
	return nil
}

// HealDiskMetadata function for object storage interface.
func (xl xlObjects) HealDiskMetadata() error {
	// generates random string on setting MINIO_DEBUG=lock, else returns empty string.
	// used for instrumentation on locks.
	opsID := getOpsID()

	nsMutex.Lock(minioMetaBucket, formatConfigFile, opsID)
	defer nsMutex.Unlock(minioMetaBucket, formatConfigFile, opsID)
	return repairDiskMetadata(xl.storageDisks)
}

// byDiskTotal is a collection satisfying sort.Interface.
type byDiskTotal []disk.Info

func (d byDiskTotal) Len() int      { return len(d) }
func (d byDiskTotal) Swap(i, j int) { d[i], d[j] = d[j], d[i] }
func (d byDiskTotal) Less(i, j int) bool {
	return d[i].Total < d[j].Total
}

// StorageInfo - returns underlying storage statistics.
func (xl xlObjects) StorageInfo() StorageInfo {
	var disksInfo []disk.Info
	for _, storageDisk := range xl.storageDisks {
		info, err := storageDisk.DiskInfo()
		if err != nil {
			errorIf(err, "Unable to fetch disk info for %#v", storageDisk)
			continue
		}
		disksInfo = append(disksInfo, info)
	}

	// Sort so that the first element is the smallest.
	sort.Sort(byDiskTotal(disksInfo))

	// Return calculated storage info, choose the lowest Total and
	// Free as the total aggregated values. Total capacity is always
	// the multiple of smallest disk among the disk list.
	return StorageInfo{
		Total: disksInfo[0].Total * int64(len(xl.storageDisks)),
		Free:  disksInfo[0].Free * int64(len(xl.storageDisks)),
	}
}
