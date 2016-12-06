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
	"os"
	"runtime/debug"
	"sort"
	"strings"
	"sync"

	humanize "github.com/dustin/go-humanize"
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

	// Represents the minimum required RAM size before
	// we enable caching.
	minRAMSize = 8 * humanize.GiByte

	// Maximum erasure blocks.
	maxErasureBlocks = 16

	// Minimum erasure blocks.
	minErasureBlocks = 4
)

// xlObjects - Implements XL object layer.
type xlObjects struct {
	mutex        *sync.Mutex
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
var xlTreeWalkIgnoredErrs = append(baseIgnoredErrs, errDiskAccessDenied, errVolumeNotFound, errFileNotFound)

// newXLObjects - initialize new xl object layer.
func newXLObjects(storageDisks []StorageAPI) (ObjectLayer, error) {
	if storageDisks == nil {
		return nil, errInvalidArgument
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

	// Initialize list pool.
	listPool := newTreeWalkPool(globalLookupTimeout)

	// Check if object cache is disabled.
	objCacheDisabled := strings.EqualFold(os.Getenv("_MINIO_CACHE"), "off")

	// Initialize xl objects.
	xl := &xlObjects{
		mutex:        &sync.Mutex{},
		storageDisks: newStorageDisks,
		dataBlocks:   dataBlocks,
		parityBlocks: parityBlocks,
		listPool:     listPool,
	}

	// Object cache is enabled when _MINIO_CACHE env is missing.
	// and cache size is > 0.
	xl.objCacheEnabled = !objCacheDisabled && globalMaxCacheSize > 0

	// Check if object cache is enabled.
	if xl.objCacheEnabled {
		// Initialize object cache.
		objCache := objcache.New(globalMaxCacheSize, globalCacheExpiry)
		objCache.OnEviction = func(key string) {
			debug.FreeOSMemory()
		}
		xl.objCache = objCache
	}

	// Initialize meta volume, if volume already exists ignores it.
	if err = initMetaVolume(storageDisks); err != nil {
		return nil, fmt.Errorf("Unable to initialize '.minio.sys' meta volume, %s", err)
	}

	// Figure out read and write quorum based on number of storage disks.
	// READ and WRITE quorum is always set to (N/2) number of disks.
	xl.readQuorum = readQuorum
	xl.writeQuorum = writeQuorum

	// Do a quick heal on the buckets themselves for any discrepancies.
	if err := quickHeal(xl.storageDisks, xl.writeQuorum, xl.readQuorum); err != nil {
		return xl, err
	}

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
			if err == errDiskNotFound {
				offlineDisks++
			}
			continue
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
	if len(validDisksInfo) == 0 {
		return StorageInfo{
			Total: -1,
			Free:  -1,
		}
	}

	// Return calculated storage info, choose the lowest Total and
	// Free as the total aggregated values. Total capacity is always
	// the multiple of smallest disk among the disk list.
	storageInfo := StorageInfo{
		Total: validDisksInfo[0].Total * int64(onlineDisks) / 2,
		Free:  validDisksInfo[0].Free * int64(onlineDisks) / 2,
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
