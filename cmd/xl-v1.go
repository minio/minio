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
	"context"
	"sort"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/bpool"
)

// XL constants.
const (
	// XL metadata file carries per object metadata.
	xlMetaJSONFile = "xl.json"
)

// OfflineDisk represents an unavailable disk.
var OfflineDisk StorageAPI // zero value is nil

// xlObjects - Implements XL object layer.
type xlObjects struct {
	// name space mutex for object layer.
	nsMutex *nsLockMap

	// getDisks returns list of storageAPIs.
	getDisks func() []StorageAPI

	// Byte pools used for temporary i/o buffers.
	bp *bpool.BytePoolCap

	// TODO: Deprecated only kept here for tests, should be removed in future.
	storageDisks []StorageAPI

	// TODO: ListObjects pool management, should be removed in future.
	listPool *treeWalkPool
}

// Shutdown function for object storage interface.
func (xl xlObjects) Shutdown(ctx context.Context) error {
	// Add any object layer shutdown activities here.
	closeStorageDisks(xl.getDisks())
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
func getDisksInfo(disks []StorageAPI) (disksInfo []DiskInfo, onlineDisks int, offlineDisks int) {
	disksInfo = make([]DiskInfo, len(disks))
	for i, storageDisk := range disks {
		if storageDisk == nil {
			// Storage disk is empty, perhaps ignored disk or not available.
			offlineDisks++
			continue
		}
		info, err := storageDisk.DiskInfo()
		if err != nil {
			ctx := context.Background()
			logger.GetReqInfo(ctx).AppendTags("disk", storageDisk.String())
			logger.LogIf(ctx, err)
			if IsErr(err, baseErrs...) {
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
func sortValidDisksInfo(disksInfo []DiskInfo) []DiskInfo {
	var validDisksInfo []DiskInfo
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
		return StorageInfo{}
	}

	_, sscParity := getRedundancyCount(standardStorageClass, len(disks))
	_, rrscparity := getRedundancyCount(reducedRedundancyStorageClass, len(disks))

	// Total number of online data drives available
	// This is the number of drives we report free and total space for
	availableDataDisks := uint64(onlineDisks - sscParity)

	// Available data disks can be zero when onlineDisks is equal to parity,
	// at that point we simply choose online disks to calculate the size.
	if availableDataDisks == 0 {
		availableDataDisks = uint64(onlineDisks)
	}

	storageInfo := StorageInfo{}

	// Combine all disks to get total usage.
	var used uint64
	for _, di := range validDisksInfo {
		used = used + di.Used
	}
	storageInfo.Used = used

	storageInfo.Backend.Type = BackendErasure
	storageInfo.Backend.OnlineDisks = onlineDisks
	storageInfo.Backend.OfflineDisks = offlineDisks

	storageInfo.Backend.StandardSCParity = sscParity
	storageInfo.Backend.RRSCParity = rrscparity

	return storageInfo
}

// StorageInfo - returns underlying storage statistics.
func (xl xlObjects) StorageInfo(ctx context.Context) StorageInfo {
	return getStorageInfo(xl.getDisks())
}
