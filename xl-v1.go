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

package main

import (
	"errors"
	"fmt"
	"sort"

	"github.com/minio/minio/pkg/disk"
)

// XL constants.
const (
	// Format config file carries backend format specific details.
	formatConfigFile = "format.json"
	// XL metadata file carries per object metadata.
	xlMetaJSONFile = "xl.json"
	// Uploads metadata file carries per multipart object metadata.
	uploadsJSONFile = "uploads.json"
)

// xlObjects - Implements XL object layer.
type xlObjects struct {
	physicalDisks []string     // Collection of regular disks.
	storageDisks  []StorageAPI // Collection of initialized backend disks.
	dataBlocks    int          // dataBlocks count caculated for erasure.
	parityBlocks  int          // parityBlocks count calculated for erasure.
	readQuorum    int          // readQuorum minimum required disks to read data.
	writeQuorum   int          // writeQuorum minimum required disks to write data.

	// List pool management.
	listPool *treeWalkerPool
}

// errXLMaxDisks - returned for reached maximum of disks.
var errXLMaxDisks = errors.New("Number of disks are higher than supported maximum count '16'")

// errXLMinDisks - returned for minimum number of disks.
var errXLMinDisks = errors.New("Number of disks are smaller than supported minimum count '8'")

// errXLNumDisks - returned for odd number of disks.
var errXLNumDisks = errors.New("Number of disks should be multiples of '2'")

// errXLReadQuorum - did not meet read quorum.
var errXLReadQuorum = errors.New("I/O error.  did not meet read quorum.")

// errXLWriteQuorum - did not meet write quorum.
var errXLWriteQuorum = errors.New("I/O error.  did not meet write quorum.")

// errXLDataCorrupt - err data corrupt.
var errXLDataCorrupt = errors.New("data likely corrupted, all blocks are zero in length")

const (
	// Maximum erasure blocks.
	maxErasureBlocks = 16
	// Minimum erasure blocks.
	minErasureBlocks = 8
)

// Validate if input disks are sufficient for initializing XL.
func checkSufficientDisks(disks []string) error {
	// Verify total number of disks.
	totalDisks := len(disks)
	if totalDisks > maxErasureBlocks {
		return errXLMaxDisks
	}
	if totalDisks < minErasureBlocks {
		return errXLMinDisks
	}

	// isEven function to verify if a given number if even.
	isEven := func(number int) bool {
		return number%2 == 0
	}

	// Verify if we have even number of disks.
	// only combination of 8, 12, 16 are supported.
	if !isEven(totalDisks) {
		return errXLNumDisks
	}

	return nil
}

// newXLObjects - initialize new xl object layer.
func newXLObjects(disks []string) (ObjectLayer, error) {
	// Validate if input disks are sufficient.
	if err := checkSufficientDisks(disks); err != nil {
		return nil, err
	}

	// Bootstrap disks.
	storageDisks := make([]StorageAPI, len(disks))
	for index, disk := range disks {
		var err error
		// Intentionally ignore disk not found errors. XL will
		// manage such errors internally.
		storageDisks[index], err = newStorageAPI(disk)
		if err != nil && err != errDiskNotFound {
			return nil, err
		}
	}

	// Runs house keeping code, like creating minioMetaBucket, cleaning up tmp files etc.
	xlHouseKeeping(storageDisks)

	// Attempt to load all `format.json`
	formatConfigs, sErrs := loadAllFormats(storageDisks)

	// Generic format check validates all necessary cases.
	if err := genericFormatCheck(formatConfigs, sErrs); err != nil {
		return nil, err
	}

	// Handles different cases properly.
	switch reduceFormatErrs(sErrs, len(storageDisks)) {
	case errUnformattedDisk:
		// All drives online but fresh, initialize format.
		if err := initFormatXL(storageDisks); err != nil {
			return nil, fmt.Errorf("Unable to initialize format, %s", err)
		}
	case errSomeDiskUnformatted:
		// All drives online but some report missing format.json.
		if err := healFormatXL(storageDisks); err != nil {
			// There was an unexpected unrecoverable error during healing.
			return nil, fmt.Errorf("Unable to heal backend %s", err)
		}
	case errSomeDiskOffline:
		// Some disks offline but some report missing format.json.
		// FIXME.
	}

	// Load saved XL format.json and validate.
	newPosixDisks, err := loadFormatXL(storageDisks)
	if err != nil {
		// errCorruptedDisk - healing failed
		return nil, fmt.Errorf("Unable to recognize backend format, %s", err)
	}

	// Calculate data and parity blocks.
	dataBlocks, parityBlocks := len(newPosixDisks)/2, len(newPosixDisks)/2

	// Initialize xl objects.
	xl := xlObjects{
		physicalDisks: disks,
		storageDisks:  newPosixDisks,
		dataBlocks:    dataBlocks,
		parityBlocks:  parityBlocks,
		listPool:      newTreeWalkerPool(globalLookupTimeout),
	}

	// Figure out read and write quorum based on number of storage disks.
	// Read quorum should be always N/2 + 1 (due to Vandermonde matrix
	// erasure requirements)
	xl.readQuorum = len(xl.storageDisks)/2 + 1

	// Write quorum is assumed if we have total disks + 3
	// parity. (Need to discuss this again)
	xl.writeQuorum = len(xl.storageDisks)/2 + 3
	if xl.writeQuorum > len(xl.storageDisks) {
		xl.writeQuorum = len(xl.storageDisks)
	}

	// Return successfully initialized object layer.
	return xl, nil
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
	for _, diskPath := range xl.physicalDisks {
		info, err := disk.GetInfo(diskPath)
		if err != nil {
			errorIf(err, "Unable to fetch disk info for "+diskPath)
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
