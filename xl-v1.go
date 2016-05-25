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
	"path/filepath"
	"strings"
	"sync"
)

const (
	formatConfigFile = "format.json"
	xlMetaJSONFile   = "xl.json"
	uploadsJSONFile  = "uploads.json"
)

// xlObjects - Implements fs object layer.
type xlObjects struct {
	storageDisks       []StorageAPI
	dataBlocks         int
	parityBlocks       int
	readQuorum         int
	writeQuorum        int
	listObjectMap      map[listParams][]*treeWalker
	listObjectMapMutex *sync.Mutex
}

// errMaxDisks - returned for reached maximum of disks.
var errMaxDisks = errors.New("Number of disks are higher than supported maximum count '16'")

// errMinDisks - returned for minimum number of disks.
var errMinDisks = errors.New("Number of disks are smaller than supported minimum count '8'")

// errNumDisks - returned for odd number of disks.
var errNumDisks = errors.New("Number of disks should be multiples of '2'")

const (
	// Maximum erasure blocks.
	maxErasureBlocks = 16
	// Minimum erasure blocks.
	minErasureBlocks = 8
)

func checkSufficientDisks(disks []string) error {
	// Verify total number of disks.
	totalDisks := len(disks)
	if totalDisks > maxErasureBlocks {
		return errMaxDisks
	}
	if totalDisks < minErasureBlocks {
		return errMinDisks
	}

	// isEven function to verify if a given number if even.
	isEven := func(number int) bool {
		return number%2 == 0
	}

	// Verify if we have even number of disks.
	// only combination of 8, 10, 12, 14, 16 are supported.
	if !isEven(totalDisks) {
		return errNumDisks
	}

	return nil
}

// Depending on the disk type network or local, initialize storage layer.
func newStorageLayer(disk string) (storage StorageAPI, err error) {
	if !strings.ContainsRune(disk, ':') || filepath.VolumeName(disk) != "" {
		// Initialize filesystem storage API.
		return newPosix(disk)
	}
	// Initialize rpc client storage API.
	return newRPCClient(disk)
}

// Initialize all storage disks to bootstrap.
func bootstrapDisks(disks []string) ([]StorageAPI, error) {
	storageDisks := make([]StorageAPI, len(disks))
	for index, disk := range disks {
		var err error
		// Intentionally ignore disk not found errors while
		// initializing POSIX, so that we have successfully
		// initialized posix Storage. Subsequent calls to XL/Erasure
		// will manage any errors related to disks.
		storageDisks[index], err = newStorageLayer(disk)
		if err != nil && err != errDiskNotFound {
			return nil, err
		}
	}
	return storageDisks, nil
}

// newXLObjects - initialize new xl object layer.
func newXLObjects(disks []string) (ObjectLayer, error) {
	if err := checkSufficientDisks(disks); err != nil {
		return nil, err
	}

	// Bootstrap disks.
	storageDisks, err := bootstrapDisks(disks)
	if err != nil {
		return nil, err
	}

	// Initialize object layer - like creating minioMetaBucket, cleaning up tmp files etc.
	initObjectLayer(storageDisks...)

	// Load saved XL format.json and validate.
	newPosixDisks, err := loadFormatXL(storageDisks)
	if err != nil {
		switch err {
		case errUnformattedDisk:
			// Save new XL format.
			errSave := initFormatXL(storageDisks)
			if errSave != nil {
				return nil, errSave
			}
			newPosixDisks = storageDisks
		default:
			// errCorruptedDisk - error.
			return nil, fmt.Errorf("Unable to recognize backend format, %s", err)
		}
	}

	// FIXME: healFormatXL(newDisks)

	// Calculate data and parity blocks.
	dataBlocks, parityBlocks := len(newPosixDisks)/2, len(newPosixDisks)/2

	xl := xlObjects{
		storageDisks:       newPosixDisks,
		dataBlocks:         dataBlocks,
		parityBlocks:       parityBlocks,
		listObjectMap:      make(map[listParams][]*treeWalker),
		listObjectMapMutex: &sync.Mutex{},
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
