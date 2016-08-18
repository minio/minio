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
	"encoding/json"
	"hash/crc32"
	"path"
	"sync"
)

// Returns number of errors that occurred the most (incl. nil) and the
// corresponding error value. N B when there is more than one error value that
// occurs maximum number of times, the error value returned depends on how
// golang's map orders keys. This doesn't affect correctness as long as quorum
// value is greater than or equal to simple majority, since none of the equally
// maximal values would occur quorum or more number of times.

func reduceErrs(errs []error, ignoredErrs []error) error {
	errorCounts := make(map[error]int)
	for _, err := range errs {
		if isErrIgnored(err, ignoredErrs) {
			continue
		}
		errorCounts[err]++
	}
	max := 0
	var errMax error
	for err, count := range errorCounts {
		if max < count {
			max = count
			errMax = err
		}
	}
	return errMax
}

// Validates if we have quorum based on the errors related to disk only.
// Returns 'true' if we have quorum, 'false' if we don't.
func isDiskQuorum(errs []error, minQuorumCount int) bool {
	var count int
	for _, err := range errs {
		switch err {
		case errDiskNotFound, errFaultyDisk, errDiskAccessDenied:
			continue
		}
		count++
	}
	return count >= minQuorumCount
}

// Similar to 'len(slice)' but returns  the actual elements count
// skipping the unallocated elements.
func diskCount(disks []StorageAPI) int {
	diskCount := 0
	for _, disk := range disks {
		if disk == nil {
			continue
		}
		diskCount++
	}
	return diskCount
}

// hashOrder - hashes input key to return returns consistent
// hashed integer slice. Returned integer order is salted
// with an input key. This results in consistent order.
// NOTE: collisions are fine, we are not looking for uniqueness
// in the slices returned.
func hashOrder(key string, cardinality int) []int {
	if cardinality < 0 {
		// Returns an empty int slice for negative cardinality.
		return nil
	}
	nums := make([]int, cardinality)
	keyCrc := crc32.Checksum([]byte(key), crc32.IEEETable)

	start := int(uint32(keyCrc)%uint32(cardinality)) | 1
	for i := 1; i <= cardinality; i++ {
		nums[i-1] = 1 + ((start + i) % cardinality)
	}
	return nums
}

// readXLMeta reads `xl.json` and returns back XL metadata structure.
func readXLMeta(disk StorageAPI, bucket string, object string) (xlMeta xlMetaV1, err error) {
	// Reads entire `xl.json`.
	buf, err := disk.ReadAll(bucket, path.Join(object, xlMetaJSONFile))
	if err != nil {
		return xlMetaV1{}, err
	}

	// Unmarshal xl metadata.
	if err = json.Unmarshal(buf, &xlMeta); err != nil {
		return xlMetaV1{}, err
	}

	// Return structured `xl.json`.
	return xlMeta, nil
}

// Reads all `xl.json` metadata as a xlMetaV1 slice.
// Returns error slice indicating the failed metadata reads.
func readAllXLMetadata(disks []StorageAPI, bucket, object string) ([]xlMetaV1, []error) {
	errs := make([]error, len(disks))
	metadataArray := make([]xlMetaV1, len(disks))
	var wg = &sync.WaitGroup{}
	// Read `xl.json` parallelly across disks.
	for index, disk := range disks {
		if disk == nil {
			errs[index] = errDiskNotFound
			continue
		}
		wg.Add(1)
		// Read `xl.json` in routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			var err error
			metadataArray[index], err = readXLMeta(disk, bucket, object)
			if err != nil {
				errs[index] = err
				return
			}
		}(index, disk)
	}

	// Wait for all the routines to finish.
	wg.Wait()

	// Return all the metadata.
	return metadataArray, errs
}

// Return ordered partsMetadata depeinding on distribution.
func getOrderedPartsMetadata(distribution []int, partsMetadata []xlMetaV1) (orderedPartsMetadata []xlMetaV1) {
	orderedPartsMetadata = make([]xlMetaV1, len(partsMetadata))
	for index := range partsMetadata {
		blockIndex := distribution[index]
		orderedPartsMetadata[blockIndex-1] = partsMetadata[index]
	}
	return orderedPartsMetadata
}

// getOrderedDisks - get ordered disks from erasure distribution.
// returns ordered slice of disks from their actual distribution.
func getOrderedDisks(distribution []int, disks []StorageAPI) (orderedDisks []StorageAPI) {
	orderedDisks = make([]StorageAPI, len(disks))
	// From disks gets ordered disks.
	for index := range disks {
		blockIndex := distribution[index]
		orderedDisks[blockIndex-1] = disks[index]
	}
	return orderedDisks
}
