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
	"encoding/json"
	"errors"
	"hash/crc32"
	"path"
)

// Returns number of errors that occurred the most (incl. nil) and the
// corresponding error value. N B when there is more than one error value that
// occurs maximum number of times, the error value returned depends on how
// golang's map orders keys. This doesn't affect correctness as long as quorum
// value is greater than or equal to simple majority, since none of the equally
// maximal values would occur quorum or more number of times.

func reduceErrs(errs []error) (int, error) {
	errorCounts := make(map[error]int)
	for _, err := range errs {
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
	return max, errMax
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

// hashOrder - returns consistent hashed integers of count slice, based on the input token.
func hashOrder(token string, count int) []int {
	if count < 0 {
		panic(errors.New("hashOrder count cannot be negative"))
	}
	nums := make([]int, count)
	tokenCrc := crc32.Checksum([]byte(token), crc32.IEEETable)

	start := int(uint32(tokenCrc)%uint32(count)) | 1
	for i := 1; i <= count; i++ {
		nums[i-1] = 1 + ((start + i) % count)
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

// Uses a map to find union of checksums of parts that were concurrently written
// but committed before this part. N B For a different, concurrent upload of
// the same part, the ongoing request's data/metadata prevails.
// cur     - corresponds to parts written to disk before the ongoing putObjectPart request
// updated - corresponds to parts written to disk while the ongoing putObjectPart is in progress
// curPartName - name of the part that is being written
// returns []checkSumInfo containing the set union of checksums of parts that
// have been written so far incl. the part being written.
func unionChecksumInfos(cur []checkSumInfo, updated []checkSumInfo, curPartName string) []checkSumInfo {
	checksumSet := make(map[string]checkSumInfo)
	var checksums []checkSumInfo

	checksums = cur
	for _, cksum := range checksums {
		checksumSet[cksum.Name] = cksum
	}

	checksums = updated
	for _, cksum := range checksums {
		// skip updating checksum of the part that is
		// written in this request because the checksum
		// from cur, corresponding to this part,
		// should remain.
		if cksum.Name == curPartName {
			continue
		}
		checksumSet[cksum.Name] = cksum
	}

	// Form the checksumInfo to be committed in xl.json
	// from the map.
	var finalChecksums []checkSumInfo
	for _, cksum := range checksumSet {
		finalChecksums = append(finalChecksums, cksum)
	}
	return finalChecksums
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
