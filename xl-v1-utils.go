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

// Returns nil even if one of the slice elements is nil.
// Else returns the error which occurs the most.
func reduceErrs(errs []error) error {
	// In case the error type is not in the known error list.
	var unknownErr = errors.New("unknown error")
	var errTypes = []struct {
		err   error // error type
		count int   // occurrence count
	}{
		// List of known error types. Any new type that can be returned from StorageAPI should
		// be added to this list. Most common errors are listed here.
		{errDiskNotFound, 0}, {errFaultyDisk, 0}, {errFileAccessDenied, 0},
		{errFileNotFound, 0}, {errFileNameTooLong, 0}, {errVolumeNotFound, 0},
		{errDiskFull, 0},
		// unknownErr count - count of the number of unknown errors.
		{unknownErr, 0},
	}
	// In case unknownErr count occurs maximum number of times, unknownErrType is used to
	// to store it so that it can be used for the return error type.
	var unknownErrType error

	// For each err in []errs increment the corresponding count value.
	for _, err := range errs {
		if err == nil {
			// Return nil even if one of the elements is nil.
			return nil
		}
		for i := range errTypes {
			if errTypes[i].err == err {
				errTypes[i].count++
				break
			}
			if errTypes[i].err == unknownErr {
				errTypes[i].count++
				unknownErrType = err
				break
			}
		}
	}
	max := 0
	// Get the error type which has the maximum count.
	for i, errType := range errTypes {
		if errType.count > errTypes[max].count {
			max = i
		}
	}
	if errTypes[max].err == unknownErr {
		// Return the unknown error.
		return unknownErrType
	}
	return errTypes[max].err
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
