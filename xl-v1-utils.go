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

// Validates if we have quorum based on the errors with errDiskNotFound.
func isQuorum(errs []error, minQuorumCount int) bool {
	var diskFoundCount int
	for _, err := range errs {
		if err == errDiskNotFound {
			continue
		}
		diskFoundCount++
	}
	return diskFoundCount >= minQuorumCount
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
