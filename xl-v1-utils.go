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
	"math/rand"
	"path"
	"time"
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

// randInts - uses Knuth Fisher-Yates shuffle algorithm for generating uniform shuffling.
func randInts(count int) []int {
	rand.Seed(time.Now().UTC().UnixNano()) // Seed with current time.
	ints := make([]int, count)
	for i := 0; i < count; i++ {
		ints[i] = i + 1
	}
	for i := 0; i < count; i++ {
		// Choose index uniformly in [i, count-1]
		r := i + rand.Intn(count-i)
		ints[r], ints[i] = ints[i], ints[r]
	}
	return ints
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
