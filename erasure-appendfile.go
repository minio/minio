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

import "sync"

// AppendFile - append data buffer at path.
func (e erasureConfig) AppendFile(volume, path string, dataBuffer []byte) (n int64, err error) {
	// Split the input buffer into data and parity blocks.
	var blocks [][]byte
	blocks, err = e.reedSolomon.Split(dataBuffer)
	if err != nil {
		return 0, err
	}

	// Encode parity blocks using data blocks.
	err = e.reedSolomon.Encode(blocks)
	if err != nil {
		return 0, err
	}

	var wg = &sync.WaitGroup{}
	var wErrs = make([]error, len(e.storageDisks))
	// Write encoded data to quorum disks in parallel.
	for index, disk := range e.storageDisks {
		if disk == nil {
			continue
		}
		wg.Add(1)
		// Write encoded data in routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			// Pick the block from the distribution.
			blockIndex := e.distribution[index] - 1
			n, wErr := disk.AppendFile(volume, path, blocks[blockIndex])
			if wErr != nil {
				wErrs[index] = wErr
				return
			}
			if n != int64(len(blocks[blockIndex])) {
				wErrs[index] = errUnexpected
				return
			}
			// Calculate hash.
			e.hashWriters[blockIndex].Write(blocks[blockIndex])

			// Successfully wrote.
			wErrs[index] = nil
		}(index, disk)
	}

	// Wait for all the appends to finish.
	wg.Wait()

	return int64(len(dataBuffer)), nil
}
