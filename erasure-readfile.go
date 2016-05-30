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

import "errors"

// ReadFile - decoded erasure coded file.
func (e erasure) ReadFile(volume, path string, startOffset int64, buffer []byte) (int64, error) {
	// Calculate the current encoded block size.
	curEncBlockSize := getEncodedBlockLen(int64(len(buffer)), e.DataBlocks)
	offsetEncOffset := getEncodedBlockLen(startOffset, e.DataBlocks)

	// Allocate encoded blocks up to storage disks.
	enBlocks := make([][]byte, len(e.storageDisks))

	// Counter to keep success data blocks.
	var successDataBlocksCount = 0
	var noReconstruct bool // Set for no reconstruction.

	// Read from all the disks.
	for index, disk := range e.storageDisks {
		blockIndex := e.distribution[index] - 1
		// Initialize shard slice and fill the data from each parts.
		enBlocks[blockIndex] = make([]byte, curEncBlockSize)
		if disk == nil {
			enBlocks[blockIndex] = nil
			continue
		}
		// Read the necessary blocks.
		_, err := disk.ReadFile(volume, path, offsetEncOffset, enBlocks[blockIndex])
		if err != nil {
			enBlocks[blockIndex] = nil
		}
		// Verify if we have successfully read all the data blocks.
		if blockIndex < e.DataBlocks && enBlocks[blockIndex] != nil {
			successDataBlocksCount++
			// Set when we have all the data blocks and no
			// reconstruction is needed, so that we can avoid
			// erasure reconstruction.
			noReconstruct = successDataBlocksCount == e.DataBlocks
			if noReconstruct {
				// Break out we have read all the data blocks.
				break
			}
		}
	}

	// Check blocks if they are all zero in length, we have corruption return error.
	if checkBlockSize(enBlocks) == 0 {
		return 0, errDataCorrupt
	}

	// Verify if reconstruction is needed, proceed with reconstruction.
	if !noReconstruct {
		err := e.ReedSolomon.Reconstruct(enBlocks)
		if err != nil {
			return 0, err
		}
		// Verify reconstructed blocks (parity).
		ok, err := e.ReedSolomon.Verify(enBlocks)
		if err != nil {
			return 0, err
		}
		if !ok {
			// Blocks cannot be reconstructed, corrupted data.
			err = errors.New("Verification failed after reconstruction, data likely corrupted.")
			return 0, err
		}
	}

	// Get data blocks from encoded blocks.
	dataBlocks, err := getDataBlocks(enBlocks, e.DataBlocks, len(buffer))
	if err != nil {
		return 0, err
	}

	// Copy data blocks.
	copy(buffer, dataBlocks)

	// Relenquish memory.
	dataBlocks = nil

	return int64(len(buffer)), nil
}
