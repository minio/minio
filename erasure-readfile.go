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
	"encoding/hex"
	"errors"
)

// isValidBlock - calculates the checksum hash for the block and
// validates if its correct returns true for valid cases, false otherwise.
func (e erasureConfig) isValidBlock(volume, path string, blockIdx int) bool {
	diskIndex := -1
	// Find out the right disk index for the input block index.
	for index, blockIndex := range e.distribution {
		if blockIndex == blockIdx {
			diskIndex = index
		}
	}
	// Unknown block index requested, treat it as error.
	if diskIndex == -1 {
		return false
	}
	// Disk is not present, treat entire block to be non existent.
	if e.storageDisks[diskIndex] == nil {
		return false
	}
	// Read everything for a given block and calculate hash.
	hashBytes, err := hashSum(e.storageDisks[diskIndex], volume, path, newHash(e.checkSumAlgo))
	if err != nil {
		return false
	}
	return hex.EncodeToString(hashBytes) == e.hashChecksums[diskIndex]
}

// ReadFile - decoded erasure coded file.
func (e erasureConfig) ReadFile(volume, path string, size int64, blockSize int64) ([]byte, error) {
	// Return data buffer.
	var buffer []byte

	// Total size left
	totalSizeLeft := size

	// Starting offset for reading.
	startOffset := int64(0)

	// Write until each parts are read and exhausted.
	for totalSizeLeft > 0 {
		// Calculate the proper block size.
		var curBlockSize int64
		if blockSize < totalSizeLeft {
			curBlockSize = blockSize
		} else {
			curBlockSize = totalSizeLeft
		}

		// Calculate the current encoded block size.
		curEncBlockSize := getEncodedBlockLen(curBlockSize, e.dataBlocks)
		offsetEncOffset := getEncodedBlockLen(startOffset, e.dataBlocks)

		// Allocate encoded blocks up to storage disks.
		enBlocks := make([][]byte, len(e.storageDisks))

		// Counter to keep success data blocks.
		var successDataBlocksCount = 0
		var noReconstruct bool // Set for no reconstruction.

		// Read from all the disks.
		for index, disk := range e.storageDisks {
			blockIndex := e.distribution[index] - 1
			if !e.isValidBlock(volume, path, blockIndex) {
				continue
			}
			// Initialize shard slice and fill the data from each parts.
			enBlocks[blockIndex] = make([]byte, curEncBlockSize)
			// Read the necessary blocks.
			_, err := disk.ReadFile(volume, path, offsetEncOffset, enBlocks[blockIndex])
			if err != nil {
				enBlocks[blockIndex] = nil
			}
			// Verify if we have successfully read all the data blocks.
			if blockIndex < e.dataBlocks && enBlocks[blockIndex] != nil {
				successDataBlocksCount++
				// Set when we have all the data blocks and no
				// reconstruction is needed, so that we can avoid
				// erasure reconstruction.
				noReconstruct = successDataBlocksCount == e.dataBlocks
				if noReconstruct {
					// Break out we have read all the data blocks.
					break
				}
			}
		}

		// Check blocks if they are all zero in length, we have corruption return error.
		if checkBlockSize(enBlocks) == 0 {
			return nil, errDataCorrupt
		}

		// Verify if reconstruction is needed, proceed with reconstruction.
		if !noReconstruct {
			err := e.reedSolomon.Reconstruct(enBlocks)
			if err != nil {
				return nil, err
			}
			// Verify reconstructed blocks (parity).
			ok, err := e.reedSolomon.Verify(enBlocks)
			if err != nil {
				return nil, err
			}
			if !ok {
				// Blocks cannot be reconstructed, corrupted data.
				err = errors.New("Verification failed after reconstruction, data likely corrupted.")
				return nil, err
			}
		}

		// Get data blocks from encoded blocks.
		dataBlocks, err := getDataBlocks(enBlocks, e.dataBlocks, int(curBlockSize))
		if err != nil {
			return nil, err
		}

		// Copy data blocks.
		buffer = append(buffer, dataBlocks...)

		// Negate the 'n' size written to client.
		totalSizeLeft -= int64(len(dataBlocks))

		// Increase the offset to move forward.
		startOffset += int64(len(dataBlocks))

		// Relenquish memory.
		dataBlocks = nil
	}
	return buffer, nil
}
