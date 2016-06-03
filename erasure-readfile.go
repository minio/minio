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

	"github.com/klauspost/reedsolomon"
)

// erasureReadFile - read an entire erasure coded file at into a byte
// array. Erasure coded parts are often few mega bytes in size and it
// is convenient to return them as byte slice. This function also
// supports bit-rot detection by verifying checksum of individual
// block's checksum.
func erasureReadFile(disks []StorageAPI, volume string, path string, partName string, size int64, eInfos []erasureInfo) ([]byte, error) {
	// Return data buffer.
	var buffer []byte

	// Total size left
	totalSizeLeft := size

	// Starting offset for reading.
	startOffset := int64(0)

	// Gather previously calculated block checksums.
	blockCheckSums := metaPartBlockChecksums(disks, eInfos, partName)

	// Pick one erasure info.
	eInfo := pickValidErasureInfo(eInfos)

	// Write until each parts are read and exhausted.
	for totalSizeLeft > 0 {
		// Calculate the proper block size.
		var curBlockSize int64
		if eInfo.BlockSize < totalSizeLeft {
			curBlockSize = eInfo.BlockSize
		} else {
			curBlockSize = totalSizeLeft
		}

		// Calculate the current encoded block size.
		curEncBlockSize := getEncodedBlockLen(curBlockSize, eInfo.DataBlocks)
		offsetEncOffset := getEncodedBlockLen(startOffset, eInfo.DataBlocks)

		// Allocate encoded blocks up to storage disks.
		enBlocks := make([][]byte, len(disks))

		// Counter to keep success data blocks.
		var successDataBlocksCount = 0
		var noReconstruct bool // Set for no reconstruction.

		// Read from all the disks.
		for index, disk := range disks {
			blockIndex := eInfo.Distribution[index] - 1
			if !isValidBlock(disks, volume, path, toDiskIndex(blockIndex, eInfo.Distribution), blockCheckSums) {
				continue
			}
			if disk == nil {
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
			if blockIndex < eInfo.DataBlocks && enBlocks[blockIndex] != nil {
				successDataBlocksCount++
				// Set when we have all the data blocks and no
				// reconstruction is needed, so that we can avoid
				// erasure reconstruction.
				noReconstruct = successDataBlocksCount == eInfo.DataBlocks
				if noReconstruct {
					// Break out we have read all the data blocks.
					break
				}
			}
		}

		// Check blocks if they are all zero in length, we have corruption return error.
		if checkBlockSize(enBlocks) == 0 {
			return nil, errXLDataCorrupt
		}

		// Verify if reconstruction is needed, proceed with reconstruction.
		if !noReconstruct {
			err := decodeData(enBlocks, eInfo.DataBlocks, eInfo.ParityBlocks)
			if err != nil {
				return nil, err
			}
		}

		// Get data blocks from encoded blocks.
		dataBlocks, err := getDataBlocks(enBlocks, eInfo.DataBlocks, int(curBlockSize))
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

// PartObjectChecksum - returns the checksum for the part name from the checksum slice.
func (e erasureInfo) PartObjectChecksum(partName string) checkSumInfo {
	for _, checksum := range e.Checksum {
		if checksum.Name == partName {
			return checksum
		}
	}
	return checkSumInfo{}
}

// xlMetaPartBlockChecksums - get block checksums for a given part.
func metaPartBlockChecksums(disks []StorageAPI, eInfos []erasureInfo, partName string) (blockCheckSums []checkSumInfo) {
	for index := range disks {
		if eInfos[index].IsValid() {
			// Save the read checksums for a given part.
			blockCheckSums = append(blockCheckSums, eInfos[index].PartObjectChecksum(partName))
		} else {
			blockCheckSums = append(blockCheckSums, checkSumInfo{})
		}
	}
	return blockCheckSums
}

// Takes block index and block distribution to get the disk index.
func toDiskIndex(blockIdx int, distribution []int) (diskIndex int) {
	diskIndex = -1
	// Find out the right disk index for the input block index.
	for index, blockIndex := range distribution {
		if blockIndex == blockIdx {
			diskIndex = index
		}
	}
	return diskIndex
}

// isValidBlock - calculates the checksum hash for the block and
// validates if its correct returns true for valid cases, false otherwise.
func isValidBlock(disks []StorageAPI, volume, path string, diskIndex int, blockCheckSums []checkSumInfo) bool {
	// Unknown block index requested, treat it as error.
	if diskIndex == -1 {
		return false
	}
	// Disk is not present, treat entire block to be non existent.
	if disks[diskIndex] == nil {
		return false
	}
	// Read everything for a given block and calculate hash.
	hashWriter := newHash(blockCheckSums[diskIndex].Algorithm)
	hashBytes, err := hashSum(disks[diskIndex], volume, path, hashWriter)
	if err != nil {
		return false
	}
	return hex.EncodeToString(hashBytes) == blockCheckSums[diskIndex].Hash
}

// decodeData - decode encoded blocks.
func decodeData(enBlocks [][]byte, dataBlocks, parityBlocks int) error {
	rs, err := reedsolomon.New(dataBlocks, parityBlocks)
	if err != nil {
		return err
	}
	err = rs.Reconstruct(enBlocks)
	if err != nil {
		return err
	}
	// Verify reconstructed blocks (parity).
	ok, err := rs.Verify(enBlocks)
	if err != nil {
		return err
	}
	if !ok {
		// Blocks cannot be reconstructed, corrupted data.
		err = errors.New("Verification failed after reconstruction, data likely corrupted.")
		return err
	}
	return nil
}
