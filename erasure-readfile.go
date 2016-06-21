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
	"bytes"
	"encoding/hex"
	"errors"
	"io"

	"github.com/klauspost/reedsolomon"
)

// erasureReadFile - read bytes from erasure coded files and writes to given writer.
// Erasure coded files are read block by block as per given erasureInfo and data chunks
// are decoded into a data block.  Data block is trimmed for given offset and length,
// then written to given writer.  This function also supports bit-rot detection by
// verifying checksum of individual block's checksum.
func erasureReadFile(writer io.Writer, disks []StorageAPI, volume string, path string, partName string, eInfos []erasureInfo, offset int64, length int64) (int64, error) {
	// Total bytes written to writer
	bytesWritten := int64(0)

	// Gather previously calculated block checksums.
	blockCheckSums := metaPartBlockChecksums(disks, eInfos, partName)

	// Pick one erasure info.
	eInfo := pickValidErasureInfo(eInfos)

	// Get block info for given offset, length and block size.
	startBlock, bytesToSkip, endBlock := getBlockInfo(offset, length, eInfo.BlockSize)

	// Data chunk size on each block.
	chunkSize := eInfo.BlockSize / int64(eInfo.DataBlocks)

	for block := startBlock; block <= endBlock; block++ {
		// Allocate encoded blocks up to storage disks.
		enBlocks := make([][]byte, len(disks))

		// Counter to keep success data blocks.
		var successDataBlocksCount = 0
		var noReconstruct bool // Set for no reconstruction.

		// Keep how many bytes are read for this block.
		// In most cases, last block in the file is shorter than chunkSize
		lastReadSize := int64(0)

		// Read from all the disks.
		for index, disk := range disks {
			blockIndex := eInfo.Distribution[index] - 1
			if !isValidBlock(disks, volume, path, toDiskIndex(blockIndex, eInfo.Distribution), blockCheckSums) {
				continue
			}
			if disk == nil {
				continue
			}

			// Initialize chunk slice and fill the data from each parts.
			enBlocks[blockIndex] = make([]byte, chunkSize)

			// Read the necessary blocks.
			n, err := disk.ReadFile(volume, path, block*chunkSize, enBlocks[blockIndex])
			if err != nil {
				enBlocks[blockIndex] = nil
			} else if n < chunkSize {
				// As the data we got is smaller than chunk size, keep only required chunk slice
				enBlocks[blockIndex] = append([]byte{}, enBlocks[blockIndex][:n]...)
			}

			// Remember bytes read at first time.
			if lastReadSize == 0 {
				lastReadSize = n
			}

			// If bytes read is not equal to bytes read lastly, treat it as corrupted chunk.
			if n != lastReadSize {
				return bytesWritten, errXLDataCorrupt
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

		// Verify if reconstruction is needed, proceed with reconstruction.
		if !noReconstruct {
			err := decodeData(enBlocks, eInfo.DataBlocks, eInfo.ParityBlocks)
			if err != nil {
				return bytesWritten, err
			}
		}

		// Get data blocks from encoded blocks.
		dataBlocks, err := getDataBlocks(enBlocks, eInfo.DataBlocks, int(lastReadSize)*eInfo.DataBlocks)
		if err != nil {
			return bytesWritten, err
		}

		// Keep required bytes into buf.
		buf := dataBlocks

		// If this is start block, skip unwanted bytes.
		if block == startBlock {
			buf = append([]byte{}, dataBlocks[bytesToSkip:]...)
		}

		// If this is end block, retain only required bytes.
		if block == endBlock {
			buf = append([]byte{}, buf[:length-bytesWritten]...)
		}

		// Copy data blocks.
		var n int64
		n, err = io.Copy(writer, bytes.NewReader(buf))
		bytesWritten += int64(n)
		if err != nil {
			return bytesWritten, err
		}
	}

	return bytesWritten, nil
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
func toDiskIndex(blockIdx int, distribution []int) int {
	// Find out the right disk index for the input block index.
	for index, blockIndex := range distribution {
		if blockIndex-1 == blockIdx {
			return index
		}
	}
	return -1
}

// isValidBlock - calculates the checksum hash for the block and
// validates if its correct returns true for valid cases, false otherwise.
func isValidBlock(disks []StorageAPI, volume, path string, diskIndex int, blockCheckSums []checkSumInfo) (ok bool) {
	ok = false
	// Unknown block index requested, treat it as error.
	if diskIndex == -1 {
		return ok
	}
	// Disk is not present, treat entire block to be non existent.
	if disks[diskIndex] == nil {
		return ok
	}
	// Read everything for a given block and calculate hash.
	hashWriter := newHash(blockCheckSums[diskIndex].Algorithm)
	hashBytes, err := hashSum(disks[diskIndex], volume, path, hashWriter)
	if err != nil {
		return ok
	}
	ok = hex.EncodeToString(hashBytes) == blockCheckSums[diskIndex].Hash
	return ok
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
