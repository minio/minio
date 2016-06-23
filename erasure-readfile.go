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
	"sync"

	"github.com/klauspost/reedsolomon"
)

// isSuccessDecodeBlocks - do we have all the blocks to be successfully decoded?.
// input disks here are expected to be ordered i.e parityBlocks
// are preceded by dataBlocks. For for information look at getOrderedDisks().
func isSuccessDecodeBlocks(disks []StorageAPI, dataBlocks int) bool {
	// Count number of data and parity blocks that were read.
	var successDataBlocksCount = 0
	var successParityBlocksCount = 0
	for index, disk := range disks {
		if disk == nil {
			continue
		}
		if index < dataBlocks {
			successDataBlocksCount++
			continue
		}
		successParityBlocksCount++
	}
	// Returns true if we have atleast dataBlocks + 1 parity.
	return successDataBlocksCount+successParityBlocksCount >= dataBlocks+1
}

// isSuccessDataBlocks - do we have all the data blocks?
// input disks here are expected to be ordered i.e parityBlocks
// are preceded by dataBlocks. For for information look at getOrderedDisks().
func isSuccessDataBlocks(disks []StorageAPI, dataBlocks int) bool {
	// Count number of data blocks that were read.
	var successDataBlocksCount = 0
	for index, disk := range disks[:dataBlocks] {
		if disk == nil {
			continue
		}
		if index < dataBlocks {
			successDataBlocksCount++
		}
	}
	// Returns true if we have all the dataBlocks.
	return successDataBlocksCount >= dataBlocks
}

// getOrderedDisks - get ordered disks from erasure distribution.
// returns ordered slice of disks from their actual distribution.
func getOrderedDisks(distribution []int, disks []StorageAPI, blockCheckSums []checkSumInfo) (orderedDisks []StorageAPI, orderedBlockCheckSums []checkSumInfo) {
	orderedDisks = make([]StorageAPI, len(disks))
	orderedBlockCheckSums = make([]checkSumInfo, len(disks))
	// From disks gets ordered disks.
	for index := range disks {
		blockIndex := distribution[index]
		orderedDisks[blockIndex-1] = disks[index]
		orderedBlockCheckSums[blockIndex-1] = blockCheckSums[index]
	}
	return orderedDisks, orderedBlockCheckSums
}

// erasureReadFile - read bytes from erasure coded files and writes to given writer.
// Erasure coded files are read block by block as per given erasureInfo and data chunks
// are decoded into a data block. Data block is trimmed for given offset and length,
// then written to given writer. This function also supports bit-rot detection by
// verifying checksum of individual block's checksum.
func erasureReadFile(writer io.Writer, disks []StorageAPI, volume string, path string, partName string, eInfos []erasureInfo, offset int64, length int64, totalLength int64) (int64, error) {
	// Pick one erasure info.
	eInfo := pickValidErasureInfo(eInfos)

	// Gather previously calculated block checksums.
	blockCheckSums := metaPartBlockChecksums(disks, eInfos, partName)

	// []orderedDisks will have first eInfo.DataBlocks disks as data
	// disks and rest will be parity.
	orderedDisks, orderedBlockCheckSums := getOrderedDisks(eInfo.Distribution, disks, blockCheckSums)

	// bitrotVerify verifies if the file on a particular disk doesn't have bitrot
	// by verifying the hash of the contents of the file.
	bitrotVerify := func() func(diskIndex int) bool {
		verified := make([]bool, len(orderedDisks))
		// Return closure so that we have reference to []verified and
		// not recalculate the hash on it everytime the function is
		// called for the same disk.
		return func(diskIndex int) bool {
			if verified[diskIndex] {
				// Already validated.
				return true
			}
			// Is this a valid block?
			isValid := isValidBlock(orderedDisks[diskIndex], volume, path, orderedBlockCheckSums[diskIndex])
			verified[diskIndex] = isValid
			return isValid
		}
	}()

	// Total bytes written to writer
	bytesWritten := int64(0)

	// Each element of enBlocks holds curChunkSize'd amount of data read from its corresponding disk.
	enBlocks := make([][]byte, len(orderedDisks))

	// chunkSize is roughly BlockSize/DataBlocks.
	// chunkSize is calculated such that chunkSize*DataBlocks accommodates BlockSize bytes.
	// So chunkSize*DataBlocks can be slightly larger than BlockSize if BlockSize is not divisible by
	// DataBlocks. The extra space will have 0-padding.
	chunkSize := getEncodedBlockLen(eInfo.BlockSize, eInfo.DataBlocks)

	// Get start and end block, also bytes to be skipped based on the input offset.
	startBlock, endBlock, bytesToSkip := getBlockInfo(offset, totalLength, eInfo.BlockSize)

	// For each block, read chunk from each disk. If we are able to read all the data disks then we don't
	// need to read parity disks. If one of the data disk is missing we need to read DataBlocks+1 number
	// of disks. Once read, we Reconstruct() missing data if needed and write it to the given writer.
	for block := startBlock; bytesWritten < length; block++ {
		// curChunkSize is chunkSize until end block.
		curChunkSize := chunkSize
		if block == endBlock && (totalLength%eInfo.BlockSize != 0) {
			// If this is the last block and size of the block is < BlockSize.
			curChunkSize = getEncodedBlockLen(totalLength%eInfo.BlockSize, eInfo.DataBlocks)
		}

		// Block offset.
		// NOTE: That for the offset calculation we have to use chunkSize and
		// not curChunkSize. If we use curChunkSize for offset calculation
		// then it can result in wrong offset for the last block.
		blockOffset := block * chunkSize

		// Figure out the number of disks that are needed for the read.
		// We will need DataBlocks number of disks if all the data disks are up.
		// We will need DataBlocks+1 number of disks even if one of the data disks is down.
		readableDiskCount := 0

		// Count the number of data disks that are up.
		for _, disk := range orderedDisks[:eInfo.DataBlocks] {
			if disk == nil {
				continue
			}
			readableDiskCount++
		}

		// Readable disks..
		if readableDiskCount < eInfo.DataBlocks {
			// Not enough data disks up, so we need DataBlocks+1 number
			// of disks for reed-solomon Reconstruct()
			readableDiskCount = eInfo.DataBlocks + 1
		}

		// Initialize wait group.
		var wg = &sync.WaitGroup{}

		// Current disk index from which to read, this will be used later
		// in case one of the parallel reads fails.
		index := 0

		// Read from the disks in parallel.
		for _, disk := range orderedDisks {
			if disk == nil {
				index++
				continue
			}

			// Increment wait group.
			wg.Add(1)

			// Start reading from disk in a go-routine.
			go func(index int, disk StorageAPI) {
				defer wg.Done()

				// Verify bit rot for this disk slice.
				if !bitrotVerify(index) {
					// So that we don't read from this disk for the next block.
					orderedDisks[index] = nil
					return
				}

				// Chunk writer.
				chunkWriter := bytes.NewBuffer(make([]byte, 0, curChunkSize))

				// CopyN copies until current chunk size.
				err := copyN(chunkWriter, disk, volume, path, blockOffset, curChunkSize)
				if err != nil {
					// So that we don't read from this disk for the next block.
					orderedDisks[index] = nil
					return
				}

				// Copy the read blocks.
				enBlocks[index] = chunkWriter.Bytes()

				// Reset the buffer.
				chunkWriter.Reset()

				// Successfully read.
			}(index, disk)

			index++
			readableDiskCount--
			// We have read all the readable disks.
			if readableDiskCount == 0 {
				break
			}
		}

		// Wait for all the reads to finish.
		wg.Wait()

		// FIXME: make this parallel.

		// If we have all the data blocks no need to decode.
		if !isSuccessDataBlocks(orderedDisks, eInfo.DataBlocks) {
			// If we don't have DataBlocks number of data blocks we
			// will have to read enough parity blocks such that we
			// have DataBlocks+1 number for blocks for rs.Reconstruct().
			// index is either dataBlocks or dataBlocks + 1.
			for ; index < len(orderedDisks); index++ {
				// We have enough blocks to decode, break out.
				if isSuccessDecodeBlocks(orderedDisks, eInfo.DataBlocks) {
					// We have DataBlocks+1 blocks, enough for rs.Reconstruct()
					break
				}

				// This disk was previously set to nil and ignored, do not read again.
				if orderedDisks[index] == nil {
					continue
				}

				// Verify bit-rot for this index.
				if !bitrotVerify(index) {
					// Mark nil so that we don't read from this disk for the next block.
					orderedDisks[index] = nil
					continue
				}

				// Chunk writer.
				chunkWriter := bytes.NewBuffer(make([]byte, 0, curChunkSize))

				// CopyN copies until current chunk size.
				err := copyN(chunkWriter, orderedDisks[index], volume, path, blockOffset, curChunkSize)
				if err != nil {
					// ERROR: Mark nil so that we don't read from
					// this disk for the next block.
					orderedDisks[index] = nil
					continue
				}

				// Copy the read blocks.
				chunkWriter.Read(enBlocks[index])

				// Reset the buffer.
				chunkWriter.Reset()
			}

			// Reconstruct the missing data blocks.
			err := decodeData(enBlocks, eInfo.DataBlocks, eInfo.ParityBlocks)
			if err != nil {
				return bytesWritten, err
			}
			// Success.
		}

		var outSize, outOffset int64
		// enBlocks data can have 0-padding hence we need to figure the exact number
		// of bytes we want to read from enBlocks.
		blockSize := eInfo.BlockSize
		if block == endBlock && totalLength%eInfo.BlockSize != 0 {
			// For the last block, the block size can be less than BlockSize.
			blockSize = totalLength % eInfo.BlockSize
		}

		// If this is start block, skip unwanted bytes.
		if block == startBlock {
			outOffset = bytesToSkip
		}

		// Total data to be read.
		outSize = blockSize
		if length-bytesWritten < blockSize {
			// We should not send more data than what was requested.
			outSize = length - bytesWritten
		}

		// Write data blocks.
		n, err := writeDataBlocks(writer, enBlocks, eInfo.DataBlocks, outOffset, outSize)
		if err != nil {
			return bytesWritten, err
		}

		// Update total bytes written.
		bytesWritten += n
	}

	// Success.
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
func isValidBlock(disk StorageAPI, volume, path string, blockCheckSum checkSumInfo) (ok bool) {
	ok = false
	if disk == nil {
		return false
	}
	// Read everything for a given block and calculate hash.
	hashWriter := newHash(blockCheckSum.Algorithm)
	hashBytes, err := hashSum(disk, volume, path, hashWriter)
	if err != nil {
		return ok
	}
	ok = hex.EncodeToString(hashBytes) == blockCheckSum.Hash
	return ok
}

// decodeData - decode encoded blocks.
func decodeData(enBlocks [][]byte, dataBlocks, parityBlocks int) error {
	// Initialized reedsolomon.
	rs, err := reedsolomon.New(dataBlocks, parityBlocks)
	if err != nil {
		return err
	}

	// Reconstruct encoded blocks.
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

	// Success.
	return nil
}
