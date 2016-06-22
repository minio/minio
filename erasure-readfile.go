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
	"io"
	"sync"

	"github.com/klauspost/reedsolomon"
)

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
	orderedBlockCheckSums := make([]checkSumInfo, len(disks))

	// []orderedDisks will have first eInfo.DataBlocks disks as data disks and rest will be parity.
	orderedDisks := make([]StorageAPI, len(disks))
	for index := range disks {
		blockIndex := eInfo.Distribution[index]
		orderedDisks[blockIndex-1] = disks[index]
		orderedBlockCheckSums[blockIndex-1] = blockCheckSums[index]
	}

	// bitrotVerify verifies if the file on a particular disk does not have bitrot by verifying the hash of
	// the contents of the file.
	bitrotVerify := func() func(diskIndex int) bool {
		verified := make([]bool, len(orderedDisks))
		// Return closure so that we have reference to []verified and not recalculate the hash on it
		// everytime the function is called for the same disk.
		return func(diskIndex int) bool {
			if verified[diskIndex] {
				return true
			}
			isValid := isValidBlock(orderedDisks[diskIndex], volume, path, orderedBlockCheckSums[diskIndex])
			verified[diskIndex] = isValid
			return isValid
		}
	}()

	// Total bytes written to writer
	bytesWritten := int64(0)

	// chunkSize is roughly BlockSize/DataBlocks.
	// chunkSize is calculated such that chunkSize*DataBlocks accommodates BlockSize bytes.
	// So chunkSize*DataBlocks can be slightly larger than BlockSize if BlockSize is not divisible by
	// DataBlocks. The extra space will have 0-padding.
	chunkSize := getEncodedBlockLen(eInfo.BlockSize, eInfo.DataBlocks)

	startBlock, endBlock, bytesToSkip := getBlockInfo(offset, totalLength, eInfo.BlockSize)

	// For each block, read chunk from each disk. If we are able to read all the data disks then we don't
	// need to read parity disks. If one of the data disk is missing we need to read DataBlocks+1 number
	// of disks. Once read, we Reconstruct() missing data if needed and write it to the given writer.
	for block := startBlock; bytesWritten < length; block++ {
		// curChunkSize will be chunkSize except for the last block because the size of the last block
		// can be less than BlockSize.
		curChunkSize := chunkSize
		if block == endBlock && (totalLength%eInfo.BlockSize != 0) {
			// If this is the last block and size of the block is < BlockSize.
			curChunkSize = getEncodedBlockLen(totalLength%eInfo.BlockSize, eInfo.DataBlocks)
		}

		// Each element of enBlocks holds curChunkSize'd amount of data read from its corresponding disk.
		enBlocks := make([][]byte, len(disks))

		// Figure out the number of disks that are needed for the read.
		// We will need DataBlocks number of disks if all the data disks are up.
		// We will need DataBlocks+1 number of disks even if one of the data disks is down.
		diskCount := 0
		// Count the number of data disks that are up.
		for _, disk := range orderedDisks[:eInfo.DataBlocks] {
			if disk == nil {
				continue
			}
			diskCount++
		}

		if diskCount < eInfo.DataBlocks {
			// Not enough data disks up, so we need DataBlocks+1 number of disks for reed-solomon Reconstruct()
			diskCount = eInfo.DataBlocks + 1
		}

		wg := &sync.WaitGroup{}

		// current disk index from which to read, this will be used later in case one of the parallel reads fails.
		index := 0
		// Read from the disks in parallel.
		for _, disk := range orderedDisks {
			if disk == nil {
				index++
				continue
			}
			wg.Add(1)
			go func(index int, disk StorageAPI) {
				defer wg.Done()
				ok := bitrotVerify(index)
				if !ok {
					// So that we don't read from this disk for the next block.
					orderedDisks[index] = nil
					return
				}
				buf := make([]byte, curChunkSize)
				// Note that for the offset calculation we have to use chunkSize and not
				// curChunkSize. If we use curChunkSize for offset calculation then it
				// can result in wrong offset for the last block.
				n, err := disk.ReadFile(volume, path, block*chunkSize, buf)
				if err != nil {
					// So that we don't read from this disk for the next block.
					orderedDisks[index] = nil
					return
				}
				enBlocks[index] = buf[:n]
			}(index, disk)
			index++
			diskCount--
			if diskCount == 0 {
				break
			}
		}
		wg.Wait()

		// Count number of data and parity blocks that were read.
		var successDataBlocksCount = 0
		var successParityBlocksCount = 0
		for bufidx, buf := range enBlocks {
			if buf == nil {
				continue
			}
			if bufidx < eInfo.DataBlocks {
				successDataBlocksCount++
				continue
			}
			successParityBlocksCount++
		}

		if successDataBlocksCount < eInfo.DataBlocks {
			// If we don't have DataBlocks number of data blocks we will have to read enough
			// parity blocks such that we have DataBlocks+1 number for blocks for reedsolomon.Reconstruct()
			for ; index < len(orderedDisks); index++ {
				if (successDataBlocksCount + successParityBlocksCount) == (eInfo.DataBlocks + 1) {
					// We have DataBlocks+1 blocks, enough for reedsolomon.Reconstruct()
					break
				}
				ok := bitrotVerify(index)
				if !ok {
					// Mark nil so that we don't read from this disk for the next block.
					orderedDisks[index] = nil
					continue
				}
				buf := make([]byte, curChunkSize)
				n, err := orderedDisks[index].ReadFile(volume, path, block*chunkSize, buf)
				if err != nil {
					// Mark nil so that we don't read from this disk for the next block.
					orderedDisks[index] = nil
					continue
				}
				successParityBlocksCount++
				enBlocks[index] = buf[:n]
			}
			// Reconstruct the missing data blocks.
			err := decodeData(enBlocks, eInfo.DataBlocks, eInfo.ParityBlocks)
			if err != nil {
				return bytesWritten, err
			}
		}

		// enBlocks data can have 0-padding hence we need to figure the exact number
		// of bytes we want to read from enBlocks.
		blockSize := eInfo.BlockSize
		if block == endBlock && totalLength%eInfo.BlockSize != 0 {
			// For the last block, the block size can be less than BlockSize.
			blockSize = totalLength % eInfo.BlockSize
		}
		data, err := getDataBlocks(enBlocks, eInfo.DataBlocks, int(blockSize))
		if err != nil {
			return bytesWritten, err
		}

		// If this is start block, skip unwanted bytes.
		if block == startBlock {
			data = data[bytesToSkip:]
		}

		if len(data) > int(length-bytesWritten) {
			// We should not send more data than what was requested.
			data = data[:length-bytesWritten]
		}

		_, err = writer.Write(data)
		if err != nil {
			return bytesWritten, err
		}
		bytesWritten += int64(len(data))
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
