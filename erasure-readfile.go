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

// erasureReadFile - read bytes from erasure coded files and writes to given writer.
// Erasure coded files are read block by block as per given erasureInfo and data chunks
// are decoded into a data block.  Data block is trimmed for given offset and length,
// then written to given writer.  This function also supports bit-rot detection by
// verifying checksum of individual block's checksum.
func erasureReadFile(writer io.Writer, disks []StorageAPI, volume string, path string, partName string, eInfos []erasureInfo, offset int64, length int64, totalLength int64) (int64, error) {
	min := func(a int64, b int64) int {
		if a < b {
			return int(a)
		}
		return int(b)
	}
	// Total bytes written to writer
	bytesWritten := int64(0)

	// Gather previously calculated block checksums.
	// blockCheckSums := metaPartBlockChecksums(disks, eInfos, partName)

	// Pick one erasure info.
	eInfo := pickValidErasureInfo(eInfos)

	// Data chunk size on each block.
	chunkSize := getEncodedBlockLen(eInfo.BlockSize, eInfo.DataBlocks)

	// Get block info for given offset, length and block size.
	startBlock, bytesToSkip := getBlockInfo(offset, eInfo.BlockSize)

	orderedDisks := make([]StorageAPI, len(disks))
	for index := range disks {
		blockIndex := eInfo.Distribution[index]
		orderedDisks[blockIndex-1] = disks[index]
	}

	for block := startBlock; bytesWritten < length; block++ {
		curChunkSize := chunkSize
		if totalLength-offset+bytesWritten < curChunkSize {
			curChunkSize = getEncodedBlockLen(totalLength-offset+bytesWritten, eInfo.DataBlocks)
		}

		// Allocate encoded blocks up to storage disks.
		enBlocks := make([][]byte, len(disks))

		// Figure out the number of disks that are needed for the read.
		// If all the data disks are available then dataDiskCount = eInfo.DataBlocks
		// Else dataDiskCount = eInfo.DataBlocks + 1

		diskCount := 0
		for _, disk := range orderedDisks[:eInfo.DataBlocks] {
			if disk == nil {
				continue
			}
			diskCount++
		}

		if diskCount < eInfo.DataBlocks {
			diskCount = eInfo.DataBlocks + 1
		}

		wg := &sync.WaitGroup{}
		index := 0
		for _, disk := range orderedDisks {
			if disk == nil {
				index++
				continue
			}
			wg.Add(1)
			go func(index int, disk StorageAPI) {
				defer wg.Done()
				buf := make([]byte, curChunkSize)
				n, err := disk.ReadFile(volume, path, block*curChunkSize, buf)
				if err != nil {
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

		// Counter to keep success data blocks.
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
			for ; index < len(orderedDisks); index++ {
				if (successDataBlocksCount + successParityBlocksCount) == (eInfo.DataBlocks + 1) {
					break
				}
				buf := make([]byte, curChunkSize)
				n, err := orderedDisks[index].ReadFile(volume, path, block*curChunkSize, buf)
				if err != nil {
					orderedDisks[index] = nil
					continue
				}
				successParityBlocksCount++
				enBlocks[index] = buf[:n]
			}
			err := decodeData(enBlocks, eInfo.DataBlocks, eInfo.ParityBlocks)
			if err != nil {
				return bytesWritten, err
			}
		}

		// Get data blocks from encoded blocks.
		dataBlocks, err := getDataBlocks(enBlocks, eInfo.DataBlocks, min(eInfo.BlockSize, totalLength-offset+bytesWritten))
		if err != nil {
			return bytesWritten, err
		}

		// Keep required bytes into buf.
		buf := dataBlocks

		// If this is start block, skip unwanted bytes.
		if block == startBlock {
			buf = buf[bytesToSkip:]
		}

		if len(buf) > int(length-bytesWritten) {
			buf = buf[:length-bytesWritten]
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
