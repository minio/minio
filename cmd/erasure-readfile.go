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

package cmd

import (
	"encoding/hex"
	"errors"
	"io"
	"sync"

	"github.com/klauspost/reedsolomon"
	"github.com/minio/minio/pkg/bpool"
)

// isSuccessDecodeBlocks - do we have all the blocks to be
// successfully decoded?. Input encoded blocks ordered matrix.
func isSuccessDecodeBlocks(enBlocks [][]byte, dataBlocks int) bool {
	// Count number of data and parity blocks that were read.
	var successDataBlocksCount = 0
	var successParityBlocksCount = 0
	for index := range enBlocks {
		if enBlocks[index] == nil {
			continue
		}
		// block index lesser than data blocks, update data block count.
		if index < dataBlocks {
			successDataBlocksCount++
			continue
		} // else { // update parity block count.
		successParityBlocksCount++
	}
	// Returns true if we have atleast dataBlocks parity.
	return successDataBlocksCount == dataBlocks || successDataBlocksCount+successParityBlocksCount >= dataBlocks
}

// isSuccessDataBlocks - do we have all the data blocks?
// Input encoded blocks ordered matrix.
func isSuccessDataBlocks(enBlocks [][]byte, dataBlocks int) bool {
	// Count number of data blocks that were read.
	var successDataBlocksCount = 0
	for index := range enBlocks[:dataBlocks] {
		if enBlocks[index] == nil {
			continue
		}
		// block index lesser than data blocks, update data block count.
		if index < dataBlocks {
			successDataBlocksCount++
		}
	}
	// Returns true if we have atleast the dataBlocks.
	return successDataBlocksCount >= dataBlocks
}

// Return readable disks slice from which we can read parallelly.
func getReadDisks(orderedDisks []StorageAPI, index int, dataBlocks int) (readDisks []StorageAPI, nextIndex int, err error) {
	readDisks = make([]StorageAPI, len(orderedDisks))
	dataDisks := 0
	parityDisks := 0
	// Count already read data and parity chunks.
	for i := 0; i < index; i++ {
		if orderedDisks[i] == nil {
			continue
		}
		if i < dataBlocks {
			dataDisks++
		} else {
			parityDisks++
		}
	}

	// Sanity checks - we should never have this situation.
	if dataDisks == dataBlocks {
		return nil, 0, traceError(errUnexpected)
	}
	if dataDisks+parityDisks >= dataBlocks {
		return nil, 0, traceError(errUnexpected)
	}

	// Find the disks from which next set of parallel reads should happen.
	for i := index; i < len(orderedDisks); i++ {
		if orderedDisks[i] == nil {
			continue
		}
		if i < dataBlocks {
			dataDisks++
		} else {
			parityDisks++
		}
		readDisks[i] = orderedDisks[i]
		if dataDisks == dataBlocks {
			return readDisks, i + 1, nil
		} else if dataDisks+parityDisks == dataBlocks {
			return readDisks, i + 1, nil
		}
	}
	return nil, 0, traceError(errXLReadQuorum)
}

// parallelRead - reads chunks in parallel from the disks specified in []readDisks.
func parallelRead(volume, path string, readDisks []StorageAPI, orderedDisks []StorageAPI, enBlocks [][]byte, blockOffset int64, curChunkSize int64, bitRotVerify func(diskIndex int) bool, pool *bpool.BytePool) {
	// WaitGroup to synchronise the read go-routines.
	wg := &sync.WaitGroup{}

	// Read disks in parallel.
	for index := range readDisks {
		if readDisks[index] == nil {
			continue
		}
		wg.Add(1)
		// Reads chunk from readDisk[index] in routine.
		go func(index int) {
			defer wg.Done()

			// Verify bit rot for the file on this disk.
			if !bitRotVerify(index) {
				// So that we don't read from this disk for the next block.
				orderedDisks[index] = nil
				return
			}

			buf, err := pool.Get()
			if err != nil {
				errorIf(err, "unable to get buffer from byte pool")
				orderedDisks[index] = nil
				return
			}
			buf = buf[:curChunkSize]

			_, err = readDisks[index].ReadFile(volume, path, blockOffset, buf)
			if err != nil {
				orderedDisks[index] = nil
				return
			}
			enBlocks[index] = buf
		}(index)
	}

	// Waiting for first routines to finish.
	wg.Wait()
}

// erasureReadFile - read bytes from erasure coded files and writes to given writer.
// Erasure coded files are read block by block as per given erasureInfo and data chunks
// are decoded into a data block. Data block is trimmed for given offset and length,
// then written to given writer. This function also supports bit-rot detection by
// verifying checksum of individual block's checksum.
func erasureReadFile(writer io.Writer, disks []StorageAPI, volume string, path string, offset int64, length int64, totalLength int64, blockSize int64, dataBlocks int, parityBlocks int, checkSums []string, algo string, pool *bpool.BytePool) (int64, error) {
	// Offset and length cannot be negative.
	if offset < 0 || length < 0 {
		return 0, traceError(errUnexpected)
	}

	// Can't request more data than what is available.
	if offset+length > totalLength {
		return 0, traceError(errUnexpected)
	}

	// chunkSize is the amount of data that needs to be read from each disk at a time.
	chunkSize := getChunkSize(blockSize, dataBlocks)

	// bitRotVerify verifies if the file on a particular disk doesn't have bitrot
	// by verifying the hash of the contents of the file.
	bitRotVerify := func() func(diskIndex int) bool {
		verified := make([]bool, len(disks))
		// Return closure so that we have reference to []verified and
		// not recalculate the hash on it every time the function is
		// called for the same disk.
		return func(diskIndex int) bool {
			if verified[diskIndex] {
				// Already validated.
				return true
			}
			// Is this a valid block?
			isValid := isValidBlock(disks[diskIndex], volume, path, checkSums[diskIndex], algo)
			verified[diskIndex] = isValid
			return isValid
		}
	}()

	// Total bytes written to writer
	bytesWritten := int64(0)

	startBlock := offset / blockSize
	endBlock := (offset + length) / blockSize

	// curChunkSize = chunk size for the current block in the for loop below.
	// curBlockSize = block size for the current block in the for loop below.
	// curChunkSize and curBlockSize can change for the last block if totalLength%blockSize != 0
	curChunkSize := chunkSize
	curBlockSize := blockSize

	// For each block, read chunk from each disk. If we are able to read all the data disks then we don't
	// need to read parity disks. If one of the data disk is missing we need to read DataBlocks+1 number
	// of disks. Once read, we Reconstruct() missing data if needed and write it to the given writer.
	for block := startBlock; block <= endBlock; block++ {
		// Mark all buffers as unused at the start of the loop so that the buffers
		// can be reused.
		pool.Reset()

		// Each element of enBlocks holds curChunkSize'd amount of data read from its corresponding disk.
		enBlocks := make([][]byte, len(disks))

		if ((offset + bytesWritten) / blockSize) == (totalLength / blockSize) {
			// This is the last block for which curBlockSize and curChunkSize can change.
			// For ex. if totalLength is 15M and blockSize is 10MB, curBlockSize for
			// the last block should be 5MB.
			curBlockSize = totalLength % blockSize
			curChunkSize = getChunkSize(curBlockSize, dataBlocks)
		}

		// NOTE: That for the offset calculation we have to use chunkSize and
		// not curChunkSize. If we use curChunkSize for offset calculation
		// then it can result in wrong offset for the last block.
		blockOffset := block * chunkSize

		// nextIndex - index from which next set of parallel reads
		// should happen.
		nextIndex := 0

		for {
			// readDisks - disks from which we need to read in parallel.
			var readDisks []StorageAPI
			var err error
			// get readable disks slice from which we can read parallelly.
			readDisks, nextIndex, err = getReadDisks(disks, nextIndex, dataBlocks)
			if err != nil {
				return bytesWritten, err
			}
			// Issue a parallel read across the disks specified in readDisks.
			parallelRead(volume, path, readDisks, disks, enBlocks, blockOffset, curChunkSize, bitRotVerify, pool)
			if isSuccessDecodeBlocks(enBlocks, dataBlocks) {
				// If enough blocks are available to do rs.Reconstruct()
				break
			}
			if nextIndex == len(disks) {
				// No more disks to read from.
				return bytesWritten, traceError(errXLReadQuorum)
			}
			// We do not have enough enough data blocks to reconstruct the data
			// hence continue the for-loop till we have enough data blocks.
		}

		// If we have all the data blocks no need to decode, continue to write.
		if !isSuccessDataBlocks(enBlocks, dataBlocks) {
			// Reconstruct the missing data blocks.
			if err := decodeData(enBlocks, dataBlocks, parityBlocks); err != nil {
				return bytesWritten, err
			}
		}

		// Offset in enBlocks from where data should be read from.
		enBlocksOffset := int64(0)

		// Total data to be read from enBlocks.
		enBlocksLength := curBlockSize

		// If this is the start block then enBlocksOffset might not be 0.
		if block == startBlock {
			enBlocksOffset = offset % blockSize
			enBlocksLength -= enBlocksOffset
		}

		remaining := length - bytesWritten
		if remaining < enBlocksLength {
			// We should not send more data than what was requested.
			enBlocksLength = remaining
		}

		// Write data blocks.
		n, err := writeDataBlocks(writer, enBlocks, dataBlocks, enBlocksOffset, enBlocksLength)
		if err != nil {
			return bytesWritten, err
		}

		// Update total bytes written.
		bytesWritten += n

		if bytesWritten == length {
			// Done writing all the requested data.
			break
		}
	}

	// Success.
	return bytesWritten, nil
}

// isValidBlock - calculates the checksum hash for the block and
// validates if its correct returns true for valid cases, false otherwise.
func isValidBlock(disk StorageAPI, volume, path, checkSum, checkSumAlgo string) (ok bool) {
	// Disk is not available, not a valid block.
	if disk == nil {
		return false
	}
	// Checksum not available, not a valid block.
	if checkSum == "" {
		return false
	}
	// Read everything for a given block and calculate hash.
	hashWriter := newHash(checkSumAlgo)
	hashBytes, err := hashSum(disk, volume, path, hashWriter)
	if err != nil {
		errorIf(err, "Unable to calculate checksum %s/%s", volume, path)
		return false
	}
	return hex.EncodeToString(hashBytes) == checkSum
}

// decodeData - decode encoded blocks.
func decodeData(enBlocks [][]byte, dataBlocks, parityBlocks int) error {
	// Initialized reedsolomon.
	rs, err := reedsolomon.New(dataBlocks, parityBlocks)
	if err != nil {
		return traceError(err)
	}

	// Reconstruct encoded blocks.
	err = rs.Reconstruct(enBlocks)
	if err != nil {
		return traceError(err)
	}

	// Verify reconstructed blocks (parity).
	ok, err := rs.Verify(enBlocks)
	if err != nil {
		return traceError(err)
	}
	if !ok {
		// Blocks cannot be reconstructed, corrupted data.
		err = errors.New("Verification failed after reconstruction, data likely corrupted")
		return traceError(err)
	}

	// Success.
	return nil
}
