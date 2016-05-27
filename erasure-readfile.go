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
	"errors"
	"io"
	"sync"
)

// ReadFile - decoded erasure coded file.
func (e erasure) ReadFile(volume, path string, startOffset int64, totalSize int64) (io.ReadCloser, error) {
	// Input validation.
	if !isValidVolname(volume) {
		return nil, errInvalidArgument
	}
	if !isValidPath(path) {
		return nil, errInvalidArgument
	}

	var rwg = &sync.WaitGroup{}
	var errs = make([]error, len(e.storageDisks))

	readers := make([]io.ReadCloser, len(e.storageDisks))
	for index, disk := range e.storageDisks {
		if disk == nil {
			continue
		}
		rwg.Add(1)
		go func(index int, disk StorageAPI) {
			defer rwg.Done()
			offset := int64(0)
			reader, err := disk.ReadFile(volume, path, offset)
			if err == nil {
				readers[index] = reader
				return
			}
			errs[index] = err
		}(index, disk)
	}

	// Wait for all readers.
	rwg.Wait()

	// For any errors in reader, we should just error out.
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}

	// Initialize pipe.
	pipeReader, pipeWriter := io.Pipe()

	go func() {
		var totalLeft = totalSize
		// Read until EOF.
		for totalLeft > 0 {
			// Figure out the right blockSize as it was encoded before.
			var curBlockSize int64
			if erasureBlockSize < totalLeft {
				curBlockSize = erasureBlockSize
			} else {
				curBlockSize = totalLeft
			}

			// Calculate the current encoded block size.
			curEncBlockSize := getEncodedBlockLen(curBlockSize, e.DataBlocks)

			// Allocate encoded blocks up to storage disks.
			enBlocks := make([][]byte, len(e.storageDisks))

			// Counter to keep success data blocks.
			var successDataBlocksCount = 0
			var noReconstruct bool // Set for no reconstruction.

			// Read all the readers.
			for index, reader := range readers {
				blockIndex := e.distribution[index] - 1
				// Initialize shard slice and fill the data from each parts.
				enBlocks[blockIndex] = make([]byte, curEncBlockSize)
				if reader == nil {
					enBlocks[blockIndex] = nil
					continue
				}

				// Close the reader when routine returns.
				defer reader.Close()

				// Read the necessary blocks.
				_, rErr := io.ReadFull(reader, enBlocks[blockIndex])
				if rErr != nil && rErr != io.ErrUnexpectedEOF {
					enBlocks[blockIndex] = nil
				}

				// Verify if we have successfully all the data blocks.
				if blockIndex < e.DataBlocks {
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

			// Check blocks if they are all zero in length, we have
			// corruption return error.
			if checkBlockSize(enBlocks) == 0 {
				pipeWriter.CloseWithError(errDataCorrupt)
				return
			}

			// Verify if reconstruction is needed, proceed with reconstruction.
			if !noReconstruct {
				err := e.ReedSolomon.Reconstruct(enBlocks)
				if err != nil {
					pipeWriter.CloseWithError(err)
					return
				}
				// Verify reconstructed blocks (parity).
				ok, err := e.ReedSolomon.Verify(enBlocks)
				if err != nil {
					pipeWriter.CloseWithError(err)
					return
				}
				if !ok {
					// Blocks cannot be reconstructed, corrupted data.
					err = errors.New("Verification failed after reconstruction, data likely corrupted.")
					pipeWriter.CloseWithError(err)
					return
				}
			}

			// Get data blocks from encoded blocks.
			dataBlocks := getDataBlocks(enBlocks, e.DataBlocks, int(curBlockSize))

			// Verify if the offset is right for the block, if not move to the next block.
			if startOffset > 0 {
				startOffset = startOffset - int64(len(dataBlocks))
				// Start offset is greater than or equal to zero, skip the dataBlocks.
				if startOffset >= 0 {
					totalLeft = totalLeft - erasureBlockSize
					continue
				}
				// Now get back the remaining offset if startOffset is negative.
				startOffset = startOffset + int64(len(dataBlocks))
			}

			// Write safely the necessary blocks to the pipe.
			_, err := pipeWriter.Write(dataBlocks[int(startOffset):])
			if err != nil {
				pipeWriter.CloseWithError(err)
				return
			}

			// Reset dataBlocks to relenquish memory.
			dataBlocks = nil

			// Reset offset to '0' to read rest of the blocks.
			startOffset = int64(0)

			// Save what's left after reading erasureBlockSize.
			totalLeft = totalLeft - erasureBlockSize
		}

		// Cleanly end the pipe after a successful decoding.
		pipeWriter.Close()
	}()

	// Return the pipe for the top level caller to start reading.
	return pipeReader, nil
}
