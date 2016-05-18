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
	"fmt"
	"io"
	slashpath "path"
	"sync"
)

// ReadFile - read file
func (xl XL) ReadFile(volume, path string, startOffset int64) (io.ReadCloser, error) {
	// Input validation.
	if !isValidVolname(volume) {
		return nil, errInvalidArgument
	}
	if !isValidPath(path) {
		return nil, errInvalidArgument
	}

	onlineDisks, metadata, heal, err := xl.listOnlineDisks(volume, path)
	if err != nil {
		return nil, err
	}

	if heal {
		// Heal in background safely, since we already have read
		// quorum disks. Let the reads continue.
		go func() {
			hErr := xl.healFile(volume, path)
			errorIf(hErr, "Unable to heal file "+volume+"/"+path+".")
		}()
	}

	readers := make([]io.ReadCloser, len(xl.storageDisks))
	for index, disk := range onlineDisks {
		if disk == nil {
			continue
		}
		erasurePart := slashpath.Join(path, fmt.Sprintf("file.%d", index))
		// If disk.ReadFile returns error and we don't have read quorum it will be taken care as
		// ReedSolomon.Reconstruct() will fail later.
		var reader io.ReadCloser
		offset := int64(0)
		if reader, err = disk.ReadFile(volume, erasurePart, offset); err == nil {
			readers[index] = reader
		}
	}

	// Initialize pipe.
	pipeReader, pipeWriter := io.Pipe()
	go func() {
		var totalLeft = metadata.Stat.Size
		// Read until the totalLeft.
		for totalLeft > 0 {
			// Figure out the right blockSize as it was encoded before.
			var curBlockSize int64
			if metadata.Erasure.BlockSize < totalLeft {
				curBlockSize = metadata.Erasure.BlockSize
			} else {
				curBlockSize = totalLeft
			}
			// Calculate the current encoded block size.
			curEncBlockSize := getEncodedBlockLen(curBlockSize, metadata.Erasure.DataBlocks)
			enBlocks := make([][]byte, len(xl.storageDisks))
			var wg = &sync.WaitGroup{}
			// Loop through all readers and read.
			for index, reader := range readers {
				// Initialize shard slice and fill the data from each parts.
				enBlocks[index] = make([]byte, curEncBlockSize)
				if reader == nil {
					continue
				}
				// Parallelize reading.
				wg.Add(1)
				go func(index int, reader io.Reader) {
					defer wg.Done()
					// Read the necessary blocks.
					_, rErr := io.ReadFull(reader, enBlocks[index])
					if rErr != nil && rErr != io.ErrUnexpectedEOF {
						readers[index] = nil
					}
				}(index, reader)
			}
			// Wait for the read routines to finish.
			wg.Wait()

			// Check blocks if they are all zero in length.
			if checkBlockSize(enBlocks) == 0 {
				pipeWriter.CloseWithError(errDataCorrupt)
				return
			}

			// Verify the blocks.
			var ok bool
			ok, err = xl.ReedSolomon.Verify(enBlocks)
			if err != nil {
				pipeWriter.CloseWithError(err)
				return
			}

			// Verification failed, blocks require reconstruction.
			if !ok {
				for index, reader := range readers {
					if reader == nil {
						// Reconstruct expects missing blocks to be nil.
						enBlocks[index] = nil
					}
				}
				err = xl.ReedSolomon.Reconstruct(enBlocks)
				if err != nil {
					pipeWriter.CloseWithError(err)
					return
				}
				// Verify reconstructed blocks again.
				ok, err = xl.ReedSolomon.Verify(enBlocks)
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

			// Get all the data blocks.
			dataBlocks := getDataBlocks(enBlocks, metadata.Erasure.DataBlocks, int(curBlockSize))

			// Verify if the offset is right for the block, if not move to
			// the next block.

			if startOffset > 0 {
				startOffset = startOffset - int64(len(dataBlocks))
				// Start offset is greater than or equal to zero, skip the dataBlocks.
				if startOffset >= 0 {
					totalLeft = totalLeft - metadata.Erasure.BlockSize
					continue
				}
				// Now get back the remaining offset if startOffset is negative.
				startOffset = startOffset + int64(len(dataBlocks))
			}

			// Write safely the necessary blocks.
			_, err = pipeWriter.Write(dataBlocks[int(startOffset):])
			if err != nil {
				pipeWriter.CloseWithError(err)
				return
			}

			// Reset offset to '0' to read rest of the blocks.
			startOffset = int64(0)

			// Save what's left after reading erasureBlockSize.
			totalLeft = totalLeft - metadata.Erasure.BlockSize
		}

		// Cleanly end the pipe after a successful decoding.
		pipeWriter.Close()

		// Cleanly close all the underlying data readers.
		for _, reader := range readers {
			if reader == nil {
				continue
			}
			reader.Close()
		}
	}()

	// Return the pipe for the top level caller to start reading.
	return pipeReader, nil
}
