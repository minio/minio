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
)

// ReadFile - read file
func (xl erasure) ReadFile(volume, path string, startOffset int64) (io.ReadCloser, error) {
	// Input validation.
	if !isValidVolname(volume) {
		return nil, errInvalidArgument
	}
	if !isValidPath(path) {
		return nil, errInvalidArgument
	}

	readers := make([]io.ReadCloser, len(xl.storageDisks))
	for index, disk := range xl.storageDisks {
		// If disk.ReadFile returns error and we don't have read quorum it will be taken care as
		// ReedSolomon.Reconstruct() will fail later.
		offset := int64(0)
		if reader, err := disk.ReadFile(volume, path, offset); err == nil {
			readers[index] = reader
		}
	}

	// Initialize pipe.
	pipeReader, pipeWriter := io.Pipe()
	go func() {
		// Read until the totalLeft.
		for {
			// Calculate the current encoded block size.
			curEncBlockSize := getEncodedBlockLen(erasureBlockSize, xl.DataBlocks)
			enBlocks := make([][]byte, len(xl.storageDisks))
			// Loop through all readers and read.
			for index, reader := range readers {
				// Initialize shard slice and fill the data from each parts.
				enBlocks[index] = make([]byte, curEncBlockSize)
				if reader == nil {
					continue
				}
				// Read the necessary blocks.
				n, rErr := io.ReadFull(reader, enBlocks[index])
				if rErr == io.EOF {
					// Close the pipe.
					pipeWriter.Close()

					// Cleanly close all the underlying data readers.
					for _, reader := range readers {
						if reader == nil {
							continue
						}
						reader.Close()
					}
					return
				}
				if rErr != nil && rErr != io.ErrUnexpectedEOF {
					readers[index] = nil
					continue
				}
				enBlocks[index] = enBlocks[index][:n]
			}

			// Check blocks if they are all zero in length.
			if checkBlockSize(enBlocks) == 0 {
				pipeWriter.CloseWithError(errDataCorrupt)
				return
			}

			// Verify the blocks.
			ok, err := xl.ReedSolomon.Verify(enBlocks)
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
			dataBlocks := getDataBlocks(enBlocks, xl.DataBlocks)

			// Verify if the offset is right for the block, if not move to
			// the next block.
			if startOffset > 0 {
				startOffset = startOffset - int64(len(dataBlocks))
				// Start offset is greater than or equal to zero, skip the dataBlocks.
				if startOffset >= 0 {
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
