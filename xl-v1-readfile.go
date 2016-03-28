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
)

// checkBlockSize return the size of a single block.
// The first non-zero size is returned,
// or 0 if all blocks are size 0.
func checkBlockSize(blocks [][]byte) int {
	for _, block := range blocks {
		if len(block) != 0 {
			return len(block)
		}
	}
	return 0
}

// calculate the blockSize based on input length and total number of
// data blocks.
func getEncodedBlockLen(inputLen, dataBlocks int) (curBlockSize int) {
	curBlockSize = (inputLen + dataBlocks - 1) / dataBlocks
	return
}

// ReadFile - read file
func (xl XL) ReadFile(volume, path string, offset int64) (io.ReadCloser, error) {
	// Input validation.
	if !isValidVolname(volume) {
		return nil, errInvalidArgument
	}
	if !isValidPath(path) {
		return nil, errInvalidArgument
	}

	// Initialize all readers.
	var readers = make([]io.ReadCloser, len(xl.storageDisks))

	// Extract metadata.
	metadata, err := xl.extractMetadata(volume, path)
	if err != nil {
		return nil, err
	}

	// Loop through and verify if all metadata files are in-tact.
	for index, disk := range xl.storageDisks {
		offset := int64(0)
		erasurePart := slashpath.Join(path, fmt.Sprintf("part.%d", index))
		var erasuredPartReader io.ReadCloser
		erasuredPartReader, err = disk.ReadFile(volume, erasurePart, offset)
		if err != nil {
			// One of parts not found, we need to re-construct.
			if err == errFileNotFound {
				readers[index] = nil
				continue
			}
			// For all other errors return to the caller.
			return nil, err
		}
		readers[index] = erasuredPartReader
	}
	totalBlocks := xl.DataBlocks + xl.ParityBlocks // Total blocks.

	// Initialize pipe.
	pipeReader, pipeWriter := io.Pipe()
	go func() {
		var totalLeft = metadata.Size
		// Read until the totalLeft.
		for totalLeft > 0 {
			// Figure out the right blockSize as it was encoded before.
			var curBlockSize int
			if erasureBlockSize < totalLeft {
				curBlockSize = erasureBlockSize
			} else {
				curBlockSize = int(totalLeft)
			}
			// Calculate the current encoded block size.
			curEncBlockSize := getEncodedBlockLen(curBlockSize, xl.DataBlocks)
			enBlocks := make([][]byte, totalBlocks)
			// Loop through all readers and read.
			for index, reader := range readers {
				if reader == nil {
					// One of files missing, save it for reconstruction.
					enBlocks[index] = nil
					continue
				}
				// Initialize shard slice and fill the data from each parts.
				enBlocks[index] = make([]byte, curEncBlockSize)
				_, err = io.ReadFull(reader, enBlocks[index])
				if err != nil && err != io.ErrUnexpectedEOF {
					enBlocks[index] = nil
				}
			}

			// TODO need to verify block512Sum.

			// Check blocks if they are all zero in length.
			if checkBlockSize(enBlocks) == 0 {
				err = errors.New("Data likely corrupted, all blocks are zero in length.")
				pipeWriter.CloseWithError(err)
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
			// Join the decoded blocks.
			err = xl.ReedSolomon.Join(pipeWriter, enBlocks, curBlockSize)
			if err != nil {
				pipeWriter.CloseWithError(err)
				return
			}
			// Save what's left after reading erasureBlockSize.
			totalLeft = totalLeft - erasureBlockSize
		}
		// Cleanly end the pipe after a successful decoding.
		pipeWriter.Close()

		// Cleanly close all the underlying data readers.
		for _, reader := range readers {
			reader.Close()
		}
	}()

	// Return the pipe for the top level caller to start reading.
	return pipeReader, nil
}
