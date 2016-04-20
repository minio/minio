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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	slashpath "path"
	"strconv"
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

// Returns data needed to for ReadFile operation:
// - slice indicating which shard is readable
// - file size
// - error
func (xl XL) getReadFileInfo(volume, path string) ([]bool, int64, error) {
	metadatas, errs := xl.getMetadata(volume, path)
	highestVersion := int64(0)
	versions := make([]int64, len(xl.storageDisks))
	quorumDisks := make([]bool, len(xl.storageDisks))
	fileSize := int64(0)
	for i, metadata := range metadatas {
		if errs[i] != nil {
			versions[i] = -1
			continue
		}
		versionStr, ok := metadata["file.version"]
		if !ok {
			versions[i] = 0
			continue
		}
		// Convert string to integer.
		version, err := strconv.ParseInt(versionStr, 10, 64)
		if err != nil {
			// Unexpected, return error.
			return nil, 0, err
		}
		if version > highestVersion {
			highestVersion = version
		}
		versions[i] = version
	}
	quorumCount := 0
	for i, version := range versions {
		if version == highestVersion {
			quorumDisks[i] = true
			quorumCount++
		}
	}
	if quorumCount < xl.readQuorum {
		return nil, 0, errReadQuorum
	}
	for i, read := range quorumDisks {
		if !read {
			continue
		}
		sizeStr, ok := metadatas[i]["file.size"]
		if !ok {
			return nil, 0, errors.New("missing 'file.size' in meta data")
		}
		var err error
		fileSize, err = strconv.ParseInt(sizeStr, 10, 64)
		if err != nil {
			return nil, 0, err
		}
		break
	}
	return quorumDisks, fileSize, nil
}

// getFileSize - extract file size from metadata.
func (xl XL) getFileSize(volume, path string, disk StorageAPI) (size int64, err error) {
	metadataFilePath := slashpath.Join(path, metadataFile)
	// set offset to 0 to read entire file
	offset := int64(0)
	metadata := make(map[string]string)

	metadataReader, err := disk.ReadFile(volume, metadataFilePath, offset)
	if err != nil {
		return 0, err
	}

	if err = json.NewDecoder(metadataReader).Decode(&metadata); err != nil {
		return 0, err
	}

	if _, ok := metadata["file.size"]; !ok {
		return 0, errors.New("missing 'file.size' in meta data")
	}

	return strconv.ParseInt(metadata["file.size"], 10, 64)
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

	// Acquire a read lock.
	readLock := true
	xl.lockNS(volume, path, readLock)
	defer xl.unlockNS(volume, path, readLock)

	quorumDisks, fileSize, err := xl.getReadFileInfo(volume, path)
	if err != nil {
		return nil, err
	}

	readers := make([]io.ReadCloser, len(quorumDisks))
	for i, read := range quorumDisks {
		if !read {
			continue
		}
		erasurePart := slashpath.Join(path, fmt.Sprintf("part.%d", i))
		// If disk.ReadFile returns error and we don't have read quorum it will be taken care as
		// ReedSolomon.Reconstruct() will fail later.
		var reader io.ReadCloser
		if reader, err = xl.storageDisks[i].ReadFile(volume, erasurePart, offset); err == nil {
			readers[i] = reader
		}
	}

	// Initialize pipe.
	pipeReader, pipeWriter := io.Pipe()
	go func() {
		var totalLeft = fileSize
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
			enBlocks := make([][]byte, len(xl.storageDisks))
			// Loop through all readers and read.
			for index, reader := range readers {
				// Initialize shard slice and fill the data from each parts.
				enBlocks[index] = make([]byte, curEncBlockSize)
				if reader == nil {
					continue
				}
				_, err = io.ReadFull(reader, enBlocks[index])
				if err != nil && err != io.ErrUnexpectedEOF {
					readers[index] = nil
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
				for i, reader := range readers {
					if reader == nil {
						// Reconstruct expects missing shard to be nil.
						enBlocks[i] = nil
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
			if reader == nil {
				continue
			}
			reader.Close()
		}
	}()

	// Return the pipe for the top level caller to start reading.
	return pipeReader, nil
}
