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

func (xl XL) getMetaFileVersionMap(volume, path string) (diskFileVersionMap map[int]int64) {
	metadataFilePath := slashpath.Join(path, metadataFile)
	// Set offset to 0 to read entire file.
	offset := int64(0)
	metadata := make(map[string]string)

	// Allocate disk index format map - do not use maps directly without allocating.
	diskFileVersionMap = make(map[int]int64)

	// TODO - all errors should be logged here.

	// Read meta data from all disks
	for index, disk := range xl.storageDisks {
		diskFileVersionMap[index] = -1

		metadataReader, err := disk.ReadFile(volume, metadataFilePath, offset)
		if err != nil {
			continue
		} else if err = json.NewDecoder(metadataReader).Decode(&metadata); err != nil {
			continue
		} else if _, ok := metadata["file.version"]; !ok {
			diskFileVersionMap[index] = 0
		}
		// Convert string to integer.
		fileVersion, err := strconv.ParseInt(metadata["file.version"], 10, 64)
		if err != nil {
			continue
		}
		diskFileVersionMap[index] = fileVersion
	}
	return diskFileVersionMap
}

type quorumDisk struct {
	disk  StorageAPI
	index int
}

// getQuorumDisks - get the current quorum disks.
func (xl XL) getQuorumDisks(volume, path string) (quorumDisks []quorumDisk, higherVersion int64) {
	diskVersionMap := xl.getMetaFileVersionMap(volume, path)
	for diskIndex, formatVersion := range diskVersionMap {
		if formatVersion > higherVersion {
			higherVersion = formatVersion
			quorumDisks = []quorumDisk{{
				disk:  xl.storageDisks[diskIndex],
				index: diskIndex,
			}}
		} else if formatVersion == higherVersion {
			quorumDisks = append(quorumDisks, quorumDisk{
				disk:  xl.storageDisks[diskIndex],
				index: diskIndex,
			})
		}
	}
	return
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

	// Check read quorum.
	quorumDisks, _ := xl.getQuorumDisks(volume, path)
	if len(quorumDisks) < xl.readQuorum {
		return nil, errReadQuorum
	}

	// Get file size.
	fileSize, err := xl.getFileSize(volume, path, quorumDisks[0].disk)
	if err != nil {
		return nil, err
	}
	totalBlocks := xl.DataBlocks + xl.ParityBlocks // Total blocks.

	readers := make([]io.ReadCloser, len(quorumDisks))
	readFileError := 0
	for _, quorumDisk := range quorumDisks {
		erasurePart := slashpath.Join(path, fmt.Sprintf("part.%d", quorumDisk.index))
		var erasuredPartReader io.ReadCloser
		if erasuredPartReader, err = quorumDisk.disk.ReadFile(volume, erasurePart, offset); err != nil {
			// We can safely allow ReadFile errors up to len(quorumDisks) - xl.readQuorum
			// otherwise return failure
			if readFileError < len(quorumDisks)-xl.readQuorum {
				// Set the reader to 'nil' to be able to reconstruct later.
				readers[quorumDisk.index] = nil
				readFileError++
				continue
			}
			// Control reaches here we do not have quorum
			// anymore. Close all the readers.
			for _, reader := range readers {
				if reader != nil {
					reader.Close()
				}
			}
			return nil, errReadQuorum
		}
		readers[quorumDisk.index] = erasuredPartReader
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
