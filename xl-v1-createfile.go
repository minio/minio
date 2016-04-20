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
	"encoding/json"
	"fmt"
	"hash"
	"io"
	slashpath "path"
	"strconv"
	"time"

	fastSha512 "github.com/minio/minio/pkg/crypto/sha512"
)

// Erasure block size.
const erasureBlockSize = 4 * 1024 * 1024 // 4MiB.

// cleanupCreateFileOps - cleans up all the temporary files and other
// temporary data upon any failure.
func (xl XL) cleanupCreateFileOps(volume, path string, writers ...io.WriteCloser) {
	closeAndRemoveWriters(writers...)
	for _, disk := range xl.storageDisks {
		disk.DeleteFile(volume, path)
	}
}

// Close and remove writers if they are safeFile.
func closeAndRemoveWriters(writers ...io.WriteCloser) {
	for _, writer := range writers {
		safeCloseAndRemove(writer)
	}
}

// WriteErasure reads predefined blocks, encodes them and writes to
// configured storage disks.
func (xl XL) writeErasure(volume, path string, reader *io.PipeReader) {
	xl.lockNS(volume, path, false)
	defer xl.unlockNS(volume, path, false)

	// get available quorum for existing file path
	_, higherVersion := xl.getQuorumDisks(volume, path)
	// increment to have next higher version
	higherVersion++

	quorumDisks := make([]quorumDisk, len(xl.storageDisks))
	writers := make([]io.WriteCloser, len(xl.storageDisks))
	sha512Writers := make([]hash.Hash, len(xl.storageDisks))

	metadataFilePath := slashpath.Join(path, metadataFile)
	metadataWriters := make([]io.WriteCloser, len(xl.storageDisks))

	// Save additional erasureMetadata.
	modTime := time.Now().UTC()

	createFileError := 0
	maxIndex := 0
	for index, disk := range xl.storageDisks {
		erasurePart := slashpath.Join(path, fmt.Sprintf("part.%d", index))
		writer, err := disk.CreateFile(volume, erasurePart)
		if err != nil {
			createFileError++

			// we can safely allow CreateFile errors up to len(xl.storageDisks) - xl.writeQuorum
			// otherwise return failure
			if createFileError <= len(xl.storageDisks)-xl.writeQuorum {
				continue
			}

			// Remove previous temp writers for any failure.
			xl.cleanupCreateFileOps(volume, path, append(writers, metadataWriters...)...)
			reader.CloseWithError(err)
			return
		}

		// create meta data file
		var metadataWriter io.WriteCloser
		metadataWriter, err = disk.CreateFile(volume, metadataFilePath)
		if err != nil {
			createFileError++

			// we can safely allow CreateFile errors up to len(xl.storageDisks) - xl.writeQuorum
			// otherwise return failure
			if createFileError <= len(xl.storageDisks)-xl.writeQuorum {
				continue
			}

			// Remove previous temp writers for any failure.
			xl.cleanupCreateFileOps(volume, path, append(writers, metadataWriters...)...)
			reader.CloseWithError(err)
			return
		}

		writers[maxIndex] = writer
		metadataWriters[maxIndex] = metadataWriter
		sha512Writers[maxIndex] = fastSha512.New()
		quorumDisks[maxIndex] = quorumDisk{disk, index}
		maxIndex++
	}

	// Allocate 4MiB block size buffer for reading.
	buffer := make([]byte, erasureBlockSize)
	var totalSize int64 // Saves total incoming stream size.
	for {
		// Read up to allocated block size.
		n, err := io.ReadFull(reader, buffer)
		if err != nil {
			// Any unexpected errors, close the pipe reader with error.
			if err != io.ErrUnexpectedEOF && err != io.EOF {
				// Remove all temp writers.
				xl.cleanupCreateFileOps(volume, path, append(writers, metadataWriters...)...)
				reader.CloseWithError(err)
				return
			}
		}
		// At EOF break out.
		if err == io.EOF {
			break
		}
		if n > 0 {
			// Split the input buffer into data and parity blocks.
			var blocks [][]byte
			blocks, err = xl.ReedSolomon.Split(buffer[0:n])
			if err != nil {
				// Remove all temp writers.
				xl.cleanupCreateFileOps(volume, path, append(writers, metadataWriters...)...)
				reader.CloseWithError(err)
				return
			}
			// Encode parity blocks using data blocks.
			err = xl.ReedSolomon.Encode(blocks)
			if err != nil {
				// Remove all temp writers upon error.
				xl.cleanupCreateFileOps(volume, path, append(writers, metadataWriters...)...)
				reader.CloseWithError(err)
				return
			}

			// Loop through and write encoded data to quorum disks.
			for i := 0; i < maxIndex; i++ {
				encodedData := blocks[quorumDisks[i].index]

				_, err = writers[i].Write(encodedData)
				if err != nil {
					// Remove all temp writers upon error.
					xl.cleanupCreateFileOps(volume, path, append(writers, metadataWriters...)...)
					reader.CloseWithError(err)
					return
				}
				sha512Writers[i].Write(encodedData)
			}
			// Update total written.
			totalSize += int64(n)
		}
	}

	// Initialize metadata map, save all erasure related metadata.
	metadata := make(map[string]string)
	metadata["version"] = minioVersion
	metadata["format.major"] = "1"
	metadata["format.minor"] = "0"
	metadata["format.patch"] = "0"
	metadata["file.size"] = strconv.FormatInt(totalSize, 10)
	if len(xl.storageDisks) > len(writers) {
		// save file.version only if we wrote to less disks than all disks
		metadata["file.version"] = strconv.FormatInt(higherVersion, 10)
	}
	metadata["file.modTime"] = modTime.Format(timeFormatAMZ)
	metadata["file.xl.blockSize"] = strconv.Itoa(erasureBlockSize)
	metadata["file.xl.dataBlocks"] = strconv.Itoa(xl.DataBlocks)
	metadata["file.xl.parityBlocks"] = strconv.Itoa(xl.ParityBlocks)

	// Write all the metadata.
	// below case is not handled here
	// Case: when storageDisks is 16 and write quorumDisks is 13,
	//       meta data write failure up to 2 can be considered.
	//       currently we fail for any meta data writes
	for i := 0; i < maxIndex; i++ {
		// Save sha512 checksum of each encoded blocks.
		metadata["file.xl.block512Sum"] = hex.EncodeToString(sha512Writers[i].Sum(nil))

		// Marshal metadata into json strings.
		metadataBytes, err := json.Marshal(metadata)
		if err != nil {
			// Remove temporary files.
			xl.cleanupCreateFileOps(volume, path, append(writers, metadataWriters...)...)
			reader.CloseWithError(err)
			return
		}

		// Write metadata to disk.
		_, err = metadataWriters[i].Write(metadataBytes)
		if err != nil {
			xl.cleanupCreateFileOps(volume, path, append(writers, metadataWriters...)...)
			reader.CloseWithError(err)
			return
		}
	}

	// Close all writers and metadata writers in routines.
	for i := 0; i < maxIndex; i++ {
		// Safely wrote, now rename to its actual location.
		writers[i].Close()
		metadataWriters[i].Close()
	}

	// Close the pipe reader and return.
	reader.Close()
	return
}

// CreateFile - create a file.
func (xl XL) CreateFile(volume, path string) (writeCloser io.WriteCloser, err error) {
	if !isValidVolname(volume) {
		return nil, errInvalidArgument
	}
	if !isValidPath(path) {
		return nil, errInvalidArgument
	}

	// Initialize pipe for data pipe line.
	pipeReader, pipeWriter := io.Pipe()

	// Start erasure encoding in routine, reading data block by block from pipeReader.
	go xl.writeErasure(volume, path, pipeReader)

	// Return the piped writer, caller should start writing to this.
	return pipeWriter, nil
}
