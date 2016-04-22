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

type quorumDisk struct {
	disk  StorageAPI
	index int
}

// getQuorumDisks - get the current quorum disks.
func (xl XL) getQuorumDisks(volume, path string) (quorumDisks []quorumDisk, higherVersion int64) {
	fileQuorumVersionMap := xl.getFileQuorumVersionMap(volume, path)
	for diskIndex, formatVersion := range fileQuorumVersionMap {
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
	return quorumDisks, higherVersion
}

func (xl XL) getFileQuorumVersionMap(volume, path string) map[int]int64 {
	metadataFilePath := slashpath.Join(path, metadataFile)
	// Set offset to 0 to read entire file.
	offset := int64(0)
	metadata := make(fileMetadata)

	// Allocate disk index format map - do not use maps directly
	// without allocating.
	fileQuorumVersionMap := make(map[int]int64)

	// TODO - all errors should be logged here.

	// Read meta data from all disks
	for index, disk := range xl.storageDisks {
		fileQuorumVersionMap[index] = -1

		metadataReader, err := disk.ReadFile(volume, metadataFilePath, offset)
		if err != nil {
			continue
		} else if err = json.NewDecoder(metadataReader).Decode(&metadata); err != nil {
			continue
		}

		version, err := metadata.GetFileVersion()
		if err == errMetadataKeyNotExist {
			fileQuorumVersionMap[index] = 0
			continue
		}
		if err != nil {
			continue
		}
		fileQuorumVersionMap[index] = version
	}
	return fileQuorumVersionMap
}

// WriteErasure reads predefined blocks, encodes them and writes to
// configured storage disks.
func (xl XL) writeErasure(volume, path string, reader *io.PipeReader) {
	xl.lockNS(volume, path, false)
	defer xl.unlockNS(volume, path, false)

	// Get available quorum for existing file path.
	_, higherVersion := xl.getQuorumDisks(volume, path)
	// Increment to have next higher version.
	higherVersion++

	writers := make([]io.WriteCloser, len(xl.storageDisks))
	sha512Writers := make([]hash.Hash, len(xl.storageDisks))

	metadataFilePath := slashpath.Join(path, metadataFile)
	metadataWriters := make([]io.WriteCloser, len(xl.storageDisks))

	// Save additional erasureMetadata.
	modTime := time.Now().UTC()

	createFileError := 0
	for index, disk := range xl.storageDisks {
		erasurePart := slashpath.Join(path, fmt.Sprintf("part.%d", index))
		writer, err := disk.CreateFile(volume, erasurePart)
		if err != nil {
			createFileError++

			// We can safely allow CreateFile errors up to len(xl.storageDisks) - xl.writeQuorum
			// otherwise return failure.
			if createFileError <= len(xl.storageDisks)-xl.writeQuorum {
				continue
			}

			// Remove previous temp writers for any failure.
			xl.cleanupCreateFileOps(volume, path, append(writers, metadataWriters...)...)
			reader.CloseWithError(errWriteQuorum)
			return
		}

		// create meta data file
		var metadataWriter io.WriteCloser
		metadataWriter, err = disk.CreateFile(volume, metadataFilePath)
		if err != nil {
			createFileError++

			// We can safely allow CreateFile errors up to
			// len(xl.storageDisks) - xl.writeQuorum otherwise return failure.
			if createFileError <= len(xl.storageDisks)-xl.writeQuorum {
				continue
			}

			// Remove previous temp writers for any failure.
			xl.cleanupCreateFileOps(volume, path, append(writers, metadataWriters...)...)
			reader.CloseWithError(errWriteQuorum)
			return
		}

		writers[index] = writer
		metadataWriters[index] = metadataWriter
		sha512Writers[index] = fastSha512.New()
	}

	// Allocate 4MiB block size buffer for reading.
	dataBuffer := make([]byte, erasureBlockSize)
	var totalSize int64 // Saves total incoming stream size.
	for {
		// Read up to allocated block size.
		n, err := io.ReadFull(reader, dataBuffer)
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
			var dataBlocks [][]byte
			dataBlocks, err = xl.ReedSolomon.Split(dataBuffer[0:n])
			if err != nil {
				// Remove all temp writers.
				xl.cleanupCreateFileOps(volume, path, append(writers, metadataWriters...)...)
				reader.CloseWithError(err)
				return
			}

			// Encode parity blocks using data blocks.
			err = xl.ReedSolomon.Encode(dataBlocks)
			if err != nil {
				// Remove all temp writers upon error.
				xl.cleanupCreateFileOps(volume, path, append(writers, metadataWriters...)...)
				reader.CloseWithError(err)
				return
			}

			// Loop through and write encoded data to quorum disks.
			for index, writer := range writers {
				if writer == nil {
					continue
				}
				encodedData := dataBlocks[index]
				_, err = writers[index].Write(encodedData)
				if err != nil {
					// Remove all temp writers upon error.
					xl.cleanupCreateFileOps(volume, path, append(writers, metadataWriters...)...)
					reader.CloseWithError(err)
					return
				}
				if sha512Writers[index] != nil {
					sha512Writers[index].Write(encodedData)
				}
			}

			// Update total written.
			totalSize += int64(n)
		}
	}

	// Initialize metadata map, save all erasure related metadata.
	metadata := make(fileMetadata)
	metadata.Set("version", minioVersion)
	metadata.Set("format.major", "1")
	metadata.Set("format.minor", "0")
	metadata.Set("format.patch", "0")
	metadata.Set("file.size", strconv.FormatInt(totalSize, 10))
	if len(xl.storageDisks) > len(writers) {
		// Save file.version only if we wrote to less disks than all
		// storage disks.
		metadata.Set("file.version", strconv.FormatInt(higherVersion, 10))
	}
	metadata.Set("file.modTime", modTime.Format(timeFormatAMZ))
	metadata.Set("file.xl.blockSize", strconv.Itoa(erasureBlockSize))
	metadata.Set("file.xl.dataBlocks", strconv.Itoa(xl.DataBlocks))
	metadata.Set("file.xl.parityBlocks", strconv.Itoa(xl.ParityBlocks))

	// Write all the metadata.
	// below case is not handled here
	// Case: when storageDisks is 16 and write quorumDisks is 13,
	//       meta data write failure up to 2 can be considered.
	//       currently we fail for any meta data writes
	for index, metadataWriter := range metadataWriters {
		if metadataWriter == nil {
			continue
		}
		if sha512Writers[index] != nil {
			// Save sha512 checksum of each encoded blocks.
			metadata.Set("file.xl.block512Sum", hex.EncodeToString(sha512Writers[index].Sum(nil)))
		}

		// Write metadata.
		err := metadata.Write(metadataWriter)
		if err != nil {
			// Remove temporary files.
			xl.cleanupCreateFileOps(volume, path, append(writers, metadataWriters...)...)
			reader.CloseWithError(err)
			return
		}
	}

	// Close all writers and metadata writers in routines.
	for index, writer := range writers {
		if writer == nil {
			continue
		}
		// Safely wrote, now rename to its actual location.
		writer.Close()

		if metadataWriters[index] == nil {
			continue
		}
		// Safely wrote, now rename to its actual location.
		metadataWriters[index].Close()
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
