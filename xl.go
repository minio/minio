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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/klauspost/reedsolomon"
)

// XL layer structure.
type XL struct {
	ReedSolomon  reedsolomon.Encoder // Erasure encoder/decoder.
	DataShards   int
	ParityShards int
	storageDisks []StorageAPI
	rwLock       *sync.RWMutex
}

// newXL instantiate a new XL.
func newXL(rootPaths ...string) (StorageAPI, error) {
	// Initialize XL.
	xl := &XL{}

	// Root paths.
	totalShards := len(rootPaths)
	isEven := func(number int) bool {
		return number%2 == 0
	}
	if !isEven(totalShards) {
		return nil, errors.New("Invalid number of directories provided, should be always multiples of '2'")
	}

	// Calculate data and parity shards.
	dataShards, parityShards := totalShards/2, totalShards/2

	// Initialize reed solomon encoding.
	rs, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		return nil, err
	}

	// Save the reedsolomon.
	xl.ReedSolomon = rs
	xl.DataShards = dataShards
	xl.ParityShards = parityShards

	// Initialize all storage disks.
	storageDisks := make([]StorageAPI, len(rootPaths))
	for index, rootPath := range rootPaths {
		var err error
		storageDisks[index], err = newFS(rootPath)
		if err != nil {
			return nil, err
		}
	}

	// Save all the initialized storage disks.
	xl.storageDisks = storageDisks

	// Read write lock.
	xl.rwLock = &sync.RWMutex{}

	// Return successfully initialized.
	return xl, nil
}

// MakeVol - make a volume.
func (xl XL) MakeVol(volume string) error {
	if volume == "" {
		return errInvalidArgument
	}
	// Make a volume entry on all underlying storage disks.
	for _, disk := range xl.storageDisks {
		if err := disk.MakeVol(volume); err != nil {
			return err
		}
	}
	return nil
}

// DeleteVol - delete a volume.
func (xl XL) DeleteVol(volume string) error {
	if volume == "" {
		return errInvalidArgument
	}
	for _, disk := range xl.storageDisks {
		if err := disk.DeleteVol(volume); err != nil {
			return err
		}
	}
	return nil
}

// ListVols - list volumes.
func (xl XL) ListVols() (volsInfo []VolInfo, err error) {
	for _, disk := range xl.storageDisks {
		volsInfo, err = disk.ListVols()
		if err == nil {
			return volsInfo, nil
		}
	}
	return nil, err
}

// StatVol - get volume stat info.
func (xl XL) StatVol(volume string) (volInfo VolInfo, err error) {
	if volume == "" {
		return VolInfo{}, errInvalidArgument
	}
	for _, disk := range xl.storageDisks {
		volInfo, err = disk.StatVol(volume)
		if err == nil {
			return volInfo, nil
		}
	}
	return VolInfo{}, err
}

// ListFiles files at prefix.
func (xl XL) ListFiles(volume, prefix, marker string, recursive bool, count int) (filesInfo []FileInfo, eof bool, err error) {
	if volume == "" {
		return nil, true, errInvalidArgument
	}
	for _, disk := range xl.storageDisks {
		filesInfo, eof, err = disk.ListFiles(volume, prefix, marker, recursive, count)
		if err == nil {
			return filesInfo, eof, nil
		}
	}
	return nil, true, err
}

// shardSize return the size of a single shard.
// The first non-zero size is returned,
// or 0 if all shards are size 0.
func shardSize(shards [][]byte) int {
	for _, shard := range shards {
		if len(shard) != 0 {
			return len(shard)
		}
	}
	return 0
}

// calculate the shardSize based on input length and total number of
// data shards.
func getEncodedBlockLen(inputLen, dataShards int) (curShardSize int) {
	curShardSize = (inputLen + dataShards - 1) / dataShards
	return
}

// Object API.

// ReadFile - read file
func (xl XL) ReadFile(volume, path string, offset int64) (io.ReadCloser, error) {
	var metadata = make(map[string]string)
	var readers = make([]io.Reader, len(xl.storageDisks))
	for index, disk := range xl.storageDisks {
		metadataFile := filepath.Join(path, "part.json")

		// Start from the beginning, we are not reading partial metadata files.
		offset := int64(0)

		metadataReader, err := disk.ReadFile(volume, metadataFile, offset)
		if err != nil {
			return nil, err
		}
		decoder := json.NewDecoder(metadataReader)
		if err = decoder.Decode(&metadata); err != nil {
			return nil, err
		}
		erasurePart := filepath.Join(path, fmt.Sprintf("part.%d", index))
		erasuredPartReader, err := disk.ReadFile(volume, erasurePart, offset)
		if err != nil {
			readers[index] = nil
		} else {
			readers[index] = erasuredPartReader
		}
	}
	size, err := strconv.ParseInt(metadata["file.size"], 10, 64)
	if err != nil {
		return nil, err
	}
	totalShards := xl.DataShards + xl.ParityShards // Total shards.

	// Initialize pipe.
	pipeReader, pipeWriter := io.Pipe()
	go func() {
		var totalLeft = size
		for totalLeft > 0 {
			// Figure out the right blockSize.
			var curBlockSize int
			if erasureBlockSize < totalLeft {
				curBlockSize = erasureBlockSize
			} else {
				curBlockSize = int(totalLeft)
			}
			// Calculate the current shard size.
			curShardSize := getEncodedBlockLen(curBlockSize, xl.DataShards)
			enShards := make([][]byte, totalShards)
			// Loop through all readers and read.
			for index, reader := range readers {
				if reader == nil {
					// One of files missing, save it for reconstruction.
					enShards[index] = nil
					continue
				}
				// Initialize shard slice and fill the data from each parts.
				enShards[index] = make([]byte, curShardSize)
				_, e := io.ReadFull(reader, enShards[index])
				if e != nil && e != io.ErrUnexpectedEOF {
					enShards[index] = nil
				}
			}
			if shardSize(enShards) == 0 {
				break
			}
			// Verify the shards.
			ok, e := xl.ReedSolomon.Verify(enShards)
			if e != nil {
				pipeWriter.CloseWithError(e)
				return
			}
			// Verification failed, shards require reconstruction.
			if !ok {
				e = xl.ReedSolomon.Reconstruct(enShards)
				if e != nil {
					pipeWriter.CloseWithError(e)
					return
				}
				// Verify reconstructed shards again.
				ok, e = xl.ReedSolomon.Verify(enShards)
				if e != nil {
					pipeWriter.CloseWithError(e)
					return
				}
				if !ok {
					// Shards cannot be reconstructed, corrupted data.
					e = errors.New("Verification failed after reconstruction, data likely corrupted.")
					pipeWriter.CloseWithError(e)
					return
				}
			}
			// Join the decoded shards.
			e = xl.ReedSolomon.Join(pipeWriter, enShards, curBlockSize)
			if e != nil {
				pipeWriter.CloseWithError(e)
				return
			}
			totalLeft = totalLeft - erasureBlockSize
		}
		// Cleanly end the pipe after a successful decoding.
		pipeWriter.Close()
	}()

	// Return the pipe for the top level caller to start reading.
	return pipeReader, nil
}

// StatFile - stat a file
func (xl XL) StatFile(volume, path string) (FileInfo, error) {
	var objMetadata = make(map[string]string)
	for index, disk := range xl.storageDisks {
		metadataFile := filepath.Join(path, "part.json")
		// We are not going to read partial data from metadata file,
		// read the whole file always.
		offset := int64(0)
		metadataReader, err := disk.ReadFile(volume, metadataFile, offset)
		if err != nil {
			return FileInfo{}, err
		}
		decoder := json.NewDecoder(metadataReader)
		// Unmarshalling failed, file corrupted.
		if err = decoder.Decode(&objMetadata); err != nil {
			return FileInfo{}, err
		}
		erasurePart := filepath.Join(path, fmt.Sprintf("part.%d", index))
		// Validate if all parts are available.
		_, err = disk.StatFile(volume, erasurePart)
		if err != nil {
			return FileInfo{}, err
		}
	}
	modTime, err := time.Parse(timeFormatAMZ, objMetadata["obj.modTime"])
	if err != nil {
		return FileInfo{}, err
	}
	size, err := strconv.ParseInt(objMetadata["obj.size"], 10, 64)
	if err != nil {
		return FileInfo{}, err
	}
	return FileInfo{
		Volume:  volume,
		Name:    path,
		ModTime: modTime,
		Size:    size,
		Mode:    os.FileMode(0644),
	}, nil
}

// Erasure block size.
const erasureBlockSize = 4 * 1024 * 1024 // 4MiB.

// CreateFile - create a file.
func (xl XL) CreateFile(volume, path string) (writeCloser io.WriteCloser, err error) {
	if volume == "" || path == "" {
		return nil, errInvalidArgument
	}

	// Save additional erasureMetadata.
	modTime := time.Now().UTC()
	metadata := make(map[string]string)
	metadata["version"] = minioVersion
	metadata["format.major"] = "1"
	metadata["format.minor"] = "0"
	metadata["format.patch"] = "0"
	//metadata["file.size"] = strconv.FormatInt(size, 10)
	metadata["file.modTime"] = modTime.Format(timeFormatAMZ)
	metadata["file.xl.blockSize"] = strconv.Itoa(erasureBlockSize)
	metadata["file.xl.dataBlocks"] = strconv.Itoa(xl.DataShards)
	metadata["file.xl.parityBlocks"] = strconv.Itoa(xl.ParityShards)
	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		return nil, err
	}
	for _, disk := range xl.storageDisks {
		// File metadata file.
		metadataFile := filepath.Join(path, "/part.json")

		// Create metadata file.
		writeCloser, err := disk.CreateFile(volume, metadataFile)
		if err != nil {
			return nil, err
		}
		_, err = io.CopyN(writeCloser, bytes.NewReader(metadataBytes), int64(len(metadataBytes)))
		if err != nil {
			return nil, err
		}
		// Close and commit the file.
		writeCloser.Close()
	}
	return nil, nil
}

// DeleteFile - delete a file
func (xl XL) DeleteFile(volume, path string) error {
	if volume == "" || path == "" {
		return errInvalidArgument
	}
	// Loop through and delete each chunks.
	for index, disk := range xl.storageDisks {
		erasureFilePart := filepath.Join(volume, fmt.Sprintf("part.%d", index))
		err := disk.DeleteFile(volume, erasureFilePart)
		if err != nil {
			return err
		}
	}
	return nil
}
