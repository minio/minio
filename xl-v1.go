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
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/reedsolomon"
	"github.com/minio/minio/pkg/safe"
)

// XL layer structure.
type XL struct {
	ReedSolomon  reedsolomon.Encoder // Erasure encoder/decoder.
	DataBlocks   int
	ParityBlocks int
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

	// TODO: verify if this makes sense in future.
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
	xl.DataBlocks = dataShards
	xl.ParityBlocks = parityShards

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
	// Pick the first node and list there always.
	disk := xl.storageDisks[0]
	volsInfo, err = disk.ListVols()
	if err == nil {
		return volsInfo, nil
	}
	return nil, err
}

// StatVol - get volume stat info.
func (xl XL) StatVol(volume string) (volInfo VolInfo, err error) {
	if volume == "" {
		return VolInfo{}, errInvalidArgument
	}
	// Pick the first node and list there always.
	disk := xl.storageDisks[0]
	volInfo, err = disk.StatVol(volume)
	if err == nil {
		return volInfo, nil
	}
	return VolInfo{}, err
}

// checkLeafDirectory - verifies if a given path is leaf directory if
// yes returns all the files inside it.
func (xl XL) checkLeafDirectory(volume, leafPath string) (isLeaf bool, fis []FileInfo) {
	var allFileInfos []FileInfo
	for {
		fileInfos, eof, e := xl.storageDisks[0].ListFiles(volume, leafPath, "", false, 1000)
		if e != nil {
			break
		}
		allFileInfos = append(allFileInfos, fileInfos...)
		if eof {
			break
		}
	}
	for _, fileInfo := range allFileInfos {
		if fileInfo.Mode.IsDir() {
			isLeaf = false
			return isLeaf, nil
		}
		fileName := path.Base(fileInfo.Name)
		if !strings.Contains(fileName, ".") {
			fis = append(fis, fileInfo)
		}
	}
	isLeaf = true
	return isLeaf, fis
}

func (xl XL) extractMetadata(volume, fspath string) (time.Time, int64, error) {
	metadataFile := path.Join(fspath, "part.json")
	// We are not going to read partial data from metadata file,
	// read the whole file always.
	offset := int64(0)
	metadataReader, err := xl.storageDisks[0].ReadFile(volume, metadataFile, offset)
	if err != nil {
		return time.Time{}, 0, err
	}

	var metadata = make(map[string]string)
	decoder := json.NewDecoder(metadataReader)
	// Unmarshalling failed, file corrupted.
	if err = decoder.Decode(&metadata); err != nil {
		return time.Time{}, 0, err
	}
	modTime, err := time.Parse(timeFormatAMZ, metadata["file.modTime"])
	if err != nil {
		return time.Time{}, 0, err
	}
	var size int64
	size, err = strconv.ParseInt(metadata["file.size"], 10, 64)
	if err != nil {
		return time.Time{}, 0, err
	}
	return modTime, size, nil
}

// ListFiles files at prefix.
func (xl XL) ListFiles(volume, prefix, marker string, recursive bool, count int) (filesInfo []FileInfo, eof bool, err error) {
	if volume == "" {
		return nil, true, errInvalidArgument
	}
	// Pick the first node and list there always.
	disk := xl.storageDisks[0]
	var fsFilesInfo []FileInfo
	fsFilesInfo, eof, err = disk.ListFiles(volume, prefix, marker, recursive, count)
	if err == nil {
		for _, fsFileInfo := range fsFilesInfo {
			fileInfo := fsFileInfo
			if fileInfo.Mode.IsDir() {
				if isLeaf, _ := xl.checkLeafDirectory(volume, fsFileInfo.Name); isLeaf {
					fileInfo.Name = path.Dir(fsFileInfo.Name)
					var modTime time.Time
					var size int64
					modTime, size, err = xl.extractMetadata(volume, fileInfo.Name)
					if err != nil {
						return nil, true, err
					}
					fileInfo.ModTime = modTime
					fileInfo.Size = size
					fileInfo.Mode = os.FileMode(0644)
				}
			}
			filesInfo = append(filesInfo, fileInfo)
		}
		return filesInfo, eof, nil
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
func (xl XL) ReadFile(volume, fspath string, offset int64) (io.ReadCloser, error) {
	if volume == "" || fspath == "" {
		return nil, errInvalidArgument
	}

	var metadata = make(map[string]string)
	var readers = make([]io.Reader, len(xl.storageDisks))
	for index, disk := range xl.storageDisks {
		metadataFile := path.Join(fspath, "part.json")

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
		erasurePart := path.Join(fspath, fmt.Sprintf("part.%d", index))
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
	totalShards := xl.DataBlocks + xl.ParityBlocks // Total shards.

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
			curShardSize := getEncodedBlockLen(curBlockSize, xl.DataBlocks)
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
func (xl XL) StatFile(volume, fspath string) (FileInfo, error) {
	if volume == "" || fspath == "" {
		return FileInfo{}, errInvalidArgument
	}

	// Extract metadata.
	modTime, size, err := xl.extractMetadata(volume, fspath)
	if err != nil {
		return FileInfo{}, err
	}

	// Return file info.
	return FileInfo{
		Volume:  volume,
		Name:    fspath,
		ModTime: modTime,
		Size:    size,
		Mode:    os.FileMode(0644),
	}, nil
}

// Erasure block size.
const erasureBlockSize = 4 * 1024 * 1024 // 4MiB.

func removeTempWriters(writers []io.WriteCloser) {
	for _, writer := range writers {
		safeWriter, ok := writer.(*safe.File)
		if ok {
			safeWriter.CloseAndRemove()
		}
	}
}

// WriteErasure reads predefined blocks, encodes them and writes to
// configured storage disks.
func (xl XL) writeErasure(volume, fspath string, reader *io.PipeReader) {
	var writers = make([]io.WriteCloser, len(xl.storageDisks))
	var metadataWriters = make([]io.WriteCloser, len(xl.storageDisks))

	// Initialize storage disks.
	for index, disk := range xl.storageDisks {
		var err error
		erasurePart := path.Join(fspath, fmt.Sprintf("part.%d", index))
		writers[index], err = disk.CreateFile(volume, erasurePart)
		if err != nil {
			reader.CloseWithError(err)
			removeTempWriters(writers)
			return
		}
		metadataFile := path.Join(fspath, "part.json")
		metadataWriters[index], err = disk.CreateFile(volume, metadataFile)
		if err != nil {
			reader.CloseWithError(err)
			removeTempWriters(writers)
			return
		}
	}

	buffer := make([]byte, erasureBlockSize)
	var totalSize int64 // Total incoming file size.
	for {
		n, err := io.ReadFull(reader, buffer)
		if err != nil {
			if err != io.ErrUnexpectedEOF && err != io.EOF {
				reader.CloseWithError(err)
				removeTempWriters(writers)
				return
			}
		}
		if err == io.EOF {
			break
		}
		if n > 0 {
			shards, err := xl.ReedSolomon.Split(buffer[0:n])
			if err != nil {
				reader.CloseWithError(err)
				removeTempWriters(writers)
				return
			}
			err = xl.ReedSolomon.Encode(shards)
			if err != nil {
				reader.CloseWithError(err)
				removeTempWriters(writers)
				return
			}
			for key, data := range shards {
				writers[key].Write(data)
			}
			totalSize += int64(n)
		}
	}

	// Save additional erasureMetadata.
	modTime := time.Now().UTC()
	metadata := make(map[string]string)
	metadata["version"] = minioVersion
	metadata["format.major"] = "1"
	metadata["format.minor"] = "0"
	metadata["format.patch"] = "0"
	metadata["file.size"] = strconv.FormatInt(totalSize, 10)
	metadata["file.modTime"] = modTime.Format(timeFormatAMZ)
	metadata["file.xl.blockSize"] = strconv.Itoa(erasureBlockSize)
	metadata["file.xl.dataBlocks"] = strconv.Itoa(xl.DataBlocks)
	metadata["file.xl.parityBlocks"] = strconv.Itoa(xl.ParityBlocks)
	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		reader.CloseWithError(err)
		removeTempWriters(writers)
		removeTempWriters(metadataWriters)
		return
	}
	for _, metadataWriter := range metadataWriters {
		metadataWriter.Write(metadataBytes)
	}

	// Close all writers and metadata writers.
	for index := range xl.storageDisks {
		// Safely wrote, now rename to its actual location.
		writers[index].Close()
		metadataWriters[index].Close()
	}
	return
}

// CreateFile - create a file.
func (xl XL) CreateFile(volume, fspath string) (writeCloser io.WriteCloser, err error) {
	if volume == "" || fspath == "" {
		return nil, errInvalidArgument
	}
	pipeReader, pipeWriter := io.Pipe()
	go xl.writeErasure(volume, fspath, pipeReader)
	return pipeWriter, nil
}

// DeleteFile - delete a file
func (xl XL) DeleteFile(volume, fspath string) error {
	if volume == "" || fspath == "" {
		return errInvalidArgument
	}
	// Loop through and delete each chunks.
	for index, disk := range xl.storageDisks {
		erasureFilePart := path.Join(fspath, fmt.Sprintf("part.%d", index))
		err := disk.DeleteFile(volume, erasureFilePart)
		if err != nil {
			return err
		}
		metadataFile := path.Join(fspath, "part.json")
		err = disk.DeleteFile(volume, metadataFile)
		if err != nil {
			return err
		}
	}
	return nil
}
