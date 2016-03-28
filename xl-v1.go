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
	slashpath "path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/reedsolomon"
)

// XL layer structure.
type XL struct {
	ReedSolomon  reedsolomon.Encoder // Erasure encoder/decoder.
	DataBlocks   int
	ParityBlocks int
	storageDisks []StorageAPI
}

const (
	// Part metadata file.
	metadataFile = "part.json"
	// Maximum erasure blocks.
	maxErasureBlocks = 16
)

// newXL instantiate a new XL.
func newXL(disks ...string) (StorageAPI, error) {
	// Initialize XL.
	xl := &XL{}

	// Verify disks.
	totalDisks := len(disks)
	if totalDisks > maxErasureBlocks {
		return nil, errors.New("Total number of disks specified is higher than supported maximum of '16'")
	}

	// isEven function to verify if a given number if even.
	isEven := func(number int) bool {
		return number%2 == 0
	}

	// TODO: verify if this makes sense in future.
	if !isEven(totalDisks) {
		return nil, errors.New("Invalid number of directories provided, should be always multiples of '2'")
	}

	// Calculate data and parity blocks.
	dataBlocks, parityBlocks := totalDisks/2, totalDisks/2

	// Initialize reed solomon encoding.
	rs, err := reedsolomon.New(dataBlocks, parityBlocks)
	if err != nil {
		return nil, err
	}

	// Save the reedsolomon.
	xl.ReedSolomon = rs
	xl.DataBlocks = dataBlocks
	xl.ParityBlocks = parityBlocks

	// Initialize all storage disks.
	storageDisks := make([]StorageAPI, len(disks))
	for index, disk := range disks {
		var err error
		storageDisks[index], err = newFS(disk)
		if err != nil {
			return nil, err
		}
	}

	// Save all the initialized storage disks.
	xl.storageDisks = storageDisks

	// Return successfully initialized.
	return xl, nil
}

// MakeVol - make a volume.
func (xl XL) MakeVol(volume string) error {
	if !isValidVolname(volume) {
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
	if !isValidVolname(volume) {
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
	if !isValidVolname(volume) {
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

// isLeafDirectory - check if a given path is leaf directory. i.e
// there are no more directories inside it. Erasure code backend
// format it means that the parent directory is the actual object name.
func (xl XL) isLeafDirectory(volume, leafPath string) (isLeaf bool) {
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
			// Directory found, not a leaf directory, return right here.
			isLeaf = false
			return isLeaf
		}
	}
	// Exhausted all the entries, no directories found must be leaf
	// return right here.
	isLeaf = true
	return isLeaf
}

// extractMetadata - extract file metadata.
func (xl XL) extractMetadata(volume, path string) (time.Time, int64, error) {
	metadataFilePath := slashpath.Join(path, metadataFile)
	// We are not going to read partial data from metadata file,
	// read the whole file always.
	offset := int64(0)
	disk := xl.storageDisks[0]
	metadataReader, err := disk.ReadFile(volume, metadataFilePath, offset)
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

const (
	slashSeparator = "/"
)

func retainSlash(path string) string {
	return strings.TrimSuffix(path, slashSeparator) + slashSeparator
}

// byFileInfoName is a collection satisfying sort.Interface.
type byFileInfoName []FileInfo

func (d byFileInfoName) Len() int           { return len(d) }
func (d byFileInfoName) Swap(i, j int)      { d[i], d[j] = d[j], d[i] }
func (d byFileInfoName) Less(i, j int) bool { return d[i].Name < d[j].Name }

// ListFiles files at prefix.
func (xl XL) ListFiles(volume, prefix, marker string, recursive bool, count int) (filesInfo []FileInfo, eof bool, err error) {
	if !isValidVolname(volume) {
		return nil, true, errInvalidArgument
	}
	// Pick the first disk and list there always.
	disk := xl.storageDisks[0]
	var fsFilesInfo []FileInfo
	var markerPath = marker
	if marker != "" {
		isLeaf := xl.isLeafDirectory(volume, retainSlash(marker))
		if isLeaf {
			// For leaf for now we just point to the first block, make it
			// dynamic in future based on the availability of storage disks.
			markerPath = slashpath.Join(marker, "part.0")
		}
	}
	// Extract file info from paths.
	extractFileInfo := func(volume, path string) (FileInfo, error) {
		var fileInfo = FileInfo{}
		fileInfo.Name = slashpath.Dir(path)
		var modTime time.Time
		var size int64
		modTime, size, err = xl.extractMetadata(volume, fileInfo.Name)
		if err != nil {
			return FileInfo{}, err
		}
		fileInfo.ModTime = modTime
		fileInfo.Size = size
		fileInfo.Mode = os.FileMode(0644)
		return fileInfo, nil
	}
	// List files.
	fsFilesInfo, eof, err = disk.ListFiles(volume, prefix, markerPath, recursive, count)
	if err != nil {
		return nil, true, err
	}
	for _, fsFileInfo := range fsFilesInfo {
		// Skip metadata files.
		if strings.HasSuffix(fsFileInfo.Name, metadataFile) {
			continue
		}
		var fileInfo FileInfo
		var isLeaf bool
		if fsFileInfo.Mode.IsDir() {
			isLeaf = xl.isLeafDirectory(volume, fsFileInfo.Name)
		}
		if isLeaf || !fsFileInfo.Mode.IsDir() {
			fileInfo, err = extractFileInfo(volume, fsFileInfo.Name)
			if err != nil {
				return nil, true, err
			}
		} else {
			fileInfo = fsFileInfo
		}
		filesInfo = append(filesInfo, fileInfo)
	}
	sort.Sort(byFileInfoName(filesInfo))
	return filesInfo, eof, nil
}

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

// Object API.

// ReadFile - read file
func (xl XL) ReadFile(volume, path string, offset int64) (io.ReadCloser, error) {
	if !isValidVolname(volume) {
		return nil, errInvalidArgument
	}
	if !isValidPath(path) {
		return nil, errInvalidArgument
	}

	var metadata = make(map[string]string)
	var readers = make([]io.Reader, len(xl.storageDisks))

	// Loop through and verify if all metadata files are in-tact.
	for index, disk := range xl.storageDisks {
		metadataFilePath := slashpath.Join(path, metadataFile)

		// Start from the beginning, we are not reading partial metadata files.
		offset := int64(0)

		metadataReader, err := disk.ReadFile(volume, metadataFilePath, offset)
		if err != nil {
			return nil, err
		}
		decoder := json.NewDecoder(metadataReader)
		if err = decoder.Decode(&metadata); err != nil {
			return nil, err
		}
		erasurePart := slashpath.Join(path, fmt.Sprintf("part.%d", index))
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
	totalBlocks := xl.DataBlocks + xl.ParityBlocks // Total blocks.

	// Initialize pipe.
	pipeReader, pipeWriter := io.Pipe()
	go func() {
		var totalLeft = size
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
	}()

	// Return the pipe for the top level caller to start reading.
	return pipeReader, nil
}

// StatFile - stat a file
func (xl XL) StatFile(volume, path string) (FileInfo, error) {
	if !isValidVolname(volume) {
		return FileInfo{}, errInvalidArgument
	}
	if !isValidPath(path) {
		return FileInfo{}, errInvalidArgument
	}

	// Extract metadata.
	modTime, size, err := xl.extractMetadata(volume, path)
	if err != nil {
		return FileInfo{}, err
	}

	// Return file info.
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

// Remove all temp writers if they belong for safeFile.
func removeTempWriters(writers ...io.WriteCloser) {
	for _, writer := range writers {
		safeCloseAndRemove(writer)
	}
}

// WriteErasure reads predefined blocks, encodes them and writes to
// configured storage disks.
func (xl XL) writeErasure(volume, path string, reader *io.PipeReader) {
	var writers = make([]io.WriteCloser, len(xl.storageDisks))
	var metadataWriters = make([]io.WriteCloser, len(xl.storageDisks))

	// Initialize storage disks, get all the writers and corresponding
	// metadata writers.
	for index, disk := range xl.storageDisks {
		var err error
		erasurePart := slashpath.Join(path, fmt.Sprintf("part.%d", index))
		writers[index], err = disk.CreateFile(volume, erasurePart)
		if err != nil {
			// Remove previous temp writers for any failure.
			removeTempWriters(writers...)
			reader.CloseWithError(err)
			return
		}
		metadataFilePath := slashpath.Join(path, metadataFile)
		metadataWriters[index], err = disk.CreateFile(volume, metadataFilePath)
		if err != nil {
			// Remove previous temp writers for any failure.
			removeTempWriters(writers...)
			removeTempWriters(metadataWriters...)
			reader.CloseWithError(err)
			return
		}
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
				removeTempWriters(writers...)
				removeTempWriters(metadataWriters...)
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
			blocks, err := xl.ReedSolomon.Split(buffer[0:n])
			if err != nil {
				// Remove all temp writers.
				removeTempWriters(writers...)
				removeTempWriters(metadataWriters...)
				reader.CloseWithError(err)
				return
			}
			// Encode parity blocks using data blocks.
			err = xl.ReedSolomon.Encode(blocks)
			if err != nil {
				// Remove all temp writers upon error.
				removeTempWriters(writers...)
				removeTempWriters(metadataWriters...)
				reader.CloseWithError(err)
				return
			}
			// Loop through and write encoded data to all the disks.
			for index, encodedData := range blocks {
				_, err = writers[index].Write(encodedData)
				if err != nil {
					// Remove all temp writers upon error.
					removeTempWriters(writers...)
					removeTempWriters(metadataWriters...)
					reader.CloseWithError(err)
					return
				}
			}
			// Update total written.
			totalSize += int64(n)
		}
	}

	// Save additional erasureMetadata.
	modTime := time.Now().UTC()
	// Initialize metadata map, save all erasure related metadata.
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
	// Marshal metadata into json strings.
	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		removeTempWriters(writers...)
		removeTempWriters(metadataWriters...)
		reader.CloseWithError(err)
		return
	}

	// Write all the metadata.
	for _, metadataWriter := range metadataWriters {
		_, err = metadataWriter.Write(metadataBytes)
		if err != nil {
			removeTempWriters(writers...)
			removeTempWriters(metadataWriters...)
			reader.CloseWithError(err)
			return
		}
	}

	// Close all writers and metadata writers in routines.
	for index := range xl.storageDisks {
		go func(index int) {
			// Safely wrote, now rename to its actual location.
			writers[index].Close()
			metadataWriters[index].Close()
		}(index)
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

// DeleteFile - delete a file
func (xl XL) DeleteFile(volume, path string) error {
	if !isValidVolname(volume) {
		return errInvalidArgument
	}
	if !isValidPath(path) {
		return errInvalidArgument
	}
	// Loop through and delete each chunks.
	for index, disk := range xl.storageDisks {
		erasureFilePart := slashpath.Join(path, fmt.Sprintf("part.%d", index))
		err := disk.DeleteFile(volume, erasureFilePart)
		if err != nil {
			return err
		}
		metadataFilePath := slashpath.Join(path, metadataFile)
		err = disk.DeleteFile(volume, metadataFilePath)
		if err != nil {
			return err
		}
	}
	return nil
}
