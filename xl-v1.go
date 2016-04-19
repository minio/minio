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
	"errors"
	"fmt"
	"io"
	"os"
	slashpath "path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/reedsolomon"
)

const (
	// Part metadata file.
	metadataFile = "part.json"
	// Maximum erasure blocks.
	maxErasureBlocks = 16
)

// Self Heal data
type selfHeal struct {
	volume string
	fsPath string
	errCh  chan error
}

// XL layer structure.
type XL struct {
	ReedSolomon           reedsolomon.Encoder // Erasure encoder/decoder.
	DataBlocks            int
	ParityBlocks          int
	storageDisks          []StorageAPI
	nameSpaceLockMap      map[nameSpaceParam]*nameSpaceLock
	nameSpaceLockMapMutex *sync.Mutex
	readQuorum            int
	writeQuorum           int
	selfHealCh            chan selfHeal
}

// lockNS - locks the given resource, using a previously allocated
// name space lock or initializing a new one.
func (xl XL) lockNS(volume, path string, readLock bool) {
	xl.nameSpaceLockMapMutex.Lock()
	defer xl.nameSpaceLockMapMutex.Unlock()

	param := nameSpaceParam{volume, path}
	nsLock, found := xl.nameSpaceLockMap[param]
	if !found {
		nsLock = newNSLock()
	}

	if readLock {
		nsLock.RLock()
	} else {
		nsLock.Lock()
	}

	xl.nameSpaceLockMap[param] = nsLock
}

// unlockNS - unlocks any previously acquired read or write locks.
func (xl XL) unlockNS(volume, path string, readLock bool) {
	xl.nameSpaceLockMapMutex.Lock()
	defer xl.nameSpaceLockMapMutex.Unlock()

	param := nameSpaceParam{volume, path}
	if nsLock, found := xl.nameSpaceLockMap[param]; found {
		if readLock {
			nsLock.RUnlock()
		} else {
			nsLock.Unlock()
		}

		if nsLock.InUse() {
			xl.nameSpaceLockMap[param] = nsLock
		}
	}
}

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

	// Initialize name space lock map.
	xl.nameSpaceLockMap = make(map[nameSpaceParam]*nameSpaceLock)
	xl.nameSpaceLockMapMutex = &sync.Mutex{}

	// Figure out read and write quorum based on number of storage disks.
	// Read quorum should be always N/2 + 1 (due to Vandermonde matrix
	// erasure requirements)
	xl.readQuorum = len(xl.storageDisks)/2 + 1
	// Write quorum is assumed if we have total disks + 3
	// parity. (Need to discuss this again)
	xl.writeQuorum = len(xl.storageDisks)/2 + 3
	if xl.writeQuorum > len(xl.storageDisks) {
		xl.writeQuorum = len(xl.storageDisks)
	}

	// Start self heal go routine.
	xl.selfHealRoutine()

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

// fileMetadata - file metadata is a structured representation of the
// unmarshalled metadata file.
type fileMetadata struct {
	Size         int64
	ModTime      time.Time
	BlockSize    int64
	Block512Sum  string
	DataBlocks   int
	ParityBlocks int
	fileVersion  int64
}

// extractMetadata - extract file metadata.
func (xl XL) extractMetadata(volume, path string) (fileMetadata, error) {
	metadataFilePath := slashpath.Join(path, metadataFile)
	// We are not going to read partial data from metadata file,
	// read the whole file always.
	offset := int64(0)
	disk := xl.storageDisks[0]
	metadataReader, err := disk.ReadFile(volume, metadataFilePath, offset)
	if err != nil {
		return fileMetadata{}, err
	}
	// Close metadata reader.
	defer metadataReader.Close()

	var metadata = make(map[string]string)
	decoder := json.NewDecoder(metadataReader)
	// Unmarshalling failed, file possibly corrupted.
	if err = decoder.Decode(&metadata); err != nil {
		return fileMetadata{}, err
	}
	modTime, err := time.Parse(timeFormatAMZ, metadata["file.modTime"])
	if err != nil {
		return fileMetadata{}, err
	}

	// Verify if size is parsable.
	var size int64
	size, err = strconv.ParseInt(metadata["file.size"], 10, 64)
	if err != nil {
		return fileMetadata{}, err
	}

	// Verify if file.version is parsable.
	var fileVersion int64
	// missing file.version is valid
	if _, ok := metadata["file.version"]; ok {
		fileVersion, err = strconv.ParseInt(metadata["file.version"], 10, 64)
		if err != nil {
			return fileMetadata{}, err
		}
	}

	// Verify if block size is parsable.
	var blockSize int64
	blockSize, err = strconv.ParseInt(metadata["file.xl.blockSize"], 10, 64)
	if err != nil {
		return fileMetadata{}, err
	}

	// Verify if data blocks and parity blocks are parsable.
	var dataBlocks, parityBlocks int
	dataBlocks, err = strconv.Atoi(metadata["file.xl.dataBlocks"])
	if err != nil {
		return fileMetadata{}, err
	}
	parityBlocks, err = strconv.Atoi(metadata["file.xl.parityBlocks"])
	if err != nil {
		return fileMetadata{}, err
	}

	// Verify if sha512sum is of proper hex format.
	sha512Sum := metadata["file.xl.block512Sum"]
	_, err = hex.DecodeString(sha512Sum)
	if err != nil {
		return fileMetadata{}, err
	}

	// Return the concocted metadata.
	return fileMetadata{
		Size:         size,
		ModTime:      modTime,
		BlockSize:    blockSize,
		Block512Sum:  sha512Sum,
		DataBlocks:   dataBlocks,
		ParityBlocks: parityBlocks,
		fileVersion:  fileVersion,
	}, nil
}

const (
	slashSeparator = "/"
)

// retainSlash - retains slash from a path.
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
		var metadata fileMetadata
		fileInfo.Name = slashpath.Dir(path)
		metadata, err = xl.extractMetadata(volume, fileInfo.Name)
		if err != nil {
			return FileInfo{}, err
		}
		fileInfo.Size = metadata.Size
		fileInfo.ModTime = metadata.ModTime
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
				// For a leaf directory, if err is FileNotFound then
				// perhaps has a missing metadata. Ignore it and let
				// healing finish its job it will become available soon.
				if err == errFileNotFound {
					continue
				}
				// For any other errors return to the caller.
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

// Object API.

// StatFile - stat a file
func (xl XL) StatFile(volume, path string) (FileInfo, error) {
	if !isValidVolname(volume) {
		return FileInfo{}, errInvalidArgument
	}
	if !isValidPath(path) {
		return FileInfo{}, errInvalidArgument
	}

	// Extract metadata.
	metadata, err := xl.extractMetadata(volume, path)
	if err != nil {
		return FileInfo{}, err
	}

	// Return file info.
	return FileInfo{
		Volume:  volume,
		Name:    path,
		Size:    metadata.Size,
		ModTime: metadata.ModTime,
		Mode:    os.FileMode(0644),
	}, nil
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

// selfHeal - called by the healing go-routine, heals using erasure coding.
func (xl XL) selfHeal(volume string, fsPath string) error {
	totalShards := xl.DataBlocks + xl.ParityBlocks
	needsSelfHeal := make([]bool, totalShards)
	var metadata = make(map[string]string)
	var readers = make([]io.Reader, totalShards)
	var writers = make([]io.WriteCloser, totalShards)
	for index, disk := range xl.storageDisks {
		metadataFile := slashpath.Join(fsPath, metadataFile)

		// Start from the beginning, we are not reading partial metadata files.
		offset := int64(0)

		metadataReader, err := disk.ReadFile(volume, metadataFile, offset)
		if err != nil {
			if err != errFileNotFound {
				continue
			}
			// Needs healing if part.json is not found
			needsSelfHeal[index] = true
			continue
		}
		defer metadataReader.Close()

		decoder := json.NewDecoder(metadataReader)
		if err = decoder.Decode(&metadata); err != nil {
			// needs healing if parts.json is not parsable
			needsSelfHeal[index] = true
		}

		erasurePart := slashpath.Join(fsPath, fmt.Sprintf("part.%d", index))
		erasuredPartReader, err := disk.ReadFile(volume, erasurePart, offset)
		if err != nil {
			if err != errFileNotFound {
				continue
			}
			// needs healing if part file not found
			needsSelfHeal[index] = true
		} else {
			readers[index] = erasuredPartReader
			defer erasuredPartReader.Close()
		}
	}
	// Check if there is atleat one part that needs to be healed.
	atleastOneSelfHeal := false
	for _, shNeeded := range needsSelfHeal {
		if shNeeded {
			atleastOneSelfHeal = true
			break
		}
	}
	if !atleastOneSelfHeal {
		// return if healing not needed anywhere.
		return nil
	}

	// create writers for parts where healing is needed.
	for i, shNeeded := range needsSelfHeal {
		if !shNeeded {
			continue
		}
		var err error
		erasurePart := slashpath.Join(fsPath, fmt.Sprintf("part.%d", i))
		writers[i], err = xl.storageDisks[i].CreateFile(volume, erasurePart)
		if err != nil {
			// Unexpected error
			closeAndRemoveWriters(writers...)
			return err
		}
	}
	size, err := strconv.ParseInt(metadata["file.size"], 10, 64)
	if err != nil {
		closeAndRemoveWriters(writers...)
		return err
	}
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
		for i, reader := range readers {
			// Initialize shard slice and fill the data from each parts.
			// ReedSolomon.Verify() expects that slice is not nil even if the particular
			// part needs healing.
			enShards[i] = make([]byte, curShardSize)
			if needsSelfHeal[i] {
				// Skip reading if the part needs healing.
				continue
			}
			_, e := io.ReadFull(reader, enShards[i])
			if e != nil && e != io.ErrUnexpectedEOF {
				enShards[i] = nil
			}
		}

		// Check blocks if they are all zero in length.
		if checkBlockSize(enShards) == 0 {
			err = errors.New("Data likely corrupted, all blocks are zero in length.")
			return err
		}

		// Verify the shards.
		ok, e := xl.ReedSolomon.Verify(enShards)
		if e != nil {
			closeAndRemoveWriters(writers...)
			return e
		}
		// Verification failed, shards require reconstruction.
		if !ok {
			for i, shNeeded := range needsSelfHeal {
				if shNeeded {
					// Reconstructs() reconstructs the parts if the array is nil.
					enShards[i] = nil
				}
			}
			e = xl.ReedSolomon.Reconstruct(enShards)
			if e != nil {
				closeAndRemoveWriters(writers...)
				return e
			}
			// Verify reconstructed shards again.
			ok, e = xl.ReedSolomon.Verify(enShards)
			if e != nil {
				closeAndRemoveWriters(writers...)
				return e
			}
			if !ok {
				// Shards cannot be reconstructed, corrupted data.
				e = errors.New("Verification failed after reconstruction, data likely corrupted.")
				closeAndRemoveWriters(writers...)
				return e
			}
		}
		for i, shNeeded := range needsSelfHeal {
			if !shNeeded {
				continue
			}
			_, e := writers[i].Write(enShards[i])
			if e != nil {
				closeAndRemoveWriters(writers...)
				return e
			}
		}
		totalLeft = totalLeft - erasureBlockSize
	}
	// After successful healing Close() the writer so that the temp files are renamed.
	for i, shNeeded := range needsSelfHeal {
		if !shNeeded {
			continue
		}
		writers[i].Close()
	}

	// Write part.json where ever healing was done.
	var metadataWriters = make([]io.WriteCloser, len(xl.storageDisks))
	for i, shNeeded := range needsSelfHeal {
		if !shNeeded {
			continue
		}
		metadataFile := slashpath.Join(fsPath, metadataFile)
		metadataWriters[i], err = xl.storageDisks[i].CreateFile(volume, metadataFile)
		if err != nil {
			closeAndRemoveWriters(writers...)
			return err
		}
	}
	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		closeAndRemoveWriters(metadataWriters...)
		return err
	}
	for i, shNeeded := range needsSelfHeal {
		if !shNeeded {
			continue
		}
		_, err := metadataWriters[i].Write(metadataBytes)
		if err != nil {
			closeAndRemoveWriters(metadataWriters...)
			return err
		}
	}

	// part.json written for all the healed parts hence Close() so that temp files can be renamed.
	for index := range xl.storageDisks {
		if !needsSelfHeal[index] {
			continue
		}
		metadataWriters[index].Close()
	}
	return nil
}

// selfHealRoutine - starts a go routine and listens on a channel for healing requests.
func (xl *XL) selfHealRoutine() {
	xl.selfHealCh = make(chan selfHeal)

	// Healing request can be made like this:
	// errCh := make(chan error)
	// xl.selfHealCh <- selfHeal{"testbucket", "testobject", errCh}
	// fmt.Println(<-errCh)

	go func() {
		for sh := range xl.selfHealCh {
			if sh.volume == "" || sh.fsPath == "" {
				sh.errCh <- errors.New("volume or path can not be empty")
				continue
			}
			xl.selfHeal(sh.volume, sh.fsPath)
			sh.errCh <- nil
		}
	}()
}
