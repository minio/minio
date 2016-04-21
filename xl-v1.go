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
	"fmt"
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

	// Heal input/output channel.
	selfHealCh chan selfHeal
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
		return nil, errMaxDisks
	}

	// isEven function to verify if a given number if even.
	isEven := func(number int) bool {
		return number%2 == 0
	}

	// TODO: verify if this makes sense in future.
	if !isEven(totalDisks) {
		return nil, errNumDisks
	}

	// Calculate data and parity blocks.
	dataBlocks, parityBlocks := totalDisks/2, totalDisks/2

	// Initialize reed solomon encoding.
	rs, err := reedsolomon.New(dataBlocks, parityBlocks)
	if err != nil {
		return nil, err
	}

	// Save the reedsolomon.
	xl.DataBlocks = dataBlocks
	xl.ParityBlocks = parityBlocks
	xl.ReedSolomon = rs

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

	// Start self heal go routine, taking inputs over self heal channel.
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
			// We ignore error if errVolumeExists and creating a volume again.
			if err == errVolumeExists {
				continue
			}
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
			// We ignore error if errVolumeNotFound.
			if err == errVolumeNotFound {
				continue
			}
			return err
		}
	}
	return nil
}

// ListVols - list volumes.
func (xl XL) ListVols() (volsInfo []VolInfo, err error) {
	emptyCount := 0
	// Success vols map carries successful results of ListVols from
	// each disks.
	var successVolsMap = make(map[int][]VolInfo)
	for index, disk := range xl.storageDisks {
		var vlsInfo []VolInfo
		vlsInfo, err = disk.ListVols()
		if err == nil {
			if len(vlsInfo) == 0 {
				emptyCount++
			} else {
				successVolsMap[index] = vlsInfo
			}
		}
	}

	// If all list operations resulted in an empty count which is same
	// as your total storage disks, then it is a valid case return
	// success with empty vols.
	if emptyCount == len(xl.storageDisks) {
		return []VolInfo{}, nil
	} else if len(successVolsMap) < xl.readQuorum {
		// If there is data and not empty, then we attempt quorum verification.
		// Verify if we have enough quorum to list vols.
		return nil, errReadQuorum
	}
	// Loop through success vols map and return the first value.
	for index := range xl.storageDisks {
		if _, ok := successVolsMap[index]; ok {
			volsInfo = successVolsMap[index]
			break
		}
	}
	return volsInfo, nil
}

// StatVol - get volume stat info.
func (xl XL) StatVol(volume string) (volInfo VolInfo, err error) {
	if !isValidVolname(volume) {
		return VolInfo{}, errInvalidArgument
	}
	var statVols []VolInfo
	volumeNotFoundErrCnt := 0
	for _, disk := range xl.storageDisks {
		volInfo, err = disk.StatVol(volume)
		if err == nil {
			// Collect all the successful attempts to verify quorum
			// subsequently.
			statVols = append(statVols, volInfo)
		} else if err == errVolumeNotFound {
			// Count total amount of volume not found errors.
			volumeNotFoundErrCnt++
		}
	}

	// If volume not found err count is same as total storage disks, we
	// really don't have the bucket, report a valid error.
	if volumeNotFoundErrCnt == len(xl.storageDisks) {
		return VolInfo{}, errVolumeNotFound
	} else if len(statVols) < xl.readQuorum {
		// If one of the disks have bucket we need to validate if we
		// have read quorum, if not fail.
		return VolInfo{}, errReadQuorum
	}

	// If successful remove all the duplicates and keep the latest one.
	volInfo = removeDuplicateVols(statVols)[0]
	return volInfo, nil
}

// isLeafDirectory - check if a given path is leaf directory. i.e
// there are no more directories inside it. Erasure code backend
// format it means that the parent directory is the actual object name.
func (xl XL) isLeafDirectory(volume, leafPath string) (isLeaf bool) {
	var allFileInfos []FileInfo
	var markerPath string
	for {
		fileInfos, eof, e := xl.storageDisks[0].ListFiles(volume, leafPath, markerPath, false, 1000)
		if e != nil {
			break
		}
		allFileInfos = append(allFileInfos, fileInfos...)
		if eof {
			break
		}
		// MarkerPath to get the next set of files.
		markerPath = allFileInfos[len(allFileInfos)-1].Name
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

// Returns file size from the metadata.
func getFileSize(metadata fileMetadata) (int64, error) {
	size := metadata.Get("file.size")
	if size == nil {
		return 0, errFileSize
	}
	return strconv.ParseInt(size[0], 10, 64)
}

func getModTime(metadata fileMetadata) (time.Time, error) {
	modTime := metadata.Get("file.modTime")
	if modTime == nil {
		return time.Time{}, errModTime
	}
	return time.Parse(timeFormatAMZ, modTime[0])
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
		return nil, err
	}
	// Close metadata reader.
	defer metadataReader.Close()

	metadata, err := fileMetadataDecode(metadataReader)
	if err != nil {
		return nil, err
	}
	return metadata, nil
}

// Extract file info from paths.
func (xl XL) extractFileInfo(volume, path string) (FileInfo, error) {
	fileInfo := FileInfo{}
	fileInfo.Volume = volume
	fileInfo.Name = path

	metadata, err := xl.extractMetadata(volume, path)
	if err != nil {
		return FileInfo{}, err
	}
	fileSize, err := getFileSize(metadata)
	if err != nil {
		return FileInfo{}, err
	}
	fileModTime, err := getModTime(metadata)
	if err != nil {
		return FileInfo{}, err
	}
	fileInfo.Size = fileSize
	fileInfo.Mode = os.FileMode(0644)
	fileInfo.ModTime = fileModTime
	return fileInfo, nil
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
			// Extract the parent of leaf directory or file to get the
			// actual name.
			path := slashpath.Dir(fsFileInfo.Name)
			fileInfo, err = xl.extractFileInfo(volume, path)
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
	fileInfo, err := xl.extractFileInfo(volume, path)
	if err != nil {
		return FileInfo{}, err
	}

	// Return fileinfo.
	return fileInfo, nil
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
