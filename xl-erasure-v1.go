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
	"io"
	"os"
	slashpath "path"
	"sort"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/klauspost/reedsolomon"
)

const (
	// Part metadata file.
	xlMetaV1File = "xl.json"
	// Maximum erasure blocks.
	maxErasureBlocks = 16
)

// XL layer structure.
type XL struct {
	ReedSolomon  reedsolomon.Encoder // Erasure encoder/decoder.
	DataBlocks   int
	ParityBlocks int
	storageDisks []StorageAPI
	readQuorum   int
	writeQuorum  int
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
		storageDisks[index], err = newPosix(disk)
		if err != nil {
			return nil, err
		}
	}

	// Save all the initialized storage disks.
	xl.storageDisks = storageDisks

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

	// Return successfully initialized.
	return xl, nil
}

// MakeVol - make a volume.
func (xl XL) MakeVol(volume string) error {
	if !isValidVolname(volume) {
		return errInvalidArgument
	}
	// Collect if all disks report volume exists.
	var volumeExistsMap = make(map[int]struct{})
	// Make a volume entry on all underlying storage disks.
	for index, disk := range xl.storageDisks {
		if err := disk.MakeVol(volume); err != nil {
			log.WithFields(logrus.Fields{
				"volume": volume,
			}).Errorf("MakeVol failed with %s", err)
			// We ignore error if errVolumeExists and creating a volume again.
			if err == errVolumeExists {
				volumeExistsMap[index] = struct{}{}
				continue
			}
			return err
		}
	}
	// Return err if all disks report volume exists.
	if len(volumeExistsMap) == len(xl.storageDisks) {
		return errVolumeExists
	}
	return nil
}

// DeleteVol - delete a volume.
func (xl XL) DeleteVol(volume string) error {
	if !isValidVolname(volume) {
		return errInvalidArgument
	}

	// Collect if all disks report volume not found.
	var volumeNotFoundMap = make(map[int]struct{})

	// Remove a volume entry on all underlying storage disks.
	for index, disk := range xl.storageDisks {
		if err := disk.DeleteVol(volume); err != nil {
			log.WithFields(logrus.Fields{
				"volume": volume,
			}).Errorf("DeleteVol failed with %s", err)
			// We ignore error if errVolumeNotFound.
			if err == errVolumeNotFound {
				volumeNotFoundMap[index] = struct{}{}
				continue
			}
			return err
		}
	}
	// Return err if all disks report volume not found.
	if len(volumeNotFoundMap) == len(xl.storageDisks) {
		return errVolumeNotFound
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

	var total, free int64
	// Loop through success vols map and get aggregated usage values.
	for index := range xl.storageDisks {
		if _, ok := successVolsMap[index]; ok {
			volsInfo = successVolsMap[index]
			free += volsInfo[0].Free
			total += volsInfo[0].Total
		}
	}
	// Save the updated usage values back into the vols.
	for index := range volsInfo {
		volsInfo[index].Free = free
		volsInfo[index].Total = total
	}

	// TODO: the assumption here is that volumes across all disks in
	// readQuorum have consistent view i.e they all have same number
	// of buckets. This is essentially not verified since healing
	// should take care of this.
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
		} else if err != nil {
			log.WithFields(logrus.Fields{
				"volume": volume,
			}).Errorf("StatVol failed with %s", err)
			return VolInfo{}, err
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

	// Loop through all statVols, calculate the actual usage values.
	var total, free int64
	for _, statVolInfo := range statVols {
		free += statVolInfo.Free
		total += statVolInfo.Total
	}
	// Filter statVols and update the volInfo.
	volInfo = removeDuplicateVols(statVols)[0]
	volInfo.Free = free
	volInfo.Total = total
	return volInfo, nil
}

// isLeafDirectory - check if a given path is leaf directory. i.e
// there are no more directories inside it. Erasure code backend
// format it means that the parent directory is the actual object name.
func isLeafDirectory(disk StorageAPI, volume, leafPath string) (isLeaf bool) {
	var markerPath string
	var xlListCount = 1000 // Count page.
	for {
		fileInfos, eof, err := disk.ListFiles(volume, leafPath, markerPath, false, xlListCount)
		if err != nil {
			log.WithFields(logrus.Fields{
				"volume":     volume,
				"leafPath":   leafPath,
				"markerPath": markerPath,
				"recursive":  false,
				"count":      xlListCount,
			}).Errorf("ListFiles failed with %s", err)
			break
		}
		for _, fileInfo := range fileInfos {
			if fileInfo.Mode.IsDir() {
				// Directory found, not a leaf directory, return right here.
				return false
			}
		}
		if eof {
			break
		}
		// MarkerPath to get the next set of files.
		markerPath = fileInfos[len(fileInfos)-1].Name
	}
	// Exhausted all the entries, no directories found must be leaf
	// return right here.
	return true
}

// extractMetadata - extract xl metadata.
func extractMetadata(disk StorageAPI, volume, path string) (xlMetaV1, error) {
	xlMetaV1FilePath := slashpath.Join(path, xlMetaV1File)
	// We are not going to read partial data from metadata file,
	// read the whole file always.
	offset := int64(0)
	metadataReader, err := disk.ReadFile(volume, xlMetaV1FilePath, offset)
	if err != nil {
		log.WithFields(logrus.Fields{
			"volume": volume,
			"path":   xlMetaV1FilePath,
			"offset": offset,
		}).Errorf("ReadFile failed with %s", err)
		return xlMetaV1{}, err
	}
	// Close metadata reader.
	defer metadataReader.Close()

	metadata, err := xlMetaV1Decode(metadataReader)
	if err != nil {
		log.WithFields(logrus.Fields{
			"volume": volume,
			"path":   xlMetaV1FilePath,
			"offset": offset,
		}).Errorf("xlMetaV1Decode failed with %s", err)
		return xlMetaV1{}, err
	}
	return metadata, nil
}

// Extract file info from paths.
func extractFileInfo(disk StorageAPI, volume, path string) (FileInfo, error) {
	fileInfo := FileInfo{}
	fileInfo.Volume = volume
	fileInfo.Name = path

	metadata, err := extractMetadata(disk, volume, path)
	if err != nil {
		log.WithFields(logrus.Fields{
			"volume": volume,
			"path":   path,
		}).Errorf("extractMetadata failed with %s", err)
		return FileInfo{}, err
	}
	fileInfo.Size = metadata.Stat.Size
	fileInfo.ModTime = metadata.Stat.ModTime
	fileInfo.Mode = os.FileMode(0644) // This is a file already.
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

	// TODO: Fix: If readQuorum is met, its assumed that disks are in consistent file list.
	// exclude disks those are not in consistent file list and check count of remaining disks
	// are met readQuorum.

	// Treat empty file list specially
	emptyCount := 0
	errCount := 0
	successCount := 0

	var firstFilesInfo []FileInfo
	var firstEOF bool
	var firstErr error

	for _, disk := range xl.storageDisks {
		if filesInfo, eof, err = listFiles(disk, volume, prefix, marker, recursive, count); err == nil {
			// we need to return first successful result
			if firstFilesInfo == nil {
				firstFilesInfo = filesInfo
				firstEOF = eof
			}

			if len(filesInfo) == 0 {
				emptyCount++
			} else {
				successCount++
			}
		} else {
			if firstErr == nil {
				firstErr = err
			}

			errCount++
		}
	}

	if errCount >= xl.readQuorum {
		return nil, false, firstErr
	} else if successCount >= xl.readQuorum {
		return firstFilesInfo, firstEOF, nil
	} else if emptyCount >= xl.readQuorum {
		return []FileInfo{}, true, nil
	}

	return nil, false, errReadQuorum
}

func listFiles(disk StorageAPI, volume, prefix, marker string, recursive bool, count int) (filesInfo []FileInfo, eof bool, err error) {
	var fsFilesInfo []FileInfo
	var markerPath = marker
	if marker != "" {
		isLeaf := isLeafDirectory(disk, volume, retainSlash(marker))
		if isLeaf {
			// For leaf for now we just point to the first block, make it
			// dynamic in future based on the availability of storage disks.
			markerPath = slashpath.Join(marker, xlMetaV1File)
		}
	}

	// Loop and capture the proper fileInfos, requires extraction and
	// separation of XL related metadata information.
	for {
		fsFilesInfo, eof, err = disk.ListFiles(volume, prefix, markerPath, recursive, count)
		if err != nil {
			log.WithFields(logrus.Fields{
				"volume":    volume,
				"prefix":    prefix,
				"marker":    markerPath,
				"recursive": recursive,
				"count":     count,
			}).Errorf("ListFiles failed with %s", err)
			return nil, true, err
		}
		for _, fsFileInfo := range fsFilesInfo {
			// Skip metadata files.
			if strings.HasSuffix(fsFileInfo.Name, xlMetaV1File) {
				continue
			}
			var fileInfo FileInfo
			var isLeaf bool
			if fsFileInfo.Mode.IsDir() {
				isLeaf = isLeafDirectory(disk, volume, fsFileInfo.Name)
			}
			if isLeaf || !fsFileInfo.Mode.IsDir() {
				// Extract the parent of leaf directory or file to get the
				// actual name.
				path := slashpath.Dir(fsFileInfo.Name)
				fileInfo, err = extractFileInfo(disk, volume, path)
				if err != nil {
					log.WithFields(logrus.Fields{
						"volume": volume,
						"path":   path,
					}).Errorf("extractFileInfo failed with %s", err)
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
			count--
			if count == 0 {
				break
			}
		}
		if len(fsFilesInfo) > 0 {
			// markerPath for the next disk.ListFiles() iteration.
			markerPath = fsFilesInfo[len(fsFilesInfo)-1].Name
		}
		if count == 0 && recursive && !strings.HasSuffix(markerPath, xlMetaV1File) {
			// If last entry is not xl.json then loop once more to check if we have reached eof.
			fsFilesInfo, eof, err = disk.ListFiles(volume, prefix, markerPath, recursive, 1)
			if err != nil {
				log.WithFields(logrus.Fields{
					"volume":    volume,
					"prefix":    prefix,
					"marker":    markerPath,
					"recursive": recursive,
					"count":     1,
				}).Errorf("ListFiles failed with %s", err)
				return nil, true, err
			}
			if !eof {
				// file.N and xl.json are always in pairs and hence this
				// entry has to be xl.json. If not better to manually investigate
				// and fix it.
				// For the next ListFiles() call we can safely assume that the
				// marker is "object/xl.json"
				if !strings.HasSuffix(fsFilesInfo[0].Name, xlMetaV1File) {
					log.WithFields(logrus.Fields{
						"volume":          volume,
						"prefix":          prefix,
						"fsFileInfo.Name": fsFilesInfo[0].Name,
					}).Errorf("ListFiles failed with %s, expected %s to be a xl.json file.", err, fsFilesInfo[0].Name)
					return nil, true, errUnexpected
				}
			}
		}
		if count == 0 || eof {
			break
		}
	}
	// Sort to make sure we sort entries back properly.
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

	// Acquire read lock.
	nsMutex.RLock(volume, path)
	_, metadata, heal, err := xl.listOnlineDisks(volume, path)
	nsMutex.RUnlock(volume, path)
	if err != nil {
		log.WithFields(logrus.Fields{
			"volume": volume,
			"path":   path,
		}).Errorf("listOnlineDisks failed with %s", err)
		return FileInfo{}, err
	}

	if heal {
		// Heal in background safely, since we already have read quorum disks.
		go func() {
			if err = xl.healFile(volume, path); err != nil {
				log.WithFields(logrus.Fields{
					"volume": volume,
					"path":   path,
				}).Errorf("healFile failed with %s", err)
				return
			}
		}()
	}

	// Return file info.
	return FileInfo{
		Volume:  volume,
		Name:    path,
		Size:    metadata.Stat.Size,
		ModTime: metadata.Stat.ModTime,
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

	// Lock right before reading from disk.
	nsMutex.RLock(volume, path)
	partsMetadata, errs := xl.getPartsMetadata(volume, path)
	nsMutex.RUnlock(volume, path)

	var err error

	// List all the file versions on existing files.
	versions := listFileVersions(partsMetadata, errs)
	// Get highest file version.
	higherVersion := highestInt(versions)

	// find online disks and meta data of higherVersion
	var mdata *xlMetaV1
	onlineDiskCount := 0
	for index, metadata := range partsMetadata {
		if errs[index] == nil {
			onlineDiskCount++
			if metadata.Stat.Version == higherVersion && mdata == nil {
				mdata = &metadata
			}
		}
	}

	// return error if mdata is empty or onlineDiskCount doesn't meet write quorum
	if mdata == nil || onlineDiskCount < xl.writeQuorum {
		return errWriteQuorum
	}

	// Increment to have next higher version.
	higherVersion++

	xlMetaV1FilePath := slashpath.Join(path, xlMetaV1File)
	deleteMetaData := (onlineDiskCount == len(xl.storageDisks))

	// Set higher version to indicate file operation
	mdata.Stat.Version = higherVersion
	mdata.Stat.Deleted = true

	nsMutex.Lock(volume, path)
	defer nsMutex.Unlock(volume, path)

	errCount := 0
	// Update meta data file and remove part file
	for index, disk := range xl.storageDisks {
		// no need to operate on failed disks
		if errs[index] != nil {
			errCount++
			continue
		}

		// update meta data about delete operation
		var metadataWriter io.WriteCloser
		metadataWriter, err = disk.CreateFile(volume, xlMetaV1FilePath)
		if err != nil {
			log.WithFields(logrus.Fields{
				"volume": volume,
				"path":   path,
			}).Errorf("CreateFile failed with %s", err)

			errCount++

			// We can safely allow CreateFile errors up to len(xl.storageDisks) - xl.writeQuorum
			// otherwise return failure.
			if errCount <= len(xl.storageDisks)-xl.writeQuorum {
				continue
			}

			return err
		}

		err = mdata.Write(metadataWriter)
		if err != nil {
			log.WithFields(logrus.Fields{
				"volume":    volume,
				"path":      path,
				"diskIndex": index,
			}).Errorf("Writing metadata failed with %s", err)

			errCount++

			// We can safely allow CreateFile errors up to len(xl.storageDisks) - xl.writeQuorum
			// otherwise return failure.
			if errCount <= len(xl.storageDisks)-xl.writeQuorum {
				continue
			}

			return err
		}

		erasureFilePart := slashpath.Join(path, fmt.Sprintf("file.%d", index))
		err = disk.DeleteFile(volume, erasureFilePart)
		if err != nil {
			log.WithFields(logrus.Fields{
				"volume": volume,
				"path":   path,
			}).Errorf("DeleteFile failed with %s", err)

			errCount++

			// We can safely allow CreateFile errors up to len(xl.storageDisks) - xl.writeQuorum
			// otherwise return failure.
			if errCount <= len(xl.storageDisks)-xl.writeQuorum {
				continue
			}

			return err
		}
	}

	// Remove meta data file only if deleteMetaData is true.
	if deleteMetaData {
		for index, disk := range xl.storageDisks {
			// no need to operate on failed disks
			if errs[index] != nil {
				continue
			}

			err = disk.DeleteFile(volume, xlMetaV1FilePath)
			if err != nil {
				// No need to return the error as we updated the meta data file previously
				log.WithFields(logrus.Fields{
					"volume": volume,
					"path":   path,
				}).Errorf("DeleteFile failed with %s", err)
			}
		}
	}

	return nil
}

// RenameFile - rename file.
func (xl XL) RenameFile(srcVolume, srcPath, dstVolume, dstPath string) error {
	// Validate inputs.
	if !isValidVolname(srcVolume) {
		return errInvalidArgument
	}
	if !isValidPath(srcPath) {
		return errInvalidArgument
	}
	if !isValidVolname(dstVolume) {
		return errInvalidArgument
	}
	if !isValidPath(dstPath) {
		return errInvalidArgument
	}
	for _, disk := range xl.storageDisks {
		if err := disk.RenameFile(srcVolume, srcPath, dstVolume, dstPath); err != nil {
			log.WithFields(logrus.Fields{
				"srcVolume": srcVolume,
				"srcPath":   srcPath,
				"dstVolume": dstVolume,
				"dstPath":   dstPath,
			}).Errorf("RenameFile failed with %s", err)
			return err
		}
	}
	return nil
}
