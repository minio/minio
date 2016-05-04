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
	"strings"

	"path"

	"github.com/Sirupsen/logrus"
	"github.com/klauspost/reedsolomon"
)

const (
	// XL erasure metadata file.
	xlMetaV1File = "file.json"
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

	// Verify if the volume already exists.
	_, errs := xl.getAllVolumeInfo(volume)

	// Count errors other than errVolumeNotFound, bigger than the allowed
	// readQuorum, if yes throw an error.
	errCount := 0
	for _, err := range errs {
		if err != nil && err != errVolumeNotFound {
			errCount++
			if errCount > xl.readQuorum {
				log.WithFields(logrus.Fields{
					"volume": volume,
				}).Errorf("%s", err)
				return err
			}
		}
	}

	createVolErr := 0
	volumeExistsErrCnt := 0
	// Make a volume entry on all underlying storage disks.
	for _, disk := range xl.storageDisks {
		if err := disk.MakeVol(volume); err != nil {
			log.WithFields(logrus.Fields{
				"volume": volume,
			}).Errorf("MakeVol failed with %s", err)
			// if volume already exists, count them.
			if err == errVolumeExists {
				volumeExistsErrCnt++
				// Return err if all disks report volume exists.
				if volumeExistsErrCnt == len(xl.storageDisks) {
					return errVolumeExists
				}
				continue
			}
			// Update error counter separately.
			createVolErr++
			if createVolErr <= len(xl.storageDisks)-xl.writeQuorum {
				continue
			}
			return errWriteQuorum
		}
	}
	return nil
}

// DeleteVol - delete a volume.
func (xl XL) DeleteVol(volume string) error {
	if !isValidVolname(volume) {
		return errInvalidArgument
	}

	// Collect if all disks report volume not found.
	var volumeNotFoundErrCnt int

	// Remove a volume entry on all underlying storage disks.
	for _, disk := range xl.storageDisks {
		if err := disk.DeleteVol(volume); err != nil {
			log.WithFields(logrus.Fields{
				"volume": volume,
			}).Errorf("DeleteVol failed with %s", err)
			// We ignore error if errVolumeNotFound.
			if err == errVolumeNotFound {
				volumeNotFoundErrCnt++
				continue
			}
			return err
		}
	}
	// Return err if all disks report volume not found.
	if volumeNotFoundErrCnt == len(xl.storageDisks) {
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

// getAllVolumeInfo - get bucket volume info from all disks.
// Returns error slice indicating the failed volume stat operations.
func (xl XL) getAllVolumeInfo(volume string) (volsInfo []VolInfo, errs []error) {
	errs = make([]error, len(xl.storageDisks))
	volsInfo = make([]VolInfo, len(xl.storageDisks))
	for index, disk := range xl.storageDisks {
		volInfo, err := disk.StatVol(volume)
		if err != nil {
			errs[index] = err
			continue
		}
		volsInfo[index] = volInfo
	}
	return volsInfo, errs
}

// listAllVolumeInfo - list all stat volume info from all disks.
// Returns
// - stat volume info for all online disks.
// - boolean to indicate if healing is necessary.
// - error if any.
func (xl XL) listAllVolumeInfo(volume string) ([]VolInfo, bool, error) {
	volsInfo, errs := xl.getAllVolumeInfo(volume)
	notFoundCount := 0
	for _, err := range errs {
		if err == errVolumeNotFound {
			notFoundCount++
			// If we have errors with file not found greater than allowed read
			// quorum we return err as errFileNotFound.
			if notFoundCount > len(xl.storageDisks)-xl.readQuorum {
				return nil, false, errVolumeNotFound
			}
		}
	}

	// Calculate online disk count.
	onlineDiskCount := 0
	for index := range errs {
		if errs[index] == nil {
			onlineDiskCount++
		}
	}

	var heal bool
	// If online disks count is lesser than configured disks, most
	// probably we need to heal the file, additionally verify if the
	// count is lesser than readQuorum, if not we throw an error.
	if onlineDiskCount < len(xl.storageDisks) {
		// Online disks lesser than total storage disks, needs to be
		// healed. unless we do not have readQuorum.
		heal = true
		// Verify if online disks count are lesser than readQuorum
		// threshold, return an error if yes.
		if onlineDiskCount < xl.readQuorum {
			log.WithFields(logrus.Fields{
				"volume":          volume,
				"onlineDiskCount": onlineDiskCount,
				"readQuorumCount": xl.readQuorum,
			}).Errorf("%s", errReadQuorum)
			return nil, false, errReadQuorum
		}
	}

	// Return success.
	return volsInfo, heal, nil
}

// healVolume - heals any missing volumes.
func (xl XL) healVolume(volume string) error {
	// Lists volume info for all online disks.
	volsInfo, heal, err := xl.listAllVolumeInfo(volume)
	if err != nil {
		log.WithFields(logrus.Fields{
			"volume": volume,
		}).Errorf("List online disks failed with %s", err)
		return err
	}
	if !heal {
		return nil
	}
	// Create volume if missing on online disks.
	for index, volInfo := range volsInfo {
		if volInfo.Name != "" {
			continue
		}
		// Volinfo name would be an empty string, create it.
		if err = xl.storageDisks[index].MakeVol(volume); err != nil {
			if err != nil {
				log.WithFields(logrus.Fields{
					"volume": volume,
				}).Errorf("MakeVol failed with error %s", err)
			}
			continue
		}
	}
	return nil
}

// StatVol - get volume stat info.
func (xl XL) StatVol(volume string) (volInfo VolInfo, err error) {
	if !isValidVolname(volume) {
		return VolInfo{}, errInvalidArgument
	}
	volsInfo, heal, err := xl.listAllVolumeInfo(volume)
	if err != nil {
		log.WithFields(logrus.Fields{
			"volume": volume,
		}).Errorf("listOnlineVolsInfo failed with %s", err)
		return VolInfo{}, err
	}

	if heal {
		go func() {
			if err = xl.healVolume(volume); err != nil {
				log.WithFields(logrus.Fields{
					"volume": volume,
				}).Errorf("healVolume failed with %s", err)
				return
			}
		}()
	}

	// Loop through all statVols, calculate the actual usage values.
	var total, free int64
	for _, volInfo := range volsInfo {
		free += volInfo.Free
		total += volInfo.Total
	}
	// Filter volsInfo and update the volInfo.
	volInfo = removeDuplicateVols(volsInfo)[0]
	volInfo.Free = free
	volInfo.Total = total
	return volInfo, nil
}

// isLeafDirectoryXL - check if a given path is leaf directory. i.e
// if it contains file xlMetaV1File
func isLeafDirectoryXL(disk StorageAPI, volume, leafPath string) (isLeaf bool) {
	_, err := disk.StatFile(volume, path.Join(leafPath, xlMetaV1File))
	return err == nil
}

// ListDir - return all the entries at the given directory path.
// If an entry is a directory it will be returned with a trailing "/".
func (xl XL) ListDir(volume, dirPath string) (entries []string, err error) {
	if !isValidVolname(volume) {
		return nil, errInvalidArgument
	}
	// FIXME: need someway to figure out which disk has the latest namespace
	// so that Listing can be done there. One option is always do Listing from
	// the "local" disk - if it is down user has to list using another XL server.
	// This way user knows from which disk he is listing from.

	for _, disk := range xl.storageDisks {
		if entries, err = disk.ListDir(volume, dirPath); err != nil {
			continue
		}
		for i, entry := range entries {
			if strings.HasSuffix(entry, slashSeparator) && isLeafDirectoryXL(disk, volume, path.Join(dirPath, entry)) {
				entries[i] = strings.TrimSuffix(entry, slashSeparator)
			}
		}
	}
	return
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
	errFileNotFoundErr := 0
	for index, metadata := range partsMetadata {
		if errs[index] == nil {
			onlineDiskCount++
			if metadata.Stat.Version == higherVersion && mdata == nil {
				mdata = &metadata
			}
		} else if errs[index] == errFileNotFound {
			errFileNotFoundErr++
		}
	}

	if errFileNotFoundErr == len(xl.storageDisks) {
		return errFileNotFound
	} else if mdata == nil || onlineDiskCount < xl.writeQuorum {
		// return error if mdata is empty or onlineDiskCount doesn't meet write quorum
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
			// Safely close and remove.
			if err = safeCloseAndRemove(metadataWriter); err != nil {
				return err
			}
			return err
		}
		// Safely wrote, now rename to its actual location.
		if err = metadataWriter.Close(); err != nil {
			log.WithFields(logrus.Fields{
				"volume":    volume,
				"path":      path,
				"diskIndex": index,
			}).Errorf("Metadata commit failed with %s", err)
			if err = safeCloseAndRemove(metadataWriter); err != nil {
				return err
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
	// Return success.
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

	// Hold read lock at source before rename.
	nsMutex.RLock(srcVolume, srcPath)
	defer nsMutex.RUnlock(srcVolume, srcPath)

	// Hold write lock at destination before rename.
	nsMutex.Lock(dstVolume, dstPath)
	defer nsMutex.Unlock(dstVolume, dstPath)

	for index, disk := range xl.storageDisks {
		// Make sure to rename all the files only, not directories.
		srcErasurePartPath := slashpath.Join(srcPath, fmt.Sprintf("file.%d", index))
		dstErasurePartPath := slashpath.Join(dstPath, fmt.Sprintf("file.%d", index))
		err := disk.RenameFile(srcVolume, srcErasurePartPath, dstVolume, dstErasurePartPath)
		if err != nil {
			log.WithFields(logrus.Fields{
				"srcVolume": srcVolume,
				"srcPath":   srcErasurePartPath,
				"dstVolume": dstVolume,
				"dstPath":   dstErasurePartPath,
			}).Errorf("RenameFile failed with %s", err)
			return err
		}
		srcXLMetaPath := slashpath.Join(srcPath, xlMetaV1File)
		dstXLMetaPath := slashpath.Join(dstPath, xlMetaV1File)
		err = disk.RenameFile(srcVolume, srcXLMetaPath, dstVolume, dstXLMetaPath)
		if err != nil {
			log.WithFields(logrus.Fields{
				"srcVolume": srcVolume,
				"srcPath":   srcXLMetaPath,
				"dstVolume": dstVolume,
				"dstPath":   dstXLMetaPath,
			}).Errorf("RenameFile failed with %s", err)
			return err
		}
		err = disk.DeleteFile(srcVolume, srcPath)
		if err != nil {
			log.WithFields(logrus.Fields{
				"srcVolume": srcVolume,
				"srcPath":   srcPath,
			}).Errorf("DeleteFile failed with %s", err)
			return err
		}
	}
	return nil
}
