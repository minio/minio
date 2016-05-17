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
	"errors"
	slashpath "path"
	"sync"
)

// Get the highest integer from a given integer slice.
func highestInt(intSlice []int64) (highestInteger int64) {
	highestInteger = int64(0)
	for _, integer := range intSlice {
		if highestInteger < integer {
			highestInteger = integer
		}
	}
	return highestInteger
}

// Extracts file versions from partsMetadata slice and returns version slice.
func listFileVersions(partsMetadata []xlMetaV1, errs []error) (versions []int64) {
	versions = make([]int64, len(partsMetadata))
	for index, metadata := range partsMetadata {
		if errs[index] == nil {
			versions[index] = metadata.Stat.Version
		} else {
			versions[index] = -1
		}
	}
	return versions
}

// reduceError - convert collection of errors into a single
// error based on total errors and read quorum.
func (xl XL) reduceError(errs []error) error {
	fileNotFoundCount := 0
	diskNotFoundCount := 0
	volumeNotFoundCount := 0
	diskAccessDeniedCount := 0
	for _, err := range errs {
		if err == errFileNotFound {
			fileNotFoundCount++
		} else if err == errDiskNotFound {
			diskNotFoundCount++
		} else if err == errVolumeAccessDenied {
			diskAccessDeniedCount++
		} else if err == errVolumeNotFound {
			volumeNotFoundCount++
		}
	}
	// If we have errors with 'file not found' greater than
	// readQuorum, return as errFileNotFound.
	// else if we have errors with 'volume not found' greater than
	// readQuorum, return as errVolumeNotFound.
	if fileNotFoundCount > len(xl.storageDisks)-xl.readQuorum {
		return errFileNotFound
	} else if volumeNotFoundCount > len(xl.storageDisks)-xl.readQuorum {
		return errVolumeNotFound
	}
	// If we have errors with disk not found equal to the
	// number of disks, return as errDiskNotFound.
	if diskNotFoundCount == len(xl.storageDisks) {
		return errDiskNotFound
	} else if diskNotFoundCount > len(xl.storageDisks)-xl.readQuorum {
		// If we have errors with 'disk not found' greater than
		// readQuorum, return as errFileNotFound.
		return errFileNotFound
	}
	// If we have errors with disk not found equal to the
	// number of disks, return as errDiskNotFound.
	if diskAccessDeniedCount == len(xl.storageDisks) {
		return errVolumeAccessDenied
	}
	return nil
}

// Returns slice of online disks needed.
// - slice returing readable disks.
// - xlMetaV1
// - bool value indicating if healing is needed.
// - error if any.
func (xl XL) listOnlineDisks(volume, path string) (onlineDisks []StorageAPI, mdata xlMetaV1, heal bool, err error) {
	partsMetadata, errs := xl.getPartsMetadata(volume, path)
	if err = xl.reduceError(errs); err != nil {
		return nil, xlMetaV1{}, false, err
	}
	highestVersion := int64(0)
	onlineDisks = make([]StorageAPI, len(xl.storageDisks))
	// List all the file versions from partsMetadata list.
	versions := listFileVersions(partsMetadata, errs)

	// Get highest file version.
	highestVersion = highestInt(versions)

	// Pick online disks with version set to highestVersion.
	onlineDiskCount := 0
	for index, version := range versions {
		if version == highestVersion {
			mdata = partsMetadata[index]
			onlineDisks[index] = xl.storageDisks[index]
			onlineDiskCount++
		} else {
			onlineDisks[index] = nil
		}
	}

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
			return nil, xlMetaV1{}, false, errReadQuorum
		}
	}
	return onlineDisks, mdata, heal, nil
}

// Get file.json metadata as a map slice.
// Returns error slice indicating the failed metadata reads.
// Read lockNS() should be done by caller.
func (xl XL) getPartsMetadata(volume, path string) ([]xlMetaV1, []error) {
	errs := make([]error, len(xl.storageDisks))
	metadataArray := make([]xlMetaV1, len(xl.storageDisks))
	xlMetaV1FilePath := slashpath.Join(path, xlMetaV1File)
	var wg = &sync.WaitGroup{}
	for index, disk := range xl.storageDisks {
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			offset := int64(0)
			metadataReader, err := disk.ReadFile(volume, xlMetaV1FilePath, offset)
			if err != nil {
				errs[index] = err
				return
			}
			defer metadataReader.Close()

			metadata, err := xlMetaV1Decode(metadataReader)
			if err != nil {
				// Unable to parse file.json, set error.
				errs[index] = err
				return
			}
			metadataArray[index] = metadata
		}(index, disk)
	}
	wg.Wait()
	return metadataArray, errs
}

// Writes/Updates `file.json` for given file. updateParts carries
// index of disks where `file.json` needs to be updated.
//
// Returns collection of errors, indexed in accordance with input
// updateParts order.
// Write lockNS() should be done by caller.
func (xl XL) updatePartsMetadata(volume, path string, metadata xlMetaV1, updateParts []bool) []error {
	xlMetaV1FilePath := pathJoin(path, xlMetaV1File)
	errs := make([]error, len(xl.storageDisks))

	for index := range updateParts {
		errs[index] = errors.New("Metadata not updated")
	}

	for index, shouldUpdate := range updateParts {
		if !shouldUpdate {
			continue
		}
		writer, err := xl.storageDisks[index].CreateFile(volume, xlMetaV1FilePath)
		errs[index] = err
		if err != nil {
			continue
		}
		err = metadata.Write(writer)
		if err != nil {
			errs[index] = err
			safeCloseAndRemove(writer)
			continue
		}
		writer.Close()
	}
	return errs
}
