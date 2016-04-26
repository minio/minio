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
	slashpath "path"
	"path/filepath"

	"github.com/Sirupsen/logrus"
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
func listFileVersions(partsMetadata []fileMetadata, errs []error) (versions []int64, err error) {
	versions = make([]int64, len(partsMetadata))
	for index, metadata := range partsMetadata {
		if errs[index] == nil {
			var version int64
			version, err = metadata.GetFileVersion()
			if err == errMetadataKeyNotExist {
				log.WithFields(logrus.Fields{
					"metadata": metadata,
				}).Errorf("Missing 'file.version', %s", errMetadataKeyNotExist)
				versions[index] = 0
				continue
			}
			if err != nil {
				log.WithFields(logrus.Fields{
					"metadata": metadata,
				}).Errorf("'file.version' decoding failed with %s", err)
				// Unexpected, return error.
				return nil, err
			}
			versions[index] = version
		} else {
			versions[index] = -1
		}
	}
	return versions, nil
}

// Returns slice of online disks needed.
// - slice returing readable disks.
// - fileMetadata
// - bool value indicating if healing is needed.
// - error if any.
func (xl XL) listOnlineDisks(volume, path string) (onlineDisks []StorageAPI, mdata fileMetadata, heal bool, err error) {
	partsMetadata, errs := xl.getPartsMetadata(volume, path)
	notFoundCount := 0
	// FIXME: take care of the situation when a disk has failed and been removed
	// by looking at the error returned from the fs layer. fs-layer will have
	// to return an error indicating that the disk is not available and should be
	// different from ErrNotExist.
	for _, err := range errs {
		if err == errFileNotFound {
			notFoundCount++
			// If we have errors with file not found greater than allowed read
			// quorum we return err as errFileNotFound.
			if notFoundCount > xl.readQuorum {
				return nil, fileMetadata{}, false, errFileNotFound
			}
		}
	}
	highestVersion := int64(0)
	onlineDisks = make([]StorageAPI, len(xl.storageDisks))
	// List all the file versions from partsMetadata list.
	versions, err := listFileVersions(partsMetadata, errs)
	if err != nil {
		log.WithFields(logrus.Fields{
			"volume": volume,
			"path":   path,
		}).Errorf("Extracting file versions failed with %s", err)
		return nil, fileMetadata{}, false, err
	}

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
			log.WithFields(logrus.Fields{
				"volume":          volume,
				"path":            path,
				"onlineDiskCount": onlineDiskCount,
				"readQuorumCount": xl.readQuorum,
			}).Errorf("%s", errReadQuorum)
			return nil, fileMetadata{}, false, errReadQuorum
		}
	}
	return onlineDisks, mdata, heal, nil
}

// Get parts.json metadata as a map slice.
// Returns error slice indicating the failed metadata reads.
// Read lockNS() should be done by caller.
func (xl XL) getPartsMetadata(volume, path string) ([]fileMetadata, []error) {
	errs := make([]error, len(xl.storageDisks))
	metadataArray := make([]fileMetadata, len(xl.storageDisks))
	metadataFilePath := slashpath.Join(path, metadataFile)
	for index, disk := range xl.storageDisks {
		offset := int64(0)
		metadataReader, err := disk.ReadFile(volume, metadataFilePath, offset)
		if err != nil {
			errs[index] = err
			continue
		}
		defer metadataReader.Close()

		metadata, err := fileMetadataDecode(metadataReader)
		if err != nil {
			// Unable to parse parts.json, set error.
			errs[index] = err
			continue
		}
		metadataArray[index] = metadata
	}
	return metadataArray, errs
}

// Writes/Updates `parts.json` for given file. updateParts carries
// index of disks where `parts.json` needs to be updated.
//
// Returns collection of errors, indexed in accordance with input
// updateParts order.
// Write lockNS() should be done by caller.
func (xl XL) setPartsMetadata(volume, path string, metadata fileMetadata, updateParts []bool) []error {
	metadataFilePath := filepath.Join(path, metadataFile)
	errs := make([]error, len(xl.storageDisks))

	for index := range updateParts {
		errs[index] = errors.New("Metadata not updated")
	}

	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		for index := range updateParts {
			errs[index] = err
		}
		return errs
	}

	for index, shouldUpdate := range updateParts {
		if !shouldUpdate {
			continue
		}
		writer, err := xl.storageDisks[index].CreateFile(volume, metadataFilePath)
		errs[index] = err
		if err != nil {
			continue
		}
		_, err = writer.Write(metadataBytes)
		if err != nil {
			errs[index] = err
			safeCloseAndRemove(writer)
			continue
		}
		writer.Close()
	}
	return errs
}
