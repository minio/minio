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
)

// Returns slice of disks needed for ReadFile operation:
// - slice returing readable disks.
// - fileMetadata
// - bool value indicating if selfHeal is needed.
// - error if any.
func (xl XL) getReadableDisks(volume, path string) ([]StorageAPI, fileMetadata, bool, error) {
	partsMetadata, errs := xl.getPartsMetadata(volume, path)
	highestVersion := int64(0)
	versions := make([]int64, len(xl.storageDisks))
	quorumDisks := make([]StorageAPI, len(xl.storageDisks))
	notFoundCount := 0
	// If quorum says errFileNotFound return errFileNotFound
	for _, err := range errs {
		if err == errFileNotFound {
			notFoundCount++
		}
	}
	if notFoundCount > xl.readQuorum {
		return nil, fileMetadata{}, false, errFileNotFound
	}
	for index, metadata := range partsMetadata {
		if errs[index] == nil {
			version, err := metadata.GetFileVersion()
			if err == errMetadataKeyNotExist {
				versions[index] = 0
				continue
			}
			if err != nil {
				// Unexpected, return error.
				return nil, fileMetadata{}, false, err
			}
			versions[index] = version
		} else {
			versions[index] = -1
		}
	}
	quorumCount := 0
	for index, version := range versions {
		if version == highestVersion {
			quorumDisks[index] = xl.storageDisks[index]
			quorumCount++
		} else {
			quorumDisks[index] = nil
		}
	}
	if quorumCount < xl.readQuorum {
		return nil, fileMetadata{}, false, errReadQuorum
	}
	var metadata fileMetadata
	for index, disk := range quorumDisks {
		if disk == nil {
			continue
		}
		metadata = partsMetadata[index]
		break
	}
	// FIXME: take care of the situation when a disk has failed and been removed
	// by looking at the error returned from the fs layer. fs-layer will have
	// to return an error indicating that the disk is not available and should be
	// different from ErrNotExist.
	doSelfHeal := quorumCount != len(xl.storageDisks)
	return quorumDisks, metadata, doSelfHeal, nil
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
