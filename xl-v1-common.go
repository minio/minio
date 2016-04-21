package main

import (
	"encoding/json"
	"errors"
	slashpath "path"
	"path/filepath"
	"strconv"
)

// Get parts.json metadata as a map slice.
// Returns error slice indicating the failed metadata reads.
// Read lockNS() should be done by caller.
func (xl XL) getPartsMetadata(volume, path string) ([]map[string]string, []error) {
	errs := make([]error, len(xl.storageDisks))
	metadataArray := make([]map[string]string, len(xl.storageDisks))
	metadataFilePath := slashpath.Join(path, metadataFile)
	for index, disk := range xl.storageDisks {
		metadata := make(map[string]string)
		offset := int64(0)
		metadataReader, err := disk.ReadFile(volume, metadataFilePath, offset)
		if err != nil {
			errs[index] = err
			continue
		}
		defer metadataReader.Close()

		decoder := json.NewDecoder(metadataReader)
		if err = decoder.Decode(&metadata); err != nil {
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
func (xl XL) setPartsMetadata(volume, path string, metadata map[string]string, updateParts []bool) []error {
	metadataFilePath := filepath.Join(path, metadataFile)
	errs := make([]error, len(xl.storageDisks))

	for index := range updateParts {
		errs[index] = errors.New("metadata not updated")
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

// Returns slice of disks needed for ReadFile operation:
// - slice returing readable disks.
// - file size
// - error if any.
func (xl XL) getReadableDisks(volume, path string) ([]StorageAPI, map[string]string, bool, error) {
	partsMetadata, errs := xl.getPartsMetadata(volume, path)
	highestVersion := int64(0)
	versions := make([]int64, len(xl.storageDisks))
	quorumDisks := make([]StorageAPI, len(xl.storageDisks))
	for index, metadata := range partsMetadata {
		if errs[index] == nil {
			if versionStr, ok := metadata["file.version"]; ok {
				// Convert string to integer.
				version, err := strconv.ParseInt(versionStr, 10, 64)
				if err != nil {
					// Unexpected, return error.
					return nil, nil, false, err
				}
				if version > highestVersion {
					highestVersion = version
				}
				versions[index] = version
			} else {
				versions[index] = 0
			}
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
		return nil, nil, false, errReadQuorum
	}
	var metadata map[string]string
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
