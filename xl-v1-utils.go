package main

import (
	"encoding/json"
	"errors"
	slashpath "path"
	"path/filepath"
)

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
