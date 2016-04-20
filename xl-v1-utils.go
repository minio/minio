package main

import (
	"encoding/json"
	"errors"
	slashpath "path"
	"path/filepath"
)

// Get parts.json metadata as a map slice.
// Returns error slice indicating the failed metadata reads.
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
