package main

import (
	"encoding/json"
	"errors"
	"path/filepath"
)

// Get parts.json metadata as a map slice.
// Returns error slice indicating the failed metadata reads.
func (xl XL) getMetadata(volume, path string) ([]map[string]string, []error) {
	errSlice := make([]error, len(xl.storageDisks))
	metadataSlice := make([]map[string]string, len(xl.storageDisks))
	metadataFilePath := filepath.Join(path, metadataFile)
	for i, disk := range xl.storageDisks {
		metadata := make(map[string]string)
		offset := int64(0)
		metadataReader, err := disk.ReadFile(volume, metadataFilePath, offset)
		if err != nil {
			errSlice[i] = err
			continue
		}
		defer metadataReader.Close()

		decoder := json.NewDecoder(metadataReader)
		if err = decoder.Decode(&metadata); err != nil {
			// Unable to parse parts.json, set error.
			errSlice[i] = err
			continue
		}
		metadataSlice[i] = metadata
	}
	return metadataSlice, errSlice
}

// Set parts.json as from map slice. updateSlice indicates where all parts.json needs to be written.
// Returns error slice indicating where errors happened.
func (xl XL) setMetadata(volume, path string, metadata map[string]string, updateSlice []bool) []error {
	metadataFilePath := filepath.Join(path, metadataFile)
	errSlice := make([]error, len(xl.storageDisks))

	for i := range updateSlice {
		errSlice[i] = errors.New("metadata not updated")
	}

	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		for i := range updateSlice {
			errSlice[i] = err
		}
		return errSlice
	}

	for i, update := range updateSlice {
		if !update {
			continue
		}
		writer, err := xl.storageDisks[i].CreateFile(volume, metadataFilePath)
		errSlice[i] = err
		if err != nil {
			continue
		}
		_, err = writer.Write(metadataBytes)
		if err != nil {
			errSlice[i] = err
			safeCloseAndRemove(writer)
			continue
		}
		writer.Close()
	}
	return errSlice
}
