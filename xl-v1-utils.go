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
	for index, disk := range xl.storageDisks {
		metadata := make(map[string]string)
		offset := int64(0)
		metadataReader, err := disk.ReadFile(volume, metadataFilePath, offset)
		if err != nil {
			errSlice[index] = err
			continue
		}
		defer metadataReader.Close()

		decoder := json.NewDecoder(metadataReader)
		if err = decoder.Decode(&metadata); err != nil {
			// Unable to parse parts.json, set error.
			errSlice[index] = err
			continue
		}
		metadataSlice[index] = metadata
	}
	return metadataSlice, errSlice
}

// Set parts.json as from map slice. updateSlice indicates where all parts.json needs to be written.
// Returns error slice indicating where errors happened.
func (xl XL) setMetadata(volume, path string, metadata map[string]string, updateSlice []bool) []error {
	metadataFilePath := filepath.Join(path, metadataFile)
	errSlice := make([]error, len(xl.storageDisks))

	for index := range updateSlice {
		errSlice[index] = errors.New("metadata not updated")
	}

	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		for index := range updateSlice {
			errSlice[index] = err
		}
		return errSlice
	}

	for index, update := range updateSlice {
		if !update {
			continue
		}
		writer, err := xl.storageDisks[index].CreateFile(volume, metadataFilePath)
		errSlice[index] = err
		if err != nil {
			continue
		}
		_, err = writer.Write(metadataBytes)
		if err != nil {
			errSlice[index] = err
			safeCloseAndRemove(writer)
			continue
		}
		writer.Close()
	}
	return errSlice
}
