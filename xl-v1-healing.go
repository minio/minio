package main

import (
	"encoding/json"
	"path"
	"sync"
)

// Get the highest integer from a given integer slice.
func highestInt(intSlice []int64) (highestInteger int64) {
	highestInteger = int64(1)
	for _, integer := range intSlice {
		if highestInteger < integer {
			highestInteger = integer
		}
	}
	return highestInteger
}

// Extracts objects versions from xlMetaV1 slice and returns version slice.
func listObjectVersions(partsMetadata []xlMetaV1, errs []error) (versions []int64) {
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

// Reads all `xl.json` metadata as a xlMetaV1 slice.
// Returns error slice indicating the failed metadata reads.
func (xl xlObjects) readAllXLMetadata(bucket, object string) ([]xlMetaV1, []error) {
	errs := make([]error, len(xl.storageDisks))
	metadataArray := make([]xlMetaV1, len(xl.storageDisks))
	xlMetaPath := path.Join(object, xlMetaJSONFile)
	var wg = &sync.WaitGroup{}
	for index, disk := range xl.storageDisks {
		if disk == nil {
			errs[index] = errDiskNotFound
			continue
		}
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			buffer, err := readAll(disk, bucket, xlMetaPath)
			if err != nil {
				errs[index] = err
				return
			}
			err = json.Unmarshal(buffer, &metadataArray[index])
			if err != nil {
				// Unable to parse xl.json, set error.
				errs[index] = err
				return
			}
			// Relinquish buffer.
			buffer = nil
			errs[index] = nil
		}(index, disk)
	}

	// Wait for all the routines to finish.
	wg.Wait()

	// Return all the metadata.
	return metadataArray, errs
}

// error based on total errors and read quorum.
func (xl xlObjects) reduceError(errs []error) error {
	fileNotFoundCount := 0
	longNameCount := 0
	diskNotFoundCount := 0
	volumeNotFoundCount := 0
	diskAccessDeniedCount := 0
	for _, err := range errs {
		if err == errFileNotFound {
			fileNotFoundCount++
		} else if err == errFileNameTooLong {
			longNameCount++
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
	// else if we have errors with 'volume not found'
	// greater than readQuorum, return as errVolumeNotFound.
	if fileNotFoundCount > len(xl.storageDisks)-xl.readQuorum {
		return errFileNotFound
	} else if longNameCount > len(xl.storageDisks)-xl.readQuorum {
		return errFileNameTooLong
	} else if volumeNotFoundCount > len(xl.storageDisks)-xl.readQuorum {
		return errVolumeNotFound
	}
	// If we have errors with disk not found equal to the
	// number of disks, return as errDiskNotFound.
	if diskNotFoundCount == len(xl.storageDisks) {
		return errDiskNotFound
	} else if diskNotFoundCount > len(xl.storageDisks)-xl.readQuorum {
		// If we have errors with 'disk not found'
		// greater than readQuorum, return as errFileNotFound.
		return errFileNotFound
	}
	// If we have errors with disk not found equal to the
	// number of disks, return as errDiskNotFound.
	if diskAccessDeniedCount == len(xl.storageDisks) {
		return errVolumeAccessDenied
	}
	return nil
}

// Similar to 'len(slice)' but returns  the actualelements count
// skipping the unallocated elements.
func diskCount(disks []StorageAPI) int {
	diskCount := 0
	for _, disk := range disks {
		if disk == nil {
			continue
		}
		diskCount++
	}
	return diskCount
}

// Returns slice of online disks needed.
// - slice returing readable disks.
// - xlMetaV1
// - bool value indicating if healing is needed.
// - error if any.
func (xl xlObjects) listOnlineDisks(partsMetadata []xlMetaV1, errs []error) (onlineDisks []StorageAPI, version int64, err error) {
	onlineDisks = make([]StorageAPI, len(xl.storageDisks))
	if err = xl.reduceError(errs); err != nil {
		if err == errFileNotFound {
			// For file not found, treat as if disks are available
			// return all the configured ones.
			onlineDisks = xl.storageDisks
			return onlineDisks, 1, nil
		}
		return nil, 0, err
	}
	highestVersion := int64(0)
	// List all the file versions from partsMetadata list.
	versions := listObjectVersions(partsMetadata, errs)

	// Get highest object version.
	highestVersion = highestInt(versions)

	// Pick online disks with version set to highestVersion.
	for index, version := range versions {
		if version == highestVersion {
			onlineDisks[index] = xl.storageDisks[index]
		} else {
			onlineDisks[index] = nil
		}
	}
	return onlineDisks, highestVersion, nil
}

// Return disks with the latest version of the object.
func (xl xlObjects) latestDisks(partsMetadata []xlMetaV1, errs []error) (latestDisks []StorageAPI) {
	latestDisks = make([]StorageAPI, len(xl.storageDisks))
	highestVersion := int64(0)
	// List all the file versions from partsMetadata list.
	versions := listObjectVersions(partsMetadata, errs)

	// Get highest object version.
	highestVersion = highestInt(versions)

	for index, version := range versions {
		if version == highestVersion {
			latestDisks[index] = xl.storageDisks[index]
		}
	}
	return latestDisks
}

// Return disks with the outdated version or missing object.
func (xl xlObjects) outDatedDisks(partsMetadata []xlMetaV1, errs []error) (outDatedDisks []StorageAPI) {
	outDatedDisks = make([]StorageAPI, len(xl.storageDisks))
	highestVersion := int64(0)
	// List all the file versions from partsMetadata list.
	versions := listObjectVersions(partsMetadata, errs)

	// Get highest object version.
	highestVersion = highestInt(versions)

	for index, version := range versions {
		if errs[index] == errFileNotFound {
			outDatedDisks[index] = xl.storageDisks[index]
			continue
		}
		if errs[index] != nil {
			continue
		}
		if version < highestVersion {
			outDatedDisks[index] = xl.storageDisks[index]
		}
	}
	return outDatedDisks
}

// Return xlMetaV1 of the latest version of the object.
func xlLatestMetadata(partsMetadata []xlMetaV1, errs []error) (latestMeta xlMetaV1) {
	highestVersion := int64(0)
	// List all the file versions from partsMetadata list.
	versions := listObjectVersions(partsMetadata, errs)

	// Get highest object version.
	highestVersion = highestInt(versions)

	for index, version := range versions {
		if version == highestVersion {
			latestMeta = partsMetadata[index]
			break
		}
	}
	return
}

// Returns if the object should be healed.
func xlShouldHeal(partsMetadata []xlMetaV1, errs []error) bool {
	highestVersion := int64(0)
	// List all the file versions from partsMetadata list.
	versions := listObjectVersions(partsMetadata, errs)

	// Get highest object version.
	highestVersion = highestInt(versions)

	for index, version := range versions {
		if errs[index] == errFileNotFound {
			return true
		}
		if errs[index] != nil {
			continue
		}
		if version < highestVersion {
			return true
		}
	}
	return false
}
