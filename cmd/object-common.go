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

package cmd

import (
	"path/filepath"
	"strings"
	"sync"
)

const (
	// Block size used for all internal operations version 1.
	blockSizeV1 = 10 * 1024 * 1024 // 10MiB.

	// Staging buffer read size for all internal operations version 1.
	readSizeV1 = 128 * 1024 // 128KiB.

	// Buckets meta prefix.
	bucketMetaPrefix = "buckets"
)

// isErrIgnored should we ignore this error?, takes a list of errors which can be ignored.
func isErrIgnored(err error, ignoredErrs []error) bool {
	for _, ignoredErr := range ignoredErrs {
		if ignoredErr == err {
			return true
		}
	}
	return false
}

// House keeping code needed for FS.
func fsHouseKeeping(storageDisk StorageAPI) error {
	// Cleanup all temp entries upon start.
	err := cleanupDir(storageDisk, minioMetaBucket, tmpMetaPrefix)
	if err != nil {
		return toObjectErr(err, minioMetaBucket, tmpMetaPrefix)
	}
	return nil
}

// Depending on the disk type network or local, initialize storage API.
func newStorageAPI(disk string) (storage StorageAPI, err error) {
	if !strings.ContainsRune(disk, ':') || filepath.VolumeName(disk) != "" {
		// Initialize filesystem storage API.
		return newPosix(disk)
	}
	// Initialize rpc client storage API.
	return newRPCClient(disk)
}

// Initializes meta volume on all input storage disks.
func initMetaVolume(storageDisks []StorageAPI) error {
	// This happens for the first time, but keep this here since this
	// is the only place where it can be made expensive optimizing all
	// other calls. Create minio meta volume, if it doesn't exist yet.
	var wg = &sync.WaitGroup{}

	// Initialize errs to collect errors inside go-routine.
	var errs = make([]error, len(storageDisks))

	// Initialize all disks in parallel.
	for index, disk := range storageDisks {
		if disk == nil {
			// Ignore create meta volume on disks which are not found.
			continue
		}
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			// Indicate this wait group is done.
			defer wg.Done()

			// Attempt to create `.minio`.
			err := disk.MakeVol(minioMetaBucket)
			if err != nil {
				switch err {
				// Ignored errors.
				case errVolumeExists, errDiskNotFound, errFaultyDisk:
				default:
					errs[index] = err
				}
			}
		}(index, disk)
	}

	// Wait for all cleanup to finish.
	wg.Wait()

	// Return upon first error.
	for _, err := range errs {
		if err == nil {
			continue
		}
		return toObjectErr(err, minioMetaBucket)
	}

	// Return success here.
	return nil
}

// House keeping code needed for XL.
func xlHouseKeeping(storageDisks []StorageAPI) error {
	// This happens for the first time, but keep this here since this
	// is the only place where it can be made expensive optimizing all
	// other calls. Create metavolume.
	var wg = &sync.WaitGroup{}

	// Initialize errs to collect errors inside go-routine.
	var errs = make([]error, len(storageDisks))

	// Initialize all disks in parallel.
	for index, disk := range storageDisks {
		if disk == nil {
			continue
		}
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			// Indicate this wait group is done.
			defer wg.Done()

			// Cleanup all temp entries upon start.
			err := cleanupDir(disk, minioMetaBucket, tmpMetaPrefix)
			if err != nil {
				errs[index] = err
			}
		}(index, disk)
	}

	// Wait for all cleanup to finish.
	wg.Wait()

	// Return upon first error.
	for _, err := range errs {
		if err == nil {
			continue
		}
		return toObjectErr(err, minioMetaBucket, tmpMetaPrefix)
	}

	// Return success here.
	return nil
}

// Cleanup a directory recursively.
func cleanupDir(storage StorageAPI, volume, dirPath string) error {
	var delFunc func(string) error
	// Function to delete entries recursively.
	delFunc = func(entryPath string) error {
		if !strings.HasSuffix(entryPath, slashSeparator) {
			// Delete the file entry.
			return storage.DeleteFile(volume, entryPath)
		}

		// If it's a directory, list and call delFunc() for each entry.
		entries, err := storage.ListDir(volume, entryPath)
		// If entryPath prefix never existed, safe to ignore.
		if err == errFileNotFound {
			return nil
		} else if err != nil { // For any other errors fail.
			return err
		} // else on success..

		// Recurse and delete all other entries.
		for _, entry := range entries {
			if err = delFunc(pathJoin(entryPath, entry)); err != nil {
				return err
			}
		}
		return nil
	}
	err := delFunc(retainSlash(pathJoin(dirPath)))
	return err
}
