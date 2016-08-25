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
	"encoding/json"
	"path"
	"sync"
	"time"
)

// updateUploadsJSON - update `uploads.json` with new uploadsJSON for all disks.
func (xl xlObjects) updateUploadsJSON(bucket, object string, uploadsJSON uploadsV1) (err error) {
	uploadsPath := path.Join(mpartMetaPrefix, bucket, object, uploadsJSONFile)
	uniqueID := getUUID()
	tmpUploadsPath := path.Join(tmpMetaPrefix, uniqueID)
	var errs = make([]error, len(xl.storageDisks))
	var wg = &sync.WaitGroup{}

	// Update `uploads.json` for all the disks.
	for index, disk := range xl.storageDisks {
		if disk == nil {
			errs[index] = errDiskNotFound
			continue
		}
		wg.Add(1)
		// Update `uploads.json` in routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			uploadsBytes, wErr := json.Marshal(uploadsJSON)
			if wErr != nil {
				errs[index] = traceError(wErr)
				return
			}
			if wErr = disk.AppendFile(minioMetaBucket, tmpUploadsPath, uploadsBytes); wErr != nil {
				errs[index] = traceError(wErr)
				return
			}
			if wErr = disk.RenameFile(minioMetaBucket, tmpUploadsPath, minioMetaBucket, uploadsPath); wErr != nil {
				errs[index] = traceError(wErr)
				return
			}
		}(index, disk)
	}

	// Wait for all the routines to finish updating `uploads.json`
	wg.Wait()

	// Count all the errors and validate if we have write quorum.
	if !isDiskQuorum(errs, xl.writeQuorum) {
		// Do we have readQuorum?.
		if isDiskQuorum(errs, xl.readQuorum) {
			return nil
		}
		// Rename `uploads.json` left over back to tmp location.
		for index, disk := range xl.storageDisks {
			if disk == nil {
				continue
			}
			// Undo rename `uploads.json` in parallel.
			wg.Add(1)
			go func(index int, disk StorageAPI) {
				defer wg.Done()
				if errs[index] != nil {
					return
				}
				_ = disk.RenameFile(minioMetaBucket, uploadsPath, minioMetaBucket, tmpUploadsPath)
			}(index, disk)
		}
		wg.Wait()
		return traceError(errXLWriteQuorum)
	}
	return nil
}

// Reads uploads.json from any of the load balanced disks.
func (xl xlObjects) readUploadsJSON(bucket, object string) (uploadsJSON uploadsV1, err error) {
	for _, disk := range xl.getLoadBalancedDisks() {
		if disk == nil {
			continue
		}
		uploadsJSON, err = readUploadsJSON(bucket, object, disk)
		if err == nil {
			return uploadsJSON, nil
		}
		if isErrIgnored(err, objMetadataOpIgnoredErrs) {
			continue
		}
		break
	}
	return uploadsV1{}, err
}

// writeUploadJSON - create `uploads.json` or update it with new uploadID.
func (xl xlObjects) writeUploadJSON(bucket, object, uploadID string, initiated time.Time) (err error) {
	uploadsPath := path.Join(mpartMetaPrefix, bucket, object, uploadsJSONFile)
	uniqueID := getUUID()
	tmpUploadsPath := path.Join(tmpMetaPrefix, uniqueID)

	var errs = make([]error, len(xl.storageDisks))
	var wg = &sync.WaitGroup{}

	// Reads `uploads.json` and returns error.
	uploadsJSON, err := xl.readUploadsJSON(bucket, object)
	if err != nil {
		if errorCause(err) != errFileNotFound {
			return err
		}
		// Set uploads format to `xl` otherwise.
		uploadsJSON = newUploadsV1("xl")
	}
	// Add a new upload id.
	uploadsJSON.AddUploadID(uploadID, initiated)

	// Update `uploads.json` on all disks.
	for index, disk := range xl.storageDisks {
		if disk == nil {
			errs[index] = traceError(errDiskNotFound)
			continue
		}
		wg.Add(1)
		// Update `uploads.json` in a routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			uploadsJSONBytes, wErr := json.Marshal(&uploadsJSON)
			if wErr != nil {
				errs[index] = traceError(wErr)
				return
			}
			// Write `uploads.json` to disk.
			if wErr = disk.AppendFile(minioMetaBucket, tmpUploadsPath, uploadsJSONBytes); wErr != nil {
				errs[index] = traceError(wErr)
				return
			}
			wErr = disk.RenameFile(minioMetaBucket, tmpUploadsPath, minioMetaBucket, uploadsPath)
			if wErr != nil {
				if dErr := disk.DeleteFile(minioMetaBucket, tmpUploadsPath); dErr != nil {
					errs[index] = traceError(dErr)
					return
				}
				errs[index] = traceError(wErr)
				return
			}
			errs[index] = nil
		}(index, disk)
	}

	// Wait here for all the writes to finish.
	wg.Wait()

	// Do we have write quorum?.
	if !isDiskQuorum(errs, xl.writeQuorum) {
		// Rename `uploads.json` left over back to tmp location.
		for index, disk := range xl.storageDisks {
			if disk == nil {
				continue
			}
			// Undo rename `uploads.json` in parallel.
			wg.Add(1)
			go func(index int, disk StorageAPI) {
				defer wg.Done()
				if errs[index] != nil {
					return
				}
				_ = disk.RenameFile(minioMetaBucket, uploadsPath, minioMetaBucket, tmpUploadsPath)
			}(index, disk)
		}
		wg.Wait()
		return traceError(errXLWriteQuorum)
	}

	// Ignored errors list.
	ignoredErrs := []error{
		errDiskNotFound,
		errFaultyDisk,
		errDiskAccessDenied,
	}
	return reduceErrs(errs, ignoredErrs)
}

// Returns if the prefix is a multipart upload.
func (xl xlObjects) isMultipartUpload(bucket, prefix string) bool {
	for _, disk := range xl.getLoadBalancedDisks() {
		if disk == nil {
			continue
		}
		_, err := disk.StatFile(bucket, pathJoin(prefix, uploadsJSONFile))
		if err == nil {
			return true
		}
		// For any reason disk was deleted or goes offline, continue
		if isErrIgnored(err, objMetadataOpIgnoredErrs) {
			continue
		}
		break
	}
	return false
}

// isUploadIDExists - verify if a given uploadID exists and is valid.
func (xl xlObjects) isUploadIDExists(bucket, object, uploadID string) bool {
	uploadIDPath := path.Join(mpartMetaPrefix, bucket, object, uploadID)
	return xl.isObject(minioMetaBucket, uploadIDPath)
}

// Removes part given by partName belonging to a mulitpart upload from minioMetaBucket
func (xl xlObjects) removeObjectPart(bucket, object, uploadID, partName string) {
	curpartPath := path.Join(mpartMetaPrefix, bucket, object, uploadID, partName)
	wg := sync.WaitGroup{}
	for i, disk := range xl.storageDisks {
		if disk == nil {
			continue
		}
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			// Ignoring failure to remove parts that weren't present in CompleteMultipartUpload
			// requests. xl.json is the authoritative source of truth on which parts constitute
			// the object. The presence of parts that don't belong in the object doesn't affect correctness.
			_ = disk.DeleteFile(minioMetaBucket, curpartPath)
		}(i, disk)
	}
	wg.Wait()
}

// statPart - returns fileInfo structure for a successful stat on part file.
func (xl xlObjects) statPart(bucket, object, uploadID, partName string) (fileInfo FileInfo, err error) {
	partNamePath := path.Join(mpartMetaPrefix, bucket, object, uploadID, partName)
	for _, disk := range xl.getLoadBalancedDisks() {
		if disk == nil {
			continue
		}
		fileInfo, err = disk.StatFile(minioMetaBucket, partNamePath)
		if err == nil {
			return fileInfo, nil
		}
		err = traceError(err)
		// For any reason disk was deleted or goes offline we continue to next disk.
		if isErrIgnored(err, objMetadataOpIgnoredErrs) {
			continue
		}

		// Catastrophic error, we return.
		break
	}
	return FileInfo{}, err
}

// commitXLMetadata - commit `xl.json` from source prefix to destination prefix in the given slice of disks.
func commitXLMetadata(disks []StorageAPI, srcPrefix, dstPrefix string, quorum int) error {
	var wg = &sync.WaitGroup{}
	var mErrs = make([]error, len(disks))

	srcJSONFile := path.Join(srcPrefix, xlMetaJSONFile)
	dstJSONFile := path.Join(dstPrefix, xlMetaJSONFile)

	// Rename `xl.json` to all disks in parallel.
	for index, disk := range disks {
		if disk == nil {
			mErrs[index] = traceError(errDiskNotFound)
			continue
		}
		wg.Add(1)
		// Rename `xl.json` in a routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			// Delete any dangling directories.
			defer disk.DeleteFile(minioMetaBucket, srcPrefix)

			// Renames `xl.json` from source prefix to destination prefix.
			rErr := disk.RenameFile(minioMetaBucket, srcJSONFile, minioMetaBucket, dstJSONFile)
			if rErr != nil {
				mErrs[index] = traceError(rErr)
				return
			}
			mErrs[index] = nil
		}(index, disk)
	}
	// Wait for all the routines.
	wg.Wait()

	// Do we have write Quorum?.
	if !isDiskQuorum(mErrs, quorum) {
		// Delete all `xl.json` successfully renamed.
		deleteAllXLMetadata(disks, minioMetaBucket, dstPrefix, mErrs)
		return traceError(errXLWriteQuorum)
	}

	// List of ignored errors.
	ignoredErrs := []error{
		errDiskNotFound,
		errDiskAccessDenied,
		errFaultyDisk,
	}
	return reduceErrs(mErrs, ignoredErrs)
}
