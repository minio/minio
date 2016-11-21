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
	"path"
	"sync"
)

// writeUploadJSON - create `uploads.json` or update it with change
// described in uCh.
func (xl xlObjects) updateUploadJSON(bucket, object string, uCh uploadIDChange) error {
	uploadsPath := path.Join(mpartMetaPrefix, bucket, object, uploadsJSONFile)
	uniqueID := getUUID()
	tmpUploadsPath := uniqueID

	// slice to store errors from disks
	errs := make([]error, len(xl.storageDisks))
	// slice to store if it is a delete operation on a disk
	isDelete := make([]bool, len(xl.storageDisks))

	wg := sync.WaitGroup{}
	for index, disk := range xl.storageDisks {
		if disk == nil {
			errs[index] = traceError(errDiskNotFound)
			continue
		}
		// Update `uploads.json` in a go routine.
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()

			// read and parse uploads.json on this disk
			uploadsJSON, err := readUploadsJSON(bucket, object, disk)
			if errorCause(err) == errFileNotFound {
				// If file is not found, we assume an
				// default (empty) upload info.
				uploadsJSON, err = newUploadsV1("xl"), nil
			}
			// If we have a read error, we store error and
			// exit.
			if err != nil {
				errs[index] = err
				return
			}

			if !uCh.isRemove {
				// Add the uploadID
				uploadsJSON.AddUploadID(uCh.uploadID, uCh.initiated)
			} else {
				// Remove the upload ID
				uploadsJSON.RemoveUploadID(uCh.uploadID)
				if len(uploadsJSON.Uploads) == 0 {
					isDelete[index] = true
				}
			}

			// For delete, rename to tmp, for the
			// possibility of recovery in case of quorum
			// failure.
			if !isDelete[index] {
				errs[index] = writeUploadJSON(&uploadsJSON, uploadsPath, tmpUploadsPath, disk)
			} else {
				wErr := disk.RenameFile(minioMetaBucket, uploadsPath, minioMetaTmpBucket, tmpUploadsPath)
				if wErr != nil {
					errs[index] = traceError(wErr)
				}

			}
		}(index, disk)
	}

	// Wait for all the writes to finish.
	wg.Wait()

	// Do we have write quorum?
	if !isDiskQuorum(errs, xl.writeQuorum) {
		// No quorum. Perform cleanup on the minority of disks
		// on which the operation succeeded.

		// There are two cases:
		//
		// 1. uploads.json file was updated -> we delete the
		//    file that we successfully overwrote on the
		//    minority of disks, so that the failed quorum
		//    operation is not partially visible.
		//
		// 2. uploads.json was deleted -> in this case since
		//    the delete failed, we restore from tmp.
		for index, disk := range xl.storageDisks {
			if disk == nil || errs[index] != nil {
				continue
			}
			wg.Add(1)
			go func(index int, disk StorageAPI) {
				defer wg.Done()
				if !isDelete[index] {
					_ = disk.DeleteFile(
						minioMetaBucket,
						uploadsPath,
					)
				} else {
					_ = disk.RenameFile(
						minioMetaTmpBucket, tmpUploadsPath,
						minioMetaBucket, uploadsPath,
					)
				}
			}(index, disk)
		}
		wg.Wait()
		return traceError(errXLWriteQuorum)
	}

	// we do have quorum, so in case of delete upload.json file
	// operation, we purge from tmp.
	for index, disk := range xl.storageDisks {
		if disk == nil || !isDelete[index] {
			continue
		}
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			// isDelete[index] = true at this point.
			_ = disk.DeleteFile(minioMetaTmpBucket, tmpUploadsPath)
		}(index, disk)
	}
	wg.Wait()

	return reduceErrs(errs, objectOpIgnoredErrs)
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
			defer disk.DeleteFile(minioMetaTmpBucket, srcPrefix)

			// Renames `xl.json` from source prefix to destination prefix.
			rErr := disk.RenameFile(minioMetaTmpBucket, srcJSONFile, minioMetaBucket, dstJSONFile)
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

	return reduceErrs(mErrs, objectOpIgnoredErrs)
}
