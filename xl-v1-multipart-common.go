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
	"bytes"
	"encoding/json"
	"path"
	"sort"
	"strings"
	"sync"
	"time"
)

// A uploadInfo represents the s3 compatible spec.
type uploadInfo struct {
	UploadID  string    `json:"uploadId"`  // UploadID for the active multipart upload.
	Deleted   bool      `json:"deleted"`   // Currently unused, for future use.
	Initiated time.Time `json:"initiated"` // Indicates when the uploadID was initiated.
}

// A uploadsV1 represents `uploads.json` metadata header.
type uploadsV1 struct {
	Version string       `json:"version"`   // Version of the current `uploads.json`
	Format  string       `json:"format"`    // Format of the current `uploads.json`
	Uploads []uploadInfo `json:"uploadIds"` // Captures all the upload ids for a given object.
}

// byInitiatedTime is a collection satisfying sort.Interface.
type byInitiatedTime []uploadInfo

func (t byInitiatedTime) Len() int      { return len(t) }
func (t byInitiatedTime) Swap(i, j int) { t[i], t[j] = t[j], t[i] }
func (t byInitiatedTime) Less(i, j int) bool {
	return t[i].Initiated.Before(t[j].Initiated)
}

// AddUploadID - adds a new upload id in order of its initiated time.
func (u *uploadsV1) AddUploadID(uploadID string, initiated time.Time) {
	u.Uploads = append(u.Uploads, uploadInfo{
		UploadID:  uploadID,
		Initiated: initiated,
	})
	sort.Sort(byInitiatedTime(u.Uploads))
}

// Index - returns the index of matching the upload id.
func (u uploadsV1) Index(uploadID string) int {
	for i, u := range u.Uploads {
		if u.UploadID == uploadID {
			return i
		}
	}
	return -1
}

// readUploadsJSON - get all the saved uploads JSON.
func readUploadsJSON(bucket, object string, disk StorageAPI) (uploadIDs uploadsV1, err error) {
	// Staging buffer of 128KiB kept for reading `uploads.json`.
	var buf = make([]byte, 128*1024)

	// Writer holding `uploads.json` content.
	var buffer = new(bytes.Buffer)

	uploadJSONPath := path.Join(mpartMetaPrefix, bucket, object, uploadsJSONFile)
	// Reads entire `uploads.json`.
	if err = copyBuffer(buffer, disk, minioMetaBucket, uploadJSONPath, buf); err != nil {
		return uploadsV1{}, err
	}

	// Decode `uploads.json`.
	d := json.NewDecoder(buffer)
	if err = d.Decode(&uploadIDs); err != nil {
		return uploadsV1{}, err
	}

	// Success.
	return uploadIDs, nil
}

// updateUploadsJSON - update `uploads.json` with new uploadsJSON for all disks.
func updateUploadsJSON(bucket, object string, uploadsJSON uploadsV1, storageDisks ...StorageAPI) error {
	uploadsPath := path.Join(mpartMetaPrefix, bucket, object, uploadsJSONFile)
	uniqueID := getUUID()
	tmpUploadsPath := path.Join(tmpMetaPrefix, uniqueID)
	var errs = make([]error, len(storageDisks))
	var wg = &sync.WaitGroup{}

	// Update `uploads.json` for all the disks.
	for index, disk := range storageDisks {
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
				errs[index] = wErr
				return
			}
			if wErr = disk.AppendFile(minioMetaBucket, tmpUploadsPath, uploadsBytes); wErr != nil {
				errs[index] = wErr
				return
			}
			if wErr = disk.RenameFile(minioMetaBucket, tmpUploadsPath, minioMetaBucket, uploadsPath); wErr != nil {
				errs[index] = wErr
				return
			}
		}(index, disk)
	}

	// Wait for all the routines to finish updating `uploads.json`
	wg.Wait()

	// For only single disk return first error.
	if len(storageDisks) == 1 {
		return errs[0]
	} // else count all the errors for quorum validation.
	var errCount = 0
	// Return for first error.
	for _, err := range errs {
		if err != nil {
			errCount++
		}
	}
	// Count all the errors and validate if we have write quorum.
	if errCount > len(storageDisks)-len(storageDisks)/2+3 {
		// Validate if we have read quorum return success.
		if errCount > len(storageDisks)-len(storageDisks)/2+1 {
			return nil
		}
		// Rename `uploads.json` left over back to tmp location.
		for index, disk := range storageDisks {
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
		return errXLWriteQuorum
	}
	return nil
}

// newUploadsV1 - initialize new uploads v1.
func newUploadsV1(format string) uploadsV1 {
	uploadIDs := uploadsV1{}
	uploadIDs.Version = "1"
	uploadIDs.Format = format
	return uploadIDs
}

// writeUploadJSON - create `uploads.json` or update it with new uploadID.
func writeUploadJSON(bucket, object, uploadID string, initiated time.Time, storageDisks ...StorageAPI) (err error) {
	uploadsPath := path.Join(mpartMetaPrefix, bucket, object, uploadsJSONFile)
	uniqueID := getUUID()
	tmpUploadsPath := path.Join(tmpMetaPrefix, uniqueID)

	var errs = make([]error, len(storageDisks))
	var wg = &sync.WaitGroup{}

	var uploadsJSON uploadsV1
	for _, disk := range storageDisks {
		if disk == nil {
			continue
		}
		uploadsJSON, err = readUploadsJSON(bucket, object, disk)
		break
	}
	if err != nil {
		// For any other errors.
		if err != errFileNotFound {
			return err
		}
		if len(storageDisks) == 1 {
			// Set uploads format to `fs` for single disk.
			uploadsJSON = newUploadsV1("fs")
		} else {
			// Set uploads format to `xl` otherwise.
			uploadsJSON = newUploadsV1("xl")
		}
	}
	// Add a new upload id.
	uploadsJSON.AddUploadID(uploadID, initiated)

	// Update `uploads.json` on all disks.
	for index, disk := range storageDisks {
		if disk == nil {
			errs[index] = errDiskNotFound
			continue
		}
		wg.Add(1)
		// Update `uploads.json` in a routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			uploadsJSONBytes, wErr := json.Marshal(&uploadsJSON)
			if wErr != nil {
				errs[index] = wErr
				return
			}
			// Write `uploads.json` to disk.
			if wErr = disk.AppendFile(minioMetaBucket, tmpUploadsPath, uploadsJSONBytes); wErr != nil {
				errs[index] = wErr
				return
			}
			wErr = disk.RenameFile(minioMetaBucket, tmpUploadsPath, minioMetaBucket, uploadsPath)
			if wErr != nil {
				if dErr := disk.DeleteFile(minioMetaBucket, tmpUploadsPath); dErr != nil {
					errs[index] = dErr
					return
				}
				errs[index] = wErr
				return
			}
			errs[index] = nil
		}(index, disk)
	}

	// Wait for all the writes to finish.
	wg.Wait()

	// For only single disk return first error.
	if len(storageDisks) == 1 {
		return errs[0]
	} // else count all the errors for quorum validation.
	var errCount = 0
	// Return for first error.
	for _, err := range errs {
		if err != nil {
			errCount++
		}
	}
	// Count all the errors and validate if we have write quorum.
	if errCount > len(storageDisks)-len(storageDisks)/2+3 {
		// Validate if we have read quorum return success.
		if errCount > len(storageDisks)-len(storageDisks)/2+1 {
			return nil
		}
		// Rename `uploads.json` left over back to tmp location.
		for index, disk := range storageDisks {
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
		return errXLWriteQuorum
	}
	return nil
}

// Wrapper which removes all the uploaded parts.
func cleanupUploadedParts(bucket, object, uploadID string, storageDisks ...StorageAPI) error {
	var errs = make([]error, len(storageDisks))
	var wg = &sync.WaitGroup{}

	// Construct uploadIDPath.
	uploadIDPath := path.Join(mpartMetaPrefix, bucket, object, uploadID)

	// Cleanup uploadID for all disks.
	for index, disk := range storageDisks {
		if disk == nil {
			errs[index] = errDiskNotFound
			continue
		}
		wg.Add(1)
		// Cleanup each uploadID in a routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			err := cleanupDir(disk, minioMetaBucket, uploadIDPath)
			if err != nil {
				errs[index] = err
				return
			}
			errs[index] = nil
		}(index, disk)
	}

	// Wait for all the cleanups to finish.
	wg.Wait()

	// Return first error.
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// listMultipartUploadIDs - list all the upload ids from a marker up to 'count'.
func listMultipartUploadIDs(bucketName, objectName, uploadIDMarker string, count int, disk StorageAPI) ([]uploadMetadata, bool, error) {
	var uploads []uploadMetadata
	// Read `uploads.json`.
	uploadsJSON, err := readUploadsJSON(bucketName, objectName, disk)
	if err != nil {
		return nil, false, err
	}
	index := 0
	if uploadIDMarker != "" {
		for ; index < len(uploadsJSON.Uploads); index++ {
			if uploadsJSON.Uploads[index].UploadID == uploadIDMarker {
				// Skip the uploadID as it would already be listed in previous listing.
				index++
				break
			}
		}
	}
	for index < len(uploadsJSON.Uploads) {
		uploads = append(uploads, uploadMetadata{
			Object:    objectName,
			UploadID:  uploadsJSON.Uploads[index].UploadID,
			Initiated: uploadsJSON.Uploads[index].Initiated,
		})
		count--
		index++
		if count == 0 {
			break
		}
	}
	end := (index == len(uploadsJSON.Uploads))
	return uploads, end, nil
}

// Returns if the prefix is a multipart upload.
func (xl xlObjects) isMultipartUpload(bucket, prefix string) bool {
	for _, disk := range xl.getLoadBalancedQuorumDisks() {
		if disk == nil {
			continue
		}
		_, err := disk.StatFile(bucket, pathJoin(prefix, uploadsJSONFile))
		if err != nil {
			// For any reason disk was deleted or goes offline, continue
			if err == errDiskNotFound || err == errFaultyDisk {
				continue
			}
			return false
		}
		break
	}
	return true
}

// listUploadsInfo - list all uploads info.
func (xl xlObjects) listUploadsInfo(prefixPath string) (uploadsInfo []uploadInfo, err error) {
	for _, disk := range xl.getLoadBalancedQuorumDisks() {
		if disk == nil {
			continue
		}
		splitPrefixes := strings.SplitN(prefixPath, "/", 3)
		var uploadsJSON uploadsV1
		uploadsJSON, err = readUploadsJSON(splitPrefixes[1], splitPrefixes[2], disk)
		if err != nil {
			// For any reason disk was deleted or goes offline, continue
			if err == errDiskNotFound || err == errFaultyDisk {
				continue
			}
			if err == errFileNotFound {
				return []uploadInfo{}, nil
			}
			return nil, err
		}
		uploadsInfo = uploadsJSON.Uploads
		break
	}
	return uploadsInfo, nil
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
	for _, disk := range xl.getLoadBalancedQuorumDisks() {
		if disk == nil {
			continue
		}
		fileInfo, err = disk.StatFile(minioMetaBucket, partNamePath)
		if err != nil {
			// For any reason disk was deleted or goes offline, continue
			if err == errDiskNotFound {
				continue
			}
			return FileInfo{}, err
		}
		break
	}
	return fileInfo, nil
}

// commitXLMetadata - commit `xl.json` from source prefix to destination prefix.
func (xl xlObjects) commitXLMetadata(srcPrefix, dstPrefix string) error {
	var wg = &sync.WaitGroup{}
	var mErrs = make([]error, len(xl.storageDisks))

	srcJSONFile := path.Join(srcPrefix, xlMetaJSONFile)
	dstJSONFile := path.Join(dstPrefix, xlMetaJSONFile)

	// Rename `xl.json` to all disks in parallel.
	for index, disk := range xl.storageDisks {
		if disk == nil {
			mErrs[index] = errDiskNotFound
			continue
		}
		wg.Add(1)
		// Rename `xl.json` in a routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			// Renames `xl.json` from source prefix to destination prefix.
			rErr := disk.RenameFile(minioMetaBucket, srcJSONFile, minioMetaBucket, dstJSONFile)
			if rErr != nil {
				mErrs[index] = rErr
				return
			}
			// Delete any dangling directories.
			dErr := disk.DeleteFile(minioMetaBucket, srcPrefix)
			if dErr != nil {
				mErrs[index] = dErr
				return
			}
			mErrs[index] = nil
		}(index, disk)
	}
	// Wait for all the routines.
	wg.Wait()

	// Do we have write quorum?.
	if !isQuorum(mErrs, xl.writeQuorum) {
		// Do we have read quorum?.
		if isQuorum(mErrs, xl.readQuorum) {
			// Return success on read quorum.
			return nil
		}
		return errXLWriteQuorum
	}
	// For all other errors return.
	for _, err := range mErrs {
		if err != nil && err != errDiskNotFound {
			return err
		}
	}
	return nil
}
