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
	"encoding/json"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/skyrings/skyring-common/tools/uuid"
)

// uploadInfo -
type uploadInfo struct {
	UploadID  string    `json:"uploadId"`
	Initiated time.Time `json:"initiated"`
}

// uploadsV1 -
type uploadsV1 struct {
	Version string       `json:"version"`
	Format  string       `json:"format"`
	Uploads []uploadInfo `json:"uploadIds"`
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
func readUploadsJSON(bucket, object string, storageDisks ...StorageAPI) (uploadIDs uploadsV1, err error) {
	uploadJSONPath := path.Join(mpartMetaPrefix, bucket, object, uploadsJSONFile)
	var errs = make([]error, len(storageDisks))
	var uploads = make([]uploadsV1, len(storageDisks))
	var wg = &sync.WaitGroup{}

	// Read `uploads.json` from all disks.
	for index, disk := range storageDisks {
		wg.Add(1)
		// Read `uploads.json` in a routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			var buffer = make([]byte, blockSizeV1) // Allocate blockSized buffer.
			n, rErr := disk.ReadFile(minioMetaBucket, uploadJSONPath, int64(0), buffer)
			if rErr != nil {
				errs[index] = rErr
				return
			}
			rErr = json.Unmarshal(buffer[:n], &uploads[index])
			if rErr != nil {
				errs[index] = rErr
				return
			}
			buffer = nil
			errs[index] = nil
		}(index, disk)
	}

	// Wait for all the routines.
	wg.Wait()

	// Return for first error.
	for _, err = range errs {
		if err != nil {
			return uploadsV1{}, err
		}
	}

	// FIXME: Do not know if it should pick the picks the first successful one and returns.
	return uploads[0], nil
}

// uploadUploadsJSON - update `uploads.json` with new uploadsJSON for all disks.
func updateUploadsJSON(bucket, object string, uploadsJSON uploadsV1, storageDisks ...StorageAPI) error {
	uploadsPath := path.Join(mpartMetaPrefix, bucket, object, uploadsJSONFile)
	uniqueID := getUUID()
	tmpUploadsPath := path.Join(tmpMetaPrefix, uniqueID)
	var errs = make([]error, len(storageDisks))
	var wg = &sync.WaitGroup{}

	// Update `uploads.json` for all the disks.
	for index, disk := range storageDisks {
		wg.Add(1)
		// Update `uploads.json` in routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			uploadsBytes, wErr := json.Marshal(uploadsJSON)
			if wErr != nil {
				errs[index] = wErr
				return
			}
			n, wErr := disk.AppendFile(minioMetaBucket, tmpUploadsPath, uploadsBytes)
			if wErr != nil {
				errs[index] = wErr
				return
			}
			if n != int64(len(uploadsBytes)) {
				errs[index] = errUnexpected
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

	// Return for first error.
	for _, err := range errs {
		if err != nil {
			return err
		}
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
	uploadsJSON, err = readUploadsJSON(bucket, object, storageDisks...)
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
		wg.Add(1)
		// Update `uploads.json` in a routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			uploadsJSONBytes, wErr := json.Marshal(&uploadsJSON)
			if wErr != nil {
				errs[index] = wErr
				return
			}
			n, wErr := disk.AppendFile(minioMetaBucket, tmpUploadsPath, uploadsJSONBytes)
			if wErr != nil {
				errs[index] = wErr
				return
			}
			if n != int64(len(uploadsJSONBytes)) {
				errs[index] = errUnexpected
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

	// Return for first error encountered.
	for _, err = range errs {
		if err != nil {
			return err
		}
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
	disk := xl.getRandomDisk() // Choose a random disk.
	_, err := disk.StatFile(bucket, pathJoin(prefix, uploadsJSONFile))
	return err == nil
}

// listUploadsInfo - list all uploads info.
func (xl xlObjects) listUploadsInfo(prefixPath string) (uploadsInfo []uploadInfo, err error) {
	disk := xl.getRandomDisk() // Choose a random disk on each attempt.
	splitPrefixes := strings.SplitN(prefixPath, "/", 3)
	uploadsJSON, err := readUploadsJSON(splitPrefixes[1], splitPrefixes[2], disk)
	if err != nil {
		if err == errFileNotFound {
			return []uploadInfo{}, nil
		}
		return nil, err
	}
	uploadsInfo = uploadsJSON.Uploads
	return uploadsInfo, nil
}

// listMultipartUploadsCommon - lists all multipart uploads, common
// function for both object layers.
func (xl xlObjects) listMultipartUploadsCommon(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (ListMultipartsInfo, error) {
	result := ListMultipartsInfo{}
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return ListMultipartsInfo{}, BucketNameInvalid{Bucket: bucket}
	}
	if !xl.isBucketExist(bucket) {
		return ListMultipartsInfo{}, BucketNotFound{Bucket: bucket}
	}
	if !IsValidObjectPrefix(prefix) {
		return ListMultipartsInfo{}, ObjectNameInvalid{Bucket: bucket, Object: prefix}
	}
	// Verify if delimiter is anything other than '/', which we do not support.
	if delimiter != "" && delimiter != slashSeparator {
		return ListMultipartsInfo{}, UnsupportedDelimiter{
			Delimiter: delimiter,
		}
	}
	// Verify if marker has prefix.
	if keyMarker != "" && !strings.HasPrefix(keyMarker, prefix) {
		return ListMultipartsInfo{}, InvalidMarkerPrefixCombination{
			Marker: keyMarker,
			Prefix: prefix,
		}
	}
	if uploadIDMarker != "" {
		if strings.HasSuffix(keyMarker, slashSeparator) {
			return result, InvalidUploadIDKeyCombination{
				UploadIDMarker: uploadIDMarker,
				KeyMarker:      keyMarker,
			}
		}
		id, err := uuid.Parse(uploadIDMarker)
		if err != nil {
			return result, err
		}
		if id.IsZero() {
			return result, MalformedUploadID{
				UploadID: uploadIDMarker,
			}
		}
	}

	recursive := true
	if delimiter == slashSeparator {
		recursive = false
	}

	result.IsTruncated = true
	result.MaxUploads = maxUploads
	result.KeyMarker = keyMarker
	result.Prefix = prefix
	result.Delimiter = delimiter

	// Not using path.Join() as it strips off the trailing '/'.
	multipartPrefixPath := pathJoin(mpartMetaPrefix, bucket, prefix)
	if prefix == "" {
		// Should have a trailing "/" if prefix is ""
		// For ex. multipartPrefixPath should be "multipart/bucket/" if prefix is ""
		multipartPrefixPath += slashSeparator
	}
	multipartMarkerPath := ""
	if keyMarker != "" {
		multipartMarkerPath = pathJoin(mpartMetaPrefix, bucket, keyMarker)
	}
	var uploads []uploadMetadata
	var err error
	var eof bool
	if uploadIDMarker != "" {
		uploads, _, err = listMultipartUploadIDs(bucket, keyMarker, uploadIDMarker, maxUploads, xl.getRandomDisk())
		if err != nil {
			return ListMultipartsInfo{}, err
		}
		maxUploads = maxUploads - len(uploads)
	}
	if maxUploads > 0 {
		walker := xl.lookupTreeWalkXL(listParams{minioMetaBucket, recursive, multipartMarkerPath, multipartPrefixPath})
		if walker == nil {
			walker = xl.startTreeWalkXL(minioMetaBucket, multipartPrefixPath, multipartMarkerPath, recursive, xl.isMultipartUpload)
		}
		for maxUploads > 0 {
			walkResult, ok := <-walker.ch
			if !ok {
				// Closed channel.
				eof = true
				break
			}
			// For any walk error return right away.
			if walkResult.err != nil {
				// File not found or Disk not found is a valid case.
				if walkResult.err == errFileNotFound || walkResult.err == errDiskNotFound {
					eof = true
					break
				}
				return ListMultipartsInfo{}, err
			}
			entry := strings.TrimPrefix(walkResult.entry, retainSlash(pathJoin(mpartMetaPrefix, bucket)))
			if strings.HasSuffix(walkResult.entry, slashSeparator) {
				uploads = append(uploads, uploadMetadata{
					Object: entry,
				})
				maxUploads--
				if maxUploads == 0 {
					if walkResult.end {
						eof = true
						break
					}
				}
				continue
			}
			var newUploads []uploadMetadata
			var end bool
			uploadIDMarker = ""
			newUploads, end, err = listMultipartUploadIDs(bucket, entry, uploadIDMarker, maxUploads, xl.getRandomDisk())
			if err != nil {
				return ListMultipartsInfo{}, err
			}
			uploads = append(uploads, newUploads...)
			maxUploads -= len(newUploads)
			if walkResult.end && end {
				eof = true
				break
			}
		}
	}
	// Loop through all the received uploads fill in the multiparts result.
	for _, upload := range uploads {
		var objectName string
		var uploadID string
		if strings.HasSuffix(upload.Object, slashSeparator) {
			// All directory entries are common prefixes.
			uploadID = "" // Upload ids are empty for CommonPrefixes.
			objectName = upload.Object
			result.CommonPrefixes = append(result.CommonPrefixes, objectName)
		} else {
			uploadID = upload.UploadID
			objectName = upload.Object
			result.Uploads = append(result.Uploads, upload)
		}
		result.NextKeyMarker = objectName
		result.NextUploadIDMarker = uploadID
	}
	result.IsTruncated = !eof
	if !result.IsTruncated {
		result.NextKeyMarker = ""
		result.NextUploadIDMarker = ""
	}
	return result, nil
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
