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
	"io"
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

// ReadFrom - read from implements io.ReaderFrom interface for unmarshalling uploads.
func (u *uploadsV1) ReadFrom(reader io.Reader) (n int64, err error) {
	var buffer bytes.Buffer
	n, err = buffer.ReadFrom(reader)
	if err != nil {
		return 0, err
	}
	err = json.Unmarshal(buffer.Bytes(), &u)
	return n, err
}

// WriteTo - write to implements io.WriterTo interface for marshalling uploads.
func (u uploadsV1) WriteTo(writer io.Writer) (n int64, err error) {
	metadataBytes, err := json.Marshal(&u)
	if err != nil {
		return 0, err
	}
	m, err := writer.Write(metadataBytes)
	return int64(m), err
}

// getUploadIDs - get all the saved upload id's.
func getUploadIDs(bucket, object string, storageDisks ...StorageAPI) (uploadIDs uploadsV1, err error) {
	uploadJSONPath := path.Join(mpartMetaPrefix, bucket, object, uploadsJSONFile)
	var errs = make([]error, len(storageDisks))
	var uploads = make([]uploadsV1, len(storageDisks))
	var wg = &sync.WaitGroup{}

	for index, disk := range storageDisks {
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			r, rErr := disk.ReadFile(minioMetaBucket, uploadJSONPath, int64(0))
			if rErr != nil {
				errs[index] = rErr
				return
			}
			defer r.Close()
			_, rErr = uploads[index].ReadFrom(r)
			if rErr != nil {
				errs[index] = rErr
				return
			}
			errs[index] = nil
		}(index, disk)
	}
	wg.Wait()

	for _, err = range errs {
		if err != nil {
			return uploadsV1{}, err
		}
	}

	// FIXME: Do not know if it should pick the picks the first successful one and returns.
	return uploads[0], nil
}

func updateUploadJSON(bucket, object string, uploadIDs uploadsV1, storageDisks ...StorageAPI) error {
	uploadsPath := path.Join(mpartMetaPrefix, bucket, object, uploadsJSONFile)
	var errs = make([]error, len(storageDisks))
	var wg = &sync.WaitGroup{}

	for index, disk := range storageDisks {
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			w, wErr := disk.CreateFile(minioMetaBucket, uploadsPath)
			if wErr != nil {
				errs[index] = wErr
				return
			}
			_, wErr = uploadIDs.WriteTo(w)
			if wErr != nil {
				errs[index] = wErr
				return
			}
			if wErr = w.Close(); wErr != nil {
				if clErr := safeCloseAndRemove(w); clErr != nil {
					errs[index] = clErr
					return
				}
				errs[index] = wErr
				return
			}
		}(index, disk)
	}

	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return err
		}
	}

	return nil
}

// writeUploadJSON - create `uploads.json` or update it with new uploadID.
func writeUploadJSON(bucket, object, uploadID string, initiated time.Time, storageDisks ...StorageAPI) error {
	uploadsPath := path.Join(mpartMetaPrefix, bucket, object, uploadsJSONFile)
	tmpUploadsPath := path.Join(tmpMetaPrefix, bucket, object, uploadsJSONFile)

	var errs = make([]error, len(storageDisks))
	var wg = &sync.WaitGroup{}

	uploadIDs, err := getUploadIDs(bucket, object, storageDisks...)
	if err != nil && err != errFileNotFound {
		return err
	}
	uploadIDs.Version = "1"
	uploadIDs.Format = "xl"
	uploadIDs.AddUploadID(uploadID, initiated)

	for index, disk := range storageDisks {
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			w, wErr := disk.CreateFile(minioMetaBucket, tmpUploadsPath)
			if wErr != nil {
				errs[index] = wErr
				return
			}
			_, wErr = uploadIDs.WriteTo(w)
			if wErr != nil {
				errs[index] = wErr
				return
			}
			if wErr = w.Close(); wErr != nil {
				if clErr := safeCloseAndRemove(w); clErr != nil {
					errs[index] = clErr
					return
				}
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

	wg.Wait()

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
	for index, disk := range storageDisks {
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			err := cleanupDir(disk, minioMetaBucket, path.Join(mpartMetaPrefix, bucket, object, uploadID))
			if err != nil {
				errs[index] = err
				return
			}
			errs[index] = nil
		}(index, disk)
	}
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// Returns if the prefix is a multipart upload.
func (xl xlObjects) isMultipartUpload(bucket, prefix string) bool {
	// Create errs and volInfo slices of storageDisks size.
	var errs = make([]error, len(xl.storageDisks))

	// Allocate a new waitgroup.
	var wg = &sync.WaitGroup{}
	for index, disk := range xl.storageDisks {
		wg.Add(1)
		// Stat file on all the disks in a routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			_, err := disk.StatFile(bucket, path.Join(prefix, uploadsJSONFile))
			if err != nil {
				errs[index] = err
				return
			}
			errs[index] = nil
		}(index, disk)
	}

	// Wait for all the Stat operations to finish.
	wg.Wait()

	var errFileNotFoundCount int
	for _, err := range errs {
		if err != nil {
			if err == errFileNotFound {
				errFileNotFoundCount++
				// If we have errors with file not found greater than allowed read
				// quorum we return err as errFileNotFound.
				if errFileNotFoundCount > len(xl.storageDisks)-xl.readQuorum {
					return false
				}
				continue
			}
			errorIf(err, "Unable to access file "+path.Join(bucket, prefix))
			return false
		}
	}
	return true
}

// listUploadsInfo - list all uploads info.
func (xl xlObjects) listUploadsInfo(prefixPath string) (uploads []uploadInfo, err error) {
	disk := xl.getRandomDisk() // Choose a random disk on each attempt.
	splitPrefixes := strings.SplitN(prefixPath, "/", 3)
	uploadIDs, err := getUploadIDs(splitPrefixes[1], splitPrefixes[2], disk)
	if err != nil {
		if err == errFileNotFound {
			return []uploadInfo{}, nil
		}
		return nil, err
	}
	uploads = uploadIDs.Uploads
	return uploads, nil
}

func (xl xlObjects) listMultipartUploadIDs(bucketName, objectName, uploadIDMarker string, count int) ([]uploadMetadata, bool, error) {
	var uploads []uploadMetadata
	uploadsJSONContent, err := getUploadIDs(bucketName, objectName, xl.getRandomDisk())
	if err != nil {
		return nil, false, err
	}
	index := 0
	if uploadIDMarker != "" {
		for ; index < len(uploadsJSONContent.Uploads); index++ {
			if uploadsJSONContent.Uploads[index].UploadID == uploadIDMarker {
				// Skip the uploadID as it would already be listed in previous listing.
				index++
				break
			}
		}
	}
	for index < len(uploadsJSONContent.Uploads) {
		uploads = append(uploads, uploadMetadata{
			Object:    objectName,
			UploadID:  uploadsJSONContent.Uploads[index].UploadID,
			Initiated: uploadsJSONContent.Uploads[index].Initiated,
		})
		count--
		index++
		if count == 0 {
			break
		}
	}
	return uploads, index == len(uploadsJSONContent.Uploads), nil
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
		uploads, _, err = xl.listMultipartUploadIDs(bucket, keyMarker, uploadIDMarker, maxUploads)
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
			var tmpUploads []uploadMetadata
			var end bool
			uploadIDMarker = ""
			tmpUploads, end, err = xl.listMultipartUploadIDs(bucket, entry, uploadIDMarker, maxUploads)
			if err != nil {
				return ListMultipartsInfo{}, err
			}
			uploads = append(uploads, tmpUploads...)
			maxUploads -= len(tmpUploads)
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
