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
	"sort"
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

// RemoveUploadID - removes upload id from uploads metadata.
func (u *uploadsV1) RemoveUploadID(uploadID string) {
	// If the uploadID is absent, we do nothing.
	for i, uInfo := range u.Uploads {
		if uInfo.UploadID == uploadID {
			u.Uploads = append(u.Uploads[:i], u.Uploads[i+1:]...)
			break
		}
	}
}

// readUploadsJSON - get all the saved uploads JSON.
func readUploadsJSON(bucket, object string, disk StorageAPI) (uploadIDs uploadsV1, err error) {
	uploadJSONPath := path.Join(bucket, object, uploadsJSONFile)
	// Reads entire `uploads.json`.
	buf, err := disk.ReadAll(minioMetaMultipartBucket, uploadJSONPath)
	if err != nil {
		return uploadsV1{}, traceError(err)
	}

	// Decode `uploads.json`.
	if err = json.Unmarshal(buf, &uploadIDs); err != nil {
		return uploadsV1{}, traceError(err)
	}

	// Success.
	return uploadIDs, nil
}

// newUploadsV1 - initialize new uploads v1.
func newUploadsV1(format string) uploadsV1 {
	uploadIDs := uploadsV1{}
	uploadIDs.Version = "1.0.0" // Should follow semantic versioning.
	uploadIDs.Format = format
	return uploadIDs
}

func writeUploadJSON(u *uploadsV1, uploadsPath, tmpPath string, disk StorageAPI) error {
	// Serialize to prepare to write to disk.
	uplBytes, wErr := json.Marshal(&u)
	if wErr != nil {
		return traceError(wErr)
	}

	// Write `uploads.json` to disk. First to tmp location and
	// then rename.
	if wErr = disk.AppendFile(minioMetaTmpBucket, tmpPath, uplBytes); wErr != nil {
		return traceError(wErr)
	}
	wErr = disk.RenameFile(minioMetaTmpBucket, tmpPath, minioMetaMultipartBucket, uploadsPath)
	if wErr != nil {
		if dErr := disk.DeleteFile(minioMetaTmpBucket, tmpPath); dErr != nil {
			// we return the most recent error.
			return traceError(dErr)
		}
		return traceError(wErr)
	}
	return nil
}

// Wrapper which removes all the uploaded parts.
func cleanupUploadedParts(bucket, object, uploadID string, storageDisks ...StorageAPI) error {
	var errs = make([]error, len(storageDisks))
	var wg = &sync.WaitGroup{}

	// Construct uploadIDPath.
	uploadIDPath := path.Join(bucket, object, uploadID)

	// Cleanup uploadID for all disks.
	for index, disk := range storageDisks {
		if disk == nil {
			errs[index] = traceError(errDiskNotFound)
			continue
		}
		wg.Add(1)
		// Cleanup each uploadID in a routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			err := cleanupDir(disk, minioMetaMultipartBucket, uploadIDPath)
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
