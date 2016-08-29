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
	"time"
)

// Returns if the prefix is a multipart upload.
func (fs fsObjects) isMultipartUpload(bucket, prefix string) bool {
	_, err := fs.storage.StatFile(bucket, pathJoin(prefix, uploadsJSONFile))
	return err == nil
}

// Checks whether bucket exists.
func (fs fsObjects) isBucketExist(bucket string) bool {
	// Check whether bucket exists.
	_, err := fs.storage.StatVol(bucket)
	if err != nil {
		if err == errVolumeNotFound {
			return false
		}
		errorIf(err, "Stat failed on bucket "+bucket+".")
		return false
	}
	return true
}

// isUploadIDExists - verify if a given uploadID exists and is valid.
func (fs fsObjects) isUploadIDExists(bucket, object, uploadID string) bool {
	uploadIDPath := path.Join(mpartMetaPrefix, bucket, object, uploadID)
	_, err := fs.storage.StatFile(minioMetaBucket, path.Join(uploadIDPath, fsMetaJSONFile))
	if err != nil {
		if err == errFileNotFound {
			return false
		}
		errorIf(err, "Unable to access upload id"+uploadIDPath)
		return false
	}
	return true
}

// writeUploadJSON - create `uploads.json` or update it with new uploadID.
func (fs fsObjects) writeUploadJSON(bucket, object, uploadID string, initiated time.Time) (err error) {
	uploadsPath := path.Join(mpartMetaPrefix, bucket, object, uploadsJSONFile)
	uniqueID := getUUID()
	tmpUploadsPath := path.Join(tmpMetaPrefix, uniqueID)
	var uploadsJSON uploadsV1
	uploadsJSON, err = readUploadsJSON(bucket, object, fs.storage)
	if err != nil {
		// uploads.json might not exist hence ignore errFileNotFound.
		if errorCause(err) != errFileNotFound {
			return err
		}
		// Set uploads format to `fs`.
		uploadsJSON = newUploadsV1("fs")
	}
	// Add a new upload id.
	uploadsJSON.AddUploadID(uploadID, initiated)

	// Update `uploads.json` on all disks.
	uploadsJSONBytes, wErr := json.Marshal(&uploadsJSON)
	if wErr != nil {
		return traceError(wErr)
	}
	// Write `uploads.json` to disk.
	if wErr = fs.storage.AppendFile(minioMetaBucket, tmpUploadsPath, uploadsJSONBytes); wErr != nil {
		return traceError(wErr)
	}
	wErr = fs.storage.RenameFile(minioMetaBucket, tmpUploadsPath, minioMetaBucket, uploadsPath)
	if wErr != nil {
		if dErr := fs.storage.DeleteFile(minioMetaBucket, tmpUploadsPath); dErr != nil {
			return traceError(dErr)
		}
		return traceError(wErr)
	}
	return nil
}

// updateUploadsJSON - update `uploads.json` with new uploadsJSON for all disks.
func (fs fsObjects) updateUploadsJSON(bucket, object string, uploadsJSON uploadsV1) (err error) {
	uploadsPath := path.Join(mpartMetaPrefix, bucket, object, uploadsJSONFile)
	uniqueID := getUUID()
	tmpUploadsPath := path.Join(tmpMetaPrefix, uniqueID)
	uploadsBytes, wErr := json.Marshal(uploadsJSON)
	if wErr != nil {
		return traceError(wErr)
	}
	if wErr = fs.storage.AppendFile(minioMetaBucket, tmpUploadsPath, uploadsBytes); wErr != nil {
		return traceError(wErr)
	}
	if wErr = fs.storage.RenameFile(minioMetaBucket, tmpUploadsPath, minioMetaBucket, uploadsPath); wErr != nil {
		return traceError(wErr)
	}
	return nil
}
