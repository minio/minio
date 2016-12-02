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
	"time"
)

// Returns if the prefix is a multipart upload.
func (fs fsObjects) isMultipartUpload(bucket, prefix string) bool {
	_, err := fs.storage.StatFile(bucket, pathJoin(prefix, uploadsJSONFile))
	return err == nil
}

// isUploadIDExists - verify if a given uploadID exists and is valid.
func (fs fsObjects) isUploadIDExists(bucket, object, uploadID string) bool {
	uploadIDPath := path.Join(bucket, object, uploadID)
	_, err := fs.storage.StatFile(minioMetaMultipartBucket, path.Join(uploadIDPath, fsMetaJSONFile))
	if err != nil {
		if err == errFileNotFound {
			return false
		}
		errorIf(err, "Unable to access upload id "+pathJoin(minioMetaMultipartBucket, uploadIDPath))
		return false
	}
	return true
}

// updateUploadJSON - add or remove upload ID info in all `uploads.json`.
func (fs fsObjects) updateUploadJSON(bucket, object, uploadID string, initiated time.Time, isRemove bool) error {
	uploadsPath := path.Join(bucket, object, uploadsJSONFile)
	tmpUploadsPath := mustGetUUID()

	uploadsJSON, err := readUploadsJSON(bucket, object, fs.storage)
	if errorCause(err) == errFileNotFound {
		// If file is not found, we assume a default (empty)
		// upload info.
		uploadsJSON, err = newUploadsV1("fs"), nil
	}
	if err != nil {
		return err
	}

	// update the uploadsJSON struct
	if !isRemove {
		// Add the uploadID
		uploadsJSON.AddUploadID(uploadID, initiated)
	} else {
		// Remove the upload ID
		uploadsJSON.RemoveUploadID(uploadID)
	}

	// update the file or delete it?
	if len(uploadsJSON.Uploads) > 0 {
		err = writeUploadJSON(&uploadsJSON, uploadsPath, tmpUploadsPath, fs.storage)
	} else {
		// no uploads, so we delete the file.
		if err = fs.storage.DeleteFile(minioMetaMultipartBucket, uploadsPath); err != nil {
			return toObjectErr(traceError(err), minioMetaMultipartBucket, uploadsPath)
		}
	}
	return err
}

// addUploadID - add upload ID and its initiated time to 'uploads.json'.
func (fs fsObjects) addUploadID(bucket, object string, uploadID string, initiated time.Time) error {
	return fs.updateUploadJSON(bucket, object, uploadID, initiated, false)
}

// removeUploadID - remove upload ID in 'uploads.json'.
func (fs fsObjects) removeUploadID(bucket, object string, uploadID string) error {
	return fs.updateUploadJSON(bucket, object, uploadID, time.Time{}, true)
}
