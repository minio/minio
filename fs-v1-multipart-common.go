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
	"path"
	"strings"
)

// Returns if the prefix is a multipart upload.
func (fs fsObjects) isMultipartUpload(bucket, prefix string) bool {
	_, err := fs.storage.StatFile(bucket, pathJoin(prefix, uploadsJSONFile))
	return err == nil
}

// listUploadsInfo - list all uploads info.
func (fs fsObjects) listUploadsInfo(prefixPath string) (uploads []uploadInfo, err error) {
	splitPrefixes := strings.SplitN(prefixPath, "/", 3)
	uploadIDs, err := readUploadsJSON(splitPrefixes[1], splitPrefixes[2], fs.storage)
	if err != nil {
		if err == errFileNotFound {
			return []uploadInfo{}, nil
		}
		return nil, err
	}
	uploads = uploadIDs.Uploads
	return uploads, nil
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
