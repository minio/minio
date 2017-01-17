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
	"fmt"
	"io"
	"runtime"
	"time"

	pathutil "path"

	"github.com/minio/minio/pkg/lock"
)

// Returns if the prefix is a multipart upload.
func (fs fsObjects) isMultipartUpload(bucket, prefix string) bool {
	uploadsIDPath := pathJoin(fs.fsPath, bucket, prefix, uploadsJSONFile)
	_, err := fsStatFile(uploadsIDPath)
	if err != nil {
		if err == errFileNotFound {
			return false
		}
		errorIf(err, "Unable to access uploads.json "+uploadsIDPath)
		return false
	}
	return true
}

// Delete uploads.json file wrapper handling a tricky case on windows.
func (fs fsObjects) deleteUploadsJSON(bucket, object, uploadID string) error {
	timeID := fmt.Sprintf("%X", time.Now().UTC().UnixNano())
	tmpPath := pathJoin(fs.fsPath, minioMetaTmpBucket, fs.fsUUID, uploadID+"+"+timeID)

	multipartBucketPath := pathJoin(fs.fsPath, minioMetaMultipartBucket)
	uploadPath := pathJoin(multipartBucketPath, bucket, object)
	uploadsMetaPath := pathJoin(uploadPath, uploadsJSONFile)

	// Special case for windows please read through.
	if runtime.GOOS == "windows" {
		// Ordinarily windows does not permit deletion or renaming of files still
		// in use, but if all open handles to that file were opened with FILE_SHARE_DELETE
		// then it can permit renames and deletions of open files.
		//
		// There are however some gotchas with this, and it is worth listing them here.
		// Firstly, Windows never allows you to really delete an open file, rather it is
		// flagged as delete pending and its entry in its directory remains visible
		// (though no new file handles may be opened to it) and when the very last
		// open handle to the file in the system is closed, only then is it truly
		// deleted. Well, actually only sort of truly deleted, because Windows only
		// appears to remove the file entry from the directory, but in fact that
		// entry is merely hidden and actually still exists and attempting to create
		// a file with the same name will return an access denied error. How long it
		// silently exists for depends on a range of factors, but put it this way:
		// if your code loops creating and deleting the same file name as you might
		// when operating a lock file, you're going to see lots of random spurious
		// access denied errors and truly dismal lock file performance compared to POSIX.
		//
		// We work-around these un-POSIX file semantics by taking a dual step to
		// deleting files. Firstly, it renames the file to tmp location into multipartTmpBucket
		// We always open files with FILE_SHARE_DELETE permission enabled, with that
		// flag Windows permits renaming and deletion, and because the name was changed
		// to a very random name somewhere not in its origin directory before deletion,
		// you don't see those unexpected random errors when creating files with the
		// same name as a recently deleted file as you do anywhere else on Windows.
		// Because the file is probably not in its original containing directory any more,
		// deletions of that directory will not fail with “directory not empty” as they
		// otherwise normally would either.
		fsRenameFile(uploadsMetaPath, tmpPath)

		// Proceed to deleting the directory.
		if err := fsDeleteFile(multipartBucketPath, uploadPath); err != nil {
			return err
		}

		// Finally delete the renamed file.
		return fsDeleteFile(pathutil.Dir(tmpPath), tmpPath)
	}
	return fsDeleteFile(multipartBucketPath, uploadsMetaPath)
}

// Removes the uploadID, called either by CompleteMultipart of AbortMultipart. If the resuling uploads
// slice is empty then we remove/purge the file.
func (fs fsObjects) removeUploadID(bucket, object, uploadID string, rwlk *lock.LockedFile) error {
	uploadIDs := uploadsV1{}
	_, err := uploadIDs.ReadFrom(io.NewSectionReader(rwlk, 0, rwlk.Size()))
	if err != nil {
		return err
	}

	// Removes upload id from the uploads list.
	uploadIDs.RemoveUploadID(uploadID)

	// Check this is the last entry.
	if uploadIDs.IsEmpty() {
		// No more uploads left, so we delete `uploads.json` file.
		return fs.deleteUploadsJSON(bucket, object, uploadID)
	} // else not empty

	// Write update `uploads.json`.
	_, err = uploadIDs.WriteTo(rwlk)
	return err
}

// Adds a new uploadID if no previous `uploads.json` is
// found we initialize a new one.
func (fs fsObjects) addUploadID(bucket, object, uploadID string, initiated time.Time, rwlk *lock.LockedFile) error {
	uploadIDs := uploadsV1{}

	_, err := uploadIDs.ReadFrom(io.NewSectionReader(rwlk, 0, rwlk.Size()))
	// For all unexpected errors, we return.
	if err != nil && errorCause(err) != io.EOF {
		return err
	}

	// If we couldn't read anything, we assume a default
	// (empty) upload info.
	if errorCause(err) == io.EOF {
		uploadIDs = newUploadsV1("fs")
	}

	// Adds new upload id to the list.
	uploadIDs.AddUploadID(uploadID, initiated)

	// Write update `uploads.json`.
	_, err = uploadIDs.WriteTo(rwlk)
	return err
}
