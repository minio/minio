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
	"fmt"
	"io"
	"path"
	"strings"
	"sync"
	"time"
)

// MultipartPartInfo Info of each part kept in the multipart metadata file after
// CompleteMultipartUpload() is called.
type MultipartPartInfo struct {
	PartNumber int
	ETag       string
	Size       int64
}

// MultipartObjectInfo - contents of the multipart metadata file after
// CompleteMultipartUpload() is called.
type MultipartObjectInfo struct {
	Parts   []MultipartPartInfo
	ModTime time.Time
	Size    int64
	MD5Sum  string
}

type byMultipartFiles []string

func (files byMultipartFiles) Len() int { return len(files) }
func (files byMultipartFiles) Less(i, j int) bool {
	first := strings.TrimSuffix(files[i], multipartSuffix)
	second := strings.TrimSuffix(files[j], multipartSuffix)
	return first < second
}
func (files byMultipartFiles) Swap(i, j int) { files[i], files[j] = files[j], files[i] }

// GetPartNumberOffset - given an offset for the whole object, return the part and offset in that part.
func (m MultipartObjectInfo) GetPartNumberOffset(offset int64) (partIndex int, partOffset int64, err error) {
	partOffset = offset
	for i, part := range m.Parts {
		partIndex = i
		if partOffset < part.Size {
			return
		}
		partOffset -= part.Size
	}
	// Offset beyond the size of the object
	err = errUnexpected
	return
}

func partNumToPartFileName(partNum int) string {
	return fmt.Sprintf("%.5d%s", partNum, multipartSuffix)
}

// ListMultipartUploads - list multipart uploads.
func (xl xlObjects) ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (ListMultipartsInfo, error) {
	return listMultipartUploadsCommon(xl, bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
}

// NewMultipartUpload - initialize a new multipart upload, returns a unique id.
func (xl xlObjects) NewMultipartUpload(bucket, object string) (string, error) {
	return newMultipartUploadCommon(xl.storage, bucket, object)
}

// PutObjectPart - writes the multipart upload chunks.
func (xl xlObjects) PutObjectPart(bucket, object, uploadID string, partID int, size int64, data io.Reader, md5Hex string) (string, error) {
	return putObjectPartCommon(xl.storage, bucket, object, uploadID, partID, size, data, md5Hex)
}

// ListObjectParts - list object parts.
func (xl xlObjects) ListObjectParts(bucket, object, uploadID string, partNumberMarker, maxParts int) (ListPartsInfo, error) {
	return listObjectPartsCommon(xl.storage, bucket, object, uploadID, partNumberMarker, maxParts)
}

func (xl xlObjects) CompleteMultipartUpload(bucket string, object string, uploadID string, parts []completePart) (string, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return "", BucketNameInvalid{Bucket: bucket}
	}
	// Verify whether the bucket exists.
	if !isBucketExist(xl.storage, bucket) {
		return "", BucketNotFound{Bucket: bucket}
	}
	if !IsValidObjectName(object) {
		return "", ObjectNameInvalid{
			Bucket: bucket,
			Object: object,
		}
	}
	if !isUploadIDExists(xl.storage, bucket, object, uploadID) {
		return "", InvalidUploadID{UploadID: uploadID}
	}

	// Calculate s3 compatible md5sum for complete multipart.
	s3MD5, err := completeMultipartMD5(parts...)
	if err != nil {
		return "", err
	}

	var metadata = MultipartObjectInfo{}
	var errs = make([]error, len(parts))

	// Waitgroup to wait for go-routines.
	var wg = &sync.WaitGroup{}

	// Loop through all parts, validate them and then commit to disk.
	for _, part := range parts {
		// Construct part suffix.
		partSuffix := fmt.Sprintf("%.5d.%s", part.PartNumber, part.ETag)
		multipartPartFile := path.Join(mpartMetaPrefix, bucket, object, uploadID, partSuffix)
		var fi FileInfo
		fi, err = xl.storage.StatFile(minioMetaBucket, multipartPartFile)
		if err != nil {
			if err == errFileNotFound {
				return "", InvalidPart{}
			}
			return "", err
		}
		if !isMinAllowedPartSize(fi.Size) {
			return "", PartTooSmall{}
		}
		// Update metadata parts.
		metadata.Parts = append(metadata.Parts, MultipartPartInfo{
			PartNumber: part.PartNumber,
			ETag:       part.ETag,
			Size:       fi.Size,
		})
		metadata.Size += fi.Size
	}

	// Loop through and atomically rename the parts to their actual location.
	for index, part := range parts {
		wg.Add(1)
		go func(index int, part completePart) {
			defer wg.Done()
			partSuffix := fmt.Sprintf("%.5d.%s", part.PartNumber, part.ETag)
			multipartPartFile := path.Join(mpartMetaPrefix, bucket, object, uploadID, partSuffix)
			tmpMultipartObjSuffix := path.Join(tmpMetaPrefix, bucket, object, partNumToPartFileName(part.PartNumber))
			rErr := xl.storage.RenameFile(minioMetaBucket, multipartPartFile, minioMetaBucket, tmpMultipartObjSuffix)
			if rErr != nil {
				errs[index] = rErr
				log.Errorf("Unable to rename file %s to %s, failed with %s", multipartPartFile, tmpMultipartObjSuffix, rErr)
				return
			}
			multipartObjSuffix := path.Join(object, partNumToPartFileName(part.PartNumber))
			rErr = xl.storage.RenameFile(minioMetaBucket, tmpMultipartObjSuffix, bucket, multipartObjSuffix)
			if rErr != nil {
				if dErr := xl.storage.DeleteFile(minioMetaBucket, tmpMultipartObjSuffix); dErr != nil {
					errs[index] = dErr
					log.Errorf("Unable to delete file %s, failed with %s", tmpMultipartObjSuffix, dErr)
					return
				}
				log.Debugf("Delete file succeeded on %s", tmpMultipartObjSuffix)
				errs[index] = rErr
				log.Errorf("Unable to rename file %s to %s, failed with %s", tmpMultipartObjSuffix, multipartObjSuffix, rErr)
			}
		}(index, part)
	}

	// Wait for all the renames to finish.
	wg.Wait()

	// Loop through errs list and return first error.
	for _, err := range errs {
		if err != nil {
			return "", toObjectErr(err, bucket, object)
		}
	}

	// Save successfully calculated md5sum.
	metadata.MD5Sum = s3MD5
	// Save modTime as well as the current time.
	metadata.ModTime = time.Now().UTC()

	// Create temporary multipart meta file to write and then rename.
	tempMultipartMetaFile := path.Join(tmpMetaPrefix, bucket, object, multipartMetaFile)
	w, err := xl.storage.CreateFile(minioMetaBucket, tempMultipartMetaFile)
	if err != nil {
		return "", toObjectErr(err, bucket, object)
	}
	encoder := json.NewEncoder(w)
	err = encoder.Encode(&metadata)
	if err != nil {
		if err = safeCloseAndRemove(w); err != nil {
			return "", err
		}
		return "", err
	}
	// Close the writer.
	if err = w.Close(); err != nil {
		if err = safeCloseAndRemove(w); err != nil {
			return "", err
		}
		return "", err
	}
	multipartObjFile := path.Join(object, multipartMetaFile)
	err = xl.storage.RenameFile(minioMetaBucket, tempMultipartMetaFile, bucket, multipartObjFile)
	if err != nil {
		if derr := xl.storage.DeleteFile(minioMetaBucket, tempMultipartMetaFile); derr != nil {
			return "", toObjectErr(err, minioMetaBucket, tempMultipartMetaFile)
		}
		return "", toObjectErr(err, bucket, multipartObjFile)
	}

	// Attempt a rename of the upload id to temporary location, if
	// successful then delete it.
	uploadIDPath := path.Join(mpartMetaPrefix, bucket, object, uploadID, incompleteFile)
	tempUploadIDPath := path.Join(tmpMetaPrefix, bucket, object, uploadID, incompleteFile)
	if err = xl.storage.RenameFile(minioMetaBucket, uploadIDPath, minioMetaBucket, tempUploadIDPath); err == nil {
		if err = xl.storage.DeleteFile(minioMetaBucket, tempUploadIDPath); err != nil {
			return "", toObjectErr(err, minioMetaBucket, tempUploadIDPath)
		}
		return s3MD5, nil
	}
	// Rename if failed attempt to delete the original file.
	err = xl.storage.DeleteFile(minioMetaBucket, uploadIDPath)
	if err != nil {
		return "", toObjectErr(err, minioMetaBucket, uploadIDPath)
	}

	uploadsJSONPath := path.Join(mpartMetaPrefix, bucket, object, uploadsJSONFile)
	err = xl.storage.DeleteFile(minioMetaBucket, uploadsJSONPath)
	if err != nil {
		return "", toObjectErr(err, minioMetaBucket, uploadsJSONPath)
	}

	// Return md5sum.
	return s3MD5, nil
}

// AbortMultipartUpload - aborts a multipart upload.
func (xl xlObjects) AbortMultipartUpload(bucket, object, uploadID string) error {
	return abortMultipartUploadCommon(xl.storage, bucket, object, uploadID)
}
