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
	"fmt"
	"io"
	"path"
)

// ListMultipartUploads - list multipart uploads.
func (fs fsObjects) ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (ListMultipartsInfo, error) {
	return listMultipartUploadsCommon(fs, bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
}

// NewMultipartUpload - initialize a new multipart upload, returns a unique id.
func (fs fsObjects) NewMultipartUpload(bucket, object string, meta map[string]string) (string, error) {
	meta = make(map[string]string) // Reset the meta value, we are not going to save headers for fs.
	return newMultipartUploadCommon(fs.storage, bucket, object, meta)
}

// PutObjectPart - writes the multipart upload chunks.
func (fs fsObjects) PutObjectPart(bucket, object, uploadID string, partID int, size int64, data io.Reader, md5Hex string) (string, error) {
	return putObjectPartCommon(fs.storage, bucket, object, uploadID, partID, size, data, md5Hex)
}

func (fs fsObjects) ListObjectParts(bucket, object, uploadID string, partNumberMarker, maxParts int) (ListPartsInfo, error) {
	return listObjectPartsCommon(fs.storage, bucket, object, uploadID, partNumberMarker, maxParts)
}

func (fs fsObjects) CompleteMultipartUpload(bucket string, object string, uploadID string, parts []completePart) (string, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return "", BucketNameInvalid{Bucket: bucket}
	}
	// Verify whether the bucket exists.
	if !isBucketExist(fs.storage, bucket) {
		return "", BucketNotFound{Bucket: bucket}
	}
	if !IsValidObjectName(object) {
		return "", ObjectNameInvalid{
			Bucket: bucket,
			Object: object,
		}
	}
	if !isUploadIDExists(fs.storage, bucket, object, uploadID) {
		return "", InvalidUploadID{UploadID: uploadID}
	}

	// Calculate s3 compatible md5sum for complete multipart.
	s3MD5, err := completeMultipartMD5(parts...)
	if err != nil {
		return "", err
	}

	tempObj := path.Join(tmpMetaPrefix, bucket, object, uploadID, incompleteFile)
	fileWriter, err := fs.storage.CreateFile(minioMetaBucket, tempObj)
	if err != nil {
		return "", toObjectErr(err, bucket, object)
	}

	// Loop through all parts, validate them and then commit to disk.
	for i, part := range parts {
		// Construct part suffix.
		partSuffix := fmt.Sprintf("%.5d.%s", part.PartNumber, part.ETag)
		multipartPartFile := path.Join(mpartMetaPrefix, bucket, object, uploadID, partSuffix)
		var fi FileInfo
		fi, err = fs.storage.StatFile(minioMetaBucket, multipartPartFile)
		if err != nil {
			if err == errFileNotFound {
				return "", InvalidPart{}
			}
			return "", err
		}
		// All parts except the last part has to be atleast 5MB.
		if (i < len(parts)-1) && !isMinAllowedPartSize(fi.Size) {
			return "", PartTooSmall{}
		}
		var fileReader io.ReadCloser
		fileReader, err = fs.storage.ReadFile(minioMetaBucket, multipartPartFile, 0)
		if err != nil {
			if clErr := safeCloseAndRemove(fileWriter); clErr != nil {
				return "", clErr
			}
			if err == errFileNotFound {
				return "", InvalidPart{}
			}
			return "", err
		}
		_, err = io.Copy(fileWriter, fileReader)
		if err != nil {
			if clErr := safeCloseAndRemove(fileWriter); clErr != nil {
				return "", clErr
			}
			return "", err
		}
		err = fileReader.Close()
		if err != nil {
			if clErr := safeCloseAndRemove(fileWriter); clErr != nil {
				return "", clErr
			}
			return "", err
		}
	}

	err = fileWriter.Close()
	if err != nil {
		if clErr := safeCloseAndRemove(fileWriter); clErr != nil {
			return "", clErr
		}
		return "", err
	}

	// Rename the file back to original location, if not delete the
	// temporary object.
	err = fs.storage.RenameFile(minioMetaBucket, tempObj, bucket, object)
	if err != nil {
		if derr := fs.storage.DeleteFile(minioMetaBucket, tempObj); derr != nil {
			return "", toObjectErr(derr, minioMetaBucket, tempObj)
		}
		return "", toObjectErr(err, bucket, object)
	}

	// Cleanup all the parts if everything else has been safely committed.
	if err = cleanupUploadedParts(fs.storage, bucket, object, uploadID); err != nil {
		return "", err
	}

	// Return md5sum.
	return s3MD5, nil
}

// AbortMultipartUpload - aborts a multipart upload.
func (fs fsObjects) AbortMultipartUpload(bucket, object, uploadID string) error {
	return abortMultipartUploadCommon(fs.storage, bucket, object, uploadID)
}
