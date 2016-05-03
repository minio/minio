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
type MultipartObjectInfo []MultipartPartInfo

// GetSize - Return the size of the object.
func (m MultipartObjectInfo) GetSize() (size int64) {
	for _, part := range m {
		size += part.Size
	}
	return
}

// GetPartNumberOffset - given an offset for the whole object, return the part and offset in that part.
func (m MultipartObjectInfo) GetPartNumberOffset(offset int64) (partIndex int, partOffset int64, err error) {
	partOffset = offset
	for i, part := range m {
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
	return listMultipartUploadsCommon(xl.storage, bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
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
	if !IsValidObjectName(object) {
		return "", ObjectNameInvalid{
			Bucket: bucket,
			Object: object,
		}
	}
	if status, err := isUploadIDExists(xl.storage, bucket, object, uploadID); err != nil {
		return "", err
	} else if !status {
		return "", InvalidUploadID{UploadID: uploadID}
	}
	var metadata MultipartObjectInfo
	var md5Sums []string
	for _, part := range parts {
		// Construct part suffix.
		partSuffix := fmt.Sprintf("%s.%.5d.%s", uploadID, part.PartNumber, part.ETag)
		multipartPartFile := path.Join(mpartMetaPrefix, bucket, object, partSuffix)
		fi, err := xl.storage.StatFile(minioMetaBucket, multipartPartFile)
		if err != nil {
			if err == errFileNotFound {
				return "", InvalidPart{}
			}
			return "", err
		}
		metadata = append(metadata, MultipartPartInfo{part.PartNumber, part.ETag, fi.Size})
		multipartObjSuffix := path.Join(object, partNumToPartFileName(part.PartNumber))
		err = xl.storage.RenameFile(minioMetaBucket, multipartPartFile, bucket, multipartObjSuffix)
		// We need a way to roll back if of the renames failed.
		if err != nil {
			return "", err
		}
		md5Sums = append(md5Sums, part.ETag)
	}

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
		return "", err
	}
	// Save the s3 md5.
	s3MD5, err := makeS3MD5(md5Sums...)
	if err != nil {
		return "", err
	}
	multipartObjFile := path.Join(object, multipartMetaFile)
	err = xl.storage.RenameFile(minioMetaBucket, tempMultipartMetaFile, bucket, multipartObjFile)
	if err != nil {
		return "", toObjectErr(err, bucket, multipartObjFile)
	}

	// Return md5sum.
	return s3MD5, nil
}

// AbortMultipartUpload - aborts a multipart upload.
func (xl xlObjects) AbortMultipartUpload(bucket, object, uploadID string) error {
	return abortMultipartUploadCommon(xl.storage, bucket, object, uploadID)
}
