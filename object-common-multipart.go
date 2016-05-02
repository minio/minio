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
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"path"

	"github.com/skyrings/skyring-common/tools/uuid"
)

/// Common multipart object layer functions.

// newMultipartUploadCommon - initialize a new multipart, is a common
// function for both object layers.
func newMultipartUploadCommon(storage StorageAPI, bucket string, object string) (uploadID string, err error) {
	// Verify if bucket name is valid.
	if !IsValidBucketName(bucket) {
		return "", (BucketNameInvalid{Bucket: bucket})
	}
	// Verify if object name is valid.
	if !IsValidObjectName(object) {
		return "", ObjectNameInvalid{Bucket: bucket, Object: object}
	}

	// Verify whether the bucket exists.
	if isExist, err := isBucketExist(storage, bucket); err != nil {
		return "", err
	} else if !isExist {
		return "", BucketNotFound{Bucket: bucket}
	}

	if _, err := storage.StatVol(minioMetaBucket); err != nil {
		if err == errVolumeNotFound {
			err = storage.MakeVol(minioMetaBucket)
			if err != nil {
				return "", toObjectErr(err)
			}
		}
	}

	// Loops through until successfully generates a new unique upload id.
	for {
		uuid, err := uuid.New()
		if err != nil {
			return "", err
		}
		uploadID := uuid.String()
		uploadIDPath := path.Join(bucket, object, uploadID)
		if _, err = storage.StatFile(minioMetaBucket, uploadIDPath); err != nil {
			if err != errFileNotFound {
				return "", toObjectErr(err, minioMetaBucket, uploadIDPath)
			}
			// uploadIDPath doesn't exist, so create empty file to reserve the name
			var w io.WriteCloser
			if w, err = storage.CreateFile(minioMetaBucket, uploadIDPath); err == nil {
				// Close the writer.
				if err = w.Close(); err != nil {
					return "", err
				}
			} else {
				return "", toObjectErr(err, minioMetaBucket, uploadIDPath)
			}
			return uploadID, nil
		}
		// uploadIDPath already exists.
		// loop again to try with different uuid generated.
	}
}

// putObjectPartCommon - put object part.
func putObjectPartCommon(storage StorageAPI, bucket string, object string, uploadID string, partID int, size int64, data io.Reader, md5Hex string) (string, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return "", BucketNameInvalid{Bucket: bucket}
	}
	if !IsValidObjectName(object) {
		return "", ObjectNameInvalid{Bucket: bucket, Object: object}
	}

	// Verify whether the bucket exists.
	if isExist, err := isBucketExist(storage, bucket); err != nil {
		return "", err
	} else if !isExist {
		return "", BucketNotFound{Bucket: bucket}
	}

	if status, err := isUploadIDExists(storage, bucket, object, uploadID); err != nil {
		return "", err
	} else if !status {
		return "", InvalidUploadID{UploadID: uploadID}
	}

	partSuffix := fmt.Sprintf("%s.%d", uploadID, partID)
	partSuffixPath := path.Join(bucket, object, partSuffix)
	fileWriter, err := storage.CreateFile(minioMetaBucket, partSuffixPath)
	if err != nil {
		return "", toObjectErr(err, bucket, object)
	}

	// Initialize md5 writer.
	md5Writer := md5.New()

	// Instantiate a new multi writer.
	multiWriter := io.MultiWriter(md5Writer, fileWriter)

	// Instantiate checksum hashers and create a multiwriter.
	if size > 0 {
		if _, err = io.CopyN(multiWriter, data, size); err != nil {
			safeCloseAndRemove(fileWriter)
			return "", toObjectErr(err, bucket, object)
		}
		// Reader shouldn't have more data what mentioned in size argument.
		// reading one more byte from the reader to validate it.
		// expected to fail, success validates existence of more data in the reader.
		if _, err = io.CopyN(ioutil.Discard, data, 1); err == nil {
			safeCloseAndRemove(fileWriter)
			return "", UnExpectedDataSize{Size: int(size)}
		}
	} else {
		if _, err = io.Copy(multiWriter, data); err != nil {
			safeCloseAndRemove(fileWriter)
			return "", toObjectErr(err, bucket, object)
		}
	}

	newMD5Hex := hex.EncodeToString(md5Writer.Sum(nil))
	if md5Hex != "" {
		if newMD5Hex != md5Hex {
			safeCloseAndRemove(fileWriter)
			return "", BadDigest{md5Hex, newMD5Hex}
		}
	}
	err = fileWriter.Close()
	if err != nil {
		return "", err
	}
	return newMD5Hex, nil
}

// abortMultipartUploadCommon - aborts a multipart upload, common
// function used by both object layers.
func abortMultipartUploadCommon(storage StorageAPI, bucket, object, uploadID string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}
	if !IsValidObjectName(object) {
		return ObjectNameInvalid{Bucket: bucket, Object: object}
	}
	if status, err := isUploadIDExists(storage, bucket, object, uploadID); err != nil {
		return err
	} else if !status {
		return InvalidUploadID{UploadID: uploadID}
	}

	markerPath := ""
	for {
		uploadIDPath := path.Join(bucket, object, uploadID)
		fileInfos, eof, err := storage.ListFiles(minioMetaBucket, uploadIDPath, markerPath, false, 1000)
		if err != nil {
			if err == errFileNotFound {
				return InvalidUploadID{UploadID: uploadID}
			}
			return toObjectErr(err)
		}
		for _, fileInfo := range fileInfos {
			storage.DeleteFile(minioMetaBucket, fileInfo.Name)
			markerPath = fileInfo.Name
		}
		if eof {
			break
		}
	}
	return nil
}
