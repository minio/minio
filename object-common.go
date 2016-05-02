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
	"io"
	"path"
	"sort"
)

/// Common object layer functions.

// makeBucket - create a bucket, is a common function for both object layers.
func makeBucket(storage StorageAPI, bucket string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}
	if err := storage.MakeVol(bucket); err != nil {
		return toObjectErr(err, bucket)
	}
	// This happens for the first time, but keep this here since this
	// is the only place where it can be made expensive optimizing all
	// other calls.
	// Create minio meta volume, if it doesn't exist yet.
	if err := storage.MakeVol(minioMetaBucket); err != nil {
		if err != errVolumeExists {
			return toObjectErr(err, minioMetaBucket)
		}
	}
	return nil
}

// getBucketInfo - fetch bucket info, is a common function for both object layers.
func getBucketInfo(storage StorageAPI, bucket string) (BucketInfo, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketInfo{}, BucketNameInvalid{Bucket: bucket}
	}
	vi, err := storage.StatVol(bucket)
	if err != nil {
		return BucketInfo{}, toObjectErr(err, bucket)
	}
	return BucketInfo{
		Name:    bucket,
		Created: vi.Created,
		Total:   vi.Total,
		Free:    vi.Free,
	}, nil
}

// listBuckets - list all buckets, is a common function for both object layers.
func listBuckets(storage StorageAPI) ([]BucketInfo, error) {
	var bucketInfos []BucketInfo
	vols, err := storage.ListVols()
	if err != nil {
		return nil, toObjectErr(err)
	}
	for _, vol := range vols {
		// StorageAPI can send volume names which are incompatible
		// with buckets, handle it and skip them.
		if !IsValidBucketName(vol.Name) {
			continue
		}
		bucketInfos = append(bucketInfos, BucketInfo{
			Name:    vol.Name,
			Created: vol.Created,
			Total:   vol.Total,
			Free:    vol.Free,
		})
	}
	sort.Sort(byBucketName(bucketInfos))
	return bucketInfos, nil
}

// deleteBucket - deletes a bucket, is a common function for both the layers.
func deleteBucket(storage StorageAPI, bucket string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}
	if err := storage.DeleteVol(bucket); err != nil {
		return toObjectErr(err)
	}
	return nil
}

// putObjectCommon - create an object, is a common function for both object layers.
func putObjectCommon(storage StorageAPI, bucket string, object string, size int64, data io.Reader, metadata map[string]string) (string, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return "", (BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(object) {
		return "", (ObjectNameInvalid{
			Bucket: bucket,
			Object: object,
		})
	}
	// Check whether the bucket exists.
	if isExist, err := isBucketExist(storage, bucket); err != nil {
		return "", err
	} else if !isExist {
		return "", BucketNotFound{Bucket: bucket}
	}

	fileWriter, err := storage.CreateFile(bucket, object)
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
			if clErr := safeCloseAndRemove(fileWriter); clErr != nil {
				return "", clErr
			}
			return "", toObjectErr(err)
		}
	} else {
		if _, err = io.Copy(multiWriter, data); err != nil {
			if clErr := safeCloseAndRemove(fileWriter); clErr != nil {
				return "", clErr
			}
			return "", err
		}
	}

	newMD5Hex := hex.EncodeToString(md5Writer.Sum(nil))
	// md5Hex representation.
	var md5Hex string
	if len(metadata) != 0 {
		md5Hex = metadata["md5Sum"]
	}
	if md5Hex != "" {
		if newMD5Hex != md5Hex {
			if err = safeCloseAndRemove(fileWriter); err != nil {
				return "", err
			}
			return "", BadDigest{md5Hex, newMD5Hex}
		}
	}
	err = fileWriter.Close()
	if err != nil {
		return "", err
	}

	// Return md5sum, successfully wrote object.
	return newMD5Hex, nil
}

// isUploadIDExists - verify if a given uploadID exists and is valid.
func isUploadIDExists(storage StorageAPI, bucket, object, uploadID string) (bool, error) {
	uploadIDPath := path.Join(bucket, object, uploadID)
	st, err := storage.StatFile(minioMetaBucket, uploadIDPath)
	if err != nil {
		// Upload id does not exist.
		if err == errFileNotFound {
			return false, nil
		}
		return false, err
	}
	// Upload id exists and is a regular file.
	return st.Mode.IsRegular(), nil
}

// checks whether bucket exists.
func isBucketExist(storage StorageAPI, bucketName string) (bool, error) {
	// Check whether bucket exists.
	if _, e := storage.StatVol(bucketName); e != nil {
		if e == errVolumeNotFound {
			return false, nil
		}
		return false, e
	}
	return true, nil
}
