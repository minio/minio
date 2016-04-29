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
	"path/filepath"
	"sort"
	"strings"

	"github.com/minio/minio/pkg/mimedb"
)

// fsObjects - Implements fs object layer.
type fsObjects struct {
	storage StorageAPI
}

// newFSObjects - initialize new fs object layer.
func newFSObjects(exportPath string) (ObjectLayer, error) {
	var storage StorageAPI
	var err error
	if !strings.ContainsRune(exportPath, ':') || filepath.VolumeName(exportPath) != "" {
		// Initialize filesystem storage API.
		storage, err = newPosix(exportPath)
		if err != nil {
			return nil, err
		}
	} else {
		// Initialize rpc client storage API.
		storage, err = newRPCClient(exportPath)
		if err != nil {
			return nil, err
		}
	}
	return fsObjects{storage}, nil
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

/// Bucket operations

// MakeBucket - make a bucket.
func (fs fsObjects) MakeBucket(bucket string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}
	if err := fs.storage.MakeVol(bucket); err != nil {
		return toObjectErr(err, bucket)
	}
	// This happens for the first time, but keep this here since this
	// is the only place where it can be made expensive optimizing all
	// other calls.
	// Create minio meta volume, if it doesn't exist yet.
	if err := fs.storage.MakeVol(minioMetaVolume); err != nil {
		if err != errVolumeExists {
			return toObjectErr(err, minioMetaVolume)
		}
	}
	return nil
}

// GetBucketInfo - get bucket info.
func (fs fsObjects) GetBucketInfo(bucket string) (BucketInfo, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketInfo{}, BucketNameInvalid{Bucket: bucket}
	}
	vi, err := fs.storage.StatVol(bucket)
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

// ListBuckets - list buckets.
func (fs fsObjects) ListBuckets() ([]BucketInfo, error) {
	var bucketInfos []BucketInfo
	vols, err := fs.storage.ListVols()
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

// DeleteBucket - delete a bucket.
func (fs fsObjects) DeleteBucket(bucket string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}
	if err := fs.storage.DeleteVol(bucket); err != nil {
		return toObjectErr(err)
	}
	return nil
}

/// Object Operations

// GetObject - get an object.
func (fs fsObjects) GetObject(bucket, object string, startOffset int64) (io.ReadCloser, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return nil, (BucketNameInvalid{Bucket: bucket})
	}
	// Verify if object is valid.
	if !IsValidObjectName(object) {
		return nil, (ObjectNameInvalid{Bucket: bucket, Object: object})
	}
	fileReader, err := fs.storage.ReadFile(bucket, object, startOffset)
	if err != nil {
		return nil, toObjectErr(err, bucket, object)
	}
	return fileReader, nil
}

// GetObjectInfo - get object info.
func (fs fsObjects) GetObjectInfo(bucket, object string) (ObjectInfo, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return ObjectInfo{}, (BucketNameInvalid{Bucket: bucket})
	}
	// Verify if object is valid.
	if !IsValidObjectName(object) {
		return ObjectInfo{}, (ObjectNameInvalid{Bucket: bucket, Object: object})
	}
	fi, err := fs.storage.StatFile(bucket, object)
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}
	contentType := "application/octet-stream"
	if objectExt := filepath.Ext(object); objectExt != "" {
		content, ok := mimedb.DB[strings.ToLower(strings.TrimPrefix(objectExt, "."))]
		if ok {
			contentType = content.ContentType
		}
	}
	return ObjectInfo{
		Bucket:      bucket,
		Name:        object,
		ModTime:     fi.ModTime,
		Size:        fi.Size,
		IsDir:       fi.Mode.IsDir(),
		ContentType: contentType,
		MD5Sum:      "", // Read from metadata.
	}, nil
}

func (fs fsObjects) PutObject(bucket string, object string, size int64, data io.Reader, metadata map[string]string) (string, error) {
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
	if isExist, err := isBucketExist(fs.storage, bucket); err != nil {
		return "", err
	} else if !isExist {
		return "", BucketNotFound{Bucket: bucket}
	}

	fileWriter, err := fs.storage.CreateFile(bucket, object)
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

func (fs fsObjects) DeleteObject(bucket, object string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}
	if !IsValidObjectName(object) {
		return ObjectNameInvalid{Bucket: bucket, Object: object}
	}
	if err := fs.storage.DeleteFile(bucket, object); err != nil {
		return toObjectErr(err, bucket, object)
	}
	return nil
}

func (fs fsObjects) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return ListObjectsInfo{}, BucketNameInvalid{Bucket: bucket}
	}
	if !IsValidObjectPrefix(prefix) {
		return ListObjectsInfo{}, ObjectNameInvalid{Bucket: bucket, Object: prefix}
	}
	// Verify if delimiter is anything other than '/', which we do not support.
	if delimiter != "" && delimiter != slashSeparator {
		return ListObjectsInfo{}, UnsupportedDelimiter{
			Delimiter: delimiter,
		}
	}
	// Verify if marker has prefix.
	if marker != "" {
		if !strings.HasPrefix(marker, prefix) {
			return ListObjectsInfo{}, InvalidMarkerPrefixCombination{
				Marker: marker,
				Prefix: prefix,
			}
		}
	}

	// Default is recursive, if delimiter is set then list non recursive.
	recursive := true
	if delimiter == slashSeparator {
		recursive = false
	}
	fileInfos, eof, err := fs.storage.ListFiles(bucket, prefix, marker, recursive, maxKeys)
	if err != nil {
		return ListObjectsInfo{}, toObjectErr(err, bucket)
	}
	if maxKeys == 0 {
		return ListObjectsInfo{}, nil
	}

	result := ListObjectsInfo{IsTruncated: !eof}
	for _, fileInfo := range fileInfos {
		// With delimiter set we fill in NextMarker and Prefixes.
		if delimiter == slashSeparator {
			result.NextMarker = fileInfo.Name
			if fileInfo.Mode.IsDir() {
				result.Prefixes = append(result.Prefixes, fileInfo.Name)
				continue
			}
		}
		result.Objects = append(result.Objects, ObjectInfo{
			Name:    fileInfo.Name,
			ModTime: fileInfo.ModTime,
			Size:    fileInfo.Size,
			IsDir:   false,
		})
	}
	return result, nil
}
