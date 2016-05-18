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
	"strings"
	"sync"

	"github.com/minio/minio/pkg/mimedb"
)

// fsObjects - Implements fs object layer.
type fsObjects struct {
	storage            StorageAPI
	listObjectMap      map[listParams][]*treeWalker
	listObjectMapMutex *sync.Mutex
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

	// Initialize object layer - like creating minioMetaBucket,
	// cleaning up tmp files etc.
	initObjectLayer(storage)

	// Return successfully initialized object layer.
	return fsObjects{
		storage:            storage,
		listObjectMap:      make(map[listParams][]*treeWalker),
		listObjectMapMutex: &sync.Mutex{},
	}, nil
}

/// Bucket operations

// MakeBucket - make a bucket.
func (fs fsObjects) MakeBucket(bucket string) error {
	return makeBucket(fs.storage, bucket)
}

// GetBucketInfo - get bucket info.
func (fs fsObjects) GetBucketInfo(bucket string) (BucketInfo, error) {
	return getBucketInfo(fs.storage, bucket)
}

// ListBuckets - list buckets.
func (fs fsObjects) ListBuckets() ([]BucketInfo, error) {
	return listBuckets(fs.storage)
}

// DeleteBucket - delete a bucket.
func (fs fsObjects) DeleteBucket(bucket string) error {
	return deleteBucket(fs.storage, bucket)
}

/// Object Operations

// GetObject - get an object.
func (fs fsObjects) GetObject(bucket, object string, startOffset int64) (io.ReadCloser, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return nil, BucketNameInvalid{Bucket: bucket}
	}
	// Verify if object is valid.
	if !IsValidObjectName(object) {
		return nil, ObjectNameInvalid{Bucket: bucket, Object: object}
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

// PutObject - create an object.
func (fs fsObjects) PutObject(bucket string, object string, size int64, data io.Reader, metadata map[string]string) (string, error) {
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
			return "", toObjectErr(err, bucket, object)
		}
	} else {
		if _, err = io.Copy(multiWriter, data); err != nil {
			if clErr := safeCloseAndRemove(fileWriter); clErr != nil {
				return "", clErr
			}
			return "", toObjectErr(err, bucket, object)
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
		if clErr := safeCloseAndRemove(fileWriter); clErr != nil {
			return "", clErr
		}
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

// ListObjects - list all objects.
func (fs fsObjects) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	return listObjectsCommon(fs, bucket, prefix, marker, delimiter, maxKeys)
}
