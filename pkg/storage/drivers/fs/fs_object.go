/*
 * Mini Object Storage, (C) 2015 Minio, Inc.
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

package filesystem

import (
	"bytes"
	"io"
	"os"
	"path"
	"strings"

	"crypto/md5"
	"encoding/base64"
	"encoding/gob"
	"encoding/hex"
	"errors"

	"github.com/minio/minio/pkg/iodine"
	"github.com/minio/minio/pkg/storage/drivers"
)

/// Object Operations

// GetPartialObject - GET object from range
func (fs *fsDriver) GetPartialObject(w io.Writer, bucket, object string, start, length int64) (int64, error) {
	// validate bucket
	if drivers.IsValidBucket(bucket) == false {
		return 0, iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, nil)
	}

	// validate object
	if drivers.IsValidObjectName(object) == false {
		return 0, iodine.New(drivers.ObjectNameInvalid{Bucket: bucket, Object: object}, nil)
	}

	objectPath := path.Join(fs.root, bucket, object)
	filestat, err := os.Stat(objectPath)
	switch err := err.(type) {
	case nil:
		{
			if filestat.IsDir() {
				return 0, iodine.New(drivers.ObjectNotFound{Bucket: bucket, Object: object}, nil)
			}
		}
	default:
		{
			if os.IsNotExist(err) {
				return 0, iodine.New(drivers.ObjectNotFound{Bucket: bucket, Object: object}, nil)
			}
			return 0, iodine.New(err, nil)
		}
	}
	file, err := os.Open(objectPath)
	if err != nil {
		return 0, iodine.New(err, nil)
	}
	defer file.Close()

	_, err = file.Seek(start, os.SEEK_SET)
	if err != nil {
		return 0, iodine.New(err, nil)
	}

	count, err := io.CopyN(w, file, length)
	if err != nil {
		return count, iodine.New(err, nil)
	}

	return count, nil
}

// GetObject - GET object from key
func (fs *fsDriver) GetObject(w io.Writer, bucket string, object string) (int64, error) {
	// validate bucket
	if drivers.IsValidBucket(bucket) == false {
		return 0, iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, nil)
	}

	// validate object
	if drivers.IsValidObjectName(object) == false {
		return 0, iodine.New(drivers.ObjectNameInvalid{Bucket: bucket, Object: object}, nil)
	}

	objectPath := path.Join(fs.root, bucket, object)
	filestat, err := os.Stat(objectPath)
	switch err := err.(type) {
	case nil:
		{
			if filestat.IsDir() {
				return 0, iodine.New(drivers.ObjectNotFound{Bucket: bucket, Object: object}, nil)
			}
		}
	default:
		{
			if os.IsNotExist(err) {
				return 0, iodine.New(drivers.ObjectNotFound{Bucket: bucket, Object: object}, nil)
			}
			return 0, iodine.New(err, nil)
		}
	}
	file, err := os.Open(objectPath)
	defer file.Close()
	if err != nil {
		return 0, drivers.EmbedError(bucket, object, err)
	}

	count, err := io.Copy(w, file)
	if err != nil {
		return count, iodine.New(err, nil)
	}
	return count, nil
}

// GetObjectMetadata - HEAD object
func (fs *fsDriver) GetObjectMetadata(bucket, object string) (drivers.ObjectMetadata, error) {
	if drivers.IsValidBucket(bucket) == false {
		return drivers.ObjectMetadata{}, iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, nil)
	}

	if drivers.IsValidObjectName(object) == false {
		return drivers.ObjectMetadata{}, iodine.New(drivers.ObjectNameInvalid{Bucket: bucket, Object: bucket}, nil)
	}

	// Do not use path.Join() since path.Join strips off any object names with '/', use them as is
	// in a static manner so that we can send a proper 'ObjectNotFound' reply back upon os.Stat()
	objectPath := fs.root + "/" + bucket + "/" + object
	stat, err := os.Stat(objectPath)
	if os.IsNotExist(err) {
		return drivers.ObjectMetadata{}, iodine.New(drivers.ObjectNotFound{Bucket: bucket, Object: object}, nil)
	}

	_, err = os.Stat(objectPath + "$metadata")
	if os.IsNotExist(err) {
		return drivers.ObjectMetadata{}, iodine.New(drivers.ObjectNotFound{Bucket: bucket, Object: object}, nil)
	}

	file, err := os.Open(objectPath + "$metadata")
	defer file.Close()
	if err != nil {
		return drivers.ObjectMetadata{}, iodine.New(err, nil)
	}

	var deserializedMetadata Metadata
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&deserializedMetadata)
	if err != nil {
		return drivers.ObjectMetadata{}, iodine.New(err, nil)
	}

	contentType := "application/octet-stream"
	if deserializedMetadata.ContentType != "" {
		contentType = deserializedMetadata.ContentType
	}
	contentType = strings.TrimSpace(contentType)

	etag := bucket + "#" + path.Base(object)
	if len(deserializedMetadata.Md5sum) != 0 {
		etag = hex.EncodeToString(deserializedMetadata.Md5sum)
	}

	metadata := drivers.ObjectMetadata{
		Bucket:      bucket,
		Key:         object,
		Created:     stat.ModTime(),
		Size:        stat.Size(),
		Md5:         etag,
		ContentType: contentType,
	}

	return metadata, nil
}

// isMD5SumEqual - returns error if md5sum mismatches, success its `nil`
func isMD5SumEqual(expectedMD5Sum, actualMD5Sum string) error {
	if strings.TrimSpace(expectedMD5Sum) != "" && strings.TrimSpace(actualMD5Sum) != "" {
		expectedMD5SumBytes, err := hex.DecodeString(expectedMD5Sum)
		if err != nil {
			return iodine.New(err, nil)
		}
		actualMD5SumBytes, err := hex.DecodeString(actualMD5Sum)
		if err != nil {
			return iodine.New(err, nil)
		}
		if !bytes.Equal(expectedMD5SumBytes, actualMD5SumBytes) {
			return iodine.New(errors.New("bad digest, md5sum mismatch"), nil)
		}
		return nil
	}
	return iodine.New(errors.New("invalid argument"), nil)
}

// CreateObject - PUT object
func (fs *fsDriver) CreateObject(bucket, key, contentType, expectedMD5Sum string, size int64, data io.Reader) (string, error) {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	// check bucket name valid
	if drivers.IsValidBucket(bucket) == false {
		return "", iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, nil)
	}

	// check bucket exists
	if _, err := os.Stat(path.Join(fs.root, bucket)); os.IsNotExist(err) {
		return "", iodine.New(drivers.BucketNotFound{Bucket: bucket}, nil)
	}

	// verify object path legal
	if drivers.IsValidObjectName(key) == false {
		return "", iodine.New(drivers.ObjectNameInvalid{Bucket: bucket, Object: key}, nil)
	}

	// verify content type
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	contentType = strings.TrimSpace(contentType)

	// get object path
	objectPath := path.Join(fs.root, bucket, key)
	objectDir := path.Dir(objectPath)
	if _, err := os.Stat(objectDir); os.IsNotExist(err) {
		err = os.MkdirAll(objectDir, 0700)
		if err != nil {
			return "", iodine.New(err, nil)
		}
	}

	// check if object exists
	if _, err := os.Stat(objectPath); !os.IsNotExist(err) {
		return "", iodine.New(drivers.ObjectExists{
			Bucket: bucket,
			Object: key,
		}, nil)
	}

	if strings.TrimSpace(expectedMD5Sum) != "" {
		expectedMD5SumBytes, err := base64.StdEncoding.DecodeString(strings.TrimSpace(expectedMD5Sum))
		if err != nil {
			// pro-actively close the connection
			return "", iodine.New(drivers.InvalidDigest{Md5: expectedMD5Sum}, nil)
		}
		expectedMD5Sum = hex.EncodeToString(expectedMD5SumBytes)
	}

	// write object
	file, err := os.OpenFile(objectPath, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return "", iodine.New(err, nil)
	}
	defer file.Close()

	h := md5.New()
	mw := io.MultiWriter(file, h)

	_, err = io.CopyN(mw, data, size)
	if err != nil {
		return "", iodine.New(err, nil)
	}

	file, err = os.OpenFile(objectPath+"$metadata", os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return "", iodine.New(err, nil)
	}
	defer file.Close()

	metadata := &Metadata{
		ContentType: contentType,
		Md5sum:      h.Sum(nil),
	}
	// serialize metadata to gob
	encoder := gob.NewEncoder(file)
	err = encoder.Encode(metadata)

	md5Sum := hex.EncodeToString(metadata.Md5sum)
	// Verify if the written object is equal to what is expected, only if it is requested as such
	if strings.TrimSpace(expectedMD5Sum) != "" {
		if err := isMD5SumEqual(strings.TrimSpace(expectedMD5Sum), md5Sum); err != nil {
			return "", iodine.New(drivers.BadDigest{Md5: expectedMD5Sum, Bucket: bucket, Key: key}, nil)
		}
	}
	return md5Sum, nil
}
