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

package file

import (
	"io"
	"os"
	"path"
	"strings"

	"crypto/md5"
	"encoding/gob"
	"encoding/hex"

	mstorage "github.com/minio-io/minio/pkg/storage"
)

/// Object Operations

// GetPartialObject - GET object from range
func (storage *Storage) GetPartialObject(w io.Writer, bucket, object string, start, length int64) (int64, error) {
	// validate bucket
	if mstorage.IsValidBucket(bucket) == false {
		return 0, mstorage.BucketNameInvalid{Bucket: bucket}
	}

	// validate object
	if mstorage.IsValidObject(object) == false {
		return 0, mstorage.ObjectNameInvalid{Bucket: bucket, Object: object}
	}

	objectPath := path.Join(storage.root, bucket, object)
	filestat, err := os.Stat(objectPath)
	switch err := err.(type) {
	case nil:
		{
			if filestat.IsDir() {
				return 0, mstorage.ObjectNotFound{Bucket: bucket, Object: object}
			}
		}
	default:
		{
			if os.IsNotExist(err) {
				return 0, mstorage.ObjectNotFound{Bucket: bucket, Object: object}
			}
			return 0, mstorage.EmbedError(bucket, object, err)
		}
	}
	file, err := os.Open(objectPath)
	defer file.Close()
	if err != nil {
		return 0, mstorage.EmbedError(bucket, object, err)
	}

	_, err = file.Seek(start, os.SEEK_SET)
	if err != nil {
		return 0, mstorage.EmbedError(bucket, object, err)
	}

	count, err := io.CopyN(w, file, length)
	if err != nil {
		return count, mstorage.EmbedError(bucket, object, err)
	}

	return count, nil
}

// GetObject - GET object from key
func (storage *Storage) GetObject(w io.Writer, bucket string, object string) (int64, error) {
	// validate bucket
	if mstorage.IsValidBucket(bucket) == false {
		return 0, mstorage.BucketNameInvalid{Bucket: bucket}
	}

	// validate object
	if mstorage.IsValidObject(object) == false {
		return 0, mstorage.ObjectNameInvalid{Bucket: bucket, Object: object}
	}

	objectPath := path.Join(storage.root, bucket, object)

	filestat, err := os.Stat(objectPath)
	switch err := err.(type) {
	case nil:
		{
			if filestat.IsDir() {
				return 0, mstorage.ObjectNotFound{Bucket: bucket, Object: object}
			}
		}
	default:
		{
			if os.IsNotExist(err) {
				return 0, mstorage.ObjectNotFound{Bucket: bucket, Object: object}
			}
			return 0, mstorage.EmbedError(bucket, object, err)
		}
	}
	file, err := os.Open(objectPath)
	defer file.Close()
	if err != nil {
		return 0, mstorage.EmbedError(bucket, object, err)
	}

	count, err := io.Copy(w, file)
	if err != nil {
		return count, mstorage.EmbedError(bucket, object, err)
	}
	return count, nil
}

// GetObjectMetadata - HEAD object
func (storage *Storage) GetObjectMetadata(bucket, object, prefix string) (mstorage.ObjectMetadata, error) {
	if mstorage.IsValidBucket(bucket) == false {
		return mstorage.ObjectMetadata{}, mstorage.BucketNameInvalid{Bucket: bucket}
	}

	if mstorage.IsValidObject(object) == false {
		return mstorage.ObjectMetadata{}, mstorage.ObjectNameInvalid{Bucket: bucket, Object: bucket}
	}

	// Do not use path.Join() since path.Join strips off any object names with '/', use them as is
	// in a static manner so that we can send a proper 'ObjectNotFound' reply back upon os.Stat()
	objectPath := storage.root + "/" + bucket + "/" + object
	stat, err := os.Stat(objectPath)
	if os.IsNotExist(err) {
		return mstorage.ObjectMetadata{}, mstorage.ObjectNotFound{Bucket: bucket, Object: object}
	}

	_, err = os.Stat(objectPath + "$metadata")
	if os.IsNotExist(err) {
		return mstorage.ObjectMetadata{}, mstorage.ObjectNotFound{Bucket: bucket, Object: object}
	}

	file, err := os.Open(objectPath + "$metadata")
	defer file.Close()
	if err != nil {
		return mstorage.ObjectMetadata{}, mstorage.EmbedError(bucket, object, err)
	}

	var deserializedMetadata Metadata
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&deserializedMetadata)
	if err != nil {
		return mstorage.ObjectMetadata{}, mstorage.EmbedError(bucket, object, err)
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
	trimmedObject := strings.TrimPrefix(object, prefix)
	metadata := mstorage.ObjectMetadata{
		Bucket:      bucket,
		Key:         trimmedObject,
		Created:     stat.ModTime(),
		Size:        stat.Size(),
		ETag:        etag,
		ContentType: contentType,
	}

	return metadata, nil
}

// CreateObject - PUT object
func (storage *Storage) CreateObject(bucket, key, contentType string, data io.Reader) error {
	// TODO Commits should stage then move instead of writing directly
	storage.lock.Lock()
	defer storage.lock.Unlock()

	// check bucket name valid
	if mstorage.IsValidBucket(bucket) == false {
		return mstorage.BucketNameInvalid{Bucket: bucket}
	}

	// check bucket exists
	if _, err := os.Stat(path.Join(storage.root, bucket)); os.IsNotExist(err) {
		return mstorage.BucketNotFound{Bucket: bucket}
	}

	// verify object path legal
	if mstorage.IsValidObject(key) == false {
		return mstorage.ObjectNameInvalid{Bucket: bucket, Object: key}
	}

	// verify content type
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	contentType = strings.TrimSpace(contentType)

	// get object path
	objectPath := path.Join(storage.root, bucket, key)
	objectDir := path.Dir(objectPath)
	if _, err := os.Stat(objectDir); os.IsNotExist(err) {
		err = os.MkdirAll(objectDir, 0700)
		if err != nil {
			return mstorage.EmbedError(bucket, key, err)
		}
	}

	// check if object exists
	if _, err := os.Stat(objectPath); !os.IsNotExist(err) {
		return mstorage.ObjectExists{
			Bucket: bucket,
			Object: key,
		}
	}

	// write object
	file, err := os.OpenFile(objectPath, os.O_WRONLY|os.O_CREATE, 0600)
	defer file.Close()
	if err != nil {
		return mstorage.EmbedError(bucket, key, err)
	}

	h := md5.New()
	mw := io.MultiWriter(file, h)

	_, err = io.Copy(mw, data)
	if err != nil {
		return mstorage.EmbedError(bucket, key, err)
	}

	//
	file, err = os.OpenFile(objectPath+"$metadata", os.O_WRONLY|os.O_CREATE, 0600)
	defer file.Close()
	if err != nil {
		return mstorage.EmbedError(bucket, key, err)
	}

	// serialize metadata to gob
	encoder := gob.NewEncoder(file)
	err = encoder.Encode(&Metadata{
		ContentType: contentType,
		Md5sum:      h.Sum(nil),
	})
	if err != nil {
		return mstorage.EmbedError(bucket, key, err)
	}

	return nil
}
