/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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
	"bytes"
	"io"
	"os"
	"path"
	"strings"

	"github.com/minio-io/objectdriver"

	"crypto/md5"
	"encoding/base64"
	"encoding/gob"
	"encoding/hex"
)

/// Object Operations

// GetPartialObject - GET object from range
func (file *fileDriver) GetPartialObject(w io.Writer, bucket, object string, start, length int64) (int64, error) {
	// validate bucket
	if drivers.IsValidBucket(bucket) == false {
		return 0, drivers.BucketNameInvalid{Bucket: bucket}
	}

	// validate object
	if drivers.IsValidObject(object) == false {
		return 0, drivers.ObjectNameInvalid{Bucket: bucket, Object: object}
	}

	objectPath := path.Join(file.root, bucket, object)
	filestat, err := os.Stat(objectPath)
	switch err := err.(type) {
	case nil:
		{
			if filestat.IsDir() {
				return 0, drivers.ObjectNotFound{Bucket: bucket, Object: object}
			}
		}
	default:
		{
			if os.IsNotExist(err) {
				return 0, drivers.ObjectNotFound{Bucket: bucket, Object: object}
			}
			return 0, drivers.EmbedError(bucket, object, err)
		}
	}
	f, err := os.Open(objectPath)
	defer f.Close()
	if err != nil {
		return 0, drivers.EmbedError(bucket, object, err)
	}

	_, err = f.Seek(start, os.SEEK_SET)
	if err != nil {
		return 0, drivers.EmbedError(bucket, object, err)
	}

	count, err := io.CopyN(w, f, length)
	if err != nil {
		return count, drivers.EmbedError(bucket, object, err)
	}

	return count, nil
}

// GetObject - GET object from key
func (file *fileDriver) GetObject(w io.Writer, bucket string, object string) (int64, error) {
	// validate bucket
	if drivers.IsValidBucket(bucket) == false {
		return 0, drivers.BucketNameInvalid{Bucket: bucket}
	}

	// check bucket exists
	if _, err := os.Stat(path.Join(file.root, bucket)); os.IsNotExist(err) {
		return 0, drivers.BucketNotFound{Bucket: bucket}
	}

	// validate object
	if drivers.IsValidObject(object) == false {
		return 0, drivers.ObjectNameInvalid{Bucket: bucket, Object: object}
	}

	objectPath := path.Join(file.root, bucket, object)

	filestat, err := os.Stat(objectPath)
	switch err := err.(type) {
	case nil:
		{
			if filestat.IsDir() {
				return 0, drivers.ObjectNotFound{Bucket: bucket, Object: object}
			}
		}
	default:
		{
			if os.IsNotExist(err) {
				return 0, drivers.ObjectNotFound{Bucket: bucket, Object: object}
			}
			return 0, drivers.EmbedError(bucket, object, err)
		}
	}
	f, err := os.Open(objectPath)
	defer f.Close()
	if err != nil {
		return 0, drivers.EmbedError(bucket, object, err)
	}

	count, err := io.Copy(w, f)
	if err != nil {
		return count, drivers.EmbedError(bucket, object, err)
	}
	return count, nil
}

// GetObjectMetadata - HEAD object
func (file *fileDriver) GetObjectMetadata(bucket, object, prefix string) (drivers.ObjectMetadata, error) {
	if drivers.IsValidBucket(bucket) == false {
		return drivers.ObjectMetadata{}, drivers.BucketNameInvalid{Bucket: bucket}
	}
	if drivers.IsValidObject(object) == false {
		return drivers.ObjectMetadata{}, drivers.ObjectNameInvalid{Bucket: bucket, Object: bucket}
	}
	// check bucket exists
	if _, err := os.Stat(path.Join(file.root, bucket)); os.IsNotExist(err) {
		return drivers.ObjectMetadata{}, drivers.BucketNotFound{Bucket: bucket}
	}
	// Do not use path.Join() since path.Join strips off any object names with '/', use them as is
	// in a static manner so that we can send a proper 'ObjectNotFound' reply back upon os.Stat()
	objectPath := file.root + "/" + bucket + "/" + object
	stat, err := os.Stat(objectPath)
	if os.IsNotExist(err) {
		return drivers.ObjectMetadata{}, drivers.ObjectNotFound{Bucket: bucket, Object: object}
	}

	_, err = os.Stat(objectPath + "$metadata")
	if os.IsNotExist(err) {
		return drivers.ObjectMetadata{}, drivers.ObjectNotFound{Bucket: bucket, Object: object}
	}

	f, err := os.Open(objectPath + "$metadata")
	defer f.Close()
	if err != nil {
		return drivers.ObjectMetadata{}, drivers.EmbedError(bucket, object, err)
	}

	var deserializedMetadata fileMetadata
	decoder := gob.NewDecoder(f)
	err = decoder.Decode(&deserializedMetadata)
	if err != nil {
		return drivers.ObjectMetadata{}, drivers.EmbedError(bucket, object, err)
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
	metadata := drivers.ObjectMetadata{
		Bucket:      bucket,
		Key:         trimmedObject,
		Created:     stat.ModTime(),
		Size:        stat.Size(),
		Md5:         etag,
		ContentType: contentType,
	}

	return metadata, nil
}

// CreateObject - PUT object
func (file *fileDriver) CreateObject(bucket, key, contentType, md5sum string, data io.Reader) error {
	// TODO Commits should stage then move instead of writing directly
	file.lock.Lock()
	defer file.lock.Unlock()

	// check bucket name valid
	if drivers.IsValidBucket(bucket) == false {
		return drivers.BucketNameInvalid{Bucket: bucket}
	}

	// check bucket exists
	if _, err := os.Stat(path.Join(file.root, bucket)); os.IsNotExist(err) {
		return drivers.BucketNotFound{Bucket: bucket}
	}

	// verify object path legal
	if drivers.IsValidObject(key) == false {
		return drivers.ObjectNameInvalid{Bucket: bucket, Object: key}
	}

	// verify content type
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	contentType = strings.TrimSpace(contentType)

	// get object path
	objectPath := path.Join(file.root, bucket, key)
	objectDir := path.Dir(objectPath)
	if _, err := os.Stat(objectDir); os.IsNotExist(err) {
		err = os.MkdirAll(objectDir, 0700)
		if err != nil {
			return drivers.EmbedError(bucket, key, err)
		}
	}

	// check if object exists
	if _, err := os.Stat(objectPath); !os.IsNotExist(err) {
		return drivers.ObjectExists{
			Bucket: bucket,
			Object: key,
		}
	}

	// write object
	f, err := os.OpenFile(objectPath, os.O_WRONLY|os.O_CREATE, 0600)
	defer f.Close()
	if err != nil {
		return drivers.EmbedError(bucket, key, err)
	}

	h := md5.New()
	mw := io.MultiWriter(f, h)

	_, err = io.Copy(mw, data)
	if err != nil {
		return drivers.EmbedError(bucket, key, err)
	}

	//
	f, err = os.OpenFile(objectPath+"$metadata", os.O_WRONLY|os.O_CREATE, 0600)
	defer f.Close()
	if err != nil {
		return drivers.EmbedError(bucket, key, err)
	}

	metadata := &fileMetadata{
		ContentType: contentType,
		Md5sum:      h.Sum(nil),
	}
	// serialize metadata to gob
	encoder := gob.NewEncoder(f)
	err = encoder.Encode(metadata)
	if err != nil {
		return drivers.EmbedError(bucket, key, err)
	}

	// Verify data received to be correct, Content-MD5 received
	if md5sum != "" {
		var data []byte
		data, err = base64.StdEncoding.DecodeString(md5sum)
		if err != nil {
			return drivers.InvalidDigest{Bucket: bucket, Key: key, Md5: md5sum}
		}
		if !bytes.Equal(metadata.Md5sum, data) {
			return drivers.BadDigest{Bucket: bucket, Key: key, Md5: md5sum}
		}
	}
	return nil
}
