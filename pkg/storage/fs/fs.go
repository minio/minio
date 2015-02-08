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

package fs

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"

	mstorage "github.com/minio-io/minio/pkg/storage"
)

type storage struct {
	root      string
	writeLock sync.Mutex
}

type SerializedMetadata struct {
	ContentType string
}

type MkdirFailedError struct{}

func (self MkdirFailedError) Error() string {
	return "Mkdir Failed"
}

func Start(root string) (chan<- string, <-chan error, *storage) {
	ctrlChannel := make(chan string)
	errorChannel := make(chan error)
	go start(ctrlChannel, errorChannel)
	return ctrlChannel, errorChannel, &storage{root: root}
}

func start(ctrlChannel <-chan string, errorChannel chan<- error) {
	close(errorChannel)
}

// Bucket Operations

func (storage *storage) ListBuckets(prefix string) ([]mstorage.BucketMetadata, error) {
	if prefix != "" {
		if mstorage.IsValidBucket(prefix) == false {
			return []mstorage.BucketMetadata{}, mstorage.BucketNameInvalid{Bucket: prefix}
		}
	}

	files, err := ioutil.ReadDir(storage.root)
	if err != nil {
		return []mstorage.BucketMetadata{}, mstorage.EmbedError("bucket", "", err)
	}

	var metadataList []mstorage.BucketMetadata
	for _, file := range files {
		if !file.IsDir() {
			return []mstorage.BucketMetadata{}, mstorage.BackendCorrupted{Path: storage.root}
		}
		if strings.HasPrefix(file.Name(), prefix) {
			metadata := mstorage.BucketMetadata{
				Name:    file.Name(),
				Created: file.ModTime(), // TODO - provide real created time
			}
			metadataList = append(metadataList, metadata)
		}
	}
	return metadataList, nil
}

func (storage *storage) StoreBucket(bucket string) error {
	storage.writeLock.Lock()
	defer storage.writeLock.Unlock()

	// verify bucket path legal
	if mstorage.IsValidBucket(bucket) == false {
		return mstorage.BucketNameInvalid{Bucket: bucket}
	}

	// get bucket path
	bucketDir := path.Join(storage.root, bucket)

	// check if bucket exists
	if _, err := os.Stat(bucketDir); err == nil {
		return mstorage.BucketExists{
			Bucket: bucket,
		}
	}

	// make bucket
	err := os.Mkdir(bucketDir, 0700)
	if err != nil {
		return mstorage.EmbedError(bucket, "", err)
	}
	return nil
}

// Object Operations

func (storage *storage) CopyObjectToWriter(w io.Writer, bucket string, object string) (int64, error) {
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
			} else {
				return 0, mstorage.EmbedError(bucket, object, err)
			}
		}
	}
	file, err := os.Open(objectPath)
	count, err := io.Copy(w, file)
	if err != nil {
		return count, mstorage.EmbedError(bucket, object, err)
	}
	return count, nil
}

func (storage *storage) GetObjectMetadata(bucket string, object string) (mstorage.ObjectMetadata, error) {
	if mstorage.IsValidBucket(bucket) == false {
		return mstorage.ObjectMetadata{}, mstorage.BucketNameInvalid{Bucket: bucket}
	}

	if mstorage.IsValidObject(bucket) == false {
		return mstorage.ObjectMetadata{}, mstorage.ObjectNameInvalid{Bucket: bucket, Object: bucket}
	}

	objectPath := path.Join(storage.root, bucket, object)

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

	metadataBuffer, err := ioutil.ReadAll(file)
	if err != nil {
		return mstorage.ObjectMetadata{}, mstorage.EmbedError(bucket, object, err)
	}

	var deserializedMetadata SerializedMetadata
	err = json.Unmarshal(metadataBuffer, &deserializedMetadata)
	if err != nil {
		return mstorage.ObjectMetadata{}, mstorage.EmbedError(bucket, object, err)
	}

	contentType := "application/octet-stream"
	if deserializedMetadata.ContentType != "" {
		contentType = deserializedMetadata.ContentType
	}
	contentType = strings.TrimSpace(contentType)

	metadata := mstorage.ObjectMetadata{
		Bucket:      bucket,
		Key:         object,
		Created:     stat.ModTime(),
		Size:        stat.Size(),
		ETag:        bucket + "#" + object,
		ContentType: contentType,
	}

	return metadata, nil
}

func (storage *storage) ListObjects(bucket, prefix string, count int) ([]mstorage.ObjectMetadata, bool, error) {
	if mstorage.IsValidBucket(bucket) == false {
		return []mstorage.ObjectMetadata{}, false, mstorage.BucketNameInvalid{Bucket: bucket}
	}
	if mstorage.IsValidObject(prefix) == false {
		return []mstorage.ObjectMetadata{}, false, mstorage.ObjectNameInvalid{Bucket: bucket, Object: prefix}
	}

	rootPrefix := path.Join(storage.root, bucket)

	// check bucket exists
	if _, err := os.Stat(rootPrefix); os.IsNotExist(err) {
		return []mstorage.ObjectMetadata{}, false, mstorage.BucketNotFound{Bucket: bucket}
	}

	files, err := ioutil.ReadDir(rootPrefix)
	if err != nil {
		return []mstorage.ObjectMetadata{}, false, mstorage.EmbedError("bucket", "", err)
	}

	var metadataList []mstorage.ObjectMetadata
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), "$metadata") {
			if len(metadataList) >= count {
				return metadataList, true, nil
			}
			if strings.HasPrefix(file.Name(), prefix) {
				metadata := mstorage.ObjectMetadata{
					Bucket:  bucket,
					Key:     file.Name(),
					Created: file.ModTime(),
					Size:    file.Size(),
					ETag:    bucket + "#" + file.Name(),
				}
				metadataList = append(metadataList, metadata)
			}
		}
	}
	return metadataList, false, nil
}

func (storage *storage) StoreObject(bucket, key, contentType string, data io.Reader) error {
	// TODO Commits should stage then move instead of writing directly
	storage.writeLock.Lock()
	defer storage.writeLock.Unlock()

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
			Key:    key,
		}
	}

	// write object
	file, err := os.OpenFile(objectPath, os.O_WRONLY|os.O_CREATE, 0600)
	defer file.Close()
	if err != nil {
		return mstorage.EmbedError(bucket, key, err)
	}

	_, err = io.Copy(file, data)
	if err != nil {
		return mstorage.EmbedError(bucket, key, err)
	}

	// serialize metadata to json

	metadataBuffer, err := json.Marshal(SerializedMetadata{ContentType: contentType})
	if err != nil {
		return mstorage.EmbedError(bucket, key, err)
	}

	//
	file, err = os.OpenFile(objectPath+"$metadata", os.O_WRONLY|os.O_CREATE, 0600)
	defer file.Close()
	if err != nil {
		return mstorage.EmbedError(bucket, key, err)
	}

	_, err = io.Copy(file, bytes.NewBuffer(metadataBuffer))
	if err != nil {
		return mstorage.EmbedError(bucket, key, err)
	}

	return nil
}
