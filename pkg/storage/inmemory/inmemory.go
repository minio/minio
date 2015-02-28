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

package inmemory

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	mstorage "github.com/minio-io/minio/pkg/storage"
	"github.com/minio-io/minio/pkg/utils/policy"
)

type storage struct {
	bucketdata map[string]storedBucket
	objectdata map[string]storedObject
	lock       *sync.RWMutex
}

type storedBucket struct {
	metadata mstorage.BucketMetadata
	//	owner    string // TODO
	//	id       string // TODO
}

type storedObject struct {
	metadata mstorage.ObjectMetadata
	data     []byte
}

func (storage *storage) CopyObjectToWriter(w io.Writer, bucket string, object string) (int64, error) {
	// TODO synchronize access
	// get object
	key := bucket + ":" + object
	if val, ok := storage.objectdata[key]; ok {
		objectBuffer := bytes.NewBuffer(val.data)
		written, err := io.Copy(w, objectBuffer)
		return written, err
	} else {
		return 0, mstorage.ObjectNotFound{Bucket: bucket, Object: object}
	}
}

func (storage *storage) StoreBucketPolicy(bucket string, policy interface{}) error {
	return mstorage.ApiNotImplemented{Api: "PutBucketPolicy"}
}

func (storage *storage) GetBucketPolicy(bucket string) (interface{}, error) {
	return policy.BucketPolicy{}, mstorage.ApiNotImplemented{Api: "GetBucketPolicy"}
}

func (storage *storage) StoreObject(bucket, key, contentType string, data io.Reader) error {
	storage.lock.Lock()
	defer storage.lock.Unlock()

	objectKey := bucket + ":" + key

	if _, ok := storage.bucketdata[bucket]; ok == false {
		return mstorage.BucketNotFound{Bucket: bucket}
	}

	if _, ok := storage.objectdata[objectKey]; ok == true {
		return mstorage.ObjectExists{Bucket: bucket, Key: key}
	}

	if contentType == "" {
		contentType = "application/octet-stream"
	}

	contentType = strings.TrimSpace(contentType)

	var bytesBuffer bytes.Buffer
	var newObject = storedObject{}
	if _, ok := io.Copy(&bytesBuffer, data); ok == nil {
		size := bytesBuffer.Len()
		etag := fmt.Sprintf("%x", sha256.Sum256(bytesBuffer.Bytes()))
		newObject.metadata = mstorage.ObjectMetadata{
			Bucket: bucket,
			Key:    key,

			ContentType: contentType,
			Created:     time.Now(),
			ETag:        etag,
			Size:        int64(size),
		}
		newObject.data = bytesBuffer.Bytes()
	}
	storage.objectdata[objectKey] = newObject
	return nil
}

func (storage *storage) StoreBucket(bucketName string) error {
	storage.lock.Lock()
	defer storage.lock.Unlock()
	if !mstorage.IsValidBucket(bucketName) {
		return mstorage.BucketNameInvalid{Bucket: bucketName}
	}

	if _, ok := storage.bucketdata[bucketName]; ok == true {
		return mstorage.BucketExists{Bucket: bucketName}
	}

	var newBucket = storedBucket{}
	newBucket.metadata = mstorage.BucketMetadata{}
	newBucket.metadata.Name = bucketName
	newBucket.metadata.Created = time.Now()
	storage.bucketdata[bucketName] = newBucket

	return nil
}

func (storage *storage) ListObjects(bucket string, resources mstorage.BucketResourcesMetadata) ([]mstorage.ObjectMetadata, mstorage.BucketResourcesMetadata, error) {
	if _, ok := storage.bucketdata[bucket]; ok == false {
		return []mstorage.ObjectMetadata{}, mstorage.BucketResourcesMetadata{IsTruncated: false}, mstorage.BucketNotFound{Bucket: bucket}
	}
	// TODO prefix and count handling
	var results []mstorage.ObjectMetadata
	var keys []string
	for key := range storage.objectdata {
		if strings.HasPrefix(key, bucket+":"+resources.Prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		if len(results) == resources.Maxkeys {
			return results, mstorage.BucketResourcesMetadata{IsTruncated: true}, nil
		}
		object := storage.objectdata[key]
		if bucket == object.metadata.Bucket {
			if strings.HasPrefix(key, bucket+":"+resources.Prefix) {
				results = append(results, object.metadata)
			}
		}
	}
	return results, resources, nil
}

type ByBucketName []mstorage.BucketMetadata

func (b ByBucketName) Len() int           { return len(b) }
func (b ByBucketName) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b ByBucketName) Less(i, j int) bool { return b[i].Name < b[j].Name }

func (storage *storage) ListBuckets() ([]mstorage.BucketMetadata, error) {
	var results []mstorage.BucketMetadata
	for _, bucket := range storage.bucketdata {
		results = append(results, bucket.metadata)
	}
	sort.Sort(ByBucketName(results))
	return results, nil
}

func Start() (chan<- string, <-chan error, *storage) {
	ctrlChannel := make(chan string)
	errorChannel := make(chan error)
	go start(ctrlChannel, errorChannel)
	return ctrlChannel, errorChannel, &storage{
		bucketdata: make(map[string]storedBucket),
		objectdata: make(map[string]storedObject),
		lock:       new(sync.RWMutex),
	}
}

func start(ctrlChannel <-chan string, errorChannel chan<- error) {
	close(errorChannel)
}

func (storage *storage) GetObjectMetadata(bucket, key string) (mstorage.ObjectMetadata, error) {
	objectKey := bucket + ":" + key

	if object, ok := storage.objectdata[objectKey]; ok == true {
		return object.metadata, nil
	} else {
		return mstorage.ObjectMetadata{}, mstorage.ObjectNotFound{Bucket: bucket, Object: key}
	}
}
