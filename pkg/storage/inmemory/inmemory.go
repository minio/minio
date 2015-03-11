/*
 * Minio Object Storage, (C) 2015 Minio, Inc.
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
	"bufio"
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	mstorage "github.com/minio-io/minio/pkg/storage"
)

// Storage - local variables
type Storage struct {
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

// Start inmemory object server
func Start() (chan<- string, <-chan error, *Storage) {
	ctrlChannel := make(chan string)
	errorChannel := make(chan error)
	go start(ctrlChannel, errorChannel)
	return ctrlChannel, errorChannel, &Storage{
		bucketdata: make(map[string]storedBucket),
		objectdata: make(map[string]storedObject),
		lock:       new(sync.RWMutex),
	}
}

func start(ctrlChannel <-chan string, errorChannel chan<- error) {
	close(errorChannel)
}

// CopyObjectToWriter - GET object from memory buffer
func (storage *Storage) CopyObjectToWriter(w io.Writer, bucket string, object string) (int64, error) {
	// get object
	key := object
	if val, ok := storage.objectdata[key]; ok {
		objectBuffer := bytes.NewBuffer(val.data)
		written, err := io.Copy(w, objectBuffer)
		return written, err
	}
	return 0, mstorage.ObjectNotFound{Bucket: bucket, Object: object}
}

// CopyObjectToWriterRange - GET object from memory buffer range
func (storage *Storage) CopyObjectToWriterRange(w io.Writer, bucket, object string, start, end int64) (int64, error) {
	return 0, mstorage.APINotImplemented{API: "GetObjectRange"}
}

// StoreBucketPolicy - Not implemented
func (storage *Storage) StoreBucketPolicy(bucket string, policy mstorage.BucketPolicy) error {
	return mstorage.APINotImplemented{API: "PutBucketPolicy"}
}

// GetBucketPolicy - Not implemented
func (storage *Storage) GetBucketPolicy(bucket string) (mstorage.BucketPolicy, error) {
	return mstorage.BucketPolicy{}, mstorage.APINotImplemented{API: "GetBucketPolicy"}
}

// StoreObject - PUT object to memory buffer
func (storage *Storage) StoreObject(bucket, key, contentType string, data io.Reader) error {
	storage.lock.Lock()
	defer storage.lock.Unlock()

	if _, ok := storage.bucketdata[bucket]; ok == false {
		return mstorage.BucketNotFound{Bucket: bucket}
	}

	if _, ok := storage.objectdata[key]; ok == true {
		return mstorage.ObjectExists{Bucket: bucket, Object: key}
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
	storage.objectdata[key] = newObject
	return nil
}

// StoreBucket - create bucket in memory
func (storage *Storage) StoreBucket(bucketName string) error {
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

func delimiter(object, delimiter string) string {
	readBuffer := bytes.NewBufferString(object)
	reader := bufio.NewReader(readBuffer)
	stringReader := strings.NewReader(delimiter)
	delimited, _ := stringReader.ReadByte()
	delimitedStr, _ := reader.ReadString(delimited)
	return delimitedStr
}

func appendUniq(slice []string, i string) []string {
	for _, ele := range slice {
		if ele == i {
			return slice
		}
	}
	return append(slice, i)
}

// ListObjects - list objects from memory
func (storage *Storage) ListObjects(bucket string, resources mstorage.BucketResourcesMetadata) ([]mstorage.ObjectMetadata, mstorage.BucketResourcesMetadata, error) {
	if _, ok := storage.bucketdata[bucket]; ok == false {
		return []mstorage.ObjectMetadata{}, mstorage.BucketResourcesMetadata{IsTruncated: false}, mstorage.BucketNotFound{Bucket: bucket}
	}
	var results []mstorage.ObjectMetadata
	var keys []string
	for key := range storage.objectdata {
		switch true {
		// Prefix absent, delimit object key based on delimiter
		case resources.Delimiter != "" && resources.Prefix == "":
			delimitedName := delimiter(key, resources.Delimiter)
			switch true {
			case delimitedName == "" || delimitedName == key:
				keys = appendUniq(keys, key)
			case delimitedName != "":
				resources.CommonPrefixes = appendUniq(resources.CommonPrefixes, delimitedName)
			}
		// Prefix present, delimit object key with prefix key based on delimiter
		case resources.Delimiter != "" && resources.Prefix != "" && strings.HasPrefix(key, resources.Prefix):
			trimmedName := strings.TrimPrefix(key, resources.Prefix)
			delimitedName := delimiter(trimmedName, resources.Delimiter)
			fmt.Println(trimmedName, delimitedName, key, resources.Prefix)
			switch true {
			case key == resources.Prefix:
				keys = appendUniq(keys, key)
			// DelimitedName - requires resources.Prefix as it was trimmed off earlier in the flow
			case key == resources.Prefix+delimitedName:
				keys = appendUniq(keys, key)
			case delimitedName != "":
				if delimitedName == resources.Delimiter {
					resources.CommonPrefixes = appendUniq(resources.CommonPrefixes, resources.Prefix+delimitedName)
				} else {
					resources.CommonPrefixes = appendUniq(resources.CommonPrefixes, delimitedName)
				}
			}
		// Prefix present, nothing to delimit
		case resources.Delimiter == "" && resources.Prefix != "" && strings.HasPrefix(key, resources.Prefix):
			keys = appendUniq(keys, key)
		// Prefix and delimiter absent
		case resources.Prefix == "" && resources.Delimiter == "":
			keys = appendUniq(keys, key)
		}
	}

	sort.Strings(keys)
	for _, key := range keys {
		if len(results) == resources.Maxkeys {
			return results, mstorage.BucketResourcesMetadata{IsTruncated: true}, nil
		}
		object := storage.objectdata[key]
		if bucket == object.metadata.Bucket {
			results = append(results, object.metadata)
		}
	}
	return results, resources, nil
}

type byBucketName []mstorage.BucketMetadata

// Len of bucket name
func (b byBucketName) Len() int { return len(b) }

// Swap bucket i, j
func (b byBucketName) Swap(i, j int) { b[i], b[j] = b[j], b[i] }

// Less
func (b byBucketName) Less(i, j int) bool { return b[i].Name < b[j].Name }

// ListBuckets - List buckets from memory
func (storage *Storage) ListBuckets() ([]mstorage.BucketMetadata, error) {
	var results []mstorage.BucketMetadata
	for _, bucket := range storage.bucketdata {
		results = append(results, bucket.metadata)
	}
	sort.Sort(byBucketName(results))
	return results, nil
}

// GetObjectMetadata - get object metadata from memory
func (storage *Storage) GetObjectMetadata(bucket, key, prefix string) (mstorage.ObjectMetadata, error) {
	if object, ok := storage.objectdata[key]; ok == true {
		return object.metadata, nil
	}
	return mstorage.ObjectMetadata{}, mstorage.ObjectNotFound{Bucket: bucket, Object: key}
}
