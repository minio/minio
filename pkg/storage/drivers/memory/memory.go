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

package memory

import (
	"bufio"
	"bytes"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/minio-io/minio/pkg/storage/drivers"

	"crypto/md5"
	"encoding/hex"

	"github.com/golang/groupcache/lru"
	"io/ioutil"
)

// memoryDriver - local variables
type memoryDriver struct {
	bucketMetadata map[string]storedBucket
	objectMetadata map[string]storedObject
	objects        *lru.Cache
	lock           *sync.RWMutex
}

type storedBucket struct {
	metadata drivers.BucketMetadata
	//	owner    string // TODO
	//	id       string // TODO
}

type storedObject struct {
	metadata drivers.ObjectMetadata
}

// Start memory object server
func Start(maxObjects int) (chan<- string, <-chan error, drivers.Driver) {
	ctrlChannel := make(chan string)
	errorChannel := make(chan error)

	memory := new(memoryDriver)
	memory.bucketMetadata = make(map[string]storedBucket)
	memory.objectMetadata = make(map[string]storedObject)
	memory.objects = lru.New(maxObjects)
	memory.lock = new(sync.RWMutex)

	memory.objects.OnEvicted = memory.evictObject

	go start(ctrlChannel, errorChannel)
	return ctrlChannel, errorChannel, memory
}

func start(ctrlChannel <-chan string, errorChannel chan<- error) {
	close(errorChannel)
}

// GetObject - GET object from memory buffer
func (memory memoryDriver) GetObject(w io.Writer, bucket string, object string) (int64, error) {
	memory.lock.RLock()
	defer memory.lock.RUnlock()
	if _, ok := memory.bucketMetadata[bucket]; ok == false {
		return 0, drivers.BucketNotFound{Bucket: bucket}
	}
	// get object
	objectKey := bucket + "/" + object
	if _, ok := memory.objectMetadata[objectKey]; ok {
		if data, ok := memory.objects.Get(objectKey); ok {
			dataSlice := data.([]byte)
			objectBuffer := bytes.NewBuffer(dataSlice)
			written, err := io.Copy(w, objectBuffer)
			return written, err
		}
	}
	return 0, drivers.ObjectNotFound{Bucket: bucket, Object: object}
}

// GetPartialObject - GET object from memory buffer range
func (memory memoryDriver) GetPartialObject(w io.Writer, bucket, object string, start, length int64) (int64, error) {
	memory.lock.RLock()
	defer memory.lock.RUnlock()
	var sourceBuffer bytes.Buffer
	if _, err := memory.GetObject(&sourceBuffer, bucket, object); err != nil {
		return 0, err
	}
	if _, err := io.CopyN(ioutil.Discard, &sourceBuffer, start); err != nil {
		return 0, err
	}
	return io.CopyN(w, &sourceBuffer, length)
}

// GetBucketMetadata -
func (memory memoryDriver) GetBucketMetadata(bucket string) (drivers.BucketMetadata, error) {
	memory.lock.RLock()
	defer memory.lock.RUnlock()
	if _, ok := memory.bucketMetadata[bucket]; ok == false {
		return drivers.BucketMetadata{}, drivers.BucketNotFound{Bucket: bucket}
	}
	return memory.bucketMetadata[bucket].metadata, nil
}

// CreateBucketPolicy - Not implemented
func (memory memoryDriver) CreateBucketPolicy(bucket string, policy drivers.BucketPolicy) error {
	return drivers.APINotImplemented{API: "PutBucketPolicy"}
}

// GetBucketPolicy - Not implemented
func (memory memoryDriver) GetBucketPolicy(bucket string) (drivers.BucketPolicy, error) {
	return drivers.BucketPolicy{}, drivers.APINotImplemented{API: "GetBucketPolicy"}
}

// CreateObject - PUT object to memory buffer
func (memory memoryDriver) CreateObject(bucket, key, contentType, md5sum string, data io.Reader) error {
	memory.lock.RLock()

	if _, ok := memory.bucketMetadata[bucket]; ok == false {
		memory.lock.RUnlock()
		return drivers.BucketNotFound{Bucket: bucket}
	}

	objectKey := bucket + "/" + key

	if _, ok := memory.objectMetadata[objectKey]; ok == true {
		memory.lock.RUnlock()
		return drivers.ObjectExists{Bucket: bucket, Object: key}
	}
	memory.lock.RUnlock()

	if contentType == "" {
		contentType = "application/octet-stream"
	}

	contentType = strings.TrimSpace(contentType)

	var bytesBuffer bytes.Buffer
	var newObject = storedObject{}
	var dataSlice []byte
	if _, ok := io.Copy(&bytesBuffer, data); ok == nil {
		size := bytesBuffer.Len()
		md5SumBytes := md5.Sum(bytesBuffer.Bytes())
		md5Sum := hex.EncodeToString(md5SumBytes[:])
		newObject.metadata = drivers.ObjectMetadata{
			Bucket: bucket,
			Key:    key,

			ContentType: contentType,
			Created:     time.Now(),
			Md5:         md5Sum,
			Size:        int64(size),
		}
		dataSlice = bytesBuffer.Bytes()
	}
	memory.lock.Lock()
	if _, ok := memory.objectMetadata[objectKey]; ok == true {
		memory.lock.Unlock()
		return drivers.ObjectExists{Bucket: bucket, Object: key}
	}
	memory.objectMetadata[objectKey] = newObject
	memory.objects.Add(objectKey, dataSlice)
	memory.lock.Unlock()
	return nil
}

// CreateBucket - create bucket in memory
func (memory memoryDriver) CreateBucket(bucketName string) error {
	memory.lock.RLock()
	if !drivers.IsValidBucket(bucketName) {
		memory.lock.RUnlock()
		return drivers.BucketNameInvalid{Bucket: bucketName}
	}

	if _, ok := memory.bucketMetadata[bucketName]; ok == true {
		memory.lock.RUnlock()
		return drivers.BucketExists{Bucket: bucketName}
	}
	memory.lock.RUnlock()

	var newBucket = storedBucket{}
	newBucket.metadata = drivers.BucketMetadata{}
	newBucket.metadata.Name = bucketName
	newBucket.metadata.Created = time.Now()
	memory.lock.Lock()
	defer memory.lock.Unlock()
	memory.bucketMetadata[bucketName] = newBucket

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

func (memory memoryDriver) filterDelimiterPrefix(keys []string, key, delimitedName string, resources drivers.BucketResourcesMetadata) (drivers.BucketResourcesMetadata, []string) {
	switch true {
	case key == resources.Prefix:
		keys = appendUniq(keys, key)
		// DelimitedName - requires resources.Prefix as it was trimmed off earlier in the flow
	case key == resources.Prefix+delimitedName:
		keys = appendUniq(keys, key)
	case delimitedName != "":
		resources.CommonPrefixes = appendUniq(resources.CommonPrefixes, resources.Prefix+delimitedName)
	}
	return resources, keys
}

// ListObjects - list objects from memory
func (memory memoryDriver) ListObjects(bucket string, resources drivers.BucketResourcesMetadata) ([]drivers.ObjectMetadata, drivers.BucketResourcesMetadata, error) {
	memory.lock.RLock()
	defer memory.lock.RUnlock()
	if _, ok := memory.bucketMetadata[bucket]; ok == false {
		return []drivers.ObjectMetadata{}, drivers.BucketResourcesMetadata{IsTruncated: false}, drivers.BucketNotFound{Bucket: bucket}
	}
	var results []drivers.ObjectMetadata
	var keys []string
	for key := range memory.objectMetadata {
		if strings.HasPrefix(key, bucket+"/") {
			key = key[len(bucket)+1:]
			switch true {
			// Prefix absent, delimit object key based on delimiter
			case resources.IsDelimiterSet():
				delimitedName := delimiter(key, resources.Delimiter)
				switch true {
				case delimitedName == "" || delimitedName == key:
					keys = appendUniq(keys, key)
				case delimitedName != "":
					resources.CommonPrefixes = appendUniq(resources.CommonPrefixes, delimitedName)
				}
			// Prefix present, delimit object key with prefix key based on delimiter
			case resources.IsDelimiterPrefixSet():
				if strings.HasPrefix(key, resources.Prefix) {
					trimmedName := strings.TrimPrefix(key, resources.Prefix)
					delimitedName := delimiter(trimmedName, resources.Delimiter)
					resources, keys = memory.filterDelimiterPrefix(keys, key, delimitedName, resources)
				}
			// Prefix present, nothing to delimit
			case resources.IsPrefixSet():
				keys = appendUniq(keys, key)
			// Prefix and delimiter absent
			case resources.IsDefault():
				keys = appendUniq(keys, key)
			}
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		if len(results) == resources.Maxkeys {
			return results, drivers.BucketResourcesMetadata{IsTruncated: true}, nil
		}
		object := memory.objectMetadata[bucket+"/"+key]
		if bucket == object.metadata.Bucket {
			results = append(results, object.metadata)
		}
	}
	return results, resources, nil
}

// ByBucketName is a type for sorting bucket metadata by bucket name
type ByBucketName []drivers.BucketMetadata

// Len of bucket name
func (b ByBucketName) Len() int { return len(b) }

// Swap bucket i, j
func (b ByBucketName) Swap(i, j int) { b[i], b[j] = b[j], b[i] }

// Less
func (b ByBucketName) Less(i, j int) bool { return b[i].Name < b[j].Name }

// ListBuckets - List buckets from memory
func (memory memoryDriver) ListBuckets() ([]drivers.BucketMetadata, error) {
	memory.lock.RLock()
	defer memory.lock.RUnlock()
	var results []drivers.BucketMetadata
	for _, bucket := range memory.bucketMetadata {
		results = append(results, bucket.metadata)
	}
	sort.Sort(ByBucketName(results))
	return results, nil
}

// GetObjectMetadata - get object metadata from memory
func (memory memoryDriver) GetObjectMetadata(bucket, key, prefix string) (drivers.ObjectMetadata, error) {
	memory.lock.RLock()
	defer memory.lock.RUnlock()
	// check if bucket exists
	if _, ok := memory.bucketMetadata[bucket]; ok == false {
		return drivers.ObjectMetadata{}, drivers.BucketNotFound{Bucket: bucket}
	}

	objectKey := bucket + "/" + key

	if object, ok := memory.objectMetadata[objectKey]; ok == true {
		return object.metadata, nil
	}
	return drivers.ObjectMetadata{}, drivers.ObjectNotFound{Bucket: bucket, Object: key}
}

func (memory memoryDriver) evictObject(key lru.Key, value interface{}) {
	k := key.(string)

	delete(memory.objectMetadata, k)
}
