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
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/minio-io/minio/pkg/iodine"
	"github.com/minio-io/minio/pkg/storage/drivers"
)

// memoryDriver - local variables
type memoryDriver struct {
	storedBuckets       map[string]storedBucket
	lock                *sync.RWMutex
	objects             *Cache
	lastAccessedObjects map[string]time.Time
	maxSize             uint64
	expiration          time.Duration
	shutdown            bool
}

type storedBucket struct {
	bucketMetadata drivers.BucketMetadata
	objectMetadata map[string]drivers.ObjectMetadata
}

const (
	totalBuckets = 100
)

// Start memory object server
func Start(maxSize uint64, expiration time.Duration) (chan<- string, <-chan error, drivers.Driver) {
	ctrlChannel := make(chan string)
	errorChannel := make(chan error)

	var memory *memoryDriver
	memory = new(memoryDriver)
	memory.storedBuckets = make(map[string]storedBucket)
	memory.lastAccessedObjects = make(map[string]time.Time)
	memory.objects = NewCache(maxSize)
	memory.maxSize = maxSize
	memory.lock = new(sync.RWMutex)
	memory.expiration = expiration
	memory.shutdown = false

	memory.objects.OnEvicted = memory.evictObject

	// set up memory expiration
	if expiration > 0 {
		go memory.expireLRUObjects()
	}
	go start(ctrlChannel, errorChannel)
	return ctrlChannel, errorChannel, memory
}

func start(ctrlChannel <-chan string, errorChannel chan<- error) {
	close(errorChannel)
}

// GetObject - GET object from memory buffer
func (memory *memoryDriver) GetObject(w io.Writer, bucket string, object string) (int64, error) {
	memory.lock.RLock()
	if !drivers.IsValidBucket(bucket) {
		memory.lock.RUnlock()
		return 0, iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, nil)
	}
	if !drivers.IsValidObjectName(object) {
		memory.lock.RUnlock()
		return 0, iodine.New(drivers.ObjectNameInvalid{Object: object}, nil)
	}
	if _, ok := memory.storedBuckets[bucket]; ok == false {
		memory.lock.RUnlock()
		return 0, iodine.New(drivers.BucketNotFound{Bucket: bucket}, nil)
	}
	storedBucket := memory.storedBuckets[bucket]
	// form objectKey
	objectKey := bucket + "/" + object
	if _, ok := storedBucket.objectMetadata[objectKey]; ok {
		if data, ok := memory.objects.Get(objectKey); ok {
			memory.lock.RUnlock()
			go memory.updateAccessTime(objectKey)
			written, err := io.Copy(w, data)
			return written, iodine.New(err, nil)
		}
	}
	memory.lock.RUnlock()
	return 0, iodine.New(drivers.ObjectNotFound{Bucket: bucket, Object: object}, nil)
}

// GetPartialObject - GET object from memory buffer range
func (memory *memoryDriver) GetPartialObject(w io.Writer, bucket, object string, start, length int64) (int64, error) {
	memory.lock.RLock()
	defer memory.lock.RUnlock()
	var sourceBuffer bytes.Buffer
	if !drivers.IsValidBucket(bucket) {
		return 0, iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, nil)
	}
	if !drivers.IsValidObjectName(object) {
		return 0, iodine.New(drivers.ObjectNameInvalid{Object: object}, nil)
	}
	if _, err := memory.GetObject(&sourceBuffer, bucket, object); err != nil {
		return 0, iodine.New(err, nil)
	}
	if _, err := io.CopyN(ioutil.Discard, &sourceBuffer, start); err != nil {
		return 0, iodine.New(err, nil)
	}
	return io.CopyN(w, &sourceBuffer, length)
}

// GetBucketMetadata -
func (memory *memoryDriver) GetBucketMetadata(bucket string) (drivers.BucketMetadata, error) {
	memory.lock.RLock()
	defer memory.lock.RUnlock()
	if !drivers.IsValidBucket(bucket) {
		return drivers.BucketMetadata{}, iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, nil)
	}
	if _, ok := memory.storedBuckets[bucket]; ok == false {
		return drivers.BucketMetadata{}, iodine.New(drivers.BucketNotFound{Bucket: bucket}, nil)
	}
	return memory.storedBuckets[bucket].bucketMetadata, nil
}

// SetBucketMetadata -
func (memory *memoryDriver) SetBucketMetadata(bucket, acl string) error {
	memory.lock.RLock()
	if !drivers.IsValidBucket(bucket) {
		memory.lock.RUnlock()
		return iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, nil)
	}
	if _, ok := memory.storedBuckets[bucket]; ok == false {
		memory.lock.RUnlock()
		return iodine.New(drivers.BucketNotFound{Bucket: bucket}, nil)
	}
	if strings.TrimSpace(acl) == "" {
		acl = "private"
	}
	memory.lock.RUnlock()
	memory.lock.Lock()
	defer memory.lock.Unlock()
	storedBucket := memory.storedBuckets[bucket]
	storedBucket.bucketMetadata.ACL = drivers.BucketACL(acl)
	memory.storedBuckets[bucket] = storedBucket
	return nil
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

func (memory *memoryDriver) CreateObject(bucket, key, contentType, expectedMD5Sum string, size int64, data io.Reader) (string, error) {
	return memory.createObject(bucket, key, contentType, expectedMD5Sum, size, data)
}

// CreateObject - PUT object to memory buffer
func (memory *memoryDriver) createObject(bucket, key, contentType, expectedMD5Sum string, size int64, data io.Reader) (string, error) {
	memory.lock.RLock()
	if !drivers.IsValidBucket(bucket) {
		memory.lock.RUnlock()
		return "", iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, nil)
	}
	if !drivers.IsValidObjectName(key) {
		memory.lock.RUnlock()
		return "", iodine.New(drivers.ObjectNameInvalid{Object: key}, nil)
	}
	if _, ok := memory.storedBuckets[bucket]; ok == false {
		memory.lock.RUnlock()
		return "", iodine.New(drivers.BucketNotFound{Bucket: bucket}, nil)
	}
	storedBucket := memory.storedBuckets[bucket]
	// get object key
	objectKey := bucket + "/" + key
	if _, ok := storedBucket.objectMetadata[objectKey]; ok == true {
		memory.lock.RUnlock()
		return "", iodine.New(drivers.ObjectExists{Bucket: bucket, Object: key}, nil)
	}
	memory.lock.RUnlock()

	if contentType == "" {
		contentType = "application/octet-stream"
	}
	contentType = strings.TrimSpace(contentType)
	if strings.TrimSpace(expectedMD5Sum) != "" {
		expectedMD5SumBytes, err := base64.StdEncoding.DecodeString(strings.TrimSpace(expectedMD5Sum))
		if err != nil {
			// pro-actively close the connection
			return "", iodine.New(drivers.InvalidDigest{Md5: expectedMD5Sum}, nil)
		}
		expectedMD5Sum = hex.EncodeToString(expectedMD5SumBytes)
	}

	memory.lock.Lock()
	md5Writer := md5.New()
	lruWriter := memory.objects.Add(objectKey, size)
	mw := io.MultiWriter(md5Writer, lruWriter)
	totalLength, err := io.CopyN(mw, data, size)
	if err != nil {
		memory.lock.Unlock()
		return "", iodine.New(err, nil)
	}
	if err := lruWriter.Close(); err != nil {
		memory.lock.Unlock()
		return "", iodine.New(err, nil)
	}
	memory.lock.Unlock()

	md5SumBytes := md5Writer.Sum(nil)
	md5Sum := hex.EncodeToString(md5SumBytes)
	// Verify if the written object is equal to what is expected, only if it is requested as such
	if strings.TrimSpace(expectedMD5Sum) != "" {
		if err := isMD5SumEqual(strings.TrimSpace(expectedMD5Sum), md5Sum); err != nil {
			return "", iodine.New(drivers.BadDigest{Md5: expectedMD5Sum, Bucket: bucket, Key: key}, nil)
		}
	}

	newObject := drivers.ObjectMetadata{
		Bucket: bucket,
		Key:    key,

		ContentType: contentType,
		Created:     time.Now().UTC(),
		Md5:         md5Sum,
		Size:        int64(totalLength),
	}

	memory.lock.Lock()
	memoryObject := make(map[string]drivers.ObjectMetadata)
	switch {
	case len(memory.storedBuckets[bucket].objectMetadata) == 0:
		storedBucket.objectMetadata = memoryObject
		storedBucket.objectMetadata[objectKey] = newObject
	default:
		storedBucket.objectMetadata[objectKey] = newObject
	}
	memory.storedBuckets[bucket] = storedBucket
	memory.lock.Unlock()

	// free
	debug.FreeOSMemory()
	return newObject.Md5, nil
}

// CreateBucket - create bucket in memory
func (memory *memoryDriver) CreateBucket(bucketName, acl string) error {
	memory.lock.RLock()
	if len(memory.storedBuckets) == totalBuckets {
		memory.lock.RLock()
		return iodine.New(drivers.TooManyBuckets{Bucket: bucketName}, nil)
	}
	if !drivers.IsValidBucket(bucketName) {
		memory.lock.RUnlock()
		return iodine.New(drivers.BucketNameInvalid{Bucket: bucketName}, nil)
	}
	if !drivers.IsValidBucketACL(acl) {
		memory.lock.RUnlock()
		return iodine.New(drivers.InvalidACL{ACL: acl}, nil)
	}
	if _, ok := memory.storedBuckets[bucketName]; ok == true {
		memory.lock.RUnlock()
		return iodine.New(drivers.BucketExists{Bucket: bucketName}, nil)
	}
	memory.lock.RUnlock()

	if strings.TrimSpace(acl) == "" {
		// default is private
		acl = "private"
	}
	var newBucket = storedBucket{}
	newBucket.objectMetadata = make(map[string]drivers.ObjectMetadata)
	newBucket.bucketMetadata = drivers.BucketMetadata{}
	newBucket.bucketMetadata.Name = bucketName
	newBucket.bucketMetadata.Created = time.Now().UTC()
	newBucket.bucketMetadata.ACL = drivers.BucketACL(acl)
	memory.lock.Lock()
	defer memory.lock.Unlock()
	memory.storedBuckets[bucketName] = newBucket
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

func (memory *memoryDriver) filterDelimiterPrefix(keys []string, key, delimitedName string, resources drivers.BucketResourcesMetadata) (drivers.BucketResourcesMetadata, []string) {
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

func (memory *memoryDriver) listObjectsInternal(keys []string, key string, resources drivers.BucketResourcesMetadata) ([]string, drivers.BucketResourcesMetadata) {
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
	return keys, resources
}

// ListObjects - list objects from memory
func (memory *memoryDriver) ListObjects(bucket string, resources drivers.BucketResourcesMetadata) ([]drivers.ObjectMetadata, drivers.BucketResourcesMetadata, error) {
	memory.lock.RLock()
	defer memory.lock.RUnlock()
	if !drivers.IsValidBucket(bucket) {
		return nil, drivers.BucketResourcesMetadata{IsTruncated: false}, iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, nil)
	}
	if !drivers.IsValidObjectName(resources.Prefix) {
		return nil, drivers.BucketResourcesMetadata{IsTruncated: false}, iodine.New(drivers.ObjectNameInvalid{Object: resources.Prefix}, nil)
	}
	if _, ok := memory.storedBuckets[bucket]; ok == false {
		return nil, drivers.BucketResourcesMetadata{IsTruncated: false}, iodine.New(drivers.BucketNotFound{Bucket: bucket}, nil)
	}
	var results []drivers.ObjectMetadata
	var keys []string
	storedBucket := memory.storedBuckets[bucket]
	for key := range storedBucket.objectMetadata {
		if strings.HasPrefix(key, bucket+"/") {
			key = key[len(bucket)+1:]
			keys, resources = memory.listObjectsInternal(keys, key, resources)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		if len(results) == resources.Maxkeys {
			resources.IsTruncated = true
			return results, resources, nil
		}
		object := storedBucket.objectMetadata[bucket+"/"+key]
		results = append(results, object)
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
func (memory *memoryDriver) ListBuckets() ([]drivers.BucketMetadata, error) {
	memory.lock.RLock()
	defer memory.lock.RUnlock()
	var results []drivers.BucketMetadata
	for _, bucket := range memory.storedBuckets {
		results = append(results, bucket.bucketMetadata)
	}
	sort.Sort(ByBucketName(results))
	return results, nil
}

// GetObjectMetadata - get object metadata from memory
func (memory *memoryDriver) GetObjectMetadata(bucket, key, prefix string) (drivers.ObjectMetadata, error) {
	memory.lock.RLock()
	defer memory.lock.RUnlock()
	// check if bucket exists
	if !drivers.IsValidBucket(bucket) {
		return drivers.ObjectMetadata{}, iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, nil)
	}
	if !drivers.IsValidObjectName(key) || !drivers.IsValidObjectName(prefix) {
		return drivers.ObjectMetadata{}, iodine.New(drivers.ObjectNameInvalid{Object: key}, nil)
	}
	if _, ok := memory.storedBuckets[bucket]; ok == false {
		return drivers.ObjectMetadata{}, iodine.New(drivers.BucketNotFound{Bucket: bucket}, nil)
	}
	storedBucket := memory.storedBuckets[bucket]
	objectKey := bucket + "/" + key
	if object, ok := storedBucket.objectMetadata[objectKey]; ok == true {
		return object, nil
	}
	return drivers.ObjectMetadata{}, iodine.New(drivers.ObjectNotFound{Bucket: bucket, Object: key}, nil)
}

func (memory *memoryDriver) evictObject(a ...interface{}) {
	cacheStats := memory.objects.Stats()
	log.Printf("CurrenSize: %d, CurrentItems: %d, TotalEvictions: %d",
		cacheStats.Bytes, memory.objects.Len(), cacheStats.Evictions)
	key := a[0].(string)
	// loop through all buckets
	for bucket, storedBucket := range memory.storedBuckets {
		delete(storedBucket.objectMetadata, key)
		delete(memory.lastAccessedObjects, key)
		// remove bucket if no objects found anymore
		if len(storedBucket.objectMetadata) == 0 {
			delete(memory.storedBuckets, bucket)
		}
	}
	debug.FreeOSMemory()
}

func (memory *memoryDriver) expireLRUObjects() {
	for {
		if memory.shutdown {
			return
		}
		var sleepDuration time.Duration
		memory.lock.Lock()
		switch {
		case memory.objects.Len() > 0:
			if k, ok := memory.objects.GetOldest(); ok {
				key := k.(string)
				switch {
				case time.Now().Sub(memory.lastAccessedObjects[key]) > memory.expiration:
					memory.objects.RemoveOldest()
				default:
					sleepDuration = memory.expiration - time.Now().Sub(memory.lastAccessedObjects[key])
				}
			}
		default:
			sleepDuration = memory.expiration
		}
		memory.lock.Unlock()
		time.Sleep(sleepDuration)
	}
}

func (memory *memoryDriver) updateAccessTime(key string) {
	memory.lock.Lock()
	defer memory.lock.Unlock()
	if _, ok := memory.lastAccessedObjects[key]; ok {
		memory.lastAccessedObjects[key] = time.Now().UTC()
	}
}
