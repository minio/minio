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
	"strconv"
	"strings"
	"sync"
	"time"

	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"errors"

	"io/ioutil"

	"github.com/golang/groupcache/lru"
	"github.com/minio-io/minio/pkg/iodine"
	"github.com/minio-io/minio/pkg/storage/drivers"
	"github.com/minio-io/minio/pkg/utils/log"
	"github.com/minio-io/minio/pkg/utils/split"
)

// memoryDriver - local variables
type memoryDriver struct {
	bucketMetadata map[string]storedBucket
	objectMetadata map[string]storedObject
	objects        *lru.Cache
	lock           *sync.RWMutex
	totalSize      uint64
	maxSize        uint64
	expiration     time.Duration
	shutdown       bool
}

type storedBucket struct {
	metadata drivers.BucketMetadata
	//	owner    string // TODO
	//	id       string // TODO
}

type storedObject struct {
	metadata drivers.ObjectMetadata
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
	memory.bucketMetadata = make(map[string]storedBucket)
	memory.objectMetadata = make(map[string]storedObject)
	memory.objects = lru.New(0)
	memory.lock = new(sync.RWMutex)
	memory.expiration = expiration
	memory.shutdown = false

	switch {
	case maxSize == 0:
		memory.maxSize = 9223372036854775807
	case maxSize > 0:
		memory.maxSize = maxSize
	default:
		log.Println("Error")
	}

	memory.objects.OnEvicted = memory.evictObject

	// set up memory expiration
	go memory.expireObjects()

	go start(ctrlChannel, errorChannel)
	return ctrlChannel, errorChannel, memory
}

func start(ctrlChannel <-chan string, errorChannel chan<- error) {
	close(errorChannel)
}

// GetObject - GET object from memory buffer
func (memory *memoryDriver) GetObject(w io.Writer, bucket string, object string) (int64, error) {
	memory.lock.RLock()
	defer memory.lock.RUnlock()
	if !drivers.IsValidBucket(bucket) {
		return 0, iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, nil)
	}
	if !drivers.IsValidObject(object) {
		return 0, iodine.New(drivers.ObjectNameInvalid{Object: object}, nil)
	}
	if _, ok := memory.bucketMetadata[bucket]; ok == false {
		return 0, iodine.New(drivers.BucketNotFound{Bucket: bucket}, nil)
	}
	// get object
	objectKey := bucket + "/" + object
	if _, ok := memory.objectMetadata[objectKey]; ok {
		if data, ok := memory.objects.Get(objectKey); ok {
			dataSlice := data.([]byte)
			objectBuffer := bytes.NewBuffer(dataSlice)
			written, err := io.Copy(w, objectBuffer)
			return written, iodine.New(err, nil)
		}
	}
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
	if !drivers.IsValidObject(object) {
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
	if _, ok := memory.bucketMetadata[bucket]; ok == false {
		return drivers.BucketMetadata{}, iodine.New(drivers.BucketNotFound{Bucket: bucket}, nil)
	}
	return memory.bucketMetadata[bucket].metadata, nil
}

// SetBucketMetadata -
func (memory *memoryDriver) SetBucketMetadata(bucket, acl string) error {
	memory.lock.RLock()
	if !drivers.IsValidBucket(bucket) {
		memory.lock.RUnlock()
		return iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, nil)
	}
	if _, ok := memory.bucketMetadata[bucket]; ok == false {
		memory.lock.RUnlock()
		return iodine.New(drivers.BucketNotFound{Bucket: bucket}, nil)
	}
	if strings.TrimSpace(acl) == "" {
		acl = "private"
	}
	memory.lock.RUnlock()
	memory.lock.Lock()
	defer memory.lock.Unlock()
	storedBucket := memory.bucketMetadata[bucket]
	storedBucket.metadata.ACL = drivers.BucketACL(acl)
	memory.bucketMetadata[bucket] = storedBucket
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

// CreateObject - PUT object to memory buffer
func (memory *memoryDriver) CreateObject(bucket, key, contentType, expectedMD5Sum string, data io.Reader) error {
	memory.lock.RLock()
	if !drivers.IsValidBucket(bucket) {
		memory.lock.RUnlock()
		return iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, nil)
	}
	if !drivers.IsValidObject(key) {
		memory.lock.RUnlock()
		return iodine.New(drivers.ObjectNameInvalid{Object: key}, nil)
	}
	if _, ok := memory.bucketMetadata[bucket]; ok == false {
		memory.lock.RUnlock()
		return iodine.New(drivers.BucketNotFound{Bucket: bucket}, nil)
	}
	objectKey := bucket + "/" + key
	if _, ok := memory.objectMetadata[objectKey]; ok == true {
		memory.lock.RUnlock()
		return iodine.New(drivers.ObjectExists{Bucket: bucket, Object: key}, nil)
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
			return iodine.New(drivers.InvalidDigest{Md5: expectedMD5Sum}, nil)
		}
		expectedMD5Sum = hex.EncodeToString(expectedMD5SumBytes)
	}

	var bytesBuffer bytes.Buffer
	var newObject = storedObject{}

	chunks := split.Stream(data, 10*1024*1024)
	totalLength := 0
	summer := md5.New()
	for chunk := range chunks {
		if chunk.Err == nil {
			totalLength = totalLength + len(chunk.Data)
			summer.Write(chunk.Data)
			_, err := io.Copy(&bytesBuffer, bytes.NewBuffer(chunk.Data))
			if err != nil {
				err := iodine.New(err, nil)
				log.Println(err)
				return err
			}
			if uint64(totalLength) > memory.maxSize {
				return iodine.New(drivers.EntityTooLarge{
					Size:      strconv.FormatInt(int64(totalLength), 10),
					TotalSize: strconv.FormatUint(memory.totalSize, 10),
				}, nil)
			}
		}
	}
	md5SumBytes := summer.Sum(nil)
	md5Sum := hex.EncodeToString(md5SumBytes)
	// Verify if the written object is equal to what is expected, only if it is requested as such
	if strings.TrimSpace(expectedMD5Sum) != "" {
		if err := isMD5SumEqual(strings.TrimSpace(expectedMD5Sum), md5Sum); err != nil {
			return iodine.New(drivers.BadDigest{Md5: expectedMD5Sum, Bucket: bucket, Key: key}, nil)
		}
	}
	newObject.metadata = drivers.ObjectMetadata{
		Bucket: bucket,
		Key:    key,

		ContentType: contentType,
		Created:     time.Now(),
		Md5:         md5Sum,
		Size:        int64(totalLength),
	}
	memory.lock.Lock()
	if _, ok := memory.objectMetadata[objectKey]; ok == true {
		memory.lock.Unlock()
		return iodine.New(drivers.ObjectExists{Bucket: bucket, Object: key}, nil)
	}
	memory.objectMetadata[objectKey] = newObject
	memory.objects.Add(objectKey, bytesBuffer.Bytes())
	memory.totalSize = memory.totalSize + uint64(newObject.metadata.Size)
	for memory.totalSize > memory.maxSize {
		memory.objects.RemoveOldest()
	}
	memory.lock.Unlock()
	return nil
}

// CreateBucket - create bucket in memory
func (memory *memoryDriver) CreateBucket(bucketName, acl string) error {
	memory.lock.RLock()
	if len(memory.bucketMetadata) == totalBuckets {
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
	if _, ok := memory.bucketMetadata[bucketName]; ok == true {
		memory.lock.RUnlock()
		return iodine.New(drivers.BucketExists{Bucket: bucketName}, nil)
	}
	memory.lock.RUnlock()

	if strings.TrimSpace(acl) == "" {
		// default is private
		acl = "private"
	}
	var newBucket = storedBucket{}
	newBucket.metadata = drivers.BucketMetadata{}
	newBucket.metadata.Name = bucketName
	newBucket.metadata.Created = time.Now()
	newBucket.metadata.ACL = drivers.BucketACL(acl)
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
	if !drivers.IsValidObject(resources.Prefix) {
		return nil, drivers.BucketResourcesMetadata{IsTruncated: false}, iodine.New(drivers.ObjectNameInvalid{Object: resources.Prefix}, nil)
	}
	if _, ok := memory.bucketMetadata[bucket]; ok == false {
		return nil, drivers.BucketResourcesMetadata{IsTruncated: false}, iodine.New(drivers.BucketNotFound{Bucket: bucket}, nil)
	}
	var results []drivers.ObjectMetadata
	var keys []string
	for key := range memory.objectMetadata {
		if strings.HasPrefix(key, bucket+"/") {
			key = key[len(bucket)+1:]
			keys, resources = memory.listObjectsInternal(keys, key, resources)
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
func (memory *memoryDriver) ListBuckets() ([]drivers.BucketMetadata, error) {
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
func (memory *memoryDriver) GetObjectMetadata(bucket, key, prefix string) (drivers.ObjectMetadata, error) {
	memory.lock.RLock()
	defer memory.lock.RUnlock()
	// check if bucket exists
	if !drivers.IsValidBucket(bucket) {
		return drivers.ObjectMetadata{}, iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, nil)
	}
	if !drivers.IsValidObject(key) || !drivers.IsValidObject(prefix) {
		return drivers.ObjectMetadata{}, iodine.New(drivers.ObjectNameInvalid{Object: key}, nil)
	}
	if _, ok := memory.bucketMetadata[bucket]; ok == false {
		return drivers.ObjectMetadata{}, iodine.New(drivers.BucketNotFound{Bucket: bucket}, nil)
	}
	objectKey := bucket + "/" + key
	if object, ok := memory.objectMetadata[objectKey]; ok == true {
		return object.metadata, nil
	}
	return drivers.ObjectMetadata{}, iodine.New(drivers.ObjectNotFound{Bucket: bucket, Object: key}, nil)
}

func (memory *memoryDriver) evictObject(key lru.Key, value interface{}) {
	k := key.(string)
	memory.totalSize = memory.totalSize - uint64(memory.objectMetadata[k].metadata.Size)
	log.Println("evicting:", k)
	delete(memory.objectMetadata, k)
}

func (memory *memoryDriver) expireObjects() {
	for {
		if memory.shutdown {
			return
		}
		var keysToRemove []string
		memory.lock.RLock()
		var earliest time.Time
		empty := true
		for key, object := range memory.objectMetadata {
			if empty {
				empty = false
			}
			if time.Now().Add(-memory.expiration).After(object.metadata.Created) {
				keysToRemove = append(keysToRemove, key)
			} else {
				if object.metadata.Created.Before(earliest) {
					earliest = object.metadata.Created
				}
			}
		}
		memory.lock.RUnlock()
		memory.lock.Lock()
		for _, key := range keysToRemove {
			memory.objects.Remove(key)
		}
		memory.lock.Unlock()
		if empty {
			time.Sleep(memory.expiration)
		} else {
			sleepFor := earliest.Sub(time.Now())
			if sleepFor > 0 {
				time.Sleep(sleepFor)
			}
		}
	}
}
