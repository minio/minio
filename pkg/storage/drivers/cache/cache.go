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

package cache

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio/pkg/iodine"
	"github.com/minio/minio/pkg/storage/drivers"
	"github.com/minio/minio/pkg/storage/trove"
)

// cacheDriver - local variables
type cacheDriver struct {
	storedBuckets    map[string]storedBucket
	lock             *sync.RWMutex
	objects          *trove.Cache
	multiPartObjects *trove.Cache
	maxSize          uint64
	expiration       time.Duration
}

type storedBucket struct {
	bucketMetadata   drivers.BucketMetadata
	objectMetadata   map[string]drivers.ObjectMetadata
	partMetadata     map[string]drivers.PartMetadata
	multiPartSession map[string]multiPartSession
}

type multiPartSession struct {
	totalParts int
	uploadID   string
	initiated  time.Time
}

const (
	totalBuckets = 100
)

// NewDriver instantiate a new cache driver
func NewDriver(maxSize uint64, expiration time.Duration) (drivers.Driver, error) {
	cache := new(cacheDriver)
	cache.storedBuckets = make(map[string]storedBucket)
	cache.objects = trove.NewCache(maxSize, expiration)
	cache.maxSize = maxSize
	cache.expiration = expiration
	cache.multiPartObjects = trove.NewCache(0, time.Duration(0))
	cache.lock = new(sync.RWMutex)

	cache.objects.OnExpired = cache.expiredObject
	cache.multiPartObjects.OnExpired = cache.expiredPart

	// set up cache expiration
	cache.objects.ExpireObjects(time.Second * 5)
	return cache, nil
}

// GetObject - GET object from cache buffer
func (cache *cacheDriver) GetObject(w io.Writer, bucket string, object string) (int64, error) {
	cache.lock.RLock()
	if !drivers.IsValidBucket(bucket) {
		cache.lock.RUnlock()
		return 0, iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, nil)
	}
	if !drivers.IsValidObjectName(object) {
		cache.lock.RUnlock()
		return 0, iodine.New(drivers.ObjectNameInvalid{Object: object}, nil)
	}
	if _, ok := cache.storedBuckets[bucket]; ok == false {
		cache.lock.RUnlock()
		return 0, iodine.New(drivers.BucketNotFound{Bucket: bucket}, nil)
	}
	objectKey := bucket + "/" + object
	data, ok := cache.objects.Get(objectKey)
	if !ok {
		cache.lock.RUnlock()
		return 0, iodine.New(drivers.ObjectNotFound{Bucket: bucket, Object: object}, nil)
	}
	written, err := io.Copy(w, bytes.NewBuffer(data))
	cache.lock.RUnlock()
	return written, iodine.New(err, nil)
}

// GetPartialObject - GET object from cache buffer range
func (cache *cacheDriver) GetPartialObject(w io.Writer, bucket, object string, start, length int64) (int64, error) {
	errParams := map[string]string{
		"bucket": bucket,
		"object": object,
		"start":  strconv.FormatInt(start, 10),
		"length": strconv.FormatInt(length, 10),
	}
	cache.lock.RLock()
	if !drivers.IsValidBucket(bucket) {
		cache.lock.RUnlock()
		return 0, iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, errParams)
	}
	if !drivers.IsValidObjectName(object) {
		cache.lock.RUnlock()
		return 0, iodine.New(drivers.ObjectNameInvalid{Object: object}, errParams)
	}
	if start < 0 {
		return 0, iodine.New(drivers.InvalidRange{
			Start:  start,
			Length: length,
		}, errParams)
	}
	objectKey := bucket + "/" + object
	data, ok := cache.objects.Get(objectKey)
	if !ok {
		cache.lock.RUnlock()
		return 0, iodine.New(drivers.ObjectNotFound{Bucket: bucket, Object: object}, errParams)
	}
	written, err := io.CopyN(w, bytes.NewBuffer(data[start:]), length)
	cache.lock.RUnlock()
	return written, iodine.New(err, nil)
}

// GetBucketMetadata -
func (cache *cacheDriver) GetBucketMetadata(bucket string) (drivers.BucketMetadata, error) {
	cache.lock.RLock()
	defer cache.lock.RUnlock()
	if !drivers.IsValidBucket(bucket) {
		return drivers.BucketMetadata{}, iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, nil)
	}
	if _, ok := cache.storedBuckets[bucket]; ok == false {
		return drivers.BucketMetadata{}, iodine.New(drivers.BucketNotFound{Bucket: bucket}, nil)
	}
	return cache.storedBuckets[bucket].bucketMetadata, nil
}

// SetBucketMetadata -
func (cache *cacheDriver) SetBucketMetadata(bucket, acl string) error {
	cache.lock.RLock()
	if !drivers.IsValidBucket(bucket) {
		cache.lock.RUnlock()
		return iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, nil)
	}
	if _, ok := cache.storedBuckets[bucket]; ok == false {
		cache.lock.RUnlock()
		return iodine.New(drivers.BucketNotFound{Bucket: bucket}, nil)
	}
	if strings.TrimSpace(acl) == "" {
		acl = "private"
	}
	cache.lock.RUnlock()
	cache.lock.Lock()
	defer cache.lock.Unlock()
	storedBucket := cache.storedBuckets[bucket]
	storedBucket.bucketMetadata.ACL = drivers.BucketACL(acl)
	cache.storedBuckets[bucket] = storedBucket
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

func (cache *cacheDriver) CreateObject(bucket, key, contentType, expectedMD5Sum string, size int64, data io.Reader) (string, error) {
	if size > int64(cache.maxSize) {
		generic := drivers.GenericObjectError{Bucket: bucket, Object: key}
		return "", iodine.New(drivers.EntityTooLarge{
			GenericObjectError: generic,
			Size:               strconv.FormatInt(size, 10),
			MaxSize:            strconv.FormatUint(cache.maxSize, 10),
		}, nil)
	}
	md5sum, err := cache.createObject(bucket, key, contentType, expectedMD5Sum, size, data)
	// free
	debug.FreeOSMemory()
	return md5sum, iodine.New(err, nil)
}

// createObject - PUT object to cache buffer
func (cache *cacheDriver) createObject(bucket, key, contentType, expectedMD5Sum string, size int64, data io.Reader) (string, error) {
	cache.lock.RLock()
	if !drivers.IsValidBucket(bucket) {
		cache.lock.RUnlock()
		return "", iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, nil)
	}
	if !drivers.IsValidObjectName(key) {
		cache.lock.RUnlock()
		return "", iodine.New(drivers.ObjectNameInvalid{Object: key}, nil)
	}
	if _, ok := cache.storedBuckets[bucket]; ok == false {
		cache.lock.RUnlock()
		return "", iodine.New(drivers.BucketNotFound{Bucket: bucket}, nil)
	}
	storedBucket := cache.storedBuckets[bucket]
	// get object key
	objectKey := bucket + "/" + key
	if _, ok := storedBucket.objectMetadata[objectKey]; ok == true {
		cache.lock.RUnlock()
		return "", iodine.New(drivers.ObjectExists{Bucket: bucket, Object: key}, nil)
	}
	cache.lock.RUnlock()

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

	// calculate md5
	hash := md5.New()
	var readBytes []byte

	var err error
	var length int
	for err == nil {
		byteBuffer := make([]byte, 1024*1024)
		length, err = data.Read(byteBuffer)
		// While hash.Write() wouldn't mind a Nil byteBuffer
		// It is necessary for us to verify this and break
		if length == 0 {
			break
		}
		hash.Write(byteBuffer[0:length])
		readBytes = append(readBytes, byteBuffer[0:length]...)
	}
	if err != io.EOF {
		return "", iodine.New(err, nil)
	}
	md5SumBytes := hash.Sum(nil)
	totalLength := len(readBytes)

	cache.lock.Lock()
	ok := cache.objects.Set(objectKey, readBytes)
	// setting up for de-allocation
	readBytes = nil
	go debug.FreeOSMemory()
	cache.lock.Unlock()
	if !ok {
		return "", iodine.New(drivers.InternalError{}, nil)
	}

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

	cache.lock.Lock()
	storedBucket.objectMetadata[objectKey] = newObject
	cache.storedBuckets[bucket] = storedBucket
	cache.lock.Unlock()
	return newObject.Md5, nil
}

// CreateBucket - create bucket in cache
func (cache *cacheDriver) CreateBucket(bucketName, acl string) error {
	cache.lock.RLock()
	if len(cache.storedBuckets) == totalBuckets {
		cache.lock.RUnlock()
		return iodine.New(drivers.TooManyBuckets{Bucket: bucketName}, nil)
	}
	if !drivers.IsValidBucket(bucketName) {
		cache.lock.RUnlock()
		return iodine.New(drivers.BucketNameInvalid{Bucket: bucketName}, nil)
	}
	if !drivers.IsValidBucketACL(acl) {
		cache.lock.RUnlock()
		return iodine.New(drivers.InvalidACL{ACL: acl}, nil)
	}
	if _, ok := cache.storedBuckets[bucketName]; ok == true {
		cache.lock.RUnlock()
		return iodine.New(drivers.BucketExists{Bucket: bucketName}, nil)
	}
	cache.lock.RUnlock()

	if strings.TrimSpace(acl) == "" {
		// default is private
		acl = "private"
	}
	var newBucket = storedBucket{}
	newBucket.objectMetadata = make(map[string]drivers.ObjectMetadata)
	newBucket.multiPartSession = make(map[string]multiPartSession)
	newBucket.partMetadata = make(map[string]drivers.PartMetadata)
	newBucket.bucketMetadata = drivers.BucketMetadata{}
	newBucket.bucketMetadata.Name = bucketName
	newBucket.bucketMetadata.Created = time.Now().UTC()
	newBucket.bucketMetadata.ACL = drivers.BucketACL(acl)
	cache.lock.Lock()
	cache.storedBuckets[bucketName] = newBucket
	cache.lock.Unlock()
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

func (cache *cacheDriver) filterDelimiterPrefix(keys []string, key, delim string, r drivers.BucketResourcesMetadata) ([]string, drivers.BucketResourcesMetadata) {
	switch true {
	case key == r.Prefix:
		keys = appendUniq(keys, key)
	// delim - requires r.Prefix as it was trimmed off earlier
	case key == r.Prefix+delim:
		keys = appendUniq(keys, key)
	case delim != "":
		r.CommonPrefixes = appendUniq(r.CommonPrefixes, r.Prefix+delim)
	}
	return keys, r
}

func (cache *cacheDriver) listObjects(keys []string, key string, r drivers.BucketResourcesMetadata) ([]string, drivers.BucketResourcesMetadata) {
	switch true {
	// Prefix absent, delimit object key based on delimiter
	case r.IsDelimiterSet():
		delim := delimiter(key, r.Delimiter)
		switch true {
		case delim == "" || delim == key:
			keys = appendUniq(keys, key)
		case delim != "":
			r.CommonPrefixes = appendUniq(r.CommonPrefixes, delim)
		}
	// Prefix present, delimit object key with prefix key based on delimiter
	case r.IsDelimiterPrefixSet():
		if strings.HasPrefix(key, r.Prefix) {
			trimmedName := strings.TrimPrefix(key, r.Prefix)
			delim := delimiter(trimmedName, r.Delimiter)
			keys, r = cache.filterDelimiterPrefix(keys, key, delim, r)
		}
	// Prefix present, nothing to delimit
	case r.IsPrefixSet():
		keys = appendUniq(keys, key)
	// Prefix and delimiter absent
	case r.IsDefault():
		keys = appendUniq(keys, key)
	}
	return keys, r
}

// ListObjects - list objects from cache
func (cache *cacheDriver) ListObjects(bucket string, resources drivers.BucketResourcesMetadata) ([]drivers.ObjectMetadata, drivers.BucketResourcesMetadata, error) {
	cache.lock.RLock()
	defer cache.lock.RUnlock()
	if !drivers.IsValidBucket(bucket) {
		return nil, drivers.BucketResourcesMetadata{IsTruncated: false}, iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, nil)
	}
	if !drivers.IsValidObjectName(resources.Prefix) {
		return nil, drivers.BucketResourcesMetadata{IsTruncated: false}, iodine.New(drivers.ObjectNameInvalid{Object: resources.Prefix}, nil)
	}
	if _, ok := cache.storedBuckets[bucket]; ok == false {
		return nil, drivers.BucketResourcesMetadata{IsTruncated: false}, iodine.New(drivers.BucketNotFound{Bucket: bucket}, nil)
	}
	var results []drivers.ObjectMetadata
	var keys []string
	storedBucket := cache.storedBuckets[bucket]
	for key := range storedBucket.objectMetadata {
		if strings.HasPrefix(key, bucket+"/") {
			key = key[len(bucket)+1:]
			keys, resources = cache.listObjects(keys, key, resources)
		}
	}
	var newKeys []string
	switch {
	case resources.Marker != "":
		for _, key := range keys {
			if key > resources.Marker {
				newKeys = appendUniq(newKeys, key)
			}
		}
	default:
		newKeys = keys
	}
	sort.Strings(newKeys)
	for _, key := range newKeys {
		if len(results) == resources.Maxkeys {
			resources.IsTruncated = true
			if resources.IsTruncated && resources.IsDelimiterSet() {
				resources.NextMarker = results[len(results)-1].Key
			}
			return results, resources, nil
		}
		object := storedBucket.objectMetadata[bucket+"/"+key]
		results = append(results, object)
	}
	return results, resources, nil
}

// byBucketName is a type for sorting bucket metadata by bucket name
type byBucketName []drivers.BucketMetadata

func (b byBucketName) Len() int           { return len(b) }
func (b byBucketName) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byBucketName) Less(i, j int) bool { return b[i].Name < b[j].Name }

// ListBuckets - List buckets from cache
func (cache *cacheDriver) ListBuckets() ([]drivers.BucketMetadata, error) {
	cache.lock.RLock()
	defer cache.lock.RUnlock()
	var results []drivers.BucketMetadata
	for _, bucket := range cache.storedBuckets {
		results = append(results, bucket.bucketMetadata)
	}
	sort.Sort(byBucketName(results))
	return results, nil
}

// GetObjectMetadata - get object metadata from cache
func (cache *cacheDriver) GetObjectMetadata(bucket, key string) (drivers.ObjectMetadata, error) {
	cache.lock.RLock()
	defer cache.lock.RUnlock()
	// check if bucket exists
	if !drivers.IsValidBucket(bucket) {
		return drivers.ObjectMetadata{}, iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, nil)
	}
	if !drivers.IsValidObjectName(key) {
		return drivers.ObjectMetadata{}, iodine.New(drivers.ObjectNameInvalid{Object: key}, nil)
	}
	if _, ok := cache.storedBuckets[bucket]; ok == false {
		return drivers.ObjectMetadata{}, iodine.New(drivers.BucketNotFound{Bucket: bucket}, nil)
	}
	storedBucket := cache.storedBuckets[bucket]
	objectKey := bucket + "/" + key
	if object, ok := storedBucket.objectMetadata[objectKey]; ok == true {
		return object, nil
	}
	return drivers.ObjectMetadata{}, iodine.New(drivers.ObjectNotFound{Bucket: bucket, Object: key}, nil)
}

func (cache *cacheDriver) expiredObject(a ...interface{}) {
	cacheStats := cache.objects.Stats()
	log.Printf("CurrentSize: %d, CurrentItems: %d, TotalExpirations: %d",
		cacheStats.Bytes, cacheStats.Items, cacheStats.Expired)
	key := a[0].(string)
	// loop through all buckets
	for bucket, storedBucket := range cache.storedBuckets {
		delete(storedBucket.objectMetadata, key)
		// remove bucket if no objects found anymore
		if len(storedBucket.objectMetadata) == 0 {
			if time.Since(cache.storedBuckets[bucket].bucketMetadata.Created) > cache.expiration {
				delete(cache.storedBuckets, bucket)
			}
		}
	}
	debug.FreeOSMemory()
}
