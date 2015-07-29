/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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

package donut

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"io"
	"io/ioutil"
	"log"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio/pkg/crypto/sha256"
	"github.com/minio/minio/pkg/donut/cache/data"
	"github.com/minio/minio/pkg/donut/cache/metadata"
	"github.com/minio/minio/pkg/iodine"
	"github.com/minio/minio/pkg/quick"
)

// total Number of buckets allowed
const (
	totalBuckets = 100
)

// Config donut config
type Config struct {
	Version     string              `json:"version"`
	MaxSize     uint64              `json:"max-size"`
	DonutName   string              `json:"donut-name"`
	NodeDiskMap map[string][]string `json:"node-disk-map"`
}

// API - local variables
type API struct {
	config           *Config
	lock             *sync.Mutex
	objects          *data.Cache
	multiPartObjects map[string]*data.Cache
	storedBuckets    *metadata.Cache
	nodes            map[string]node
	buckets          map[string]bucket
}

// storedBucket saved bucket
type storedBucket struct {
	bucketMetadata   BucketMetadata
	objectMetadata   map[string]ObjectMetadata
	partMetadata     map[string]map[int]PartMetadata
	multiPartSession map[string]MultiPartSession
}

// New instantiate a new donut
func New() (Interface, error) {
	var conf *Config
	var err error
	conf, err = LoadConfig()
	if err != nil {
		conf = &Config{
			Version:     "0.0.1",
			MaxSize:     512000000,
			NodeDiskMap: nil,
			DonutName:   "",
		}
		if err := quick.CheckData(conf); err != nil {
			return nil, iodine.New(err, nil)
		}
	}
	a := API{config: conf}
	a.storedBuckets = metadata.NewCache()
	a.nodes = make(map[string]node)
	a.buckets = make(map[string]bucket)
	a.objects = data.NewCache(a.config.MaxSize)
	a.multiPartObjects = make(map[string]*data.Cache)
	a.objects.OnEvicted = a.evictedObject
	a.lock = new(sync.Mutex)

	if len(a.config.NodeDiskMap) > 0 {
		for k, v := range a.config.NodeDiskMap {
			if len(v) == 0 {
				return nil, iodine.New(InvalidDisksArgument{}, nil)
			}
			err := a.AttachNode(k, v)
			if err != nil {
				return nil, iodine.New(err, nil)
			}
		}
		/// Initialization, populate all buckets into memory
		buckets, err := a.listBuckets()
		if err != nil {
			return nil, iodine.New(err, nil)
		}
		for k, v := range buckets {
			var newBucket = storedBucket{}
			newBucket.bucketMetadata = v
			newBucket.objectMetadata = make(map[string]ObjectMetadata)
			newBucket.multiPartSession = make(map[string]MultiPartSession)
			newBucket.partMetadata = make(map[string]map[int]PartMetadata)
			a.storedBuckets.Set(k, newBucket)
		}
		a.Heal()
	}
	return a, nil
}

/// V2 API functions

// GetObject - GET object from cache buffer
func (donut API) GetObject(w io.Writer, bucket string, object string, start, length int64) (int64, error) {
	donut.lock.Lock()
	defer donut.lock.Unlock()

	errParams := map[string]string{
		"bucket": bucket,
		"object": object,
		"start":  strconv.FormatInt(start, 10),
		"length": strconv.FormatInt(length, 10),
	}

	if !IsValidBucket(bucket) {
		return 0, iodine.New(BucketNameInvalid{Bucket: bucket}, errParams)
	}
	if !IsValidObjectName(object) {
		return 0, iodine.New(ObjectNameInvalid{Object: object}, errParams)
	}
	if start < 0 {
		return 0, iodine.New(InvalidRange{
			Start:  start,
			Length: length,
		}, errParams)
	}
	if !donut.storedBuckets.Exists(bucket) {
		return 0, iodine.New(BucketNotFound{Bucket: bucket}, errParams)
	}
	objectKey := bucket + "/" + object
	data, ok := donut.objects.Get(objectKey)
	var written int64
	var err error
	if !ok {
		if len(donut.config.NodeDiskMap) > 0 {
			reader, size, err := donut.getObject(bucket, object)
			if err != nil {
				return 0, iodine.New(err, nil)
			}
			if start > 0 {
				if _, err := io.CopyN(ioutil.Discard, reader, start); err != nil {
					return 0, iodine.New(err, errParams)
				}
			}
			// new proxy writer to capture data read from disk
			pw := NewProxyWriter(w)
			if length > 0 {
				written, err = io.CopyN(pw, reader, length)
				if err != nil {
					return 0, iodine.New(err, errParams)
				}
			} else {
				written, err = io.CopyN(pw, reader, size)
				if err != nil {
					return 0, iodine.New(err, errParams)
				}
			}
			/// cache object read from disk
			ok := donut.objects.Append(objectKey, pw.writtenBytes)
			pw.writtenBytes = nil
			go debug.FreeOSMemory()
			if !ok {
				return 0, iodine.New(InternalError{}, errParams)
			}
			return written, nil
		}
		return 0, iodine.New(ObjectNotFound{Object: object}, errParams)
	}
	if start == 0 && length == 0 {
		written, err = io.CopyN(w, bytes.NewBuffer(data), int64(donut.objects.Len(objectKey)))
		if err != nil {
			return 0, iodine.New(err, nil)
		}
	} else {
		written, err = io.CopyN(w, bytes.NewBuffer(data[start:]), length)
		if err != nil {
			return 0, iodine.New(err, nil)
		}
	}
	return written, nil
}

// GetBucketMetadata -
func (donut API) GetBucketMetadata(bucket string, signature *Signature) (BucketMetadata, error) {
	donut.lock.Lock()
	defer donut.lock.Unlock()

	if signature != nil {
		ok, err := signature.DoesSignatureMatch("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
		if err != nil {
			return BucketMetadata{}, iodine.New(err, nil)
		}
		if !ok {
			return BucketMetadata{}, iodine.New(SignatureDoesNotMatch{}, nil)
		}
	}

	if !IsValidBucket(bucket) {
		return BucketMetadata{}, iodine.New(BucketNameInvalid{Bucket: bucket}, nil)
	}
	if !donut.storedBuckets.Exists(bucket) {
		if len(donut.config.NodeDiskMap) > 0 {
			bucketMetadata, err := donut.getBucketMetadata(bucket)
			if err != nil {
				return BucketMetadata{}, iodine.New(err, nil)
			}
			storedBucket := donut.storedBuckets.Get(bucket).(storedBucket)
			storedBucket.bucketMetadata = bucketMetadata
			donut.storedBuckets.Set(bucket, storedBucket)
		}
		return BucketMetadata{}, iodine.New(BucketNotFound{Bucket: bucket}, nil)
	}
	return donut.storedBuckets.Get(bucket).(storedBucket).bucketMetadata, nil
}

// SetBucketMetadata -
func (donut API) SetBucketMetadata(bucket string, metadata map[string]string, signature *Signature) error {
	donut.lock.Lock()
	defer donut.lock.Unlock()

	if signature != nil {
		ok, err := signature.DoesSignatureMatch("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
		if err != nil {
			return iodine.New(err, nil)
		}
		if !ok {
			return iodine.New(SignatureDoesNotMatch{}, nil)
		}
	}

	if !IsValidBucket(bucket) {
		return iodine.New(BucketNameInvalid{Bucket: bucket}, nil)
	}
	if !donut.storedBuckets.Exists(bucket) {
		return iodine.New(BucketNotFound{Bucket: bucket}, nil)
	}
	if len(donut.config.NodeDiskMap) > 0 {
		if err := donut.setBucketMetadata(bucket, metadata); err != nil {
			return iodine.New(err, nil)
		}
	}
	storedBucket := donut.storedBuckets.Get(bucket).(storedBucket)
	storedBucket.bucketMetadata.ACL = BucketACL(metadata["acl"])
	donut.storedBuckets.Set(bucket, storedBucket)
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
			return iodine.New(BadDigest{}, nil)
		}
		return nil
	}
	return iodine.New(InvalidArgument{}, nil)
}

// CreateObject - create an object
func (donut API) CreateObject(bucket, key, expectedMD5Sum string, size int64, data io.Reader, metadata map[string]string, signature *Signature) (ObjectMetadata, error) {
	donut.lock.Lock()
	defer donut.lock.Unlock()

	contentType := metadata["contentType"]
	objectMetadata, err := donut.createObject(bucket, key, contentType, expectedMD5Sum, size, data, signature)
	// free
	debug.FreeOSMemory()

	return objectMetadata, iodine.New(err, nil)
}

// createObject - PUT object to cache buffer
func (donut API) createObject(bucket, key, contentType, expectedMD5Sum string, size int64, data io.Reader, signature *Signature) (ObjectMetadata, error) {
	if len(donut.config.NodeDiskMap) == 0 {
		if size > int64(donut.config.MaxSize) {
			generic := GenericObjectError{Bucket: bucket, Object: key}
			return ObjectMetadata{}, iodine.New(EntityTooLarge{
				GenericObjectError: generic,
				Size:               strconv.FormatInt(size, 10),
				MaxSize:            strconv.FormatUint(donut.config.MaxSize, 10),
			}, nil)
		}
	}
	if !IsValidBucket(bucket) {
		return ObjectMetadata{}, iodine.New(BucketNameInvalid{Bucket: bucket}, nil)
	}
	if !IsValidObjectName(key) {
		return ObjectMetadata{}, iodine.New(ObjectNameInvalid{Object: key}, nil)
	}
	if !donut.storedBuckets.Exists(bucket) {
		return ObjectMetadata{}, iodine.New(BucketNotFound{Bucket: bucket}, nil)
	}
	storedBucket := donut.storedBuckets.Get(bucket).(storedBucket)
	// get object key
	objectKey := bucket + "/" + key
	if _, ok := storedBucket.objectMetadata[objectKey]; ok == true {
		return ObjectMetadata{}, iodine.New(ObjectExists{Object: key}, nil)
	}

	if contentType == "" {
		contentType = "application/octet-stream"
	}
	contentType = strings.TrimSpace(contentType)
	if strings.TrimSpace(expectedMD5Sum) != "" {
		expectedMD5SumBytes, err := base64.StdEncoding.DecodeString(strings.TrimSpace(expectedMD5Sum))
		if err != nil {
			// pro-actively close the connection
			return ObjectMetadata{}, iodine.New(InvalidDigest{Md5: expectedMD5Sum}, nil)
		}
		expectedMD5Sum = hex.EncodeToString(expectedMD5SumBytes)
	}

	if len(donut.config.NodeDiskMap) > 0 {
		objMetadata, err := donut.putObject(
			bucket,
			key,
			expectedMD5Sum,
			data,
			size,
			map[string]string{
				"contentType":   contentType,
				"contentLength": strconv.FormatInt(size, 10),
			},
			signature,
		)
		if err != nil {
			return ObjectMetadata{}, iodine.New(err, nil)
		}
		storedBucket.objectMetadata[objectKey] = objMetadata
		donut.storedBuckets.Set(bucket, storedBucket)
		return objMetadata, nil
	}
	// calculate md5
	hash := md5.New()
	sha256hash := sha256.New()

	var err error
	var totalLength int64
	for err == nil {
		var length int
		byteBuffer := make([]byte, 1024*1024)
		length, err = data.Read(byteBuffer)
		hash.Write(byteBuffer[0:length])
		sha256hash.Write(byteBuffer[0:length])
		ok := donut.objects.Append(objectKey, byteBuffer[0:length])
		if !ok {
			return ObjectMetadata{}, iodine.New(InternalError{}, nil)
		}
		totalLength += int64(length)
		go debug.FreeOSMemory()
	}
	if totalLength != size {
		// Delete perhaps the object is already saved, due to the nature of append()
		donut.objects.Delete(objectKey)
		return ObjectMetadata{}, iodine.New(IncompleteBody{Bucket: bucket, Object: key}, nil)
	}
	if err != io.EOF {
		return ObjectMetadata{}, iodine.New(err, nil)
	}
	md5SumBytes := hash.Sum(nil)
	md5Sum := hex.EncodeToString(md5SumBytes)
	// Verify if the written object is equal to what is expected, only if it is requested as such
	if strings.TrimSpace(expectedMD5Sum) != "" {
		if err := isMD5SumEqual(strings.TrimSpace(expectedMD5Sum), md5Sum); err != nil {
			return ObjectMetadata{}, iodine.New(BadDigest{}, nil)
		}
	}
	if signature != nil {
		ok, err := signature.DoesSignatureMatch(hex.EncodeToString(sha256hash.Sum(nil)))
		if err != nil {
			return ObjectMetadata{}, iodine.New(err, nil)
		}
		if !ok {
			return ObjectMetadata{}, iodine.New(SignatureDoesNotMatch{}, nil)
		}
	}

	m := make(map[string]string)
	m["contentType"] = contentType
	newObject := ObjectMetadata{
		Bucket: bucket,
		Object: key,

		Metadata: m,
		Created:  time.Now().UTC(),
		MD5Sum:   md5Sum,
		Size:     int64(totalLength),
	}

	storedBucket.objectMetadata[objectKey] = newObject
	donut.storedBuckets.Set(bucket, storedBucket)
	return newObject, nil
}

// MakeBucket - create bucket in cache
func (donut API) MakeBucket(bucketName, acl string, location io.Reader, signature *Signature) error {
	donut.lock.Lock()
	defer donut.lock.Unlock()

	// do not have to parse location constraint, using this just for signature verification
	locationSum := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	if location != nil {
		locationConstraintBytes, err := ioutil.ReadAll(location)
		if err != nil {
			return iodine.New(InternalError{}, nil)
		}
		locationSum = hex.EncodeToString(sha256.Sum256(locationConstraintBytes)[:])
	}

	if signature != nil {
		ok, err := signature.DoesSignatureMatch(locationSum)
		if err != nil {
			return iodine.New(err, nil)
		}
		if !ok {
			return iodine.New(SignatureDoesNotMatch{}, nil)
		}
	}

	if donut.storedBuckets.Stats().Items == totalBuckets {
		return iodine.New(TooManyBuckets{Bucket: bucketName}, nil)
	}
	if !IsValidBucket(bucketName) {
		return iodine.New(BucketNameInvalid{Bucket: bucketName}, nil)
	}
	if !IsValidBucketACL(acl) {
		return iodine.New(InvalidACL{ACL: acl}, nil)
	}
	if donut.storedBuckets.Exists(bucketName) {
		return iodine.New(BucketExists{Bucket: bucketName}, nil)
	}

	if strings.TrimSpace(acl) == "" {
		// default is private
		acl = "private"
	}
	if len(donut.config.NodeDiskMap) > 0 {
		if err := donut.makeBucket(bucketName, BucketACL(acl)); err != nil {
			return iodine.New(err, nil)
		}
	}
	var newBucket = storedBucket{}
	newBucket.objectMetadata = make(map[string]ObjectMetadata)
	newBucket.multiPartSession = make(map[string]MultiPartSession)
	newBucket.partMetadata = make(map[string]map[int]PartMetadata)
	newBucket.bucketMetadata = BucketMetadata{}
	newBucket.bucketMetadata.Name = bucketName
	newBucket.bucketMetadata.Created = time.Now().UTC()
	newBucket.bucketMetadata.ACL = BucketACL(acl)
	donut.storedBuckets.Set(bucketName, newBucket)
	return nil
}

// ListObjects - list objects from cache
func (donut API) ListObjects(bucket string, resources BucketResourcesMetadata, signature *Signature) ([]ObjectMetadata, BucketResourcesMetadata, error) {
	donut.lock.Lock()
	defer donut.lock.Unlock()

	if signature != nil {
		ok, err := signature.DoesSignatureMatch("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
		if err != nil {
			return nil, BucketResourcesMetadata{}, iodine.New(err, nil)
		}
		if !ok {
			return nil, BucketResourcesMetadata{}, iodine.New(SignatureDoesNotMatch{}, nil)
		}
	}

	if !IsValidBucket(bucket) {
		return nil, BucketResourcesMetadata{IsTruncated: false}, iodine.New(BucketNameInvalid{Bucket: bucket}, nil)
	}
	if !IsValidPrefix(resources.Prefix) {
		return nil, BucketResourcesMetadata{IsTruncated: false}, iodine.New(ObjectNameInvalid{Object: resources.Prefix}, nil)
	}
	if !donut.storedBuckets.Exists(bucket) {
		return nil, BucketResourcesMetadata{IsTruncated: false}, iodine.New(BucketNotFound{Bucket: bucket}, nil)
	}
	var results []ObjectMetadata
	var keys []string
	if len(donut.config.NodeDiskMap) > 0 {
		listObjects, err := donut.listObjects(
			bucket,
			resources.Prefix,
			resources.Marker,
			resources.Delimiter,
			resources.Maxkeys,
		)
		if err != nil {
			return nil, BucketResourcesMetadata{IsTruncated: false}, iodine.New(err, nil)
		}
		resources.CommonPrefixes = listObjects.CommonPrefixes
		resources.IsTruncated = listObjects.IsTruncated
		for key := range listObjects.Objects {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			results = append(results, listObjects.Objects[key])
		}
		if resources.IsTruncated && resources.Delimiter != "" {
			resources.NextMarker = results[len(results)-1].Object
		}
		return results, resources, nil
	}
	storedBucket := donut.storedBuckets.Get(bucket).(storedBucket)
	for key := range storedBucket.objectMetadata {
		if strings.HasPrefix(key, bucket+"/") {
			key = key[len(bucket)+1:]
			if strings.HasPrefix(key, resources.Prefix) {
				if key > resources.Marker {
					keys = append(keys, key)
				}
			}
		}
	}
	if strings.TrimSpace(resources.Prefix) != "" {
		keys = TrimPrefix(keys, resources.Prefix)
	}
	var prefixes []string
	var filteredKeys []string
	filteredKeys = keys
	if strings.TrimSpace(resources.Delimiter) != "" {
		filteredKeys = HasNoDelimiter(keys, resources.Delimiter)
		prefixes = HasDelimiter(keys, resources.Delimiter)
		prefixes = SplitDelimiter(prefixes, resources.Delimiter)
		prefixes = SortUnique(prefixes)
	}
	for _, commonPrefix := range prefixes {
		resources.CommonPrefixes = append(resources.CommonPrefixes, resources.Prefix+commonPrefix)
	}
	filteredKeys = RemoveDuplicates(filteredKeys)
	sort.Strings(filteredKeys)

	for _, key := range filteredKeys {
		if len(results) == resources.Maxkeys {
			resources.IsTruncated = true
			if resources.IsTruncated && resources.Delimiter != "" {
				resources.NextMarker = results[len(results)-1].Object
			}
			return results, resources, nil
		}
		object := storedBucket.objectMetadata[bucket+"/"+resources.Prefix+key]
		results = append(results, object)
	}
	resources.CommonPrefixes = RemoveDuplicates(resources.CommonPrefixes)
	sort.Strings(resources.CommonPrefixes)
	return results, resources, nil
}

// byBucketName is a type for sorting bucket metadata by bucket name
type byBucketName []BucketMetadata

func (b byBucketName) Len() int           { return len(b) }
func (b byBucketName) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byBucketName) Less(i, j int) bool { return b[i].Name < b[j].Name }

// ListBuckets - List buckets from cache
func (donut API) ListBuckets(signature *Signature) ([]BucketMetadata, error) {
	donut.lock.Lock()
	defer donut.lock.Unlock()

	if signature != nil {
		ok, err := signature.DoesSignatureMatch("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
		if err != nil {
			return nil, iodine.New(err, nil)
		}
		if !ok {
			return nil, iodine.New(SignatureDoesNotMatch{}, nil)
		}
	}

	var results []BucketMetadata
	if len(donut.config.NodeDiskMap) > 0 {
		buckets, err := donut.listBuckets()
		if err != nil {
			return nil, iodine.New(err, nil)
		}
		for _, bucketMetadata := range buckets {
			results = append(results, bucketMetadata)
		}
		sort.Sort(byBucketName(results))
		return results, nil
	}
	for _, bucket := range donut.storedBuckets.GetAll() {
		results = append(results, bucket.(storedBucket).bucketMetadata)
	}
	sort.Sort(byBucketName(results))
	return results, nil
}

// GetObjectMetadata - get object metadata from cache
func (donut API) GetObjectMetadata(bucket, key string, signature *Signature) (ObjectMetadata, error) {
	donut.lock.Lock()
	defer donut.lock.Unlock()

	if signature != nil {
		ok, err := signature.DoesSignatureMatch("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
		if err != nil {
			return ObjectMetadata{}, iodine.New(err, nil)
		}
		if !ok {
			return ObjectMetadata{}, iodine.New(SignatureDoesNotMatch{}, nil)
		}
	}

	// check if bucket exists
	if !IsValidBucket(bucket) {
		return ObjectMetadata{}, iodine.New(BucketNameInvalid{Bucket: bucket}, nil)
	}
	if !IsValidObjectName(key) {
		return ObjectMetadata{}, iodine.New(ObjectNameInvalid{Object: key}, nil)
	}
	if !donut.storedBuckets.Exists(bucket) {
		return ObjectMetadata{}, iodine.New(BucketNotFound{Bucket: bucket}, nil)
	}
	storedBucket := donut.storedBuckets.Get(bucket).(storedBucket)
	objectKey := bucket + "/" + key
	if objMetadata, ok := storedBucket.objectMetadata[objectKey]; ok == true {
		return objMetadata, nil
	}
	if len(donut.config.NodeDiskMap) > 0 {
		objMetadata, err := donut.getObjectMetadata(bucket, key)
		if err != nil {
			return ObjectMetadata{}, iodine.New(err, nil)
		}
		// update
		storedBucket.objectMetadata[objectKey] = objMetadata
		donut.storedBuckets.Set(bucket, storedBucket)
		return objMetadata, nil
	}
	return ObjectMetadata{}, iodine.New(ObjectNotFound{Object: key}, nil)
}

// evictedObject callback function called when an item is evicted from memory
func (donut API) evictedObject(a ...interface{}) {
	cacheStats := donut.objects.Stats()
	log.Printf("CurrentSize: %d, CurrentItems: %d, TotalEvicted: %d",
		cacheStats.Bytes, cacheStats.Items, cacheStats.Evicted)
	key := a[0].(string)
	// loop through all buckets
	for _, bucket := range donut.storedBuckets.GetAll() {
		delete(bucket.(storedBucket).objectMetadata, key)
	}
	debug.FreeOSMemory()
}
