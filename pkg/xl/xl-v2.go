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

package xl

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
	"github.com/minio/minio/pkg/probe"
	"github.com/minio/minio/pkg/quick"
	signV4 "github.com/minio/minio/pkg/signature"
	"github.com/minio/minio/pkg/xl/cache/data"
	"github.com/minio/minio/pkg/xl/cache/metadata"
)

// total Number of buckets allowed
const (
	totalBuckets = 100
)

// Config xl config
type Config struct {
	Version     string              `json:"version"`
	MaxSize     uint64              `json:"max-size"`
	XLName      string              `json:"xl-name"`
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

// New instantiate a new xl
func New() (Interface, *probe.Error) {
	var conf *Config
	var err *probe.Error
	conf, err = LoadConfig()
	if err != nil {
		conf = &Config{
			Version:     "0.0.1",
			MaxSize:     512000000,
			NodeDiskMap: nil,
			XLName:      "",
		}
		if err := quick.CheckData(conf); err != nil {
			return nil, err.Trace()
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
				return nil, probe.NewError(InvalidDisksArgument{})
			}
			err := a.AttachNode(k, v)
			if err != nil {
				return nil, err.Trace()
			}
		}
		/// Initialization, populate all buckets into memory
		buckets, err := a.listBuckets()
		if err != nil {
			return nil, err.Trace()
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
func (xl API) GetObject(w io.Writer, bucket string, object string, start, length int64) (int64, *probe.Error) {
	xl.lock.Lock()
	defer xl.lock.Unlock()

	if !IsValidBucket(bucket) {
		return 0, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(object) {
		return 0, probe.NewError(ObjectNameInvalid{Object: object})
	}
	if start < 0 {
		return 0, probe.NewError(InvalidRange{
			Start:  start,
			Length: length,
		})
	}
	if !xl.storedBuckets.Exists(bucket) {
		return 0, probe.NewError(BucketNotFound{Bucket: bucket})
	}
	objectKey := bucket + "/" + object
	data, ok := xl.objects.Get(objectKey)
	var written int64
	if !ok {
		if len(xl.config.NodeDiskMap) > 0 {
			reader, size, err := xl.getObject(bucket, object)
			if err != nil {
				return 0, err.Trace()
			}
			if start > 0 {
				if _, err := io.CopyN(ioutil.Discard, reader, start); err != nil {
					return 0, probe.NewError(err)
				}
			}
			// new proxy writer to capture data read from disk
			pw := NewProxyWriter(w)
			{
				var err error
				if length > 0 {
					written, err = io.CopyN(pw, reader, length)
					if err != nil {
						return 0, probe.NewError(err)
					}
				} else {
					written, err = io.CopyN(pw, reader, size)
					if err != nil {
						return 0, probe.NewError(err)
					}
				}
			}
			/// cache object read from disk
			ok := xl.objects.Append(objectKey, pw.writtenBytes)
			pw.writtenBytes = nil
			go debug.FreeOSMemory()
			if !ok {
				return 0, probe.NewError(InternalError{})
			}
			return written, nil
		}
		return 0, probe.NewError(ObjectNotFound{Object: object})
	}
	var err error
	if start == 0 && length == 0 {
		written, err = io.CopyN(w, bytes.NewBuffer(data), int64(xl.objects.Len(objectKey)))
		if err != nil {
			return 0, probe.NewError(err)
		}
		return written, nil
	}
	written, err = io.CopyN(w, bytes.NewBuffer(data[start:]), length)
	if err != nil {
		return 0, probe.NewError(err)
	}
	return written, nil
}

// GetBucketMetadata -
func (xl API) GetBucketMetadata(bucket string) (BucketMetadata, *probe.Error) {
	xl.lock.Lock()
	defer xl.lock.Unlock()

	if !IsValidBucket(bucket) {
		return BucketMetadata{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if !xl.storedBuckets.Exists(bucket) {
		if len(xl.config.NodeDiskMap) > 0 {
			bucketMetadata, err := xl.getBucketMetadata(bucket)
			if err != nil {
				return BucketMetadata{}, err.Trace()
			}
			storedBucket := xl.storedBuckets.Get(bucket).(storedBucket)
			storedBucket.bucketMetadata = bucketMetadata
			xl.storedBuckets.Set(bucket, storedBucket)
		}
		return BucketMetadata{}, probe.NewError(BucketNotFound{Bucket: bucket})
	}
	return xl.storedBuckets.Get(bucket).(storedBucket).bucketMetadata, nil
}

// SetBucketMetadata -
func (xl API) SetBucketMetadata(bucket string, metadata map[string]string) *probe.Error {
	xl.lock.Lock()
	defer xl.lock.Unlock()

	if !IsValidBucket(bucket) {
		return probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if !xl.storedBuckets.Exists(bucket) {
		return probe.NewError(BucketNotFound{Bucket: bucket})
	}
	if len(xl.config.NodeDiskMap) > 0 {
		if err := xl.setBucketMetadata(bucket, metadata); err != nil {
			return err.Trace()
		}
	}
	storedBucket := xl.storedBuckets.Get(bucket).(storedBucket)
	storedBucket.bucketMetadata.ACL = BucketACL(metadata["acl"])
	xl.storedBuckets.Set(bucket, storedBucket)
	return nil
}

// isMD5SumEqual - returns error if md5sum mismatches, success its `nil`
func isMD5SumEqual(expectedMD5Sum, actualMD5Sum string) *probe.Error {
	if strings.TrimSpace(expectedMD5Sum) != "" && strings.TrimSpace(actualMD5Sum) != "" {
		expectedMD5SumBytes, err := hex.DecodeString(expectedMD5Sum)
		if err != nil {
			return probe.NewError(err)
		}
		actualMD5SumBytes, err := hex.DecodeString(actualMD5Sum)
		if err != nil {
			return probe.NewError(err)
		}
		if !bytes.Equal(expectedMD5SumBytes, actualMD5SumBytes) {
			return probe.NewError(BadDigest{})
		}
		return nil
	}
	return probe.NewError(InvalidArgument{})
}

// CreateObject - create an object
func (xl API) CreateObject(bucket, key, expectedMD5Sum string, size int64, data io.Reader, metadata map[string]string, signature *signV4.Signature) (ObjectMetadata, *probe.Error) {
	xl.lock.Lock()
	defer xl.lock.Unlock()

	contentType := metadata["contentType"]
	objectMetadata, err := xl.createObject(bucket, key, contentType, expectedMD5Sum, size, data, signature)
	// free
	debug.FreeOSMemory()

	return objectMetadata, err.Trace()
}

// createObject - PUT object to cache buffer
func (xl API) createObject(bucket, key, contentType, expectedMD5Sum string, size int64, data io.Reader, signature *signV4.Signature) (ObjectMetadata, *probe.Error) {
	if len(xl.config.NodeDiskMap) == 0 {
		if size > int64(xl.config.MaxSize) {
			generic := GenericObjectError{Bucket: bucket, Object: key}
			return ObjectMetadata{}, probe.NewError(EntityTooLarge{
				GenericObjectError: generic,
				Size:               strconv.FormatInt(size, 10),
				MaxSize:            strconv.FormatUint(xl.config.MaxSize, 10),
			})
		}
	}
	if !IsValidBucket(bucket) {
		return ObjectMetadata{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(key) {
		return ObjectMetadata{}, probe.NewError(ObjectNameInvalid{Object: key})
	}
	if !xl.storedBuckets.Exists(bucket) {
		return ObjectMetadata{}, probe.NewError(BucketNotFound{Bucket: bucket})
	}
	storedBucket := xl.storedBuckets.Get(bucket).(storedBucket)
	// get object key
	objectKey := bucket + "/" + key
	if _, ok := storedBucket.objectMetadata[objectKey]; ok == true {
		return ObjectMetadata{}, probe.NewError(ObjectExists{Object: key})
	}

	if contentType == "" {
		contentType = "application/octet-stream"
	}
	contentType = strings.TrimSpace(contentType)
	if strings.TrimSpace(expectedMD5Sum) != "" {
		expectedMD5SumBytes, err := base64.StdEncoding.DecodeString(strings.TrimSpace(expectedMD5Sum))
		if err != nil {
			// pro-actively close the connection
			return ObjectMetadata{}, probe.NewError(InvalidDigest{Md5: expectedMD5Sum})
		}
		expectedMD5Sum = hex.EncodeToString(expectedMD5SumBytes)
	}

	if len(xl.config.NodeDiskMap) > 0 {
		objMetadata, err := xl.putObject(
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
			return ObjectMetadata{}, err.Trace()
		}
		storedBucket.objectMetadata[objectKey] = objMetadata
		xl.storedBuckets.Set(bucket, storedBucket)
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
		if length != 0 {
			hash.Write(byteBuffer[0:length])
			sha256hash.Write(byteBuffer[0:length])
			ok := xl.objects.Append(objectKey, byteBuffer[0:length])
			if !ok {
				return ObjectMetadata{}, probe.NewError(InternalError{})
			}
			totalLength += int64(length)
			go debug.FreeOSMemory()
		}
	}
	if size != 0 {
		if totalLength != size {
			// Delete perhaps the object is already saved, due to the nature of append()
			xl.objects.Delete(objectKey)
			return ObjectMetadata{}, probe.NewError(IncompleteBody{Bucket: bucket, Object: key})
		}
	}
	if err != io.EOF {
		return ObjectMetadata{}, probe.NewError(err)
	}
	md5SumBytes := hash.Sum(nil)
	md5Sum := hex.EncodeToString(md5SumBytes)
	// Verify if the written object is equal to what is expected, only if it is requested as such
	if strings.TrimSpace(expectedMD5Sum) != "" {
		if err := isMD5SumEqual(strings.TrimSpace(expectedMD5Sum), md5Sum); err != nil {
			// Delete perhaps the object is already saved, due to the nature of append()
			xl.objects.Delete(objectKey)
			return ObjectMetadata{}, probe.NewError(BadDigest{})
		}
	}
	if signature != nil {
		ok, err := signature.DoesSignatureMatch(hex.EncodeToString(sha256hash.Sum(nil)))
		if err != nil {
			// Delete perhaps the object is already saved, due to the nature of append()
			xl.objects.Delete(objectKey)
			return ObjectMetadata{}, err.Trace()
		}
		if !ok {
			// Delete perhaps the object is already saved, due to the nature of append()
			xl.objects.Delete(objectKey)
			return ObjectMetadata{}, probe.NewError(SignDoesNotMatch{})
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
	xl.storedBuckets.Set(bucket, storedBucket)
	return newObject, nil
}

// MakeBucket - create bucket in cache
func (xl API) MakeBucket(bucketName, acl string, location io.Reader, signature *signV4.Signature) *probe.Error {
	xl.lock.Lock()
	defer xl.lock.Unlock()

	// do not have to parse location constraint, using this just for signature verification
	locationSum := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	if location != nil {
		locationConstraintBytes, err := ioutil.ReadAll(location)
		if err != nil {
			return probe.NewError(InternalError{})
		}
		locationConstraintHashBytes := sha256.Sum256(locationConstraintBytes)
		locationSum = hex.EncodeToString(locationConstraintHashBytes[:])
	}

	if signature != nil {
		ok, err := signature.DoesSignatureMatch(locationSum)
		if err != nil {
			return err.Trace()
		}
		if !ok {
			return probe.NewError(SignDoesNotMatch{})
		}
	}

	if xl.storedBuckets.Stats().Items == totalBuckets {
		return probe.NewError(TooManyBuckets{Bucket: bucketName})
	}
	if !IsValidBucket(bucketName) {
		return probe.NewError(BucketNameInvalid{Bucket: bucketName})
	}
	if !IsValidBucketACL(acl) {
		return probe.NewError(InvalidACL{ACL: acl})
	}
	if xl.storedBuckets.Exists(bucketName) {
		return probe.NewError(BucketExists{Bucket: bucketName})
	}

	if strings.TrimSpace(acl) == "" {
		// default is private
		acl = "private"
	}
	if len(xl.config.NodeDiskMap) > 0 {
		if err := xl.makeBucket(bucketName, BucketACL(acl)); err != nil {
			return err.Trace()
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
	xl.storedBuckets.Set(bucketName, newBucket)
	return nil
}

// ListObjects - list objects from cache
func (xl API) ListObjects(bucket string, resources BucketResourcesMetadata) ([]ObjectMetadata, BucketResourcesMetadata, *probe.Error) {
	xl.lock.Lock()
	defer xl.lock.Unlock()

	if !IsValidBucket(bucket) {
		return nil, BucketResourcesMetadata{IsTruncated: false}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidPrefix(resources.Prefix) {
		return nil, BucketResourcesMetadata{IsTruncated: false}, probe.NewError(ObjectNameInvalid{Object: resources.Prefix})
	}
	if !xl.storedBuckets.Exists(bucket) {
		return nil, BucketResourcesMetadata{IsTruncated: false}, probe.NewError(BucketNotFound{Bucket: bucket})
	}
	var results []ObjectMetadata
	var keys []string
	if len(xl.config.NodeDiskMap) > 0 {
		listObjects, err := xl.listObjects(
			bucket,
			resources.Prefix,
			resources.Marker,
			resources.Delimiter,
			resources.Maxkeys,
		)
		if err != nil {
			return nil, BucketResourcesMetadata{IsTruncated: false}, err.Trace()
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
	storedBucket := xl.storedBuckets.Get(bucket).(storedBucket)
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
func (xl API) ListBuckets() ([]BucketMetadata, *probe.Error) {
	xl.lock.Lock()
	defer xl.lock.Unlock()

	var results []BucketMetadata
	if len(xl.config.NodeDiskMap) > 0 {
		buckets, err := xl.listBuckets()
		if err != nil {
			return nil, err.Trace()
		}
		for _, bucketMetadata := range buckets {
			results = append(results, bucketMetadata)
		}
		sort.Sort(byBucketName(results))
		return results, nil
	}
	for _, bucket := range xl.storedBuckets.GetAll() {
		results = append(results, bucket.(storedBucket).bucketMetadata)
	}
	sort.Sort(byBucketName(results))
	return results, nil
}

// GetObjectMetadata - get object metadata from cache
func (xl API) GetObjectMetadata(bucket, key string) (ObjectMetadata, *probe.Error) {
	xl.lock.Lock()
	defer xl.lock.Unlock()

	// check if bucket exists
	if !IsValidBucket(bucket) {
		return ObjectMetadata{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(key) {
		return ObjectMetadata{}, probe.NewError(ObjectNameInvalid{Object: key})
	}
	if !xl.storedBuckets.Exists(bucket) {
		return ObjectMetadata{}, probe.NewError(BucketNotFound{Bucket: bucket})
	}
	storedBucket := xl.storedBuckets.Get(bucket).(storedBucket)
	objectKey := bucket + "/" + key
	if objMetadata, ok := storedBucket.objectMetadata[objectKey]; ok == true {
		return objMetadata, nil
	}
	if len(xl.config.NodeDiskMap) > 0 {
		objMetadata, err := xl.getObjectMetadata(bucket, key)
		if err != nil {
			return ObjectMetadata{}, err.Trace()
		}
		// update
		storedBucket.objectMetadata[objectKey] = objMetadata
		xl.storedBuckets.Set(bucket, storedBucket)
		return objMetadata, nil
	}
	return ObjectMetadata{}, probe.NewError(ObjectNotFound{Object: key})
}

// evictedObject callback function called when an item is evicted from memory
func (xl API) evictedObject(a ...interface{}) {
	cacheStats := xl.objects.Stats()
	log.Printf("CurrentSize: %d, CurrentItems: %d, TotalEvicted: %d",
		cacheStats.Bytes, cacheStats.Items, cacheStats.Evicted)
	key := a[0].(string)
	// loop through all buckets
	for _, bucket := range xl.storedBuckets.GetAll() {
		delete(bucket.(storedBucket).objectMetadata, key)
	}
	debug.FreeOSMemory()
}
