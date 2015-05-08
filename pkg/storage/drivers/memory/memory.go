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
	"crypto/sha512"
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

	"math/rand"

	"github.com/minio-io/minio/pkg/iodine"
	"github.com/minio-io/minio/pkg/storage/drivers"
)

// memoryDriver - local variables
type memoryDriver struct {
	storedBuckets map[string]storedBucket
	lock          *sync.RWMutex
	objects       *Intelligent
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
	memory.objects = NewIntelligent(maxSize, expiration)
	memory.lock = new(sync.RWMutex)

	memory.objects.OnEvicted = memory.evictObject

	// set up memory expiration
	memory.objects.ExpireObjects(time.Second * 5)

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
	objectKey := bucket + "/" + object
	data, ok := memory.objects.Get(objectKey)
	if !ok {
		memory.lock.RUnlock()
		return 0, iodine.New(drivers.ObjectNotFound{Bucket: bucket, Object: object}, nil)
	}
	written, err := io.Copy(w, bytes.NewBuffer(data.([]byte)))
	memory.lock.RUnlock()
	return written, iodine.New(err, nil)
}

// GetPartialObject - GET object from memory buffer range
func (memory *memoryDriver) GetPartialObject(w io.Writer, bucket, object string, start, length int64) (int64, error) {
	errParams := map[string]string{
		"bucket": bucket,
		"object": object,
		"start":  strconv.FormatInt(start, 10),
		"length": strconv.FormatInt(length, 10),
	}
	memory.lock.RLock()
	if !drivers.IsValidBucket(bucket) {
		memory.lock.RUnlock()
		return 0, iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, errParams)
	}
	if !drivers.IsValidObjectName(object) {
		memory.lock.RUnlock()
		return 0, iodine.New(drivers.ObjectNameInvalid{Object: object}, errParams)
	}
	if start < 0 {
		return 0, iodine.New(drivers.InvalidRange{
			Start:  start,
			Length: length,
		}, errParams)
	}
	objectKey := bucket + "/" + object
	data, ok := memory.objects.Get(objectKey)
	if !ok {
		memory.lock.RUnlock()
		return 0, iodine.New(drivers.ObjectNotFound{Bucket: bucket, Object: object}, errParams)
	}
	written, err := io.CopyN(w, bytes.NewBuffer(data.([]byte)[start:]), length)
	memory.lock.RUnlock()
	return written, iodine.New(err, nil)
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
	md5sum, err := memory.createObject(bucket, key, contentType, expectedMD5Sum, size, data)
	// free
	debug.FreeOSMemory()
	return md5sum, iodine.New(err, nil)
}

// getMD5AndData - this is written as a wrapper to capture md5sum and data in a more memory efficient way
func getMD5AndData(reader io.Reader) ([]byte, []byte, error) {
	hash := md5.New()
	var data []byte

	var err error
	var length int
	for err == nil {
		byteBuffer := make([]byte, 1024*1024)
		length, err = reader.Read(byteBuffer)
		// While hash.Write() wouldn't mind a Nil byteBuffer
		// It is necessary for us to verify this and break
		if length == 0 {
			break
		}
		hash.Write(byteBuffer[0:length])
		data = append(data, byteBuffer[0:length]...)
	}
	if err != io.EOF {
		return nil, nil, err
	}
	return hash.Sum(nil), data, nil
}

// createObject - PUT object to memory buffer
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
	md5SumBytes, readBytes, err := getMD5AndData(data)
	if err != nil {
		return "", iodine.New(err, nil)
	}
	totalLength := len(readBytes)
	memory.lock.Lock()
	memory.objects.Set(objectKey, readBytes)
	memory.lock.Unlock()
	// de-allocating
	readBytes = nil

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

func (memory *memoryDriver) filterDelimiterPrefix(keys []string, key, delim string, r drivers.BucketResourcesMetadata) ([]string, drivers.BucketResourcesMetadata) {
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

func (memory *memoryDriver) listObjects(keys []string, key string, r drivers.BucketResourcesMetadata) ([]string, drivers.BucketResourcesMetadata) {
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
			keys, r = memory.filterDelimiterPrefix(keys, key, delim, r)
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
			keys, resources = memory.listObjects(keys, key, resources)
		}
	}
	// Marker logic - TODO in-efficient right now fix it
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
	log.Printf("CurrentSize: %d, CurrentItems: %d, TotalEvictions: %d",
		cacheStats.Bytes, cacheStats.Items, cacheStats.Evictions)
	key := a[0].(string)
	// loop through all buckets
	for _, storedBucket := range memory.storedBuckets {
		delete(storedBucket.objectMetadata, key)
		// remove bucket if no objects found anymore
		if len(storedBucket.objectMetadata) == 0 {
			// TODO (y4m4)
			// for now refrain from deleting buckets, due to multipart deletes before fullobject being written
			// this case gets trigerred and we can't store the actual data at all receiving 404 on the client
			// delete(memory.storedBuckets, bucket)
		}
	}
	debug.FreeOSMemory()
}

func (memory *memoryDriver) NewMultipartUpload(bucket, key, contentType string) (string, error) {
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
	objectKey := bucket + "/" + key
	if _, ok := storedBucket.objectMetadata[objectKey]; ok == true {
		memory.lock.RUnlock()
		return "", iodine.New(drivers.ObjectExists{Bucket: bucket, Object: key}, nil)
	}
	memory.lock.RUnlock()

	id := []byte(strconv.FormatInt(rand.Int63(), 10) + bucket + key + time.Now().String())
	uploadIDSum := sha512.Sum512(id)
	uploadID := base64.URLEncoding.EncodeToString(uploadIDSum[:])
	md5sumBytes := md5.Sum([]byte(uploadID))
	md5sum := hex.EncodeToString(md5sumBytes[:])

	// Create UploadID session, this is a temporary work around to instantiate a session.
	// It would not be valid in future, since we need to work out proper sessions so that
	// we can cleanly abort session and propagate failures.
	_, err := memory.CreateObject(bucket, key+"?uploadId="+uploadID, contentType, md5sum, int64(len(uploadID)), bytes.NewBufferString(uploadID))
	return uploadID, iodine.New(err, nil)
}

func getMultipartKey(key string, uploadID string, partNumber int) string {
	return key + "?uploadId=" + uploadID + "&partNumber=" + strconv.Itoa(partNumber)
}

func (memory *memoryDriver) CreateObjectPart(bucket, key, uploadID string, partID int, contentType, expectedMD5Sum string, size int64, data io.Reader) (string, error) {
	// Verify upload id
	_, ok := memory.objects.Get(key + "?uploadId=" + uploadID)
	if !ok {
		return "", iodine.New(drivers.InvalidUploadID{UploadID: uploadID}, nil)
	}
	return memory.CreateObject(bucket, getMultipartKey(key, uploadID, partID), "", expectedMD5Sum, size, data)
}

func (memory *memoryDriver) CompleteMultipartUpload(bucket, key, uploadID string, parts map[int]string) (string, error) {
	// Verify upload id
	_, ok := memory.objects.Get(key + "?uploadId=" + uploadID)
	if !ok {
		return "", iodine.New(drivers.InvalidUploadID{UploadID: uploadID}, nil)
	}
	if !drivers.IsValidBucket(bucket) {
		return "", iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, nil)
	}
	if !drivers.IsValidObjectName(key) {
		return "", iodine.New(drivers.ObjectNameInvalid{Object: key}, nil)
	}
	memory.lock.RLock()
	if _, ok := memory.storedBuckets[bucket]; ok == false {
		memory.lock.RUnlock()
		return "", iodine.New(drivers.BucketNotFound{Bucket: bucket}, nil)
	}
	memory.lock.RUnlock()

	memory.lock.Lock()
	var size int64
	for i := 1; i <= len(parts); i++ {
		if _, ok := parts[i]; ok {
			if object, ok := memory.storedBuckets[bucket].objectMetadata[bucket+"/"+getMultipartKey(key, uploadID, i)]; ok == true {
				size += object.Size
			}
		} else {
			memory.lock.Unlock()
			return "", iodine.New(errors.New("missing part: "+strconv.Itoa(i)), nil)
		}
	}

	var fullObject bytes.Buffer
	for i := 1; i <= len(parts); i++ {
		if _, ok := parts[i]; ok {
			if object, ok := memory.objects.Get(bucket + "/" + getMultipartKey(key, uploadID, i)); ok == true {
				obj := object.([]byte)
				_, err := io.Copy(&fullObject, bytes.NewBuffer(obj))
				if err != nil {
					return "", iodine.New(err, nil)
				}
			} else {
				log.Println("Cannot fetch: ", getMultipartKey(key, uploadID, i))
			}
		} else {
			memory.lock.Unlock()
			return "", iodine.New(errors.New("missing part: "+strconv.Itoa(i)), nil)
		}
	}

	for i := 1; i <= len(parts); i++ {
		if _, ok := parts[i]; ok {
			objectKey := bucket + "/" + getMultipartKey(key, uploadID, i)
			memory.objects.Delete(objectKey)
		}
	}

	memory.lock.Unlock()

	md5sumSlice := md5.Sum(fullObject.Bytes())
	md5sum := base64.StdEncoding.EncodeToString(md5sumSlice[:])
	return memory.CreateObject(bucket, key, "", md5sum, size, &fullObject)
}
