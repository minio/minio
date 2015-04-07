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

	"crypto/md5"
	"encoding/hex"

	"io/ioutil"

	"github.com/minio-io/objectdriver"
)

// memoryDriver - local variables
type memoryDriver struct {
	bucketdata map[string]storedBucket
	objectdata map[string]storedObject
	lock       *sync.RWMutex
}

type storedBucket struct {
	metadata drivers.BucketMetadata
	//	owner    string // TODO
	//	id       string // TODO
}

type storedObject struct {
	metadata drivers.ObjectMetadata
	data     []byte
}

// Start memory object server
func Start() (chan<- string, <-chan error, drivers.Driver) {
	ctrlChannel := make(chan string)
	errorChannel := make(chan error)

	memory := new(memoryDriver)
	memory.bucketdata = make(map[string]storedBucket)
	memory.objectdata = make(map[string]storedObject)
	memory.lock = new(sync.RWMutex)

	go start(ctrlChannel, errorChannel)
	return ctrlChannel, errorChannel, memory
}

func start(ctrlChannel <-chan string, errorChannel chan<- error) {
	close(errorChannel)
}

// GetObject - GET object from memory buffer
func (memory memoryDriver) GetObject(w io.Writer, bucket string, object string) (int64, error) {
	// get object
	key := object
	if val, ok := memory.objectdata[key]; ok {
		objectBuffer := bytes.NewBuffer(val.data)
		written, err := io.Copy(w, objectBuffer)
		return written, err
	}
	return 0, drivers.ObjectNotFound{Bucket: bucket, Object: object}
}

// GetPartialObject - GET object from memory buffer range
func (memory memoryDriver) GetPartialObject(w io.Writer, bucket, object string, start, length int64) (int64, error) {
	var sourceBuffer bytes.Buffer
	if _, err := memory.GetObject(&sourceBuffer, bucket, object); err != nil {
		return 0, err
	}
	if _, err := io.CopyN(ioutil.Discard, &sourceBuffer, start); err != nil {
		return 0, err
	}
	return io.CopyN(w, &sourceBuffer, length)
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
	memory.lock.Lock()
	defer memory.lock.Unlock()

	if _, ok := memory.bucketdata[bucket]; ok == false {
		return drivers.BucketNotFound{Bucket: bucket}
	}

	if _, ok := memory.objectdata[key]; ok == true {
		return drivers.ObjectExists{Bucket: bucket, Object: key}
	}

	if contentType == "" {
		contentType = "application/octet-stream"
	}

	contentType = strings.TrimSpace(contentType)

	var bytesBuffer bytes.Buffer
	var newObject = storedObject{}
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
		newObject.data = bytesBuffer.Bytes()
	}
	memory.objectdata[key] = newObject
	return nil
}

// CreateBucket - create bucket in memory
func (memory memoryDriver) CreateBucket(bucketName string) error {
	memory.lock.Lock()
	defer memory.lock.Unlock()
	if !drivers.IsValidBucket(bucketName) {
		return drivers.BucketNameInvalid{Bucket: bucketName}
	}

	if _, ok := memory.bucketdata[bucketName]; ok == true {
		return drivers.BucketExists{Bucket: bucketName}
	}

	var newBucket = storedBucket{}
	newBucket.metadata = drivers.BucketMetadata{}
	newBucket.metadata.Name = bucketName
	newBucket.metadata.Created = time.Now()
	memory.bucketdata[bucketName] = newBucket

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
		if delimitedName == resources.Delimiter {
			resources.CommonPrefixes = appendUniq(resources.CommonPrefixes, resources.Prefix+delimitedName)
		} else {
			resources.CommonPrefixes = appendUniq(resources.CommonPrefixes, delimitedName)
		}
	}
	return resources, keys
}

// ListObjects - list objects from memory
func (memory memoryDriver) ListObjects(bucket string, resources drivers.BucketResourcesMetadata) ([]drivers.ObjectMetadata, drivers.BucketResourcesMetadata, error) {
	if _, ok := memory.bucketdata[bucket]; ok == false {
		return []drivers.ObjectMetadata{}, drivers.BucketResourcesMetadata{IsTruncated: false}, drivers.BucketNotFound{Bucket: bucket}
	}
	var results []drivers.ObjectMetadata
	var keys []string
	for key := range memory.objectdata {
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
	sort.Strings(keys)
	for _, key := range keys {
		if len(results) == resources.Maxkeys {
			return results, drivers.BucketResourcesMetadata{IsTruncated: true}, nil
		}
		object := memory.objectdata[key]
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
	var results []drivers.BucketMetadata
	for _, bucket := range memory.bucketdata {
		results = append(results, bucket.metadata)
	}
	sort.Sort(ByBucketName(results))
	return results, nil
}

// GetObjectMetadata - get object metadata from memory
func (memory memoryDriver) GetObjectMetadata(bucket, key, prefix string) (drivers.ObjectMetadata, error) {
	if object, ok := memory.objectdata[key]; ok == true {
		return object.metadata, nil
	}
	return drivers.ObjectMetadata{}, drivers.ObjectNotFound{Bucket: bucket, Object: key}
}
