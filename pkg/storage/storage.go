/*
 * Mini Object Storage, (C) 2014 Minio, Inc.
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

package storage

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"crypto/sha256"
)

type Storage struct {
	bucketdata map[string]storedBucket
	objectdata map[string]storedObject
}

type storedObject struct {
	metadata ObjectMetadata
	data     []byte
}

type storedBucket struct {
	metadata BucketMetadata
	//	owner    string // TODO
	//	id       string // TODO
}

type BucketMetadata struct {
	Name    string
	Created int64
}

type ObjectMetadata struct {
	Key        string
	SecCreated int64
	Size       int
	ETag       string
}

func isValidBucket(bucket string) bool {
	l := len(bucket)
	if l < 3 || l > 63 {
		return false
	}

	valid := false
	prev := byte('.')
	for i := 0; i < len(bucket); i++ {
		c := bucket[i]
		switch {
		default:
			return false
		case 'a' <= c && c <= 'z':
			valid = true
		case '0' <= c && c <= '9':
			// Is allowed, but bucketname can't be just numbers.
			// Therefore, don't set valid to true
		case c == '-':
			if prev == '.' {
				return false
			}
		case c == '.':
			if prev == '.' || prev == '-' {
				return false
			}
		}
		prev = c
	}

	if prev == '-' || prev == '.' {
		return false
	}
	return valid
}

func (storage *Storage) CopyObjectToWriter(w io.Writer, bucket string, object string) (int64, error) {
	// TODO synchronize access
	// get object
	key := bucket + ":" + object
	if val, ok := storage.objectdata[key]; ok {
		objectBuffer := bytes.NewBuffer(val.data)
		written, err := io.Copy(w, objectBuffer)
		return written, err
	} else {
		return 0, ObjectNotFound{bucket: bucket, path: object}
	}
}

func (storage *Storage) StoreObject(bucket string, key string, data io.Reader) error {
	objectKey := bucket + ":" + key
	if _, ok := storage.objectdata[objectKey]; ok == true {
		return ObjectExists{bucket: bucket, key: key}
	}
	var bytesBuffer bytes.Buffer
	newObject := storedObject{}
	if _, ok := io.Copy(&bytesBuffer, data); ok == nil {
		size := bytesBuffer.Len()
		etag := fmt.Sprintf("%x", sha256.Sum256(bytesBuffer.Bytes()))
		newObject.metadata = ObjectMetadata{
			Key:        key,
			SecCreated: time.Now().Unix(),
			Size:       size,
			ETag:       etag,
		}
		newObject.data = bytesBuffer.Bytes()
	}
	storage.objectdata[objectKey] = newObject
	return nil
}

func (storage *Storage) StoreBucket(bucketName string) error {
	if !isValidBucket(bucketName) {
		return BucketNameInvalid{bucket: bucketName}
	}

	if _, ok := storage.bucketdata[bucketName]; ok == true {
		return BucketExists{bucket: bucketName}
	}
	newBucket := storedBucket{}
	newBucket.metadata = BucketMetadata{
		Name:    bucketName,
		Created: time.Now().Unix(),
	}
	log.Println(bucketName)
	storage.bucketdata[bucketName] = newBucket
	return nil
}

func (storage *Storage) ListObjects(bucket, prefix string, count int) []ObjectMetadata {
	// TODO prefix and count handling
	var results []ObjectMetadata
	for key, object := range storage.objectdata {
		if strings.HasPrefix(key, bucket+":") {
			results = append(results, object.metadata)
		}
	}
	return results
}

func (storage *Storage) ListBuckets(prefix string) []BucketMetadata {
	// TODO prefix handling
	var results []BucketMetadata
	for _, bucket := range storage.bucketdata {
		results = append(results, bucket.metadata)
	}
	return results
}

func Start() (chan<- string, <-chan error, *Storage) {
	ctrlChannel := make(chan string)
	errorChannel := make(chan error)
	go start(ctrlChannel, errorChannel)
	return ctrlChannel, errorChannel, &Storage{
		bucketdata: make(map[string]storedBucket),
		objectdata: make(map[string]storedObject),
	}
}

func start(ctrlChannel <-chan string, errorChannel chan<- error) {
	close(errorChannel)
}
