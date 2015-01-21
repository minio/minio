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
	"io"
	"log"
	"strings"
	"time"
)

type Storage struct {
	data map[string]storedObject
}

type storedObject struct {
	metadata ObjectMetadata
	data     []byte
}

type ObjectMetadata struct {
	Key        string
	SecCreated int64
	Size       int
}

func (storage *Storage) CopyObjectToWriter(w io.Writer, bucket string, object string) (int64, error) {
	// TODO synchronize access
	// get object
	key := bucket + ":" + object
	if val, ok := storage.data[key]; ok {
		objectBuffer := bytes.NewBuffer(val.data)
		written, err := io.Copy(w, objectBuffer)
		return written, err
	} else {
		return 0, ObjectNotFound{bucket: bucket, path: object}
	}
}

func (storage *Storage) StoreObject(bucket string, key string, data io.Reader) error {
	objectKey := bucket + ":" + key
	if _, ok := storage.data[objectKey]; ok == true {
		return ObjectExists{bucket: bucket, key: key}
	}
	var bytesBuffer bytes.Buffer
	newObject := storedObject{}
	if _, ok := io.Copy(&bytesBuffer, data); ok == nil {
		newObject.metadata = ObjectMetadata{
			Key:        key,
			SecCreated: time.Now().Unix(),
			Size:       len(bytesBuffer.Bytes()),
		}
		newObject.data = bytesBuffer.Bytes()
	}
	storage.data[objectKey] = newObject
	return nil
}

func (storage *Storage) ListObjects(bucket, prefix string, count int) []ObjectMetadata {
	var results []ObjectMetadata
	for key, object := range storage.data {
		log.Println(key)
		if strings.HasPrefix(key, bucket+":") {
			results = append(results, object.metadata)
		}
	}
	return results
}

func Start() (chan<- string, <-chan error, *Storage) {
	ctrlChannel := make(chan string)
	errorChannel := make(chan error)
	go start(ctrlChannel, errorChannel)
	return ctrlChannel, errorChannel, &Storage{
		data: make(map[string]storedObject),
	}
}

func start(ctrlChannel <-chan string, errorChannel chan<- error) {
	close(errorChannel)
}
