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
	"bytes"
	"crypto/md5"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"math/rand"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio/pkg/iodine"
	"github.com/minio/minio/pkg/storage/drivers"
)

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

	memory.lock.Lock()
	id := []byte(strconv.FormatInt(rand.Int63(), 10) + bucket + key + time.Now().String())
	uploadIDSum := sha512.Sum512(id)
	uploadID := base64.URLEncoding.EncodeToString(uploadIDSum[:])[:47]

	memory.storedBuckets[bucket].multiPartSession[key] = multiPartSession{
		uploadID:   uploadID,
		initiated:  time.Now(),
		totalParts: 0,
	}
	memory.lock.Unlock()

	return uploadID, nil
}

func (memory *memoryDriver) AbortMultipartUpload(bucket, key, uploadID string) error {
	memory.lock.RLock()
	storedBucket := memory.storedBuckets[bucket]
	if storedBucket.multiPartSession[key].uploadID != uploadID {
		memory.lock.RUnlock()
		return iodine.New(drivers.InvalidUploadID{UploadID: uploadID}, nil)
	}
	memory.lock.RUnlock()

	memory.cleanupMultiparts(bucket, key, uploadID)
	memory.cleanupMultipartSession(bucket, key, uploadID)
	return nil
}

func getMultipartKey(key string, uploadID string, partNumber int) string {
	return key + "?uploadId=" + uploadID + "&partNumber=" + strconv.Itoa(partNumber)
}

func (memory *memoryDriver) CreateObjectPart(bucket, key, uploadID string, partID int, contentType, expectedMD5Sum string, size int64, data io.Reader) (string, error) {
	// Verify upload id
	memory.lock.RLock()
	storedBucket := memory.storedBuckets[bucket]
	if storedBucket.multiPartSession[key].uploadID != uploadID {
		memory.lock.RUnlock()
		return "", iodine.New(drivers.InvalidUploadID{UploadID: uploadID}, nil)
	}
	memory.lock.RUnlock()

	etag, err := memory.createObjectPart(bucket, key, uploadID, partID, "", expectedMD5Sum, size, data)
	if err != nil {
		return "", iodine.New(err, nil)
	}
	// free
	debug.FreeOSMemory()
	return etag, nil
}

// createObject - PUT object to memory buffer
func (memory *memoryDriver) createObjectPart(bucket, key, uploadID string, partID int, contentType, expectedMD5Sum string, size int64, data io.Reader) (string, error) {
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
	partKey := bucket + "/" + getMultipartKey(key, uploadID, partID)
	if _, ok := storedBucket.partMetadata[partKey]; ok == true {
		memory.lock.RUnlock()
		return storedBucket.partMetadata[partKey].ETag, nil
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
	go debug.FreeOSMemory()
	md5SumBytes := hash.Sum(nil)
	totalLength := int64(len(readBytes))

	memory.lock.Lock()
	memory.multiPartObjects.Set(partKey, readBytes)
	memory.lock.Unlock()
	// setting up for de-allocation
	readBytes = nil

	md5Sum := hex.EncodeToString(md5SumBytes)
	// Verify if the written object is equal to what is expected, only if it is requested as such
	if strings.TrimSpace(expectedMD5Sum) != "" {
		if err := isMD5SumEqual(strings.TrimSpace(expectedMD5Sum), md5Sum); err != nil {
			return "", iodine.New(drivers.BadDigest{Md5: expectedMD5Sum, Bucket: bucket, Key: key}, nil)
		}
	}
	newPart := drivers.PartMetadata{
		PartNumber:   partID,
		LastModified: time.Now().UTC(),
		ETag:         md5Sum,
		Size:         totalLength,
	}

	memory.lock.Lock()
	storedBucket.partMetadata[partKey] = newPart
	multiPartSession := storedBucket.multiPartSession[key]
	multiPartSession.totalParts++
	storedBucket.multiPartSession[key] = multiPartSession
	memory.storedBuckets[bucket] = storedBucket
	memory.lock.Unlock()

	return md5Sum, nil
}

func (memory *memoryDriver) cleanupMultipartSession(bucket, key, uploadID string) {
	memory.lock.Lock()
	defer memory.lock.Unlock()
	delete(memory.storedBuckets[bucket].multiPartSession, key)
}

func (memory *memoryDriver) cleanupMultiparts(bucket, key, uploadID string) {
	memory.lock.Lock()
	defer memory.lock.Unlock()
	for i := 1; i <= memory.storedBuckets[bucket].multiPartSession[key].totalParts; i++ {
		objectKey := bucket + "/" + getMultipartKey(key, uploadID, i)
		memory.multiPartObjects.Delete(objectKey)
	}
}

func (memory *memoryDriver) CompleteMultipartUpload(bucket, key, uploadID string, parts map[int]string) (string, error) {
	if !drivers.IsValidBucket(bucket) {
		return "", iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, nil)
	}
	if !drivers.IsValidObjectName(key) {
		return "", iodine.New(drivers.ObjectNameInvalid{Object: key}, nil)
	}
	// Verify upload id
	memory.lock.RLock()
	if _, ok := memory.storedBuckets[bucket]; ok == false {
		memory.lock.RUnlock()
		return "", iodine.New(drivers.BucketNotFound{Bucket: bucket}, nil)
	}
	storedBucket := memory.storedBuckets[bucket]
	if storedBucket.multiPartSession[key].uploadID != uploadID {
		memory.lock.RUnlock()
		return "", iodine.New(drivers.InvalidUploadID{UploadID: uploadID}, nil)
	}
	memory.lock.RUnlock()

	memory.lock.Lock()
	var size int64
	var fullObject bytes.Buffer
	for i := 1; i <= len(parts); i++ {
		recvMD5 := parts[i]
		object, ok := memory.multiPartObjects.Get(bucket + "/" + getMultipartKey(key, uploadID, i))
		if ok == false {
			memory.lock.Unlock()
			return "", iodine.New(errors.New("missing part: "+strconv.Itoa(i)), nil)
		}
		obj := object.([]byte)
		size += int64(len(obj))
		calcMD5Bytes := md5.Sum(obj)
		// complete multi part request header md5sum per part is hex encoded
		recvMD5Bytes, err := hex.DecodeString(strings.Trim(recvMD5, "\""))
		if err != nil {
			return "", iodine.New(drivers.InvalidDigest{Md5: recvMD5}, nil)
		}
		if !bytes.Equal(recvMD5Bytes, calcMD5Bytes[:]) {
			return "", iodine.New(drivers.BadDigest{Md5: recvMD5, Bucket: bucket, Key: getMultipartKey(key, uploadID, i)}, nil)
		}
		_, err = io.Copy(&fullObject, bytes.NewBuffer(obj))
		if err != nil {
			return "", iodine.New(err, nil)
		}
	}
	memory.lock.Unlock()

	md5sumSlice := md5.Sum(fullObject.Bytes())
	md5sum := base64.StdEncoding.EncodeToString(md5sumSlice[:])
	etag, err := memory.CreateObject(bucket, key, "", md5sum, size, &fullObject)
	if err != nil {
		// No need to call internal cleanup functions here, caller will call AbortMultipartUpload()
		// which would in-turn cleanup properly in accordance with S3 Spec
		return "", iodine.New(err, nil)
	}
	memory.cleanupMultiparts(bucket, key, uploadID)
	memory.cleanupMultipartSession(bucket, key, uploadID)
	return etag, nil
}

// byKey is a sortable interface for UploadMetadata slice
type byKey []*drivers.UploadMetadata

func (a byKey) Len() int           { return len(a) }
func (a byKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func (memory *memoryDriver) ListMultipartUploads(bucket string, resources drivers.BucketMultipartResourcesMetadata) (drivers.BucketMultipartResourcesMetadata, error) {
	// TODO handle delimiter
	memory.lock.RLock()
	defer memory.lock.RUnlock()
	if _, ok := memory.storedBuckets[bucket]; ok == false {
		return drivers.BucketMultipartResourcesMetadata{}, iodine.New(drivers.BucketNotFound{Bucket: bucket}, nil)
	}
	storedBucket := memory.storedBuckets[bucket]
	var uploads []*drivers.UploadMetadata

	for key, session := range storedBucket.multiPartSession {
		if strings.HasPrefix(key, resources.Prefix) {
			if len(uploads) > resources.MaxUploads {
				sort.Sort(byKey(uploads))
				resources.Upload = uploads
				resources.NextKeyMarker = key
				resources.NextUploadIDMarker = session.uploadID
				resources.IsTruncated = true
				return resources, nil
			}
			// uploadIDMarker is ignored if KeyMarker is empty
			switch {
			case resources.KeyMarker != "" && resources.UploadIDMarker == "":
				if key > resources.KeyMarker {
					upload := new(drivers.UploadMetadata)
					upload.Key = key
					upload.UploadID = session.uploadID
					upload.Initiated = session.initiated
					uploads = append(uploads, upload)
				}
			case resources.KeyMarker != "" && resources.UploadIDMarker != "":
				if session.uploadID > resources.UploadIDMarker {
					if key >= resources.KeyMarker {
						upload := new(drivers.UploadMetadata)
						upload.Key = key
						upload.UploadID = session.uploadID
						upload.Initiated = session.initiated
						uploads = append(uploads, upload)
					}
				}
			default:
				upload := new(drivers.UploadMetadata)
				upload.Key = key
				upload.UploadID = session.uploadID
				upload.Initiated = session.initiated
				uploads = append(uploads, upload)
			}
		}
	}
	sort.Sort(byKey(uploads))
	resources.Upload = uploads
	return resources, nil
}

// partNumber is a sortable interface for Part slice
type partNumber []*drivers.PartMetadata

func (a partNumber) Len() int           { return len(a) }
func (a partNumber) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a partNumber) Less(i, j int) bool { return a[i].PartNumber < a[j].PartNumber }

func (memory *memoryDriver) ListObjectParts(bucket, key string, resources drivers.ObjectResourcesMetadata) (drivers.ObjectResourcesMetadata, error) {
	// Verify upload id
	memory.lock.RLock()
	defer memory.lock.RUnlock()
	if _, ok := memory.storedBuckets[bucket]; ok == false {
		return drivers.ObjectResourcesMetadata{}, iodine.New(drivers.BucketNotFound{Bucket: bucket}, nil)
	}
	storedBucket := memory.storedBuckets[bucket]
	if _, ok := storedBucket.multiPartSession[key]; ok == false {
		return drivers.ObjectResourcesMetadata{}, iodine.New(drivers.ObjectNotFound{Bucket: bucket, Object: key}, nil)
	}
	if storedBucket.multiPartSession[key].uploadID != resources.UploadID {
		return drivers.ObjectResourcesMetadata{}, iodine.New(drivers.InvalidUploadID{UploadID: resources.UploadID}, nil)
	}
	objectResourcesMetadata := resources
	objectResourcesMetadata.Bucket = bucket
	objectResourcesMetadata.Key = key
	var parts []*drivers.PartMetadata
	var startPartNumber int
	switch {
	case objectResourcesMetadata.PartNumberMarker == 0:
		startPartNumber = 1
	default:
		startPartNumber = objectResourcesMetadata.PartNumberMarker
	}
	for i := startPartNumber; i <= storedBucket.multiPartSession[key].totalParts; i++ {
		if len(parts) > objectResourcesMetadata.MaxParts {
			sort.Sort(partNumber(parts))
			objectResourcesMetadata.IsTruncated = true
			objectResourcesMetadata.Part = parts
			objectResourcesMetadata.NextPartNumberMarker = i
			return objectResourcesMetadata, nil
		}
		part, ok := storedBucket.partMetadata[bucket+"/"+getMultipartKey(key, resources.UploadID, i)]
		if !ok {
			return drivers.ObjectResourcesMetadata{}, iodine.New(errors.New("missing part: "+strconv.Itoa(i)), nil)
		}
		parts = append(parts, &part)
	}
	sort.Sort(partNumber(parts))
	objectResourcesMetadata.Part = parts
	return objectResourcesMetadata, nil
}

func (memory *memoryDriver) expiredPart(a ...interface{}) {
	key := a[0].(string)
	// loop through all buckets
	for _, storedBucket := range memory.storedBuckets {
		delete(storedBucket.partMetadata, key)
	}
	debug.FreeOSMemory()
}
