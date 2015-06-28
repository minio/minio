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

package donut

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

func (d donutDriver) NewMultipartUpload(bucketName, objectName, contentType string) (string, error) {
	d.lock.RLock()
	if !drivers.IsValidBucket(bucketName) {
		d.lock.RUnlock()
		return "", iodine.New(drivers.BucketNameInvalid{Bucket: bucketName}, nil)
	}
	if !drivers.IsValidObjectName(objectName) {
		d.lock.RUnlock()
		return "", iodine.New(drivers.ObjectNameInvalid{Object: objectName}, nil)
	}
	d.lock.RUnlock()
	buckets, err := d.donut.ListBuckets()
	if err != nil {
		return "", iodine.New(err, nil)
	}
	for bucketName, metadata := range buckets {
		result := drivers.BucketMetadata{
			Name:    metadata.Name,
			Created: metadata.Created,
			ACL:     drivers.BucketACL(metadata.ACL),
		}
		d.lock.Lock()
		storedBucket := d.storedBuckets[bucketName]
		storedBucket.bucketMetadata = result
		if len(storedBucket.multiPartSession) == 0 {
			storedBucket.multiPartSession = make(map[string]multiPartSession)
		}
		if len(storedBucket.objectMetadata) == 0 {
			storedBucket.objectMetadata = make(map[string]drivers.ObjectMetadata)
		}
		if len(storedBucket.partMetadata) == 0 {
			storedBucket.partMetadata = make(map[string]drivers.PartMetadata)
		}
		d.storedBuckets[bucketName] = storedBucket
		d.lock.Unlock()
	}
	d.lock.RLock()
	if _, ok := d.storedBuckets[bucketName]; ok == false {
		d.lock.RUnlock()
		return "", iodine.New(drivers.BucketNotFound{Bucket: bucketName}, nil)
	}
	storedBucket := d.storedBuckets[bucketName]
	objectKey := bucketName + "/" + objectName
	if _, ok := storedBucket.objectMetadata[objectKey]; ok == true {
		d.lock.RUnlock()
		return "", iodine.New(drivers.ObjectExists{Bucket: bucketName, Object: objectName}, nil)
	}
	d.lock.RUnlock()

	d.lock.Lock()
	id := []byte(strconv.FormatInt(rand.Int63(), 10) + bucketName + objectName + time.Now().String())
	uploadIDSum := sha512.Sum512(id)
	uploadID := base64.URLEncoding.EncodeToString(uploadIDSum[:])[:47]

	d.storedBuckets[bucketName].multiPartSession[objectName] = multiPartSession{
		uploadID:   uploadID,
		initiated:  time.Now().UTC(),
		totalParts: 0,
	}
	d.lock.Unlock()
	return uploadID, nil
}

func (d donutDriver) AbortMultipartUpload(bucket, key, uploadID string) error {
	d.lock.RLock()
	storedBucket := d.storedBuckets[bucket]
	if storedBucket.multiPartSession[key].uploadID != uploadID {
		d.lock.RUnlock()
		return iodine.New(drivers.InvalidUploadID{UploadID: uploadID}, nil)
	}
	d.lock.RUnlock()

	d.cleanupMultiparts(bucket, key, uploadID)
	d.cleanupMultipartSession(bucket, key, uploadID)
	return nil
}

func getMultipartKey(key string, uploadID string, partNumber int) string {
	return key + "?uploadId=" + uploadID + "&partNumber=" + strconv.Itoa(partNumber)
}

func (d donutDriver) CreateObjectPart(bucket, key, uploadID string, partID int, contentType, expectedMD5Sum string, size int64, data io.Reader) (string, error) {
	// Verify upload id
	d.lock.RLock()
	storedBucket := d.storedBuckets[bucket]
	if storedBucket.multiPartSession[key].uploadID != uploadID {
		d.lock.RUnlock()
		return "", iodine.New(drivers.InvalidUploadID{UploadID: uploadID}, nil)
	}
	d.lock.RUnlock()

	etag, err := d.createObjectPart(bucket, key, uploadID, partID, "", expectedMD5Sum, size, data)
	if err != nil {
		return "", iodine.New(err, nil)
	}
	// free
	debug.FreeOSMemory()
	return etag, nil
}

// createObject - PUT object to memory buffer
func (d donutDriver) createObjectPart(bucket, key, uploadID string, partID int, contentType, expectedMD5Sum string, size int64, data io.Reader) (string, error) {
	d.lock.RLock()
	if !drivers.IsValidBucket(bucket) {
		d.lock.RUnlock()
		return "", iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, nil)
	}
	if !drivers.IsValidObjectName(key) {
		d.lock.RUnlock()
		return "", iodine.New(drivers.ObjectNameInvalid{Object: key}, nil)
	}
	if _, ok := d.storedBuckets[bucket]; ok == false {
		d.lock.RUnlock()
		return "", iodine.New(drivers.BucketNotFound{Bucket: bucket}, nil)
	}
	storedBucket := d.storedBuckets[bucket]
	// get object key
	partKey := bucket + "/" + getMultipartKey(key, uploadID, partID)
	if _, ok := storedBucket.partMetadata[partKey]; ok == true {
		d.lock.RUnlock()
		return storedBucket.partMetadata[partKey].ETag, nil
	}
	d.lock.RUnlock()

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

	d.lock.Lock()
	d.multiPartObjects.Set(partKey, readBytes)
	d.lock.Unlock()
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

	d.lock.Lock()
	storedBucket.partMetadata[partKey] = newPart
	multiPartSession := storedBucket.multiPartSession[key]
	multiPartSession.totalParts++
	storedBucket.multiPartSession[key] = multiPartSession
	d.storedBuckets[bucket] = storedBucket
	d.lock.Unlock()

	return md5Sum, nil
}

func (d donutDriver) cleanupMultipartSession(bucket, key, uploadID string) {
	d.lock.Lock()
	defer d.lock.Unlock()
	delete(d.storedBuckets[bucket].multiPartSession, key)
}

func (d donutDriver) cleanupMultiparts(bucket, key, uploadID string) {
	for i := 1; i <= d.storedBuckets[bucket].multiPartSession[key].totalParts; i++ {
		objectKey := bucket + "/" + getMultipartKey(key, uploadID, i)
		d.multiPartObjects.Delete(objectKey)
	}
}

func (d donutDriver) CompleteMultipartUpload(bucket, key, uploadID string, parts map[int]string) (string, error) {
	if !drivers.IsValidBucket(bucket) {
		return "", iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, nil)
	}
	if !drivers.IsValidObjectName(key) {
		return "", iodine.New(drivers.ObjectNameInvalid{Object: key}, nil)
	}
	// Verify upload id
	d.lock.RLock()
	if _, ok := d.storedBuckets[bucket]; ok == false {
		d.lock.RUnlock()
		return "", iodine.New(drivers.BucketNotFound{Bucket: bucket}, nil)
	}
	storedBucket := d.storedBuckets[bucket]
	if storedBucket.multiPartSession[key].uploadID != uploadID {
		d.lock.RUnlock()
		return "", iodine.New(drivers.InvalidUploadID{UploadID: uploadID}, nil)
	}
	d.lock.RUnlock()

	d.lock.Lock()
	var size int64
	var fullObject bytes.Buffer
	for i := 1; i <= len(parts); i++ {
		recvMD5 := parts[i]
		object, ok := d.multiPartObjects.Get(bucket + "/" + getMultipartKey(key, uploadID, i))
		if ok == false {
			d.lock.Unlock()
			return "", iodine.New(errors.New("missing part: "+strconv.Itoa(i)), nil)
		}
		size += int64(len(object))
		calcMD5Bytes := md5.Sum(object)
		// complete multi part request header md5sum per part is hex encoded
		recvMD5Bytes, err := hex.DecodeString(strings.Trim(recvMD5, "\""))
		if err != nil {
			return "", iodine.New(drivers.InvalidDigest{Md5: recvMD5}, nil)
		}
		if !bytes.Equal(recvMD5Bytes, calcMD5Bytes[:]) {
			return "", iodine.New(drivers.BadDigest{Md5: recvMD5, Bucket: bucket, Key: getMultipartKey(key, uploadID, i)}, nil)
		}
		_, err = io.Copy(&fullObject, bytes.NewBuffer(object))
		if err != nil {
			return "", iodine.New(err, nil)
		}
		object = nil
		go debug.FreeOSMemory()
	}
	d.lock.Unlock()

	md5sumSlice := md5.Sum(fullObject.Bytes())
	// this is needed for final verification inside CreateObject, do not convert this to hex
	md5sum := base64.StdEncoding.EncodeToString(md5sumSlice[:])
	etag, err := d.CreateObject(bucket, key, "", md5sum, size, &fullObject)
	if err != nil {
		// No need to call internal cleanup functions here, caller will call AbortMultipartUpload()
		// which would in-turn cleanup properly in accordance with S3 Spec
		return "", iodine.New(err, nil)
	}
	fullObject.Reset()
	d.cleanupMultiparts(bucket, key, uploadID)
	d.cleanupMultipartSession(bucket, key, uploadID)
	return etag, nil
}

// byKey is a sortable interface for UploadMetadata slice
type byKey []*drivers.UploadMetadata

func (a byKey) Len() int           { return len(a) }
func (a byKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func (d donutDriver) ListMultipartUploads(bucket string, resources drivers.BucketMultipartResourcesMetadata) (drivers.BucketMultipartResourcesMetadata, error) {
	// TODO handle delimiter
	d.lock.RLock()
	defer d.lock.RUnlock()
	if _, ok := d.storedBuckets[bucket]; ok == false {
		return drivers.BucketMultipartResourcesMetadata{}, iodine.New(drivers.BucketNotFound{Bucket: bucket}, nil)
	}
	storedBucket := d.storedBuckets[bucket]
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

func (d donutDriver) ListObjectParts(bucket, key string, resources drivers.ObjectResourcesMetadata) (drivers.ObjectResourcesMetadata, error) {
	// Verify upload id
	d.lock.RLock()
	defer d.lock.RUnlock()
	if _, ok := d.storedBuckets[bucket]; ok == false {
		return drivers.ObjectResourcesMetadata{}, iodine.New(drivers.BucketNotFound{Bucket: bucket}, nil)
	}
	storedBucket := d.storedBuckets[bucket]
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

func (d donutDriver) expiredPart(a ...interface{}) {
	key := a[0].(string)
	// loop through all buckets
	for _, storedBucket := range d.storedBuckets {
		delete(storedBucket.partMetadata, key)
	}
	debug.FreeOSMemory()
}
