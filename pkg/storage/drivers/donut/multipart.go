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

func (d donutDriver) AbortMultipartUpload(bucketName, objectName, uploadID string) error {
	d.lock.RLock()
	storedBucket := d.storedBuckets[bucketName]
	if storedBucket.multiPartSession[objectName].uploadID != uploadID {
		d.lock.RUnlock()
		return iodine.New(drivers.InvalidUploadID{UploadID: uploadID}, nil)
	}
	d.lock.RUnlock()

	d.cleanupMultiparts(bucketName, objectName, uploadID)
	d.cleanupMultipartSession(bucketName, objectName, uploadID)
	return nil
}

func getMultipartKey(key string, uploadID string, partNumber int) string {
	return key + "?uploadId=" + uploadID + "&partNumber=" + strconv.Itoa(partNumber)
}

func (d donutDriver) CreateObjectPart(bucketName, objectName, uploadID string, partID int, contentType, expectedMD5Sum string, size int64, data io.Reader) (string, error) {
	// Verify upload id
	d.lock.RLock()
	storedBucket := d.storedBuckets[bucketName]
	if storedBucket.multiPartSession[objectName].uploadID != uploadID {
		d.lock.RUnlock()
		return "", iodine.New(drivers.InvalidUploadID{UploadID: uploadID}, nil)
	}
	d.lock.RUnlock()

	etag, err := d.createObjectPart(bucketName, objectName, uploadID, partID, "", expectedMD5Sum, size, data)
	if err != nil {
		return "", iodine.New(err, nil)
	}
	// free
	debug.FreeOSMemory()
	return etag, nil
}

// createObject - PUT object to memory buffer
func (d donutDriver) createObjectPart(bucketName, objectName, uploadID string, partID int, contentType, expectedMD5Sum string, size int64, data io.Reader) (string, error) {
	d.lock.RLock()
	if !drivers.IsValidBucket(bucketName) {
		d.lock.RUnlock()
		return "", iodine.New(drivers.BucketNameInvalid{Bucket: bucketName}, nil)
	}
	if !drivers.IsValidObjectName(objectName) {
		d.lock.RUnlock()
		return "", iodine.New(drivers.ObjectNameInvalid{Object: objectName}, nil)
	}
	if _, ok := d.storedBuckets[bucketName]; ok == false {
		d.lock.RUnlock()
		return "", iodine.New(drivers.BucketNotFound{Bucket: bucketName}, nil)
	}
	storedBucket := d.storedBuckets[bucketName]
	// get object key
	partKey := bucketName + "/" + getMultipartKey(objectName, uploadID, partID)
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
			return "", iodine.New(drivers.BadDigest{
				Md5:    expectedMD5Sum,
				Bucket: bucketName,
				Key:    objectName,
			}, nil)
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
	multiPartSession := storedBucket.multiPartSession[objectName]
	multiPartSession.totalParts++
	storedBucket.multiPartSession[objectName] = multiPartSession
	d.storedBuckets[bucketName] = storedBucket
	d.lock.Unlock()

	return md5Sum, nil
}

func (d donutDriver) cleanupMultipartSession(bucketName, objectName, uploadID string) {
	d.lock.Lock()
	defer d.lock.Unlock()
	delete(d.storedBuckets[bucketName].multiPartSession, objectName)
}

func (d donutDriver) cleanupMultiparts(bucketName, objectName, uploadID string) {
	for i := 1; i <= d.storedBuckets[bucketName].multiPartSession[objectName].totalParts; i++ {
		objectKey := bucketName + "/" + getMultipartKey(objectName, uploadID, i)
		d.multiPartObjects.Delete(objectKey)
	}
}

func (d donutDriver) CompleteMultipartUpload(bucketName, objectName, uploadID string, parts map[int]string) (string, error) {
	if !drivers.IsValidBucket(bucketName) {
		return "", iodine.New(drivers.BucketNameInvalid{Bucket: bucketName}, nil)
	}
	if !drivers.IsValidObjectName(objectName) {
		return "", iodine.New(drivers.ObjectNameInvalid{Object: objectName}, nil)
	}
	// Verify upload id
	d.lock.RLock()
	if _, ok := d.storedBuckets[bucketName]; ok == false {
		d.lock.RUnlock()
		return "", iodine.New(drivers.BucketNotFound{Bucket: bucketName}, nil)
	}
	storedBucket := d.storedBuckets[bucketName]
	if storedBucket.multiPartSession[objectName].uploadID != uploadID {
		d.lock.RUnlock()
		return "", iodine.New(drivers.InvalidUploadID{UploadID: uploadID}, nil)
	}
	d.lock.RUnlock()

	d.lock.Lock()
	var size int64
	var fullObject bytes.Buffer
	for i := 1; i <= len(parts); i++ {
		recvMD5 := parts[i]
		object, ok := d.multiPartObjects.Get(bucketName + "/" + getMultipartKey(objectName, uploadID, i))
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
			return "", iodine.New(drivers.BadDigest{
				Md5:    recvMD5,
				Bucket: bucketName,
				Key:    getMultipartKey(objectName, uploadID, i),
			}, nil)
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
	etag, err := d.CreateObject(bucketName, objectName, "", md5sum, size, &fullObject)
	if err != nil {
		// No need to call internal cleanup functions here, caller will call AbortMultipartUpload()
		// which would in-turn cleanup properly in accordance with S3 Spec
		return "", iodine.New(err, nil)
	}
	fullObject.Reset()
	d.cleanupMultiparts(bucketName, objectName, uploadID)
	d.cleanupMultipartSession(bucketName, objectName, uploadID)
	return etag, nil
}

// byKey is a sortable interface for UploadMetadata slice
type byKey []*drivers.UploadMetadata

func (a byKey) Len() int           { return len(a) }
func (a byKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func (d donutDriver) ListMultipartUploads(bucketName string, resources drivers.BucketMultipartResourcesMetadata) (drivers.BucketMultipartResourcesMetadata, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()
	if _, ok := d.storedBuckets[bucketName]; ok == false {
		return drivers.BucketMultipartResourcesMetadata{}, iodine.New(drivers.BucketNotFound{Bucket: bucketName}, nil)
	}
	storedBucket := d.storedBuckets[bucketName]
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

func (d donutDriver) ListObjectParts(bucketName, objectName string, resources drivers.ObjectResourcesMetadata) (drivers.ObjectResourcesMetadata, error) {
	// Verify upload id
	d.lock.RLock()
	defer d.lock.RUnlock()
	if _, ok := d.storedBuckets[bucketName]; ok == false {
		return drivers.ObjectResourcesMetadata{}, iodine.New(drivers.BucketNotFound{Bucket: bucketName}, nil)
	}
	storedBucket := d.storedBuckets[bucketName]
	if _, ok := storedBucket.multiPartSession[objectName]; ok == false {
		return drivers.ObjectResourcesMetadata{}, iodine.New(drivers.ObjectNotFound{Bucket: bucketName, Object: objectName}, nil)
	}
	if storedBucket.multiPartSession[objectName].uploadID != resources.UploadID {
		return drivers.ObjectResourcesMetadata{}, iodine.New(drivers.InvalidUploadID{UploadID: resources.UploadID}, nil)
	}
	objectResourcesMetadata := resources
	objectResourcesMetadata.Bucket = bucketName
	objectResourcesMetadata.Key = objectName
	var parts []*drivers.PartMetadata
	var startPartNumber int
	switch {
	case objectResourcesMetadata.PartNumberMarker == 0:
		startPartNumber = 1
	default:
		startPartNumber = objectResourcesMetadata.PartNumberMarker
	}
	for i := startPartNumber; i <= storedBucket.multiPartSession[objectName].totalParts; i++ {
		if len(parts) > objectResourcesMetadata.MaxParts {
			sort.Sort(partNumber(parts))
			objectResourcesMetadata.IsTruncated = true
			objectResourcesMetadata.Part = parts
			objectResourcesMetadata.NextPartNumberMarker = i
			return objectResourcesMetadata, nil
		}
		part, ok := storedBucket.partMetadata[bucketName+"/"+getMultipartKey(objectName, resources.UploadID, i)]
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
