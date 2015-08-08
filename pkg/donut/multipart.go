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
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"io"
	"io/ioutil"
	"math/rand"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio/pkg/crypto/sha256"
	"github.com/minio/minio/pkg/donut/cache/data"
	"github.com/minio/minio/pkg/probe"
)

/// V2 API functions

// NewMultipartUpload - initiate a new multipart session
func (donut API) NewMultipartUpload(bucket, key, contentType string, signature *Signature) (string, *probe.Error) {
	donut.lock.Lock()
	defer donut.lock.Unlock()

	if !IsValidBucket(bucket) {
		return "", probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(key) {
		return "", probe.NewError(ObjectNameInvalid{Object: key})
	}
	if signature != nil {
		ok, err := signature.DoesSignatureMatch("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
		if err != nil {
			return "", err.Trace()
		}
		if !ok {
			return "", probe.NewError(SignatureDoesNotMatch{})
		}
	}
	if len(donut.config.NodeDiskMap) > 0 {
		return donut.newMultipartUpload(bucket, key, contentType)
	}
	if !donut.storedBuckets.Exists(bucket) {
		return "", probe.NewError(BucketNotFound{Bucket: bucket})
	}
	storedBucket := donut.storedBuckets.Get(bucket).(storedBucket)
	objectKey := bucket + "/" + key
	if _, ok := storedBucket.objectMetadata[objectKey]; ok == true {
		return "", probe.NewError(ObjectExists{Object: key})
	}
	id := []byte(strconv.Itoa(rand.Int()) + bucket + key + time.Now().UTC().String())
	uploadIDSum := sha512.Sum512(id)
	uploadID := base64.URLEncoding.EncodeToString(uploadIDSum[:])[:47]

	storedBucket.multiPartSession[key] = MultiPartSession{
		UploadID:   uploadID,
		Initiated:  time.Now().UTC(),
		TotalParts: 0,
	}
	storedBucket.partMetadata[key] = make(map[int]PartMetadata)
	multiPartCache := data.NewCache(0)
	multiPartCache.OnEvicted = donut.evictedPart
	donut.multiPartObjects[uploadID] = multiPartCache
	donut.storedBuckets.Set(bucket, storedBucket)
	return uploadID, nil
}

// AbortMultipartUpload - abort an incomplete multipart session
func (donut API) AbortMultipartUpload(bucket, key, uploadID string, signature *Signature) *probe.Error {
	donut.lock.Lock()
	defer donut.lock.Unlock()

	if !IsValidBucket(bucket) {
		return probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(key) {
		return probe.NewError(ObjectNameInvalid{Object: key})
	}
	if signature != nil {
		ok, err := signature.DoesSignatureMatch("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
		if err != nil {
			return err.Trace()
		}
		if !ok {
			return probe.NewError(SignatureDoesNotMatch{})
		}
	}
	if len(donut.config.NodeDiskMap) > 0 {
		return donut.abortMultipartUpload(bucket, key, uploadID)
	}
	if !donut.storedBuckets.Exists(bucket) {
		return probe.NewError(BucketNotFound{Bucket: bucket})
	}
	storedBucket := donut.storedBuckets.Get(bucket).(storedBucket)
	if storedBucket.multiPartSession[key].UploadID != uploadID {
		return probe.NewError(InvalidUploadID{UploadID: uploadID})
	}
	donut.cleanupMultipartSession(bucket, key, uploadID)
	return nil
}

// CreateObjectPart - create a part in a multipart session
func (donut API) CreateObjectPart(bucket, key, uploadID string, partID int, contentType, expectedMD5Sum string, size int64, data io.Reader, signature *Signature) (string, *probe.Error) {
	donut.lock.Lock()
	etag, err := donut.createObjectPart(bucket, key, uploadID, partID, "", expectedMD5Sum, size, data, signature)
	donut.lock.Unlock()
	// possible free
	debug.FreeOSMemory()

	return etag, err.Trace()
}

// createObject - internal wrapper function called by CreateObjectPart
func (donut API) createObjectPart(bucket, key, uploadID string, partID int, contentType, expectedMD5Sum string, size int64, data io.Reader, signature *Signature) (string, *probe.Error) {
	if !IsValidBucket(bucket) {
		return "", probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(key) {
		return "", probe.NewError(ObjectNameInvalid{Object: key})
	}
	if len(donut.config.NodeDiskMap) > 0 {
		metadata := make(map[string]string)
		if contentType == "" {
			contentType = "application/octet-stream"
		}
		contentType = strings.TrimSpace(contentType)
		metadata["contentType"] = contentType
		if strings.TrimSpace(expectedMD5Sum) != "" {
			expectedMD5SumBytes, err := base64.StdEncoding.DecodeString(strings.TrimSpace(expectedMD5Sum))
			if err != nil {
				// pro-actively close the connection
				return "", probe.NewError(InvalidDigest{Md5: expectedMD5Sum})
			}
			expectedMD5Sum = hex.EncodeToString(expectedMD5SumBytes)
		}
		partMetadata, err := donut.putObjectPart(bucket, key, expectedMD5Sum, uploadID, partID, data, size, metadata, signature)
		if err != nil {
			return "", err.Trace()
		}
		return partMetadata.ETag, nil
	}

	if !donut.storedBuckets.Exists(bucket) {
		return "", probe.NewError(BucketNotFound{Bucket: bucket})
	}
	strBucket := donut.storedBuckets.Get(bucket).(storedBucket)
	// Verify upload id
	if strBucket.multiPartSession[key].UploadID != uploadID {
		return "", probe.NewError(InvalidUploadID{UploadID: uploadID})
	}

	// get object key
	parts := strBucket.partMetadata[key]
	if _, ok := parts[partID]; ok {
		return parts[partID].ETag, nil
	}

	if contentType == "" {
		contentType = "application/octet-stream"
	}
	contentType = strings.TrimSpace(contentType)
	if strings.TrimSpace(expectedMD5Sum) != "" {
		expectedMD5SumBytes, err := base64.StdEncoding.DecodeString(strings.TrimSpace(expectedMD5Sum))
		if err != nil {
			// pro-actively close the connection
			return "", probe.NewError(InvalidDigest{Md5: expectedMD5Sum})
		}
		expectedMD5Sum = hex.EncodeToString(expectedMD5SumBytes)
	}

	// calculate md5
	hash := md5.New()
	sha256hash := sha256.New()

	var totalLength int64
	var err error
	for err == nil {
		var length int
		byteBuffer := make([]byte, 1024*1024)
		length, err = data.Read(byteBuffer) // do not read error return error here, we will handle this error later
		hash.Write(byteBuffer[0:length])
		sha256hash.Write(byteBuffer[0:length])
		ok := donut.multiPartObjects[uploadID].Append(partID, byteBuffer[0:length])
		if !ok {
			return "", probe.NewError(InternalError{})
		}
		totalLength += int64(length)
		go debug.FreeOSMemory()
	}
	if totalLength != size {
		donut.multiPartObjects[uploadID].Delete(partID)
		return "", probe.NewError(IncompleteBody{Bucket: bucket, Object: key})
	}
	if err != io.EOF {
		return "", probe.NewError(err)
	}

	md5SumBytes := hash.Sum(nil)
	md5Sum := hex.EncodeToString(md5SumBytes)
	// Verify if the written object is equal to what is expected, only if it is requested as such
	if strings.TrimSpace(expectedMD5Sum) != "" {
		if err := isMD5SumEqual(strings.TrimSpace(expectedMD5Sum), md5Sum); err != nil {
			return "", err.Trace()
		}
	}

	if signature != nil {
		{
			ok, err := signature.DoesSignatureMatch(hex.EncodeToString(sha256hash.Sum(nil)))
			if err != nil {
				return "", err.Trace()
			}
			if !ok {
				return "", probe.NewError(SignatureDoesNotMatch{})
			}
		}
	}

	newPart := PartMetadata{
		PartNumber:   partID,
		LastModified: time.Now().UTC(),
		ETag:         md5Sum,
		Size:         totalLength,
	}

	parts[partID] = newPart
	strBucket.partMetadata[key] = parts
	multiPartSession := strBucket.multiPartSession[key]
	multiPartSession.TotalParts++
	strBucket.multiPartSession[key] = multiPartSession
	donut.storedBuckets.Set(bucket, strBucket)
	return md5Sum, nil
}

// cleanupMultipartSession invoked during an abort or complete multipart session to cleanup session from memory
func (donut API) cleanupMultipartSession(bucket, key, uploadID string) {
	storedBucket := donut.storedBuckets.Get(bucket).(storedBucket)
	for i := 1; i <= storedBucket.multiPartSession[key].TotalParts; i++ {
		donut.multiPartObjects[uploadID].Delete(i)
	}
	delete(storedBucket.multiPartSession, key)
	delete(storedBucket.partMetadata, key)
	donut.storedBuckets.Set(bucket, storedBucket)
}

// CompleteMultipartUpload - complete a multipart upload and persist the data
func (donut API) CompleteMultipartUpload(bucket, key, uploadID string, data io.Reader, signature *Signature) (ObjectMetadata, *probe.Error) {
	donut.lock.Lock()

	if !IsValidBucket(bucket) {
		donut.lock.Unlock()
		return ObjectMetadata{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(key) {
		donut.lock.Unlock()
		return ObjectMetadata{}, probe.NewError(ObjectNameInvalid{Object: key})
	}
	if len(donut.config.NodeDiskMap) > 0 {
		donut.lock.Unlock()
		return donut.completeMultipartUpload(bucket, key, uploadID, data, signature)
	}

	if !donut.storedBuckets.Exists(bucket) {
		donut.lock.Unlock()
		return ObjectMetadata{}, probe.NewError(BucketNotFound{Bucket: bucket})
	}
	storedBucket := donut.storedBuckets.Get(bucket).(storedBucket)
	// Verify upload id
	if storedBucket.multiPartSession[key].UploadID != uploadID {
		donut.lock.Unlock()
		return ObjectMetadata{}, probe.NewError(InvalidUploadID{UploadID: uploadID})
	}
	partBytes, err := ioutil.ReadAll(data)
	if err != nil {
		donut.lock.Unlock()
		return ObjectMetadata{}, probe.NewError(err)
	}
	if signature != nil {
		ok, err := signature.DoesSignatureMatch(hex.EncodeToString(sha256.Sum256(partBytes)[:]))
		if err != nil {
			donut.lock.Unlock()
			return ObjectMetadata{}, err.Trace()
		}
		if !ok {
			donut.lock.Unlock()
			return ObjectMetadata{}, probe.NewError(SignatureDoesNotMatch{})
		}
	}
	parts := &CompleteMultipartUpload{}
	if err := xml.Unmarshal(partBytes, parts); err != nil {
		donut.lock.Unlock()
		return ObjectMetadata{}, probe.NewError(MalformedXML{})
	}
	if !sort.IsSorted(completedParts(parts.Part)) {
		donut.lock.Unlock()
		return ObjectMetadata{}, probe.NewError(InvalidPartOrder{})
	}

	var size int64
	var fullObject bytes.Buffer
	for i := 0; i < len(parts.Part); i++ {
		recvMD5 := parts.Part[i].ETag
		object, ok := donut.multiPartObjects[uploadID].Get(parts.Part[i].PartNumber)
		if ok == false {
			donut.lock.Unlock()
			return ObjectMetadata{}, probe.NewError(InvalidPart{})
		}
		size += int64(len(object))
		calcMD5Bytes := md5.Sum(object)
		// complete multi part request header md5sum per part is hex encoded
		recvMD5Bytes, err := hex.DecodeString(strings.Trim(recvMD5, "\""))
		if err != nil {
			donut.lock.Unlock()
			return ObjectMetadata{}, probe.NewError(InvalidDigest{Md5: recvMD5})
		}
		if !bytes.Equal(recvMD5Bytes, calcMD5Bytes[:]) {
			donut.lock.Unlock()
			return ObjectMetadata{}, probe.NewError(BadDigest{})
		}
		if _, err := io.Copy(&fullObject, bytes.NewBuffer(object)); err != nil {
			donut.lock.Unlock()
			return ObjectMetadata{}, probe.NewError(err)
		}
		object = nil
		go debug.FreeOSMemory()
	}

	md5sumSlice := md5.Sum(fullObject.Bytes())
	// this is needed for final verification inside CreateObject, do not convert this to hex
	md5sum := base64.StdEncoding.EncodeToString(md5sumSlice[:])
	donut.lock.Unlock()
	{
		objectMetadata, err := donut.CreateObject(bucket, key, md5sum, size, &fullObject, nil, nil)
		if err != nil {
			// No need to call internal cleanup functions here, caller should call AbortMultipartUpload()
			// which would in-turn cleanup properly in accordance with S3 Spec
			return ObjectMetadata{}, err.Trace()
		}
		fullObject.Reset()

		donut.lock.Lock()
		donut.cleanupMultipartSession(bucket, key, uploadID)
		donut.lock.Unlock()
		return objectMetadata, nil
	}
}

// byKey is a sortable interface for UploadMetadata slice
type byKey []*UploadMetadata

func (a byKey) Len() int           { return len(a) }
func (a byKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// ListMultipartUploads - list incomplete multipart sessions for a given bucket
func (donut API) ListMultipartUploads(bucket string, resources BucketMultipartResourcesMetadata, signature *Signature) (BucketMultipartResourcesMetadata, *probe.Error) {
	// TODO handle delimiter
	donut.lock.Lock()
	defer donut.lock.Unlock()

	if signature != nil {
		ok, err := signature.DoesSignatureMatch("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
		if err != nil {
			return BucketMultipartResourcesMetadata{}, err.Trace()
		}
		if !ok {
			return BucketMultipartResourcesMetadata{}, probe.NewError(SignatureDoesNotMatch{})
		}
	}

	if !IsValidBucket(bucket) {
		return BucketMultipartResourcesMetadata{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}

	if len(donut.config.NodeDiskMap) > 0 {
		return donut.listMultipartUploads(bucket, resources)
	}

	if !donut.storedBuckets.Exists(bucket) {
		return BucketMultipartResourcesMetadata{}, probe.NewError(BucketNotFound{Bucket: bucket})
	}

	storedBucket := donut.storedBuckets.Get(bucket).(storedBucket)
	var uploads []*UploadMetadata

	for key, session := range storedBucket.multiPartSession {
		if strings.HasPrefix(key, resources.Prefix) {
			if len(uploads) > resources.MaxUploads {
				sort.Sort(byKey(uploads))
				resources.Upload = uploads
				resources.NextKeyMarker = key
				resources.NextUploadIDMarker = session.UploadID
				resources.IsTruncated = true
				return resources, nil
			}
			// uploadIDMarker is ignored if KeyMarker is empty
			switch {
			case resources.KeyMarker != "" && resources.UploadIDMarker == "":
				if key > resources.KeyMarker {
					upload := new(UploadMetadata)
					upload.Key = key
					upload.UploadID = session.UploadID
					upload.Initiated = session.Initiated
					uploads = append(uploads, upload)
				}
			case resources.KeyMarker != "" && resources.UploadIDMarker != "":
				if session.UploadID > resources.UploadIDMarker {
					if key >= resources.KeyMarker {
						upload := new(UploadMetadata)
						upload.Key = key
						upload.UploadID = session.UploadID
						upload.Initiated = session.Initiated
						uploads = append(uploads, upload)
					}
				}
			default:
				upload := new(UploadMetadata)
				upload.Key = key
				upload.UploadID = session.UploadID
				upload.Initiated = session.Initiated
				uploads = append(uploads, upload)
			}
		}
	}
	sort.Sort(byKey(uploads))
	resources.Upload = uploads
	return resources, nil
}

// partNumber is a sortable interface for Part slice
type partNumber []*PartMetadata

func (a partNumber) Len() int           { return len(a) }
func (a partNumber) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a partNumber) Less(i, j int) bool { return a[i].PartNumber < a[j].PartNumber }

// ListObjectParts - list parts from incomplete multipart session for a given object
func (donut API) ListObjectParts(bucket, key string, resources ObjectResourcesMetadata, signature *Signature) (ObjectResourcesMetadata, *probe.Error) {
	// Verify upload id
	donut.lock.Lock()
	defer donut.lock.Unlock()

	if signature != nil {
		ok, err := signature.DoesSignatureMatch("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
		if err != nil {
			return ObjectResourcesMetadata{}, err.Trace()
		}
		if !ok {
			return ObjectResourcesMetadata{}, probe.NewError(SignatureDoesNotMatch{})
		}
	}

	if !IsValidBucket(bucket) {
		return ObjectResourcesMetadata{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(key) {
		return ObjectResourcesMetadata{}, probe.NewError(ObjectNameInvalid{Object: key})
	}

	if len(donut.config.NodeDiskMap) > 0 {
		return donut.listObjectParts(bucket, key, resources)
	}

	if !donut.storedBuckets.Exists(bucket) {
		return ObjectResourcesMetadata{}, probe.NewError(BucketNotFound{Bucket: bucket})
	}
	storedBucket := donut.storedBuckets.Get(bucket).(storedBucket)
	if _, ok := storedBucket.multiPartSession[key]; ok == false {
		return ObjectResourcesMetadata{}, probe.NewError(ObjectNotFound{Object: key})
	}
	if storedBucket.multiPartSession[key].UploadID != resources.UploadID {
		return ObjectResourcesMetadata{}, probe.NewError(InvalidUploadID{UploadID: resources.UploadID})
	}
	storedParts := storedBucket.partMetadata[key]
	objectResourcesMetadata := resources
	objectResourcesMetadata.Bucket = bucket
	objectResourcesMetadata.Key = key
	var parts []*PartMetadata
	var startPartNumber int
	switch {
	case objectResourcesMetadata.PartNumberMarker == 0:
		startPartNumber = 1
	default:
		startPartNumber = objectResourcesMetadata.PartNumberMarker
	}
	for i := startPartNumber; i <= storedBucket.multiPartSession[key].TotalParts; i++ {
		if len(parts) > objectResourcesMetadata.MaxParts {
			sort.Sort(partNumber(parts))
			objectResourcesMetadata.IsTruncated = true
			objectResourcesMetadata.Part = parts
			objectResourcesMetadata.NextPartNumberMarker = i
			return objectResourcesMetadata, nil
		}
		part, ok := storedParts[i]
		if !ok {
			return ObjectResourcesMetadata{}, probe.NewError(InvalidPart{})
		}
		parts = append(parts, &part)
	}
	sort.Sort(partNumber(parts))
	objectResourcesMetadata.Part = parts
	return objectResourcesMetadata, nil
}

// evictedPart - call back function called by caching module during individual cache evictions
func (donut API) evictedPart(a ...interface{}) {
	// loop through all buckets
	buckets := donut.storedBuckets.GetAll()
	for bucketName, bucket := range buckets {
		b := bucket.(storedBucket)
		donut.storedBuckets.Set(bucketName, b)
	}
	debug.FreeOSMemory()
}
