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
	"github.com/minio/minio/pkg/probe"
	signV4 "github.com/minio/minio/pkg/signature"
	"github.com/minio/minio/pkg/xl/cache/data"
)

/// V2 API functions

// NewMultipartUpload - initiate a new multipart session
func (xl API) NewMultipartUpload(bucket, key, contentType string) (string, *probe.Error) {
	xl.lock.Lock()
	defer xl.lock.Unlock()

	if !IsValidBucket(bucket) {
		return "", probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(key) {
		return "", probe.NewError(ObjectNameInvalid{Object: key})
	}
	//	if len(xl.config.NodeDiskMap) > 0 {
	//		return xl.newMultipartUpload(bucket, key, contentType)
	//	}
	if !xl.storedBuckets.Exists(bucket) {
		return "", probe.NewError(BucketNotFound{Bucket: bucket})
	}
	storedBucket := xl.storedBuckets.Get(bucket).(storedBucket)
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
	multiPartCache.OnEvicted = xl.evictedPart
	xl.multiPartObjects[uploadID] = multiPartCache
	xl.storedBuckets.Set(bucket, storedBucket)
	return uploadID, nil
}

// AbortMultipartUpload - abort an incomplete multipart session
func (xl API) AbortMultipartUpload(bucket, key, uploadID string) *probe.Error {
	xl.lock.Lock()
	defer xl.lock.Unlock()

	if !IsValidBucket(bucket) {
		return probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(key) {
		return probe.NewError(ObjectNameInvalid{Object: key})
	}
	// TODO: multipart support for xl is broken, since we haven't finalized the format in which
	//       it can be stored, disabling this for now until we get the underlying layout stable.
	//
	//	if len(xl.config.NodeDiskMap) > 0 {
	//		return xl.abortMultipartUpload(bucket, key, uploadID)
	//	}
	if !xl.storedBuckets.Exists(bucket) {
		return probe.NewError(BucketNotFound{Bucket: bucket})
	}
	storedBucket := xl.storedBuckets.Get(bucket).(storedBucket)
	if storedBucket.multiPartSession[key].UploadID != uploadID {
		return probe.NewError(InvalidUploadID{UploadID: uploadID})
	}
	xl.cleanupMultipartSession(bucket, key, uploadID)
	return nil
}

// CreateObjectPart - create a part in a multipart session
func (xl API) CreateObjectPart(bucket, key, uploadID string, partID int, contentType, expectedMD5Sum string, size int64, data io.Reader, signature *signV4.Signature) (string, *probe.Error) {
	xl.lock.Lock()
	etag, err := xl.createObjectPart(bucket, key, uploadID, partID, "", expectedMD5Sum, size, data, signature)
	xl.lock.Unlock()
	// possible free
	debug.FreeOSMemory()

	return etag, err.Trace()
}

// createObject - internal wrapper function called by CreateObjectPart
func (xl API) createObjectPart(bucket, key, uploadID string, partID int, contentType, expectedMD5Sum string, size int64, data io.Reader, signature *signV4.Signature) (string, *probe.Error) {
	if !IsValidBucket(bucket) {
		return "", probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(key) {
		return "", probe.NewError(ObjectNameInvalid{Object: key})
	}
	// TODO: multipart support for xl is broken, since we haven't finalized the format in which
	//       it can be stored, disabling this for now until we get the underlying layout stable.
	//
	/*
		if len(xl.config.NodeDiskMap) > 0 {
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
			partMetadata, err := xl.putObjectPart(bucket, key, expectedMD5Sum, uploadID, partID, data, size, metadata, signature)
			if err != nil {
				return "", err.Trace()
			}
			return partMetadata.ETag, nil
		}
	*/
	if !xl.storedBuckets.Exists(bucket) {
		return "", probe.NewError(BucketNotFound{Bucket: bucket})
	}
	strBucket := xl.storedBuckets.Get(bucket).(storedBucket)
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
		if length != 0 {
			hash.Write(byteBuffer[0:length])
			sha256hash.Write(byteBuffer[0:length])
			ok := xl.multiPartObjects[uploadID].Append(partID, byteBuffer[0:length])
			if !ok {
				return "", probe.NewError(InternalError{})
			}
			totalLength += int64(length)
			go debug.FreeOSMemory()
		}
	}
	if totalLength != size {
		xl.multiPartObjects[uploadID].Delete(partID)
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
				return "", probe.NewError(signV4.SigDoesNotMatch{})
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
	xl.storedBuckets.Set(bucket, strBucket)
	return md5Sum, nil
}

// cleanupMultipartSession invoked during an abort or complete multipart session to cleanup session from memory
func (xl API) cleanupMultipartSession(bucket, key, uploadID string) {
	storedBucket := xl.storedBuckets.Get(bucket).(storedBucket)
	for i := 1; i <= storedBucket.multiPartSession[key].TotalParts; i++ {
		xl.multiPartObjects[uploadID].Delete(i)
	}
	delete(storedBucket.multiPartSession, key)
	delete(storedBucket.partMetadata, key)
	xl.storedBuckets.Set(bucket, storedBucket)
}

func (xl API) mergeMultipart(parts *CompleteMultipartUpload, uploadID string, fullObjectWriter *io.PipeWriter) {
	for _, part := range parts.Part {
		recvMD5 := part.ETag
		object, ok := xl.multiPartObjects[uploadID].Get(part.PartNumber)
		if ok == false {
			fullObjectWriter.CloseWithError(probe.WrapError(probe.NewError(InvalidPart{})))
			return
		}
		calcMD5Bytes := md5.Sum(object)
		// complete multi part request header md5sum per part is hex encoded
		recvMD5Bytes, err := hex.DecodeString(strings.Trim(recvMD5, "\""))
		if err != nil {
			fullObjectWriter.CloseWithError(probe.WrapError(probe.NewError(InvalidDigest{Md5: recvMD5})))
			return
		}
		if !bytes.Equal(recvMD5Bytes, calcMD5Bytes[:]) {
			fullObjectWriter.CloseWithError(probe.WrapError(probe.NewError(BadDigest{})))
			return
		}

		if _, err := io.Copy(fullObjectWriter, bytes.NewReader(object)); err != nil {
			fullObjectWriter.CloseWithError(probe.WrapError(probe.NewError(err)))
			return
		}
		object = nil
	}
	fullObjectWriter.Close()
	return
}

// CompleteMultipartUpload - complete a multipart upload and persist the data
func (xl API) CompleteMultipartUpload(bucket, key, uploadID string, data io.Reader, signature *signV4.Signature) (ObjectMetadata, *probe.Error) {
	xl.lock.Lock()
	defer xl.lock.Unlock()
	size := int64(xl.multiPartObjects[uploadID].Stats().Bytes)
	fullObjectReader, err := xl.completeMultipartUploadV2(bucket, key, uploadID, data, signature)
	if err != nil {
		return ObjectMetadata{}, err.Trace()
	}
	objectMetadata, err := xl.createObject(bucket, key, "", "", size, fullObjectReader, nil)
	if err != nil {
		// No need to call internal cleanup functions here, caller should call AbortMultipartUpload()
		// which would in-turn cleanup properly in accordance with S3 Spec
		return ObjectMetadata{}, err.Trace()
	}
	xl.cleanupMultipartSession(bucket, key, uploadID)
	return objectMetadata, nil
}

func (xl API) completeMultipartUploadV2(bucket, key, uploadID string, data io.Reader, signature *signV4.Signature) (io.Reader, *probe.Error) {
	if !IsValidBucket(bucket) {
		return nil, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(key) {
		return nil, probe.NewError(ObjectNameInvalid{Object: key})
	}

	// TODO: multipart support for xl is broken, since we haven't finalized the format in which
	//       it can be stored, disabling this for now until we get the underlying layout stable.
	//
	//	if len(xl.config.NodeDiskMap) > 0 {
	//		xl.lock.Unlock()
	//		return xl.completeMultipartUpload(bucket, key, uploadID, data, signature)
	//	}

	if !xl.storedBuckets.Exists(bucket) {
		return nil, probe.NewError(BucketNotFound{Bucket: bucket})
	}
	storedBucket := xl.storedBuckets.Get(bucket).(storedBucket)
	// Verify upload id
	if storedBucket.multiPartSession[key].UploadID != uploadID {
		return nil, probe.NewError(InvalidUploadID{UploadID: uploadID})
	}
	partBytes, err := ioutil.ReadAll(data)
	if err != nil {
		return nil, probe.NewError(err)
	}
	if signature != nil {
		partHashBytes := sha256.Sum256(partBytes)
		ok, err := signature.DoesSignatureMatch(hex.EncodeToString(partHashBytes[:]))
		if err != nil {
			return nil, err.Trace()
		}
		if !ok {
			return nil, probe.NewError(signV4.SigDoesNotMatch{})
		}
	}
	parts := &CompleteMultipartUpload{}
	if err := xml.Unmarshal(partBytes, parts); err != nil {
		return nil, probe.NewError(MalformedXML{})
	}
	if !sort.IsSorted(completedParts(parts.Part)) {
		return nil, probe.NewError(InvalidPartOrder{})
	}

	fullObjectReader, fullObjectWriter := io.Pipe()
	go xl.mergeMultipart(parts, uploadID, fullObjectWriter)

	return fullObjectReader, nil
}

// byKey is a sortable interface for UploadMetadata slice
type byKey []*UploadMetadata

func (a byKey) Len() int           { return len(a) }
func (a byKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// ListMultipartUploads - list incomplete multipart sessions for a given bucket
func (xl API) ListMultipartUploads(bucket string, resources BucketMultipartResourcesMetadata) (BucketMultipartResourcesMetadata, *probe.Error) {
	// TODO handle delimiter, low priority
	xl.lock.Lock()
	defer xl.lock.Unlock()

	if !IsValidBucket(bucket) {
		return BucketMultipartResourcesMetadata{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}

	// TODO: multipart support for xl is broken, since we haven't finalized the format in which
	//       it can be stored, disabling this for now until we get the underlying layout stable.
	//
	//	if len(xl.config.NodeDiskMap) > 0 {
	//		return xl.listMultipartUploads(bucket, resources)
	//	}

	if !xl.storedBuckets.Exists(bucket) {
		return BucketMultipartResourcesMetadata{}, probe.NewError(BucketNotFound{Bucket: bucket})
	}

	storedBucket := xl.storedBuckets.Get(bucket).(storedBucket)
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
func (xl API) ListObjectParts(bucket, key string, resources ObjectResourcesMetadata) (ObjectResourcesMetadata, *probe.Error) {
	// Verify upload id
	xl.lock.Lock()
	defer xl.lock.Unlock()

	if !IsValidBucket(bucket) {
		return ObjectResourcesMetadata{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(key) {
		return ObjectResourcesMetadata{}, probe.NewError(ObjectNameInvalid{Object: key})
	}

	// TODO: multipart support for xl is broken, since we haven't finalized the format in which
	//       it can be stored, disabling this for now until we get the underlying layout stable.
	//
	//	if len(xl.config.NodeDiskMap) > 0 {
	//		return xl.listObjectParts(bucket, key, resources)
	//	}

	if !xl.storedBuckets.Exists(bucket) {
		return ObjectResourcesMetadata{}, probe.NewError(BucketNotFound{Bucket: bucket})
	}
	storedBucket := xl.storedBuckets.Get(bucket).(storedBucket)
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
func (xl API) evictedPart(a ...interface{}) {
	// loop through all buckets
	buckets := xl.storedBuckets.GetAll()
	for bucketName, bucket := range buckets {
		b := bucket.(storedBucket)
		xl.storedBuckets.Set(bucketName, b)
	}
	debug.FreeOSMemory()
}
