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

package fs

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio-xl/pkg/atomic"
	"github.com/minio/minio-xl/pkg/crypto/sha256"
	"github.com/minio/minio-xl/pkg/crypto/sha512"
	"github.com/minio/minio-xl/pkg/probe"
	"github.com/minio/minio/pkg/disk"
)

func (fs Filesystem) isValidUploadID(object, uploadID string) bool {
	s, ok := fs.multiparts.ActiveSession[object]
	if !ok {
		return false
	}
	if uploadID == s.UploadID {
		return true
	}
	return false
}

// ListMultipartUploads - list incomplete multipart sessions for a given BucketMultipartResourcesMetadata
func (fs Filesystem) ListMultipartUploads(bucket string, resources BucketMultipartResourcesMetadata) (BucketMultipartResourcesMetadata, *probe.Error) {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	if !IsValidBucketName(bucket) {
		return BucketMultipartResourcesMetadata{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	bucket = fs.denormalizeBucket(bucket)
	bucketPath := filepath.Join(fs.path, bucket)
	if _, e := os.Stat(bucketPath); e != nil {
		// check bucket exists
		if os.IsNotExist(e) {
			return BucketMultipartResourcesMetadata{}, probe.NewError(BucketNotFound{Bucket: bucket})
		}
		return BucketMultipartResourcesMetadata{}, probe.NewError(e)
	}
	var uploads []*UploadMetadata
	for object, session := range fs.multiparts.ActiveSession {
		if strings.HasPrefix(object, resources.Prefix) {
			if len(uploads) > resources.MaxUploads {
				sort.Sort(byUploadMetadataKey(uploads))
				resources.Upload = uploads
				resources.NextKeyMarker = object
				resources.NextUploadIDMarker = session.UploadID
				resources.IsTruncated = true
				return resources, nil
			}
			// uploadIDMarker is ignored if KeyMarker is empty
			switch {
			case resources.KeyMarker != "" && resources.UploadIDMarker == "":
				if object > resources.KeyMarker {
					upload := new(UploadMetadata)
					upload.Object = object
					upload.UploadID = session.UploadID
					upload.Initiated = session.Initiated
					uploads = append(uploads, upload)
				}
			case resources.KeyMarker != "" && resources.UploadIDMarker != "":
				if session.UploadID > resources.UploadIDMarker {
					if object >= resources.KeyMarker {
						upload := new(UploadMetadata)
						upload.Object = object
						upload.UploadID = session.UploadID
						upload.Initiated = session.Initiated
						uploads = append(uploads, upload)
					}
				}
			default:
				upload := new(UploadMetadata)
				upload.Object = object
				upload.UploadID = session.UploadID
				upload.Initiated = session.Initiated
				uploads = append(uploads, upload)
			}
		}
	}
	sort.Sort(byUploadMetadataKey(uploads))
	resources.Upload = uploads
	return resources, nil
}

func (fs Filesystem) concatParts(parts *CompleteMultipartUpload, objectPath string, mw io.Writer) *probe.Error {
	for _, part := range parts.Part {
		partFile, e := os.OpenFile(objectPath+fmt.Sprintf("$%d-$multiparts", part.PartNumber), os.O_RDONLY, 0600)
		defer partFile.Close()
		if e != nil {
			return probe.NewError(e)
		}

		recvMD5 := part.ETag
		// complete multipart request header md5sum per part is hex encoded
		// trim it and decode if possible.
		_, e = hex.DecodeString(strings.Trim(recvMD5, "\""))
		if e != nil {
			return probe.NewError(InvalidDigest{Md5: recvMD5})
		}

		_, e = io.Copy(mw, partFile)
		if e != nil {
			return probe.NewError(e)
		}
	}
	return nil
}

// NewMultipartUpload - initiate a new multipart session
func (fs Filesystem) NewMultipartUpload(bucket, object string) (string, *probe.Error) {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	di, e := disk.GetInfo(fs.path)
	if e != nil {
		return "", probe.NewError(e)
	}

	// Remove 5% from total space for cumulative disk space used for journalling, inodes etc.
	availableDiskSpace := (float64(di.Free) / (float64(di.Total) - (0.05 * float64(di.Total)))) * 100
	if int64(availableDiskSpace) <= fs.minFreeDisk {
		return "", probe.NewError(RootPathFull{Path: fs.path})
	}

	if !IsValidBucketName(bucket) {
		return "", probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(object) {
		return "", probe.NewError(ObjectNameInvalid{Object: object})
	}

	bucket = fs.denormalizeBucket(bucket)
	bucketPath := filepath.Join(fs.path, bucket)
	if _, e = os.Stat(bucketPath); e != nil {
		// check bucket exists
		if os.IsNotExist(e) {
			return "", probe.NewError(BucketNotFound{Bucket: bucket})
		}
		return "", probe.NewError(e)
	}

	objectPath := filepath.Join(bucketPath, object)
	objectDir := filepath.Dir(objectPath)
	if _, e = os.Stat(objectDir); e != nil {
		if !os.IsNotExist(e) {
			return "", probe.NewError(e)
		}
		e = os.MkdirAll(objectDir, 0700)
		if e != nil {
			return "", probe.NewError(e)
		}
	}

	id := []byte(strconv.FormatInt(rand.Int63(), 10) + bucket + object + time.Now().String())
	uploadIDSum := sha512.Sum512(id)
	uploadID := base64.URLEncoding.EncodeToString(uploadIDSum[:])[:47]

	multiPartfile, e := os.OpenFile(objectPath+"$multiparts", os.O_WRONLY|os.O_CREATE, 0600)
	if e != nil {
		return "", probe.NewError(e)
	}
	defer multiPartfile.Close()

	mpartSession := &MultipartSession{}
	mpartSession.TotalParts = 0
	mpartSession.UploadID = uploadID
	mpartSession.Initiated = time.Now().UTC()
	var parts []*PartMetadata
	mpartSession.Parts = parts
	fs.multiparts.ActiveSession[object] = mpartSession

	encoder := json.NewEncoder(multiPartfile)
	if e = encoder.Encode(mpartSession); e != nil {
		return "", probe.NewError(e)
	}
	if err := saveMultipartsSession(fs.multiparts); err != nil {
		return "", err.Trace()
	}
	return uploadID, nil
}

// partNumber is a sortable interface for Part slice
type partNumber []*PartMetadata

func (a partNumber) Len() int           { return len(a) }
func (a partNumber) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a partNumber) Less(i, j int) bool { return a[i].PartNumber < a[j].PartNumber }

// CreateObjectPart - create a part in a multipart session
func (fs Filesystem) CreateObjectPart(bucket, object, uploadID, expectedMD5Sum string, partID int, size int64, data io.Reader, signature *Signature) (string, *probe.Error) {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	di, err := disk.GetInfo(fs.path)
	if err != nil {
		return "", probe.NewError(err)
	}

	// Remove 5% from total space for cumulative disk space used for journalling, inodes etc.
	availableDiskSpace := (float64(di.Free) / (float64(di.Total) - (0.05 * float64(di.Total)))) * 100
	if int64(availableDiskSpace) <= fs.minFreeDisk {
		return "", probe.NewError(RootPathFull{Path: fs.path})
	}

	if partID <= 0 {
		return "", probe.NewError(errors.New("invalid part id, cannot be zero or less than zero"))
	}
	// check bucket name valid
	if !IsValidBucketName(bucket) {
		return "", probe.NewError(BucketNameInvalid{Bucket: bucket})
	}

	// verify object path legal
	if !IsValidObjectName(object) {
		return "", probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}

	if !fs.isValidUploadID(object, uploadID) {
		return "", probe.NewError(InvalidUploadID{UploadID: uploadID})
	}

	if strings.TrimSpace(expectedMD5Sum) != "" {
		var expectedMD5SumBytes []byte
		expectedMD5SumBytes, err = base64.StdEncoding.DecodeString(strings.TrimSpace(expectedMD5Sum))
		if err != nil {
			// pro-actively close the connection
			return "", probe.NewError(InvalidDigest{Md5: expectedMD5Sum})
		}
		expectedMD5Sum = hex.EncodeToString(expectedMD5SumBytes)
	}

	bucket = fs.denormalizeBucket(bucket)
	bucketPath := filepath.Join(fs.path, bucket)
	if _, err = os.Stat(bucketPath); err != nil {
		// check bucket exists
		if os.IsNotExist(err) {
			return "", probe.NewError(BucketNotFound{Bucket: bucket})
		}
		return "", probe.NewError(err)
	}

	objectPath := filepath.Join(bucketPath, object)
	partPath := objectPath + fmt.Sprintf("$%d-$multiparts", partID)
	partFile, err := atomic.FileCreateWithPrefix(partPath, "$multiparts")
	if err != nil {
		return "", probe.NewError(err)
	}
	h := md5.New()
	sh := sha256.New()
	mw := io.MultiWriter(partFile, h, sh)
	_, err = io.CopyN(mw, data, size)
	if err != nil {
		partFile.CloseAndPurge()
		return "", probe.NewError(err)
	}
	md5sum := hex.EncodeToString(h.Sum(nil))
	// Verify if the written object is equal to what is expected, only if it is requested as such
	if strings.TrimSpace(expectedMD5Sum) != "" {
		if err := isMD5SumEqual(strings.TrimSpace(expectedMD5Sum), md5sum); err != nil {
			partFile.CloseAndPurge()
			return "", probe.NewError(BadDigest{Md5: expectedMD5Sum, Bucket: bucket, Object: object})
		}
	}
	if signature != nil {
		ok, perr := signature.DoesSignatureMatch(hex.EncodeToString(sh.Sum(nil)))
		if perr != nil {
			partFile.CloseAndPurge()
			return "", perr.Trace()
		}
		if !ok {
			partFile.CloseAndPurge()
			return "", probe.NewError(SignatureDoesNotMatch{})
		}
	}
	partFile.Close()

	fi, err := os.Stat(partPath)
	if err != nil {
		return "", probe.NewError(err)
	}
	partMetadata := PartMetadata{}
	partMetadata.ETag = md5sum
	partMetadata.PartNumber = partID
	partMetadata.Size = fi.Size()
	partMetadata.LastModified = fi.ModTime()

	multiPartfile, err := os.OpenFile(objectPath+"$multiparts", os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return "", probe.NewError(err)
	}
	defer multiPartfile.Close()

	var deserializedMultipartSession MultipartSession
	decoder := json.NewDecoder(multiPartfile)
	err = decoder.Decode(&deserializedMultipartSession)
	if err != nil {
		return "", probe.NewError(err)
	}
	deserializedMultipartSession.Parts = append(deserializedMultipartSession.Parts, &partMetadata)
	deserializedMultipartSession.TotalParts++
	fs.multiparts.ActiveSession[object] = &deserializedMultipartSession

	sort.Sort(partNumber(deserializedMultipartSession.Parts))
	encoder := json.NewEncoder(multiPartfile)
	err = encoder.Encode(&deserializedMultipartSession)
	if err != nil {
		return "", probe.NewError(err)
	}
	return partMetadata.ETag, nil
}

// CompleteMultipartUpload - complete a multipart upload and persist the data
func (fs Filesystem) CompleteMultipartUpload(bucket, object, uploadID string, data io.Reader, signature *Signature) (ObjectMetadata, *probe.Error) {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	// check bucket name valid
	if !IsValidBucketName(bucket) {
		return ObjectMetadata{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}

	// verify object path legal
	if !IsValidObjectName(object) {
		return ObjectMetadata{}, probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}

	if !fs.isValidUploadID(object, uploadID) {
		return ObjectMetadata{}, probe.NewError(InvalidUploadID{UploadID: uploadID})
	}

	bucket = fs.denormalizeBucket(bucket)
	bucketPath := filepath.Join(fs.path, bucket)
	if _, err := os.Stat(bucketPath); err != nil {
		// check bucket exists
		if os.IsNotExist(err) {
			return ObjectMetadata{}, probe.NewError(BucketNotFound{Bucket: bucket})
		}
		return ObjectMetadata{}, probe.NewError(InternalError{})
	}

	objectPath := filepath.Join(bucketPath, object)
	file, err := atomic.FileCreateWithPrefix(objectPath, "")
	if err != nil {
		return ObjectMetadata{}, probe.NewError(err)
	}
	h := md5.New()
	mw := io.MultiWriter(file, h)

	partBytes, err := ioutil.ReadAll(data)
	if err != nil {
		file.CloseAndPurge()
		return ObjectMetadata{}, probe.NewError(err)
	}
	if signature != nil {
		sh := sha256.New()
		sh.Write(partBytes)
		ok, perr := signature.DoesSignatureMatch(hex.EncodeToString(sh.Sum(nil)))
		if perr != nil {
			file.CloseAndPurge()
			return ObjectMetadata{}, probe.NewError(err)
		}
		if !ok {
			file.CloseAndPurge()
			return ObjectMetadata{}, probe.NewError(SignatureDoesNotMatch{})
		}
	}
	parts := &CompleteMultipartUpload{}
	if err := xml.Unmarshal(partBytes, parts); err != nil {
		file.CloseAndPurge()
		return ObjectMetadata{}, probe.NewError(MalformedXML{})
	}
	if !sort.IsSorted(completedParts(parts.Part)) {
		file.CloseAndPurge()
		return ObjectMetadata{}, probe.NewError(InvalidPartOrder{})
	}

	if err := fs.concatParts(parts, objectPath, mw); err != nil {
		file.CloseAndPurge()
		return ObjectMetadata{}, err.Trace()
	}

	delete(fs.multiparts.ActiveSession, object)
	for _, part := range parts.Part {
		err = os.Remove(objectPath + fmt.Sprintf("$%d-$multiparts", part.PartNumber))
		if err != nil {
			file.CloseAndPurge()
			return ObjectMetadata{}, probe.NewError(err)
		}
	}
	if err := os.Remove(objectPath + "$multiparts"); err != nil {
		file.CloseAndPurge()
		return ObjectMetadata{}, probe.NewError(err)
	}
	if err := saveMultipartsSession(fs.multiparts); err != nil {
		file.CloseAndPurge()
		return ObjectMetadata{}, err.Trace()
	}
	file.Close()

	st, err := os.Stat(objectPath)
	if err != nil {
		return ObjectMetadata{}, probe.NewError(err)
	}
	newObject := ObjectMetadata{
		Bucket:      bucket,
		Object:      object,
		Created:     st.ModTime(),
		Size:        st.Size(),
		ContentType: "application/octet-stream",
		Md5:         hex.EncodeToString(h.Sum(nil)),
	}
	return newObject, nil
}

// ListObjectParts - list parts from incomplete multipart session for a given ObjectResourcesMetadata
func (fs Filesystem) ListObjectParts(bucket, object string, resources ObjectResourcesMetadata) (ObjectResourcesMetadata, *probe.Error) {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	// check bucket name valid
	if !IsValidBucketName(bucket) {
		return ObjectResourcesMetadata{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}

	// verify object path legal
	if !IsValidObjectName(object) {
		return ObjectResourcesMetadata{}, probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}

	if !fs.isValidUploadID(object, resources.UploadID) {
		return ObjectResourcesMetadata{}, probe.NewError(InvalidUploadID{UploadID: resources.UploadID})
	}

	objectResourcesMetadata := resources
	objectResourcesMetadata.Bucket = bucket
	objectResourcesMetadata.Object = object
	var startPartNumber int
	switch {
	case objectResourcesMetadata.PartNumberMarker == 0:
		startPartNumber = 1
	default:
		startPartNumber = objectResourcesMetadata.PartNumberMarker
	}

	bucket = fs.denormalizeBucket(bucket)
	bucketPath := filepath.Join(fs.path, bucket)
	if _, e := os.Stat(bucketPath); e != nil {
		// check bucket exists
		if os.IsNotExist(e) {
			return ObjectResourcesMetadata{}, probe.NewError(BucketNotFound{Bucket: bucket})
		}
		return ObjectResourcesMetadata{}, probe.NewError(e)
	}

	objectPath := filepath.Join(bucketPath, object)
	multiPartfile, err := os.OpenFile(objectPath+"$multiparts", os.O_RDONLY, 0600)
	if err != nil {
		return ObjectResourcesMetadata{}, probe.NewError(err)
	}
	defer multiPartfile.Close()

	var deserializedMultipartSession MultipartSession
	decoder := json.NewDecoder(multiPartfile)
	err = decoder.Decode(&deserializedMultipartSession)
	if err != nil {
		return ObjectResourcesMetadata{}, probe.NewError(err)
	}
	var parts []*PartMetadata
	for i := startPartNumber; i <= deserializedMultipartSession.TotalParts; i++ {
		if len(parts) > objectResourcesMetadata.MaxParts {
			sort.Sort(partNumber(parts))
			objectResourcesMetadata.IsTruncated = true
			objectResourcesMetadata.Part = parts
			objectResourcesMetadata.NextPartNumberMarker = i
			return objectResourcesMetadata, nil
		}
		parts = append(parts, deserializedMultipartSession.Parts[i-1])
	}
	sort.Sort(partNumber(parts))
	objectResourcesMetadata.Part = parts
	return objectResourcesMetadata, nil
}

// AbortMultipartUpload - abort an incomplete multipart session
func (fs Filesystem) AbortMultipartUpload(bucket, object, uploadID string) *probe.Error {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	// check bucket name valid
	if !IsValidBucketName(bucket) {
		return probe.NewError(BucketNameInvalid{Bucket: bucket})
	}

	// verify object path legal
	if !IsValidObjectName(object) {
		return probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}

	if !fs.isValidUploadID(object, uploadID) {
		return probe.NewError(InvalidUploadID{UploadID: uploadID})
	}

	bucket = fs.denormalizeBucket(bucket)
	bucketPath := filepath.Join(fs.path, bucket)
	if _, e := os.Stat(bucketPath); e != nil {
		// check bucket exists
		if os.IsNotExist(e) {
			return probe.NewError(BucketNotFound{Bucket: bucket})
		}
		return probe.NewError(e)
	}

	objectPath := filepath.Join(bucketPath, object)
	for _, part := range fs.multiparts.ActiveSession[object].Parts {
		e := os.RemoveAll(objectPath + fmt.Sprintf("$%d-$multiparts", part.PartNumber))
		if e != nil {
			return probe.NewError(e)
		}
	}
	delete(fs.multiparts.ActiveSession, object)
	if e := os.RemoveAll(objectPath + "$multiparts"); e != nil {
		return probe.NewError(e)
	}
	return nil
}
