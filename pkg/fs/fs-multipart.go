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
	"github.com/minio/minio/pkg/mimedb"
)

// isValidUploadID - is upload id.
func (fs Filesystem) isValidUploadID(object, uploadID string) (ok bool) {
	fs.rwLock.RLock()
	defer fs.rwLock.RUnlock()
	_, ok = fs.multiparts.ActiveSession[uploadID]
	if !ok {
		return
	}
	return
}

// ListMultipartUploads - list incomplete multipart sessions for a given BucketMultipartResourcesMetadata
func (fs Filesystem) ListMultipartUploads(bucket string, resources BucketMultipartResourcesMetadata) (BucketMultipartResourcesMetadata, *probe.Error) {
	// Input validation.
	if !IsValidBucketName(bucket) {
		return BucketMultipartResourcesMetadata{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	bucket = fs.denormalizeBucket(bucket)
	bucketPath := filepath.Join(fs.path, bucket)
	if _, e := os.Stat(bucketPath); e != nil {
		// Check bucket exists.
		if os.IsNotExist(e) {
			return BucketMultipartResourcesMetadata{}, probe.NewError(BucketNotFound{Bucket: bucket})
		}
		return BucketMultipartResourcesMetadata{}, probe.NewError(e)
	}
	var uploads []*UploadMetadata
	fs.rwLock.RLock()
	defer fs.rwLock.RUnlock()
	for uploadID, session := range fs.multiparts.ActiveSession {
		objectName := session.ObjectName
		if strings.HasPrefix(objectName, resources.Prefix) {
			if len(uploads) > resources.MaxUploads {
				sort.Sort(byUploadMetadataKey(uploads))
				resources.Upload = uploads
				resources.NextKeyMarker = session.ObjectName
				resources.NextUploadIDMarker = uploadID
				resources.IsTruncated = true
				return resources, nil
			}
			// UploadIDMarker is ignored if KeyMarker is empty.
			switch {
			case resources.KeyMarker != "" && resources.UploadIDMarker == "":
				if objectName > resources.KeyMarker {
					upload := new(UploadMetadata)
					upload.Object = objectName
					upload.UploadID = uploadID
					upload.Initiated = session.Initiated
					uploads = append(uploads, upload)
				}
			case resources.KeyMarker != "" && resources.UploadIDMarker != "":
				if session.UploadID > resources.UploadIDMarker {
					if objectName >= resources.KeyMarker {
						upload := new(UploadMetadata)
						upload.Object = objectName
						upload.UploadID = uploadID
						upload.Initiated = session.Initiated
						uploads = append(uploads, upload)
					}
				}
			default:
				upload := new(UploadMetadata)
				upload.Object = objectName
				upload.UploadID = uploadID
				upload.Initiated = session.Initiated
				uploads = append(uploads, upload)
			}
		}
	}
	sort.Sort(byUploadMetadataKey(uploads))
	resources.Upload = uploads
	return resources, nil
}

// verify if parts sent over the network do really match with what we
// have for the session.
func doPartsMatch(parts []CompletePart, savedParts []PartMetadata) bool {
	if parts == nil || savedParts == nil {
		return false
	}
	// Range of incoming parts and compare them with saved parts.
	for i, part := range parts {
		if strings.Trim(part.ETag, "\"") != savedParts[i].ETag {
			return false
		}
	}
	return true
}

type multiCloser struct {
	Closers []io.Closer
}

func (m multiCloser) Close() error {
	for _, c := range m.Closers {
		if e := c.Close(); e != nil {
			return e
		}
	}
	return nil
}

// MultiCloser - returns a Closer that's the logical
// concatenation of the provided input closers. They're closed
// sequentially. If any of the closers return a non-nil error, Close
// will return that error.
func MultiCloser(closers ...io.Closer) io.Closer {
	return multiCloser{closers}
}

// removeParts - remove all parts.
func removeParts(partPathPrefix string, parts []PartMetadata) *probe.Error {
	for _, part := range parts {
		// We are on purpose ignoring the return values here, since
		// another thread would have purged these entries.
		os.Remove(partPathPrefix + part.ETag + fmt.Sprintf("$%d-$multiparts", part.PartNumber))
	}
	return nil
}

// saveParts - concantenate and save all parts.
func saveParts(partPathPrefix string, mw io.Writer, parts []CompletePart) *probe.Error {
	var partReaders []io.Reader
	var partClosers []io.Closer
	for _, part := range parts {
		md5Sum := strings.TrimPrefix(part.ETag, "\"")
		md5Sum = strings.TrimSuffix(md5Sum, "\"")
		partFile, e := os.OpenFile(partPathPrefix+md5Sum+fmt.Sprintf("$%d-$multiparts", part.PartNumber), os.O_RDONLY, 0600)
		if e != nil {
			return probe.NewError(e)
		}
		partReaders = append(partReaders, partFile)
		partClosers = append(partClosers, partFile)
	}
	// Concatenate a list of closers and close upon return.
	closer := MultiCloser(partClosers...)
	defer closer.Close()

	reader := io.MultiReader(partReaders...)
	readBuffer := make([]byte, 4*1024*1024)
	if _, e := io.CopyBuffer(mw, reader, readBuffer); e != nil {
		return probe.NewError(e)
	}
	return nil
}

// NewMultipartUpload - initiate a new multipart session
func (fs Filesystem) NewMultipartUpload(bucket, object string) (string, *probe.Error) {
	di, e := disk.GetInfo(fs.path)
	if e != nil {
		return "", probe.NewError(e)
	}

	// Remove 5% from total space for cumulative disk space used for
	// journalling, inodes etc.
	availableDiskSpace := (float64(di.Free) / (float64(di.Total) - (0.05 * float64(di.Total)))) * 100
	if int64(availableDiskSpace) <= fs.minFreeDisk {
		return "", probe.NewError(RootPathFull{Path: fs.path})
	}

	// Input validation.
	if !IsValidBucketName(bucket) {
		return "", probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(object) {
		return "", probe.NewError(ObjectNameInvalid{Object: object})
	}

	bucket = fs.denormalizeBucket(bucket)
	bucketPath := filepath.Join(fs.path, bucket)
	if _, e = os.Stat(bucketPath); e != nil {
		// Check bucket exists.
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

	// Generate new upload id.
	id := []byte(strconv.FormatInt(rand.Int63(), 10) + bucket + object + time.Now().String())
	uploadIDSum := sha512.Sum512(id)
	uploadID := base64.URLEncoding.EncodeToString(uploadIDSum[:])[:47]

	fs.rwLock.Lock()
	// Initialize multipart session.
	mpartSession := &MultipartSession{}
	mpartSession.TotalParts = 0
	mpartSession.ObjectName = object
	mpartSession.UploadID = uploadID
	mpartSession.Initiated = time.Now().UTC()
	var parts []PartMetadata
	mpartSession.Parts = parts

	fs.multiparts.ActiveSession[uploadID] = mpartSession
	if err := saveMultipartsSession(*fs.multiparts); err != nil {
		fs.rwLock.Unlock()
		return "", err.Trace(objectPath)
	}
	fs.rwLock.Unlock()
	return uploadID, nil
}

// partNumber is a sortable interface for Part slice.
type partNumber []PartMetadata

func (a partNumber) Len() int           { return len(a) }
func (a partNumber) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a partNumber) Less(i, j int) bool { return a[i].PartNumber < a[j].PartNumber }

// CreateObjectPart - create a part in a multipart session
func (fs Filesystem) CreateObjectPart(bucket, object, uploadID, expectedMD5Sum string, partID int, size int64, data io.Reader, signature *Signature) (string, *probe.Error) {
	di, err := disk.GetInfo(fs.path)
	if err != nil {
		return "", probe.NewError(err)
	}

	// Remove 5% from total space for cumulative disk space used for
	// journalling, inodes etc.
	availableDiskSpace := (float64(di.Free) / (float64(di.Total) - (0.05 * float64(di.Total)))) * 100
	if int64(availableDiskSpace) <= fs.minFreeDisk {
		return "", probe.NewError(RootPathFull{Path: fs.path})
	}

	// Part id cannot be negative.
	if partID <= 0 {
		return "", probe.NewError(errors.New("invalid part id, cannot be zero or less than zero"))
	}

	// Check bucket name valid.
	if !IsValidBucketName(bucket) {
		return "", probe.NewError(BucketNameInvalid{Bucket: bucket})
	}

	// Verify object path legal.
	if !IsValidObjectName(object) {
		return "", probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}

	// Verify upload is valid for the incoming object.
	if !fs.isValidUploadID(object, uploadID) {
		return "", probe.NewError(InvalidUploadID{UploadID: uploadID})
	}

	if strings.TrimSpace(expectedMD5Sum) != "" {
		var expectedMD5SumBytes []byte
		expectedMD5SumBytes, err = base64.StdEncoding.DecodeString(strings.TrimSpace(expectedMD5Sum))
		if err != nil {
			// Pro-actively close the connection
			return "", probe.NewError(InvalidDigest{MD5: expectedMD5Sum})
		}
		expectedMD5Sum = hex.EncodeToString(expectedMD5SumBytes)
	}

	bucket = fs.denormalizeBucket(bucket)
	bucketPath := filepath.Join(fs.path, bucket)
	if _, err = os.Stat(bucketPath); err != nil {
		// Check bucket exists.
		if os.IsNotExist(err) {
			return "", probe.NewError(BucketNotFound{Bucket: bucket})
		}
		return "", probe.NewError(err)
	}

	objectPath := filepath.Join(bucketPath, object)
	partPathPrefix := objectPath + uploadID
	partPath := partPathPrefix + expectedMD5Sum + fmt.Sprintf("$%d-$multiparts", partID)
	partFile, e := atomic.FileCreateWithPrefix(partPath, "$multiparts")
	if e != nil {
		return "", probe.NewError(e)
	}

	md5Hasher := md5.New()
	sha256Hasher := sha256.New()
	partWriter := io.MultiWriter(partFile, md5Hasher, sha256Hasher)
	if _, e = io.CopyN(partWriter, data, size); e != nil {
		partFile.CloseAndPurge()
		return "", probe.NewError(e)
	}

	md5sum := hex.EncodeToString(md5Hasher.Sum(nil))
	// Verify if the written object is equal to what is expected, only
	// if it is requested as such.
	if strings.TrimSpace(expectedMD5Sum) != "" {
		if !isMD5SumEqual(strings.TrimSpace(expectedMD5Sum), md5sum) {
			partFile.CloseAndPurge()
			return "", probe.NewError(BadDigest{MD5: expectedMD5Sum, Bucket: bucket, Object: object})
		}
	}
	if signature != nil {
		ok, err := signature.DoesSignatureMatch(hex.EncodeToString(sha256Hasher.Sum(nil)))
		if err != nil {
			partFile.CloseAndPurge()
			return "", err.Trace()
		}
		if !ok {
			partFile.CloseAndPurge()
			return "", probe.NewError(SignatureDoesNotMatch{})
		}
	}
	partFile.Close()

	fi, e := os.Stat(partPath)
	if e != nil {
		return "", probe.NewError(e)
	}
	partMetadata := PartMetadata{}
	partMetadata.ETag = md5sum
	partMetadata.PartNumber = partID
	partMetadata.Size = fi.Size()
	partMetadata.LastModified = fi.ModTime()

	fs.rwLock.RLock()
	deserializedMultipartSession, ok := fs.multiparts.ActiveSession[uploadID]
	fs.rwLock.RUnlock()
	if !ok {
		return "", probe.NewError(InvalidUploadID{UploadID: uploadID})
	}

	// Append any pre-existing partNumber with new metadata, otherwise
	// append to the list.
	if len(deserializedMultipartSession.Parts) < partID {
		deserializedMultipartSession.Parts = append(deserializedMultipartSession.Parts, partMetadata)
	} else {
		deserializedMultipartSession.Parts[partID-1] = partMetadata
	}
	deserializedMultipartSession.TotalParts = len(deserializedMultipartSession.Parts)
	// Sort by part number before saving.
	sort.Sort(partNumber(deserializedMultipartSession.Parts))

	fs.rwLock.Lock()
	fs.multiparts.ActiveSession[uploadID] = deserializedMultipartSession
	if err := saveMultipartsSession(*fs.multiparts); err != nil {
		fs.rwLock.Unlock()
		return "", err.Trace(partPathPrefix)
	}
	fs.rwLock.Unlock()

	return partMetadata.ETag, nil
}

// CompleteMultipartUpload - complete a multipart upload and persist the data
func (fs Filesystem) CompleteMultipartUpload(bucket, object, uploadID string, data io.Reader, signature *Signature) (ObjectMetadata, *probe.Error) {
	// Check bucket name is valid.
	if !IsValidBucketName(bucket) {
		return ObjectMetadata{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}

	// Verify object path is legal.
	if !IsValidObjectName(object) {
		return ObjectMetadata{}, probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}

	// Verify if valid upload for incoming object.
	if !fs.isValidUploadID(object, uploadID) {
		return ObjectMetadata{}, probe.NewError(InvalidUploadID{UploadID: uploadID})
	}

	bucket = fs.denormalizeBucket(bucket)
	bucketPath := filepath.Join(fs.path, bucket)
	if _, e := os.Stat(bucketPath); e != nil {
		// Check bucket exists.
		if os.IsNotExist(e) {
			return ObjectMetadata{}, probe.NewError(BucketNotFound{Bucket: bucket})
		}
		return ObjectMetadata{}, probe.NewError(InternalError{})
	}

	objectPath := filepath.Join(bucketPath, object)
	file, e := atomic.FileCreateWithPrefix(objectPath, "$tmpobject")
	if e != nil {
		return ObjectMetadata{}, probe.NewError(e)
	}
	md5Hasher := md5.New()
	objectWriter := io.MultiWriter(file, md5Hasher)

	partBytes, e := ioutil.ReadAll(data)
	if e != nil {
		file.CloseAndPurge()
		return ObjectMetadata{}, probe.NewError(e)
	}
	if signature != nil {
		sh := sha256.New()
		sh.Write(partBytes)
		ok, err := signature.DoesSignatureMatch(hex.EncodeToString(sh.Sum(nil)))
		if err != nil {
			file.CloseAndPurge()
			return ObjectMetadata{}, err.Trace()
		}
		if !ok {
			file.CloseAndPurge()
			return ObjectMetadata{}, probe.NewError(SignatureDoesNotMatch{})
		}
	}
	completeMultipartUpload := &CompleteMultipartUpload{}
	if e := xml.Unmarshal(partBytes, completeMultipartUpload); e != nil {
		file.CloseAndPurge()
		return ObjectMetadata{}, probe.NewError(MalformedXML{})
	}
	if !sort.IsSorted(completedParts(completeMultipartUpload.Part)) {
		file.CloseAndPurge()
		return ObjectMetadata{}, probe.NewError(InvalidPartOrder{})
	}

	// Save parts for verification.
	parts := completeMultipartUpload.Part

	fs.rwLock.RLock()
	savedParts := fs.multiparts.ActiveSession[uploadID].Parts
	fs.rwLock.RUnlock()

	if !doPartsMatch(parts, savedParts) {
		file.CloseAndPurge()
		return ObjectMetadata{}, probe.NewError(InvalidPart{})
	}

	partPathPrefix := objectPath + uploadID
	if err := saveParts(partPathPrefix, objectWriter, parts); err != nil {
		file.CloseAndPurge()
		return ObjectMetadata{}, err.Trace(partPathPrefix)
	}
	// Successfully saved, remove all parts.
	removeParts(partPathPrefix, savedParts)

	fs.rwLock.Lock()
	delete(fs.multiparts.ActiveSession, uploadID)
	if err := saveMultipartsSession(*fs.multiparts); err != nil {
		fs.rwLock.Unlock()
		file.CloseAndPurge()
		return ObjectMetadata{}, err.Trace(partPathPrefix)
	}
	fs.rwLock.Unlock()

	file.Close()

	st, e := os.Stat(objectPath)
	if e != nil {
		return ObjectMetadata{}, probe.NewError(e)
	}
	contentType := "application/octet-stream"
	if objectExt := filepath.Ext(objectPath); objectExt != "" {
		content, ok := mimedb.DB[strings.ToLower(strings.TrimPrefix(objectExt, "."))]
		if ok {
			contentType = content.ContentType
		}
	}
	newObject := ObjectMetadata{
		Bucket:      bucket,
		Object:      object,
		Created:     st.ModTime(),
		Size:        st.Size(),
		ContentType: contentType,
		MD5:         hex.EncodeToString(md5Hasher.Sum(nil)),
	}
	return newObject, nil
}

// ListObjectParts - list parts from incomplete multipart session for a given ObjectResourcesMetadata
func (fs Filesystem) ListObjectParts(bucket, object string, resources ObjectResourcesMetadata) (ObjectResourcesMetadata, *probe.Error) {
	// Check bucket name is valid.
	if !IsValidBucketName(bucket) {
		return ObjectResourcesMetadata{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}

	// Verify object path legal.
	if !IsValidObjectName(object) {
		return ObjectResourcesMetadata{}, probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}

	// Save upload id.
	uploadID := resources.UploadID

	// Verify if upload id is valid for incoming object.
	if !fs.isValidUploadID(object, uploadID) {
		return ObjectResourcesMetadata{}, probe.NewError(InvalidUploadID{UploadID: uploadID})
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
		// Check bucket exists.
		if os.IsNotExist(e) {
			return ObjectResourcesMetadata{}, probe.NewError(BucketNotFound{Bucket: bucket})
		}
		return ObjectResourcesMetadata{}, probe.NewError(e)
	}

	fs.rwLock.RLock()
	deserializedMultipartSession, ok := fs.multiparts.ActiveSession[uploadID]
	fs.rwLock.RUnlock()
	if !ok {
		return ObjectResourcesMetadata{}, probe.NewError(InvalidUploadID{UploadID: resources.UploadID})
	}
	var parts []PartMetadata
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
	// Check bucket name valid.
	if !IsValidBucketName(bucket) {
		return probe.NewError(BucketNameInvalid{Bucket: bucket})
	}

	// Verify object path legal.
	if !IsValidObjectName(object) {
		return probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}

	if !fs.isValidUploadID(object, uploadID) {
		return probe.NewError(InvalidUploadID{UploadID: uploadID})
	}

	bucket = fs.denormalizeBucket(bucket)
	bucketPath := filepath.Join(fs.path, bucket)
	if _, e := os.Stat(bucketPath); e != nil {
		// Check bucket exists.
		if os.IsNotExist(e) {
			return probe.NewError(BucketNotFound{Bucket: bucket})
		}
		return probe.NewError(e)
	}

	objectPath := filepath.Join(bucketPath, object)
	partPathPrefix := objectPath + uploadID
	fs.rwLock.RLock()
	savedParts := fs.multiparts.ActiveSession[uploadID].Parts
	fs.rwLock.RUnlock()

	if err := removeParts(partPathPrefix, savedParts); err != nil {
		return err.Trace(partPathPrefix)
	}

	fs.rwLock.Lock()
	delete(fs.multiparts.ActiveSession, uploadID)
	if err := saveMultipartsSession(*fs.multiparts); err != nil {
		fs.rwLock.Unlock()
		return err.Trace(partPathPrefix)
	}
	fs.rwLock.Unlock()
	return nil
}
