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

package main

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio/pkg/atomic"
	"github.com/minio/minio/pkg/crypto/sha512"
	"github.com/minio/minio/pkg/disk"
	"github.com/minio/minio/pkg/mimedb"
	"github.com/minio/minio/pkg/probe"
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

// byObjectInfoKey is a sortable interface for UploadMetadata slice
type byUploadMetadataKey []*UploadMetadata

func (b byUploadMetadataKey) Len() int           { return len(b) }
func (b byUploadMetadataKey) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byUploadMetadataKey) Less(i, j int) bool { return b[i].Object < b[j].Object }

// ListMultipartUploads - list incomplete multipart sessions for a given BucketMultipartResourcesMetadata
func (fs Filesystem) ListMultipartUploads(bucket string, resources BucketMultipartResourcesMetadata) (BucketMultipartResourcesMetadata, *probe.Error) {
	// Input validation.
	if !IsValidBucketName(bucket) {
		return BucketMultipartResourcesMetadata{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	bucket = getActualBucketname(fs.path, bucket)
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
	if len(parts) != len(savedParts) {
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

// Create an s3 compatible MD5sum for complete multipart transaction.
func makeS3MD5(md5Strs ...string) (string, *probe.Error) {
	var finalMD5Bytes []byte
	for _, md5Str := range md5Strs {
		md5Bytes, e := hex.DecodeString(md5Str)
		if e != nil {
			return "", probe.NewError(e)
		}
		finalMD5Bytes = append(finalMD5Bytes, md5Bytes...)
	}
	md5Hasher := md5.New()
	md5Hasher.Write(finalMD5Bytes)
	s3MD5 := fmt.Sprintf("%s-%d", hex.EncodeToString(md5Hasher.Sum(nil)), len(md5Strs))
	return s3MD5, nil
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
		// Trim prefix
		md5Sum := strings.TrimPrefix(part.ETag, "\"")
		// Trim suffix
		md5Sum = strings.TrimSuffix(md5Sum, "\"")
		partFile, e := os.OpenFile(partPathPrefix+md5Sum+fmt.Sprintf("$%d-$multiparts", part.PartNumber), os.O_RDONLY, 0600)
		if e != nil {
			if !os.IsNotExist(e) {
				return probe.NewError(e)
			}
			// Some clients do not set Content-Md5, so we would have
			// created part files without 'ETag' in them.
			partFile, e = os.OpenFile(partPathPrefix+fmt.Sprintf("$%d-$multiparts", part.PartNumber), os.O_RDONLY, 0600)
			if e != nil {
				return probe.NewError(e)
			}
		}
		partReaders = append(partReaders, partFile)
		partClosers = append(partClosers, partFile)
	}
	// Concatenate a list of closers and close upon return.
	closer := MultiCloser(partClosers...)
	defer closer.Close()

	reader := io.MultiReader(partReaders...)
	readBufferSize := 8 * 1024 * 1024          // 8MiB
	readBuffer := make([]byte, readBufferSize) // Allocate 8MiB buffer.
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

	bucket = getActualBucketname(fs.path, bucket)
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

	// Critical region requiring write lock.
	fs.rwLock.Lock()
	defer fs.rwLock.Unlock()
	// Initialize multipart session.
	mpartSession := &MultipartSession{}
	mpartSession.TotalParts = 0
	mpartSession.ObjectName = object
	mpartSession.UploadID = uploadID
	mpartSession.Initiated = time.Now().UTC()
	// Multipart has maximum of 10000 parts.
	var parts []PartMetadata
	mpartSession.Parts = parts

	fs.multiparts.ActiveSession[uploadID] = mpartSession
	if err := saveMultipartsSession(*fs.multiparts); err != nil {
		return "", err.Trace(objectPath)
	}
	return uploadID, nil
}

// Remove all duplicated parts based on the latest time of their upload.
func removeDuplicateParts(parts []PartMetadata) []PartMetadata {
	length := len(parts) - 1
	for i := 0; i < length; i++ {
		for j := i + 1; j <= length; j++ {
			if parts[i].PartNumber == parts[j].PartNumber {
				if parts[i].LastModified.Sub(parts[j].LastModified) > 0 {
					parts[i] = parts[length]
				} else {
					parts[j] = parts[length]
				}
				parts = parts[0:length]
				length--
				j--
			}
		}
	}
	return parts
}

// partNumber is a sortable interface for Part slice.
type partNumber []PartMetadata

func (a partNumber) Len() int           { return len(a) }
func (a partNumber) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a partNumber) Less(i, j int) bool { return a[i].PartNumber < a[j].PartNumber }

// PutObjectPart - create a part in a multipart session
func (fs Filesystem) PutObjectPart(bucket, object, uploadID string, partID int, size int64, data io.Reader, md5Hex string) (string, *probe.Error) {
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

	bucket = getActualBucketname(fs.path, bucket)
	bucketPath := filepath.Join(fs.path, bucket)
	if _, e := os.Stat(bucketPath); e != nil {
		// Check bucket exists.
		if os.IsNotExist(e) {
			return "", probe.NewError(BucketNotFound{Bucket: bucket})
		}
		return "", probe.NewError(e)
	}

	objectPath := filepath.Join(bucketPath, object)
	partPathPrefix := objectPath + uploadID
	partPath := partPathPrefix + md5Hex + fmt.Sprintf("$%d-$multiparts", partID)
	partFile, e := atomic.FileCreateWithPrefix(partPath, "$multiparts")
	if e != nil {
		return "", probe.NewError(e)
	}
	defer partFile.Close()

	// Initialize md5 writer.
	md5Writer := md5.New()

	// Create a multiwriter.
	multiWriter := io.MultiWriter(md5Writer, partFile)

	if _, e = io.CopyN(multiWriter, data, size); e != nil {
		partFile.CloseAndPurge()
		return "", probe.NewError(e)
	}

	// Finalize new md5.
	newMD5Hex := hex.EncodeToString(md5Writer.Sum(nil))
	if md5Hex != "" {
		if newMD5Hex != md5Hex {
			return "", probe.NewError(BadDigest{md5Hex, newMD5Hex})
		}
	}

	// Stat the file to get the latest information.
	fi, e := os.Stat(partFile.Name())
	if e != nil {
		return "", probe.NewError(e)
	}
	partMetadata := PartMetadata{}
	partMetadata.PartNumber = partID
	partMetadata.ETag = newMD5Hex
	partMetadata.Size = fi.Size()
	partMetadata.LastModified = fi.ModTime()

	// Critical region requiring read lock.
	fs.rwLock.RLock()
	deserializedMultipartSession, ok := fs.multiparts.ActiveSession[uploadID]
	fs.rwLock.RUnlock()
	if !ok {
		return "", probe.NewError(InvalidUploadID{UploadID: uploadID})
	}

	// Add all incoming parts.
	deserializedMultipartSession.Parts = append(deserializedMultipartSession.Parts, partMetadata)

	// Remove duplicate parts based on the most recent uploaded.
	deserializedMultipartSession.Parts = removeDuplicateParts(deserializedMultipartSession.Parts)
	// Save total parts uploaded.
	deserializedMultipartSession.TotalParts = len(deserializedMultipartSession.Parts)

	// Sort by part number before saving.
	sort.Sort(partNumber(deserializedMultipartSession.Parts))

	// Critical region requiring write lock.
	fs.rwLock.Lock()
	defer fs.rwLock.Unlock()

	fs.multiparts.ActiveSession[uploadID] = deserializedMultipartSession
	if err := saveMultipartsSession(*fs.multiparts); err != nil {
		return "", err.Trace(partPathPrefix)
	}
	return newMD5Hex, nil
}

// CompleteMultipartUpload - complete a multipart upload and persist the data
func (fs Filesystem) CompleteMultipartUpload(bucket string, object string, uploadID string, parts []CompletePart) (ObjectInfo, *probe.Error) {
	// Check bucket name is valid.
	if !IsValidBucketName(bucket) {
		return ObjectInfo{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}

	// Verify object path is legal.
	if !IsValidObjectName(object) {
		return ObjectInfo{}, probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}

	// Verify if valid upload for incoming object.
	if !fs.isValidUploadID(object, uploadID) {
		return ObjectInfo{}, probe.NewError(InvalidUploadID{UploadID: uploadID})
	}

	bucket = getActualBucketname(fs.path, bucket)
	bucketPath := filepath.Join(fs.path, bucket)
	if _, e := os.Stat(bucketPath); e != nil {
		// Check bucket exists.
		if os.IsNotExist(e) {
			return ObjectInfo{}, probe.NewError(BucketNotFound{Bucket: bucket})
		}
		return ObjectInfo{}, probe.NewError(InternalError{})
	}

	objectPath := filepath.Join(bucketPath, object)
	objectWriter, e := atomic.FileCreateWithPrefix(objectPath, "$tmpobject")
	if e != nil {
		return ObjectInfo{}, probe.NewError(e)
	}

	// Critical region requiring read lock.
	fs.rwLock.RLock()
	savedParts := fs.multiparts.ActiveSession[uploadID].Parts
	fs.rwLock.RUnlock()

	if !doPartsMatch(parts, savedParts) {
		objectWriter.CloseAndPurge()
		return ObjectInfo{}, probe.NewError(InvalidPart{})
	}

	// Parts successfully validated, save all the parts.
	partPathPrefix := objectPath + uploadID
	if err := saveParts(partPathPrefix, objectWriter, parts); err != nil {
		objectWriter.CloseAndPurge()
		return ObjectInfo{}, err.Trace(partPathPrefix)
	}
	var md5Strs []string
	for _, part := range savedParts {
		md5Strs = append(md5Strs, part.ETag)
	}
	// Save the s3 md5.
	s3MD5, err := makeS3MD5(md5Strs...)
	if err != nil {
		objectWriter.CloseAndPurge()
		return ObjectInfo{}, err.Trace(md5Strs...)
	}

	// Successfully saved multipart, remove all parts in a routine.
	go removeParts(partPathPrefix, savedParts)

	// Critical region requiring write lock.
	fs.rwLock.Lock()
	delete(fs.multiparts.ActiveSession, uploadID)
	if err := saveMultipartsSession(*fs.multiparts); err != nil {
		fs.rwLock.Unlock()
		objectWriter.CloseAndPurge()
		return ObjectInfo{}, err.Trace(partPathPrefix)
	}
	if e = objectWriter.Close(); e != nil {
		fs.rwLock.Unlock()
		return ObjectInfo{}, probe.NewError(e)
	}
	fs.rwLock.Unlock()

	// Send stat again to get object metadata.
	st, e := os.Stat(objectPath)
	if e != nil {
		return ObjectInfo{}, probe.NewError(e)
	}

	contentType := "application/octet-stream"
	if objectExt := filepath.Ext(objectPath); objectExt != "" {
		content, ok := mimedb.DB[strings.ToLower(strings.TrimPrefix(objectExt, "."))]
		if ok {
			contentType = content.ContentType
		}
	}
	newObject := ObjectInfo{
		Bucket:       bucket,
		Name:         object,
		ModifiedTime: st.ModTime(),
		Size:         st.Size(),
		ContentType:  contentType,
		MD5Sum:       s3MD5,
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

	bucket = getActualBucketname(fs.path, bucket)
	bucketPath := filepath.Join(fs.path, bucket)
	if _, e := os.Stat(bucketPath); e != nil {
		// Check bucket exists.
		if os.IsNotExist(e) {
			return ObjectResourcesMetadata{}, probe.NewError(BucketNotFound{Bucket: bucket})
		}
		return ObjectResourcesMetadata{}, probe.NewError(e)
	}

	// Critical region requiring read lock.
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

	bucket = getActualBucketname(fs.path, bucket)
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

	// Critical region requiring read lock.
	fs.rwLock.RLock()
	savedParts := fs.multiparts.ActiveSession[uploadID].Parts
	fs.rwLock.RUnlock()

	// Remove all parts.
	if err := removeParts(partPathPrefix, savedParts); err != nil {
		return err.Trace(partPathPrefix)
	}

	// Critical region requiring write lock.
	fs.rwLock.Lock()
	defer fs.rwLock.Unlock()

	delete(fs.multiparts.ActiveSession, uploadID)
	if err := saveMultipartsSession(*fs.multiparts); err != nil {
		return err.Trace(partPathPrefix)
	}
	return nil
}
