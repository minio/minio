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
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"

	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"runtime"

	"github.com/minio/minio/pkg/atomic"
	"github.com/minio/minio/pkg/crypto/sha256"
	"github.com/minio/minio/pkg/disk"
	"github.com/minio/minio/pkg/mimedb"
	"github.com/minio/minio/pkg/probe"
	"github.com/minio/minio/pkg/s3/signature4"
)

/// Object Operations

// GetObject - GET object
func (fs Filesystem) GetObject(w io.Writer, bucket, object string, start, length int64) (int64, *probe.Error) {
	// Critical region requiring read lock.
	fs.rwLock.RLock()
	defer fs.rwLock.RUnlock()

	// Input validation.
	if !IsValidBucketName(bucket) {
		return 0, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(object) {
		return 0, probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}

	// normalize buckets.
	bucket = fs.denormalizeBucket(bucket)
	objectPath := filepath.Join(fs.path, bucket, object)

	file, e := os.Open(objectPath)
	if e != nil {
		// If the object doesn't exist, the bucket might not exist either. Stat for
		// the bucket and give a better error message if that is true.
		if os.IsNotExist(e) {
			_, e = os.Stat(filepath.Join(fs.path, bucket))
			if os.IsNotExist(e) {
				return 0, probe.NewError(BucketNotFound{Bucket: bucket})
			}

			return 0, probe.NewError(ObjectNotFound{Bucket: bucket, Object: object})
		}
		return 0, probe.NewError(e)
	}
	defer file.Close()

	_, e = file.Seek(start, os.SEEK_SET)
	if e != nil {
		// When the "handle is invalid", the file might be a directory on Windows.
		if runtime.GOOS == "windows" && strings.Contains(e.Error(), "handle is invalid") {
			return 0, probe.NewError(ObjectNotFound{Bucket: bucket, Object: object})
		}

		return 0, probe.NewError(e)
	}

	var count int64
	// Copy over the whole file if the length is non-positive.
	if length > 0 {
		count, e = io.CopyN(w, file, length)
	} else {
		count, e = io.Copy(w, file)
	}

	if e != nil {
		// This call will fail if the object is a directory. Stat the file to see if
		// this is true, if so, return an ObjectNotFound error.
		stat, e := os.Stat(objectPath)
		if e == nil && stat.IsDir() {
			return count, probe.NewError(ObjectNotFound{Bucket: bucket, Object: object})
		}

		return count, probe.NewError(e)
	}

	return count, nil
}

// GetObjectInfo - get object info.
func (fs Filesystem) GetObjectInfo(bucket, object string) (ObjectInfo, *probe.Error) {
	// Input validation.
	if !IsValidBucketName(bucket) {
		return ObjectInfo{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}

	if !IsValidObjectName(object) {
		return ObjectInfo{}, probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}

	// Normalize buckets.
	bucket = fs.denormalizeBucket(bucket)
	bucketPath := filepath.Join(fs.path, bucket)
	if _, e := os.Stat(bucketPath); e != nil {
		if os.IsNotExist(e) {
			return ObjectInfo{}, probe.NewError(BucketNotFound{Bucket: bucket})
		}
		return ObjectInfo{}, probe.NewError(e)
	}

	info, err := getObjectInfo(fs.path, bucket, object)
	if err != nil {
		if os.IsNotExist(err.ToGoError()) {
			return ObjectInfo{}, probe.NewError(ObjectNotFound{Bucket: bucket, Object: object})
		}
		return ObjectInfo{}, err.Trace(bucket, object)
	}
	if info.IsDir {
		return ObjectInfo{}, probe.NewError(ObjectNotFound{Bucket: bucket, Object: object})
	}
	return info, nil
}

// getObjectInfo - get object stat info.
func getObjectInfo(rootPath, bucket, object string) (ObjectInfo, *probe.Error) {
	// Do not use filepath.Join() since filepath.Join strips off any
	// object names with '/', use them as is in a static manner so
	// that we can send a proper 'ObjectNotFound' reply back upon os.Stat().
	var objectPath string
	// For windows use its special os.PathSeparator == "\\"
	if runtime.GOOS == "windows" {
		objectPath = rootPath + string(os.PathSeparator) + bucket + string(os.PathSeparator) + object
	} else {
		objectPath = rootPath + string(os.PathSeparator) + bucket + string(os.PathSeparator) + object
	}
	stat, e := os.Stat(objectPath)
	if e != nil {
		return ObjectInfo{}, probe.NewError(e)
	}
	contentType := "application/octet-stream"
	if runtime.GOOS == "windows" {
		object = filepath.ToSlash(object)
	}

	if objectExt := filepath.Ext(object); objectExt != "" {
		content, ok := mimedb.DB[strings.ToLower(strings.TrimPrefix(objectExt, "."))]
		if ok {
			contentType = content.ContentType
		}
	}
	metadata := ObjectInfo{
		Bucket:       bucket,
		Name:         object,
		ModifiedTime: stat.ModTime(),
		Size:         stat.Size(),
		ContentType:  contentType,
		IsDir:        stat.Mode().IsDir(),
	}
	return metadata, nil
}

// isMD5SumEqual - returns error if md5sum mismatches, success its `nil`
func isMD5SumEqual(expectedMD5Sum, actualMD5Sum string) bool {
	// Verify the md5sum.
	if expectedMD5Sum != "" && actualMD5Sum != "" {
		// Decode md5sum to bytes from their hexadecimal
		// representations.
		expectedMD5SumBytes, err := hex.DecodeString(expectedMD5Sum)
		if err != nil {
			return false
		}
		actualMD5SumBytes, err := hex.DecodeString(actualMD5Sum)
		if err != nil {
			return false
		}
		// Verify md5sum bytes are equal after successful decoding.
		if !bytes.Equal(expectedMD5SumBytes, actualMD5SumBytes) {
			return false
		}
		return true
	}
	return false
}

// CreateObject - create an object.
func (fs Filesystem) CreateObject(bucket, object, expectedMD5Sum string, size int64, data io.Reader, sig *signature4.Sign) (ObjectInfo, *probe.Error) {
	di, e := disk.GetInfo(fs.path)
	if e != nil {
		return ObjectInfo{}, probe.NewError(e)
	}

	// Remove 5% from total space for cumulative disk space used for
	// journalling, inodes etc.
	availableDiskSpace := (float64(di.Free) / (float64(di.Total) - (0.05 * float64(di.Total)))) * 100
	if int64(availableDiskSpace) <= fs.minFreeDisk {
		return ObjectInfo{}, probe.NewError(RootPathFull{Path: fs.path})
	}

	// Check bucket name valid.
	if !IsValidBucketName(bucket) {
		return ObjectInfo{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}

	bucket = fs.denormalizeBucket(bucket)
	bucketPath := filepath.Join(fs.path, bucket)
	if _, e = os.Stat(bucketPath); e != nil {
		if os.IsNotExist(e) {
			return ObjectInfo{}, probe.NewError(BucketNotFound{Bucket: bucket})
		}
		return ObjectInfo{}, probe.NewError(e)
	}

	// Verify object path legal.
	if !IsValidObjectName(object) {
		return ObjectInfo{}, probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}

	// Get object path.
	objectPath := filepath.Join(bucketPath, object)
	if expectedMD5Sum != "" {
		var expectedMD5SumBytes []byte
		expectedMD5SumBytes, e = base64.StdEncoding.DecodeString(expectedMD5Sum)
		if e != nil {
			// Pro-actively close the connection.
			return ObjectInfo{}, probe.NewError(InvalidDigest{MD5: expectedMD5Sum})
		}
		expectedMD5Sum = hex.EncodeToString(expectedMD5SumBytes)
	}

	// Write object.
	file, e := atomic.FileCreateWithPrefix(objectPath, expectedMD5Sum+"$tmpobject")
	if e != nil {
		switch e := e.(type) {
		case *os.PathError:
			if e.Op == "mkdir" {
				if strings.Contains(e.Error(), "not a directory") {
					return ObjectInfo{}, probe.NewError(ObjectExistsAsPrefix{Bucket: bucket, Prefix: object})
				}
			}
			return ObjectInfo{}, probe.NewError(e)
		default:
			return ObjectInfo{}, probe.NewError(e)
		}
	}

	// Instantiate checksum hashers and create a multiwriter.
	md5Hasher := md5.New()
	sha256Hasher := sha256.New()
	objectWriter := io.MultiWriter(file, md5Hasher, sha256Hasher)

	if size > 0 {
		if _, e = io.CopyN(objectWriter, data, size); e != nil {
			file.CloseAndPurge()
			return ObjectInfo{}, probe.NewError(e)
		}
	} else {
		if _, e = io.Copy(objectWriter, data); e != nil {
			file.CloseAndPurge()
			return ObjectInfo{}, probe.NewError(e)
		}
	}

	md5Sum := hex.EncodeToString(md5Hasher.Sum(nil))
	// Verify if the written object is equal to what is expected, only
	// if it is requested as such.
	if expectedMD5Sum != "" {
		if !isMD5SumEqual(expectedMD5Sum, md5Sum) {
			file.CloseAndPurge()
			return ObjectInfo{}, probe.NewError(BadDigest{MD5: expectedMD5Sum, Bucket: bucket, Object: object})
		}
	}
	sha256Sum := hex.EncodeToString(sha256Hasher.Sum(nil))
	if sig != nil {
		ok, err := sig.DoesSignatureMatch(sha256Sum)
		if err != nil {
			file.CloseAndPurge()
			return ObjectInfo{}, err.Trace()
		}
		if !ok {
			file.CloseAndPurge()
			return ObjectInfo{}, probe.NewError(SignDoesNotMatch{})
		}
	}
	file.Close()

	// Set stat again to get the latest metadata.
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
		MD5Sum:       md5Sum,
	}
	return newObject, nil
}

// deleteObjectPath - delete object path if its empty.
func deleteObjectPath(basePath, deletePath, bucket, object string) *probe.Error {
	if basePath == deletePath {
		return nil
	}
	// Verify if the path exists.
	pathSt, e := os.Stat(deletePath)
	if e != nil {
		if os.IsNotExist(e) {
			return probe.NewError(ObjectNotFound{Bucket: bucket, Object: object})
		}
		return probe.NewError(e)
	}
	if pathSt.IsDir() {
		// Verify if directory is empty.
		empty, e := isDirEmpty(deletePath)
		if e != nil {
			return probe.NewError(e)
		}
		if !empty {
			return nil
		}
	}
	// Attempt to remove path.
	if e := os.Remove(deletePath); e != nil {
		return probe.NewError(e)
	}
	// Recursively go down the next path and delete again.
	if err := deleteObjectPath(basePath, filepath.Dir(deletePath), bucket, object); err != nil {
		return err.Trace(basePath, deletePath, bucket, object)
	}
	return nil
}

// DeleteObject - delete object.
func (fs Filesystem) DeleteObject(bucket, object string) *probe.Error {
	// Check bucket name valid
	if !IsValidBucketName(bucket) {
		return probe.NewError(BucketNameInvalid{Bucket: bucket})
	}

	bucket = fs.denormalizeBucket(bucket)
	bucketPath := filepath.Join(fs.path, bucket)
	// Check bucket exists
	if _, e := os.Stat(bucketPath); e != nil {
		if os.IsNotExist(e) {
			return probe.NewError(BucketNotFound{Bucket: bucket})
		}
		return probe.NewError(e)
	}

	// Verify object path legal
	if !IsValidObjectName(object) {
		return probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}

	// Do not use filepath.Join() since filepath.Join strips off any
	// object names with '/', use them as is in a static manner so
	// that we can send a proper 'ObjectNotFound' reply back upon
	// os.Stat().
	var objectPath string
	if runtime.GOOS == "windows" {
		objectPath = fs.path + string(os.PathSeparator) + bucket + string(os.PathSeparator) + object
	} else {
		objectPath = fs.path + string(os.PathSeparator) + bucket + string(os.PathSeparator) + object
	}
	// Delete object path if its empty.
	err := deleteObjectPath(bucketPath, objectPath, bucket, object)
	if err != nil {
		if os.IsNotExist(err.ToGoError()) {
			return probe.NewError(ObjectNotFound{Bucket: bucket, Object: object})
		}
		return err.Trace(bucketPath, objectPath, bucket, object)
	}
	return nil
}
