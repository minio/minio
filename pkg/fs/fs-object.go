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
	"github.com/minio/minio/pkg/ioutils"
	"github.com/minio/minio/pkg/mimedb"
	"github.com/minio/minio/pkg/probe"
	signV4 "github.com/minio/minio/pkg/signature"
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
	bucketPath := filepath.Join(fs.path, bucket)
	if _, e := os.Stat(bucketPath); e != nil {
		if os.IsNotExist(e) {
			return 0, probe.NewError(BucketNotFound{Bucket: bucket})
		}
		return 0, probe.NewError(e)
	}

	objectPath := filepath.Join(bucketPath, object)
	filestat, err := os.Stat(objectPath)
	switch err := err.(type) {
	case nil:
		if filestat.IsDir() {
			return 0, probe.NewError(ObjectNotFound{Bucket: bucket, Object: object})
		}
	default:
		if os.IsNotExist(err) {
			return 0, probe.NewError(ObjectNotFound{Bucket: bucket, Object: object})
		}
		return 0, probe.NewError(err)
	}
	file, err := os.Open(objectPath)
	if err != nil {
		return 0, probe.NewError(err)
	}
	defer file.Close()

	_, err = file.Seek(start, os.SEEK_SET)
	if err != nil {
		return 0, probe.NewError(err)
	}

	var count int64
	if length > 0 {
		count, err = io.CopyN(w, file, length)
		if err != nil {
			return count, probe.NewError(err)
		}
	} else {
		count, err = io.Copy(w, file)
		if err != nil {
			return count, probe.NewError(err)
		}
	}
	return count, nil
}

// GetObjectMetadata - get object metadata.
func (fs Filesystem) GetObjectMetadata(bucket, object string) (ObjectMetadata, *probe.Error) {
	// Input validation.
	if !IsValidBucketName(bucket) {
		return ObjectMetadata{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}

	if !IsValidObjectName(object) {
		return ObjectMetadata{}, probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: bucket})
	}

	// Normalize buckets.
	bucket = fs.denormalizeBucket(bucket)
	bucketPath := filepath.Join(fs.path, bucket)
	if _, e := os.Stat(bucketPath); e != nil {
		if os.IsNotExist(e) {
			return ObjectMetadata{}, probe.NewError(BucketNotFound{Bucket: bucket})
		}
		return ObjectMetadata{}, probe.NewError(e)
	}

	metadata, err := getMetadata(fs.path, bucket, object)
	if err != nil {
		return ObjectMetadata{}, err.Trace(bucket, object)
	}
	if metadata.Mode.IsDir() {
		return ObjectMetadata{}, probe.NewError(ObjectNotFound{Bucket: bucket, Object: object})
	}
	return metadata, nil
}

// getMetadata - get object metadata.
func getMetadata(rootPath, bucket, object string) (ObjectMetadata, *probe.Error) {
	// Do not use filepath.Join() since filepath.Join strips off any
	// object names with '/', use them as is in a static manner so
	// that we can send a proper 'ObjectNotFound' reply back upon
	// os.Stat().
	var objectPath string
	// For windows use its special os.PathSeparator == "\\"
	if runtime.GOOS == "windows" {
		objectPath = rootPath + string(os.PathSeparator) + bucket + string(os.PathSeparator) + object
	} else {
		objectPath = rootPath + string(os.PathSeparator) + bucket + string(os.PathSeparator) + object
	}
	stat, err := os.Stat(objectPath)
	if err != nil {
		if os.IsNotExist(err) {
			return ObjectMetadata{}, probe.NewError(ObjectNotFound{Bucket: bucket, Object: object})
		}
		return ObjectMetadata{}, probe.NewError(err)
	}
	contentType := "application/octet-stream"
	if runtime.GOOS == "windows" {
		object = sanitizeWindowsPath(object)
	}

	if objectExt := filepath.Ext(object); objectExt != "" {
		content, ok := mimedb.DB[strings.ToLower(strings.TrimPrefix(objectExt, "."))]
		if ok {
			contentType = content.ContentType
		}
	}
	metadata := ObjectMetadata{
		Bucket:      bucket,
		Object:      object,
		Created:     stat.ModTime(),
		Size:        stat.Size(),
		ContentType: contentType,
		Mode:        stat.Mode(),
	}
	return metadata, nil
}

// isMD5SumEqual - returns error if md5sum mismatches, success its `nil`
func isMD5SumEqual(expectedMD5Sum, actualMD5Sum string) bool {
	// Verify the md5sum.
	if strings.TrimSpace(expectedMD5Sum) != "" && strings.TrimSpace(actualMD5Sum) != "" {
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
func (fs Filesystem) CreateObject(bucket, object, expectedMD5Sum string, size int64, data io.Reader, signature *signV4.Signature) (ObjectMetadata, *probe.Error) {
	di, e := disk.GetInfo(fs.path)
	if e != nil {
		return ObjectMetadata{}, probe.NewError(e)
	}

	// Remove 5% from total space for cumulative disk space used for
	// journalling, inodes etc.
	availableDiskSpace := (float64(di.Free) / (float64(di.Total) - (0.05 * float64(di.Total)))) * 100
	if int64(availableDiskSpace) <= fs.minFreeDisk {
		return ObjectMetadata{}, probe.NewError(RootPathFull{Path: fs.path})
	}

	// Check bucket name valid.
	if !IsValidBucketName(bucket) {
		return ObjectMetadata{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}

	bucket = fs.denormalizeBucket(bucket)
	bucketPath := filepath.Join(fs.path, bucket)
	if _, e = os.Stat(bucketPath); e != nil {
		if os.IsNotExist(e) {
			return ObjectMetadata{}, probe.NewError(BucketNotFound{Bucket: bucket})
		}
		return ObjectMetadata{}, probe.NewError(e)
	}

	// Verify object path legal.
	if !IsValidObjectName(object) {
		return ObjectMetadata{}, probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}

	// Get object path.
	objectPath := filepath.Join(bucketPath, object)
	if strings.TrimSpace(expectedMD5Sum) != "" {
		var expectedMD5SumBytes []byte
		expectedMD5SumBytes, e = base64.StdEncoding.DecodeString(strings.TrimSpace(expectedMD5Sum))
		if e != nil {
			// Pro-actively close the connection.
			return ObjectMetadata{}, probe.NewError(InvalidDigest{MD5: expectedMD5Sum})
		}
		expectedMD5Sum = hex.EncodeToString(expectedMD5SumBytes)
	}

	// Write object.
	file, e := atomic.FileCreateWithPrefix(objectPath, "$tmpobject")
	if e != nil {
		switch e := e.(type) {
		case *os.PathError:
			if e.Op == "mkdir" {
				if strings.Contains(e.Error(), "not a directory") {
					return ObjectMetadata{}, probe.NewError(ObjectExistsAsPrefix{Bucket: bucket, Prefix: object})
				}
			}
			return ObjectMetadata{}, probe.NewError(e)
		default:
			return ObjectMetadata{}, probe.NewError(e)
		}
	}

	// Instantiate checksum hashers and create a multiwriter.
	md5Hasher := md5.New()
	sha256Hasher := sha256.New()
	objectWriter := io.MultiWriter(file, md5Hasher, sha256Hasher)

	if size > 0 {
		if _, e = io.CopyN(objectWriter, data, size); e != nil {
			file.CloseAndPurge()
			return ObjectMetadata{}, probe.NewError(e)
		}
	} else {
		if _, e = io.Copy(objectWriter, data); e != nil {
			file.CloseAndPurge()
			return ObjectMetadata{}, probe.NewError(e)
		}
	}

	md5Sum := hex.EncodeToString(md5Hasher.Sum(nil))
	// Verify if the written object is equal to what is expected, only
	// if it is requested as such.
	if strings.TrimSpace(expectedMD5Sum) != "" {
		if !isMD5SumEqual(strings.TrimSpace(expectedMD5Sum), md5Sum) {
			file.CloseAndPurge()
			return ObjectMetadata{}, probe.NewError(BadDigest{MD5: expectedMD5Sum, Bucket: bucket, Object: object})
		}
	}
	sha256Sum := hex.EncodeToString(sha256Hasher.Sum(nil))
	if signature != nil {
		ok, err := signature.DoesSignatureMatch(sha256Sum)
		if err != nil {
			file.CloseAndPurge()
			return ObjectMetadata{}, err.Trace()
		}
		if !ok {
			file.CloseAndPurge()
			return ObjectMetadata{}, probe.NewError(signV4.SigDoesNotMatch{})
		}
	}
	file.Close()

	// Set stat again to get the latest metadata.
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
		MD5:         md5Sum,
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
		empty, e := ioutils.IsDirEmpty(deletePath)
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
