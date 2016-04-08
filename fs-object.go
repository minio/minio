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
	"bytes"
	"crypto/md5"
	"io"
	"os"
	"path/filepath"
	"strings"

	"encoding/hex"
	"runtime"

	"github.com/minio/minio/pkg/mimedb"
	"github.com/minio/minio/pkg/probe"
	"github.com/minio/minio/pkg/safe"
)

/// Object Operations

// GetObject - GET object
func (fs Filesystem) GetObject(bucket, object string, startOffset int64) (io.ReadCloser, *probe.Error) {
	// Input validation.
	bucket, e := fs.checkBucketArg(bucket)
	if e != nil {
		return nil, probe.NewError(e)
	}
	if !IsValidObjectName(object) {
		return nil, probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}

	objectPath := filepath.Join(fs.diskPath, bucket, object)
	file, e := os.Open(objectPath)
	if e != nil {
		// If the object doesn't exist, the bucket might not exist either. Stat for
		// the bucket and give a better error message if that is true.
		if os.IsNotExist(e) {
			_, e = os.Stat(filepath.Join(fs.diskPath, bucket))
			if os.IsNotExist(e) {
				return nil, probe.NewError(BucketNotFound{Bucket: bucket})
			}
			return nil, probe.NewError(ObjectNotFound{Bucket: bucket, Object: object})
		}
		return nil, probe.NewError(e)
	}
	// Initiate a cached stat operation on the file handler.
	st, e := file.Stat()
	if e != nil {
		return nil, probe.NewError(e)
	}
	// Object path is a directory prefix, return object not found error.
	if st.IsDir() {
		return nil, probe.NewError(ObjectExistsAsPrefix{
			Bucket: bucket,
			Prefix: object,
		})
	}

	// Seek to a starting offset.
	_, e = file.Seek(startOffset, os.SEEK_SET)
	if e != nil {
		// When the "handle is invalid", the file might be a directory on Windows.
		if runtime.GOOS == "windows" && strings.Contains(e.Error(), "handle is invalid") {
			return nil, probe.NewError(ObjectNotFound{Bucket: bucket, Object: object})
		}
		return nil, probe.NewError(e)
	}
	return file, nil
}

// GetObjectInfo - get object info.
func (fs Filesystem) GetObjectInfo(bucket, object string) (ObjectInfo, *probe.Error) {
	// Input validation.
	bucket, e := fs.checkBucketArg(bucket)
	if e != nil {
		return ObjectInfo{}, probe.NewError(e)
	}

	if !IsValidObjectName(object) {
		return ObjectInfo{}, probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}

	info, err := getObjectInfo(fs.diskPath, bucket, object)
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
	objInfo := ObjectInfo{
		Bucket:       bucket,
		Name:         object,
		ModifiedTime: stat.ModTime(),
		Size:         stat.Size(),
		ContentType:  contentType,
		IsDir:        stat.Mode().IsDir(),
	}
	return objInfo, nil
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

// PutObject - create an object.
func (fs Filesystem) PutObject(bucket string, object string, size int64, data io.Reader, metadata map[string]string) (ObjectInfo, *probe.Error) {
	// Check bucket name valid.
	bucket, e := fs.checkBucketArg(bucket)
	if e != nil {
		return ObjectInfo{}, probe.NewError(e)
	}

	bucketDir := filepath.Join(fs.diskPath, bucket)

	// Verify object path legal.
	if !IsValidObjectName(object) {
		return ObjectInfo{}, probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}

	if e = checkDiskFree(fs.diskPath, fs.minFreeDisk); e != nil {
		return ObjectInfo{}, probe.NewError(e)
	}

	// Get object path.
	objectPath := filepath.Join(bucketDir, object)

	// md5Hex representation.
	var md5Hex string
	if len(metadata) != 0 {
		md5Hex = metadata["md5Sum"]
	}

	// Write object.
	safeFile, e := safe.CreateFileWithPrefix(objectPath, md5Hex+"$tmpobject")
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

	// Initialize md5 writer.
	md5Writer := md5.New()

	// Instantiate a new multi writer.
	multiWriter := io.MultiWriter(md5Writer, safeFile)

	// Instantiate checksum hashers and create a multiwriter.
	if size > 0 {
		if _, e = io.CopyN(multiWriter, data, size); e != nil {
			safeFile.CloseAndRemove()
			return ObjectInfo{}, probe.NewError(e)
		}
	} else {
		if _, e = io.Copy(multiWriter, data); e != nil {
			safeFile.CloseAndRemove()
			return ObjectInfo{}, probe.NewError(e)
		}
	}

	newMD5Hex := hex.EncodeToString(md5Writer.Sum(nil))
	if md5Hex != "" {
		if newMD5Hex != md5Hex {
			return ObjectInfo{}, probe.NewError(BadDigest{md5Hex, newMD5Hex})
		}
	}

	// Set stat again to get the latest metadata.
	st, e := os.Stat(safeFile.Name())
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
		MD5Sum:       newMD5Hex,
		ContentType:  contentType,
	}

	// Safely close and atomically rename the file.
	safeFile.Close()

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
	bucket, e := fs.checkBucketArg(bucket)
	if e != nil {
		return probe.NewError(e)
	}

	bucketDir := filepath.Join(fs.diskPath, bucket)

	// Verify object path legal
	if !IsValidObjectName(object) {
		return probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}

	// Do not use filepath.Join() since filepath.Join strips off any
	// object names with '/', use them as is in a static manner so
	// that we can send a proper 'ObjectNotFound' reply back upon os.Stat().
	var objectPath string
	if runtime.GOOS == "windows" {
		objectPath = fs.diskPath + string(os.PathSeparator) + bucket + string(os.PathSeparator) + object
	} else {
		objectPath = fs.diskPath + string(os.PathSeparator) + bucket + string(os.PathSeparator) + object
	}
	// Delete object path if its empty.
	err := deleteObjectPath(bucketDir, objectPath, bucket, object)
	if err != nil {
		if os.IsNotExist(err.ToGoError()) {
			return probe.NewError(ObjectNotFound{Bucket: bucket, Object: object})
		}
		return err.Trace(bucketDir, objectPath, bucket, object)
	}
	return nil
}
