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
	"errors"
	"runtime"

	"github.com/minio/minio-xl/pkg/atomic"
	"github.com/minio/minio-xl/pkg/crypto/sha256"
	"github.com/minio/minio-xl/pkg/probe"
)

/// Object Operations

// GetObject - GET object
func (fs API) GetObject(w io.Writer, bucket, object string, start, length int64) (int64, *probe.Error) {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	// validate bucket
	if !IsValidBucket(bucket) {
		return 0, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}

	// validate object
	if !IsValidObjectName(object) {
		return 0, probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}

	objectPath := filepath.Join(fs.path, bucket, object)
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

// GetObjectMetadata - HEAD object
func (fs API) GetObjectMetadata(bucket, object string) (ObjectMetadata, *probe.Error) {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	if !IsValidBucket(bucket) {
		return ObjectMetadata{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}

	if !IsValidObjectName(object) {
		return ObjectMetadata{}, probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: bucket})
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

func getMetadata(rootPath, bucket, object string) (ObjectMetadata, *probe.Error) {
	// Do not use filepath.Join() since filepath.Join strips off any object names with '/', use them as is
	// in a static manner so that we can send a proper 'ObjectNotFound' reply back upon os.Stat()
	var objectPath string
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
func isMD5SumEqual(expectedMD5Sum, actualMD5Sum string) *probe.Error {
	if strings.TrimSpace(expectedMD5Sum) != "" && strings.TrimSpace(actualMD5Sum) != "" {
		expectedMD5SumBytes, err := hex.DecodeString(expectedMD5Sum)
		if err != nil {
			return probe.NewError(err)
		}
		actualMD5SumBytes, err := hex.DecodeString(actualMD5Sum)
		if err != nil {
			return probe.NewError(err)
		}
		if !bytes.Equal(expectedMD5SumBytes, actualMD5SumBytes) {
			return probe.NewError(BadDigest{Md5: expectedMD5Sum})
		}
		return nil
	}
	return probe.NewError(errors.New("invalid argument"))
}

// CreateObject - PUT object
func (fs API) CreateObject(bucket, object, expectedMD5Sum string, size int64, data io.Reader, signature *Signature) (ObjectMetadata, *probe.Error) {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	// check bucket name valid
	if !IsValidBucket(bucket) {
		return ObjectMetadata{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	// check bucket exists
	if _, err := os.Stat(filepath.Join(fs.path, bucket)); os.IsNotExist(err) {
		return ObjectMetadata{}, probe.NewError(BucketNotFound{Bucket: bucket})
	}
	// verify object path legal
	if !IsValidObjectName(object) {
		return ObjectMetadata{}, probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}

	// get object path
	objectPath := filepath.Join(fs.path, bucket, object)
	if strings.TrimSpace(expectedMD5Sum) != "" {
		expectedMD5SumBytes, err := base64.StdEncoding.DecodeString(strings.TrimSpace(expectedMD5Sum))
		if err != nil {
			// pro-actively close the connection
			return ObjectMetadata{}, probe.NewError(InvalidDigest{Md5: expectedMD5Sum})
		}
		expectedMD5Sum = hex.EncodeToString(expectedMD5SumBytes)
	}

	// write object
	file, err := atomic.FileCreate(objectPath)
	if err != nil {
		return ObjectMetadata{}, probe.NewError(err)
	}

	h := md5.New()
	sh := sha256.New()
	mw := io.MultiWriter(file, h, sh)

	if size > 0 {
		_, err = io.CopyN(mw, data, size)
		if err != nil {
			file.CloseAndPurge()
			return ObjectMetadata{}, probe.NewError(err)
		}
	} else {
		_, err = io.Copy(mw, data)
		if err != nil {
			file.CloseAndPurge()
			return ObjectMetadata{}, probe.NewError(err)
		}
	}

	md5Sum := hex.EncodeToString(h.Sum(nil))
	// Verify if the written object is equal to what is expected, only if it is requested as such
	if strings.TrimSpace(expectedMD5Sum) != "" {
		if err := isMD5SumEqual(strings.TrimSpace(expectedMD5Sum), md5Sum); err != nil {
			file.CloseAndPurge()
			return ObjectMetadata{}, probe.NewError(BadDigest{Md5: expectedMD5Sum, Bucket: bucket, Object: object})
		}
	}
	sha256Sum := hex.EncodeToString(sh.Sum(nil))
	if signature != nil {
		ok, perr := signature.DoesSignatureMatch(sha256Sum)
		if perr != nil {
			file.CloseAndPurge()
			return ObjectMetadata{}, perr.Trace()
		}
		if !ok {
			file.CloseAndPurge()
			return ObjectMetadata{}, probe.NewError(SignatureDoesNotMatch{})
		}
	}
	file.File.Sync()
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
		Md5:         md5Sum,
	}
	return newObject, nil
}

// DeleteObject - delete and object
func (fs API) DeleteObject(bucket, object string) *probe.Error {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	// check bucket name valid
	if !IsValidBucket(bucket) {
		return probe.NewError(BucketNameInvalid{Bucket: bucket})
	}

	// check bucket exists
	if _, err := os.Stat(filepath.Join(fs.path, bucket)); os.IsNotExist(err) {
		return probe.NewError(BucketNotFound{Bucket: bucket})
	}

	// verify object path legal
	if !IsValidObjectName(object) {
		return probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}

	// Do not use filepath.Join() since filepath.Join strips off any object names with '/', use them as is
	// in a static manner so that we can send a proper 'ObjectNotFound' reply back upon os.Stat()
	var objectPath string
	if runtime.GOOS == "windows" {
		objectPath = fs.path + string(os.PathSeparator) + bucket + string(os.PathSeparator) + object
	} else {
		objectPath = fs.path + string(os.PathSeparator) + bucket + string(os.PathSeparator) + object
	}

	_, err := os.Stat(objectPath)
	if err != nil {
		if os.IsNotExist(err) {
			return probe.NewError(ObjectNotFound{Bucket: bucket, Object: object})
		}
		return probe.NewError(err)
	}
	if err := os.Remove(objectPath); err != nil {
		return probe.NewError(err)
	}
	return nil
}
