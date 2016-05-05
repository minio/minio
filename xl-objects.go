/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"encoding/json"
	"io"
	"path"
	"path/filepath"
	"strings"

	"github.com/minio/minio/pkg/mimedb"
)

const (
	multipartSuffix   = ".minio.multipart"
	multipartMetaFile = "00000" + multipartSuffix
)

// xlObjects - Implements fs object layer.
type xlObjects struct {
	storage StorageAPI
}

// newXLObjects - initialize new xl object layer.
func newXLObjects(exportPaths ...string) (ObjectLayer, error) {
	storage, err := newXL(exportPaths...)
	if err != nil {
		return nil, err
	}

	// Cleanup all temporary entries.
	cleanupAllTmpEntries(storage)

	// Return successfully initialized object layer.
	return xlObjects{storage}, nil
}

/// Bucket operations

// MakeBucket - make a bucket.
func (xl xlObjects) MakeBucket(bucket string) error {
	return makeBucket(xl.storage, bucket)
}

// GetBucketInfo - get bucket info.
func (xl xlObjects) GetBucketInfo(bucket string) (BucketInfo, error) {
	return getBucketInfo(xl.storage, bucket)
}

// ListBuckets - list buckets.
func (xl xlObjects) ListBuckets() ([]BucketInfo, error) {
	return listBuckets(xl.storage)
}

// DeleteBucket - delete a bucket.
func (xl xlObjects) DeleteBucket(bucket string) error {
	return deleteBucket(xl.storage, bucket)
}

/// Object Operations

// GetObject - get an object.
func (xl xlObjects) GetObject(bucket, object string, startOffset int64) (io.ReadCloser, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return nil, BucketNameInvalid{Bucket: bucket}
	}
	// Verify if object is valid.
	if !IsValidObjectName(object) {
		return nil, ObjectNameInvalid{Bucket: bucket, Object: object}
	}
	if ok, err := isMultipartObject(xl.storage, bucket, object); err != nil {
		return nil, toObjectErr(err, bucket, object)
	} else if !ok {
		if _, err = xl.storage.StatFile(bucket, object); err == nil {
			var reader io.ReadCloser
			reader, err = xl.storage.ReadFile(bucket, object, startOffset)
			if err != nil {
				return nil, toObjectErr(err, bucket, object)
			}
			return reader, nil
		}
		return nil, toObjectErr(err, bucket, object)
	}
	fileReader, fileWriter := io.Pipe()
	info, err := getMultipartObjectInfo(xl.storage, bucket, object)
	if err != nil {
		return nil, toObjectErr(err, bucket, object)
	}
	partIndex, offset, err := info.GetPartNumberOffset(startOffset)
	if err != nil {
		return nil, toObjectErr(err, bucket, object)
	}
	go func() {
		for ; partIndex < len(info.Parts); partIndex++ {
			part := info.Parts[partIndex]
			r, err := xl.storage.ReadFile(bucket, pathJoin(object, partNumToPartFileName(part.PartNumber)), offset)
			if err != nil {
				fileWriter.CloseWithError(err)
				return
			}
			if _, err := io.Copy(fileWriter, r); err != nil {
				fileWriter.CloseWithError(err)
				return
			}
		}
		fileWriter.Close()
	}()
	return fileReader, nil
}

// Return the partsInfo of a special multipart object.
func getMultipartObjectInfo(storage StorageAPI, bucket, object string) (info MultipartObjectInfo, err error) {
	offset := int64(0)
	r, err := storage.ReadFile(bucket, pathJoin(object, multipartMetaFile), offset)
	if err != nil {
		return
	}
	decoder := json.NewDecoder(r)
	err = decoder.Decode(&info)
	return
}

// GetObjectInfo - get object info.
func (xl xlObjects) GetObjectInfo(bucket, object string) (ObjectInfo, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return ObjectInfo{}, BucketNameInvalid{Bucket: bucket}
	}
	// Verify if object is valid.
	if !IsValidObjectName(object) {
		return ObjectInfo{}, ObjectNameInvalid{Bucket: bucket, Object: object}
	}
	fi, err := xl.storage.StatFile(bucket, object)
	if err != nil {
		info, err := getMultipartObjectInfo(xl.storage, bucket, object)
		if err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
		fi.Size = info.Size
		fi.ModTime = info.ModTime
		fi.MD5Sum = info.MD5Sum
	}
	contentType := "application/octet-stream"
	if objectExt := filepath.Ext(object); objectExt != "" {
		content, ok := mimedb.DB[strings.ToLower(strings.TrimPrefix(objectExt, "."))]
		if ok {
			contentType = content.ContentType
		}
	}
	return ObjectInfo{
		Bucket:      bucket,
		Name:        object,
		ModTime:     fi.ModTime,
		Size:        fi.Size,
		IsDir:       fi.Mode.IsDir(),
		ContentType: contentType,
		MD5Sum:      fi.MD5Sum,
	}, nil
}

// PutObject - create an object.
func (xl xlObjects) PutObject(bucket string, object string, size int64, data io.Reader, metadata map[string]string) (string, error) {
	return putObjectCommon(xl.storage, bucket, object, size, data, metadata)
}

// isMultipartObject - verifies if an object is special multipart file.
func isMultipartObject(storage StorageAPI, bucket, object string) (bool, error) {
	_, err := storage.StatFile(bucket, pathJoin(object, multipartMetaFile))
	if err != nil {
		if err == errFileNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (xl xlObjects) DeleteObject(bucket, object string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}
	if !IsValidObjectName(object) {
		return ObjectNameInvalid{Bucket: bucket, Object: object}
	}
	// Verify if the object is a multipart object.
	if ok, err := isMultipartObject(xl.storage, bucket, object); err != nil {
		return toObjectErr(err, bucket, object)
	} else if !ok {
		if err := xl.storage.DeleteFile(bucket, object); err != nil {
			return toObjectErr(err, bucket, object)
		}
	}
	// Get parts info.
	info, err := getMultipartObjectInfo(xl.storage, bucket, object)
	if err != nil {
		return toObjectErr(err, bucket, object)
	}
	// Range through all files and delete it.
	for _, part := range info.Parts {
		err = xl.storage.DeleteFile(bucket, pathJoin(object, partNumToPartFileName(part.PartNumber)))
		if err != nil {
			return toObjectErr(err, bucket, object)
		}
	}
	err = xl.storage.DeleteFile(bucket, pathJoin(object, multipartMetaFile))
	if err != nil {
		return toObjectErr(err, bucket, object)
	}
	return nil
}

// TODO - support non-recursive case, figure out file size for files uploaded using multipart.

func (xl xlObjects) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return ListObjectsInfo{}, BucketNameInvalid{Bucket: bucket}
	}
	if !IsValidObjectPrefix(prefix) {
		return ListObjectsInfo{}, ObjectNameInvalid{Bucket: bucket, Object: prefix}
	}
	// Verify if delimiter is anything other than '/', which we do not support.
	if delimiter != "" && delimiter != slashSeparator {
		return ListObjectsInfo{}, UnsupportedDelimiter{
			Delimiter: delimiter,
		}
	}
	// Verify if marker has prefix.
	if marker != "" {
		if !strings.HasPrefix(marker, prefix) {
			return ListObjectsInfo{}, InvalidMarkerPrefixCombination{
				Marker: marker,
				Prefix: prefix,
			}
		}
	}

	if maxKeys == 0 {
		return ListObjectsInfo{}, nil
	}

	// Default is recursive, if delimiter is set then list non recursive.
	recursive := true
	if delimiter == slashSeparator {
		recursive = false
	}
	var allFileInfos, fileInfos []FileInfo
	var eof bool
	var err error
	for {
		fileInfos, eof, err = xl.storage.ListFiles(bucket, prefix, marker, recursive, maxKeys)
		if err != nil {
			return ListObjectsInfo{}, toObjectErr(err, bucket)
		}
		for _, fileInfo := range fileInfos {
			// FIXME: use fileInfo.Mode.IsDir() instead after fixing the bug in
			// XL listing which is not reseting the Mode to 0 for leaf dirs.
			if strings.HasSuffix(fileInfo.Name, slashSeparator) && isLeafDirectory(xl.storage, bucket, fileInfo.Name) {
				// Set the Mode to a "regular" file.
				var info MultipartObjectInfo
				info, err = getMultipartObjectInfo(xl.storage, bucket, fileInfo.Name)
				if err == nil {
					fileInfo.Mode = 0

					fileInfo.Name = strings.TrimSuffix(fileInfo.Name, slashSeparator)
					fileInfo.Size = info.Size
					fileInfo.ModTime = info.ModTime
					fileInfo.MD5Sum = info.MD5Sum
				} else if err != errFileNotFound {
					return ListObjectsInfo{}, toObjectErr(err, bucket, fileInfo.Name)
				}
				allFileInfos = append(allFileInfos, fileInfo)
				maxKeys--
				continue
			} else if strings.HasSuffix(fileInfo.Name, multipartMetaFile) {
				fileInfo.Name = path.Dir(fileInfo.Name)
				var info MultipartObjectInfo
				info, err = getMultipartObjectInfo(xl.storage, bucket, fileInfo.Name)
				if err != nil {
					return ListObjectsInfo{}, toObjectErr(err, bucket, fileInfo.Name)
				}
				fileInfo.Size = info.Size
				fileInfo.ModTime = info.ModTime
				fileInfo.MD5Sum = info.MD5Sum
				allFileInfos = append(allFileInfos, fileInfo)
				maxKeys--
				continue
			} else if strings.HasSuffix(fileInfo.Name, multipartSuffix) {
				continue
			}
			allFileInfos = append(allFileInfos, fileInfo)
			maxKeys--
		}
		if maxKeys == 0 {
			break
		}
		if eof {
			break
		}
	}

	result := ListObjectsInfo{IsTruncated: !eof}

	for _, fileInfo := range allFileInfos {
		// With delimiter set we fill in NextMarker and Prefixes.
		if delimiter == slashSeparator {
			result.NextMarker = fileInfo.Name
			if fileInfo.Mode.IsDir() {
				result.Prefixes = append(result.Prefixes, fileInfo.Name)
				continue
			}
		}
		result.Objects = append(result.Objects, ObjectInfo{
			Name:    fileInfo.Name,
			ModTime: fileInfo.ModTime,
			Size:    fileInfo.Size,
			IsDir:   false,
		})
	}
	return result, nil
}
