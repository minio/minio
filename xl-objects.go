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
	"crypto/md5"
	"encoding/hex"
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
	if _, err := xl.storage.StatFile(bucket, pathJoin(object, multipartMetaFile)); err != nil {
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
	info, err := xl.getMultipartObjectInfo(bucket, object)
	if err != nil {
		return nil, toObjectErr(err, bucket, object)
	}
	partIndex, offset, err := info.GetPartNumberOffset(startOffset)
	if err != nil {
		return nil, toObjectErr(err, bucket, object)
	}
	go func() {
		for ; partIndex < len(info); partIndex++ {
			part := info[partIndex]
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

// Return the parts of a multipart upload.
func (xl xlObjects) getMultipartObjectInfo(bucket, object string) (info MultipartObjectInfo, err error) {
	offset := int64(0)
	r, err := xl.storage.ReadFile(bucket, pathJoin(object, multipartMetaFile), offset)
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
		info, err := xl.getMultipartObjectInfo(bucket, object)
		if err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
		fi.Size = info.GetSize()
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
		MD5Sum:      "", // Read from metadata.
	}, nil
}

func (xl xlObjects) PutObject(bucket string, object string, size int64, data io.Reader, metadata map[string]string) (string, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return "", (BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(object) {
		return "", (ObjectNameInvalid{
			Bucket: bucket,
			Object: object,
		})
	}
	// Check whether the bucket exists.
	if isExist, err := isBucketExist(xl.storage, bucket); err != nil {
		return "", err
	} else if !isExist {
		return "", BucketNotFound{Bucket: bucket}
	}

	fileWriter, err := xl.storage.CreateFile(bucket, object)
	if err != nil {
		return "", toObjectErr(err, bucket, object)
	}

	// Initialize md5 writer.
	md5Writer := md5.New()

	// Instantiate a new multi writer.
	multiWriter := io.MultiWriter(md5Writer, fileWriter)

	// Instantiate checksum hashers and create a multiwriter.
	if size > 0 {
		if _, err = io.CopyN(multiWriter, data, size); err != nil {
			if clErr := safeCloseAndRemove(fileWriter); clErr != nil {
				return "", clErr
			}
			return "", toObjectErr(err)
		}
	} else {
		if _, err = io.Copy(multiWriter, data); err != nil {
			if clErr := safeCloseAndRemove(fileWriter); clErr != nil {
				return "", clErr
			}
			return "", err
		}
	}

	newMD5Hex := hex.EncodeToString(md5Writer.Sum(nil))
	// md5Hex representation.
	var md5Hex string
	if len(metadata) != 0 {
		md5Hex = metadata["md5Sum"]
	}
	if md5Hex != "" {
		if newMD5Hex != md5Hex {
			if err = safeCloseAndRemove(fileWriter); err != nil {
				return "", err
			}
			return "", BadDigest{md5Hex, newMD5Hex}
		}
	}
	err = fileWriter.Close()
	if err != nil {
		return "", err
	}

	// Return md5sum, successfully wrote object.
	return newMD5Hex, nil
}

func (xl xlObjects) DeleteObject(bucket, object string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}
	if !IsValidObjectName(object) {
		return ObjectNameInvalid{Bucket: bucket, Object: object}
	}
	if err := xl.storage.DeleteFile(bucket, object); err != nil {
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
			if strings.HasSuffix(fileInfo.Name, slashSeparator) {
				if isLeafDirectory(xl.storage, bucket, fileInfo.Name) {
					fileInfo.Name = strings.TrimSuffix(fileInfo.Name, slashSeparator)
					// Set the Mode to a "regular" file.
					fileInfo.Mode = 0
					var info MultipartObjectInfo
					info, err = xl.getMultipartObjectInfo(bucket, fileInfo.Name)
					if err != nil {
						return ListObjectsInfo{}, toObjectErr(err, bucket)
					}
					fileInfo.Size = info.GetSize()
					allFileInfos = append(allFileInfos, fileInfo)
					maxKeys--
					continue
				}
			}
			if strings.HasSuffix(fileInfo.Name, multipartMetaFile) {
				fileInfo.Name = path.Dir(fileInfo.Name)
				var info MultipartObjectInfo
				info, err = xl.getMultipartObjectInfo(bucket, fileInfo.Name)
				if err != nil {
					return ListObjectsInfo{}, toObjectErr(err, bucket)
				}
				fileInfo.Size = info.GetSize()
				allFileInfos = append(allFileInfos, fileInfo)
				maxKeys--
				continue
			}
			if strings.HasSuffix(fileInfo.Name, multipartSuffix) {
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
