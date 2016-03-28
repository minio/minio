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
	"errors"
	"io"
	"path/filepath"
	"strings"

	"github.com/minio/minio/pkg/mimedb"
	"github.com/minio/minio/pkg/probe"
	"github.com/minio/minio/pkg/safe"
)

type objectAPI struct {
	storage StorageAPI
}

func newObjectLayer(storage StorageAPI) *objectAPI {
	return &objectAPI{storage}
}

/// Bucket operations

// MakeBucket - make a bucket.
func (o objectAPI) MakeBucket(bucket string) *probe.Error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if e := o.storage.MakeVol(bucket); e != nil {
		if e == errVolumeExists {
			return probe.NewError(BucketExists{Bucket: bucket})
		}
		return probe.NewError(e)
	}
	return nil
}

// GetBucketInfo - get bucket info.
func (o objectAPI) GetBucketInfo(bucket string) (BucketInfo, *probe.Error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketInfo{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	vi, e := o.storage.StatVol(bucket)
	if e != nil {
		if e == errVolumeNotFound {
			return BucketInfo{}, probe.NewError(BucketNotFound{Bucket: bucket})
		}
		return BucketInfo{}, probe.NewError(e)
	}
	return BucketInfo{
		Name:    bucket,
		Created: vi.Created,
	}, nil
}

// ListBuckets - list buckets.
func (o objectAPI) ListBuckets() ([]BucketInfo, *probe.Error) {
	var bucketInfos []BucketInfo
	vols, e := o.storage.ListVols()
	if e != nil {
		return nil, probe.NewError(e)
	}
	for _, vol := range vols {
		// StorageAPI can send volume names which are incompatible
		// with buckets, handle it and skip them.
		if !IsValidBucketName(vol.Name) {
			continue
		}
		bucketInfos = append(bucketInfos, BucketInfo{
			Name:    vol.Name,
			Created: vol.Created,
		})
	}
	return bucketInfos, nil
}

// DeleteBucket - delete a bucket.
func (o objectAPI) DeleteBucket(bucket string) *probe.Error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if e := o.storage.DeleteVol(bucket); e != nil {
		if e == errVolumeNotFound {
			return probe.NewError(BucketNotFound{Bucket: bucket})
		} else if e == errVolumeNotEmpty {
			return probe.NewError(BucketNotEmpty{Bucket: bucket})
		}
		return probe.NewError(e)
	}
	return nil
}

/// Object Operations

// GetObject - get an object.
func (o objectAPI) GetObject(bucket, object string, startOffset int64) (io.ReadCloser, *probe.Error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return nil, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	// Verify if object is valid.
	if !IsValidObjectName(object) {
		return nil, probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}
	r, e := o.storage.ReadFile(bucket, object, startOffset)
	if e != nil {
		if e == errVolumeNotFound {
			return nil, probe.NewError(BucketNotFound{Bucket: bucket})
		} else if e == errFileNotFound {
			return nil, probe.NewError(ObjectNotFound{Bucket: bucket, Object: object})
		}
		return nil, probe.NewError(e)
	}
	return r, nil
}

// GetObjectInfo - get object info.
func (o objectAPI) GetObjectInfo(bucket, object string) (ObjectInfo, *probe.Error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return ObjectInfo{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	// Verify if object is valid.
	if !IsValidObjectName(object) {
		return ObjectInfo{}, probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}
	fi, e := o.storage.StatFile(bucket, object)
	if e != nil {
		if e == errVolumeNotFound {
			return ObjectInfo{}, probe.NewError(BucketNotFound{Bucket: bucket})
		} else if e == errFileNotFound || e == errIsNotRegular {
			return ObjectInfo{}, probe.NewError(ObjectNotFound{Bucket: bucket, Object: object})
			// Handle more lower level errors if needed.
		} else {
			return ObjectInfo{}, probe.NewError(e)
		}
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

// safeCloseAndRemove - safely closes and removes underlying temporary
// file writer if possible.
func safeCloseAndRemove(writer io.WriteCloser) error {
	// If writer is a safe file, Attempt to close and remove.
	safeWriter, ok := writer.(*safe.File)
	if ok {
		return safeWriter.CloseAndRemove()
	}
	pipeWriter, ok := writer.(*io.PipeWriter)
	if ok {
		return pipeWriter.CloseWithError(errors.New("Close and error out."))
	}
	return nil
}

func (o objectAPI) PutObject(bucket string, object string, size int64, data io.Reader, metadata map[string]string) (string, *probe.Error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return "", probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(object) {
		return "", probe.NewError(ObjectNameInvalid{
			Bucket: bucket,
			Object: object,
		})
	}
	fileWriter, e := o.storage.CreateFile(bucket, object)
	if e != nil {
		if e == errVolumeNotFound {
			return "", probe.NewError(BucketNotFound{
				Bucket: bucket,
			})
		} else if e == errIsNotRegular {
			return "", probe.NewError(ObjectExistsAsPrefix{
				Bucket: bucket,
				Prefix: object,
			})
		}
		return "", probe.NewError(e)
	}

	// Initialize md5 writer.
	md5Writer := md5.New()

	// Instantiate a new multi writer.
	multiWriter := io.MultiWriter(md5Writer, fileWriter)

	// Instantiate checksum hashers and create a multiwriter.
	if size > 0 {
		if _, e = io.CopyN(multiWriter, data, size); e != nil {
			if e = safeCloseAndRemove(fileWriter); e != nil {
				return "", probe.NewError(e)
			}
			return "", probe.NewError(e)
		}
	} else {
		if _, e = io.Copy(multiWriter, data); e != nil {
			if e = safeCloseAndRemove(fileWriter); e != nil {
				return "", probe.NewError(e)
			}
			return "", probe.NewError(e)
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
			if e = safeCloseAndRemove(fileWriter); e != nil {
				return "", probe.NewError(e)
			}
			return "", probe.NewError(BadDigest{md5Hex, newMD5Hex})
		}
	}
	e = fileWriter.Close()
	if e != nil {
		return "", probe.NewError(e)
	}

	// Return md5sum, successfully wrote object.
	return newMD5Hex, nil
}

func (o objectAPI) DeleteObject(bucket, object string) *probe.Error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(object) {
		return probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}
	if e := o.storage.DeleteFile(bucket, object); e != nil {
		if e == errVolumeNotFound {
			return probe.NewError(BucketNotFound{Bucket: bucket})
		} else if e == errFileNotFound {
			return probe.NewError(ObjectNotFound{Bucket: bucket})
		}
		if e == errFileNotFound {
			return probe.NewError(ObjectNotFound{
				Bucket: bucket,
				Object: object,
			})
		}
		return probe.NewError(e)
	}
	return nil
}

func (o objectAPI) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, *probe.Error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return ListObjectsInfo{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectPrefix(prefix) {
		return ListObjectsInfo{}, probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: prefix})
	}
	// Verify if delimiter is anything other than '/', which we do not support.
	if delimiter != "" && delimiter != slashPathSeparator {
		return ListObjectsInfo{}, probe.NewError(UnsupportedDelimiter{
			Delimiter: delimiter,
		})
	}
	// Verify if marker has prefix.
	if marker != "" {
		if !strings.HasPrefix(marker, prefix) {
			return ListObjectsInfo{}, probe.NewError(InvalidMarkerPrefixCombination{
				Marker: marker,
				Prefix: prefix,
			})
		}
	}
	recursive := true
	if delimiter == slashPathSeparator {
		recursive = false
	}
	fileInfos, eof, e := o.storage.ListFiles(bucket, prefix, marker, recursive, maxKeys)
	if e != nil {
		if e == errVolumeNotFound {
			return ListObjectsInfo{}, probe.NewError(BucketNotFound{Bucket: bucket})
		}
		return ListObjectsInfo{}, probe.NewError(e)
	}
	if maxKeys == 0 {
		return ListObjectsInfo{}, nil
	}
	result := ListObjectsInfo{IsTruncated: !eof}
	for _, fileInfo := range fileInfos {
		// With delimiter set we fill in NextMarker and Prefixes.
		if delimiter == slashPathSeparator {
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
