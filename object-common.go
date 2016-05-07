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
	"io"
	"path"
	"sort"
	"strings"

	"github.com/Sirupsen/logrus"
)

// Common initialization needed for both object layers.
func initObjectLayer(storage StorageAPI) error {
	// This happens for the first time, but keep this here since this
	// is the only place where it can be made expensive optimizing all
	// other calls. Create minio meta volume, if it doesn't exist yet.
	if err := storage.MakeVol(minioMetaBucket); err != nil {
		if err != errVolumeExists {
			return toObjectErr(err, minioMetaBucket)
		}
	}
	// Cleanup all temp entries upon start.
	err := cleanupAllTmpEntries(storage)
	if err != nil {
		return toObjectErr(err, minioMetaBucket, tmpMetaPrefix)
	}
	return nil
}

/// Common object layer functions.

// makeBucket - create a bucket, is a common function for both object layers.
func makeBucket(storage StorageAPI, bucket string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}
	if err := storage.MakeVol(bucket); err != nil {
		return toObjectErr(err, bucket)
	}
	return nil
}

// getBucketInfo - fetch bucket info, is a common function for both object layers.
func getBucketInfo(storage StorageAPI, bucket string) (BucketInfo, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketInfo{}, BucketNameInvalid{Bucket: bucket}
	}
	vi, err := storage.StatVol(bucket)
	if err != nil {
		return BucketInfo{}, toObjectErr(err, bucket)
	}
	return BucketInfo{
		Name:    bucket,
		Created: vi.Created,
		Total:   vi.Total,
		Free:    vi.Free,
	}, nil
}

// listBuckets - list all buckets, is a common function for both object layers.
func listBuckets(storage StorageAPI) ([]BucketInfo, error) {
	var bucketInfos []BucketInfo
	vols, err := storage.ListVols()
	if err != nil {
		return nil, toObjectErr(err)
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
			Total:   vol.Total,
			Free:    vol.Free,
		})
	}
	sort.Sort(byBucketName(bucketInfos))
	return bucketInfos, nil
}

// deleteBucket - deletes a bucket, is a common function for both the layers.
func deleteBucket(storage StorageAPI, bucket string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}
	if err := storage.DeleteVol(bucket); err != nil {
		return toObjectErr(err, bucket)
	}
	return nil
}

// putObjectCommon - create an object, is a common function for both object layers.
func putObjectCommon(storage StorageAPI, bucket string, object string, size int64, data io.Reader, metadata map[string]string) (string, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return "", BucketNameInvalid{Bucket: bucket}
	}
	// Check whether the bucket exists.
	if !isBucketExist(storage, bucket) {
		return "", BucketNotFound{Bucket: bucket}
	}
	if !IsValidObjectName(object) {
		return "", ObjectNameInvalid{
			Bucket: bucket,
			Object: object,
		}
	}

	tempObj := path.Join(tmpMetaPrefix, bucket, object)
	fileWriter, err := storage.CreateFile(minioMetaBucket, tempObj)
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
			return "", toObjectErr(err, bucket, object)
		}
	} else {
		if _, err = io.Copy(multiWriter, data); err != nil {
			if clErr := safeCloseAndRemove(fileWriter); clErr != nil {
				return "", clErr
			}
			return "", toObjectErr(err, bucket, object)
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
		if clErr := safeCloseAndRemove(fileWriter); clErr != nil {
			return "", clErr
		}
		return "", err
	}
	err = storage.RenameFile(minioMetaBucket, tempObj, bucket, object)
	if err != nil {
		if derr := storage.DeleteFile(minioMetaBucket, tempObj); derr != nil {
			return "", derr
		}
		return "", toObjectErr(err, bucket, object)
	}

	// Return md5sum, successfully wrote object.
	return newMD5Hex, nil
}

func listObjectsCommon(layer ObjectLayer, bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	var storage StorageAPI
	switch l := layer.(type) {
	case xlObjects:
		storage = l.storage
	case fsObjects:
		storage = l.storage
	}

	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return ListObjectsInfo{}, BucketNameInvalid{Bucket: bucket}
	}

	// Verify whether the bucket exists.
	if !isBucketExist(storage, bucket) {
		return ListObjectsInfo{}, BucketNotFound{Bucket: bucket}
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

	// Over flowing count - reset to maxObjectList.
	if maxKeys < 0 || maxKeys > maxObjectList {
		maxKeys = maxObjectList
	}

	// Default is recursive, if delimiter is set then list non recursive.
	recursive := true
	if delimiter == slashSeparator {
		recursive = false
	}

	walker := lookupTreeWalk(layer, listParams{bucket, recursive, marker, prefix})
	if walker == nil {
		walker = startTreeWalk(layer, bucket, prefix, marker, recursive)
	}
	var fileInfos []FileInfo
	var eof bool
	var nextMarker string
	log.Debugf("Reading from the tree walk channel has begun.")
	for i := 0; i < maxKeys; {
		walkResult, ok := <-walker.ch
		if !ok {
			// Closed channel.
			eof = true
			break
		}
		// For any walk error return right away.
		if walkResult.err != nil {
			log.WithFields(logrus.Fields{
				"bucket":    bucket,
				"prefix":    prefix,
				"marker":    marker,
				"recursive": recursive,
			}).Debugf("Walk resulted in an error %s", walkResult.err)
			// File not found is a valid case.
			if walkResult.err == errFileNotFound {
				return ListObjectsInfo{}, nil
			}
			return ListObjectsInfo{}, toObjectErr(walkResult.err, bucket, prefix)
		}
		fileInfo := walkResult.fileInfo
		nextMarker = fileInfo.Name
		fileInfos = append(fileInfos, fileInfo)
		if walkResult.end {
			eof = true
			break
		}
		i++
	}
	params := listParams{bucket, recursive, nextMarker, prefix}
	log.WithFields(logrus.Fields{
		"bucket":    params.bucket,
		"recursive": params.recursive,
		"marker":    params.marker,
		"prefix":    params.prefix,
	}).Debugf("Save the tree walk into map for subsequent requests.")
	if !eof {
		saveTreeWalk(layer, params, walker)
	}

	result := ListObjectsInfo{IsTruncated: !eof}
	for _, fileInfo := range fileInfos {
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

// checks whether bucket exists.
func isBucketExist(storage StorageAPI, bucketName string) bool {
	// Check whether bucket exists.
	_, err := storage.StatVol(bucketName)
	if err != nil {
		if err == errVolumeNotFound {
			return false
		}
		log.Errorf("StatVol failed with %s", err)
		return false
	}
	return true
}
