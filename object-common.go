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
	"sort"
	"strings"
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
	err := cleanupDir(storage, minioMetaBucket, tmpMetaPrefix)
	if err != nil {
		return toObjectErr(err, minioMetaBucket, tmpMetaPrefix)
	}
	return nil
}

// Cleanup a directory recursively.
func cleanupDir(storage StorageAPI, volume, dirPath string) error {
	var delFunc func(string) error
	// Function to delete entries recursively.
	delFunc = func(entryPath string) error {
		if !strings.HasSuffix(entryPath, slashSeparator) {
			// No trailing "/" means that this is a file which can be deleted.
			return storage.DeleteFile(volume, entryPath)
		}
		// If it's a directory, list and call delFunc() for each entry.
		entries, err := storage.ListDir(volume, entryPath)
		if err != nil {
			if err == errFileNotFound {
				// if dirPath prefix never existed.
				return nil
			}
			return err
		}
		for _, entry := range entries {
			err = delFunc(pathJoin(entryPath, entry))
			if err != nil {
				return err
			}
		}
		return nil
	}
	return delFunc(retainSlash(pathJoin(dirPath)))
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
	// Verify if bucket exists.
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

	// With max keys of zero we have reached eof, return right here.
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
	for i := 0; i < maxKeys; {
		walkResult, ok := <-walker.ch
		if !ok {
			// Closed channel.
			eof = true
			break
		}
		// For any walk error return right away.
		if walkResult.err != nil {
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
		errorIf(err, "Stat failed on bucket "+bucketName+".")
		return false
	}
	return true
}
