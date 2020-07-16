/*
 * MinIO Cloud Storage, (C) 2016-2019 MinIO, Inc.
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

package cmd

import (
	"context"
	"sync"

	"strings"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/minio/cmd/logger"
)

const (
	// Block size used for all internal operations version 1.
	blockSizeV1 = 10 * humanize.MiByte

	// Staging buffer read size for all internal operations version 1.
	readSizeV1 = 1 * humanize.MiByte

	// Buckets meta prefix.
	bucketMetaPrefix = "buckets"

	// ETag (hex encoded md5sum) of empty string.
	emptyETag = "d41d8cd98f00b204e9800998ecf8427e"
)

// Global object layer mutex, used for safely updating object layer.
var globalObjLayerMutex sync.RWMutex

// Global object layer, only accessed by globalObjectAPI.
var globalObjectAPI ObjectLayer

//Global cacheObjects, only accessed by newCacheObjectsFn().
var globalCacheObjectAPI CacheObjectLayer

// Checks if the object is a directory, this logic uses
// if size == 0 and object ends with SlashSeparator then
// returns true.
func isObjectDir(object string, size int64) bool {
	return HasSuffix(object, SlashSeparator) && size == 0
}

// Converts just bucket, object metadata into ObjectInfo datatype.
func dirObjectInfo(bucket, object string, size int64, metadata map[string]string) ObjectInfo {
	// This is a special case with size as '0' and object ends with
	// a slash separator, we treat it like a valid operation and
	// return success.
	etag := metadata["etag"]
	delete(metadata, "etag")
	if etag == "" {
		etag = emptyETag
	}

	return ObjectInfo{
		Bucket:      bucket,
		Name:        object,
		ModTime:     UTCNow(),
		ContentType: "application/octet-stream",
		IsDir:       true,
		Size:        size,
		ETag:        etag,
		UserDefined: metadata,
	}
}

// Depending on the disk type network or local, initialize storage API.
func newStorageAPI(endpoint Endpoint) (storage StorageAPI, err error) {
	if endpoint.IsLocal {
		storage, err := newXLStorage(endpoint.Path, endpoint.Host)
		if err != nil {
			return nil, err
		}
		return &xlStorageDiskIDCheck{storage: storage}, nil
	}

	return newStorageRESTClient(endpoint), nil
}

// Cleanup a directory recursively.
func cleanupDir(ctx context.Context, storage StorageAPI, volume, dirPath string) error {
	var delFunc func(string) error
	// Function to delete entries recursively.
	delFunc = func(entryPath string) error {
		if !HasSuffix(entryPath, SlashSeparator) {
			// Delete the file entry.
			err := storage.DeleteFile(volume, entryPath)
			logger.LogIf(ctx, err)
			return err
		}

		// If it's a directory, list and call delFunc() for each entry.
		entries, err := storage.ListDir(volume, entryPath, -1)
		// If entryPath prefix never existed, safe to ignore.
		if err == errFileNotFound {
			return nil
		} else if err != nil { // For any other errors fail.
			logger.LogIf(ctx, err)
			return err
		} // else on success..

		// Entry path is empty, just delete it.
		if len(entries) == 0 {
			err = storage.DeleteFile(volume, entryPath)
			logger.LogIf(ctx, err)
			return err
		}

		// Recurse and delete all other entries.
		for _, entry := range entries {
			if err = delFunc(pathJoin(entryPath, entry)); err != nil {
				return err
			}
		}
		return nil
	}
	err := delFunc(retainSlash(pathJoin(dirPath)))
	return err
}

func listObjectsNonSlash(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int, tpool *TreeWalkPool, listDir ListDirFunc, getObjInfo func(context.Context, string, string) (ObjectInfo, error), getObjectInfoDirs ...func(context.Context, string, string) (ObjectInfo, error)) (loi ListObjectsInfo, err error) {
	endWalkCh := make(chan struct{})
	defer close(endWalkCh)
	recursive := true
	walkResultCh := startTreeWalk(ctx, bucket, prefix, "", recursive, listDir, endWalkCh)

	var objInfos []ObjectInfo
	var eof bool
	var prevPrefix string

	for {
		if len(objInfos) == maxKeys {
			break
		}
		result, ok := <-walkResultCh
		if !ok {
			eof = true
			break
		}

		var objInfo ObjectInfo
		var err error

		index := strings.Index(strings.TrimPrefix(result.entry, prefix), delimiter)
		if index == -1 {
			objInfo, err = getObjInfo(ctx, bucket, result.entry)
			if err != nil {
				// Ignore errFileNotFound as the object might have got
				// deleted in the interim period of listing and getObjectInfo(),
				// ignore quorum error as it might be an entry from an outdated disk.
				if IsErrIgnored(err, []error{
					errFileNotFound,
					errErasureReadQuorum,
				}...) {
					continue
				}
				return loi, toObjectErr(err, bucket, prefix)
			}
		} else {
			index = len(prefix) + index + len(delimiter)
			currPrefix := result.entry[:index]
			if currPrefix == prevPrefix {
				continue
			}
			prevPrefix = currPrefix

			objInfo = ObjectInfo{
				Bucket: bucket,
				Name:   currPrefix,
				IsDir:  true,
			}
		}

		if objInfo.Name <= marker {
			continue
		}

		objInfos = append(objInfos, objInfo)
		if result.end {
			eof = true
			break
		}
	}

	result := ListObjectsInfo{}
	for _, objInfo := range objInfos {
		if objInfo.IsDir {
			result.Prefixes = append(result.Prefixes, objInfo.Name)
			continue
		}
		result.Objects = append(result.Objects, objInfo)
	}

	if !eof {
		result.IsTruncated = true
		if len(objInfos) > 0 {
			result.NextMarker = objInfos[len(objInfos)-1].Name
		}
	}

	return result, nil
}

// Walk a bucket, optionally prefix recursively, until we have returned
// all the content to objectInfo channel, it is callers responsibility
// to allocate a receive channel for ObjectInfo, upon any unhandled
// error walker returns error. Optionally if context.Done() is received
// then Walk() stops the walker.
func fsWalk(ctx context.Context, obj ObjectLayer, bucket, prefix string, listDir ListDirFunc, results chan<- ObjectInfo, getObjInfo func(context.Context, string, string) (ObjectInfo, error), getObjectInfoDirs ...func(context.Context, string, string) (ObjectInfo, error)) error {
	if err := checkListObjsArgs(ctx, bucket, prefix, "", obj); err != nil {
		// Upon error close the channel.
		close(results)
		return err
	}

	walkResultCh := startTreeWalk(ctx, bucket, prefix, "", true, listDir, ctx.Done())

	go func() {
		defer close(results)

		for {
			walkResult, ok := <-walkResultCh
			if !ok {
				break
			}

			var objInfo ObjectInfo
			var err error
			if HasSuffix(walkResult.entry, SlashSeparator) {
				for _, getObjectInfoDir := range getObjectInfoDirs {
					objInfo, err = getObjectInfoDir(ctx, bucket, walkResult.entry)
					if err == nil {
						break
					}
					if err == errFileNotFound {
						err = nil
						objInfo = ObjectInfo{
							Bucket: bucket,
							Name:   walkResult.entry,
							IsDir:  true,
						}
					}
				}
			} else {
				objInfo, err = getObjInfo(ctx, bucket, walkResult.entry)
			}
			if err != nil {
				continue
			}
			results <- objInfo
			if walkResult.end {
				break
			}
		}
	}()
	return nil
}

func listObjects(ctx context.Context, obj ObjectLayer, bucket, prefix, marker, delimiter string, maxKeys int, tpool *TreeWalkPool, listDir ListDirFunc, getObjInfo func(context.Context, string, string) (ObjectInfo, error), getObjectInfoDirs ...func(context.Context, string, string) (ObjectInfo, error)) (loi ListObjectsInfo, err error) {
	if delimiter != SlashSeparator && delimiter != "" {
		return listObjectsNonSlash(ctx, bucket, prefix, marker, delimiter, maxKeys, tpool, listDir, getObjInfo, getObjectInfoDirs...)
	}

	if err := checkListObjsArgs(ctx, bucket, prefix, marker, obj); err != nil {
		return loi, err
	}

	// Marker is set validate pre-condition.
	if marker != "" {
		// Marker not common with prefix is not implemented. Send an empty response
		if !HasPrefix(marker, prefix) {
			return loi, nil
		}
	}

	// With max keys of zero we have reached eof, return right here.
	if maxKeys == 0 {
		return loi, nil
	}

	// For delimiter and prefix as '/' we do not list anything at all
	// since according to s3 spec we stop at the 'delimiter'
	// along // with the prefix. On a flat namespace with 'prefix'
	// as '/' we don't have any entries, since all the keys are
	// of form 'keyName/...'
	if delimiter == SlashSeparator && prefix == SlashSeparator {
		return loi, nil
	}

	// Over flowing count - reset to maxObjectList.
	if maxKeys < 0 || maxKeys > maxObjectList {
		maxKeys = maxObjectList
	}

	// Default is recursive, if delimiter is set then list non recursive.
	recursive := true
	if delimiter == SlashSeparator {
		recursive = false
	}

	walkResultCh, endWalkCh := tpool.Release(listParams{bucket, recursive, marker, prefix})
	if walkResultCh == nil {
		endWalkCh = make(chan struct{})
		walkResultCh = startTreeWalk(ctx, bucket, prefix, marker, recursive, listDir, endWalkCh)
	}

	var objInfos []ObjectInfo
	var eof bool
	var nextMarker string

	// List until maxKeys requested.
	for i := 0; i < maxKeys; {
		walkResult, ok := <-walkResultCh
		if !ok {
			// Closed channel.
			eof = true
			break
		}

		var objInfo ObjectInfo
		var err error
		if HasSuffix(walkResult.entry, SlashSeparator) {
			for _, getObjectInfoDir := range getObjectInfoDirs {
				objInfo, err = getObjectInfoDir(ctx, bucket, walkResult.entry)
				if err == nil {
					break
				}
				if err == errFileNotFound {
					err = nil
					objInfo = ObjectInfo{
						Bucket: bucket,
						Name:   walkResult.entry,
						IsDir:  true,
					}
				}
			}
		} else {
			objInfo, err = getObjInfo(ctx, bucket, walkResult.entry)
		}
		if err != nil {
			// Ignore errFileNotFound as the object might have got
			// deleted in the interim period of listing and getObjectInfo(),
			// ignore quorum error as it might be an entry from an outdated disk.
			if IsErrIgnored(err, []error{
				errFileNotFound,
				errErasureReadQuorum,
			}...) {
				continue
			}
			return loi, toObjectErr(err, bucket, prefix)
		}
		nextMarker = objInfo.Name
		objInfos = append(objInfos, objInfo)
		if walkResult.end {
			eof = true
			break
		}
		i++
	}

	// Save list routine for the next marker if we haven't reached EOF.
	params := listParams{bucket, recursive, nextMarker, prefix}
	if !eof {
		tpool.Set(params, walkResultCh, endWalkCh)
	}

	result := ListObjectsInfo{}
	for _, objInfo := range objInfos {
		if objInfo.IsDir && delimiter == SlashSeparator {
			result.Prefixes = append(result.Prefixes, objInfo.Name)
			continue
		}
		result.Objects = append(result.Objects, objInfo)
	}

	if !eof {
		result.IsTruncated = true
		if len(objInfos) > 0 {
			result.NextMarker = objInfos[len(objInfos)-1].Name
		}
	}

	// Success.
	return result, nil
}
