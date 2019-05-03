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
	"path"
	"sync"

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
var globalObjLayerMutex *sync.RWMutex

// Global object layer, only accessed by newObjectLayerFn().
var globalObjectAPI ObjectLayer

//Global cacheObjects, only accessed by newCacheObjectsFn().
var globalCacheObjectAPI CacheObjectLayer

func init() {
	// Initialize this once per server initialization.
	globalObjLayerMutex = &sync.RWMutex{}
}

// Checks if the object is a directory, this logic uses
// if size == 0 and object ends with slashSeparator then
// returns true.
func isObjectDir(object string, size int64) bool {
	return hasSuffix(object, slashSeparator) && size == 0
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

func deleteBucketMetadata(ctx context.Context, bucket string, objAPI ObjectLayer) {
	// Delete bucket access policy, if present - ignore any errors.
	removePolicyConfig(ctx, objAPI, bucket)

	// Delete notification config, if present - ignore any errors.
	removeNotificationConfig(ctx, objAPI, bucket)

	// Delete listener config, if present - ignore any errors.
	removeListenerConfig(ctx, objAPI, bucket)
}

// Depending on the disk type network or local, initialize storage API.
func newStorageAPI(endpoint Endpoint) (storage StorageAPI, err error) {
	if endpoint.IsLocal {
		return newPosix(endpoint.Path)
	}

	return newStorageRESTClient(endpoint)
}

// Cleanup a directory recursively.
func cleanupDir(ctx context.Context, storage StorageAPI, volume, dirPath string) error {
	var delFunc func(string) error
	// Function to delete entries recursively.
	delFunc = func(entryPath string) error {
		if !hasSuffix(entryPath, slashSeparator) {
			// Delete the file entry.
			err := storage.DeleteFile(volume, entryPath)
			logger.LogIf(ctx, err)
			return err
		}

		// If it's a directory, list and call delFunc() for each entry.
		entries, err := storage.ListDir(volume, entryPath, -1, "")
		// If entryPath prefix never existed, safe to ignore.
		if err == errFileNotFound {
			return nil
		} else if err != nil { // For any other errors fail.
			logger.LogIf(ctx, err)
			return err
		} // else on success..

		// Entry path is empty, just delete it.
		if len(entries) == 0 {
			err = storage.DeleteFile(volume, path.Clean(entryPath))
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

// Removes notification.xml for a given bucket, only used during DeleteBucket.
func removeNotificationConfig(ctx context.Context, objAPI ObjectLayer, bucket string) error {
	// Verify bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}

	ncPath := path.Join(bucketConfigPrefix, bucket, bucketNotificationConfig)
	return objAPI.DeleteObject(ctx, minioMetaBucket, ncPath)
}

// Remove listener configuration from storage layer. Used when a bucket is deleted.
func removeListenerConfig(ctx context.Context, objAPI ObjectLayer, bucket string) error {
	// make the path
	lcPath := path.Join(bucketConfigPrefix, bucket, bucketListenerConfig)
	return objAPI.DeleteObject(ctx, minioMetaBucket, lcPath)
}

func listObjects(ctx context.Context, obj ObjectLayer, bucket, prefix, marker, delimiter string, maxKeys int, tpool *TreeWalkPool, listDir ListDirFunc, getObjInfo func(context.Context, string, string) (ObjectInfo, error), getObjectInfoDirs ...func(context.Context, string, string) (ObjectInfo, error)) (loi ListObjectsInfo, err error) {
	if err := checkListObjsArgs(ctx, bucket, prefix, marker, delimiter, obj); err != nil {
		return loi, err
	}

	// Marker is set validate pre-condition.
	if marker != "" {
		// Marker not common with prefix is not implemented. Send an empty response
		if !hasPrefix(marker, prefix) {
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
	if delimiter == slashSeparator && prefix == slashSeparator {
		return loi, nil
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
		if hasSuffix(walkResult.entry, slashSeparator) {
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
				errXLReadQuorum,
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
		if objInfo.IsDir && delimiter == slashSeparator {
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
