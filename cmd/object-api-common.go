/*
 * Minio Cloud Storage, (C) 2016, 2017, 2018 Minio, Inc.
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
	"github.com/minio/minio/pkg/errors"
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

func deleteBucketMetadata(bucket string, objAPI ObjectLayer) {
	// Delete bucket access policy, if present - ignore any errors.
	_ = removeBucketPolicy(bucket, objAPI)

	// Delete notification config, if present - ignore any errors.
	_ = removeNotificationConfig(objAPI, bucket)

	// Delete listener config, if present - ignore any errors.
	_ = removeListenerConfig(objAPI, bucket)
}

// Depending on the disk type network or local, initialize storage API.
func newStorageAPI(endpoint Endpoint) (storage StorageAPI, err error) {
	if endpoint.IsLocal {
		return newPosix(endpoint.Path)
	}

	return newStorageRPC(endpoint), nil
}

// Cleanup a directory recursively.
func cleanupDir(storage StorageAPI, volume, dirPath string) error {
	var delFunc func(string) error
	// Function to delete entries recursively.
	delFunc = func(entryPath string) error {
		if !hasSuffix(entryPath, slashSeparator) {
			// Delete the file entry.
			return errors.Trace(storage.DeleteFile(volume, entryPath))
		}

		// If it's a directory, list and call delFunc() for each entry.
		entries, err := storage.ListDir(volume, entryPath)
		// If entryPath prefix never existed, safe to ignore.
		if err == errFileNotFound {
			return nil
		} else if err != nil { // For any other errors fail.
			return errors.Trace(err)
		} // else on success..

		// Entry path is empty, just delete it.
		if len(entries) == 0 {
			return errors.Trace(storage.DeleteFile(volume, path.Clean(entryPath)))
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
func removeNotificationConfig(objAPI ObjectLayer, bucket string) error {
	// Verify bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}

	ncPath := path.Join(bucketConfigPrefix, bucket, bucketNotificationConfig)

	return objAPI.DeleteObject(context.Background(), minioMetaBucket, ncPath)
}

// Remove listener configuration from storage layer. Used when a bucket is deleted.
func removeListenerConfig(objAPI ObjectLayer, bucket string) error {
	// make the path
	lcPath := path.Join(bucketConfigPrefix, bucket, bucketListenerConfig)

	return objAPI.DeleteObject(context.Background(), minioMetaBucket, lcPath)
}
