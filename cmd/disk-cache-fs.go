/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/disk"
	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/lock"
)

const (
	// cache.json object metadata for cached objects.
	cacheMetaJSONFile = "cache.json"

	cacheEnvDelimiter = ";"
)

// cacheFSObjects implements the cache backend operations.
type cacheFSObjects struct {
	*FSObjects
	// caching drive path (from cache "drives" in config.json)
	dir string
	// expiry in days specified in config.json
	expiry int
	// max disk usage pct
	maxDiskUsagePct int
	// purge() listens on this channel to start the cache-purge process
	purgeChan chan struct{}
	// mark false if drive is offline
	online bool
	// mutex to protect updates to online variable
	onlineMutex *sync.RWMutex
}

// Inits the cache directory if it is not init'ed already.
// Initializing implies creation of new FS Object layer.
func newCacheFSObjects(dir string, expiry int, maxDiskUsagePct int) (*cacheFSObjects, error) {
	// Assign a new UUID for FS minio mode. Each server instance
	// gets its own UUID for temporary file transaction.
	fsUUID := mustGetUUID()

	// Initialize meta volume, if volume already exists ignores it.
	if err := initMetaVolumeFS(dir, fsUUID); err != nil {
		return nil, fmt.Errorf("Unable to initialize '.minio.sys' meta volume, %s", err)
	}

	trashPath := pathJoin(dir, minioMetaBucket, cacheTrashDir)
	if err := os.MkdirAll(trashPath, 0777); err != nil {
		return nil, err
	}

	if expiry == 0 {
		expiry = globalCacheExpiry
	}

	// Initialize fs objects.
	fsObjects := &FSObjects{
		fsPath:       dir,
		metaJSONFile: cacheMetaJSONFile,
		fsUUID:       fsUUID,
		rwPool: &fsIOPool{
			readersMap: make(map[string]*lock.RLockedFile),
		},
		nsMutex:       newNSLock(false),
		listPool:      newTreeWalkPool(globalLookupTimeout),
		appendFileMap: make(map[string]*fsAppendFile),
	}

	go fsObjects.cleanupStaleMultipartUploads(context.Background(), globalMultipartCleanupInterval, globalMultipartExpiry, globalServiceDoneCh)

	cacheFS := cacheFSObjects{
		FSObjects:       fsObjects,
		dir:             dir,
		expiry:          expiry,
		maxDiskUsagePct: maxDiskUsagePct,
		purgeChan:       make(chan struct{}),
		online:          true,
		onlineMutex:     &sync.RWMutex{},
	}
	return &cacheFS, nil
}

// Returns if the disk usage is low.
// Disk usage is low if usage is < 80% of cacheMaxDiskUsagePct
// Ex. for a 100GB disk, if maxUsage is configured as 70% then cacheMaxDiskUsagePct is 70G
// hence disk usage is low if the disk usage is less than 56G (because 80% of 70G is 56G)
func (cfs *cacheFSObjects) diskUsageLow() bool {

	minUsage := cfs.maxDiskUsagePct * 80 / 100
	di, err := disk.GetInfo(cfs.dir)
	if err != nil {
		reqInfo := (&logger.ReqInfo{}).AppendTags("cachePath", cfs.dir)
		ctx := logger.SetReqInfo(context.Background(), reqInfo)
		logger.LogIf(ctx, err)
		return false
	}
	usedPercent := (di.Total - di.Free) * 100 / di.Total
	return int(usedPercent) < minUsage
}

// Return if the disk usage is high.
// Disk usage is high if disk used is > cacheMaxDiskUsagePct
func (cfs *cacheFSObjects) diskUsageHigh() bool {
	di, err := disk.GetInfo(cfs.dir)
	if err != nil {
		reqInfo := (&logger.ReqInfo{}).AppendTags("cachePath", cfs.dir)
		ctx := logger.SetReqInfo(context.Background(), reqInfo)
		logger.LogIf(ctx, err)
		return true
	}
	usedPercent := (di.Total - di.Free) * 100 / di.Total
	return int(usedPercent) > cfs.maxDiskUsagePct
}

// Returns if size space can be allocated without exceeding
// max disk usable for caching
func (cfs *cacheFSObjects) diskAvailable(size int64) bool {
	di, err := disk.GetInfo(cfs.dir)
	if err != nil {
		reqInfo := (&logger.ReqInfo{}).AppendTags("cachePath", cfs.dir)
		ctx := logger.SetReqInfo(context.Background(), reqInfo)
		logger.LogIf(ctx, err)
		return false
	}
	usedPercent := (di.Total - (di.Free - uint64(size))) * 100 / di.Total
	return int(usedPercent) < cfs.maxDiskUsagePct
}

// purges all content marked trash from the cache.
func (cfs *cacheFSObjects) purgeTrash() {
	ticker := time.NewTicker(time.Minute * cacheCleanupInterval)
	for {
		select {
		case <-globalServiceDoneCh:
			// Stop the timer.
			ticker.Stop()
			return
		case <-ticker.C:
			trashPath := path.Join(cfs.fsPath, minioMetaBucket, cacheTrashDir)
			entries, err := readDir(trashPath)
			if err != nil {
				return
			}
			for _, entry := range entries {
				ctx := logger.SetReqInfo(context.Background(), &logger.ReqInfo{})
				fi, err := fsStatVolume(ctx, pathJoin(trashPath, entry))
				if err != nil {
					continue
				}
				dir := path.Join(trashPath, fi.Name())

				// Delete all expired cache content.
				fsRemoveAll(ctx, dir)
			}
		}
	}
}

// Purge cache entries that were not accessed.
func (cfs *cacheFSObjects) purge() {
	delimiter := slashSeparator
	maxKeys := 1000
	ctx := context.Background()
	for {
		olderThan := cfs.expiry
		for !cfs.diskUsageLow() {
			// delete unaccessed objects older than expiry duration
			expiry := UTCNow().AddDate(0, 0, -1*olderThan)
			olderThan /= 2
			if olderThan < 1 {
				break
			}
			deletedCount := 0
			buckets, err := cfs.ListBuckets(ctx)
			if err != nil {
				logger.LogIf(ctx, err)
			}
			// Reset cache online status if drive was offline earlier.
			if !cfs.IsOnline() {
				cfs.setOnline(true)
			}
			for _, bucket := range buckets {
				var continuationToken string
				var marker string
				for {
					objects, err := cfs.ListObjects(ctx, bucket.Name, marker, continuationToken, delimiter, maxKeys)
					if err != nil {
						break
					}

					if !objects.IsTruncated {
						break
					}
					marker = objects.NextMarker
					for _, object := range objects.Objects {
						// purge objects that qualify because of cache-control directives or
						// past cache expiry duration.
						if !filterFromCache(object.UserDefined) ||
							!isStaleCache(object) ||
							object.AccTime.After(expiry) {
							continue
						}
						if err = cfs.DeleteObject(ctx, bucket.Name, object.Name); err != nil {
							logger.LogIf(ctx, err)
							continue
						}
						deletedCount++
					}
				}
			}
			if deletedCount == 0 {
				// to avoid a busy loop
				time.Sleep(time.Minute * 30)
			}
		}
		<-cfs.purgeChan
	}
}

// sets cache drive status
func (cfs *cacheFSObjects) setOnline(status bool) {
	cfs.onlineMutex.Lock()
	cfs.online = status
	cfs.onlineMutex.Unlock()
}

// returns true if cache drive is online
func (cfs *cacheFSObjects) IsOnline() bool {
	cfs.onlineMutex.RLock()
	defer cfs.onlineMutex.RUnlock()
	return cfs.online
}

// Caches the object to disk
func (cfs *cacheFSObjects) Put(ctx context.Context, bucket, object string, data *hash.Reader, metadata map[string]string) error {
	if cfs.diskUsageHigh() {
		select {
		case cfs.purgeChan <- struct{}{}:
		default:
		}
		return errDiskFull
	}
	if !cfs.diskAvailable(data.Size()) {
		return errDiskFull
	}
	if _, err := cfs.GetBucketInfo(ctx, bucket); err != nil {
		pErr := cfs.MakeBucketWithLocation(ctx, bucket, "")
		if pErr != nil {
			return pErr
		}
	}
	_, err := cfs.PutObject(ctx, bucket, object, data, metadata)
	// if err is due to disk being offline , mark cache drive as offline
	if IsErr(err, baseErrs...) {
		cfs.setOnline(false)
	}
	return err
}

// Returns the handle for the cached object
func (cfs *cacheFSObjects) Get(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, etag string) (err error) {
	return cfs.GetObject(ctx, bucket, object, startOffset, length, writer, etag)
}

// Deletes the cached object
func (cfs *cacheFSObjects) Delete(ctx context.Context, bucket, object string) (err error) {
	return cfs.DeleteObject(ctx, bucket, object)
}

// convenience function to check if object is cached on this cacheFSObjects
func (cfs *cacheFSObjects) Exists(ctx context.Context, bucket, object string) bool {
	_, err := cfs.GetObjectInfo(ctx, bucket, object)
	return err == nil
}

// Identical to fs PutObject operation except that it uses ETag in metadata
// headers.
func (cfs *cacheFSObjects) PutObject(ctx context.Context, bucket string, object string, data *hash.Reader, metadata map[string]string) (objInfo ObjectInfo, retErr error) {
	fs := cfs.FSObjects
	// Lock the object.
	objectLock := fs.nsMutex.NewNSLock(bucket, object)
	if err := objectLock.GetLock(globalObjectTimeout); err != nil {
		return objInfo, err
	}
	defer objectLock.Unlock()

	// No metadata is set, allocate a new one.
	meta := make(map[string]string)
	for k, v := range metadata {
		meta[k] = v
	}

	var err error

	// Validate if bucket name is valid and exists.
	if _, err = fs.statBucketDir(ctx, bucket); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket)
	}

	fsMeta := newFSMetaV1()
	fsMeta.Meta = meta

	// This is a special case with size as '0' and object ends
	// with a slash separator, we treat it like a valid operation
	// and return success.
	if isObjectDir(object, data.Size()) {
		// Check if an object is present as one of the parent dir.
		if fs.parentDirIsObject(ctx, bucket, path.Dir(object)) {
			return ObjectInfo{}, toObjectErr(errFileAccessDenied, bucket, object)
		}
		if err = mkdirAll(pathJoin(fs.fsPath, bucket, object), 0777); err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
		var fi os.FileInfo
		if fi, err = fsStatDir(ctx, pathJoin(fs.fsPath, bucket, object)); err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
		return fsMeta.ToObjectInfo(bucket, object, fi), nil
	}

	if err = checkPutObjectArgs(ctx, bucket, object, fs, data.Size()); err != nil {
		return ObjectInfo{}, err
	}

	// Check if an object is present as one of the parent dir.
	if fs.parentDirIsObject(ctx, bucket, path.Dir(object)) {
		return ObjectInfo{}, toObjectErr(errFileAccessDenied, bucket, object)
	}

	// Validate input data size and it can never be less than zero.
	if data.Size() < 0 {
		logger.LogIf(ctx, errInvalidArgument)
		return ObjectInfo{}, errInvalidArgument
	}

	var wlk *lock.LockedFile
	if bucket != minioMetaBucket {
		bucketMetaDir := pathJoin(fs.fsPath, minioMetaBucket, bucketMetaPrefix)
		fsMetaPath := pathJoin(bucketMetaDir, bucket, object, fs.metaJSONFile)

		wlk, err = fs.rwPool.Create(fsMetaPath)
		if err != nil {
			logger.LogIf(ctx, err)
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
		// This close will allow for locks to be synchronized on `fs.json`.
		defer wlk.Close()
		defer func() {
			// Remove meta file when PutObject encounters any error
			if retErr != nil {
				tmpDir := pathJoin(fs.fsPath, minioMetaTmpBucket, fs.fsUUID)
				fsRemoveMeta(ctx, bucketMetaDir, fsMetaPath, tmpDir)
			}
		}()
	}

	// Uploaded object will first be written to the temporary location which will eventually
	// be renamed to the actual location. It is first written to the temporary location
	// so that cleaning it up will be easy if the server goes down.
	tempObj := mustGetUUID()

	// Allocate a buffer to Read() from request body
	bufSize := int64(readSizeV1)
	if size := data.Size(); size > 0 && bufSize > size {
		bufSize = size
	}

	buf := make([]byte, int(bufSize))
	fsTmpObjPath := pathJoin(fs.fsPath, minioMetaTmpBucket, fs.fsUUID, tempObj)
	bytesWritten, err := fsCreateFile(ctx, fsTmpObjPath, data, buf, data.Size())
	if err != nil {
		fsRemoveFile(ctx, fsTmpObjPath)
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}
	if fsMeta.Meta["etag"] == "" {
		fsMeta.Meta["etag"] = hex.EncodeToString(data.MD5Current())
	}
	// Should return IncompleteBody{} error when reader has fewer
	// bytes than specified in request header.
	if bytesWritten < data.Size() {
		fsRemoveFile(ctx, fsTmpObjPath)
		return ObjectInfo{}, IncompleteBody{}
	}

	// Delete the temporary object in the case of a
	// failure. If PutObject succeeds, then there would be
	// nothing to delete.
	defer fsRemoveFile(ctx, fsTmpObjPath)

	// Entire object was written to the temp location, now it's safe to rename it to the actual location.
	fsNSObjPath := pathJoin(fs.fsPath, bucket, object)
	if err = fsRenameFile(ctx, fsTmpObjPath, fsNSObjPath); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	if bucket != minioMetaBucket {
		// Write FS metadata after a successful namespace operation.
		if _, err = fsMeta.WriteTo(wlk); err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
	}

	// Stat the file to fetch timestamp, size.
	fi, err := fsStatFile(ctx, pathJoin(fs.fsPath, bucket, object))
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}
	// Success.
	return fsMeta.ToObjectInfo(bucket, object, fi), nil
}

// Implements S3 compatible initiate multipart API. Operation here is identical
// to fs backend implementation - with the exception that cache FS uses the uploadID
// generated on the backend
func (cfs *cacheFSObjects) NewMultipartUpload(ctx context.Context, bucket, object string, meta map[string]string, uploadID string) (string, error) {
	if cfs.diskUsageHigh() {
		select {
		case cfs.purgeChan <- struct{}{}:
		default:
		}
		return "", errDiskFull
	}

	if _, err := cfs.GetBucketInfo(ctx, bucket); err != nil {
		pErr := cfs.MakeBucketWithLocation(ctx, bucket, "")
		if pErr != nil {
			return "", pErr
		}
	}
	fs := cfs.FSObjects
	if err := checkNewMultipartArgs(ctx, bucket, object, fs); err != nil {
		return "", toObjectErr(err, bucket)
	}

	if _, err := fs.statBucketDir(ctx, bucket); err != nil {
		return "", toObjectErr(err, bucket)
	}

	uploadIDDir := fs.getUploadIDDir(bucket, object, uploadID)

	err := mkdirAll(uploadIDDir, 0755)
	if err != nil {
		logger.LogIf(ctx, err)
		return "", err
	}

	// Initialize fs.json values.
	fsMeta := newFSMetaV1()
	fsMeta.Meta = meta

	fsMetaBytes, err := json.Marshal(fsMeta)
	if err != nil {
		logger.LogIf(ctx, err)
		return "", err
	}

	if err = ioutil.WriteFile(pathJoin(uploadIDDir, fs.metaJSONFile), fsMetaBytes, 0644); err != nil {
		logger.LogIf(ctx, err)
		return "", err
	}
	return uploadID, nil
}

// moveBucketToTrash clears cacheFSObjects of bucket contents and moves it to trash folder.
func (cfs *cacheFSObjects) moveBucketToTrash(ctx context.Context, bucket string) (err error) {
	fs := cfs.FSObjects
	bucketLock := fs.nsMutex.NewNSLock(bucket, "")
	if err = bucketLock.GetLock(globalObjectTimeout); err != nil {
		return err
	}
	defer bucketLock.Unlock()
	bucketDir, err := fs.getBucketDir(ctx, bucket)
	if err != nil {
		return toObjectErr(err, bucket)
	}
	trashPath := pathJoin(cfs.fsPath, minioMetaBucket, cacheTrashDir)
	expiredDir := path.Join(trashPath, bucket)
	// Attempt to move regular bucket to expired directory.
	if err = fsRenameDir(bucketDir, expiredDir); err != nil {
		logger.LogIf(ctx, err)
		return toObjectErr(err, bucket)
	}
	// Cleanup all the bucket metadata.
	ominioMetadataBucketDir := pathJoin(fs.fsPath, minioMetaBucket, bucketMetaPrefix, bucket)
	nminioMetadataBucketDir := pathJoin(trashPath, MustGetUUID())
	logger.LogIf(ctx, fsRenameDir(ominioMetadataBucketDir, nminioMetadataBucketDir))
	return nil
}

// Removes a directory only if its empty, handles long
// paths for windows automatically.
func fsRenameDir(dirPath, newPath string) (err error) {
	if dirPath == "" || newPath == "" {
		return errInvalidArgument
	}

	if err = checkPathLength(dirPath); err != nil {
		return err
	}
	if err = checkPathLength(newPath); err != nil {
		return err
	}
	if err = os.Rename(dirPath, newPath); err != nil {
		if os.IsNotExist(err) {
			return errVolumeNotFound
		} else if isSysErrNotEmpty(err) {
			return errVolumeNotEmpty
		}
		return err
	}
	return nil
}
