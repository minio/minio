/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/djherbis/atime"
	"github.com/minio/minio/cmd/config/cache"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/color"
	"github.com/minio/minio/pkg/sync/errgroup"
	"github.com/minio/minio/pkg/wildcard"
)

const (
	cacheBlkSize = int64(1 * 1024 * 1024)
)

// CacheStorageInfo - represents total, free capacity of
// underlying cache storage.
type CacheStorageInfo struct {
	Total uint64 // Total cache disk space.
	Free  uint64 // Free cache available space.
}

// CacheObjectLayer implements primitives for cache object API layer.
type CacheObjectLayer interface {
	// Object operations.
	GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (gr *GetObjectReader, err error)
	GetObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (objInfo ObjectInfo, err error)
	DeleteObject(ctx context.Context, bucket, object string) error
	DeleteObjects(ctx context.Context, bucket string, objects []string) ([]error, error)
	PutObject(ctx context.Context, bucket, object string, data *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error)
	// Storage operations.
	StorageInfo(ctx context.Context) CacheStorageInfo
	CacheStats() *CacheStats
}

// Abstracts disk caching - used by the S3 layer
type cacheObjects struct {
	// slice of cache drives
	cache []*diskCache
	// file path patterns to exclude from cache
	exclude []string

	// if true migration is in progress from v1 to v2
	migrating bool
	// mutex to protect migration bool
	migMutex sync.Mutex

	// nsMutex namespace lock
	nsMutex *nsLockMap

	// Cache stats
	cacheStats *CacheStats

	// Object functions pointing to the corresponding functions of backend implementation.
	NewNSLockFn      func(ctx context.Context, bucket, object string) RWLocker
	GetObjectNInfoFn func(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (gr *GetObjectReader, err error)
	GetObjectInfoFn  func(ctx context.Context, bucket, object string, opts ObjectOptions) (objInfo ObjectInfo, err error)
	DeleteObjectFn   func(ctx context.Context, bucket, object string) error
	DeleteObjectsFn  func(ctx context.Context, bucket string, objects []string) ([]error, error)
	PutObjectFn      func(ctx context.Context, bucket, object string, data *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error)
}

func (c *cacheObjects) delete(ctx context.Context, dcache *diskCache, bucket, object string) (err error) {
	cLock := c.NewNSLockFn(ctx, bucket, object)
	if err := cLock.GetLock(globalObjectTimeout); err != nil {
		return err
	}
	defer cLock.Unlock()
	return dcache.Delete(ctx, bucket, object)
}

func (c *cacheObjects) put(ctx context.Context, dcache *diskCache, bucket, object string, data io.Reader, size int64, rs *HTTPRangeSpec, opts ObjectOptions) error {
	cLock := c.NewNSLockFn(ctx, bucket, object)
	if err := cLock.GetLock(globalObjectTimeout); err != nil {
		return err
	}
	defer cLock.Unlock()
	return dcache.Put(ctx, bucket, object, data, size, rs, opts)
}

func (c *cacheObjects) get(ctx context.Context, dcache *diskCache, bucket, object string, rs *HTTPRangeSpec, h http.Header, opts ObjectOptions) (gr *GetObjectReader, err error) {
	cLock := c.NewNSLockFn(ctx, bucket, object)
	if err := cLock.GetRLock(globalObjectTimeout); err != nil {
		return nil, err
	}

	defer cLock.RUnlock()
	return dcache.Get(ctx, bucket, object, rs, h, opts)
}

func (c *cacheObjects) stat(ctx context.Context, dcache *diskCache, bucket, object string) (oi ObjectInfo, err error) {
	cLock := c.NewNSLockFn(ctx, bucket, object)
	if err := cLock.GetRLock(globalObjectTimeout); err != nil {
		return oi, err
	}

	defer cLock.RUnlock()
	return dcache.Stat(ctx, bucket, object)
}

func (c *cacheObjects) statRange(ctx context.Context, dcache *diskCache, bucket, object string, rs *HTTPRangeSpec) (oi ObjectInfo, err error) {
	cLock := c.NewNSLockFn(ctx, bucket, object)
	if err := cLock.GetRLock(globalObjectTimeout); err != nil {
		return oi, err
	}

	defer cLock.RUnlock()
	oi, _, err = dcache.statRange(ctx, bucket, object, rs)
	return oi, err
}

// DeleteObject clears cache entry if backend delete operation succeeds
func (c *cacheObjects) DeleteObject(ctx context.Context, bucket, object string) (err error) {
	if err = c.DeleteObjectFn(ctx, bucket, object); err != nil {
		return
	}
	if c.isCacheExclude(bucket, object) || c.skipCache() {
		return
	}

	dcache, cerr := c.getCacheLoc(bucket, object)
	if cerr != nil {
		return
	}
	if dcache.Exists(ctx, bucket, object) {
		c.delete(ctx, dcache, bucket, object)
	}
	return
}

// DeleteObjects batch deletes objects in slice, and clears any cached entries
func (c *cacheObjects) DeleteObjects(ctx context.Context, bucket string, objects []string) ([]error, error) {
	errs := make([]error, len(objects))
	for idx, object := range objects {
		errs[idx] = c.DeleteObject(ctx, bucket, object)
	}
	return errs, nil
}

// construct a metadata k-v map
func getMetadata(objInfo ObjectInfo) map[string]string {
	metadata := make(map[string]string)
	metadata["etag"] = objInfo.ETag
	metadata["content-type"] = objInfo.ContentType
	if objInfo.ContentEncoding != "" {
		metadata["content-encoding"] = objInfo.ContentEncoding
	}
	if objInfo.Expires != timeSentinel {
		metadata["expires"] = objInfo.Expires.Format(http.TimeFormat)
	}
	for k, v := range objInfo.UserDefined {
		metadata[k] = v
	}
	return metadata
}

// marks cache hit
func (c *cacheObjects) incCacheStats(size int64) {
	c.cacheStats.incHit()
	c.cacheStats.incBytesServed(size)
}

func (c *cacheObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (gr *GetObjectReader, err error) {
	if c.isCacheExclude(bucket, object) || c.skipCache() {
		return c.GetObjectNInfoFn(ctx, bucket, object, rs, h, lockType, opts)
	}
	var cc cacheControl
	var cacheObjSize int64
	// fetch diskCache if object is currently cached or nearest available cache drive
	dcache, err := c.getCacheToLoc(ctx, bucket, object)
	if err != nil {
		return c.GetObjectNInfoFn(ctx, bucket, object, rs, h, lockType, opts)
	}

	cacheReader, cacheErr := c.get(ctx, dcache, bucket, object, rs, h, opts)
	if cacheErr == nil {
		cacheObjSize = cacheReader.ObjInfo.Size
		if rs != nil {
			if _, len, err := rs.GetOffsetLength(cacheObjSize); err == nil {
				cacheObjSize = len
			}
		}
		cc = cacheControlOpts(cacheReader.ObjInfo)
		if (!cc.isEmpty() && !cc.isStale(cacheReader.ObjInfo.ModTime)) ||
			cc.onlyIfCached {
			// This is a cache hit, mark it so
			bytesServed := cacheReader.ObjInfo.Size
			if rs != nil {
				if _, len, err := rs.GetOffsetLength(bytesServed); err == nil {
					bytesServed = len
				}
			}
			c.cacheStats.incHit()
			c.cacheStats.incBytesServed(bytesServed)
			return cacheReader, nil
		}
		if cc.noStore {
			c.cacheStats.incMiss()
			return c.GetObjectNInfo(ctx, bucket, object, rs, h, lockType, opts)
		}
	}

	objInfo, err := c.GetObjectInfoFn(ctx, bucket, object, opts)
	if backendDownError(err) && cacheErr == nil {
		c.incCacheStats(cacheObjSize)
		return cacheReader, nil
	} else if err != nil {
		if _, ok := err.(ObjectNotFound); ok {
			if cacheErr == nil {
				cacheReader.Close()
				// Delete cached entry if backend object
				// was deleted.
				dcache.Delete(ctx, bucket, object)
			}
		}
		c.cacheStats.incMiss()
		return nil, err
	}

	if !objInfo.IsCacheable() {
		c.cacheStats.incMiss()
		return c.GetObjectNInfoFn(ctx, bucket, object, rs, h, lockType, opts)
	}

	if cacheErr == nil {
		// if ETag matches for stale cache entry, serve from cache
		if cacheReader.ObjInfo.ETag == objInfo.ETag {
			// Update metadata in case server-side copy might have changed object metadata
			dcache.updateMetadataIfChanged(ctx, bucket, object, objInfo, cacheReader.ObjInfo)
			c.incCacheStats(cacheObjSize)
			return cacheReader, nil
		}
		cacheReader.Close()
		// Object is stale, so delete from cache
		c.delete(ctx, dcache, bucket, object)
	}

	// Reaching here implies cache miss
	c.cacheStats.incMiss()
	// Since we got here, we are serving the request from backend,
	// and also adding the object to the cache.
	if !dcache.diskUsageLow() {
		select {
		case dcache.purgeChan <- struct{}{}:
		default:
		}
	}
	if !dcache.diskAvailable(objInfo.Size) {
		return c.GetObjectNInfoFn(ctx, bucket, object, rs, h, lockType, opts)
	}

	if rs != nil {
		go func() {
			// fill cache in the background for range GET requests
			bReader, bErr := c.GetObjectNInfoFn(ctx, bucket, object, rs, h, lockType, opts)
			if bErr != nil {
				return
			}
			defer bReader.Close()
			oi, err := c.statRange(ctx, dcache, bucket, object, rs)
			// avoid cache overwrite if another background routine filled cache
			if err != nil || oi.ETag != bReader.ObjInfo.ETag {
				c.put(ctx, dcache, bucket, object, bReader, bReader.ObjInfo.Size, rs, ObjectOptions{UserDefined: getMetadata(bReader.ObjInfo)})
			}
		}()
		return c.GetObjectNInfoFn(ctx, bucket, object, rs, h, lockType, opts)
	}
	bkReader, bkErr := c.GetObjectNInfoFn(ctx, bucket, object, rs, h, lockType, opts)
	if bkErr != nil {
		return nil, bkErr
	}
	// Initialize pipe.
	pipeReader, pipeWriter := io.Pipe()
	teeReader := io.TeeReader(bkReader, pipeWriter)
	go func() {
		putErr := c.put(ctx, dcache, bucket, object, io.LimitReader(pipeReader, bkReader.ObjInfo.Size), bkReader.ObjInfo.Size, nil, ObjectOptions{UserDefined: getMetadata(bkReader.ObjInfo)})
		// close the write end of the pipe, so the error gets
		// propagated to getObjReader
		pipeWriter.CloseWithError(putErr)
	}()
	cleanupBackend := func() { bkReader.Close() }
	cleanupPipe := func() { pipeWriter.Close() }
	return NewGetObjectReaderFromReader(teeReader, bkReader.ObjInfo, opts.CheckCopyPrecondFn, cleanupBackend, cleanupPipe)
}

// Returns ObjectInfo from cache if available.
func (c *cacheObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	getObjectInfoFn := c.GetObjectInfoFn

	if c.isCacheExclude(bucket, object) || c.skipCache() {
		return getObjectInfoFn(ctx, bucket, object, opts)
	}

	// fetch diskCache if object is currently cached or nearest available cache drive
	dcache, err := c.getCacheToLoc(ctx, bucket, object)
	if err != nil {
		return getObjectInfoFn(ctx, bucket, object, opts)
	}
	var cc cacheControl
	// if cache control setting is valid, avoid HEAD operation to backend
	cachedObjInfo, cerr := c.stat(ctx, dcache, bucket, object)
	if cerr == nil {
		cc = cacheControlOpts(cachedObjInfo)
		if !cc.isStale(cachedObjInfo.ModTime) {
			// This is a cache hit, mark it so
			c.cacheStats.incHit()
			return cachedObjInfo, nil
		}
	}

	objInfo, err := getObjectInfoFn(ctx, bucket, object, opts)
	if err != nil {
		if _, ok := err.(ObjectNotFound); ok {
			// Delete the cached entry if backend object was deleted.
			c.delete(ctx, dcache, bucket, object)
			c.cacheStats.incMiss()
			return ObjectInfo{}, err
		}
		if !backendDownError(err) {
			c.cacheStats.incMiss()
			return ObjectInfo{}, err
		}
		if cerr == nil {
			// This is a cache hit, mark it so
			c.cacheStats.incHit()
			return cachedObjInfo, nil
		}
		c.cacheStats.incMiss()
		return ObjectInfo{}, BackendDown{}
	}
	// Reaching here implies cache miss
	c.cacheStats.incMiss()
	// when backend is up, do a sanity check on cached object
	if cerr != nil {
		return objInfo, nil
	}
	if cachedObjInfo.ETag != objInfo.ETag {
		// Delete the cached entry if the backend object was replaced.
		c.delete(ctx, dcache, bucket, object)
	}
	return objInfo, nil
}

// StorageInfo - returns underlying storage statistics.
func (c *cacheObjects) StorageInfo(ctx context.Context) (cInfo CacheStorageInfo) {
	var total, free uint64
	for _, cache := range c.cache {
		if cache == nil {
			continue
		}
		info, err := getDiskInfo(cache.dir)
		logger.GetReqInfo(ctx).AppendTags("cachePath", cache.dir)
		logger.LogIf(ctx, err)
		total += info.Total
		free += info.Free
	}
	return CacheStorageInfo{
		Total: total,
		Free:  free,
	}
}

// CacheStats - returns underlying storage statistics.
func (c *cacheObjects) CacheStats() (cs *CacheStats) {
	return c.cacheStats
}

// skipCache() returns true if cache migration is in progress
func (c *cacheObjects) skipCache() bool {
	c.migMutex.Lock()
	defer c.migMutex.Unlock()
	return c.migrating
}

// Returns true if object should be excluded from cache
func (c *cacheObjects) isCacheExclude(bucket, object string) bool {
	// exclude directories from cache
	if strings.HasSuffix(object, SlashSeparator) {
		return true
	}
	for _, pattern := range c.exclude {
		matchStr := fmt.Sprintf("%s/%s", bucket, object)
		if ok := wildcard.MatchSimple(pattern, matchStr); ok {
			return true
		}
	}
	return false
}

// choose a cache deterministically based on hash of bucket,object. The hash index is treated as
// a hint. In the event that the cache drive at hash index is offline, treat the list of cache drives
// as a circular buffer and walk through them starting at hash index until an online drive is found.
func (c *cacheObjects) getCacheLoc(bucket, object string) (*diskCache, error) {
	index := c.hashIndex(bucket, object)
	numDisks := len(c.cache)
	for k := 0; k < numDisks; k++ {
		i := (index + k) % numDisks
		if c.cache[i] == nil {
			continue
		}
		if c.cache[i].IsOnline() {
			return c.cache[i], nil
		}
	}
	return nil, errDiskNotFound
}

// get cache disk where object is currently cached for a GET operation. If object does not exist at that location,
// treat the list of cache drives as a circular buffer and walk through them starting at hash index
// until an online drive is found.If object is not found, fall back to the first online cache drive
// closest to the hash index, so that object can be re-cached.
func (c *cacheObjects) getCacheToLoc(ctx context.Context, bucket, object string) (*diskCache, error) {
	index := c.hashIndex(bucket, object)

	numDisks := len(c.cache)
	// save first online cache disk closest to the hint index
	var firstOnlineDisk *diskCache
	for k := 0; k < numDisks; k++ {
		i := (index + k) % numDisks
		if c.cache[i] == nil {
			continue
		}
		if c.cache[i].IsOnline() {
			if firstOnlineDisk == nil {
				firstOnlineDisk = c.cache[i]
			}
			if c.cache[i].Exists(ctx, bucket, object) {
				return c.cache[i], nil
			}
		}
	}

	if firstOnlineDisk != nil {
		return firstOnlineDisk, nil
	}
	return nil, errDiskNotFound
}

// Compute a unique hash sum for bucket and object
func (c *cacheObjects) hashIndex(bucket, object string) int {
	return crcHashMod(pathJoin(bucket, object), len(c.cache))
}

// newCache initializes the cacheFSObjects for the "drives" specified in config.json
// or the global env overrides.
func newCache(config cache.Config) ([]*diskCache, bool, error) {
	var caches []*diskCache
	ctx := logger.SetReqInfo(context.Background(), &logger.ReqInfo{})
	formats, migrating, err := loadAndValidateCacheFormat(ctx, config.Drives)
	if err != nil {
		return nil, false, err
	}
	for i, dir := range config.Drives {
		// skip diskCache creation for cache drives missing a format.json
		if formats[i] == nil {
			caches = append(caches, nil)
			continue
		}
		if err := checkAtimeSupport(dir); err != nil {
			return nil, false, errors.New("Atime support required for disk caching")
		}

		quota := config.MaxUse
		if quota == 0 {
			quota = config.Quota
		}

		cache, err := newDiskCache(dir, config.Expiry, quota)
		if err != nil {
			return nil, false, err
		}
		// Start the purging go-routine for entries that have expired if no migration in progress
		if !migrating {
			go cache.purge()
		}

		caches = append(caches, cache)
	}
	return caches, migrating, nil
}

// Return error if Atime is disabled on the O/S
func checkAtimeSupport(dir string) (err error) {
	file, err := ioutil.TempFile(dir, "prefix")
	if err != nil {
		return
	}
	defer os.Remove(file.Name())
	finfo1, err := os.Stat(file.Name())
	if err != nil {
		return
	}
	// add a sleep to ensure atime change is detected
	time.Sleep(10 * time.Millisecond)

	if _, err = io.Copy(ioutil.Discard, file); err != nil {
		return
	}

	finfo2, err := os.Stat(file.Name())

	if atime.Get(finfo2).Equal(atime.Get(finfo1)) {
		return errors.New("Atime not supported")
	}
	return
}
func (c *cacheObjects) migrateCacheFromV1toV2(ctx context.Context) {
	logStartupMessage(color.Blue("Cache migration initiated ...."))

	g := errgroup.WithNErrs(len(c.cache))
	for index, dc := range c.cache {
		if dc == nil {
			continue
		}
		index := index
		g.Go(func() error {
			// start migration from V1 to V2
			return migrateOldCache(ctx, c.cache[index])
		}, index)
	}

	errCnt := 0
	for index, err := range g.Wait() {
		if err != nil {
			errCnt++
			logger.LogIf(ctx, err)
			continue
		}
		go c.cache[index].purge()
	}

	if errCnt > 0 {
		return
	}

	// update migration status
	c.migMutex.Lock()
	defer c.migMutex.Unlock()
	c.migrating = false
	logStartupMessage(color.Blue("Cache migration completed successfully."))
}

// PutObject - caches the uploaded object for single Put operations
func (c *cacheObjects) PutObject(ctx context.Context, bucket, object string, r *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	putObjectFn := c.PutObjectFn
	dcache, err := c.getCacheToLoc(ctx, bucket, object)
	if err != nil {
		// disk cache could not be located,execute backend call.
		return putObjectFn(ctx, bucket, object, r, opts)
	}
	size := r.Size()
	if c.skipCache() {
		return putObjectFn(ctx, bucket, object, r, opts)
	}

	// fetch from backend if there is no space on cache drive
	if !dcache.diskAvailable(size) {
		return putObjectFn(ctx, bucket, object, r, opts)
	}
	if opts.ServerSideEncryption != nil {
		dcache.Delete(ctx, bucket, object)
		return putObjectFn(ctx, bucket, object, r, opts)
	}

	// skip cache for objects with locks
	objRetention := getObjectRetentionMeta(opts.UserDefined)
	if objRetention.Mode == Governance || objRetention.Mode == Compliance {
		dcache.Delete(ctx, bucket, object)
		return putObjectFn(ctx, bucket, object, r, opts)
	}

	// fetch from backend if cache exclude pattern or cache-control
	// directive set to exclude
	if c.isCacheExclude(bucket, object) {
		dcache.Delete(ctx, bucket, object)
		return putObjectFn(ctx, bucket, object, r, opts)
	}

	objInfo, err = putObjectFn(ctx, bucket, object, r, opts)

	if err == nil {
		go func() {
			// fill cache in the background
			bReader, bErr := c.GetObjectNInfoFn(ctx, bucket, object, nil, http.Header{}, readLock, ObjectOptions{})
			if bErr != nil {
				return
			}
			defer bReader.Close()
			oi, err := c.stat(ctx, dcache, bucket, object)
			// avoid cache overwrite if another background routine filled cache
			if err != nil || oi.ETag != bReader.ObjInfo.ETag {
				c.put(ctx, dcache, bucket, object, bReader, bReader.ObjInfo.Size, nil, ObjectOptions{UserDefined: getMetadata(bReader.ObjInfo)})
			}
		}()
	}

	return objInfo, err

}

// Returns cacheObjects for use by Server.
func newServerCacheObjects(ctx context.Context, config cache.Config) (CacheObjectLayer, error) {
	// list of disk caches for cache "drives" specified in config.json or MINIO_CACHE_DRIVES env var.
	cache, migrateSw, err := newCache(config)
	if err != nil {
		return nil, err
	}

	c := &cacheObjects{
		cache:      cache,
		exclude:    config.Exclude,
		migrating:  migrateSw,
		migMutex:   sync.Mutex{},
		nsMutex:    newNSLock(false),
		cacheStats: newCacheStats(),
		GetObjectInfoFn: func(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
			return newObjectLayerFn().GetObjectInfo(ctx, bucket, object, opts)
		},
		GetObjectNInfoFn: func(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (gr *GetObjectReader, err error) {
			return newObjectLayerFn().GetObjectNInfo(ctx, bucket, object, rs, h, lockType, opts)
		},
		DeleteObjectFn: func(ctx context.Context, bucket, object string) error {
			return newObjectLayerFn().DeleteObject(ctx, bucket, object)
		},
		DeleteObjectsFn: func(ctx context.Context, bucket string, objects []string) ([]error, error) {
			errs := make([]error, len(objects))
			for idx, object := range objects {
				errs[idx] = newObjectLayerFn().DeleteObject(ctx, bucket, object)
			}
			return errs, nil
		},
		PutObjectFn: func(ctx context.Context, bucket, object string, data *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error) {
			return newObjectLayerFn().PutObject(ctx, bucket, object, data, opts)
		},
	}
	c.NewNSLockFn = func(ctx context.Context, bucket, object string) RWLocker {
		return c.nsMutex.NewNSLock(ctx, nil, bucket, object)
	}

	if migrateSw {
		go c.migrateCacheFromV1toV2(ctx)
	}
	return c, nil
}
