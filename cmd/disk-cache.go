package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/djherbis/atime"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/wildcard"
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
	// Storage operations.
	StorageInfo(ctx context.Context) CacheStorageInfo
}

// Abstracts disk caching - used by the S3 layer
type cacheObjects struct {
	// slice of cache drives
	cache []*diskCache
	// file path patterns to exclude from cache
	exclude []string
	// Object functions pointing to the corresponding functions of backend implementation.
	GetObjectNInfoFn func(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (gr *GetObjectReader, err error)
	GetObjectInfoFn  func(ctx context.Context, bucket, object string, opts ObjectOptions) (objInfo ObjectInfo, err error)
	DeleteObjectFn   func(ctx context.Context, bucket, object string) error
	DeleteObjectsFn  func(ctx context.Context, bucket string, objects []string) ([]error, error)
}

// DeleteObject clears cache entry if backend delete operation succeeds
func (c cacheObjects) DeleteObject(ctx context.Context, bucket, object string) (err error) {
	if err = c.DeleteObjectFn(ctx, bucket, object); err != nil {
		return
	}
	if c.isCacheExclude(bucket, object) {
		return
	}
	if dcache, cerr := c.getCachedLoc(ctx, bucket, object); cerr == nil {
		dcache.Delete(ctx, bucket, object)
	}
	return
}

// DeleteObjects batch deletes objects in slice, and clears any cached entries
func (c cacheObjects) DeleteObjects(ctx context.Context, bucket string, objects []string) ([]error, error) {
	errs := make([]error, len(objects))
	for idx, object := range objects {
		errs[idx] = c.DeleteObject(ctx, bucket, object)
	}
	return errs, nil
}

// construct a metadata k-v map
func (c cacheObjects) getMetadata(objInfo ObjectInfo) map[string]string {
	metadata := make(map[string]string)
	metadata["etag"] = objInfo.ETag
	metadata["content-type"] = objInfo.ContentType
	metadata["content-encoding"] = objInfo.ContentEncoding

	for key, val := range objInfo.UserDefined {
		metadata[key] = val
	}
	return metadata
}

func (c cacheObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (gr *GetObjectReader, err error) {
	if c.isCacheExclude(bucket, object) {
		return c.GetObjectNInfoFn(ctx, bucket, object, rs, h, lockType, opts)
	}

	// fetch diskCache if object is currently cached or nearest available cache drive
	dcache, err := c.getCachedLoc(ctx, bucket, object)
	if err != nil {
		return c.GetObjectNInfoFn(ctx, bucket, object, rs, h, lockType, opts)
	}

	cacheReader, cacheErr := dcache.Get(ctx, bucket, object, rs, h, opts)
	objInfo, err := c.GetObjectInfoFn(ctx, bucket, object, opts)

	if backendDownError(err) && cacheErr == nil {
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
		return nil, err
	}

	if !objInfo.IsCacheable() || filterFromCache(objInfo.UserDefined) {
		return c.GetObjectNInfoFn(ctx, bucket, object, rs, h, lockType, opts)
	}

	if cacheErr == nil {
		if cacheReader.ObjInfo.ETag == objInfo.ETag && !isStaleCache(objInfo) {
			// Object is not stale, so serve from cache
			return cacheReader, nil
		}
		cacheReader.Close()
		// Object is stale, so delete from cache
		dcache.Delete(ctx, bucket, object)
	}

	// Since we got here, we are serving the request from backend,
	// and also adding the object to the cache.

	if !dcache.diskAvailable(objInfo.Size) {
		return c.GetObjectNInfoFn(ctx, bucket, object, rs, h, lockType, opts)
	}

	if rs != nil {
		go func() {
			// fill cache in the background for range GET requests
			bReader, bErr := c.GetObjectNInfoFn(ctx, bucket, object, nil, h, lockType, opts)
			if bErr != nil {
				return
			}
			defer bReader.Close()
			oi, err := dcache.Stat(ctx, bucket, object)
			// avoid cache overwrite if another background routine filled cache
			if err != nil || oi.ETag != bReader.ObjInfo.ETag {
				dcache.Put(ctx, bucket, object, bReader, bReader.ObjInfo.Size, ObjectOptions{UserDefined: c.getMetadata(bReader.ObjInfo)})
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
	hashReader, herr := hash.NewReader(pipeReader, bkReader.ObjInfo.Size, "", "", bkReader.ObjInfo.Size, globalCLIContext.StrictS3Compat)
	if herr != nil {
		bkReader.Close()
		return nil, herr
	}

	go func() {
		putErr := dcache.Put(ctx, bucket, object, hashReader, hashReader.Size(), ObjectOptions{UserDefined: c.getMetadata(bkReader.ObjInfo)})
		// close the write end of the pipe, so the error gets
		// propagated to getObjReader
		pipeWriter.CloseWithError(putErr)
	}()
	cleanupBackend := func() { bkReader.Close() }
	cleanupPipe := func() { pipeReader.Close() }
	return NewGetObjectReaderFromReader(teeReader, bkReader.ObjInfo, opts.CheckCopyPrecondFn, cleanupBackend, cleanupPipe)
}

// Returns ObjectInfo from cache if available.
func (c cacheObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	getObjectInfoFn := c.GetObjectInfoFn

	if c.isCacheExclude(bucket, object) {
		return getObjectInfoFn(ctx, bucket, object, opts)
	}

	// fetch diskCache if object is currently cached or nearest available cache drive
	dcache, err := c.getCachedLoc(ctx, bucket, object)
	if err != nil {
		return getObjectInfoFn(ctx, bucket, object, opts)
	}
	objInfo, err := getObjectInfoFn(ctx, bucket, object, opts)
	if err != nil {
		if _, ok := err.(ObjectNotFound); ok {
			// Delete the cached entry if backend object was deleted.
			dcache.Delete(ctx, bucket, object)
			return ObjectInfo{}, err
		}
		if !backendDownError(err) {
			return ObjectInfo{}, err
		}
		// when backend is down, serve from cache.
		cachedObjInfo, cerr := dcache.Stat(ctx, bucket, object)
		if cerr == nil {
			return cachedObjInfo, nil
		}
		return ObjectInfo{}, BackendDown{}
	}
	// when backend is up, do a sanity check on cached object
	cachedObjInfo, err := dcache.Stat(ctx, bucket, object)
	if err != nil {
		return objInfo, nil
	}
	if cachedObjInfo.ETag != objInfo.ETag {
		// Delete the cached entry if the backend object was replaced.
		dcache.Delete(ctx, bucket, object)
	}
	return objInfo, nil
}

// StorageInfo - returns underlying storage statistics.
func (c cacheObjects) StorageInfo(ctx context.Context) (cInfo CacheStorageInfo) {
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

// Returns true if object should be excluded from cache
func (c cacheObjects) isCacheExclude(bucket, object string) bool {
	for _, pattern := range c.exclude {
		matchStr := fmt.Sprintf("%s/%s", bucket, object)
		if ok := wildcard.MatchSimple(pattern, matchStr); ok {
			return true
		}
	}
	return false
}

// get cache disk where object is currently cached for a GET operation. If object does not exist at that location,
// treat the list of cache drives as a circular buffer and walk through them starting at hash index
// until an online drive is found.If object is not found, fall back to the first online cache drive
// closest to the hash index, so that object can be re-cached.
func (c cacheObjects) getCachedLoc(ctx context.Context, bucket, object string) (*diskCache, error) {
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
func (c cacheObjects) hashIndex(bucket, object string) int {
	return crcHashMod(pathJoin(bucket, object), len(c.cache))
}

// newCache initializes the cacheFSObjects for the "drives" specified in config.json
// or the global env overrides.
func newCache(config CacheConfig) ([]*diskCache, error) {
	var caches []*diskCache
	ctx := logger.SetReqInfo(context.Background(), &logger.ReqInfo{})
	formats, err := loadAndValidateCacheFormat(ctx, config.Drives)
	if err != nil {
		return nil, err
	}
	for i, dir := range config.Drives {
		// skip diskCache creation for cache drives missing a format.json
		if formats[i] == nil {
			caches = append(caches, nil)
			continue
		}
		if err := checkAtimeSupport(dir); err != nil {
			return nil, errors.New("Atime support required for disk caching")
		}
		cache, err := newdiskCache(dir, config.Expiry, config.MaxUse)
		if err != nil {
			return nil, err
		}
		// Start the purging go-routine for entries that have expired
		go cache.purge()

		caches = append(caches, cache)
	}
	return caches, nil
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
	if _, err = io.Copy(ioutil.Discard, file); err != io.EOF {
		return
	}

	finfo2, err := os.Stat(file.Name())

	if atime.Get(finfo2).Equal(atime.Get(finfo1)) {
		return errors.New("Atime not supported")
	}
	return
}

// Returns cacheObjects for use by Server.
func newServerCacheObjects(config CacheConfig) (CacheObjectLayer, error) {
	// list of disk caches for cache "drives" specified in config.json or MINIO_CACHE_DRIVES env var.
	cache, err := newCache(config)
	if err != nil {
		return nil, err
	}
	return &cacheObjects{
		cache:   cache,
		exclude: config.Exclude,
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
	}, nil
}
