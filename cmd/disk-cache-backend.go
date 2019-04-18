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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"sync"
	"time"

	"github.com/djherbis/atime"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/disk"
)

const (
	// cache.json object metadata for cached objects.
	cacheMetaJSONFile = "cache.json"
	cacheDataFile     = "data"

	cacheEnvDelimiter = ";"
)

// Represents the cache metadata struct
type cacheMeta struct {
	// Metadata map for current object.
	Meta map[string]string `json:"meta,omitempty"`
}

func (m *cacheMeta) ToObjectInfo(bucket, object string, fi os.FileInfo) (o ObjectInfo) {
	if len(m.Meta) == 0 {
		m.Meta = make(map[string]string)
	}

	o = ObjectInfo{
		Bucket: bucket,
		Name:   object,
	}

	// We set file info only if its valid.
	o.ModTime = timeSentinel
	if fi != nil {
		o.ModTime = fi.ModTime()
		o.Size = fi.Size()
		if fi.IsDir() {
			// Directory is always 0 bytes in S3 API, treat it as such.
			o.Size = 0
			o.IsDir = fi.IsDir()
		}
	}

	o.ETag = extractETag(m.Meta)
	o.ContentType = m.Meta["content-type"]
	o.ContentEncoding = m.Meta["content-encoding"]
	if storageClass, ok := m.Meta[amzStorageClass]; ok {
		o.StorageClass = storageClass
	} else {
		o.StorageClass = globalMinioDefaultStorageClass
	}
	var (
		t time.Time
		e error
	)
	if exp, ok := m.Meta["expires"]; ok {
		if t, e = time.Parse(http.TimeFormat, exp); e == nil {
			o.Expires = t.UTC()
		}
	}
	// etag/md5Sum has already been extracted. We need to
	// remove to avoid it from appearing as part of
	o.UserDefined = cleanMetadata(m.Meta)
	return o
}

// disk cache
type diskCache struct {
	dir             string // caching directory
	maxDiskUsagePct int    // max usage in %
	expiry          int    // cache expiry in days
	// to manage cache operations
	nsMutex *nsLockMap
	// mark false if drive is offline
	online bool
	// mutex to protect updates to online variable
	onlineMutex *sync.RWMutex
	// purge() listens on this channel to start the cache-purge process
	purgeChan chan struct{}
}

// Inits the disk cache dir if it is not init'ed already.
func newdiskCache(dir string, expiry int, maxDiskUsagePct int) (*diskCache, error) {
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, fmt.Errorf("Unable to initialize '%s' dir, %s", dir, err)
	}

	if expiry == 0 {
		expiry = globalCacheExpiry
	}

	cache := diskCache{
		dir:             dir,
		expiry:          expiry,
		maxDiskUsagePct: maxDiskUsagePct,
		purgeChan:       make(chan struct{}),
		online:          true,
		onlineMutex:     &sync.RWMutex{},
		nsMutex:         newNSLock(false),
	}
	return &cache, nil
}

// Returns if the disk usage is low.
// Disk usage is low if usage is < 80% of cacheMaxDiskUsagePct
// Ex. for a 100GB disk, if maxUsage is configured as 70% then cacheMaxDiskUsagePct is 70G
// hence disk usage is low if the disk usage is less than 56G (because 80% of 70G is 56G)
func (c *diskCache) diskUsageLow() bool {
	minUsage := c.maxDiskUsagePct * 80 / 100
	di, err := disk.GetInfo(c.dir)
	if err != nil {
		reqInfo := (&logger.ReqInfo{}).AppendTags("cachePath", c.dir)
		ctx := logger.SetReqInfo(context.Background(), reqInfo)
		logger.LogIf(ctx, err)
		return false
	}
	usedPercent := (di.Total - di.Free) * 100 / di.Total
	return int(usedPercent) < minUsage
}

// Return if the disk usage is high.
// Disk usage is high if disk used is > cacheMaxDiskUsagePct
func (c *diskCache) diskUsageHigh() bool {
	di, err := disk.GetInfo(c.dir)
	if err != nil {
		reqInfo := (&logger.ReqInfo{}).AppendTags("cachePath", c.dir)
		ctx := logger.SetReqInfo(context.Background(), reqInfo)
		logger.LogIf(ctx, err)
		return true
	}
	usedPercent := (di.Total - di.Free) * 100 / di.Total
	return int(usedPercent) > c.maxDiskUsagePct
}

// Returns if size space can be allocated without exceeding
// max disk usable for caching
func (c *diskCache) diskAvailable(size int64) bool {
	di, err := disk.GetInfo(c.dir)
	if err != nil {
		reqInfo := (&logger.ReqInfo{}).AppendTags("cachePath", c.dir)
		ctx := logger.SetReqInfo(context.Background(), reqInfo)
		logger.LogIf(ctx, err)
		return false
	}
	usedPercent := (di.Total - (di.Free - uint64(size))) * 100 / di.Total
	return int(usedPercent) < c.maxDiskUsagePct
}

// Purge cache entries that were not accessed.
func (c *diskCache) purge() {
	ctx := context.Background()
	for {
		olderThan := c.expiry
		for !c.diskUsageLow() {
			// delete unaccessed objects older than expiry duration
			expiry := UTCNow().AddDate(0, 0, -1*olderThan)
			olderThan /= 2
			if olderThan < 1 {
				break
			}
			deletedCount := 0

			objDirs, err := ioutil.ReadDir(c.dir)
			if err != nil {
				log.Fatal(err)
			}

			for _, obj := range objDirs {
				var fi os.FileInfo
				fi, err := os.Stat(pathJoin(c.dir, obj.Name()))
				if err != nil {
					continue
				}
				objInfo, err := c.statCache(ctx, pathJoin(c.dir, obj.Name()))
				if err != nil {
					continue
				}
				if !filterFromCache(objInfo.UserDefined) ||
					!isStaleCache(objInfo) ||
					atime.Get(fi).After(expiry) {
					continue
				}
				if err = os.RemoveAll(pathJoin(c.dir, obj.Name())); err != nil {
					logger.LogIf(ctx, err)
					continue
				}
				deletedCount++
			}
			if deletedCount == 0 {
				// to avoid a busy loop
				time.Sleep(time.Minute * 30)
			}
		}
		<-c.purgeChan
	}
}

// sets cache drive status
func (c *diskCache) setOnline(status bool) {
	c.onlineMutex.Lock()
	c.online = status
	c.onlineMutex.Unlock()
}

// returns true if cache drive is online
func (c *diskCache) IsOnline() bool {
	c.onlineMutex.RLock()
	defer c.onlineMutex.RUnlock()
	return c.online
}

// Stat returns ObjectInfo from disk cache
func (c *diskCache) Stat(ctx context.Context, bucket, object string) (oi ObjectInfo, err error) {
	cachePath := c.getCacheSHADir(bucket, object)
	cLock := c.nsMutex.NewNSLock(cachePath, "")
	if err := cLock.GetRLock(globalObjectTimeout); err != nil {
		return oi, err
	}
	defer cLock.RUnlock()

	oi, err = c.stat(ctx, bucket, object)
	return
}

// lockless helper to Stat
func (c *diskCache) stat(ctx context.Context, bucket, object string) (oi ObjectInfo, e error) {
	cacheObjPath := c.getCacheSHADir(bucket, object)
	oi, e = c.statCache(ctx, cacheObjPath)
	if e != nil {
		return
	}
	oi.Bucket = bucket
	oi.Name = object
	return
}

// statCache is a convenience function for purge() to get ObjectInfo for cached object
func (c *diskCache) statCache(ctx context.Context, cacheObjPath string) (oi ObjectInfo, e error) {
	// Stat the file to get file size.
	fi, err := fsStatFile(ctx, pathJoin(cacheObjPath, cacheDataFile))
	if err != nil {
		return oi, err
	}
	metaPath := pathJoin(cacheObjPath, cacheMetaJSONFile)

	f, err := os.Open(metaPath)
	if err != nil {
		return oi, err
	}
	defer f.Close()
	meta := &cacheMeta{}
	if err := jsonLoad(f, meta); err != nil {
		return oi, err
	}
	return meta.ToObjectInfo("", "", fi), nil
}

// caches object metadata to disk cache
func (c *diskCache) saveMetadata(ctx context.Context, bucket, object string, meta map[string]string) error {
	fileName := c.getCacheSHADir(bucket, object)
	metaPath := pathJoin(fileName, cacheMetaJSONFile)

	f, err := os.Create(metaPath)
	if err != nil {
		return err
	}
	defer f.Close()
	m := cacheMeta{Meta: meta}
	jsonData, err := json.Marshal(m)
	if err != nil {
		return err
	}
	f.Write(jsonData)
	return nil
}

func (c *diskCache) getCacheSHADir(bucket, object string) string {
	return path.Join(c.dir, getSHA256Hash([]byte(path.Join(bucket, object))))
}

// Caches the object to disk
func (c *diskCache) Put(ctx context.Context, bucket, object string, data io.Reader, size int64, opts ObjectOptions) error {
	if c.diskUsageHigh() {
		select {
		case c.purgeChan <- struct{}{}:
		default:
		}
		return errDiskFull
	}
	if !c.diskAvailable(size) {
		return errDiskFull
	}
	cachePath := c.getCacheSHADir(bucket, object)
	cLock := c.nsMutex.NewNSLock(cachePath, "")
	if err := cLock.GetLock(globalObjectTimeout); err != nil {
		return err
	}
	defer cLock.Unlock()
	if err := os.MkdirAll(cachePath, 0777); err != nil {
		return err
	}
	bufSize := int64(readSizeV1)
	if size > 0 && bufSize > size {
		bufSize = size
	}
	filePath := path.Join(cachePath, cacheDataFile)
	buf := make([]byte, int(bufSize))
	_, err := fsCreateFile(ctx, filePath, data, buf, size)
	if IsErr(err, baseErrs...) {
		c.setOnline(false)
	}
	if err != nil {
		return err
	}
	return c.saveMetadata(ctx, bucket, object, opts.UserDefined)
}

// Get returns ObjectInfo and reader for object from disk cache
func (c *diskCache) Get(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, opts ObjectOptions) (gr *GetObjectReader, err error) {
	cachePath := c.getCacheSHADir(bucket, object)
	fileName := path.Join(cachePath, cacheDataFile)

	cLock := c.nsMutex.NewNSLock(cachePath, "")
	if err := cLock.GetRLock(globalObjectTimeout); err != nil {
		return nil, err
	}

	defer cLock.RUnlock()
	var objInfo ObjectInfo

	if objInfo, err = c.stat(ctx, bucket, object); err != nil {
		return nil, toObjectErr(err, bucket, object)
	}
	var nsUnlocker = func() {}
	// For a directory, we need to send an reader that returns no bytes.
	if hasSuffix(object, slashSeparator) {
		// The lock taken above is released when
		// objReader.Close() is called by the caller.
		return NewGetObjectReaderFromReader(bytes.NewBuffer(nil), objInfo, opts.CheckCopyPrecondFn, nsUnlocker)
	}
	objReaderFn, off, length, rErr := NewGetObjectReader(rs, objInfo, opts.CheckCopyPrecondFn, nsUnlocker)
	if rErr != nil {
		return nil, rErr
	}

	// Read the object, doesn't exist returns an s3 compatible error.
	readCloser, size, err := fsOpenFile(ctx, fileName, off)
	if err != nil {
		return nil, toObjectErr(err, bucket, object)
	}
	reader := io.LimitReader(readCloser, length)
	closeFn := func() {
		readCloser.Close()
	}

	// Check if range is valid
	if off > size || off+length > size {
		err = InvalidRange{off, length, size}
		logger.LogIf(ctx, err)
		closeFn()
		nsUnlocker()
		return nil, err
	}

	return objReaderFn(reader, h, opts.CheckCopyPrecondFn, closeFn)
}

// Deletes the cached object
func (c *diskCache) Delete(ctx context.Context, bucket, object string) (err error) {
	cachePath := c.getCacheSHADir(bucket, object)
	cLock := c.nsMutex.NewNSLock(cachePath, "")
	if err := cLock.GetLock(globalObjectTimeout); err != nil {
		return err
	}
	defer cLock.Unlock()
	return os.RemoveAll(cachePath)

}

// convenience function to check if object is cached on this diskCache
func (c *diskCache) Exists(ctx context.Context, bucket, object string) bool {
	fileName := getSHA256Hash([]byte(path.Join(bucket, object)))
	if _, err := os.Stat(path.Join(c.dir, fileName)); err != nil {
		return false
	}
	return true
}
