// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/djherbis/atime"
	"github.com/minio/minio/internal/config/cache"
	"github.com/minio/minio/internal/crypto"
	"github.com/minio/minio/internal/disk"
	"github.com/minio/minio/internal/fips"
	"github.com/minio/minio/internal/hash"
	xhttp "github.com/minio/minio/internal/http"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/kms"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/sio"
)

const (
	// cache.json object metadata for cached objects.
	cacheMetaJSONFile   = "cache.json"
	cacheDataFile       = "part.1"
	cacheDataFilePrefix = "part"

	cacheMetaVersion = "1.0.0"
	cacheExpiryDays  = 90 * time.Hour * 24 // defaults to 90 days
	// SSECacheEncrypted is the metadata key indicating that the object
	// is a cache entry encrypted with cache KMS master key in globalCacheKMS.
	SSECacheEncrypted               = "X-Minio-Internal-Encrypted-Cache"
	cacheMultipartDir               = "multipart"
	cacheStaleUploadCleanupInterval = time.Hour * 24
	cacheStaleUploadExpiry          = time.Hour * 24
)

// CacheChecksumInfoV1 - carries checksums of individual blocks on disk.
type CacheChecksumInfoV1 struct {
	Algorithm string `json:"algorithm"`
	Blocksize int64  `json:"blocksize"`
}

// Represents the cache metadata struct
type cacheMeta struct {
	Version string   `json:"version"`
	Stat    StatInfo `json:"stat"` // Stat of the current object `cache.json`.

	// checksums of blocks on disk.
	Checksum CacheChecksumInfoV1 `json:"checksum,omitempty"`
	// Metadata map for current object.
	Meta map[string]string `json:"meta,omitempty"`
	// Ranges maps cached range to associated filename.
	Ranges map[string]string `json:"ranges,omitempty"`
	// Hits is a counter on the number of times this object has been accessed so far.
	Hits   int    `json:"hits,omitempty"`
	Bucket string `json:"bucket,omitempty"`
	Object string `json:"object,omitempty"`
	// for multipart upload
	PartNumbers     []int    `json:"partNums,omitempty"`   // Part Numbers
	PartETags       []string `json:"partETags,omitempty"`  // Part ETags
	PartSizes       []int64  `json:"partSizes,omitempty"`  // Part Sizes
	PartActualSizes []int64  `json:"partASizes,omitempty"` // Part ActualSizes (compression)
}

// RangeInfo has the range, file and range length information for a cached range.
type RangeInfo struct {
	Range string
	File  string
	Size  int64
}

// Empty returns true if this is an empty struct
func (r *RangeInfo) Empty() bool {
	return r.Range == "" && r.File == "" && r.Size == 0
}

func (m *cacheMeta) ToObjectInfo(bucket, object string) (o ObjectInfo) {
	if len(m.Meta) == 0 {
		m.Meta = make(map[string]string)
		m.Stat.ModTime = timeSentinel
	}

	o = ObjectInfo{
		Bucket:            bucket,
		Name:              object,
		CacheStatus:       CacheHit,
		CacheLookupStatus: CacheHit,
	}
	meta := cloneMSS(m.Meta)
	// We set file info only if its valid.
	o.Size = m.Stat.Size
	o.ETag = extractETag(meta)
	o.ContentType = meta["content-type"]
	o.ContentEncoding = meta["content-encoding"]
	if storageClass, ok := meta[xhttp.AmzStorageClass]; ok {
		o.StorageClass = storageClass
	} else {
		o.StorageClass = globalMinioDefaultStorageClass
	}
	var (
		t time.Time
		e error
	)
	if exp, ok := meta["expires"]; ok {
		if t, e = time.Parse(http.TimeFormat, exp); e == nil {
			o.Expires = t.UTC()
		}
	}
	if mtime, ok := meta["last-modified"]; ok {
		if t, e = time.Parse(http.TimeFormat, mtime); e == nil {
			o.ModTime = t.UTC()
		}
	}
	o.Parts = make([]ObjectPartInfo, len(m.PartNumbers))
	for i := range m.PartNumbers {
		o.Parts[i].Number = m.PartNumbers[i]
		o.Parts[i].Size = m.PartSizes[i]
		o.Parts[i].ETag = m.PartETags[i]
		o.Parts[i].ActualSize = m.PartActualSizes[i]
	}
	// etag/md5Sum has already been extracted. We need to
	// remove to avoid it from appearing as part of user-defined metadata
	o.UserDefined = cleanMetadata(meta)
	return o
}

// represents disk cache struct
type diskCache struct {
	// is set to 0 if drive is offline
	online       uint32 // ref: https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	purgeRunning int32

	triggerGC          chan struct{}
	dir                string         // caching directory
	stats              CacheDiskStats // disk cache stats for prometheus
	quotaPct           int            // max usage in %
	pool               sync.Pool
	after              int // minimum accesses before an object is cached.
	lowWatermark       int
	highWatermark      int
	enableRange        bool
	commitWriteback    bool
	commitWritethrough bool

	retryWritebackCh chan ObjectInfo
	// nsMutex namespace lock
	nsMutex *nsLockMap
	// Object functions pointing to the corresponding functions of backend implementation.
	NewNSLockFn func(cachePath string) RWLocker
}

// Inits the disk cache dir if it is not initialized already.
func newDiskCache(ctx context.Context, dir string, config cache.Config) (*diskCache, error) {
	quotaPct := config.MaxUse
	if quotaPct == 0 {
		quotaPct = config.Quota
	}

	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, fmt.Errorf("Unable to initialize '%s' dir, %w", dir, err)
	}
	cache := diskCache{
		dir:                dir,
		triggerGC:          make(chan struct{}, 1),
		stats:              CacheDiskStats{Dir: dir},
		quotaPct:           quotaPct,
		after:              config.After,
		lowWatermark:       config.WatermarkLow,
		highWatermark:      config.WatermarkHigh,
		enableRange:        config.Range,
		commitWriteback:    config.CacheCommitMode == CommitWriteBack,
		commitWritethrough: config.CacheCommitMode == CommitWriteThrough,

		retryWritebackCh: make(chan ObjectInfo, 10000),
		online:           1,
		pool: sync.Pool{
			New: func() interface{} {
				b := disk.AlignedBlock(int(cacheBlkSize))
				return &b
			},
		},
		nsMutex: newNSLock(false),
	}
	go cache.purgeWait(ctx)
	go cache.cleanupStaleUploads(ctx)
	if cache.commitWriteback {
		go cache.scanCacheWritebackFailures(ctx)
	}
	cache.diskSpaceAvailable(0) // update if cache usage is already high.
	cache.NewNSLockFn = func(cachePath string) RWLocker {
		return cache.nsMutex.NewNSLock(nil, cachePath, "")
	}
	return &cache, nil
}

// diskUsageLow() returns true if disk usage falls below the low watermark w.r.t configured cache quota.
// Ex. for a 100GB disk, if quota is configured as 70%  and watermark_low = 80% and
// watermark_high = 90% then garbage collection starts when 63% of disk is used and
// stops when disk usage drops to 56%
func (c *diskCache) diskUsageLow() bool {
	gcStopPct := c.quotaPct * c.lowWatermark / 100
	di, err := disk.GetInfo(c.dir)
	if err != nil {
		reqInfo := (&logger.ReqInfo{}).AppendTags("cachePath", c.dir)
		ctx := logger.SetReqInfo(GlobalContext, reqInfo)
		logger.LogIf(ctx, err)
		return false
	}
	usedPercent := float64(di.Used) * 100 / float64(di.Total)
	low := int(usedPercent) < gcStopPct
	atomic.StoreUint64(&c.stats.UsagePercent, uint64(usedPercent))
	if low {
		atomic.StoreInt32(&c.stats.UsageState, 0)
	}
	return low
}

// Returns if the disk usage reaches  or exceeds configured cache quota when size is added.
// If current usage without size exceeds high watermark a GC is automatically queued.
func (c *diskCache) diskSpaceAvailable(size int64) bool {
	gcTriggerPct := c.quotaPct * c.highWatermark / 100
	di, err := disk.GetInfo(c.dir)
	if err != nil {
		reqInfo := (&logger.ReqInfo{}).AppendTags("cachePath", c.dir)
		ctx := logger.SetReqInfo(GlobalContext, reqInfo)
		logger.LogIf(ctx, err)
		return false
	}
	if di.Total == 0 {
		logger.Info("diskCache: Received 0 total disk size")
		return false
	}
	usedPercent := float64(di.Used) * 100 / float64(di.Total)
	if usedPercent >= float64(gcTriggerPct) {
		atomic.StoreInt32(&c.stats.UsageState, 1)
		c.queueGC()
	}
	atomic.StoreUint64(&c.stats.UsagePercent, uint64(usedPercent))

	// Recalculate percentage with provided size added.
	usedPercent = float64(di.Used+uint64(size)) * 100 / float64(di.Total)

	return usedPercent < float64(c.quotaPct)
}

// queueGC will queue a GC.
// Calling this function is always non-blocking.
func (c *diskCache) queueGC() {
	select {
	case c.triggerGC <- struct{}{}:
	default:
	}
}

// toClear returns how many bytes should be cleared to reach the low watermark quota.
// returns 0 if below quota.
func (c *diskCache) toClear() uint64 {
	di, err := disk.GetInfo(c.dir)
	if err != nil {
		reqInfo := (&logger.ReqInfo{}).AppendTags("cachePath", c.dir)
		ctx := logger.SetReqInfo(GlobalContext, reqInfo)
		logger.LogIf(ctx, err)
		return 0
	}
	return bytesToClear(int64(di.Total), int64(di.Free), uint64(c.quotaPct), uint64(c.lowWatermark), uint64(c.highWatermark))
}

func (c *diskCache) purgeWait(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
		case <-c.triggerGC: // wait here until someone triggers.
			c.purge(ctx)
		}
	}
}

// Purge cache entries that were not accessed.
func (c *diskCache) purge(ctx context.Context) {
	if atomic.LoadInt32(&c.purgeRunning) == 1 || c.diskUsageLow() {
		return
	}

	toFree := c.toClear()
	if toFree == 0 {
		return
	}

	atomic.StoreInt32(&c.purgeRunning, 1) // do not run concurrent purge()
	defer atomic.StoreInt32(&c.purgeRunning, 0)

	// expiry for cleaning up old cache.json files that
	// need to be cleaned up.
	expiry := UTCNow().Add(-cacheExpiryDays)
	// defaulting max hits count to 100
	// ignore error we know what value we are passing.
	scorer, _ := newFileScorer(toFree, time.Now().Unix(), 100)

	// this function returns FileInfo for cached range files.
	fiStatRangesFn := func(ranges map[string]string, pathPrefix string) map[string]os.FileInfo {
		fm := make(map[string]os.FileInfo)
		for _, rngFile := range ranges {
			fname := pathJoin(pathPrefix, rngFile)
			if fi, err := os.Stat(fname); err == nil {
				fm[fname] = fi
			}
		}
		return fm
	}

	// this function returns most recent Atime among cached part files.
	lastAtimeFn := func(partNums []int, pathPrefix string) time.Time {
		lastATime := timeSentinel
		for _, pnum := range partNums {
			fname := pathJoin(pathPrefix, fmt.Sprintf("%s.%d", cacheDataFilePrefix, pnum))
			if fi, err := os.Stat(fname); err == nil {
				if atime.Get(fi).After(lastATime) {
					lastATime = atime.Get(fi)
				}
			}
		}
		if len(partNums) == 0 {
			fname := pathJoin(pathPrefix, cacheDataFile)
			if fi, err := os.Stat(fname); err == nil {
				lastATime = atime.Get(fi)
			}
		}
		return lastATime
	}

	filterFn := func(name string, typ os.FileMode) error {
		if name == minioMetaBucket {
			// Proceed to next file.
			return nil
		}

		cacheDir := pathJoin(c.dir, name)
		meta, _, numHits, err := c.statCachedMeta(ctx, cacheDir)
		if err != nil {
			// delete any partially filled cache entry left behind.
			removeAll(cacheDir)
			// Proceed to next file.
			return nil
		}
		// get last access time of cache part files
		lastAtime := lastAtimeFn(meta.PartNumbers, pathJoin(c.dir, name))
		// stat all cached file ranges.
		cachedRngFiles := fiStatRangesFn(meta.Ranges, pathJoin(c.dir, name))
		objInfo := meta.ToObjectInfo("", "")
		// prevent gc from clearing un-synced commits. This metadata is present when
		// cache writeback commit setting is enabled.
		status, ok := objInfo.UserDefined[writeBackStatusHeader]
		if ok && status != CommitComplete.String() {
			return nil
		}
		cc := cacheControlOpts(objInfo)
		switch {
		case cc != nil:
			if cc.isStale(objInfo.ModTime) {
				if err = removeAll(cacheDir); err != nil {
					logger.LogIf(ctx, err)
				}
				scorer.adjustSaveBytes(-objInfo.Size)
				// break early if sufficient disk space reclaimed.
				if c.diskUsageLow() {
					// if we found disk usage is already low, we return nil filtering is complete.
					return errDoneForNow
				}
			}
		case lastAtime != timeSentinel:
			// cached multipart or single part
			objInfo.AccTime = lastAtime
			objInfo.Name = pathJoin(c.dir, name, cacheDataFile)
			scorer.addFileWithObjInfo(objInfo, numHits)
		}

		for fname, fi := range cachedRngFiles {
			if cc != nil {
				if cc.isStale(objInfo.ModTime) {
					if err = removeAll(fname); err != nil {
						logger.LogIf(ctx, err)
					}
					scorer.adjustSaveBytes(-fi.Size())

					// break early if sufficient disk space reclaimed.
					if c.diskUsageLow() {
						// if we found disk usage is already low, we return nil filtering is complete.
						return errDoneForNow
					}
				}
				continue
			}
			scorer.addFile(fname, atime.Get(fi), fi.Size(), numHits)
		}
		// clean up stale cache.json files for objects that never got cached but access count was maintained in cache.json
		fi, err := os.Stat(pathJoin(cacheDir, cacheMetaJSONFile))
		if err != nil || (fi.ModTime().Before(expiry) && len(cachedRngFiles) == 0) {
			removeAll(cacheDir)
			scorer.adjustSaveBytes(-fi.Size())
			// Proceed to next file.
			return nil
		}

		// if we found disk usage is already low, we return nil filtering is complete.
		if c.diskUsageLow() {
			return errDoneForNow
		}

		// Proceed to next file.
		return nil
	}

	if err := readDirFn(c.dir, filterFn); err != nil {
		logger.LogIf(ctx, err)
		return
	}

	scorer.purgeFunc(func(qfile queuedFile) {
		fileName := qfile.name
		removeAll(fileName)
		slashIdx := strings.LastIndex(fileName, SlashSeparator)
		if slashIdx >= 0 {
			fileNamePrefix := fileName[0:slashIdx]
			fname := fileName[slashIdx+1:]
			if fname == cacheDataFile {
				removeAll(fileNamePrefix)
			}
		}
	})

	scorer.reset()
}

// sets cache drive status
func (c *diskCache) setOffline() {
	atomic.StoreUint32(&c.online, 0)
}

// returns true if cache drive is online
func (c *diskCache) IsOnline() bool {
	return atomic.LoadUint32(&c.online) != 0
}

// Stat returns ObjectInfo from disk cache
func (c *diskCache) Stat(ctx context.Context, bucket, object string) (oi ObjectInfo, numHits int, err error) {
	var partial bool
	var meta *cacheMeta

	cacheObjPath := getCacheSHADir(c.dir, bucket, object)
	// Stat the file to get file size.
	meta, partial, numHits, err = c.statCachedMeta(ctx, cacheObjPath)
	if err != nil {
		return
	}
	if partial {
		return oi, numHits, errFileNotFound
	}
	oi = meta.ToObjectInfo("", "")
	oi.Bucket = bucket
	oi.Name = object

	if err = decryptCacheObjectETag(&oi); err != nil {
		return
	}
	return
}

// statCachedMeta returns metadata from cache - including ranges cached, partial to indicate
// if partial object is cached.
func (c *diskCache) statCachedMeta(ctx context.Context, cacheObjPath string) (meta *cacheMeta, partial bool, numHits int, err error) {
	cLock := c.NewNSLockFn(cacheObjPath)
	lkctx, err := cLock.GetRLock(ctx, globalOperationTimeout)
	if err != nil {
		return
	}
	ctx = lkctx.Context()
	defer cLock.RUnlock(lkctx.Cancel)
	return c.statCache(ctx, cacheObjPath)
}

// statRange returns ObjectInfo and RangeInfo from disk cache
func (c *diskCache) statRange(ctx context.Context, bucket, object string, rs *HTTPRangeSpec) (oi ObjectInfo, rngInfo RangeInfo, numHits int, err error) {
	// Stat the file to get file size.
	cacheObjPath := getCacheSHADir(c.dir, bucket, object)
	var meta *cacheMeta
	var partial bool

	meta, partial, numHits, err = c.statCachedMeta(ctx, cacheObjPath)
	if err != nil {
		return
	}

	oi = meta.ToObjectInfo("", "")
	oi.Bucket = bucket
	oi.Name = object
	if !partial {
		err = decryptCacheObjectETag(&oi)
		return
	}

	actualSize := uint64(meta.Stat.Size)
	var length int64
	_, length, err = rs.GetOffsetLength(int64(actualSize))
	if err != nil {
		return
	}

	actualRngSize := uint64(length)
	if globalCacheKMS != nil {
		actualRngSize, _ = sio.EncryptedSize(uint64(length))
	}

	rng := rs.String(int64(actualSize))
	rngFile, ok := meta.Ranges[rng]
	if !ok {
		return oi, rngInfo, numHits, ObjectNotFound{Bucket: bucket, Object: object}
	}
	if _, err = os.Stat(pathJoin(cacheObjPath, rngFile)); err != nil {
		return oi, rngInfo, numHits, ObjectNotFound{Bucket: bucket, Object: object}
	}
	rngInfo = RangeInfo{Range: rng, File: rngFile, Size: int64(actualRngSize)}

	err = decryptCacheObjectETag(&oi)
	return
}

// statCache is a convenience function for purge() to get ObjectInfo for cached object
func (c *diskCache) statCache(ctx context.Context, cacheObjPath string) (meta *cacheMeta, partial bool, numHits int, err error) {
	// Stat the file to get file size.
	metaPath := pathJoin(cacheObjPath, cacheMetaJSONFile)
	f, err := os.Open(metaPath)
	if err != nil {
		return meta, partial, 0, err
	}
	defer f.Close()
	meta = &cacheMeta{Version: cacheMetaVersion}
	if err := jsonLoad(f, meta); err != nil {
		return meta, partial, 0, err
	}
	// get metadata of part.1 if full file has been cached.
	partial = true
	if _, err := os.Stat(pathJoin(cacheObjPath, cacheDataFile)); err == nil {
		partial = false
	}
	return meta, partial, meta.Hits, nil
}

// saves object metadata to disk cache
// incHitsOnly is true if metadata update is incrementing only the hit counter
func (c *diskCache) SaveMetadata(ctx context.Context, bucket, object string, meta map[string]string, actualSize int64, rs *HTTPRangeSpec, rsFileName string, incHitsOnly bool) error {
	cachedPath := getCacheSHADir(c.dir, bucket, object)
	cLock := c.NewNSLockFn(cachedPath)
	lkctx, err := cLock.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return err
	}
	ctx = lkctx.Context()
	defer cLock.Unlock(lkctx.Cancel)
	return c.saveMetadata(ctx, bucket, object, meta, actualSize, rs, rsFileName, incHitsOnly)
}

// saves object metadata to disk cache
// incHitsOnly is true if metadata update is incrementing only the hit counter
func (c *diskCache) saveMetadata(ctx context.Context, bucket, object string, meta map[string]string, actualSize int64, rs *HTTPRangeSpec, rsFileName string, incHitsOnly bool) error {
	cachedPath := getCacheSHADir(c.dir, bucket, object)
	metaPath := pathJoin(cachedPath, cacheMetaJSONFile)
	// Create cache directory if needed
	if err := os.MkdirAll(cachedPath, 0777); err != nil {
		return err
	}
	f, err := os.OpenFile(metaPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer f.Close()

	m := &cacheMeta{
		Version: cacheMetaVersion,
		Bucket:  bucket,
		Object:  object,
	}
	if err := jsonLoad(f, m); err != nil && err != io.EOF {
		return err
	}
	// increment hits
	if rs != nil {
		// rsFileName gets set by putRange. Check for blank values here
		// coming from other code paths that set rs only (eg initial creation or hit increment).
		if rsFileName != "" {
			if m.Ranges == nil {
				m.Ranges = make(map[string]string)
			}
			m.Ranges[rs.String(actualSize)] = rsFileName
		}
	}
	if rs == nil && !incHitsOnly {
		// this is necessary cleanup of range files if entire object is cached.
		if _, err := os.Stat(pathJoin(cachedPath, cacheDataFile)); err == nil {
			for _, f := range m.Ranges {
				removeAll(pathJoin(cachedPath, f))
			}
			m.Ranges = nil
		}
	}
	m.Stat.Size = actualSize
	if !incHitsOnly {
		// reset meta
		m.Meta = meta
	} else {
		if m.Meta == nil {
			m.Meta = make(map[string]string)
		}
		// save etag in m.Meta if missing
		if _, ok := m.Meta["etag"]; !ok {
			if etag, ok := meta["etag"]; ok {
				m.Meta["etag"] = etag
			}
		}
	}
	m.Hits++

	m.Checksum = CacheChecksumInfoV1{Algorithm: HighwayHash256S.String(), Blocksize: cacheBlkSize}
	return jsonSave(f, m)
}

// updates the ETag and ModTime on cache with ETag from backend
func (c *diskCache) updateMetadata(ctx context.Context, bucket, object, etag string, modTime time.Time, size int64) error {
	cachedPath := getCacheSHADir(c.dir, bucket, object)
	metaPath := pathJoin(cachedPath, cacheMetaJSONFile)
	// Create cache directory if needed
	if err := os.MkdirAll(cachedPath, 0777); err != nil {
		return err
	}
	f, err := os.OpenFile(metaPath, os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer f.Close()

	m := &cacheMeta{
		Version: cacheMetaVersion,
		Bucket:  bucket,
		Object:  object,
	}
	if err := jsonLoad(f, m); err != nil && err != io.EOF {
		return err
	}
	if m.Meta == nil {
		m.Meta = make(map[string]string)
	}
	var key []byte
	var objectEncryptionKey crypto.ObjectKey

	if globalCacheKMS != nil {
		// Calculating object encryption key
		key, err = decryptObjectInfo(key, bucket, object, m.Meta)
		if err != nil {
			return err
		}
		copy(objectEncryptionKey[:], key)
		m.Meta["etag"] = hex.EncodeToString(objectEncryptionKey.SealETag([]byte(etag)))
	} else {
		m.Meta["etag"] = etag
	}
	m.Meta["last-modified"] = modTime.UTC().Format(http.TimeFormat)
	m.Meta["Content-Length"] = strconv.Itoa(int(size))
	return jsonSave(f, m)
}

func getCacheSHADir(dir, bucket, object string) string {
	return pathJoin(dir, getSHA256Hash([]byte(pathJoin(bucket, object))))
}

// Cache data to disk with bitrot checksum added for each block of 1MB
func (c *diskCache) bitrotWriteToCache(cachePath, fileName string, reader io.Reader, size uint64) (int64, string, error) {
	if err := os.MkdirAll(cachePath, 0777); err != nil {
		return 0, "", err
	}
	filePath := pathJoin(cachePath, fileName)

	if filePath == "" || reader == nil {
		return 0, "", errInvalidArgument
	}

	if err := checkPathLength(filePath); err != nil {
		return 0, "", err
	}
	f, err := os.Create(filePath)
	if err != nil {
		return 0, "", osErrToFileErr(err)
	}
	defer f.Close()

	var bytesWritten int64

	h := HighwayHash256S.New()

	bufp := c.pool.Get().(*[]byte)
	defer c.pool.Put(bufp)
	md5Hash := md5.New()
	var n, n2 int
	for {
		n, err = io.ReadFull(reader, *bufp)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return 0, "", err
		}
		eof := err == io.EOF || err == io.ErrUnexpectedEOF
		if n == 0 && size != 0 {
			// Reached EOF, nothing more to be done.
			break
		}
		h.Reset()
		if _, err = h.Write((*bufp)[:n]); err != nil {
			return 0, "", err
		}
		hashBytes := h.Sum(nil)
		// compute md5Hash of original data stream if writeback commit to cache
		if c.commitWriteback || c.commitWritethrough {
			if _, err = md5Hash.Write((*bufp)[:n]); err != nil {
				return 0, "", err
			}
		}
		if _, err = f.Write(hashBytes); err != nil {
			return 0, "", err
		}
		if n2, err = f.Write((*bufp)[:n]); err != nil {
			return 0, "", err
		}
		bytesWritten += int64(n2)
		if eof {
			break
		}
	}
	md5sumCurr := md5Hash.Sum(nil)

	return bytesWritten, base64.StdEncoding.EncodeToString(md5sumCurr), nil
}

func newCacheEncryptReader(content io.Reader, bucket, object string, metadata map[string]string) (r io.Reader, err error) {
	objectEncryptionKey, err := newCacheEncryptMetadata(bucket, object, metadata)
	if err != nil {
		return nil, err
	}

	reader, err := sio.EncryptReader(content, sio.Config{Key: objectEncryptionKey[:], MinVersion: sio.Version20, CipherSuites: fips.CipherSuitesDARE()})
	if err != nil {
		return nil, crypto.ErrInvalidCustomerKey
	}
	return reader, nil
}
func newCacheEncryptMetadata(bucket, object string, metadata map[string]string) ([]byte, error) {
	var sealedKey crypto.SealedKey
	if globalCacheKMS == nil {
		return nil, errKMSNotConfigured
	}
	key, err := globalCacheKMS.GenerateKey("", kms.Context{bucket: pathJoin(bucket, object)})
	if err != nil {
		return nil, err
	}

	objectKey := crypto.GenerateKey(key.Plaintext, rand.Reader)
	sealedKey = objectKey.Seal(key.Plaintext, crypto.GenerateIV(rand.Reader), crypto.S3.String(), bucket, object)
	crypto.S3.CreateMetadata(metadata, key.KeyID, key.Ciphertext, sealedKey)

	if etag, ok := metadata["etag"]; ok {
		metadata["etag"] = hex.EncodeToString(objectKey.SealETag([]byte(etag)))
	}
	metadata[SSECacheEncrypted] = ""
	return objectKey[:], nil
}
func (c *diskCache) GetLockContext(ctx context.Context, bucket, object string) (RWLocker, LockContext, error) {
	cachePath := getCacheSHADir(c.dir, bucket, object)
	cLock := c.NewNSLockFn(cachePath)
	lkctx, err := cLock.GetLock(ctx, globalOperationTimeout)
	return cLock, lkctx, err
}

// Caches the object to disk
func (c *diskCache) Put(ctx context.Context, bucket, object string, data io.Reader, size int64, rs *HTTPRangeSpec, opts ObjectOptions, incHitsOnly, writeback bool) (oi ObjectInfo, err error) {
	if !c.diskSpaceAvailable(size) {
		io.Copy(ioutil.Discard, data)
		return oi, errDiskFull
	}
	cLock, lkctx, err := c.GetLockContext(ctx, bucket, object)
	if err != nil {
		return oi, err
	}
	ctx = lkctx.Context()
	defer cLock.Unlock(lkctx.Cancel)

	return c.put(ctx, bucket, object, data, size, rs, opts, incHitsOnly, writeback)
}

// Caches the object to disk
func (c *diskCache) put(ctx context.Context, bucket, object string, data io.Reader, size int64, rs *HTTPRangeSpec, opts ObjectOptions, incHitsOnly, writeback bool) (oi ObjectInfo, err error) {
	if !c.diskSpaceAvailable(size) {
		io.Copy(ioutil.Discard, data)
		return oi, errDiskFull
	}
	cachePath := getCacheSHADir(c.dir, bucket, object)
	meta, _, numHits, err := c.statCache(ctx, cachePath)
	// Case where object not yet cached
	if osIsNotExist(err) && c.after >= 1 {
		return oi, c.saveMetadata(ctx, bucket, object, opts.UserDefined, size, nil, "", false)
	}
	// Case where object already has a cache metadata entry but not yet cached
	if err == nil && numHits < c.after {
		cETag := extractETag(meta.Meta)
		bETag := extractETag(opts.UserDefined)
		if cETag == bETag {
			return oi, c.saveMetadata(ctx, bucket, object, opts.UserDefined, size, nil, "", false)
		}
		incHitsOnly = true
	}

	if rs != nil {
		return oi, c.putRange(ctx, bucket, object, data, size, rs, opts)
	}
	if !c.diskSpaceAvailable(size) {
		return oi, errDiskFull
	}
	if err := os.MkdirAll(cachePath, 0777); err != nil {
		return oi, err
	}
	var metadata = cloneMSS(opts.UserDefined)
	var reader = data
	var actualSize = uint64(size)
	if globalCacheKMS != nil {
		reader, err = newCacheEncryptReader(data, bucket, object, metadata)
		if err != nil {
			return oi, err
		}
		actualSize, _ = sio.EncryptedSize(uint64(size))
	}
	n, md5sum, err := c.bitrotWriteToCache(cachePath, cacheDataFile, reader, actualSize)
	if IsErr(err, baseErrs...) {
		// take the cache drive offline
		c.setOffline()
	}
	if err != nil {
		removeAll(cachePath)
		return oi, err
	}

	if actualSize != uint64(n) {
		removeAll(cachePath)
		return oi, IncompleteBody{Bucket: bucket, Object: object}
	}
	if writeback {
		metadata["content-md5"] = md5sum
		if md5bytes, err := base64.StdEncoding.DecodeString(md5sum); err == nil {
			metadata["etag"] = hex.EncodeToString(md5bytes)
		}
		metadata[writeBackStatusHeader] = CommitPending.String()
	}
	return ObjectInfo{
			Bucket:      bucket,
			Name:        object,
			ETag:        metadata["etag"],
			Size:        n,
			UserDefined: metadata,
		},
		c.saveMetadata(ctx, bucket, object, metadata, n, nil, "", incHitsOnly)
}

// Caches the range to disk
func (c *diskCache) putRange(ctx context.Context, bucket, object string, data io.Reader, size int64, rs *HTTPRangeSpec, opts ObjectOptions) error {
	rlen, err := rs.GetLength(size)
	if err != nil {
		return err
	}
	if !c.diskSpaceAvailable(rlen) {
		return errDiskFull
	}
	cachePath := getCacheSHADir(c.dir, bucket, object)
	if err := os.MkdirAll(cachePath, 0777); err != nil {
		return err
	}
	var metadata = cloneMSS(opts.UserDefined)
	var reader = data
	var actualSize = uint64(rlen)
	// objSize is the actual size of object (with encryption overhead if any)
	var objSize = uint64(size)
	if globalCacheKMS != nil {
		reader, err = newCacheEncryptReader(data, bucket, object, metadata)
		if err != nil {
			return err
		}
		actualSize, _ = sio.EncryptedSize(uint64(rlen))
		objSize, _ = sio.EncryptedSize(uint64(size))

	}
	cacheFile := MustGetUUID()
	n, _, err := c.bitrotWriteToCache(cachePath, cacheFile, reader, actualSize)
	if IsErr(err, baseErrs...) {
		// take the cache drive offline
		c.setOffline()
	}
	if err != nil {
		removeAll(cachePath)
		return err
	}
	if actualSize != uint64(n) {
		removeAll(cachePath)
		return IncompleteBody{Bucket: bucket, Object: object}
	}
	return c.saveMetadata(ctx, bucket, object, metadata, int64(objSize), rs, cacheFile, false)
}

// checks streaming bitrot checksum of cached object before returning data
func (c *diskCache) bitrotReadFromCache(ctx context.Context, filePath string, offset, length int64, writer io.Writer) error {
	h := HighwayHash256S.New()

	checksumHash := make([]byte, h.Size())

	startBlock := offset / cacheBlkSize
	endBlock := (offset + length) / cacheBlkSize

	// get block start offset
	var blockStartOffset int64
	if startBlock > 0 {
		blockStartOffset = (cacheBlkSize + int64(h.Size())) * startBlock
	}

	tillLength := (cacheBlkSize + int64(h.Size())) * (endBlock - startBlock + 1)

	// Start offset cannot be negative.
	if offset < 0 {
		logger.LogIf(ctx, errUnexpected)
		return errUnexpected
	}

	// Writer cannot be nil.
	if writer == nil {
		logger.LogIf(ctx, errUnexpected)
		return errUnexpected
	}
	var blockOffset, blockLength int64
	rc, err := readCacheFileStream(filePath, blockStartOffset, tillLength)
	if err != nil {
		return err
	}
	bufp := c.pool.Get().(*[]byte)
	defer c.pool.Put(bufp)

	for block := startBlock; block <= endBlock; block++ {
		switch {
		case startBlock == endBlock:
			blockOffset = offset % cacheBlkSize
			blockLength = length
		case block == startBlock:
			blockOffset = offset % cacheBlkSize
			blockLength = cacheBlkSize - blockOffset
		case block == endBlock:
			blockOffset = 0
			blockLength = (offset + length) % cacheBlkSize
		default:
			blockOffset = 0
			blockLength = cacheBlkSize
		}
		if blockLength == 0 {
			break
		}
		if _, err := io.ReadFull(rc, checksumHash); err != nil {
			return err
		}
		h.Reset()
		n, err := io.ReadFull(rc, *bufp)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			logger.LogIf(ctx, err)
			return err
		}
		eof := err == io.EOF || err == io.ErrUnexpectedEOF
		if n == 0 && length != 0 {
			// Reached EOF, nothing more to be done.
			break
		}

		if _, e := h.Write((*bufp)[:n]); e != nil {
			return e
		}
		hashBytes := h.Sum(nil)

		if !bytes.Equal(hashBytes, checksumHash) {
			err = fmt.Errorf("hashes do not match expected %s, got %s",
				hex.EncodeToString(checksumHash), hex.EncodeToString(hashBytes))
			logger.LogIf(GlobalContext, err)
			return err
		}

		if _, err = io.Copy(writer, bytes.NewReader((*bufp)[blockOffset:blockOffset+blockLength])); err != nil {
			if err != io.ErrClosedPipe {
				logger.LogIf(ctx, err)
				return err
			}
			eof = true
		}
		if eof {
			break
		}
	}

	return nil
}

// Get returns ObjectInfo and reader for object from disk cache
func (c *diskCache) Get(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, opts ObjectOptions) (gr *GetObjectReader, numHits int, err error) {
	cacheObjPath := getCacheSHADir(c.dir, bucket, object)
	cLock := c.NewNSLockFn(cacheObjPath)
	lkctx, err := cLock.GetRLock(ctx, globalOperationTimeout)
	if err != nil {
		return nil, numHits, err
	}
	ctx = lkctx.Context()
	defer cLock.RUnlock(lkctx.Cancel)

	var objInfo ObjectInfo
	var rngInfo RangeInfo
	if objInfo, rngInfo, numHits, err = c.statRange(ctx, bucket, object, rs); err != nil {
		return nil, numHits, toObjectErr(err, bucket, object)
	}
	cacheFile := cacheDataFile
	objSize := objInfo.Size
	if !rngInfo.Empty() {
		// for cached ranges, need to pass actual range file size to GetObjectReader
		// and clear out range spec
		cacheFile = rngInfo.File
		objInfo.Size = rngInfo.Size
		rs = nil
	}

	if objInfo.IsCompressed() {
		// Cache isn't compressed.
		delete(objInfo.UserDefined, ReservedMetadataPrefix+"compression")
	}

	// For a directory, we need to send an reader that returns no bytes.
	if HasSuffix(object, SlashSeparator) {
		// The lock taken above is released when
		// objReader.Close() is called by the caller.
		gr, gerr := NewGetObjectReaderFromReader(bytes.NewBuffer(nil), objInfo, opts)
		return gr, numHits, gerr
	}
	fn, startOffset, length, nErr := NewGetObjectReader(rs, objInfo, opts)
	if nErr != nil {
		return nil, numHits, nErr
	}
	var totalBytesRead int64

	pr, pw := xioutil.WaitPipe()
	if len(objInfo.Parts) > 0 {
		// For negative length read everything.
		if length < 0 {
			length = objInfo.Size - startOffset
		}

		// Reply back invalid range if the input offset and length fall out of range.
		if startOffset > objInfo.Size || startOffset+length > objInfo.Size {
			logger.LogIf(ctx, InvalidRange{startOffset, length, objInfo.Size}, logger.Application)
			return nil, numHits, InvalidRange{startOffset, length, objInfo.Size}
		}
		// Get start part index and offset.
		partIndex, partOffset, err := cacheObjectToPartOffset(objInfo, startOffset)
		if err != nil {
			return nil, numHits, InvalidRange{startOffset, length, objInfo.Size}
		}
		// Calculate endOffset according to length
		endOffset := startOffset
		if length > 0 {
			endOffset += length - 1
		}

		// Get last part index to read given length.
		lastPartIndex, _, err := cacheObjectToPartOffset(objInfo, endOffset)
		if err != nil {
			return nil, numHits, InvalidRange{startOffset, length, objInfo.Size}
		}
		go func() {
			for ; partIndex <= lastPartIndex; partIndex++ {
				if length == totalBytesRead {
					break
				}
				partNumber := objInfo.Parts[partIndex].Number
				// Save the current part name and size.
				partSize := objInfo.Parts[partIndex].Size
				partLength := partSize - partOffset
				// partLength should be adjusted so that we don't write more data than what was requested.
				if partLength > (length - totalBytesRead) {
					partLength = length - totalBytesRead
				}
				filePath := pathJoin(cacheObjPath, fmt.Sprintf("part.%d", partNumber))
				err := c.bitrotReadFromCache(ctx, filePath, partOffset, partLength, pw)
				if err != nil {
					removeAll(cacheObjPath)
					pw.CloseWithError(err)
					break
				}
				totalBytesRead += partLength
				// partOffset will be valid only for the first part, hence reset it to 0 for
				// the remaining parts.
				partOffset = 0
			} // End of read all parts loop.
			pw.CloseWithError(err)
		}()
	} else {
		go func() {
			filePath := pathJoin(cacheObjPath, cacheFile)
			err := c.bitrotReadFromCache(ctx, filePath, startOffset, length, pw)
			if err != nil {
				removeAll(cacheObjPath)
			}
			pw.CloseWithError(err)
		}()
	}

	// Cleanup function to cause the go routine above to exit, in
	// case of incomplete read.
	pipeCloser := func() { pr.CloseWithError(nil) }

	gr, gerr := fn(pr, h, pipeCloser)
	if gerr != nil {
		return gr, numHits, gerr
	}
	if globalCacheKMS != nil {
		// clean up internal SSE cache metadata
		delete(gr.ObjInfo.UserDefined, xhttp.AmzServerSideEncryption)
	}
	if !rngInfo.Empty() {
		// overlay Size with actual object size and not the range size
		gr.ObjInfo.Size = objSize
	}
	return gr, numHits, nil
}

// deletes the cached object - caller should have taken write lock
func (c *diskCache) delete(bucket, object string) (err error) {
	cacheObjPath := getCacheSHADir(c.dir, bucket, object)
	return removeAll(cacheObjPath)
}

// Deletes the cached object
func (c *diskCache) Delete(ctx context.Context, bucket, object string) (err error) {
	cacheObjPath := getCacheSHADir(c.dir, bucket, object)
	cLock := c.NewNSLockFn(cacheObjPath)
	lkctx, err := cLock.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return err
	}
	defer cLock.Unlock(lkctx.Cancel)
	return removeAll(cacheObjPath)
}

// convenience function to check if object is cached on this diskCache
func (c *diskCache) Exists(ctx context.Context, bucket, object string) bool {
	if _, err := os.Stat(getCacheSHADir(c.dir, bucket, object)); err != nil {
		return false
	}
	return true
}

// queues writeback upload failures on server startup
func (c *diskCache) scanCacheWritebackFailures(ctx context.Context) {
	defer close(c.retryWritebackCh)
	filterFn := func(name string, typ os.FileMode) error {
		if name == minioMetaBucket {
			// Proceed to next file.
			return nil
		}
		cacheDir := pathJoin(c.dir, name)
		meta, _, _, err := c.statCachedMeta(ctx, cacheDir)
		if err != nil {
			return nil
		}

		objInfo := meta.ToObjectInfo("", "")
		status, ok := objInfo.UserDefined[writeBackStatusHeader]
		if !ok || status == CommitComplete.String() {
			return nil
		}
		select {
		case c.retryWritebackCh <- objInfo:
		default:
		}

		return nil
	}

	if err := readDirFn(c.dir, filterFn); err != nil {
		logger.LogIf(ctx, err)
		return
	}
}

// NewMultipartUpload caches multipart uploads when writethrough is MINIO_CACHE_COMMIT mode
// multiparts are saved in .minio.sys/multipart/cachePath/uploadID dir until finalized. Then the individual parts
// are moved from the upload dir to cachePath/ directory.
func (c *diskCache) NewMultipartUpload(ctx context.Context, bucket, object, uID string, opts ObjectOptions) (uploadID string, err error) {
	uploadID = uID
	if uploadID == "" {
		return "", InvalidUploadID{
			Bucket:   bucket,
			Object:   object,
			UploadID: uploadID,
		}
	}

	cachePath := getMultipartCacheSHADir(c.dir, bucket, object)
	uploadIDDir := path.Join(cachePath, uploadID)
	if err := os.MkdirAll(uploadIDDir, 0777); err != nil {
		return uploadID, err
	}
	metaPath := pathJoin(uploadIDDir, cacheMetaJSONFile)

	f, err := os.OpenFile(metaPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return uploadID, err
	}
	defer f.Close()

	m := &cacheMeta{
		Version: cacheMetaVersion,
		Bucket:  bucket,
		Object:  object,
	}
	if err := jsonLoad(f, m); err != nil && err != io.EOF {
		return uploadID, err
	}

	m.Meta = opts.UserDefined

	m.Checksum = CacheChecksumInfoV1{Algorithm: HighwayHash256S.String(), Blocksize: cacheBlkSize}
	m.Stat.ModTime = UTCNow()
	if globalCacheKMS != nil {
		m.Meta[ReservedMetadataPrefix+"Encrypted-Multipart"] = ""
		if _, err := newCacheEncryptMetadata(bucket, object, m.Meta); err != nil {
			return uploadID, err
		}
	}
	err = jsonSave(f, m)
	return uploadID, err
}

// PutObjectPart caches part to cache multipart path.
func (c *diskCache) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data io.Reader, size int64, opts ObjectOptions) (partInfo PartInfo, err error) {
	oi := PartInfo{}
	if !c.diskSpaceAvailable(size) {
		io.Copy(ioutil.Discard, data)
		return oi, errDiskFull
	}
	cachePath := getMultipartCacheSHADir(c.dir, bucket, object)
	uploadIDDir := path.Join(cachePath, uploadID)

	partIDLock := c.NewNSLockFn(pathJoin(uploadIDDir, strconv.Itoa(partID)))
	lkctx, err := partIDLock.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return oi, err
	}

	ctx = lkctx.Context()
	defer partIDLock.Unlock(lkctx.Cancel)
	meta, _, _, err := c.statCache(ctx, uploadIDDir)
	// Case where object not yet cached
	if err != nil {
		return oi, err
	}

	if !c.diskSpaceAvailable(size) {
		return oi, errDiskFull
	}
	reader := data
	var actualSize = uint64(size)
	if globalCacheKMS != nil {
		reader, err = newCachePartEncryptReader(ctx, bucket, object, partID, data, size, meta.Meta)
		if err != nil {
			return oi, err
		}
		actualSize, _ = sio.EncryptedSize(uint64(size))
	}
	n, md5sum, err := c.bitrotWriteToCache(uploadIDDir, fmt.Sprintf("part.%d", partID), reader, actualSize)
	if IsErr(err, baseErrs...) {
		// take the cache drive offline
		c.setOffline()
	}
	if err != nil {
		return oi, err
	}

	if actualSize != uint64(n) {
		return oi, IncompleteBody{Bucket: bucket, Object: object}
	}
	var md5hex string
	if md5bytes, err := base64.StdEncoding.DecodeString(md5sum); err == nil {
		md5hex = hex.EncodeToString(md5bytes)
	}

	pInfo := PartInfo{
		PartNumber:   partID,
		ETag:         md5hex,
		Size:         n,
		ActualSize:   int64(actualSize),
		LastModified: UTCNow(),
	}
	return pInfo, nil
}

// SavePartMetadata saves part upload metadata to uploadID directory on disk cache
func (c *diskCache) SavePartMetadata(ctx context.Context, bucket, object, uploadID string, partID int, pinfo PartInfo) error {
	cachePath := getMultipartCacheSHADir(c.dir, bucket, object)
	uploadDir := path.Join(cachePath, uploadID)

	// acquire a write lock at upload path to update cache.json
	uploadLock := c.NewNSLockFn(uploadDir)
	ulkctx, err := uploadLock.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return err
	}
	defer uploadLock.Unlock(ulkctx.Cancel)

	metaPath := pathJoin(uploadDir, cacheMetaJSONFile)
	f, err := os.OpenFile(metaPath, os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer f.Close()

	m := &cacheMeta{}
	if err := jsonLoad(f, m); err != nil && err != io.EOF {
		return err
	}
	var key []byte
	var objectEncryptionKey crypto.ObjectKey
	if globalCacheKMS != nil {
		// Calculating object encryption key
		key, err = decryptObjectInfo(key, bucket, object, m.Meta)
		if err != nil {
			return err
		}
		copy(objectEncryptionKey[:], key)
		pinfo.ETag = hex.EncodeToString(objectEncryptionKey.SealETag([]byte(pinfo.ETag)))

	}

	pIdx := cacheObjPartIndex(m, partID)
	if pIdx == -1 {
		m.PartActualSizes = append(m.PartActualSizes, pinfo.ActualSize)
		m.PartNumbers = append(m.PartNumbers, pinfo.PartNumber)
		m.PartETags = append(m.PartETags, pinfo.ETag)
		m.PartSizes = append(m.PartSizes, pinfo.Size)
	} else {
		m.PartActualSizes[pIdx] = pinfo.ActualSize
		m.PartNumbers[pIdx] = pinfo.PartNumber
		m.PartETags[pIdx] = pinfo.ETag
		m.PartSizes[pIdx] = pinfo.Size
	}
	return jsonSave(f, m)
}

// newCachePartEncryptReader returns encrypted cache part reader, with part data encrypted with part encryption key
func newCachePartEncryptReader(ctx context.Context, bucket, object string, partID int, content io.Reader, size int64, metadata map[string]string) (r io.Reader, err error) {
	var key []byte
	var objectEncryptionKey, partEncryptionKey crypto.ObjectKey

	// Calculating object encryption key
	key, err = decryptObjectInfo(key, bucket, object, metadata)
	if err != nil {
		return nil, err
	}
	copy(objectEncryptionKey[:], key)

	partEnckey := objectEncryptionKey.DerivePartKey(uint32(partID))
	copy(partEncryptionKey[:], partEnckey[:])
	wantSize := int64(-1)
	if size >= 0 {
		info := ObjectInfo{Size: size}
		wantSize = info.EncryptedSize()
	}
	hReader, err := hash.NewReader(content, wantSize, "", "", size)
	if err != nil {
		return nil, err
	}

	pReader := NewPutObjReader(hReader)
	content, err = pReader.WithEncryption(hReader, &partEncryptionKey)
	if err != nil {
		return nil, err
	}

	reader, err := sio.EncryptReader(content, sio.Config{Key: partEncryptionKey[:], MinVersion: sio.Version20, CipherSuites: fips.CipherSuitesDARE()})
	if err != nil {
		return nil, crypto.ErrInvalidCustomerKey
	}
	return reader, nil
}

// uploadIDExists returns error if uploadID is not being cached.
func (c *diskCache) uploadIDExists(bucket, object, uploadID string) (err error) {
	mpartCachePath := getMultipartCacheSHADir(c.dir, bucket, object)
	uploadIDDir := path.Join(mpartCachePath, uploadID)
	if _, err := os.Stat(uploadIDDir); err != nil {
		return err
	}
	return nil
}

// CompleteMultipartUpload completes multipart upload on cache. The parts and cache.json are moved from the temporary location in
// .minio.sys/multipart/cacheSHA/.. to cacheSHA path after part verification succeeds.
func (c *diskCache) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []CompletePart, roi ObjectInfo, opts ObjectOptions) (oi ObjectInfo, err error) {
	cachePath := getCacheSHADir(c.dir, bucket, object)
	cLock := c.NewNSLockFn(cachePath)
	lkctx, err := cLock.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return oi, err
	}

	ctx = lkctx.Context()
	defer cLock.Unlock(lkctx.Cancel)
	mpartCachePath := getMultipartCacheSHADir(c.dir, bucket, object)
	uploadIDDir := path.Join(mpartCachePath, uploadID)

	uploadMeta, _, _, uerr := c.statCache(ctx, uploadIDDir)
	if uerr != nil {
		return oi, errUploadIDNotFound
	}

	// Case where object not yet cached
	// Calculate full object size.
	var objectSize int64

	// Calculate consolidated actual size.
	var objectActualSize int64

	var partETags []string
	partETags, err = decryptCachePartETags(uploadMeta)
	if err != nil {
		return oi, err
	}
	for i, pi := range uploadedParts {
		pIdx := cacheObjPartIndex(uploadMeta, pi.PartNumber)
		if pIdx == -1 {
			invp := InvalidPart{
				PartNumber: pi.PartNumber,
				GotETag:    pi.ETag,
			}
			return oi, invp
		}
		pi.ETag = canonicalizeETag(pi.ETag)
		if partETags[pIdx] != pi.ETag {
			invp := InvalidPart{
				PartNumber: pi.PartNumber,
				ExpETag:    partETags[pIdx],
				GotETag:    pi.ETag,
			}
			return oi, invp
		}
		// All parts except the last part has to be atleast 5MB.
		if (i < len(uploadedParts)-1) && !isMinAllowedPartSize(uploadMeta.PartActualSizes[pIdx]) {
			return oi, PartTooSmall{
				PartNumber: pi.PartNumber,
				PartSize:   uploadMeta.PartActualSizes[pIdx],
				PartETag:   pi.ETag,
			}
		}

		// Save for total object size.
		objectSize += uploadMeta.PartSizes[pIdx]

		// Save the consolidated actual size.
		objectActualSize += uploadMeta.PartActualSizes[pIdx]

	}
	uploadMeta.Stat.Size = objectSize
	uploadMeta.Stat.ModTime = roi.ModTime
	// if encrypted - make sure ETag updated

	uploadMeta.Meta["etag"] = roi.ETag
	uploadMeta.Meta[ReservedMetadataPrefix+"actual-size"] = strconv.FormatInt(objectActualSize, 10)
	var cpartETags []string
	var cpartNums []int
	var cpartSizes, cpartActualSizes []int64
	for _, pi := range uploadedParts {
		pIdx := cacheObjPartIndex(uploadMeta, pi.PartNumber)
		if pIdx != -1 {
			cpartETags = append(cpartETags, uploadMeta.PartETags[pIdx])
			cpartNums = append(cpartNums, uploadMeta.PartNumbers[pIdx])
			cpartSizes = append(cpartSizes, uploadMeta.PartSizes[pIdx])
			cpartActualSizes = append(cpartActualSizes, uploadMeta.PartActualSizes[pIdx])
		}
	}
	uploadMeta.PartETags = cpartETags
	uploadMeta.PartSizes = cpartSizes
	uploadMeta.PartActualSizes = cpartActualSizes
	uploadMeta.PartNumbers = cpartNums
	uploadMeta.Hits++
	metaPath := pathJoin(uploadIDDir, cacheMetaJSONFile)

	f, err := os.OpenFile(metaPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return oi, err
	}
	defer f.Close()
	jsonSave(f, uploadMeta)
	for _, pi := range uploadedParts {
		part := fmt.Sprintf("part.%d", pi.PartNumber)
		renameAll(pathJoin(uploadIDDir, part), pathJoin(cachePath, part))
	}
	renameAll(pathJoin(uploadIDDir, cacheMetaJSONFile), pathJoin(cachePath, cacheMetaJSONFile))
	removeAll(uploadIDDir) // clean up any unused parts in the uploadIDDir
	return uploadMeta.ToObjectInfo(bucket, object), nil
}

func (c *diskCache) AbortUpload(bucket, object, uploadID string) (err error) {
	mpartCachePath := getMultipartCacheSHADir(c.dir, bucket, object)
	uploadDir := path.Join(mpartCachePath, uploadID)
	return removeAll(uploadDir)
}

// cacheObjPartIndex - returns the index of matching object part number.
func cacheObjPartIndex(m *cacheMeta, partNumber int) int {
	for i, part := range m.PartNumbers {
		if partNumber == part {
			return i
		}
	}
	return -1
}

// cacheObjectToPartOffset calculates part index and part offset for requested offset for content on cache.
func cacheObjectToPartOffset(objInfo ObjectInfo, offset int64) (partIndex int, partOffset int64, err error) {
	if offset == 0 {
		// Special case - if offset is 0, then partIndex and partOffset are always 0.
		return 0, 0, nil
	}
	partOffset = offset
	// Seek until object offset maps to a particular part offset.
	for i, part := range objInfo.Parts {
		partIndex = i
		// Offset is smaller than size we have reached the proper part offset.
		if partOffset < part.Size {
			return partIndex, partOffset, nil
		}
		// Continue to towards the next part.
		partOffset -= part.Size
	}
	// Offset beyond the size of the object return InvalidRange.
	return 0, 0, InvalidRange{}
}

// get path of on-going multipart caching
func getMultipartCacheSHADir(dir, bucket, object string) string {
	return pathJoin(dir, minioMetaBucket, cacheMultipartDir, getSHA256Hash([]byte(pathJoin(bucket, object))))
}

// clean up stale cache multipart uploads according to cleanup interval.
func (c *diskCache) cleanupStaleUploads(ctx context.Context) {
	if !c.commitWritethrough {
		return
	}
	timer := time.NewTimer(cacheStaleUploadCleanupInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			// Reset for the next interval
			timer.Reset(cacheStaleUploadCleanupInterval)
			now := time.Now()
			readDirFn(pathJoin(c.dir, minioMetaBucket, cacheMultipartDir), func(shaDir string, typ os.FileMode) error {
				return readDirFn(pathJoin(c.dir, minioMetaBucket, cacheMultipartDir, shaDir), func(uploadIDDir string, typ os.FileMode) error {
					uploadIDPath := pathJoin(c.dir, minioMetaBucket, cacheMultipartDir, shaDir, uploadIDDir)
					fi, err := os.Stat(uploadIDPath)
					if err != nil {
						return nil
					}
					if now.Sub(fi.ModTime()) > cacheStaleUploadExpiry {
						removeAll(uploadIDPath)
					}
					return nil
				})
			})
		}
	}
}
