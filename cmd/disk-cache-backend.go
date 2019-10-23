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
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"reflect"
	"sync"
	"time"

	"github.com/djherbis/atime"
	"github.com/minio/minio/cmd/crypto"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/disk"
	"github.com/minio/sio"
	"github.com/ncw/directio"
)

const (
	// cache.json object metadata for cached objects.
	cacheMetaJSONFile = "cache.json"
	cacheDataFile     = "part.1"
	cacheMetaVersion  = "1.0.0"

	// SSECacheEncrypted is the metadata key indicating that the object
	// is a cache entry encrypted with cache KMS master key in globalCacheKMS.
	SSECacheEncrypted = "X-Minio-Internal-Encrypted-Cache"
)

// CacheChecksumInfoV1 - carries checksums of individual blocks on disk.
type CacheChecksumInfoV1 struct {
	Algorithm string `json:"algorithm"`
	Blocksize int64  `json:"blocksize"`
}

// Represents the cache metadata struct
type cacheMeta struct {
	Version string   `json:"version"`
	Stat    statInfo `json:"stat"` // Stat of the current object `cache.json`.

	// checksums of blocks on disk.
	Checksum CacheChecksumInfoV1 `json:"checksum,omitempty"`
	// Metadata map for current object.
	Meta map[string]string `json:"meta,omitempty"`
}

func (m *cacheMeta) ToObjectInfo(bucket, object string) (o ObjectInfo) {
	if len(m.Meta) == 0 {
		m.Meta = make(map[string]string)
		m.Stat.ModTime = timeSentinel
	}

	o = ObjectInfo{
		Bucket: bucket,
		Name:   object,
	}

	// We set file info only if its valid.
	o.ModTime = m.Stat.ModTime
	o.Size = m.Stat.Size
	o.ETag = extractETag(m.Meta)
	o.ContentType = m.Meta["content-type"]
	o.ContentEncoding = m.Meta["content-encoding"]
	if storageClass, ok := m.Meta[xhttp.AmzStorageClass]; ok {
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
	// remove to avoid it from appearing as part of user-defined metadata
	o.UserDefined = cleanMetadata(m.Meta)
	return o
}

// represents disk cache struct
type diskCache struct {
	dir      string // caching directory
	quotaPct int    // max usage in %
	expiry   int    // cache expiry in days
	// mark false if drive is offline
	online bool
	// mutex to protect updates to online variable
	onlineMutex *sync.RWMutex
	// purge() listens on this channel to start the cache-purge process
	purgeChan chan struct{}
	pool      sync.Pool
}

// Inits the disk cache dir if it is not initialized already.
func newDiskCache(dir string, expiry int, quotaPct int) (*diskCache, error) {
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, fmt.Errorf("Unable to initialize '%s' dir, %s", dir, err)
	}
	cache := diskCache{
		dir:         dir,
		expiry:      expiry,
		quotaPct:    quotaPct,
		purgeChan:   make(chan struct{}),
		online:      true,
		onlineMutex: &sync.RWMutex{},
		pool: sync.Pool{
			New: func() interface{} {
				b := directio.AlignedBlock(int(cacheBlkSize))
				return &b
			},
		},
	}
	return &cache, nil
}

// Returns if the disk usage is low.
// Disk usage is low if usage is < 80% of cacheMaxDiskUsagePct
// Ex. for a 100GB disk, if maxUsage is configured as 70% then cacheMaxDiskUsagePct is 70G
// hence disk usage is low if the disk usage is less than 56G (because 80% of 70G is 56G)
func (c *diskCache) diskUsageLow() bool {
	minUsage := c.quotaPct * 80 / 100
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
	return int(usedPercent) > c.quotaPct
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
	return int(usedPercent) < c.quotaPct
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
				if obj.Name() == minioMetaBucket {
					continue
				}
				// stat entry to get atime
				var fi os.FileInfo
				fi, err := os.Stat(pathJoin(c.dir, obj.Name(), cacheDataFile))
				if err != nil {
					continue
				}

				objInfo, err := c.statCache(pathJoin(c.dir, obj.Name()))
				if err != nil {
					// delete any partially filled cache entry left behind.
					removeAll(pathJoin(c.dir, obj.Name()))
					continue
				}
				cc := cacheControlOpts(objInfo)

				if atime.Get(fi).Before(expiry) ||
					cc.isStale(objInfo.ModTime) {
					if err = removeAll(pathJoin(c.dir, obj.Name())); err != nil {
						logger.LogIf(ctx, err)
					}
					deletedCount++
					// break early if sufficient disk space reclaimed.
					if !c.diskUsageLow() {
						break
					}
				}
			}
			if deletedCount == 0 {
				break
			}
		}
		lastRunTime := time.Now()
		for {
			<-c.purgeChan
			timeElapsed := time.Since(lastRunTime)
			if timeElapsed > time.Hour {
				break
			}
		}
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
	cacheObjPath := getCacheSHADir(c.dir, bucket, object)
	oi, err = c.statCache(cacheObjPath)
	if err != nil {
		return
	}
	oi.Bucket = bucket
	oi.Name = object

	if err = decryptCacheObjectETag(&oi); err != nil {
		return oi, err
	}
	return
}

// statCache is a convenience function for purge() to get ObjectInfo for cached object
func (c *diskCache) statCache(cacheObjPath string) (oi ObjectInfo, e error) {
	// Stat the file to get file size.
	metaPath := path.Join(cacheObjPath, cacheMetaJSONFile)
	f, err := os.Open(metaPath)
	if err != nil {
		return oi, err
	}
	defer f.Close()

	meta := &cacheMeta{Version: cacheMetaVersion}
	if err := jsonLoad(f, meta); err != nil {
		return oi, err
	}
	fi, err := os.Stat(pathJoin(cacheObjPath, cacheDataFile))
	if err != nil {
		return oi, err
	}
	meta.Stat.ModTime = atime.Get(fi)
	return meta.ToObjectInfo("", ""), nil
}

// saves object metadata to disk cache
func (c *diskCache) saveMetadata(ctx context.Context, bucket, object string, meta map[string]string, actualSize int64) error {
	fileName := getCacheSHADir(c.dir, bucket, object)
	metaPath := pathJoin(fileName, cacheMetaJSONFile)

	f, err := os.Create(metaPath)
	if err != nil {
		return err
	}
	defer f.Close()

	m := cacheMeta{Meta: meta, Version: cacheMetaVersion}
	m.Stat.Size = actualSize
	m.Stat.ModTime = UTCNow()
	m.Checksum = CacheChecksumInfoV1{Algorithm: HighwayHash256S.String(), Blocksize: cacheBlkSize}
	jsonData, err := json.Marshal(m)
	if err != nil {
		return err
	}
	_, err = f.Write(jsonData)
	return err
}

// Backend metadata could have changed through server side copy - reset cache metadata if that is the case
func (c *diskCache) updateMetadataIfChanged(ctx context.Context, bucket, object string, bkObjectInfo, cacheObjInfo ObjectInfo) error {

	bkMeta := make(map[string]string)
	cacheMeta := make(map[string]string)
	for k, v := range bkObjectInfo.UserDefined {
		if hasPrefix(k, ReservedMetadataPrefix) {
			// Do not need to send any internal metadata
			continue
		}
		bkMeta[http.CanonicalHeaderKey(k)] = v
	}
	for k, v := range cacheObjInfo.UserDefined {
		if hasPrefix(k, ReservedMetadataPrefix) {
			// Do not need to send any internal metadata
			continue
		}
		cacheMeta[http.CanonicalHeaderKey(k)] = v
	}
	if !reflect.DeepEqual(bkMeta, cacheMeta) ||
		bkObjectInfo.ETag != cacheObjInfo.ETag ||
		bkObjectInfo.ContentType != cacheObjInfo.ContentType ||
		bkObjectInfo.Expires != cacheObjInfo.Expires {
		return c.saveMetadata(ctx, bucket, object, getMetadata(bkObjectInfo), bkObjectInfo.Size)
	}
	return nil
}

func getCacheSHADir(dir, bucket, object string) string {
	return path.Join(dir, getSHA256Hash([]byte(path.Join(bucket, object))))
}

// Cache data to disk with bitrot checksum added for each block of 1MB
func (c *diskCache) bitrotWriteToCache(cachePath string, reader io.Reader, size uint64) (int64, error) {
	if err := os.MkdirAll(cachePath, 0777); err != nil {
		return 0, err
	}
	filePath := path.Join(cachePath, cacheDataFile)

	if filePath == "" || reader == nil {
		return 0, errInvalidArgument
	}

	if err := checkPathLength(filePath); err != nil {
		return 0, err
	}
	f, err := os.Create(filePath)
	if err != nil {
		return 0, osErrToFSFileErr(err)
	}
	defer f.Close()

	var bytesWritten int64

	h := HighwayHash256S.New()

	bufp := c.pool.Get().(*[]byte)
	defer c.pool.Put(bufp)

	var n, n2 int
	for {
		n, err = io.ReadFull(reader, *bufp)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return 0, err
		}
		eof := err == io.EOF || err == io.ErrUnexpectedEOF
		if n == 0 && size != 0 {
			// Reached EOF, nothing more to be done.
			break
		}
		h.Reset()
		if _, err = h.Write((*bufp)[:n]); err != nil {
			return 0, err
		}
		hashBytes := h.Sum(nil)
		if _, err = f.Write(hashBytes); err != nil {
			return 0, err
		}
		if n2, err = f.Write((*bufp)[:n]); err != nil {
			return 0, err
		}
		bytesWritten += int64(n2)
		if eof {
			break
		}
	}
	return bytesWritten, nil
}

func newCacheEncryptReader(content io.Reader, bucket, object string, metadata map[string]string) (r io.Reader, err error) {
	objectEncryptionKey, err := newCacheEncryptMetadata(bucket, object, metadata)
	if err != nil {
		return nil, err
	}

	reader, err := sio.EncryptReader(content, sio.Config{Key: objectEncryptionKey[:], MinVersion: sio.Version20})
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
	key, encKey, err := globalCacheKMS.GenerateKey(globalCacheKMS.KeyID(), crypto.Context{bucket: path.Join(bucket, object)})
	if err != nil {
		return nil, err
	}

	objectKey := crypto.GenerateKey(key, rand.Reader)
	sealedKey = objectKey.Seal(key, crypto.GenerateIV(rand.Reader), crypto.S3.String(), bucket, object)
	crypto.S3.CreateMetadata(metadata, globalCacheKMS.KeyID(), encKey, sealedKey)

	if etag, ok := metadata["etag"]; ok {
		metadata["etag"] = hex.EncodeToString(objectKey.SealETag([]byte(etag)))
	}
	metadata[SSECacheEncrypted] = ""
	return objectKey[:], nil
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
	cachePath := getCacheSHADir(c.dir, bucket, object)
	if err := os.MkdirAll(cachePath, 0777); err != nil {
		return err
	}
	var metadata = make(map[string]string)
	for k, v := range opts.UserDefined {
		metadata[k] = v
	}
	var reader = data
	var actualSize = uint64(size)
	var err error
	if globalCacheKMS != nil {
		reader, err = newCacheEncryptReader(data, bucket, object, metadata)
		if err != nil {
			return err
		}
		actualSize, _ = sio.EncryptedSize(uint64(size))
	}
	n, err := c.bitrotWriteToCache(cachePath, reader, actualSize)
	if IsErr(err, baseErrs...) {
		c.setOnline(false)
	}
	if err != nil {
		removeAll(cachePath)
		return err
	}
	if actualSize != uint64(n) {
		removeAll(cachePath)
		return IncompleteBody{}
	}
	return c.saveMetadata(ctx, bucket, object, metadata, n)
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
			logger.LogIf(context.Background(), err)
			return err
		}

		if _, err := io.Copy(writer, bytes.NewReader((*bufp)[blockOffset:blockOffset+blockLength])); err != nil {
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
func (c *diskCache) Get(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, opts ObjectOptions) (gr *GetObjectReader, err error) {
	var objInfo ObjectInfo
	cacheObjPath := getCacheSHADir(c.dir, bucket, object)

	if objInfo, err = c.Stat(ctx, bucket, object); err != nil {
		return nil, toObjectErr(err, bucket, object)
	}

	var nsUnlocker = func() {}
	// For a directory, we need to send an reader that returns no bytes.
	if hasSuffix(object, SlashSeparator) {
		// The lock taken above is released when
		// objReader.Close() is called by the caller.
		return NewGetObjectReaderFromReader(bytes.NewBuffer(nil), objInfo, opts.CheckCopyPrecondFn, nsUnlocker)
	}

	fn, off, length, nErr := NewGetObjectReader(rs, objInfo, opts.CheckCopyPrecondFn, nsUnlocker)
	if nErr != nil {
		return nil, nErr
	}

	filePath := path.Join(cacheObjPath, cacheDataFile)
	pr, pw := io.Pipe()
	go func() {
		err := c.bitrotReadFromCache(ctx, filePath, off, length, pw)
		if err != nil {
			removeAll(cacheObjPath)
		}
		pw.CloseWithError(err)
	}()
	// Cleanup function to cause the go routine above to exit, in
	// case of incomplete read.
	pipeCloser := func() { pr.Close() }
	return fn(pr, h, opts.CheckCopyPrecondFn, pipeCloser)

}

// Deletes the cached object
func (c *diskCache) Delete(ctx context.Context, bucket, object string) (err error) {
	cachePath := getCacheSHADir(c.dir, bucket, object)
	return removeAll(cachePath)

}

// convenience function to check if object is cached on this diskCache
func (c *diskCache) Exists(ctx context.Context, bucket, object string) bool {
	if _, err := os.Stat(getCacheSHADir(c.dir, bucket, object)); err != nil {
		return false
	}
	return true
}
