/*
 * MinIO Cloud Storage, (C) 2016, 2017, 2018 MinIO, Inc.
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
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/user"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/minio-go/v6/pkg/s3utils"
	"github.com/minio/minio-go/v6/pkg/tags"
	"github.com/minio/minio/cmd/config"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/bucket/policy"
	"github.com/minio/minio/pkg/color"
	"github.com/minio/minio/pkg/lock"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/minio/pkg/mimedb"
	"github.com/minio/minio/pkg/mountinfo"
)

// Default etag is used for pre-existing objects.
var defaultEtag = "00000000000000000000000000000000-1"

// FSObjects - Implements fs object layer.
type FSObjects struct {
	GatewayUnsupported

	// Disk usage metrics
	totalUsed uint64 // ref: https://golang.org/pkg/sync/atomic/#pkg-note-BUG

	// The count of concurrent calls on FSObjects API
	activeIOCount int64
	// The active IO count ceiling for crawling to work
	maxActiveIOCount int64

	// Path to be exported over S3 API.
	fsPath string
	// meta json filename, varies by fs / cache backend.
	metaJSONFile string
	// Unique value to be used for all
	// temporary transactions.
	fsUUID string

	// This value shouldn't be touched, once initialized.
	fsFormatRlk *lock.RLockedFile // Is a read lock on `format.json`.

	// FS rw pool.
	rwPool *fsIOPool

	// ListObjects pool management.
	listPool *TreeWalkPool

	diskMount bool

	appendFileMap   map[string]*fsAppendFile
	appendFileMapMu sync.Mutex

	// To manage the appendRoutine go-routines
	nsMutex *nsLockMap
}

// Represents the background append file.
type fsAppendFile struct {
	sync.Mutex
	parts    []PartInfo // List of parts appended.
	filePath string     // Absolute path of the file in the temp location.
}

// Initializes meta volume on all the fs path.
func initMetaVolumeFS(fsPath, fsUUID string) error {
	// This happens for the first time, but keep this here since this
	// is the only place where it can be made less expensive
	// optimizing all other calls. Create minio meta volume,
	// if it doesn't exist yet.
	metaBucketPath := pathJoin(fsPath, minioMetaBucket)

	if err := os.MkdirAll(metaBucketPath, 0777); err != nil {
		return err
	}

	metaTmpPath := pathJoin(fsPath, minioMetaTmpBucket, fsUUID)
	if err := os.MkdirAll(metaTmpPath, 0777); err != nil {
		return err
	}

	if err := os.MkdirAll(pathJoin(fsPath, dataUsageBucket), 0777); err != nil {
		return err
	}

	metaMultipartPath := pathJoin(fsPath, minioMetaMultipartBucket)
	return os.MkdirAll(metaMultipartPath, 0777)

}

// NewFSObjectLayer - initialize new fs object layer.
func NewFSObjectLayer(fsPath string) (ObjectLayer, error) {
	ctx := GlobalContext
	if fsPath == "" {
		return nil, errInvalidArgument
	}

	var err error
	if fsPath, err = getValidPath(fsPath, false); err != nil {
		if err == errMinDiskSize {
			return nil, config.ErrUnableToWriteInBackend(err).Hint(err.Error())
		}

		// Show a descriptive error with a hint about how to fix it.
		var username string
		if u, err := user.Current(); err == nil {
			username = u.Username
		} else {
			username = "<your-username>"
		}
		hint := fmt.Sprintf("Use 'sudo chown -R %s %s && sudo chmod u+rxw %s' to provide sufficient permissions.", username, fsPath, fsPath)
		return nil, config.ErrUnableToWriteInBackend(err).Hint(hint)
	}

	// Assign a new UUID for FS minio mode. Each server instance
	// gets its own UUID for temporary file transaction.
	fsUUID := mustGetUUID()

	// Initialize meta volume, if volume already exists ignores it.
	if err = initMetaVolumeFS(fsPath, fsUUID); err != nil {
		return nil, err
	}

	// Initialize `format.json`, this function also returns.
	rlk, err := initFormatFS(ctx, fsPath)
	if err != nil {
		return nil, err
	}

	// Initialize fs objects.
	fs := &FSObjects{
		fsPath:       fsPath,
		metaJSONFile: fsMetaJSONFile,
		fsUUID:       fsUUID,
		rwPool: &fsIOPool{
			readersMap: make(map[string]*lock.RLockedFile),
		},
		nsMutex:       newNSLock(false),
		listPool:      NewTreeWalkPool(globalLookupTimeout),
		appendFileMap: make(map[string]*fsAppendFile),
		diskMount:     mountinfo.IsLikelyMountPoint(fsPath),

		maxActiveIOCount: 10,
	}

	// Once the filesystem has initialized hold the read lock for
	// the life time of the server. This is done to ensure that under
	// shared backend mode for FS, remote servers do not migrate
	// or cause changes on backend format.
	fs.fsFormatRlk = rlk

	go fs.cleanupStaleMultipartUploads(ctx, GlobalMultipartCleanupInterval, GlobalMultipartExpiry)
	go intDataUpdateTracker.start(GlobalContext, fsPath)

	// Return successfully initialized object layer.
	return fs, nil
}

// NewNSLock - initialize a new namespace RWLocker instance.
func (fs *FSObjects) NewNSLock(ctx context.Context, bucket string, objects ...string) RWLocker {
	// lockers are explicitly 'nil' for FS mode since there are only local lockers
	return fs.nsMutex.NewNSLock(ctx, nil, bucket, objects...)
}

// Shutdown - should be called when process shuts down.
func (fs *FSObjects) Shutdown(ctx context.Context) error {
	fs.fsFormatRlk.Close()

	// Cleanup and delete tmp uuid.
	return fsRemoveAll(ctx, pathJoin(fs.fsPath, minioMetaTmpBucket, fs.fsUUID))
}

// StorageInfo - returns underlying storage statistics.
func (fs *FSObjects) StorageInfo(ctx context.Context, _ bool) (StorageInfo, []error) {

	atomic.AddInt64(&fs.activeIOCount, 1)
	defer func() {
		atomic.AddInt64(&fs.activeIOCount, -1)
	}()

	di, err := getDiskInfo(fs.fsPath)
	if err != nil {
		return StorageInfo{}, []error{err}
	}
	used := di.Total - di.Free
	if !fs.diskMount {
		used = atomic.LoadUint64(&fs.totalUsed)
	}

	storageInfo := StorageInfo{
		Used:       []uint64{used},
		Total:      []uint64{di.Total},
		Available:  []uint64{di.Free},
		MountPaths: []string{fs.fsPath},
	}
	storageInfo.Backend.Type = BackendFS
	return storageInfo, nil
}

func (fs *FSObjects) waitForLowActiveIO() {
	for atomic.LoadInt64(&fs.activeIOCount) >= fs.maxActiveIOCount {
		time.Sleep(lowActiveIOWaitTick)
	}
}

// CrawlAndGetDataUsage returns data usage stats of the current FS deployment
func (fs *FSObjects) CrawlAndGetDataUsage(ctx context.Context, bf *bloomFilter, updates chan<- DataUsageInfo) error {
	// Load bucket totals
	var totalCache dataUsageCache
	err := totalCache.load(ctx, fs, dataUsageCacheName)
	if err != nil {
		return err
	}
	totalCache.Info.Name = dataUsageRoot
	buckets, err := fs.ListBuckets(ctx)
	if err != nil {
		return err
	}
	totalCache.Info.BloomFilter = bf.bytes()

	// Clear totals.
	var root dataUsageEntry
	if r := totalCache.root(); r != nil {
		root.Children = r.Children
	}
	totalCache.replace(dataUsageRoot, "", root)

	// Delete all buckets that does not exist anymore.
	totalCache.keepBuckets(buckets)

	for _, b := range buckets {
		// Load bucket cache.
		var bCache dataUsageCache
		err := bCache.load(ctx, fs, path.Join(b.Name, dataUsageCacheName))
		if err != nil {
			return err
		}
		if bCache.Info.Name == "" {
			bCache.Info.Name = b.Name
		}
		bCache.Info.BloomFilter = totalCache.Info.BloomFilter

		cache, err := fs.crawlBucket(ctx, b.Name, bCache)
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		logger.LogIf(ctx, err)
		cache.Info.BloomFilter = nil
		if cache.Info.LastUpdate.After(bCache.Info.LastUpdate) {
			if intDataUpdateTracker.debug {
				logger.Info(color.Green("CrawlAndGetDataUsage:")+" Saving bucket %q cache with %d entries", b.Name, len(cache.Cache))
			}
			logger.LogIf(ctx, cache.save(ctx, fs, path.Join(b.Name, dataUsageCacheName)))
		}
		// Merge, save and send update.
		// We do it even if unchanged.
		cl := cache.clone()
		entry := cl.flatten(*cl.root())
		totalCache.replace(cl.Info.Name, dataUsageRoot, entry)
		if intDataUpdateTracker.debug {
			logger.Info(color.Green("CrawlAndGetDataUsage:")+" Saving totals cache with %d entries", len(totalCache.Cache))
		}
		logger.LogIf(ctx, totalCache.save(ctx, fs, dataUsageCacheName))
		cloned := totalCache.clone()
		updates <- cloned.dui(dataUsageRoot, buckets)
	}

	return nil
}

// crawlBucket crawls a single bucket in FS mode.
// The updated cache for the bucket is returned.
// A partially updated bucket may be returned.
func (fs *FSObjects) crawlBucket(ctx context.Context, bucket string, cache dataUsageCache) (dataUsageCache, error) {
	// Get bucket policy
	// Check if the current bucket has a configured lifecycle policy
	lc, err := globalLifecycleSys.Get(bucket)
	if err == nil && lc.HasActiveRules("", true) {
		if intDataUpdateTracker.debug {
			logger.Info(color.Green("crawlBucket:") + " lifecycle: Active rules found")
		}
		cache.Info.lifeCycle = lc
	}

	// Load bucket info.
	cache, err = crawlDataFolder(ctx, fs.fsPath, cache, fs.waitForLowActiveIO, func(item crawlItem) (int64, error) {
		bucket, object := item.bucket, item.objectPath()
		fsMetaBytes, err := ioutil.ReadFile(pathJoin(fs.fsPath, minioMetaBucket, bucketMetaPrefix, bucket, object, fs.metaJSONFile))
		if err != nil && !os.IsNotExist(err) {
			return 0, errSkipFile
		}

		fsMeta := newFSMetaV1()
		metaOk := false
		if len(fsMetaBytes) > 0 {
			var json = jsoniter.ConfigCompatibleWithStandardLibrary
			if err = json.Unmarshal(fsMetaBytes, &fsMeta); err == nil {
				metaOk = true
			}
		}
		if !metaOk {
			fsMeta = fs.defaultFsJSON(object)
		}

		// Stat the file.
		fi, fiErr := os.Stat(item.Path)
		if fiErr != nil {
			return 0, errSkipFile
		}

		oi := fsMeta.ToObjectInfo(bucket, object, fi)
		sz := item.applyActions(ctx, fs, actionMeta{oi: oi})
		if sz >= 0 {
			return sz, nil
		}

		return fi.Size(), nil
	})

	return cache, err
}

/// Bucket operations

// getBucketDir - will convert incoming bucket names to
// corresponding valid bucket names on the backend in a platform
// compatible way for all operating systems.
func (fs *FSObjects) getBucketDir(ctx context.Context, bucket string) (string, error) {
	if bucket == "" || bucket == "." || bucket == ".." {
		return "", errVolumeNotFound
	}
	bucketDir := pathJoin(fs.fsPath, bucket)
	return bucketDir, nil
}

func (fs *FSObjects) statBucketDir(ctx context.Context, bucket string) (os.FileInfo, error) {
	bucketDir, err := fs.getBucketDir(ctx, bucket)
	if err != nil {
		return nil, err
	}
	st, err := fsStatVolume(ctx, bucketDir)
	if err != nil {
		return nil, err
	}
	return st, nil
}

// MakeBucketWithLocation - create a new bucket, returns if it already exists.
func (fs *FSObjects) MakeBucketWithLocation(ctx context.Context, bucket string, opts BucketOptions) error {
	if opts.LockEnabled || opts.VersioningEnabled {
		return NotImplemented{}
	}

	// Verify if bucket is valid.
	if s3utils.CheckValidBucketNameStrict(bucket) != nil {
		return BucketNameInvalid{Bucket: bucket}
	}

	atomic.AddInt64(&fs.activeIOCount, 1)
	defer func() {
		atomic.AddInt64(&fs.activeIOCount, -1)
	}()

	bucketDir, err := fs.getBucketDir(ctx, bucket)
	if err != nil {
		return toObjectErr(err, bucket)
	}

	if err = fsMkdir(ctx, bucketDir); err != nil {
		return toObjectErr(err, bucket)
	}

	meta := newBucketMetadata(bucket)
	if err := meta.Save(ctx, fs); err != nil {
		return toObjectErr(err, bucket)
	}

	globalBucketMetadataSys.Set(bucket, meta)

	return nil
}

// GetBucketPolicy - only needed for FS in NAS mode
func (fs *FSObjects) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	meta, err := loadBucketMetadata(ctx, fs, bucket)
	if err != nil {
		return nil, BucketPolicyNotFound{Bucket: bucket}
	}
	if meta.policyConfig == nil {
		return nil, BucketPolicyNotFound{Bucket: bucket}
	}
	return meta.policyConfig, nil
}

// SetBucketPolicy - only needed for FS in NAS mode
func (fs *FSObjects) SetBucketPolicy(ctx context.Context, bucket string, p *policy.Policy) error {
	meta, err := loadBucketMetadata(ctx, fs, bucket)
	if err != nil {
		return err
	}

	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	configData, err := json.Marshal(p)
	if err != nil {
		return err
	}
	meta.PolicyConfigJSON = configData

	return meta.Save(ctx, fs)
}

// DeleteBucketPolicy - only needed for FS in NAS mode
func (fs *FSObjects) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	meta, err := loadBucketMetadata(ctx, fs, bucket)
	if err != nil {
		return err
	}
	meta.PolicyConfigJSON = nil
	return meta.Save(ctx, fs)
}

// GetBucketInfo - fetch bucket metadata info.
func (fs *FSObjects) GetBucketInfo(ctx context.Context, bucket string) (bi BucketInfo, e error) {
	atomic.AddInt64(&fs.activeIOCount, 1)
	defer func() {
		atomic.AddInt64(&fs.activeIOCount, -1)
	}()

	st, err := fs.statBucketDir(ctx, bucket)
	if err != nil {
		return bi, toObjectErr(err, bucket)
	}

	createdTime := st.ModTime()
	meta, err := globalBucketMetadataSys.Get(bucket)
	if err == nil {
		createdTime = meta.Created
	}

	return BucketInfo{
		Name:    bucket,
		Created: createdTime,
	}, nil
}

// ListBuckets - list all s3 compatible buckets (directories) at fsPath.
func (fs *FSObjects) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	if err := checkPathLength(fs.fsPath); err != nil {
		logger.LogIf(ctx, err)
		return nil, err
	}

	atomic.AddInt64(&fs.activeIOCount, 1)
	defer func() {
		atomic.AddInt64(&fs.activeIOCount, -1)
	}()

	var bucketInfos []BucketInfo
	entries, err := readDir(fs.fsPath)
	if err != nil {
		logger.LogIf(ctx, errDiskNotFound)
		return nil, toObjectErr(errDiskNotFound)
	}

	for _, entry := range entries {
		// Ignore all reserved bucket names and invalid bucket names.
		if isReservedOrInvalidBucket(entry, false) {
			continue
		}
		var fi os.FileInfo
		fi, err = fsStatVolume(ctx, pathJoin(fs.fsPath, entry))
		// There seems like no practical reason to check for errors
		// at this point, if there are indeed errors we can simply
		// just ignore such buckets and list only those which
		// return proper Stat information instead.
		if err != nil {
			// Ignore any errors returned here.
			continue
		}
		var created = fi.ModTime()
		meta, err := globalBucketMetadataSys.Get(fi.Name())
		if err == nil {
			created = meta.Created
		}

		bucketInfos = append(bucketInfos, BucketInfo{
			Name:    fi.Name(),
			Created: created,
		})
	}

	// Sort bucket infos by bucket name.
	sort.Sort(byBucketName(bucketInfos))

	// Succes.
	return bucketInfos, nil
}

// DeleteBucket - delete a bucket and all the metadata associated
// with the bucket including pending multipart, object metadata.
func (fs *FSObjects) DeleteBucket(ctx context.Context, bucket string, forceDelete bool) error {
	atomic.AddInt64(&fs.activeIOCount, 1)
	defer func() {
		atomic.AddInt64(&fs.activeIOCount, -1)
	}()

	bucketDir, err := fs.getBucketDir(ctx, bucket)
	if err != nil {
		return toObjectErr(err, bucket)
	}

	if !forceDelete {
		// Attempt to delete regular bucket.
		if err = fsRemoveDir(ctx, bucketDir); err != nil {
			return toObjectErr(err, bucket)
		}
	} else {
		tmpBucketPath := pathJoin(fs.fsPath, minioMetaTmpBucket, bucket+"."+mustGetUUID())
		if err = fsSimpleRenameFile(ctx, bucketDir, tmpBucketPath); err != nil {
			return toObjectErr(err, bucket)
		}

		go func() {
			fsRemoveAll(ctx, tmpBucketPath) // ignore returned error if any.
		}()
	}

	// Cleanup all the bucket metadata.
	minioMetadataBucketDir := pathJoin(fs.fsPath, minioMetaBucket, bucketMetaPrefix, bucket)
	if err = fsRemoveAll(ctx, minioMetadataBucketDir); err != nil {
		return toObjectErr(err, bucket)
	}

	// Delete all bucket metadata.
	deleteBucketMetadata(ctx, fs, bucket)

	return nil
}

/// Object Operations

// CopyObject - copy object source object to destination object.
// if source object and destination object are same we only
// update metadata.
func (fs *FSObjects) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (oi ObjectInfo, e error) {
	if srcOpts.VersionID != "" && srcOpts.VersionID != nullVersionID {
		return oi, VersionNotFound{
			Bucket:    srcBucket,
			Object:    srcObject,
			VersionID: srcOpts.VersionID,
		}
	}

	cpSrcDstSame := isStringEqual(pathJoin(srcBucket, srcObject), pathJoin(dstBucket, dstObject))
	defer ObjectPathUpdated(path.Join(dstBucket, dstObject))

	if !cpSrcDstSame {
		objectDWLock := fs.NewNSLock(ctx, dstBucket, dstObject)
		if err := objectDWLock.GetLock(globalObjectTimeout); err != nil {
			return oi, err
		}
		defer objectDWLock.Unlock()
	}

	atomic.AddInt64(&fs.activeIOCount, 1)
	defer func() {
		atomic.AddInt64(&fs.activeIOCount, -1)
	}()

	if _, err := fs.statBucketDir(ctx, srcBucket); err != nil {
		return oi, toObjectErr(err, srcBucket)
	}

	if cpSrcDstSame && srcInfo.metadataOnly {
		fsMetaPath := pathJoin(fs.fsPath, minioMetaBucket, bucketMetaPrefix, srcBucket, srcObject, fs.metaJSONFile)
		wlk, err := fs.rwPool.Write(fsMetaPath)
		if err != nil {
			logger.LogIf(ctx, err)
			return oi, toObjectErr(err, srcBucket, srcObject)
		}
		// This close will allow for locks to be synchronized on `fs.json`.
		defer wlk.Close()

		// Save objects' metadata in `fs.json`.
		fsMeta := newFSMetaV1()
		if _, err = fsMeta.ReadFrom(ctx, wlk); err != nil {
			// For any error to read fsMeta, set default ETag and proceed.
			fsMeta = fs.defaultFsJSON(srcObject)
		}

		fsMeta.Meta = srcInfo.UserDefined
		fsMeta.Meta["etag"] = srcInfo.ETag
		if _, err = fsMeta.WriteTo(wlk); err != nil {
			return oi, toObjectErr(err, srcBucket, srcObject)
		}

		// Stat the file to get file size.
		fi, err := fsStatFile(ctx, pathJoin(fs.fsPath, srcBucket, srcObject))
		if err != nil {
			return oi, toObjectErr(err, srcBucket, srcObject)
		}

		// Return the new object info.
		return fsMeta.ToObjectInfo(srcBucket, srcObject, fi), nil
	}

	if err := checkPutObjectArgs(ctx, dstBucket, dstObject, fs, srcInfo.PutObjReader.Size()); err != nil {
		return ObjectInfo{}, err
	}

	objInfo, err := fs.putObject(ctx, dstBucket, dstObject, srcInfo.PutObjReader, ObjectOptions{ServerSideEncryption: dstOpts.ServerSideEncryption, UserDefined: srcInfo.UserDefined})
	if err != nil {
		return oi, toObjectErr(err, dstBucket, dstObject)
	}

	return objInfo, nil
}

// GetObjectNInfo - returns object info and a reader for object
// content.
func (fs *FSObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (gr *GetObjectReader, err error) {
	if opts.VersionID != "" && opts.VersionID != nullVersionID {
		return nil, VersionNotFound{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
		}
	}
	if err = checkGetObjArgs(ctx, bucket, object); err != nil {
		return nil, err
	}

	atomic.AddInt64(&fs.activeIOCount, 1)
	defer func() {
		atomic.AddInt64(&fs.activeIOCount, -1)
	}()

	if _, err = fs.statBucketDir(ctx, bucket); err != nil {
		return nil, toObjectErr(err, bucket)
	}

	var nsUnlocker = func() {}

	if lockType != noLock {
		// Lock the object before reading.
		lock := fs.NewNSLock(ctx, bucket, object)
		switch lockType {
		case writeLock:
			if err = lock.GetLock(globalObjectTimeout); err != nil {
				return nil, err
			}
			nsUnlocker = lock.Unlock
		case readLock:
			if err = lock.GetRLock(globalObjectTimeout); err != nil {
				return nil, err
			}
			nsUnlocker = lock.RUnlock
		}
	}

	// Otherwise we get the object info
	var objInfo ObjectInfo
	if objInfo, err = fs.getObjectInfo(ctx, bucket, object); err != nil {
		nsUnlocker()
		return nil, toObjectErr(err, bucket, object)
	}
	// For a directory, we need to send an reader that returns no bytes.
	if HasSuffix(object, SlashSeparator) {
		// The lock taken above is released when
		// objReader.Close() is called by the caller.
		return NewGetObjectReaderFromReader(bytes.NewBuffer(nil), objInfo, opts, nsUnlocker)
	}
	// Take a rwPool lock for NFS gateway type deployment
	rwPoolUnlocker := func() {}
	if bucket != minioMetaBucket && lockType != noLock {
		fsMetaPath := pathJoin(fs.fsPath, minioMetaBucket, bucketMetaPrefix, bucket, object, fs.metaJSONFile)
		_, err = fs.rwPool.Open(fsMetaPath)
		if err != nil && err != errFileNotFound {
			logger.LogIf(ctx, err)
			nsUnlocker()
			return nil, toObjectErr(err, bucket, object)
		}
		// Need to clean up lock after getObject is
		// completed.
		rwPoolUnlocker = func() { fs.rwPool.Close(fsMetaPath) }
	}

	objReaderFn, off, length, rErr := NewGetObjectReader(rs, objInfo, opts, nsUnlocker, rwPoolUnlocker)
	if rErr != nil {
		return nil, rErr
	}

	// Read the object, doesn't exist returns an s3 compatible error.
	fsObjPath := pathJoin(fs.fsPath, bucket, object)
	readCloser, size, err := fsOpenFile(ctx, fsObjPath, off)
	if err != nil {
		rwPoolUnlocker()
		nsUnlocker()
		return nil, toObjectErr(err, bucket, object)
	}
	reader := io.LimitReader(readCloser, length)
	closeFn := func() {
		readCloser.Close()
	}

	// Check if range is valid
	if off > size || off+length > size {
		err = InvalidRange{off, length, size}
		logger.LogIf(ctx, err, logger.Application)
		closeFn()
		rwPoolUnlocker()
		nsUnlocker()
		return nil, err
	}

	return objReaderFn(reader, h, opts.CheckCopyPrecondFn, closeFn)
}

// GetObject - reads an object from the disk.
// Supports additional parameters like offset and length
// which are synonymous with HTTP Range requests.
//
// startOffset indicates the starting read location of the object.
// length indicates the total length of the object.
func (fs *FSObjects) GetObject(ctx context.Context, bucket, object string, offset int64, length int64, writer io.Writer, etag string, opts ObjectOptions) (err error) {
	if opts.VersionID != "" && opts.VersionID != nullVersionID {
		return VersionNotFound{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
		}
	}

	if err = checkGetObjArgs(ctx, bucket, object); err != nil {
		return err
	}

	// Lock the object before reading.
	lk := fs.NewNSLock(ctx, bucket, object)
	if err := lk.GetRLock(globalObjectTimeout); err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	defer lk.RUnlock()

	atomic.AddInt64(&fs.activeIOCount, 1)
	defer func() {
		atomic.AddInt64(&fs.activeIOCount, -1)
	}()

	return fs.getObject(ctx, bucket, object, offset, length, writer, etag, true)
}

// getObject - wrapper for GetObject
func (fs *FSObjects) getObject(ctx context.Context, bucket, object string, offset int64, length int64, writer io.Writer, etag string, lock bool) (err error) {
	if _, err = fs.statBucketDir(ctx, bucket); err != nil {
		return toObjectErr(err, bucket)
	}

	// Offset cannot be negative.
	if offset < 0 {
		logger.LogIf(ctx, errUnexpected, logger.Application)
		return toObjectErr(errUnexpected, bucket, object)
	}

	// Writer cannot be nil.
	if writer == nil {
		logger.LogIf(ctx, errUnexpected, logger.Application)
		return toObjectErr(errUnexpected, bucket, object)
	}

	// If its a directory request, we return an empty body.
	if HasSuffix(object, SlashSeparator) {
		_, err = writer.Write([]byte(""))
		logger.LogIf(ctx, err)
		return toObjectErr(err, bucket, object)
	}

	if bucket != minioMetaBucket {
		fsMetaPath := pathJoin(fs.fsPath, minioMetaBucket, bucketMetaPrefix, bucket, object, fs.metaJSONFile)
		if lock {
			_, err = fs.rwPool.Open(fsMetaPath)
			if err != nil && err != errFileNotFound {
				logger.LogIf(ctx, err)
				return toObjectErr(err, bucket, object)
			}
			defer fs.rwPool.Close(fsMetaPath)
		}
	}

	if etag != "" && etag != defaultEtag {
		objEtag, perr := fs.getObjectETag(ctx, bucket, object, lock)
		if perr != nil {
			return toObjectErr(perr, bucket, object)
		}
		if objEtag != etag {
			logger.LogIf(ctx, InvalidETag{}, logger.Application)
			return toObjectErr(InvalidETag{}, bucket, object)
		}
	}

	// Read the object, doesn't exist returns an s3 compatible error.
	fsObjPath := pathJoin(fs.fsPath, bucket, object)
	reader, size, err := fsOpenFile(ctx, fsObjPath, offset)
	if err != nil {
		return toObjectErr(err, bucket, object)
	}
	defer reader.Close()

	bufSize := int64(readSizeV1)
	if length > 0 && bufSize > length {
		bufSize = length
	}

	// For negative length we read everything.
	if length < 0 {
		length = size - offset
	}

	// Reply back invalid range if the input offset and length fall out of range.
	if offset > size || offset+length > size {
		err = InvalidRange{offset, length, size}
		logger.LogIf(ctx, err, logger.Application)
		return err
	}

	// Allocate a staging buffer.
	buf := make([]byte, int(bufSize))

	_, err = io.CopyBuffer(writer, io.LimitReader(reader, length), buf)
	// The writer will be closed incase of range queries, which will emit ErrClosedPipe.
	if err == io.ErrClosedPipe {
		err = nil
	}
	return toObjectErr(err, bucket, object)
}

// Create a new fs.json file, if the existing one is corrupt. Should happen very rarely.
func (fs *FSObjects) createFsJSON(object, fsMetaPath string) error {
	fsMeta := newFSMetaV1()
	fsMeta.Meta = make(map[string]string)
	fsMeta.Meta["etag"] = GenETag()
	contentType := mimedb.TypeByExtension(path.Ext(object))
	fsMeta.Meta["content-type"] = contentType
	wlk, werr := fs.rwPool.Create(fsMetaPath)
	if werr == nil {
		_, err := fsMeta.WriteTo(wlk)
		wlk.Close()
		return err
	}
	return werr
}

// Used to return default etag values when a pre-existing object's meta data is queried.
func (fs *FSObjects) defaultFsJSON(object string) fsMetaV1 {
	fsMeta := newFSMetaV1()
	fsMeta.Meta = make(map[string]string)
	fsMeta.Meta["etag"] = defaultEtag
	contentType := mimedb.TypeByExtension(path.Ext(object))
	fsMeta.Meta["content-type"] = contentType
	return fsMeta
}

// getObjectInfo - wrapper for reading object metadata and constructs ObjectInfo.
func (fs *FSObjects) getObjectInfo(ctx context.Context, bucket, object string) (oi ObjectInfo, e error) {
	fsMeta := fsMetaV1{}
	if HasSuffix(object, SlashSeparator) {
		fi, err := fsStatDir(ctx, pathJoin(fs.fsPath, bucket, object))
		if err != nil {
			return oi, err
		}
		return fsMeta.ToObjectInfo(bucket, object, fi), nil
	}

	fsMetaPath := pathJoin(fs.fsPath, minioMetaBucket, bucketMetaPrefix, bucket, object, fs.metaJSONFile)
	// Read `fs.json` to perhaps contend with
	// parallel Put() operations.

	rlk, err := fs.rwPool.Open(fsMetaPath)
	if err == nil {
		// Read from fs metadata only if it exists.
		_, rerr := fsMeta.ReadFrom(ctx, rlk.LockedFile)
		fs.rwPool.Close(fsMetaPath)
		if rerr != nil {
			// For any error to read fsMeta, set default ETag and proceed.
			fsMeta = fs.defaultFsJSON(object)
		}
	}

	// Return a default etag and content-type based on the object's extension.
	if err == errFileNotFound {
		fsMeta = fs.defaultFsJSON(object)
	}

	// Ignore if `fs.json` is not available, this is true for pre-existing data.
	if err != nil && err != errFileNotFound {
		logger.LogIf(ctx, err)
		return oi, err
	}

	// Stat the file to get file size.
	fi, err := fsStatFile(ctx, pathJoin(fs.fsPath, bucket, object))
	if err != nil {
		return oi, err
	}

	return fsMeta.ToObjectInfo(bucket, object, fi), nil
}

// getObjectInfoWithLock - reads object metadata and replies back ObjectInfo.
func (fs *FSObjects) getObjectInfoWithLock(ctx context.Context, bucket, object string) (oi ObjectInfo, e error) {
	// Lock the object before reading.
	lk := fs.NewNSLock(ctx, bucket, object)
	if err := lk.GetRLock(globalObjectTimeout); err != nil {
		return oi, err
	}
	defer lk.RUnlock()

	if err := checkGetObjArgs(ctx, bucket, object); err != nil {
		return oi, err
	}

	if _, err := fs.statBucketDir(ctx, bucket); err != nil {
		return oi, err
	}

	if strings.HasSuffix(object, SlashSeparator) && !fs.isObjectDir(bucket, object) {
		return oi, errFileNotFound
	}

	return fs.getObjectInfo(ctx, bucket, object)
}

// GetObjectInfo - reads object metadata and replies back ObjectInfo.
func (fs *FSObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (oi ObjectInfo, e error) {
	if opts.VersionID != "" && opts.VersionID != nullVersionID {
		return oi, VersionNotFound{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
		}
	}

	atomic.AddInt64(&fs.activeIOCount, 1)
	defer func() {
		atomic.AddInt64(&fs.activeIOCount, -1)
	}()

	oi, err := fs.getObjectInfoWithLock(ctx, bucket, object)
	if err == errCorruptedFormat || err == io.EOF {
		lk := fs.NewNSLock(ctx, bucket, object)
		if err = lk.GetLock(globalObjectTimeout); err != nil {
			return oi, toObjectErr(err, bucket, object)
		}

		fsMetaPath := pathJoin(fs.fsPath, minioMetaBucket, bucketMetaPrefix, bucket, object, fs.metaJSONFile)
		err = fs.createFsJSON(object, fsMetaPath)
		lk.Unlock()
		if err != nil {
			return oi, toObjectErr(err, bucket, object)
		}

		oi, err = fs.getObjectInfoWithLock(ctx, bucket, object)
	}
	return oi, toObjectErr(err, bucket, object)
}

// This function does the following check, suppose
// object is "a/b/c/d", stat makes sure that objects ""a/b/c""
// "a/b" and "a" do not exist.
func (fs *FSObjects) parentDirIsObject(ctx context.Context, bucket, parent string) bool {
	var isParentDirObject func(string) bool
	isParentDirObject = func(p string) bool {
		if p == "." || p == SlashSeparator {
			return false
		}
		if fsIsFile(ctx, pathJoin(fs.fsPath, bucket, p)) {
			// If there is already a file at prefix "p", return true.
			return true
		}

		// Check if there is a file as one of the parent paths.
		return isParentDirObject(path.Dir(p))
	}
	return isParentDirObject(parent)
}

// PutObject - creates an object upon reading from the input stream
// until EOF, writes data directly to configured filesystem path.
// Additionally writes `fs.json` which carries the necessary metadata
// for future object operations.
func (fs *FSObjects) PutObject(ctx context.Context, bucket string, object string, r *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, retErr error) {
	if opts.Versioned {
		return objInfo, NotImplemented{}
	}

	if err := checkPutObjectArgs(ctx, bucket, object, fs, r.Size()); err != nil {
		return ObjectInfo{}, err
	}

	// Lock the object.
	lk := fs.NewNSLock(ctx, bucket, object)
	if err := lk.GetLock(globalObjectTimeout); err != nil {
		logger.LogIf(ctx, err)
		return objInfo, err
	}
	defer lk.Unlock()
	defer ObjectPathUpdated(path.Join(bucket, object))

	atomic.AddInt64(&fs.activeIOCount, 1)
	defer func() {
		atomic.AddInt64(&fs.activeIOCount, -1)
	}()

	return fs.putObject(ctx, bucket, object, r, opts)
}

// putObject - wrapper for PutObject
func (fs *FSObjects) putObject(ctx context.Context, bucket string, object string, r *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, retErr error) {
	data := r.Reader

	// No metadata is set, allocate a new one.
	meta := make(map[string]string)
	for k, v := range opts.UserDefined {
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
			return ObjectInfo{}, toObjectErr(errFileParentIsFile, bucket, object)
		}
		if err = mkdirAll(pathJoin(fs.fsPath, bucket, object), 0777); err != nil {
			logger.LogIf(ctx, err)
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
		var fi os.FileInfo
		if fi, err = fsStatDir(ctx, pathJoin(fs.fsPath, bucket, object)); err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
		return fsMeta.ToObjectInfo(bucket, object, fi), nil
	}

	// Check if an object is present as one of the parent dir.
	if fs.parentDirIsObject(ctx, bucket, path.Dir(object)) {
		return ObjectInfo{}, toObjectErr(errFileParentIsFile, bucket, object)
	}

	// Validate input data size and it can never be less than zero.
	if data.Size() < -1 {
		logger.LogIf(ctx, errInvalidArgument, logger.Application)
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
	fsMeta.Meta["etag"] = r.MD5CurrentHexString()

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

// DeleteObjects - deletes an object from a bucket, this operation is destructive
// and there are no rollbacks supported.
func (fs *FSObjects) DeleteObjects(ctx context.Context, bucket string, objects []ObjectToDelete, opts ObjectOptions) ([]DeletedObject, []error) {
	errs := make([]error, len(objects))
	dobjects := make([]DeletedObject, len(objects))
	for idx, object := range objects {
		if object.VersionID != "" {
			errs[idx] = VersionNotFound{
				Bucket:    bucket,
				Object:    object.ObjectName,
				VersionID: object.VersionID,
			}
			continue
		}
		_, errs[idx] = fs.DeleteObject(ctx, bucket, object.ObjectName, opts)
		if errs[idx] == nil || isErrObjectNotFound(errs[idx]) {
			dobjects[idx] = DeletedObject{
				ObjectName: object.ObjectName,
			}
			errs[idx] = nil
		}
	}
	return dobjects, errs
}

// DeleteObject - deletes an object from a bucket, this operation is destructive
// and there are no rollbacks supported.
func (fs *FSObjects) DeleteObject(ctx context.Context, bucket, object string, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	if opts.VersionID != "" && opts.VersionID != nullVersionID {
		return objInfo, VersionNotFound{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
		}
	}

	// Acquire a write lock before deleting the object.
	lk := fs.NewNSLock(ctx, bucket, object)
	if err = lk.GetLock(globalOperationTimeout); err != nil {
		return objInfo, err
	}
	defer lk.Unlock()

	if err = checkDelObjArgs(ctx, bucket, object); err != nil {
		return objInfo, err
	}

	defer ObjectPathUpdated(path.Join(bucket, object))

	atomic.AddInt64(&fs.activeIOCount, 1)
	defer func() {
		atomic.AddInt64(&fs.activeIOCount, -1)
	}()

	if _, err = fs.statBucketDir(ctx, bucket); err != nil {
		return objInfo, toObjectErr(err, bucket)
	}

	minioMetaBucketDir := pathJoin(fs.fsPath, minioMetaBucket)
	fsMetaPath := pathJoin(minioMetaBucketDir, bucketMetaPrefix, bucket, object, fs.metaJSONFile)
	if bucket != minioMetaBucket {
		rwlk, lerr := fs.rwPool.Write(fsMetaPath)
		if lerr == nil {
			// This close will allow for fs locks to be synchronized on `fs.json`.
			defer rwlk.Close()
		}
		if lerr != nil && lerr != errFileNotFound {
			logger.LogIf(ctx, lerr)
			return objInfo, toObjectErr(lerr, bucket, object)
		}
	}

	// Delete the object.
	if err = fsDeleteFile(ctx, pathJoin(fs.fsPath, bucket), pathJoin(fs.fsPath, bucket, object)); err != nil {
		return objInfo, toObjectErr(err, bucket, object)
	}

	if bucket != minioMetaBucket {
		// Delete the metadata object.
		err = fsDeleteFile(ctx, minioMetaBucketDir, fsMetaPath)
		if err != nil && err != errFileNotFound {
			return objInfo, toObjectErr(err, bucket, object)
		}
	}
	return ObjectInfo{Bucket: bucket, Name: object}, nil
}

// Returns function "listDir" of the type listDirFunc.
// isLeaf - is used by listDir function to check if an entry
// is a leaf or non-leaf entry.
func (fs *FSObjects) listDirFactory() ListDirFunc {
	// listDir - lists all the entries at a given prefix and given entry in the prefix.
	listDir := func(bucket, prefixDir, prefixEntry string) (emptyDir bool, entries []string) {
		var err error
		entries, err = readDir(pathJoin(fs.fsPath, bucket, prefixDir))
		if err != nil && err != errFileNotFound {
			logger.LogIf(GlobalContext, err)
			return false, nil
		}
		if len(entries) == 0 {
			return true, nil
		}
		sort.Strings(entries)
		return false, filterMatchingPrefix(entries, prefixEntry)
	}

	// Return list factory instance.
	return listDir
}

// isObjectDir returns true if the specified bucket & prefix exists
// and the prefix represents an empty directory. An S3 empty directory
// is also an empty directory in the FS backend.
func (fs *FSObjects) isObjectDir(bucket, prefix string) bool {
	entries, err := readDirN(pathJoin(fs.fsPath, bucket, prefix), 1)
	if err != nil {
		return false
	}
	return len(entries) == 0
}

// getObjectETag is a helper function, which returns only the md5sum
// of the file on the disk.
func (fs *FSObjects) getObjectETag(ctx context.Context, bucket, entry string, lock bool) (string, error) {
	fsMetaPath := pathJoin(fs.fsPath, minioMetaBucket, bucketMetaPrefix, bucket, entry, fs.metaJSONFile)

	var reader io.Reader
	var fi os.FileInfo
	var size int64
	if lock {
		// Read `fs.json` to perhaps contend with
		// parallel Put() operations.
		rlk, err := fs.rwPool.Open(fsMetaPath)
		// Ignore if `fs.json` is not available, this is true for pre-existing data.
		if err != nil && err != errFileNotFound {
			logger.LogIf(ctx, err)
			return "", toObjectErr(err, bucket, entry)
		}

		// If file is not found, we don't need to proceed forward.
		if err == errFileNotFound {
			return "", nil
		}

		// Read from fs metadata only if it exists.
		defer fs.rwPool.Close(fsMetaPath)

		// Fetch the size of the underlying file.
		fi, err = rlk.LockedFile.Stat()
		if err != nil {
			logger.LogIf(ctx, err)
			return "", toObjectErr(err, bucket, entry)
		}

		size = fi.Size()
		reader = io.NewSectionReader(rlk.LockedFile, 0, fi.Size())
	} else {
		var err error
		reader, size, err = fsOpenFile(ctx, fsMetaPath, 0)
		if err != nil {
			return "", toObjectErr(err, bucket, entry)
		}
	}

	// `fs.json` can be empty due to previously failed
	// PutObject() transaction, if we arrive at such
	// a situation we just ignore and continue.
	if size == 0 {
		return "", nil
	}

	fsMetaBuf, err := ioutil.ReadAll(reader)
	if err != nil {
		logger.LogIf(ctx, err)
		return "", toObjectErr(err, bucket, entry)
	}

	var fsMeta fsMetaV1
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	if err = json.Unmarshal(fsMetaBuf, &fsMeta); err != nil {
		return "", err
	}

	// Check if FS metadata is valid, if not return error.
	if !isFSMetaValid(fsMeta.Version) {
		logger.LogIf(ctx, errCorruptedFormat)
		return "", toObjectErr(errCorruptedFormat, bucket, entry)
	}

	return extractETag(fsMeta.Meta), nil
}

// ListObjectVersions not implemented for FS mode.
func (fs *FSObjects) ListObjectVersions(ctx context.Context, bucket, prefix, marker, versionMarker, delimiter string, maxKeys int) (loi ListObjectVersionsInfo, e error) {
	return loi, NotImplemented{}
}

// ListObjects - list all objects at prefix upto maxKeys., optionally delimited by '/'. Maintains the list pool
// state for future re-entrant list requests.
func (fs *FSObjects) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi ListObjectsInfo, e error) {

	atomic.AddInt64(&fs.activeIOCount, 1)
	defer func() {
		atomic.AddInt64(&fs.activeIOCount, -1)
	}()

	return listObjects(ctx, fs, bucket, prefix, marker, delimiter, maxKeys, fs.listPool,
		fs.listDirFactory(), fs.getObjectInfo, fs.getObjectInfo)
}

// GetObjectTags - get object tags from an existing object
func (fs *FSObjects) GetObjectTags(ctx context.Context, bucket, object string, opts ObjectOptions) (*tags.Tags, error) {
	if opts.VersionID != "" && opts.VersionID != nullVersionID {
		return nil, VersionNotFound{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
		}
	}
	oi, err := fs.GetObjectInfo(ctx, bucket, object, ObjectOptions{})
	if err != nil {
		return nil, err
	}

	return tags.ParseObjectTags(oi.UserTags)
}

// PutObjectTags - replace or add tags to an existing object
func (fs *FSObjects) PutObjectTags(ctx context.Context, bucket, object string, tags string, opts ObjectOptions) error {
	if opts.VersionID != "" && opts.VersionID != nullVersionID {
		return VersionNotFound{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
		}
	}

	fsMetaPath := pathJoin(fs.fsPath, minioMetaBucket, bucketMetaPrefix, bucket, object, fs.metaJSONFile)
	fsMeta := fsMetaV1{}
	wlk, err := fs.rwPool.Write(fsMetaPath)
	if err != nil {
		logger.LogIf(ctx, err)
		return toObjectErr(err, bucket, object)
	}
	// This close will allow for locks to be synchronized on `fs.json`.
	defer wlk.Close()

	// Read objects' metadata in `fs.json`.
	if _, err = fsMeta.ReadFrom(ctx, wlk); err != nil {
		// For any error to read fsMeta, set default ETag and proceed.
		fsMeta = fs.defaultFsJSON(object)
	}

	// clean fsMeta.Meta of tag key, before updating the new tags
	delete(fsMeta.Meta, xhttp.AmzObjectTagging)

	// Do not update for empty tags
	if tags != "" {
		fsMeta.Meta[xhttp.AmzObjectTagging] = tags
	}

	if _, err = fsMeta.WriteTo(wlk); err != nil {
		return toObjectErr(err, bucket, object)
	}
	return nil
}

// DeleteObjectTags - delete object tags from an existing object
func (fs *FSObjects) DeleteObjectTags(ctx context.Context, bucket, object string, opts ObjectOptions) error {
	return fs.PutObjectTags(ctx, bucket, object, "", opts)
}

// ReloadFormat - no-op for fs, Valid only for Erasure.
func (fs *FSObjects) ReloadFormat(ctx context.Context, dryRun bool) error {
	logger.LogIf(ctx, NotImplemented{})
	return NotImplemented{}
}

// HealFormat - no-op for fs, Valid only for Erasure.
func (fs *FSObjects) HealFormat(ctx context.Context, dryRun bool) (madmin.HealResultItem, error) {
	logger.LogIf(ctx, NotImplemented{})
	return madmin.HealResultItem{}, NotImplemented{}
}

// HealObject - no-op for fs. Valid only for Erasure.
func (fs *FSObjects) HealObject(ctx context.Context, bucket, object, versionID string, opts madmin.HealOpts) (
	res madmin.HealResultItem, err error) {
	logger.LogIf(ctx, NotImplemented{})
	return res, NotImplemented{}
}

// HealBucket - no-op for fs, Valid only for Erasure.
func (fs *FSObjects) HealBucket(ctx context.Context, bucket string, dryRun, remove bool) (madmin.HealResultItem,
	error) {
	logger.LogIf(ctx, NotImplemented{})
	return madmin.HealResultItem{}, NotImplemented{}
}

// Walk a bucket, optionally prefix recursively, until we have returned
// all the content to objectInfo channel, it is callers responsibility
// to allocate a receive channel for ObjectInfo, upon any unhandled
// error walker returns error. Optionally if context.Done() is received
// then Walk() stops the walker.
func (fs *FSObjects) Walk(ctx context.Context, bucket, prefix string, results chan<- ObjectInfo) error {
	return fsWalk(ctx, fs, bucket, prefix, fs.listDirFactory(), results, fs.getObjectInfo, fs.getObjectInfo)
}

// HealObjects - no-op for fs. Valid only for Erasure.
func (fs *FSObjects) HealObjects(ctx context.Context, bucket, prefix string, opts madmin.HealOpts, fn HealObjectFn) (e error) {
	logger.LogIf(ctx, NotImplemented{})
	return NotImplemented{}
}

// ListBucketsHeal - list all buckets to be healed. Valid only for Erasure
func (fs *FSObjects) ListBucketsHeal(ctx context.Context) ([]BucketInfo, error) {
	logger.LogIf(ctx, NotImplemented{})
	return []BucketInfo{}, NotImplemented{}
}

// GetMetrics - no op
func (fs *FSObjects) GetMetrics(ctx context.Context) (*Metrics, error) {
	logger.LogIf(ctx, NotImplemented{})
	return &Metrics{}, NotImplemented{}
}

// ListObjectsV2 lists all blobs in bucket filtered by prefix
func (fs *FSObjects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result ListObjectsV2Info, err error) {
	marker := continuationToken
	if marker == "" {
		marker = startAfter
	}

	loi, err := fs.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		return result, err
	}

	listObjectsV2Info := ListObjectsV2Info{
		IsTruncated:           loi.IsTruncated,
		ContinuationToken:     continuationToken,
		NextContinuationToken: loi.NextMarker,
		Objects:               loi.Objects,
		Prefixes:              loi.Prefixes,
	}
	return listObjectsV2Info, err
}

// IsNotificationSupported returns whether bucket notification is applicable for this layer.
func (fs *FSObjects) IsNotificationSupported() bool {
	return true
}

// IsListenBucketSupported returns whether listen bucket notification is applicable for this layer.
func (fs *FSObjects) IsListenBucketSupported() bool {
	return true
}

// IsEncryptionSupported returns whether server side encryption is implemented for this layer.
func (fs *FSObjects) IsEncryptionSupported() bool {
	return true
}

// IsCompressionSupported returns whether compression is applicable for this layer.
func (fs *FSObjects) IsCompressionSupported() bool {
	return true
}

// IsTaggingSupported returns true, object tagging is supported in fs object layer.
func (fs *FSObjects) IsTaggingSupported() bool {
	return true
}

// IsReady - Check if the backend disk is ready to accept traffic.
func (fs *FSObjects) IsReady(_ context.Context) bool {
	if _, err := os.Stat(fs.fsPath); err != nil {
		return false
	}

	globalObjLayerMutex.RLock()
	res := globalObjectAPI != nil && !globalSafeMode
	globalObjLayerMutex.RUnlock()

	return res
}
