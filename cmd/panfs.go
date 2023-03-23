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
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/minio/madmin-go"
	"github.com/minio/minio-go/v7/pkg/s3utils"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/minio/minio/internal/color"
	"github.com/minio/minio/internal/config"
	xhttp "github.com/minio/minio/internal/http"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/lock"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/mountinfo"
	"github.com/minio/pkg/bucket/policy"
	"github.com/minio/pkg/env"
	"github.com/minio/pkg/mimedb"

	panconfig "github.com/minio/minio/cmd/panasas/config"
)

const (
	panfsBucketListPrefix     = "buckets"
	panfsObjectsPrefix        = "data"
	panfsConfigAgentNamespace = "s3"
)

// PANdefaultEtag etag is used for pre-existing objects.
var PANdefaultEtag = "00000000000000000000000000000000-2"

const (
	panfsMetaDir        = ".s3"
	objMetadataDir      = "metadata"
	tmpDir              = "tmp"
	panfsS3MultipartDir = panfsMetaDir + SlashSeparator + mpartMetaPrefix
	panfsS3TmpDir       = panfsMetaDir + SlashSeparator + tmpDir
	panfsS3MetadataDir  = panfsMetaDir + SlashSeparator + objMetadataDir
)

// PANFSObjects - Implements panfs object layer.
type PANFSObjects struct {
	GatewayUnsupported

	// Path to be exported over S3 API.
	fsPath string
	// meta json filename, varies by fs / cache backend.
	metaJSONFile string
	// Unique value to be used for all
	// temporary transactions.
	nodeDataSerial string

	// This value shouldn't be touched, once initialized.
	fsFormatRlk *lock.RLockedFile // Is a read lock on `format.json`.

	// FS rw pool.
	rwPool *fsIOPool

	// ListObjects pool management.
	listPool *TreeWalkPool

	diskMount bool

	appendFileMap   map[string]*panfsAppendFile
	appendFileMapMu sync.Mutex

	// To manage the appendRoutine go-routines
	nsMutex *nsLockMap

	tmpDirsCount     uint64
	currentTmpFolder uint64

	configAgent *panconfig.Client
}

// Represents the background append file.
type panfsAppendFile struct {
	sync.Mutex
	parts    []PartInfo // List of parts appended.
	filePath string     // Absolute path of the file in the temp location.
}

// Initializes meta volume on all the fs path.
func initMetaVolumePANFS(fsPath, nodeDataSerial string) error {
	// This happens for the first time, but keep this here since this
	// is the only place where it can be made less expensive
	// optimizing all other calls. Create minio meta volume,
	// if it doesn't exist yet.
	metaBucketPath := pathJoin(fsPath, minioMetaBucket)

	if err := os.MkdirAll(metaBucketPath, 0o777); err != nil {
		return err
	}
	metaTmpPath := pathJoin(fsPath, minioMetaTmpBucket, nodeDataSerial)
	if err := os.MkdirAll(metaTmpPath, 0o777); err != nil {
		return err
	}

	return os.MkdirAll(pathJoin(fsPath, dataUsageBucket), 0o777)
}

// NewPANFSObjectLayer - initialize new panfs object layer.
func NewPANFSObjectLayer(ctx context.Context, fsPath string) (ObjectLayer, error) {
	if fsPath == "" {
		return nil, errInvalidArgument
	}

	var err error
	if fsPath, err = getValidPath(fsPath); err != nil {
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
	nodeDataSerial := env.Get(config.EnvPanUUID, mustGetUUID())
	if _, err := uuid.Parse(nodeDataSerial); err != nil {
		return nil, fmt.Errorf("can't parse uuid provided via env variable \"%s\". Value:\"%s\" Error: %w", config.EnvPanUUID, nodeDataSerial, err)
	}

	// Initialize meta volume, if volume already exists ignores it.
	if err = initMetaVolumePANFS(fsPath, nodeDataSerial); err != nil {
		return nil, err
	}

	// Initialize `format.json`, this function also returns.
	rlk, err := initFormatFS(ctx, fsPath)
	if err != nil {
		return nil, err
	}

	panasasConfigClient := panconfig.NewClient(
		env.Get(config.EnvPanasasConfigAgentURL, ""),
		panfsConfigAgentNamespace,
	)

	tmpDirsCount, err := strconv.ParseUint(env.Get(config.EnvPanTmpDirsCount, "10"), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("can't parse count of tmp directories. Error: %w", err)
	}

	// Initialize fs objects.
	fs := &PANFSObjects{
		fsPath:         fsPath,
		metaJSONFile:   panfsMetaJSONFile,
		nodeDataSerial: nodeDataSerial,
		rwPool: &fsIOPool{
			readersMap: make(map[string]*lock.RLockedFile),
		},
		nsMutex:       newNSLock(false),
		listPool:      NewTreeWalkPool(globalLookupTimeout),
		appendFileMap: make(map[string]*panfsAppendFile),
		diskMount:     mountinfo.IsLikelyMountPoint(fsPath),
		configAgent:   panasasConfigClient,
		tmpDirsCount:  tmpDirsCount,
	}

	// Once the filesystem has initialized hold the read lock for
	// the life time of the server. This is done to ensure that under
	// shared backend mode for FS, remote servers do not migrate
	// or cause changes on backend format.
	fs.fsFormatRlk = rlk

	go fs.cleanupStaleUploads(ctx)
	go intDataUpdateTracker.start(ctx, fsPath)

	// Return successfully initialized object layer.
	return fs, nil
}

// NewNSLock - initialize a new namespace RWLocker instance.
func (fs *PANFSObjects) NewNSLock(bucket string, objects ...string) RWLocker {
	// lockers are explicitly 'nil' for FS mode since there are only local lockers
	return fs.nsMutex.NewNSLock(nil, bucket, objects...)
}

// SetDriveCounts no-op
func (fs *PANFSObjects) SetDriveCounts() []int {
	return nil
}

// Shutdown - should be called when process shuts down.
func (fs *PANFSObjects) Shutdown(ctx context.Context) error {
	fs.fsFormatRlk.Close()

	// Cleanup and delete tmp uuid.
	return fsRemoveAll(ctx, pathJoin(fs.fsPath, minioMetaTmpBucket, fs.nodeDataSerial))
}

// BackendInfo - returns backend information
func (fs *PANFSObjects) BackendInfo() madmin.BackendInfo {
	return madmin.BackendInfo{Type: madmin.FS}
}

// LocalStorageInfo - returns underlying storage statistics.
func (fs *PANFSObjects) LocalStorageInfo(ctx context.Context) (StorageInfo, []error) {
	return fs.StorageInfo(ctx)
}

// StorageInfo - returns underlying storage statistics.
// TODO: update in data usage story
func (fs *PANFSObjects) StorageInfo(ctx context.Context) (StorageInfo, []error) {
	di, err := getDiskInfo(fs.fsPath)
	if err != nil {
		return StorageInfo{}, []error{err}
	}
	storageInfo := StorageInfo{
		Disks: []madmin.Disk{
			{
				State:          madmin.DriveStateOk,
				TotalSpace:     di.Total,
				UsedSpace:      di.Used,
				AvailableSpace: di.Free,
				DrivePath:      fs.fsPath,
			},
		},
	}
	storageInfo.Backend = fs.BackendInfo()
	return storageInfo, nil
}

// NSScanner returns data usage stats of the current FS deployment
func (fs *PANFSObjects) NSScanner(ctx context.Context, bf *bloomFilter, updates chan<- DataUsageInfo, wantCycle uint32, _ madmin.HealScanMode) error {
	defer close(updates)
	// Load bucket totals
	var totalCache dataUsageCache
	err := totalCache.load(ctx, fs, dataUsageCacheName)
	if err != nil {
		return err
	}
	totalCache.Info.Name = dataUsageRoot
	buckets, err := fs.ListBuckets(ctx, BucketOptions{})
	if err != nil {
		return err
	}
	if len(buckets) == 0 {
		totalCache.keepBuckets(buckets)
		updates <- totalCache.dui(dataUsageRoot, buckets)
		return nil
	}
	for i, b := range buckets {
		if isReservedOrInvalidBucket(b.Name, false) {
			// Delete bucket...
			buckets = append(buckets[:i], buckets[i+1:]...)
		}
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
		bCache.Info.NextCycle = wantCycle
		upds := make(chan dataUsageEntry, 1)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for update := range upds {
				totalCache.replace(b.Name, dataUsageRoot, update)
				if intDataUpdateTracker.debug {
					logger.Info(color.Green("NSScanner:")+" Got update: %v", len(totalCache.Cache))
				}
				cloned := totalCache.clone()
				updates <- cloned.dui(dataUsageRoot, buckets)
			}
		}()
		bCache.Info.updates = upds
		cache, err := fs.scanBucket(ctx, b.Name, bCache)
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		logger.LogIf(ctx, err)
		cache.Info.BloomFilter = nil
		wg.Wait()

		if cache.root() == nil {
			if intDataUpdateTracker.debug {
				logger.Info(color.Green("NSScanner:") + " No root added. Adding empty")
			}
			cache.replace(cache.Info.Name, dataUsageRoot, dataUsageEntry{})
		}
		if cache.Info.LastUpdate.After(bCache.Info.LastUpdate) {
			if intDataUpdateTracker.debug {
				logger.Info(color.Green("NSScanner:")+" Saving bucket %q cache with %d entries", b.Name, len(cache.Cache))
			}
			logger.LogIf(ctx, cache.save(ctx, fs, path.Join(b.Name, dataUsageCacheName)))
		}
		// Merge, save and send update.
		// We do it even if unchanged.
		cl := cache.clone()
		entry := cl.flatten(*cl.root())
		totalCache.replace(cl.Info.Name, dataUsageRoot, entry)
		if intDataUpdateTracker.debug {
			logger.Info(color.Green("NSScanner:")+" Saving totals cache with %d entries", len(totalCache.Cache))
		}
		totalCache.Info.LastUpdate = time.Now()
		logger.LogIf(ctx, totalCache.save(ctx, fs, dataUsageCacheName))
		cloned := totalCache.clone()
		updates <- cloned.dui(dataUsageRoot, buckets)

	}

	return nil
}

// scanBucket scans a single bucket in FS mode.
// The updated cache for the bucket is returned.
// A partially updated bucket may be returned.
func (fs *PANFSObjects) scanBucket(ctx context.Context, bucket string, cache dataUsageCache) (dataUsageCache, error) {
	defer close(cache.Info.updates)
	defer globalScannerMetrics.log(scannerMetricScanBucketDisk, fs.fsPath, bucket)()

	// Get bucket policy
	// Check if the current bucket has a configured lifecycle policy
	lc, err := globalLifecycleSys.Get(bucket)
	if err == nil && lc.HasActiveRules("", true) {
		if intDataUpdateTracker.debug {
			logger.Info(color.Green("scanBucket:") + " lifecycle: Active rules found")
		}
		cache.Info.lifeCycle = lc
	}

	// Load bucket info.
	cache, err = scanDataFolder(ctx, -1, -1, fs.fsPath, cache, func(item scannerItem) (sizeSummary, error) {
		bucket, object := item.bucket, item.objectPath()
		stopFn := globalScannerMetrics.log(scannerMetricScanObject, fs.fsPath, PathJoin(item.bucket, item.objectPath()))
		defer stopFn()

		var fsMetaBytes []byte
		done := globalScannerMetrics.timeSize(scannerMetricReadMetadata)
		defer func() {
			if done != nil {
				done(len(fsMetaBytes))
			}
		}()
		bucketPath, err := fs.getBucketPanFSPath(ctx, bucket)
		if err != nil {
			return sizeSummary{}, errSkipFile
		}

		fsMetaBytes, err = xioutil.ReadFile(fs.getObjectMetadataPath(bucketPath, object))
		if err != nil && !osIsNotExist(err) {
			if intDataUpdateTracker.debug {
				logger.Info(color.Green("scanBucket:")+" object return unexpected error: %v/%v: %w", item.bucket, item.objectPath(), err)
			}
			return sizeSummary{}, errSkipFile
		}

		fsMeta := newPANFSMeta()
		metaOk := false
		if len(fsMetaBytes) > 0 {
			json := jsoniter.ConfigCompatibleWithStandardLibrary
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
			if intDataUpdateTracker.debug {
				logger.Info(color.Green("scanBucket:")+" object path missing: %v: %w", item.Path, fiErr)
			}
			return sizeSummary{}, errSkipFile
		}
		done(len(fsMetaBytes))
		done = nil

		// FS has no "all versions". Increment the counter, though
		globalScannerMetrics.incNoTime(scannerMetricApplyAll)

		oi := fsMeta.ToObjectInfo(bucket, object, fi)
		doneVer := globalScannerMetrics.time(scannerMetricApplyVersion)
		sz := item.applyActions(ctx, fs, oi, &sizeSummary{})
		doneVer()
		if sz >= 0 {
			return sizeSummary{totalSize: sz, versions: 1}, nil
		}

		return sizeSummary{totalSize: fi.Size(), versions: 1}, nil
	}, 0)

	return cache, err
}

// Bucket operations

// loadBucketMetadata loads bucket metadata from the disk.
// Returns an error when the bucket metadata file not found
func (fs *PANFSObjects) loadBucketMetadata(ctx context.Context, bucket string) (BucketMetadata, error) {
	b := newBucketMetadata(bucket)
	err := b.Load(ctx, fs, b.Name)
	if err != nil {
		if errors.Is(err, errConfigNotFound) {
			return b, BucketNotFound{Bucket: bucket}
		}
		return b, err
	}
	b.defaultTimestamps()

	// migrate unencrypted remote targets
	if err := b.migrateTargetConfig(ctx, fs); err != nil {
		return b, err
	}

	return b, nil
}

// getObjectMetadataPath returns path to the object metadata based on bucket path and object name
func (fs *PANFSObjects) getObjectMetadataPath(bucketDir, object string) string {
	return pathJoin(bucketDir, panfsS3MetadataDir, object)
}

// getBucketPanFSPath returns path to the bucket.
func (fs *PANFSObjects) getBucketPanFSPath(ctx context.Context, bucket string) (path string, err error) {
	if bucket != minioMetaBucket {
		// Trim trailing slash if exists
		bucket = strings.Trim(bucket, slashSeparator)
		meta, err := fs.loadBucketMetadata(ctx, bucket)
		if err != nil {
			return "", err
		}
		path = pathJoin(meta.PanFSPath, bucket)
	} else {
		path = pathJoin(fs.fsPath, bucket)
	}
	return path, nil
}

func (fs *PANFSObjects) statPanFSBucketDir(ctx context.Context, bucket string) (os.FileInfo, error) {
	bucketDir, err := fs.getBucketPanFSPath(ctx, bucket)
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
func (fs *PANFSObjects) MakeBucketWithLocation(ctx context.Context, bucket string, opts MakeBucketOptions) error {
	if opts.LockEnabled || opts.VersioningEnabled {
		return NotImplemented{}
	}

	// Do not allow to create bucket with .s3 name
	if err := dotS3PrefixCheck(bucket); err != nil {
		return err
	}

	// Verify if bucket is valid.
	if s3utils.CheckValidBucketNameStrict(bucket) != nil {
		return BucketNameInvalid{Bucket: bucket}
	}

	defer NSUpdated(bucket, slashSeparator)

	// TODO: if bucket is not minio.sys do not create .s3
	bucketMetaDir := pathJoin(opts.PanFSBucketPath, bucket, panfsMetaDir)
	for _, dir := range []string{
		pathJoin(opts.PanFSBucketPath, bucket),
		bucketMetaDir,
		pathJoin(bucketMetaDir, objMetadataDir),
		pathJoin(bucketMetaDir, tmpDir),
		pathJoin(bucketMetaDir, mpartMetaPrefix),
	} {
		if err := fsMkdir(ctx, dir); err != nil {
			return toObjectErr(err, bucket)
		}
	}

	meta := newBucketMetadata(bucket)
	meta.PanFSPath = opts.PanFSBucketPath

	if err := meta.Save(ctx, fs); err != nil {
		return toObjectErr(err, bucket)
	}

	if fs.configAgent != nil {
		if err := fs.configAgent.PutObject(pathJoin(panfsBucketListPrefix, bucket), nil); err != nil {
			return err
		}
	}

	globalBucketMetadataSys.Set(bucket, meta)

	return nil
}

// GetBucketPolicy - only needed for FS in NAS mode
func (fs *PANFSObjects) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
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
func (fs *PANFSObjects) SetBucketPolicy(ctx context.Context, bucket string, p *policy.Policy) error {
	meta, err := loadBucketMetadata(ctx, fs, bucket)
	if err != nil {
		return err
	}

	json := jsoniter.ConfigCompatibleWithStandardLibrary
	configData, err := json.Marshal(p)
	if err != nil {
		return err
	}
	meta.PolicyConfigJSON = configData

	return meta.Save(ctx, fs)
}

// DeleteBucketPolicy - only needed for FS in NAS mode
func (fs *PANFSObjects) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	meta, err := loadBucketMetadata(ctx, fs, bucket)
	if err != nil {
		return err
	}
	meta.PolicyConfigJSON = nil
	return meta.Save(ctx, fs)
}

// GetBucketInfo - fetch bucket metadata info.
func (fs *PANFSObjects) GetBucketInfo(ctx context.Context, bucket string, opts BucketOptions) (bi BucketInfo, e error) {
	// We still have global metabucket here so we need to know whether the target bucket is minio metabucket or not.
	// There are several calls of GetBucketInfo with `.minio.sys` bucket at the initialization time.
	// See: listIAMConfigItems.go:listIAMConfigItems
	var st os.FileInfo
	st, err := fs.statPanFSBucketDir(ctx, bucket)
	if err != nil {
		return bi, toObjectErr(err, bucket)
	}

	createdTime := st.ModTime()
	meta, err := loadBucketMetadata(ctx, fs, bucket)
	if err == nil {
		createdTime = meta.Created
	}

	bi.Name = bucket
	bi.Created = createdTime
	bi.PanFSPath = meta.PanFSPath

	return
}

// ListBuckets - list all s3 compatible buckets (directories) at fsPath.
func (fs *PANFSObjects) ListBuckets(ctx context.Context, opts BucketOptions) ([]BucketInfo, error) {
	if err := checkPathLength(fs.fsPath); err != nil {
		logger.LogIf(ctx, err)
		return nil, err
	}

	var entries []string
	var err error

	if fs.configAgent != nil {
		// We need "buckets/" here, not "buckets" to list all the
		// objects included in the "buckets" directory but ignore a
		// directory/object whose name would begin with "buckets".
		entries, err = fs.configAgent.GetObjectsList(panfsBucketListPrefix + SlashSeparator)

		if err == nil {
			// The entries will have a prefix. In order to get the
			// names of the buffers, we need to remove the prefixes.
			for idx, entry := range entries {
				entries[idx] = strings.TrimPrefix(entry, panfsBucketListPrefix+SlashSeparator)
			}
		}
	} else {
		// Read bucket list from folder where theirs metadata are stored
		entries, err = readDirWithOpts(pathJoin(fs.fsPath, minioMetaBucket, bucketMetaPrefix), readDirOpts{count: -1, followDirSymlink: true})
	}

	if err != nil {
		logger.LogIf(ctx, errDiskNotFound)
		return nil, toObjectErr(errDiskNotFound)
	}

	bucketInfos := make([]BucketInfo, 0, len(entries))
	for _, entry := range entries {
		// Ignore all reserved bucket names and invalid bucket names.
		if isReservedOrInvalidBucket(entry, false) {
			continue
		}
		var fi os.FileInfo

		bucketDir, err := fs.getBucketPanFSPath(ctx, entry)
		if err != nil {
			continue
		}
		fi, err = fsStatVolume(ctx, bucketDir)
		// There seems like no practical reason to check for errors
		// at this point, if there are indeed errors we can simply
		// just ignore such buckets and list only those which
		// return proper Stat information instead.
		if err != nil {
			// Ignore any errors returned here.
			continue
		}
		created := fi.ModTime()
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
	sort.Slice(bucketInfos, func(i, j int) bool {
		return bucketInfos[i].Name < bucketInfos[j].Name
	})

	// Succes.
	return bucketInfos, nil
}

// DeleteBucket - delete a bucket and all the metadata associated
// with the bucket including pending multipart, object metadata.
// TODO: there is no need to delete user data when deleting bucket.
func (fs *PANFSObjects) DeleteBucket(ctx context.Context, bucket string, opts DeleteBucketOptions) error {
	defer NSUpdated(bucket, slashSeparator)

	if err := dotS3PrefixCheck(bucket); err != nil {
		return err
	}

	bucketDir, err := fs.getBucketPanFSPath(ctx, bucket)
	if err != nil {
		return toObjectErr(err, bucket)
	}

	// TODO: delete bucket operation should preserve user objects on the realm
	if !opts.Force {
		// Attempt to delete regular bucket.
		if err = removePanFSBucketDir(ctx, bucketDir); err != nil {
			return toObjectErr(err, bucket)
		}
	} else {
		// Still using here .minio.sys as temp dir for delete bucket
		tmpBucketPath := pathJoin(fs.fsPath, minioMetaTmpBucket, bucket+"."+mustGetUUID())
		if err = Rename(bucketDir, tmpBucketPath); err != nil {
			return toObjectErr(err, bucket)
		}

		go func() {
			fsRemoveAll(ctx, tmpBucketPath) // ignore returned error if any.
		}()
	}

	// Delete all bucket metadata.
	deleteBucketMetadata(ctx, fs, bucket)

	if fs.configAgent != nil {
		noLockID := ""
		err = fs.configAgent.DeleteObject(
			pathJoin(panfsBucketListPrefix, bucket),
			noLockID,
		)
		if err != nil {
			return toObjectErr(err, bucket)
		}
	}

	return nil
}

// Object Operations

// CopyObject - copy object source object to destination object.
// if source object and destination object are same we only
// update metadata.
func (fs *PANFSObjects) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (oi ObjectInfo, err error) {
	if srcOpts.VersionID != "" && srcOpts.VersionID != nullVersionID {
		return oi, VersionNotFound{
			Bucket:    srcBucket,
			Object:    srcObject,
			VersionID: srcOpts.VersionID,
		}
	}

	if err = dotS3PrefixCheck(srcBucket, dstBucket, srcObject, dstObject); err != nil {
		return oi, err
	}

	cpSrcDstSame := isStringEqual(pathJoin(srcBucket, srcObject), pathJoin(dstBucket, dstObject))
	defer NSUpdated(dstBucket, dstObject)

	if !cpSrcDstSame {
		objectDWLock := fs.NewNSLock(dstBucket, dstObject)
		lkctx, err := objectDWLock.GetLock(ctx, globalOperationTimeout)
		if err != nil {
			return oi, err
		}
		ctx = lkctx.Context()
		defer objectDWLock.Unlock(lkctx.Cancel)
	}

	if _, err := fs.statPanFSBucketDir(ctx, srcBucket); err != nil {
		return oi, toObjectErr(err, srcBucket)
	}

	if cpSrcDstSame && srcInfo.metadataOnly {
		bucketDir, err := fs.getBucketPanFSPath(ctx, srcBucket)
		if err != nil {
			return oi, err
		}
		fsMetaPath := fs.getObjectMetadataPath(bucketDir, srcObject)
		wlk, err := fs.rwPool.Write(fsMetaPath)
		if err != nil {
			wlk, err = fs.rwPool.Create(fsMetaPath)
			if err != nil {
				logger.LogIf(ctx, err)
				return oi, toObjectErr(err, srcBucket, srcObject)
			}
		}
		// This close will allow for locks to be synchronized on `fs.json`.
		defer wlk.Close()

		// Save objects' metadata in `fs.json`.
		fsMeta := newPANFSMeta()
		if _, err = fsMeta.ReadFrom(ctx, wlk); err != nil {
			// For any error to read fsMeta, set default ETag and proceed.
			fsMeta = fs.defaultFsJSON(srcObject)
		}

		fsMeta.Meta = cloneMSS(srcInfo.UserDefined)
		fsMeta.Meta["etag"] = srcInfo.ETag
		if _, err = fsMeta.WriteTo(wlk); err != nil {
			return oi, toObjectErr(err, srcBucket, srcObject)
		}

		fsObjectPath := pathJoin(bucketDir, srcObject)

		// Update object modtime
		err = fsTouch(ctx, fsObjectPath)
		if err != nil {
			return oi, toObjectErr(err, srcBucket, srcObject)
		}
		// Stat the file to get object info
		fi, err := fsStatFile(ctx, fsObjectPath)
		if err != nil {
			return oi, toObjectErr(err, srcBucket, srcObject)
		}

		// Return the new object info.
		return fsMeta.ToObjectInfo(srcBucket, srcObject, fi), nil
	}

	if err := checkPutObjectArgs(ctx, dstBucket, dstObject, fs); err != nil {
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
func (fs *PANFSObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (gr *GetObjectReader, err error) {
	if opts.VersionID != "" && opts.VersionID != nullVersionID {
		return nil, VersionNotFound{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
		}
	}

	if err = dotS3PrefixCheck(bucket, object); err != nil {
		return
	}

	if err = checkGetObjArgs(ctx, bucket, object); err != nil {
		return nil, err
	}

	nsUnlocker := func() {}

	if lockType != noLock {
		// Lock the object before reading.
		lock := fs.NewNSLock(bucket, object)
		switch lockType {
		case writeLock:
			lkctx, err := lock.GetLock(ctx, globalOperationTimeout)
			if err != nil {
				return nil, err
			}
			ctx = lkctx.Context()
			nsUnlocker = func() { lock.Unlock(lkctx.Cancel) }
		case readLock:
			lkctx, err := lock.GetRLock(ctx, globalOperationTimeout)
			if err != nil {
				return nil, err
			}
			ctx = lkctx.Context()
			nsUnlocker = func() { lock.RUnlock(lkctx.Cancel) }
		}
	}

	// Otherwise we get the object info
	var objInfo ObjectInfo
	if objInfo, err = fs.getObjectInfo(ctx, bucket, object); err != nil {
		nsUnlocker()
		return nil, toObjectErr(err, bucket, object)
	}
	// For a directory, we need to return a reader that returns no bytes.
	if HasSuffix(object, SlashSeparator) {
		// The lock taken above is released when
		// objReader.Close() is called by the caller.
		return NewGetObjectReaderFromReader(bytes.NewBuffer(nil), objInfo, opts, nsUnlocker)
	}

	bucketDir, err := fs.getBucketPanFSPath(ctx, bucket)
	if err != nil {
		return nil, err
	}
	// Take a rwPool lock for NFS gateway type deployment
	rwPoolUnlocker := func() {}
	if bucket != minioMetaBucket && lockType != noLock {
		fsMetaPath := fs.getObjectMetadataPath(bucketDir, object)
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

	objReaderFn, off, length, err := NewGetObjectReader(rs, objInfo, opts)
	if err != nil {
		rwPoolUnlocker()
		nsUnlocker()
		return nil, err
	}

	var readCloser io.ReadCloser
	var size int64
	if fs.isConfigAgentObject(bucket, object) {
		objectName := pathJoin(panfsObjectsPrefix, bucket, object)
		readCloser, objectInfo, err := fs.configAgent.GetObject(objectName)
		if err == nil {
			size = objectInfo.Size()
			io.Copy(io.Discard, io.LimitReader(readCloser, off))
		}
	} else {
		// Read the object, doesn't exist returns an s3 compatible error.
		fsObjPath := pathJoin(bucketDir, object)
		readCloser, size, err = fsOpenFile(ctx, fsObjPath, off)
	}

	if err != nil {
		rwPoolUnlocker()
		nsUnlocker()
		return nil, toObjectErr(err, bucket, object)
	}

	closeFn := func() {
		readCloser.Close()
	}
	reader := io.LimitReader(readCloser, length)

	// Check if range is valid
	if off > size || off+length > size {
		err = InvalidRange{off, length, size}
		logger.LogIf(ctx, err, logger.Application)
		closeFn()
		rwPoolUnlocker()
		nsUnlocker()
		return nil, err
	}

	return objReaderFn(reader, h, closeFn, rwPoolUnlocker, nsUnlocker)
}

// Create a new fs.json file, if the existing one is corrupt. Should happen very rarely.
func (fs *PANFSObjects) createFsJSON(object, fsMetaPath string) error {
	fsMeta := newPANFSMeta()
	fsMeta.Meta = map[string]string{
		"etag":         GenETag(),
		"content-type": mimedb.TypeByExtension(path.Ext(object)),
	}
	wlk, werr := fs.rwPool.Create(fsMetaPath)
	if werr == nil {
		_, err := fsMeta.WriteTo(wlk)
		wlk.Close()
		return err
	}
	return werr
}

// Used to return default etag values when a pre-existing object's meta data is queried.
func (fs *PANFSObjects) defaultFsJSON(object string) panfsMeta {
	fsMeta := newPANFSMeta()
	fsMeta.Meta = map[string]string{
		"etag":         PANdefaultEtag,
		"content-type": mimedb.TypeByExtension(path.Ext(object)),
	}
	return fsMeta
}

func (fs *PANFSObjects) getObjectInfoNoFSLock(ctx context.Context, bucket, object string) (oi ObjectInfo, e error) {
	fsMeta := panfsMeta{}

	bucketDir, err := fs.getBucketPanFSPath(ctx, bucket)
	if err != nil {
		return oi, err
	}
	if HasSuffix(object, SlashSeparator) {
		fi, err := fsStatDir(ctx, pathJoin(bucketDir, object))
		if err != nil {
			return oi, err
		}
		return fsMeta.ToObjectInfo(bucket, object, fi), nil
	}

	if !globalCLIContext.StrictS3Compat {
		// Stat the file to get file size.
		fi, err := fsStatFile(ctx, pathJoin(bucketDir, object))
		if err != nil {
			return oi, err
		}
		return fsMeta.ToObjectInfo(bucket, object, fi), nil
	}

	fsMetaPath := fs.getObjectMetadataPath(bucketDir, object)
	// Read `fs.json` to perhaps contend with
	// parallel Put() operations.

	rc, _, err := fsOpenFile(ctx, fsMetaPath, 0)
	if err == nil {
		fsMetaBuf, rerr := ioutil.ReadAll(rc)
		rc.Close()
		if rerr == nil {
			json := jsoniter.ConfigCompatibleWithStandardLibrary
			if rerr = json.Unmarshal(fsMetaBuf, &fsMeta); rerr != nil {
				// For any error to read fsMeta, set default ETag and proceed.
				fsMeta = fs.defaultFsJSON(object)
			}
		} else {
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
	var fi os.FileInfo
	if fs.isConfigAgentObject(bucket, object) {
		fi, err = fs.configAgent.GetObjectInfo(pathJoin(panfsObjectsPrefix, bucket, object))
	} else {
		fi, err = fsStatFile(ctx, pathJoin(bucketDir, object))
	}
	if err != nil {
		return oi, err
	}

	return fsMeta.ToObjectInfo(bucket, object, fi), nil
}

// getObjectInfo - wrapper for reading object metadata and constructs ObjectInfo.
func (fs *PANFSObjects) getObjectInfo(ctx context.Context, bucket, object string) (oi ObjectInfo, e error) {
	bucketDir, err := fs.getBucketPanFSPath(ctx, bucket)
	if err != nil {
		return oi, err
	}
	if strings.HasSuffix(object, SlashSeparator) && !fs.isObjectDir(bucketDir, object) {
		return oi, errFileNotFound
	}

	fsMeta := panfsMeta{}
	if HasSuffix(object, SlashSeparator) {
		fi, err := fsStatDir(ctx, pathJoin(bucketDir, object))
		if err != nil {
			return oi, err
		}
		return fsMeta.ToObjectInfo(bucket, object, fi), nil
	}
	fsMetaPath := fs.getObjectMetadataPath(bucketDir, object)
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
	var fi os.FileInfo
	if fs.isConfigAgentObject(bucket, object) {
		fi, err = fs.configAgent.GetObjectInfo(pathJoin(panfsObjectsPrefix, bucket, object))
	} else {
		fi, err = fsStatFile(ctx, pathJoin(bucketDir, object))
	}
	if err != nil {
		return oi, err
	}

	return fsMeta.ToObjectInfo(bucket, object, fi), nil
}

// getObjectInfoWithLock - reads object metadata and replies back ObjectInfo.
func (fs *PANFSObjects) getObjectInfoWithLock(ctx context.Context, bucket, object string) (oi ObjectInfo, err error) {
	// Lock the object before reading.
	lk := fs.NewNSLock(bucket, object)
	lkctx, err := lk.GetRLock(ctx, globalOperationTimeout)
	if err != nil {
		return oi, err
	}
	ctx = lkctx.Context()
	defer lk.RUnlock(lkctx.Cancel)

	if err := checkGetObjArgs(ctx, bucket, object); err != nil {
		return oi, err
	}

	bucketDir, err := fs.getBucketPanFSPath(ctx, bucket)
	if err != nil {
		return oi, err
	}

	if strings.HasSuffix(object, SlashSeparator) && !fs.isObjectDir(bucketDir, object) {
		return oi, errFileNotFound
	}

	return fs.getObjectInfo(ctx, bucket, object)
}

// GetObjectInfo - reads object metadata and replies back ObjectInfo.
func (fs *PANFSObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (oi ObjectInfo, err error) {
	if opts.VersionID != "" && opts.VersionID != nullVersionID {
		return oi, VersionNotFound{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
		}
	}

	if err = dotS3PrefixCheck(bucket, object); err != nil {
		return oi, err
	}

	oi, err = fs.getObjectInfoWithLock(ctx, bucket, object)
	if err == errCorruptedFormat || err == io.EOF {
		lk := fs.NewNSLock(bucket, object)
		lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
		if err != nil {
			return oi, toObjectErr(err, bucket, object)
		}

		bucketDir, err := fs.getBucketPanFSPath(ctx, bucket)
		if err != nil {
			return oi, err
		}
		fsMetaPath := fs.getObjectMetadataPath(bucketDir, object)
		err = fs.createFsJSON(object, fsMetaPath)
		lk.Unlock(lkctx.Cancel)
		if err != nil {
			return oi, toObjectErr(err, bucket, object)
		}

		oi, err = fs.getObjectInfoWithLock(ctx, bucket, object)
		return oi, toObjectErr(err, bucket, object)
	}
	return oi, toObjectErr(err, bucket, object)
}

// PutObject - creates an object upon reading from the input stream
// until EOF, writes data directly to configured filesystem path.
// Additionally writes `fs.json` which carries the necessary metadata
// for future object operations.
func (fs *PANFSObjects) PutObject(ctx context.Context, bucket string, object string, r *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	if opts.Versioned {
		return objInfo, NotImplemented{}
	}

	if err := dotS3PrefixCheck(bucket, object); err != nil {
		return objInfo, err
	}

	if err := checkPutObjectArgs(ctx, bucket, object, fs); err != nil {
		return ObjectInfo{}, err
	}

	defer NSUpdated(bucket, object)

	// Lock the object.
	lk := fs.NewNSLock(bucket, object)
	lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		logger.LogIf(ctx, err)
		return objInfo, err
	}
	ctx = lkctx.Context()
	defer lk.Unlock(lkctx.Cancel)

	return fs.putObject(ctx, bucket, object, r, opts)
}

// putObject - wrapper for PutObject
// This function must be refactored when panasas configuration agent will be fully implemented. Configuration
// agent moves all configs (IAM, users, groups etc) to the Blob DB instead of storing them on the fs. So the final
// version of that function should never handle metabucket operations (bucket config, user creation etc). At the moment
// configs will be stored on the global minio metabucket but the object metadata is moved to the new location - relative
// to the bucket directory
func (fs *PANFSObjects) putObject(ctx context.Context, bucket string, object string, r *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, retErr error) {
	data := r.Reader

	// No metadata is set, allocate a new one.
	meta := cloneMSS(opts.UserDefined)
	var err error

	bucketDir, err := fs.getBucketPanFSPath(ctx, bucket)
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket)
	}

	// Validate if bucket name is valid and exists.
	if _, err = fsStatVolume(ctx, bucketDir); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket)
	}

	fsMeta := newPANFSMeta()
	fsMeta.Meta = meta

	// This is a special case with size as '0' and object ends
	// with a slash separator, we treat it like a valid operation
	// and return success.
	if isObjectDir(pathJoin(bucketDir, object), data.Size()) {
		if err = mkdirAll(pathJoin(bucketDir, object), 0o777); err != nil {
			logger.LogIf(ctx, err)
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
		var fi os.FileInfo
		if fi, err = fsStatDir(ctx, pathJoin(bucketDir, object)); err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
		return fsMeta.ToObjectInfo(bucket, object, fi), nil
	}

	// Validate input data size and it can never be less than zero.
	if data.Size() < -1 {
		logger.LogIf(ctx, errInvalidArgument, logger.Application)
		return ObjectInfo{}, errInvalidArgument
	}

	tempDir := fs.getTempDir(bucketDir)
	var wlk *lock.LockedFile
	if bucket != minioMetaBucket {
		objectMetaPath := fs.getObjectMetadataPath(bucketDir, object)
		wlk, err = fs.rwPool.Write(objectMetaPath)
		var freshFile bool
		if err != nil {
			wlk, err = fs.rwPool.Create(objectMetaPath)
			if err != nil {
				logger.LogIf(ctx, err)
				return ObjectInfo{}, toObjectErr(err, bucket, object)
			}
			freshFile = true
		}
		// This close will allow for locks to be synchronized on `fs.json`.
		defer wlk.Close()
		defer func() {
			// Remove meta file when PutObject encounters
			// any error and it is a fresh file.
			// We should preserve the meta file of any
			// existing object
			if retErr != nil && freshFile {
				fsRemoveFile(ctx, objectMetaPath)
			}
		}()
	}

	// Uploaded object will first be written to the temporary location which will eventually
	// be renamed to the actual location. It is first written to the temporary location
	// so that cleaning it up will be easy if the server goes down.
	tempObj := mustGetUUID()

	var fsNSObjPath string

	if fs.isConfigAgentObject(bucket, object) {
		fsNSObjPath = pathJoin(panfsObjectsPrefix, bucket, object)
		retErr = fs.configAgent.PutObject(fsNSObjPath, data)
		if retErr != nil {
			return ObjectInfo{}, retErr
		}
	} else {
		fsTmpObjPath := pathJoin(tempDir, tempObj)
		bytesWritten, err := fsCreateFile(ctx, fsTmpObjPath, data, data.Size())

		// Delete the temporary object in the case of a
		// failure. If PutObject succeeds, then there would be
		// nothing to delete.
		defer fsRemoveFile(ctx, fsTmpObjPath)

		if err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
		fsMeta.Meta["etag"] = r.MD5CurrentHexString()

		// Should return IncompleteBody{} error when reader has fewer
		// bytes than specified in request header.
		if bytesWritten < data.Size() {
			return ObjectInfo{}, IncompleteBody{Bucket: bucket, Object: object}
		}

		// Entire object was written to the temp location, now it's safe to rename it to the actual location.
		fsNSObjPath = pathJoin(bucketDir, object)
		if err = panfsRenameFile(ctx, fsTmpObjPath, fsNSObjPath); err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
	}

	if bucket != minioMetaBucket {
		// Write FS metadata after a successful namespace operation.
		if _, err = fsMeta.WriteTo(wlk); err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
	}

	// Stat the file to fetch timestamp, size.
	var fi os.FileInfo
	if fs.isConfigAgentObject(bucket, object) {
		fi, err = fs.configAgent.GetObjectInfo(pathJoin(panfsObjectsPrefix, bucket, object))
	} else {
		fi, err = fsStatFile(ctx, fsNSObjPath)
	}

	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}
	// Success.
	return fsMeta.ToObjectInfo(bucket, object, fi), nil
}

// DeleteObjects - deletes an object from a bucket, this operation is destructive
// and there are no rollbacks supported.
func (fs *PANFSObjects) DeleteObjects(ctx context.Context, bucket string, objects []ObjectToDelete, opts ObjectOptions) ([]DeletedObject, []error) {
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
func (fs *PANFSObjects) DeleteObject(ctx context.Context, bucket, object string, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	if opts.VersionID != "" && opts.VersionID != nullVersionID {
		return objInfo, VersionNotFound{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
		}
	}

	if err = dotS3PrefixCheck(bucket, object); err != nil {
		return objInfo, err
	}

	defer NSUpdated(bucket, object)

	// Acquire a write lock before deleting the object.
	lk := fs.NewNSLock(bucket, object)
	lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return objInfo, err
	}
	ctx = lkctx.Context()
	defer lk.Unlock(lkctx.Cancel)

	if err = checkDelObjArgs(ctx, bucket, object); err != nil {
		return objInfo, err
	}

	bucketDir, err := fs.getBucketPanFSPath(ctx, bucket)
	if err != nil {
		return objInfo, err
	}

	if _, err = fsStatVolume(ctx, bucketDir); err != nil {
		return objInfo, toObjectErr(err, bucket)
	}

	var rwlk *lock.LockedFile

	if err != nil {
		return objInfo, toObjectErr(err, bucket)
	}
	fsMetaPath := fs.getObjectMetadataPath(bucketDir, object)
	if bucket != minioMetaBucket {
		rwlk, err = fs.rwPool.Write(fsMetaPath)
		if err != nil && err != errFileNotFound {
			logger.LogIf(ctx, err)
			return objInfo, toObjectErr(err, bucket, object)
		}
	}

	// Delete the object.
	if fs.isConfigAgentObject(bucket, object) {
		objectName := pathJoin(panfsObjectsPrefix, bucket, object)
		noLock := ""
		err = fs.configAgent.DeleteObject(objectName, noLock)
	} else {
		err = fsDeleteFile(ctx, pathJoin(bucketDir), pathJoin(bucketDir, object))
	}

	// Close fsMetaPath before returning due to error or fsMetaPath
	// deletion.
	if rwlk != nil {
		rwlk.Close()
	}

	if err != nil {
		return objInfo, toObjectErr(err, bucket, object)
	}

	if bucket != minioMetaBucket {
		// Delete the metadata object.
		err = fsRemoveFile(ctx, fsMetaPath)
		if err != nil && err != errFileNotFound {
			return objInfo, toObjectErr(err, bucket, object)
		}
	}
	return ObjectInfo{Bucket: bucket, Name: object}, nil
}

func (fs *PANFSObjects) isLeafDir(bucket string, leafPath string) bool {
	return fs.isObjectDir(bucket, leafPath)
}

func (fs *PANFSObjects) isLeaf(bucket string, leafPath string) bool {
	return !strings.HasSuffix(leafPath, slashSeparator)
}

// filterImmediateDirectoryContents leaves only the entries which are included
// in directory (excluding its subdirectories).
// Assumption: all entries are prefixed with directory name.
func filterImmediateDirectoryContents(entries []string, directory string) []string {
	if len(entries) == 0 {
		return entries
	}

	result := entries[:0]

	directoryLength := len(directory)

	var needSlashPrefix bool
	var entryNameOffset int
	if strings.HasSuffix(directory, slashSeparator) {
		needSlashPrefix = false
		entryNameOffset = directoryLength
	} else {
		needSlashPrefix = true
		entryNameOffset = directoryLength + 1
	}

	subdirectories := map[string]bool{}

	for _, entry := range entries {
		if needSlashPrefix && !strings.HasPrefix(entry[directoryLength:], slashSeparator) {
			continue
		}

		relativeName := entry[entryNameOffset:]
		slashIdx := strings.Index(relativeName, slashSeparator)
		if slashIdx < 0 {
			// OK - no "/" found - this entry is not in a subdirectory.
		} else if slashIdx == len(relativeName)-1 {
			// OK - "/" found but is the suffix of the entry.
		} else {
			// Skip the entry - it is in a subdirectory.
			dirName := relativeName[:slashIdx+1]
			subdirectories[dirName] = true
			continue
		}

		result = append(result, relativeName)
	}
	for subDir := range subdirectories {
		result = append(result, subDir)
	}
	return result
}

// Returns function "listDir" of the type listDirFunc.
// isLeaf - is used by listDir function to check if an entry
// is a leaf or non-leaf entry.
func (fs *PANFSObjects) listDirFactory() ListDirFunc {
	// listDir - lists all the entries at a given prefix and given entry in the prefix.
	listDir := func(bucket, prefixDir, prefixEntry string) (emptyDir bool, entries []string, delayIsLeaf bool) {
		var err error

		bucketDir, err := fs.getBucketPanFSPath(context.Background(), bucket)
		if err != nil {
			return false, nil, false
		}
		entries, err = readDir(pathJoin(bucketDir, prefixDir))
		if err != nil && err != errFileNotFound {
			logger.LogIf(GlobalContext, err)
			return false, nil, false
		}
		var agentEntries []string
		if fs.configAgent != nil {
			prefix := pathJoin(panfsObjectsPrefix, bucket, prefixDir)
			agentEntries, err = fs.configAgent.GetObjectsList(prefix)
			if err != nil {
				return false, nil, false
			}
			agentEntries = filterImmediateDirectoryContents(agentEntries, prefix)
			if len(agentEntries) > 0 {
				entries = append(entries, agentEntries...)
			}
		}

		if len(entries) == 0 {
			return true, nil, false
		}
		entries = fs.filterOutPanFSS3Dir(entries)
		entries, delayIsLeaf = filterListEntries(bucket, prefixDir, entries, prefixEntry, fs.isLeaf)
		return false, entries, delayIsLeaf
	}

	// Return list factory instance.
	return listDir
}

// filterPanFSS3Dir removes entries with .s3 prefix from the result
func (fs *PANFSObjects) filterOutPanFSS3Dir(entries []string) (filtered []string) {
	filtered = entries[:0]
	for _, entry := range entries {
		if HasPrefix(entry, ".s3") {
			continue
		}
		filtered = append(filtered, entry)
	}
	return
}

// isObjectDir returns true if the specified bucket & prefix exists
// and the prefix represents an empty directory. An S3 empty directory
// is also an empty directory in the PanFS backend.
func (fs *PANFSObjects) isObjectDir(bucketDir, prefix string) bool {
	if fs.configAgent != nil {
		entries, err := fs.configAgent.GetObjectsList(pathJoin(panfsObjectsPrefix, bucketDir, prefix, slashSeparator))
		return err != nil && len(entries) == 0
	}
	entries, err := readDirN(pathJoin(bucketDir, prefix), 1)
	if err != nil {
		return false
	}
	return len(entries) == 0
}

// ListObjectVersions not implemented for FS mode.
func (fs *PANFSObjects) ListObjectVersions(ctx context.Context, bucket, prefix, marker, versionMarker, delimiter string, maxKeys int) (loi ListObjectVersionsInfo, e error) {
	return loi, NotImplemented{}
}

// ListObjects - list all objects at prefix upto maxKeys., optionally delimited by '/'. Maintains the list pool
// state for future re-entrant list requests.
func (fs *PANFSObjects) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi ListObjectsInfo, err error) {
	// listObjects may in rare cases not be able to find any valid results.
	// Therefore, it cannot set a NextMarker.
	// In that case we retry the operation, but we add a
	// max limit, so we never end up in an infinite loop.

	if err = dotS3PrefixCheck(bucket, prefix); err != nil {
		return loi, err
	}

	tries := 50
	for {
		loi, err = listObjects(ctx, fs, bucket, prefix, marker, delimiter, maxKeys, fs.listPool,
			fs.listDirFactory(), fs.isLeaf, fs.isLeafDir, fs.getObjectInfoNoFSLock, fs.getObjectInfoNoFSLock)
		if err != nil {
			return loi, err
		}
		if !loi.IsTruncated || loi.NextMarker != "" || tries == 0 {
			return loi, nil
		}
		tries--
	}
}

// GetObjectTags - get object tags from an existing object
func (fs *PANFSObjects) GetObjectTags(ctx context.Context, bucket, object string, opts ObjectOptions) (*tags.Tags, error) {
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
func (fs *PANFSObjects) PutObjectTags(ctx context.Context, bucket, object string, tags string, opts ObjectOptions) (ObjectInfo, error) {
	if opts.VersionID != "" && opts.VersionID != nullVersionID {
		return ObjectInfo{}, VersionNotFound{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
		}
	}
	bucketDir, err := fs.getBucketPanFSPath(ctx, bucket)
	if err != nil {
		return ObjectInfo{}, err
	}
	fsMetaPath := fs.getObjectMetadataPath(bucketDir, object)
	fsMeta := panfsMeta{}
	wlk, err := fs.rwPool.Write(fsMetaPath)
	if err != nil {
		wlk, err = fs.rwPool.Create(fsMetaPath)
		if err != nil {
			logger.LogIf(ctx, err)
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
	}
	// This close will allow for locks to be synchronized on meta file.
	defer wlk.Close()

	// Read objects' metadata in meta file.
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
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// Stat the file to get file size.
	fi, err := fsStatFile(ctx, pathJoin(bucketDir, object))
	if err != nil {
		return ObjectInfo{}, err
	}

	return fsMeta.ToObjectInfo(bucket, object, fi), nil
}

// DeleteObjectTags - delete object tags from an existing object
func (fs *PANFSObjects) DeleteObjectTags(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	return fs.PutObjectTags(ctx, bucket, object, "", opts)
}

// HealFormat - no-op for fs, Valid only for Erasure.
func (fs *PANFSObjects) HealFormat(ctx context.Context, dryRun bool) (madmin.HealResultItem, error) {
	return madmin.HealResultItem{}, NotImplemented{}
}

// HealObject - no-op for fs. Valid only for Erasure.
func (fs *PANFSObjects) HealObject(ctx context.Context, bucket, object, versionID string, opts madmin.HealOpts) (
	res madmin.HealResultItem, err error,
) {
	return res, NotImplemented{}
}

// HealBucket - no-op for fs, Valid only for Erasure.
func (fs *PANFSObjects) HealBucket(ctx context.Context, bucket string, opts madmin.HealOpts) (madmin.HealResultItem,
	error,
) {
	return madmin.HealResultItem{}, NotImplemented{}
}

// Walk a bucket, optionally prefix recursively, until we have returned
// all the content to objectInfo channel, it is callers responsibility
// to allocate a receive channel for ObjectInfo, upon any unhandled
// error walker returns error. Optionally if context.Done() is received
// then Walk() stops the walker.
func (fs *PANFSObjects) Walk(ctx context.Context, bucket, prefix string, results chan<- ObjectInfo, opts ObjectOptions) error {
	return fsWalk(ctx, fs, bucket, prefix, fs.listDirFactory(), fs.isLeaf, fs.isLeafDir, results, fs.getObjectInfoNoFSLock, fs.getObjectInfoNoFSLock)
}

// HealObjects - no-op for fs. Valid only for Erasure.
func (fs *PANFSObjects) HealObjects(ctx context.Context, bucket, prefix string, opts madmin.HealOpts, fn HealObjectFn) (e error) {
	logger.LogIf(ctx, NotImplemented{})
	return NotImplemented{}
}

// GetMetrics - no op
func (fs *PANFSObjects) GetMetrics(ctx context.Context) (*BackendMetrics, error) {
	logger.LogIf(ctx, NotImplemented{})
	return &BackendMetrics{}, NotImplemented{}
}

// ListObjectsV2 lists all blobs in bucket filtered by prefix
func (fs *PANFSObjects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result ListObjectsV2Info, err error) {
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
func (fs *PANFSObjects) IsNotificationSupported() bool {
	return true
}

// IsListenSupported returns whether listen bucket notification is applicable for this layer.
func (fs *PANFSObjects) IsListenSupported() bool {
	return true
}

// IsEncryptionSupported returns whether server side encryption is implemented for this layer.
func (fs *PANFSObjects) IsEncryptionSupported() bool {
	return true
}

// IsCompressionSupported returns whether compression is applicable for this layer.
func (fs *PANFSObjects) IsCompressionSupported() bool {
	return true
}

// IsTaggingSupported returns true, object tagging is supported in fs object layer.
func (fs *PANFSObjects) IsTaggingSupported() bool {
	return true
}

// Health returns health of the object layer
func (fs *PANFSObjects) Health(ctx context.Context, opts HealthOptions) HealthResult {
	if _, err := os.Stat(fs.fsPath); err != nil {
		return HealthResult{}
	}
	return HealthResult{
		Healthy: newObjectLayerFn() != nil,
	}
}

// ReadHealth returns "read" health of the object layer
func (fs *PANFSObjects) ReadHealth(ctx context.Context) bool {
	_, err := os.Stat(fs.fsPath)
	return err == nil
}

// TransitionObject - transition object content to target tier.
func (fs *PANFSObjects) TransitionObject(ctx context.Context, bucket, object string, opts ObjectOptions) error {
	return NotImplemented{}
}

// RestoreTransitionedObject - restore transitioned object content locally on this cluster.
func (fs *PANFSObjects) RestoreTransitionedObject(ctx context.Context, bucket, object string, opts ObjectOptions) error {
	return NotImplemented{}
}

// GetRawData returns raw file data to the callback.
// Errors are ignored, only errors from the callback are returned.
// For now only direct file paths are supported.
func (fs *PANFSObjects) GetRawData(ctx context.Context, volume, file string, fn func(r io.Reader, host string, disk string, filename string, size int64, modtime time.Time, isDir bool) error) error {
	bucketDir, err := fs.getBucketPanFSPath(ctx, volume)
	if err != nil {
		return nil
	}
	f, err := os.Open(filepath.Join(bucketDir, file))
	if err != nil {
		return nil
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || st.IsDir() {
		return nil
	}
	return fn(f, "fs", fs.nodeDataSerial, file, st.Size(), st.ModTime(), st.IsDir())
}

// isConfigAgentObject - return true if the requested object should be
// accessed using the Panasas Config Agent API.
//
// The result will be false if:
// - the Config Agent is not configured (we should fall back to the local mode
// in that case),
// - the object should be stored locally - on the filesystem.
func (fs *PANFSObjects) isConfigAgentObject(bucket, object string) bool {
	if fs.configAgent == nil {
		// Config Agent has not been configured.
		return false
	}

	if bucket != minioMetaBucket {
		return false
	}

	switch {
	case strings.HasPrefix(object, "buckets/"):
		return false
	case strings.HasPrefix(object, "multipart/"):
		return false
	case strings.HasPrefix(object, "tmp/"):
		return false
	case object == formatConfigFile:
		return false
	}

	return true
}

func (fs *PANFSObjects) getTempDir(bucketDir string) string {
	return pathJoin(bucketDir, panfsS3TmpDir, fs.nodeDataSerial, strconv.FormatUint(atomic.AddUint64(&fs.currentTmpFolder, 1)%fs.tmpDirsCount, 10))
}
