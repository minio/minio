// Copyright (c) 2015-2023 MinIO, Inc.
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
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	pathutil "path"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/klauspost/filepathx"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/bucket/lifecycle"
	"github.com/minio/minio/internal/bucket/replication"
	"github.com/minio/minio/internal/cachevalue"
	"github.com/minio/minio/internal/config/storageclass"

	"github.com/minio/minio/internal/disk"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/logger"
	"github.com/pkg/xattr"
)

const (
	nullVersionID = "null"

	// Small file threshold below which data accompanies metadata from storage layer.
	smallFileThreshold = 128 * humanize.KiByte // Optimized for NVMe/SSDs

	// For hardrives it is possible to set this to a lower value to avoid any
	// spike in latency. But currently we are simply keeping it optimal for SSDs.

	// bigFileThreshold is the point where we add readahead to put operations.
	bigFileThreshold = 128 * humanize.MiByte

	// XL metadata file carries per object metadata.
	xlStorageFormatFile = "xl.meta"

	// XL metadata file backup file carries previous per object metadata.
	xlStorageFormatFileBackup = "xl.meta.bkp"
)

var alignedBuf []byte

func init() {
	alignedBuf = disk.AlignedBlock(xioutil.DirectioAlignSize)
	_, _ = rand.Read(alignedBuf)
}

// isValidVolname verifies a volname name in accordance with object
// layer requirements.
func isValidVolname(volname string) bool {
	if len(volname) < 3 {
		return false
	}

	if runtime.GOOS == "windows" {
		// Volname shouldn't have reserved characters in Windows.
		return !strings.ContainsAny(volname, `\:*?\"<>|`)
	}

	return true
}

// xlStorage - implements StorageAPI interface.
type xlStorage struct {
	// Indicate of NSScanner is in progress in this disk
	scanning int32

	drivePath string
	endpoint  Endpoint

	globalSync bool
	oDirect    bool // indicates if this disk supports ODirect

	diskID string

	formatFileInfo  os.FileInfo
	formatFile      string
	formatLegacy    bool
	formatLastCheck time.Time

	diskInfoCache *cachevalue.Cache[DiskInfo]
	sync.RWMutex
	formatData []byte

	nrRequests   uint64
	major, minor uint32
	fsType       string

	immediatePurge       chan string
	immediatePurgeCancel context.CancelFunc

	// mutex to prevent concurrent read operations overloading walks.
	rotational bool
	walkMu     *sync.Mutex
	walkReadMu *sync.Mutex
}

// checkPathLength - returns error if given path name length more than 255
func checkPathLength(pathName string) error {
	// Apple OS X path length is limited to 1016
	if runtime.GOOS == "darwin" && len(pathName) > 1016 {
		return errFileNameTooLong
	}

	// Disallow more than 1024 characters on windows, there
	// are no known name_max limits on Windows.
	if runtime.GOOS == "windows" && len(pathName) > 1024 {
		return errFileNameTooLong
	}

	// On Unix we reject paths if they are just '.', '..' or '/'
	if pathName == "." || pathName == ".." || pathName == slashSeparator {
		return errFileAccessDenied
	}

	// Check each path segment length is > 255 on all Unix
	// platforms, look for this value as NAME_MAX in
	// /usr/include/linux/limits.h
	var count int64
	for _, p := range pathName {
		switch p {
		case '/':
			count = 0 // Reset
		case '\\':
			if runtime.GOOS == globalWindowsOSName {
				count = 0
			}
		default:
			count++
			if count > 255 {
				return errFileNameTooLong
			}
		}
	} // Success.
	return nil
}

func getValidPath(path string) (string, error) {
	if path == "" {
		return path, errInvalidArgument
	}

	var err error
	// Disallow relative paths, figure out absolute paths.
	path, err = filepath.Abs(path)
	if err != nil {
		return path, err
	}

	fi, err := Lstat(path)
	if err != nil && !osIsNotExist(err) {
		return path, err
	}
	if osIsNotExist(err) {
		// Disk not found create it.
		if err = mkdirAll(path, 0o777, ""); err != nil {
			return path, err
		}
	}
	if fi != nil && !fi.IsDir() {
		return path, errDiskNotDir
	}

	return path, nil
}

// Make Erasure backend meta volumes.
func makeFormatErasureMetaVolumes(disk StorageAPI) error {
	if disk == nil {
		return errDiskNotFound
	}
	volumes := []string{
		minioMetaTmpDeletedBucket, // creates .minio.sys/tmp as well as .minio.sys/tmp/.trash
		minioMetaMultipartBucket,  // creates .minio.sys/multipart
		dataUsageBucket,           // creates .minio.sys/buckets
		minioConfigBucket,         // creates .minio.sys/config
	}
	// Attempt to create MinIO internal buckets.
	return disk.MakeVolBulk(context.TODO(), volumes...)
}

// Initialize a new storage disk.
func newXLStorage(ep Endpoint, cleanUp bool) (s *xlStorage, err error) {
	immediatePurgeQueue := 100000
	if globalIsTesting || globalIsCICD {
		immediatePurgeQueue = 1
	}

	ctx, cancel := context.WithCancel(GlobalContext)

	s = &xlStorage{
		drivePath:            ep.Path,
		endpoint:             ep,
		globalSync:           globalFSOSync,
		diskInfoCache:        cachevalue.New[DiskInfo](),
		immediatePurge:       make(chan string, immediatePurgeQueue),
		immediatePurgeCancel: cancel,
	}

	defer func() {
		if cleanUp && err == nil {
			go s.cleanupTrashImmediateCallers(ctx)
		}
	}()

	s.drivePath, err = getValidPath(ep.Path)
	if err != nil {
		s.drivePath = ep.Path
		return s, err
	}

	info, rootDrive, err := getDiskInfo(s.drivePath)
	if err != nil {
		return s, err
	}

	s.major = info.Major
	s.minor = info.Minor
	s.fsType = info.FSType

	if rootDrive {
		return s, errDriveIsRoot
	}

	// Sanitize before setting it
	if info.NRRequests > 0 {
		s.nrRequests = info.NRRequests
	}

	// We stagger listings only on HDDs.
	if info.Rotational == nil || *info.Rotational {
		s.rotational = true
		s.walkMu = &sync.Mutex{}
		s.walkReadMu = &sync.Mutex{}
	}

	if cleanUp {
		bgFormatErasureCleanupTmp(s.drivePath) // cleanup any old data.
	}

	formatData, formatFi, err := formatErasureMigrate(s.drivePath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		if os.IsPermission(err) {
			return s, errDiskAccessDenied
		} else if isSysErrIO(err) {
			return s, errFaultyDisk
		}
		return s, err
	}
	s.formatData = formatData
	s.formatFileInfo = formatFi
	s.formatFile = pathJoin(s.drivePath, minioMetaBucket, formatConfigFile)

	// Create all necessary bucket folders if possible.
	if err = makeFormatErasureMetaVolumes(s); err != nil {
		return s, err
	}

	if len(s.formatData) > 0 {
		format := &formatErasureV3{}
		json := jsoniter.ConfigCompatibleWithStandardLibrary
		if err = json.Unmarshal(s.formatData, &format); err != nil {
			return s, errCorruptedFormat
		}
		m, n, err := findDiskIndexByDiskID(format, format.Erasure.This)
		if err != nil {
			return s, err
		}
		diskID := format.Erasure.This
		if m != ep.SetIdx || n != ep.DiskIdx {
			storageLogOnceIf(context.Background(),
				fmt.Errorf("unexpected drive ordering on pool: %s: found drive at (set=%s, drive=%s), expected at (set=%s, drive=%s): %s(%s): %w",
					humanize.Ordinal(ep.PoolIdx+1), humanize.Ordinal(m+1), humanize.Ordinal(n+1), humanize.Ordinal(ep.SetIdx+1), humanize.Ordinal(ep.DiskIdx+1),
					s, s.diskID, errInconsistentDisk), "drive-order-format-json")
			return s, errInconsistentDisk
		}
		s.diskID = diskID
		s.formatLastCheck = time.Now()
		s.formatLegacy = format.Erasure.DistributionAlgo == formatErasureVersionV2DistributionAlgoV1
	}

	// Return an error if ODirect is not supported. Single disk will have
	// oDirect off.
	if globalIsErasureSD || !disk.ODirectPlatform {
		s.oDirect = false
	} else if err := s.checkODirectDiskSupport(info.FSType); err == nil {
		s.oDirect = true
	} else {
		return s, err
	}

	// Initialize DiskInfo cache
	s.diskInfoCache.InitOnce(time.Second, cachevalue.Opts{},
		func(ctx context.Context) (DiskInfo, error) {
			dcinfo := DiskInfo{}
			di, root, err := getDiskInfo(s.drivePath)
			if err != nil {
				return dcinfo, err
			}
			dcinfo.RootDisk = root
			dcinfo.Major = di.Major
			dcinfo.Minor = di.Minor
			dcinfo.Total = di.Total
			dcinfo.Free = di.Free
			dcinfo.Used = di.Used
			dcinfo.UsedInodes = di.Files - di.Ffree
			dcinfo.FreeInodes = di.Ffree
			dcinfo.FSType = di.FSType
			if root {
				return dcinfo, errDriveIsRoot
			}

			diskID, err := s.GetDiskID()
			// Healing is 'true' when
			// - if we found an unformatted disk (no 'format.json')
			// - if we found healing tracker 'healing.bin'
			dcinfo.Healing = errors.Is(err, errUnformattedDisk)
			if !dcinfo.Healing {
				if hi := s.Healing(); hi != nil && !hi.Finished {
					dcinfo.Healing = true
				}
			}

			dcinfo.ID = diskID
			return dcinfo, err
		},
	)

	// Success.
	return s, nil
}

// getDiskInfo returns given disk information.
func getDiskInfo(drivePath string) (di disk.Info, rootDrive bool, err error) {
	if err = checkPathLength(drivePath); err == nil {
		di, err = disk.GetInfo(drivePath, false)

		if !globalIsCICD && !globalIsErasureSD {
			if globalRootDiskThreshold > 0 {
				// Use MINIO_ROOTDISK_THRESHOLD_SIZE to figure out if
				// this disk is a root disk. treat those disks with
				// size less than or equal to the threshold as rootDrives.
				rootDrive = di.Total <= globalRootDiskThreshold
			} else {
				rootDrive, err = disk.IsRootDisk(drivePath, SlashSeparator)
			}
		}
	}

	switch {
	case osIsNotExist(err):
		err = errDiskNotFound
	case isSysErrTooLong(err):
		err = errFileNameTooLong
	case isSysErrIO(err):
		err = errFaultyDisk
	}

	return
}

// Implements stringer compatible interface.
func (s *xlStorage) String() string {
	return s.drivePath
}

func (s *xlStorage) Hostname() string {
	return s.endpoint.Host
}

func (s *xlStorage) Endpoint() Endpoint {
	return s.endpoint
}

func (s *xlStorage) Close() error {
	s.immediatePurgeCancel()
	return nil
}

func (s *xlStorage) IsOnline() bool {
	return true
}

func (s *xlStorage) LastConn() time.Time {
	return time.Time{}
}

func (s *xlStorage) IsLocal() bool {
	return true
}

// Retrieve location indexes.
func (s *xlStorage) GetDiskLoc() (poolIdx, setIdx, diskIdx int) {
	return s.endpoint.PoolIdx, s.endpoint.SetIdx, s.endpoint.DiskIdx
}

func (s *xlStorage) Healing() *healingTracker {
	healingFile := pathJoin(s.drivePath, minioMetaBucket,
		bucketMetaPrefix, healingTrackerFilename)
	b, err := os.ReadFile(healingFile)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			internalLogIf(GlobalContext, fmt.Errorf("unable to read %s: %w", healingFile, err))
		}
		return nil
	}
	if len(b) == 0 {
		internalLogIf(GlobalContext, fmt.Errorf("%s is empty", healingFile))
		// 'healing.bin' might be truncated
		return nil
	}
	h := newHealingTracker()
	_, err = h.UnmarshalMsg(b)
	internalLogIf(GlobalContext, err)
	return h
}

// checkODirectDiskSupport asks the disk to write some data
// with O_DIRECT support, return an error if any and return
// errUnsupportedDisk if there is no O_DIRECT support
func (s *xlStorage) checkODirectDiskSupport(fsType string) error {
	if !disk.ODirectPlatform {
		return errUnsupportedDisk
	}

	// We know XFS already supports O_DIRECT no need to check.
	if fsType == "XFS" {
		return nil
	}

	// For all other FS pay the price of not using our recommended filesystem.

	// Check if backend is writable and supports O_DIRECT
	uuid := mustGetUUID()
	filePath := pathJoin(s.drivePath, minioMetaTmpDeletedBucket, ".writable-check-"+uuid+".tmp")

	// Create top level directories if they don't exist.
	// with mode 0o777 mkdir honors system umask.
	mkdirAll(pathutil.Dir(filePath), 0o777, s.drivePath) // don't need to fail here

	w, err := s.openFileDirect(filePath, os.O_CREATE|os.O_WRONLY|os.O_EXCL)
	if err != nil {
		return err
	}
	_, err = w.Write(alignedBuf)
	w.Close()
	if err != nil {
		if isSysErrInvalidArg(err) {
			err = errUnsupportedDisk
		}
	}
	return err
}

// readsMetadata and returns disk mTime information for xl.meta
func (s *xlStorage) readMetadataWithDMTime(ctx context.Context, itemPath string) ([]byte, time.Time, error) {
	if contextCanceled(ctx) {
		return nil, time.Time{}, ctx.Err()
	}

	if err := checkPathLength(itemPath); err != nil {
		return nil, time.Time{}, err
	}

	f, err := OpenFile(itemPath, readMode, 0o666)
	if err != nil {
		return nil, time.Time{}, err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return nil, time.Time{}, err
	}
	if stat.IsDir() {
		return nil, time.Time{}, &os.PathError{
			Op:   "open",
			Path: itemPath,
			Err:  syscall.EISDIR,
		}
	}
	buf, err := readXLMetaNoData(f, stat.Size())
	if err != nil {
		return nil, stat.ModTime().UTC(), fmt.Errorf("%w -> %s", err, itemPath)
	}
	return buf, stat.ModTime().UTC(), err
}

func (s *xlStorage) readMetadata(ctx context.Context, itemPath string) ([]byte, error) {
	return xioutil.WithDeadline[[]byte](ctx, globalDriveConfig.GetMaxTimeout(), func(ctx context.Context) ([]byte, error) {
		buf, _, err := s.readMetadataWithDMTime(ctx, itemPath)
		return buf, err
	})
}

func (s *xlStorage) NSScanner(ctx context.Context, cache dataUsageCache, updates chan<- dataUsageEntry, scanMode madmin.HealScanMode, weSleep func() bool) (dataUsageCache, error) {
	atomic.AddInt32(&s.scanning, 1)
	defer atomic.AddInt32(&s.scanning, -1)

	var err error
	stopFn := globalScannerMetrics.log(scannerMetricScanBucketDrive, s.drivePath, cache.Info.Name)
	defer func() {
		res := make(map[string]string)
		if err != nil {
			res["err"] = err.Error()
		}
		stopFn(res)
	}()

	// Updates must be closed before we return.
	defer xioutil.SafeClose(updates)
	var lc *lifecycle.Lifecycle

	// Check if the current bucket has a configured lifecycle policy
	if globalLifecycleSys != nil {
		lc, err = globalLifecycleSys.Get(cache.Info.Name)
		if err == nil && lc.HasActiveRules("") {
			cache.Info.lifeCycle = lc
		}
	}

	// Check if the current bucket has replication configuration
	var rcfg *replication.Config
	if rcfg, _, err = globalBucketMetadataSys.GetReplicationConfig(ctx, cache.Info.Name); err == nil {
		if rcfg.HasActiveRules("", true) {
			tgts, err := globalBucketTargetSys.ListBucketTargets(ctx, cache.Info.Name)
			if err == nil {
				cache.Info.replication = replicationConfig{
					Config:  rcfg,
					remotes: tgts,
				}
			}
		}
	}

	// Check if bucket is object locked.
	lr, err := globalBucketObjectLockSys.Get(cache.Info.Name)
	if err != nil {
		scannerLogOnceIf(ctx, err, cache.Info.Name)
		return cache, err
	}

	vcfg, _ := globalBucketVersioningSys.Get(cache.Info.Name)

	// return initialized object layer
	objAPI := newObjectLayerFn()
	// object layer not initialized, return.
	if objAPI == nil {
		return cache, errServerNotInitialized
	}

	poolIdx, setIdx, _ := s.GetDiskLoc()

	disks, err := objAPI.GetDisks(poolIdx, setIdx)
	if err != nil {
		return cache, err
	}

	cache.Info.updates = updates

	dataUsageInfo, err := scanDataFolder(ctx, disks, s, cache, func(item scannerItem) (sizeSummary, error) {
		// Look for `xl.meta/xl.json' at the leaf.
		if !strings.HasSuffix(item.Path, SlashSeparator+xlStorageFormatFile) &&
			!strings.HasSuffix(item.Path, SlashSeparator+xlStorageFormatFileV1) {
			// if no xl.meta/xl.json found, skip the file.
			return sizeSummary{}, errSkipFile
		}
		stopFn := globalScannerMetrics.log(scannerMetricScanObject, s.drivePath, pathJoin(item.bucket, item.objectPath()))
		res := make(map[string]string, 8)
		defer func() {
			stopFn(res)
		}()

		doneSz := globalScannerMetrics.timeSize(scannerMetricReadMetadata)
		buf, err := s.readMetadata(ctx, item.Path)
		doneSz(len(buf))
		res["metasize"] = strconv.Itoa(len(buf))
		if err != nil {
			res["err"] = err.Error()
			return sizeSummary{}, errSkipFile
		}

		// Remove filename which is the meta file.
		item.transformMetaDir()

		fivs, err := getFileInfoVersions(buf, item.bucket, item.objectPath(), false)
		metaDataPoolPut(buf)
		if err != nil {
			res["err"] = err.Error()
			return sizeSummary{}, errSkipFile
		}

		versioned := vcfg != nil && vcfg.Versioned(item.objectPath())
		objInfos := make([]ObjectInfo, len(fivs.Versions))
		for i, fi := range fivs.Versions {
			objInfos[i] = fi.ToObjectInfo(item.bucket, item.objectPath(), versioned)
		}
		sizeS := sizeSummary{}
		for _, tier := range globalTierConfigMgr.ListTiers() {
			if sizeS.tiers == nil {
				sizeS.tiers = make(map[string]tierStats)
			}
			sizeS.tiers[tier.Name] = tierStats{}
		}
		if sizeS.tiers != nil {
			sizeS.tiers[storageclass.STANDARD] = tierStats{}
			sizeS.tiers[storageclass.RRS] = tierStats{}
		}

		if err != nil {
			res["err"] = err.Error()
			return sizeSummary{}, errSkipFile
		}

		var objPresent bool
		item.applyActions(ctx, objAPI, objInfos, lr, &sizeS, func(oi ObjectInfo, sz, actualSz int64, sizeS *sizeSummary) {
			objPresent = true
			if oi.DeleteMarker {
				sizeS.deleteMarkers++
			}
			if oi.VersionID != "" && sz == actualSz {
				sizeS.versions++
			}
			sizeS.totalSize += sz

			// Skip tier accounting if object version is a delete-marker or a free-version
			// tracking deleted transitioned objects
			switch {
			case oi.DeleteMarker, oi.TransitionedObject.FreeVersion:
				return
			}
			tier := oi.StorageClass
			if tier == "" {
				tier = storageclass.STANDARD // no SC means "STANDARD"
			}
			if oi.TransitionedObject.Status == lifecycle.TransitionComplete {
				tier = oi.TransitionedObject.Tier
			}
			if sizeS.tiers != nil {
				if st, ok := sizeS.tiers[tier]; ok {
					sizeS.tiers[tier] = st.add(oi.tierStats())
				}
			}
		})

		// apply tier sweep action on free versions
		for _, freeVersion := range fivs.FreeVersions {
			oi := freeVersion.ToObjectInfo(item.bucket, item.objectPath(), versioned)
			done := globalScannerMetrics.time(scannerMetricTierObjSweep)
			globalExpiryState.enqueueFreeVersion(oi)
			done()
		}

		// These are rather expensive. Skip if nobody listens.
		if globalTrace.NumSubscribers(madmin.TraceScanner) > 0 {
			if len(fivs.FreeVersions) > 0 {
				res["free-versions"] = strconv.Itoa(len(fivs.FreeVersions))
			}

			if sizeS.versions > 0 {
				res["versions"] = strconv.FormatUint(sizeS.versions, 10)
			}
			res["size"] = strconv.FormatInt(sizeS.totalSize, 10)
			for name, tier := range sizeS.tiers {
				res["tier-size-"+name] = strconv.FormatUint(tier.TotalSize, 10)
				res["tier-versions-"+name] = strconv.Itoa(tier.NumVersions)
			}
			if sizeS.failedCount > 0 {
				res["repl-failed"] = fmt.Sprintf("%d versions, %d bytes", sizeS.failedCount, sizeS.failedSize)
			}
			if sizeS.pendingCount > 0 {
				res["repl-pending"] = fmt.Sprintf("%d versions, %d bytes", sizeS.pendingCount, sizeS.pendingSize)
			}
			for tgt, st := range sizeS.replTargetStats {
				res["repl-size-"+tgt] = strconv.FormatInt(st.replicatedSize, 10)
				res["repl-count-"+tgt] = strconv.FormatInt(st.replicatedCount, 10)
				if st.failedCount > 0 {
					res["repl-failed-"+tgt] = fmt.Sprintf("%d versions, %d bytes", st.failedCount, st.failedSize)
				}
				if st.pendingCount > 0 {
					res["repl-pending-"+tgt] = fmt.Sprintf("%d versions, %d bytes", st.pendingCount, st.pendingSize)
				}
			}
		}
		if !objPresent {
			// we return errIgnoreFileContrib to signal this function's
			// callers to skip this object's contribution towards
			// usage.
			return sizeSummary{}, errIgnoreFileContrib
		}
		return sizeS, nil
	}, scanMode, weSleep)
	if err != nil {
		return dataUsageInfo, err
	}

	dataUsageInfo.Info.LastUpdate = time.Now()
	return dataUsageInfo, nil
}

func (s *xlStorage) getDeleteAttribute() uint64 {
	attr := "user.total_deletes"
	buf, err := xattr.LGet(s.formatFile, attr)
	if err != nil {
		// We start off with '0' if we can read the attributes
		return 0
	}
	return binary.LittleEndian.Uint64(buf[:8])
}

func (s *xlStorage) getWriteAttribute() uint64 {
	attr := "user.total_writes"
	buf, err := xattr.LGet(s.formatFile, attr)
	if err != nil {
		// We start off with '0' if we can read the attributes
		return 0
	}

	return binary.LittleEndian.Uint64(buf[:8])
}

func (s *xlStorage) setDeleteAttribute(deleteCount uint64) error {
	attr := "user.total_deletes"

	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, deleteCount)
	return xattr.LSet(s.formatFile, attr, data)
}

func (s *xlStorage) setWriteAttribute(writeCount uint64) error {
	attr := "user.total_writes"

	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, writeCount)
	return xattr.LSet(s.formatFile, attr, data)
}

// DiskInfo provides current information about disk space usage,
// total free inodes and underlying filesystem.
func (s *xlStorage) DiskInfo(ctx context.Context, _ DiskInfoOptions) (info DiskInfo, err error) {
	info, err = s.diskInfoCache.GetWithCtx(ctx)
	info.NRRequests = s.nrRequests
	info.Rotational = s.rotational
	info.MountPath = s.drivePath
	info.Endpoint = s.endpoint.String()
	info.Scanning = atomic.LoadInt32(&s.scanning) == 1
	return info, err
}

// getVolDir - will convert incoming volume names to
// corresponding valid volume names on the backend in a platform
// compatible way for all operating systems. If volume is not found
// an error is generated.
func (s *xlStorage) getVolDir(volume string) (string, error) {
	if volume == "" || volume == "." || volume == ".." {
		return "", errVolumeNotFound
	}
	volumeDir := pathJoin(s.drivePath, volume)
	return volumeDir, nil
}

func (s *xlStorage) checkFormatJSON() (os.FileInfo, error) {
	fi, err := Lstat(s.formatFile)
	if err != nil {
		// If the disk is still not initialized.
		if osIsNotExist(err) {
			if err = Access(s.drivePath); err == nil {
				// Disk is present but missing `format.json`
				return nil, errUnformattedDisk
			}
			if osIsNotExist(err) {
				return nil, errDiskNotFound
			} else if osIsPermission(err) {
				return nil, errDiskAccessDenied
			}
			storageLogOnceIf(GlobalContext, err, "check-format-json") // log unexpected errors
			return nil, errCorruptedBackend
		} else if osIsPermission(err) {
			return nil, errDiskAccessDenied
		}
		storageLogOnceIf(GlobalContext, err, "check-format-json") // log unexpected errors
		return nil, errCorruptedBackend
	}
	return fi, nil
}

// GetDiskID - returns the cached disk uuid
func (s *xlStorage) GetDiskID() (string, error) {
	s.RLock()
	diskID := s.diskID
	fileInfo := s.formatFileInfo
	lastCheck := s.formatLastCheck

	// check if we have a valid disk ID that is less than 1 seconds old.
	if fileInfo != nil && diskID != "" && time.Since(lastCheck) <= 1*time.Second {
		s.RUnlock()
		return diskID, nil
	}
	s.RUnlock()

	fi, err := s.checkFormatJSON()
	if err != nil {
		return "", err
	}

	if xioutil.SameFile(fi, fileInfo) && diskID != "" {
		s.Lock()
		// If the file has not changed, just return the cached diskID information.
		s.formatLastCheck = time.Now()
		s.Unlock()
		return diskID, nil
	}

	b, err := os.ReadFile(s.formatFile)
	if err != nil {
		// If the disk is still not initialized.
		if osIsNotExist(err) {
			if err = Access(s.drivePath); err == nil {
				// Disk is present but missing `format.json`
				return "", errUnformattedDisk
			}
			if osIsNotExist(err) {
				return "", errDiskNotFound
			} else if osIsPermission(err) {
				return "", errDiskAccessDenied
			}
			storageLogOnceIf(GlobalContext, err, "check-format-json") // log unexpected errors
			return "", errCorruptedBackend
		} else if osIsPermission(err) {
			return "", errDiskAccessDenied
		}
		storageLogOnceIf(GlobalContext, err, "check-format-json") // log unexpected errors
		return "", errCorruptedBackend
	}

	format := &formatErasureV3{}
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	if err = json.Unmarshal(b, &format); err != nil {
		bugLogIf(GlobalContext, err) // log unexpected errors
		return "", errCorruptedFormat
	}

	m, n, err := findDiskIndexByDiskID(format, format.Erasure.This)
	if err != nil {
		return "", err
	}

	diskID = format.Erasure.This
	ep := s.endpoint
	if m != ep.SetIdx || n != ep.DiskIdx {
		storageLogOnceIf(GlobalContext,
			fmt.Errorf("unexpected drive ordering on pool: %s: found drive at (set=%s, drive=%s), expected at (set=%s, drive=%s): %s(%s): %w",
				humanize.Ordinal(ep.PoolIdx+1), humanize.Ordinal(m+1), humanize.Ordinal(n+1), humanize.Ordinal(ep.SetIdx+1), humanize.Ordinal(ep.DiskIdx+1),
				s, s.diskID, errInconsistentDisk), "drive-order-format-json")
		return "", errInconsistentDisk
	}
	s.Lock()
	s.diskID = diskID
	s.formatLegacy = format.Erasure.DistributionAlgo == formatErasureVersionV2DistributionAlgoV1
	s.formatFileInfo = fi
	s.formatData = b
	s.formatLastCheck = time.Now()
	s.Unlock()
	return diskID, nil
}

// Make a volume entry.
func (s *xlStorage) SetDiskID(id string) {
	// NO-OP for xlStorage as it is handled either by xlStorageDiskIDCheck{} for local disks or
	// storage rest server for remote disks.
}

func (s *xlStorage) MakeVolBulk(ctx context.Context, volumes ...string) error {
	for _, volume := range volumes {
		err := s.MakeVol(ctx, volume)
		if err != nil && !errors.Is(err, errVolumeExists) {
			return err
		}
		diskHealthCheckOK(ctx, err)
	}
	return nil
}

// Make a volume entry.
func (s *xlStorage) MakeVol(ctx context.Context, volume string) error {
	if !isValidVolname(volume) {
		return errInvalidArgument
	}

	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return err
	}

	if err = Access(volumeDir); err != nil {
		// Volume does not exist we proceed to create.
		if osIsNotExist(err) {
			// Make a volume entry, with mode 0777 mkdir honors system umask.
			err = mkdirAll(volumeDir, 0o777, s.drivePath)
		}
		if osIsPermission(err) {
			return errDiskAccessDenied
		} else if isSysErrIO(err) {
			return errFaultyDisk
		}
		return err
	}

	// Stat succeeds we return errVolumeExists.
	return errVolumeExists
}

// ListVols - list volumes.
func (s *xlStorage) ListVols(ctx context.Context) (volsInfo []VolInfo, err error) {
	return listVols(ctx, s.drivePath)
}

// List all the volumes from drivePath.
func listVols(ctx context.Context, dirPath string) ([]VolInfo, error) {
	if err := checkPathLength(dirPath); err != nil {
		return nil, err
	}
	entries, err := readDir(dirPath)
	if err != nil {
		if errors.Is(err, errFileAccessDenied) {
			return nil, errDiskAccessDenied
		} else if errors.Is(err, errFileNotFound) {
			return nil, errDiskNotFound
		}
		return nil, err
	}
	volsInfo := make([]VolInfo, 0, len(entries))
	for _, entry := range entries {
		if !HasSuffix(entry, SlashSeparator) || !isValidVolname(pathutil.Clean(entry)) {
			// Skip if entry is neither a directory not a valid volume name.
			continue
		}
		volsInfo = append(volsInfo, VolInfo{
			Name: pathutil.Clean(entry),
		})
	}
	return volsInfo, nil
}

// StatVol - get volume info.
func (s *xlStorage) StatVol(ctx context.Context, volume string) (vol VolInfo, err error) {
	// Verify if volume is valid and it exists.
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return VolInfo{}, err
	}

	// Stat a volume entry.
	var st os.FileInfo
	st, err = Lstat(volumeDir)
	if err != nil {
		switch {
		case osIsNotExist(err):
			return VolInfo{}, errVolumeNotFound
		case osIsPermission(err):
			return VolInfo{}, errDiskAccessDenied
		case isSysErrIO(err):
			return VolInfo{}, errFaultyDisk
		default:
			return VolInfo{}, err
		}
	}
	// As os.Lstat() doesn't carry other than ModTime(), use ModTime()
	// as CreatedTime.
	createdTime := st.ModTime()
	return VolInfo{
		Name:    volume,
		Created: createdTime,
	}, nil
}

// DeleteVol - delete a volume.
func (s *xlStorage) DeleteVol(ctx context.Context, volume string, forceDelete bool) (err error) {
	// Verify if volume is valid and it exists.
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return err
	}

	if forceDelete {
		err = s.moveToTrash(volumeDir, true, true)
	} else {
		err = Remove(volumeDir)
	}

	if err != nil {
		switch {
		case errors.Is(err, errFileNotFound):
			return errVolumeNotFound
		case osIsNotExist(err):
			return errVolumeNotFound
		case isSysErrNotEmpty(err):
			return errVolumeNotEmpty
		case osIsPermission(err):
			return errDiskAccessDenied
		case isSysErrIO(err):
			return errFaultyDisk
		default:
			return err
		}
	}
	return nil
}

// ListDir - return all the entries at the given directory path.
// If an entry is a directory it will be returned with a trailing SlashSeparator.
func (s *xlStorage) ListDir(ctx context.Context, origvolume, volume, dirPath string, count int) (entries []string, err error) {
	if contextCanceled(ctx) {
		return nil, ctx.Err()
	}

	if origvolume != "" {
		if !skipAccessChecks(origvolume) {
			origvolumeDir, err := s.getVolDir(origvolume)
			if err != nil {
				return nil, err
			}
			if err = Access(origvolumeDir); err != nil {
				return nil, convertAccessError(err, errVolumeAccessDenied)
			}
		}
	}

	// Verify if volume is valid and it exists.
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return nil, err
	}

	dirPathAbs := pathJoin(volumeDir, dirPath)
	if count > 0 {
		entries, err = readDirN(dirPathAbs, count)
	} else {
		entries, err = readDir(dirPathAbs)
	}
	if err != nil {
		if errors.Is(err, errFileNotFound) && !skipAccessChecks(volume) {
			if ierr := Access(volumeDir); ierr != nil {
				return nil, convertAccessError(ierr, errVolumeAccessDenied)
			}
		}
		return nil, err
	}

	return entries, nil
}

func (s *xlStorage) deleteVersions(ctx context.Context, volume, path string, fis ...FileInfo) error {
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return err
	}

	s.RLock()
	legacy := s.formatLegacy
	s.RUnlock()

	var legacyJSON bool
	buf, err := xioutil.WithDeadline[[]byte](ctx, globalDriveConfig.GetMaxTimeout(), func(ctx context.Context) ([]byte, error) {
		buf, _, err := s.readAllDataWithDMTime(ctx, volume, volumeDir, pathJoin(volumeDir, path, xlStorageFormatFile))
		if err != nil && !errors.Is(err, errFileNotFound) {
			return nil, err
		}

		if errors.Is(err, errFileNotFound) && legacy {
			buf, _, err = s.readAllDataWithDMTime(ctx, volume, volumeDir, pathJoin(volumeDir, path, xlStorageFormatFileV1))
			if err != nil {
				return nil, err
			}
			legacyJSON = true
		}

		if len(buf) == 0 {
			if errors.Is(err, errFileNotFound) && !skipAccessChecks(volume) {
				if aerr := Access(volumeDir); aerr != nil && osIsNotExist(aerr) {
					return nil, errVolumeNotFound
				}
				return nil, errFileNotFound
			}
		}
		return buf, nil
	})
	if err != nil {
		return err
	}

	if legacyJSON {
		// Delete the meta file, if there are no more versions the
		// top level parent is automatically removed.
		return s.deleteFile(volumeDir, pathJoin(volumeDir, path), true, false)
	}

	var xlMeta xlMetaV2
	if err = xlMeta.LoadOrConvert(buf); err != nil {
		return err
	}

	for _, fi := range fis {
		dataDir, err := xlMeta.DeleteVersion(fi)
		if err != nil {
			if !fi.Deleted && (err == errFileNotFound || err == errFileVersionNotFound) {
				// Ignore these since they do not exist
				continue
			}
			return err
		}
		if dataDir != "" {
			versionID := fi.VersionID
			if versionID == "" {
				versionID = nullVersionID
			}

			// PR #11758 used DataDir, preserve it
			// for users who might have used master
			// branch
			xlMeta.data.remove(versionID, dataDir)

			// We need to attempt delete "dataDir" on the disk
			// due to a CopyObject() bug where it might have
			// inlined the data incorrectly, to avoid a situation
			// where we potentially leave "DataDir"
			filePath := pathJoin(volumeDir, path, dataDir)
			if err = checkPathLength(filePath); err != nil {
				return err
			}
			if err = s.moveToTrash(filePath, true, false); err != nil {
				if err != errFileNotFound {
					return err
				}
			}
		}
	}

	lastVersion := len(xlMeta.versions) == 0
	if !lastVersion {
		buf, err = xlMeta.AppendTo(metaDataPoolGet())
		defer metaDataPoolPut(buf)
		if err != nil {
			return err
		}

		return s.WriteAll(ctx, volume, pathJoin(path, xlStorageFormatFile), buf)
	}

	return s.deleteFile(volumeDir, pathJoin(volumeDir, path, xlStorageFormatFile), true, false)
}

// DeleteVersions deletes slice of versions, it can be same object
// or multiple objects.
func (s *xlStorage) DeleteVersions(ctx context.Context, volume string, versions []FileInfoVersions, opts DeleteOptions) []error {
	errs := make([]error, len(versions))

	for i, fiv := range versions {
		if contextCanceled(ctx) {
			errs[i] = ctx.Err()
			continue
		}
		errs[i] = s.deleteVersions(ctx, volume, fiv.Name, fiv.Versions...)
		diskHealthCheckOK(ctx, errs[i])
	}

	return errs
}

func (s *xlStorage) cleanupTrashImmediateCallers(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case entry := <-s.immediatePurge:
			// Add deadlines such that immediate purge is not
			// perpetually hung here.
			w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
			w.Run(func() error {
				return removeAll(entry)
			})
		}
	}
}

const almostFilledPercent = 0.05

func (s *xlStorage) diskAlmostFilled() bool {
	info, err := s.diskInfoCache.Get()
	if err != nil {
		return false
	}
	if info.Used == 0 || info.UsedInodes == 0 {
		return false
	}
	return (float64(info.Free)/float64(info.Used)) < almostFilledPercent || (float64(info.FreeInodes)/float64(info.UsedInodes)) < almostFilledPercent
}

func (s *xlStorage) moveToTrashNoDeadline(filePath string, recursive, immediatePurge bool) (err error) {
	pathUUID := mustGetUUID()
	targetPath := pathutil.Join(s.drivePath, minioMetaTmpDeletedBucket, pathUUID)

	if recursive {
		err = renameAll(filePath, targetPath, pathutil.Join(s.drivePath, minioMetaBucket))
	} else {
		err = Rename(filePath, targetPath)
	}

	var targetPath2 string
	if immediatePurge && HasSuffix(filePath, SlashSeparator) {
		// With immediate purge also attempt deleting for `__XL_DIR__` folder/directory objects.
		targetPath2 = pathutil.Join(s.drivePath, minioMetaTmpDeletedBucket, mustGetUUID())
		renameAll(encodeDirObject(filePath), targetPath2, pathutil.Join(s.drivePath, minioMetaBucket))
	}

	// ENOSPC is a valid error from rename(); remove instead of rename in that case
	if errors.Is(err, errDiskFull) || isSysErrNoSpace(err) {
		if recursive {
			err = removeAll(filePath)
		} else {
			err = Remove(filePath)
		}
		return err // Avoid the immediate purge since not needed
	}

	if err != nil {
		return err
	}

	if !immediatePurge && s.diskAlmostFilled() {
		immediatePurge = true
	}

	// immediately purge the target
	if immediatePurge {
		for _, target := range []string{
			targetPath,
			targetPath2,
		} {
			if target == "" {
				continue
			}
			select {
			case s.immediatePurge <- target:
			default:
				// Too much back pressure, we will perform the delete
				// blocking at this point we need to serialize operations.
				removeAll(target)
			}
		}
	}
	return nil
}

func (s *xlStorage) readAllData(ctx context.Context, volume, volumeDir string, filePath string) (buf []byte, err error) {
	return xioutil.WithDeadline[[]byte](ctx, globalDriveConfig.GetMaxTimeout(), func(ctx context.Context) ([]byte, error) {
		data, _, err := s.readAllDataWithDMTime(ctx, volume, volumeDir, filePath)
		return data, err
	})
}

func (s *xlStorage) moveToTrash(filePath string, recursive, immediatePurge bool) (err error) {
	w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
	return w.Run(func() (err error) {
		return s.moveToTrashNoDeadline(filePath, recursive, immediatePurge)
	})
}

// DeleteVersion - deletes FileInfo metadata for path at `xl.meta`. forceDelMarker
// will force creating a new `xl.meta` to create a new delete marker
func (s *xlStorage) DeleteVersion(ctx context.Context, volume, path string, fi FileInfo, forceDelMarker bool, opts DeleteOptions) (err error) {
	if HasSuffix(path, SlashSeparator) {
		return s.Delete(ctx, volume, path, DeleteOptions{
			Recursive: false,
			Immediate: false,
		})
	}

	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return err
	}

	// Validate file path length, before reading.
	filePath := pathJoin(volumeDir, path)
	if err = checkPathLength(filePath); err != nil {
		return err
	}

	var legacyJSON bool
	buf, err := s.readAllData(ctx, volume, volumeDir, pathJoin(filePath, xlStorageFormatFile))
	if err != nil {
		if !errors.Is(err, errFileNotFound) {
			return err
		}
		metaDataPoolPut(buf) // Never used, return it
		if fi.Deleted && forceDelMarker {
			// Create a new xl.meta with a delete marker in it
			return s.WriteMetadata(ctx, "", volume, path, fi)
		}

		s.RLock()
		legacy := s.formatLegacy
		s.RUnlock()
		if legacy {
			buf, err = s.ReadAll(ctx, volume, pathJoin(path, xlStorageFormatFileV1))
			if err != nil {
				if errors.Is(err, errFileNotFound) && fi.VersionID != "" {
					return errFileVersionNotFound
				}
				return err
			}
			legacyJSON = true
		}
	}

	if len(buf) == 0 {
		if fi.VersionID != "" {
			return errFileVersionNotFound
		}
		return errFileNotFound
	}

	if legacyJSON {
		// Delete the meta file, if there are no more versions the
		// top level parent is automatically removed.
		return s.deleteFile(volumeDir, pathJoin(volumeDir, path), true, false)
	}

	var xlMeta xlMetaV2
	if err = xlMeta.LoadOrConvert(buf); err != nil {
		return err
	}

	dataDir, err := xlMeta.DeleteVersion(fi)
	if err != nil {
		return err
	}
	if dataDir != "" {
		versionID := fi.VersionID
		if versionID == "" {
			versionID = nullVersionID
		}
		// PR #11758 used DataDir, preserve it
		// for users who might have used master
		// branch
		xlMeta.data.remove(versionID, dataDir)

		// We need to attempt delete "dataDir" on the disk
		// due to a CopyObject() bug where it might have
		// inlined the data incorrectly, to avoid a situation
		// where we potentially leave "DataDir"
		filePath := pathJoin(volumeDir, path, dataDir)
		if err = checkPathLength(filePath); err != nil {
			return err
		}
		if err = s.moveToTrash(filePath, true, false); err != nil {
			if err != errFileNotFound {
				return err
			}
		}
	}

	if len(xlMeta.versions) != 0 {
		// xl.meta must still exist for other versions, dataDir is purged.
		buf, err = xlMeta.AppendTo(metaDataPoolGet())
		defer metaDataPoolPut(buf)
		if err != nil {
			return err
		}

		return s.writeAllMeta(ctx, volume, pathJoin(path, xlStorageFormatFile), buf, true)
	}

	if opts.UndoWrite && opts.OldDataDir != "" {
		return renameAll(pathJoin(filePath, opts.OldDataDir, xlStorageFormatFileBackup), pathJoin(filePath, xlStorageFormatFile), filePath)
	}

	return s.deleteFile(volumeDir, pathJoin(volumeDir, path, xlStorageFormatFile), true, false)
}

// Updates only metadata for a given version.
func (s *xlStorage) UpdateMetadata(ctx context.Context, volume, path string, fi FileInfo, opts UpdateMetadataOpts) (err error) {
	if len(fi.Metadata) == 0 {
		return errInvalidArgument
	}

	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return err
	}

	// Validate file path length, before reading.
	filePath := pathJoin(volumeDir, path)
	if err = checkPathLength(filePath); err != nil {
		return err
	}

	buf, err := s.ReadAll(ctx, volume, pathJoin(path, xlStorageFormatFile))
	if err != nil {
		if err == errFileNotFound && fi.VersionID != "" {
			return errFileVersionNotFound
		}
		return err
	}
	defer metaDataPoolPut(buf)

	if !isXL2V1Format(buf) {
		return errFileVersionNotFound
	}

	var xlMeta xlMetaV2
	if err = xlMeta.Load(buf); err != nil {
		return err
	}

	if err = xlMeta.UpdateObjectVersion(fi); err != nil {
		return err
	}

	wbuf, err := xlMeta.AppendTo(metaDataPoolGet())
	if err != nil {
		return err
	}
	defer metaDataPoolPut(wbuf)

	return s.writeAllMeta(ctx, volume, pathJoin(path, xlStorageFormatFile), wbuf, !opts.NoPersistence)
}

// WriteMetadata - writes FileInfo metadata for path at `xl.meta`
func (s *xlStorage) WriteMetadata(ctx context.Context, origvolume, volume, path string, fi FileInfo) (err error) {
	if fi.Fresh {
		if origvolume != "" {
			origvolumeDir, err := s.getVolDir(origvolume)
			if err != nil {
				return err
			}

			if !skipAccessChecks(origvolume) {
				// Stat a volume entry.
				if err = Access(origvolumeDir); err != nil {
					return convertAccessError(err, errVolumeAccessDenied)
				}
			}
		}

		var xlMeta xlMetaV2
		if err := xlMeta.AddVersion(fi); err != nil {
			return err
		}
		buf, err := xlMeta.AppendTo(metaDataPoolGet())
		defer metaDataPoolPut(buf)
		if err != nil {
			return err
		}
		// First writes for special situations do not write to stable storage.
		// this is currently used by
		// - emphemeral objects such as objects created during listObjects() calls
		ok := volume == minioMetaMultipartBucket // - newMultipartUpload() call must be synced to drives.
		return s.writeAll(ctx, volume, pathJoin(path, xlStorageFormatFile), buf, ok, "")
	}

	buf, err := s.ReadAll(ctx, volume, pathJoin(path, xlStorageFormatFile))
	if err != nil && err != errFileNotFound {
		return err
	}
	defer metaDataPoolPut(buf)

	var xlMeta xlMetaV2
	if !isXL2V1Format(buf) {
		// This is both legacy and without proper version.
		if err = xlMeta.AddVersion(fi); err != nil {
			return err
		}

		buf, err = xlMeta.AppendTo(metaDataPoolGet())
		defer metaDataPoolPut(buf)
		if err != nil {
			return err
		}
	} else {
		if err = xlMeta.Load(buf); err != nil {
			// Corrupted data, reset and write.
			xlMeta = xlMetaV2{}
		}

		if err = xlMeta.AddVersion(fi); err != nil {
			return err
		}

		buf, err = xlMeta.AppendTo(metaDataPoolGet())
		defer metaDataPoolPut(buf)
		if err != nil {
			return err
		}
	}

	return s.WriteAll(ctx, volume, pathJoin(path, xlStorageFormatFile), buf)
}

func (s *xlStorage) renameLegacyMetadata(volumeDir, path string) (err error) {
	s.RLock()
	legacy := s.formatLegacy
	s.RUnlock()
	if !legacy {
		// if its not a legacy backend then this function is
		// a no-op always returns errFileNotFound
		return errFileNotFound
	}

	// Validate file path length, before reading.
	filePath := pathJoin(volumeDir, path)
	if err = checkPathLength(filePath); err != nil {
		return err
	}

	srcFilePath := pathJoin(filePath, xlStorageFormatFileV1)
	dstFilePath := pathJoin(filePath, xlStorageFormatFile)

	// Renaming xl.json to xl.meta should be fully synced to disk.
	defer func() {
		if err == nil && s.globalSync {
			// Sync to disk only upon success.
			globalSync()
		}
	}()

	if err = Rename(srcFilePath, dstFilePath); err != nil {
		switch {
		case isSysErrNotDir(err):
			return errFileNotFound
		case isSysErrPathNotFound(err):
			return errFileNotFound
		case isSysErrCrossDevice(err):
			return fmt.Errorf("%w (%s)->(%s)", errCrossDeviceLink, srcFilePath, dstFilePath)
		case osIsNotExist(err):
			return errFileNotFound
		case osIsExist(err):
			// This is returned only when destination is a directory and we
			// are attempting a rename from file to directory.
			return errIsNotRegular
		default:
			return err
		}
	}
	return nil
}

func (s *xlStorage) readRaw(ctx context.Context, volume, volumeDir, filePath string, readData bool) (buf []byte, dmTime time.Time, err error) {
	if filePath == "" {
		return nil, dmTime, errFileNotFound
	}

	xlPath := pathJoin(filePath, xlStorageFormatFile)
	if readData {
		buf, dmTime, err = s.readAllDataWithDMTime(ctx, volume, volumeDir, xlPath)
	} else {
		buf, dmTime, err = s.readMetadataWithDMTime(ctx, xlPath)
		if err != nil {
			if osIsNotExist(err) {
				if !skipAccessChecks(volume) {
					if aerr := Access(volumeDir); aerr != nil && osIsNotExist(aerr) {
						return nil, time.Time{}, errVolumeNotFound
					}
				}
			}
			err = osErrToFileErr(err)
		}
	}

	s.RLock()
	legacy := s.formatLegacy
	s.RUnlock()

	if err != nil && errors.Is(err, errFileNotFound) && legacy {
		buf, dmTime, err = s.readAllDataWithDMTime(ctx, volume, volumeDir, pathJoin(filePath, xlStorageFormatFileV1))
		if err != nil {
			return nil, time.Time{}, err
		}
	}

	if len(buf) == 0 {
		if err != nil {
			return nil, time.Time{}, err
		}
		return nil, time.Time{}, errFileNotFound
	}

	return buf, dmTime, nil
}

// ReadXL reads from path/xl.meta, does not interpret the data it read. This
// is a raw call equivalent of ReadVersion().
func (s *xlStorage) ReadXL(ctx context.Context, volume, path string, readData bool) (RawFileInfo, error) {
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return RawFileInfo{}, err
	}

	// Validate file path length, before reading.
	filePath := pathJoin(volumeDir, path)
	if err = checkPathLength(filePath); err != nil {
		return RawFileInfo{}, err
	}

	buf, _, err := s.readRaw(ctx, volume, volumeDir, filePath, readData)
	return RawFileInfo{
		Buf: buf,
	}, err
}

// ReadOptions optional inputs for ReadVersion
type ReadOptions struct {
	InclFreeVersions bool
	ReadData         bool
	Healing          bool
}

// ReadVersion - reads metadata and returns FileInfo at path `xl.meta`
// for all objects less than `32KiB` this call returns data as well
// along with metadata.
func (s *xlStorage) ReadVersion(ctx context.Context, origvolume, volume, path, versionID string, opts ReadOptions) (fi FileInfo, err error) {
	if origvolume != "" {
		origvolumeDir, err := s.getVolDir(origvolume)
		if err != nil {
			return fi, err
		}

		if !skipAccessChecks(origvolume) {
			// Stat a volume entry.
			if err = Access(origvolumeDir); err != nil {
				return fi, convertAccessError(err, errVolumeAccessDenied)
			}
		}
	}

	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return fi, err
	}

	// Validate file path length, before reading.
	filePath := pathJoin(volumeDir, path)
	if err = checkPathLength(filePath); err != nil {
		return fi, err
	}

	readData := opts.ReadData

	buf, _, err := s.readRaw(ctx, volume, volumeDir, filePath, readData)
	if err != nil {
		if err == errFileNotFound {
			if versionID != "" {
				return fi, errFileVersionNotFound
			}
		}
		return fi, err
	}

	fi, err = getFileInfo(buf, volume, path, versionID, fileInfoOpts{
		Data:             opts.ReadData,
		InclFreeVersions: opts.InclFreeVersions,
	})
	if err != nil {
		return fi, err
	}

	if len(fi.Data) == 0 {
		// We did not read inline data, so we have no references.
		defer metaDataPoolPut(buf)
	}

	if readData {
		if len(fi.Data) > 0 || fi.Size == 0 {
			if fi.InlineData() {
				// If written with header we are fine.
				return fi, nil
			}
			if fi.Size == 0 || (fi.VersionID == "" || fi.VersionID == nullVersionID) {
				// If versioned we have no conflicts.
				fi.SetInlineData()
				return fi, nil
			}

			// For overwritten objects without header we might have a
			// conflict with data written later. Check the data path
			// if there is a part with data.
			partPath := fmt.Sprintf("part.%d", fi.Parts[0].Number)
			dataPath := pathJoin(path, fi.DataDir, partPath)
			_, lerr := Lstat(pathJoin(volumeDir, dataPath))
			if lerr != nil {
				// Set the inline header, our inlined data is fine.
				fi.SetInlineData()
				return fi, nil
			}
			// Data exists on disk, remove the version from metadata.
			fi.Data = nil
		}

		attemptInline := fi.TransitionStatus == "" && fi.DataDir != "" && len(fi.Parts) == 1
		// Reading data for small objects when
		// - object has not yet transitioned
		// - object has maximum of 1 parts
		if attemptInline {
			inlineBlock := globalStorageClass.InlineBlock()
			if inlineBlock <= 0 {
				inlineBlock = 128 * humanize.KiByte
			}

			canInline := fi.ShardFileSize(fi.Parts[0].ActualSize) <= inlineBlock
			if canInline {
				dataPath := pathJoin(volumeDir, path, fi.DataDir, fmt.Sprintf("part.%d", fi.Parts[0].Number))
				fi.Data, err = s.readAllData(ctx, volume, volumeDir, dataPath)
				if err != nil {
					return FileInfo{}, err
				}
			}
		}
	}

	return fi, nil
}

func (s *xlStorage) readAllDataWithDMTime(ctx context.Context, volume, volumeDir string, filePath string) (buf []byte, dmTime time.Time, err error) {
	if filePath == "" {
		return nil, dmTime, errFileNotFound
	}

	if contextCanceled(ctx) {
		return nil, time.Time{}, ctx.Err()
	}

	f, err := OpenFile(filePath, readMode, 0o666)
	if err != nil {
		switch {
		case osIsNotExist(err):
			// Check if the object doesn't exist because its bucket
			// is missing in order to return the correct error.
			if !skipAccessChecks(volume) {
				if err = Access(volumeDir); err != nil && osIsNotExist(err) {
					return nil, dmTime, errVolumeNotFound
				}
			}
			return nil, dmTime, errFileNotFound
		case osIsPermission(err):
			return nil, dmTime, errFileAccessDenied
		case isSysErrNotDir(err) || isSysErrIsDir(err):
			return nil, dmTime, errFileNotFound
		case isSysErrHandleInvalid(err):
			// This case is special and needs to be handled for windows.
			return nil, dmTime, errFileNotFound
		case isSysErrIO(err):
			return nil, dmTime, errFaultyDisk
		case isSysErrTooManyFiles(err):
			return nil, dmTime, errTooManyOpenFiles
		case isSysErrInvalidArg(err):
			st, _ := Lstat(filePath)
			if st != nil && st.IsDir() {
				// Linux returns InvalidArg for directory O_DIRECT
				// we need to keep this fallback code to return correct
				// errors upwards.
				return nil, dmTime, errFileNotFound
			}
			return nil, dmTime, errUnsupportedDisk
		}
		return nil, dmTime, err
	}
	defer f.Close()

	// Get size for precise allocation.
	stat, err := f.Stat()
	if err != nil {
		buf, err = io.ReadAll(f)
		return buf, dmTime, osErrToFileErr(err)
	}
	if stat.IsDir() {
		return nil, dmTime, errFileNotFound
	}

	// Read into appropriate buffer.
	sz := stat.Size()
	if sz <= metaDataReadDefault {
		buf = metaDataPoolGet()
		buf = buf[:sz]
	} else {
		buf = make([]byte, sz)
	}

	// Read file...
	_, err = io.ReadFull(f, buf)

	return buf, stat.ModTime().UTC(), osErrToFileErr(err)
}

// ReadAll is a raw call, reads content at any path and returns the buffer.
func (s *xlStorage) ReadAll(ctx context.Context, volume string, path string) (buf []byte, err error) {
	// Specific optimization to avoid re-read from the drives for `format.json`
	// in-case the caller is a network operation.
	if volume == minioMetaBucket && path == formatConfigFile {
		s.RLock()
		formatData := make([]byte, len(s.formatData))
		copy(formatData, s.formatData)
		s.RUnlock()
		if len(formatData) > 0 {
			return formatData, nil
		}
	}
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return nil, err
	}

	// Validate file path length, before reading.
	filePath := pathJoin(volumeDir, path)
	if err = checkPathLength(filePath); err != nil {
		return nil, err
	}

	return s.readAllData(ctx, volume, volumeDir, filePath)
}

// ReadFile reads exactly len(buf) bytes into buf. It returns the
// number of bytes copied. The error is EOF only if no bytes were
// read. On return, n == len(buf) if and only if err == nil. n == 0
// for io.EOF.
//
// If an EOF happens after reading some but not all the bytes,
// ReadFile returns ErrUnexpectedEOF.
//
// If the BitrotVerifier is not nil or not verified ReadFile
// tries to verify whether the disk has bitrot.
//
// Additionally ReadFile also starts reading from an offset. ReadFile
// semantics are same as io.ReadFull.
func (s *xlStorage) ReadFile(ctx context.Context, volume string, path string, offset int64, buffer []byte, verifier *BitrotVerifier) (int64, error) {
	if offset < 0 {
		return 0, errInvalidArgument
	}

	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return 0, err
	}

	var n int

	if !skipAccessChecks(volume) {
		// Stat a volume entry.
		if err = Access(volumeDir); err != nil {
			return 0, convertAccessError(err, errFileAccessDenied)
		}
	}

	// Validate effective path length before reading.
	filePath := pathJoin(volumeDir, path)
	if err = checkPathLength(filePath); err != nil {
		return 0, err
	}

	// Open the file for reading.
	file, err := OpenFile(filePath, readMode, 0o666)
	if err != nil {
		switch {
		case osIsNotExist(err):
			return 0, errFileNotFound
		case osIsPermission(err):
			return 0, errFileAccessDenied
		case isSysErrNotDir(err):
			return 0, errFileAccessDenied
		case isSysErrIO(err):
			return 0, errFaultyDisk
		case isSysErrTooManyFiles(err):
			return 0, errTooManyOpenFiles
		default:
			return 0, err
		}
	}

	// Close the file descriptor.
	defer file.Close()

	st, err := file.Stat()
	if err != nil {
		return 0, err
	}

	// Verify it is a regular file, otherwise subsequent Seek is
	// undefined.
	if !st.Mode().IsRegular() {
		return 0, errIsNotRegular
	}

	if verifier == nil {
		n, err = file.ReadAt(buffer, offset)
		return int64(n), err
	}

	h := verifier.algorithm.New()
	if _, err = io.Copy(h, io.LimitReader(file, offset)); err != nil {
		return 0, err
	}

	if n, err = io.ReadFull(file, buffer); err != nil {
		return int64(n), err
	}

	if _, err = h.Write(buffer); err != nil {
		return 0, err
	}

	if _, err = io.Copy(h, file); err != nil {
		return 0, err
	}

	if !bytes.Equal(h.Sum(nil), verifier.sum) {
		return 0, errFileCorrupt
	}

	return int64(len(buffer)), nil
}

func (s *xlStorage) openFileDirect(path string, mode int) (f *os.File, err error) {
	w, err := OpenFileDirectIO(path, mode, 0o666)
	if err != nil {
		switch {
		case isSysErrInvalidArg(err):
			return nil, errUnsupportedDisk
		case osIsPermission(err):
			return nil, errDiskAccessDenied
		case isSysErrIO(err):
			return nil, errFaultyDisk
		case isSysErrNotDir(err):
			return nil, errDiskNotDir
		case os.IsNotExist(err):
			return nil, errDiskNotFound
		}
	}

	return w, nil
}

func (s *xlStorage) openFileSync(filePath string, mode int, skipParent string) (f *os.File, err error) {
	return s.openFile(filePath, mode|writeMode, skipParent)
}

func (s *xlStorage) openFile(filePath string, mode int, skipParent string) (f *os.File, err error) {
	if skipParent == "" {
		skipParent = s.drivePath
	}
	// Create top level directories if they don't exist.
	// with mode 0777 mkdir honors system umask.
	if err = mkdirAll(pathutil.Dir(filePath), 0o777, skipParent); err != nil {
		return nil, osErrToFileErr(err)
	}

	w, err := OpenFile(filePath, mode, 0o666)
	if err != nil {
		// File path cannot be verified since one of the parents is a file.
		switch {
		case isSysErrIsDir(err):
			return nil, errIsNotRegular
		case osIsPermission(err):
			return nil, errFileAccessDenied
		case isSysErrNotDir(err):
			return nil, errFileAccessDenied
		case isSysErrIO(err):
			return nil, errFaultyDisk
		case isSysErrTooManyFiles(err):
			return nil, errTooManyOpenFiles
		default:
			return nil, err
		}
	}

	return w, nil
}

type sendFileReader struct {
	io.Reader
	io.Closer
}

// ReadFileStream - Returns the read stream of the file.
func (s *xlStorage) ReadFileStream(ctx context.Context, volume, path string, offset, length int64) (io.ReadCloser, error) {
	if offset < 0 {
		return nil, errInvalidArgument
	}

	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return nil, err
	}

	// Validate effective path length before reading.
	filePath := pathJoin(volumeDir, path)
	if err = checkPathLength(filePath); err != nil {
		return nil, err
	}

	file, err := OpenFile(filePath, readMode, 0o666)
	if err != nil {
		switch {
		case osIsNotExist(err):
			if !skipAccessChecks(volume) {
				if err = Access(volumeDir); err != nil && osIsNotExist(err) {
					return nil, errVolumeNotFound
				}
			}
			return nil, errFileNotFound
		case osIsPermission(err):
			return nil, errFileAccessDenied
		case isSysErrNotDir(err):
			return nil, errFileAccessDenied
		case isSysErrIO(err):
			return nil, errFaultyDisk
		case isSysErrTooManyFiles(err):
			return nil, errTooManyOpenFiles
		case isSysErrInvalidArg(err):
			return nil, errUnsupportedDisk
		default:
			return nil, err
		}
	}

	if length < 0 {
		return file, nil
	}

	st, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	// Verify it is a regular file, otherwise subsequent Seek is
	// undefined.
	if !st.Mode().IsRegular() {
		file.Close()
		return nil, errIsNotRegular
	}

	if st.Size() < offset+length {
		// Expected size cannot be satisfied for
		// requested offset and length
		file.Close()
		return nil, errFileCorrupt
	}

	if offset > 0 {
		if _, err = file.Seek(offset, io.SeekStart); err != nil {
			file.Close()
			return nil, err
		}
	}
	return &sendFileReader{Reader: io.LimitReader(file, length), Closer: file}, nil
}

// CreateFile - creates the file.
func (s *xlStorage) CreateFile(ctx context.Context, origvolume, volume, path string, fileSize int64, r io.Reader) (err error) {
	if origvolume != "" {
		origvolumeDir, err := s.getVolDir(origvolume)
		if err != nil {
			return err
		}

		if !skipAccessChecks(origvolume) {
			// Stat a volume entry.
			if err = Access(origvolumeDir); err != nil {
				return convertAccessError(err, errVolumeAccessDenied)
			}
		}
	}

	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return err
	}

	filePath := pathJoin(volumeDir, path)
	if err = checkPathLength(filePath); err != nil {
		return err
	}

	parentFilePath := pathutil.Dir(filePath)
	defer func() {
		if err != nil {
			if volume == minioMetaTmpBucket {
				// only cleanup parent path if the
				// parent volume name is minioMetaTmpBucket
				removeAll(parentFilePath)
			}
		}
	}()

	return s.writeAllDirect(ctx, filePath, fileSize, r, os.O_CREATE|os.O_WRONLY|os.O_EXCL, volumeDir, false)
}

func (s *xlStorage) writeAllDirect(ctx context.Context, filePath string, fileSize int64, r io.Reader, flags int, skipParent string, truncate bool) (err error) {
	if contextCanceled(ctx) {
		return ctx.Err()
	}

	if skipParent == "" {
		skipParent = s.drivePath
	}

	// Create top level directories if they don't exist.
	// with mode 0777 mkdir honors system umask.
	parentFilePath := pathutil.Dir(filePath)
	if err = mkdirAll(parentFilePath, 0o777, skipParent); err != nil {
		return osErrToFileErr(err)
	}

	odirectEnabled := globalAPIConfig.odirectEnabled() && s.oDirect && fileSize > 0

	var w *os.File
	if odirectEnabled {
		w, err = OpenFileDirectIO(filePath, flags, 0o666)
	} else {
		w, err = OpenFile(filePath, flags, 0o666)
	}
	if err != nil {
		return osErrToFileErr(err)
	}

	var bufp *[]byte
	switch {
	case fileSize <= xioutil.SmallBlock:
		bufp = xioutil.ODirectPoolSmall.Get()
		defer xioutil.ODirectPoolSmall.Put(bufp)
	default:
		bufp = xioutil.ODirectPoolLarge.Get()
		defer xioutil.ODirectPoolLarge.Put(bufp)
	}

	var written int64
	if odirectEnabled {
		written, err = xioutil.CopyAligned(diskHealthWriter(ctx, w), r, *bufp, fileSize, w)
	} else {
		written, err = io.CopyBuffer(diskHealthWriter(ctx, w), r, *bufp)
	}
	if err != nil {
		w.Close()
		return err
	}

	if written < fileSize && fileSize >= 0 {
		if truncate {
			w.Truncate(0) // zero-in the file size to indicate that its unreadable
		}
		w.Close()
		return errLessData
	} else if written > fileSize && fileSize >= 0 {
		if truncate {
			w.Truncate(0) // zero-in the file size to indicate that its unreadable
		}
		w.Close()
		return errMoreData
	}

	// Only interested in flushing the size_t not mtime/atime
	if err = Fdatasync(w); err != nil {
		w.Close()
		return err
	}

	// Dealing with error returns from close() - 'man 2 close'
	//
	// A careful programmer will check the return value of close(), since it is quite possible that
	// errors on a previous write(2) operation are reported only on the final close() that releases
	// the open file descriptor.
	//
	// Failing to check the return value when closing a file may lead to silent loss of data.
	// This can especially be observed with NFS and with disk quota.
	return w.Close()
}

// writeAllMeta - writes all metadata to a temp file and then links it to the final destination.
func (s *xlStorage) writeAllMeta(ctx context.Context, volume string, path string, b []byte, sync bool) (err error) {
	if contextCanceled(ctx) {
		return ctx.Err()
	}

	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return err
	}

	filePath := pathJoin(volumeDir, path)
	if err = checkPathLength(filePath); err != nil {
		return err
	}

	tmpVolumeDir, err := s.getVolDir(minioMetaTmpBucket)
	if err != nil {
		return err
	}

	tmpFilePath := pathJoin(tmpVolumeDir, mustGetUUID())
	defer func() {
		if err != nil {
			Remove(tmpFilePath)
		}
	}()

	if err = s.writeAllInternal(ctx, tmpFilePath, b, sync, tmpVolumeDir); err != nil {
		return err
	}

	return renameAll(tmpFilePath, filePath, volumeDir)
}

// Create or truncate an existing file before writing
func (s *xlStorage) writeAllInternal(ctx context.Context, filePath string, b []byte, sync bool, skipParent string) (err error) {
	flags := os.O_CREATE | os.O_WRONLY | os.O_TRUNC

	var w *os.File
	if sync {
		// Perform DirectIO along with fdatasync for larger xl.meta, mostly when
		// xl.meta has "inlined data" we prefer writing O_DIRECT and then doing
		// fdatasync() at the end instead of opening the file with O_DSYNC.
		//
		// This is an optimization mainly to ensure faster I/O.
		if len(b) > xioutil.DirectioAlignSize {
			r := bytes.NewReader(b)
			return s.writeAllDirect(ctx, filePath, r.Size(), r, flags, skipParent, true)
		}
		w, err = s.openFileSync(filePath, flags, skipParent)
	} else {
		w, err = s.openFile(filePath, flags, skipParent)
	}
	if err != nil {
		return err
	}

	_, err = w.Write(b)
	if err != nil {
		w.Truncate(0) // to indicate that we did partial write.
		w.Close()
		return err
	}

	// Dealing with error returns from close() - 'man 2 close'
	//
	// A careful programmer will check the return value of close(), since it is quite possible that
	// errors on a previous write(2) operation are reported only on the final close() that releases
	// the open file descriptor.
	//
	// Failing to check the return value when closing a file may lead to silent loss of data.
	// This can especially be observed with NFS and with disk quota.
	return w.Close()
}

func (s *xlStorage) writeAll(ctx context.Context, volume string, path string, b []byte, sync bool, skipParent string) (err error) {
	if contextCanceled(ctx) {
		return ctx.Err()
	}

	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return err
	}

	filePath := pathJoin(volumeDir, path)
	if err = checkPathLength(filePath); err != nil {
		return err
	}

	return s.writeAllInternal(ctx, filePath, b, sync, skipParent)
}

func (s *xlStorage) WriteAll(ctx context.Context, volume string, path string, b []byte) (err error) {
	// Specific optimization to avoid re-read from the drives for `format.json`
	// in-case the caller is a network operation.
	if volume == minioMetaBucket && path == formatConfigFile {
		s.Lock()
		s.formatData = b
		s.Unlock()
	}

	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return err
	}

	return s.writeAll(ctx, volume, path, b, true, volumeDir)
}

// AppendFile - append a byte array at path, if file doesn't exist at
// path this call explicitly creates it.
func (s *xlStorage) AppendFile(ctx context.Context, volume string, path string, buf []byte) (err error) {
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return err
	}

	if !skipAccessChecks(volume) {
		// Stat a volume entry.
		if err = Access(volumeDir); err != nil {
			return convertAccessError(err, errVolumeAccessDenied)
		}
	}

	filePath := pathJoin(volumeDir, path)
	if err = checkPathLength(filePath); err != nil {
		return err
	}

	var w *os.File
	// Create file if not found. Not doing O_DIRECT here to avoid the code that does buffer aligned writes.
	// AppendFile() is only used by healing code to heal objects written in old format.
	w, err = s.openFileSync(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, volumeDir)
	if err != nil {
		return err
	}
	defer w.Close()

	n, err := w.Write(buf)
	if err != nil {
		return err
	}

	if n != len(buf) {
		return io.ErrShortWrite
	}

	return nil
}

// checkPart is a light check of an existing and size of a part, without doing a bitrot operation
// For any unexpected error, return checkPartUnknown (zero)
func (s *xlStorage) checkPart(volumeDir, path, dataDir string, partNum int, expectedSize int64, skipAccessCheck bool) (resp int) {
	partPath := pathJoin(path, dataDir, fmt.Sprintf("part.%d", partNum))
	filePath := pathJoin(volumeDir, partPath)
	st, err := Lstat(filePath)
	if err != nil {
		if osIsNotExist(err) {
			if !skipAccessCheck {
				// Stat a volume entry.
				if verr := Access(volumeDir); verr != nil {
					if osIsNotExist(verr) {
						resp = checkPartVolumeNotFound
					}
					return
				}
			}
		}
		if osErrToFileErr(err) == errFileNotFound {
			resp = checkPartFileNotFound
		}
		return
	}
	if st.Mode().IsDir() {
		resp = checkPartFileNotFound
		return
	}
	// Check if shard is truncated.
	if st.Size() < expectedSize {
		resp = checkPartFileCorrupt
		return
	}
	return checkPartSuccess
}

// CheckParts check if path has necessary parts available.
func (s *xlStorage) CheckParts(ctx context.Context, volume string, path string, fi FileInfo) (*CheckPartsResp, error) {
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return nil, err
	}

	err = checkPathLength(pathJoin(volumeDir, path))
	if err != nil {
		return nil, err
	}

	resp := CheckPartsResp{
		// By default, all results have an unknown status
		Results: make([]int, len(fi.Parts)),
	}

	for i, part := range fi.Parts {
		resp.Results[i], err = xioutil.WithDeadline[int](ctx, globalDriveConfig.GetMaxTimeout(), func(ctx context.Context) (int, error) {
			return s.checkPart(volumeDir, path, fi.DataDir, part.Number, fi.Erasure.ShardFileSize(part.Size), skipAccessChecks(volume)), nil
		})
		if err != nil {
			return nil, err
		}
	}

	return &resp, nil
}

// deleteFile deletes a file or a directory if its empty unless recursive
// is set to true. If the target is successfully deleted, it will recursively
// move up the tree, deleting empty parent directories until it finds one
// with files in it. Returns nil for a non-empty directory even when
// recursive is set to false.
func (s *xlStorage) deleteFile(basePath, deletePath string, recursive, immediate bool) error {
	if basePath == "" || deletePath == "" {
		return nil
	}

	bp := pathutil.Clean(basePath) // do not override basepath / or deletePath /
	dp := pathutil.Clean(deletePath)
	if !strings.HasPrefix(dp, bp) || dp == bp {
		return nil
	}

	var err error
	if recursive {
		err = s.moveToTrash(deletePath, true, immediate)
	} else {
		err = Remove(deletePath)
	}
	if err != nil {
		switch {
		case isSysErrNotEmpty(err):
			// if object is a directory, but if its not empty
			// return FileNotFound to indicate its an empty prefix.
			if HasSuffix(deletePath, SlashSeparator) {
				return errFileNotFound
			}
			// if we have .DS_Store only on macOS
			if runtime.GOOS == globalMacOSName {
				storeFilePath := pathJoin(deletePath, ".DS_Store")
				_, err := Stat(storeFilePath)
				// .DS_Store exists
				if err == nil {
					// delete first
					Remove(storeFilePath)
					// try again
					Remove(deletePath)
				}
			}
			// Ignore errors if the directory is not empty. The server relies on
			// this functionality, and sometimes uses recursion that should not
			// error on parent directories.
			return nil
		case osIsNotExist(err):
			return nil
		case errors.Is(err, errFileNotFound):
			return nil
		case osIsPermission(err):
			return errFileAccessDenied
		case isSysErrIO(err):
			return errFaultyDisk
		default:
			return err
		}
	}

	// Delete parent directory obviously not recursively. Errors for
	// parent directories shouldn't trickle down.
	s.deleteFile(basePath, pathutil.Dir(pathutil.Clean(deletePath)), false, false)

	return nil
}

// DeleteBulk - delete many files in bulk to trash.
// this delete does not recursively delete empty
// parents, if you need empty parent delete support
// please use Delete() instead. This API is meant as
// an optimization for Multipart operations.
func (s *xlStorage) DeleteBulk(ctx context.Context, volume string, paths ...string) (err error) {
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return err
	}

	if !skipAccessChecks(volume) {
		// Stat a volume entry.
		if err = Access(volumeDir); err != nil {
			return convertAccessError(err, errVolumeAccessDenied)
		}
	}

	for _, fp := range paths {
		// Following code is needed so that we retain SlashSeparator suffix if any in
		// path argument.
		filePath := pathJoin(volumeDir, fp)
		if err = checkPathLength(filePath); err != nil {
			return err
		}

		if err = s.moveToTrash(filePath, false, false); err != nil {
			return err
		}
	}

	return nil
}

// Delete - delete a file at path.
func (s *xlStorage) Delete(ctx context.Context, volume string, path string, deleteOpts DeleteOptions) (err error) {
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return err
	}

	if !skipAccessChecks(volume) {
		// Stat a volume entry.
		if err = Access(volumeDir); err != nil {
			return convertAccessError(err, errVolumeAccessDenied)
		}
	}

	// Following code is needed so that we retain SlashSeparator suffix if any in
	// path argument.
	filePath := pathJoin(volumeDir, path)
	if err = checkPathLength(filePath); err != nil {
		return err
	}

	// Delete file and delete parent directory as well if it's empty.
	return s.deleteFile(volumeDir, filePath, deleteOpts.Recursive, deleteOpts.Immediate)
}

func skipAccessChecks(volume string) (ok bool) {
	return strings.HasPrefix(volume, minioMetaBucket)
}

// RenameData - rename source path to destination path atomically, metadata and data directory.
func (s *xlStorage) RenameData(ctx context.Context, srcVolume, srcPath string, fi FileInfo, dstVolume, dstPath string, opts RenameOptions) (res RenameDataResp, err error) {
	defer func() {
		ignoredErrs := []error{
			errFileNotFound,
			errVolumeNotFound,
			errFileVersionNotFound,
			errDiskNotFound,
			errUnformattedDisk,
			errMaxVersionsExceeded,
			errFileAccessDenied,
		}
		if err != nil && !IsErr(err, ignoredErrs...) && !contextCanceled(ctx) {
			// Only log these errors if context is not yet canceled.
			storageLogOnceIf(ctx, fmt.Errorf("drive:%s, srcVolume: %s, srcPath: %s, dstVolume: %s:, dstPath: %s - error %v",
				s.drivePath,
				srcVolume, srcPath,
				dstVolume, dstPath,
				err), "xl-storage-rename-data-"+dstVolume)
		}
		if s.globalSync {
			globalSync()
		}
	}()

	srcVolumeDir, err := s.getVolDir(srcVolume)
	if err != nil {
		return res, err
	}

	dstVolumeDir, err := s.getVolDir(dstVolume)
	if err != nil {
		return res, err
	}

	if !skipAccessChecks(srcVolume) {
		// Stat a volume entry.
		if err = Access(srcVolumeDir); err != nil {
			return res, convertAccessError(err, errVolumeAccessDenied)
		}
	}

	if !skipAccessChecks(dstVolume) {
		if err = Access(dstVolumeDir); err != nil {
			return res, convertAccessError(err, errVolumeAccessDenied)
		}
	}

	srcFilePath := pathutil.Join(srcVolumeDir, pathJoin(srcPath, xlStorageFormatFile))
	dstFilePath := pathutil.Join(dstVolumeDir, pathJoin(dstPath, xlStorageFormatFile))

	var srcDataPath string
	var dstDataPath string
	var dataDir string
	if !fi.IsRemote() {
		dataDir = retainSlash(fi.DataDir)
	}
	if dataDir != "" {
		srcDataPath = retainSlash(pathJoin(srcVolumeDir, srcPath, dataDir))
		// make sure to always use path.Join here, do not use pathJoin as
		// it would additionally add `/` at the end and it comes in the
		// way of renameAll(), parentDir creation.
		dstDataPath = pathutil.Join(dstVolumeDir, dstPath, dataDir)
	}

	if err = checkPathLength(srcFilePath); err != nil {
		return res, err
	}

	if err = checkPathLength(dstFilePath); err != nil {
		return res, err
	}

	s.RLock()
	formatLegacy := s.formatLegacy
	s.RUnlock()

	dstBuf, err := xioutil.ReadFile(dstFilePath)
	if err != nil {
		// handle situations when dstFilePath is 'file'
		// for example such as someone is trying to
		// upload an object such as `prefix/object/xl.meta`
		// where `prefix/object` is already an object
		if isSysErrNotDir(err) && runtime.GOOS != globalWindowsOSName {
			// NOTE: On windows the error happens at
			// next line and returns appropriate error.
			return res, errFileAccessDenied
		}
		if !osIsNotExist(err) {
			return res, osErrToFileErr(err)
		}
		if formatLegacy {
			// errFileNotFound comes here.
			err = s.renameLegacyMetadata(dstVolumeDir, dstPath)
			if err != nil && err != errFileNotFound {
				return res, err
			}
			if err == nil {
				dstBuf, err = xioutil.ReadFile(dstFilePath)
				if err != nil && !osIsNotExist(err) {
					return res, osErrToFileErr(err)
				}
			}
		}
	}

	// Preserve all the legacy data, could be slow, but at max there can be 10,000 parts.
	currentDataPath := pathJoin(dstVolumeDir, dstPath)

	var xlMeta xlMetaV2
	var legacyPreserved bool
	var legacyEntries []string
	if len(dstBuf) > 0 {
		if isXL2V1Format(dstBuf) {
			if err = xlMeta.Load(dstBuf); err != nil {
				// Data appears corrupt. Drop data.
				xlMeta = xlMetaV2{}
			}
		} else {
			// This code-path is to preserve the legacy data.
			xlMetaLegacy := &xlMetaV1Object{}
			json := jsoniter.ConfigCompatibleWithStandardLibrary
			if err := json.Unmarshal(dstBuf, xlMetaLegacy); err != nil {
				storageLogOnceIf(ctx, err, "read-data-unmarshal-"+dstFilePath)
				// Data appears corrupt. Drop data.
			} else {
				xlMetaLegacy.DataDir = legacyDataDir
				if err = xlMeta.AddLegacy(xlMetaLegacy); err != nil {
					storageLogOnceIf(ctx, err, "read-data-add-legacy-"+dstFilePath)
				}
				legacyPreserved = true
			}
		}
	} else {
		// It is possible that some drives may not have `xl.meta` file
		// in such scenarios verify if at least `part.1` files exist
		// to verify for legacy version.
		if formatLegacy {
			// We only need this code if we are moving
			// from `xl.json` to `xl.meta`, we can avoid
			// one extra readdir operation here for all
			// new deployments.
			entries, err := readDir(currentDataPath)
			if err != nil && err != errFileNotFound {
				return res, osErrToFileErr(err)
			}
			for _, entry := range entries {
				if entry == xlStorageFormatFile || strings.HasSuffix(entry, slashSeparator) {
					continue
				}
				if strings.HasPrefix(entry, "part.") {
					legacyPreserved = true
					legacyEntries = entries
					break
				}
			}
		}
	}

	var legacyDataPath string
	if formatLegacy {
		legacyDataPath = pathJoin(dstVolumeDir, dstPath, legacyDataDir)
		if legacyPreserved {
			if contextCanceled(ctx) {
				return res, ctx.Err()
			}

			if len(legacyEntries) > 0 {
				// legacy data dir means its old content, honor system umask.
				if err = mkdirAll(legacyDataPath, 0o777, dstVolumeDir); err != nil {
					// any failed mkdir-calls delete them.
					s.deleteFile(dstVolumeDir, legacyDataPath, true, false)
					return res, osErrToFileErr(err)
				}
				for _, entry := range legacyEntries {
					// Skip xl.meta renames further, also ignore any directories such as `legacyDataDir`
					if entry == xlStorageFormatFile || strings.HasSuffix(entry, slashSeparator) {
						continue
					}

					if err = Rename(pathJoin(currentDataPath, entry), pathJoin(legacyDataPath, entry)); err != nil {
						// Any failed rename calls un-roll previous transaction.
						s.deleteFile(dstVolumeDir, legacyDataPath, true, false)

						return res, osErrToFileErr(err)
					}
				}
			}
		}
	}

	// Set skipParent to skip mkdirAll() calls for deeply nested objects
	// - if its an overwrite
	// - if its a versioned object
	//
	// This can potentiall reduce syscalls by strings.Split(path, "/")
	// times relative to the object name.
	skipParent := dstVolumeDir
	if len(dstBuf) > 0 {
		skipParent = pathutil.Dir(dstFilePath)
	}

	var reqVID string
	if fi.VersionID == "" {
		reqVID = nullVersionID
	} else {
		reqVID = fi.VersionID
	}

	// Empty fi.VersionID indicates that versioning is either
	// suspended or disabled on this bucket. RenameData will replace
	// the 'null' version. We add a free-version to track its tiered
	// content for asynchronous deletion.
	//
	// Note: RestoreObject and HealObject requests don't end up replacing the
	// null version and therefore don't require the free-version to track
	// anything
	if fi.VersionID == "" && !fi.IsRestoreObjReq() && !fi.Healing() {
		// Note: Restore object request reuses PutObject/Multipart
		// upload to copy back its data from the remote tier. This
		// doesn't replace the existing version, so we don't need to add
		// a free-version.
		xlMeta.AddFreeVersion(fi)
	}

	// indicates if RenameData() is called by healing.
	healing := fi.Healing()

	// Replace the data of null version or any other existing version-id
	_, ver, err := xlMeta.findVersionStr(reqVID)
	if err == nil {
		dataDir := ver.getDataDir()
		if dataDir != "" && (xlMeta.SharedDataDirCountStr(reqVID, dataDir) == 0) {
			// Purge the destination path as we are not preserving anything
			// versioned object was not requested.
			res.OldDataDir = dataDir
			if healing {
				// if old destination path is same as new destination path
				// there is nothing to purge, this is true in case of healing
				// avoid setting OldDataDir at that point.
				res.OldDataDir = ""
			} else {
				xlMeta.data.remove(reqVID, dataDir)
			}
		}
	}

	if err = xlMeta.AddVersion(fi); err != nil {
		if legacyPreserved {
			// Any failed rename calls un-roll previous transaction.
			s.deleteFile(dstVolumeDir, legacyDataPath, true, false)
		}
		return res, err
	}

	if len(xlMeta.versions) <= 10 {
		// any number of versions beyond this is excessive
		// avoid healing such objects in this manner, let
		// it heal during the regular scanner cycle.
		dst := []byte{}
		for _, ver := range xlMeta.versions {
			dst = slices.Grow(dst, 16)
			copy(dst[len(dst):], ver.header.VersionID[:])
		}
		res.Sign = dst
	}

	newDstBuf, err := xlMeta.AppendTo(metaDataPoolGet())
	defer metaDataPoolPut(newDstBuf)
	if err != nil {
		if legacyPreserved {
			s.deleteFile(dstVolumeDir, legacyDataPath, true, false)
		}
		return res, errFileCorrupt
	}

	if contextCanceled(ctx) {
		return res, ctx.Err()
	}

	if err = s.WriteAll(ctx, srcVolume, pathJoin(srcPath, xlStorageFormatFile), newDstBuf); err != nil {
		if legacyPreserved {
			s.deleteFile(dstVolumeDir, legacyDataPath, true, false)
		}
		return res, osErrToFileErr(err)
	}
	diskHealthCheckOK(ctx, err)

	notInline := srcDataPath != "" && len(fi.Data) == 0 && fi.Size > 0
	if notInline {
		if healing {
			// renameAll only for objects that have xl.meta not saved inline.
			// this must be done in healing only, otherwise it is expected
			// that for fresh PutObject() call dstDataPath can never exist.
			// if its an overwrite then the caller deletes the DataDir
			// in a separate RPC call.
			s.moveToTrash(dstDataPath, true, false)

			// If we are healing we should purge any legacyDataPath content,
			// that was previously preserved during PutObject() call
			// on a versioned bucket.
			s.moveToTrash(legacyDataPath, true, false)
		}
		if contextCanceled(ctx) {
			return res, ctx.Err()
		}
		if err = renameAll(srcDataPath, dstDataPath, skipParent); err != nil {
			if legacyPreserved {
				// Any failed rename calls un-roll previous transaction.
				s.deleteFile(dstVolumeDir, legacyDataPath, true, false)
			}
			// if its a partial rename() do not attempt to delete recursively.
			s.deleteFile(dstVolumeDir, dstDataPath, false, false)
			return res, osErrToFileErr(err)
		}
		diskHealthCheckOK(ctx, err)
	}

	// If we have oldDataDir then we must preserve current xl.meta
	// as backup, in-case needing renames().
	if res.OldDataDir != "" {
		if contextCanceled(ctx) {
			return res, ctx.Err()
		}

		// preserve current xl.meta inside the oldDataDir.
		if err = s.writeAll(ctx, dstVolume, pathJoin(dstPath, res.OldDataDir, xlStorageFormatFileBackup), dstBuf, true, skipParent); err != nil {
			if legacyPreserved {
				s.deleteFile(dstVolumeDir, legacyDataPath, true, false)
			}
			return res, osErrToFileErr(err)
		}
		diskHealthCheckOK(ctx, err)
	}

	if contextCanceled(ctx) {
		return res, ctx.Err()
	}

	// Commit meta-file
	if err = renameAll(srcFilePath, dstFilePath, skipParent); err != nil {
		if legacyPreserved {
			// Any failed rename calls un-roll previous transaction.
			s.deleteFile(dstVolumeDir, legacyDataPath, true, false)
		}
		// if its a partial rename() do not attempt to delete recursively.
		// this can be healed since all parts are available.
		s.deleteFile(dstVolumeDir, dstDataPath, false, false)
		return res, osErrToFileErr(err)
	}

	if srcVolume != minioMetaMultipartBucket {
		// srcFilePath is some-times minioMetaTmpBucket, an attempt to
		// remove the temporary folder is enough since at this point
		// ideally all transaction should be complete.
		Remove(pathutil.Dir(srcFilePath))
	} else {
		s.deleteFile(srcVolumeDir, pathutil.Dir(srcFilePath), true, false)
	}
	return res, nil
}

// RenamePart - rename part path to destination path atomically, this is meant to be used
// only with multipart API
func (s *xlStorage) RenamePart(ctx context.Context, srcVolume, srcPath, dstVolume, dstPath string, meta []byte, skipParent string) (err error) {
	srcVolumeDir, err := s.getVolDir(srcVolume)
	if err != nil {
		return err
	}
	dstVolumeDir, err := s.getVolDir(dstVolume)
	if err != nil {
		return err
	}
	if !skipAccessChecks(srcVolume) {
		// Stat a volume entry.
		if err = Access(srcVolumeDir); err != nil {
			if osIsNotExist(err) {
				return errVolumeNotFound
			} else if isSysErrIO(err) {
				return errFaultyDisk
			}
			return err
		}
	}
	if !skipAccessChecks(dstVolume) {
		if err = Access(dstVolumeDir); err != nil {
			if osIsNotExist(err) {
				return errVolumeNotFound
			} else if isSysErrIO(err) {
				return errFaultyDisk
			}
			return err
		}
	}

	srcIsDir := HasSuffix(srcPath, SlashSeparator)
	dstIsDir := HasSuffix(dstPath, SlashSeparator)
	// either source or destination is a directory return error.
	if srcIsDir || dstIsDir {
		return errFileAccessDenied
	}

	srcFilePath := pathutil.Join(srcVolumeDir, srcPath)
	if err = checkPathLength(srcFilePath); err != nil {
		return err
	}

	dstFilePath := pathutil.Join(dstVolumeDir, dstPath)
	if err = checkPathLength(dstFilePath); err != nil {
		return err
	}
	// when skipParent is from rpc. it’s ok for not adding another rpc HandlerID like HandlerRenamePart2
	// For this case, skipParent is empty, destBaseDir is equal to dstVolumeDir, that behavior is the same as the previous one
	destBaseDir := pathutil.Join(dstVolumeDir, skipParent)
	if err = checkPathLength(destBaseDir); err != nil {
		return err
	}

	if err = renameAll(srcFilePath, dstFilePath, destBaseDir); err != nil {
		if isSysErrNotEmpty(err) || isSysErrNotDir(err) {
			return errFileAccessDenied
		}
		err = osErrToFileErr(err)
		if errors.Is(err, errFileNotFound) || errors.Is(err, errFileAccessDenied) {
			return errUploadIDNotFound
		}
		return err
	}

	if err = s.WriteAll(ctx, dstVolume, dstPath+".meta", meta); err != nil {
		return osErrToFileErr(err)
	}

	// Remove parent dir of the source file if empty
	parentDir := pathutil.Dir(srcFilePath)
	s.deleteFile(srcVolumeDir, parentDir, false, false)

	return nil
}

// RenameFile - rename source path to destination path atomically.
func (s *xlStorage) RenameFile(ctx context.Context, srcVolume, srcPath, dstVolume, dstPath string) (err error) {
	srcVolumeDir, err := s.getVolDir(srcVolume)
	if err != nil {
		return err
	}
	dstVolumeDir, err := s.getVolDir(dstVolume)
	if err != nil {
		return err
	}
	if !skipAccessChecks(srcVolume) {
		// Stat a volume entry.
		if err = Access(srcVolumeDir); err != nil {
			if osIsNotExist(err) {
				return errVolumeNotFound
			} else if isSysErrIO(err) {
				return errFaultyDisk
			}
			return err
		}
	}
	if !skipAccessChecks(dstVolume) {
		if err = Access(dstVolumeDir); err != nil {
			if osIsNotExist(err) {
				return errVolumeNotFound
			} else if isSysErrIO(err) {
				return errFaultyDisk
			}
			return err
		}
	}
	srcIsDir := HasSuffix(srcPath, SlashSeparator)
	dstIsDir := HasSuffix(dstPath, SlashSeparator)
	// Either src and dst have to be directories or files, else return error.
	if (!srcIsDir || !dstIsDir) && (srcIsDir || dstIsDir) {
		return errFileAccessDenied
	}
	srcFilePath := pathutil.Join(srcVolumeDir, srcPath)
	if err = checkPathLength(srcFilePath); err != nil {
		return err
	}
	dstFilePath := pathutil.Join(dstVolumeDir, dstPath)
	if err = checkPathLength(dstFilePath); err != nil {
		return err
	}
	if srcIsDir {
		// If source is a directory, we expect the destination to be non-existent but we
		// we still need to allow overwriting an empty directory since it represents
		// an object empty directory.
		dirInfo, err := Lstat(dstFilePath)
		if isSysErrIO(err) {
			return errFaultyDisk
		}
		if err != nil {
			if !osIsNotExist(err) {
				return err
			}
		} else {
			if !dirInfo.IsDir() {
				return errFileAccessDenied
			}
			if err = Remove(dstFilePath); err != nil {
				if isSysErrNotEmpty(err) || isSysErrNotDir(err) {
					return errFileAccessDenied
				} else if isSysErrIO(err) {
					return errFaultyDisk
				}
				return err
			}
		}
	}

	if err = renameAll(srcFilePath, dstFilePath, dstVolumeDir); err != nil {
		if isSysErrNotEmpty(err) || isSysErrNotDir(err) {
			return errFileAccessDenied
		}
		return osErrToFileErr(err)
	}

	// Remove parent dir of the source file if empty
	parentDir := pathutil.Dir(srcFilePath)
	s.deleteFile(srcVolumeDir, parentDir, false, false)

	return nil
}

func (s *xlStorage) bitrotVerify(ctx context.Context, partPath string, partSize int64, algo BitrotAlgorithm, sum []byte, shardSize int64) error {
	// Open the file for reading.
	file, err := OpenFile(partPath, readMode, 0o666)
	if err != nil {
		return osErrToFileErr(err)
	}

	// Close the file descriptor.
	defer file.Close()
	fi, err := file.Stat()
	if err != nil {
		// Unable to stat on the file, return an expected error
		// for healing code to fix this file.
		return err
	}
	return bitrotVerify(diskHealthReader(ctx, file), fi.Size(), partSize, algo, sum, shardSize)
}

func (s *xlStorage) VerifyFile(ctx context.Context, volume, path string, fi FileInfo) (*CheckPartsResp, error) {
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return nil, err
	}

	if !skipAccessChecks(volume) {
		// Stat a volume entry.
		if err = Access(volumeDir); err != nil {
			return nil, convertAccessError(err, errVolumeAccessDenied)
		}
	}

	resp := CheckPartsResp{
		// By default, the result is unknown per part
		Results: make([]int, len(fi.Parts)),
	}

	erasure := fi.Erasure
	for i, part := range fi.Parts {
		checksumInfo := erasure.GetChecksumInfo(part.Number)
		partPath := pathJoin(volumeDir, path, fi.DataDir, fmt.Sprintf("part.%d", part.Number))
		err := s.bitrotVerify(ctx, partPath,
			erasure.ShardFileSize(part.Size),
			checksumInfo.Algorithm,
			checksumInfo.Hash, erasure.ShardSize())

		resp.Results[i] = convPartErrToInt(err)

		// Only log unknown errors
		if resp.Results[i] == checkPartUnknown && err != errFileAccessDenied {
			logger.GetReqInfo(ctx).AppendTags("disk", s.String())
			storageLogOnceIf(ctx, err, partPath)
		}
	}

	return &resp, nil
}

func (s *xlStorage) ReadParts(ctx context.Context, volume string, partMetaPaths ...string) ([]*ObjectPartInfo, error) {
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return nil, err
	}

	parts := make([]*ObjectPartInfo, len(partMetaPaths))
	for idx, partMetaPath := range partMetaPaths {
		var partNumber int
		fmt.Sscanf(pathutil.Base(partMetaPath), "part.%d.meta", &partNumber)

		if contextCanceled(ctx) {
			parts[idx] = &ObjectPartInfo{
				Error:  ctx.Err().Error(),
				Number: partNumber,
			}
			continue
		}

		if err := Access(pathJoin(volumeDir, pathutil.Dir(partMetaPath), fmt.Sprintf("part.%d", partNumber))); err != nil {
			parts[idx] = &ObjectPartInfo{
				Error:  err.Error(),
				Number: partNumber,
			}
			continue
		}

		data, err := s.readAllData(ctx, volume, volumeDir, pathJoin(volumeDir, partMetaPath))
		if err != nil {
			parts[idx] = &ObjectPartInfo{
				Error:  err.Error(),
				Number: partNumber,
			}
			continue
		}

		pinfo := &ObjectPartInfo{}
		if _, err = pinfo.UnmarshalMsg(data); err != nil {
			parts[idx] = &ObjectPartInfo{
				Error:  err.Error(),
				Number: partNumber,
			}
			continue
		}

		parts[idx] = pinfo
	}
	diskHealthCheckOK(ctx, nil)
	return parts, nil
}

// ReadMultiple will read multiple files and send each back as response.
// Files are read and returned in the given order.
// The resp channel is closed before the call returns.
// Only a canceled context will return an error.
func (s *xlStorage) ReadMultiple(ctx context.Context, req ReadMultipleReq, resp chan<- ReadMultipleResp) error {
	defer xioutil.SafeClose(resp)

	volumeDir := pathJoin(s.drivePath, req.Bucket)
	found := 0
	for _, f := range req.Files {
		if contextCanceled(ctx) {
			return ctx.Err()
		}
		r := ReadMultipleResp{
			Bucket: req.Bucket,
			Prefix: req.Prefix,
			File:   f,
		}

		var data []byte
		var mt time.Time

		fullPath := pathJoin(volumeDir, req.Prefix, f)
		w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
		if err := w.Run(func() (err error) {
			if req.MetadataOnly {
				data, mt, err = s.readMetadataWithDMTime(ctx, fullPath)
			} else {
				data, mt, err = s.readAllDataWithDMTime(ctx, req.Bucket, volumeDir, fullPath)
			}
			return err
		}); err != nil {
			if !IsErr(err, errFileNotFound, errVolumeNotFound) {
				r.Exists = true
				r.Error = err.Error()
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case resp <- r:
			}
			if req.AbortOn404 && !r.Exists {
				// We stop at first file not found.
				// We have already reported the error, return nil.
				return nil
			}
			continue
		}
		diskHealthCheckOK(ctx, nil)
		if req.MaxSize > 0 && int64(len(data)) > req.MaxSize {
			r.Exists = true
			r.Error = fmt.Sprintf("max size (%d) exceeded: %d", req.MaxSize, len(data))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case resp <- r:
				continue
			}
		}
		found++
		r.Exists = true
		r.Data = data
		r.Modtime = mt
		select {
		case <-ctx.Done():
			return ctx.Err()
		case resp <- r:
		}
		if req.MaxResults > 0 && found >= req.MaxResults {
			return nil
		}
	}
	return nil
}

func (s *xlStorage) StatInfoFile(ctx context.Context, volume, path string, glob bool) (stat []StatInfo, err error) {
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return stat, err
	}

	files := []string{pathJoin(volumeDir, path)}
	if glob {
		files, err = filepathx.Glob(filepath.Join(volumeDir, path))
		if err != nil {
			return nil, err
		}
	}
	for _, filePath := range files {
		if err := checkPathLength(filePath); err != nil {
			return stat, err
		}
		st, _ := Lstat(filePath)
		if st == nil {
			if !skipAccessChecks(volume) {
				// Stat a volume entry.
				if verr := Access(volumeDir); verr != nil {
					return stat, convertAccessError(verr, errVolumeAccessDenied)
				}
			}
			return stat, errPathNotFound
		}
		name, err := filepath.Rel(volumeDir, filePath)
		if err != nil {
			name = filePath
		}
		stat = append(stat, StatInfo{
			Name:    filepath.ToSlash(name),
			Size:    st.Size(),
			Dir:     st.IsDir(),
			Mode:    uint32(st.Mode()),
			ModTime: st.ModTime(),
		})
	}
	return stat, nil
}

// CleanAbandonedData will read metadata of the object on disk
// and delete any data directories and inline data that isn't referenced in metadata.
// Metadata itself is not modified, only inline data.
func (s *xlStorage) CleanAbandonedData(ctx context.Context, volume string, path string) error {
	if volume == "" || path == "" {
		return nil // Ignore
	}

	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return err
	}
	baseDir := pathJoin(volumeDir, path+slashSeparator)
	metaPath := pathutil.Join(baseDir, xlStorageFormatFile)
	buf, err := s.readAllData(ctx, volume, volumeDir, metaPath)
	if err != nil {
		return err
	}
	defer metaDataPoolPut(buf)

	if !isXL2V1Format(buf) {
		return nil
	}
	var xl xlMetaV2
	err = xl.LoadOrConvert(buf)
	if err != nil {
		return err
	}
	foundDirs := make(map[string]struct{}, len(xl.versions))
	err = readDirFn(baseDir, func(name string, typ os.FileMode) error {
		if !typ.IsDir() {
			return nil
		}
		// See if directory has a UUID name.
		base := filepath.Base(name)
		_, err := uuid.Parse(base)
		if err == nil {
			foundDirs[base] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return err
	}
	wantDirs, err := xl.getDataDirs()
	if err != nil {
		return err
	}

	// Delete all directories we expect to be there.
	for _, dir := range wantDirs {
		delete(foundDirs, dir)
	}

	// Delete excessive directories.
	// Do not abort on context errors.
	for dir := range foundDirs {
		toRemove := pathJoin(volumeDir, path, dir+SlashSeparator)
		err = s.deleteFile(volumeDir, toRemove, true, true)
		diskHealthCheckOK(ctx, err)
		if err != nil {
			return err
		}
	}

	// Do the same for inline data
	dirs, err := xl.data.list()
	if err != nil {
		return err
	}

	// Clear and repopulate
	clear(foundDirs)

	// Populate into map
	for _, k := range dirs {
		foundDirs[k] = struct{}{}
	}

	// Delete all directories we expect to be there.
	for _, dir := range wantDirs {
		delete(foundDirs, dir)
	}

	// Nothing to delete
	if len(foundDirs) == 0 {
		return nil
	}

	// Delete excessive inline entries.
	// Convert to slice.
	dirs = dirs[:0]
	for dir := range foundDirs {
		dirs = append(dirs, dir)
	}
	if xl.data.remove(dirs...) {
		newBuf, err := xl.AppendTo(metaDataPoolGet())
		if err == nil {
			defer metaDataPoolPut(newBuf)
			return s.WriteAll(ctx, volume, pathJoin(path, xlStorageFormatFile), buf)
		}
	}
	return nil
}

func convertAccessError(err, permErr error) error {
	switch {
	case osIsNotExist(err):
		return errVolumeNotFound
	case isSysErrIO(err):
		return errFaultyDisk
	case osIsPermission(err):
		return permErr
	default:
		return err
	}
}
