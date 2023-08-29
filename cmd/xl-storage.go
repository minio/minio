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
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	pathutil "path"
	"path/filepath"
	"runtime"
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
	"github.com/minio/minio/internal/disk"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/logger"
	"github.com/zeebo/xxh3"
)

const (
	nullVersionID = "null"
	// Largest streams threshold per shard.
	largestFileThreshold = 64 * humanize.MiByte // Optimized for HDDs

	// Small file threshold below which data accompanies metadata from storage layer.
	smallFileThreshold = 128 * humanize.KiByte // Optimized for NVMe/SSDs

	// For hardrives it is possible to set this to a lower value to avoid any
	// spike in latency. But currently we are simply keeping it optimal for SSDs.

	// bigFileThreshold is the point where we add readahead to put operations.
	bigFileThreshold = 128 * humanize.MiByte

	// XL metadata file carries per object metadata.
	xlStorageFormatFile = "xl.meta"
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
	drivePath string
	endpoint  Endpoint

	globalSync bool
	oDirect    bool // indicates if this disk supports ODirect
	rootDisk   bool

	diskID string

	// Indexes, will be -1 until assigned a set.
	poolIndex, setIndex, diskIndex int

	// Indicate of NSScanner is in progress in this disk
	scanning int32

	formatFileInfo  os.FileInfo
	formatLegacy    bool
	formatLastCheck time.Time

	diskInfoCache timedValue
	sync.RWMutex

	formatData []byte

	nrRequests uint64

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
		if err = mkdirAll(path, 0o777); err != nil {
			return path, err
		}
	}
	if fi != nil && !fi.IsDir() {
		return path, errDiskNotDir
	}

	return path, nil
}

// isDirEmpty - returns whether given directory is empty or not.
func isDirEmpty(dirname string) bool {
	entries, err := readDirN(dirname, 1)
	if err != nil {
		return false
	}
	return len(entries) == 0
}

// Initialize a new storage disk.
func newLocalXLStorage(path string) (*xlStorage, error) {
	u := url.URL{Path: path}
	return newXLStorage(Endpoint{
		URL:     &u,
		IsLocal: true,
	}, true)
}

// Initialize a new storage disk.
func newXLStorage(ep Endpoint, cleanUp bool) (s *xlStorage, err error) {
	path := ep.Path
	if path, err = getValidPath(path); err != nil {
		return nil, err
	}

	info, err := disk.GetInfo(path, true)
	if err != nil {
		return nil, err
	}

	var rootDisk bool
	if !globalIsCICD && !globalIsErasureSD {
		if globalRootDiskThreshold > 0 {
			// Use MINIO_ROOTDISK_THRESHOLD_SIZE to figure out if
			// this disk is a root disk. treat those disks with
			// size less than or equal to the threshold as rootDisks.
			rootDisk = info.Total <= globalRootDiskThreshold
		} else {
			rootDisk, err = disk.IsRootDisk(path, SlashSeparator)
			if err != nil {
				return nil, err
			}
		}
	}

	s = &xlStorage{
		drivePath:  path,
		endpoint:   ep,
		globalSync: globalFSOSync,
		rootDisk:   rootDisk,
		poolIndex:  -1,
		setIndex:   -1,
		diskIndex:  -1,
	}

	// Sanitaize before setting it
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
			return nil, errDiskAccessDenied
		} else if isSysErrIO(err) {
			return nil, errFaultyDisk
		}
		return nil, err
	}
	s.formatData = formatData
	s.formatFileInfo = formatFi

	if len(s.formatData) == 0 {
		// Create all necessary bucket folders if possible.
		if err = makeFormatErasureMetaVolumes(s); err != nil {
			return nil, err
		}
	} else {
		format := &formatErasureV3{}
		json := jsoniter.ConfigCompatibleWithStandardLibrary
		if err = json.Unmarshal(s.formatData, &format); err != nil {
			return s, errCorruptedFormat
		}
		s.diskID = format.Erasure.This
		s.formatLastCheck = time.Now()
		s.formatLegacy = format.Erasure.DistributionAlgo == formatErasureVersionV2DistributionAlgoV1
	}

	if globalAPIConfig.odirectEnabled() {
		// Return an error if ODirect is not supported
		// unless it is a single erasure disk mode
		if err := s.checkODirectDiskSupport(); err == nil {
			s.oDirect = true
		} else {
			// Allow if unsupported platform or single disk.
			if errors.Is(err, errUnsupportedDisk) && globalIsErasureSD || !disk.ODirectPlatform {
				s.oDirect = false
			} else {
				return s, err
			}
		}
	}

	// Success.
	return s, nil
}

// getDiskInfo returns given disk information.
func getDiskInfo(drivePath string) (di disk.Info, err error) {
	if err = checkPathLength(drivePath); err == nil {
		di, err = disk.GetInfo(drivePath, false)
	}
	switch {
	case osIsNotExist(err):
		err = errDiskNotFound
	case isSysErrTooLong(err):
		err = errFileNameTooLong
	case isSysErrIO(err):
		err = errFaultyDisk
	}

	return di, err
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

func (*xlStorage) Close() error {
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
	s.RLock()
	defer s.RUnlock()
	// If unset, see if we can locate it.
	if s.poolIndex < 0 || s.setIndex < 0 || s.diskIndex < 0 {
		return getXLDiskLoc(s.diskID)
	}
	return s.poolIndex, s.setIndex, s.diskIndex
}

// Set location indexes.
func (s *xlStorage) SetDiskLoc(poolIdx, setIdx, diskIdx int) {
	s.poolIndex = poolIdx
	s.setIndex = setIdx
	s.diskIndex = diskIdx
}

func (s *xlStorage) Healing() *healingTracker {
	healingFile := pathJoin(s.drivePath, minioMetaBucket,
		bucketMetaPrefix, healingTrackerFilename)
	b, err := os.ReadFile(healingFile)
	if err != nil {
		return nil
	}
	h := newHealingTracker()
	_, err = h.UnmarshalMsg(b)
	logger.LogIf(GlobalContext, err)
	return h
}

// checkODirectDiskSupport asks the disk to write some data
// with O_DIRECT support, return an error if any and return
// errUnsupportedDisk if there is no O_DIRECT support
func (s *xlStorage) checkODirectDiskSupport() error {
	if !disk.ODirectPlatform {
		return errUnsupportedDisk
	}

	// Check if backend is writable and supports O_DIRECT
	uuid := mustGetUUID()
	filePath := pathJoin(s.drivePath, minioMetaTmpDeletedBucket, ".writable-check-"+uuid+".tmp")
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
	var buf []byte
	err := xioutil.NewDeadlineWorker(diskMaxTimeout).Run(func() error {
		var rerr error
		buf, _, rerr = s.readMetadataWithDMTime(ctx, itemPath)
		return rerr
	})
	return buf, err
}

func (s *xlStorage) NSScanner(ctx context.Context, cache dataUsageCache, updates chan<- dataUsageEntry, scanMode madmin.HealScanMode) (dataUsageCache, error) {
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
	defer close(updates)
	var lc *lifecycle.Lifecycle

	// Check if the current bucket has a configured lifecycle policy
	if globalLifecycleSys != nil {
		lc, err = globalLifecycleSys.Get(cache.Info.Name)
		if err == nil && lc.HasActiveRules("") {
			cache.Info.lifeCycle = lc
		}
	}

	// Check if the current bucket has replication configuration
	if rcfg, _, err := globalBucketMetadataSys.GetReplicationConfig(ctx, cache.Info.Name); err == nil {
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

	dataUsageInfo, err := scanDataFolder(ctx, disks, s.drivePath, cache, func(item scannerItem) (sizeSummary, error) {
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

		sizeS := sizeSummary{}
		var noTiers bool
		if noTiers = globalTierConfigMgr.Empty(); !noTiers {
			sizeS.tiers = make(map[string]tierStats)
		}

		done := globalScannerMetrics.time(scannerMetricApplyAll)
		objInfos, err := item.applyVersionActions(ctx, objAPI, fivs.Versions)
		done()

		if err != nil {
			res["err"] = err.Error()
			return sizeSummary{}, errSkipFile
		}

		versioned := vcfg != nil && vcfg.Versioned(item.objectPath())

		for _, oi := range objInfos {
			done = globalScannerMetrics.time(scannerMetricApplyVersion)
			sz := item.applyActions(ctx, objAPI, oi, &sizeS)
			done()

			actualSz, err := oi.GetActualSize()
			if err != nil {
				continue
			}

			if oi.DeleteMarker {
				sizeS.deleteMarkers++
			}
			if oi.VersionID != "" && sz == actualSz {
				sizeS.versions++
			}
			sizeS.totalSize += sz

			// Skip tier accounting if,
			// 1. no tiers configured
			// 2. object version is a delete-marker or a free-version
			//    tracking deleted transitioned objects
			switch {
			case noTiers, oi.DeleteMarker, oi.TransitionedObject.FreeVersion:
				continue
			}
			tier := minioHotTier
			if oi.TransitionedObject.Status == lifecycle.TransitionComplete {
				tier = oi.TransitionedObject.Tier
			}
			sizeS.tiers[tier] = sizeS.tiers[tier].add(oi.tierStats())
		}

		// apply tier sweep action on free versions
		if len(fivs.FreeVersions) > 0 {
			res["free-versions"] = strconv.Itoa(len(fivs.FreeVersions))
		}
		for _, freeVersion := range fivs.FreeVersions {
			oi := freeVersion.ToObjectInfo(item.bucket, item.objectPath(), versioned)
			done = globalScannerMetrics.time(scannerMetricTierObjSweep)
			item.applyTierObjSweep(ctx, objAPI, oi)
			done()
		}

		// These are rather expensive. Skip if nobody listens.
		if globalTrace.NumSubscribers(madmin.TraceScanner) > 0 {
			if sizeS.versions > 0 {
				res["versions"] = strconv.FormatUint(sizeS.versions, 10)
			}
			res["size"] = strconv.FormatInt(sizeS.totalSize, 10)
			if len(sizeS.tiers) > 0 {
				for name, tier := range sizeS.tiers {
					res["size-"+name] = strconv.FormatUint(tier.TotalSize, 10)
					res["versions-"+name] = strconv.Itoa(tier.NumVersions)
				}
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
		return sizeS, nil
	}, scanMode)
	if err != nil {
		return dataUsageInfo, err
	}

	dataUsageInfo.Info.LastUpdate = time.Now()
	return dataUsageInfo, nil
}

// DiskInfo provides current information about disk space usage,
// total free inodes and underlying filesystem.
func (s *xlStorage) DiskInfo(_ context.Context, _ bool) (info DiskInfo, err error) {
	s.diskInfoCache.Once.Do(func() {
		s.diskInfoCache.TTL = time.Second
		s.diskInfoCache.Update = func() (interface{}, error) {
			dcinfo := DiskInfo{
				RootDisk:  s.rootDisk,
				MountPath: s.drivePath,
				Endpoint:  s.endpoint.String(),
			}
			di, err := getDiskInfo(s.drivePath)
			if err != nil {
				return dcinfo, err
			}
			dcinfo.Major = di.Major
			dcinfo.Minor = di.Minor
			dcinfo.Total = di.Total
			dcinfo.Free = di.Free
			dcinfo.Used = di.Used
			dcinfo.UsedInodes = di.Files - di.Ffree
			dcinfo.FreeInodes = di.Ffree
			dcinfo.FSType = di.FSType
			dcinfo.NRRequests = s.nrRequests
			dcinfo.Rotational = s.rotational
			diskID, err := s.GetDiskID()
			// Healing is 'true' when
			// - if we found an unformatted disk (no 'format.json')
			// - if we found healing tracker 'healing.bin'
			dcinfo.Healing = errors.Is(err, errUnformattedDisk) || (s.Healing() != nil)
			dcinfo.Scanning = atomic.LoadInt32(&s.scanning) == 1
			dcinfo.ID = diskID
			return dcinfo, err
		}
	})

	v, err := s.diskInfoCache.Get()
	if v != nil {
		info = v.(DiskInfo)
	}
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
	formatFile := pathJoin(s.drivePath, minioMetaBucket, formatConfigFile)
	fi, err := Lstat(formatFile)
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
			logger.LogOnceIf(GlobalContext, err, "check-format-json") // log unexpected errors
			return nil, errCorruptedFormat
		} else if osIsPermission(err) {
			return nil, errDiskAccessDenied
		}
		logger.LogOnceIf(GlobalContext, err, "check-format-json") // log unexpected errors
		return nil, errCorruptedFormat
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

	formatFile := pathJoin(s.drivePath, minioMetaBucket, formatConfigFile)
	b, err := os.ReadFile(formatFile)
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
			logger.LogOnceIf(GlobalContext, err, "check-format-json") // log unexpected errors
			return "", errCorruptedFormat
		} else if osIsPermission(err) {
			return "", errDiskAccessDenied
		}
		logger.LogOnceIf(GlobalContext, err, "check-format-json") // log unexpected errors
		return "", errCorruptedFormat
	}

	format := &formatErasureV3{}
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	if err = json.Unmarshal(b, &format); err != nil {
		logger.LogOnceIf(GlobalContext, err, "check-format-json") // log unexpected errors
		return "", errCorruptedFormat
	}

	s.Lock()
	defer s.Unlock()
	s.formatData = b
	s.diskID = format.Erasure.This
	s.formatLegacy = format.Erasure.DistributionAlgo == formatErasureVersionV2DistributionAlgoV1
	s.formatFileInfo = fi
	s.formatLastCheck = time.Now()
	return s.diskID, nil
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
			err = mkdirAll(volumeDir, 0o777)
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
func (s *xlStorage) ListDir(ctx context.Context, volume, dirPath string, count int) (entries []string, err error) {
	if contextCanceled(ctx) {
		return nil, ctx.Err()
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
		if err == errFileNotFound {
			if !skipAccessChecks(volume) {
				if ierr := Access(volumeDir); ierr != nil {
					if osIsNotExist(ierr) {
						return nil, errVolumeNotFound
					} else if isSysErrIO(ierr) {
						return nil, errFaultyDisk
					}
				}
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

	var legacyJSON bool
	buf, _, err := s.readAllData(ctx, volume, volumeDir, pathJoin(volumeDir, path, xlStorageFormatFile), false)
	if err != nil {
		if !errors.Is(err, errFileNotFound) {
			return err
		}

		s.RLock()
		legacy := s.formatLegacy
		s.RUnlock()
		if legacy {
			buf, _, err = s.readAllData(ctx, volume, volumeDir, pathJoin(volumeDir, path, xlStorageFormatFileV1), false)
			if err != nil {
				return err
			}
			legacyJSON = true
		}
	}

	if len(buf) == 0 {
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

	// Move xl.meta to trash
	err = s.moveToTrash(pathJoin(volumeDir, path, xlStorageFormatFile), false, false)
	if err == nil || err == errFileNotFound {
		s.deleteFile(volumeDir, pathJoin(volumeDir, path), false, false)
	}
	return err
}

// DeleteVersions deletes slice of versions, it can be same object
// or multiple objects.
func (s *xlStorage) DeleteVersions(ctx context.Context, volume string, versions []FileInfoVersions) []error {
	errs := make([]error, len(versions))

	for i, fiv := range versions {
		if contextCanceled(ctx) {
			errs[i] = ctx.Err()
			continue
		}
		w := xioutil.NewDeadlineWorker(diskMaxTimeout)
		if err := w.Run(func() error { return s.deleteVersions(ctx, volume, fiv.Name, fiv.Versions...) }); err != nil {
			errs[i] = err
		}
		diskHealthCheckOK(ctx, errs[i])
	}

	return errs
}

func (s *xlStorage) moveToTrash(filePath string, recursive, force bool) error {
	pathUUID := mustGetUUID()
	targetPath := pathutil.Join(s.drivePath, minioMetaTmpDeletedBucket, pathUUID)

	var renameFn func(source, target string) error
	if recursive {
		renameFn = renameAll
	} else {
		renameFn = Rename
	}

	if err := renameFn(filePath, targetPath); err != nil {
		return err
	}
	// immediately purge the target
	if force {
		removeAll(targetPath)
	}

	return nil
}

// DeleteVersion - deletes FileInfo metadata for path at `xl.meta`. forceDelMarker
// will force creating a new `xl.meta` to create a new delete marker
func (s *xlStorage) DeleteVersion(ctx context.Context, volume, path string, fi FileInfo, forceDelMarker bool) error {
	if HasSuffix(path, SlashSeparator) {
		return s.Delete(ctx, volume, path, DeleteOptions{
			Recursive: false,
			Force:     false,
		})
	}

	var legacyJSON bool
	buf, err := s.ReadAll(ctx, volume, pathJoin(path, xlStorageFormatFile))
	if err != nil {
		if !errors.Is(err, errFileNotFound) {
			return err
		}
		metaDataPoolPut(buf) // Never used, return it
		if fi.Deleted && forceDelMarker {
			// Create a new xl.meta with a delete marker in it
			return s.WriteMetadata(ctx, volume, path, fi)
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

	volumeDir, err := s.getVolDir(volume)
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

		return s.WriteAll(ctx, volume, pathJoin(path, xlStorageFormatFile), buf)
	}

	// No more versions, this is the last version purge everything.
	filePath := pathJoin(volumeDir, path)
	if err = checkPathLength(filePath); err != nil {
		return err
	}

	return s.deleteFile(volumeDir, filePath, true, false)
}

// UpdateMetadataOpts provides an optional input to indicate if xl.meta updates need to be fully synced to disk.
type UpdateMetadataOpts struct {
	NoPersistence bool
}

// Updates only metadata for a given version.
func (s *xlStorage) UpdateMetadata(ctx context.Context, volume, path string, fi FileInfo, opts UpdateMetadataOpts) error {
	if len(fi.Metadata) == 0 {
		return errInvalidArgument
	}

	buf, err := s.ReadAll(ctx, volume, pathJoin(path, xlStorageFormatFile))
	if err != nil {
		if err == errFileNotFound {
			if fi.VersionID != "" {
				return errFileVersionNotFound
			}
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

	return s.writeAll(ctx, volume, pathJoin(path, xlStorageFormatFile), wbuf, !opts.NoPersistence)
}

// WriteMetadata - writes FileInfo metadata for path at `xl.meta`
func (s *xlStorage) WriteMetadata(ctx context.Context, volume, path string, fi FileInfo) error {
	if fi.Fresh {
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
		// - newMultipartUpload() call..
		return s.writeAll(ctx, volume, pathJoin(path, xlStorageFormatFile), buf, false)
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
	if readData {
		buf, dmTime, err = s.readAllData(ctx, volume, volumeDir, pathJoin(filePath, xlStorageFormatFile), true)
	} else {
		buf, dmTime, err = s.readMetadataWithDMTime(ctx, pathJoin(filePath, xlStorageFormatFile))
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

	if err != nil {
		if err == errFileNotFound {
			buf, dmTime, err = s.readAllData(ctx, volume, volumeDir, pathJoin(filePath, xlStorageFormatFileV1), true)
			if err != nil {
				return nil, time.Time{}, err
			}
		} else {
			return nil, time.Time{}, err
		}
	}

	if len(buf) == 0 {
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

	buf, dmTime, err := s.readRaw(ctx, volume, volumeDir, filePath, readData)
	return RawFileInfo{
		Buf:       buf,
		DiskMTime: dmTime,
	}, err
}

// ReadVersion - reads metadata and returns FileInfo at path `xl.meta`
// for all objects less than `32KiB` this call returns data as well
// along with metadata.
func (s *xlStorage) ReadVersion(ctx context.Context, volume, path, versionID string, readData bool) (fi FileInfo, err error) {
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return fi, err
	}
	// Validate file path length, before reading.
	filePath := pathJoin(volumeDir, path)
	if err = checkPathLength(filePath); err != nil {
		return fi, err
	}

	buf, dmTime, err := s.readRaw(ctx, volume, volumeDir, filePath, readData)
	if err != nil {
		if err == errFileNotFound {
			if versionID != "" {
				return fi, errFileVersionNotFound
			}
		}
		return fi, err
	}

	fi, err = getFileInfo(buf, volume, path, versionID, readData, true)
	if err != nil {
		return fi, err
	}
	fi.DiskMTime = dmTime

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
			if fi.Size == 0 || !(fi.VersionID != "" && fi.VersionID != nullVersionID) {
				// If versioned we have no conflicts.
				fi.SetInlineData()
				return fi, nil
			}

			// For overwritten objects without header we might have a conflict with
			// data written later.
			// Check the data path if there is a part with data.
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

		// Reading data for small objects when
		// - object has not yet transitioned
		// - object size lesser than 128KiB
		// - object has maximum of 1 parts
		if fi.TransitionStatus == "" &&
			fi.DataDir != "" && fi.Size <= smallFileThreshold &&
			len(fi.Parts) == 1 {
			partPath := fmt.Sprintf("part.%d", fi.Parts[0].Number)
			dataPath := pathJoin(volumeDir, path, fi.DataDir, partPath)
			fi.Data, _, err = s.readAllData(ctx, volume, volumeDir, dataPath, true)
			if err != nil {
				return FileInfo{}, err
			}
		}
	}

	return fi, nil
}

func (s *xlStorage) readAllData(ctx context.Context, volume, volumeDir string, filePath string, sync bool) (buf []byte, dmTime time.Time, err error) {
	if contextCanceled(ctx) {
		return nil, time.Time{}, ctx.Err()
	}

	odirectEnabled := globalAPIConfig.odirectEnabled() && s.oDirect
	var f *os.File
	if odirectEnabled && sync {
		f, err = OpenFileDirectIO(filePath, readMode, 0o666)
	} else {
		f, err = OpenFile(filePath, readMode, 0o666)
	}
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
	r := io.Reader(f)
	var dr *xioutil.ODirectReader
	if odirectEnabled {
		dr = &xioutil.ODirectReader{
			File:      f,
			SmallFile: true,
		}
		defer dr.Close()
		r = dr
	} else {
		defer f.Close()
	}

	// Get size for precise allocation.
	stat, err := f.Stat()
	if err != nil {
		buf, err = io.ReadAll(diskHealthReader(ctx, r))
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
	if dr != nil {
		dr.SmallFile = sz <= xioutil.BlockSizeSmall*2
	}
	// Read file...
	_, err = io.ReadFull(diskHealthReader(ctx, r), buf)

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

	buf, _, err = s.readAllData(ctx, volume, volumeDir, filePath, true)
	return buf, err
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
	// Create top level directories if they don't exist.
	// with mode 0o777 mkdir honors system umask.
	mkdirAll(pathutil.Dir(path), 0o777) // don't need to fail here

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

func (s *xlStorage) openFileSync(filePath string, mode int) (f *os.File, err error) {
	return s.openFile(filePath, mode|writeMode)
}

func (s *xlStorage) openFile(filePath string, mode int) (f *os.File, err error) {
	// Create top level directories if they don't exist.
	// with mode 0777 mkdir honors system umask.
	if err = mkdirAll(pathutil.Dir(filePath), 0o777); err != nil {
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

	odirectEnabled := globalAPIConfig.odirectEnabled() && s.oDirect

	var file *os.File
	if odirectEnabled {
		file, err = OpenFileDirectIO(filePath, readMode, 0o666)
	} else {
		file, err = OpenFile(filePath, readMode, 0o666)
	}
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

	alignment := offset%xioutil.DirectioAlignSize == 0
	if !alignment && odirectEnabled {
		if err = disk.DisableDirectIO(file); err != nil {
			file.Close()
			return nil, err
		}
	}

	if offset > 0 {
		if _, err = file.Seek(offset, io.SeekStart); err != nil {
			file.Close()
			return nil, err
		}
	}

	or := &xioutil.ODirectReader{
		File: file,
		// Select bigger blocks when reading at least 50% of a big block.
		SmallFile: length <= xioutil.BlockSizeLarge/2,
	}

	r := struct {
		io.Reader
		io.Closer
	}{Reader: io.LimitReader(diskHealthReader(ctx, or), length), Closer: closeWrapper(func() error {
		if (!alignment || offset+length%xioutil.DirectioAlignSize != 0) && odirectEnabled {
			// invalidate page-cache for unaligned reads.
			// skip removing from page-cache only
			// if O_DIRECT was disabled.
			disk.FadviseDontNeed(file)
		}
		return or.Close()
	})}

	return r, nil
}

// closeWrapper converts a function to an io.Closer
type closeWrapper func() error

// Close calls the wrapped function.
func (c closeWrapper) Close() error {
	return c()
}

// CreateFile - creates the file.
func (s *xlStorage) CreateFile(ctx context.Context, volume, path string, fileSize int64, r io.Reader) (err error) {
	if fileSize < -1 {
		return errInvalidArgument
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

	return s.writeAllDirect(ctx, filePath, fileSize, r, os.O_CREATE|os.O_WRONLY|os.O_EXCL)
}

func (s *xlStorage) writeAllDirect(ctx context.Context, filePath string, fileSize int64, r io.Reader, flags int) (err error) {
	if contextCanceled(ctx) {
		return ctx.Err()
	}

	// Create top level directories if they don't exist.
	// with mode 0777 mkdir honors system umask.
	parentFilePath := pathutil.Dir(filePath)
	if err = mkdirAll(parentFilePath, 0o777); err != nil {
		return osErrToFileErr(err)
	}

	odirectEnabled := globalAPIConfig.odirectEnabled() && s.oDirect

	var w *os.File
	if odirectEnabled {
		w, err = OpenFileDirectIO(filePath, flags, 0o666)
	} else {
		w, err = OpenFile(filePath, flags, 0o666)
	}
	if err != nil {
		return osErrToFileErr(err)
	}
	defer w.Close()

	var bufp *[]byte
	switch {
	case fileSize > 0 && fileSize >= largestFileThreshold:
		// use a larger 4MiB buffer for a really large streams.
		bufp = xioutil.ODirectPoolXLarge.Get().(*[]byte)
		defer xioutil.ODirectPoolXLarge.Put(bufp)
	case fileSize <= smallFileThreshold:
		bufp = xioutil.ODirectPoolSmall.Get().(*[]byte)
		defer xioutil.ODirectPoolSmall.Put(bufp)
	default:
		bufp = xioutil.ODirectPoolLarge.Get().(*[]byte)
		defer xioutil.ODirectPoolLarge.Put(bufp)
	}

	var written int64
	if odirectEnabled {
		written, err = xioutil.CopyAligned(diskHealthWriter(ctx, w), r, *bufp, fileSize, w)
	} else {
		written, err = io.CopyBuffer(diskHealthWriter(ctx, w), r, *bufp)
	}
	if err != nil {
		return err
	}

	if written < fileSize && fileSize >= 0 {
		return errLessData
	} else if written > fileSize && fileSize >= 0 {
		return errMoreData
	}

	// Only interested in flushing the size_t not mtime/atime
	return Fdatasync(w)
}

func (s *xlStorage) writeAll(ctx context.Context, volume string, path string, b []byte, sync bool) (err error) {
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

	flags := os.O_CREATE | os.O_WRONLY | os.O_TRUNC

	var w *os.File
	if sync {
		// Perform directIO along with fdatasync for larger xl.meta, mostly when
		// xl.meta has "inlined data" we prefer writing O_DIRECT and then doing
		// fdatasync() at the end instead of opening the file with O_DSYNC.
		//
		// This is an optimization mainly to ensure faster I/O.
		if len(b) > xioutil.DirectioAlignSize {
			r := bytes.NewReader(b)
			return s.writeAllDirect(ctx, filePath, r.Size(), r, flags)
		}
		w, err = s.openFileSync(filePath, flags)
	} else {
		w, err = s.openFile(filePath, flags)
	}
	if err != nil {
		return err
	}
	defer w.Close()

	n, err := w.Write(b)
	if err != nil {
		return err
	}

	if n != len(b) {
		return io.ErrShortWrite
	}

	return nil
}

func (s *xlStorage) WriteAll(ctx context.Context, volume string, path string, b []byte) (err error) {
	return s.writeAll(ctx, volume, path, b, true)
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
	w, err = s.openFileSync(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY)
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

// CheckParts check if path has necessary parts available.
func (s *xlStorage) CheckParts(ctx context.Context, volume string, path string, fi FileInfo) error {
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return err
	}

	for _, part := range fi.Parts {
		partPath := pathJoin(path, fi.DataDir, fmt.Sprintf("part.%d", part.Number))
		filePath := pathJoin(volumeDir, partPath)
		if err = checkPathLength(filePath); err != nil {
			return err
		}
		st, err := Lstat(filePath)
		if err != nil {
			if osIsNotExist(err) {
				if !skipAccessChecks(volume) {
					// Stat a volume entry.
					if verr := Access(volumeDir); verr != nil {
						if osIsNotExist(verr) {
							return errVolumeNotFound
						}
						return verr
					}
				}
			}
			return osErrToFileErr(err)
		}
		if st.Mode().IsDir() {
			return errFileNotFound
		}
		// Check if shard is truncated.
		if st.Size() < fi.Erasure.ShardFileSize(part.Size) {
			return errFileCorrupt
		}
	}

	return nil
}

// deleteFile deletes a file or a directory if its empty unless recursive
// is set to true. If the target is successfully deleted, it will recursively
// move up the tree, deleting empty parent directories until it finds one
// with files in it. Returns nil for a non-empty directory even when
// recursive is set to false.
func (s *xlStorage) deleteFile(basePath, deletePath string, recursive, force bool) error {
	if basePath == "" || deletePath == "" {
		return nil
	}
	isObjectDir := HasSuffix(deletePath, SlashSeparator)
	basePath = pathutil.Clean(basePath)
	deletePath = pathutil.Clean(deletePath)
	if !strings.HasPrefix(deletePath, basePath) || deletePath == basePath {
		return nil
	}

	var err error
	if recursive {
		err = s.moveToTrash(deletePath, true, force)
	} else {
		err = Remove(deletePath)
	}
	if err != nil {
		switch {
		case isSysErrNotEmpty(err):
			// if object is a directory, but if its not empty
			// return FileNotFound to indicate its an empty prefix.
			if isObjectDir {
				return errFileNotFound
			}
			// if we have .DS_Store only on macOS
			if runtime.GOOS == globalMacOSName {
				storeFilePath := pathJoin(deletePath, ".DS_Store")
				_, err := Stat(storeFilePath)
				// .DS_Store exsits
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

	deletePath = pathutil.Dir(deletePath)

	// Delete parent directory obviously not recursively. Errors for
	// parent directories shouldn't trickle down.
	s.deleteFile(basePath, deletePath, false, false)

	return nil
}

// DeleteFile - delete a file at path.
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
	return s.deleteFile(volumeDir, filePath, deleteOpts.Recursive, deleteOpts.Force)
}

func skipAccessChecks(volume string) (ok bool) {
	for _, prefix := range []string{
		minioMetaBucket,
		minioMetaMultipartBucket,
		minioMetaTmpDeletedBucket,
		minioMetaTmpBucket,
	} {
		if strings.HasPrefix(volume, prefix) {
			return true
		}
	}
	return ok
}

// RenameData - rename source path to destination path atomically, metadata and data directory.
func (s *xlStorage) RenameData(ctx context.Context, srcVolume, srcPath string, fi FileInfo, dstVolume, dstPath string) (sign uint64, err error) {
	defer func() {
		ignoredErrs := []error{
			errFileNotFound,
			errVolumeNotFound,
			errFileVersionNotFound,
			errDiskNotFound,
			errUnformattedDisk,
			errMaxVersionsExceeded,
		}
		if err != nil && !IsErr(err, ignoredErrs...) && !contextCanceled(ctx) {
			// Only log these errors if context is not yet canceled.
			logger.LogOnceIf(ctx, fmt.Errorf("drive:%s, srcVolume: %s, srcPath: %s, dstVolume: %s:, dstPath: %s - error %v",
				s.drivePath,
				srcVolume, srcPath,
				dstVolume, dstPath,
				err), "xl-storage-rename-data-"+srcVolume+"-"+dstVolume)
		}
		if err == nil && s.globalSync {
			globalSync()
		}
	}()

	srcVolumeDir, err := s.getVolDir(srcVolume)
	if err != nil {
		return 0, err
	}

	dstVolumeDir, err := s.getVolDir(dstVolume)
	if err != nil {
		return 0, err
	}

	if !skipAccessChecks(srcVolume) {
		// Stat a volume entry.
		if err = Access(srcVolumeDir); err != nil {
			if osIsNotExist(err) {
				return 0, errVolumeNotFound
			} else if isSysErrIO(err) {
				return 0, errFaultyDisk
			}
			return 0, err
		}
	}

	if !skipAccessChecks(dstVolume) {
		if err = Access(dstVolumeDir); err != nil {
			if osIsNotExist(err) {
				return 0, errVolumeNotFound
			} else if isSysErrIO(err) {
				return 0, errFaultyDisk
			}
			return 0, err
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
		return 0, err
	}

	if err = checkPathLength(dstFilePath); err != nil {
		return 0, err
	}

	dstBuf, err := xioutil.ReadFile(dstFilePath)
	if err != nil {
		// handle situations when dstFilePath is 'file'
		// for example such as someone is trying to
		// upload an object such as `prefix/object/xl.meta`
		// where `prefix/object` is already an object
		if isSysErrNotDir(err) && runtime.GOOS != globalWindowsOSName {
			// NOTE: On windows the error happens at
			// next line and returns appropriate error.
			return 0, errFileAccessDenied
		}
		if !osIsNotExist(err) {
			return 0, osErrToFileErr(err)
		}
		// errFileNotFound comes here.
		err = s.renameLegacyMetadata(dstVolumeDir, dstPath)
		if err != nil && err != errFileNotFound {
			return 0, err
		}
		if err == nil {
			dstBuf, err = xioutil.ReadFile(dstFilePath)
			if err != nil && !osIsNotExist(err) {
				return 0, osErrToFileErr(err)
			}
		}
	}

	var xlMeta xlMetaV2
	var legacyPreserved bool
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
				logger.LogOnceIf(ctx, err, "read-data-unmarshal-"+dstFilePath)
				// Data appears corrupt. Drop data.
			} else {
				xlMetaLegacy.DataDir = legacyDataDir
				if err = xlMeta.AddLegacy(xlMetaLegacy); err != nil {
					logger.LogOnceIf(ctx, err, "read-data-add-legacy-"+dstFilePath)
				}
				legacyPreserved = true
			}
		}
	} else {
		s.RLock()
		formatLegacy := s.formatLegacy
		s.RUnlock()
		// It is possible that some drives may not have `xl.meta` file
		// in such scenarios verify if atleast `part.1` files exist
		// to verify for legacy version.
		if formatLegacy {
			// We only need this code if we are moving
			// from `xl.json` to `xl.meta`, we can avoid
			// one extra readdir operation here for all
			// new deployments.
			currentDataPath := pathJoin(dstVolumeDir, dstPath)
			entries, err := readDirN(currentDataPath, 1)
			if err != nil && err != errFileNotFound {
				return 0, osErrToFileErr(err)
			}
			for _, entry := range entries {
				if entry == xlStorageFormatFile || strings.HasSuffix(entry, slashSeparator) {
					continue
				}
				if strings.HasPrefix(entry, "part.") {
					legacyPreserved = true
					break
				}
			}
		}
	}

	legacyDataPath := pathJoin(dstVolumeDir, dstPath, legacyDataDir)
	if legacyPreserved {
		// Preserve all the legacy data, could be slow, but at max there can be 10,000 parts.
		currentDataPath := pathJoin(dstVolumeDir, dstPath)
		entries, err := readDir(currentDataPath)
		if err != nil {
			return 0, osErrToFileErr(err)
		}

		// legacy data dir means its old content, honor system umask.
		if err = mkdirAll(legacyDataPath, 0o777); err != nil {
			// any failed mkdir-calls delete them.
			s.deleteFile(dstVolumeDir, legacyDataPath, true, false)
			return 0, osErrToFileErr(err)
		}

		for _, entry := range entries {
			// Skip xl.meta renames further, also ignore any directories such as `legacyDataDir`
			if entry == xlStorageFormatFile || strings.HasSuffix(entry, slashSeparator) {
				continue
			}

			if err = Rename(pathJoin(currentDataPath, entry), pathJoin(legacyDataPath, entry)); err != nil {
				// Any failed rename calls un-roll previous transaction.
				s.deleteFile(dstVolumeDir, legacyDataPath, true, false)

				return 0, osErrToFileErr(err)
			}
		}
	}

	var oldDstDataPath, reqVID string

	if fi.VersionID == "" {
		reqVID = nullVersionID
	} else {
		reqVID = fi.VersionID
	}

	// Replace the data of null version or any other existing version-id
	ofi, err := xlMeta.ToFileInfo(dstVolume, dstPath, reqVID, false, false)
	if err == nil && !ofi.Deleted {
		if xlMeta.SharedDataDirCountStr(reqVID, ofi.DataDir) == 0 {
			// Purge the destination path as we are not preserving anything
			// versioned object was not requested.
			oldDstDataPath = pathJoin(dstVolumeDir, dstPath, ofi.DataDir)
			// if old destination path is same as new destination path
			// there is nothing to purge, this is true in case of healing
			// avoid setting oldDstDataPath at that point.
			if oldDstDataPath == dstDataPath {
				oldDstDataPath = ""
			} else {
				xlMeta.data.remove(reqVID, ofi.DataDir)
			}
		}
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
	// healing doesn't preserve the dataDir as 'legacy'
	healing := fi.XLV1 && fi.DataDir != legacyDataDir

	if err = xlMeta.AddVersion(fi); err != nil {
		if legacyPreserved {
			// Any failed rename calls un-roll previous transaction.
			s.deleteFile(dstVolumeDir, legacyDataPath, true, false)
		}
		return 0, err
	}

	var sbuf bytes.Buffer
	for _, ver := range xlMeta.versions {
		sbuf.Write(ver.header.Signature[:])
	}
	sign = xxh3.Hash(sbuf.Bytes())

	dstBuf, err = xlMeta.AppendTo(metaDataPoolGet())
	defer metaDataPoolPut(dstBuf)
	if err != nil {
		if legacyPreserved {
			// Any failed rename calls un-roll previous transaction.
			s.deleteFile(dstVolumeDir, legacyDataPath, true, false)
		}
		return 0, errFileCorrupt
	}

	if srcDataPath != "" {
		if err = s.WriteAll(ctx, srcVolume, pathJoin(srcPath, xlStorageFormatFile), dstBuf); err != nil {
			if legacyPreserved {
				// Any failed rename calls un-roll previous transaction.
				s.deleteFile(dstVolumeDir, legacyDataPath, true, false)
			}
			return 0, osErrToFileErr(err)
		}
		diskHealthCheckOK(ctx, err)

		if !fi.Versioned && !fi.Healing() {
			// Use https://man7.org/linux/man-pages/man2/rename.2.html if possible on unversioned bucket.
			if err := Rename2(pathutil.Join(srcVolumeDir, srcPath), pathutil.Join(dstVolumeDir, dstPath)); err == nil {
				return sign, nil
			} // if Rename2 is not successful fallback.
		}

		// renameAll only for objects that have xl.meta not saved inline.
		if len(fi.Data) == 0 && fi.Size > 0 {
			s.moveToTrash(dstDataPath, true, false)
			if healing {
				// If we are healing we should purge any legacyDataPath content,
				// that was previously preserved during PutObject() call
				// on a versioned bucket.
				s.moveToTrash(legacyDataPath, true, false)
			}
			if err = renameAll(srcDataPath, dstDataPath); err != nil {
				if legacyPreserved {
					// Any failed rename calls un-roll previous transaction.
					s.deleteFile(dstVolumeDir, legacyDataPath, true, false)
				}
				s.deleteFile(dstVolumeDir, dstDataPath, false, false)
				return 0, osErrToFileErr(err)
			}
		}

		// Commit meta-file
		if err = renameAll(srcFilePath, dstFilePath); err != nil {
			if legacyPreserved {
				// Any failed rename calls un-roll previous transaction.
				s.deleteFile(dstVolumeDir, legacyDataPath, true, false)
			}
			s.deleteFile(dstVolumeDir, dstFilePath, false, false)
			return 0, osErrToFileErr(err)
		}

		// additionally only purge older data at the end of the transaction of new data-dir
		// movement, this is to ensure that previous data references can co-exist for
		// any recoverability.
		if oldDstDataPath != "" {
			s.moveToTrash(oldDstDataPath, true, false)
		}
	} else {
		// Write meta-file directly, no data
		if err = s.WriteAll(ctx, dstVolume, pathJoin(dstPath, xlStorageFormatFile), dstBuf); err != nil {
			if legacyPreserved {
				// Any failed rename calls un-roll previous transaction.
				s.deleteFile(dstVolumeDir, legacyDataPath, true, false)
			}
			s.deleteFile(dstVolumeDir, dstFilePath, false, false)
			return 0, err
		}
	}

	if srcVolume != minioMetaMultipartBucket {
		// srcFilePath is some-times minioMetaTmpBucket, an attempt to
		// remove the temporary folder is enough since at this point
		// ideally all transaction should be complete.
		Remove(pathutil.Dir(srcFilePath))
	} else {
		s.deleteFile(srcVolumeDir, pathutil.Dir(srcFilePath), true, false)
	}
	return sign, nil
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
	if !(srcIsDir && dstIsDir || !srcIsDir && !dstIsDir) {
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

	if err = renameAll(srcFilePath, dstFilePath); err != nil {
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

func (s *xlStorage) VerifyFile(ctx context.Context, volume, path string, fi FileInfo) (err error) {
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

	erasure := fi.Erasure
	for _, part := range fi.Parts {
		checksumInfo := erasure.GetChecksumInfo(part.Number)
		partPath := pathJoin(volumeDir, path, fi.DataDir, fmt.Sprintf("part.%d", part.Number))
		if err := s.bitrotVerify(ctx, partPath,
			erasure.ShardFileSize(part.Size),
			checksumInfo.Algorithm,
			checksumInfo.Hash, erasure.ShardSize()); err != nil {
			if !IsErr(err, []error{
				errFileNotFound,
				errVolumeNotFound,
				errFileCorrupt,
				errFileAccessDenied,
				errFileVersionNotFound,
			}...) {
				logger.GetReqInfo(ctx).AppendTags("disk", s.String())
				logger.LogOnceIf(ctx, err, partPath)
			}
			return err
		}
	}

	return nil
}

// ReadMultiple will read multiple files and send each back as response.
// Files are read and returned in the given order.
// The resp channel is closed before the call returns.
// Only a canceled context will return an error.
func (s *xlStorage) ReadMultiple(ctx context.Context, req ReadMultipleReq, resp chan<- ReadMultipleResp) error {
	defer close(resp)

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
		w := xioutil.NewDeadlineWorker(diskMaxTimeout)
		if err := w.Run(func() (err error) {
			if req.MetadataOnly {
				data, mt, err = s.readMetadataWithDMTime(ctx, fullPath)
			} else {
				data, mt, err = s.readAllData(ctx, req.Bucket, volumeDir, fullPath, false)
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
			resp <- r
			continue
		}
		found++
		r.Exists = true
		r.Data = data
		r.Modtime = mt
		resp <- r
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
	buf, _, err := s.readAllData(ctx, volume, volumeDir, metaPath, false)
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
		err := s.deleteFile(volumeDir, toRemove, true, true)
		diskHealthCheckOK(ctx, err)
	}

	// Do the same for inline data
	dirs, err := xl.data.list()
	if err != nil {
		return err
	}
	// Clear and repopulate
	for k := range foundDirs {
		delete(foundDirs, k)
	}
	// Populate into map
	for _, k := range dirs {
		foundDirs[k] = struct{}{}
	}
	// Delete all directories we expect to be there.
	for _, dir := range wantDirs {
		delete(foundDirs, dir)
	}

	// Delete excessive inline entries.
	if len(foundDirs) > 0 {
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
