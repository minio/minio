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
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	pathutil "path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
	jsoniter "github.com/json-iterator/go"
	"github.com/minio/minio/internal/bucket/lifecycle"
	"github.com/minio/minio/internal/color"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/disk"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/console"
	"github.com/minio/pkg/env"
	"github.com/yargevad/filepathx"
)

const (
	nullVersionID = "null"
	// Really large streams threshold per shard.
	reallyLargeFileThreshold = 64 * humanize.MiByte // Optimized for HDDs

	// Small file threshold below which data accompanies metadata from storage layer.
	smallFileThreshold = 128 * humanize.KiByte // Optimized for NVMe/SSDs
	// For hardrives it is possible to set this to a lower value to avoid any
	// spike in latency. But currently we are simply keeping it optimal for SSDs.

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
	diskPath string
	endpoint Endpoint

	globalSync bool

	rootDisk bool

	diskID string

	// Indexes, will be -1 until assigned a set.
	poolIndex, setIndex, diskIndex int

	formatFileInfo  os.FileInfo
	formatLegacy    bool
	formatLastCheck time.Time

	diskInfoCache timedValue

	ctx context.Context
	sync.RWMutex

	// mutex to prevent concurrent read operations overloading walks.
	walkMu     sync.Mutex
	walkReadMu sync.Mutex
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
		if err = mkdirAll(path, 0777); err != nil {
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
		if err != errFileNotFound {
			logger.LogIf(GlobalContext, err)
		}
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
	})
}

// Sanitize - sanitizes the `format.json`, cleanup tmp.
// all other future cleanups should be added here.
func (s *xlStorage) Sanitize() error {
	if err := formatErasureMigrate(s.diskPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	return formatErasureCleanupTmp(s.diskPath)
}

// Initialize a new storage disk.
func newXLStorage(ep Endpoint) (*xlStorage, error) {
	path := ep.Path
	var err error
	if path, err = getValidPath(path); err != nil {
		return nil, err
	}

	var rootDisk bool
	if env.Get("MINIO_CI_CD", "") != "" {
		rootDisk = true
	} else {
		rootDisk, err = disk.IsRootDisk(path, SlashSeparator)
		if err != nil {
			return nil, err
		}
		if !rootDisk {
			// If for some reason we couldn't detect the
			// root disk use - MINIO_ROOTDISK_THRESHOLD_SIZE
			// to figure out if the disk is root disk or not.
			if rootDiskSize := env.Get(config.EnvRootDiskThresholdSize, ""); rootDiskSize != "" {
				info, err := disk.GetInfo(path)
				if err != nil {
					return nil, err
				}
				size, err := humanize.ParseBytes(rootDiskSize)
				if err != nil {
					return nil, err
				}
				// size of the disk is less than the threshold or
				// equal to the size of the disk at path, treat
				// such disks as rootDisks and reject them.
				rootDisk = info.Total <= size
			}
		}
	}

	p := &xlStorage{
		diskPath:   path,
		endpoint:   ep,
		globalSync: env.Get(config.EnvFSOSync, config.EnableOff) == config.EnableOn,
		ctx:        GlobalContext,
		rootDisk:   rootDisk,
		poolIndex:  -1,
		setIndex:   -1,
		diskIndex:  -1,
	}

	// Create all necessary bucket folders if possible.
	if err = p.MakeVolBulk(context.TODO(), minioMetaBucket, minioMetaTmpBucket, minioMetaMultipartBucket, dataUsageBucket, minioMetaSpeedTestBucket); err != nil {
		return nil, err
	}

	// Check if backend is writable and supports O_DIRECT
	var rnd [8]byte
	_, _ = rand.Read(rnd[:])
	tmpFile := ".writable-check-" + hex.EncodeToString(rnd[:]) + ".tmp"
	filePath := pathJoin(p.diskPath, minioMetaTmpBucket, tmpFile)
	w, err := OpenFileDirectIO(filePath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0666)
	if err != nil {
		return p, err
	}
	if _, err = w.Write(alignedBuf[:]); err != nil {
		w.Close()
		return p, err
	}
	w.Close()
	Remove(filePath)

	// Success.
	return p, nil
}

// getDiskInfo returns given disk information.
func getDiskInfo(diskPath string) (di disk.Info, err error) {
	if err = checkPathLength(diskPath); err == nil {
		di, err = disk.GetInfo(diskPath)
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
	return s.diskPath
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
	healingFile := pathJoin(s.diskPath, minioMetaBucket,
		bucketMetaPrefix, healingTrackerFilename)
	b, err := xioutil.ReadFile(healingFile)
	if err != nil {
		return nil
	}
	var h healingTracker
	_, err = h.UnmarshalMsg(b)
	logger.LogIf(GlobalContext, err)
	return &h
}

func (s *xlStorage) readMetadata(ctx context.Context, itemPath string) ([]byte, error) {
	if contextCanceled(ctx) {
		return nil, ctx.Err()
	}

	if err := checkPathLength(itemPath); err != nil {
		return nil, err
	}

	f, err := OpenFile(itemPath, readMode, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if stat.IsDir() {
		return nil, &os.PathError{
			Op:   "open",
			Path: itemPath,
			Err:  syscall.EISDIR,
		}
	}
	return readXLMetaNoData(f, stat.Size())
}

func (s *xlStorage) NSScanner(ctx context.Context, cache dataUsageCache, updates chan<- dataUsageEntry) (dataUsageCache, error) {
	// Updates must be closed before we return.
	defer close(updates)
	var lc *lifecycle.Lifecycle
	var err error

	// Check if the current bucket has a configured lifecycle policy
	if globalLifecycleSys != nil {
		lc, err = globalLifecycleSys.Get(cache.Info.Name)
		if err == nil && lc.HasActiveRules("", true) {
			cache.Info.lifeCycle = lc
			if intDataUpdateTracker.debug {
				console.Debugln(color.Green("scannerDisk:") + " lifecycle: Active rules found")
			}
		}
	}

	// Check if the current bucket has replication configuration
	if rcfg, err := globalBucketMetadataSys.GetReplicationConfig(ctx, cache.Info.Name); err == nil {
		if rcfg.HasActiveRules("", true) {
			tgts, err := globalBucketTargetSys.ListBucketTargets(ctx, cache.Info.Name)
			if err == nil {
				cache.Info.replication = replicationConfig{
					Config:  rcfg,
					remotes: tgts,
				}
				if intDataUpdateTracker.debug {
					console.Debugln(color.Green("scannerDisk:") + " replication: Active rules found")
				}

			}
		}
	}
	// return initialized object layer
	objAPI := newObjectLayerFn()
	// object layer not initialized, return.
	if objAPI == nil {
		return cache, errServerNotInitialized
	}

	cache.Info.updates = updates

	dataUsageInfo, err := scanDataFolder(ctx, s.diskPath, cache, func(item scannerItem) (sizeSummary, error) {
		// Look for `xl.meta/xl.json' at the leaf.
		if !strings.HasSuffix(item.Path, SlashSeparator+xlStorageFormatFile) &&
			!strings.HasSuffix(item.Path, SlashSeparator+xlStorageFormatFileV1) {
			// if no xl.meta/xl.json found, skip the file.
			return sizeSummary{}, errSkipFile
		}

		buf, err := s.readMetadata(ctx, item.Path)
		if err != nil {
			if intDataUpdateTracker.debug {
				console.Debugf(color.Green("scannerBucket:")+" object path missing: %v: %w\n", item.Path, err)
			}
			return sizeSummary{}, errSkipFile
		}
		defer metaDataPoolPut(buf)

		// Remove filename which is the meta file.
		item.transformMetaDir()

		fivs, err := getAllFileInfoVersions(buf, item.bucket, item.objectPath())
		if err != nil {
			if intDataUpdateTracker.debug {
				console.Debugf(color.Green("scannerBucket:")+" reading xl.meta failed: %v: %w\n", item.Path, err)
			}
			return sizeSummary{}, errSkipFile
		}
		sizeS := sizeSummary{}
		var noTiers bool
		if noTiers = globalTierConfigMgr.Empty(); !noTiers {
			sizeS.tiers = make(map[string]tierStats)
		}
		atomic.AddUint64(&globalScannerStats.accTotalObjects, 1)
		for _, version := range fivs.Versions {
			atomic.AddUint64(&globalScannerStats.accTotalVersions, 1)
			oi := version.ToObjectInfo(item.bucket, item.objectPath())
			sz := item.applyActions(ctx, objAPI, oi, &sizeS)
			if !oi.DeleteMarker && sz == oi.Size {
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
		return sizeS, nil
	})

	if err != nil {
		return dataUsageInfo, err
	}

	dataUsageInfo.Info.LastUpdate = time.Now()
	return dataUsageInfo, nil
}

// DiskInfo provides current information about disk space usage,
// total free inodes and underlying filesystem.
func (s *xlStorage) DiskInfo(context.Context) (info DiskInfo, err error) {
	s.diskInfoCache.Once.Do(func() {
		s.diskInfoCache.TTL = time.Second
		s.diskInfoCache.Update = func() (interface{}, error) {
			dcinfo := DiskInfo{
				RootDisk:  s.rootDisk,
				MountPath: s.diskPath,
				Endpoint:  s.endpoint.String(),
			}
			di, err := getDiskInfo(s.diskPath)
			if err != nil {
				return dcinfo, err
			}
			dcinfo.Total = di.Total
			dcinfo.Free = di.Free
			dcinfo.Used = di.Used
			dcinfo.UsedInodes = di.Files - di.Ffree
			dcinfo.FreeInodes = di.Ffree
			dcinfo.FSType = di.FSType
			diskID, err := s.GetDiskID()
			if errors.Is(err, errUnformattedDisk) {
				// if we found an unformatted disk then
				// healing is automatically true.
				dcinfo.Healing = true
			} else {
				// Check if the disk is being healed if GetDiskID
				// returned any error other than fresh disk
				dcinfo.Healing = s.Healing() != nil
			}
			dcinfo.ID = diskID
			return dcinfo, err
		}
	})

	v, err := s.diskInfoCache.Get()
	info = v.(DiskInfo)
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
	volumeDir := pathJoin(s.diskPath, volume)
	return volumeDir, nil
}

func (s *xlStorage) checkFormatJSON() (os.FileInfo, error) {
	formatFile := pathJoin(s.diskPath, minioMetaBucket, formatConfigFile)
	fi, err := Lstat(formatFile)
	if err != nil {
		// If the disk is still not initialized.
		if osIsNotExist(err) {
			if err = Access(s.diskPath); err == nil {
				// Disk is present but missing `format.json`
				return nil, errUnformattedDisk
			}
			if osIsNotExist(err) {
				return nil, errDiskNotFound
			} else if osIsPermission(err) {
				return nil, errDiskAccessDenied
			}
			logger.LogIf(GlobalContext, err) // log unexpected errors
			return nil, errCorruptedFormat
		} else if osIsPermission(err) {
			return nil, errDiskAccessDenied
		}
		logger.LogIf(GlobalContext, err) // log unexpected errors
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

	// check if we have a valid disk ID that is less than 1 second old.
	if fileInfo != nil && diskID != "" && time.Since(lastCheck) <= time.Second {
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

	formatFile := pathJoin(s.diskPath, minioMetaBucket, formatConfigFile)
	b, err := xioutil.ReadFile(formatFile)
	if err != nil {
		// If the disk is still not initialized.
		if osIsNotExist(err) {
			if err = Access(s.diskPath); err == nil {
				// Disk is present but missing `format.json`
				return "", errUnformattedDisk
			}
			if osIsNotExist(err) {
				return "", errDiskNotFound
			} else if osIsPermission(err) {
				return "", errDiskAccessDenied
			}
			logger.LogIf(GlobalContext, err) // log unexpected errors
			return "", errCorruptedFormat
		} else if osIsPermission(err) {
			return "", errDiskAccessDenied
		}
		logger.LogIf(GlobalContext, err) // log unexpected errors
		return "", errCorruptedFormat
	}

	format := &formatErasureV3{}
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	if err = json.Unmarshal(b, &format); err != nil {
		logger.LogIf(GlobalContext, err) // log unexpected errors
		return "", errCorruptedFormat
	}

	s.Lock()
	defer s.Unlock()
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
		if err := s.MakeVol(ctx, volume); err != nil {
			if errors.Is(err, errDiskAccessDenied) {
				return errDiskAccessDenied
			}
		}
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
			err = mkdirAll(volumeDir, 0777)
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
func (s *xlStorage) ListVols(context.Context) (volsInfo []VolInfo, err error) {
	return listVols(s.diskPath)
}

// List all the volumes from diskPath.
func listVols(dirPath string) ([]VolInfo, error) {
	if err := checkPathLength(dirPath); err != nil {
		return nil, err
	}
	entries, err := readDir(dirPath)
	if err != nil {
		return nil, errDiskNotFound
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
		err = s.moveToTrash(volumeDir, true)
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
			if ierr := Access(volumeDir); ierr != nil {
				if osIsNotExist(ierr) {
					return nil, errVolumeNotFound
				} else if isSysErrIO(ierr) {
					return nil, errFaultyDisk
				}
			}
		}
		return nil, err
	}

	return entries, nil
}

func (s *xlStorage) deleteVersions(ctx context.Context, volume, path string, fis ...FileInfo) error {
	buf, err := s.ReadAll(ctx, volume, pathJoin(path, xlStorageFormatFile))
	if err != nil {
		if err != errFileNotFound {
			return err
		}
		metaDataPoolPut(buf) // Never used, return it

		buf, err = s.ReadAll(ctx, volume, pathJoin(path, xlStorageFormatFileV1))
		if err != nil {
			return err
		}
	}

	if len(buf) == 0 {
		return errFileNotFound
	}

	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return err
	}

	if !isXL2V1Format(buf) {
		// Delete the meta file, if there are no more versions the
		// top level parent is automatically removed.
		return s.deleteFile(volumeDir, pathJoin(volumeDir, path), true)
	}

	var xlMeta xlMetaV2
	if err = xlMeta.Load(buf); err != nil {
		return err
	}

	var (
		dataDir     string
		lastVersion bool
	)

	for _, fi := range fis {
		dataDir, lastVersion, err = xlMeta.DeleteVersion(fi)
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
			if !xlMeta.data.remove(versionID, dataDir) {
				filePath := pathJoin(volumeDir, path, dataDir)
				if err = checkPathLength(filePath); err != nil {
					return err
				}
				if err = s.moveToTrash(filePath, true); err != nil {
					if err != errFileNotFound {
						return err
					}
				}
			}
		}
	}

	if !lastVersion {
		buf, err = xlMeta.AppendTo(metaDataPoolGet())
		defer metaDataPoolPut(buf)
		if err != nil {
			return err
		}

		return s.WriteAll(ctx, volume, pathJoin(path, xlStorageFormatFile), buf)
	}

	// Move xl.meta to trash
	filePath := pathJoin(volumeDir, path, xlStorageFormatFile)
	if err = checkPathLength(filePath); err != nil {
		return err
	}

	err = s.moveToTrash(filePath, false)
	if err == nil || err == errFileNotFound {
		s.deleteFile(volumeDir, pathJoin(volumeDir, path), false)
	}
	return err
}

// DeleteVersions deletes slice of versions, it can be same object
// or multiple objects.
func (s *xlStorage) DeleteVersions(ctx context.Context, volume string, versions []FileInfoVersions) []error {
	errs := make([]error, len(versions))

	for i, fiv := range versions {
		if err := s.deleteVersions(ctx, volume, fiv.Name, fiv.Versions...); err != nil {
			errs[i] = err
		}
	}

	return errs
}

func (s *xlStorage) moveToTrash(filePath string, recursive bool) error {
	pathUUID := mustGetUUID()
	if recursive {
		return renameAll(filePath, pathutil.Join(s.diskPath, minioMetaTmpDeletedBucket, pathUUID))
	}
	return Rename(filePath, pathutil.Join(s.diskPath, minioMetaTmpDeletedBucket, pathUUID))
}

// DeleteVersion - deletes FileInfo metadata for path at `xl.meta`. forceDelMarker
// will force creating a new `xl.meta` to create a new delete marker
func (s *xlStorage) DeleteVersion(ctx context.Context, volume, path string, fi FileInfo, forceDelMarker bool) error {
	if HasSuffix(path, SlashSeparator) {
		return s.Delete(ctx, volume, path, false)
	}

	buf, err := s.ReadAll(ctx, volume, pathJoin(path, xlStorageFormatFile))
	if err != nil {
		if err != errFileNotFound {
			return err
		}
		if fi.Deleted && forceDelMarker {
			// Create a new xl.meta with a delete marker in it
			return s.WriteMetadata(ctx, volume, path, fi)
		}
		metaDataPoolPut(buf) // Never used, return it

		buf, err = s.ReadAll(ctx, volume, pathJoin(path, xlStorageFormatFileV1))
		if err != nil {
			if err == errFileNotFound && fi.VersionID != "" {
				return errFileVersionNotFound
			}
			return err
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

	if !isXL2V1Format(buf) {
		// Delete the meta file, if there are no more versions the
		// top level parent is automatically removed.
		return s.deleteFile(volumeDir, pathJoin(volumeDir, path), true)
	}

	var xlMeta xlMetaV2
	if err = xlMeta.Load(buf); err != nil {
		return err
	}

	dataDir, lastVersion, err := xlMeta.DeleteVersion(fi)
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
		if !xlMeta.data.remove(versionID, dataDir) {
			filePath := pathJoin(volumeDir, path, dataDir)
			if err = checkPathLength(filePath); err != nil {
				return err
			}
			if err = s.moveToTrash(filePath, true); err != nil {
				if err != errFileNotFound {
					return err
				}
			}
		}
	}
	if !lastVersion {
		buf, err = xlMeta.AppendTo(metaDataPoolGet())
		defer metaDataPoolPut(buf)
		if err != nil {
			return err
		}

		return s.WriteAll(ctx, volume, pathJoin(path, xlStorageFormatFile), buf)
	}

	// Move xl.meta to trash
	filePath := pathJoin(volumeDir, path, xlStorageFormatFile)
	if err = checkPathLength(filePath); err != nil {
		return err
	}

	err = s.moveToTrash(filePath, false)
	if err == nil || err == errFileNotFound {
		s.deleteFile(volumeDir, pathJoin(volumeDir, path), false)
	}
	return err
}

// Updates only metadata for a given version.
func (s *xlStorage) UpdateMetadata(ctx context.Context, volume, path string, fi FileInfo) error {
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

	if !isXL2V1Format(buf) {
		return errFileVersionNotFound
	}

	var xlMeta xlMetaV2
	if err = xlMeta.Load(buf); err != nil {
		logger.LogIf(ctx, err)
		return err
	}

	if err = xlMeta.UpdateObjectVersion(fi); err != nil {
		return err
	}

	buf, err = xlMeta.AppendTo(nil)
	if err != nil {
		return err
	}

	return s.WriteAll(ctx, volume, pathJoin(path, xlStorageFormatFile), buf)
}

// WriteMetadata - writes FileInfo metadata for path at `xl.meta`
func (s *xlStorage) WriteMetadata(ctx context.Context, volume, path string, fi FileInfo) error {
	if fi.Fresh {
		var xlMeta xlMetaV2
		if err := xlMeta.AddVersion(fi); err != nil {
			logger.LogIf(ctx, err)
			return err
		}
		buf, err := xlMeta.AppendTo(metaDataPoolGet())
		defer metaDataPoolPut(buf)
		if err != nil {
			logger.LogIf(ctx, err)
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
		err = xlMeta.AddVersion(fi)
		if err != nil {
			logger.LogIf(ctx, err)
			return err
		}

		buf, err = xlMeta.AppendTo(metaDataPoolGet())
		defer metaDataPoolPut(buf)
		if err != nil {
			logger.LogIf(ctx, err)
			return err
		}
	} else {
		if err = xlMeta.Load(buf); err != nil {
			logger.LogIf(ctx, err)
			// Corrupted data, reset and write.
			xlMeta = xlMetaV2{}
		}

		if err = xlMeta.AddVersion(fi); err != nil {
			logger.LogIf(ctx, err)
			return err
		}

		buf, err = xlMeta.AppendTo(metaDataPoolGet())
		defer metaDataPoolPut(buf)
		if err != nil {
			logger.LogIf(ctx, err)
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
		if err == nil {
			if s.globalSync {
				// Sync to disk only upon success.
				globalSync()
			}
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

// ReadVersion - reads metadata and returns FileInfo at path `xl.meta`
// for all objects less than `32KiB` this call returns data as well
// along with metadata.
func (s *xlStorage) ReadVersion(ctx context.Context, volume, path, versionID string, readData bool) (fi FileInfo, err error) {
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return fi, err
	}
	var buf []byte
	if readData {
		buf, err = s.ReadAll(ctx, volume, pathJoin(path, xlStorageFormatFile))
	} else {
		buf, err = s.readMetadata(ctx, pathJoin(volumeDir, path, xlStorageFormatFile))
		if err != nil {
			if osIsNotExist(err) {
				if aerr := Access(volumeDir); aerr != nil && osIsNotExist(aerr) {
					return fi, errVolumeNotFound
				}
			}
			err = osErrToFileErr(err)
		}
	}

	if err != nil {
		if err == errFileNotFound {
			buf, err = s.ReadAll(ctx, volume, pathJoin(path, xlStorageFormatFileV1))
			if err != nil {
				if err == errFileNotFound {
					if versionID != "" {
						return fi, errFileVersionNotFound
					}
					return fi, errFileNotFound
				}
				return fi, err
			}
		} else {
			return fi, err
		}
	}

	if len(buf) == 0 {
		if versionID != "" {
			return fi, errFileVersionNotFound
		}
		return fi, errFileNotFound
	}

	fi, err = getFileInfo(buf, volume, path, versionID, readData)
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
			_, err = s.StatInfoFile(ctx, volume, dataPath, false)
			if err != nil {
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
			fi.Data, err = s.readAllData(volumeDir, dataPath)
			if err != nil {
				return FileInfo{}, err
			}
		}
	}

	return fi, nil
}

func (s *xlStorage) readAllData(volumeDir string, filePath string) (buf []byte, err error) {
	f, err := OpenFileDirectIO(filePath, readMode, 0666)
	if err != nil {
		if osIsNotExist(err) {
			// Check if the object doesn't exist because its bucket
			// is missing in order to return the correct error.
			if err = Access(volumeDir); err != nil && osIsNotExist(err) {
				return nil, errVolumeNotFound
			}
			return nil, errFileNotFound
		} else if osIsPermission(err) {
			return nil, errFileAccessDenied
		} else if isSysErrNotDir(err) || isSysErrIsDir(err) {
			return nil, errFileNotFound
		} else if isSysErrHandleInvalid(err) {
			// This case is special and needs to be handled for windows.
			return nil, errFileNotFound
		} else if isSysErrIO(err) {
			return nil, errFaultyDisk
		} else if isSysErrTooManyFiles(err) {
			return nil, errTooManyOpenFiles
		} else if isSysErrInvalidArg(err) {
			st, _ := Lstat(filePath)
			if st != nil && st.IsDir() {
				// Linux returns InvalidArg for directory O_DIRECT
				// we need to keep this fallback code to return correct
				// errors upwards.
				return nil, errFileNotFound
			}
			return nil, errUnsupportedDisk
		}
		return nil, err
	}
	r := &xioutil.ODirectReader{
		File:      f,
		SmallFile: true,
	}
	defer r.Close()

	// Get size for precise allocation.
	stat, err := f.Stat()
	if err != nil {
		buf, err = ioutil.ReadAll(r)
		return buf, osErrToFileErr(err)
	}
	if stat.IsDir() {
		return nil, errFileNotFound
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
	_, err = io.ReadFull(r, buf)

	return buf, osErrToFileErr(err)
}

// ReadAll reads from r until an error or EOF and returns the data it read.
// A successful call returns err == nil, not err == EOF. Because ReadAll is
// defined to read from src until EOF, it does not treat an EOF from Read
// as an error to be reported.
// This API is meant to be used on files which have small memory footprint, do
// not use this on large files as it would cause server to crash.
func (s *xlStorage) ReadAll(ctx context.Context, volume string, path string) (buf []byte, err error) {
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return nil, err
	}

	// Validate file path length, before reading.
	filePath := pathJoin(volumeDir, path)
	if err = checkPathLength(filePath); err != nil {
		return nil, err
	}

	return s.readAllData(volumeDir, filePath)
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

	// Stat a volume entry.
	if err = Access(volumeDir); err != nil {
		if osIsNotExist(err) {
			return 0, errVolumeNotFound
		} else if isSysErrIO(err) {
			return 0, errFaultyDisk
		} else if osIsPermission(err) {
			return 0, errFileAccessDenied
		}
		return 0, err
	}

	// Validate effective path length before reading.
	filePath := pathJoin(volumeDir, path)
	if err = checkPathLength(filePath); err != nil {
		return 0, err
	}

	// Open the file for reading.
	file, err := Open(filePath)
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

func (s *xlStorage) openFileSync(filePath string, mode int) (f *os.File, err error) {
	// Create top level directories if they don't exist.
	// with mode 0777 mkdir honors system umask.
	if err = mkdirAll(pathutil.Dir(filePath), 0777); err != nil {
		return nil, osErrToFileErr(err)
	}

	w, err := OpenFile(filePath, mode|writeMode, 0666)
	if err != nil {
		// File path cannot be verified since one of the parents is a file.
		switch {
		case isSysErrIsDir(err):
			return nil, errIsNotRegular
		case osIsPermission(err):
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

func (s *xlStorage) openFileNoSync(filePath string, mode int) (f *os.File, err error) {
	// Create top level directories if they don't exist.
	// with mode 0777 mkdir honors system umask.
	if err = mkdirAll(pathutil.Dir(filePath), 0777); err != nil {
		return nil, osErrToFileErr(err)
	}

	w, err := OpenFile(filePath, mode, 0666)
	if err != nil {
		// File path cannot be verified since one of the parents is a file.
		switch {
		case isSysErrIsDir(err):
			return nil, errIsNotRegular
		case osIsPermission(err):
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

	file, err := OpenFileDirectIO(filePath, readMode, 0666)
	if err != nil {
		switch {
		case osIsNotExist(err):
			if err = Access(volumeDir); err != nil && osIsNotExist(err) {
				return nil, errVolumeNotFound
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
	if !alignment {
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
		File:      file,
		SmallFile: false,
	}

	if length <= smallFileThreshold {
		or = &xioutil.ODirectReader{
			File:      file,
			SmallFile: true,
		}
	}

	r := struct {
		io.Reader
		io.Closer
	}{Reader: io.LimitReader(or, length), Closer: closeWrapper(func() error {
		if !alignment || offset+length%xioutil.DirectioAlignSize != 0 {
			// invalidate page-cache for unaligned reads.
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

	if fileSize >= 0 && fileSize <= smallFileThreshold {
		// For streams smaller than 128KiB we simply write them as O_DSYNC (fdatasync)
		// and not O_DIRECT to avoid the complexities of aligned I/O.
		w, err := s.openFileSync(filePath, os.O_CREATE|os.O_WRONLY|os.O_EXCL)
		if err != nil {
			return err
		}
		defer w.Close()

		written, err := io.Copy(w, r)
		if err != nil {
			return osErrToFileErr(err)
		}

		if written < fileSize {
			return errLessData
		} else if written > fileSize {
			return errMoreData
		}

		return nil
	}

	// Create top level directories if they don't exist.
	// with mode 0777 mkdir honors system umask.
	if err = mkdirAll(parentFilePath, 0777); err != nil {
		return osErrToFileErr(err)
	}

	w, err := OpenFileDirectIO(filePath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0666)
	if err != nil {
		return osErrToFileErr(err)
	}

	defer func() {
		disk.Fdatasync(w) // Only interested in flushing the size_t not mtime/atime
		w.Close()
	}()

	var bufp *[]byte
	if fileSize > 0 && fileSize >= reallyLargeFileThreshold {
		// use a larger 4MiB buffer for really large streams.
		bufp = xioutil.ODirectPoolXLarge.Get().(*[]byte)
		defer xioutil.ODirectPoolXLarge.Put(bufp)
	} else {
		bufp = xioutil.ODirectPoolLarge.Get().(*[]byte)
		defer xioutil.ODirectPoolLarge.Put(bufp)
	}

	written, err := xioutil.CopyAligned(w, r, *bufp, fileSize)
	if err != nil {
		return err
	}

	if written < fileSize && fileSize >= 0 {
		return errLessData
	} else if written > fileSize && fileSize >= 0 {
		return errMoreData
	}

	return nil
}

func (s *xlStorage) writeAll(ctx context.Context, volume string, path string, b []byte, sync bool) (err error) {
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return err
	}

	filePath := pathJoin(volumeDir, path)
	if err = checkPathLength(filePath); err != nil {
		return err
	}

	var w *os.File
	if sync {
		w, err = s.openFileSync(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC)
	} else {
		w, err = s.openFileNoSync(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC)
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

	// Stat a volume entry.
	if err = Access(volumeDir); err != nil {
		if osIsNotExist(err) {
			return errVolumeNotFound
		} else if osIsPermission(err) {
			return errVolumeAccessDenied
		} else if isSysErrIO(err) {
			return errFaultyDisk
		}
		return err
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

	// Stat a volume entry.
	if err = Access(volumeDir); err != nil {
		if osIsNotExist(err) {
			return errVolumeNotFound
		}
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
func (s *xlStorage) deleteFile(basePath, deletePath string, recursive bool) error {
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
		err = s.moveToTrash(deletePath, true)
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
	s.deleteFile(basePath, deletePath, false)

	return nil
}

// DeleteFile - delete a file at path.
func (s *xlStorage) Delete(ctx context.Context, volume string, path string, recursive bool) (err error) {
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return err
	}

	// Stat a volume entry.
	if err = Access(volumeDir); err != nil {
		if osIsNotExist(err) {
			return errVolumeNotFound
		} else if osIsPermission(err) {
			return errVolumeAccessDenied
		} else if isSysErrIO(err) {
			return errFaultyDisk
		}
		return err
	}

	// Following code is needed so that we retain SlashSeparator suffix if any in
	// path argument.
	filePath := pathJoin(volumeDir, path)
	if err = checkPathLength(filePath); err != nil {
		return err
	}

	// Delete file and delete parent directory as well if it's empty.
	return s.deleteFile(volumeDir, filePath, recursive)
}

// RenameData - rename source path to destination path atomically, metadata and data directory.
func (s *xlStorage) RenameData(ctx context.Context, srcVolume, srcPath string, fi FileInfo, dstVolume, dstPath string) (err error) {
	defer func() {
		if err == nil {
			if s.globalSync {
				globalSync()
			}
		}
	}()

	srcVolumeDir, err := s.getVolDir(srcVolume)
	if err != nil {
		return err
	}

	dstVolumeDir, err := s.getVolDir(dstVolume)
	if err != nil {
		return err
	}

	// Stat a volume entry.
	if err = Access(srcVolumeDir); err != nil {
		if osIsNotExist(err) {
			return errVolumeNotFound
		} else if isSysErrIO(err) {
			return errFaultyDisk
		}
		return err
	}

	if err = Access(dstVolumeDir); err != nil {
		if osIsNotExist(err) {
			return errVolumeNotFound
		} else if isSysErrIO(err) {
			return errFaultyDisk
		}
		return err
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
		return err
	}

	if err = checkPathLength(dstFilePath); err != nil {
		return err
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
			return errFileAccessDenied
		}
		if !osIsNotExist(err) {
			return osErrToFileErr(err)
		}
		// errFileNotFound comes here.
		err = s.renameLegacyMetadata(dstVolumeDir, dstPath)
		if err != nil && err != errFileNotFound {
			return err
		}
		if err == nil {
			dstBuf, err = xioutil.ReadFile(dstFilePath)
			if err != nil && !osIsNotExist(err) {
				return osErrToFileErr(err)
			}
		}
	}

	var xlMeta xlMetaV2
	var legacyPreserved bool
	if len(dstBuf) > 0 {
		if isXL2V1Format(dstBuf) {
			if err = xlMeta.Load(dstBuf); err != nil {
				logger.LogIf(s.ctx, err)
				// Data appears corrupt. Drop data.
				xlMeta = xlMetaV2{}
			}
		} else {
			// This code-path is to preserve the legacy data.
			xlMetaLegacy := &xlMetaV1Object{}
			var json = jsoniter.ConfigCompatibleWithStandardLibrary
			if err := json.Unmarshal(dstBuf, xlMetaLegacy); err != nil {
				logger.LogIf(s.ctx, err)
				// Data appears corrupt. Drop data.
			} else {
				if err = xlMeta.AddLegacy(xlMetaLegacy); err != nil {
					logger.LogIf(s.ctx, err)
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
				return osErrToFileErr(err)
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
			return osErrToFileErr(err)
		}

		// legacy data dir means its old content, honor system umask.
		if err = mkdirAll(legacyDataPath, 0777); err != nil {
			// any failed mkdir-calls delete them.
			s.deleteFile(dstVolumeDir, legacyDataPath, true)
			return osErrToFileErr(err)
		}

		for _, entry := range entries {
			// Skip xl.meta renames further, also ignore any directories such as `legacyDataDir`
			if entry == xlStorageFormatFile || strings.HasSuffix(entry, slashSeparator) {
				continue
			}

			if err = Rename(pathJoin(currentDataPath, entry), pathJoin(legacyDataPath, entry)); err != nil {
				// Any failed rename calls un-roll previous transaction.
				s.deleteFile(dstVolumeDir, legacyDataPath, true)

				return osErrToFileErr(err)
			}
		}
	}

	var oldDstDataPath string
	if fi.VersionID == "" {
		// return the latest "null" versionId info
		ofi, err := xlMeta.ToFileInfo(dstVolume, dstPath, nullVersionID)
		if err == nil && !ofi.Deleted {
			if xlMeta.SharedDataDirCountStr(nullVersionID, ofi.DataDir) == 0 {
				// Purge the destination path as we are not preserving anything
				// versioned object was not requested.
				oldDstDataPath = pathJoin(dstVolumeDir, dstPath, ofi.DataDir)
				// if old destination path is same as new destination path
				// there is nothing to purge, this is true in case of healing
				// avoid setting oldDstDataPath at that point.
				if oldDstDataPath == dstDataPath {
					oldDstDataPath = ""
				}
				xlMeta.data.remove(nullVersionID, ofi.DataDir)
			}
		}
		// Empty fi.VersionID indicates that versioning is either
		// suspended or disabled on this bucket. RenameData will replace
		// the 'null' version. We add a free-version to track its tiered
		// content for asynchronous deletion.
		xlMeta.AddFreeVersion(fi)
	}

	if err = xlMeta.AddVersion(fi); err != nil {
		if legacyPreserved {
			// Any failed rename calls un-roll previous transaction.
			s.deleteFile(dstVolumeDir, legacyDataPath, true)
		}
		return err
	}

	dstBuf, err = xlMeta.AppendTo(metaDataPoolGet())
	defer metaDataPoolPut(dstBuf)
	if err != nil {
		logger.LogIf(ctx, err)
		if legacyPreserved {
			// Any failed rename calls un-roll previous transaction.
			s.deleteFile(dstVolumeDir, legacyDataPath, true)
		}
		return errFileCorrupt
	}

	if srcDataPath != "" {
		if err = s.WriteAll(ctx, srcVolume, pathJoin(srcPath, xlStorageFormatFile), dstBuf); err != nil {
			if legacyPreserved {
				// Any failed rename calls un-roll previous transaction.
				s.deleteFile(dstVolumeDir, legacyDataPath, true)
			}
			return err
		}

		// renameAll only for objects that have xl.meta not saved inline.
		if len(fi.Data) == 0 && fi.Size > 0 {
			s.moveToTrash(dstDataPath, true)
			if err = renameAll(srcDataPath, dstDataPath); err != nil {
				if legacyPreserved {
					// Any failed rename calls un-roll previous transaction.
					s.deleteFile(dstVolumeDir, legacyDataPath, true)
				}
				s.deleteFile(dstVolumeDir, dstDataPath, false)

				if err != errFileNotFound {
					logger.LogIf(ctx, err)
				}
				return osErrToFileErr(err)
			}
		}

		// Commit meta-file
		if err = renameAll(srcFilePath, dstFilePath); err != nil {
			if legacyPreserved {
				// Any failed rename calls un-roll previous transaction.
				s.deleteFile(dstVolumeDir, legacyDataPath, true)
			}
			s.deleteFile(dstVolumeDir, dstFilePath, false)

			if err != errFileNotFound {
				logger.LogIf(ctx, err)
			}
			return osErrToFileErr(err)
		}

		// additionally only purge older data at the end of the transaction of new data-dir
		// movement, this is to ensure that previous data references can co-exist for
		// any recoverability.
		if oldDstDataPath != "" {
			s.moveToTrash(oldDstDataPath, true)
		}
	} else {
		// Write meta-file directly, no data
		if err = s.WriteAll(ctx, dstVolume, pathJoin(dstPath, xlStorageFormatFile), dstBuf); err != nil {
			if legacyPreserved {
				// Any failed rename calls un-roll previous transaction.
				s.deleteFile(dstVolumeDir, legacyDataPath, true)
			}
			s.deleteFile(dstVolumeDir, dstFilePath, false)

			logger.LogIf(ctx, err)
			return err
		}
	}

	// srcFilePath is always in minioMetaTmpBucket, an attempt to
	// remove the temporary folder is enough since at this point
	// ideally all transaction should be complete.

	Remove(pathutil.Dir(srcFilePath))
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
	// Stat a volume entry.
	if err = Access(srcVolumeDir); err != nil {
		if osIsNotExist(err) {
			return errVolumeNotFound
		} else if isSysErrIO(err) {
			return errFaultyDisk
		}
		return err
	}

	if err = Access(dstVolumeDir); err != nil {
		if osIsNotExist(err) {
			return errVolumeNotFound
		} else if isSysErrIO(err) {
			return errFaultyDisk
		}
		return err
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
				if isSysErrNotEmpty(err) {
					return errFileAccessDenied
				}
				return err
			}
		}
	}

	if err = renameAll(srcFilePath, dstFilePath); err != nil {
		return osErrToFileErr(err)
	}

	// Remove parent dir of the source file if empty
	parentDir := pathutil.Dir(srcFilePath)
	s.deleteFile(srcVolumeDir, parentDir, false)

	return nil
}

func (s *xlStorage) bitrotVerify(partPath string, partSize int64, algo BitrotAlgorithm, sum []byte, shardSize int64) error {
	// Open the file for reading.
	file, err := Open(partPath)
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
	return bitrotVerify(file, fi.Size(), partSize, algo, sum, shardSize)
}

func (s *xlStorage) VerifyFile(ctx context.Context, volume, path string, fi FileInfo) (err error) {
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return err
	}

	// Stat a volume entry.
	if err = Access(volumeDir); err != nil {
		if osIsNotExist(err) {
			return errVolumeNotFound
		} else if isSysErrIO(err) {
			return errFaultyDisk
		} else if osIsPermission(err) {
			return errVolumeAccessDenied
		}
		return err
	}

	erasure := fi.Erasure
	for _, part := range fi.Parts {
		checksumInfo := erasure.GetChecksumInfo(part.Number)
		partPath := pathJoin(volumeDir, path, fi.DataDir, fmt.Sprintf("part.%d", part.Number))
		if err := s.bitrotVerify(partPath,
			erasure.ShardFileSize(part.Size),
			checksumInfo.Algorithm,
			checksumInfo.Hash, erasure.ShardSize()); err != nil {
			if !IsErr(err, []error{
				errFileNotFound,
				errVolumeNotFound,
				errFileCorrupt,
			}...) {
				logger.GetReqInfo(s.ctx).AppendTags("disk", s.String())
				logger.LogIf(s.ctx, err)
			}
			return err
		}
	}

	return nil
}

func (s *xlStorage) StatInfoFile(ctx context.Context, volume, path string, glob bool) (stat []StatInfo, err error) {
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return stat, err
	}

	// Stat a volume entry.
	if err = Access(volumeDir); err != nil {
		if osIsNotExist(err) {
			return stat, errVolumeNotFound
		} else if isSysErrIO(err) {
			return stat, errFaultyDisk
		} else if osIsPermission(err) {
			return stat, errVolumeAccessDenied
		}
		return stat, err
	}
	var files = []string{pathJoin(volumeDir, path)}
	if glob {
		files, err = filepathx.Glob(pathJoin(volumeDir, path))
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
			return stat, errPathNotFound
		}
		name, err := filepath.Rel(volumeDir, filePath)
		if err != nil {
			name = filePath
		}
		if os.PathSeparator != '/' {
			name = strings.Replace(name, string(os.PathSeparator), "/", -1)
		}
		stat = append(stat, StatInfo{ModTime: st.ModTime(), Size: st.Size(), Name: name, Dir: st.IsDir(), Mode: uint32(st.Mode())})
	}
	return stat, nil
}
