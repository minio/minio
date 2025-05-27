// Copyright (c) 2015-2024 MinIO, Inc.
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
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/cachevalue"
	"github.com/minio/minio/internal/grid"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/logger"
)

//go:generate stringer -type=storageMetric -trimprefix=storageMetric $GOFILE

type storageMetric uint8

const (
	storageMetricMakeVolBulk storageMetric = iota
	storageMetricMakeVol
	storageMetricListVols
	storageMetricStatVol
	storageMetricDeleteVol
	storageMetricWalkDir
	storageMetricListDir
	storageMetricReadFile
	storageMetricAppendFile
	storageMetricCreateFile
	storageMetricReadFileStream
	storageMetricRenameFile
	storageMetricRenameData
	storageMetricCheckParts
	storageMetricDelete
	storageMetricDeleteVersions
	storageMetricVerifyFile
	storageMetricWriteAll
	storageMetricDeleteVersion
	storageMetricWriteMetadata
	storageMetricUpdateMetadata
	storageMetricReadVersion
	storageMetricReadXL
	storageMetricReadAll
	storageMetricStatInfoFile
	storageMetricReadMultiple
	storageMetricDeleteAbandonedParts
	storageMetricDiskInfo
	storageMetricDeleteBulk
	storageMetricRenamePart
	storageMetricReadParts

	// .... add more

	storageMetricLast
)

// Detects change in underlying disk.
type xlStorageDiskIDCheck struct {
	totalWrites           atomic.Uint64
	totalDeletes          atomic.Uint64
	totalErrsAvailability atomic.Uint64 // Captures all data availability errors such as faulty disk, timeout errors.
	totalErrsTimeout      atomic.Uint64 // Captures all timeout only errors

	// apiCalls should be placed first so alignment is guaranteed for atomic operations.
	apiCalls     [storageMetricLast]uint64
	apiLatencies [storageMetricLast]*lockedLastMinuteLatency
	diskID       atomic.Pointer[string]
	storage      *xlStorage
	health       *diskHealthTracker
	healthCheck  bool

	metricsCache *cachevalue.Cache[DiskMetrics]
	diskCtx      context.Context
	diskCancel   context.CancelFunc
}

func (p *xlStorageDiskIDCheck) getMetrics() DiskMetrics {
	p.metricsCache.InitOnce(5*time.Second,
		cachevalue.Opts{},
		func(ctx context.Context) (DiskMetrics, error) {
			diskMetric := DiskMetrics{
				LastMinute: make(map[string]AccElem, len(p.apiLatencies)),
				APICalls:   make(map[string]uint64, len(p.apiCalls)),
			}
			for i, v := range p.apiLatencies {
				diskMetric.LastMinute[storageMetric(i).String()] = v.total()
			}
			for i := range p.apiCalls {
				diskMetric.APICalls[storageMetric(i).String()] = atomic.LoadUint64(&p.apiCalls[i])
			}
			return diskMetric, nil
		},
	)

	diskMetric, _ := p.metricsCache.GetWithCtx(context.Background())
	// Do not need this value to be cached.
	diskMetric.TotalErrorsTimeout = p.totalErrsTimeout.Load()
	diskMetric.TotalErrorsAvailability = p.totalErrsAvailability.Load()

	return diskMetric
}

// lockedLastMinuteLatency accumulates totals lockless for each second.
type lockedLastMinuteLatency struct {
	cachedSec int64
	cached    atomic.Pointer[AccElem]
	mu        sync.Mutex
	init      sync.Once
	lastMinuteLatency
}

func (e *lockedLastMinuteLatency) add(value time.Duration) {
	e.addSize(value, 0)
}

// addSize will add a duration and size.
func (e *lockedLastMinuteLatency) addSize(value time.Duration, sz int64) {
	// alloc on every call, so we have a clean entry to swap in.
	t := time.Now().Unix()
	e.init.Do(func() {
		e.cached.Store(&AccElem{})
		atomic.StoreInt64(&e.cachedSec, t)
	})
	acc := e.cached.Load()
	if lastT := atomic.LoadInt64(&e.cachedSec); lastT != t {
		// Check if lastT was changed by someone else.
		if atomic.CompareAndSwapInt64(&e.cachedSec, lastT, t) {
			// Now we swap in a new.
			newAcc := &AccElem{}
			old := e.cached.Swap(newAcc)
			var a AccElem
			a.Size = atomic.LoadInt64(&old.Size)
			a.Total = atomic.LoadInt64(&old.Total)
			a.N = atomic.LoadInt64(&old.N)
			e.mu.Lock()
			e.addAll(t-1, a)
			e.mu.Unlock()
			acc = newAcc
		} else {
			// We may be able to grab the new accumulator by yielding.
			runtime.Gosched()
			acc = e.cached.Load()
		}
	}
	atomic.AddInt64(&acc.N, 1)
	atomic.AddInt64(&acc.Total, int64(value))
	atomic.AddInt64(&acc.Size, sz)
}

// total returns the total call count and latency for the last minute.
func (e *lockedLastMinuteLatency) total() AccElem {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.getTotal()
}

func newXLStorageDiskIDCheck(storage *xlStorage, healthCheck bool) *xlStorageDiskIDCheck {
	xl := xlStorageDiskIDCheck{
		storage:      storage,
		health:       newDiskHealthTracker(),
		healthCheck:  healthCheck && globalDriveMonitoring,
		metricsCache: cachevalue.New[DiskMetrics](),
	}
	xl.SetDiskID(emptyDiskID)

	xl.totalWrites.Store(xl.storage.getWriteAttribute())
	xl.totalDeletes.Store(xl.storage.getDeleteAttribute())
	xl.diskCtx, xl.diskCancel = context.WithCancel(context.TODO())
	for i := range xl.apiLatencies[:] {
		xl.apiLatencies[i] = &lockedLastMinuteLatency{}
	}
	if xl.healthCheck {
		go xl.monitorDiskWritable(xl.diskCtx)
	}
	return &xl
}

func (p *xlStorageDiskIDCheck) String() string {
	return p.storage.String()
}

func (p *xlStorageDiskIDCheck) IsOnline() bool {
	storedDiskID, err := p.storage.GetDiskID()
	if err != nil {
		return false
	}
	return storedDiskID == *p.diskID.Load()
}

func (p *xlStorageDiskIDCheck) LastConn() time.Time {
	return p.storage.LastConn()
}

func (p *xlStorageDiskIDCheck) IsLocal() bool {
	return p.storage.IsLocal()
}

func (p *xlStorageDiskIDCheck) Endpoint() Endpoint {
	return p.storage.Endpoint()
}

func (p *xlStorageDiskIDCheck) Hostname() string {
	return p.storage.Hostname()
}

func (p *xlStorageDiskIDCheck) Healing() *healingTracker {
	return p.storage.Healing()
}

func (p *xlStorageDiskIDCheck) NSScanner(ctx context.Context, cache dataUsageCache, updates chan<- dataUsageEntry, scanMode madmin.HealScanMode, _ func() bool) (dataUsageCache, error) {
	if contextCanceled(ctx) {
		xioutil.SafeClose(updates)
		return dataUsageCache{}, ctx.Err()
	}

	if err := p.checkDiskStale(); err != nil {
		xioutil.SafeClose(updates)
		return dataUsageCache{}, err
	}

	weSleep := func() bool {
		return scannerIdleMode.Load() == 0
	}

	return p.storage.NSScanner(ctx, cache, updates, scanMode, weSleep)
}

func (p *xlStorageDiskIDCheck) GetDiskLoc() (poolIdx, setIdx, diskIdx int) {
	return p.storage.GetDiskLoc()
}

func (p *xlStorageDiskIDCheck) Close() error {
	p.diskCancel()
	return p.storage.Close()
}

func (p *xlStorageDiskIDCheck) GetDiskID() (string, error) {
	return p.storage.GetDiskID()
}

func (p *xlStorageDiskIDCheck) SetDiskID(id string) {
	p.diskID.Store(&id)
}

func (p *xlStorageDiskIDCheck) checkDiskStale() error {
	if *p.diskID.Load() == emptyDiskID {
		// For empty disk-id we allow the call as the server might be
		// coming up and trying to read format.json or create format.json
		return nil
	}
	storedDiskID, err := p.storage.GetDiskID()
	if err != nil {
		// return any error generated while reading `format.json`
		return err
	}
	if err == nil && *p.diskID.Load() == storedDiskID {
		return nil
	}
	// not the same disk we remember, take it offline.
	return errDiskNotFound
}

func (p *xlStorageDiskIDCheck) DiskInfo(ctx context.Context, opts DiskInfoOptions) (info DiskInfo, err error) {
	if contextCanceled(ctx) {
		return DiskInfo{}, ctx.Err()
	}

	si := p.updateStorageMetrics(storageMetricDiskInfo)
	defer si(0, &err)

	if opts.NoOp {
		if opts.Metrics {
			info.Metrics = p.getMetrics()
		}
		info.Metrics.TotalWrites = p.totalWrites.Load()
		info.Metrics.TotalDeletes = p.totalDeletes.Load()
		info.Metrics.TotalWaiting = uint32(p.health.waiting.Load())
		info.Metrics.TotalErrorsTimeout = p.totalErrsTimeout.Load()
		info.Metrics.TotalErrorsAvailability = p.totalErrsAvailability.Load()
		if p.health.isFaulty() {
			// if disk is already faulty return faulty for 'mc admin info' output and prometheus alerts.
			return info, errFaultyDisk
		}
		return info, nil
	}

	defer func() {
		if opts.Metrics {
			info.Metrics = p.getMetrics()
		}
		info.Metrics.TotalWrites = p.totalWrites.Load()
		info.Metrics.TotalDeletes = p.totalDeletes.Load()
		info.Metrics.TotalWaiting = uint32(p.health.waiting.Load())
		info.Metrics.TotalErrorsTimeout = p.totalErrsTimeout.Load()
		info.Metrics.TotalErrorsAvailability = p.totalErrsAvailability.Load()
	}()

	if p.health.isFaulty() {
		// if disk is already faulty return faulty for 'mc admin info' output and prometheus alerts.
		return info, errFaultyDisk
	}

	info, err = p.storage.DiskInfo(ctx, opts)
	if err != nil {
		return info, err
	}

	// check cached diskID against backend
	// only if its non-empty.
	cachedID := *p.diskID.Load()
	if cachedID != "" && cachedID != info.ID {
		return info, errDiskNotFound
	}
	return info, nil
}

func (p *xlStorageDiskIDCheck) MakeVolBulk(ctx context.Context, volumes ...string) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricMakeVolBulk, volumes...)
	if err != nil {
		return err
	}
	defer done(0, &err)

	w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
	return w.Run(func() error { return p.storage.MakeVolBulk(ctx, volumes...) })
}

func (p *xlStorageDiskIDCheck) MakeVol(ctx context.Context, volume string) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricMakeVol, volume)
	if err != nil {
		return err
	}
	defer done(0, &err)

	w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
	return w.Run(func() error { return p.storage.MakeVol(ctx, volume) })
}

func (p *xlStorageDiskIDCheck) ListVols(ctx context.Context) (vi []VolInfo, err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricListVols, "/")
	if err != nil {
		return nil, err
	}
	defer done(0, &err)

	return p.storage.ListVols(ctx)
}

func (p *xlStorageDiskIDCheck) StatVol(ctx context.Context, volume string) (vol VolInfo, err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricStatVol, volume)
	if err != nil {
		return vol, err
	}
	defer done(0, &err)

	return xioutil.WithDeadline[VolInfo](ctx, globalDriveConfig.GetMaxTimeout(), func(ctx context.Context) (result VolInfo, err error) {
		return p.storage.StatVol(ctx, volume)
	})
}

func (p *xlStorageDiskIDCheck) DeleteVol(ctx context.Context, volume string, forceDelete bool) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricDeleteVol, volume)
	if err != nil {
		return err
	}
	defer done(0, &err)

	w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
	return w.Run(func() error { return p.storage.DeleteVol(ctx, volume, forceDelete) })
}

func (p *xlStorageDiskIDCheck) ListDir(ctx context.Context, origvolume, volume, dirPath string, count int) (s []string, err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricListDir, volume, dirPath)
	if err != nil {
		return nil, err
	}
	defer done(0, &err)

	return p.storage.ListDir(ctx, origvolume, volume, dirPath, count)
}

// Legacy API - does not have any deadlines
func (p *xlStorageDiskIDCheck) ReadFile(ctx context.Context, volume string, path string, offset int64, buf []byte, verifier *BitrotVerifier) (n int64, err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricReadFile, volume, path)
	if err != nil {
		return 0, err
	}
	defer func() {
		done(n, &err)
	}()

	return xioutil.WithDeadline[int64](ctx, globalDriveConfig.GetMaxTimeout(), func(ctx context.Context) (result int64, err error) {
		return p.storage.ReadFile(ctx, volume, path, offset, buf, verifier)
	})
}

// Legacy API - does not have any deadlines
func (p *xlStorageDiskIDCheck) AppendFile(ctx context.Context, volume string, path string, buf []byte) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricAppendFile, volume, path)
	if err != nil {
		return err
	}
	defer done(int64(len(buf)), &err)

	w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
	return w.Run(func() error {
		return p.storage.AppendFile(ctx, volume, path, buf)
	})
}

func (p *xlStorageDiskIDCheck) CreateFile(ctx context.Context, origvolume, volume, path string, size int64, reader io.Reader) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricCreateFile, volume, path)
	if err != nil {
		return err
	}
	defer done(size, &err)

	return p.storage.CreateFile(ctx, origvolume, volume, path, size, io.NopCloser(reader))
}

func (p *xlStorageDiskIDCheck) ReadFileStream(ctx context.Context, volume, path string, offset, length int64) (io.ReadCloser, error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricReadFileStream, volume, path)
	if err != nil {
		return nil, err
	}
	defer done(length, &err)

	return xioutil.WithDeadline[io.ReadCloser](ctx, globalDriveConfig.GetMaxTimeout(), func(ctx context.Context) (result io.ReadCloser, err error) {
		return p.storage.ReadFileStream(ctx, volume, path, offset, length)
	})
}

func (p *xlStorageDiskIDCheck) RenamePart(ctx context.Context, srcVolume, srcPath, dstVolume, dstPath string, meta []byte, skipParent string) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricRenamePart, srcVolume, srcPath, dstVolume, dstPath)
	if err != nil {
		return err
	}
	defer done(0, &err)

	w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
	return w.Run(func() error {
		return p.storage.RenamePart(ctx, srcVolume, srcPath, dstVolume, dstPath, meta, skipParent)
	})
}

func (p *xlStorageDiskIDCheck) RenameFile(ctx context.Context, srcVolume, srcPath, dstVolume, dstPath string) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricRenameFile, srcVolume, srcPath, dstVolume, dstPath)
	if err != nil {
		return err
	}
	defer done(0, &err)

	w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
	return w.Run(func() error { return p.storage.RenameFile(ctx, srcVolume, srcPath, dstVolume, dstPath) })
}

func (p *xlStorageDiskIDCheck) RenameData(ctx context.Context, srcVolume, srcPath string, fi FileInfo, dstVolume, dstPath string, opts RenameOptions) (res RenameDataResp, err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricRenameData, srcPath, fi.DataDir, dstVolume, dstPath)
	if err != nil {
		return res, err
	}
	defer func() {
		if err == nil && !skipAccessChecks(dstVolume) {
			p.storage.setWriteAttribute(p.totalWrites.Add(1))
		}
		done(0, &err)
	}()

	// Copy inline data to a new buffer to function with deadlines.
	if len(fi.Data) > 0 {
		fi.Data = append(grid.GetByteBufferCap(len(fi.Data))[:0], fi.Data...)
	}
	return xioutil.WithDeadline[RenameDataResp](ctx, globalDriveConfig.GetMaxTimeout(), func(ctx context.Context) (res RenameDataResp, err error) {
		if len(fi.Data) > 0 {
			defer grid.PutByteBuffer(fi.Data)
		}
		return p.storage.RenameData(ctx, srcVolume, srcPath, fi, dstVolume, dstPath, opts)
	})
}

func (p *xlStorageDiskIDCheck) CheckParts(ctx context.Context, volume string, path string, fi FileInfo) (*CheckPartsResp, error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricCheckParts, volume, path)
	if err != nil {
		return nil, err
	}
	defer done(0, &err)

	return p.storage.CheckParts(ctx, volume, path, fi)
}

func (p *xlStorageDiskIDCheck) DeleteBulk(ctx context.Context, volume string, paths ...string) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricDeleteBulk, append([]string{volume}, paths...)...)
	if err != nil {
		return err
	}
	defer done(0, &err)

	return p.storage.DeleteBulk(ctx, volume, paths...)
}

func (p *xlStorageDiskIDCheck) Delete(ctx context.Context, volume string, path string, deleteOpts DeleteOptions) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricDelete, volume, path)
	if err != nil {
		return err
	}
	defer done(0, &err)

	w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
	return w.Run(func() error { return p.storage.Delete(ctx, volume, path, deleteOpts) })
}

// DeleteVersions deletes slice of versions, it can be same object
// or multiple objects.
func (p *xlStorageDiskIDCheck) DeleteVersions(ctx context.Context, volume string, versions []FileInfoVersions, opts DeleteOptions) (errs []error) {
	// Merely for tracing storage
	path := ""
	if len(versions) > 0 {
		path = versions[0].Name
	}
	errs = make([]error, len(versions))
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricDeleteVersions, volume, path)
	if err != nil {
		for i := range errs {
			errs[i] = ctx.Err()
		}
		return errs
	}
	defer func() {
		if !skipAccessChecks(volume) {
			var permanentDeletes uint64
			var deleteMarkers uint64

			for i, nerr := range errs {
				if nerr != nil {
					continue
				}
				for _, fi := range versions[i].Versions {
					if fi.Deleted {
						// Delete markers are a write operation not a permanent delete.
						deleteMarkers++
						continue
					}
					permanentDeletes++
				}
			}
			if deleteMarkers > 0 {
				p.storage.setWriteAttribute(p.totalWrites.Add(deleteMarkers))
			}
			if permanentDeletes > 0 {
				p.storage.setDeleteAttribute(p.totalDeletes.Add(permanentDeletes))
			}
		}
		done(0, &err)
	}()

	errs = p.storage.DeleteVersions(ctx, volume, versions, opts)
	for i := range errs {
		if errs[i] != nil {
			err = errs[i]
			break
		}
	}

	return errs
}

func (p *xlStorageDiskIDCheck) VerifyFile(ctx context.Context, volume, path string, fi FileInfo) (*CheckPartsResp, error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricVerifyFile, volume, path)
	if err != nil {
		return nil, err
	}
	defer done(0, &err)

	return p.storage.VerifyFile(ctx, volume, path, fi)
}

func (p *xlStorageDiskIDCheck) WriteAll(ctx context.Context, volume string, path string, b []byte) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricWriteAll, volume, path)
	if err != nil {
		return err
	}
	defer done(int64(len(b)), &err)

	w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
	return w.Run(func() error { return p.storage.WriteAll(ctx, volume, path, b) })
}

func (p *xlStorageDiskIDCheck) DeleteVersion(ctx context.Context, volume, path string, fi FileInfo, forceDelMarker bool, opts DeleteOptions) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricDeleteVersion, volume, path)
	if err != nil {
		return err
	}
	defer func() {
		defer done(0, &err)

		if err == nil && !skipAccessChecks(volume) {
			if opts.UndoWrite {
				p.storage.setWriteAttribute(p.totalWrites.Add(^uint64(0)))
				return
			}

			if fi.Deleted {
				// Delete markers are a write operation not a permanent delete.
				p.storage.setWriteAttribute(p.totalWrites.Add(1))
				return
			}

			p.storage.setDeleteAttribute(p.totalDeletes.Add(1))
		}
	}()

	w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
	return w.Run(func() error { return p.storage.DeleteVersion(ctx, volume, path, fi, forceDelMarker, opts) })
}

func (p *xlStorageDiskIDCheck) UpdateMetadata(ctx context.Context, volume, path string, fi FileInfo, opts UpdateMetadataOpts) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricUpdateMetadata, volume, path)
	if err != nil {
		return err
	}
	defer done(0, &err)

	w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
	return w.Run(func() error { return p.storage.UpdateMetadata(ctx, volume, path, fi, opts) })
}

func (p *xlStorageDiskIDCheck) WriteMetadata(ctx context.Context, origvolume, volume, path string, fi FileInfo) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricWriteMetadata, volume, path)
	if err != nil {
		return err
	}
	defer done(0, &err)

	w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
	return w.Run(func() error { return p.storage.WriteMetadata(ctx, origvolume, volume, path, fi) })
}

func (p *xlStorageDiskIDCheck) ReadVersion(ctx context.Context, origvolume, volume, path, versionID string, opts ReadOptions) (fi FileInfo, err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricReadVersion, volume, path)
	if err != nil {
		return fi, err
	}
	defer done(0, &err)

	return xioutil.WithDeadline[FileInfo](ctx, globalDriveConfig.GetMaxTimeout(), func(ctx context.Context) (result FileInfo, err error) {
		return p.storage.ReadVersion(ctx, origvolume, volume, path, versionID, opts)
	})
}

func (p *xlStorageDiskIDCheck) ReadAll(ctx context.Context, volume string, path string) (buf []byte, err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricReadAll, volume, path)
	if err != nil {
		return nil, err
	}
	var sz int
	defer func() {
		sz = len(buf)
		done(int64(sz), &err)
	}()

	return xioutil.WithDeadline[[]byte](ctx, globalDriveConfig.GetMaxTimeout(), func(ctx context.Context) (result []byte, err error) {
		return p.storage.ReadAll(ctx, volume, path)
	})
}

func (p *xlStorageDiskIDCheck) ReadXL(ctx context.Context, volume string, path string, readData bool) (rf RawFileInfo, err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricReadXL, volume, path)
	if err != nil {
		return RawFileInfo{}, err
	}
	defer func() {
		done(int64(len(rf.Buf)), &err)
	}()

	return xioutil.WithDeadline[RawFileInfo](ctx, globalDriveConfig.GetMaxTimeout(), func(ctx context.Context) (result RawFileInfo, err error) {
		return p.storage.ReadXL(ctx, volume, path, readData)
	})
}

func (p *xlStorageDiskIDCheck) StatInfoFile(ctx context.Context, volume, path string, glob bool) (stat []StatInfo, err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricStatInfoFile, volume, path)
	if err != nil {
		return nil, err
	}
	defer done(0, &err)

	return p.storage.StatInfoFile(ctx, volume, path, glob)
}

func (p *xlStorageDiskIDCheck) ReadParts(ctx context.Context, volume string, partMetaPaths ...string) ([]*ObjectPartInfo, error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricReadParts, volume, path.Dir(partMetaPaths[0]))
	if err != nil {
		return nil, err
	}
	defer done(0, &err)

	return p.storage.ReadParts(ctx, volume, partMetaPaths...)
}

// ReadMultiple will read multiple files and send each files as response.
// Files are read and returned in the given order.
// The resp channel is closed before the call returns.
// Only a canceled context will return an error.
func (p *xlStorageDiskIDCheck) ReadMultiple(ctx context.Context, req ReadMultipleReq, resp chan<- ReadMultipleResp) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricReadMultiple, req.Bucket, req.Prefix)
	if err != nil {
		xioutil.SafeClose(resp)
		return err
	}
	defer done(0, &err)

	return p.storage.ReadMultiple(ctx, req, resp)
}

// CleanAbandonedData will read metadata of the object on disk
// and delete any data directories and inline data that isn't referenced in metadata.
func (p *xlStorageDiskIDCheck) CleanAbandonedData(ctx context.Context, volume string, path string) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricDeleteAbandonedParts, volume, path)
	if err != nil {
		return err
	}
	defer done(0, &err)

	w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
	return w.Run(func() error { return p.storage.CleanAbandonedData(ctx, volume, path) })
}

func storageTrace(s storageMetric, startTime time.Time, duration time.Duration, path string, size int64, err string, custom map[string]string) madmin.TraceInfo {
	return madmin.TraceInfo{
		TraceType: madmin.TraceStorage,
		Time:      startTime,
		NodeName:  globalLocalNodeName,
		FuncName:  "storage." + s.String(),
		Duration:  duration,
		Bytes:     size,
		Path:      path,
		Error:     err,
		Custom:    custom,
	}
}

func scannerTrace(s scannerMetric, startTime time.Time, duration time.Duration, path string, custom map[string]string) madmin.TraceInfo {
	return madmin.TraceInfo{
		TraceType: madmin.TraceScanner,
		Time:      startTime,
		NodeName:  globalLocalNodeName,
		FuncName:  "scanner." + s.String(),
		Duration:  duration,
		Path:      path,
		Custom:    custom,
	}
}

// Update storage metrics
func (p *xlStorageDiskIDCheck) updateStorageMetrics(s storageMetric, paths ...string) func(sz int64, err *error) {
	startTime := time.Now()
	trace := globalTrace.NumSubscribers(madmin.TraceStorage) > 0
	return func(sz int64, errp *error) {
		duration := time.Since(startTime)

		var err error
		if errp != nil && *errp != nil {
			err = *errp
		}

		atomic.AddUint64(&p.apiCalls[s], 1)
		if IsErr(err, []error{
			errFaultyDisk,
			errFaultyRemoteDisk,
			context.DeadlineExceeded,
		}...) {
			p.totalErrsAvailability.Add(1)
			if errors.Is(err, context.DeadlineExceeded) {
				p.totalErrsTimeout.Add(1)
			}
		}

		p.apiLatencies[s].add(duration)

		if trace {
			custom := make(map[string]string, 2)
			paths = append([]string{p.String()}, paths...)
			var errStr string
			if err != nil {
				errStr = err.Error()
			}
			custom["total-errs-timeout"] = strconv.FormatUint(p.totalErrsTimeout.Load(), 10)
			custom["total-errs-availability"] = strconv.FormatUint(p.totalErrsAvailability.Load(), 10)
			globalTrace.Publish(storageTrace(s, startTime, duration, strings.Join(paths, " "), sz, errStr, custom))
		}
	}
}

const (
	diskHealthOK int32 = iota
	diskHealthFaulty
)

type diskHealthTracker struct {
	// atomic time of last success
	lastSuccess int64

	// atomic time of last time a token was grabbed.
	lastStarted int64

	// Atomic status of disk.
	status atomic.Int32

	// Atomic number indicates if a disk is hung
	waiting atomic.Int32
}

// newDiskHealthTracker creates a new disk health tracker.
func newDiskHealthTracker() *diskHealthTracker {
	d := diskHealthTracker{
		lastSuccess: time.Now().UnixNano(),
		lastStarted: time.Now().UnixNano(),
	}
	d.status.Store(diskHealthOK)
	return &d
}

// logSuccess will update the last successful operation time.
func (d *diskHealthTracker) logSuccess() {
	atomic.StoreInt64(&d.lastSuccess, time.Now().UnixNano())
}

func (d *diskHealthTracker) isFaulty() bool {
	return d.status.Load() == diskHealthFaulty
}

type (
	healthDiskCtxKey   struct{}
	healthDiskCtxValue struct {
		lastSuccess *int64
	}
)

// logSuccess will update the last successful operation time.
func (h *healthDiskCtxValue) logSuccess() {
	atomic.StoreInt64(h.lastSuccess, time.Now().UnixNano())
}

// noopDoneFunc is a no-op done func.
// Can be reused.
var noopDoneFunc = func(_ int64, _ *error) {}

// TrackDiskHealth for this request.
// When a non-nil error is returned 'done' MUST be called
// with the status of the response, if it corresponds to disk health.
// If the pointer sent to done is non-nil AND the error
// is either nil or io.EOF the disk is considered good.
// So if unsure if the disk status is ok, return nil as a parameter to done.
// Shadowing will work as long as return error is named: https://go.dev/play/p/sauq86SsTN2
func (p *xlStorageDiskIDCheck) TrackDiskHealth(ctx context.Context, s storageMetric, paths ...string) (c context.Context, done func(int64, *error), err error) {
	done = noopDoneFunc
	if contextCanceled(ctx) {
		return ctx, done, ctx.Err()
	}

	if p.health.status.Load() != diskHealthOK {
		return ctx, done, errFaultyDisk
	}

	// Verify if the disk is not stale
	// - missing format.json (unformatted drive)
	// - format.json is valid but invalid 'uuid'
	if err = p.checkDiskStale(); err != nil {
		return ctx, done, err
	}

	// Disallow recursive tracking to avoid deadlocks.
	if ctx.Value(healthDiskCtxKey{}) != nil {
		done = p.updateStorageMetrics(s, paths...)
		return ctx, done, nil
	}

	if contextCanceled(ctx) {
		return ctx, done, ctx.Err()
	}

	atomic.StoreInt64(&p.health.lastStarted, time.Now().UnixNano())
	p.health.waiting.Add(1)

	ctx = context.WithValue(ctx, healthDiskCtxKey{}, &healthDiskCtxValue{lastSuccess: &p.health.lastSuccess})
	si := p.updateStorageMetrics(s, paths...)
	var once sync.Once
	return ctx, func(sz int64, errp *error) {
		p.health.waiting.Add(-1)
		once.Do(func() {
			if errp != nil {
				err := *errp
				if err == nil || errors.Is(err, io.EOF) {
					p.health.logSuccess()
				}
			}
			si(sz, errp)
		})
	}, nil
}

var toWrite = []byte{2048: 42}

// monitorDiskStatus should be called once when a drive has been marked offline.
// Once the disk has been deemed ok, it will return to online status.
func (p *xlStorageDiskIDCheck) monitorDiskStatus(spent time.Duration, fn string) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()

	for range t.C {
		if contextCanceled(p.diskCtx) {
			return
		}

		err := p.storage.WriteAll(context.Background(), minioMetaTmpBucket, fn, toWrite)
		if err != nil {
			continue
		}

		b, err := p.storage.ReadAll(context.Background(), minioMetaTmpBucket, fn)
		if err != nil || len(b) != len(toWrite) {
			continue
		}

		err = p.storage.Delete(context.Background(), minioMetaTmpBucket, fn, DeleteOptions{
			Recursive: false,
			Immediate: false,
		})

		if err == nil {
			logger.Event(context.Background(), "healthcheck",
				"node(%s): Read/Write/Delete successful, bringing drive %s online", globalLocalNodeName, p.storage.String())
			p.health.status.Store(diskHealthOK)
			p.health.waiting.Add(-1)
			return
		}
	}
}

// monitorDiskStatus should be called once when a drive has been marked offline.
// Once the disk has been deemed ok, it will return to online status.
func (p *xlStorageDiskIDCheck) monitorDiskWritable(ctx context.Context) {
	var (
		// We check every 15 seconds if the disk is writable and we can read back.
		checkEvery = 15 * time.Second

		// If the disk has completed an operation successfully within last 5 seconds, don't check it.
		skipIfSuccessBefore = 5 * time.Second
	)

	// if disk max timeout is smaller than checkEvery window
	// reduce checks by a second.
	if globalDriveConfig.GetMaxTimeout() <= checkEvery {
		checkEvery = globalDriveConfig.GetMaxTimeout() - time.Second
		if checkEvery <= 0 {
			checkEvery = globalDriveConfig.GetMaxTimeout()
		}
	}

	// if disk max timeout is smaller than skipIfSuccessBefore window
	// reduce the skipIfSuccessBefore by a second.
	if globalDriveConfig.GetMaxTimeout() <= skipIfSuccessBefore {
		skipIfSuccessBefore = globalDriveConfig.GetMaxTimeout() - time.Second
		if skipIfSuccessBefore <= 0 {
			skipIfSuccessBefore = globalDriveConfig.GetMaxTimeout()
		}
	}

	t := time.NewTicker(checkEvery)
	defer t.Stop()
	fn := mustGetUUID()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	monitor := func() bool {
		if contextCanceled(ctx) {
			return false
		}

		if p.health.status.Load() != diskHealthOK {
			return true
		}

		if time.Since(time.Unix(0, atomic.LoadInt64(&p.health.lastSuccess))) < skipIfSuccessBefore {
			// We recently saw a success - no need to check.
			return true
		}

		goOffline := func(err error, spent time.Duration) {
			if p.health.status.CompareAndSwap(diskHealthOK, diskHealthFaulty) {
				storageLogAlwaysIf(ctx, fmt.Errorf("node(%s): taking drive %s offline: %v", globalLocalNodeName, p.storage.String(), err))
				p.health.waiting.Add(1)
				go p.monitorDiskStatus(spent, fn)
			}
		}

		// Offset checks a bit.
		time.Sleep(time.Duration(rng.Int63n(int64(1 * time.Second))))

		dctx, dcancel := context.WithCancel(ctx)
		started := time.Now()
		go func() {
			timeout := time.NewTimer(globalDriveConfig.GetMaxTimeout())
			select {
			case <-dctx.Done():
				if !timeout.Stop() {
					<-timeout.C
				}
			case <-timeout.C:
				spent := time.Since(started)
				goOffline(fmt.Errorf("unable to write+read for %v", spent.Round(time.Millisecond)), spent)
			}
		}()

		func() {
			defer dcancel()

			err := p.storage.WriteAll(ctx, minioMetaTmpBucket, fn, toWrite)
			if err != nil {
				if osErrToFileErr(err) == errFaultyDisk {
					goOffline(fmt.Errorf("unable to write: %w", err), 0)
				}
				return
			}
			b, err := p.storage.ReadAll(context.Background(), minioMetaTmpBucket, fn)
			if err != nil || len(b) != len(toWrite) {
				if osErrToFileErr(err) == errFaultyDisk {
					goOffline(fmt.Errorf("unable to read: %w", err), 0)
				}
				return
			}
		}()

		// Continue to monitor
		return true
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if !monitor() {
				return
			}
		}
	}
}

// checkID will check if the disk ID matches the provided ID.
func (p *xlStorageDiskIDCheck) checkID(wantID string) (err error) {
	if wantID == "" {
		return nil
	}
	id, err := p.storage.GetDiskID()
	if err != nil {
		return err
	}
	if id != wantID {
		return fmt.Errorf("disk ID %s does not match. disk reports %s", wantID, id)
	}
	return nil
}

// diskHealthCheckOK will check if the provided error is nil
// and update disk status if good.
// For convenience a bool is returned to indicate any error state
// that is not io.EOF.
func diskHealthCheckOK(ctx context.Context, err error) bool {
	// Check if context has a disk health check.
	tracker, ok := ctx.Value(healthDiskCtxKey{}).(*healthDiskCtxValue)
	if !ok {
		// No tracker, return
		return err == nil || errors.Is(err, io.EOF)
	}
	if err == nil || errors.Is(err, io.EOF) {
		tracker.logSuccess()
		return true
	}
	return false
}

// diskHealthWrapper provides either a io.Reader or io.Writer
// that updates status of the provided tracker.
// Use through diskHealthReader or diskHealthWriter.
type diskHealthWrapper struct {
	tracker *healthDiskCtxValue
	r       io.Reader
	w       io.Writer
}

func (d *diskHealthWrapper) Read(p []byte) (int, error) {
	if d.r == nil {
		return 0, fmt.Errorf("diskHealthWrapper: Read with no reader")
	}
	n, err := d.r.Read(p)
	if err == nil || err == io.EOF && n > 0 {
		d.tracker.logSuccess()
	}
	return n, err
}

func (d *diskHealthWrapper) Write(p []byte) (int, error) {
	if d.w == nil {
		return 0, fmt.Errorf("diskHealthWrapper: Write with no writer")
	}
	n, err := d.w.Write(p)
	if err == nil && n == len(p) {
		d.tracker.logSuccess()
	}
	return n, err
}

// diskHealthReader provides a wrapper that will update disk health on
// ctx, on every successful read.
// This should only be used directly at the os/syscall level,
// otherwise buffered operations may return false health checks.
func diskHealthReader(ctx context.Context, r io.Reader) io.Reader {
	// Check if context has a disk health check.
	tracker, ok := ctx.Value(healthDiskCtxKey{}).(*healthDiskCtxValue)
	if !ok {
		// No need to wrap
		return r
	}
	return &diskHealthWrapper{r: r, tracker: tracker}
}

// diskHealthWriter provides a wrapper that will update disk health on
// ctx, on every successful write.
// This should only be used directly at the os/syscall level,
// otherwise buffered operations may return false health checks.
func diskHealthWriter(ctx context.Context, w io.Writer) io.Writer {
	// Check if context has a disk health check.
	tracker, ok := ctx.Value(healthDiskCtxKey{}).(*healthDiskCtxValue)
	if !ok {
		// No need to wrap
		return w
	}
	return &diskHealthWrapper{w: w, tracker: tracker}
}
