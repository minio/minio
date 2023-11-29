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
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/config"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v2/env"
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

	// .... add more

	storageMetricLast
)

// Detects change in underlying disk.
type xlStorageDiskIDCheck struct {
	totalErrsAvailability uint64 // Captures all data availability errors such as permission denied, faulty disk and timeout errors.
	totalErrsTimeout      uint64 // Captures all timeout only errors
	// apiCalls should be placed first so alignment is guaranteed for atomic operations.
	apiCalls     [storageMetricLast]uint64
	apiLatencies [storageMetricLast]*lockedLastMinuteLatency
	diskID       string
	storage      *xlStorage
	health       *diskHealthTracker

	// diskStartChecking is a threshold above which we will start to check
	// the state of disks, generally this value is less than diskMaxConcurrent
	diskStartChecking int

	// diskMaxConcurrent represents maximum number of running concurrent
	// operations for local and (incoming) remote disk operations.
	diskMaxConcurrent int

	metricsCache timedValue
	diskCtx      context.Context
	cancel       context.CancelFunc
}

func (p *xlStorageDiskIDCheck) getMetrics() DiskMetrics {
	p.metricsCache.Once.Do(func() {
		p.metricsCache.TTL = 1 * time.Second
		p.metricsCache.Update = func() (interface{}, error) {
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
			diskMetric.TotalErrorsAvailability = atomic.LoadUint64(&p.totalErrsAvailability)
			diskMetric.TotalErrorsTimeout = atomic.LoadUint64(&p.totalErrsTimeout)
			return diskMetric, nil
		}
	})
	m, _ := p.metricsCache.Get()
	return m.(DiskMetrics)
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
			e.lastMinuteLatency.addAll(t-1, a)
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
	return e.lastMinuteLatency.getTotal()
}

var maxConcurrentOnce sync.Once

func newXLStorageDiskIDCheck(storage *xlStorage, healthCheck bool) *xlStorageDiskIDCheck {
	// diskMaxConcurrent represents maximum number of running concurrent
	// operations for local and (incoming) remote disk operations.
	//
	// this value is a placeholder it is overridden via ENV for custom settings
	// or this default value is used to pick the correct value HDDs v/s NVMe's
	diskMaxConcurrent := -1
	maxConcurrentOnce.Do(func() {
		s := env.Get("_MINIO_DRIVE_MAX_CONCURRENT", "")
		if s == "" {
			s = env.Get("_MINIO_DISK_MAX_CONCURRENT", "")
		}
		if s != "" {
			diskMaxConcurrent, _ = strconv.Atoi(s)
		}
	})

	if diskMaxConcurrent <= 0 {
		diskMaxConcurrent = 512
		if storage.rotational {
			diskMaxConcurrent = int(storage.nrRequests) / 2
			if diskMaxConcurrent < 32 {
				diskMaxConcurrent = 32
			}
		}
	}

	diskStartChecking := 16 + diskMaxConcurrent/8
	if diskStartChecking > diskMaxConcurrent {
		diskStartChecking = diskMaxConcurrent
	}

	xl := xlStorageDiskIDCheck{
		storage:           storage,
		health:            newDiskHealthTracker(diskMaxConcurrent),
		diskMaxConcurrent: diskMaxConcurrent,
		diskStartChecking: diskStartChecking,
	}
	xl.diskCtx, xl.cancel = context.WithCancel(context.TODO())
	for i := range xl.apiLatencies[:] {
		xl.apiLatencies[i] = &lockedLastMinuteLatency{}
	}
	if healthCheck && diskActiveMonitoring {
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
	return storedDiskID == p.diskID
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

func (p *xlStorageDiskIDCheck) NSScanner(ctx context.Context, cache dataUsageCache, updates chan<- dataUsageEntry, scanMode madmin.HealScanMode) (dataUsageCache, error) {
	if contextCanceled(ctx) {
		close(updates)
		return dataUsageCache{}, ctx.Err()
	}

	if err := p.checkDiskStale(); err != nil {
		close(updates)
		return dataUsageCache{}, err
	}
	return p.storage.NSScanner(ctx, cache, updates, scanMode)
}

func (p *xlStorageDiskIDCheck) GetDiskLoc() (poolIdx, setIdx, diskIdx int) {
	return p.storage.GetDiskLoc()
}

func (p *xlStorageDiskIDCheck) SetDiskLoc(poolIdx, setIdx, diskIdx int) {
	p.storage.SetDiskLoc(poolIdx, setIdx, diskIdx)
}

func (p *xlStorageDiskIDCheck) Close() error {
	p.cancel()
	return p.storage.Close()
}

func (p *xlStorageDiskIDCheck) GetDiskID() (string, error) {
	return p.storage.GetDiskID()
}

func (p *xlStorageDiskIDCheck) SetDiskID(id string) {
	p.diskID = id
}

func (p *xlStorageDiskIDCheck) checkDiskStale() error {
	if p.diskID == "" {
		// For empty disk-id we allow the call as the server might be
		// coming up and trying to read format.json or create format.json
		return nil
	}
	storedDiskID, err := p.storage.GetDiskID()
	if err != nil {
		// return any error generated while reading `format.json`
		return err
	}
	if err == nil && p.diskID == storedDiskID {
		return nil
	}
	// not the same disk we remember, take it offline.
	return errDiskNotFound
}

func (p *xlStorageDiskIDCheck) DiskInfo(ctx context.Context, metrics bool) (info DiskInfo, err error) {
	if contextCanceled(ctx) {
		return DiskInfo{}, ctx.Err()
	}

	si := p.updateStorageMetrics(storageMetricDiskInfo)
	defer si(&err)

	defer func() {
		if metrics {
			info.Metrics = p.getMetrics()
		}
	}()

	if p.health.isFaulty() {
		// if disk is already faulty return faulty for 'mc admin info' output and prometheus alerts.
		return info, errFaultyDisk
	}

	info, err = p.storage.DiskInfo(ctx, metrics)
	if err != nil {
		return info, err
	}

	// check cached diskID against backend
	// only if its non-empty.
	if p.diskID != "" && p.diskID != info.ID {
		return info, errDiskNotFound
	}

	return info, nil
}

func (p *xlStorageDiskIDCheck) MakeVolBulk(ctx context.Context, volumes ...string) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricMakeVolBulk, volumes...)
	if err != nil {
		return err
	}
	defer done(&err)

	w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
	return w.Run(func() error { return p.storage.MakeVolBulk(ctx, volumes...) })
}

func (p *xlStorageDiskIDCheck) MakeVol(ctx context.Context, volume string) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricMakeVol, volume)
	if err != nil {
		return err
	}
	defer done(&err)

	w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
	return w.Run(func() error { return p.storage.MakeVol(ctx, volume) })
}

func (p *xlStorageDiskIDCheck) ListVols(ctx context.Context) (vi []VolInfo, err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricListVols, "/")
	if err != nil {
		return nil, err
	}
	defer done(&err)

	return p.storage.ListVols(ctx)
}

func (p *xlStorageDiskIDCheck) StatVol(ctx context.Context, volume string) (vol VolInfo, err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricStatVol, volume)
	if err != nil {
		return vol, err
	}
	defer done(&err)

	w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
	err = w.Run(func() error {
		var ierr error
		vol, ierr = p.storage.StatVol(ctx, volume)
		return ierr
	})
	return vol, err
}

func (p *xlStorageDiskIDCheck) DeleteVol(ctx context.Context, volume string, forceDelete bool) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricDeleteVol, volume)
	if err != nil {
		return err
	}
	defer done(&err)

	w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
	return w.Run(func() error { return p.storage.DeleteVol(ctx, volume, forceDelete) })
}

func (p *xlStorageDiskIDCheck) ListDir(ctx context.Context, volume, dirPath string, count int) (s []string, err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricListDir, volume, dirPath)
	if err != nil {
		return nil, err
	}
	defer done(&err)

	return p.storage.ListDir(ctx, volume, dirPath, count)
}

// Legacy API - does not have any deadlines
func (p *xlStorageDiskIDCheck) ReadFile(ctx context.Context, volume string, path string, offset int64, buf []byte, verifier *BitrotVerifier) (n int64, err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricReadFile, volume, path)
	if err != nil {
		return 0, err
	}
	defer done(&err)

	w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
	err = w.Run(func() error {
		n, err = p.storage.ReadFile(ctx, volume, path, offset, buf, verifier)
		return err
	})

	return n, err
}

// Legacy API - does not have any deadlines
func (p *xlStorageDiskIDCheck) AppendFile(ctx context.Context, volume string, path string, buf []byte) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricAppendFile, volume, path)
	if err != nil {
		return err
	}
	defer done(&err)

	w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
	return w.Run(func() error {
		return p.storage.AppendFile(ctx, volume, path, buf)
	})
}

func (p *xlStorageDiskIDCheck) CreateFile(ctx context.Context, volume, path string, size int64, reader io.Reader) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricCreateFile, volume, path)
	if err != nil {
		return err
	}
	defer done(&err)

	return p.storage.CreateFile(ctx, volume, path, size, xioutil.NewDeadlineReader(io.NopCloser(reader), globalDriveConfig.GetMaxTimeout()))
}

func (p *xlStorageDiskIDCheck) ReadFileStream(ctx context.Context, volume, path string, offset, length int64) (io.ReadCloser, error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricReadFileStream, volume, path)
	if err != nil {
		return nil, err
	}
	defer done(&err)

	w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())

	var rc io.ReadCloser
	err = w.Run(func() error {
		var ierr error
		rc, ierr = p.storage.ReadFileStream(ctx, volume, path, offset, length)
		return ierr
	})
	if err != nil {
		return nil, err
	}

	return xioutil.NewDeadlineReader(rc, globalDriveConfig.GetMaxTimeout()), nil
}

func (p *xlStorageDiskIDCheck) RenameFile(ctx context.Context, srcVolume, srcPath, dstVolume, dstPath string) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricRenameFile, srcVolume, srcPath, dstVolume, dstPath)
	if err != nil {
		return err
	}
	defer done(&err)

	w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
	return w.Run(func() error { return p.storage.RenameFile(ctx, srcVolume, srcPath, dstVolume, dstPath) })
}

func (p *xlStorageDiskIDCheck) RenameData(ctx context.Context, srcVolume, srcPath string, fi FileInfo, dstVolume, dstPath string) (sign uint64, err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricRenameData, srcPath, fi.DataDir, dstVolume, dstPath)
	if err != nil {
		return 0, err
	}
	defer done(&err)

	w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
	err = w.Run(func() error {
		var ierr error
		sign, ierr = p.storage.RenameData(ctx, srcVolume, srcPath, fi, dstVolume, dstPath)
		return ierr
	})
	return sign, err
}

func (p *xlStorageDiskIDCheck) CheckParts(ctx context.Context, volume string, path string, fi FileInfo) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricCheckParts, volume, path)
	if err != nil {
		return err
	}
	defer done(&err)

	w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
	return w.Run(func() error { return p.storage.CheckParts(ctx, volume, path, fi) })
}

func (p *xlStorageDiskIDCheck) Delete(ctx context.Context, volume string, path string, deleteOpts DeleteOptions) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricDelete, volume, path)
	if err != nil {
		return err
	}
	defer done(&err)

	w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
	return w.Run(func() error { return p.storage.Delete(ctx, volume, path, deleteOpts) })
}

// DeleteVersions deletes slice of versions, it can be same object
// or multiple objects.
func (p *xlStorageDiskIDCheck) DeleteVersions(ctx context.Context, volume string, versions []FileInfoVersions) (errs []error) {
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
	defer done(&err)

	errs = p.storage.DeleteVersions(ctx, volume, versions)
	for i := range errs {
		if errs[i] != nil {
			err = errs[i]
			break
		}
	}

	return errs
}

func (p *xlStorageDiskIDCheck) VerifyFile(ctx context.Context, volume, path string, fi FileInfo) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricVerifyFile, volume, path)
	if err != nil {
		return err
	}
	defer done(&err)

	return p.storage.VerifyFile(ctx, volume, path, fi)
}

func (p *xlStorageDiskIDCheck) WriteAll(ctx context.Context, volume string, path string, b []byte) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricWriteAll, volume, path)
	if err != nil {
		return err
	}
	defer done(&err)

	w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
	return w.Run(func() error { return p.storage.WriteAll(ctx, volume, path, b) })
}

func (p *xlStorageDiskIDCheck) DeleteVersion(ctx context.Context, volume, path string, fi FileInfo, forceDelMarker bool) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricDeleteVersion, volume, path)
	if err != nil {
		return err
	}
	defer done(&err)

	w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
	return w.Run(func() error { return p.storage.DeleteVersion(ctx, volume, path, fi, forceDelMarker) })
}

func (p *xlStorageDiskIDCheck) UpdateMetadata(ctx context.Context, volume, path string, fi FileInfo, opts UpdateMetadataOpts) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricUpdateMetadata, volume, path)
	if err != nil {
		return err
	}
	defer done(&err)

	w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
	return w.Run(func() error { return p.storage.UpdateMetadata(ctx, volume, path, fi, opts) })
}

func (p *xlStorageDiskIDCheck) WriteMetadata(ctx context.Context, volume, path string, fi FileInfo) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricWriteMetadata, volume, path)
	if err != nil {
		return err
	}
	defer done(&err)

	w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
	return w.Run(func() error { return p.storage.WriteMetadata(ctx, volume, path, fi) })
}

func (p *xlStorageDiskIDCheck) ReadVersion(ctx context.Context, volume, path, versionID string, opts ReadOptions) (fi FileInfo, err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricReadVersion, volume, path)
	if err != nil {
		return fi, err
	}
	defer done(&err)

	w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
	rerr := w.Run(func() error {
		fi, err = p.storage.ReadVersion(ctx, volume, path, versionID, opts)
		return err
	})
	if rerr != nil {
		return fi, rerr
	}
	return fi, err
}

func (p *xlStorageDiskIDCheck) ReadAll(ctx context.Context, volume string, path string) (buf []byte, err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricReadAll, volume, path)
	if err != nil {
		return nil, err
	}
	defer done(&err)

	w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
	rerr := w.Run(func() error {
		buf, err = p.storage.ReadAll(ctx, volume, path)
		return err
	})
	if rerr != nil {
		return buf, rerr
	}
	return buf, err
}

func (p *xlStorageDiskIDCheck) ReadXL(ctx context.Context, volume string, path string, readData bool) (rf RawFileInfo, err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricReadXL, volume, path)
	if err != nil {
		return RawFileInfo{}, err
	}
	defer done(&err)

	w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
	rerr := w.Run(func() error {
		rf, err = p.storage.ReadXL(ctx, volume, path, readData)
		return err
	})
	if rerr != nil {
		return rf, rerr
	}
	return rf, err
}

func (p *xlStorageDiskIDCheck) StatInfoFile(ctx context.Context, volume, path string, glob bool) (stat []StatInfo, err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricStatInfoFile, volume, path)
	if err != nil {
		return nil, err
	}
	defer done(&err)

	return p.storage.StatInfoFile(ctx, volume, path, glob)
}

// ReadMultiple will read multiple files and send each files as response.
// Files are read and returned in the given order.
// The resp channel is closed before the call returns.
// Only a canceled context will return an error.
func (p *xlStorageDiskIDCheck) ReadMultiple(ctx context.Context, req ReadMultipleReq, resp chan<- ReadMultipleResp) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricReadMultiple, req.Bucket, req.Prefix)
	if err != nil {
		close(resp)
		return err
	}
	defer done(&err)

	return p.storage.ReadMultiple(ctx, req, resp)
}

// CleanAbandonedData will read metadata of the object on disk
// and delete any data directories and inline data that isn't referenced in metadata.
func (p *xlStorageDiskIDCheck) CleanAbandonedData(ctx context.Context, volume string, path string) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricDeleteAbandonedParts, volume, path)
	if err != nil {
		return err
	}
	defer done(&err)

	w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
	return w.Run(func() error { return p.storage.CleanAbandonedData(ctx, volume, path) })
}

func storageTrace(s storageMetric, startTime time.Time, duration time.Duration, path string, err string) madmin.TraceInfo {
	return madmin.TraceInfo{
		TraceType: madmin.TraceStorage,
		Time:      startTime,
		NodeName:  globalLocalNodeName,
		FuncName:  "storage." + s.String(),
		Duration:  duration,
		Path:      path,
		Error:     err,
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
func (p *xlStorageDiskIDCheck) updateStorageMetrics(s storageMetric, paths ...string) func(err *error) {
	startTime := time.Now()
	trace := globalTrace.NumSubscribers(madmin.TraceStorage) > 0
	return func(errp *error) {
		duration := time.Since(startTime)

		var err error
		if errp != nil && *errp != nil {
			err = *errp
		}

		atomic.AddUint64(&p.apiCalls[s], 1)
		if IsErr(err, []error{
			errVolumeAccessDenied,
			errFileAccessDenied,
			errDiskAccessDenied,
			errFaultyDisk,
			errFaultyRemoteDisk,
			context.DeadlineExceeded,
			context.Canceled,
		}...) {
			atomic.AddUint64(&p.totalErrsAvailability, 1)
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				atomic.AddUint64(&p.totalErrsTimeout, 1)
			}
		}
		p.apiLatencies[s].add(duration)

		if trace {
			paths = append([]string{p.String()}, paths...)
			var errStr string
			if err != nil {
				errStr = err.Error()
			}
			globalTrace.Publish(storageTrace(s, startTime, duration, strings.Join(paths, " "), errStr))
		}
	}
}

const (
	diskHealthOK = iota
	diskHealthFaulty
)

// diskActiveMonitoring indicates if we have enabled "active" disk monitoring
var diskActiveMonitoring = true

func init() {
	diskActiveMonitoring = (env.Get("_MINIO_DRIVE_ACTIVE_MONITORING", config.EnableOn) == config.EnableOn) ||
		(env.Get("_MINIO_DISK_ACTIVE_MONITORING", config.EnableOn) == config.EnableOn)
}

type diskHealthTracker struct {
	// atomic time of last success
	lastSuccess int64

	// atomic time of last time a token was grabbed.
	lastStarted int64

	// Atomic status of disk.
	status int32

	// Atomic number of requests blocking for a token.
	blocked int32

	// Concurrency tokens.
	tokens chan struct{}
}

// newDiskHealthTracker creates a new disk health tracker.
func newDiskHealthTracker(diskMaxConcurrent int) *diskHealthTracker {
	d := diskHealthTracker{
		lastSuccess: time.Now().UnixNano(),
		lastStarted: time.Now().UnixNano(),
		status:      diskHealthOK,
		tokens:      make(chan struct{}, diskMaxConcurrent),
	}
	for i := 0; i < diskMaxConcurrent; i++ {
		d.tokens <- struct{}{}
	}
	return &d
}

// logSuccess will update the last successful operation time.
func (d *diskHealthTracker) logSuccess() {
	atomic.StoreInt64(&d.lastSuccess, time.Now().UnixNano())
}

func (d *diskHealthTracker) isFaulty() bool {
	return atomic.LoadInt32(&d.status) == diskHealthFaulty
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
var noopDoneFunc = func(_ *error) {}

// TrackDiskHealth for this request.
// When a non-nil error is returned 'done' MUST be called
// with the status of the response, if it corresponds to disk health.
// If the pointer sent to done is non-nil AND the error
// is either nil or io.EOF the disk is considered good.
// So if unsure if the disk status is ok, return nil as a parameter to done.
// Shadowing will work as long as return error is named: https://go.dev/play/p/sauq86SsTN2
func (p *xlStorageDiskIDCheck) TrackDiskHealth(ctx context.Context, s storageMetric, paths ...string) (c context.Context, done func(*error), err error) {
	done = noopDoneFunc
	if contextCanceled(ctx) {
		return ctx, done, ctx.Err()
	}

	// Return early if disk is faulty already.
	if err := p.checkHealth(ctx); err != nil {
		return ctx, done, err
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

	select {
	case <-ctx.Done():
		return ctx, done, ctx.Err()
	case <-p.health.tokens:
		// Fast path, got token.
	default:
		// We ran out of tokens, check health before blocking.
		err = p.waitForToken(ctx)
		if err != nil {
			return ctx, done, err
		}
	}
	// We only progress here if we got a token.

	atomic.StoreInt64(&p.health.lastStarted, time.Now().UnixNano())
	ctx = context.WithValue(ctx, healthDiskCtxKey{}, &healthDiskCtxValue{lastSuccess: &p.health.lastSuccess})
	si := p.updateStorageMetrics(s, paths...)
	var once sync.Once
	return ctx, func(errp *error) {
		once.Do(func() {
			p.health.tokens <- struct{}{}
			if errp != nil {
				err := *errp
				if err == nil || errors.Is(err, io.EOF) {
					p.health.logSuccess()
				}
			}
			si(errp)
		})
	}, nil
}

// waitForToken will wait for a token, while periodically
// checking the disk status.
// If nil is returned a token was picked up.
func (p *xlStorageDiskIDCheck) waitForToken(ctx context.Context) (err error) {
	atomic.AddInt32(&p.health.blocked, 1)
	defer func() {
		atomic.AddInt32(&p.health.blocked, -1)
	}()
	// Avoid stampeding herd...
	ticker := time.NewTicker(5*time.Second + time.Duration(rand.Int63n(int64(5*time.Second))))
	defer ticker.Stop()
	for {
		err = p.checkHealth(ctx)
		if err != nil {
			return err
		}
		select {
		case <-ticker.C:
		// Ticker expired, check health again.
		case <-ctx.Done():
			return ctx.Err()
		case <-p.health.tokens:
			return nil
		}
	}
}

// checkHealth should only be called when tokens have run out.
// This will check if disk should be taken offline.
func (p *xlStorageDiskIDCheck) checkHealth(ctx context.Context) (err error) {
	if atomic.LoadInt32(&p.health.status) == diskHealthFaulty {
		return errFaultyDisk
	}
	// Check if there are tokens.
	if p.diskMaxConcurrent-len(p.health.tokens) < p.diskStartChecking {
		return nil
	}

	const maxTimeSinceLastSuccess = 30 * time.Second
	const minTimeSinceLastOpStarted = 15 * time.Second

	// To avoid stampeding herd (100s of simultaneous starting requests)
	// there must be a delay between the last started request and now
	// for the last lastSuccess to be useful.
	t := time.Since(time.Unix(0, atomic.LoadInt64(&p.health.lastStarted)))
	if t < minTimeSinceLastOpStarted {
		return nil
	}

	// If also more than 15 seconds since last success, take disk offline.
	t = time.Since(time.Unix(0, atomic.LoadInt64(&p.health.lastSuccess)))
	if t > maxTimeSinceLastSuccess {
		if atomic.CompareAndSwapInt32(&p.health.status, diskHealthOK, diskHealthFaulty) {
			logger.LogAlwaysIf(ctx, fmt.Errorf("node(%s): taking drive %s offline, time since last response %v", globalLocalNodeName, p.storage.String(), t.Round(time.Millisecond)))
			go p.monitorDiskStatus(t)
		}
		return errFaultyDisk
	}
	return nil
}

// monitorDiskStatus should be called once when a drive has been marked offline.
// Once the disk has been deemed ok, it will return to online status.
func (p *xlStorageDiskIDCheck) monitorDiskStatus(spent time.Duration) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()

	fn := mustGetUUID()
	for range t.C {
		if len(p.health.tokens) == 0 {
			// Queue is still full, no need to check.
			continue
		}
		err := p.storage.WriteAll(context.Background(), minioMetaTmpBucket, fn, []byte{10000: 42})
		if err != nil {
			continue
		}
		b, err := p.storage.ReadAll(context.Background(), minioMetaTmpBucket, fn)
		if err != nil || len(b) != 10001 {
			continue
		}
		err = p.storage.Delete(context.Background(), minioMetaTmpBucket, fn, DeleteOptions{
			Recursive: false,
			Immediate: false,
		})
		if err == nil {
			t := time.Unix(0, atomic.LoadInt64(&p.health.lastSuccess))
			if spent > 0 {
				t = t.Add(spent)
			}
			logger.Info("node(%s): Read/Write/Delete successful, bringing drive %s online. Drive was offline for %s.", globalLocalNodeName, p.storage.String(), time.Since(t))
			atomic.StoreInt32(&p.health.status, diskHealthOK)
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

	// Be just above directio size.
	toWrite := []byte{xioutil.DirectioAlignSize + 1: 42}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	monitor := func() bool {
		if contextCanceled(ctx) {
			return false
		}

		if atomic.LoadInt32(&p.health.status) != diskHealthOK {
			return true
		}

		if time.Since(time.Unix(0, atomic.LoadInt64(&p.health.lastSuccess))) < skipIfSuccessBefore {
			// We recently saw a success - no need to check.
			return true
		}

		goOffline := func(err error, spent time.Duration) {
			if atomic.CompareAndSwapInt32(&p.health.status, diskHealthOK, diskHealthFaulty) {
				logger.LogAlwaysIf(ctx, fmt.Errorf("node(%s): taking drive %s offline: %v", globalLocalNodeName, p.storage.String(), err))
				go p.monitorDiskStatus(spent)
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
