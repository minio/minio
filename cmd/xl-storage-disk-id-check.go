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
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/madmin-go"
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

	// .... add more

	storageMetricLast
)

// Detects change in underlying disk.
type xlStorageDiskIDCheck struct {
	// apiCalls should be placed first so alignment is guaranteed for atomic operations.
	apiCalls     [storageMetricLast]uint64
	apiLatencies [storageMetricLast]*lockedLastMinuteLatency
	diskID       string
	storage      *xlStorage
	health       *diskHealthTracker
	metricsCache timedValue
}

func (p *xlStorageDiskIDCheck) getMetrics() DiskMetrics {
	p.metricsCache.Once.Do(func() {
		p.metricsCache.TTL = 100 * time.Millisecond
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
			return diskMetric, nil
		}
	})
	m, _ := p.metricsCache.Get()
	return m.(DiskMetrics)
}

type lockedLastMinuteLatency struct {
	sync.Mutex
	lastMinuteLatency
}

func (e *lockedLastMinuteLatency) add(value time.Duration) {
	e.Lock()
	defer e.Unlock()
	e.lastMinuteLatency.add(value)
}

// addSize will add a duration and size.
func (e *lockedLastMinuteLatency) addSize(value time.Duration, sz int64) {
	e.Lock()
	defer e.Unlock()
	e.lastMinuteLatency.addSize(value, sz)
}

// total returns the total call count and latency for the last minute.
func (e *lockedLastMinuteLatency) total() AccElem {
	e.Lock()
	defer e.Unlock()
	return e.lastMinuteLatency.getTotal()
}

func newXLStorageDiskIDCheck(storage *xlStorage) *xlStorageDiskIDCheck {
	xl := xlStorageDiskIDCheck{
		storage: storage,
		health:  newDiskHealthTracker(),
	}
	for i := range xl.apiLatencies[:] {
		xl.apiLatencies[i] = &lockedLastMinuteLatency{}
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
		return dataUsageCache{}, ctx.Err()
	}

	if err := p.checkDiskStale(); err != nil {
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

func (p *xlStorageDiskIDCheck) DiskInfo(ctx context.Context) (info DiskInfo, err error) {
	if contextCanceled(ctx) {
		return DiskInfo{}, ctx.Err()
	}

	info, err = p.storage.DiskInfo(ctx)
	if err != nil {
		return info, err
	}

	info.Metrics = p.getMetrics()
	// check cached diskID against backend
	// only if its non-empty.
	if p.diskID != "" {
		if p.diskID != info.ID {
			return info, errDiskNotFound
		}
	}

	if p.health.isFaulty() {
		// if disk is already faulty return faulty for 'mc admin info' output and prometheus alerts.
		return info, errFaultyDisk
	}

	return info, nil
}

func (p *xlStorageDiskIDCheck) MakeVolBulk(ctx context.Context, volumes ...string) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricMakeVolBulk, volumes...)
	if err != nil {
		return err
	}
	defer done(&err)

	return p.storage.MakeVolBulk(ctx, volumes...)
}

func (p *xlStorageDiskIDCheck) MakeVol(ctx context.Context, volume string) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricMakeVol, volume)
	if err != nil {
		return err
	}
	defer done(&err)
	if contextCanceled(ctx) {
		return ctx.Err()
	}

	if err = p.checkDiskStale(); err != nil {
		return err
	}
	return p.storage.MakeVol(ctx, volume)
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

	return p.storage.StatVol(ctx, volume)
}

func (p *xlStorageDiskIDCheck) DeleteVol(ctx context.Context, volume string, forceDelete bool) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricDeleteVol, volume)
	if err != nil {
		return err
	}
	defer done(&err)

	return p.storage.DeleteVol(ctx, volume, forceDelete)
}

func (p *xlStorageDiskIDCheck) ListDir(ctx context.Context, volume, dirPath string, count int) (s []string, err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricListDir, volume, dirPath)
	if err != nil {
		return nil, err
	}
	defer done(&err)

	return p.storage.ListDir(ctx, volume, dirPath, count)
}

func (p *xlStorageDiskIDCheck) ReadFile(ctx context.Context, volume string, path string, offset int64, buf []byte, verifier *BitrotVerifier) (n int64, err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricReadFile, volume, path)
	if err != nil {
		return 0, err
	}
	defer done(&err)

	return p.storage.ReadFile(ctx, volume, path, offset, buf, verifier)
}

func (p *xlStorageDiskIDCheck) AppendFile(ctx context.Context, volume string, path string, buf []byte) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricAppendFile, volume, path)
	if err != nil {
		return err
	}
	defer done(&err)

	return p.storage.AppendFile(ctx, volume, path, buf)
}

func (p *xlStorageDiskIDCheck) CreateFile(ctx context.Context, volume, path string, size int64, reader io.Reader) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricCreateFile, volume, path)
	if err != nil {
		return err
	}
	defer done(&err)

	return p.storage.CreateFile(ctx, volume, path, size, reader)
}

func (p *xlStorageDiskIDCheck) ReadFileStream(ctx context.Context, volume, path string, offset, length int64) (io.ReadCloser, error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricReadFileStream, volume, path)
	if err != nil {
		return nil, err
	}
	defer done(&err)

	return p.storage.ReadFileStream(ctx, volume, path, offset, length)
}

func (p *xlStorageDiskIDCheck) RenameFile(ctx context.Context, srcVolume, srcPath, dstVolume, dstPath string) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricRenameFile, srcVolume, srcPath, dstVolume, dstPath)
	if err != nil {
		return err
	}
	defer done(&err)

	return p.storage.RenameFile(ctx, srcVolume, srcPath, dstVolume, dstPath)
}

func (p *xlStorageDiskIDCheck) RenameData(ctx context.Context, srcVolume, srcPath string, fi FileInfo, dstVolume, dstPath string) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricRenameData, srcPath, fi.DataDir, dstVolume, dstPath)
	if err != nil {
		return err
	}
	defer done(&err)

	return p.storage.RenameData(ctx, srcVolume, srcPath, fi, dstVolume, dstPath)
}

func (p *xlStorageDiskIDCheck) CheckParts(ctx context.Context, volume string, path string, fi FileInfo) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricCheckParts, volume, path)
	if err != nil {
		return err
	}
	defer done(&err)

	return p.storage.CheckParts(ctx, volume, path, fi)
}

func (p *xlStorageDiskIDCheck) Delete(ctx context.Context, volume string, path string, deleteOpts DeleteOptions) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricDelete, volume, path)
	if err != nil {
		return err
	}
	defer done(&err)

	return p.storage.Delete(ctx, volume, path, deleteOpts)
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

	return p.storage.WriteAll(ctx, volume, path, b)
}

func (p *xlStorageDiskIDCheck) DeleteVersion(ctx context.Context, volume, path string, fi FileInfo, forceDelMarker bool) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricDeleteVersion, volume, path)
	if err != nil {
		return err
	}
	defer done(&err)

	return p.storage.DeleteVersion(ctx, volume, path, fi, forceDelMarker)
}

func (p *xlStorageDiskIDCheck) UpdateMetadata(ctx context.Context, volume, path string, fi FileInfo) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricUpdateMetadata, volume, path)
	if err != nil {
		return err
	}
	defer done(&err)

	return p.storage.UpdateMetadata(ctx, volume, path, fi)
}

func (p *xlStorageDiskIDCheck) WriteMetadata(ctx context.Context, volume, path string, fi FileInfo) (err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricWriteMetadata, volume, path)
	if err != nil {
		return err
	}
	defer done(&err)

	return p.storage.WriteMetadata(ctx, volume, path, fi)
}

func (p *xlStorageDiskIDCheck) ReadVersion(ctx context.Context, volume, path, versionID string, readData bool) (fi FileInfo, err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricReadVersion, volume, path)
	if err != nil {
		return fi, err
	}
	defer done(&err)

	return p.storage.ReadVersion(ctx, volume, path, versionID, readData)
}

func (p *xlStorageDiskIDCheck) ReadAll(ctx context.Context, volume string, path string) (buf []byte, err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricReadAll, volume, path)
	if err != nil {
		return nil, err
	}
	defer done(&err)

	return p.storage.ReadAll(ctx, volume, path)
}

func (p *xlStorageDiskIDCheck) ReadXL(ctx context.Context, volume string, path string, readData bool) (rf RawFileInfo, err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricReadXL, volume, path)
	if err != nil {
		return RawFileInfo{}, err
	}
	defer done(&err)

	return p.storage.ReadXL(ctx, volume, path, readData)
}

func (p *xlStorageDiskIDCheck) StatInfoFile(ctx context.Context, volume, path string, glob bool) (stat []StatInfo, err error) {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricStatInfoFile, volume, path)
	if err != nil {
		return nil, err
	}
	defer done(&err)

	return p.storage.StatInfoFile(ctx, volume, path, glob)
}

// ReadMultiple will read multiple files and send each back as response.
// Files are read and returned in the given order.
// The resp channel is closed before the call returns.
// Only a canceled context will return an error.
func (p *xlStorageDiskIDCheck) ReadMultiple(ctx context.Context, req ReadMultipleReq, resp chan<- ReadMultipleResp) error {
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricReadMultiple, req.Bucket, req.Prefix)
	if err != nil {
		close(resp)
		return err
	}
	defer done(&err)

	return p.storage.ReadMultiple(ctx, req, resp)
}

func storageTrace(s storageMetric, startTime time.Time, duration time.Duration, path string) madmin.TraceInfo {
	return madmin.TraceInfo{
		TraceType: madmin.TraceStorage,
		Time:      startTime,
		NodeName:  globalLocalNodeName,
		FuncName:  "storage." + s.String(),
		Duration:  duration,
		Path:      path,
	}
}

func scannerTrace(s scannerMetric, startTime time.Time, duration time.Duration, path string) madmin.TraceInfo {
	return madmin.TraceInfo{
		TraceType: madmin.TraceScanner,
		Time:      startTime,
		NodeName:  globalLocalNodeName,
		FuncName:  "scanner." + s.String(),
		Duration:  duration,
		Path:      path,
	}
}

// Update storage metrics
func (p *xlStorageDiskIDCheck) updateStorageMetrics(s storageMetric, paths ...string) func(err *error) {
	startTime := time.Now()
	trace := globalTrace.NumSubscribers(madmin.TraceStorage) > 0
	return func(err *error) {
		duration := time.Since(startTime)

		atomic.AddUint64(&p.apiCalls[s], 1)
		p.apiLatencies[s].add(duration)

		paths = append([]string{p.String()}, paths...)
		if trace {
			globalTrace.Publish(storageTrace(s, startTime, duration, strings.Join(paths, " ")))
		}
	}
}

const (
	diskHealthOK = iota
	diskHealthFaulty
)

// diskMaxConcurrent is the maximum number of running concurrent operations
// for local and (incoming) remote disk ops respectively.
var diskMaxConcurrent = 512

func init() {
	if s, ok := os.LookupEnv("_MINIO_DISK_MAX_CONCURRENT"); ok && s != "" {
		var err error
		diskMaxConcurrent, err = strconv.Atoi(s)
		if err != nil {
			logger.Fatal(err, "invalid _MINIO_DISK_MAX_CONCURRENT value")
		}
	}
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
func newDiskHealthTracker() *diskHealthTracker {
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
	if atomic.LoadInt32(&p.health.status) == diskHealthFaulty {
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
				if err != nil && !errors.Is(err, io.EOF) {
					return
				}
				p.health.logSuccess()
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
	if len(p.health.tokens) > 0 {
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
			logger.LogAlwaysIf(ctx, fmt.Errorf("taking disk %s offline, time since last response %v", p.storage.String(), t.Round(time.Millisecond)))
			go p.monitorDiskStatus()
		}
		return errFaultyDisk
	}
	return nil
}

// monitorDiskStatus should be called once when a drive has been marked offline.
// Once the disk has been deemed ok, it will return to online status.
func (p *xlStorageDiskIDCheck) monitorDiskStatus() {
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
			Force:     false,
		})
		if err == nil {
			logger.Info("Able to read+write, bringing disk %s online.", p.storage.String())
			atomic.StoreInt32(&p.health.status, diskHealthOK)
			return
		}
	}
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
