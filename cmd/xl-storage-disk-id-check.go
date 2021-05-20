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
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VividCortex/ewma"
	"github.com/minio/madmin-go"
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
	storageMetricCheckFile
	storageMetricDelete
	storageMetricDeleteVersions
	storageMetricVerifyFile
	storageMetricWriteAll
	storageMetricDeleteVersion
	storageMetricWriteMetadata
	storageMetricUpdateMetadata
	storageMetricReadVersion
	storageMetricReadAll

	// .... add more

	storageMetricLast
)

// Detects change in underlying disk.
type xlStorageDiskIDCheck struct {
	// fields position optimized for memory please
	// do not re-order them, if you add new fields
	// please use `fieldalignment ./...` to check
	// if your changes are not causing any problems.
	storage      StorageAPI
	apiLatencies [storageMetricLast]ewma.MovingAverage
	diskID       string
	apiCalls     [storageMetricLast]uint64
}

func (p *xlStorageDiskIDCheck) getMetrics() DiskMetrics {
	diskMetric := DiskMetrics{
		APILatencies: make(map[string]string),
		APICalls:     make(map[string]uint64),
	}
	for i, v := range p.apiLatencies {
		diskMetric.APILatencies[storageMetric(i).String()] = time.Duration(v.Value()).String()
	}
	for i := range p.apiCalls {
		diskMetric.APICalls[storageMetric(i).String()] = atomic.LoadUint64(&p.apiCalls[i])
	}
	return diskMetric
}

type lockedSimpleEWMA struct {
	sync.RWMutex
	*ewma.SimpleEWMA
}

func (e *lockedSimpleEWMA) Add(value float64) {
	e.Lock()
	defer e.Unlock()
	e.SimpleEWMA.Add(value)
}

func (e *lockedSimpleEWMA) Set(value float64) {
	e.Lock()
	defer e.Unlock()

	e.SimpleEWMA.Set(value)
}

func (e *lockedSimpleEWMA) Value() float64 {
	e.RLock()
	defer e.RUnlock()
	return e.SimpleEWMA.Value()
}

func newXLStorageDiskIDCheck(storage *xlStorage) *xlStorageDiskIDCheck {
	xl := xlStorageDiskIDCheck{
		storage: storage,
	}
	for i := range xl.apiLatencies[:] {
		xl.apiLatencies[i] = &lockedSimpleEWMA{
			SimpleEWMA: new(ewma.SimpleEWMA),
		}
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

func (p *xlStorageDiskIDCheck) NSScanner(ctx context.Context, cache dataUsageCache, updates chan<- dataUsageEntry) (dataUsageCache, error) {
	select {
	case <-ctx.Done():
		return dataUsageCache{}, ctx.Err()
	default:
	}

	if err := p.checkDiskStale(); err != nil {
		return dataUsageCache{}, err
	}
	return p.storage.NSScanner(ctx, cache, updates)
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
	select {
	case <-ctx.Done():
		return DiskInfo{}, ctx.Err()
	default:
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
	return info, nil
}

func (p *xlStorageDiskIDCheck) MakeVolBulk(ctx context.Context, volumes ...string) (err error) {
	defer p.updateStorageMetrics(storageMetricMakeVolBulk, volumes...)()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if err = p.checkDiskStale(); err != nil {
		return err
	}
	return p.storage.MakeVolBulk(ctx, volumes...)
}

func (p *xlStorageDiskIDCheck) MakeVol(ctx context.Context, volume string) (err error) {
	defer p.updateStorageMetrics(storageMetricMakeVol, volume)()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if err = p.checkDiskStale(); err != nil {
		return err
	}
	return p.storage.MakeVol(ctx, volume)
}

func (p *xlStorageDiskIDCheck) ListVols(ctx context.Context) ([]VolInfo, error) {
	defer p.updateStorageMetrics(storageMetricListVols, "/")()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if err := p.checkDiskStale(); err != nil {
		return nil, err
	}
	return p.storage.ListVols(ctx)
}

func (p *xlStorageDiskIDCheck) StatVol(ctx context.Context, volume string) (vol VolInfo, err error) {
	defer p.updateStorageMetrics(storageMetricStatVol, volume)()

	select {
	case <-ctx.Done():
		return VolInfo{}, ctx.Err()
	default:
	}

	if err = p.checkDiskStale(); err != nil {
		return vol, err
	}
	return p.storage.StatVol(ctx, volume)
}

func (p *xlStorageDiskIDCheck) DeleteVol(ctx context.Context, volume string, forceDelete bool) (err error) {
	defer p.updateStorageMetrics(storageMetricDeleteVol, volume)()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if err = p.checkDiskStale(); err != nil {
		return err
	}
	return p.storage.DeleteVol(ctx, volume, forceDelete)
}

func (p *xlStorageDiskIDCheck) ListDir(ctx context.Context, volume, dirPath string, count int) ([]string, error) {
	defer p.updateStorageMetrics(storageMetricListDir, volume, dirPath)()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if err := p.checkDiskStale(); err != nil {
		return nil, err
	}

	return p.storage.ListDir(ctx, volume, dirPath, count)
}

func (p *xlStorageDiskIDCheck) ReadFile(ctx context.Context, volume string, path string, offset int64, buf []byte, verifier *BitrotVerifier) (n int64, err error) {
	defer p.updateStorageMetrics(storageMetricReadFile, volume, path)()

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	if err := p.checkDiskStale(); err != nil {
		return 0, err
	}

	return p.storage.ReadFile(ctx, volume, path, offset, buf, verifier)
}

func (p *xlStorageDiskIDCheck) AppendFile(ctx context.Context, volume string, path string, buf []byte) (err error) {
	defer p.updateStorageMetrics(storageMetricAppendFile, volume, path)()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if err = p.checkDiskStale(); err != nil {
		return err
	}

	return p.storage.AppendFile(ctx, volume, path, buf)
}

func (p *xlStorageDiskIDCheck) CreateFile(ctx context.Context, volume, path string, size int64, reader io.Reader) error {
	defer p.updateStorageMetrics(storageMetricCreateFile, volume, path)()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if err := p.checkDiskStale(); err != nil {
		return err
	}

	return p.storage.CreateFile(ctx, volume, path, size, reader)
}

func (p *xlStorageDiskIDCheck) ReadFileStream(ctx context.Context, volume, path string, offset, length int64) (io.ReadCloser, error) {
	defer p.updateStorageMetrics(storageMetricReadFileStream, volume, path)()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if err := p.checkDiskStale(); err != nil {
		return nil, err
	}

	return p.storage.ReadFileStream(ctx, volume, path, offset, length)
}

func (p *xlStorageDiskIDCheck) RenameFile(ctx context.Context, srcVolume, srcPath, dstVolume, dstPath string) error {
	defer p.updateStorageMetrics(storageMetricRenameFile, srcVolume, srcPath, dstVolume, dstPath)()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if err := p.checkDiskStale(); err != nil {
		return err
	}

	return p.storage.RenameFile(ctx, srcVolume, srcPath, dstVolume, dstPath)
}

func (p *xlStorageDiskIDCheck) RenameData(ctx context.Context, srcVolume, srcPath string, fi FileInfo, dstVolume, dstPath string) error {
	defer p.updateStorageMetrics(storageMetricRenameData, srcPath, fi.DataDir, dstVolume, dstPath)()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if err := p.checkDiskStale(); err != nil {
		return err
	}

	return p.storage.RenameData(ctx, srcVolume, srcPath, fi, dstVolume, dstPath)
}

func (p *xlStorageDiskIDCheck) CheckParts(ctx context.Context, volume string, path string, fi FileInfo) (err error) {
	defer p.updateStorageMetrics(storageMetricCheckParts, volume, path)()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if err = p.checkDiskStale(); err != nil {
		return err
	}

	return p.storage.CheckParts(ctx, volume, path, fi)
}

func (p *xlStorageDiskIDCheck) CheckFile(ctx context.Context, volume string, path string) (err error) {
	defer p.updateStorageMetrics(storageMetricCheckFile, volume, path)()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if err = p.checkDiskStale(); err != nil {
		return err
	}

	return p.storage.CheckFile(ctx, volume, path)
}

func (p *xlStorageDiskIDCheck) Delete(ctx context.Context, volume string, path string, recursive bool) (err error) {
	defer p.updateStorageMetrics(storageMetricDelete, volume, path)()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if err = p.checkDiskStale(); err != nil {
		return err
	}

	return p.storage.Delete(ctx, volume, path, recursive)
}

// DeleteVersions deletes slice of versions, it can be same object
// or multiple objects.
func (p *xlStorageDiskIDCheck) DeleteVersions(ctx context.Context, volume string, versions []FileInfo) (errs []error) {
	// Mererly for tracing storage
	path := ""
	if len(versions) > 0 {
		path = versions[0].Name
	}

	defer p.updateStorageMetrics(storageMetricDeleteVersions, volume, path)()

	errs = make([]error, len(versions))

	select {
	case <-ctx.Done():
		for i := range errs {
			errs[i] = ctx.Err()
		}
		return errs
	default:
	}

	if err := p.checkDiskStale(); err != nil {
		for i := range errs {
			errs[i] = err
		}
		return errs
	}

	return p.storage.DeleteVersions(ctx, volume, versions)
}

func (p *xlStorageDiskIDCheck) VerifyFile(ctx context.Context, volume, path string, fi FileInfo) error {
	defer p.updateStorageMetrics(storageMetricVerifyFile, volume, path)()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if err := p.checkDiskStale(); err != nil {
		return err
	}

	return p.storage.VerifyFile(ctx, volume, path, fi)
}

func (p *xlStorageDiskIDCheck) WriteAll(ctx context.Context, volume string, path string, b []byte) (err error) {
	defer p.updateStorageMetrics(storageMetricWriteAll, volume, path)()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if err = p.checkDiskStale(); err != nil {
		return err
	}

	return p.storage.WriteAll(ctx, volume, path, b)
}

func (p *xlStorageDiskIDCheck) DeleteVersion(ctx context.Context, volume, path string, fi FileInfo, forceDelMarker bool) (err error) {
	defer p.updateStorageMetrics(storageMetricDeleteVersion, volume, path)()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if err = p.checkDiskStale(); err != nil {
		return err
	}

	return p.storage.DeleteVersion(ctx, volume, path, fi, forceDelMarker)
}

func (p *xlStorageDiskIDCheck) UpdateMetadata(ctx context.Context, volume, path string, fi FileInfo) (err error) {
	defer p.updateStorageMetrics(storageMetricUpdateMetadata, volume, path)()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if err = p.checkDiskStale(); err != nil {
		return err
	}

	return p.storage.UpdateMetadata(ctx, volume, path, fi)
}

func (p *xlStorageDiskIDCheck) WriteMetadata(ctx context.Context, volume, path string, fi FileInfo) (err error) {
	defer p.updateStorageMetrics(storageMetricWriteMetadata, volume, path)()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if err = p.checkDiskStale(); err != nil {
		return err
	}

	return p.storage.WriteMetadata(ctx, volume, path, fi)
}

func (p *xlStorageDiskIDCheck) ReadVersion(ctx context.Context, volume, path, versionID string, readData bool) (fi FileInfo, err error) {
	defer p.updateStorageMetrics(storageMetricReadVersion, volume, path)()

	select {
	case <-ctx.Done():
		return fi, ctx.Err()
	default:
	}

	if err = p.checkDiskStale(); err != nil {
		return fi, err
	}

	return p.storage.ReadVersion(ctx, volume, path, versionID, readData)
}

func (p *xlStorageDiskIDCheck) ReadAll(ctx context.Context, volume string, path string) (buf []byte, err error) {
	defer p.updateStorageMetrics(storageMetricReadAll, volume, path)()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if err = p.checkDiskStale(); err != nil {
		return nil, err
	}

	return p.storage.ReadAll(ctx, volume, path)
}

func storageTrace(s storageMetric, startTime time.Time, duration time.Duration, path string) madmin.TraceInfo {
	return madmin.TraceInfo{
		TraceType: madmin.TraceStorage,
		Time:      startTime,
		NodeName:  globalLocalNodeName,
		FuncName:  "storage." + s.String(),
		StorageStats: madmin.TraceStorageStats{
			Duration: duration,
			Path:     path,
		},
	}
}

// Update storage metrics
func (p *xlStorageDiskIDCheck) updateStorageMetrics(s storageMetric, paths ...string) func() {
	startTime := time.Now()
	trace := globalTrace.NumSubscribers() > 0
	return func() {
		duration := time.Since(startTime)

		atomic.AddUint64(&p.apiCalls[s], 1)
		p.apiLatencies[s].Add(float64(duration))

		if trace {
			globalTrace.Publish(storageTrace(s, startTime, duration, strings.Join(paths, " ")))
		}
	}
}
