/*
 * MinIO Cloud Storage, (C) 2019-2020 MinIO, Inc.
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
	"context"
	"io"
	"time"

	ewma "github.com/VividCortex/ewma"
)

//go:generate stringer -type=storageMetric -trimprefix=storageMetric $GOFILE

type storageMetric uint8

const (
	storageMetricIsOnline storageMetric = iota
	storageMetricHealing
	storageMetricGetDiskID
	storageMetricDiskInfo
	storageMetricMakeVolBulk
	storageMetricMakeVol
	storageMetricListVols
	storageMetricStatVol
	storageMetricDeleteVol
	storageMetricWalkVersions
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
	storageMetricReadVersion
	storageMetricReadAll

	// .... add more

	metricLast
)

// Detects change in underlying disk.
type xlStorageDiskIDCheck struct {
	storage *xlStorage
	diskID  string

	apisCount   [metricLast]uint64
	apisLatency [metricLast]ewma.MovingAverage
}

func newXLStorageDiskIDCheck(storage *xlStorage) *xlStorageDiskIDCheck {
	xl := xlStorageDiskIDCheck{
		storage: storage,
	}
	for i := range xl.apisLatency[:] {
		xl.apisLatency[i] = ewma.NewMovingAverage()
	}
	return &xl
}

func (p *xlStorageDiskIDCheck) String() string {
	return p.storage.String()
}

func (p *xlStorageDiskIDCheck) IsOnline() bool {
	defer p.storageMetrics(storageMetricIsOnline)()
	storedDiskID, err := p.storage.GetDiskID()
	if err != nil {
		return false
	}
	return storedDiskID == p.diskID
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
	defer p.storageMetrics(storageMetricHealing)()
	return p.storage.Healing()
}

func (p *xlStorageDiskIDCheck) NSScanner(ctx context.Context, cache dataUsageCache) (dataUsageCache, error) {
	select {
	case <-ctx.Done():
		return dataUsageCache{}, ctx.Err()
	default:
	}

	if err := p.checkDiskStale(); err != nil {
		return dataUsageCache{}, err
	}
	return p.storage.NSScanner(ctx, cache)
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
	defer p.storageMetrics(storageMetricGetDiskID)()
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

	defer p.storageMetrics(storageMetricDiskInfo)()
	info, err = p.storage.DiskInfo(ctx)
	if err != nil {
		return info, err
	}
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
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	defer p.storageMetrics(storageMetricMakeVolBulk)()
	if err = p.checkDiskStale(); err != nil {
		return err
	}
	return p.storage.MakeVolBulk(ctx, volumes...)
}

func (p *xlStorageDiskIDCheck) MakeVol(ctx context.Context, volume string) (err error) {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	defer p.storageMetrics(storageMetricMakeVol)()
	if err = p.checkDiskStale(); err != nil {
		return err
	}
	return p.storage.MakeVol(ctx, volume)
}

func (p *xlStorageDiskIDCheck) ListVols(ctx context.Context) ([]VolInfo, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	defer p.storageMetrics(storageMetricListVols)()
	if err := p.checkDiskStale(); err != nil {
		return nil, err
	}
	return p.storage.ListVols(ctx)
}

func (p *xlStorageDiskIDCheck) StatVol(ctx context.Context, volume string) (vol VolInfo, err error) {
	select {
	case <-ctx.Done():
		return VolInfo{}, ctx.Err()
	default:
	}

	defer p.storageMetrics(storageMetricStatVol)()
	if err = p.checkDiskStale(); err != nil {
		return vol, err
	}
	return p.storage.StatVol(ctx, volume)
}

func (p *xlStorageDiskIDCheck) DeleteVol(ctx context.Context, volume string, forceDelete bool) (err error) {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	defer p.storageMetrics(storageMetricDeleteVol)()
	if err = p.checkDiskStale(); err != nil {
		return err
	}
	return p.storage.DeleteVol(ctx, volume, forceDelete)
}

func (p *xlStorageDiskIDCheck) ListDir(ctx context.Context, volume, dirPath string, count int) ([]string, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	defer p.storageMetrics(storageMetricListDir)()
	if err := p.checkDiskStale(); err != nil {
		return nil, err
	}

	return p.storage.ListDir(ctx, volume, dirPath, count)
}

func (p *xlStorageDiskIDCheck) ReadFile(ctx context.Context, volume string, path string, offset int64, buf []byte, verifier *BitrotVerifier) (n int64, err error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	defer p.storageMetrics(storageMetricReadFile)()
	if err := p.checkDiskStale(); err != nil {
		return 0, err
	}

	return p.storage.ReadFile(ctx, volume, path, offset, buf, verifier)
}

func (p *xlStorageDiskIDCheck) AppendFile(ctx context.Context, volume string, path string, buf []byte) (err error) {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	defer p.storageMetrics(storageMetricAppendFile)()
	if err = p.checkDiskStale(); err != nil {
		return err
	}

	return p.storage.AppendFile(ctx, volume, path, buf)
}

func (p *xlStorageDiskIDCheck) CreateFile(ctx context.Context, volume, path string, size int64, reader io.Reader) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	defer p.storageMetrics(storageMetricCreateFile)()
	if err := p.checkDiskStale(); err != nil {
		return err
	}

	return p.storage.CreateFile(ctx, volume, path, size, reader)
}

func (p *xlStorageDiskIDCheck) ReadFileStream(ctx context.Context, volume, path string, offset, length int64) (io.ReadCloser, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	defer p.storageMetrics(storageMetricReadFileStream)()
	if err := p.checkDiskStale(); err != nil {
		return nil, err
	}

	return p.storage.ReadFileStream(ctx, volume, path, offset, length)
}

func (p *xlStorageDiskIDCheck) RenameFile(ctx context.Context, srcVolume, srcPath, dstVolume, dstPath string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	defer p.storageMetrics(storageMetricRenameFile)()
	if err := p.checkDiskStale(); err != nil {
		return err
	}

	return p.storage.RenameFile(ctx, srcVolume, srcPath, dstVolume, dstPath)
}

func (p *xlStorageDiskIDCheck) RenameData(ctx context.Context, srcVolume, srcPath, dataDir, dstVolume, dstPath string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	defer p.storageMetrics(storageMetricRenameData)()
	if err := p.checkDiskStale(); err != nil {
		return err
	}

	return p.storage.RenameData(ctx, srcVolume, srcPath, dataDir, dstVolume, dstPath)
}

func (p *xlStorageDiskIDCheck) CheckParts(ctx context.Context, volume string, path string, fi FileInfo) (err error) {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	defer p.storageMetrics(storageMetricCheckParts)()
	if err = p.checkDiskStale(); err != nil {
		return err
	}

	return p.storage.CheckParts(ctx, volume, path, fi)
}

func (p *xlStorageDiskIDCheck) CheckFile(ctx context.Context, volume string, path string) (err error) {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	defer p.storageMetrics(storageMetricCheckFile)()
	if err = p.checkDiskStale(); err != nil {
		return err
	}

	return p.storage.CheckFile(ctx, volume, path)
}

func (p *xlStorageDiskIDCheck) Delete(ctx context.Context, volume string, path string, recursive bool) (err error) {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	defer p.storageMetrics(storageMetricDelete)()
	if err = p.checkDiskStale(); err != nil {
		return err
	}

	return p.storage.Delete(ctx, volume, path, recursive)
}

func (p *xlStorageDiskIDCheck) DeleteVersions(ctx context.Context, volume string, versions []FileInfo) (errs []error) {
	defer p.storageMetrics(storageMetricDeleteVersions)()
	if err := p.checkDiskStale(); err != nil {
		errs = make([]error, len(versions))
		for i := range errs {
			errs[i] = err
		}
		return errs
	}
	return p.storage.DeleteVersions(ctx, volume, versions)
}

func (p *xlStorageDiskIDCheck) VerifyFile(ctx context.Context, volume, path string, fi FileInfo) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	defer p.storageMetrics(storageMetricVerifyFile)()
	if err := p.checkDiskStale(); err != nil {
		return err
	}

	return p.storage.VerifyFile(ctx, volume, path, fi)
}

func (p *xlStorageDiskIDCheck) WriteAll(ctx context.Context, volume string, path string, b []byte) (err error) {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	defer p.storageMetrics(storageMetricWriteAll)()
	if err = p.checkDiskStale(); err != nil {
		return err
	}

	return p.storage.WriteAll(ctx, volume, path, b)
}

func (p *xlStorageDiskIDCheck) DeleteVersion(ctx context.Context, volume, path string, fi FileInfo, forceDelMarker bool) (err error) {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	defer p.storageMetrics(storageMetricDeleteVersion)()
	if err = p.checkDiskStale(); err != nil {
		return err
	}

	return p.storage.DeleteVersion(ctx, volume, path, fi, forceDelMarker)
}

func (p *xlStorageDiskIDCheck) WriteMetadata(ctx context.Context, volume, path string, fi FileInfo) (err error) {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	defer p.storageMetrics(storageMetricWriteMetadata)()
	if err = p.checkDiskStale(); err != nil {
		return err
	}

	return p.storage.WriteMetadata(ctx, volume, path, fi)
}

func (p *xlStorageDiskIDCheck) ReadVersion(ctx context.Context, volume, path, versionID string, readData bool) (fi FileInfo, err error) {
	select {
	case <-ctx.Done():
		return fi, ctx.Err()
	default:
	}

	defer p.storageMetrics(storageMetricReadVersion)()
	if err = p.checkDiskStale(); err != nil {
		return fi, err
	}

	return p.storage.ReadVersion(ctx, volume, path, versionID, readData)
}

func (p *xlStorageDiskIDCheck) ReadAll(ctx context.Context, volume string, path string) (buf []byte, err error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	defer p.storageMetrics(storageMetricReadAll)()
	if err = p.checkDiskStale(); err != nil {
		return nil, err
	}

	return p.storage.ReadAll(ctx, volume, path)
}

// Update storage metrics
func (p *xlStorageDiskIDCheck) storageMetrics(s storageMetric) func() {
	startTime := time.Now()
	return func() {
		p.apisCount[s]++
		p.apisLatency[s].Add(float64(time.Since(startTime)))
	}
}
