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
	"io"
	"sync"
	"time"

	"github.com/minio/madmin-go/v3"
)

// naughtyDisk wraps a POSIX disk and returns programmed errors
// specified by the developer. The purpose is to simulate errors
// that are hard to simulate in practice like DiskNotFound.
// Programmed errors are stored in errors field.
type naughtyDisk struct {
	// The real disk
	disk StorageAPI
	// Programmed errors: API call number => error to return
	errors map[int]error
	// The error to return when no error value is programmed
	defaultErr error
	// The current API call number
	callNR int
	// Data protection
	mu sync.Mutex
}

func newNaughtyDisk(d StorageAPI, errs map[int]error, defaultErr error) *naughtyDisk {
	return &naughtyDisk{disk: d, errors: errs, defaultErr: defaultErr}
}

func (d *naughtyDisk) String() string {
	return d.disk.String()
}

func (d *naughtyDisk) IsOnline() bool {
	if err := d.calcError(); err != nil {
		return err == errDiskNotFound
	}
	return d.disk.IsOnline()
}

func (d *naughtyDisk) LastConn() time.Time {
	return d.disk.LastConn()
}

func (d *naughtyDisk) IsLocal() bool {
	return d.disk.IsLocal()
}

func (d *naughtyDisk) Endpoint() Endpoint {
	return d.disk.Endpoint()
}

func (d *naughtyDisk) Hostname() string {
	return d.disk.Hostname()
}

func (d *naughtyDisk) Healing() *healingTracker {
	return d.disk.Healing()
}

func (d *naughtyDisk) Close() (err error) {
	if err = d.calcError(); err != nil {
		return err
	}
	return d.disk.Close()
}

func (d *naughtyDisk) calcError() (err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.callNR++
	if err, ok := d.errors[d.callNR]; ok {
		return err
	}
	if d.defaultErr != nil {
		return d.defaultErr
	}
	return nil
}

func (d *naughtyDisk) GetDiskLoc() (poolIdx, setIdx, diskIdx int) {
	return -1, -1, -1
}

func (d *naughtyDisk) GetDiskID() (string, error) {
	return d.disk.GetDiskID()
}

func (d *naughtyDisk) SetDiskID(id string) {
	d.disk.SetDiskID(id)
}

func (d *naughtyDisk) NSScanner(ctx context.Context, cache dataUsageCache, updates chan<- dataUsageEntry, scanMode madmin.HealScanMode, weSleep func() bool) (info dataUsageCache, err error) {
	if err := d.calcError(); err != nil {
		return info, err
	}
	return d.disk.NSScanner(ctx, cache, updates, scanMode, weSleep)
}

func (d *naughtyDisk) DiskInfo(ctx context.Context, opts DiskInfoOptions) (info DiskInfo, err error) {
	if err := d.calcError(); err != nil {
		return info, err
	}
	return d.disk.DiskInfo(ctx, opts)
}

func (d *naughtyDisk) MakeVolBulk(ctx context.Context, volumes ...string) (err error) {
	if err := d.calcError(); err != nil {
		return err
	}
	return d.disk.MakeVolBulk(ctx, volumes...)
}

func (d *naughtyDisk) MakeVol(ctx context.Context, volume string) (err error) {
	if err := d.calcError(); err != nil {
		return err
	}
	return d.disk.MakeVol(ctx, volume)
}

func (d *naughtyDisk) ListVols(ctx context.Context) (vols []VolInfo, err error) {
	if err := d.calcError(); err != nil {
		return nil, err
	}
	return d.disk.ListVols(ctx)
}

func (d *naughtyDisk) StatVol(ctx context.Context, volume string) (vol VolInfo, err error) {
	if err := d.calcError(); err != nil {
		return VolInfo{}, err
	}
	return d.disk.StatVol(ctx, volume)
}

func (d *naughtyDisk) DeleteVol(ctx context.Context, volume string, forceDelete bool) (err error) {
	if err := d.calcError(); err != nil {
		return err
	}
	return d.disk.DeleteVol(ctx, volume, forceDelete)
}

func (d *naughtyDisk) WalkDir(ctx context.Context, opts WalkDirOptions, wr io.Writer) error {
	if err := d.calcError(); err != nil {
		return err
	}
	return d.disk.WalkDir(ctx, opts, wr)
}

func (d *naughtyDisk) ListDir(ctx context.Context, origvolume, volume, dirPath string, count int) (entries []string, err error) {
	if err := d.calcError(); err != nil {
		return []string{}, err
	}
	return d.disk.ListDir(ctx, origvolume, volume, dirPath, count)
}

func (d *naughtyDisk) ReadFile(ctx context.Context, volume string, path string, offset int64, buf []byte, verifier *BitrotVerifier) (n int64, err error) {
	if err := d.calcError(); err != nil {
		return 0, err
	}
	return d.disk.ReadFile(ctx, volume, path, offset, buf, verifier)
}

func (d *naughtyDisk) ReadFileStream(ctx context.Context, volume, path string, offset, length int64) (io.ReadCloser, error) {
	if err := d.calcError(); err != nil {
		return nil, err
	}
	return d.disk.ReadFileStream(ctx, volume, path, offset, length)
}

func (d *naughtyDisk) CreateFile(ctx context.Context, origvolume, volume, path string, size int64, reader io.Reader) error {
	if err := d.calcError(); err != nil {
		return err
	}
	return d.disk.CreateFile(ctx, origvolume, volume, path, size, reader)
}

func (d *naughtyDisk) AppendFile(ctx context.Context, volume string, path string, buf []byte) error {
	if err := d.calcError(); err != nil {
		return err
	}
	return d.disk.AppendFile(ctx, volume, path, buf)
}

func (d *naughtyDisk) RenameData(ctx context.Context, srcVolume, srcPath string, fi FileInfo, dstVolume, dstPath string, opts RenameOptions) (RenameDataResp, error) {
	if err := d.calcError(); err != nil {
		return RenameDataResp{}, err
	}
	return d.disk.RenameData(ctx, srcVolume, srcPath, fi, dstVolume, dstPath, opts)
}

func (d *naughtyDisk) RenamePart(ctx context.Context, srcVolume, srcPath, dstVolume, dstPath string, meta []byte, skipParent string) error {
	if err := d.calcError(); err != nil {
		return err
	}
	return d.disk.RenamePart(ctx, srcVolume, srcPath, dstVolume, dstPath, meta, skipParent)
}

func (d *naughtyDisk) ReadParts(ctx context.Context, bucket string, partMetaPaths ...string) ([]*ObjectPartInfo, error) {
	if err := d.calcError(); err != nil {
		return nil, err
	}
	return d.disk.ReadParts(ctx, bucket, partMetaPaths...)
}

func (d *naughtyDisk) RenameFile(ctx context.Context, srcVolume, srcPath, dstVolume, dstPath string) error {
	if err := d.calcError(); err != nil {
		return err
	}
	return d.disk.RenameFile(ctx, srcVolume, srcPath, dstVolume, dstPath)
}

func (d *naughtyDisk) CheckParts(ctx context.Context, volume string, path string, fi FileInfo) (*CheckPartsResp, error) {
	if err := d.calcError(); err != nil {
		return nil, err
	}
	return d.disk.CheckParts(ctx, volume, path, fi)
}

func (d *naughtyDisk) DeleteBulk(ctx context.Context, volume string, paths ...string) (err error) {
	if err := d.calcError(); err != nil {
		return err
	}
	return d.disk.DeleteBulk(ctx, volume, paths...)
}

func (d *naughtyDisk) Delete(ctx context.Context, volume string, path string, deleteOpts DeleteOptions) (err error) {
	if err := d.calcError(); err != nil {
		return err
	}
	return d.disk.Delete(ctx, volume, path, deleteOpts)
}

func (d *naughtyDisk) DeleteVersions(ctx context.Context, volume string, versions []FileInfoVersions, opts DeleteOptions) []error {
	if err := d.calcError(); err != nil {
		errs := make([]error, len(versions))
		for i := range errs {
			errs[i] = err
		}
		return errs
	}
	return d.disk.DeleteVersions(ctx, volume, versions, opts)
}

func (d *naughtyDisk) WriteMetadata(ctx context.Context, origvolume, volume, path string, fi FileInfo) (err error) {
	if err := d.calcError(); err != nil {
		return err
	}
	return d.disk.WriteMetadata(ctx, origvolume, volume, path, fi)
}

func (d *naughtyDisk) UpdateMetadata(ctx context.Context, volume, path string, fi FileInfo, opts UpdateMetadataOpts) (err error) {
	if err := d.calcError(); err != nil {
		return err
	}
	return d.disk.UpdateMetadata(ctx, volume, path, fi, opts)
}

func (d *naughtyDisk) DeleteVersion(ctx context.Context, volume, path string, fi FileInfo, forceDelMarker bool, opts DeleteOptions) (err error) {
	if err := d.calcError(); err != nil {
		return err
	}
	return d.disk.DeleteVersion(ctx, volume, path, fi, forceDelMarker, opts)
}

func (d *naughtyDisk) ReadVersion(ctx context.Context, origvolume, volume, path, versionID string, opts ReadOptions) (fi FileInfo, err error) {
	if err := d.calcError(); err != nil {
		return FileInfo{}, err
	}
	return d.disk.ReadVersion(ctx, origvolume, volume, path, versionID, opts)
}

func (d *naughtyDisk) WriteAll(ctx context.Context, volume string, path string, b []byte) (err error) {
	if err := d.calcError(); err != nil {
		return err
	}
	return d.disk.WriteAll(ctx, volume, path, b)
}

func (d *naughtyDisk) ReadAll(ctx context.Context, volume string, path string) (buf []byte, err error) {
	if err := d.calcError(); err != nil {
		return nil, err
	}
	return d.disk.ReadAll(ctx, volume, path)
}

func (d *naughtyDisk) ReadXL(ctx context.Context, volume string, path string, readData bool) (rf RawFileInfo, err error) {
	if err := d.calcError(); err != nil {
		return rf, err
	}
	return d.disk.ReadXL(ctx, volume, path, readData)
}

func (d *naughtyDisk) VerifyFile(ctx context.Context, volume, path string, fi FileInfo) (*CheckPartsResp, error) {
	if err := d.calcError(); err != nil {
		return nil, err
	}
	return d.disk.VerifyFile(ctx, volume, path, fi)
}

func (d *naughtyDisk) StatInfoFile(ctx context.Context, volume, path string, glob bool) (stat []StatInfo, err error) {
	if err := d.calcError(); err != nil {
		return stat, err
	}
	return d.disk.StatInfoFile(ctx, volume, path, glob)
}

func (d *naughtyDisk) ReadMultiple(ctx context.Context, req ReadMultipleReq, resp chan<- ReadMultipleResp) error {
	if err := d.calcError(); err != nil {
		close(resp)
		return err
	}
	return d.disk.ReadMultiple(ctx, req, resp)
}

func (d *naughtyDisk) CleanAbandonedData(ctx context.Context, volume string, path string) error {
	if err := d.calcError(); err != nil {
		return err
	}
	return d.disk.CleanAbandonedData(ctx, volume, path)
}
