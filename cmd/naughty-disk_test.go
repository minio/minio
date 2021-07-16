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
	"sync"
	"time"
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

func (d *naughtyDisk) SetDiskLoc(poolIdx, setIdx, diskIdx int) {}

func (d *naughtyDisk) GetDiskID() (string, error) {
	return d.disk.GetDiskID()
}

func (d *naughtyDisk) SetDiskID(id string) {
	d.disk.SetDiskID(id)
}

func (d *naughtyDisk) NSScanner(ctx context.Context, cache dataUsageCache, updates chan<- dataUsageEntry) (info dataUsageCache, err error) {
	return d.disk.NSScanner(ctx, cache, updates)
}

func (d *naughtyDisk) DiskInfo(ctx context.Context) (info DiskInfo, err error) {
	if err := d.calcError(); err != nil {
		return info, err
	}
	return d.disk.DiskInfo(ctx)
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

func (d *naughtyDisk) ListDir(ctx context.Context, volume, dirPath string, count int) (entries []string, err error) {
	if err := d.calcError(); err != nil {
		return []string{}, err
	}
	return d.disk.ListDir(ctx, volume, dirPath, count)
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

func (d *naughtyDisk) CreateFile(ctx context.Context, volume, path string, size int64, reader io.Reader) error {
	if err := d.calcError(); err != nil {
		return err
	}
	return d.disk.CreateFile(ctx, volume, path, size, reader)
}

func (d *naughtyDisk) AppendFile(ctx context.Context, volume string, path string, buf []byte) error {
	if err := d.calcError(); err != nil {
		return err
	}
	return d.disk.AppendFile(ctx, volume, path, buf)
}

func (d *naughtyDisk) RenameData(ctx context.Context, srcVolume, srcPath string, fi FileInfo, dstVolume, dstPath string) error {
	if err := d.calcError(); err != nil {
		return err
	}
	return d.disk.RenameData(ctx, srcVolume, srcPath, fi, dstVolume, dstPath)
}

func (d *naughtyDisk) RenameFile(ctx context.Context, srcVolume, srcPath, dstVolume, dstPath string) error {
	if err := d.calcError(); err != nil {
		return err
	}
	return d.disk.RenameFile(ctx, srcVolume, srcPath, dstVolume, dstPath)
}

func (d *naughtyDisk) CheckParts(ctx context.Context, volume string, path string, fi FileInfo) (err error) {
	if err := d.calcError(); err != nil {
		return err
	}
	return d.disk.CheckParts(ctx, volume, path, fi)
}

func (d *naughtyDisk) CheckFile(ctx context.Context, volume string, path string) (err error) {
	if err := d.calcError(); err != nil {
		return err
	}
	return d.disk.CheckFile(ctx, volume, path)
}

func (d *naughtyDisk) Delete(ctx context.Context, volume string, path string, recursive bool) (err error) {
	if err := d.calcError(); err != nil {
		return err
	}
	return d.disk.Delete(ctx, volume, path, recursive)
}

func (d *naughtyDisk) DeleteVersions(ctx context.Context, volume string, versions []FileInfo) []error {
	if err := d.calcError(); err != nil {
		errs := make([]error, len(versions))
		for i := range errs {
			errs[i] = err
		}
		return errs
	}
	return d.disk.DeleteVersions(ctx, volume, versions)
}

func (d *naughtyDisk) WriteMetadata(ctx context.Context, volume, path string, fi FileInfo) (err error) {
	if err := d.calcError(); err != nil {
		return err
	}
	return d.disk.WriteMetadata(ctx, volume, path, fi)
}

func (d *naughtyDisk) UpdateMetadata(ctx context.Context, volume, path string, fi FileInfo) (err error) {
	if err := d.calcError(); err != nil {
		return err
	}
	return d.disk.UpdateMetadata(ctx, volume, path, fi)
}

func (d *naughtyDisk) DeleteVersion(ctx context.Context, volume, path string, fi FileInfo, forceDelMarker bool) (err error) {
	if err := d.calcError(); err != nil {
		return err
	}
	return d.disk.DeleteVersion(ctx, volume, path, fi, forceDelMarker)
}

func (d *naughtyDisk) ReadVersion(ctx context.Context, volume, path, versionID string, readData bool) (fi FileInfo, err error) {
	if err := d.calcError(); err != nil {
		return FileInfo{}, err
	}
	return d.disk.ReadVersion(ctx, volume, path, versionID, readData)
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

func (d *naughtyDisk) VerifyFile(ctx context.Context, volume, path string, fi FileInfo) error {
	if err := d.calcError(); err != nil {
		return err
	}
	return d.disk.VerifyFile(ctx, volume, path, fi)
}

func (d *naughtyDisk) StatInfoFile(ctx context.Context, volume, path string) (stat StatInfo, err error) {
	if err := d.calcError(); err != nil {
		return stat, err
	}
	return d.disk.StatInfoFile(ctx, volume, path)
}
