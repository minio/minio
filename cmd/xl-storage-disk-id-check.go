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
)

// Detects change in underlying disk.
type xlStorageDiskIDCheck struct {
	storage *xlStorage
	diskID  string
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

func (p *xlStorageDiskIDCheck) IsLocal() bool {
	return p.storage.IsLocal()
}

func (p *xlStorageDiskIDCheck) Endpoint() Endpoint {
	return p.storage.Endpoint()
}

func (p *xlStorageDiskIDCheck) Hostname() string {
	return p.storage.Hostname()
}

func (p *xlStorageDiskIDCheck) Healing() bool {
	return p.storage.Healing()
}

func (p *xlStorageDiskIDCheck) CrawlAndGetDataUsage(ctx context.Context, cache dataUsageCache) (dataUsageCache, error) {
	if err := p.checkDiskStale(); err != nil {
		return dataUsageCache{}, err
	}
	return p.storage.CrawlAndGetDataUsage(ctx, cache)
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
	if err = p.checkDiskStale(); err != nil {
		return err
	}
	return p.storage.MakeVolBulk(ctx, volumes...)
}

func (p *xlStorageDiskIDCheck) MakeVol(ctx context.Context, volume string) (err error) {
	if err = p.checkDiskStale(); err != nil {
		return err
	}
	return p.storage.MakeVol(ctx, volume)
}

func (p *xlStorageDiskIDCheck) ListVols(ctx context.Context) ([]VolInfo, error) {
	if err := p.checkDiskStale(); err != nil {
		return nil, err
	}
	return p.storage.ListVols(ctx)
}

func (p *xlStorageDiskIDCheck) StatVol(ctx context.Context, volume string) (vol VolInfo, err error) {
	if err = p.checkDiskStale(); err != nil {
		return vol, err
	}
	return p.storage.StatVol(ctx, volume)
}

func (p *xlStorageDiskIDCheck) DeleteVol(ctx context.Context, volume string, forceDelete bool) (err error) {
	if err = p.checkDiskStale(); err != nil {
		return err
	}
	return p.storage.DeleteVol(ctx, volume, forceDelete)
}

func (p *xlStorageDiskIDCheck) WalkVersions(ctx context.Context, volume, dirPath, marker string, recursive bool, endWalkCh <-chan struct{}) (chan FileInfoVersions, error) {
	if err := p.checkDiskStale(); err != nil {
		return nil, err
	}
	return p.storage.WalkVersions(ctx, volume, dirPath, marker, recursive, endWalkCh)
}

func (p *xlStorageDiskIDCheck) Walk(ctx context.Context, volume, dirPath, marker string, recursive bool, endWalkCh <-chan struct{}) (chan FileInfo, error) {
	if err := p.checkDiskStale(); err != nil {
		return nil, err
	}

	return p.storage.Walk(ctx, volume, dirPath, marker, recursive, endWalkCh)
}

func (p *xlStorageDiskIDCheck) WalkSplunk(ctx context.Context, volume, dirPath, marker string, endWalkCh <-chan struct{}) (chan FileInfo, error) {
	if err := p.checkDiskStale(); err != nil {
		return nil, err
	}

	return p.storage.WalkSplunk(ctx, volume, dirPath, marker, endWalkCh)
}

func (p *xlStorageDiskIDCheck) ListDir(ctx context.Context, volume, dirPath string, count int) ([]string, error) {
	if err := p.checkDiskStale(); err != nil {
		return nil, err
	}

	return p.storage.ListDir(ctx, volume, dirPath, count)
}

func (p *xlStorageDiskIDCheck) ReadFile(ctx context.Context, volume string, path string, offset int64, buf []byte, verifier *BitrotVerifier) (n int64, err error) {
	if err := p.checkDiskStale(); err != nil {
		return 0, err
	}

	return p.storage.ReadFile(ctx, volume, path, offset, buf, verifier)
}

func (p *xlStorageDiskIDCheck) AppendFile(ctx context.Context, volume string, path string, buf []byte) (err error) {
	if err = p.checkDiskStale(); err != nil {
		return err
	}

	return p.storage.AppendFile(ctx, volume, path, buf)
}

func (p *xlStorageDiskIDCheck) CreateFile(ctx context.Context, volume, path string, size int64, reader io.Reader) error {
	if err := p.checkDiskStale(); err != nil {
		return err
	}

	return p.storage.CreateFile(ctx, volume, path, size, reader)
}

func (p *xlStorageDiskIDCheck) ReadFileStream(ctx context.Context, volume, path string, offset, length int64) (io.ReadCloser, error) {
	if err := p.checkDiskStale(); err != nil {
		return nil, err
	}

	return p.storage.ReadFileStream(ctx, volume, path, offset, length)
}

func (p *xlStorageDiskIDCheck) RenameFile(ctx context.Context, srcVolume, srcPath, dstVolume, dstPath string) error {
	if err := p.checkDiskStale(); err != nil {
		return err
	}

	return p.storage.RenameFile(ctx, srcVolume, srcPath, dstVolume, dstPath)
}

func (p *xlStorageDiskIDCheck) RenameData(ctx context.Context, srcVolume, srcPath, dataDir, dstVolume, dstPath string) error {
	if err := p.checkDiskStale(); err != nil {
		return err
	}

	return p.storage.RenameData(ctx, srcVolume, srcPath, dataDir, dstVolume, dstPath)
}

func (p *xlStorageDiskIDCheck) CheckParts(ctx context.Context, volume string, path string, fi FileInfo) (err error) {
	if err = p.checkDiskStale(); err != nil {
		return err
	}

	return p.storage.CheckParts(ctx, volume, path, fi)
}

func (p *xlStorageDiskIDCheck) CheckFile(ctx context.Context, volume string, path string) (err error) {
	if err = p.checkDiskStale(); err != nil {
		return err
	}

	return p.storage.CheckFile(ctx, volume, path)
}

func (p *xlStorageDiskIDCheck) DeleteFile(ctx context.Context, volume string, path string) (err error) {
	if err = p.checkDiskStale(); err != nil {
		return err
	}

	return p.storage.DeleteFile(ctx, volume, path)
}

func (p *xlStorageDiskIDCheck) DeleteVersions(ctx context.Context, volume string, versions []FileInfo) (errs []error) {
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
	if err := p.checkDiskStale(); err != nil {
		return err
	}

	return p.storage.VerifyFile(ctx, volume, path, fi)
}

func (p *xlStorageDiskIDCheck) WriteAll(ctx context.Context, volume string, path string, reader io.Reader) (err error) {
	if err = p.checkDiskStale(); err != nil {
		return err
	}

	return p.storage.WriteAll(ctx, volume, path, reader)
}

func (p *xlStorageDiskIDCheck) DeleteVersion(ctx context.Context, volume, path string, fi FileInfo) (err error) {
	if err = p.checkDiskStale(); err != nil {
		return err
	}

	return p.storage.DeleteVersion(ctx, volume, path, fi)
}

func (p *xlStorageDiskIDCheck) WriteMetadata(ctx context.Context, volume, path string, fi FileInfo) (err error) {
	if err = p.checkDiskStale(); err != nil {
		return err
	}

	return p.storage.WriteMetadata(ctx, volume, path, fi)
}

func (p *xlStorageDiskIDCheck) ReadVersion(ctx context.Context, volume, path, versionID string) (fi FileInfo, err error) {
	if err = p.checkDiskStale(); err != nil {
		return fi, err
	}

	return p.storage.ReadVersion(ctx, volume, path, versionID)
}

func (p *xlStorageDiskIDCheck) ReadAll(ctx context.Context, volume string, path string) (buf []byte, err error) {
	if err = p.checkDiskStale(); err != nil {
		return nil, err
	}

	return p.storage.ReadAll(ctx, volume, path)
}
