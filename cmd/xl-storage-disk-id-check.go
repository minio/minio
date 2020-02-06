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

func (p *xlStorageDiskIDCheck) Hostname() string {
	return p.storage.Hostname()
}

func (p *xlStorageDiskIDCheck) CrawlAndGetDataUsage(ctx context.Context, cache dataUsageCache) (dataUsageCache, error) {
	if p.isDiskStale() {
		return dataUsageCache{}, errDiskNotFound
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

func (p *xlStorageDiskIDCheck) isDiskStale() bool {
	if p.diskID == "" {
		// For empty disk-id we allow the call as the server might be coming up and trying to read format.json
		// or create format.json
		return false
	}
	storedDiskID, err := p.storage.GetDiskID()
	if err == nil && p.diskID == storedDiskID {
		return false
	}
	return true
}

func (p *xlStorageDiskIDCheck) DiskInfo() (info DiskInfo, err error) {
	if p.isDiskStale() {
		return info, errDiskNotFound
	}
	return p.storage.DiskInfo()
}

func (p *xlStorageDiskIDCheck) MakeVolBulk(volumes ...string) (err error) {
	if p.isDiskStale() {
		return errDiskNotFound
	}
	return p.storage.MakeVolBulk(volumes...)
}

func (p *xlStorageDiskIDCheck) MakeVol(volume string) (err error) {
	if p.isDiskStale() {
		return errDiskNotFound
	}
	return p.storage.MakeVol(volume)
}

func (p *xlStorageDiskIDCheck) ListVols() ([]VolInfo, error) {
	if p.isDiskStale() {
		return nil, errDiskNotFound
	}
	return p.storage.ListVols()
}

func (p *xlStorageDiskIDCheck) StatVol(volume string) (vol VolInfo, err error) {
	if p.isDiskStale() {
		return vol, errDiskNotFound
	}
	return p.storage.StatVol(volume)
}

func (p *xlStorageDiskIDCheck) DeleteVol(volume string, forceDelete bool) (err error) {
	if p.isDiskStale() {
		return errDiskNotFound
	}
	return p.storage.DeleteVol(volume, forceDelete)
}

func (p *xlStorageDiskIDCheck) WalkVersions(volume, dirPath string, marker string, recursive bool, endWalkCh <-chan struct{}) (chan FileInfoVersions, error) {
	if p.isDiskStale() {
		return nil, errDiskNotFound
	}
	return p.storage.WalkVersions(volume, dirPath, marker, recursive, endWalkCh)
}

func (p *xlStorageDiskIDCheck) Walk(volume, dirPath string, marker string, recursive bool, endWalkCh <-chan struct{}) (chan FileInfo, error) {
	if p.isDiskStale() {
		return nil, errDiskNotFound
	}
	return p.storage.Walk(volume, dirPath, marker, recursive, endWalkCh)
}

func (p *xlStorageDiskIDCheck) WalkSplunk(volume, dirPath string, marker string, endWalkCh <-chan struct{}) (chan FileInfo, error) {
	if p.isDiskStale() {
		return nil, errDiskNotFound
	}
	return p.storage.WalkSplunk(volume, dirPath, marker, endWalkCh)
}

func (p *xlStorageDiskIDCheck) ListDir(volume, dirPath string, count int) ([]string, error) {
	if p.isDiskStale() {
		return nil, errDiskNotFound
	}
	return p.storage.ListDir(volume, dirPath, count)
}

func (p *xlStorageDiskIDCheck) ReadFile(volume string, path string, offset int64, buf []byte, verifier *BitrotVerifier) (n int64, err error) {
	if p.isDiskStale() {
		return 0, errDiskNotFound
	}
	return p.storage.ReadFile(volume, path, offset, buf, verifier)
}

func (p *xlStorageDiskIDCheck) AppendFile(volume string, path string, buf []byte) (err error) {
	if p.isDiskStale() {
		return errDiskNotFound
	}
	return p.storage.AppendFile(volume, path, buf)
}

func (p *xlStorageDiskIDCheck) CreateFile(volume, path string, size int64, reader io.Reader) error {
	if p.isDiskStale() {
		return errDiskNotFound
	}
	return p.storage.CreateFile(volume, path, size, reader)
}

func (p *xlStorageDiskIDCheck) ReadFileStream(volume, path string, offset, length int64) (io.ReadCloser, error) {
	if p.isDiskStale() {
		return nil, errDiskNotFound
	}
	return p.storage.ReadFileStream(volume, path, offset, length)
}

func (p *xlStorageDiskIDCheck) RenameFile(srcVolume, srcPath, dstVolume, dstPath string) error {
	if p.isDiskStale() {
		return errDiskNotFound
	}
	return p.storage.RenameFile(srcVolume, srcPath, dstVolume, dstPath)
}

func (p *xlStorageDiskIDCheck) RenameData(srcVolume, srcPath, dataDir, dstVolume, dstPath string) error {
	if p.isDiskStale() {
		return errDiskNotFound
	}
	return p.storage.RenameData(srcVolume, srcPath, dataDir, dstVolume, dstPath)
}

func (p *xlStorageDiskIDCheck) CheckParts(volume string, path string, fi FileInfo) (err error) {
	if p.isDiskStale() {
		return errDiskNotFound
	}
	return p.storage.CheckParts(volume, path, fi)
}

func (p *xlStorageDiskIDCheck) CheckFile(volume string, path string) (err error) {
	if p.isDiskStale() {
		return errDiskNotFound
	}
	return p.storage.CheckFile(volume, path)
}

func (p *xlStorageDiskIDCheck) DeleteFile(volume string, path string) (err error) {
	if p.isDiskStale() {
		return errDiskNotFound
	}
	return p.storage.DeleteFile(volume, path)
}

func (p *xlStorageDiskIDCheck) DeleteVersions(volume string, versions []FileInfo) (errs []error) {
	if p.isDiskStale() {
		errs = make([]error, len(versions))
		for i := range errs {
			errs[i] = errDiskNotFound
		}
		return errs
	}
	return p.storage.DeleteVersions(volume, versions)
}

func (p *xlStorageDiskIDCheck) VerifyFile(volume, path string, fi FileInfo) error {
	if p.isDiskStale() {
		return errDiskNotFound
	}
	return p.storage.VerifyFile(volume, path, fi)
}

func (p *xlStorageDiskIDCheck) WriteAll(volume string, path string, reader io.Reader) (err error) {
	if p.isDiskStale() {
		return errDiskNotFound
	}
	return p.storage.WriteAll(volume, path, reader)
}

func (p *xlStorageDiskIDCheck) DeleteVersion(volume, path string, fi FileInfo) (err error) {
	if p.isDiskStale() {
		return errDiskNotFound
	}
	return p.storage.DeleteVersion(volume, path, fi)
}

func (p *xlStorageDiskIDCheck) WriteMetadata(volume, path string, fi FileInfo) (err error) {
	if p.isDiskStale() {
		return errDiskNotFound
	}
	return p.storage.WriteMetadata(volume, path, fi)
}

func (p *xlStorageDiskIDCheck) ReadVersion(volume, path, versionID string) (fi FileInfo, err error) {
	if p.isDiskStale() {
		return fi, errDiskNotFound
	}
	return p.storage.ReadVersion(volume, path, versionID)
}

func (p *xlStorageDiskIDCheck) ReadAll(volume string, path string) (buf []byte, err error) {
	if p.isDiskStale() {
		return nil, errDiskNotFound
	}
	return p.storage.ReadAll(volume, path)
}
