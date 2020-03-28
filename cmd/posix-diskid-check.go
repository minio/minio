/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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
type posixDiskIDCheck struct {
	storage *posix
	diskID  string
}

func (p *posixDiskIDCheck) String() string {
	return p.storage.String()
}

func (p *posixDiskIDCheck) IsOnline() bool {
	storedDiskID, err := p.storage.GetDiskID()
	if err != nil {
		return false
	}
	return storedDiskID == p.diskID
}

func (p *posixDiskIDCheck) CrawlAndGetDataUsage(ctx context.Context, cache dataUsageCache) (dataUsageCache, error) {
	return p.storage.CrawlAndGetDataUsage(ctx, cache)
}

func (p *posixDiskIDCheck) Hostname() string {
	return p.storage.Hostname()
}

func (p *posixDiskIDCheck) Close() error {
	return p.storage.Close()
}

func (p *posixDiskIDCheck) GetDiskID() (string, error) {
	return p.diskID, nil
}

func (p *posixDiskIDCheck) SetDiskID(id string) {
	p.diskID = id
}

func (p *posixDiskIDCheck) isDiskStale() bool {
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

func (p *posixDiskIDCheck) DiskInfo() (info DiskInfo, err error) {
	if p.isDiskStale() {
		return info, errDiskNotFound
	}
	return p.storage.DiskInfo()
}

func (p *posixDiskIDCheck) MakeVolBulk(volumes ...string) (err error) {
	if p.isDiskStale() {
		return errDiskNotFound
	}
	return p.storage.MakeVolBulk(volumes...)
}

func (p *posixDiskIDCheck) MakeVol(volume string) (err error) {
	if p.isDiskStale() {
		return errDiskNotFound
	}
	return p.storage.MakeVol(volume)
}

func (p *posixDiskIDCheck) ListVols() ([]VolInfo, error) {
	if p.isDiskStale() {
		return nil, errDiskNotFound
	}
	return p.storage.ListVols()
}

func (p *posixDiskIDCheck) StatVol(volume string) (vol VolInfo, err error) {
	if p.isDiskStale() {
		return vol, errDiskNotFound
	}
	return p.storage.StatVol(volume)
}

func (p *posixDiskIDCheck) DeleteVol(volume string, forceDelete bool) (err error) {
	if p.isDiskStale() {
		return errDiskNotFound
	}
	return p.storage.DeleteVol(volume, forceDelete)
}

func (p *posixDiskIDCheck) Walk(volume, dirPath string, marker string, recursive bool, leafFile string, readMetadataFn readMetadataFunc, endWalkCh <-chan struct{}) (chan FileInfo, error) {
	if p.isDiskStale() {
		return nil, errDiskNotFound
	}
	return p.storage.Walk(volume, dirPath, marker, recursive, leafFile, readMetadataFn, endWalkCh)
}

func (p *posixDiskIDCheck) WalkSplunk(volume, dirPath string, marker string, endWalkCh <-chan struct{}) (chan FileInfo, error) {
	if p.isDiskStale() {
		return nil, errDiskNotFound
	}
	return p.storage.WalkSplunk(volume, dirPath, marker, endWalkCh)
}

func (p *posixDiskIDCheck) ListDir(volume, dirPath string, count int, leafFile string) ([]string, error) {
	if p.isDiskStale() {
		return nil, errDiskNotFound
	}
	return p.storage.ListDir(volume, dirPath, count, leafFile)
}

func (p *posixDiskIDCheck) ReadFile(volume string, path string, offset int64, buf []byte, verifier *BitrotVerifier) (n int64, err error) {
	if p.isDiskStale() {
		return 0, errDiskNotFound
	}
	return p.storage.ReadFile(volume, path, offset, buf, verifier)
}

func (p *posixDiskIDCheck) AppendFile(volume string, path string, buf []byte) (err error) {
	if p.isDiskStale() {
		return errDiskNotFound
	}
	return p.storage.AppendFile(volume, path, buf)
}

func (p *posixDiskIDCheck) CreateFile(volume, path string, size int64, reader io.Reader) error {
	if p.isDiskStale() {
		return errDiskNotFound
	}
	return p.storage.CreateFile(volume, path, size, reader)
}

func (p *posixDiskIDCheck) ReadFileStream(volume, path string, offset, length int64) (io.ReadCloser, error) {
	if p.isDiskStale() {
		return nil, errDiskNotFound
	}
	return p.storage.ReadFileStream(volume, path, offset, length)
}

func (p *posixDiskIDCheck) RenameFile(srcVolume, srcPath, dstVolume, dstPath string) error {
	if p.isDiskStale() {
		return errDiskNotFound
	}
	return p.storage.RenameFile(srcVolume, srcPath, dstVolume, dstPath)
}

func (p *posixDiskIDCheck) StatFile(volume string, path string) (file FileInfo, err error) {
	if p.isDiskStale() {
		return file, errDiskNotFound
	}
	return p.storage.StatFile(volume, path)
}

func (p *posixDiskIDCheck) DeleteFile(volume string, path string) (err error) {
	if p.isDiskStale() {
		return errDiskNotFound
	}
	return p.storage.DeleteFile(volume, path)
}

func (p *posixDiskIDCheck) DeleteFileBulk(volume string, paths []string) (errs []error, err error) {
	if p.isDiskStale() {
		return nil, errDiskNotFound
	}
	return p.storage.DeleteFileBulk(volume, paths)
}

func (p *posixDiskIDCheck) DeletePrefixes(volume string, paths []string) (errs []error, err error) {
	if p.isDiskStale() {
		return nil, errDiskNotFound
	}
	return p.storage.DeletePrefixes(volume, paths)
}

func (p *posixDiskIDCheck) VerifyFile(volume, path string, size int64, algo BitrotAlgorithm, sum []byte, shardSize int64) error {
	if p.isDiskStale() {
		return errDiskNotFound
	}
	return p.storage.VerifyFile(volume, path, size, algo, sum, shardSize)
}

func (p *posixDiskIDCheck) WriteAll(volume string, path string, reader io.Reader) (err error) {
	if p.isDiskStale() {
		return errDiskNotFound
	}
	return p.storage.WriteAll(volume, path, reader)
}

func (p *posixDiskIDCheck) ReadAll(volume string, path string) (buf []byte, err error) {
	if p.isDiskStale() {
		return nil, errDiskNotFound
	}
	return p.storage.ReadAll(volume, path)
}
