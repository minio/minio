/*
 * MinIO Cloud Storage, (C) 2021 MinIO, Inc.
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

// offlineDisk is a dummy disk used instead of 'nil' to avoid code crashes
type offlineDisk struct {
	endpoint Endpoint
	err      error
}

func newOfflineDisk(ep Endpoint, err error) StorageAPI {
	return &offlineDisk{endpoint: ep, err: err}
}

func (d *offlineDisk) String() string {
	return d.endpoint.String()
}

func (d *offlineDisk) IsOnline() bool {
	return false
}

func (d *offlineDisk) IsLocal() bool {
	return d.endpoint.IsLocal
}

func (d *offlineDisk) Endpoint() Endpoint {
	return d.endpoint
}

func (d *offlineDisk) Hostname() string {
	return d.endpoint.Host
}

func (d *offlineDisk) Healing() bool {
	return false
}

func (d *offlineDisk) Close() (err error) {
	return nil
}

func (d *offlineDisk) GetDiskID() (string, error) {
	return "", d.err
}

func (d *offlineDisk) SetDiskID(id string) {
	// no-op
}

func (d *offlineDisk) CrawlAndGetDataUsage(ctx context.Context, cache dataUsageCache) (info dataUsageCache, err error) {
	return info, d.err
}

func (d *offlineDisk) DiskInfo(ctx context.Context) (info DiskInfo, err error) {
	return info, d.err
}

func (d *offlineDisk) MakeVolBulk(ctx context.Context, volumes ...string) (err error) {
	return d.err
}

func (d *offlineDisk) MakeVol(ctx context.Context, volume string) (err error) {
	return d.err
}

func (d *offlineDisk) ListVols(ctx context.Context) (vols []VolInfo, err error) {
	return vols, d.err
}

func (d *offlineDisk) StatVol(ctx context.Context, volume string) (vol VolInfo, err error) {
	return vol, d.err
}
func (d *offlineDisk) DeleteVol(ctx context.Context, volume string, forceDelete bool) (err error) {
	return d.err
}

func (d *offlineDisk) WalkDir(ctx context.Context, opts WalkDirOptions, wr io.Writer) error {
	return d.err
}

func (d *offlineDisk) WalkVersions(ctx context.Context, volume, dirPath, marker string, recursive bool, endWalkCh <-chan struct{}) (chan FileInfoVersions, error) {
	return nil, d.err
}

func (d *offlineDisk) ListDir(ctx context.Context, volume, dirPath string, count int) (entries []string, err error) {
	return nil, d.err
}

func (d *offlineDisk) ReadFile(ctx context.Context, volume string, path string, offset int64, buf []byte, verifier *BitrotVerifier) (n int64, err error) {
	return 0, d.err
}

func (d *offlineDisk) ReadFileStream(ctx context.Context, volume, path string, offset, length int64) (io.ReadCloser, error) {
	return nil, d.err
}

func (d *offlineDisk) CreateFile(ctx context.Context, volume, path string, size int64, reader io.Reader) error {
	return d.err
}

func (d *offlineDisk) AppendFile(ctx context.Context, volume string, path string, buf []byte) error {
	return d.err
}

func (d *offlineDisk) RenameData(ctx context.Context, srcVolume, srcPath, dataDir, dstVolume, dstPath string) error {
	return d.err
}

func (d *offlineDisk) RenameFile(ctx context.Context, srcVolume, srcPath, dstVolume, dstPath string) error {
	return d.err
}

func (d *offlineDisk) CheckParts(ctx context.Context, volume string, path string, fi FileInfo) (err error) {
	return d.err
}

func (d *offlineDisk) CheckFile(ctx context.Context, volume string, path string) (err error) {
	return d.err
}

func (d *offlineDisk) Delete(ctx context.Context, volume string, path string, recursive bool) (err error) {
	return d.err
}

func (d *offlineDisk) DeleteVersions(ctx context.Context, volume string, versions []FileInfo) []error {
	errs := make([]error, len(versions))
	for i := range errs {
		errs[i] = d.err
	}
	return errs
}

func (d *offlineDisk) WriteMetadata(ctx context.Context, volume, path string, fi FileInfo) (err error) {
	return d.err
}

func (d *offlineDisk) DeleteVersion(ctx context.Context, volume, path string, fi FileInfo) (err error) {
	return d.err
}

func (d *offlineDisk) ReadVersion(ctx context.Context, volume, path, versionID string, readData bool) (fi FileInfo, err error) {
	return fi, d.err
}

func (d *offlineDisk) WriteAll(ctx context.Context, volume string, path string, b []byte) (err error) {
	return d.err
}

func (d *offlineDisk) ReadAll(ctx context.Context, volume string, path string) (buf []byte, err error) {
	return nil, d.err
}

func (d *offlineDisk) VerifyFile(ctx context.Context, volume, path string, fi FileInfo) error {
	return d.err
}
