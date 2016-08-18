/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

// Simulates disk returning errFaultyDisk on all methods of StorageAPI
// interface after successCount number of successes.
type faultyDisk struct {
	disk         *posix
	successCount int
}

// instantiates a faulty
func newFaultyDisk(disk *posix, n int) *faultyDisk {
	return &faultyDisk{disk: disk, successCount: n}
}

func (f *faultyDisk) MakeVol(volume string) (err error) {
	if f.successCount > 0 {
		f.successCount--
		return f.disk.MakeVol(volume)
	}
	return errFaultyDisk
}
func (f *faultyDisk) ListVols() (vols []VolInfo, err error) {
	if f.successCount > 0 {
		f.successCount--
		return f.disk.ListVols()
	}
	return nil, errFaultyDisk
}

func (f *faultyDisk) StatVol(volume string) (volInfo VolInfo, err error) {
	if f.successCount > 0 {
		f.successCount--
		return f.disk.StatVol(volume)
	}
	return VolInfo{}, errFaultyDisk
}
func (f *faultyDisk) DeleteVol(volume string) (err error) {
	if f.successCount > 0 {
		f.successCount--
		return f.disk.DeleteVol(volume)
	}
	return errFaultyDisk
}

func (f *faultyDisk) ListDir(volume, path string) (entries []string, err error) {
	if f.successCount > 0 {
		f.successCount--
		return f.disk.ListDir(volume, path)
	}
	return []string{}, errFaultyDisk
}

func (f *faultyDisk) ReadFile(volume string, path string, offset int64, buf []byte) (n int64, err error) {
	if f.successCount > 0 {
		f.successCount--
		return f.disk.ReadFile(volume, path, offset, buf)
	}
	return 0, errFaultyDisk
}

func (f *faultyDisk) AppendFile(volume, path string, buf []byte) error {
	if f.successCount > 0 {
		f.successCount--
		return f.disk.AppendFile(volume, path, buf)
	}
	return errFaultyDisk
}

func (f *faultyDisk) RenameFile(srcVolume, srcPath, dstVolume, dstPath string) error {
	if f.successCount > 0 {
		f.successCount--
		return f.disk.RenameFile(srcVolume, srcPath, dstVolume, dstPath)
	}
	return errFaultyDisk
}

func (f *faultyDisk) StatFile(volume string, path string) (file FileInfo, err error) {
	if f.successCount > 0 {
		f.successCount--
		return f.disk.StatFile(volume, path)
	}
	return FileInfo{}, errFaultyDisk
}

func (f *faultyDisk) DeleteFile(volume string, path string) (err error) {
	if f.successCount > 0 {
		f.successCount--
		return f.disk.DeleteFile(volume, path)
	}
	return errFaultyDisk
}

func (f *faultyDisk) ReadAll(volume string, path string) (buf []byte, err error) {
	if f.successCount > 0 {
		f.successCount--
		return f.disk.ReadAll(volume, path)
	}
	return nil, errFaultyDisk
}
