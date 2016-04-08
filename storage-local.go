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

package main

import (
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/minio/minio/pkg/disk"
	"github.com/minio/minio/pkg/safe"
)

// ErrDiskPathFull - cannot create volume or files when disk is full.
var ErrDiskPathFull = errors.New("Disk path full.")

// ErrVolumeExists - cannot create same volume again.
var ErrVolumeExists = errors.New("Volume already exists.")

// ErrIsNotRegular - is not a regular file type.
var ErrIsNotRegular = errors.New("Not a regular file type.")

// localStorage implements StorageAPI on top of provided diskPath.
type localStorage struct {
	diskPath    string
	fsInfo      disk.Info
	minFreeDisk int64
}

// Initialize a new local storage.
func newLocalStorage(diskPath string) (StorageAPI, error) {
	if diskPath == "" {
		return nil, errInvalidArgument
	}
	st, e := os.Stat(diskPath)
	if e != nil {
		return nil, e
	}
	if !st.IsDir() {
		return nil, syscall.ENOTDIR
	}

	info, e := disk.GetInfo(diskPath)
	if e != nil {
		return nil, e
	}
	disk := localStorage{
		diskPath:    diskPath,
		fsInfo:      info,
		minFreeDisk: 5, // Minimum 5% disk should be free.
	}
	return disk, nil
}

// checkDiskFree verifies if disk path has sufficient minium free disk
// space.
func checkDiskFree(diskPath string, minFreeDisk int64) error {
	di, e := disk.GetInfo(diskPath)
	if e != nil {
		return e
	}

	// Remove 5% from total space for cumulative disk space used for journalling, inodes etc.
	availableDiskSpace := (float64(di.Free) / (float64(di.Total) - (0.05 * float64(di.Total)))) * 100
	if int64(availableDiskSpace) <= minFreeDisk {
		return ErrDiskPathFull
	}

	// Success.
	return nil
}

// Make a volume entry.
func (s localStorage) MakeVol(volume string) error {
	if e := checkDiskFree(s.diskPath, s.minFreeDisk); e != nil {
		return e
	}
	volumeDir := getVolumeDir(s.diskPath, volume)
	if _, e := os.Stat(volumeDir); e == nil {
		return ErrVolumeExists
	}

	// Make a volume entry.
	if e := os.Mkdir(volumeDir, 0700); e != nil {
		return e
	}
	return nil
}

// removeDuplicateVols - remove duplicate volumes.
func removeDuplicateVols(vols []VolInfo) []VolInfo {
	length := len(vols) - 1
	for i := 0; i < length; i++ {
		for j := i + 1; j <= length; j++ {
			if vols[i].Name == vols[j].Name {
				// Pick the latest volume from a duplicate entry.
				if vols[i].Created.Sub(vols[j].Created) > 0 {
					vols[i] = vols[length]
				} else {
					vols[j] = vols[length]
				}
				vols = vols[0:length]
				length--
				j--
			}
		}
	}
	return vols
}

// ListVols - list volumes.
func (s localStorage) ListVols() ([]VolInfo, error) {
	files, e := ioutil.ReadDir(s.diskPath)
	if e != nil {
		return nil, e
	}
	var volsInfo []VolInfo
	for _, file := range files {
		if !file.IsDir() {
			// If not directory, ignore all file types.
			continue
		}
		volInfo := VolInfo{
			Name:    file.Name(),
			Created: file.ModTime(),
		}
		volsInfo = append(volsInfo, volInfo)
	}
	// Remove duplicated volume entries.
	volsInfo = removeDuplicateVols(volsInfo)
	return volsInfo, nil
}

// getVolumeDir - will convert incoming volume names to
// corresponding valid volume names on the backend in a platform
// compatible way for all operating systems.
func getVolumeDir(diskPath, volume string) string {
	volumes, e := ioutil.ReadDir(diskPath)
	if e != nil {
		return volume
	}
	for _, vol := range volumes {
		// Verify if lowercase version of the volume
		// is equal to the incoming volume, then use the proper name.
		if strings.ToLower(vol.Name()) == volume {
			return filepath.Join(diskPath, vol.Name())
		}
	}
	return filepath.Join(diskPath, volume)
}

// StatVol - get volume info.
func (s localStorage) StatVol(volume string) (VolInfo, error) {
	volumeDir := getVolumeDir(s.diskPath, volume)
	// Stat a volume entry.
	st, e := os.Stat(volumeDir)
	if e != nil {
		return VolInfo{}, e
	}
	volInfo := VolInfo{}
	volInfo.Name = st.Name()
	volInfo.Created = st.ModTime()
	return volInfo, nil
}

// DeleteVol - delete a volume.
func (s localStorage) DeleteVol(volume string) error {
	return os.Remove(getVolumeDir(s.diskPath, volume))
}

/// File operations.

// ListFiles - list files are prefix and marker.
func (s localStorage) ListFiles(volume, prefix, marker string, recursive bool, count int) (files []FileInfo, isEOF bool, err error) {
	// TODO
	return files, true, nil
}

// ReadFile - read a file at a given offset.
func (s localStorage) ReadFile(volume string, path string, offset int64) (io.ReadCloser, error) {
	filePath := filepath.Join(getVolumeDir(s.diskPath, volume), path)
	file, e := os.Open(filePath)
	if e != nil {
		return nil, e
	}
	st, e := file.Stat()
	if e != nil {
		return nil, e
	}
	// Verify if its not a regular file, since subsequent Seek is undefined.
	if !st.Mode().IsRegular() {
		return nil, ErrIsNotRegular
	}
	_, e = file.Seek(offset, os.SEEK_SET)
	if e != nil {
		return nil, e
	}
	return file, nil
}

// CreateFile - create a file at path.
func (s localStorage) CreateFile(volume, path string) (writeCloser io.WriteCloser, err error) {
	if e := checkDiskFree(s.diskPath, s.minFreeDisk); e != nil {
		return nil, e
	}
	filePath := filepath.Join(getVolumeDir(s.diskPath, volume), path)
	// Creates a safe file.
	return safe.CreateFileWithPrefix(filePath, "$tmpfile")
}

// StatFile - get file info.
func (s localStorage) StatFile(volume, path string) (file FileInfo, err error) {
	filePath := filepath.Join(getVolumeDir(s.diskPath, volume), path)
	st, e := os.Stat(filePath)
	if e != nil {
		return FileInfo{}, e
	}
	file = FileInfo{
		Volume:  volume,
		Name:    st.Name(),
		ModTime: st.ModTime(),
		Size:    st.Size(),
		Type:    st.Mode(),
	}
	return file, nil
}

// deleteFile - delete file path if its empty.
func deleteFile(basePath, deletePath, volume, path string) error {
	if basePath == deletePath {
		return nil
	}
	// Verify if the path exists.
	pathSt, e := os.Stat(deletePath)
	if e != nil {
		return e
	}
	if pathSt.IsDir() {
		// Verify if directory is empty.
		empty, e := isDirEmpty(deletePath)
		if e != nil {
			return e
		}
		if !empty {
			return nil
		}
	}
	// Attempt to remove path.
	if e := os.Remove(deletePath); e != nil {
		return e
	}
	// Recursively go down the next path and delete again.
	if e := deleteFile(basePath, filepath.Dir(deletePath), volume, path); e != nil {
		return e
	}
	return nil
}

// DeleteFile - delete a file at path.
func (s localStorage) DeleteFile(volume, path string) error {
	volumeDir := getVolumeDir(s.diskPath, volume)

	// Following code is needed so that we retain "/" suffix if any
	// in path argument. Do not use filepath.Join() since it would
	// strip off any suffixes.
	filePath := s.diskPath + string(os.PathSeparator) + volume + string(os.PathSeparator) + path

	// Convert to platform friendly paths.
	filePath = filepath.FromSlash(filePath)

	// Delete file and delete parent directory as well if its empty.
	return deleteFile(volumeDir, filePath, volume, path)
}
