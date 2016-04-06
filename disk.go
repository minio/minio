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
	"time"

	"github.com/minio/minio/pkg/disk"
	"github.com/minio/minio/pkg/safe"
)

type storageDisk struct {
	diskPath    string
	fsInfo      disk.Info
	minFreeDisk int
}

// Initialize a new storage disk.
func newStorageDisk(diskPath string) (StorageAPI, error) {
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
	disk := storageDisk{
		diskPath:    diskPath,
		fsInfo:      info,
		minFreeDisk: 5, // Minimum 10% disk should be free.
	}
	return disk, nil
}

var errDiskPathFull = errors.New("Disk path full.")
var errVolumeExists = errors.New("Volume already exists.")

// Make a volume entry.
func (s storageDisk) MakeVol(volume string) error {
	di, e := disk.GetInfo(s.diskPath)
	if e != nil {
		return e
	}

	// Remove 5% from total space for cumulative disk space used for journalling, inodes etc.
	availableDiskSpace := (float64(di.Free) / (float64(di.Total) - (0.05 * float64(di.Total)))) * 100
	if int(availableDiskSpace) <= s.minFreeDisk {
		return errDiskPathFull
	}

	volumeDir := getVolumeDir(s.diskPath, volume)
	if _, e := os.Stat(volumeDir); e == nil {
		return errVolumeExists
	}

	// Make a volume entry.
	if e := os.Mkdir(volumeDir, 0700); e != nil {
		return e
	}

	return nil
}

// VolInfo - volume info
type VolInfo struct {
	Name    string
	Created time.Time
}

// removeDuplicateVols - remove duplicate volumes.
func removeDuplicateVols(vols []VolInfo) []VolInfo {
	length := len(vols) - 1
	for i := 0; i < length; i++ {
		for j := i + 1; j <= length; j++ {
			if vols[i].Name == vols[j].Name {
				// Pick the latest volume, if there is a duplicate.
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
func (s storageDisk) ListVols() ([]VolInfo, error) {
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
func (s storageDisk) StatVol(volume string) (VolInfo, error) {
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

func (s storageDisk) DeleteVol(volume string) error {
	return os.Remove(getVolumeDir(s.diskPath, volume))
}

// File operations.
func (s storageDisk) ListFiles(volume, prefix, marker string, recursive bool, count int) (files []FileInfo, isEOF bool, err error) {
	return files, true, nil
}

func (s storageDisk) ReadFile(volume string, path string, offset int64) (io.ReadCloser, error) {
	filePath := filepath.Join(getVolumeDir(s.diskPath, volume), path)
	file, e := os.Open(filePath)
	if e != nil {
		return nil, e
	}
	_, e = file.Seek(offset, os.SEEK_SET)
	if e != nil {
		return nil, e
	}
	return file, nil
}

// CreateFile - create a file at path.
func (s storageDisk) CreateFile(volume, path string) (writeCloser io.WriteCloser, err error) {
	filePath := filepath.Join(getVolumeDir(s.diskPath, volume), path)
	return safe.CreateFileWithPrefix(filePath, "$tmpfile")
}

// FileInfo - file stat information.
type FileInfo struct {
	Volume  string
	Name    string
	ModTime time.Time
	Size    int64
	Type    os.FileMode
}

func (s storageDisk) StatFile(volume, path string) (file FileInfo, err error) {
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

func (s storageDisk) DeleteFile(volume, path string) error {
	filePath := filepath.Join(getVolumeDir(s.diskPath, volume), path)
	return os.Remove(filePath)
}
