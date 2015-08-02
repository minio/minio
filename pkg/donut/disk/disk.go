/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or impliedisk.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package disk

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/minio/minio/pkg/iodine"
	"github.com/minio/minio/pkg/utils/atomic"
)

// Disk container for disk parameters
type Disk struct {
	lock   *sync.Mutex
	path   string
	fsInfo map[string]string
}

// New - instantiate new disk
func New(diskPath string) (Disk, error) {
	if diskPath == "" {
		return Disk{}, iodine.New(InvalidArgument{}, nil)
	}
	st, err := os.Stat(diskPath)
	if err != nil {
		return Disk{}, iodine.New(err, nil)
	}
	if !st.IsDir() {
		return Disk{}, iodine.New(syscall.ENOTDIR, nil)
	}
	s := syscall.Statfs_t{}
	err = syscall.Statfs(diskPath, &s)
	if err != nil {
		return Disk{}, iodine.New(err, nil)
	}
	disk := Disk{
		lock:   &sync.Mutex{},
		path:   diskPath,
		fsInfo: make(map[string]string),
	}
	if fsType := getFSType(s.Type); fsType != "UNKNOWN" {
		disk.fsInfo["FSType"] = fsType
		disk.fsInfo["MountPoint"] = disk.path
		return disk, nil
	}
	return Disk{}, iodine.New(UnsupportedFilesystem{Type: strconv.FormatInt(int64(s.Type), 10)},
		map[string]string{"Type": strconv.FormatInt(int64(s.Type), 10)})
}

// IsUsable - is disk usable, alive
func (disk Disk) IsUsable() bool {
	_, err := os.Stat(disk.path)
	if err != nil {
		return false
	}
	return true
}

// GetPath - get root disk path
func (disk Disk) GetPath() string {
	return disk.path
}

// GetFSInfo - get disk filesystem and its usage information
func (disk Disk) GetFSInfo() map[string]string {
	disk.lock.Lock()
	defer disk.lock.Unlock()

	s := syscall.Statfs_t{}
	err := syscall.Statfs(disk.path, &s)
	if err != nil {
		return nil
	}
	disk.fsInfo["Total"] = formatBytes(int64(s.Bsize) * int64(s.Blocks))
	disk.fsInfo["Free"] = formatBytes(int64(s.Bsize) * int64(s.Bfree))
	disk.fsInfo["TotalB"] = strconv.FormatInt(int64(s.Bsize)*int64(s.Blocks), 10)
	disk.fsInfo["FreeB"] = strconv.FormatInt(int64(s.Bsize)*int64(s.Bfree), 10)
	return disk.fsInfo
}

// MakeDir - make a directory inside disk root path
func (disk Disk) MakeDir(dirname string) error {
	disk.lock.Lock()
	defer disk.lock.Unlock()
	return os.MkdirAll(filepath.Join(disk.path, dirname), 0700)
}

// ListDir - list a directory inside disk root path, get only directories
func (disk Disk) ListDir(dirname string) ([]os.FileInfo, error) {
	disk.lock.Lock()
	defer disk.lock.Unlock()

	dir, err := os.Open(filepath.Join(disk.path, dirname))
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	defer dir.Close()
	contents, err := dir.Readdir(-1)
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	var directories []os.FileInfo
	for _, content := range contents {
		// Include only directories, ignore everything else
		if content.IsDir() {
			directories = append(directories, content)
		}
	}
	return directories, nil
}

// ListFiles - list a directory inside disk root path, get only files
func (disk Disk) ListFiles(dirname string) ([]os.FileInfo, error) {
	disk.lock.Lock()
	defer disk.lock.Unlock()

	dir, err := os.Open(filepath.Join(disk.path, dirname))
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	defer dir.Close()
	contents, err := dir.Readdir(-1)
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	var files []os.FileInfo
	for _, content := range contents {
		// Include only regular files, ignore everything else
		if content.Mode().IsRegular() {
			files = append(files, content)
		}
	}
	return files, nil
}

// CreateFile - create a file inside disk root path, replies with custome disk.File which provides atomic writes
func (disk Disk) CreateFile(filename string) (*atomic.File, error) {
	disk.lock.Lock()
	defer disk.lock.Unlock()

	if filename == "" {
		return nil, iodine.New(InvalidArgument{}, nil)
	}

	f, err := atomic.FileCreate(filepath.Join(disk.path, filename))
	if err != nil {
		return nil, iodine.New(err, nil)
	}

	return f, nil
}

// Open - read a file inside disk root path
func (disk Disk) Open(filename string) (*os.File, error) {
	disk.lock.Lock()
	defer disk.lock.Unlock()

	if filename == "" {
		return nil, iodine.New(InvalidArgument{}, nil)
	}
	dataFile, err := os.Open(filepath.Join(disk.path, filename))
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	return dataFile, nil
}

// OpenFile - Use with caution
func (disk Disk) OpenFile(filename string, flags int, perm os.FileMode) (*os.File, error) {
	disk.lock.Lock()
	defer disk.lock.Unlock()

	if filename == "" {
		return nil, iodine.New(InvalidArgument{}, nil)
	}
	dataFile, err := os.OpenFile(filepath.Join(disk.path, filename), flags, perm)
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	return dataFile, nil
}

// formatBytes - Convert bytes to human readable string. Like a 2 MB, 64.2 KB, 52 B
func formatBytes(i int64) (result string) {
	switch {
	case i > (1024 * 1024 * 1024 * 1024):
		result = fmt.Sprintf("%.02f TB", float64(i)/1024/1024/1024/1024)
	case i > (1024 * 1024 * 1024):
		result = fmt.Sprintf("%.02f GB", float64(i)/1024/1024/1024)
	case i > (1024 * 1024):
		result = fmt.Sprintf("%.02f MB", float64(i)/1024/1024)
	case i > 1024:
		result = fmt.Sprintf("%.02f KB", float64(i)/1024)
	default:
		result = fmt.Sprintf("%d B", i)
	}
	result = strings.Trim(result, " ")
	return
}
