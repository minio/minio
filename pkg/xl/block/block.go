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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package block

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/minio/minio/pkg/atomic"
	"github.com/minio/minio/pkg/disk"
	"github.com/minio/minio/pkg/probe"
)

// Block container for block disk parameters
type Block struct {
	lock   *sync.Mutex
	path   string
	fsInfo disk.Info
}

// ErrInvalidArgument - invalid argument.
var ErrInvalidArgument = errors.New("Invalid argument")

// New - instantiate new disk
func New(diskPath string) (Block, *probe.Error) {
	if diskPath == "" {
		return Block{}, probe.NewError(ErrInvalidArgument)
	}
	st, err := os.Stat(diskPath)
	if err != nil {
		return Block{}, probe.NewError(err)
	}

	if !st.IsDir() {
		return Block{}, probe.NewError(syscall.ENOTDIR)
	}
	info, err := disk.GetInfo(diskPath)
	if err != nil {
		return Block{}, probe.NewError(err)
	}
	disk := Block{
		lock:   &sync.Mutex{},
		path:   diskPath,
		fsInfo: info,
	}
	return disk, nil
}

// IsUsable - is disk usable, alive
func (d Block) IsUsable() bool {
	_, err := os.Stat(d.path)
	if err != nil {
		return false
	}
	return true
}

// GetPath - get root disk path
func (d Block) GetPath() string {
	return d.path
}

// GetFSInfo - get disk filesystem and its usage information
func (d Block) GetFSInfo() disk.Info {
	d.lock.Lock()
	defer d.lock.Unlock()

	info, err := disk.GetInfo(d.path)
	if err != nil {
		return d.fsInfo
	}
	d.fsInfo = info
	return info
}

// MakeDir - make a directory inside disk root path
func (d Block) MakeDir(dirname string) *probe.Error {
	d.lock.Lock()
	defer d.lock.Unlock()
	if err := os.MkdirAll(filepath.Join(d.path, dirname), 0700); err != nil {
		return probe.NewError(err)
	}
	return nil
}

// ListDir - list a directory inside disk root path, get only directories
func (d Block) ListDir(dirname string) ([]os.FileInfo, *probe.Error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	dir, err := os.Open(filepath.Join(d.path, dirname))
	if err != nil {
		return nil, probe.NewError(err)
	}
	defer dir.Close()
	contents, err := dir.Readdir(-1)
	if err != nil {
		return nil, probe.NewError(err)
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
func (d Block) ListFiles(dirname string) ([]os.FileInfo, *probe.Error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	dir, err := os.Open(filepath.Join(d.path, dirname))
	if err != nil {
		return nil, probe.NewError(err)
	}
	defer dir.Close()
	contents, err := dir.Readdir(-1)
	if err != nil {
		return nil, probe.NewError(err)
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

// CreateFile - create a file inside disk root path, replies with custome d.File which provides atomic writes
func (d Block) CreateFile(filename string) (*atomic.File, *probe.Error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	if filename == "" {
		return nil, probe.NewError(ErrInvalidArgument)
	}

	f, err := atomic.FileCreate(filepath.Join(d.path, filename))
	if err != nil {
		return nil, probe.NewError(err)
	}

	return f, nil
}

// Open - read a file inside disk root path
func (d Block) Open(filename string) (*os.File, *probe.Error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	if filename == "" {
		return nil, probe.NewError(ErrInvalidArgument)
	}
	dataFile, err := os.Open(filepath.Join(d.path, filename))
	if err != nil {
		return nil, probe.NewError(err)
	}
	return dataFile, nil
}

// OpenFile - Use with caution
func (d Block) OpenFile(filename string, flags int, perm os.FileMode) (*os.File, *probe.Error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	if filename == "" {
		return nil, probe.NewError(ErrInvalidArgument)
	}
	dataFile, err := os.OpenFile(filepath.Join(d.path, filename), flags, perm)
	if err != nil {
		return nil, probe.NewError(err)
	}
	return dataFile, nil
}
