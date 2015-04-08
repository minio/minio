/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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

package donut

import (
	"errors"
	"os"
	"path"
	"syscall"

	"io/ioutil"

	"github.com/minio-io/iodine"
)

// internal disk struct
type disk struct {
	root       string
	order      int
	filesystem map[string]string
}

// NewDisk - instantiate new disk
func NewDisk(diskPath string, diskOrder int) (Disk, error) {
	if diskPath == "" || diskOrder < 0 {
		return nil, iodine.New(errors.New("invalid argument"), nil)
	}
	s := syscall.Statfs_t{}
	err := syscall.Statfs(diskPath, &s)
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	st, err := os.Stat(diskPath)
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	if !st.IsDir() {
		return nil, iodine.New(syscall.ENOTDIR, nil)
	}
	d := disk{
		root:       diskPath,
		order:      diskOrder,
		filesystem: make(map[string]string),
	}
	if fsType := d.getFSType(s.Type); fsType != "UNKNOWN" {
		d.filesystem["FSType"] = fsType
		d.filesystem["MountPoint"] = d.root
		return d, nil
	}
	return nil, iodine.New(errors.New("unsupported filesystem"), nil)
}

// GetPath - get root disk path
func (d disk) GetPath() string {
	return d.root
}

// GetOrder - get order of disk present in graph
func (d disk) GetOrder() int {
	return d.order
}

// GetFSInfo - get disk filesystem and its usage information
func (d disk) GetFSInfo() map[string]string {
	s := syscall.Statfs_t{}
	err := syscall.Statfs(d.root, &s)
	if err != nil {
		return nil
	}
	d.filesystem["Total"] = d.formatBytes(s.Bsize * int64(s.Blocks))
	d.filesystem["Free"] = d.formatBytes(s.Bsize * int64(s.Bfree))
	return d.filesystem
}

// MakeDir - make a directory inside disk root path
func (d disk) MakeDir(dirname string) error {
	return os.MkdirAll(path.Join(d.root, dirname), 0700)
}

// ListDir - list a directory inside disk root path, get only directories
func (d disk) ListDir(dirname string) ([]os.FileInfo, error) {
	contents, err := ioutil.ReadDir(path.Join(d.root, dirname))
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
func (d disk) ListFiles(dirname string) ([]os.FileInfo, error) {
	contents, err := ioutil.ReadDir(path.Join(d.root, dirname))
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

// MakeFile - create a file inside disk root path
func (d disk) MakeFile(filename string) (*os.File, error) {
	if filename == "" {
		return nil, iodine.New(errors.New("Invalid argument"), nil)
	}
	filePath := path.Join(d.root, filename)
	// Create directories if they don't exist
	if err := os.MkdirAll(path.Dir(filePath), 0700); err != nil {
		return nil, iodine.New(err, nil)
	}
	dataFile, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	return dataFile, nil
}

// OpenFile - read a file inside disk root path
func (d disk) OpenFile(filename string) (*os.File, error) {
	if filename == "" {
		return nil, iodine.New(errors.New("Invalid argument"), nil)
	}
	dataFile, err := os.Open(path.Join(d.root, filename))
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	return dataFile, nil
}
