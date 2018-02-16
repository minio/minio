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

import (
	"io"

	"github.com/minio/minio/pkg/disk"
)

// StorageAPI interface.
type StorageAPI interface {
	// Stringified version of disk.
	String() string

	// Storage operations.
	IsOnline() bool // Returns true if disk is online.
	Close() error
	DiskInfo() (info disk.Info, err error)

	// Volume operations.
	MakeVol(volume string) (err error)
	ListVols() (vols []VolInfo, err error)
	StatVol(volume string) (vol VolInfo, err error)
	DeleteVol(volume string) (err error)

	// File operations.
	ListDir(volume, dirPath string) ([]string, error)
	ReadFile(volume string, path string, offset int64, buf []byte, verifier *BitrotVerifier) (n int64, err error)
	PrepareFile(volume string, path string, len int64) (err error)
	AppendFile(volume string, path string, buf []byte) (err error)
	RenameFile(srcVolume, srcPath, dstVolume, dstPath string) error
	StatFile(volume string, path string) (file FileInfo, err error)
	DeleteFile(volume string, path string) (err error)

	// Read all.
	ReadAll(volume string, path string) (buf []byte, err error)
}

// storageReader is an io.Reader view of a disk
type storageReader struct {
	storage      StorageAPI
	volume, path string
	offset       int64
}

func (r *storageReader) Read(p []byte) (n int, err error) {
	nn, err := r.storage.ReadFile(r.volume, r.path, r.offset, p, nil)
	r.offset += nn
	n = int(nn)

	if err == io.ErrUnexpectedEOF && nn > 0 {
		err = io.EOF
	}
	return
}

// storageWriter is a io.Writer view of a disk.
type storageWriter struct {
	storage      StorageAPI
	volume, path string
}

func (w *storageWriter) Write(p []byte) (n int, err error) {
	err = w.storage.AppendFile(w.volume, w.path, p)
	if err == nil {
		n = len(p)
	}
	return
}

// StorageWriter returns a new io.Writer which appends data to the file
// at the given disk, volume and path.
func StorageWriter(storage StorageAPI, volume, path string) io.Writer {
	return &storageWriter{storage, volume, path}
}

// StorageReader returns a new io.Reader which reads data to the file
// at the given disk, volume, path and offset.
func StorageReader(storage StorageAPI, volume, path string, offset int64) io.Reader {
	return &storageReader{storage, volume, path, offset}
}
