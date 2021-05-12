// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"io"
	"time"
)

// StorageAPI interface.
type StorageAPI interface {
	// Stringified version of disk.
	String() string

	// Storage operations.
	IsOnline() bool      // Returns true if disk is online.
	LastConn() time.Time // Returns the last time this disk (re)-connected

	IsLocal() bool

	Hostname() string   // Returns host name if remote host.
	Endpoint() Endpoint // Returns endpoint.

	Close() error
	GetDiskID() (string, error)
	SetDiskID(id string)
	Healing() *healingTracker // Returns nil if disk is not healing.

	DiskInfo(ctx context.Context) (info DiskInfo, err error)
	NSScanner(ctx context.Context, cache dataUsageCache, updates chan<- dataUsageEntry) (dataUsageCache, error)

	// Volume operations.
	MakeVol(ctx context.Context, volume string) (err error)
	MakeVolBulk(ctx context.Context, volumes ...string) (err error)
	ListVols(ctx context.Context) (vols []VolInfo, err error)
	StatVol(ctx context.Context, volume string) (vol VolInfo, err error)
	DeleteVol(ctx context.Context, volume string, forceDelete bool) (err error)

	// WalkDir will walk a directory on disk and return a metacache stream on wr.
	WalkDir(ctx context.Context, opts WalkDirOptions, wr io.Writer) error

	// Metadata operations
	DeleteVersion(ctx context.Context, volume, path string, fi FileInfo, forceDelMarker bool) error
	DeleteVersions(ctx context.Context, volume string, versions []FileInfo) []error
	WriteMetadata(ctx context.Context, volume, path string, fi FileInfo) error
	UpdateMetadata(ctx context.Context, volume, path string, fi FileInfo) error
	ReadVersion(ctx context.Context, volume, path, versionID string, readData bool) (FileInfo, error)
	RenameData(ctx context.Context, srcVolume, srcPath string, fi FileInfo, dstVolume, dstPath string) error

	// File operations.
	ListDir(ctx context.Context, volume, dirPath string, count int) ([]string, error)
	ReadFile(ctx context.Context, volume string, path string, offset int64, buf []byte, verifier *BitrotVerifier) (n int64, err error)
	AppendFile(ctx context.Context, volume string, path string, buf []byte) (err error)
	CreateFile(ctx context.Context, volume, path string, size int64, reader io.Reader) error
	ReadFileStream(ctx context.Context, volume, path string, offset, length int64) (io.ReadCloser, error)
	RenameFile(ctx context.Context, srcVolume, srcPath, dstVolume, dstPath string) error
	CheckParts(ctx context.Context, volume string, path string, fi FileInfo) error
	CheckFile(ctx context.Context, volume string, path string) (err error)
	Delete(ctx context.Context, volume string, path string, recursive bool) (err error)
	VerifyFile(ctx context.Context, volume, path string, fi FileInfo) error

	// Write all data, syncs the data to disk.
	// Should be used for smaller payloads.
	WriteAll(ctx context.Context, volume string, path string, b []byte) (err error)

	// Read all.
	ReadAll(ctx context.Context, volume string, path string) (buf []byte, err error)

	GetDiskLoc() (poolIdx, setIdx, diskIdx int) // Retrieve location indexes.
	SetDiskLoc(poolIdx, setIdx, diskIdx int)    // Set location indexes.
}

// storageReader is an io.Reader view of a disk
type storageReader struct {
	storage      StorageAPI
	volume, path string
	offset       int64
}

func (r *storageReader) Read(p []byte) (n int, err error) {
	nn, err := r.storage.ReadFile(context.TODO(), r.volume, r.path, r.offset, p, nil)
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
	err = w.storage.AppendFile(context.TODO(), w.volume, w.path, p)
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
