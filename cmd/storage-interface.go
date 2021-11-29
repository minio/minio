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
	DeleteVersions(ctx context.Context, volume string, versions []FileInfoVersions) []error
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
	Delete(ctx context.Context, volume string, path string, recursive bool) (err error)
	VerifyFile(ctx context.Context, volume, path string, fi FileInfo) error
	StatInfoFile(ctx context.Context, volume, path string, glob bool) (stat []StatInfo, err error)

	// Write all data, syncs the data to disk.
	// Should be used for smaller payloads.
	WriteAll(ctx context.Context, volume string, path string, b []byte) (err error)

	// Read all.
	ReadAll(ctx context.Context, volume string, path string) (buf []byte, err error)

	GetDiskLoc() (poolIdx, setIdx, diskIdx int) // Retrieve location indexes.
	SetDiskLoc(poolIdx, setIdx, diskIdx int)    // Set location indexes.
}

type unrecognizedDisk struct {
	storage StorageAPI
}

func (p *unrecognizedDisk) WalkDir(ctx context.Context, opts WalkDirOptions, wr io.Writer) (err error) {
	return errDiskNotFound
}

func (p *unrecognizedDisk) String() string {
	return p.storage.String()
}

func (p *unrecognizedDisk) IsOnline() bool {
	return false
}

func (p *unrecognizedDisk) LastConn() time.Time {
	return p.storage.LastConn()
}

func (p *unrecognizedDisk) IsLocal() bool {
	return p.storage.IsLocal()
}

func (p *unrecognizedDisk) Endpoint() Endpoint {
	return p.storage.Endpoint()
}

func (p *unrecognizedDisk) Hostname() string {
	return p.storage.Hostname()
}

func (p *unrecognizedDisk) Healing() *healingTracker {
	return nil
}

func (p *unrecognizedDisk) NSScanner(ctx context.Context, cache dataUsageCache, updates chan<- dataUsageEntry) (dataUsageCache, error) {
	return dataUsageCache{}, errDiskNotFound
}

func (p *unrecognizedDisk) GetDiskLoc() (poolIdx, setIdx, diskIdx int) {
	return -1, -1, -1
}

func (p *unrecognizedDisk) SetDiskLoc(poolIdx, setIdx, diskIdx int) {
}

func (p *unrecognizedDisk) Close() error {
	return p.storage.Close()
}

func (p *unrecognizedDisk) GetDiskID() (string, error) {
	return "", errDiskNotFound
}

func (p *unrecognizedDisk) SetDiskID(id string) {
}

func (p *unrecognizedDisk) DiskInfo(ctx context.Context) (info DiskInfo, err error) {
	return info, errDiskNotFound
}

func (p *unrecognizedDisk) MakeVolBulk(ctx context.Context, volumes ...string) (err error) {
	return errDiskNotFound
}

func (p *unrecognizedDisk) MakeVol(ctx context.Context, volume string) (err error) {
	return errDiskNotFound
}

func (p *unrecognizedDisk) ListVols(ctx context.Context) ([]VolInfo, error) {
	return nil, errDiskNotFound
}

func (p *unrecognizedDisk) StatVol(ctx context.Context, volume string) (vol VolInfo, err error) {
	return vol, errDiskNotFound
}

func (p *unrecognizedDisk) DeleteVol(ctx context.Context, volume string, forceDelete bool) (err error) {
	return errDiskNotFound
}

func (p *unrecognizedDisk) ListDir(ctx context.Context, volume, dirPath string, count int) ([]string, error) {
	return nil, errDiskNotFound
}

func (p *unrecognizedDisk) ReadFile(ctx context.Context, volume string, path string, offset int64, buf []byte, verifier *BitrotVerifier) (n int64, err error) {
	return 0, errDiskNotFound
}

func (p *unrecognizedDisk) AppendFile(ctx context.Context, volume string, path string, buf []byte) (err error) {
	return errDiskNotFound
}

func (p *unrecognizedDisk) CreateFile(ctx context.Context, volume, path string, size int64, reader io.Reader) error {
	return errDiskNotFound
}

func (p *unrecognizedDisk) ReadFileStream(ctx context.Context, volume, path string, offset, length int64) (io.ReadCloser, error) {
	return nil, errDiskNotFound
}

func (p *unrecognizedDisk) RenameFile(ctx context.Context, srcVolume, srcPath, dstVolume, dstPath string) error {
	return errDiskNotFound
}

func (p *unrecognizedDisk) RenameData(ctx context.Context, srcVolume, srcPath string, fi FileInfo, dstVolume, dstPath string) error {
	return errDiskNotFound
}

func (p *unrecognizedDisk) CheckParts(ctx context.Context, volume string, path string, fi FileInfo) (err error) {
	return errDiskNotFound
}

func (p *unrecognizedDisk) Delete(ctx context.Context, volume string, path string, recursive bool) (err error) {
	return errDiskNotFound
}

// DeleteVersions deletes slice of versions, it can be same object
// or multiple objects.
func (p *unrecognizedDisk) DeleteVersions(ctx context.Context, volume string, versions []FileInfoVersions) (errs []error) {
	errs = make([]error, len(versions))

	for i := range errs {
		errs[i] = errDiskNotFound
	}
	return errs
}

func (p *unrecognizedDisk) VerifyFile(ctx context.Context, volume, path string, fi FileInfo) error {
	return errDiskNotFound
}

func (p *unrecognizedDisk) WriteAll(ctx context.Context, volume string, path string, b []byte) (err error) {
	return errDiskNotFound
}

func (p *unrecognizedDisk) DeleteVersion(ctx context.Context, volume, path string, fi FileInfo, forceDelMarker bool) (err error) {
	return errDiskNotFound
}

func (p *unrecognizedDisk) UpdateMetadata(ctx context.Context, volume, path string, fi FileInfo) (err error) {
	return errDiskNotFound
}

func (p *unrecognizedDisk) WriteMetadata(ctx context.Context, volume, path string, fi FileInfo) (err error) {
	return errDiskNotFound
}

func (p *unrecognizedDisk) ReadVersion(ctx context.Context, volume, path, versionID string, readData bool) (fi FileInfo, err error) {
	return fi, errDiskNotFound
}

func (p *unrecognizedDisk) ReadAll(ctx context.Context, volume string, path string) (buf []byte, err error) {
	return nil, errDiskNotFound
}

func (p *unrecognizedDisk) StatInfoFile(ctx context.Context, volume, path string, glob bool) (stat []StatInfo, err error) {
	return nil, errDiskNotFound
}
