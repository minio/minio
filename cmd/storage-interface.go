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

	"github.com/minio/madmin-go/v3"
)

// StorageAPI interface.
type StorageAPI interface {
	// Stringified version of disk.
	String() string

	// Storage operations.

	// Returns true if disk is online and its valid i.e valid format.json.
	// This has nothing to do with if the drive is hung or not responding.
	// For that individual storage API calls will fail properly. The purpose
	// of this function is to know if the "drive" has "format.json" or not
	// if it has a "format.json" then is it correct "format.json" or not.
	IsOnline() bool

	// Returns the last time this disk (re)-connected
	LastConn() time.Time

	// Indicates if disk is local or not.
	IsLocal() bool

	// Returns hostname if disk is remote.
	Hostname() string

	// Returns the entire endpoint.
	Endpoint() Endpoint

	// Close the disk, mark it purposefully closed, only implemented for remote disks.
	Close() error

	// Returns the unique 'uuid' of this disk.
	GetDiskID() (string, error)

	// Set a unique 'uuid' for this disk, only used when
	// disk is replaced and formatted.
	SetDiskID(id string)

	// Returns healing information for a newly replaced disk,
	// returns 'nil' once healing is complete or if the disk
	// has never been replaced.
	Healing() *healingTracker
	DiskInfo(ctx context.Context, opts DiskInfoOptions) (info DiskInfo, err error)
	NSScanner(ctx context.Context, cache dataUsageCache, updates chan<- dataUsageEntry, scanMode madmin.HealScanMode, shouldSleep func() bool) (dataUsageCache, error)

	// Volume operations.
	MakeVol(ctx context.Context, volume string) (err error)
	MakeVolBulk(ctx context.Context, volumes ...string) (err error)
	ListVols(ctx context.Context) (vols []VolInfo, err error)
	StatVol(ctx context.Context, volume string) (vol VolInfo, err error)
	DeleteVol(ctx context.Context, volume string, forceDelete bool) (err error)

	// WalkDir will walk a directory on disk and return a metacache stream on wr.
	WalkDir(ctx context.Context, opts WalkDirOptions, wr io.Writer) error

	// Metadata operations
	DeleteVersion(ctx context.Context, volume, path string, fi FileInfo, forceDelMarker bool, opts DeleteOptions) error
	DeleteVersions(ctx context.Context, volume string, versions []FileInfoVersions, opts DeleteOptions) []error
	DeleteBulk(ctx context.Context, volume string, paths ...string) error
	WriteMetadata(ctx context.Context, origvolume, volume, path string, fi FileInfo) error
	UpdateMetadata(ctx context.Context, volume, path string, fi FileInfo, opts UpdateMetadataOpts) error
	ReadVersion(ctx context.Context, origvolume, volume, path, versionID string, opts ReadOptions) (FileInfo, error)
	ReadXL(ctx context.Context, volume, path string, readData bool) (RawFileInfo, error)
	RenameData(ctx context.Context, srcVolume, srcPath string, fi FileInfo, dstVolume, dstPath string, opts RenameOptions) (RenameDataResp, error)

	// File operations.
	ListDir(ctx context.Context, origvolume, volume, dirPath string, count int) ([]string, error)
	ReadFile(ctx context.Context, volume string, path string, offset int64, buf []byte, verifier *BitrotVerifier) (n int64, err error)
	AppendFile(ctx context.Context, volume string, path string, buf []byte) (err error)
	CreateFile(ctx context.Context, origvolume, olume, path string, size int64, reader io.Reader) error
	ReadFileStream(ctx context.Context, volume, path string, offset, length int64) (io.ReadCloser, error)
	RenameFile(ctx context.Context, srcVolume, srcPath, dstVolume, dstPath string) error
	RenamePart(ctx context.Context, srcVolume, srcPath, dstVolume, dstPath string, meta []byte, skipParent string) error
	CheckParts(ctx context.Context, volume string, path string, fi FileInfo) (*CheckPartsResp, error)
	Delete(ctx context.Context, volume string, path string, opts DeleteOptions) (err error)
	VerifyFile(ctx context.Context, volume, path string, fi FileInfo) (*CheckPartsResp, error)
	StatInfoFile(ctx context.Context, volume, path string, glob bool) (stat []StatInfo, err error)
	ReadParts(ctx context.Context, bucket string, partMetaPaths ...string) ([]*ObjectPartInfo, error)
	ReadMultiple(ctx context.Context, req ReadMultipleReq, resp chan<- ReadMultipleResp) error
	CleanAbandonedData(ctx context.Context, volume string, path string) error

	// Write all data, syncs the data to disk.
	// Should be used for smaller payloads.
	WriteAll(ctx context.Context, volume string, path string, b []byte) (err error)

	// Read all.
	ReadAll(ctx context.Context, volume string, path string) (buf []byte, err error)
	GetDiskLoc() (poolIdx, setIdx, diskIdx int) // Retrieve location indexes.
}
