/*
 * MinIO Cloud Storage, (C) 2016-2020 MinIO, Inc.
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
	"time"
)

//go:generate msgp -file=$GOFILE

// DiskInfo is an extended type which returns current
// disk usage per path.
//msgp:tuple DiskInfo
// The above means that any added/deleted fields are incompatible.
type DiskInfo struct {
	Total      uint64
	Free       uint64
	Used       uint64
	UsedInodes uint64
	FSType     string
	RootDisk   bool
	Healing    bool
	Endpoint   string
	MountPath  string
	ID         string
	Error      string // carries the error over the network
}

// VolsInfo is a collection of volume(bucket) information
type VolsInfo []VolInfo

// VolInfo - represents volume stat information.
//msgp:tuple VolInfo
// The above means that any added/deleted fields are incompatible.
type VolInfo struct {
	// Name of the volume.
	Name string

	// Date and time when the volume was created.
	Created time.Time
}

// FilesInfo represent a list of files, additionally
// indicates if the list is last.
type FilesInfo struct {
	Files       []FileInfo
	IsTruncated bool
}

// FilesInfoVersions represents a list of file versions,
// additionally indicates if the list is last.
type FilesInfoVersions struct {
	FilesVersions []FileInfoVersions
	IsTruncated   bool
}

// FileInfoVersions represent a list of versions for a given file.
type FileInfoVersions struct {
	// Name of the volume.
	Volume string

	// Name of the file.
	Name string

	IsEmptyDir bool

	// Represents the latest mod time of the
	// latest version.
	LatestModTime time.Time

	Versions []FileInfo
}

// forwardPastVersion will truncate the result to only contain versions after 'v'.
// If v is empty or the version isn't found no changes will be made.
func (f *FileInfoVersions) forwardPastVersion(v string) {
	if v == "" {
		return
	}
	for i, ver := range f.Versions {
		if ver.VersionID == v {
			f.Versions = f.Versions[i+1:]
			return
		}
	}
}

// FileInfo - represents file stat information.
//msgp:tuple FileInfo
// The above means that any added/deleted fields are incompatible.
type FileInfo struct {
	// Name of the volume.
	Volume string

	// Name of the file.
	Name string

	// Version of the file.
	VersionID string

	// Indicates if the version is the latest
	IsLatest bool

	// Deleted is set when this FileInfo represents
	// a deleted marker for a versioned bucket.
	Deleted bool

	// TransitionStatus is set to Pending/Complete for transitioned
	// entries based on state of transition
	TransitionStatus string

	// DataDir of the file
	DataDir string

	// Indicates if this object is still in V1 format.
	XLV1 bool

	// Date and time when the file was last modified, if Deleted
	// is 'true' this value represents when while was deleted.
	ModTime time.Time

	// Total file size.
	Size int64

	// File mode bits.
	Mode uint32

	// File metadata
	Metadata map[string]string

	// All the parts per object.
	Parts []ObjectPartInfo

	// Erasure info for all objects.
	Erasure ErasureInfo

	// DeleteMarkerReplicationStatus is set when this FileInfo represents
	// replication on a DeleteMarker
	MarkDeleted                   bool // mark this version as deleted
	DeleteMarkerReplicationStatus string
	VersionPurgeStatus            VersionPurgeStatusType

	Data []byte // optionally carries object data
}

// VersionPurgeStatusKey denotes purge status in metadata
const VersionPurgeStatusKey = "purgestatus"

// VersionPurgeStatusType represents status of a versioned delete or permanent delete w.r.t bucket replication
type VersionPurgeStatusType string

const (
	// Pending - versioned delete replication is pending.
	Pending VersionPurgeStatusType = "PENDING"

	// Complete - versioned delete replication is now complete, erase version on disk.
	Complete VersionPurgeStatusType = "COMPLETE"

	// Failed - versioned delete replication failed.
	Failed VersionPurgeStatusType = "FAILED"
)

// Empty returns true if purge status was not set.
func (v VersionPurgeStatusType) Empty() bool {
	return string(v) == ""
}

// Pending returns true if the version is pending purge.
func (v VersionPurgeStatusType) Pending() bool {
	return v == Pending || v == Failed
}

// newFileInfo - initializes new FileInfo, allocates a fresh erasure info.
func newFileInfo(object string, dataBlocks, parityBlocks int) (fi FileInfo) {
	fi.Erasure = ErasureInfo{
		Algorithm:    erasureAlgorithm,
		DataBlocks:   dataBlocks,
		ParityBlocks: parityBlocks,
		BlockSize:    blockSizeV1,
		Distribution: hashOrder(object, dataBlocks+parityBlocks),
	}
	return fi
}
