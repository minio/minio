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
	FreeInodes uint64
	FSType     string
	RootDisk   bool
	Healing    bool
	Endpoint   string
	MountPath  string
	ID         string
	Metrics    DiskMetrics
	Error      string // carries the error over the network
}

// DiskMetrics has the information about XL Storage APIs
// the number of calls of each API and the moving average of
// the duration of each API.
type DiskMetrics struct {
	APILatencies map[string]string `json:"apiLatencies,omitempty"`
	APICalls     map[string]uint64 `json:"apiCalls,omitempty"`
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
//msgp:tuple FileInfoVersions
// The above means that any added/deleted fields are incompatible.
type FileInfoVersions struct {
	// Name of the volume.
	Volume string `msg:"v,omitempty"`

	// Name of the file.
	Name string `msg:"n,omitempty"`

	// Represents the latest mod time of the
	// latest version.
	LatestModTime time.Time `msg:"lm"`

	Versions []FileInfo `msg:"vs"`
}

// findVersionIndex will return the version index where the version
// was found. Returns -1 if not found.
func (f *FileInfoVersions) findVersionIndex(v string) int {
	if f == nil || v == "" {
		return -1
	}
	for i, ver := range f.Versions {
		if ver.VersionID == v {
			return i
		}
	}
	return -1
}

// FileInfo - represents file stat information.
//msgp:tuple FileInfo
// The above means that any added/deleted fields are incompatible.
type FileInfo struct {
	// Name of the volume.
	Volume string `msg:"v,omitempty"`

	// Name of the file.
	Name string `msg:"n,omitempty"`

	// Version of the file.
	VersionID string `msg:"vid,omitempty"`

	// Indicates if the version is the latest
	IsLatest bool `msg:"is"`

	// Deleted is set when this FileInfo represents
	// a deleted marker for a versioned bucket.
	Deleted bool `msg:"del"`

	// TransitionStatus is set to Pending/Complete for transitioned
	// entries based on state of transition
	TransitionStatus string `msg:"ts"`
	// TransitionedObjName is the object name on the remote tier corresponding
	// to object (version) on the source tier.
	TransitionedObjName string `msg:"to"`
	// TransitionTier is the storage class label assigned to remote tier.
	TransitionTier string `msg:"tt"`
	// TransitionVersionID stores a version ID of the object associate
	// with the remote tier.
	TransitionVersionID string `msg:"tv"`
	// ExpireRestored indicates that the restored object is to be expired.
	ExpireRestored bool `msg:"exp"`

	// DataDir of the file
	DataDir string `msg:"dd"`

	// Indicates if this object is still in V1 format.
	XLV1 bool `msg:"v1"`

	// Date and time when the file was last modified, if Deleted
	// is 'true' this value represents when while was deleted.
	ModTime time.Time `msg:"mt"`

	// Total file size.
	Size int64 `msg:"sz"`

	// File mode bits.
	Mode uint32 `msg:"m"`

	// File metadata
	Metadata map[string]string `msg:"meta"`

	// All the parts per object.
	Parts []ObjectPartInfo `msg:"parts"`

	// Erasure info for all objects.
	Erasure ErasureInfo `msg:"ei"`

	MarkDeleted      bool             `msg:"md"` // mark this version as deleted
	ReplicationState ReplicationState `msg:"rs"` // Internal replication state to be passed back in ObjectInfo

	Data []byte `msg:"d,allownil"` // optionally carries object data

	NumVersions      int       `msg:"nv"`
	SuccessorModTime time.Time `msg:"smt"`

	Fresh bool `msg:"fr"` // indicates this is a first time call to write FileInfo.

	// Position of this version or object in a multi-object delete call,
	// no other caller must set this value other than multi-object delete call.
	// usage in other calls in undefined please avoid.
	Idx int `msg:"i"`
}

// InlineData returns true if object contents are inlined alongside its metadata.
func (fi FileInfo) InlineData() bool {
	_, ok := fi.Metadata[ReservedMetadataPrefixLower+"inline-data"]
	return ok
}

// SetInlineData marks object (version) as inline.
func (fi *FileInfo) SetInlineData() {
	if fi.Metadata == nil {
		fi.Metadata = make(map[string]string, 1)
	}
	fi.Metadata[ReservedMetadataPrefixLower+"inline-data"] = "true"
}

// VersionPurgeStatusKey denotes purge status in metadata
const VersionPurgeStatusKey = "purgestatus"

// newFileInfo - initializes new FileInfo, allocates a fresh erasure info.
func newFileInfo(object string, dataBlocks, parityBlocks int) (fi FileInfo) {
	fi.Erasure = ErasureInfo{
		Algorithm:    erasureAlgorithm,
		DataBlocks:   dataBlocks,
		ParityBlocks: parityBlocks,
		BlockSize:    blockSizeV2,
		Distribution: hashOrder(object, dataBlocks+parityBlocks),
	}
	return fi
}
