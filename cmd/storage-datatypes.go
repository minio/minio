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

	"github.com/minio/minio/internal/crypto"
	"github.com/minio/minio/internal/grid"
	xioutil "github.com/minio/minio/internal/ioutil"
)

//msgp:clearomitted

//go:generate msgp -file=$GOFILE

// DeleteOptions represents the disk level delete options available for the APIs
type DeleteOptions struct {
	BaseOptions
	Recursive bool `msg:"r"`
	Immediate bool `msg:"i"`
	UndoWrite bool `msg:"u"`
	// OldDataDir of the previous object
	OldDataDir string `msg:"o,omitempty"` // old data dir used only when to revert a rename()
}

// BaseOptions represents common options for all Storage API calls
type BaseOptions struct{}

// RenameOptions represents rename API options, currently its same as BaseOptions
type RenameOptions struct {
	BaseOptions
}

// DiskInfoOptions options for requesting custom results.
type DiskInfoOptions struct {
	DiskID  string `msg:"id"`
	Metrics bool   `msg:"m"`
	NoOp    bool   `msg:"np"`
}

// DiskInfo is an extended type which returns current
// disk usage per path.
// The above means that any added/deleted fields are incompatible.
//
// The above means that any added/deleted fields are incompatible.
//
//msgp:tuple DiskInfo
type DiskInfo struct {
	Total      uint64
	Free       uint64
	Used       uint64
	UsedInodes uint64
	FreeInodes uint64
	Major      uint32
	Minor      uint32
	NRRequests uint64
	FSType     string
	RootDisk   bool
	Healing    bool
	Scanning   bool
	Endpoint   string
	MountPath  string
	ID         string
	Rotational bool
	Metrics    DiskMetrics
	Error      string // carries the error over the network
}

// DiskMetrics has the information about XL Storage APIs
// the number of calls of each API and the moving average of
// the duration of each API.
type DiskMetrics struct {
	LastMinute              map[string]AccElem `json:"apiLatencies,omitempty"`
	APICalls                map[string]uint64  `json:"apiCalls,omitempty"`
	TotalWaiting            uint32             `json:"totalWaiting,omitempty"`
	TotalErrorsAvailability uint64             `json:"totalErrsAvailability"`
	TotalErrorsTimeout      uint64             `json:"totalErrsTimeout"`
	TotalWrites             uint64             `json:"totalWrites"`
	TotalDeletes            uint64             `json:"totalDeletes"`
}

// VolsInfo is a collection of volume(bucket) information
type VolsInfo []VolInfo

// VolInfo - represents volume stat information.
// The above means that any added/deleted fields are incompatible.
//
//msgp:tuple VolInfo
type VolInfo struct {
	// Name of the volume.
	Name string

	// Date and time when the volume was created.
	Created time.Time

	// total VolInfo counts
	count int

	// Date and time when the volume was deleted, if Deleted
	Deleted time.Time
}

// FilesInfo represent a list of files, additionally
// indicates if the list is last.
//
//msgp:tuple FileInfo
type FilesInfo struct {
	Files       []FileInfo
	IsTruncated bool
}

// Size returns size of all versions for the object 'Name'
func (f FileInfoVersions) Size() (size int64) {
	for _, v := range f.Versions {
		size += v.Size
	}
	return size
}

// FileInfoVersions represent a list of versions for a given file.
// The above means that any added/deleted fields are incompatible.
//
// The above means that any added/deleted fields are incompatible.
//
//msgp:tuple FileInfoVersions
type FileInfoVersions struct {
	// Name of the volume.
	Volume string `msg:"v,omitempty"`

	// Name of the file.
	Name string `msg:"n,omitempty"`

	// Represents the latest mod time of the
	// latest version.
	LatestModTime time.Time `msg:"lm"`

	Versions     []FileInfo `msg:"vs"`
	FreeVersions []FileInfo `msg:"fvs"`
}

// findVersionIndex will return the version index where the version
// was found. Returns -1 if not found.
func (f *FileInfoVersions) findVersionIndex(v string) int {
	if f == nil || v == "" {
		return -1
	}
	if v == nullVersionID {
		for i, ver := range f.Versions {
			if ver.VersionID == "" {
				return i
			}
		}
		return -1
	}

	for i, ver := range f.Versions {
		if ver.VersionID == v {
			return i
		}
	}
	return -1
}

// RawFileInfo - represents raw file stat information as byte array.
// The above means that any added/deleted fields are incompatible.
// Make sure to bump the internode version at storage-rest-common.go
type RawFileInfo struct {
	// Content of entire xl.meta (may contain data depending on what was requested by the caller.
	Buf []byte `msg:"b,allownil"`
}

// FileInfo - represents file stat information.
// The above means that any added/deleted fields are incompatible.
// Make sure to bump the internode version at storage-rest-common.go
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

	// WrittenByVersion is the unix time stamp of the MinIO
	// version that created this version of the object.
	WrittenByVersion uint64 `msg:"wv"`

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

	// Combined checksum when object was uploaded.
	Checksum []byte `msg:"cs,allownil"`

	// Versioned - indicates if this file is versioned or not.
	Versioned bool `msg:"vs"`
}

func (fi FileInfo) shardSize() int64 {
	return ceilFrac(fi.Erasure.BlockSize, int64(fi.Erasure.DataBlocks))
}

// ShardFileSize - returns final erasure size from original size.
func (fi FileInfo) ShardFileSize(totalLength int64) int64 {
	if totalLength == 0 {
		return 0
	}
	if totalLength == -1 {
		return -1
	}
	numShards := totalLength / fi.Erasure.BlockSize
	lastBlockSize := totalLength % fi.Erasure.BlockSize
	lastShardSize := ceilFrac(lastBlockSize, int64(fi.Erasure.DataBlocks))
	return numShards*fi.shardSize() + lastShardSize
}

// ShallowCopy - copies minimal information for READ MRF checks.
func (fi FileInfo) ShallowCopy() (n FileInfo) {
	n.Volume = fi.Volume
	n.Name = fi.Name
	n.VersionID = fi.VersionID
	n.Deleted = fi.Deleted
	n.Erasure = fi.Erasure
	return n
}

// WriteQuorum returns expected write quorum for this FileInfo
func (fi FileInfo) WriteQuorum(dquorum int) int {
	if fi.Deleted {
		return dquorum
	}
	quorum := fi.Erasure.DataBlocks
	if fi.Erasure.DataBlocks == fi.Erasure.ParityBlocks {
		quorum++
	}
	return quorum
}

// ReadQuorum returns expected read quorum for this FileInfo
func (fi FileInfo) ReadQuorum(dquorum int) int {
	if fi.Deleted {
		return dquorum
	}
	return fi.Erasure.DataBlocks
}

// Equals checks if fi(FileInfo) matches ofi(FileInfo)
func (fi FileInfo) Equals(ofi FileInfo) (ok bool) {
	typ1, ok1 := crypto.IsEncrypted(fi.Metadata)
	typ2, ok2 := crypto.IsEncrypted(ofi.Metadata)
	if ok1 != ok2 {
		return false
	}
	if typ1 != typ2 {
		return false
	}
	if fi.IsCompressed() != ofi.IsCompressed() {
		return false
	}
	if !fi.TransitionInfoEquals(ofi) {
		return false
	}
	if !fi.ModTime.Equal(ofi.ModTime) {
		return false
	}
	return fi.Erasure.Equal(ofi.Erasure)
}

// GetDataDir returns an expected dataDir given FileInfo
//   - deleteMarker returns "delete-marker"
//   - returns "legacy" if FileInfo is XLV1 and DataDir is
//     empty, returns DataDir otherwise
//   - returns "dataDir"
func (fi FileInfo) GetDataDir() string {
	if fi.Deleted {
		return "delete-marker"
	}
	if fi.XLV1 && fi.DataDir == "" {
		return "legacy"
	}
	return fi.DataDir
}

// IsCompressed returns true if the object is marked as compressed.
func (fi FileInfo) IsCompressed() bool {
	_, ok := fi.Metadata[ReservedMetadataPrefix+"compression"]
	return ok
}

// InlineData returns true if object contents are inlined alongside its metadata.
func (fi FileInfo) InlineData() bool {
	_, ok := fi.Metadata[ReservedMetadataPrefixLower+"inline-data"]
	// Earlier MinIO versions didn't reset "x-minio-internal-inline-data"
	// from fi.Metadata when the object was tiered. So, tiered objects
	// would return true for InlineData() in these versions even though the
	// object isn't inlined in xl.meta
	return ok && !fi.IsRemote()
}

// SetInlineData marks object (version) as inline.
func (fi *FileInfo) SetInlineData() {
	if fi.Metadata == nil {
		fi.Metadata = make(map[string]string, 1)
	}
	fi.Metadata[ReservedMetadataPrefixLower+"inline-data"] = "true"
}

// VersionPurgeStatusKey denotes purge status in metadata
const (
	VersionPurgeStatusKey = ReservedMetadataPrefixLower + "purgestatus"
)

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

// ReadMultipleReq contains information of multiple files to read from disk.
type ReadMultipleReq struct {
	Bucket       string   `msg:"bk"`           // Bucket. Can be empty if multiple buckets.
	Prefix       string   `msg:"pr,omitempty"` // Shared prefix of all files. Can be empty. Will be joined to filename without modification.
	Files        []string `msg:"fl"`           // Individual files to read.
	MaxSize      int64    `msg:"ms"`           // Return error if size is exceed.
	MetadataOnly bool     `msg:"mo"`           // Read as XL meta and truncate data.
	AbortOn404   bool     `msg:"ab"`           // Stop reading after first file not found.
	MaxResults   int      `msg:"mr"`           // Stop after this many successful results. <= 0 means all.
}

// ReadMultipleResp contains a single response from a ReadMultipleReq.
type ReadMultipleResp struct {
	Bucket  string    `msg:"bk"`           // Bucket as given by request.
	Prefix  string    `msg:"pr,omitempty"` // Prefix as given by request.
	File    string    `msg:"fl"`           // File name as given in request.
	Exists  bool      `msg:"ex"`           // Returns whether the file existed on disk.
	Error   string    `msg:"er,omitempty"` // Returns any error when reading.
	Data    []byte    `msg:"d"`            // Contains all data of file.
	Modtime time.Time `msg:"m"`            // Modtime of file on disk.
}

// DeleteVersionHandlerParams are parameters for DeleteVersionHandler
type DeleteVersionHandlerParams struct {
	DiskID         string        `msg:"id"`
	Volume         string        `msg:"v"`
	FilePath       string        `msg:"fp"`
	ForceDelMarker bool          `msg:"fdm"`
	Opts           DeleteOptions `msg:"do"`
	FI             FileInfo      `msg:"fi"`
}

// MetadataHandlerParams is request info for UpdateMetadataHandle and WriteMetadataHandler.
type MetadataHandlerParams struct {
	DiskID     string             `msg:"id"`
	Volume     string             `msg:"v"`
	OrigVolume string             `msg:"ov"`
	FilePath   string             `msg:"fp"`
	UpdateOpts UpdateMetadataOpts `msg:"uo"`
	FI         FileInfo           `msg:"fi"`
}

// UpdateMetadataOpts provides an optional input to indicate if xl.meta updates need to be fully synced to disk.
type UpdateMetadataOpts struct {
	NoPersistence bool `msg:"np"`
}

// CheckPartsHandlerParams are parameters for CheckPartsHandler
type CheckPartsHandlerParams struct {
	DiskID   string   `msg:"id"`
	Volume   string   `msg:"v"`
	FilePath string   `msg:"fp"`
	FI       FileInfo `msg:"fi"`
}

// DeleteFileHandlerParams are parameters for DeleteFileHandler
type DeleteFileHandlerParams struct {
	DiskID   string        `msg:"id"`
	Volume   string        `msg:"v"`
	FilePath string        `msg:"fp"`
	Opts     DeleteOptions `msg:"do"`
}

// RenameDataHandlerParams are parameters for RenameDataHandler.
type RenameDataHandlerParams struct {
	DiskID    string        `msg:"id"`
	SrcVolume string        `msg:"sv"`
	SrcPath   string        `msg:"sp"`
	DstVolume string        `msg:"dv"`
	DstPath   string        `msg:"dp"`
	FI        FileInfo      `msg:"fi"`
	Opts      RenameOptions `msg:"ro"`
}

// RenameDataInlineHandlerParams are parameters for RenameDataHandler with a buffer for inline data.
type RenameDataInlineHandlerParams struct {
	RenameDataHandlerParams `msg:"p"`
}

func newRenameDataInlineHandlerParams() *RenameDataInlineHandlerParams {
	buf := grid.GetByteBufferCap(32 + 16<<10)
	return &RenameDataInlineHandlerParams{RenameDataHandlerParams{FI: FileInfo{Data: buf[:0]}}}
}

// Recycle will reuse the memory allocated for the FileInfo data.
func (r *RenameDataInlineHandlerParams) Recycle() {
	if r == nil {
		return
	}
	if cap(r.FI.Data) >= xioutil.SmallBlock {
		grid.PutByteBuffer(r.FI.Data)
		r.FI.Data = nil
	}
}

// RenameFileHandlerParams are parameters for RenameFileHandler.
type RenameFileHandlerParams struct {
	DiskID      string `msg:"id"`
	SrcVolume   string `msg:"sv"`
	SrcFilePath string `msg:"sp"`
	DstVolume   string `msg:"dv"`
	DstFilePath string `msg:"dp"`
}

// RenamePartHandlerParams are parameters for RenamePartHandler.
type RenamePartHandlerParams struct {
	DiskID      string `msg:"id"`
	SrcVolume   string `msg:"sv"`
	SrcFilePath string `msg:"sp"`
	DstVolume   string `msg:"dv"`
	DstFilePath string `msg:"dp"`
	Meta        []byte `msg:"m"`
	SkipParent  string `msg:"kp"`
}

// ReadAllHandlerParams are parameters for ReadAllHandler.
type ReadAllHandlerParams struct {
	DiskID   string `msg:"id"`
	Volume   string `msg:"v"`
	FilePath string `msg:"fp"`
}

// WriteAllHandlerParams are parameters for WriteAllHandler.
type WriteAllHandlerParams struct {
	DiskID   string `msg:"id"`
	Volume   string `msg:"v"`
	FilePath string `msg:"fp"`
	Buf      []byte `msg:"b"`
}

// RenameDataResp - RenameData()'s response.
// Provides information about the final state of Rename()
//   - on xl.meta (array of versions) on disk to check for version disparity
//   - on rewrite dataDir on disk that must be additionally purged
//     only after as a 2-phase call, allowing the older dataDir to
//     hang-around in-case we need some form of recovery.
type RenameDataResp struct {
	Sign       []byte `msg:"s"`
	OldDataDir string `msg:"od"` // contains '<uuid>', it is designed to be passed as value to Delete(bucket, pathJoin(object, dataDir))
}

const (
	checkPartUnknown int = iota

	// Changing the order can cause a data loss
	// when running two nodes with incompatible versions
	checkPartSuccess
	checkPartDiskNotFound
	checkPartVolumeNotFound
	checkPartFileNotFound
	checkPartFileCorrupt
)

// CheckPartsResp is a response of the storage CheckParts and VerifyFile APIs
type CheckPartsResp struct {
	Results []int `msg:"r"`
}

// LocalDiskIDs - GetLocalIDs response.
type LocalDiskIDs struct {
	IDs []string `msg:"i"`
}

// ListDirResult - ListDir()'s response.
type ListDirResult struct {
	Entries []string `msg:"e"`
}

// ReadPartsReq - send multiple part paths to read from
type ReadPartsReq struct {
	Paths []string `msg:"p"`
}

// ReadPartsResp - is the response for ReadPartsReq
type ReadPartsResp struct {
	Infos []*ObjectPartInfo `msg:"is"`
}

// DeleteBulkReq - send multiple paths in same delete request.
type DeleteBulkReq struct {
	Paths []string `msg:"p"`
}

// DeleteVersionsErrsResp - collection of delete errors
// for bulk version deletes
type DeleteVersionsErrsResp struct {
	Errs []string `msg:"e"`
}
