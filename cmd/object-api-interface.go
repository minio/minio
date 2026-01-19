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
	"net/http"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7/pkg/encrypt"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/minio/minio/internal/hash"

	"github.com/minio/minio/internal/bucket/replication"
	xioutil "github.com/minio/minio/internal/ioutil"
)

//go:generate msgp -file $GOFILE -io=false -tests=false -unexported=false

//msgp:ignore ObjectOptions TransitionOptions DeleteBucketOptions

// CheckPreconditionFn returns true if precondition check failed.
type CheckPreconditionFn func(o ObjectInfo) bool

// EvalMetadataFn validates input objInfo and GetObjectInfo error and returns an updated metadata and replication decision if any
type EvalMetadataFn func(o *ObjectInfo, gerr error) (ReplicateDecision, error)

// EvalRetentionBypassFn validates input objInfo and GetObjectInfo error and returns an error if retention bypass is not allowed.
type EvalRetentionBypassFn func(o ObjectInfo, gerr error) error

// GetObjectInfoFn is the signature of GetObjectInfo function.
type GetObjectInfoFn func(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error)

// WalkVersionsSortOrder represents the sort order in which versions of an
// object should be returned by ObjectLayer.Walk method
type WalkVersionsSortOrder uint8

const (
	// WalkVersionsSortAsc - Sort in ascending order of ModTime
	WalkVersionsSortAsc WalkVersionsSortOrder = iota
	// WalkVersionsSortDesc - Sort in descending order of ModTime
	WalkVersionsSortDesc
)

// ObjectOptions represents object options for ObjectLayer object operations
type ObjectOptions struct {
	ServerSideEncryption encrypt.ServerSide
	VersionSuspended     bool      // indicates if the bucket was previously versioned but is currently suspended.
	Versioned            bool      // indicates if the bucket is versioned
	VersionID            string    // Specifies the versionID which needs to be overwritten or read
	MTime                time.Time // Is only set in POST/PUT operations
	Expires              time.Time // Is only used in POST/PUT operations

	DeleteMarker            bool // Is only set in DELETE operations for delete marker replication
	CheckDMReplicationReady bool // Is delete marker ready to be replicated - set only during HEAD
	Tagging                 bool // Is only in GET/HEAD operations to return tagging metadata along with regular metadata and body.

	UserDefined         map[string]string   // only set in case of POST/PUT operations
	ObjectAttributes    map[string]struct{} // Attribute tags defined by the users for the GetObjectAttributes request
	MaxParts            int                 // used in GetObjectAttributes. Signals how many parts we should return
	PartNumberMarker    int                 // used in GetObjectAttributes. Signals the part number after which results should be returned
	PartNumber          int                 // only useful in case of GetObject/HeadObject
	CheckPrecondFn      CheckPreconditionFn // only set during GetObject/HeadObject/CopyObjectPart preconditional valuation
	EvalMetadataFn      EvalMetadataFn      // only set for retention settings, meant to be used only when updating metadata in-place.
	DeleteReplication   ReplicationState    // Represents internal replication state needed for Delete replication
	Transition          TransitionOptions
	Expiration          ExpirationOptions
	LifecycleAuditEvent lcAuditEvent

	WantChecksum *hash.Checksum // x-amz-checksum-XXX checksum sent to PutObject/ CompleteMultipartUpload.

	WantServerSideChecksumType hash.ChecksumType // if set, we compute a server-side checksum of this type

	NoDecryption                        bool      // indicates if the stream must be decrypted.
	PreserveETag                        string    // preserves this etag during a PUT call.
	NoLock                              bool      // indicates to lower layers if the caller is expecting to hold locks.
	HasIfMatch                          bool      // indicates if the request has If-Match header
	ProxyRequest                        bool      // only set for GET/HEAD in active-active replication scenario
	ProxyHeaderSet                      bool      // only set for GET/HEAD in active-active replication scenario
	ReplicationRequest                  bool      // true only if replication request
	ReplicationSourceTaggingTimestamp   time.Time // set if MinIOSourceTaggingTimestamp received
	ReplicationSourceLegalholdTimestamp time.Time // set if MinIOSourceObjectLegalholdTimestamp received
	ReplicationSourceRetentionTimestamp time.Time // set if MinIOSourceObjectRetentionTimestamp received
	DeletePrefix                        bool      // set true to enforce a prefix deletion, only application for DeleteObject API,
	DeletePrefixObject                  bool      // set true when object's erasure set is resolvable by object name (using getHashedSetIndex)

	Speedtest bool // object call specifically meant for SpeedTest code, set to 'true' when invoked by SpeedtestHandler.

	// Use the maximum parity (N/2), used when saving server configuration files
	MaxParity bool

	// Provides a per object encryption function, allowing metadata encryption.
	EncryptFn objectMetaEncryptFn

	// SkipDecommissioned set to 'true' if the call requires skipping the pool being decommissioned.
	// mainly set for certain WRITE operations.
	SkipDecommissioned bool
	// SkipRebalancing should be set to 'true' if the call should skip pools
	// participating in a rebalance operation. Typically set for 'write' operations.
	SkipRebalancing bool

	SrcPoolIdx int // set by PutObject/CompleteMultipart operations due to rebalance; used to prevent rebalance src, dst pools to be the same

	DataMovement bool // indicates an going decommisionning or rebalacing

	PrefixEnabledFn func(prefix string) bool // function which returns true if versioning is enabled on prefix

	// IndexCB will return any index created but the compression.
	// Object must have been read at this point.
	IndexCB func() []byte

	// InclFreeVersions indicates that free versions need to be included
	// when looking up a version by fi.VersionID
	InclFreeVersions bool
	// SkipFreeVersion skips adding a free version when a tiered version is
	// being 'replaced'
	// Note: Used only when a tiered object is being expired.
	SkipFreeVersion bool

	MetadataChg           bool                  // is true if it is a metadata update operation.
	EvalRetentionBypassFn EvalRetentionBypassFn // only set for enforcing retention bypass on DeleteObject.

	FastGetObjInfo bool // Only for S3 Head/Get Object calls for now
	NoAuditLog     bool // Only set for decom, rebalance, to avoid double audits.
}

// WalkOptions provides filtering, marker and other Walk() specific options.
type WalkOptions struct {
	Filter       func(info FileInfo) bool // return WalkFilter returns 'true/false'
	Marker       string                   // set to skip until this object
	LatestOnly   bool                     // returns only latest versions for all matching objects
	AskDisks     string                   // dictates how many disks are being listed
	VersionsSort WalkVersionsSortOrder    // sort order for versions of the same object; default: Ascending order in ModTime
	Limit        int                      // maximum number of items, 0 means no limit
}

// ExpirationOptions represents object options for object expiration at objectLayer.
type ExpirationOptions struct {
	Expire bool
}

// TransitionOptions represents object options for transition ObjectLayer operation
type TransitionOptions struct {
	Status         string
	Tier           string
	ETag           string
	RestoreRequest *RestoreObjectRequest
	RestoreExpiry  time.Time
	ExpireRestored bool
}

// MakeBucketOptions represents bucket options for ObjectLayer bucket operations
type MakeBucketOptions struct {
	LockEnabled       bool
	VersioningEnabled bool
	ForceCreate       bool      // Create buckets even if they are already created.
	CreatedAt         time.Time // only for site replication
	NoLock            bool      // does not lock the make bucket call if set to 'true'
}

// DeleteBucketOptions provides options for DeleteBucket calls.
type DeleteBucketOptions struct {
	NoLock     bool             // does not lock the delete bucket call if set to 'true'
	NoRecreate bool             // do not recreate bucket on delete failures
	Force      bool             // Force deletion
	SRDeleteOp SRBucketDeleteOp // only when site replication is enabled
}

// BucketOptions provides options for ListBuckets and GetBucketInfo call.
type BucketOptions struct {
	Deleted    bool // true only when site replication is enabled
	Cached     bool // true only when we are requesting a cached response instead of hitting the disk for example ListBuckets() call.
	NoMetadata bool
}

// SetReplicaStatus sets replica status and timestamp for delete operations in ObjectOptions
func (o *ObjectOptions) SetReplicaStatus(st replication.StatusType) {
	o.DeleteReplication.ReplicaStatus = st
	o.DeleteReplication.ReplicaTimeStamp = UTCNow()
}

// DeleteMarkerReplicationStatus - returns replication status of delete marker from DeleteReplication state in ObjectOptions
func (o *ObjectOptions) DeleteMarkerReplicationStatus() replication.StatusType {
	return o.DeleteReplication.CompositeReplicationStatus()
}

// VersionPurgeStatus - returns version purge status from DeleteReplication state in ObjectOptions
func (o *ObjectOptions) VersionPurgeStatus() VersionPurgeStatusType {
	return o.DeleteReplication.CompositeVersionPurgeStatus()
}

// SetDeleteReplicationState sets the delete replication options.
func (o *ObjectOptions) SetDeleteReplicationState(dsc ReplicateDecision, vID string) {
	o.DeleteReplication = ReplicationState{
		ReplicateDecisionStr: dsc.String(),
	}
	switch o.VersionID {
	case "":
		o.DeleteReplication.ReplicationStatusInternal = dsc.PendingStatus()
		o.DeleteReplication.Targets = replicationStatusesMap(o.DeleteReplication.ReplicationStatusInternal)
	default:
		o.DeleteReplication.VersionPurgeStatusInternal = dsc.PendingStatus()
		o.DeleteReplication.PurgeTargets = versionPurgeStatusesMap(o.DeleteReplication.VersionPurgeStatusInternal)
	}
}

// PutReplicationState gets ReplicationState for PUT operation from ObjectOptions
func (o *ObjectOptions) PutReplicationState() (r ReplicationState) {
	rstatus, ok := o.UserDefined[ReservedMetadataPrefixLower+ReplicationStatus]
	if !ok {
		return r
	}
	r.ReplicationStatusInternal = rstatus
	r.Targets = replicationStatusesMap(rstatus)
	return r
}

// SetEvalMetadataFn sets the metadata evaluation function
func (o *ObjectOptions) SetEvalMetadataFn(f EvalMetadataFn) {
	o.EvalMetadataFn = f
}

// SetEvalRetentionBypassFn sets the retention bypass function
func (o *ObjectOptions) SetEvalRetentionBypassFn(f EvalRetentionBypassFn) {
	o.EvalRetentionBypassFn = f
}

// ObjectLayer implements primitives for object API layer.
type ObjectLayer interface {
	// Locking operations on object.
	NewNSLock(bucket string, objects ...string) RWLocker

	// Storage operations.
	Shutdown(context.Context) error
	NSScanner(ctx context.Context, updates chan<- DataUsageInfo, wantCycle uint32, scanMode madmin.HealScanMode) error
	BackendInfo() madmin.BackendInfo
	Legacy() bool // Only returns true for deployments which use CRCMOD as its object distribution algorithm.
	StorageInfo(ctx context.Context, metrics bool) StorageInfo
	LocalStorageInfo(ctx context.Context, metrics bool) StorageInfo

	// Bucket operations.
	MakeBucket(ctx context.Context, bucket string, opts MakeBucketOptions) error
	GetBucketInfo(ctx context.Context, bucket string, opts BucketOptions) (bucketInfo BucketInfo, err error)
	ListBuckets(ctx context.Context, opts BucketOptions) (buckets []BucketInfo, err error)
	DeleteBucket(ctx context.Context, bucket string, opts DeleteBucketOptions) error
	ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (result ListObjectsInfo, err error)
	ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result ListObjectsV2Info, err error)
	ListObjectVersions(ctx context.Context, bucket, prefix, marker, versionMarker, delimiter string, maxKeys int) (result ListObjectVersionsInfo, err error)
	// Walk lists all objects including versions, delete markers.
	Walk(ctx context.Context, bucket, prefix string, results chan<- itemOrErr[ObjectInfo], opts WalkOptions) error

	// Object operations.

	// GetObjectNInfo returns a GetObjectReader that satisfies the
	// ReadCloser interface. The Close method runs any cleanup
	// functions, so it must always be called after reading till EOF
	//
	// IMPORTANTLY, when implementations return err != nil, this
	// function MUST NOT return a non-nil ReadCloser.
	GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, opts ObjectOptions) (reader *GetObjectReader, err error)
	GetObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (objInfo ObjectInfo, err error)
	PutObject(ctx context.Context, bucket, object string, data *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error)
	CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (objInfo ObjectInfo, err error)
	DeleteObject(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error)
	DeleteObjects(ctx context.Context, bucket string, objects []ObjectToDelete, opts ObjectOptions) ([]DeletedObject, []error)
	TransitionObject(ctx context.Context, bucket, object string, opts ObjectOptions) error
	RestoreTransitionedObject(ctx context.Context, bucket, object string, opts ObjectOptions) error

	// Multipart operations.
	ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result ListMultipartsInfo, err error)
	NewMultipartUpload(ctx context.Context, bucket, object string, opts ObjectOptions) (result *NewMultipartUploadResult, err error)
	CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int,
		startOffset int64, length int64, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (info PartInfo, err error)
	PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *PutObjReader, opts ObjectOptions) (info PartInfo, err error)
	GetMultipartInfo(ctx context.Context, bucket, object, uploadID string, opts ObjectOptions) (info MultipartInfo, err error)
	ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int, opts ObjectOptions) (result ListPartsInfo, err error)
	AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string, opts ObjectOptions) error
	CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []CompletePart, opts ObjectOptions) (objInfo ObjectInfo, err error)

	GetDisks(poolIdx, setIdx int) ([]StorageAPI, error) // return the disks belonging to pool and set.
	SetDriveCounts() []int                              // list of erasure stripe size for each pool in order.

	// Healing operations.
	HealFormat(ctx context.Context, dryRun bool) (madmin.HealResultItem, error)
	HealBucket(ctx context.Context, bucket string, opts madmin.HealOpts) (madmin.HealResultItem, error)
	HealObject(ctx context.Context, bucket, object, versionID string, opts madmin.HealOpts) (madmin.HealResultItem, error)
	HealObjects(ctx context.Context, bucket, prefix string, opts madmin.HealOpts, fn HealObjectFn) error
	CheckAbandonedParts(ctx context.Context, bucket, object string, opts madmin.HealOpts) error

	// Returns health of the backend
	Health(ctx context.Context, opts HealthOptions) HealthResult

	// Metadata operations
	PutObjectMetadata(context.Context, string, string, ObjectOptions) (ObjectInfo, error)
	DecomTieredObject(context.Context, string, string, FileInfo, ObjectOptions) error

	// ObjectTagging operations
	PutObjectTags(context.Context, string, string, string, ObjectOptions) (ObjectInfo, error)
	GetObjectTags(context.Context, string, string, ObjectOptions) (*tags.Tags, error)
	DeleteObjectTags(context.Context, string, string, ObjectOptions) (ObjectInfo, error)
}

// GetObject - TODO(aead): This function just acts as an adapter for GetObject tests and benchmarks
// since the GetObject method of the ObjectLayer interface has been removed. Once, the
// tests are adjusted to use GetObjectNInfo this function can be removed.
func GetObject(ctx context.Context, api ObjectLayer, bucket, object string, startOffset int64, length int64, writer io.Writer, etag string, opts ObjectOptions) (err error) {
	var header http.Header
	if etag != "" {
		header.Set("ETag", etag)
	}
	Range := &HTTPRangeSpec{Start: startOffset, End: startOffset + length}

	reader, err := api.GetObjectNInfo(ctx, bucket, object, Range, header, opts)
	if err != nil {
		return err
	}
	defer reader.Close()

	_, err = xioutil.Copy(writer, reader)
	return err
}
