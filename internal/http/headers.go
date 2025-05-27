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

package http

// Standard S3 HTTP response constants
const (
	LastModified       = "Last-Modified"
	Date               = "Date"
	ETag               = "ETag"
	ContentType        = "Content-Type"
	ContentMD5         = "Content-Md5"
	ContentEncoding    = "Content-Encoding"
	Expires            = "Expires"
	ContentLength      = "Content-Length"
	ContentLanguage    = "Content-Language"
	ContentRange       = "Content-Range"
	Connection         = "Connection"
	AcceptRanges       = "Accept-Ranges"
	AmzBucketRegion    = "X-Amz-Bucket-Region"
	ServerInfo         = "Server"
	RetryAfter         = "Retry-After"
	Location           = "Location"
	CacheControl       = "Cache-Control"
	ContentDisposition = "Content-Disposition"
	Authorization      = "Authorization"
	Action             = "Action"
	Range              = "Range"
)

// Non standard S3 HTTP response constants
const (
	XCache       = "X-Cache"
	XCacheLookup = "X-Cache-Lookup"
)

// Standard S3 HTTP request constants
const (
	IfModifiedSince   = "If-Modified-Since"
	IfUnmodifiedSince = "If-Unmodified-Since"
	IfMatch           = "If-Match"
	IfNoneMatch       = "If-None-Match"

	// Request tags used in GetObjectAttributes
	Checksum     = "Checksum"
	StorageClass = "StorageClass"
	ObjectSize   = "ObjectSize"
	ObjectParts  = "ObjectParts"

	// S3 storage class
	AmzStorageClass = "x-amz-storage-class"

	// S3 object version ID
	AmzVersionID    = "x-amz-version-id"
	AmzDeleteMarker = "x-amz-delete-marker"

	// S3 object tagging
	AmzObjectTagging = "X-Amz-Tagging"
	AmzTagCount      = "x-amz-tagging-count"
	AmzTagDirective  = "X-Amz-Tagging-Directive"

	// S3 transition restore
	AmzRestore            = "x-amz-restore"
	AmzRestoreExpiryDays  = "X-Amz-Restore-Expiry-Days"
	AmzRestoreRequestDate = "X-Amz-Restore-Request-Date"
	AmzRestoreOutputPath  = "x-amz-restore-output-path"

	// S3 extensions
	AmzCopySourceIfModifiedSince   = "x-amz-copy-source-if-modified-since"
	AmzCopySourceIfUnmodifiedSince = "x-amz-copy-source-if-unmodified-since"

	AmzCopySourceIfNoneMatch = "x-amz-copy-source-if-none-match"
	AmzCopySourceIfMatch     = "x-amz-copy-source-if-match"

	AmzCopySource                 = "X-Amz-Copy-Source"
	AmzCopySourceVersionID        = "X-Amz-Copy-Source-Version-Id"
	AmzCopySourceRange            = "X-Amz-Copy-Source-Range"
	AmzMetadataDirective          = "X-Amz-Metadata-Directive"
	AmzObjectLockMode             = "X-Amz-Object-Lock-Mode"
	AmzObjectLockRetainUntilDate  = "X-Amz-Object-Lock-Retain-Until-Date"
	AmzObjectLockLegalHold        = "X-Amz-Object-Lock-Legal-Hold"
	AmzObjectLockBypassGovernance = "X-Amz-Bypass-Governance-Retention"
	AmzBucketReplicationStatus    = "X-Amz-Replication-Status"

	// AmzSnowballExtract will trigger unpacking of an archive content
	AmzSnowballExtract = "X-Amz-Meta-Snowball-Auto-Extract"
	// MinIOSnowballIgnoreDirs will skip creating empty directory objects.
	MinIOSnowballIgnoreDirs = "X-Amz-Meta-Minio-Snowball-Ignore-Dirs"
	// MinIOSnowballIgnoreErrors will ignore recoverable errors, typically single files failing to upload.
	// An error will be printed to console instead.
	MinIOSnowballIgnoreErrors = "X-Amz-Meta-Minio-Snowball-Ignore-Errors"
	// MinIOSnowballPrefix will apply this prefix (plus / at end) to all extracted objects
	MinIOSnowballPrefix = "X-Amz-Meta-Minio-Snowball-Prefix"

	// Object lock enabled
	AmzObjectLockEnabled = "x-amz-bucket-object-lock-enabled"

	// Multipart parts count
	AmzMpPartsCount = "x-amz-mp-parts-count"

	// Object date/time of expiration
	AmzExpiration = "x-amz-expiration"

	// Dummy putBucketACL
	AmzACL = "x-amz-acl"

	// Signature V4 related constants.
	AmzContentSha256        = "X-Amz-Content-Sha256"
	AmzDate                 = "X-Amz-Date"
	AmzAlgorithm            = "X-Amz-Algorithm"
	AmzExpires              = "X-Amz-Expires"
	AmzSignedHeaders        = "X-Amz-SignedHeaders"
	AmzSignature            = "X-Amz-Signature"
	AmzCredential           = "X-Amz-Credential"
	AmzSecurityToken        = "X-Amz-Security-Token"
	AmzDecodedContentLength = "X-Amz-Decoded-Content-Length"
	AmzTrailer              = "X-Amz-Trailer"
	AmzMaxParts             = "X-Amz-Max-Parts"
	AmzPartNumberMarker     = "X-Amz-Part-Number-Marker"

	// Constants used for GetObjectAttributes and GetObjectVersionAttributes
	AmzObjectAttributes = "X-Amz-Object-Attributes"

	AmzMetaUnencryptedContentLength = "X-Amz-Meta-X-Amz-Unencrypted-Content-Length"
	AmzMetaUnencryptedContentMD5    = "X-Amz-Meta-X-Amz-Unencrypted-Content-Md5"

	// AWS server-side encryption headers for SSE-S3, SSE-KMS and SSE-C.
	AmzServerSideEncryption                      = "X-Amz-Server-Side-Encryption"
	AmzServerSideEncryptionKmsID                 = AmzServerSideEncryption + "-Aws-Kms-Key-Id"
	AmzServerSideEncryptionKmsContext            = AmzServerSideEncryption + "-Context"
	AmzServerSideEncryptionCustomerAlgorithm     = AmzServerSideEncryption + "-Customer-Algorithm"
	AmzServerSideEncryptionCustomerKey           = AmzServerSideEncryption + "-Customer-Key"
	AmzServerSideEncryptionCustomerKeyMD5        = AmzServerSideEncryption + "-Customer-Key-Md5"
	AmzServerSideEncryptionCopyCustomerAlgorithm = "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm"
	AmzServerSideEncryptionCopyCustomerKey       = "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key"
	AmzServerSideEncryptionCopyCustomerKeyMD5    = "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key-Md5"

	AmzEncryptionAES = "AES256"
	AmzEncryptionKMS = "aws:kms"

	// Signature v2 related constants
	AmzSignatureV2 = "Signature"
	AmzAccessKeyID = "AWSAccessKeyId"

	// Response request id.
	AmzRequestID     = "x-amz-request-id"
	AmzRequestHostID = "x-amz-id-2"

	// Deployment id.
	MinioDeploymentID = "x-minio-deployment-id"

	// Peer call
	MinIOPeerCall = "x-minio-from-peer"

	// Server-Status
	MinIOServerStatus = "x-minio-server-status"

	// Content Checksums
	AmzChecksumAlgo           = "x-amz-checksum-algorithm"
	AmzChecksumCRC32          = "x-amz-checksum-crc32"
	AmzChecksumCRC32C         = "x-amz-checksum-crc32c"
	AmzChecksumSHA1           = "x-amz-checksum-sha1"
	AmzChecksumSHA256         = "x-amz-checksum-sha256"
	AmzChecksumCRC64NVME      = "x-amz-checksum-crc64nvme"
	AmzChecksumMode           = "x-amz-checksum-mode"
	AmzChecksumType           = "x-amz-checksum-type"
	AmzChecksumTypeFullObject = "FULL_OBJECT"
	AmzChecksumTypeComposite  = "COMPOSITE"

	// S3 Express API related constant reject it.
	AmzWriteOffsetBytes = "x-amz-write-offset-bytes"

	// Post Policy related
	AmzMetaUUID = "X-Amz-Meta-Uuid"
	AmzMetaName = "X-Amz-Meta-Name"

	// Delete special flag to force delete a bucket or a prefix
	MinIOForceDelete = "x-minio-force-delete"

	// Create special flag to force create a bucket
	MinIOForceCreate = "x-minio-force-create"

	// Header indicates if the mtime should be preserved by client
	MinIOSourceMTime = "x-minio-source-mtime"

	// Header indicates if the etag should be preserved by client
	MinIOSourceETag = "x-minio-source-etag"

	// Writes expected write quorum
	MinIOWriteQuorum = "x-minio-write-quorum"

	// Reads expected read quorum
	MinIOReadQuorum = "x-minio-read-quorum"

	// Indicates if we are using default storage class and there was problem loading config
	// if this header is set to "true"
	MinIOStorageClassDefaults = "x-minio-storage-class-defaults"

	// Reports number of drives currently healing
	MinIOHealingDrives = "x-minio-healing-drives"

	// Header indicates if the delete marker should be preserved by client
	MinIOSourceDeleteMarker = "x-minio-source-deletemarker"

	// Header indicates if the delete marker version needs to be purged.
	MinIOSourceDeleteMarkerDelete = "x-minio-source-deletemarker-delete"

	// Header indicates permanent delete replication status.
	MinIODeleteReplicationStatus = "X-Minio-Replication-Delete-Status"
	// Header indicates delete-marker replication status.
	MinIODeleteMarkerReplicationStatus = "X-Minio-Replication-DeleteMarker-Status"
	// Header indicates if its a GET/HEAD proxy request for active-active replication
	MinIOSourceProxyRequest = "X-Minio-Source-Proxy-Request"
	// Header indicates that this request is a replication request to create a REPLICA
	MinIOSourceReplicationRequest = "X-Minio-Source-Replication-Request"
	// Header checks replication permissions without actually completing replication
	MinIOSourceReplicationCheck = "X-Minio-Source-Replication-Check"
	// Header indicates replication reset status.
	MinIOReplicationResetStatus = "X-Minio-Replication-Reset-Status"
	// Header indicating target cluster can receive delete marker replication requests because object has been replicated
	MinIOTargetReplicationReady = "X-Minio-Replication-Ready"
	// Header asking if cluster can receive delete marker replication request now.
	MinIOCheckDMReplicationReady = "X-Minio-Check-Replication-Ready"
	// Header indiicates last tag update time on source
	MinIOSourceTaggingTimestamp = "X-Minio-Source-Replication-Tagging-Timestamp"
	// Header indiicates last rtention update time on source
	MinIOSourceObjectRetentionTimestamp = "X-Minio-Source-Replication-Retention-Timestamp"
	// Header indiicates last rtention update time on source
	MinIOSourceObjectLegalHoldTimestamp = "X-Minio-Source-Replication-LegalHold-Timestamp"
	// Header indicates a Tag operation was performed on one/more peers successfully, though the
	// current cluster does not have the object yet. This is in a site/bucket replication scenario.
	MinIOTaggingProxied = "X-Minio-Tagging-Proxied"
	// Header indicates the actual replicated object size
	// In case of SSEC objects getting replicated (multipart) actual size would be needed at target
	MinIOReplicationActualObjectSize = "X-Minio-Replication-Actual-Object-Size"

	// predicted date/time of transition
	MinIOTransition            = "X-Minio-Transition"
	MinIOLifecycleCfgUpdatedAt = "X-Minio-LifecycleConfig-UpdatedAt"
	// MinIOCompressed is returned when object is compressed
	MinIOCompressed = "X-Minio-Compressed"

	// SUBNET related
	SubnetAPIKey = "x-subnet-api-key"
)

// Common http query params S3 API
const (
	VersionID = "versionId"

	PartNumber = "partNumber"

	UploadID = "uploadId"
)

// http headers sent to webhook targets
const (
	// Reports the version of MinIO server
	MinIOVersion             = "x-minio-version"
	WebhookEventPayloadCount = "x-minio-webhook-payload-count"
)
