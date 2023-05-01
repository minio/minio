// Copyright (c) 2015-2023 MinIO, Inc.
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

// Object Lambda headers
const (
	AmzRequestRoute = "x-amz-request-route"
	AmzRequestToken = "x-amz-request-token"

	AmzFwdStatus                   = "x-amz-fwd-status"
	AmzFwdErrorCode                = "x-amz-fwd-error-code"
	AmzFwdErrorMessage             = "x-amz-fwd-error-message"
	AmzFwdHeaderAcceptRanges       = "x-amz-fwd-header-accept-ranges"
	AmzFwdHeaderCacheControl       = "x-amz-fwd-header-Cache-Control"
	AmzFwdHeaderContentDisposition = "x-amz-fwd-header-Content-Disposition"
	AmzFwdHeaderContentEncoding    = "x-amz-fwd-header-Content-Encoding"
	AmzFwdHeaderContentLanguage    = "x-amz-fwd-header-Content-Language"
	AmzFwdHeaderContentRange       = "x-amz-fwd-header-Content-Range"
	AmzFwdHeaderContentType        = "x-amz-fwd-header-Content-Type"
	AmzFwdHeaderChecksumCrc32      = "x-amz-fwd-header-x-amz-checksum-crc32"
	AmzFwdHeaderChecksumCrc32c     = "x-amz-fwd-header-x-amz-checksum-crc32c"
	AmzFwdHeaderChecksumSha1       = "x-amz-fwd-header-x-amz-checksum-sha1"
	AmzFwdHeaderChecksumSha256     = "x-amz-fwd-header-x-amz-checksum-sha256"
	AmzFwdHeaderDeleteMarker       = "x-amz-fwd-header-x-amz-delete-marker"
	AmzFwdHeaderETag               = "x-amz-fwd-header-ETag"
	AmzFwdHeaderExpires            = "x-amz-fwd-header-Expires"
	AmzFwdHeaderExpiration         = "x-amz-fwd-header-x-amz-expiration"
	AmzFwdHeaderLastModified       = "x-amz-fwd-header-Last-Modified"

	AmzFwdHeaderObjectLockMode        = "x-amz-fwd-header-x-amz-object-lock-mode"
	AmzFwdHeaderObjectLockLegalHold   = "x-amz-fwd-header-x-amz-object-lock-legal-hold"
	AmzFwdHeaderObjectLockRetainUntil = "x-amz-fwd-header-x-amz-object-lock-retain-until-date"
	AmzFwdHeaderMPPartsCount          = "x-amz-fwd-header-x-amz-mp-parts-count"
	AmzFwdHeaderReplicationStatus     = "x-amz-fwd-header-x-amz-replication-status"
	AmzFwdHeaderSSE                   = "x-amz-fwd-header-x-amz-server-side-encryption"
	AmzFwdHeaderSSEC                  = "x-amz-fwd-header-x-amz-server-side-encryption-customer-algorithm"
	AmzFwdHeaderSSEKMSID              = "x-amz-fwd-header-x-amz-server-side-encryption-aws-kms-key-id"
	AmzFwdHeaderSSECMD5               = "x-amz-fwd-header-x-amz-server-side-encryption-customer-key-MD5"
	AmzFwdHeaderStorageClass          = "x-amz-fwd-header-x-amz-storage-class"
	AmzFwdHeaderTaggingCount          = "x-amz-fwd-header-x-amz-tagging-count"
	AmzFwdHeaderVersionID             = "x-amz-fwd-header-x-amz-version-id"
)
