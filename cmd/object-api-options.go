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
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7/pkg/encrypt"
	"github.com/minio/minio/internal/crypto"
	"github.com/minio/minio/internal/hash"
	xhttp "github.com/minio/minio/internal/http"
)

func getDefaultOpts(header http.Header, copySource bool, metadata map[string]string) (opts ObjectOptions, err error) {
	var clientKey [32]byte
	var sse encrypt.ServerSide

	opts = ObjectOptions{UserDefined: metadata}
	if v, ok := header[xhttp.MinIOSourceProxyRequest]; ok {
		opts.ProxyHeaderSet = true
		opts.ProxyRequest = strings.Join(v, "") == "true"
	}
	if _, ok := header[xhttp.MinIOSourceReplicationRequest]; ok {
		opts.ReplicationRequest = true
	}
	opts.Speedtest = header.Get(globalObjectPerfUserMetadata) != ""

	if copySource {
		if crypto.SSECopy.IsRequested(header) {
			clientKey, err = crypto.SSECopy.ParseHTTP(header)
			if err != nil {
				return opts, err
			}
			if sse, err = encrypt.NewSSEC(clientKey[:]); err != nil {
				return opts, err
			}
			opts.ServerSideEncryption = encrypt.SSECopy(sse)
			return opts, err
		}
		return opts, err
	}

	if crypto.SSEC.IsRequested(header) {
		clientKey, err = crypto.SSEC.ParseHTTP(header)
		if err != nil {
			return opts, err
		}
		if sse, err = encrypt.NewSSEC(clientKey[:]); err != nil {
			return opts, err
		}
		opts.ServerSideEncryption = sse
		return opts, err
	}
	if crypto.S3.IsRequested(header) || (metadata != nil && crypto.S3.IsEncrypted(metadata)) {
		opts.ServerSideEncryption = encrypt.NewSSE()
	}

	return opts, err
}

// get ObjectOptions for GET calls from encryption headers
func getOpts(ctx context.Context, r *http.Request, bucket, object string) (ObjectOptions, error) {
	var opts ObjectOptions

	var partNumber int
	var err error
	if pn := r.Form.Get(xhttp.PartNumber); pn != "" {
		partNumber, err = strconv.Atoi(pn)
		if err != nil {
			return opts, err
		}
		if isMaxPartID(partNumber) {
			return opts, errInvalidMaxParts
		}
		if partNumber <= 0 {
			return opts, errInvalidArgument
		}
	}

	vid := strings.TrimSpace(r.Form.Get(xhttp.VersionID))
	if vid != "" && vid != nullVersionID {
		if _, err := uuid.Parse(vid); err != nil {
			return opts, InvalidVersionID{
				Bucket:    bucket,
				Object:    object,
				VersionID: vid,
			}
		}
	}

	// default case of passing encryption headers to backend
	opts, err = getDefaultOpts(r.Header, false, nil)
	if err != nil {
		return opts, err
	}
	opts.PartNumber = partNumber
	opts.VersionID = vid

	delMarker, err := parseBoolHeader(bucket, object, r.Header, xhttp.MinIOSourceDeleteMarker)
	if err != nil {
		return opts, err
	}
	opts.DeleteMarker = delMarker

	replReadyCheck, err := parseBoolHeader(bucket, object, r.Header, xhttp.MinIOCheckDMReplicationReady)
	if err != nil {
		return opts, err
	}
	opts.CheckDMReplicationReady = replReadyCheck

	opts.Tagging = r.Header.Get(xhttp.AmzTagDirective) == accessDirective
	opts.Versioned = globalBucketVersioningSys.PrefixEnabled(bucket, object)
	opts.VersionSuspended = globalBucketVersioningSys.PrefixSuspended(bucket, object)
	return opts, nil
}

func getAndValidateAttributesOpts(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, object string) (opts ObjectOptions, valid bool) {
	var argumentName string
	var argumentValue string
	var apiErr APIError
	var err error
	valid = true

	defer func() {
		if valid {
			return
		}

		errResp := objectAttributesErrorResponse{
			ArgumentName:  &argumentName,
			ArgumentValue: &argumentValue,
			APIErrorResponse: getAPIErrorResponse(
				ctx,
				apiErr,
				r.URL.Path,
				w.Header().Get(xhttp.AmzRequestID),
				w.Header().Get(xhttp.AmzRequestHostID),
			),
		}

		writeResponse(w, apiErr.HTTPStatusCode, encodeResponse(errResp), mimeXML)
	}()

	opts, err = getOpts(ctx, r, bucket, object)
	if err != nil {
		switch vErr := err.(type) {
		case InvalidVersionID:
			apiErr = toAPIError(ctx, vErr)
			argumentName = strings.ToLower("versionId")
			argumentValue = vErr.VersionID
		default:
			apiErr = toAPIError(ctx, vErr)
		}
		valid = false
		return opts, valid
	}

	opts.MaxParts, err = parseIntHeader(bucket, object, r.Header, xhttp.AmzMaxParts)
	if err != nil {
		apiErr = toAPIError(ctx, err)
		argumentName = strings.ToLower(xhttp.AmzMaxParts)
		valid = false
		return opts, valid
	}

	if opts.MaxParts == 0 {
		opts.MaxParts = maxPartsList
	}

	opts.PartNumberMarker, err = parseIntHeader(bucket, object, r.Header, xhttp.AmzPartNumberMarker)
	if err != nil {
		apiErr = toAPIError(ctx, err)
		argumentName = strings.ToLower(xhttp.AmzPartNumberMarker)
		valid = false
		return opts, valid
	}

	opts.ObjectAttributes = parseObjectAttributes(r.Header)
	if len(opts.ObjectAttributes) < 1 {
		apiErr = errorCodes.ToAPIErr(ErrInvalidAttributeName)
		argumentName = strings.ToLower(xhttp.AmzObjectAttributes)
		valid = false
		return opts, valid
	}

	for tag := range opts.ObjectAttributes {
		switch tag {
		case xhttp.ETag:
		case xhttp.Checksum:
		case xhttp.StorageClass:
		case xhttp.ObjectSize:
		case xhttp.ObjectParts:
		default:
			apiErr = errorCodes.ToAPIErr(ErrInvalidAttributeName)
			argumentName = strings.ToLower(xhttp.AmzObjectAttributes)
			argumentValue = tag
			valid = false
			return opts, valid
		}
	}

	return opts, valid
}

func parseObjectAttributes(h http.Header) (attributes map[string]struct{}) {
	attributes = make(map[string]struct{})
	for _, headerVal := range h.Values(xhttp.AmzObjectAttributes) {
		for v := range strings.SplitSeq(strings.TrimSpace(headerVal), ",") {
			if v != "" {
				attributes[v] = struct{}{}
			}
		}
	}

	return attributes
}

func parseIntHeader(bucket, object string, h http.Header, headerName string) (value int, err error) {
	stringInt := strings.TrimSpace(h.Get(headerName))
	if stringInt == "" {
		return value, err
	}
	value, err = strconv.Atoi(stringInt)
	if err != nil {
		return 0, InvalidArgument{
			Bucket: bucket,
			Object: object,
			Err:    fmt.Errorf("Unable to parse %s, value should be an integer", headerName),
		}
	}
	return value, err
}

func parseBoolHeader(bucket, object string, h http.Header, headerName string) (bool, error) {
	value := strings.TrimSpace(h.Get(headerName))
	if value != "" {
		switch value {
		case "true":
			return true, nil
		case "false":
		default:
			return false, InvalidArgument{
				Bucket: bucket,
				Object: object,
				Err:    fmt.Errorf("Unable to parse %s, value should be either 'true' or 'false'", headerName),
			}
		}
	}
	return false, nil
}

func delOpts(ctx context.Context, r *http.Request, bucket, object string) (opts ObjectOptions, err error) {
	opts, err = getOpts(ctx, r, bucket, object)
	if err != nil {
		return opts, err
	}

	deletePrefix := false
	if d := r.Header.Get(xhttp.MinIOForceDelete); d != "" {
		if b, err := strconv.ParseBool(d); err == nil {
			deletePrefix = b
		} else {
			return opts, err
		}
	}

	opts.DeletePrefix = deletePrefix
	opts.Versioned = globalBucketVersioningSys.PrefixEnabled(bucket, object)
	// Objects matching prefixes should not leave delete markers,
	// dramatically reduces namespace pollution while keeping the
	// benefits of replication, make sure to apply version suspension
	// only at bucket level instead.
	opts.VersionSuspended = globalBucketVersioningSys.Suspended(bucket)
	// For directory objects, delete `null` version permanently.
	if isDirObject(object) && opts.VersionID == "" {
		opts.VersionID = nullVersionID
	}

	delMarker, err := parseBoolHeader(bucket, object, r.Header, xhttp.MinIOSourceDeleteMarker)
	if err != nil {
		return opts, err
	}
	opts.DeleteMarker = delMarker

	mtime := strings.TrimSpace(r.Header.Get(xhttp.MinIOSourceMTime))
	if mtime != "" {
		opts.MTime, err = time.Parse(time.RFC3339Nano, mtime)
		if err != nil {
			return opts, InvalidArgument{
				Bucket: bucket,
				Object: object,
				Err:    fmt.Errorf("Unable to parse %s, failed with %w", xhttp.MinIOSourceMTime, err),
			}
		}
	}
	return opts, nil
}

// get ObjectOptions for PUT calls from encryption headers and metadata
func putOptsFromReq(ctx context.Context, r *http.Request, bucket, object string, metadata map[string]string) (opts ObjectOptions, err error) {
	return putOpts(ctx, bucket, object, r.Form.Get(xhttp.VersionID), r.Header, metadata)
}

func putOpts(ctx context.Context, bucket, object, vid string, hdrs http.Header, metadata map[string]string) (opts ObjectOptions, err error) {
	versioned := globalBucketVersioningSys.PrefixEnabled(bucket, object)
	versionSuspended := globalBucketVersioningSys.PrefixSuspended(bucket, object)

	vid = strings.TrimSpace(vid)
	if vid != "" && vid != nullVersionID {
		_, err := uuid.Parse(vid)
		if err != nil {
			return opts, InvalidVersionID{
				Bucket:    bucket,
				Object:    object,
				VersionID: vid,
			}
		}
		if !versioned {
			return opts, InvalidArgument{
				Bucket: bucket,
				Object: object,
				Err:    fmt.Errorf("VersionID specified %s, but versioning not enabled on bucket=%s", opts.VersionID, bucket),
			}
		}
	}
	opts, err = putOptsFromHeaders(ctx, hdrs, metadata)
	if err != nil {
		return opts, InvalidArgument{
			Bucket: bucket,
			Object: object,
			Err:    err,
		}
	}

	opts.VersionID = vid
	opts.Versioned = versioned
	opts.VersionSuspended = versionSuspended

	// For directory objects skip creating new versions.
	if isDirObject(object) && vid == "" {
		opts.VersionID = nullVersionID
	}

	return opts, nil
}

func putOptsFromHeaders(ctx context.Context, hdr http.Header, metadata map[string]string) (opts ObjectOptions, err error) {
	mtimeStr := strings.TrimSpace(hdr.Get(xhttp.MinIOSourceMTime))
	var mtime time.Time
	if mtimeStr != "" {
		mtime, err = time.Parse(time.RFC3339Nano, mtimeStr)
		if err != nil {
			return opts, fmt.Errorf("Unable to parse %s, failed with %w", xhttp.MinIOSourceMTime, err)
		}
	}
	retaintimeStr := strings.TrimSpace(hdr.Get(xhttp.MinIOSourceObjectRetentionTimestamp))
	var retaintimestmp time.Time
	if retaintimeStr != "" {
		retaintimestmp, err = time.Parse(time.RFC3339, retaintimeStr)
		if err != nil {
			return opts, fmt.Errorf("Unable to parse %s, failed with %w", xhttp.MinIOSourceObjectRetentionTimestamp, err)
		}
	}

	lholdtimeStr := strings.TrimSpace(hdr.Get(xhttp.MinIOSourceObjectLegalHoldTimestamp))
	var lholdtimestmp time.Time
	if lholdtimeStr != "" {
		lholdtimestmp, err = time.Parse(time.RFC3339, lholdtimeStr)
		if err != nil {
			return opts, fmt.Errorf("Unable to parse %s, failed with %w", xhttp.MinIOSourceObjectLegalHoldTimestamp, err)
		}
	}
	tagtimeStr := strings.TrimSpace(hdr.Get(xhttp.MinIOSourceTaggingTimestamp))
	var taggingtimestmp time.Time
	if tagtimeStr != "" {
		taggingtimestmp, err = time.Parse(time.RFC3339, tagtimeStr)
		if err != nil {
			return opts, fmt.Errorf("Unable to parse %s, failed with %w", xhttp.MinIOSourceTaggingTimestamp, err)
		}
	}

	if metadata == nil {
		metadata = make(map[string]string)
	}

	etag := strings.TrimSpace(hdr.Get(xhttp.MinIOSourceETag))
	if crypto.S3KMS.IsRequested(hdr) {
		keyID, context, err := crypto.S3KMS.ParseHTTP(hdr)
		if err != nil {
			return ObjectOptions{}, err
		}
		sseKms, err := encrypt.NewSSEKMS(keyID, context)
		if err != nil {
			return ObjectOptions{}, err
		}
		op := ObjectOptions{
			ServerSideEncryption: sseKms,
			UserDefined:          metadata,
			MTime:                mtime,
			PreserveETag:         etag,
		}
		return op, nil
	}
	// default case of passing encryption headers and UserDefined metadata to backend
	opts, err = getDefaultOpts(hdr, false, metadata)
	if err != nil {
		return opts, err
	}

	opts.MTime = mtime
	opts.ReplicationSourceLegalholdTimestamp = lholdtimestmp
	opts.ReplicationSourceRetentionTimestamp = retaintimestmp
	opts.ReplicationSourceTaggingTimestamp = taggingtimestmp
	opts.PreserveETag = etag

	return opts, nil
}

// get ObjectOptions for Copy calls with encryption headers provided on the target side and source side metadata
func copyDstOpts(ctx context.Context, r *http.Request, bucket, object string, metadata map[string]string) (opts ObjectOptions, err error) {
	return putOptsFromReq(ctx, r, bucket, object, metadata)
}

// get ObjectOptions for Copy calls with encryption headers provided on the source side
func copySrcOpts(ctx context.Context, r *http.Request, bucket, object string) (ObjectOptions, error) {
	var opts ObjectOptions

	// default case of passing encryption headers to backend
	opts, err := getDefaultOpts(r.Header, false, nil)
	if err != nil {
		return opts, err
	}
	return opts, nil
}

// get ObjectOptions for CompleteMultipart calls
func completeMultipartOpts(ctx context.Context, r *http.Request, bucket, object string) (opts ObjectOptions, err error) {
	mtimeStr := strings.TrimSpace(r.Header.Get(xhttp.MinIOSourceMTime))
	var mtime time.Time
	if mtimeStr != "" {
		mtime, err = time.Parse(time.RFC3339Nano, mtimeStr)
		if err != nil {
			return opts, InvalidArgument{
				Bucket: bucket,
				Object: object,
				Err:    fmt.Errorf("Unable to parse %s, failed with %w", xhttp.MinIOSourceMTime, err),
			}
		}
	}

	opts.WantChecksum, err = hash.GetContentChecksum(r.Header)
	if err != nil {
		return opts, err
	}
	opts.MTime = mtime
	opts.UserDefined = make(map[string]string)
	// Transfer SSEC key in opts.EncryptFn
	if crypto.SSEC.IsRequested(r.Header) {
		key, err := ParseSSECustomerRequest(r)
		if err == nil {
			// Set EncryptFn to return SSEC key
			opts.EncryptFn = func(baseKey string, data []byte) []byte {
				return key
			}
		}
	}
	if _, ok := r.Header[xhttp.MinIOSourceReplicationRequest]; ok {
		opts.ReplicationRequest = true
		opts.UserDefined[ReservedMetadataPrefix+"Actual-Object-Size"] = r.Header.Get(xhttp.MinIOReplicationActualObjectSize)
	}
	if r.Header.Get(ReplicationSsecChecksumHeader) != "" {
		opts.UserDefined[ReplicationSsecChecksumHeader] = r.Header.Get(ReplicationSsecChecksumHeader)
	}
	return opts, nil
}
