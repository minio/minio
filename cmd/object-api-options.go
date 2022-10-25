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
	"github.com/minio/minio/internal/logger"
)

func getDefaultOpts(header http.Header, copySource bool, metadata map[string]string) (opts ObjectOptions, err error) {
	var clientKey [32]byte
	var sse encrypt.ServerSide

	opts = ObjectOptions{UserDefined: metadata}
	if copySource {
		if crypto.SSECopy.IsRequested(header) {
			clientKey, err = crypto.SSECopy.ParseHTTP(header)
			if err != nil {
				return
			}
			if sse, err = encrypt.NewSSEC(clientKey[:]); err != nil {
				return
			}
			opts.ServerSideEncryption = encrypt.SSECopy(sse)
			return
		}
		return
	}

	if crypto.SSEC.IsRequested(header) {
		clientKey, err = crypto.SSEC.ParseHTTP(header)
		if err != nil {
			return
		}
		if sse, err = encrypt.NewSSEC(clientKey[:]); err != nil {
			return
		}
		opts.ServerSideEncryption = sse
		return
	}
	if crypto.S3.IsRequested(header) || (metadata != nil && crypto.S3.IsEncrypted(metadata)) {
		opts.ServerSideEncryption = encrypt.NewSSE()
	}
	if v, ok := header[xhttp.MinIOSourceProxyRequest]; ok {
		opts.ProxyHeaderSet = true
		opts.ProxyRequest = strings.Join(v, "") == "true"
	}
	if _, ok := header[xhttp.MinIOSourceReplicationRequest]; ok {
		opts.ReplicationRequest = true
	}
	opts.Speedtest = header.Get(globalObjectPerfUserMetadata) != ""
	return
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
		if partNumber <= 0 {
			return opts, errInvalidArgument
		}
	}

	vid := strings.TrimSpace(r.Form.Get(xhttp.VersionID))
	if vid != "" && vid != nullVersionID {
		_, err := uuid.Parse(vid)
		if err != nil {
			logger.LogIf(ctx, err)
			return opts, InvalidVersionID{
				Bucket:    bucket,
				Object:    object,
				VersionID: vid,
			}
		}
	}

	deletePrefix := false
	if d := r.Header.Get(xhttp.MinIOForceDelete); d != "" {
		if b, err := strconv.ParseBool(d); err == nil {
			deletePrefix = b
		} else {
			return opts, err
		}
	}

	// default case of passing encryption headers to backend
	opts, err = getDefaultOpts(r.Header, false, nil)
	if err != nil {
		return opts, err
	}
	opts.DeletePrefix = deletePrefix
	opts.PartNumber = partNumber
	opts.VersionID = vid
	delMarker := strings.TrimSpace(r.Header.Get(xhttp.MinIOSourceDeleteMarker))
	if delMarker != "" {
		switch delMarker {
		case "true":
			opts.DeleteMarker = true
		case "false":
		default:
			err = fmt.Errorf("Unable to parse %s, failed with %w", xhttp.MinIOSourceDeleteMarker, fmt.Errorf("DeleteMarker should be true or false"))
			logger.LogIf(ctx, err)
			return opts, InvalidArgument{
				Bucket: bucket,
				Object: object,
				Err:    err,
			}
		}
	}
	replReadyCheck := strings.TrimSpace(r.Header.Get(xhttp.MinIOCheckDMReplicationReady))
	if replReadyCheck != "" {
		switch replReadyCheck {
		case "true":
			opts.CheckDMReplicationReady = true
		case "false":
		default:
			err = fmt.Errorf("Unable to parse %s, failed with %w", xhttp.MinIOCheckDMReplicationReady, fmt.Errorf("should be true or false"))
			logger.LogIf(ctx, err)
			return opts, InvalidArgument{
				Bucket: bucket,
				Object: object,
				Err:    err,
			}
		}
	}

	opts.Versioned = globalBucketVersioningSys.PrefixEnabled(bucket, object)
	opts.VersionSuspended = globalBucketVersioningSys.PrefixSuspended(bucket, object)
	return opts, nil
}

func delOpts(ctx context.Context, r *http.Request, bucket, object string) (opts ObjectOptions, err error) {
	opts, err = getOpts(ctx, r, bucket, object)
	if err != nil {
		return opts, err
	}
	opts.Versioned = globalBucketVersioningSys.PrefixEnabled(bucket, object)
	// Objects matching prefixes should not leave delete markers,
	// dramatically reduces namespace pollution while keeping the
	// benefits of replication, make sure to apply version suspension
	// only at bucket level instead.
	opts.VersionSuspended = globalBucketVersioningSys.Suspended(bucket)
	delMarker := strings.TrimSpace(r.Header.Get(xhttp.MinIOSourceDeleteMarker))
	if delMarker != "" {
		switch delMarker {
		case "true":
			opts.DeleteMarker = true
		case "false":
		default:
			err = fmt.Errorf("Unable to parse %s, failed with %w", xhttp.MinIOSourceDeleteMarker, fmt.Errorf("DeleteMarker should be true or false"))
			logger.LogIf(ctx, err)
			return opts, InvalidArgument{
				Bucket: bucket,
				Object: object,
				Err:    err,
			}
		}
	}

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
	} else {
		opts.MTime = UTCNow()
	}
	return opts, nil
}

// get ObjectOptions for PUT calls from encryption headers and metadata
func putOpts(ctx context.Context, r *http.Request, bucket, object string, metadata map[string]string) (opts ObjectOptions, err error) {
	versioned := globalBucketVersioningSys.PrefixEnabled(bucket, object)
	versionSuspended := globalBucketVersioningSys.PrefixSuspended(bucket, object)

	vid := strings.TrimSpace(r.Form.Get(xhttp.VersionID))
	if vid != "" && vid != nullVersionID {
		_, err := uuid.Parse(vid)
		if err != nil {
			logger.LogIf(ctx, err)
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
				Err:    fmt.Errorf("VersionID specified %s, but versioning not enabled on  %s", opts.VersionID, bucket),
			}
		}
	}

	mtimeStr := strings.TrimSpace(r.Header.Get(xhttp.MinIOSourceMTime))
	mtime := UTCNow()
	if mtimeStr != "" {
		mtime, err = time.Parse(time.RFC3339, mtimeStr)
		if err != nil {
			return opts, InvalidArgument{
				Bucket: bucket,
				Object: object,
				Err:    fmt.Errorf("Unable to parse %s, failed with %w", xhttp.MinIOSourceMTime, err),
			}
		}
	}
	retaintimeStr := strings.TrimSpace(r.Header.Get(xhttp.MinIOSourceObjectRetentionTimestamp))
	retaintimestmp := mtime
	if retaintimeStr != "" {
		retaintimestmp, err = time.Parse(time.RFC3339, retaintimeStr)
		if err != nil {
			return opts, InvalidArgument{
				Bucket: bucket,
				Object: object,
				Err:    fmt.Errorf("Unable to parse %s, failed with %w", xhttp.MinIOSourceObjectRetentionTimestamp, err),
			}
		}
	}

	lholdtimeStr := strings.TrimSpace(r.Header.Get(xhttp.MinIOSourceObjectLegalHoldTimestamp))
	lholdtimestmp := mtime
	if lholdtimeStr != "" {
		lholdtimestmp, err = time.Parse(time.RFC3339, lholdtimeStr)
		if err != nil {
			return opts, InvalidArgument{
				Bucket: bucket,
				Object: object,
				Err:    fmt.Errorf("Unable to parse %s, failed with %w", xhttp.MinIOSourceObjectLegalHoldTimestamp, err),
			}
		}
	}
	tagtimeStr := strings.TrimSpace(r.Header.Get(xhttp.MinIOSourceTaggingTimestamp))
	taggingtimestmp := mtime
	if tagtimeStr != "" {
		taggingtimestmp, err = time.Parse(time.RFC3339, tagtimeStr)
		if err != nil {
			return opts, InvalidArgument{
				Bucket: bucket,
				Object: object,
				Err:    fmt.Errorf("Unable to parse %s, failed with %w", xhttp.MinIOSourceTaggingTimestamp, err),
			}
		}
	}

	if metadata == nil {
		metadata = make(map[string]string)
	}

	wantCRC, err := hash.GetContentChecksum(r)
	if err != nil {
		return opts, InvalidArgument{
			Bucket: bucket,
			Object: object,
			Err:    fmt.Errorf("invalid/unknown checksum sent: %v", err),
		}
	}

	if crypto.S3KMS.IsRequested(r.Header) {
		keyID, context, err := crypto.S3KMS.ParseHTTP(r.Header)
		if err != nil {
			return ObjectOptions{}, err
		}
		sseKms, err := encrypt.NewSSEKMS(keyID, context)
		if err != nil {
			return ObjectOptions{}, err
		}
		return ObjectOptions{
			ServerSideEncryption: sseKms,
			UserDefined:          metadata,
			VersionID:            vid,
			Versioned:            versioned,
			VersionSuspended:     versionSuspended,
			MTime:                mtime,
			WantChecksum:         wantCRC,
		}, nil
	}
	// default case of passing encryption headers and UserDefined metadata to backend
	opts, err = getDefaultOpts(r.Header, false, metadata)
	if err != nil {
		return opts, err
	}
	opts.VersionID = vid
	opts.Versioned = versioned
	opts.VersionSuspended = versionSuspended
	opts.MTime = mtime
	opts.ReplicationSourceLegalholdTimestamp = lholdtimestmp
	opts.ReplicationSourceRetentionTimestamp = retaintimestmp
	opts.ReplicationSourceTaggingTimestamp = taggingtimestmp
	opts.WantChecksum = wantCRC

	return opts, nil
}

// get ObjectOptions for Copy calls with encryption headers provided on the target side and source side metadata
func copyDstOpts(ctx context.Context, r *http.Request, bucket, object string, metadata map[string]string) (opts ObjectOptions, err error) {
	return putOpts(ctx, r, bucket, object, metadata)
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
	mtime := UTCNow()
	if mtimeStr != "" {
		mtime, err = time.Parse(time.RFC3339, mtimeStr)
		if err != nil {
			return opts, InvalidArgument{
				Bucket: bucket,
				Object: object,
				Err:    fmt.Errorf("Unable to parse %s, failed with %w", xhttp.MinIOSourceMTime, err),
			}
		}
	}
	opts.WantChecksum, err = hash.GetContentChecksum(r)
	if err != nil {
		return opts, InvalidArgument{
			Bucket: bucket,
			Object: object,
			Err:    fmt.Errorf("invalid/unknown checksum sent: %v", err),
		}
	}
	opts.MTime = mtime
	opts.UserDefined = make(map[string]string)
	return opts, nil
}
