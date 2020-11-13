/*
 * MinIO Cloud Storage, (C) 2017-2020 MinIO, Inc.
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
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7/pkg/encrypt"
	"github.com/minio/minio/cmd/crypto"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
)

// set encryption options for pass through to backend in the case of gateway and UserDefined metadata
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
	return
}

// get ObjectOptions for GET calls from encryption headers
func getOpts(ctx context.Context, r *http.Request, bucket, object string) (ObjectOptions, error) {
	var (
		encryption encrypt.ServerSide
		opts       ObjectOptions
	)

	var partNumber int
	var err error
	if pn := r.URL.Query().Get(xhttp.PartNumber); pn != "" {
		partNumber, err = strconv.Atoi(pn)
		if err != nil {
			return opts, err
		}
		if partNumber <= 0 {
			return opts, errInvalidArgument
		}
	}

	vid := strings.TrimSpace(r.URL.Query().Get(xhttp.VersionID))
	if vid != "" && vid != nullVersionID {
		_, err := uuid.Parse(vid)
		if err != nil {
			logger.LogIf(ctx, err)
			return opts, VersionNotFound{
				Bucket:    bucket,
				Object:    object,
				VersionID: vid,
			}
		}
	}

	if GlobalGatewaySSE.SSEC() && crypto.SSEC.IsRequested(r.Header) {
		key, err := crypto.SSEC.ParseHTTP(r.Header)
		if err != nil {
			return opts, err
		}
		derivedKey := deriveClientKey(key, bucket, object)
		encryption, err = encrypt.NewSSEC(derivedKey[:])
		logger.CriticalIf(ctx, err)
		return ObjectOptions{
			ServerSideEncryption: encryption,
			VersionID:            vid,
			PartNumber:           partNumber,
		}, nil
	}

	// default case of passing encryption headers to backend
	opts, err = getDefaultOpts(r.Header, false, nil)
	if err != nil {
		return opts, err
	}
	opts.PartNumber = partNumber
	opts.VersionID = vid
	return opts, nil
}

func delOpts(ctx context.Context, r *http.Request, bucket, object string) (opts ObjectOptions, err error) {
	versioned := globalBucketVersioningSys.Enabled(bucket)
	opts, err = getOpts(ctx, r, bucket, object)
	if err != nil {
		return opts, err
	}
	opts.Versioned = versioned
	opts.VersionSuspended = globalBucketVersioningSys.Suspended(bucket)
	return opts, nil
}

// get ObjectOptions for PUT calls from encryption headers and metadata
func putOpts(ctx context.Context, r *http.Request, bucket, object string, metadata map[string]string) (opts ObjectOptions, err error) {
	versioned := globalBucketVersioningSys.Enabled(bucket)
	vid := strings.TrimSpace(r.URL.Query().Get(xhttp.VersionID))
	if vid != "" && vid != nullVersionID {
		_, err := uuid.Parse(vid)
		if err != nil {
			logger.LogIf(ctx, err)
			return opts, VersionNotFound{
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
	var mtime time.Time
	if mtimeStr != "" {
		mtime, err = time.Parse(time.RFC3339, mtimeStr)
		if err != nil {
			return opts, InvalidArgument{
				Bucket: bucket,
				Object: object,
				Err:    fmt.Errorf("Unable to parse %s, failed with %w", xhttp.MinIOSourceMTime, err),
			}
		}
	} else {
		mtime = UTCNow()
	}
	etag := strings.TrimSpace(r.Header.Get(xhttp.MinIOSourceETag))
	if etag != "" {
		if metadata == nil {
			metadata = make(map[string]string, 1)
		}
		metadata["etag"] = etag
	}
	// In the case of multipart custom format, the metadata needs to be checked in addition to header to see if it
	// is SSE-S3 encrypted, primarily because S3 protocol does not require SSE-S3 headers in PutObjectPart calls
	if GlobalGatewaySSE.SSES3() && (crypto.S3.IsRequested(r.Header) || crypto.S3.IsEncrypted(metadata)) {
		return ObjectOptions{
			ServerSideEncryption: encrypt.NewSSE(),
			UserDefined:          metadata,
			VersionID:            vid,
			Versioned:            versioned,
			MTime:                mtime,
		}, nil
	}
	if GlobalGatewaySSE.SSEC() && crypto.SSEC.IsRequested(r.Header) {
		opts, err = getOpts(ctx, r, bucket, object)
		opts.VersionID = vid
		opts.Versioned = versioned
		opts.UserDefined = metadata
		return
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
			MTime:                mtime,
		}, nil
	}
	// default case of passing encryption headers and UserDefined metadata to backend
	opts, err = getDefaultOpts(r.Header, false, metadata)
	if err != nil {
		return opts, err
	}
	opts.VersionID = vid
	opts.Versioned = versioned
	opts.MTime = mtime
	return opts, nil
}

// get ObjectOptions for Copy calls with encryption headers provided on the target side and source side metadata
func copyDstOpts(ctx context.Context, r *http.Request, bucket, object string, metadata map[string]string) (opts ObjectOptions, err error) {
	return putOpts(ctx, r, bucket, object, metadata)
}

// get ObjectOptions for Copy calls with encryption headers provided on the source side
func copySrcOpts(ctx context.Context, r *http.Request, bucket, object string) (ObjectOptions, error) {
	var (
		ssec encrypt.ServerSide
		opts ObjectOptions
	)

	if GlobalGatewaySSE.SSEC() && crypto.SSECopy.IsRequested(r.Header) {
		key, err := crypto.SSECopy.ParseHTTP(r.Header)
		if err != nil {
			return opts, err
		}
		derivedKey := deriveClientKey(key, bucket, object)
		ssec, err = encrypt.NewSSEC(derivedKey[:])
		if err != nil {
			return opts, err
		}
		return ObjectOptions{ServerSideEncryption: encrypt.SSECopy(ssec)}, nil
	}

	// default case of passing encryption headers to backend
	opts, err := getDefaultOpts(r.Header, false, nil)
	if err != nil {
		return opts, err
	}
	return opts, nil
}
