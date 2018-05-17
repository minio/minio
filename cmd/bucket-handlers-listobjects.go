/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"fmt"
	"net/http"
	"strings"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/policy"
)

// ListObjectsV2Handler - GET Bucket (List Objects) Version 2.
// --------------------------
// This implementation of the GET operation returns some or all (up to 1000)
// of the objects in a bucket. You can use the request parameters as selection
// criteria to return a subset of the objects in a bucket.
//
// NOTE: It is recommended that this API to be used for application development.
// Minio continues to support ListObjectsV1 for supporting legacy tools.
func (api objectAPIHandlers) ListObjectsV2Handler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, "ListObjectsV2")

	args := newRequestArgs(r)

	bucket, err := args.CompatBucketName()
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponse(w, ErrInvalidBucketName, r.URL)
		return
	}

	prefix, err := args.Prefix()
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponse(w, ErrInvalidObjectName, r.URL)
		return
	}

	startAfter, err := args.StartAfter()
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponse(w, ErrInvalidObjectName, r.URL)
		return
	}

	token := args.ContinuationToken()

	delimiter, err := args.Delimiter()
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponse(w, ErrNotImplemented, r.URL)
		return
	}

	maxKeys, err := args.MaxKeys()
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponse(w, ErrInvalidMaxKeys, r.URL)
		return
	}

	fetchOwner := args.FetchOwner()

	// Marker is continuation-token. When it is empty, take start-after as marker.
	marker := token
	if marker == "" {
		marker = startAfter
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.ListBucketAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	if _, err = objectAPI.GetBucketInfo(ctx, bucket); err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	listObjectsV2 := objectAPI.ListObjectsV2
	if api.CacheAPI() != nil {
		listObjectsV2 = api.CacheAPI().ListObjectsV2
	}
	// Inititate a list objects operation based on the input params.
	// On success would return back ListObjectsInfo object to be
	// marshalled into S3 compatible XML header.
	listObjectsV2Info, err := listObjectsV2(ctx, bucket, prefix, marker, delimiter, maxKeys, fetchOwner, startAfter)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	for i := range listObjectsV2Info.Objects {
		if listObjectsV2Info.Objects[i].IsEncrypted() {
			listObjectsV2Info.Objects[i].Size, err = listObjectsV2Info.Objects[i].DecryptedSize()
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
				return
			}
		}
	}

	response := generateListObjectsV2Response(bucket, prefix, token, listObjectsV2Info.NextContinuationToken, startAfter,
		delimiter, fetchOwner, listObjectsV2Info.IsTruncated, maxKeys, listObjectsV2Info.Objects, listObjectsV2Info.Prefixes)

	// Write success response.
	writeSuccessResponseXML(w, encodeResponse(response))
}

// ListObjectsV1Handler - GET Bucket (List Objects) Version 1.
// --------------------------
// This implementation of the GET operation returns some or all (up to 1000)
// of the objects in a bucket. You can use the request parameters as selection
// criteria to return a subset of the objects in a bucket.
//
func (api objectAPIHandlers) ListObjectsV1Handler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, "ListObjectsV1")

	args := newRequestArgs(r)

	bucket, err := args.CompatBucketName()
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponse(w, ErrInvalidBucketName, r.URL)
		return
	}

	prefix, err := args.Prefix()
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponse(w, ErrInvalidObjectName, r.URL)
		return
	}

	marker, err := args.Marker()
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponse(w, ErrInvalidObjectName, r.URL)
		return
	}

	// Verify if marker has prefix only on S3 gateway.
	if globalGatewayName != "s3" {
		if marker != "" && !strings.HasPrefix(marker, prefix) {
			logger.LogIf(ctx, fmt.Errorf("marker %v does not start with prefix %v", marker, prefix))
			writeErrorResponse(w, ErrNotImplemented, r.URL)
			return
		}
	}

	delimiter, err := args.Delimiter()
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponse(w, ErrNotImplemented, r.URL)
		return
	}

	maxKeys, err := args.MaxKeys()
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponse(w, ErrInvalidMaxKeys, r.URL)
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.ListBucketAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	if _, err = objectAPI.GetBucketInfo(ctx, bucket); err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	listObjects := objectAPI.ListObjects
	if api.CacheAPI() != nil {
		listObjects = api.CacheAPI().ListObjects
	}
	// Inititate a list objects operation based on the input params.
	// On success would return back ListObjectsInfo object to be
	// marshalled into S3 compatible XML header.
	listObjectsInfo, err := listObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	for i := range listObjectsInfo.Objects {
		if listObjectsInfo.Objects[i].IsEncrypted() {
			listObjectsInfo.Objects[i].Size, err = listObjectsInfo.Objects[i].DecryptedSize()
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(err), r.URL)
				return
			}
		}
	}

	response := generateListObjectsV1Response(bucket, prefix, marker, delimiter, maxKeys, listObjectsInfo)

	// Write success response.
	writeSuccessResponseXML(w, encodeResponse(response))
}
