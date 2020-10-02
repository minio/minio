/*
 * MinIO Cloud Storage, (C) 2016 MinIO, Inc.
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

	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/logger"

	"github.com/minio/minio/pkg/bucket/policy"
	"github.com/minio/minio/pkg/handlers"
	"github.com/minio/minio/pkg/sync/errgroup"
)

func concurrentDecryptETag(ctx context.Context, objects []ObjectInfo) {
	inParallel := func(objects []ObjectInfo) {
		g := errgroup.WithNErrs(len(objects))
		for index := range objects {
			index := index
			g.Go(func() error {
				objects[index].ETag = objects[index].GetActualETag(nil)
				objects[index].Size, _ = objects[index].GetActualSize()
				return nil
			}, index)
		}
		g.Wait()
	}
	const maxConcurrent = 500
	for {
		if len(objects) < maxConcurrent {
			inParallel(objects)
			return
		}
		inParallel(objects[:maxConcurrent])
		objects = objects[maxConcurrent:]
	}
}

// Validate all the ListObjects query arguments, returns an APIErrorCode
// if one of the args do not meet the required conditions.
// Special conditions required by MinIO server are as below
// - delimiter if set should be equal to '/', otherwise the request is rejected.
// - marker if set should have a common prefix with 'prefix' param, otherwise
//   the request is rejected.
func validateListObjectsArgs(marker, delimiter, encodingType string, maxKeys int) APIErrorCode {
	// Max keys cannot be negative.
	if maxKeys < 0 {
		return ErrInvalidMaxKeys
	}

	if encodingType != "" {
		// Only url encoding type is supported
		if strings.ToLower(encodingType) != "url" {
			return ErrInvalidEncodingMethod
		}
	}

	return ErrNone
}

// ListObjectVersions - GET Bucket Object versions
// You can use the versions subresource to list metadata about all
// of the versions of objects in a bucket.
func (api objectAPIHandlers) ListObjectVersionsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListObjectVersions")

	defer logger.AuditLog(w, r, "ListObjectVersions", mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.ListBucketVersionsAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	urlValues := r.URL.Query()

	// Extract all the listBucketVersions query params to their native values.
	prefix, marker, delimiter, maxkeys, encodingType, versionIDMarker, errCode := getListBucketObjectVersionsArgs(urlValues)
	if errCode != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(errCode), r.URL, guessIsBrowserReq(r))
		return
	}

	// Validate the query params before beginning to serve the request.
	if s3Error := validateListObjectsArgs(marker, delimiter, encodingType, maxkeys); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	// Forward the request using Source IP or bucket
	forwardStr := handlers.GetSourceIPFromHeaders(r)
	if forwardStr == "" {
		forwardStr = bucket
	}
	if proxyRequestByStringHash(ctx, w, r, forwardStr) {
		return
	}

	listObjectVersions := objectAPI.ListObjectVersions

	// Inititate a list object versions operation based on the input params.
	// On success would return back ListObjectsInfo object to be
	// marshaled into S3 compatible XML header.
	listObjectVersionsInfo, err := listObjectVersions(ctx, bucket, prefix, marker, versionIDMarker, delimiter, maxkeys)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	concurrentDecryptETag(ctx, listObjectVersionsInfo.Objects)

	response := generateListVersionsResponse(bucket, prefix, marker, versionIDMarker, delimiter, encodingType, maxkeys, listObjectVersionsInfo)

	// Write success response.
	writeSuccessResponseXML(w, encodeResponse(response))
}

// ListObjectsV2MHandler - GET Bucket (List Objects) Version 2 with metadata.
// --------------------------
// This implementation of the GET operation returns some or all (up to 10000)
// of the objects in a bucket. You can use the request parame<ters as selection
// criteria to return a subset of the objects in a bucket.
//
// NOTE: It is recommended that this API to be used for application development.
// MinIO continues to support ListObjectsV1 and V2 for supporting legacy tools.
func (api objectAPIHandlers) ListObjectsV2MHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListObjectsV2M")

	defer logger.AuditLog(w, r, "ListObjectsV2M", mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.ListBucketAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	urlValues := r.URL.Query()

	// Extract all the listObjectsV2 query params to their native values.
	prefix, token, startAfter, delimiter, fetchOwner, maxKeys, encodingType, errCode := getListObjectsV2Args(urlValues)
	if errCode != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(errCode), r.URL, guessIsBrowserReq(r))
		return
	}

	// Validate the query params before beginning to serve the request.
	// fetch-owner is not validated since it is a boolean
	if s3Error := validateListObjectsArgs(token, delimiter, encodingType, maxKeys); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	// Analyze continuation token and route the request accordingly
	var success bool
	token, success = proxyRequestByToken(ctx, w, r, token)
	if success {
		return
	}

	listObjectsV2 := objectAPI.ListObjectsV2

	// Inititate a list objects operation based on the input params.
	// On success would return back ListObjectsInfo object to be
	// marshaled into S3 compatible XML header.
	listObjectsV2Info, err := listObjectsV2(ctx, bucket, prefix, token, delimiter, maxKeys, fetchOwner, startAfter)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	concurrentDecryptETag(ctx, listObjectsV2Info.Objects)

	// The next continuation token has id@node_index format to optimize paginated listing
	nextContinuationToken := listObjectsV2Info.NextContinuationToken
	if nextContinuationToken != "" && listObjectsV2Info.IsTruncated {
		nextContinuationToken = fmt.Sprintf("%s@%d", listObjectsV2Info.NextContinuationToken, getLocalNodeIndex())
	}

	response := generateListObjectsV2Response(bucket, prefix, token, nextContinuationToken, startAfter,
		delimiter, encodingType, fetchOwner, listObjectsV2Info.IsTruncated,
		maxKeys, listObjectsV2Info.Objects, listObjectsV2Info.Prefixes, true)

	// Write success response.
	writeSuccessResponseXML(w, encodeResponse(response))
}

// ListObjectsV2Handler - GET Bucket (List Objects) Version 2.
// --------------------------
// This implementation of the GET operation returns some or all (up to 10000)
// of the objects in a bucket. You can use the request parameters as selection
// criteria to return a subset of the objects in a bucket.
//
// NOTE: It is recommended that this API to be used for application development.
// MinIO continues to support ListObjectsV1 for supporting legacy tools.
func (api objectAPIHandlers) ListObjectsV2Handler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListObjectsV2")

	defer logger.AuditLog(w, r, "ListObjectsV2", mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.ListBucketAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	urlValues := r.URL.Query()

	// Extract all the listObjectsV2 query params to their native values.
	prefix, token, startAfter, delimiter, fetchOwner, maxKeys, encodingType, errCode := getListObjectsV2Args(urlValues)
	if errCode != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(errCode), r.URL, guessIsBrowserReq(r))
		return
	}

	// Validate the query params before beginning to serve the request.
	// fetch-owner is not validated since it is a boolean
	if s3Error := validateListObjectsArgs(token, delimiter, encodingType, maxKeys); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	// Analyze continuation token and route the request accordingly
	var success bool
	token, success = proxyRequestByToken(ctx, w, r, token)
	if success {
		return
	}

	listObjectsV2 := objectAPI.ListObjectsV2

	// Inititate a list objects operation based on the input params.
	// On success would return back ListObjectsInfo object to be
	// marshaled into S3 compatible XML header.
	listObjectsV2Info, err := listObjectsV2(ctx, bucket, prefix, token, delimiter, maxKeys, fetchOwner, startAfter)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	concurrentDecryptETag(ctx, listObjectsV2Info.Objects)

	// The next continuation token has id@node_index format to optimize paginated listing
	nextContinuationToken := listObjectsV2Info.NextContinuationToken
	if nextContinuationToken != "" && listObjectsV2Info.IsTruncated {
		nextContinuationToken = fmt.Sprintf("%s@%d", listObjectsV2Info.NextContinuationToken, getLocalNodeIndex())
	}

	response := generateListObjectsV2Response(bucket, prefix, token, nextContinuationToken, startAfter,
		delimiter, encodingType, fetchOwner, listObjectsV2Info.IsTruncated,
		maxKeys, listObjectsV2Info.Objects, listObjectsV2Info.Prefixes, false)

	// Write success response.
	writeSuccessResponseXML(w, encodeResponse(response))
}

func getLocalNodeIndex() int {
	if len(globalProxyEndpoints) == 0 {
		return -1
	}
	for i, ep := range globalProxyEndpoints {
		if ep.IsLocal {
			return i
		}
	}
	return -1
}

func parseRequestToken(token string) (subToken string, nodeIndex int) {
	if token == "" {
		return token, -1
	}
	i := strings.Index(token, "@")
	if i < 0 {
		return token, -1
	}
	nodeIndex, err := strconv.Atoi(token[i+1:])
	if err != nil {
		return token, -1
	}
	subToken = token[:i]
	return subToken, nodeIndex
}

func proxyRequestByToken(ctx context.Context, w http.ResponseWriter, r *http.Request, token string) (string, bool) {
	subToken, nodeIndex := parseRequestToken(token)
	if nodeIndex > 0 {
		return subToken, proxyRequestByNodeIndex(ctx, w, r, nodeIndex)
	}
	return subToken, false
}

func proxyRequestByNodeIndex(ctx context.Context, w http.ResponseWriter, r *http.Request, index int) (success bool) {
	if len(globalProxyEndpoints) == 0 {
		return false
	}
	if index < 0 || index >= len(globalProxyEndpoints) {
		return false
	}
	ep := globalProxyEndpoints[index]
	if ep.IsLocal {
		return false
	}
	return proxyRequest(ctx, w, r, ep)
}

func proxyRequestByStringHash(ctx context.Context, w http.ResponseWriter, r *http.Request, str string) (success bool) {
	return proxyRequestByNodeIndex(ctx, w, r, crcHashMod(str, len(globalProxyEndpoints)))
}

// ListObjectsV1Handler - GET Bucket (List Objects) Version 1.
// --------------------------
// This implementation of the GET operation returns some or all (up to 10000)
// of the objects in a bucket. You can use the request parameters as selection
// criteria to return a subset of the objects in a bucket.
//
func (api objectAPIHandlers) ListObjectsV1Handler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListObjectsV1")

	defer logger.AuditLog(w, r, "ListObjectsV1", mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.ListBucketAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	// Extract all the litsObjectsV1 query params to their native values.
	prefix, marker, delimiter, maxKeys, encodingType, s3Error := getListObjectsV1Args(r.URL.Query())
	if s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	// Validate all the query params before beginning to serve the request.
	if s3Error := validateListObjectsArgs(marker, delimiter, encodingType, maxKeys); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	// Forward the request using Source IP or bucket
	forwardStr := handlers.GetSourceIPFromHeaders(r)
	if forwardStr == "" {
		forwardStr = bucket
	}
	if proxyRequestByStringHash(ctx, w, r, forwardStr) {
		return
	}

	listObjects := objectAPI.ListObjects

	// Inititate a list objects operation based on the input params.
	// On success would return back ListObjectsInfo object to be
	// marshaled into S3 compatible XML header.
	listObjectsInfo, err := listObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	concurrentDecryptETag(ctx, listObjectsInfo.Objects)

	response := generateListObjectsV1Response(bucket, prefix, marker, delimiter, encodingType, maxKeys, listObjectsInfo)

	// Write success response.
	writeSuccessResponseXML(w, encodeResponse(response))
}
