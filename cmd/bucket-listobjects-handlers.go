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
	"net/http"
	"strconv"
	"strings"

	"github.com/minio/minio/internal/logger"
	"github.com/minio/mux"

	"github.com/minio/pkg/v3/policy"
)

// Validate all the ListObjects query arguments, returns an APIErrorCode
// if one of the args do not meet the required conditions.
// Special conditions required by MinIO server are as below
//   - delimiter if set should be equal to '/', otherwise the request is rejected.
//   - marker if set should have a common prefix with 'prefix' param, otherwise
//     the request is rejected.
func validateListObjectsArgs(prefix, marker, delimiter, encodingType string, maxKeys int) APIErrorCode {
	// Max keys cannot be negative.
	if maxKeys < 0 {
		return ErrInvalidMaxKeys
	}

	if encodingType != "" {
		// AWS S3 spec only supports 'url' encoding type
		if !strings.EqualFold(encodingType, "url") {
			return ErrInvalidEncodingMethod
		}
	}

	if !IsValidObjectPrefix(prefix) {
		return ErrInvalidObjectName
	}

	if marker != "" && !HasPrefix(marker, prefix) {
		return ErrNotImplemented
	}

	return ErrNone
}

func (api objectAPIHandlers) ListObjectVersionsHandler(w http.ResponseWriter, r *http.Request) {
	api.listObjectVersionsHandler(w, r, false)
}

func (api objectAPIHandlers) ListObjectVersionsMHandler(w http.ResponseWriter, r *http.Request) {
	api.listObjectVersionsHandler(w, r, true)
}

// ListObjectVersionsHandler - GET Bucket Object versions
// You can use the versions subresource to list metadata about all
// of the versions of objects in a bucket.
func (api objectAPIHandlers) listObjectVersionsHandler(w http.ResponseWriter, r *http.Request, metadata bool) {
	ctx := newContext(r, w, "ListObjectVersions")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.ListBucketVersionsAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}
	var checkObjMeta metaCheckFn
	if metadata {
		checkObjMeta = func(name string, action policy.Action) (s3Err APIErrorCode) {
			return checkRequestAuthType(ctx, r, action, bucket, name)
		}
	}
	urlValues := r.Form

	// Extract all the listBucketVersions query params to their native values.
	prefix, marker, delimiter, maxkeys, encodingType, versionIDMarker, errCode := getListBucketObjectVersionsArgs(urlValues)
	if errCode != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(errCode), r.URL)
		return
	}

	// Validate the query params before beginning to serve the request.
	if s3Error := validateListObjectsArgs(prefix, marker, delimiter, encodingType, maxkeys); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	listObjectVersions := objectAPI.ListObjectVersions

	// Initiate a list object versions operation based on the input params.
	// On success would return back ListObjectsInfo object to be
	// marshaled into S3 compatible XML header.
	listObjectVersionsInfo, err := listObjectVersions(ctx, bucket, prefix, marker, versionIDMarker, delimiter, maxkeys)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	if err = DecryptETags(ctx, GlobalKMS, listObjectVersionsInfo.Objects); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	response := generateListVersionsResponse(ctx, bucket, prefix, marker, versionIDMarker, delimiter, encodingType, maxkeys, listObjectVersionsInfo, checkObjMeta)

	// Write success response.
	writeSuccessResponseXML(w, encodeResponseList(response))
}

// ListObjectsV2MHandler - GET Bucket (List Objects) Version 2 with metadata.
// --------------------------
// This implementation of the GET operation returns some or all (up to 1000)
// of the objects in a bucket. You can use the request parameters as selection
// criteria to return a subset of the objects in a bucket.
//
// NOTE: It is recommended that this API to be used for application development.
// MinIO continues to support ListObjectsV1 and V2 for supporting legacy tools.
func (api objectAPIHandlers) ListObjectsV2MHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListObjectsV2M")
	api.listObjectsV2Handler(ctx, w, r, true)
}

// ListObjectsV2Handler - GET Bucket (List Objects) Version 2.
// --------------------------
// This implementation of the GET operation returns some or all (up to 1000)
// of the objects in a bucket. You can use the request parameters as selection
// criteria to return a subset of the objects in a bucket.
//
// NOTE: It is recommended that this API to be used for application development.
// MinIO continues to support ListObjectsV1 for supporting legacy tools.
func (api objectAPIHandlers) ListObjectsV2Handler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListObjectsV2")
	api.listObjectsV2Handler(ctx, w, r, false)
}

// listObjectsV2Handler performs listing either with or without extra metadata.
func (api objectAPIHandlers) listObjectsV2Handler(ctx context.Context, w http.ResponseWriter, r *http.Request, metadata bool) {
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.ListBucketAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	var checkObjMeta metaCheckFn
	if metadata {
		checkObjMeta = func(name string, action policy.Action) (s3Err APIErrorCode) {
			return checkRequestAuthType(ctx, r, action, bucket, name)
		}
	}
	urlValues := r.Form

	// Extract all the listObjectsV2 query params to their native values.
	prefix, token, startAfter, delimiter, fetchOwner, maxKeys, encodingType, errCode := getListObjectsV2Args(urlValues)
	if errCode != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(errCode), r.URL)
		return
	}

	// Validate the query params before beginning to serve the request.
	if s3Error := validateListObjectsArgs(prefix, token, delimiter, encodingType, maxKeys); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	var (
		listObjectsV2Info ListObjectsV2Info
		err               error
	)

	if r.Header.Get(xMinIOExtract) == "true" && strings.Contains(prefix, archivePattern) {
		// Initiate a list objects operation inside a zip file based in the input params
		listObjectsV2Info, err = listObjectsV2InArchive(ctx, objectAPI, bucket, prefix, token, delimiter, maxKeys, startAfter, r.Header)
	} else {
		// Initiate a list objects operation based on the input params.
		// On success would return back ListObjectsInfo object to be
		// marshaled into S3 compatible XML header.
		listObjectsV2Info, err = objectAPI.ListObjectsV2(ctx, bucket, prefix, token, delimiter, maxKeys, fetchOwner, startAfter)
	}
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	if err = DecryptETags(ctx, GlobalKMS, listObjectsV2Info.Objects); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	response := generateListObjectsV2Response(ctx, bucket, prefix, token, listObjectsV2Info.NextContinuationToken, startAfter,
		delimiter, encodingType, fetchOwner, listObjectsV2Info.IsTruncated,
		maxKeys, listObjectsV2Info.Objects, listObjectsV2Info.Prefixes, checkObjMeta)

	// Write success response.
	writeSuccessResponseXML(w, encodeResponseList(response))
}

func parseRequestToken(token string) (subToken string, nodeIndex int) {
	if token == "" {
		return token, -1
	}
	i := strings.Index(token, getKeySeparator())
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

func proxyRequestByToken(ctx context.Context, w http.ResponseWriter, r *http.Request, token string, returnErr bool) (subToken string, proxied bool, success bool) {
	var nodeIndex int
	if subToken, nodeIndex = parseRequestToken(token); nodeIndex >= 0 {
		proxied, success = proxyRequestByNodeIndex(ctx, w, r, nodeIndex, returnErr)
	}
	return
}

func proxyRequestByNodeIndex(ctx context.Context, w http.ResponseWriter, r *http.Request, index int, returnErr bool) (proxied, success bool) {
	if len(globalProxyEndpoints) == 0 {
		return
	}
	if index < 0 || index >= len(globalProxyEndpoints) {
		return
	}
	ep := globalProxyEndpoints[index]
	if ep.IsLocal {
		return
	}
	return true, proxyRequest(ctx, w, r, ep, returnErr)
}

// ListObjectsV1Handler - GET Bucket (List Objects) Version 1.
// --------------------------
// This implementation of the GET operation returns some or all (up to 1000)
// of the objects in a bucket. You can use the request parameters as selection
// criteria to return a subset of the objects in a bucket.
func (api objectAPIHandlers) ListObjectsV1Handler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListObjectsV1")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.ListBucketAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	// Extract all the listObjectsV1 query params to their native values.
	prefix, marker, delimiter, maxKeys, encodingType, s3Error := getListObjectsV1Args(r.Form)
	if s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	// Validate all the query params before beginning to serve the request.
	if s3Error := validateListObjectsArgs(prefix, marker, delimiter, encodingType, maxKeys); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	listObjects := objectAPI.ListObjects

	// Initiate a list objects operation based on the input params.
	// On success would return back ListObjectsInfo object to be
	// marshaled into S3 compatible XML header.
	listObjectsInfo, err := listObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	if err = DecryptETags(ctx, GlobalKMS, listObjectsInfo.Objects); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	response := generateListObjectsV1Response(ctx, bucket, prefix, marker, delimiter, encodingType, maxKeys, listObjectsInfo)

	// Write success response.
	writeSuccessResponseXML(w, encodeResponseList(response))
}
