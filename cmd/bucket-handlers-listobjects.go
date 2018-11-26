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
	"net/http"

	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/crypto"
	"github.com/minio/minio/cmd/logger"

	"github.com/minio/minio/pkg/policy"
)

// Validate all the ListObjects query arguments, returns an APIErrorCode
// if one of the args do not meet the required conditions.
// Special conditions required by Minio server are as below
// - delimiter if set should be equal to '/', otherwise the request is rejected.
// - marker if set should have a common prefix with 'prefix' param, otherwise
//   the request is rejected.
func validateListObjectsArgs(prefix, marker, delimiter string, maxKeys int) APIErrorCode {
	// Max keys cannot be negative.
	if maxKeys < 0 {
		return ErrInvalidMaxKeys
	}

	/// Minio special conditions for ListObjects.

	// Verify if delimiter is anything other than '/', which we do not support.
	if delimiter != "" && delimiter != "/" {
		return ErrNotImplemented
	}
	// Success.
	return ErrNone
}

// ListObjectsV2Handler - GET Bucket (List Objects) Version 2.
// --------------------------
// This implementation of the GET operation returns some or all (up to 1000)
// of the objects in a bucket. You can use the request parameters as selection
// criteria to return a subset of the objects in a bucket.
//
// NOTE: It is recommended that this API to be used for application development.
// Minio continues to support ListObjectsV1 for supporting legacy tools.
func (api objectAPIHandlers) ListObjectsV2Handler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListObjectsV2")

	defer logger.AuditLog(w, r, "ListObjectsV2", mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL, guessIsBrowserReq(r))
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.ListBucketAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}

	urlValues := r.URL.Query()

	// Extract all the listObjectsV2 query params to their native values.
	prefix, token, startAfter, delimiter, fetchOwner, maxKeys, _, errCode := getListObjectsV2Args(urlValues)

	if errCode != ErrNone {
		writeErrorResponse(w, errCode, r.URL, guessIsBrowserReq(r))
		return
	}

	// Validate the query params before beginning to serve the request.
	// fetch-owner is not validated since it is a boolean
	if s3Error := validateListObjectsArgs(prefix, token, delimiter, maxKeys); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}
	listObjectsV2 := objectAPI.ListObjectsV2
	if api.CacheAPI() != nil {
		listObjectsV2 = api.CacheAPI().ListObjectsV2
	}
	// Inititate a list objects operation based on the input params.
	// On success would return back ListObjectsInfo object to be
	// marshaled into S3 compatible XML header.
	listObjectsV2Info, err := listObjectsV2(ctx, bucket, prefix, token, delimiter, maxKeys, fetchOwner, startAfter)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	for i := range listObjectsV2Info.Objects {
		var actualSize int64
		if listObjectsV2Info.Objects[i].IsCompressed() {
			// Read the decompressed size from the meta.json.
			actualSize = listObjectsV2Info.Objects[i].GetActualSize()
			if actualSize < 0 {
				writeErrorResponse(w, ErrInvalidDecompressedSize, r.URL, guessIsBrowserReq(r))
				return
			}
			// Set the info.Size to the actualSize.
			listObjectsV2Info.Objects[i].Size = actualSize
		} else if crypto.IsEncrypted(listObjectsV2Info.Objects[i].UserDefined) {
			listObjectsV2Info.Objects[i].ETag = getDecryptedETag(r.Header, listObjectsV2Info.Objects[i], false)
			listObjectsV2Info.Objects[i].Size, err = listObjectsV2Info.Objects[i].DecryptedSize()
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
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
	ctx := newContext(r, w, "ListObjectsV1")

	defer logger.AuditLog(w, r, "ListObjectsV1", mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL, guessIsBrowserReq(r))
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.ListBucketAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}

	// Extract all the litsObjectsV1 query params to their native values.
	prefix, marker, delimiter, maxKeys, _, s3Error := getListObjectsV1Args(r.URL.Query())
	if s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}

	// Validate the maxKeys lowerbound. When maxKeys > 1000, S3 returns 1000 but
	// does not throw an error.
	if maxKeys < 0 {
		writeErrorResponse(w, ErrInvalidMaxKeys, r.URL, guessIsBrowserReq(r))
		return
	} // Validate all the query params before beginning to serve the request.
	if s3Error := validateListObjectsArgs(prefix, marker, delimiter, maxKeys); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}
	listObjects := objectAPI.ListObjects
	if api.CacheAPI() != nil {
		listObjects = api.CacheAPI().ListObjects
	}
	// Inititate a list objects operation based on the input params.
	// On success would return back ListObjectsInfo object to be
	// marshaled into S3 compatible XML header.
	listObjectsInfo, err := listObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	for i := range listObjectsInfo.Objects {
		var actualSize int64
		if listObjectsInfo.Objects[i].IsCompressed() {
			// Read the decompressed size from the meta.json.
			actualSize = listObjectsInfo.Objects[i].GetActualSize()
			if actualSize < 0 {
				writeErrorResponse(w, ErrInvalidDecompressedSize, r.URL, guessIsBrowserReq(r))
				return
			}
			// Set the info.Size to the actualSize.
			listObjectsInfo.Objects[i].Size = actualSize
		} else if crypto.IsEncrypted(listObjectsInfo.Objects[i].UserDefined) {
			listObjectsInfo.Objects[i].ETag = getDecryptedETag(r.Header, listObjectsInfo.Objects[i], false)
			listObjectsInfo.Objects[i].Size, err = listObjectsInfo.Objects[i].DecryptedSize()
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
				return
			}
		}
	}
	response := generateListObjectsV1Response(bucket, prefix, marker, delimiter, maxKeys, listObjectsInfo)

	// Write success response.
	writeSuccessResponseXML(w, encodeResponse(response))
}
