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
	// Marker is set validate pre-condition.
	if marker != "" {
		// Marker not common with prefix is not implemented.
		if !hasPrefix(marker, prefix) {
			return ErrInvalidPrefixMarker
		}
	}
	// Success.
	return ErrNone
}

// Validate all the ListObjectsV2 query arguments, returns an APIErrorCode
// if one of the args do not meet the required conditions.
// Special conditions required by Minio server are as below
// - delimiter if set should be equal to '/', otherwise the request is rejected.
func validateGatewayListObjectsV2Args(prefix, marker, delimiter string, maxKeys int) APIErrorCode {
	// Max keys cannot be negative.
	if maxKeys < 0 {
		return ErrInvalidMaxKeys
	}

	/// Minio special conditions for ListObjects.

	// Verify if delimiter is anything other than '/', which we do not support.
	if delimiter != "" && delimiter != "/" {
		return ErrNotImplemented
	}

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
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	if s3Error := checkRequestAuthType(r, bucket, "s3:ListBucket", globalServerConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	// Extract all the listObjectsV2 query params to their native values.
	prefix, token, startAfter, delimiter, fetchOwner, maxKeys, _ := getListObjectsV2Args(r.URL.Query())

	// In ListObjectsV2 'continuation-token' is the marker.
	marker := token
	// Check if 'continuation-token' is empty.
	if token == "" {
		// Then we need to use 'start-after' as marker instead.
		marker = startAfter
	}
	// Validate the query params before beginning to serve the request.
	// fetch-owner is not validated since it is a boolean
	if s3Error := validateListObjectsArgs(prefix, marker, delimiter, maxKeys); s3Error != ErrNone {
		// return empty response if invalid marker
		//TODO: avoid this pattern when moving to errors package
		if s3Error == ErrInvalidPrefixMarker {
			listObjectsInfo := ListObjectsInfo{}
			response := generateListObjectsV2Response(bucket, prefix, token, marker, startAfter, delimiter, fetchOwner, listObjectsInfo.IsTruncated, maxKeys, listObjectsInfo.Objects, listObjectsInfo.Prefixes)
			writeSuccessResponseXML(w, encodeResponse(response))
			return
		}
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	// Inititate a list objects operation based on the input params.
	// On success would return back ListObjectsInfo object to be
	// marshalled into S3 compatible XML header.
	listObjectsInfo, err := objectAPI.ListObjects(bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	response := generateListObjectsV2Response(bucket, prefix, token, listObjectsInfo.NextMarker, startAfter, delimiter, fetchOwner, listObjectsInfo.IsTruncated, maxKeys, listObjectsInfo.Objects, listObjectsInfo.Prefixes)

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
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	if s3Error := checkRequestAuthType(r, bucket, "s3:ListBucket", globalServerConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	// Extract all the litsObjectsV1 query params to their native values.
	prefix, marker, delimiter, maxKeys, _ := getListObjectsV1Args(r.URL.Query())

	// Validate all the query params before beginning to serve the request.
	if s3Error := validateListObjectsArgs(prefix, marker, delimiter, maxKeys); s3Error != ErrNone {
		// return empty response if invalid marker
		//TODO: avoid this pattern when moving to errors package
		if s3Error == ErrInvalidPrefixMarker {
			response := generateListObjectsV1Response(bucket, prefix, marker, delimiter, maxKeys, ListObjectsInfo{})
			writeSuccessResponseXML(w, encodeResponse(response))
			return
		}
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	// Inititate a list objects operation based on the input params.
	// On success would return back ListObjectsInfo object to be
	// marshalled into S3 compatible XML header.
	listObjectsInfo, err := objectAPI.ListObjects(bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}
	response := generateListObjectsV1Response(bucket, prefix, marker, delimiter, maxKeys, listObjectsInfo)

	// Write success response.
	writeSuccessResponseXML(w, encodeResponse(response))
}
