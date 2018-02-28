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
	"strings"

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
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	cred, s3Error := checkRequestAuthType(r, bucket, "s3:ListBucket", globalServerConfig.GetRegion())
	if s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	// Extract all the listObjectsV2 query params to their native values.
	prefix, token, startAfter, delimiter, fetchOwner, maxKeys, _ := getListObjectsV2Args(r.URL.Query())
	restrictedPrefix, valid := cred.GetValidFilterPrefix(bucket, prefix)
	if !valid {
		writeErrorResponse(w, ErrAccessDenied, r.URL)
		return
	}

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
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	// Inititate a list objects operation based on the input params.
	// On success would return back ListObjectsInfo object to be
	// marshalled into S3 compatible XML header.
	listObjectsV2Info, err := objectAPI.ListObjectsV2(bucket, prefix, marker, delimiter, maxKeys, fetchOwner, startAfter)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	if cred.Scope != "" {
		var objects []ObjectInfo
		for _, object := range listObjectsV2Info.Objects {
			if strings.HasPrefix(object.Name, restrictedPrefix) {
				objects = append(objects, object)
			}
		}
		listObjectsV2Info.Objects = objects
		var prefixes []string
		for _, prefix := range listObjectsV2Info.Prefixes {
			if strings.HasPrefix(prefix, restrictedPrefix) {
				prefixes = append(prefixes, prefix)
			}
		}
		listObjectsV2Info.Prefixes = prefixes
	}

	response := generateListObjectsV2Response(bucket, prefix, token, listObjectsV2Info.NextContinuationToken, startAfter, delimiter, fetchOwner, listObjectsV2Info.IsTruncated, maxKeys, listObjectsV2Info.Objects, listObjectsV2Info.Prefixes)

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

	cred, s3Error := checkRequestAuthType(r, bucket, "s3:ListBucket", globalServerConfig.GetRegion())
	if s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	// Extract all the litsObjectsV1 query params to their native values.
	prefix, marker, delimiter, maxKeys, _ := getListObjectsV1Args(r.URL.Query())
	restrictedPrefix, valid := cred.GetValidFilterPrefix(bucket, prefix)
	if !valid {
		writeErrorResponse(w, ErrAccessDenied, r.URL)
		return
	}

	// Validate the maxKeys lowerbound. When maxKeys > 1000, S3 returns 1000 but
	// does not throw an error.
	if maxKeys < 0 {
		writeErrorResponse(w, ErrInvalidMaxKeys, r.URL)
		return
	} // Validate all the query params before beginning to serve the request.
	if s3Error := validateListObjectsArgs(prefix, marker, delimiter, maxKeys); s3Error != ErrNone {
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
	if cred.Scope != "" {
		var objects []ObjectInfo
		for _, object := range listObjectsInfo.Objects {
			if strings.HasPrefix(object.Name, restrictedPrefix) {
				objects = append(objects, object)
			}
		}
		listObjectsInfo.Objects = objects
		var prefixes []string
		for _, prefix := range listObjectsInfo.Prefixes {
			if strings.HasPrefix(prefix, restrictedPrefix) {
				prefixes = append(prefixes, prefix)
			}
		}
		listObjectsInfo.Prefixes = prefixes
	}
	response := generateListObjectsV1Response(bucket, prefix, marker, delimiter, maxKeys, listObjectsInfo)

	// Write success response.
	writeSuccessResponseXML(w, encodeResponse(response))
}
