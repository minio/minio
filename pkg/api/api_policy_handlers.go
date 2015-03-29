/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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

package api

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/minio-io/minio/pkg/drivers"
	"github.com/minio-io/minio/pkg/utils/log"
)

// PUT Bucket policy
// -----------------
// This implementation of the PUT operation uses the policy subresource
// to add to or replace a policy on a bucket
func (server *minioAPI) putBucketPolicyHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]
	acceptsContentType := getContentType(req)

	policy, ok := drivers.Parsepolicy(req.Body)
	if ok == false {
		error := getErrorCode(InvalidPolicyDocument)
		errorResponse := getErrorResponse(error, bucket)
		setCommonHeaders(w, getContentTypeString(acceptsContentType))
		w.WriteHeader(error.HTTPStatusCode)
		w.Write(encodeErrorResponse(errorResponse, acceptsContentType))
		return
	}

	err := server.driver.CreateBucketPolicy(bucket, policy)
	switch err := err.(type) {
	case nil:
		{
			w.WriteHeader(http.StatusNoContent)
			setCommonHeaders(w, getContentTypeString(acceptsContentType))
			w.Header().Set("Connection", "keep-alive")
		}
	case drivers.BucketNameInvalid:
		{
			error := getErrorCode(InvalidBucketName)
			errorResponse := getErrorResponse(error, bucket)
			setCommonHeaders(w, getContentTypeString(acceptsContentType))
			w.WriteHeader(error.HTTPStatusCode)
			w.Write(encodeErrorResponse(errorResponse, acceptsContentType))
		}
	case drivers.BucketNotFound:
		{
			error := getErrorCode(NoSuchBucket)
			errorResponse := getErrorResponse(error, bucket)
			setCommonHeaders(w, getContentTypeString(acceptsContentType))
			w.WriteHeader(error.HTTPStatusCode)
			w.Write(encodeErrorResponse(errorResponse, acceptsContentType))
		}
	case drivers.BackendCorrupted:
		{
			log.Error.Println(err)
			error := getErrorCode(InternalError)
			errorResponse := getErrorResponse(error, bucket)
			setCommonHeaders(w, getContentTypeString(acceptsContentType))
			w.WriteHeader(error.HTTPStatusCode)
			w.Write(encodeErrorResponse(errorResponse, acceptsContentType))
		}
	case drivers.ImplementationError:
		{
			log.Error.Println(err)
			error := getErrorCode(InternalError)
			errorResponse := getErrorResponse(error, bucket)
			setCommonHeaders(w, getContentTypeString(acceptsContentType))
			w.WriteHeader(error.HTTPStatusCode)
			w.Write(encodeErrorResponse(errorResponse, acceptsContentType))
		}
	}
}

// GET Bucket policy
// -----------------
// This implementation of the GET operation uses the policy subresource
// to return the policy of a specified bucket.
func (server *minioAPI) getBucketPolicyHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]
	acceptsContentType := getContentType(req)

	p, err := server.driver.GetBucketPolicy(bucket)
	switch err := err.(type) {
	case nil:
		{
			responsePolicy, ret := json.Marshal(p)
			if ret != nil {
				error := getErrorCode(InternalError)
				errorResponse := getErrorResponse(error, bucket)
				setCommonHeaders(w, getContentTypeString(acceptsContentType))
				w.WriteHeader(error.HTTPStatusCode)
				w.Write(encodeErrorResponse(errorResponse, acceptsContentType))
			}
			setCommonHeaders(w, getContentTypeString(acceptsContentType))
			w.Header().Set("Connection", "keep-alive")
			w.Write(responsePolicy)
		}
	case drivers.BucketNameInvalid:
		{
			error := getErrorCode(InvalidBucketName)
			errorResponse := getErrorResponse(error, bucket)
			setCommonHeaders(w, getContentTypeString(acceptsContentType))
			w.WriteHeader(error.HTTPStatusCode)
			w.Write(encodeErrorResponse(errorResponse, acceptsContentType))
		}
	case drivers.BucketNotFound:
		{
			error := getErrorCode(NoSuchBucket)
			errorResponse := getErrorResponse(error, bucket)
			setCommonHeaders(w, getContentTypeString(acceptsContentType))
			w.WriteHeader(error.HTTPStatusCode)
			w.Write(encodeErrorResponse(errorResponse, acceptsContentType))
		}
	case drivers.BucketPolicyNotFound:
		{
			error := getErrorCode(NoSuchBucketPolicy)
			errorResponse := getErrorResponse(error, bucket)
			setCommonHeaders(w, getContentTypeString(acceptsContentType))
			w.WriteHeader(error.HTTPStatusCode)
			w.Write(encodeErrorResponse(errorResponse, acceptsContentType))
		}
	case drivers.BackendCorrupted:
		{
			log.Error.Println(err)
			error := getErrorCode(InternalError)
			errorResponse := getErrorResponse(error, bucket)
			setCommonHeaders(w, getContentTypeString(acceptsContentType))
			w.WriteHeader(error.HTTPStatusCode)
			w.Write(encodeErrorResponse(errorResponse, acceptsContentType))
		}
	case drivers.ImplementationError:
		{
			log.Error.Println(err)
			error := getErrorCode(InternalError)
			errorResponse := getErrorResponse(error, bucket)
			setCommonHeaders(w, getContentTypeString(acceptsContentType))
			w.WriteHeader(error.HTTPStatusCode)
			w.Write(encodeErrorResponse(errorResponse, acceptsContentType))
		}
	}
}
