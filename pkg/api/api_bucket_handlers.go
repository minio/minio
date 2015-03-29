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
	"net/http"

	"github.com/gorilla/mux"
	"github.com/minio-io/minio/pkg/drivers"
	"github.com/minio-io/minio/pkg/utils/log"
)

// GET Bucket (List Objects)
// -------------------------
// This implementation of the GET operation returns some or all (up to 1000)
// of the objects in a bucket. You can use the request parameters as selection
// criteria to return a subset of the objects in a bucket.
//
func (server *minioAPI) listObjectsHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]

	resources := getBucketResources(req.URL.Query())
	if resources.Policy == true {
		server.getBucketPolicyHandler(w, req)
		return
	}

	if resources.Maxkeys == 0 {
		resources.Maxkeys = maxObjectList
	}

	acceptsContentType := getContentType(req)
	objects, resources, err := server.driver.ListObjects(bucket, resources)
	switch err.(type) {
	case nil: // success
		{
			// write headers
			setCommonHeaders(w, getContentTypeString(acceptsContentType))
			w.WriteHeader(http.StatusOK)
			// write body
			response := generateObjectsListResult(bucket, objects, resources)
			encodedResponse := encodeResponse(response, acceptsContentType)
			w.Write(encodedResponse)
		}
	case drivers.BucketNotFound:
		{
			// get error
			error := getErrorCode(NoSuchBucket)
			errorResponse := getErrorResponse(error, bucket)

			// write headers
			setCommonHeaders(w, getContentTypeString(acceptsContentType))
			w.WriteHeader(error.HTTPStatusCode)

			// write body
			encodedErrorResponse := encodeErrorResponse(errorResponse, acceptsContentType)
			w.Write(encodedErrorResponse)
		}
	case drivers.ImplementationError:
		{
			// get error
			error := getErrorCode(InternalError)
			errorResponse := getErrorResponse(error, bucket)

			// write headers
			setCommonHeaders(w, getContentTypeString(acceptsContentType))
			w.WriteHeader(error.HTTPStatusCode)

			// write body
			encodedErrorResponse := encodeErrorResponse(errorResponse, acceptsContentType)
			w.Write(encodedErrorResponse)
		}
	case drivers.BucketNameInvalid:
		{
			error := getErrorCode(InvalidBucketName)
			errorResponse := getErrorResponse(error, bucket)

			// write headers
			setCommonHeaders(w, getContentTypeString(acceptsContentType))
			w.WriteHeader(error.HTTPStatusCode)

			// write body
			encodedErrorResponse := encodeErrorResponse(errorResponse, acceptsContentType)
			w.Write(encodedErrorResponse)
		}
	case drivers.ObjectNameInvalid:
		{
			error := getErrorCode(NoSuchKey)
			errorResponse := getErrorResponse(error, resources.Prefix)

			// write headers
			setCommonHeaders(w, getContentTypeString(acceptsContentType))
			w.WriteHeader(error.HTTPStatusCode)

			// write body
			encodedErrorResponse := encodeErrorResponse(errorResponse, acceptsContentType)
			w.Write(encodedErrorResponse)
		}
	}
}

// GET Service
// -----------
// This implementation of the GET operation returns a list of all buckets
// owned by the authenticated sender of the request.
func (server *minioAPI) listBucketsHandler(w http.ResponseWriter, req *http.Request) {
	acceptsContentType := getContentType(req)
	buckets, err := server.driver.ListBuckets()
	switch err := err.(type) {
	case nil:
		{
			response := generateBucketsListResult(buckets)
			// write headers
			setCommonHeaders(w, getContentTypeString(acceptsContentType))
			w.WriteHeader(http.StatusOK)
			// write response
			encodedResponse := encodeResponse(response, acceptsContentType)
			w.Write(encodedResponse)
		}
	case drivers.BackendCorrupted:
		{
			log.Error.Println(err)
			// get error
			errorCode := getErrorCode(InternalError)
			errorResponse := getErrorResponse(errorCode, "")
			// write headers
			setCommonHeaders(w, getContentTypeString(acceptsContentType))
			w.WriteHeader(errorCode.HTTPStatusCode)
			// write
			encodedErrorResponse := encodeErrorResponse(errorResponse, acceptsContentType)
			w.Write(encodedErrorResponse)
		}
	default:
		{
			log.Trace.Println(err)
			// get error
			errorCode := getErrorCode(InternalError)
			errorResponse := getErrorResponse(errorCode, "")
			// write headers
			w.WriteHeader(errorCode.HTTPStatusCode)
			// write body
			encodedError := encodeErrorResponse(errorResponse, acceptsContentType)
			w.Write(encodedError)
		}
	}
}

// PUT Bucket
// ----------
// This implementation of the PUT operation creates a new bucket for authenticated request
func (server *minioAPI) putBucketHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]
	err := server.driver.CreateBucket(bucket)

	resources := getBucketResources(req.URL.Query())
	if resources.Policy == true {
		server.putBucketPolicyHandler(w, req)
		return
	}

	acceptsContentType := getContentType(req)
	switch err.(type) {
	case nil:
		{
			w.Header().Set("Server", "Minio")
			w.Header().Set("Connection", "close")
		}
	case drivers.BucketNameInvalid:
		{
			error := getErrorCode(InvalidBucketName)
			errorResponse := getErrorResponse(error, bucket)
			// write header
			setCommonHeaders(w, getContentTypeString(acceptsContentType))
			w.WriteHeader(error.HTTPStatusCode)
			// write object
			encodedResponse := encodeErrorResponse(errorResponse, acceptsContentType)
			w.Write(encodedResponse)
		}
	case drivers.BucketExists:
		{
			error := getErrorCode(BucketAlreadyExists)
			errorResponse := getErrorResponse(error, bucket)
			// write header
			setCommonHeaders(w, getContentTypeString(acceptsContentType))
			w.WriteHeader(error.HTTPStatusCode)
			// write object
			encodedResponse := encodeErrorResponse(errorResponse, acceptsContentType)
			w.Write(encodedResponse)
		}
	case drivers.ImplementationError:
		{
			// Embed errors log on server side
			error := getErrorCode(InternalError)
			errorResponse := getErrorResponse(error, bucket)
			// write header
			setCommonHeaders(w, getContentTypeString(acceptsContentType))
			w.WriteHeader(error.HTTPStatusCode)
			// write object
			encodedResponse := encodeErrorResponse(errorResponse, acceptsContentType)
			w.Write(encodedResponse)
		}
	}
}
