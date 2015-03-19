/*
 * Mini Object Storage, (C) 2015 Minio, Inc.
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
	"log"
	"net/http"

	"github.com/gorilla/mux"
	mstorage "github.com/minio-io/minio/pkg/storage"
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
	objects, resources, err := server.storage.ListObjects(bucket, resources)
	switch err := err.(type) {
	case nil: // success
		{
			response := generateObjectsListResult(bucket, objects, resources)
			w.Write(writeObjectHeadersAndResponse(w, response, acceptsContentType))
		}
	case mstorage.BucketNotFound:
		{
			error := errorCodeError(NoSuchBucket)
			errorResponse := getErrorResponse(error, bucket)
			w.WriteHeader(error.HTTPStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case mstorage.ImplementationError:
		{
			// Embed error log on server side
			log.Println(err)
			error := errorCodeError(InternalError)
			errorResponse := getErrorResponse(error, bucket)
			w.WriteHeader(error.HTTPStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case mstorage.BucketNameInvalid:
		{
			error := errorCodeError(InvalidBucketName)
			errorResponse := getErrorResponse(error, bucket)
			w.WriteHeader(error.HTTPStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case mstorage.ObjectNameInvalid:
		{
			error := errorCodeError(NoSuchKey)
			errorResponse := getErrorResponse(error, resources.Prefix)
			w.WriteHeader(error.HTTPStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	}
}

// GET Service
// -----------
// This implementation of the GET operation returns a list of all buckets
// owned by the authenticated sender of the request.
func (server *minioAPI) listBucketsHandler(w http.ResponseWriter, req *http.Request) {
	acceptsContentType := getContentType(req)
	buckets, err := server.storage.ListBuckets()
	switch err := err.(type) {
	case nil:
		{
			response := generateBucketsListResult(buckets)
			w.Write(writeObjectHeadersAndResponse(w, response, acceptsContentType))
		}
	case mstorage.ImplementationError:
		{
			log.Println(err)
			error := errorCodeError(InternalError)
			errorResponse := getErrorResponse(error, "")
			w.WriteHeader(error.HTTPStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case mstorage.BackendCorrupted:
		{
			log.Println(err)
			error := errorCodeError(InternalError)
			errorResponse := getErrorResponse(error, "")
			w.WriteHeader(error.HTTPStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	}
}

// PUT Bucket
// ----------
// This implementation of the PUT operation creates a new bucket for authenticated request
func (server *minioAPI) putBucketHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]
	err := server.storage.CreateBucket(bucket)

	resources := getBucketResources(req.URL.Query())
	if resources.Policy == true {
		server.putBucketPolicyHandler(w, req)
		return
	}

	acceptsContentType := getContentType(req)
	switch err := err.(type) {
	case nil:
		{
			w.Header().Set("Server", "Minio")
			w.Header().Set("Connection", "close")
		}
	case mstorage.BucketNameInvalid:
		{
			error := errorCodeError(InvalidBucketName)
			errorResponse := getErrorResponse(error, bucket)
			w.WriteHeader(error.HTTPStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case mstorage.BucketExists:
		{
			error := errorCodeError(BucketAlreadyExists)
			errorResponse := getErrorResponse(error, bucket)
			w.WriteHeader(error.HTTPStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case mstorage.ImplementationError:
		{
			// Embed errors log on server side
			log.Println(err)
			error := errorCodeError(InternalError)
			errorResponse := getErrorResponse(error, bucket)
			w.WriteHeader(error.HTTPStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	}
}
