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

// GET Object
// ----------
// This implementation of the GET operation retrieves object. To use GET,
// you must have READ access to the object.
func (server *minioAPI) getObjectHandler(w http.ResponseWriter, req *http.Request) {
	var object, bucket string
	acceptsContentType := getContentType(req)
	vars := mux.Vars(req)
	bucket = vars["bucket"]
	object = vars["object"]

	metadata, err := server.driver.GetObjectMetadata(bucket, object, "")
	switch err := err.(type) {
	case nil: // success
		{
			log.Println("Found: " + bucket + "#" + object)
			httpRange, err := newRange(req, metadata.Size)
			if err != nil {
				log.Errorln(err)
				error := errorCodeError(InvalidRange)
				errorResponse := getErrorResponse(error, "/"+bucket+"/"+object)
				w.WriteHeader(error.HTTPStatusCode)
				w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
				return
			}
			switch httpRange.start == 0 && httpRange.length == 0 {
			case true:
				writeObjectHeaders(w, metadata)
				if _, err := server.driver.GetObject(w, bucket, object); err != nil {
					log.Errorln(err)
					error := errorCodeError(InternalError)
					errorResponse := getErrorResponse(error, "/"+bucket+"/"+object)
					w.WriteHeader(error.HTTPStatusCode)
					w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
					return
				}
			case false:
				metadata.Size = httpRange.length
				writeRangeObjectHeaders(w, metadata, httpRange.getContentRange())
				w.WriteHeader(http.StatusPartialContent)
				_, err := server.driver.GetPartialObject(w, bucket, object, httpRange.start, httpRange.length)
				if err != nil {
					log.Errorln(err)
					error := errorCodeError(InternalError)
					errorResponse := getErrorResponse(error, "/"+bucket+"/"+object)
					w.WriteHeader(error.HTTPStatusCode)
					w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
					return
				}

			}
		}
	case drivers.ObjectNotFound:
		{
			error := errorCodeError(NoSuchKey)
			errorResponse := getErrorResponse(error, "/"+bucket+"/"+object)
			w.WriteHeader(error.HTTPStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case drivers.BucketNotFound:
		{
			error := errorCodeError(NoSuchBucket)
			errorResponse := getErrorResponse(error, "/"+bucket+"/"+object)
			w.WriteHeader(error.HTTPStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case drivers.ObjectNameInvalid:
		{
			error := errorCodeError(NoSuchKey)
			errorResponse := getErrorResponse(error, "/"+bucket+"/"+object)
			w.WriteHeader(error.HTTPStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case drivers.BucketNameInvalid:
		{
			error := errorCodeError(InvalidBucketName)
			errorResponse := getErrorResponse(error, "/"+bucket+"/"+object)
			w.WriteHeader(error.HTTPStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	default:
		{
			// Embed errors log on serve side
			log.Errorln(err)
			error := errorCodeError(InternalError)
			errorResponse := getErrorResponse(error, "/"+bucket+"/"+object)
			w.WriteHeader(error.HTTPStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	}
}

// HEAD Object
// -----------
// The HEAD operation retrieves metadata from an object without returning the object itself.
func (server *minioAPI) headObjectHandler(w http.ResponseWriter, req *http.Request) {
	var object, bucket string
	acceptsContentType := getContentType(req)
	vars := mux.Vars(req)
	bucket = vars["bucket"]
	object = vars["object"]

	metadata, err := server.driver.GetObjectMetadata(bucket, object, "")
	switch err := err.(type) {
	case nil:
		writeObjectHeaders(w, metadata)
	case drivers.ObjectNotFound:
		{
			error := errorCodeError(NoSuchKey)
			errorResponse := getErrorResponse(error, "/"+bucket+"/"+object)
			w.WriteHeader(error.HTTPStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case drivers.ObjectNameInvalid:
		{
			error := errorCodeError(NoSuchKey)
			errorResponse := getErrorResponse(error, "/"+bucket+"/"+object)
			w.WriteHeader(error.HTTPStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case drivers.ImplementationError:
		{
			// Embed error log on server side
			log.Errorln(err)
			error := errorCodeError(InternalError)
			errorResponse := getErrorResponse(error, "/"+bucket+"/"+object)
			w.WriteHeader(error.HTTPStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	}
}

// PUT Object
// ----------
// This implementation of the PUT operation adds an object to a bucket.
func (server *minioAPI) putObjectHandler(w http.ResponseWriter, req *http.Request) {
	var object, bucket string
	vars := mux.Vars(req)
	acceptsContentType := getContentType(req)
	bucket = vars["bucket"]
	object = vars["object"]

	resources := getBucketResources(req.URL.Query())
	if resources.Policy == true && object == "" {
		server.putBucketPolicyHandler(w, req)
		return
	}

	// get Content-MD5 sent by client
	md5 := req.Header.Get("Content-MD5")
	err := server.driver.CreateObject(bucket, object, "", md5, req.Body)
	switch err := err.(type) {
	case nil:
		w.Header().Set("Server", "Minio")
		w.Header().Set("Connection", "close")
	case drivers.ImplementationError:
		{
			// Embed error log on server side
			log.Errorln(err)
			error := errorCodeError(InternalError)
			errorResponse := getErrorResponse(error, "/"+bucket+"/"+object)
			w.WriteHeader(error.HTTPStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case drivers.BucketNotFound:
		{
			error := errorCodeError(NoSuchBucket)
			errorResponse := getErrorResponse(error, "/"+bucket+"/"+object)
			w.WriteHeader(error.HTTPStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case drivers.BucketNameInvalid:
		{
			error := errorCodeError(InvalidBucketName)
			errorResponse := getErrorResponse(error, "/"+bucket+"/"+object)
			w.WriteHeader(error.HTTPStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case drivers.ObjectExists:
		{
			error := errorCodeError(NotImplemented)
			errorResponse := getErrorResponse(error, "/"+bucket+"/"+object)
			w.WriteHeader(error.HTTPStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case drivers.BadDigest:
		{
			error := errorCodeError(BadDigest)
			errorResponse := getErrorResponse(error, "/"+bucket+"/"+object)
			w.WriteHeader(error.HTTPStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case drivers.InvalidDigest:
		{
			error := errorCodeError(InvalidDigest)
			errorResponse := getErrorResponse(error, "/"+bucket+"/"+object)
			w.WriteHeader(error.HTTPStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	}

}
