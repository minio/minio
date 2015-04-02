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
	"github.com/minio-io/iodine"
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
			httpRange, err := getRequestedRange(req, metadata.Size)
			if err != nil {
				writeErrorResponse(w, req, InvalidRange, acceptsContentType, req.URL.Path)
				return
			}
			switch httpRange.start == 0 && httpRange.length == 0 {
			case true:
				{
					setObjectHeaders(w, metadata)
					if _, err := server.driver.GetObject(w, bucket, object); err != nil {
						// unable to write headers, we've already printed data. Just close the connection.
						log.Error.Println(err)
					}
				}
			case false:
				{
					metadata.Size = httpRange.length
					setRangeObjectHeaders(w, metadata, httpRange)
					w.WriteHeader(http.StatusPartialContent)
					_, err := server.driver.GetPartialObject(w, bucket, object, httpRange.start, httpRange.length)
					if err != nil {
						err = iodine.New(err, nil)
						// unable to write headers, we've already printed data. Just close the connection.
						log.Error.Println(err)
					}
				}
			}
		}
	case drivers.ObjectNotFound:
		{
			writeErrorResponse(w, req, NoSuchKey, acceptsContentType, req.URL.Path)
		}
	case drivers.BucketNotFound:
		{
			writeErrorResponse(w, req, NoSuchBucket, acceptsContentType, req.URL.Path)
		}
	case drivers.ObjectNameInvalid:
		{
			writeErrorResponse(w, req, NoSuchKey, acceptsContentType, req.URL.Path)
		}
	case drivers.BucketNameInvalid:
		{
			writeErrorResponse(w, req, InvalidBucketName, acceptsContentType, req.URL.Path)
		}
	case drivers.ImplementationError:
		{
			log.Error.Println(err)
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
		}
	default:
		{
			log.Error.Println(err)
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
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
		{
			setObjectHeaders(w, metadata)
			w.WriteHeader(http.StatusOK)
		}
	case drivers.ObjectNotFound:
		{
			writeErrorResponse(w, req, NoSuchKey, acceptsContentType, req.URL.Path)
		}
	case drivers.ObjectNameInvalid:
		{
			writeErrorResponse(w, req, NoSuchKey, acceptsContentType, req.URL.Path)
		}
	case drivers.ImplementationError:
		{
			log.Error.Println(err)
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
		}
	default:
		{
			log.Error.Println(err)
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
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
		// TODO
		// ----
		// This is handled here instead of router, is only because semantically
		// resource queries are not treated differently by Gorilla mux
		//
		// In-fact a request coming in as /bucket/?policy={} and /bucket/object are
		// treated similarly. A proper fix would be to remove this comment and
		// find a right regex pattern for individual requests
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
		w.WriteHeader(http.StatusOK)
	case drivers.ImplementationError:
		{
			log.Error.Println(err)
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
		}
	case drivers.BucketNotFound:
		{
			writeErrorResponse(w, req, NoSuchBucket, acceptsContentType, req.URL.Path)
		}
	case drivers.BucketNameInvalid:
		{
			writeErrorResponse(w, req, InvalidBucketName, acceptsContentType, req.URL.Path)
		}
	case drivers.ObjectExists:
		{
			writeErrorResponse(w, req, NotImplemented, acceptsContentType, req.URL.Path)
		}
	case drivers.BadDigest:
		{
			writeErrorResponse(w, req, BadDigest, acceptsContentType, req.URL.Path)
		}
	case drivers.InvalidDigest:
		{
			writeErrorResponse(w, req, InvalidDigest, acceptsContentType, req.URL.Path)
		}
	default:
		{
			log.Error.Println(err)
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
		}
	}
}
