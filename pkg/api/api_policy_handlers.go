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
	"github.com/minio-io/minio/pkg/iodine"
	"github.com/minio-io/minio/pkg/storage/drivers"
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
		writeErrorResponse(w, req, InvalidPolicyDocument, acceptsContentType, req.URL.Path)
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
			writeErrorResponse(w, req, InvalidBucketName, acceptsContentType, req.URL.Path)
		}
	case drivers.BucketNotFound:
		{
			writeErrorResponse(w, req, NoSuchBucket, acceptsContentType, req.URL.Path)
		}
	case drivers.BackendCorrupted:
		{
			log.Error.Println(err)
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
		}
	case drivers.ImplementationError:
		{
			log.Error.Println(err)
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
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
			responsePolicy, err := json.Marshal(p)
			if err != nil {
				// log error
				log.Error.Println(iodine.New(err, nil))
				writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
				return
			}
			setCommonHeaders(w, getContentTypeString(acceptsContentType))
			w.Header().Set("Connection", "keep-alive")
			w.WriteHeader(http.StatusOK)
			w.Write(responsePolicy)
		}
	case drivers.BucketNameInvalid:
		{
			writeErrorResponse(w, req, InvalidBucketName, acceptsContentType, req.URL.Path)
		}
	case drivers.BucketNotFound:
		{
			writeErrorResponse(w, req, NoSuchBucket, acceptsContentType, req.URL.Path)
		}
	case drivers.BucketPolicyNotFound:
		{
			writeErrorResponse(w, req, NoSuchBucketPolicy, acceptsContentType, req.URL.Path)
		}
	case drivers.BackendCorrupted:
		{
			log.Error.Println(err)
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
		}
	case drivers.ImplementationError:
		{
			log.Error.Println(err)
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
		}
	}
}
