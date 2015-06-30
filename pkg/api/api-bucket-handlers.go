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
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

func (server *minioAPI) isValidOp(w http.ResponseWriter, req *http.Request, acceptsContentType contentType) bool {
	vars := mux.Vars(req)
	bucket := vars["bucket"]
	log.Println(bucket)
	return true
}

// GET Bucket (List Multipart uploads)
// -------------------------
// This operation lists in-progress multipart uploads. An in-progress
// multipart upload is a multipart upload that has been initiated,
// using the Initiate Multipart Upload request, but has not yet been completed or aborted.
// This operation returns at most 1,000 multipart uploads in the response.
//
func (server *minioAPI) listMultipartUploadsHandler(w http.ResponseWriter, req *http.Request) {
	acceptsContentType := getContentType(req)
	log.Println(acceptsContentType)

	resources := getBucketMultipartResources(req.URL.Query())
	if resources.MaxUploads == 0 {
		resources.MaxUploads = maxObjectList
	}

	vars := mux.Vars(req)
	bucket := vars["bucket"]
	log.Println(bucket)
}

// GET Bucket (List Objects)
// -------------------------
// This implementation of the GET operation returns some or all (up to 1000)
// of the objects in a bucket. You can use the request parameters as selection
// criteria to return a subset of the objects in a bucket.
//
func (server *minioAPI) listObjectsHandler(w http.ResponseWriter, req *http.Request) {
	acceptsContentType := getContentType(req)
	// verify if bucket allows this operation
	if !server.isValidOp(w, req, acceptsContentType) {
		return
	}

	if isRequestUploads(req.URL.Query()) {
		server.listMultipartUploadsHandler(w, req)
		return
	}

	resources := getBucketResources(req.URL.Query())
	if resources.Maxkeys == 0 {
		resources.Maxkeys = maxObjectList
	}

	vars := mux.Vars(req)
	bucket := vars["bucket"]
	log.Println(bucket)

}

// GET Service
// -----------
// This implementation of the GET operation returns a list of all buckets
// owned by the authenticated sender of the request.
func (server *minioAPI) listBucketsHandler(w http.ResponseWriter, req *http.Request) {
	acceptsContentType := getContentType(req)
	// uncomment this when we have webcli
	// without access key credentials one cannot list buckets
	// if _, err := stripAuth(req); err != nil {
	//	writeErrorResponse(w, req, AccessDenied, acceptsContentType, req.URL.Path)
	//	return
	// }
	log.Println(acceptsContentType)
}

// PUT Bucket
// ----------
// This implementation of the PUT operation creates a new bucket for authenticated request
func (server *minioAPI) putBucketHandler(w http.ResponseWriter, req *http.Request) {
	acceptsContentType := getContentType(req)
	// uncomment this when we have webcli
	// without access key credentials one cannot create a bucket
	// if _, err := stripAuth(req); err != nil {
	// 	writeErrorResponse(w, req, AccessDenied, acceptsContentType, req.URL.Path)
	//	return
	// }
	if isRequestBucketACL(req.URL.Query()) {
		server.putBucketACLHandler(w, req)
		return
	}
	// read from 'x-amz-acl'
	aclType := getACLType(req)
	if aclType == unsupportedACLType {
		writeErrorResponse(w, req, NotImplemented, acceptsContentType, req.URL.Path)
		return
	}

	vars := mux.Vars(req)
	bucket := vars["bucket"]
	log.Println(bucket)
}

// PUT Bucket ACL
// ----------
// This implementation of the PUT operation modifies the bucketACL for authenticated request
func (server *minioAPI) putBucketACLHandler(w http.ResponseWriter, req *http.Request) {
	acceptsContentType := getContentType(req)
	// read from 'x-amz-acl'
	aclType := getACLType(req)
	if aclType == unsupportedACLType {
		writeErrorResponse(w, req, NotImplemented, acceptsContentType, req.URL.Path)
		return
	}

	vars := mux.Vars(req)
	bucket := vars["bucket"]
	log.Println(bucket)
}

// HEAD Bucket
// ----------
// This operation is useful to determine if a bucket exists.
// The operation returns a 200 OK if the bucket exists and you
// have permission to access it. Otherwise, the operation might
// return responses such as 404 Not Found and 403 Forbidden.
func (server *minioAPI) headBucketHandler(w http.ResponseWriter, req *http.Request) {
	acceptsContentType := getContentType(req)
	log.Println(acceptsContentType)

	vars := mux.Vars(req)
	bucket := vars["bucket"]
	log.Println(bucket)
}
