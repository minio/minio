/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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
	"github.com/minio/minio/pkg/donut"
	"github.com/minio/minio/pkg/probe"
	"github.com/minio/minio/pkg/utils/log"
)

func (api Minio) isValidOp(w http.ResponseWriter, req *http.Request, acceptsContentType contentType) bool {
	vars := mux.Vars(req)
	bucket := vars["bucket"]

	bucketMetadata, err := api.Donut.GetBucketMetadata(bucket, nil)
	if err == nil {
		if _, err := StripAccessKeyID(req.Header.Get("Authorization")); err != nil {
			if bucketMetadata.ACL.IsPrivate() {
				return true
				//uncomment this when we have webcli
				//writeErrorResponse(w, req, AccessDenied, acceptsContentType, req.URL.Path)
				//return false
			}
			if bucketMetadata.ACL.IsPublicRead() && req.Method == "PUT" {
				return true
				//uncomment this when we have webcli
				//writeErrorResponse(w, req, AccessDenied, acceptsContentType, req.URL.Path)
				//return false
			}
		}
		return true
	}
	switch err.ToGoError().(type) {
	case donut.BucketNotFound:
		writeErrorResponse(w, req, NoSuchBucket, acceptsContentType, req.URL.Path)
		return false
	case donut.BucketNameInvalid:
		writeErrorResponse(w, req, InvalidBucketName, acceptsContentType, req.URL.Path)
		return false
	default:
		log.Error.Println(err.Trace())
		writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
		return false
	}
}

// ListMultipartUploadsHandler - GET Bucket (List Multipart uploads)
// -------------------------
// This operation lists in-progress multipart uploads. An in-progress
// multipart upload is a multipart upload that has been initiated,
// using the Initiate Multipart Upload request, but has not yet been completed or aborted.
// This operation returns at most 1,000 multipart uploads in the response.
//
func (api Minio) ListMultipartUploadsHandler(w http.ResponseWriter, req *http.Request) {
	// Ticket master block
	{
		op := Operation{}
		op.ProceedCh = make(chan struct{})
		api.OP <- op
		// block until ticket master gives us a go
		<-op.ProceedCh
	}

	acceptsContentType := getContentType(req)
	if !api.isValidOp(w, req, acceptsContentType) {
		return
	}

	resources := getBucketMultipartResources(req.URL.Query())
	if resources.MaxUploads < 0 {
		writeErrorResponse(w, req, InvalidMaxUploads, acceptsContentType, req.URL.Path)
		return
	}
	if resources.MaxUploads == 0 {
		resources.MaxUploads = maxObjectList
	}

	vars := mux.Vars(req)
	bucket := vars["bucket"]

	var signature *donut.Signature
	if _, ok := req.Header["Authorization"]; ok {
		// Init signature V4 verification
		var err *probe.Error
		signature, err = InitSignatureV4(req)
		if err != nil {
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
			return
		}
	}

	resources, err := api.Donut.ListMultipartUploads(bucket, resources, signature)
	if err == nil {
		// generate response
		response := generateListMultipartUploadsResponse(bucket, resources)
		encodedSuccessResponse := encodeSuccessResponse(response, acceptsContentType)
		// write headers
		setCommonHeaders(w, getContentTypeString(acceptsContentType), len(encodedSuccessResponse))
		// write body
		w.Write(encodedSuccessResponse)
		return
	}
	switch err.ToGoError().(type) {
	case donut.SignatureDoesNotMatch:
		writeErrorResponse(w, req, SignatureDoesNotMatch, acceptsContentType, req.URL.Path)
	case donut.BucketNotFound:
		writeErrorResponse(w, req, NoSuchBucket, acceptsContentType, req.URL.Path)
	default:
		log.Error.Println(err.Trace())
		writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
	}
}

// ListObjectsHandler - GET Bucket (List Objects)
// -------------------------
// This implementation of the GET operation returns some or all (up to 1000)
// of the objects in a bucket. You can use the request parameters as selection
// criteria to return a subset of the objects in a bucket.
//
func (api Minio) ListObjectsHandler(w http.ResponseWriter, req *http.Request) {
	// Ticket master block
	{
		op := Operation{}
		op.ProceedCh = make(chan struct{})
		api.OP <- op
		// block until Ticket master gives us a go
		<-op.ProceedCh
	}

	acceptsContentType := getContentType(req)
	if !api.isValidOp(w, req, acceptsContentType) {
		return
	}

	if isRequestUploads(req.URL.Query()) {
		api.ListMultipartUploadsHandler(w, req)
		return
	}

	resources := getBucketResources(req.URL.Query())
	if resources.Maxkeys < 0 {
		writeErrorResponse(w, req, InvalidMaxKeys, acceptsContentType, req.URL.Path)
		return
	}
	if resources.Maxkeys == 0 {
		resources.Maxkeys = maxObjectList
	}

	vars := mux.Vars(req)
	bucket := vars["bucket"]

	var signature *donut.Signature
	if _, ok := req.Header["Authorization"]; ok {
		// Init signature V4 verification
		var err *probe.Error
		signature, err = InitSignatureV4(req)
		if err != nil {
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
			return
		}
	}

	objects, resources, err := api.Donut.ListObjects(bucket, resources, signature)
	if err == nil {
		// generate response
		response := generateListObjectsResponse(bucket, objects, resources)
		encodedSuccessResponse := encodeSuccessResponse(response, acceptsContentType)
		// write headers
		setCommonHeaders(w, getContentTypeString(acceptsContentType), len(encodedSuccessResponse))
		// write body
		w.Write(encodedSuccessResponse)
		return
	}
	switch err.ToGoError().(type) {
	case donut.SignatureDoesNotMatch:
		writeErrorResponse(w, req, SignatureDoesNotMatch, acceptsContentType, req.URL.Path)
	case donut.BucketNameInvalid:
		writeErrorResponse(w, req, InvalidBucketName, acceptsContentType, req.URL.Path)
	case donut.BucketNotFound:
		writeErrorResponse(w, req, NoSuchBucket, acceptsContentType, req.URL.Path)
	case donut.ObjectNotFound:
		writeErrorResponse(w, req, NoSuchKey, acceptsContentType, req.URL.Path)
	case donut.ObjectNameInvalid:
		writeErrorResponse(w, req, NoSuchKey, acceptsContentType, req.URL.Path)
	default:
		log.Error.Println(err.Trace())
		writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
	}
}

// ListBucketsHandler - GET Service
// -----------
// This implementation of the GET operation returns a list of all buckets
// owned by the authenticated sender of the request.
func (api Minio) ListBucketsHandler(w http.ResponseWriter, req *http.Request) {
	// Ticket master block
	{
		op := Operation{}
		op.ProceedCh = make(chan struct{})
		api.OP <- op
		// block until Ticket master gives us a go
		<-op.ProceedCh
	}

	acceptsContentType := getContentType(req)
	// uncomment this when we have webcli
	// without access key credentials one cannot list buckets
	// if _, err := StripAccessKeyID(req); err != nil {
	//	writeErrorResponse(w, req, AccessDenied, acceptsContentType, req.URL.Path)
	//	return
	// }

	var signature *donut.Signature
	if _, ok := req.Header["Authorization"]; ok {
		// Init signature V4 verification
		var err *probe.Error
		signature, err = InitSignatureV4(req)
		if err != nil {
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
			return
		}
	}

	buckets, err := api.Donut.ListBuckets(signature)
	if err == nil {
		// generate response
		response := generateListBucketsResponse(buckets)
		encodedSuccessResponse := encodeSuccessResponse(response, acceptsContentType)
		// write headers
		setCommonHeaders(w, getContentTypeString(acceptsContentType), len(encodedSuccessResponse))
		// write response
		w.Write(encodedSuccessResponse)
		return
	}
	switch err.ToGoError().(type) {
	case donut.SignatureDoesNotMatch:
		writeErrorResponse(w, req, SignatureDoesNotMatch, acceptsContentType, req.URL.Path)
	default:
		log.Error.Println(err.Trace())
		writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
	}
}

// PutBucketHandler - PUT Bucket
// ----------
// This implementation of the PUT operation creates a new bucket for authenticated request
func (api Minio) PutBucketHandler(w http.ResponseWriter, req *http.Request) {
	// Ticket master block
	{
		op := Operation{}
		op.ProceedCh = make(chan struct{})
		api.OP <- op
		// block until Ticket master gives us a go
		<-op.ProceedCh
	}

	acceptsContentType := getContentType(req)
	// uncomment this when we have webcli
	// without access key credentials one cannot create a bucket
	// if _, err := StripAccessKeyID(req); err != nil {
	// 	writeErrorResponse(w, req, AccessDenied, acceptsContentType, req.URL.Path)
	//	return
	// }

	if isRequestBucketACL(req.URL.Query()) {
		api.PutBucketACLHandler(w, req)
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

	var signature *donut.Signature
	if _, ok := req.Header["Authorization"]; ok {
		// Init signature V4 verification
		var err *probe.Error
		signature, err = InitSignatureV4(req)
		if err != nil {
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
			return
		}
	}

	// if body of request is non-nil then check for validity of Content-Length
	if req.Body != nil {
		/// if Content-Length missing, deny the request
		size := req.Header.Get("Content-Length")
		if size == "" {
			writeErrorResponse(w, req, MissingContentLength, acceptsContentType, req.URL.Path)
			return
		}
	}

	err := api.Donut.MakeBucket(bucket, getACLTypeString(aclType), req.Body, signature)
	if err == nil {
		// Make sure to add Location information here only for bucket
		w.Header().Set("Location", "/"+bucket)
		writeSuccessResponse(w, acceptsContentType)
		return
	}
	switch err.ToGoError().(type) {
	case donut.SignatureDoesNotMatch:
		writeErrorResponse(w, req, SignatureDoesNotMatch, acceptsContentType, req.URL.Path)
	case donut.TooManyBuckets:
		writeErrorResponse(w, req, TooManyBuckets, acceptsContentType, req.URL.Path)
	case donut.BucketNameInvalid:
		writeErrorResponse(w, req, InvalidBucketName, acceptsContentType, req.URL.Path)
	case donut.BucketExists:
		writeErrorResponse(w, req, BucketAlreadyExists, acceptsContentType, req.URL.Path)
	default:
		log.Error.Println(err.Trace())
		writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
	}
}

// PutBucketACLHandler - PUT Bucket ACL
// ----------
// This implementation of the PUT operation modifies the bucketACL for authenticated request
func (api Minio) PutBucketACLHandler(w http.ResponseWriter, req *http.Request) {
	// Ticket master block
	{
		op := Operation{}
		op.ProceedCh = make(chan struct{})
		api.OP <- op
		// block until Ticket master gives us a go
		<-op.ProceedCh
	}

	acceptsContentType := getContentType(req)

	// read from 'x-amz-acl'
	aclType := getACLType(req)
	if aclType == unsupportedACLType {
		writeErrorResponse(w, req, NotImplemented, acceptsContentType, req.URL.Path)
		return
	}

	vars := mux.Vars(req)
	bucket := vars["bucket"]

	var signature *donut.Signature
	if _, ok := req.Header["Authorization"]; ok {
		// Init signature V4 verification
		var err *probe.Error
		signature, err = InitSignatureV4(req)
		if err != nil {
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
			return
		}
	}

	err := api.Donut.SetBucketMetadata(bucket, map[string]string{"acl": getACLTypeString(aclType)}, signature)
	if err == nil {
		writeSuccessResponse(w, acceptsContentType)
		return
	}
	switch err.ToGoError().(type) {
	case donut.SignatureDoesNotMatch:
		writeErrorResponse(w, req, SignatureDoesNotMatch, acceptsContentType, req.URL.Path)
	case donut.BucketNameInvalid:
		writeErrorResponse(w, req, InvalidBucketName, acceptsContentType, req.URL.Path)
	case donut.BucketNotFound:
		writeErrorResponse(w, req, NoSuchBucket, acceptsContentType, req.URL.Path)
	default:
		log.Error.Println(err.Trace())
		writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
	}
}

// HeadBucketHandler - HEAD Bucket
// ----------
// This operation is useful to determine if a bucket exists.
// The operation returns a 200 OK if the bucket exists and you
// have permission to access it. Otherwise, the operation might
// return responses such as 404 Not Found and 403 Forbidden.
func (api Minio) HeadBucketHandler(w http.ResponseWriter, req *http.Request) {
	// Ticket master block
	{
		op := Operation{}
		op.ProceedCh = make(chan struct{})
		api.OP <- op
		// block until Ticket master gives us a go
		<-op.ProceedCh
	}

	acceptsContentType := getContentType(req)

	vars := mux.Vars(req)
	bucket := vars["bucket"]

	var signature *donut.Signature
	if _, ok := req.Header["Authorization"]; ok {
		// Init signature V4 verification
		var err *probe.Error
		signature, err = InitSignatureV4(req)
		if err != nil {
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
			return
		}
	}

	_, err := api.Donut.GetBucketMetadata(bucket, signature)
	if err == nil {
		writeSuccessResponse(w, acceptsContentType)
		return
	}
	switch err.ToGoError().(type) {
	case donut.SignatureDoesNotMatch:
		writeErrorResponse(w, req, SignatureDoesNotMatch, acceptsContentType, req.URL.Path)
	case donut.BucketNotFound:
		writeErrorResponse(w, req, NoSuchBucket, acceptsContentType, req.URL.Path)
	case donut.BucketNameInvalid:
		writeErrorResponse(w, req, InvalidBucketName, acceptsContentType, req.URL.Path)
	default:
		log.Error.Println(err.Trace())
		writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
	}
}
