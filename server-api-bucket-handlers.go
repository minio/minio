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

package main

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/minio/minio/pkg/donut"
	"github.com/minio/minio/pkg/probe"
	signv4 "github.com/minio/minio/pkg/signature"
)

func (api API) isValidOp(w http.ResponseWriter, req *http.Request, acceptsContentType contentType) bool {
	vars := mux.Vars(req)
	bucket := vars["bucket"]

	bucketMetadata, err := api.Donut.GetBucketMetadata(bucket, nil)
	if err != nil {
		errorIf(err.Trace(), "GetBucketMetadata failed.", nil)
		switch err.ToGoError().(type) {
		case donut.BucketNotFound:
			writeErrorResponse(w, req, NoSuchBucket, acceptsContentType, req.URL.Path)
			return false
		case donut.BucketNameInvalid:
			writeErrorResponse(w, req, InvalidBucketName, acceptsContentType, req.URL.Path)
			return false
		default:
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
			return false
		}
	}
	if _, err = stripAccessKeyID(req.Header.Get("Authorization")); err != nil {
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

// ListMultipartUploadsHandler - GET Bucket (List Multipart uploads)
// -------------------------
// This operation lists in-progress multipart uploads. An in-progress
// multipart upload is a multipart upload that has been initiated,
// using the Initiate Multipart Upload request, but has not yet been completed or aborted.
// This operation returns at most 1,000 multipart uploads in the response.
//
func (api API) ListMultipartUploadsHandler(w http.ResponseWriter, req *http.Request) {
	// Ticket master block
	{
		op := APIOperation{}
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

	var signature *signv4.Signature
	if _, ok := req.Header["Authorization"]; ok {
		// Init signature V4 verification
		var err *probe.Error
		signature, err = initSignatureV4(req)
		if err != nil {
			errorIf(err.Trace(), "Initializing signature v4 failed.", nil)
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
			return
		}
	}

	resources, err := api.Donut.ListMultipartUploads(bucket, resources, signature)
	if err != nil {
		errorIf(err.Trace(), "ListMultipartUploads failed.", nil)
		switch err.ToGoError().(type) {
		case signv4.DoesNotMatch:
			writeErrorResponse(w, req, SignatureDoesNotMatch, acceptsContentType, req.URL.Path)
		case donut.BucketNotFound:
			writeErrorResponse(w, req, NoSuchBucket, acceptsContentType, req.URL.Path)
		default:
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
		}
		return
	}
	// generate response
	response := generateListMultipartUploadsResponse(bucket, resources)
	encodedSuccessResponse := encodeSuccessResponse(response, acceptsContentType)
	// write headers
	setCommonHeaders(w, getContentTypeString(acceptsContentType), len(encodedSuccessResponse))
	// write body
	w.Write(encodedSuccessResponse)
}

// ListObjectsHandler - GET Bucket (List Objects)
// -------------------------
// This implementation of the GET operation returns some or all (up to 1000)
// of the objects in a bucket. You can use the request parameters as selection
// criteria to return a subset of the objects in a bucket.
//
func (api API) ListObjectsHandler(w http.ResponseWriter, req *http.Request) {
	// Ticket master block
	{
		op := APIOperation{}
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

	var signature *signv4.Signature
	if _, ok := req.Header["Authorization"]; ok {
		// Init signature V4 verification
		var err *probe.Error
		signature, err = initSignatureV4(req)
		if err != nil {
			errorIf(err.Trace(), "Initializing signature v4 failed.", nil)
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
	case signv4.DoesNotMatch:
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
		errorIf(err.Trace(), "ListObjects failed.", nil)
		writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
	}
}

// ListBucketsHandler - GET Service
// -----------
// This implementation of the GET operation returns a list of all buckets
// owned by the authenticated sender of the request.
func (api API) ListBucketsHandler(w http.ResponseWriter, req *http.Request) {
	// Ticket master block
	{
		op := APIOperation{}
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

	var signature *signv4.Signature
	if _, ok := req.Header["Authorization"]; ok {
		// Init signature V4 verification
		var err *probe.Error
		signature, err = initSignatureV4(req)
		if err != nil {
			errorIf(err.Trace(), "Initializing signature v4 failed.", nil)
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
	case signv4.DoesNotMatch:
		writeErrorResponse(w, req, SignatureDoesNotMatch, acceptsContentType, req.URL.Path)
	default:
		errorIf(err.Trace(), "ListBuckets failed.", nil)
		writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
	}
}

// PutBucketHandler - PUT Bucket
// ----------
// This implementation of the PUT operation creates a new bucket for authenticated request
func (api API) PutBucketHandler(w http.ResponseWriter, req *http.Request) {
	// Ticket master block
	{
		op := APIOperation{}
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

	var signature *signv4.Signature
	if _, ok := req.Header["Authorization"]; ok {
		// Init signature V4 verification
		var err *probe.Error
		signature, err = initSignatureV4(req)
		if err != nil {
			errorIf(err.Trace(), "Initializing signature v4 failed.", nil)
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
	if err != nil {
		errorIf(err.Trace(), "MakeBucket failed.", nil)
		switch err.ToGoError().(type) {
		case signv4.DoesNotMatch:
			writeErrorResponse(w, req, SignatureDoesNotMatch, acceptsContentType, req.URL.Path)
		case donut.TooManyBuckets:
			writeErrorResponse(w, req, TooManyBuckets, acceptsContentType, req.URL.Path)
		case donut.BucketNameInvalid:
			writeErrorResponse(w, req, InvalidBucketName, acceptsContentType, req.URL.Path)
		case donut.BucketExists:
			writeErrorResponse(w, req, BucketAlreadyExists, acceptsContentType, req.URL.Path)
		default:
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
		}
		return
	}
	// Make sure to add Location information here only for bucket
	w.Header().Set("Location", "/"+bucket)
	writeSuccessResponse(w, acceptsContentType)
}

// PostPolicyBucketHandler - POST policy
// ----------
// This implementation of the POST operation handles object creation with a specified
// signature policy in multipart/form-data
func (api API) PostPolicyBucketHandler(w http.ResponseWriter, req *http.Request) {
	// Ticket master block
	{
		op := APIOperation{}
		op.ProceedCh = make(chan struct{})
		api.OP <- op
		// block until Ticket master gives us a go
		<-op.ProceedCh
	}

	// Here the parameter is the size of the form data that should
	// be loaded in memory, the remaining being put in temporary
	// files
	reader, err := req.MultipartReader()
	if err != nil {
		errorIf(probe.NewError(err), "Unable to initialize multipart reader.", nil)
		writeErrorResponse(w, req, MalformedPOSTRequest, 1, req.URL.Path)
		return
	}

	fileBody, formValues, perr := extractHTTPFormValues(reader)
	if perr != nil {
		errorIf(perr.Trace(), "Unable to parse form values.", nil)
		writeErrorResponse(w, req, MalformedPOSTRequest, 1, req.URL.Path)
		return
	}
	bucket := mux.Vars(req)["bucket"]
	formValues["Bucket"] = bucket
	object := formValues["key"]
	signature, perr := initPostPresignedPolicyV4(formValues)
	if perr != nil {
		errorIf(perr.Trace(), "Unable to initialize post policy presigned.", nil)
		writeErrorResponse(w, req, MalformedPOSTRequest, 1, req.URL.Path)
		return
	}
	if perr = applyPolicy(formValues, signature.PresignedPolicy); perr != nil {
		errorIf(perr.Trace(), "Invalid request, policy doesn't match with the endpoint.", nil)
		writeErrorResponse(w, req, MalformedPOSTRequest, 1, req.URL.Path)
		return
	}
	var ok bool
	if ok, perr = signature.DoesPolicySignatureMatch(formValues["X-Amz-Date"]); perr != nil {
		errorIf(perr.Trace(), "Unable to verify signature.", nil)
		writeErrorResponse(w, req, SignatureDoesNotMatch, 1, req.URL.Path)
		return
	}
	if ok == false {
		writeErrorResponse(w, req, SignatureDoesNotMatch, 1, req.URL.Path)
		return
	}
	metadata, perr := api.Donut.CreateObject(bucket, object, "", 0, fileBody, nil, nil)
	if perr != nil {
		errorIf(perr.Trace(), "CreateObject failed.", nil)
		switch perr.ToGoError().(type) {
		case donut.BucketNotFound:
			writeErrorResponse(w, req, NoSuchBucket, 1, req.URL.Path)
		case donut.BucketNameInvalid:
			writeErrorResponse(w, req, InvalidBucketName, 1, req.URL.Path)
		case donut.ObjectExists:
			writeErrorResponse(w, req, MethodNotAllowed, 1, req.URL.Path)
		case donut.BadDigest:
			writeErrorResponse(w, req, BadDigest, 1, req.URL.Path)
		case signv4.MissingDateHeader:
			writeErrorResponse(w, req, RequestTimeTooSkewed, 1, req.URL.Path)
		case signv4.DoesNotMatch:
			writeErrorResponse(w, req, SignatureDoesNotMatch, 1, req.URL.Path)
		case donut.IncompleteBody:
			writeErrorResponse(w, req, IncompleteBody, 1, req.URL.Path)
		case donut.EntityTooLarge:
			writeErrorResponse(w, req, EntityTooLarge, 1, req.URL.Path)
		case donut.InvalidDigest:
			writeErrorResponse(w, req, InvalidDigest, 1, req.URL.Path)
		default:
			writeErrorResponse(w, req, InternalError, 1, req.URL.Path)
		}
		return
	}
	w.Header().Set("ETag", metadata.MD5Sum)
	writeSuccessResponse(w, 1)
}

// PutBucketACLHandler - PUT Bucket ACL
// ----------
// This implementation of the PUT operation modifies the bucketACL for authenticated request
func (api API) PutBucketACLHandler(w http.ResponseWriter, req *http.Request) {
	// Ticket master block
	{
		op := APIOperation{}
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

	var signature *signv4.Signature
	if _, ok := req.Header["Authorization"]; ok {
		// Init signature V4 verification
		var err *probe.Error
		signature, err = initSignatureV4(req)
		if err != nil {
			errorIf(err.Trace(), "Initializing signature v4 failed.", nil)
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
			return
		}
	}

	err := api.Donut.SetBucketMetadata(bucket, map[string]string{"acl": getACLTypeString(aclType)}, signature)
	if err != nil {
		errorIf(err.Trace(), "PutBucketACL failed.", nil)
		switch err.ToGoError().(type) {
		case signv4.DoesNotMatch:
			writeErrorResponse(w, req, SignatureDoesNotMatch, acceptsContentType, req.URL.Path)
		case donut.BucketNameInvalid:
			writeErrorResponse(w, req, InvalidBucketName, acceptsContentType, req.URL.Path)
		case donut.BucketNotFound:
			writeErrorResponse(w, req, NoSuchBucket, acceptsContentType, req.URL.Path)
		default:
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
		}
		return
	}
	writeSuccessResponse(w, acceptsContentType)
}

// HeadBucketHandler - HEAD Bucket
// ----------
// This operation is useful to determine if a bucket exists.
// The operation returns a 200 OK if the bucket exists and you
// have permission to access it. Otherwise, the operation might
// return responses such as 404 Not Found and 403 Forbidden.
func (api API) HeadBucketHandler(w http.ResponseWriter, req *http.Request) {
	// Ticket master block
	{
		op := APIOperation{}
		op.ProceedCh = make(chan struct{})
		api.OP <- op
		// block until Ticket master gives us a go
		<-op.ProceedCh
	}

	acceptsContentType := getContentType(req)

	vars := mux.Vars(req)
	bucket := vars["bucket"]

	var signature *signv4.Signature
	if _, ok := req.Header["Authorization"]; ok {
		// Init signature V4 verification
		var err *probe.Error
		signature, err = initSignatureV4(req)
		if err != nil {
			errorIf(err.Trace(), "Initializing signature v4 failed.", nil)
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
			return
		}
	}

	_, err := api.Donut.GetBucketMetadata(bucket, signature)
	if err != nil {
		errorIf(err.Trace(), "GetBucketMetadata failed.", nil)
		switch err.ToGoError().(type) {
		case signv4.DoesNotMatch:
			writeErrorResponse(w, req, SignatureDoesNotMatch, acceptsContentType, req.URL.Path)
		case donut.BucketNotFound:
			writeErrorResponse(w, req, NoSuchBucket, acceptsContentType, req.URL.Path)
		case donut.BucketNameInvalid:
			writeErrorResponse(w, req, InvalidBucketName, acceptsContentType, req.URL.Path)
		default:
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
		}
		return
	}
	writeSuccessResponse(w, acceptsContentType)
}
