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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implieapi.Donut.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/minio/minio/pkg/donut"
	"github.com/minio/minio/pkg/probe"
)

const (
	maxPartsList = 1000
)

// GetObjectHandler - GET Object
// ----------
// This implementation of the GET operation retrieves object. To use GET,
// you must have READ access to the object.
func (api MinioAPI) GetObjectHandler(w http.ResponseWriter, req *http.Request) {
	// ticket master block
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

	var object, bucket string
	vars := mux.Vars(req)
	bucket = vars["bucket"]
	object = vars["object"]

	var signature *donut.Signature
	if _, ok := req.Header["Authorization"]; ok {
		// Init signature V4 verification
		var err *probe.Error
		signature, err = initSignatureV4(req)
		if err != nil {
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
			return
		}
	}

	metadata, err := api.Donut.GetObjectMetadata(bucket, object, signature)
	if err == nil {
		var hrange *httpRange
		hrange, err = getRequestedRange(req.Header.Get("Range"), metadata.Size)
		if err != nil {
			writeErrorResponse(w, req, InvalidRange, acceptsContentType, req.URL.Path)
			return
		}
		setObjectHeaders(w, metadata, hrange)
		if _, err = api.Donut.GetObject(w, bucket, object, hrange.start, hrange.length); err != nil {
			// unable to write headers, we've already printed data. Just close the connection.
			// log.Error.Println(err.Trace())
			return
		}
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
		// log.Error.Println(err.Trace())
		writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
	}
}

// HeadObjectHandler - HEAD Object
// -----------
// The HEAD operation retrieves metadata from an object without returning the object itself.
func (api MinioAPI) HeadObjectHandler(w http.ResponseWriter, req *http.Request) {
	// ticket master block
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

	var object, bucket string
	vars := mux.Vars(req)
	bucket = vars["bucket"]
	object = vars["object"]

	var signature *donut.Signature
	if _, ok := req.Header["Authorization"]; ok {
		// Init signature V4 verification
		var err *probe.Error
		signature, err = initSignatureV4(req)
		if err != nil {
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
			return
		}
	}

	metadata, err := api.Donut.GetObjectMetadata(bucket, object, signature)
	if err == nil {
		setObjectHeaders(w, metadata, nil)
		w.WriteHeader(http.StatusOK)
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
		// log.Error.Println(err.Trace())
		writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
	}
}

// PutObjectHandler - PUT Object
// ----------
// This implementation of the PUT operation adds an object to a bucket.
func (api MinioAPI) PutObjectHandler(w http.ResponseWriter, req *http.Request) {
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

	var object, bucket string
	vars := mux.Vars(req)
	bucket = vars["bucket"]
	object = vars["object"]

	// get Content-MD5 sent by client and verify if valid
	md5 := req.Header.Get("Content-MD5")
	if !isValidMD5(md5) {
		writeErrorResponse(w, req, InvalidDigest, acceptsContentType, req.URL.Path)
		return
	}
	/// if Content-Length missing, deny the request
	size := req.Header.Get("Content-Length")
	if size == "" {
		writeErrorResponse(w, req, MissingContentLength, acceptsContentType, req.URL.Path)
		return
	}
	/// maximum Upload size for objects in a single operation
	if isMaxObjectSize(size) {
		writeErrorResponse(w, req, EntityTooLarge, acceptsContentType, req.URL.Path)
		return
	}
	/// minimum Upload size for objects in a single operation
	//
	// Surprisingly while Amazon in their document states that S3 objects have 1byte
	// as the minimum limit, they do not seem to enforce it one can successfully
	// create a 0byte file using a regular putObject() operation
	//
	// if isMinObjectSize(size) {
	//      writeErrorResponse(w, req, EntityTooSmall, acceptsContentType, req.URL.Path)
	//	return
	// }
	var sizeInt64 int64
	{
		var err error
		sizeInt64, err = strconv.ParseInt(size, 10, 64)
		if err != nil {
			writeErrorResponse(w, req, InvalidRequest, acceptsContentType, req.URL.Path)
			return
		}
	}

	var signature *donut.Signature
	if _, ok := req.Header["Authorization"]; ok {
		// Init signature V4 verification
		var err *probe.Error
		signature, err = initSignatureV4(req)
		if err != nil {
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
			return
		}
	}

	metadata, err := api.Donut.CreateObject(bucket, object, md5, sizeInt64, req.Body, nil, signature)
	if err == nil {
		w.Header().Set("ETag", metadata.MD5Sum)
		writeSuccessResponse(w, acceptsContentType)
		return
	}
	switch err.ToGoError().(type) {
	case donut.BucketNotFound:
		writeErrorResponse(w, req, NoSuchBucket, acceptsContentType, req.URL.Path)
	case donut.BucketNameInvalid:
		writeErrorResponse(w, req, InvalidBucketName, acceptsContentType, req.URL.Path)
	case donut.ObjectExists:
		writeErrorResponse(w, req, MethodNotAllowed, acceptsContentType, req.URL.Path)
	case donut.BadDigest:
		writeErrorResponse(w, req, BadDigest, acceptsContentType, req.URL.Path)
	case donut.MissingDateHeader:
		writeErrorResponse(w, req, RequestTimeTooSkewed, acceptsContentType, req.URL.Path)
	case donut.SignatureDoesNotMatch:
		writeErrorResponse(w, req, SignatureDoesNotMatch, acceptsContentType, req.URL.Path)
	case donut.IncompleteBody:
		writeErrorResponse(w, req, IncompleteBody, acceptsContentType, req.URL.Path)
	case donut.EntityTooLarge:
		writeErrorResponse(w, req, EntityTooLarge, acceptsContentType, req.URL.Path)
	case donut.InvalidDigest:
		writeErrorResponse(w, req, InvalidDigest, acceptsContentType, req.URL.Path)
	default:
		// log.Error.Println(err.Trace())
		writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
	}
}

/// Multipart API

// NewMultipartUploadHandler - New multipart upload
func (api MinioAPI) NewMultipartUploadHandler(w http.ResponseWriter, req *http.Request) {
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

	if !isRequestUploads(req.URL.Query()) {
		writeErrorResponse(w, req, MethodNotAllowed, acceptsContentType, req.URL.Path)
		return
	}

	var object, bucket string
	vars := mux.Vars(req)
	bucket = vars["bucket"]
	object = vars["object"]

	var signature *donut.Signature
	if _, ok := req.Header["Authorization"]; ok {
		// Init signature V4 verification
		var err *probe.Error
		signature, err = initSignatureV4(req)
		if err != nil {
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
			return
		}
	}

	uploadID, err := api.Donut.NewMultipartUpload(bucket, object, req.Header.Get("Content-Type"), signature)
	if err == nil {
		response := generateInitiateMultipartUploadResponse(bucket, object, uploadID)
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
	case donut.ObjectExists:
		writeErrorResponse(w, req, MethodNotAllowed, acceptsContentType, req.URL.Path)
	default:
		// log.Error.Println(err.Trace())
		writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
	}
}

// PutObjectPartHandler - Upload part
func (api MinioAPI) PutObjectPartHandler(w http.ResponseWriter, req *http.Request) {
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

	// get Content-MD5 sent by client and verify if valid
	md5 := req.Header.Get("Content-MD5")
	if !isValidMD5(md5) {
		writeErrorResponse(w, req, InvalidDigest, acceptsContentType, req.URL.Path)
		return
	}

	/// if Content-Length missing, throw away
	size := req.Header.Get("Content-Length")
	if size == "" {
		writeErrorResponse(w, req, MissingContentLength, acceptsContentType, req.URL.Path)
		return
	}

	/// maximum Upload size for multipart objects in a single operation
	if isMaxObjectSize(size) {
		writeErrorResponse(w, req, EntityTooLarge, acceptsContentType, req.URL.Path)
		return
	}

	var sizeInt64 int64
	{
		var err error
		sizeInt64, err = strconv.ParseInt(size, 10, 64)
		if err != nil {
			writeErrorResponse(w, req, InvalidRequest, acceptsContentType, req.URL.Path)
			return
		}
	}

	vars := mux.Vars(req)
	bucket := vars["bucket"]
	object := vars["object"]

	uploadID := req.URL.Query().Get("uploadId")
	partIDString := req.URL.Query().Get("partNumber")

	var partID int
	{
		var err error
		partID, err = strconv.Atoi(partIDString)
		if err != nil {
			writeErrorResponse(w, req, InvalidPart, acceptsContentType, req.URL.Path)
		}
	}

	var signature *donut.Signature
	if _, ok := req.Header["Authorization"]; ok {
		// Init signature V4 verification
		var err *probe.Error
		signature, err = initSignatureV4(req)
		if err != nil {
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
			return
		}
	}

	calculatedMD5, err := api.Donut.CreateObjectPart(bucket, object, uploadID, partID, "", md5, sizeInt64, req.Body, signature)
	if err == nil {
		w.Header().Set("ETag", calculatedMD5)
		writeSuccessResponse(w, acceptsContentType)
		return
	}
	switch err.ToGoError().(type) {
	case donut.InvalidUploadID:
		writeErrorResponse(w, req, NoSuchUpload, acceptsContentType, req.URL.Path)
	case donut.ObjectExists:
		writeErrorResponse(w, req, MethodNotAllowed, acceptsContentType, req.URL.Path)
	case donut.BadDigest:
		writeErrorResponse(w, req, BadDigest, acceptsContentType, req.URL.Path)
	case donut.SignatureDoesNotMatch:
		writeErrorResponse(w, req, SignatureDoesNotMatch, acceptsContentType, req.URL.Path)
	case donut.IncompleteBody:
		writeErrorResponse(w, req, IncompleteBody, acceptsContentType, req.URL.Path)
	case donut.EntityTooLarge:
		writeErrorResponse(w, req, EntityTooLarge, acceptsContentType, req.URL.Path)
	case donut.InvalidDigest:
		writeErrorResponse(w, req, InvalidDigest, acceptsContentType, req.URL.Path)
	default:
		// log.Error.Println(err.Trace())
		writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
	}
}

// AbortMultipartUploadHandler - Abort multipart upload
func (api MinioAPI) AbortMultipartUploadHandler(w http.ResponseWriter, req *http.Request) {
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

	vars := mux.Vars(req)
	bucket := vars["bucket"]
	object := vars["object"]

	objectResourcesMetadata := getObjectResources(req.URL.Query())

	var signature *donut.Signature
	if _, ok := req.Header["Authorization"]; ok {
		// Init signature V4 verification
		var err *probe.Error
		signature, err = initSignatureV4(req)
		if err != nil {
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
			return
		}
	}

	err := api.Donut.AbortMultipartUpload(bucket, object, objectResourcesMetadata.UploadID, signature)
	if err == nil {
		setCommonHeaders(w, getContentTypeString(acceptsContentType), 0)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	switch err.ToGoError().(type) {
	case donut.SignatureDoesNotMatch:
		writeErrorResponse(w, req, SignatureDoesNotMatch, acceptsContentType, req.URL.Path)
	case donut.InvalidUploadID:
		writeErrorResponse(w, req, NoSuchUpload, acceptsContentType, req.URL.Path)
	default:
		// log.Error.Println(err.Trace())
		writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
	}
}

// ListObjectPartsHandler - List object parts
func (api MinioAPI) ListObjectPartsHandler(w http.ResponseWriter, req *http.Request) {
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

	objectResourcesMetadata := getObjectResources(req.URL.Query())
	if objectResourcesMetadata.PartNumberMarker < 0 {
		writeErrorResponse(w, req, InvalidPartNumberMarker, acceptsContentType, req.URL.Path)
		return
	}
	if objectResourcesMetadata.MaxParts < 0 {
		writeErrorResponse(w, req, InvalidMaxParts, acceptsContentType, req.URL.Path)
		return
	}
	if objectResourcesMetadata.MaxParts == 0 {
		objectResourcesMetadata.MaxParts = maxPartsList
	}

	vars := mux.Vars(req)
	bucket := vars["bucket"]
	object := vars["object"]

	var signature *donut.Signature
	if _, ok := req.Header["Authorization"]; ok {
		// Init signature V4 verification
		var err *probe.Error
		signature, err = initSignatureV4(req)
		if err != nil {
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
			return
		}
	}

	objectResourcesMetadata, err := api.Donut.ListObjectParts(bucket, object, objectResourcesMetadata, signature)
	if err == nil {
		response := generateListPartsResponse(objectResourcesMetadata)
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
	case donut.InvalidUploadID:
		writeErrorResponse(w, req, NoSuchUpload, acceptsContentType, req.URL.Path)
	default:
		// log.Error.Println(err.Trace())
		writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
	}
}

// CompleteMultipartUploadHandler - Complete multipart upload
func (api MinioAPI) CompleteMultipartUploadHandler(w http.ResponseWriter, req *http.Request) {
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

	vars := mux.Vars(req)
	bucket := vars["bucket"]
	object := vars["object"]

	objectResourcesMetadata := getObjectResources(req.URL.Query())

	var signature *donut.Signature
	if _, ok := req.Header["Authorization"]; ok {
		// Init signature V4 verification
		var err *probe.Error
		signature, err = initSignatureV4(req)
		if err != nil {
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
			return
		}
	}
	metadata, err := api.Donut.CompleteMultipartUpload(bucket, object, objectResourcesMetadata.UploadID, req.Body, signature)
	if err == nil {
		response := generateCompleteMultpartUploadResponse(bucket, object, "", metadata.MD5Sum)
		encodedSuccessResponse := encodeSuccessResponse(response, acceptsContentType)
		// write headers
		setCommonHeaders(w, getContentTypeString(acceptsContentType), len(encodedSuccessResponse))
		// write body
		w.Write(encodedSuccessResponse)
		return
	}
	switch err.ToGoError().(type) {
	case donut.InvalidUploadID:
		writeErrorResponse(w, req, NoSuchUpload, acceptsContentType, req.URL.Path)
	case donut.InvalidPart:
		writeErrorResponse(w, req, InvalidPart, acceptsContentType, req.URL.Path)
	case donut.InvalidPartOrder:
		writeErrorResponse(w, req, InvalidPartOrder, acceptsContentType, req.URL.Path)
	case donut.MissingDateHeader:
		writeErrorResponse(w, req, RequestTimeTooSkewed, acceptsContentType, req.URL.Path)
	case donut.SignatureDoesNotMatch:
		writeErrorResponse(w, req, SignatureDoesNotMatch, acceptsContentType, req.URL.Path)
	case donut.IncompleteBody:
		writeErrorResponse(w, req, IncompleteBody, acceptsContentType, req.URL.Path)
	case donut.MalformedXML:
		writeErrorResponse(w, req, MalformedXML, acceptsContentType, req.URL.Path)
	default:
		// log.Error.Println(err.Trace())
		writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
	}
}

/// Delete API

// DeleteBucketHandler - Delete bucket
func (api MinioAPI) DeleteBucketHandler(w http.ResponseWriter, req *http.Request) {
	error := getErrorCode(MethodNotAllowed)
	w.WriteHeader(error.HTTPStatusCode)
}

// DeleteObjectHandler - Delete object
func (api MinioAPI) DeleteObjectHandler(w http.ResponseWriter, req *http.Request) {
	error := getErrorCode(MethodNotAllowed)
	w.WriteHeader(error.HTTPStatusCode)
}
