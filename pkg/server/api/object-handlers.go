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

package api

import (
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/minio/minio/pkg/donut"
	"github.com/minio/minio/pkg/iodine"
	"github.com/minio/minio/pkg/utils/log"
)

const (
	maxPartsList = 1000
)

// GetObjectHandler - GET Object
// ----------
// This implementation of the GET operation retrieves object. To use GET,
// you must have READ access to the object.
func (api Minio) GetObjectHandler(w http.ResponseWriter, req *http.Request) {
	// ticket master block
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

	var object, bucket string
	vars := mux.Vars(req)
	bucket = vars["bucket"]
	object = vars["object"]

	var signature *donut.Signature
	if _, ok := req.Header["Authorization"]; ok {
		// Init signature V4 verification
		var err error
		signature, err = InitSignatureV4(req)
		if err != nil {
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
			return
		}
	}

	metadata, err := api.Donut.GetObjectMetadata(bucket, object, signature)
	switch iodine.ToError(err).(type) {
	case nil: // success
		{
			httpRange, err := getRequestedRange(req, metadata.Size)
			if err != nil {
				writeErrorResponse(w, req, InvalidRange, acceptsContentType, req.URL.Path)
				return
			}
			switch httpRange.start == 0 && httpRange.length == 0 {
			case true:
				setObjectHeaders(w, metadata)
				if _, err := api.Donut.GetObject(w, bucket, object); err != nil {
					// unable to write headers, we've already printed data. Just close the connection.
					log.Error.Println(iodine.New(err, nil))
				}
			case false:
				metadata.Size = httpRange.length
				setRangeObjectHeaders(w, metadata, httpRange)
				w.WriteHeader(http.StatusPartialContent)
				if _, err := api.Donut.GetPartialObject(w, bucket, object, httpRange.start, httpRange.length); err != nil {
					// unable to write headers, we've already printed data. Just close the connection.
					log.Error.Println(iodine.New(err, nil))
				}
			}
		}
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
		log.Error.Println(iodine.New(err, nil))
		writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
	}
}

// HeadObjectHandler - HEAD Object
// -----------
// The HEAD operation retrieves metadata from an object without returning the object itself.
func (api Minio) HeadObjectHandler(w http.ResponseWriter, req *http.Request) {
	// ticket master block
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

	var object, bucket string
	vars := mux.Vars(req)
	bucket = vars["bucket"]
	object = vars["object"]

	var signature *donut.Signature
	if _, ok := req.Header["Authorization"]; ok {
		// Init signature V4 verification
		var err error
		signature, err = InitSignatureV4(req)
		if err != nil {
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
			return
		}
	}

	metadata, err := api.Donut.GetObjectMetadata(bucket, object, signature)
	switch iodine.ToError(err).(type) {
	case nil:
		setObjectHeaders(w, metadata)
		w.WriteHeader(http.StatusOK)
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
		log.Error.Println(iodine.New(err, nil))
		writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
	}
}

// PutObjectHandler - PUT Object
// ----------
// This implementation of the PUT operation adds an object to a bucket.
func (api Minio) PutObjectHandler(w http.ResponseWriter, req *http.Request) {
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
	sizeInt64, err := strconv.ParseInt(size, 10, 64)
	if err != nil {
		writeErrorResponse(w, req, InvalidRequest, acceptsContentType, req.URL.Path)
		return
	}

	var signature *donut.Signature
	if _, ok := req.Header["Authorization"]; ok {
		// Init signature V4 verification
		var err error
		signature, err = InitSignatureV4(req)
		if err != nil {
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
			return
		}
	}

	metadata, err := api.Donut.CreateObject(bucket, object, md5, sizeInt64, req.Body, nil, signature)
	switch iodine.ToError(err).(type) {
	case nil:
		w.Header().Set("ETag", metadata.MD5Sum)
		writeSuccessResponse(w, acceptsContentType)
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
		log.Error.Println(iodine.New(err, nil))
		writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
	}
}

/// Multipart API

// NewMultipartUploadHandler - New multipart upload
func (api Minio) NewMultipartUploadHandler(w http.ResponseWriter, req *http.Request) {
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
		var err error
		signature, err = InitSignatureV4(req)
		if err != nil {
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
			return
		}
	}

	uploadID, err := api.Donut.NewMultipartUpload(bucket, object, req.Header.Get("Content-Type"), signature)
	switch iodine.ToError(err).(type) {
	case nil:
		{
			response := generateInitiateMultipartUploadResponse(bucket, object, uploadID)
			encodedSuccessResponse := encodeSuccessResponse(response, acceptsContentType)
			// write headers
			setCommonHeaders(w, getContentTypeString(acceptsContentType), len(encodedSuccessResponse))
			// write body
			w.Write(encodedSuccessResponse)
		}
	case donut.SignatureDoesNotMatch:
		writeErrorResponse(w, req, SignatureDoesNotMatch, acceptsContentType, req.URL.Path)
	case donut.ObjectExists:
		writeErrorResponse(w, req, MethodNotAllowed, acceptsContentType, req.URL.Path)
	default:
		log.Error.Println(iodine.New(err, nil))
		writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
	}
}

// PutObjectPartHandler - Upload part
func (api Minio) PutObjectPartHandler(w http.ResponseWriter, req *http.Request) {
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

	sizeInt64, err := strconv.ParseInt(size, 10, 64)
	if err != nil {
		writeErrorResponse(w, req, InvalidRequest, acceptsContentType, req.URL.Path)
		return
	}

	vars := mux.Vars(req)
	bucket := vars["bucket"]
	object := vars["object"]

	uploadID := req.URL.Query().Get("uploadId")
	partIDString := req.URL.Query().Get("partNumber")

	partID, err := strconv.Atoi(partIDString)
	if err != nil {
		writeErrorResponse(w, req, InvalidPart, acceptsContentType, req.URL.Path)
	}

	var signature *donut.Signature
	if _, ok := req.Header["Authorization"]; ok {
		// Init signature V4 verification
		var err error
		signature, err = InitSignatureV4(req)
		if err != nil {
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
			return
		}
	}

	calculatedMD5, err := api.Donut.CreateObjectPart(bucket, object, uploadID, partID, "", md5, sizeInt64, req.Body, signature)
	switch iodine.ToError(err).(type) {
	case nil:
		w.Header().Set("ETag", calculatedMD5)
		writeSuccessResponse(w, acceptsContentType)
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
		log.Error.Println(iodine.New(err, nil))
		writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
	}
}

// AbortMultipartUploadHandler - Abort multipart upload
func (api Minio) AbortMultipartUploadHandler(w http.ResponseWriter, req *http.Request) {
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

	vars := mux.Vars(req)
	bucket := vars["bucket"]
	object := vars["object"]

	objectResourcesMetadata := getObjectResources(req.URL.Query())

	var signature *donut.Signature
	if _, ok := req.Header["Authorization"]; ok {
		// Init signature V4 verification
		var err error
		signature, err = InitSignatureV4(req)
		if err != nil {
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
			return
		}
	}

	err := api.Donut.AbortMultipartUpload(bucket, object, objectResourcesMetadata.UploadID, signature)
	switch iodine.ToError(err).(type) {
	case nil:
		setCommonHeaders(w, getContentTypeString(acceptsContentType), 0)
		w.WriteHeader(http.StatusNoContent)
	case donut.SignatureDoesNotMatch:
		writeErrorResponse(w, req, SignatureDoesNotMatch, acceptsContentType, req.URL.Path)
	case donut.InvalidUploadID:
		writeErrorResponse(w, req, NoSuchUpload, acceptsContentType, req.URL.Path)
	default:
		log.Error.Println(iodine.New(err, nil))
		writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
	}
}

// ListObjectPartsHandler - List object parts
func (api Minio) ListObjectPartsHandler(w http.ResponseWriter, req *http.Request) {
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
		var err error
		signature, err = InitSignatureV4(req)
		if err != nil {
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
			return
		}
	}

	objectResourcesMetadata, err := api.Donut.ListObjectParts(bucket, object, objectResourcesMetadata, signature)
	switch iodine.ToError(err).(type) {
	case nil:
		{
			response := generateListPartsResponse(objectResourcesMetadata)
			encodedSuccessResponse := encodeSuccessResponse(response, acceptsContentType)
			// write headers
			setCommonHeaders(w, getContentTypeString(acceptsContentType), len(encodedSuccessResponse))
			// write body
			w.Write(encodedSuccessResponse)
		}
	case donut.SignatureDoesNotMatch:
		writeErrorResponse(w, req, SignatureDoesNotMatch, acceptsContentType, req.URL.Path)
	case donut.InvalidUploadID:
		writeErrorResponse(w, req, NoSuchUpload, acceptsContentType, req.URL.Path)
	default:
		log.Error.Println(iodine.New(err, nil))
		writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
	}
}

// CompleteMultipartUploadHandler - Complete multipart upload
func (api Minio) CompleteMultipartUploadHandler(w http.ResponseWriter, req *http.Request) {
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

	vars := mux.Vars(req)
	bucket := vars["bucket"]
	object := vars["object"]

	objectResourcesMetadata := getObjectResources(req.URL.Query())

	var signature *donut.Signature
	if _, ok := req.Header["Authorization"]; ok {
		// Init signature V4 verification
		var err error
		signature, err = InitSignatureV4(req)
		if err != nil {
			writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
			return
		}
	}
	metadata, err := api.Donut.CompleteMultipartUpload(bucket, object, objectResourcesMetadata.UploadID, req.Body, signature)
	switch iodine.ToError(err).(type) {
	case nil:
		{
			response := generateCompleteMultpartUploadResponse(bucket, object, "", metadata.MD5Sum)
			encodedSuccessResponse := encodeSuccessResponse(response, acceptsContentType)
			// write headers
			setCommonHeaders(w, getContentTypeString(acceptsContentType), len(encodedSuccessResponse))
			// write body
			w.Write(encodedSuccessResponse)
		}
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
		log.Error.Println(iodine.New(err, nil))
		writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
	}
}

/// Delete API

// DeleteBucketHandler - Delete bucket
func (api Minio) DeleteBucketHandler(w http.ResponseWriter, req *http.Request) {
	error := getErrorCode(MethodNotAllowed)
	w.WriteHeader(error.HTTPStatusCode)
}

// DeleteObjectHandler - Delete object
func (api Minio) DeleteObjectHandler(w http.ResponseWriter, req *http.Request) {
	error := getErrorCode(MethodNotAllowed)
	w.WriteHeader(error.HTTPStatusCode)
}
