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
	"sort"
	"strconv"

	"encoding/xml"

	"github.com/gorilla/mux"
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
	acceptsContentType := getContentType(req)

	op := Operation{}
	op.ProceedCh = make(chan struct{})
	api.OP <- op
	// block until Ticket master gives us a go
	<-op.ProceedCh
	{
		// do you operation
	}
	log.Println(acceptsContentType)

	// verify if this operation is allowed
	if !api.isValidOp(w, req, acceptsContentType) {
		return
	}

	var object, bucket string
	vars := mux.Vars(req)
	bucket = vars["bucket"]
	object = vars["object"]
	log.Println(bucket, object)

}

// HeadObjectHandler - HEAD Object
// -----------
// The HEAD operation retrieves metadata from an object without returning the object itself.
func (api Minio) HeadObjectHandler(w http.ResponseWriter, req *http.Request) {
	acceptsContentType := getContentType(req)

	op := Operation{}
	op.ProceedCh = make(chan struct{})
	api.OP <- op
	// block until Ticket master gives us a go
	<-op.ProceedCh
	{
		// do you operation
	}
	log.Println(acceptsContentType)

	// verify if this operation is allowed
	if !api.isValidOp(w, req, acceptsContentType) {
		return
	}

	var object, bucket string
	vars := mux.Vars(req)
	bucket = vars["bucket"]
	object = vars["object"]
	log.Println(bucket, object)
}

// PutObjectHandler - PUT Object
// ----------
// This implementation of the PUT operation adds an object to a bucket.
func (api Minio) PutObjectHandler(w http.ResponseWriter, req *http.Request) {
	acceptsContentType := getContentType(req)

	op := Operation{}
	op.ProceedCh = make(chan struct{})
	api.OP <- op
	// block until Ticket master gives us a go
	<-op.ProceedCh
	{
		// do you operation
	}
	log.Println(acceptsContentType)

	// verify if this operation is allowed
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
	/// if Content-Length missing, throw away
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
	log.Println(bucket, object, sizeInt64)
}

/// Multipart API

// NewMultipartUploadHandler - New multipart upload
func (api Minio) NewMultipartUploadHandler(w http.ResponseWriter, req *http.Request) {
	acceptsContentType := getContentType(req)

	op := Operation{}
	op.ProceedCh = make(chan struct{})
	api.OP <- op
	// block until Ticket master gives us a go
	<-op.ProceedCh
	{
		// do you operation
	}
	log.Println(acceptsContentType)

	// handle ACL's here at bucket level
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
	log.Println(bucket, object)
}

// PutObjectPartHandler - Upload part
func (api Minio) PutObjectPartHandler(w http.ResponseWriter, req *http.Request) {
	acceptsContentType := getContentType(req)

	op := Operation{}
	op.ProceedCh = make(chan struct{})
	api.OP <- op
	// block until Ticket master gives us a go
	<-op.ProceedCh
	{
		// do you operation
	}
	log.Println(acceptsContentType)

	// handle ACL's here at bucket level
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
	log.Println(bucket, object, sizeInt64)

	uploadID := req.URL.Query().Get("uploadId")
	partIDString := req.URL.Query().Get("partNumber")

	partID, err := strconv.Atoi(partIDString)
	if err != nil {
		writeErrorResponse(w, req, InvalidPart, acceptsContentType, req.URL.Path)
	}
	log.Println(uploadID, partID)
}

// AbortMultipartUploadHandler - Abort multipart upload
func (api Minio) AbortMultipartUploadHandler(w http.ResponseWriter, req *http.Request) {
	acceptsContentType := getContentType(req)

	op := Operation{}
	op.ProceedCh = make(chan struct{})
	api.OP <- op
	// block until Ticket master gives us a go
	<-op.ProceedCh
	{
		// do you operation
	}
	log.Println(acceptsContentType)

	// handle ACL's here at bucket level
	if !api.isValidOp(w, req, acceptsContentType) {
		return
	}

	vars := mux.Vars(req)
	bucket := vars["bucket"]
	object := vars["object"]

	//objectResourcesMetadata := getObjectResources(req.URL.Query())
	log.Println(bucket, object)
}

// ListObjectPartsHandler - List object parts
func (api Minio) ListObjectPartsHandler(w http.ResponseWriter, req *http.Request) {
	acceptsContentType := getContentType(req)

	op := Operation{}
	op.ProceedCh = make(chan struct{})
	api.OP <- op
	// block until Ticket master gives us a go
	<-op.ProceedCh
	{
		// do you operation
	}
	log.Println(acceptsContentType)

	// handle ACL's here at bucket level
	if !api.isValidOp(w, req, acceptsContentType) {
		return
	}

	objectResourcesMetadata := getObjectResources(req.URL.Query())
	if objectResourcesMetadata.MaxParts == 0 {
		objectResourcesMetadata.MaxParts = maxPartsList
	}

	vars := mux.Vars(req)
	bucket := vars["bucket"]
	object := vars["object"]
	log.Println(bucket, object)
}

// CompleteMultipartUploadHandler - Complete multipart upload
func (api Minio) CompleteMultipartUploadHandler(w http.ResponseWriter, req *http.Request) {
	acceptsContentType := getContentType(req)

	op := Operation{}
	op.ProceedCh = make(chan struct{})
	api.OP <- op
	// block until Ticket master gives us a go
	<-op.ProceedCh
	{
		// do you operation
	}
	log.Println(acceptsContentType)

	// handle ACL's here at bucket level
	if !api.isValidOp(w, req, acceptsContentType) {
		return
	}

	decoder := xml.NewDecoder(req.Body)
	parts := &CompleteMultipartUpload{}
	err := decoder.Decode(parts)
	if err != nil {
		log.Error.Println(iodine.New(err, nil))
		writeErrorResponse(w, req, InternalError, acceptsContentType, req.URL.Path)
		return
	}
	if !sort.IsSorted(completedParts(parts.Part)) {
		writeErrorResponse(w, req, InvalidPartOrder, acceptsContentType, req.URL.Path)
		return
	}

	vars := mux.Vars(req)
	bucket := vars["bucket"]
	object := vars["object"]
	log.Println(bucket, object)

	//objectResourcesMetadata := getObjectResources(req.URL.Query())

	partMap := make(map[int]string)
	for _, part := range parts.Part {
		partMap[part.PartNumber] = part.ETag
	}

}

/// Delete API

// DeleteBucketHandler - Delete bucket
func (api Minio) DeleteBucketHandler(w http.ResponseWriter, req *http.Request) {
	error := getErrorCode(NotImplemented)
	w.WriteHeader(error.HTTPStatusCode)
}

// DeleteObjectHandler - Delete object
func (api Minio) DeleteObjectHandler(w http.ResponseWriter, req *http.Request) {
	error := getErrorCode(NotImplemented)
	w.WriteHeader(error.HTTPStatusCode)
}
