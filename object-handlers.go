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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implieapi.Filesystem.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	mux "github.com/gorilla/mux"
	"github.com/minio/minio/pkg/fs"
	"github.com/minio/minio/pkg/probe"
)

const (
	maxPartsList = 1000
)

// supportedGetReqParams - supported request parameters for GET
// presigned request.
var supportedGetReqParams = map[string]string{
	"response-expires":             "Expires",
	"response-content-type":        "Content-Type",
	"response-cache-control":       "Cache-Control",
	"response-content-disposition": "Content-Disposition",
}

// setGetRespHeaders - set any requested parameters as response headers.
func setGetRespHeaders(w http.ResponseWriter, reqParams url.Values) {
	for k, v := range reqParams {
		if header, ok := supportedGetReqParams[k]; ok {
			w.Header()[header] = v
		}
	}
}

// GetObjectHandler - GET Object
// ----------
// This implementation of the GET operation retrieves object. To use GET,
// you must have READ access to the object.
func (api storageAPI) GetObjectHandler(w http.ResponseWriter, r *http.Request) {
	var object, bucket string
	vars := mux.Vars(r)
	bucket = vars["bucket"]
	object = vars["object"]

	switch getRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, r, AccessDenied, r.URL.Path)
		return
	case authTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/mpuAndPermissions.html
		if isAllowed, s3Error := enforceBucketPolicy("s3:GetObject", bucket, r.URL); !isAllowed {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	case authTypePresigned, authTypeSigned:
		if match, s3Error := isSignV4ReqAuthenticated(api.Signature, r); !match {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}

	metadata, err := api.Filesystem.GetObjectMetadata(bucket, object)
	if err != nil {
		errorIf(err.Trace(), "GetObject failed.", nil)
		switch err.ToGoError().(type) {
		case fs.BucketNameInvalid:
			writeErrorResponse(w, r, InvalidBucketName, r.URL.Path)
		case fs.BucketNotFound:
			writeErrorResponse(w, r, NoSuchBucket, r.URL.Path)
		case fs.ObjectNotFound:
			writeErrorResponse(w, r, NoSuchKey, r.URL.Path)
		case fs.ObjectNameInvalid:
			writeErrorResponse(w, r, NoSuchKey, r.URL.Path)
		default:
			writeErrorResponse(w, r, InternalError, r.URL.Path)
		}
		return
	}

	var hrange *httpRange
	hrange, err = getRequestedRange(r.Header.Get("Range"), metadata.Size)
	if err != nil {
		writeErrorResponse(w, r, InvalidRange, r.URL.Path)
		return
	}

	// Set standard object headers.
	setObjectHeaders(w, metadata, hrange)

	// Verify 'If-Modified-Since' and 'If-Unmodified-Since'.
	if checkLastModified(w, r, metadata.LastModified) {
		return
	}
	// Verify 'If-Match' and 'If-None-Match'.
	if checkETag(w, r) {
		return
	}

	// Set any additional requested response headers.
	setGetRespHeaders(w, r.URL.Query())

	// Get the object.
	if _, err = api.Filesystem.GetObject(w, bucket, object, hrange.start, hrange.length); err != nil {
		errorIf(err.Trace(), "GetObject failed.", nil)
		return
	}
}

var unixEpochTime = time.Unix(0, 0)

// checkLastModified implements If-Modified-Since and
// If-Unmodified-Since checks.
//
// modtime is the modification time of the resource to be served, or
// IsZero(). return value is whether this request is now complete.
func checkLastModified(w http.ResponseWriter, r *http.Request, modtime time.Time) bool {
	if modtime.IsZero() || modtime.Equal(unixEpochTime) {
		// If the object doesn't have a modtime (IsZero), or the modtime
		// is obviously garbage (Unix time == 0), then ignore modtimes
		// and don't process the If-Modified-Since header.
		return false
	}

	// The Date-Modified header truncates sub-second precision, so
	// use mtime < t+1s instead of mtime <= t to check for unmodified.
	if _, ok := r.Header["If-Modified-Since"]; ok {
		// Return the object only if it has been modified since the
		// specified time, otherwise return a 304 (not modified).
		t, err := time.Parse(http.TimeFormat, r.Header.Get("If-Modified-Since"))
		if err == nil && modtime.Before(t.Add(1*time.Second)) {
			h := w.Header()
			// Remove following headers if already set.
			delete(h, "Content-Type")
			delete(h, "Content-Length")
			delete(h, "Content-Range")
			w.WriteHeader(http.StatusNotModified)
			return true
		}
	} else if _, ok := r.Header["If-Unmodified-Since"]; ok {
		// Return the object only if it has not been modified since
		// the specified time, otherwise return a 412 (precondition failed).
		t, err := time.Parse(http.TimeFormat, r.Header.Get("If-Unmodified-Since"))
		if err == nil && modtime.After(t.Add(1*time.Second)) {
			h := w.Header()
			// Remove following headers if already set.
			delete(h, "Content-Type")
			delete(h, "Content-Length")
			delete(h, "Content-Range")
			w.WriteHeader(http.StatusPreconditionFailed)
			return true
		}
	}
	w.Header().Set("Last-Modified", modtime.UTC().Format(http.TimeFormat))
	return false
}

// checkETag implements If-None-Match and If-Match checks.
//
// The ETag or modtime must have been previously set in the
// ResponseWriter's headers.  The modtime is only compared at second
// granularity and may be the zero value to mean unknown.
//
// The return value is whether this request is now considered done.
func checkETag(w http.ResponseWriter, r *http.Request) bool {
	etag := w.Header().Get("ETag")
	// Must know ETag.
	if etag == "" {
		return false
	}
	if inm := r.Header.Get("If-None-Match"); inm != "" {
		// Return the object only if its entity tag (ETag) is
		// different from the one specified; otherwise, return a 304
		// (not modified).
		if r.Method != "GET" && r.Method != "HEAD" {
			return false
		}
		if inm == etag || inm == "*" {
			h := w.Header()
			// Remove following headers if already set.
			delete(h, "Content-Type")
			delete(h, "Content-Length")
			delete(h, "Content-Range")
			w.WriteHeader(http.StatusNotModified)
			return true
		}
	} else if im := r.Header.Get("If-Match"); im != "" {
		// Return the object only if its entity tag (ETag) is the same
		// as the one specified; otherwise, return a 412 (precondition failed).
		if r.Method != "GET" && r.Method != "HEAD" {
			return false
		}
		if im != etag {
			h := w.Header()
			// Remove following headers if already set.
			delete(h, "Content-Type")
			delete(h, "Content-Length")
			delete(h, "Content-Range")
			w.WriteHeader(http.StatusPreconditionFailed)
			return true
		}
	}
	return false
}

// HeadObjectHandler - HEAD Object
// -----------
// The HEAD operation retrieves metadata from an object without returning the object itself.
func (api storageAPI) HeadObjectHandler(w http.ResponseWriter, r *http.Request) {
	var object, bucket string
	vars := mux.Vars(r)
	bucket = vars["bucket"]
	object = vars["object"]

	if match, s3Error := isSignV4ReqAuthenticated(api.Signature, r); !match {
		writeErrorResponse(w, r, s3Error, r.URL.Path)
		return
	}

	metadata, err := api.Filesystem.GetObjectMetadata(bucket, object)
	if err != nil {
		switch err.ToGoError().(type) {
		case fs.BucketNameInvalid:
			writeErrorResponse(w, r, InvalidBucketName, r.URL.Path)
		case fs.BucketNotFound:
			writeErrorResponse(w, r, NoSuchBucket, r.URL.Path)
		case fs.ObjectNotFound:
			writeErrorResponse(w, r, NoSuchKey, r.URL.Path)
		case fs.ObjectNameInvalid:
			writeErrorResponse(w, r, NoSuchKey, r.URL.Path)
		default:
			writeErrorResponse(w, r, InternalError, r.URL.Path)
		}
		return
	}

	// Set standard object headers.
	setObjectHeaders(w, metadata, nil)

	// Verify 'If-Modified-Since' and 'If-Unmodified-Since'.
	if checkLastModified(w, r, metadata.LastModified) {
		return
	}

	// Verify 'If-Match' and 'If-None-Match'.
	if checkETag(w, r) {
		return
	}

	// Successfull response.
	w.WriteHeader(http.StatusOK)
}

// CopyObjectHandler - Copy Object
// ----------
// This implementation of the PUT operation adds an object to a bucket
// while reading the object from another source.
//
// TODO: Does not support following request headers just yet.
//  - x-amz-copy-source-if-match
//  - x-amz-copy-source-if-none-match
//  - x-amz-copy-source-if-unmodified-since
//  - x-amz-copy-source-if-modified-since
func (api storageAPI) CopyObjectHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	switch getRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, r, AccessDenied, r.URL.Path)
		return
	case authTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/mpuAndPermissions.html
		if isAllowed, s3Error := enforceBucketPolicy("s3:GetBucketLocation", bucket, r.URL); !isAllowed {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	case authTypePresigned, authTypeSigned:
		if match, s3Error := isSignV4ReqAuthenticated(api.Signature, r); !match {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}

	// TODO: Reject requests where body/payload is present, for now we
	// don't even read it.

	// objectSource
	objectSource := r.Header.Get("X-Amz-Copy-Source")

	// Skip the first element if it is '/', split the rest.
	if strings.HasPrefix(objectSource, "/") {
		objectSource = objectSource[1:]
	}
	splits := strings.SplitN(objectSource, "/", 2)

	// Save sourceBucket and sourceObject extracted from url Path.
	var sourceBucket, sourceObject string
	if len(splits) == 2 {
		sourceBucket = splits[0]
		sourceObject = splits[1]
	}
	// If source object is empty, reply back error.
	if sourceObject == "" {
		writeErrorResponse(w, r, InvalidCopySource, r.URL.Path)
		return
	}

	// Source and destination objects cannot be same, reply back error.
	if sourceObject == object && sourceBucket == bucket {
		writeErrorResponse(w, r, InvalidCopyDest, r.URL.Path)
		return
	}

	metadata, err := api.Filesystem.GetObjectMetadata(sourceBucket, sourceObject)
	if err != nil {
		errorIf(err.Trace(), "GetObjectMetadata failed.", nil)
		switch err.ToGoError().(type) {
		case fs.BucketNameInvalid:
			writeErrorResponse(w, r, InvalidBucketName, objectSource)
		case fs.BucketNotFound:
			writeErrorResponse(w, r, NoSuchBucket, objectSource)
		case fs.ObjectNotFound:
			writeErrorResponse(w, r, NoSuchKey, objectSource)
		case fs.ObjectNameInvalid:
			writeErrorResponse(w, r, NoSuchKey, objectSource)
		default:
			writeErrorResponse(w, r, InternalError, objectSource)
		}
		return
	}

	/// maximum Upload size for object in a single CopyObject operation.
	if isMaxObjectSize(metadata.Size) {
		writeErrorResponse(w, r, EntityTooLarge, objectSource)
		return
	}

	// Initialize a pipe for data pipe line.
	reader, writer := io.Pipe()

	// Start writing in a routine.
	go func() {
		defer writer.Close()
		if _, getErr := api.Filesystem.GetObject(writer, sourceBucket, sourceObject, 0, 0); getErr != nil {
			writer.CloseWithError(probe.WrapError(getErr))
			return
		}
	}()

	// Verify md5sum.
	expectedMD5Sum := metadata.MD5
	// Size of object.
	size := metadata.Size

	// Create the object.
	metadata, err = api.Filesystem.CreateObject(bucket, object, expectedMD5Sum, size, reader, nil)
	if err != nil {
		errorIf(err.Trace(), "CreateObject failed.", nil)
		switch err.ToGoError().(type) {
		case fs.RootPathFull:
			writeErrorResponse(w, r, RootPathFull, r.URL.Path)
		case fs.BucketNotFound:
			writeErrorResponse(w, r, NoSuchBucket, r.URL.Path)
		case fs.BucketNameInvalid:
			writeErrorResponse(w, r, InvalidBucketName, r.URL.Path)
		case fs.BadDigest:
			writeErrorResponse(w, r, BadDigest, r.URL.Path)
		case fs.IncompleteBody:
			writeErrorResponse(w, r, IncompleteBody, r.URL.Path)
		case fs.InvalidDigest:
			writeErrorResponse(w, r, InvalidDigest, r.URL.Path)
		case fs.ObjectExistsAsPrefix:
			writeErrorResponse(w, r, ObjectExistsAsPrefix, r.URL.Path)
		default:
			writeErrorResponse(w, r, InternalError, r.URL.Path)
		}
		return
	}
	response := generateCopyObjectResponse(metadata.MD5, metadata.LastModified)
	encodedSuccessResponse := encodeResponse(response)
	// write headers
	setCommonHeaders(w)
	// write success response.
	writeSuccessResponse(w, encodedSuccessResponse)
}

// PutObjectHandler - PUT Object
// ----------
// This implementation of the PUT operation adds an object to a bucket.
func (api storageAPI) PutObjectHandler(w http.ResponseWriter, r *http.Request) {
	// If the matching failed, it means that the X-Amz-Copy-Source was
	// wrong, fail right here.
	if _, ok := r.Header["X-Amz-Copy-Source"]; ok {
		writeErrorResponse(w, r, InvalidCopySource, r.URL.Path)
		return
	}
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	// get Content-Md5 sent by client and verify if valid
	md5 := r.Header.Get("Content-Md5")
	if !isValidMD5(md5) {
		writeErrorResponse(w, r, InvalidDigest, r.URL.Path)
		return
	}
	/// if Content-Length is unknown/missing, deny the request
	size := r.ContentLength
	if size == -1 && !contains(r.TransferEncoding, "chunked") {
		writeErrorResponse(w, r, MissingContentLength, r.URL.Path)
		return
	}
	/// maximum Upload size for objects in a single operation
	if isMaxObjectSize(size) {
		writeErrorResponse(w, r, EntityTooLarge, r.URL.Path)
		return
	}

	// Set http request for signature.
	auth := api.Signature.SetHTTPRequestToVerify(r)
	var metadata fs.ObjectMetadata
	var err *probe.Error
	switch getRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, r, AccessDenied, r.URL.Path)
		return
	case authTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/mpuAndPermissions.html
		if isAllowed, s3Error := enforceBucketPolicy("s3:PutObject", bucket, r.URL); !isAllowed {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
		// Create anonymous object.
		metadata, err = api.Filesystem.CreateObject(bucket, object, md5, size, r.Body, nil)
	case authTypePresigned:
		// For presigned requests verify them right here.
		var ok bool
		ok, err = auth.DoesPresignedSignatureMatch()
		if err != nil {
			errorIf(err.Trace(r.URL.String()), "Presigned signature verification failed.", nil)
			writeErrorResponse(w, r, SignatureDoesNotMatch, r.URL.Path)
			return
		}
		if !ok {
			writeErrorResponse(w, r, SignatureDoesNotMatch, r.URL.Path)
			return
		}
		// Create presigned object.
		metadata, err = api.Filesystem.CreateObject(bucket, object, md5, size, r.Body, nil)
	case authTypeSigned:
		// Create object.
		metadata, err = api.Filesystem.CreateObject(bucket, object, md5, size, r.Body, &auth)
	}
	if err != nil {
		errorIf(err.Trace(), "CreateObject failed.", nil)
		switch err.ToGoError().(type) {
		case fs.RootPathFull:
			writeErrorResponse(w, r, RootPathFull, r.URL.Path)
		case fs.BucketNotFound:
			writeErrorResponse(w, r, NoSuchBucket, r.URL.Path)
		case fs.BucketNameInvalid:
			writeErrorResponse(w, r, InvalidBucketName, r.URL.Path)
		case fs.BadDigest:
			writeErrorResponse(w, r, BadDigest, r.URL.Path)
		case fs.SignDoesNotMatch:
			writeErrorResponse(w, r, SignatureDoesNotMatch, r.URL.Path)
		case fs.IncompleteBody:
			writeErrorResponse(w, r, IncompleteBody, r.URL.Path)
		case fs.InvalidDigest:
			writeErrorResponse(w, r, InvalidDigest, r.URL.Path)
		case fs.ObjectExistsAsPrefix:
			writeErrorResponse(w, r, ObjectExistsAsPrefix, r.URL.Path)
		default:
			writeErrorResponse(w, r, InternalError, r.URL.Path)
		}
		return
	}
	if metadata.MD5 != "" {
		w.Header().Set("ETag", "\""+metadata.MD5+"\"")
	}
	writeSuccessResponse(w, nil)
}

/// Multipart storageAPI

// NewMultipartUploadHandler - New multipart upload
func (api storageAPI) NewMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	var object, bucket string
	vars := mux.Vars(r)
	bucket = vars["bucket"]
	object = vars["object"]

	switch getRequestAuthType(r) {
	case authTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/mpuAndPermissions.html
		if isAllowed, s3Error := enforceBucketPolicy("s3:PutObject", bucket, r.URL); !isAllowed {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	default:
		if match, s3Error := isSignV4ReqAuthenticated(api.Signature, r); !match {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}

	uploadID, err := api.Filesystem.NewMultipartUpload(bucket, object)
	if err != nil {
		errorIf(err.Trace(), "NewMultipartUpload failed.", nil)
		switch err.ToGoError().(type) {
		case fs.RootPathFull:
			writeErrorResponse(w, r, RootPathFull, r.URL.Path)
		case fs.BucketNameInvalid:
			writeErrorResponse(w, r, InvalidBucketName, r.URL.Path)
		case fs.BucketNotFound:
			writeErrorResponse(w, r, NoSuchBucket, r.URL.Path)
		case fs.ObjectNotFound:
			writeErrorResponse(w, r, NoSuchKey, r.URL.Path)
		case fs.ObjectNameInvalid:
			writeErrorResponse(w, r, NoSuchKey, r.URL.Path)
		default:
			writeErrorResponse(w, r, InternalError, r.URL.Path)
		}
		return
	}

	response := generateInitiateMultipartUploadResponse(bucket, object, uploadID)
	encodedSuccessResponse := encodeResponse(response)
	// write headers
	setCommonHeaders(w)
	// write success response.
	writeSuccessResponse(w, encodedSuccessResponse)
}

// PutObjectPartHandler - Upload part
func (api storageAPI) PutObjectPartHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	// get Content-Md5 sent by client and verify if valid
	md5 := r.Header.Get("Content-Md5")
	if !isValidMD5(md5) {
		writeErrorResponse(w, r, InvalidDigest, r.URL.Path)
		return
	}

	/// if Content-Length is unknown/missing, throw away
	size := r.ContentLength
	if size == -1 {
		writeErrorResponse(w, r, MissingContentLength, r.URL.Path)
		return
	}

	/// maximum Upload size for multipart objects in a single operation
	if isMaxObjectSize(size) {
		writeErrorResponse(w, r, EntityTooLarge, r.URL.Path)
		return
	}

	uploadID := r.URL.Query().Get("uploadId")
	partIDString := r.URL.Query().Get("partNumber")

	partID, e := strconv.Atoi(partIDString)
	if e != nil {
		writeErrorResponse(w, r, InvalidPart, r.URL.Path)
		return
	}

	// Set http request for signature.
	auth := api.Signature.SetHTTPRequestToVerify(r)
	var partMD5 string
	var err *probe.Error
	switch getRequestAuthType(r) {
	case authTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/mpuAndPermissions.html
		if isAllowed, s3Error := enforceBucketPolicy("s3:PutObject", bucket, r.URL); !isAllowed {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
		// No need to verify signature, anonymous request access is
		// already allowed.
		partMD5, err = api.Filesystem.CreateObjectPart(bucket, object, uploadID, md5, partID, size, r.Body, nil)
	case authTypePresigned:
		// For presigned requests verify right here.
		var ok bool
		ok, err = auth.DoesPresignedSignatureMatch()
		if err != nil {
			errorIf(err.Trace(r.URL.String()), "Presigned signature verification failed.", nil)
			writeErrorResponse(w, r, SignatureDoesNotMatch, r.URL.Path)
			return
		}
		if !ok {
			writeErrorResponse(w, r, SignatureDoesNotMatch, r.URL.Path)
			return
		}
		partMD5, err = api.Filesystem.CreateObjectPart(bucket, object, uploadID, md5, partID, size, r.Body, nil)
	default:
		partMD5, err = api.Filesystem.CreateObjectPart(bucket, object, uploadID, md5, partID, size, r.Body, &auth)
	}
	if err != nil {
		errorIf(err.Trace(), "CreateObjectPart failed.", nil)
		switch err.ToGoError().(type) {
		case fs.RootPathFull:
			writeErrorResponse(w, r, RootPathFull, r.URL.Path)
		case fs.InvalidUploadID:
			writeErrorResponse(w, r, NoSuchUpload, r.URL.Path)
		case fs.BadDigest:
			writeErrorResponse(w, r, BadDigest, r.URL.Path)
		case fs.SignDoesNotMatch:
			writeErrorResponse(w, r, SignatureDoesNotMatch, r.URL.Path)
		case fs.IncompleteBody:
			writeErrorResponse(w, r, IncompleteBody, r.URL.Path)
		case fs.InvalidDigest:
			writeErrorResponse(w, r, InvalidDigest, r.URL.Path)
		default:
			writeErrorResponse(w, r, InternalError, r.URL.Path)
		}
		return
	}
	if partMD5 != "" {
		w.Header().Set("ETag", "\""+partMD5+"\"")
	}
	writeSuccessResponse(w, nil)
}

// AbortMultipartUploadHandler - Abort multipart upload
func (api storageAPI) AbortMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	switch getRequestAuthType(r) {
	case authTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/mpuAndPermissions.html
		if isAllowed, s3Error := enforceBucketPolicy("s3:AbortMultipartUpload", bucket, r.URL); !isAllowed {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	default:
		if match, s3Error := isSignV4ReqAuthenticated(api.Signature, r); !match {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}

	objectResourcesMetadata := getObjectResources(r.URL.Query())
	err := api.Filesystem.AbortMultipartUpload(bucket, object, objectResourcesMetadata.UploadID)
	if err != nil {
		errorIf(err.Trace(), "AbortMutlipartUpload failed.", nil)
		switch err.ToGoError().(type) {
		case fs.BucketNameInvalid:
			writeErrorResponse(w, r, InvalidBucketName, r.URL.Path)
		case fs.BucketNotFound:
			writeErrorResponse(w, r, NoSuchBucket, r.URL.Path)
		case fs.ObjectNotFound:
			writeErrorResponse(w, r, NoSuchKey, r.URL.Path)
		case fs.ObjectNameInvalid:
			writeErrorResponse(w, r, NoSuchKey, r.URL.Path)
		case fs.InvalidUploadID:
			writeErrorResponse(w, r, NoSuchUpload, r.URL.Path)
		default:
			writeErrorResponse(w, r, InternalError, r.URL.Path)
		}
		return
	}
	writeSuccessNoContent(w)
}

// ListObjectPartsHandler - List object parts
func (api storageAPI) ListObjectPartsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	switch getRequestAuthType(r) {
	case authTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/mpuAndPermissions.html
		if isAllowed, s3Error := enforceBucketPolicy("s3:ListMultipartUploadParts", bucket, r.URL); !isAllowed {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	default:
		if match, s3Error := isSignV4ReqAuthenticated(api.Signature, r); !match {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}

	objectResourcesMetadata := getObjectResources(r.URL.Query())
	if objectResourcesMetadata.PartNumberMarker < 0 {
		writeErrorResponse(w, r, InvalidPartNumberMarker, r.URL.Path)
		return
	}
	if objectResourcesMetadata.MaxParts < 0 {
		writeErrorResponse(w, r, InvalidMaxParts, r.URL.Path)
		return
	}
	if objectResourcesMetadata.MaxParts == 0 {
		objectResourcesMetadata.MaxParts = maxPartsList
	}

	objectResourcesMetadata, err := api.Filesystem.ListObjectParts(bucket, object, objectResourcesMetadata)
	if err != nil {
		errorIf(err.Trace(), "ListObjectParts failed.", nil)
		switch err.ToGoError().(type) {
		case fs.BucketNameInvalid:
			writeErrorResponse(w, r, InvalidBucketName, r.URL.Path)
		case fs.BucketNotFound:
			writeErrorResponse(w, r, NoSuchBucket, r.URL.Path)
		case fs.ObjectNotFound:
			writeErrorResponse(w, r, NoSuchKey, r.URL.Path)
		case fs.ObjectNameInvalid:
			writeErrorResponse(w, r, NoSuchKey, r.URL.Path)
		case fs.InvalidUploadID:
			writeErrorResponse(w, r, NoSuchUpload, r.URL.Path)
		default:
			writeErrorResponse(w, r, InternalError, r.URL.Path)
		}
		return
	}
	response := generateListPartsResponse(objectResourcesMetadata)
	encodedSuccessResponse := encodeResponse(response)
	// Write headers.
	setCommonHeaders(w)
	// Write success response.
	writeSuccessResponse(w, encodedSuccessResponse)
}

// CompleteMultipartUploadHandler - Complete multipart upload
func (api storageAPI) CompleteMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	// Extract object resources.
	objectResourcesMetadata := getObjectResources(r.URL.Query())

	// Set http request for signature.
	auth := api.Signature.SetHTTPRequestToVerify(r)

	var metadata fs.ObjectMetadata
	var err *probe.Error
	switch getRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, r, AccessDenied, r.URL.Path)
		return
	case authTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/mpuAndPermissions.html
		if isAllowed, s3Error := enforceBucketPolicy("s3:PutObject", bucket, r.URL); !isAllowed {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
		// Complete multipart upload anonymous.
		metadata, err = api.Filesystem.CompleteMultipartUpload(bucket, object, objectResourcesMetadata.UploadID, r.Body, nil)
	case authTypePresigned:
		// For presigned requests verify right here.
		var ok bool
		ok, err = auth.DoesPresignedSignatureMatch()
		if err != nil {
			errorIf(err.Trace(r.URL.String()), "Presigned signature verification failed.", nil)
			writeErrorResponse(w, r, SignatureDoesNotMatch, r.URL.Path)
			return
		}
		if !ok {
			writeErrorResponse(w, r, SignatureDoesNotMatch, r.URL.Path)
			return
		}
		// Complete multipart upload presigned.
		metadata, err = api.Filesystem.CompleteMultipartUpload(bucket, object, objectResourcesMetadata.UploadID, r.Body, nil)
	case authTypeSigned:
		// Complete multipart upload.
		metadata, err = api.Filesystem.CompleteMultipartUpload(bucket, object, objectResourcesMetadata.UploadID, r.Body, &auth)
	}
	if err != nil {
		errorIf(err.Trace(), "CompleteMultipartUpload failed.", nil)
		switch err.ToGoError().(type) {
		case fs.BucketNameInvalid:
			writeErrorResponse(w, r, InvalidBucketName, r.URL.Path)
		case fs.BucketNotFound:
			writeErrorResponse(w, r, NoSuchBucket, r.URL.Path)
		case fs.ObjectNotFound:
			writeErrorResponse(w, r, NoSuchKey, r.URL.Path)
		case fs.ObjectNameInvalid:
			writeErrorResponse(w, r, NoSuchKey, r.URL.Path)
		case fs.InvalidUploadID:
			writeErrorResponse(w, r, NoSuchUpload, r.URL.Path)
		case fs.InvalidPart:
			writeErrorResponse(w, r, InvalidPart, r.URL.Path)
		case fs.InvalidPartOrder:
			writeErrorResponse(w, r, InvalidPartOrder, r.URL.Path)
		case fs.SignDoesNotMatch:
			writeErrorResponse(w, r, SignatureDoesNotMatch, r.URL.Path)
		case fs.IncompleteBody:
			writeErrorResponse(w, r, IncompleteBody, r.URL.Path)
		case fs.MalformedXML:
			writeErrorResponse(w, r, MalformedXML, r.URL.Path)
		default:
			writeErrorResponse(w, r, InternalError, r.URL.Path)
		}
		return
	}
	// get object location.
	location := getLocation(r)
	// Generate complete multipart response.
	response := generateCompleteMultpartUploadResponse(bucket, object, location, metadata.MD5)
	encodedSuccessResponse := encodeResponse(response)
	// write headers
	setCommonHeaders(w)
	// write success response.
	writeSuccessResponse(w, encodedSuccessResponse)
}

/// Delete storageAPI

// DeleteObjectHandler - delete an object
func (api storageAPI) DeleteObjectHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	switch getRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, r, AccessDenied, r.URL.Path)
		return
	case authTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/mpuAndPermissions.html
		if isAllowed, s3Error := enforceBucketPolicy("s3:DeleteObject", bucket, r.URL); !isAllowed {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	case authTypeSigned, authTypePresigned:
		if match, s3Error := isSignV4ReqAuthenticated(api.Signature, r); !match {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}
	err := api.Filesystem.DeleteObject(bucket, object)
	if err != nil {
		errorIf(err.Trace(), "DeleteObject failed.", nil)
		switch err.ToGoError().(type) {
		case fs.BucketNameInvalid:
			writeErrorResponse(w, r, InvalidBucketName, r.URL.Path)
		case fs.BucketNotFound:
			writeErrorResponse(w, r, NoSuchBucket, r.URL.Path)
		case fs.ObjectNotFound:
			writeErrorResponse(w, r, NoSuchKey, r.URL.Path)
		case fs.ObjectNameInvalid:
			writeErrorResponse(w, r, NoSuchKey, r.URL.Path)
		default:
			writeErrorResponse(w, r, InternalError, r.URL.Path)
		}
	}
	writeSuccessNoContent(w)
}
