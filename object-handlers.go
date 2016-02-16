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
	"net/http"
	"net/url"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/minio/minio/pkg/fs"
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

// setResponseHeaders - set any requested parameters as response headers.
func setResponseHeaders(w http.ResponseWriter, reqParams url.Values) {
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
func (api CloudStorageAPI) GetObjectHandler(w http.ResponseWriter, r *http.Request) {
	var object, bucket string
	vars := mux.Vars(r)
	bucket = vars["bucket"]
	object = vars["object"]

	if isRequestRequiresACLCheck(r) {
		if api.Filesystem.IsPrivateBucket(bucket) {
			writeErrorResponse(w, r, AccessDenied, r.URL.Path)
			return
		}
	}

	if !isSignV4ReqAuthenticated(api.Signature, r) {
		writeErrorResponse(w, r, SignatureDoesNotMatch, r.URL.Path)
		return
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

	// Set any additional ruested response headers.
	setResponseHeaders(w, r.URL.Query())

	// Get the object.
	if _, err = api.Filesystem.GetObject(w, bucket, object, hrange.start, hrange.length); err != nil {
		errorIf(err.Trace(), "GetObject failed.", nil)
		return
	}
}

// HeadObjectHandler - HEAD Object
// -----------
// The HEAD operation retrieves metadata from an object without returning the object itself.
func (api CloudStorageAPI) HeadObjectHandler(w http.ResponseWriter, r *http.Request) {
	var object, bucket string
	vars := mux.Vars(r)
	bucket = vars["bucket"]
	object = vars["object"]

	if isRequestRequiresACLCheck(r) {
		if api.Filesystem.IsPrivateBucket(bucket) {
			writeErrorResponse(w, r, AccessDenied, r.URL.Path)
			return
		}
	}

	if !isSignV4ReqAuthenticated(api.Signature, r) {
		writeErrorResponse(w, r, SignatureDoesNotMatch, r.URL.Path)
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
	setObjectHeaders(w, metadata, nil)
	w.WriteHeader(http.StatusOK)
}

// PutObjectHandler - PUT Object
// ----------
// This implementation of the PUT operation adds an object to a bucket.
func (api CloudStorageAPI) PutObjectHandler(w http.ResponseWriter, r *http.Request) {
	var object, bucket string
	vars := mux.Vars(r)
	bucket = vars["bucket"]
	object = vars["object"]

	if isRequestRequiresACLCheck(r) {
		if api.Filesystem.IsPrivateBucket(bucket) || api.Filesystem.IsReadOnlyBucket(bucket) {
			writeErrorResponse(w, r, AccessDenied, r.URL.Path)
			return
		}
	}

	// get Content-MD5 sent by client and verify if valid
	md5 := r.Header.Get("Content-MD5")
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
	api.Signature.SetHTTPRequestToVerify(r)

	// Create object.
	metadata, err := api.Filesystem.CreateObject(bucket, object, md5, size, r.Body, api.Signature)
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

/// Multipart CloudStorageAPI

// NewMultipartUploadHandler - New multipart upload
func (api CloudStorageAPI) NewMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	var object, bucket string
	vars := mux.Vars(r)
	bucket = vars["bucket"]
	object = vars["object"]

	if isRequestRequiresACLCheck(r) {
		if api.Filesystem.IsPrivateBucket(bucket) || api.Filesystem.IsReadOnlyBucket(bucket) {
			writeErrorResponse(w, r, AccessDenied, r.URL.Path)
			return
		}
	}

	if !isSignV4ReqAuthenticated(api.Signature, r) {
		writeErrorResponse(w, r, SignatureDoesNotMatch, r.URL.Path)
		return
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
	encodedSuccessResponse := encodeSuccessResponse(response)
	// write headers
	setCommonHeaders(w)
	// write success response.
	writeSuccessResponse(w, encodedSuccessResponse)
}

// PutObjectPartHandler - Upload part
func (api CloudStorageAPI) PutObjectPartHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	if isRequestRequiresACLCheck(r) {
		if api.Filesystem.IsPrivateBucket(bucket) || api.Filesystem.IsReadOnlyBucket(bucket) {
			writeErrorResponse(w, r, AccessDenied, r.URL.Path)
			return
		}
	}

	// get Content-MD5 sent by client and verify if valid
	md5 := r.Header.Get("Content-MD5")
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

	var partID int
	{
		var err error
		partID, err = strconv.Atoi(partIDString)
		if err != nil {
			writeErrorResponse(w, r, InvalidPart, r.URL.Path)
			return
		}
	}

	// Set http request.
	api.Signature.SetHTTPRequestToVerify(r)

	calculatedMD5, err := api.Filesystem.CreateObjectPart(bucket, object, uploadID, md5, partID, size, r.Body, api.Signature)
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
	if calculatedMD5 != "" {
		w.Header().Set("ETag", "\""+calculatedMD5+"\"")
	}
	writeSuccessResponse(w, nil)
}

// AbortMultipartUploadHandler - Abort multipart upload
func (api CloudStorageAPI) AbortMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	if isRequestRequiresACLCheck(r) {
		if api.Filesystem.IsPrivateBucket(bucket) || api.Filesystem.IsReadOnlyBucket(bucket) {
			writeErrorResponse(w, r, AccessDenied, r.URL.Path)
			return
		}
	}

	if !isSignV4ReqAuthenticated(api.Signature, r) {
		writeErrorResponse(w, r, SignatureDoesNotMatch, r.URL.Path)
		return
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
func (api CloudStorageAPI) ListObjectPartsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	if isRequestRequiresACLCheck(r) {
		if api.Filesystem.IsPrivateBucket(bucket) || api.Filesystem.IsReadOnlyBucket(bucket) {
			writeErrorResponse(w, r, AccessDenied, r.URL.Path)
			return
		}
	}

	if !isSignV4ReqAuthenticated(api.Signature, r) {
		writeErrorResponse(w, r, SignatureDoesNotMatch, r.URL.Path)
		return
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
	encodedSuccessResponse := encodeSuccessResponse(response)
	// write headers.
	setCommonHeaders(w)
	// write success response.
	writeSuccessResponse(w, encodedSuccessResponse)
}

// CompleteMultipartUploadHandler - Complete multipart upload
func (api CloudStorageAPI) CompleteMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	if isRequestRequiresACLCheck(r) {
		if api.Filesystem.IsPrivateBucket(bucket) || api.Filesystem.IsReadOnlyBucket(bucket) {
			writeErrorResponse(w, r, AccessDenied, r.URL.Path)
			return
		}
	}

	// Set http request for signature.
	api.Signature.SetHTTPRequestToVerify(r)

	// Extract object resources.
	objectResourcesMetadata := getObjectResources(r.URL.Query())

	// Complete multipart upload.
	metadata, err := api.Filesystem.CompleteMultipartUpload(bucket, object, objectResourcesMetadata.UploadID, r.Body, api.Signature)
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
	response := generateCompleteMultpartUploadResponse(bucket, object, r.URL.String(), metadata.MD5)
	encodedSuccessResponse := encodeSuccessResponse(response)
	// write headers
	setCommonHeaders(w)
	// write success response.
	writeSuccessResponse(w, encodedSuccessResponse)
}

/// Delete CloudStorageAPI

// DeleteObjectHandler - Delete object
func (api CloudStorageAPI) DeleteObjectHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	if isRequestRequiresACLCheck(r) {
		if api.Filesystem.IsPrivateBucket(bucket) || api.Filesystem.IsReadOnlyBucket(bucket) {
			writeErrorResponse(w, r, AccessDenied, r.URL.Path)
			return
		}
	}

	if !isSignV4ReqAuthenticated(api.Signature, r) {
		writeErrorResponse(w, r, SignatureDoesNotMatch, r.URL.Path)
		return
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
