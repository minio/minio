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
	"strconv"

	"github.com/gorilla/mux"
	"github.com/minio/minio-xl/pkg/probe"
	"github.com/minio/minio/pkg/fs"
)

const (
	maxPartsList = 1000
)

// GetObjectHandler - GET Object
// ----------
// This implementation of the GET operation retrieves object. To use GET,
// you must have READ access to the object.
func (api CloudStorageAPI) GetObjectHandler(w http.ResponseWriter, req *http.Request) {
	var object, bucket string
	vars := mux.Vars(req)
	bucket = vars["bucket"]
	object = vars["object"]

	if !api.Anonymous {
		if isRequestRequiresACLCheck(req) {
			if api.Filesystem.IsPrivateBucket(bucket) {
				writeErrorResponse(w, req, AccessDenied, req.URL.Path)
				return
			}
		}
	}

	metadata, err := api.Filesystem.GetObjectMetadata(bucket, object)
	if err != nil {
		errorIf(err.Trace(), "GetObject failed.", nil)
		switch err.ToGoError().(type) {
		case fs.BucketNameInvalid:
			writeErrorResponse(w, req, InvalidBucketName, req.URL.Path)
		case fs.BucketNotFound:
			writeErrorResponse(w, req, NoSuchBucket, req.URL.Path)
		case fs.ObjectNotFound:
			writeErrorResponse(w, req, NoSuchKey, req.URL.Path)
		case fs.ObjectNameInvalid:
			writeErrorResponse(w, req, NoSuchKey, req.URL.Path)
		default:
			writeErrorResponse(w, req, InternalError, req.URL.Path)
		}
		return
	}
	var hrange *httpRange
	hrange, err = getRequestedRange(req.Header.Get("Range"), metadata.Size)
	if err != nil {
		writeErrorResponse(w, req, InvalidRange, req.URL.Path)
		return
	}
	setObjectHeaders(w, metadata, hrange)
	if _, err = api.Filesystem.GetObject(w, bucket, object, hrange.start, hrange.length); err != nil {
		errorIf(err.Trace(), "GetObject failed.", nil)
		return
	}
}

// HeadObjectHandler - HEAD Object
// -----------
// The HEAD operation retrieves metadata from an object without returning the object itself.
func (api CloudStorageAPI) HeadObjectHandler(w http.ResponseWriter, req *http.Request) {
	var object, bucket string
	vars := mux.Vars(req)
	bucket = vars["bucket"]
	object = vars["object"]

	if !api.Anonymous {
		if isRequestRequiresACLCheck(req) {
			if api.Filesystem.IsPrivateBucket(bucket) {
				writeErrorResponse(w, req, AccessDenied, req.URL.Path)
				return
			}
		}
	}

	metadata, err := api.Filesystem.GetObjectMetadata(bucket, object)
	if err != nil {
		switch err.ToGoError().(type) {
		case fs.BucketNameInvalid:
			writeErrorResponse(w, req, InvalidBucketName, req.URL.Path)
		case fs.BucketNotFound:
			writeErrorResponse(w, req, NoSuchBucket, req.URL.Path)
		case fs.ObjectNotFound:
			writeErrorResponse(w, req, NoSuchKey, req.URL.Path)
		case fs.ObjectNameInvalid:
			writeErrorResponse(w, req, NoSuchKey, req.URL.Path)
		default:
			writeErrorResponse(w, req, InternalError, req.URL.Path)
		}
		return
	}
	setObjectHeaders(w, metadata, nil)
	w.WriteHeader(http.StatusOK)
}

// PutObjectHandler - PUT Object
// ----------
// This implementation of the PUT operation adds an object to a bucket.
func (api CloudStorageAPI) PutObjectHandler(w http.ResponseWriter, req *http.Request) {
	var object, bucket string
	vars := mux.Vars(req)
	bucket = vars["bucket"]
	object = vars["object"]

	if !api.Anonymous {
		if isRequestRequiresACLCheck(req) {
			if api.Filesystem.IsPrivateBucket(bucket) || api.Filesystem.IsReadOnlyBucket(bucket) {
				writeErrorResponse(w, req, AccessDenied, req.URL.Path)
				return
			}
		}
	}

	// get Content-MD5 sent by client and verify if valid
	md5 := req.Header.Get("Content-MD5")
	if !isValidMD5(md5) {
		writeErrorResponse(w, req, InvalidDigest, req.URL.Path)
		return
	}
	/// if Content-Length is unknown/missing, deny the request
	size := req.ContentLength
	if size == -1 {
		writeErrorResponse(w, req, MissingContentLength, req.URL.Path)
		return
	}
	/// maximum Upload size for objects in a single operation
	if isMaxObjectSize(size) {
		writeErrorResponse(w, req, EntityTooLarge, req.URL.Path)
		return
	}

	var signature *fs.Signature
	if !api.Anonymous {
		if isRequestSignatureV4(req) {
			// Init signature V4 verification
			var err *probe.Error
			signature, err = initSignatureV4(req)
			if err != nil {
				errorIf(err.Trace(), "Initializing signature v4 failed.", nil)
				writeErrorResponse(w, req, InternalError, req.URL.Path)
				return
			}
		}
	}

	metadata, err := api.Filesystem.CreateObject(bucket, object, md5, size, req.Body, signature)
	if err != nil {
		errorIf(err.Trace(), "CreateObject failed.", nil)
		switch err.ToGoError().(type) {
		case fs.RootPathFull:
			writeErrorResponse(w, req, RootPathFull, req.URL.Path)
		case fs.BucketNotFound:
			writeErrorResponse(w, req, NoSuchBucket, req.URL.Path)
		case fs.BucketNameInvalid:
			writeErrorResponse(w, req, InvalidBucketName, req.URL.Path)
		case fs.BadDigest:
			writeErrorResponse(w, req, BadDigest, req.URL.Path)
		case fs.MissingDateHeader:
			writeErrorResponse(w, req, RequestTimeTooSkewed, req.URL.Path)
		case fs.SignatureDoesNotMatch:
			writeErrorResponse(w, req, SignatureDoesNotMatch, req.URL.Path)
		case fs.IncompleteBody:
			writeErrorResponse(w, req, IncompleteBody, req.URL.Path)
		case fs.EntityTooLarge:
			writeErrorResponse(w, req, EntityTooLarge, req.URL.Path)
		case fs.InvalidDigest:
			writeErrorResponse(w, req, InvalidDigest, req.URL.Path)
		default:
			writeErrorResponse(w, req, InternalError, req.URL.Path)
		}
		return
	}
	w.Header().Set("ETag", "\""+metadata.Md5+"\"")
	writeSuccessResponse(w, nil)
}

/// Multipart CloudStorageAPI

// NewMultipartUploadHandler - New multipart upload
func (api CloudStorageAPI) NewMultipartUploadHandler(w http.ResponseWriter, req *http.Request) {
	var object, bucket string
	vars := mux.Vars(req)
	bucket = vars["bucket"]
	object = vars["object"]

	if !api.Anonymous {
		// Unauthorized multipart uploads are not supported
		if isRequestRequiresACLCheck(req) {
			writeErrorResponse(w, req, AccessDenied, req.URL.Path)
			return
		}
	}

	uploadID, err := api.Filesystem.NewMultipartUpload(bucket, object)
	if err != nil {
		errorIf(err.Trace(), "NewMultipartUpload failed.", nil)
		switch err.ToGoError().(type) {
		case fs.RootPathFull:
			writeErrorResponse(w, req, RootPathFull, req.URL.Path)
		case fs.BucketNameInvalid:
			writeErrorResponse(w, req, InvalidBucketName, req.URL.Path)
		case fs.BucketNotFound:
			writeErrorResponse(w, req, NoSuchBucket, req.URL.Path)
		case fs.ObjectNotFound:
			writeErrorResponse(w, req, NoSuchKey, req.URL.Path)
		case fs.ObjectNameInvalid:
			writeErrorResponse(w, req, NoSuchKey, req.URL.Path)
		default:
			writeErrorResponse(w, req, InternalError, req.URL.Path)
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
func (api CloudStorageAPI) PutObjectPartHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]
	object := vars["object"]

	if !api.Anonymous {
		if isRequestRequiresACLCheck(req) {
			writeErrorResponse(w, req, AccessDenied, req.URL.Path)
			return
		}
	}

	// get Content-MD5 sent by client and verify if valid
	md5 := req.Header.Get("Content-MD5")
	if !isValidMD5(md5) {
		writeErrorResponse(w, req, InvalidDigest, req.URL.Path)
		return
	}

	/// if Content-Length is unknown/missing, throw away
	size := req.ContentLength
	if size == -1 {
		writeErrorResponse(w, req, MissingContentLength, req.URL.Path)
		return
	}

	/// maximum Upload size for multipart objects in a single operation
	if isMaxObjectSize(size) {
		writeErrorResponse(w, req, EntityTooLarge, req.URL.Path)
		return
	}

	uploadID := req.URL.Query().Get("uploadId")
	partIDString := req.URL.Query().Get("partNumber")

	var partID int
	{
		var err error
		partID, err = strconv.Atoi(partIDString)
		if err != nil {
			writeErrorResponse(w, req, InvalidPart, req.URL.Path)
			return
		}
	}

	var signature *fs.Signature
	if !api.Anonymous {
		if isRequestSignatureV4(req) {
			// Init signature V4 verification
			var err *probe.Error
			signature, err = initSignatureV4(req)
			if err != nil {
				errorIf(err.Trace(), "Initializing signature v4 failed.", nil)
				writeErrorResponse(w, req, InternalError, req.URL.Path)
				return
			}
		}
	}

	calculatedMD5, err := api.Filesystem.CreateObjectPart(bucket, object, uploadID, md5, partID, size, req.Body, signature)
	if err != nil {
		errorIf(err.Trace(), "CreateObjectPart failed.", nil)
		switch err.ToGoError().(type) {
		case fs.RootPathFull:
			writeErrorResponse(w, req, RootPathFull, req.URL.Path)
		case fs.InvalidUploadID:
			writeErrorResponse(w, req, NoSuchUpload, req.URL.Path)
		case fs.BadDigest:
			writeErrorResponse(w, req, BadDigest, req.URL.Path)
		case fs.SignatureDoesNotMatch:
			writeErrorResponse(w, req, SignatureDoesNotMatch, req.URL.Path)
		case fs.IncompleteBody:
			writeErrorResponse(w, req, IncompleteBody, req.URL.Path)
		case fs.EntityTooLarge:
			writeErrorResponse(w, req, EntityTooLarge, req.URL.Path)
		case fs.InvalidDigest:
			writeErrorResponse(w, req, InvalidDigest, req.URL.Path)
		default:
			writeErrorResponse(w, req, InternalError, req.URL.Path)
		}
		return
	}
	w.Header().Set("ETag", "\""+calculatedMD5+"\"")
	writeSuccessResponse(w, nil)
}

// AbortMultipartUploadHandler - Abort multipart upload
func (api CloudStorageAPI) AbortMultipartUploadHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]
	object := vars["object"]

	if !api.Anonymous {
		if isRequestRequiresACLCheck(req) {
			writeErrorResponse(w, req, AccessDenied, req.URL.Path)
			return
		}
	}

	objectResourcesMetadata := getObjectResources(req.URL.Query())
	err := api.Filesystem.AbortMultipartUpload(bucket, object, objectResourcesMetadata.UploadID)
	if err != nil {
		errorIf(err.Trace(), "AbortMutlipartUpload failed.", nil)
		switch err.ToGoError().(type) {
		case fs.BucketNameInvalid:
			writeErrorResponse(w, req, InvalidBucketName, req.URL.Path)
		case fs.BucketNotFound:
			writeErrorResponse(w, req, NoSuchBucket, req.URL.Path)
		case fs.ObjectNotFound:
			writeErrorResponse(w, req, NoSuchKey, req.URL.Path)
		case fs.ObjectNameInvalid:
			writeErrorResponse(w, req, NoSuchKey, req.URL.Path)
		case fs.InvalidUploadID:
			writeErrorResponse(w, req, NoSuchUpload, req.URL.Path)
		default:
			writeErrorResponse(w, req, InternalError, req.URL.Path)
		}
		return
	}
	writeSuccessNoContent(w)
}

// ListObjectPartsHandler - List object parts
func (api CloudStorageAPI) ListObjectPartsHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]
	object := vars["object"]

	if !api.Anonymous {
		if isRequestRequiresACLCheck(req) {
			writeErrorResponse(w, req, AccessDenied, req.URL.Path)
			return
		}
	}

	objectResourcesMetadata := getObjectResources(req.URL.Query())
	if objectResourcesMetadata.PartNumberMarker < 0 {
		writeErrorResponse(w, req, InvalidPartNumberMarker, req.URL.Path)
		return
	}
	if objectResourcesMetadata.MaxParts < 0 {
		writeErrorResponse(w, req, InvalidMaxParts, req.URL.Path)
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
			writeErrorResponse(w, req, InvalidBucketName, req.URL.Path)
		case fs.BucketNotFound:
			writeErrorResponse(w, req, NoSuchBucket, req.URL.Path)
		case fs.ObjectNotFound:
			writeErrorResponse(w, req, NoSuchKey, req.URL.Path)
		case fs.ObjectNameInvalid:
			writeErrorResponse(w, req, NoSuchKey, req.URL.Path)
		case fs.InvalidUploadID:
			writeErrorResponse(w, req, NoSuchUpload, req.URL.Path)
		default:
			writeErrorResponse(w, req, InternalError, req.URL.Path)
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
func (api CloudStorageAPI) CompleteMultipartUploadHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]
	object := vars["object"]

	if !api.Anonymous {
		if isRequestRequiresACLCheck(req) {
			writeErrorResponse(w, req, AccessDenied, req.URL.Path)
			return
		}
	}

	objectResourcesMetadata := getObjectResources(req.URL.Query())
	var signature *fs.Signature
	if !api.Anonymous {
		if isRequestSignatureV4(req) {
			// Init signature V4 verification
			var err *probe.Error
			signature, err = initSignatureV4(req)
			if err != nil {
				errorIf(err.Trace(), "Initializing signature v4 failed.", nil)
				writeErrorResponse(w, req, InternalError, req.URL.Path)
				return
			}
		}
	}

	metadata, err := api.Filesystem.CompleteMultipartUpload(bucket, object, objectResourcesMetadata.UploadID, req.Body, signature)
	if err != nil {
		errorIf(err.Trace(), "CompleteMultipartUpload failed.", nil)
		switch err.ToGoError().(type) {
		case fs.BucketNameInvalid:
			writeErrorResponse(w, req, InvalidBucketName, req.URL.Path)
		case fs.BucketNotFound:
			writeErrorResponse(w, req, NoSuchBucket, req.URL.Path)
		case fs.ObjectNotFound:
			writeErrorResponse(w, req, NoSuchKey, req.URL.Path)
		case fs.ObjectNameInvalid:
			writeErrorResponse(w, req, NoSuchKey, req.URL.Path)
		case fs.InvalidUploadID:
			writeErrorResponse(w, req, NoSuchUpload, req.URL.Path)
		case fs.InvalidPart:
			writeErrorResponse(w, req, InvalidPart, req.URL.Path)
		case fs.InvalidPartOrder:
			writeErrorResponse(w, req, InvalidPartOrder, req.URL.Path)
		case fs.SignatureDoesNotMatch:
			writeErrorResponse(w, req, SignatureDoesNotMatch, req.URL.Path)
		case fs.IncompleteBody:
			writeErrorResponse(w, req, IncompleteBody, req.URL.Path)
		case fs.MalformedXML:
			writeErrorResponse(w, req, MalformedXML, req.URL.Path)
		default:
			writeErrorResponse(w, req, InternalError, req.URL.Path)
		}
		return
	}
	response := generateCompleteMultpartUploadResponse(bucket, object, req.URL.String(), metadata.Md5)
	encodedSuccessResponse := encodeSuccessResponse(response)
	// write headers
	setCommonHeaders(w)
	// write success response.
	writeSuccessResponse(w, encodedSuccessResponse)
}

/// Delete CloudStorageAPI

// DeleteObjectHandler - Delete object
func (api CloudStorageAPI) DeleteObjectHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]
	object := vars["object"]

	if !api.Anonymous {
		if isRequestRequiresACLCheck(req) {
			if api.Filesystem.IsPrivateBucket(bucket) || api.Filesystem.IsReadOnlyBucket(bucket) {
				writeErrorResponse(w, req, AccessDenied, req.URL.Path)
				return
			}
		}
	}

	err := api.Filesystem.DeleteObject(bucket, object)
	if err != nil {
		errorIf(err.Trace(), "DeleteObject failed.", nil)
		switch err.ToGoError().(type) {
		case fs.BucketNameInvalid:
			writeErrorResponse(w, req, InvalidBucketName, req.URL.Path)
		case fs.BucketNotFound:
			writeErrorResponse(w, req, NoSuchBucket, req.URL.Path)
		case fs.ObjectNotFound:
			writeErrorResponse(w, req, NoSuchKey, req.URL.Path)
		case fs.ObjectNameInvalid:
			writeErrorResponse(w, req, NoSuchKey, req.URL.Path)
		default:
			writeErrorResponse(w, req, InternalError, req.URL.Path)
		}
	}
	writeSuccessNoContent(w)
}
