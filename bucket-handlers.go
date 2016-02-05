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
	"encoding/hex"
	"io/ioutil"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/minio/minio-xl/pkg/crypto/sha256"
	"github.com/minio/minio-xl/pkg/probe"
	"github.com/minio/minio/pkg/fs"
)

// GetBucketLocationHandler - GET Bucket location.
// -------------------------
// This operation returns bucket location.
func (api CloudStorageAPI) GetBucketLocationHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]

	if isRequestRequiresACLCheck(req) {
		writeErrorResponse(w, req, AccessDenied, req.URL.Path)
		return
	}

	_, err := api.Filesystem.GetBucketMetadata(bucket)
	if err != nil {
		errorIf(err.Trace(), "GetBucketMetadata failed.", nil)
		switch err.ToGoError().(type) {
		case fs.BucketNotFound:
			writeErrorResponse(w, req, NoSuchBucket, req.URL.Path)
		case fs.BucketNameInvalid:
			writeErrorResponse(w, req, InvalidBucketName, req.URL.Path)
		default:
			writeErrorResponse(w, req, InternalError, req.URL.Path)
		}
		return
	}

	// TODO: Location value for LocationResponse is deliberately not used, until
	//       we bring in a mechanism of configurable regions. For the time being
	//       default region is empty i.e 'us-east-1'.
	encodedSuccessResponse := encodeSuccessResponse(LocationResponse{}) // generate response
	setCommonHeaders(w)                                                 // write headers
	writeSuccessResponse(w, encodedSuccessResponse)
}

// ListMultipartUploadsHandler - GET Bucket (List Multipart uploads)
// -------------------------
// This operation lists in-progress multipart uploads. An in-progress
// multipart upload is a multipart upload that has been initiated,
// using the Initiate Multipart Upload request, but has not yet been
// completed or aborted. This operation returns at most 1,000 multipart
// uploads in the response.
//
func (api CloudStorageAPI) ListMultipartUploadsHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]

	if isRequestRequiresACLCheck(req) {
		writeErrorResponse(w, req, AccessDenied, req.URL.Path)
		return
	}

	resources := getBucketMultipartResources(req.URL.Query())
	if resources.MaxUploads < 0 {
		writeErrorResponse(w, req, InvalidMaxUploads, req.URL.Path)
		return
	}
	if resources.MaxUploads == 0 {
		resources.MaxUploads = maxObjectList
	}

	resources, err := api.Filesystem.ListMultipartUploads(bucket, resources)
	if err != nil {
		errorIf(err.Trace(), "ListMultipartUploads failed.", nil)
		switch err.ToGoError().(type) {
		case fs.BucketNotFound:
			writeErrorResponse(w, req, NoSuchBucket, req.URL.Path)
		default:
			writeErrorResponse(w, req, InternalError, req.URL.Path)
		}
		return
	}
	// generate response
	response := generateListMultipartUploadsResponse(bucket, resources)
	encodedSuccessResponse := encodeSuccessResponse(response)
	// write headers.
	setCommonHeaders(w)
	// write success response.
	writeSuccessResponse(w, encodedSuccessResponse)
}

// ListObjectsHandler - GET Bucket (List Objects)
// -------------------------
// This implementation of the GET operation returns some or all (up to 1000)
// of the objects in a bucket. You can use the request parameters as selection
// criteria to return a subset of the objects in a bucket.
//
func (api CloudStorageAPI) ListObjectsHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]

	if isRequestRequiresACLCheck(req) {
		if api.Filesystem.IsPrivateBucket(bucket) {
			writeErrorResponse(w, req, AccessDenied, req.URL.Path)
			return
		}
	}

	// TODO handle encoding type.
	prefix, marker, delimiter, maxkeys, _ := getBucketResources(req.URL.Query())
	if maxkeys < 0 {
		writeErrorResponse(w, req, InvalidMaxKeys, req.URL.Path)
		return
	}
	if maxkeys == 0 {
		maxkeys = maxObjectList
	}

	listResp, err := api.Filesystem.ListObjects(bucket, prefix, marker, delimiter, maxkeys)
	if err == nil {
		// generate response
		response := generateListObjectsResponse(bucket, prefix, marker, delimiter, maxkeys, listResp)
		encodedSuccessResponse := encodeSuccessResponse(response)
		// Write headers
		setCommonHeaders(w)
		// Write success response.
		writeSuccessResponse(w, encodedSuccessResponse)
		return
	}
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
		errorIf(err.Trace(), "ListObjects failed.", nil)
		writeErrorResponse(w, req, InternalError, req.URL.Path)
	}
}

// ListBucketsHandler - GET Service
// -----------
// This implementation of the GET operation returns a list of all buckets
// owned by the authenticated sender of the request.
func (api CloudStorageAPI) ListBucketsHandler(w http.ResponseWriter, req *http.Request) {
	if isRequestRequiresACLCheck(req) {
		writeErrorResponse(w, req, AccessDenied, req.URL.Path)
		return
	}

	buckets, err := api.Filesystem.ListBuckets()
	if err == nil {
		// generate response
		response := generateListBucketsResponse(buckets)
		encodedSuccessResponse := encodeSuccessResponse(response)
		// write headers
		setCommonHeaders(w)
		// write response
		writeSuccessResponse(w, encodedSuccessResponse)
		return
	}
	errorIf(err.Trace(), "ListBuckets failed.", nil)
	writeErrorResponse(w, req, InternalError, req.URL.Path)
}

// PutBucketHandler - PUT Bucket
// ----------
// This implementation of the PUT operation creates a new bucket for authenticated request
func (api CloudStorageAPI) PutBucketHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]

	if isRequestRequiresACLCheck(req) {
		writeErrorResponse(w, req, AccessDenied, req.URL.Path)
		return
	}

	// read from 'x-amz-acl'
	aclType := getACLType(req)
	if aclType == unsupportedACLType {
		writeErrorResponse(w, req, NotImplemented, req.URL.Path)
		return
	}

	var signature *fs.Signature
	// Init signature V4 verification
	if isRequestSignatureV4(req) {
		var err *probe.Error
		signature, err = initSignatureV4(req)
		if err != nil {
			switch err.ToGoError() {
			case errInvalidRegion:
				errorIf(err.Trace(), "Unknown region in authorization header.", nil)
				writeErrorResponse(w, req, AuthorizationHeaderMalformed, req.URL.Path)
				return
			case errAccessKeyIDInvalid:
				errorIf(err.Trace(), "Invalid access key id.", nil)
				writeErrorResponse(w, req, InvalidAccessKeyID, req.URL.Path)
				return
			default:
				errorIf(err.Trace(), "Initializing signature v4 failed.", nil)
				writeErrorResponse(w, req, InternalError, req.URL.Path)
				return
			}
		}
	}

	// if body of request is non-nil then check for validity of Content-Length
	if req.Body != nil {
		/// if Content-Length is unknown/missing, deny the request
		if req.ContentLength == -1 && !contains(req.TransferEncoding, "chunked") {
			writeErrorResponse(w, req, MissingContentLength, req.URL.Path)
			return
		}
		if signature != nil {
			locationBytes, e := ioutil.ReadAll(req.Body)
			if e != nil {
				errorIf(probe.NewError(e), "MakeBucket failed.", nil)
				writeErrorResponse(w, req, InternalError, req.URL.Path)
				return
			}
			sh := sha256.New()
			sh.Write(locationBytes)
			ok, err := signature.DoesSignatureMatch(hex.EncodeToString(sh.Sum(nil)))
			if err != nil {
				errorIf(err.Trace(), "MakeBucket failed.", nil)
				writeErrorResponse(w, req, InternalError, req.URL.Path)
				return
			}
			if !ok {
				writeErrorResponse(w, req, SignatureDoesNotMatch, req.URL.Path)
				return
			}
		}
	}

	err := api.Filesystem.MakeBucket(bucket, getACLTypeString(aclType))
	if err != nil {
		errorIf(err.Trace(), "MakeBucket failed.", nil)
		switch err.ToGoError().(type) {
		case fs.BucketNameInvalid:
			writeErrorResponse(w, req, InvalidBucketName, req.URL.Path)
		case fs.BucketExists:
			writeErrorResponse(w, req, BucketAlreadyExists, req.URL.Path)
		default:
			writeErrorResponse(w, req, InternalError, req.URL.Path)
		}
		return
	}
	// Make sure to add Location information here only for bucket
	w.Header().Set("Location", "/"+bucket)
	writeSuccessResponse(w, nil)
}

// PostPolicyBucketHandler - POST policy
// ----------
// This implementation of the POST operation handles object creation with a specified
// signature policy in multipart/form-data
func (api CloudStorageAPI) PostPolicyBucketHandler(w http.ResponseWriter, req *http.Request) {
	// if body of request is non-nil then check for validity of Content-Length
	if req.Body != nil {
		/// if Content-Length is unknown/missing, deny the request
		if req.ContentLength == -1 {
			writeErrorResponse(w, req, MissingContentLength, req.URL.Path)
			return
		}
	}

	// Here the parameter is the size of the form data that should
	// be loaded in memory, the remaining being put in temporary
	// files
	reader, e := req.MultipartReader()
	if e != nil {
		errorIf(probe.NewError(e), "Unable to initialize multipart reader.", nil)
		writeErrorResponse(w, req, MalformedPOSTRequest, req.URL.Path)
		return
	}

	fileBody, formValues, err := extractHTTPFormValues(reader)
	if err != nil {
		errorIf(err.Trace(), "Unable to parse form values.", nil)
		writeErrorResponse(w, req, MalformedPOSTRequest, req.URL.Path)
		return
	}
	bucket := mux.Vars(req)["bucket"]
	formValues["Bucket"] = bucket
	object := formValues["Key"]
	signature, err := initPostPresignedPolicyV4(formValues)
	if err != nil {
		errorIf(err.Trace(), "Unable to initialize post policy presigned.", nil)
		writeErrorResponse(w, req, MalformedPOSTRequest, req.URL.Path)
		return
	}
	var ok bool
	if ok, err = signature.DoesPolicySignatureMatch(formValues["X-Amz-Date"]); err != nil {
		errorIf(err.Trace(), "Unable to verify signature.", nil)
		writeErrorResponse(w, req, SignatureDoesNotMatch, req.URL.Path)
		return
	}
	if ok == false {
		writeErrorResponse(w, req, SignatureDoesNotMatch, req.URL.Path)
		return
	}
	if err = applyPolicy(formValues); err != nil {
		errorIf(err.Trace(), "Invalid request, policy doesn't match with the endpoint.", nil)
		writeErrorResponse(w, req, MalformedPOSTRequest, req.URL.Path)
		return
	}
	metadata, err := api.Filesystem.CreateObject(bucket, object, "", 0, fileBody, nil)
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
	if metadata.MD5 != "" {
		w.Header().Set("ETag", "\""+metadata.MD5+"\"")
	}
	writeSuccessResponse(w, nil)
}

// PutBucketACLHandler - PUT Bucket ACL
// ----------
// This implementation of the PUT operation modifies the bucketACL for authenticated request
func (api CloudStorageAPI) PutBucketACLHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]

	if isRequestRequiresACLCheck(req) {
		writeErrorResponse(w, req, AccessDenied, req.URL.Path)
		return
	}

	// read from 'x-amz-acl'
	aclType := getACLType(req)
	if aclType == unsupportedACLType {
		writeErrorResponse(w, req, NotImplemented, req.URL.Path)
		return
	}
	err := api.Filesystem.SetBucketMetadata(bucket, map[string]string{"acl": getACLTypeString(aclType)})
	if err != nil {
		errorIf(err.Trace(), "PutBucketACL failed.", nil)
		switch err.ToGoError().(type) {
		case fs.BucketNameInvalid:
			writeErrorResponse(w, req, InvalidBucketName, req.URL.Path)
		case fs.BucketNotFound:
			writeErrorResponse(w, req, NoSuchBucket, req.URL.Path)
		default:
			writeErrorResponse(w, req, InternalError, req.URL.Path)
		}
		return
	}
	writeSuccessResponse(w, nil)
}

// GetBucketACLHandler - GET ACL on a Bucket
// ----------
// This operation uses acl subresource to the return the ``acl``
// of a bucket. One must have permission to access the bucket to
// know its ``acl``. This operation willl return response of 404
// if bucket not found and 403 for invalid credentials.
func (api CloudStorageAPI) GetBucketACLHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]

	if isRequestRequiresACLCheck(req) {
		writeErrorResponse(w, req, AccessDenied, req.URL.Path)
		return
	}

	bucketMetadata, err := api.Filesystem.GetBucketMetadata(bucket)
	if err != nil {
		errorIf(err.Trace(), "GetBucketMetadata failed.", nil)
		switch err.ToGoError().(type) {
		case fs.BucketNotFound:
			writeErrorResponse(w, req, NoSuchBucket, req.URL.Path)
		case fs.BucketNameInvalid:
			writeErrorResponse(w, req, InvalidBucketName, req.URL.Path)
		default:
			writeErrorResponse(w, req, InternalError, req.URL.Path)
		}
		return
	}
	// Generate response
	response := generateAccessControlPolicyResponse(bucketMetadata.ACL)
	encodedSuccessResponse := encodeSuccessResponse(response)
	// Write headers
	setCommonHeaders(w)
	// Write success response.
	writeSuccessResponse(w, encodedSuccessResponse)
}

// HeadBucketHandler - HEAD Bucket
// ----------
// This operation is useful to determine if a bucket exists.
// The operation returns a 200 OK if the bucket exists and you
// have permission to access it. Otherwise, the operation might
// return responses such as 404 Not Found and 403 Forbidden.
func (api CloudStorageAPI) HeadBucketHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]

	if isRequestRequiresACLCheck(req) {
		if api.Filesystem.IsPrivateBucket(bucket) {
			writeErrorResponse(w, req, AccessDenied, req.URL.Path)
			return
		}
	}

	_, err := api.Filesystem.GetBucketMetadata(bucket)
	if err != nil {
		errorIf(err.Trace(), "GetBucketMetadata failed.", nil)
		switch err.ToGoError().(type) {
		case fs.BucketNotFound:
			writeErrorResponse(w, req, NoSuchBucket, req.URL.Path)
		case fs.BucketNameInvalid:
			writeErrorResponse(w, req, InvalidBucketName, req.URL.Path)
		default:
			writeErrorResponse(w, req, InternalError, req.URL.Path)
		}
		return
	}
	writeSuccessResponse(w, nil)
}

// DeleteBucketHandler - Delete bucket
func (api CloudStorageAPI) DeleteBucketHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]

	if isRequestRequiresACLCheck(req) {
		writeErrorResponse(w, req, AccessDenied, req.URL.Path)
		return
	}

	err := api.Filesystem.DeleteBucket(bucket)
	if err != nil {
		errorIf(err.Trace(), "DeleteBucket failed.", nil)
		switch err.ToGoError().(type) {
		case fs.BucketNotFound:
			writeErrorResponse(w, req, NoSuchBucket, req.URL.Path)
		case fs.BucketNotEmpty:
			writeErrorResponse(w, req, BucketNotEmpty, req.URL.Path)
		default:
			writeErrorResponse(w, req, InternalError, req.URL.Path)
		}
		return
	}
	writeSuccessNoContent(w)
}
