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
	"bytes"
	"encoding/hex"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"

	mux "github.com/gorilla/mux"
	"github.com/minio/minio/pkg/crypto/sha256"
	"github.com/minio/minio/pkg/fs"
	"github.com/minio/minio/pkg/probe"
	"github.com/minio/minio/pkg/s3/signature4"
)

// GetBucketLocationHandler - GET Bucket location.
// -------------------------
// This operation returns bucket location.
func (api storageAPI) GetBucketLocationHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	if isRequestRequiresACLCheck(r) {
		writeErrorResponse(w, r, AccessDenied, r.URL.Path)
		return
	}

	if !isSignV4ReqAuthenticated(api.Signature, r) {
		writeErrorResponse(w, r, SignatureDoesNotMatch, r.URL.Path)
		return
	}

	_, err := api.Filesystem.GetBucketMetadata(bucket)
	if err != nil {
		errorIf(err.Trace(), "GetBucketMetadata failed.", nil)
		switch err.ToGoError().(type) {
		case fs.BucketNotFound:
			writeErrorResponse(w, r, NoSuchBucket, r.URL.Path)
		case fs.BucketNameInvalid:
			writeErrorResponse(w, r, InvalidBucketName, r.URL.Path)
		default:
			writeErrorResponse(w, r, InternalError, r.URL.Path)
		}
		return
	}

	// Generate response.
	encodedSuccessResponse := encodeSuccessResponse(LocationResponse{})
	if api.Region != "us-east-1" {
		encodedSuccessResponse = encodeSuccessResponse(LocationResponse{
			Location: api.Region,
		})
	}
	setCommonHeaders(w) // write headers.
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
func (api storageAPI) ListMultipartUploadsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	if isRequestRequiresACLCheck(r) {
		writeErrorResponse(w, r, AccessDenied, r.URL.Path)
		return
	}

	if !isSignV4ReqAuthenticated(api.Signature, r) {
		writeErrorResponse(w, r, SignatureDoesNotMatch, r.URL.Path)
		return
	}

	resources := getBucketMultipartResources(r.URL.Query())
	if resources.MaxUploads < 0 {
		writeErrorResponse(w, r, InvalidMaxUploads, r.URL.Path)
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
			writeErrorResponse(w, r, NoSuchBucket, r.URL.Path)
		default:
			writeErrorResponse(w, r, InternalError, r.URL.Path)
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
// -- -----------------------
// This implementation of the GET operation returns some or all (up to 1000)
// of the objects in a bucket. You can use the request parameters as selection
// criteria to return a subset of the objects in a bucket.
//
func (api storageAPI) ListObjectsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

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

	// TODO handle encoding type.
	prefix, marker, delimiter, maxkeys, _ := getBucketResources(r.URL.Query())
	if maxkeys < 0 {
		writeErrorResponse(w, r, InvalidMaxKeys, r.URL.Path)
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
		writeErrorResponse(w, r, InvalidBucketName, r.URL.Path)
	case fs.BucketNotFound:
		writeErrorResponse(w, r, NoSuchBucket, r.URL.Path)
	case fs.ObjectNotFound:
		writeErrorResponse(w, r, NoSuchKey, r.URL.Path)
	case fs.ObjectNameInvalid:
		writeErrorResponse(w, r, NoSuchKey, r.URL.Path)
	default:
		errorIf(err.Trace(), "ListObjects failed.", nil)
		writeErrorResponse(w, r, InternalError, r.URL.Path)
	}
}

// ListBucketsHandler - GET Service
// -----------
// This implementation of the GET operation returns a list of all buckets
// owned by the authenticated sender of the request.
func (api storageAPI) ListBucketsHandler(w http.ResponseWriter, r *http.Request) {
	if isRequestRequiresACLCheck(r) {
		writeErrorResponse(w, r, AccessDenied, r.URL.Path)
		return
	}

	if !isSignV4ReqAuthenticated(api.Signature, r) {
		writeErrorResponse(w, r, SignatureDoesNotMatch, r.URL.Path)
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
	writeErrorResponse(w, r, InternalError, r.URL.Path)
}

// PutBucketHandler - PUT Bucket
// ----------
// This implementation of the PUT operation creates a new bucket for authenticated request
func (api storageAPI) PutBucketHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	if isRequestRequiresACLCheck(r) {
		writeErrorResponse(w, r, AccessDenied, r.URL.Path)
		return
	}

	// read from 'x-amz-acl'
	aclType := getACLType(r)
	if aclType == unsupportedACLType {
		writeErrorResponse(w, r, NotImplemented, r.URL.Path)
		return
	}

	// if body of request is non-nil then check for validity of Content-Length
	if r.Body != nil {
		/// if Content-Length is unknown/missing, deny the request
		if r.ContentLength == -1 && !contains(r.TransferEncoding, "chunked") {
			writeErrorResponse(w, r, MissingContentLength, r.URL.Path)
			return
		}
	}

	// Set http request for signature.
	auth := api.Signature.SetHTTPRequestToVerify(r)

	if isRequestPresignedSignatureV4(r) {
		ok, err := auth.DoesPresignedSignatureMatch()
		if err != nil {
			errorIf(err.Trace(r.URL.String()), "Presigned signature verification failed.", nil)
			writeErrorResponse(w, r, SignatureDoesNotMatch, r.URL.Path)
			return
		}
		if !ok {
			writeErrorResponse(w, r, SignatureDoesNotMatch, r.URL.Path)
			return
		}
	} else if isRequestSignatureV4(r) {
		// Verify signature for the incoming body if any.
		locationBytes, e := ioutil.ReadAll(r.Body)
		if e != nil {
			errorIf(probe.NewError(e), "MakeBucket failed.", nil)
			writeErrorResponse(w, r, InternalError, r.URL.Path)
			return
		}
		sh := sha256.New()
		sh.Write(locationBytes)
		ok, err := auth.DoesSignatureMatch(hex.EncodeToString(sh.Sum(nil)))
		if err != nil {
			errorIf(err.Trace(), "MakeBucket failed.", nil)
			writeErrorResponse(w, r, InternalError, r.URL.Path)
			return
		}
		if !ok {
			writeErrorResponse(w, r, SignatureDoesNotMatch, r.URL.Path)
			return
		}
	}

	// Make bucket.
	err := api.Filesystem.MakeBucket(bucket, getACLTypeString(aclType))
	if err != nil {
		errorIf(err.Trace(), "MakeBucket failed.", nil)
		switch err.ToGoError().(type) {
		case fs.BucketNameInvalid:
			writeErrorResponse(w, r, InvalidBucketName, r.URL.Path)
		case fs.BucketExists:
			writeErrorResponse(w, r, BucketAlreadyExists, r.URL.Path)
		default:
			writeErrorResponse(w, r, InternalError, r.URL.Path)
		}
		return
	}
	// Make sure to add Location information here only for bucket
	w.Header().Set("Location", "/"+bucket)
	writeSuccessResponse(w, nil)
}

func extractHTTPFormValues(reader *multipart.Reader) (io.Reader, map[string]string, *probe.Error) {
	/// HTML Form values
	formValues := make(map[string]string)
	filePart := new(bytes.Buffer)
	var e error
	for e == nil {
		var part *multipart.Part
		part, e = reader.NextPart()
		if part != nil {
			if part.FileName() == "" {
				buffer, e := ioutil.ReadAll(part)
				if e != nil {
					return nil, nil, probe.NewError(e)
				}
				formValues[http.CanonicalHeaderKey(part.FormName())] = string(buffer)
			} else {
				if _, e := io.Copy(filePart, part); e != nil {
					return nil, nil, probe.NewError(e)
				}
			}
		}
	}
	return filePart, formValues, nil
}

// PostPolicyBucketHandler - POST policy
// ----------
// This implementation of the POST operation handles object creation with a specified
// signature policy in multipart/form-data
func (api storageAPI) PostPolicyBucketHandler(w http.ResponseWriter, r *http.Request) {
	// if body of request is non-nil then check for validity of Content-Length
	if r.Body != nil {
		/// if Content-Length is unknown/missing, deny the request
		if r.ContentLength == -1 {
			writeErrorResponse(w, r, MissingContentLength, r.URL.Path)
			return
		}
	}

	// Here the parameter is the size of the form data that should
	// be loaded in memory, the remaining being put in temporary
	// files
	reader, e := r.MultipartReader()
	if e != nil {
		errorIf(probe.NewError(e), "Unable to initialize multipart reader.", nil)
		writeErrorResponse(w, r, MalformedPOSTRequest, r.URL.Path)
		return
	}

	fileBody, formValues, err := extractHTTPFormValues(reader)
	if err != nil {
		errorIf(err.Trace(), "Unable to parse form values.", nil)
		writeErrorResponse(w, r, MalformedPOSTRequest, r.URL.Path)
		return
	}
	bucket := mux.Vars(r)["bucket"]
	formValues["Bucket"] = bucket
	object := formValues["Key"]
	var ok bool

	// Set http request for signature.
	auth := api.Signature.SetHTTPRequestToVerify(r)

	// Verify policy signature.
	ok, err = auth.DoesPolicySignatureMatch(formValues)
	if err != nil {
		errorIf(err.Trace(), "Unable to verify signature.", nil)
		writeErrorResponse(w, r, SignatureDoesNotMatch, r.URL.Path)
		return
	}
	if !ok {
		writeErrorResponse(w, r, SignatureDoesNotMatch, r.URL.Path)
		return
	}
	if err = signature4.ApplyPolicyCond(formValues); err != nil {
		errorIf(err.Trace(), "Invalid request, policy doesn't match with the endpoint.", nil)
		writeErrorResponse(w, r, MalformedPOSTRequest, r.URL.Path)
		return
	}
	metadata, err := api.Filesystem.CreateObject(bucket, object, "", -1, fileBody, nil)
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

// PutBucketACLHandler - PUT Bucket ACL
// ----------
// This implementation of the PUT operation modifies the bucketACL for authenticated request
func (api storageAPI) PutBucketACLHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	if isRequestRequiresACLCheck(r) {
		writeErrorResponse(w, r, AccessDenied, r.URL.Path)
		return
	}

	if !isSignV4ReqAuthenticated(api.Signature, r) {
		writeErrorResponse(w, r, SignatureDoesNotMatch, r.URL.Path)
		return
	}

	// read from 'x-amz-acl'
	aclType := getACLType(r)
	if aclType == unsupportedACLType {
		writeErrorResponse(w, r, NotImplemented, r.URL.Path)
		return
	}
	err := api.Filesystem.SetBucketMetadata(bucket, map[string]string{"acl": getACLTypeString(aclType)})
	if err != nil {
		errorIf(err.Trace(), "PutBucketACL failed.", nil)
		switch err.ToGoError().(type) {
		case fs.BucketNameInvalid:
			writeErrorResponse(w, r, InvalidBucketName, r.URL.Path)
		case fs.BucketNotFound:
			writeErrorResponse(w, r, NoSuchBucket, r.URL.Path)
		default:
			writeErrorResponse(w, r, InternalError, r.URL.Path)
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
func (api storageAPI) GetBucketACLHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	if isRequestRequiresACLCheck(r) {
		writeErrorResponse(w, r, AccessDenied, r.URL.Path)
		return
	}

	if !isSignV4ReqAuthenticated(api.Signature, r) {
		writeErrorResponse(w, r, SignatureDoesNotMatch, r.URL.Path)
		return
	}

	bucketMetadata, err := api.Filesystem.GetBucketMetadata(bucket)
	if err != nil {
		errorIf(err.Trace(), "GetBucketMetadata failed.", nil)
		switch err.ToGoError().(type) {
		case fs.BucketNotFound:
			writeErrorResponse(w, r, NoSuchBucket, r.URL.Path)
		case fs.BucketNameInvalid:
			writeErrorResponse(w, r, InvalidBucketName, r.URL.Path)
		default:
			writeErrorResponse(w, r, InternalError, r.URL.Path)
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
func (api storageAPI) HeadBucketHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

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

	_, err := api.Filesystem.GetBucketMetadata(bucket)
	if err != nil {
		errorIf(err.Trace(), "GetBucketMetadata failed.", nil)
		switch err.ToGoError().(type) {
		case fs.BucketNotFound:
			writeErrorResponse(w, r, NoSuchBucket, r.URL.Path)
		case fs.BucketNameInvalid:
			writeErrorResponse(w, r, InvalidBucketName, r.URL.Path)
		default:
			writeErrorResponse(w, r, InternalError, r.URL.Path)
		}
		return
	}
	writeSuccessResponse(w, nil)
}

// DeleteBucketHandler - Delete bucket
func (api storageAPI) DeleteBucketHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	if isRequestRequiresACLCheck(r) {
		writeErrorResponse(w, r, AccessDenied, r.URL.Path)
		return
	}

	if !isSignV4ReqAuthenticated(api.Signature, r) {
		writeErrorResponse(w, r, SignatureDoesNotMatch, r.URL.Path)
		return
	}

	err := api.Filesystem.DeleteBucket(bucket)
	if err != nil {
		errorIf(err.Trace(), "DeleteBucket failed.", nil)
		switch err.ToGoError().(type) {
		case fs.BucketNotFound:
			writeErrorResponse(w, r, NoSuchBucket, r.URL.Path)
		case fs.BucketNotEmpty:
			writeErrorResponse(w, r, BucketNotEmpty, r.URL.Path)
		default:
			writeErrorResponse(w, r, InternalError, r.URL.Path)
		}
		return
	}
	writeSuccessNoContent(w)
}
