/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"

	mux "github.com/gorilla/mux"
	"github.com/minio/minio/pkg/crypto/sha256"
	"github.com/minio/minio/pkg/fs"
	"github.com/minio/minio/pkg/probe"
	"github.com/minio/minio/pkg/s3/access"
	"github.com/minio/minio/pkg/s3/signature4"
)

// http://docs.aws.amazon.com/AmazonS3/latest/dev/mpuAndPermissions.html
func enforceBucketPolicy(action string, bucket string, reqURL *url.URL) (s3Error APIErrorCode) {
	// Read saved bucket policy.
	policy, err := readBucketPolicy(bucket)
	if err != nil {
		errorIf(err.Trace(bucket), "GetBucketPolicy failed.", nil)
		switch err.ToGoError().(type) {
		case fs.BucketNotFound:
			return ErrNoSuchBucket
		case fs.BucketNameInvalid:
			return ErrInvalidBucketName
		default:
			// For any other error just return AccessDenied.
			return ErrAccessDenied
		}
	}
	// Parse the saved policy.
	accessPolicy, e := accesspolicy.Validate(policy)
	if e != nil {
		errorIf(probe.NewError(e), "Parse policy failed.", nil)
		return ErrAccessDenied
	}

	// Construct resource in 'arn:aws:s3:::examplebucket' format.
	resource := accesspolicy.AWSResourcePrefix + strings.TrimPrefix(reqURL.Path, "/")

	// Get conditions for policy verification.
	conditions := make(map[string]string)
	for queryParam := range reqURL.Query() {
		conditions[queryParam] = reqURL.Query().Get("queryParam")
	}

	// Validate action, resource and conditions with current policy statements.
	if !bucketPolicyEvalStatements(action, resource, conditions, accessPolicy.Statements) {
		return ErrAccessDenied
	}
	return ErrNone
}

// GetBucketLocationHandler - GET Bucket location.
// -------------------------
// This operation returns bucket location.
func (api storageAPI) GetBucketLocationHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	switch getRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case authTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/mpuAndPermissions.html
		if s3Error := enforceBucketPolicy("s3:GetBucketLocation", bucket, r.URL); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	case authTypeSigned, authTypePresigned:
		if s3Error := isReqAuthenticated(api.Signature, r); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}

	_, err := api.Filesystem.GetBucketInfo(bucket)
	if err != nil {
		errorIf(err.Trace(), "GetBucketInfo failed.", nil)
		switch err.ToGoError().(type) {
		case fs.BucketNotFound:
			writeErrorResponse(w, r, ErrNoSuchBucket, r.URL.Path)
		case fs.BucketNameInvalid:
			writeErrorResponse(w, r, ErrInvalidBucketName, r.URL.Path)
		default:
			writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		}
		return
	}

	// Generate response.
	encodedSuccessResponse := encodeResponse(LocationResponse{})
	if api.Region != "us-east-1" {
		encodedSuccessResponse = encodeResponse(LocationResponse{
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

	switch getRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case authTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/mpuAndPermissions.html
		if s3Error := enforceBucketPolicy("s3:ListBucketMultipartUploads", bucket, r.URL); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	case authTypePresigned, authTypeSigned:
		if s3Error := isReqAuthenticated(api.Signature, r); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}

	resources := getBucketMultipartResources(r.URL.Query())
	if resources.MaxUploads < 0 {
		writeErrorResponse(w, r, ErrInvalidMaxUploads, r.URL.Path)
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
			writeErrorResponse(w, r, ErrNoSuchBucket, r.URL.Path)
		default:
			writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		}
		return
	}
	// generate response
	response := generateListMultipartUploadsResponse(bucket, resources)
	encodedSuccessResponse := encodeResponse(response)
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

	switch getRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case authTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/mpuAndPermissions.html
		if s3Error := enforceBucketPolicy("s3:ListBucket", bucket, r.URL); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	case authTypeSigned, authTypePresigned:
		if s3Error := isReqAuthenticated(api.Signature, r); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}

	// TODO handle encoding type.
	prefix, marker, delimiter, maxkeys, _ := getBucketResources(r.URL.Query())
	if maxkeys < 0 {
		writeErrorResponse(w, r, ErrInvalidMaxKeys, r.URL.Path)
		return
	}
	if maxkeys == 0 {
		maxkeys = maxObjectList
	}

	listResp, err := api.Filesystem.ListObjects(bucket, prefix, marker, delimiter, maxkeys)
	if err == nil {
		// generate response
		response := generateListObjectsResponse(bucket, prefix, marker, delimiter, maxkeys, listResp)
		encodedSuccessResponse := encodeResponse(response)
		// Write headers
		setCommonHeaders(w)
		// Write success response.
		writeSuccessResponse(w, encodedSuccessResponse)
		return
	}
	switch err.ToGoError().(type) {
	case fs.BucketNameInvalid:
		writeErrorResponse(w, r, ErrInvalidBucketName, r.URL.Path)
	case fs.BucketNotFound:
		writeErrorResponse(w, r, ErrNoSuchBucket, r.URL.Path)
	case fs.ObjectNotFound:
		writeErrorResponse(w, r, ErrNoSuchKey, r.URL.Path)
	case fs.ObjectNameInvalid:
		writeErrorResponse(w, r, ErrNoSuchKey, r.URL.Path)
	default:
		errorIf(err.Trace(), "ListObjects failed.", nil)
		writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
	}
}

// ListBucketsHandler - GET Service
// -----------
// This implementation of the GET operation returns a list of all buckets
// owned by the authenticated sender of the request.
func (api storageAPI) ListBucketsHandler(w http.ResponseWriter, r *http.Request) {
	// List buckets does not support bucket policies.
	switch getRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case authTypeSigned, authTypePresigned:
		if s3Error := isReqAuthenticated(api.Signature, r); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}

	buckets, err := api.Filesystem.ListBuckets()
	if err == nil {
		// generate response
		response := generateListBucketsResponse(buckets)
		encodedSuccessResponse := encodeResponse(response)
		// write headers
		setCommonHeaders(w)
		// write response
		writeSuccessResponse(w, encodedSuccessResponse)
		return
	}
	errorIf(err.Trace(), "ListBuckets failed.", nil)
	writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
}

// DeleteMultipleObjectsHandler - deletes multiple objects.
func (api storageAPI) DeleteMultipleObjectsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	// Content-Length is required and should be non-zero
	// http://docs.aws.amazon.com/AmazonS3/latest/API/multiobjectdeleteapi.html
	if r.ContentLength <= 0 {
		writeErrorResponse(w, r, ErrMissingContentLength, r.URL.Path)
		return
	}

	// Content-Md5 is requied should be set
	// http://docs.aws.amazon.com/AmazonS3/latest/API/multiobjectdeleteapi.html
	if _, ok := r.Header["Content-Md5"]; !ok {
		writeErrorResponse(w, r, ErrMissingContentMD5, r.URL.Path)
		return
	}

	// Set http request for signature.
	auth := api.Signature.SetHTTPRequestToVerify(r)

	// Allocate incoming content length bytes.
	deleteXMLBytes := make([]byte, r.ContentLength)

	// Read incoming body XML bytes.
	_, e := io.ReadFull(r.Body, deleteXMLBytes)
	if e != nil {
		errorIf(probe.NewError(e), "DeleteMultipleObjects failed.", nil)
		writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		return
	}

	switch getRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case authTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/mpuAndPermissions.html
		if s3Error := enforceBucketPolicy("s3:DeleteObject", bucket, r.URL); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	case authTypePresigned:
		// Check if request is presigned.
		ok, err := auth.DoesPresignedSignatureMatch()
		if err != nil {
			errorIf(err.Trace(r.URL.String()), "Presigned signature verification failed.", nil)
			writeErrorResponse(w, r, ErrSignatureDoesNotMatch, r.URL.Path)
			return
		}
		if !ok {
			writeErrorResponse(w, r, ErrSignatureDoesNotMatch, r.URL.Path)
			return
		}
	case authTypeSigned:
		// Check if request is signed.
		sha := sha256.New()
		mdSh := md5.New()
		sha.Write(deleteXMLBytes)
		mdSh.Write(deleteXMLBytes)
		ok, err := auth.DoesSignatureMatch(hex.EncodeToString(sha.Sum(nil)))
		if err != nil {
			errorIf(err.Trace(), "DeleteMultipleObjects failed.", nil)
			writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
			return
		}
		if !ok {
			writeErrorResponse(w, r, ErrSignatureDoesNotMatch, r.URL.Path)
			return
		}
		// Verify content md5.
		if r.Header.Get("Content-Md5") != base64.StdEncoding.EncodeToString(mdSh.Sum(nil)) {
			writeErrorResponse(w, r, ErrBadDigest, r.URL.Path)
			return
		}
	}

	// Unmarshal list of keys to be deleted.
	deleteObjects := &DeleteObjectsRequest{}
	if e := xml.Unmarshal(deleteXMLBytes, deleteObjects); e != nil {
		writeErrorResponse(w, r, ErrMalformedXML, r.URL.Path)
		return
	}

	var deleteErrors []DeleteError
	var deletedObjects []ObjectIdentifier
	// Loop through all the objects and delete them sequentially.
	for _, object := range deleteObjects.Objects {
		err := api.Filesystem.DeleteObject(bucket, object.ObjectName)
		if err == nil {
			deletedObjects = append(deletedObjects, ObjectIdentifier{
				ObjectName: object.ObjectName,
			})
		} else {
			errorIf(err.Trace(object.ObjectName), "DeleteObject failed.", nil)
			switch err.ToGoError().(type) {
			case fs.BucketNameInvalid:
				deleteErrors = append(deleteErrors, DeleteError{
					Code:    errorCodeResponse[ErrInvalidBucketName].Code,
					Message: errorCodeResponse[ErrInvalidBucketName].Description,
					Key:     object.ObjectName,
				})
			case fs.BucketNotFound:
				deleteErrors = append(deleteErrors, DeleteError{
					Code:    errorCodeResponse[ErrNoSuchBucket].Code,
					Message: errorCodeResponse[ErrNoSuchBucket].Description,
					Key:     object.ObjectName,
				})
			case fs.ObjectNotFound:
				deleteErrors = append(deleteErrors, DeleteError{
					Code:    errorCodeResponse[ErrNoSuchKey].Code,
					Message: errorCodeResponse[ErrNoSuchKey].Description,
					Key:     object.ObjectName,
				})
			case fs.ObjectNameInvalid:
				deleteErrors = append(deleteErrors, DeleteError{
					Code:    errorCodeResponse[ErrNoSuchKey].Code,
					Message: errorCodeResponse[ErrNoSuchKey].Description,
					Key:     object.ObjectName,
				})
			default:
				deleteErrors = append(deleteErrors, DeleteError{
					Code:    errorCodeResponse[ErrInternalError].Code,
					Message: errorCodeResponse[ErrInternalError].Description,
					Key:     object.ObjectName,
				})
			}
		}
	}
	// Generate response
	response := generateMultiDeleteResponse(deleteObjects.Quiet, deletedObjects, deleteErrors)
	encodedSuccessResponse := encodeResponse(response)
	// Write headers
	setCommonHeaders(w)
	// Write success response.
	writeSuccessResponse(w, encodedSuccessResponse)
}

// PutBucketHandler - PUT Bucket
// ----------
// This implementation of the PUT operation creates a new bucket for authenticated request
func (api storageAPI) PutBucketHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	// Set http request for signature.
	auth := api.Signature.SetHTTPRequestToVerify(r)
	switch getRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case authTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/mpuAndPermissions.html
		if s3Error := enforceBucketPolicy("s3:CreateBucket", bucket, r.URL); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	case authTypePresigned:
		ok, err := auth.DoesPresignedSignatureMatch()
		if err != nil {
			errorIf(err.Trace(r.URL.String()), "Presigned signature verification failed.", nil)
			writeErrorResponse(w, r, ErrSignatureDoesNotMatch, r.URL.Path)
			return
		}
		if !ok {
			writeErrorResponse(w, r, ErrSignatureDoesNotMatch, r.URL.Path)
			return
		}
	case authTypeSigned:
		// Verify signature for the incoming body if any.
		locationBytes, e := ioutil.ReadAll(r.Body)
		if e != nil {
			errorIf(probe.NewError(e), "MakeBucket failed.", nil)
			writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
			return
		}
		sh := sha256.New()
		sh.Write(locationBytes)
		ok, err := auth.DoesSignatureMatch(hex.EncodeToString(sh.Sum(nil)))
		if err != nil {
			errorIf(err.Trace(), "MakeBucket failed.", nil)
			writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
			return
		}
		if !ok {
			writeErrorResponse(w, r, ErrSignatureDoesNotMatch, r.URL.Path)
			return
		}
	}

	// Make bucket.
	err := api.Filesystem.MakeBucket(bucket)
	if err != nil {
		errorIf(err.Trace(), "MakeBucket failed.", nil)
		switch err.ToGoError().(type) {
		case fs.BucketNameInvalid:
			writeErrorResponse(w, r, ErrInvalidBucketName, r.URL.Path)
		case fs.BucketExists:
			writeErrorResponse(w, r, ErrBucketAlreadyExists, r.URL.Path)
		default:
			writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		}
		return
	}
	// Make sure to add Location information here only for bucket
	w.Header().Set("Location", getLocation(r))
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
	// Here the parameter is the size of the form data that should
	// be loaded in memory, the remaining being put in temporary
	// files
	reader, e := r.MultipartReader()
	if e != nil {
		errorIf(probe.NewError(e), "Unable to initialize multipart reader.", nil)
		writeErrorResponse(w, r, ErrMalformedPOSTRequest, r.URL.Path)
		return
	}

	fileBody, formValues, err := extractHTTPFormValues(reader)
	if err != nil {
		errorIf(err.Trace(), "Unable to parse form values.", nil)
		writeErrorResponse(w, r, ErrMalformedPOSTRequest, r.URL.Path)
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
		writeErrorResponse(w, r, ErrSignatureDoesNotMatch, r.URL.Path)
		return
	}
	if !ok {
		writeErrorResponse(w, r, ErrSignatureDoesNotMatch, r.URL.Path)
		return
	}
	if err = signature4.ApplyPolicyCond(formValues); err != nil {
		errorIf(err.Trace(), "Invalid request, policy doesn't match with the endpoint.", nil)
		writeErrorResponse(w, r, ErrMalformedPOSTRequest, r.URL.Path)
		return
	}
	objectInfo, err := api.Filesystem.CreateObject(bucket, object, "", -1, fileBody, nil)
	if err != nil {
		errorIf(err.Trace(), "CreateObject failed.", nil)
		switch err.ToGoError().(type) {
		case fs.RootPathFull:
			writeErrorResponse(w, r, ErrRootPathFull, r.URL.Path)
		case fs.BucketNotFound:
			writeErrorResponse(w, r, ErrNoSuchBucket, r.URL.Path)
		case fs.BucketNameInvalid:
			writeErrorResponse(w, r, ErrInvalidBucketName, r.URL.Path)
		case fs.BadDigest:
			writeErrorResponse(w, r, ErrBadDigest, r.URL.Path)
		case fs.IncompleteBody:
			writeErrorResponse(w, r, ErrIncompleteBody, r.URL.Path)
		case fs.InvalidDigest:
			writeErrorResponse(w, r, ErrInvalidDigest, r.URL.Path)
		default:
			writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		}
		return
	}
	if objectInfo.MD5Sum != "" {
		w.Header().Set("ETag", "\""+objectInfo.MD5Sum+"\"")
	}
	writeSuccessResponse(w, nil)
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

	switch getRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case authTypePresigned, authTypeSigned:
		if s3Error := isReqAuthenticated(api.Signature, r); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}

	_, err := api.Filesystem.GetBucketInfo(bucket)
	if err != nil {
		errorIf(err.Trace(), "GetBucketInfo failed.", nil)
		switch err.ToGoError().(type) {
		case fs.BucketNotFound:
			writeErrorResponse(w, r, ErrNoSuchBucket, r.URL.Path)
		case fs.BucketNameInvalid:
			writeErrorResponse(w, r, ErrInvalidBucketName, r.URL.Path)
		default:
			writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		}
		return
	}
	writeSuccessResponse(w, nil)
}

// DeleteBucketHandler - Delete bucket
func (api storageAPI) DeleteBucketHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	switch getRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case authTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/mpuAndPermissions.html
		if s3Error := enforceBucketPolicy("s3:DeleteBucket", bucket, r.URL); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	case authTypePresigned, authTypeSigned:
		if s3Error := isReqAuthenticated(api.Signature, r); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}

	err := api.Filesystem.DeleteBucket(bucket)
	if err != nil {
		errorIf(err.Trace(), "DeleteBucket failed.", nil)
		switch err.ToGoError().(type) {
		case fs.BucketNotFound:
			writeErrorResponse(w, r, ErrNoSuchBucket, r.URL.Path)
		case fs.BucketNotEmpty:
			writeErrorResponse(w, r, ErrBucketNotEmpty, r.URL.Path)
		default:
			writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		}
		return
	}

	// Delete bucket access policy, if present - ignore any errors.
	removeBucketPolicy(bucket)

	// Write success response.
	writeSuccessNoContent(w)
}
