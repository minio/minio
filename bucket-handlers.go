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
	"github.com/minio/minio/pkg/probe"
)

// http://docs.aws.amazon.com/AmazonS3/latest/dev/using-with-s3-actions.html
func enforceBucketPolicy(action string, bucket string, reqURL *url.URL) (s3Error APIErrorCode) {
	// Read saved bucket policy.
	policy, err := readBucketPolicy(bucket)
	if err != nil {
		errorIf(err.Trace(bucket), "GetBucketPolicy failed.", nil)
		switch err.ToGoError().(type) {
		case BucketNotFound:
			return ErrNoSuchBucket
		case BucketNameInvalid:
			return ErrInvalidBucketName
		default:
			// For any other error just return AccessDenied.
			return ErrAccessDenied
		}
	}
	// Parse the saved policy.
	bucketPolicy, e := parseBucketPolicy(policy)
	if e != nil {
		errorIf(probe.NewError(e), "Parse policy failed.", nil)
		return ErrAccessDenied
	}

	// Construct resource in 'arn:aws:s3:::examplebucket' format.
	resource := AWSResourcePrefix + strings.TrimPrefix(reqURL.Path, "/")

	// Get conditions for policy verification.
	conditions := make(map[string]string)
	for queryParam := range reqURL.Query() {
		conditions[queryParam] = reqURL.Query().Get(queryParam)
	}

	// Validate action, resource and conditions with current policy statements.
	if !bucketPolicyEvalStatements(action, resource, conditions, bucketPolicy.Statements) {
		return ErrAccessDenied
	}
	return ErrNone
}

// GetBucketLocationHandler - GET Bucket location.
// -------------------------
// This operation returns bucket location.
func (api objectAPIHandlers) GetBucketLocationHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	switch getRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case authTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/using-with-s3-actions.html
		if s3Error := enforceBucketPolicy("s3:GetBucketLocation", bucket, r.URL); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	case authTypeSigned, authTypePresigned:
		payload, e := ioutil.ReadAll(r.Body)
		if e != nil {
			writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
			return
		}
		// Verify Content-Md5, if payload is set.
		if r.Header.Get("Content-Md5") != "" {
			if r.Header.Get("Content-Md5") != base64.StdEncoding.EncodeToString(sumMD5(payload)) {
				writeErrorResponse(w, r, ErrBadDigest, r.URL.Path)
				return
			}
		}
		// Populate back the payload.
		r.Body = ioutil.NopCloser(bytes.NewReader(payload))
		var s3Error APIErrorCode // API error code.
		validateRegion := false  // Validate region.
		if isRequestSignatureV4(r) {
			s3Error = doesSignatureMatch(hex.EncodeToString(sum256(payload)), r, validateRegion)
		} else if isRequestPresignedSignatureV4(r) {
			s3Error = doesPresignedSignatureMatch(hex.EncodeToString(sum256(payload)), r, validateRegion)
		}
		if s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}

	_, err := api.ObjectAPI.GetBucketInfo(bucket)
	if err != nil {
		errorIf(err.Trace(), "GetBucketInfo failed.", nil)
		switch err.ToGoError().(type) {
		case BucketNotFound:
			writeErrorResponse(w, r, ErrNoSuchBucket, r.URL.Path)
		case BucketNameInvalid:
			writeErrorResponse(w, r, ErrInvalidBucketName, r.URL.Path)
		default:
			writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		}
		return
	}

	// Generate response.
	encodedSuccessResponse := encodeResponse(LocationResponse{})
	// Get current region.
	region := serverConfig.GetRegion()
	if region != "us-east-1" {
		encodedSuccessResponse = encodeResponse(LocationResponse{
			Location: region,
		})
	}
	setCommonHeaders(w) // Write headers.
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
func (api objectAPIHandlers) ListMultipartUploadsHandler(w http.ResponseWriter, r *http.Request) {
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
		if s3Error := isReqAuthenticated(r); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}

	prefix, keyMarker, uploadIDMarker, delimiter, maxUploads, _ := getBucketMultipartResources(r.URL.Query())
	if maxUploads < 0 {
		writeErrorResponse(w, r, ErrInvalidMaxUploads, r.URL.Path)
		return
	}
	if keyMarker != "" {
		// Unescape keyMarker string
		keyMarkerUnescaped, e := url.QueryUnescape(keyMarker)
		if e != nil {
			if e != nil {
				// Return 'NoSuchKey' to indicate invalid marker key.
				writeErrorResponse(w, r, ErrNoSuchKey, r.URL.Path)
				return
			}
			keyMarker = keyMarkerUnescaped
			// Marker not common with prefix is not implemented.
			if !strings.HasPrefix(keyMarker, prefix) {
				writeErrorResponse(w, r, ErrNotImplemented, r.URL.Path)
				return
			}
		}
	}

	listMultipartsInfo, err := api.ObjectAPI.ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
	if err != nil {
		errorIf(err.Trace(), "ListMultipartUploads failed.", nil)
		switch err.ToGoError().(type) {
		case BucketNotFound:
			writeErrorResponse(w, r, ErrNoSuchBucket, r.URL.Path)
		default:
			writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		}
		return
	}
	// generate response
	response := generateListMultipartUploadsResponse(bucket, listMultipartsInfo)
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
func (api objectAPIHandlers) ListObjectsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	switch getRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case authTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/using-with-s3-actions.html
		if s3Error := enforceBucketPolicy("s3:ListBucket", bucket, r.URL); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	case authTypeSigned, authTypePresigned:
		if s3Error := isReqAuthenticated(r); s3Error != ErrNone {
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
	// Verify if delimiter is anything other than '/', which we do not support.
	if delimiter != "" && delimiter != "/" {
		writeErrorResponse(w, r, ErrNotImplemented, r.URL.Path)
		return
	}
	// If marker is set unescape.
	if marker != "" {
		// Try to unescape marker.
		markerUnescaped, e := url.QueryUnescape(marker)
		if e != nil {
			// Return 'NoSuchKey' to indicate invalid marker key.
			writeErrorResponse(w, r, ErrNoSuchKey, r.URL.Path)
			return
		}
		marker = markerUnescaped
		// Marker not common with prefix is not implemented.
		if !strings.HasPrefix(marker, prefix) {
			writeErrorResponse(w, r, ErrNotImplemented, r.URL.Path)
			return
		}
	}

	listObjectsInfo, err := api.ObjectAPI.ListObjects(bucket, prefix, marker, delimiter, maxkeys)
	if err == nil {
		// generate response
		response := generateListObjectsResponse(bucket, prefix, marker, delimiter, maxkeys, listObjectsInfo)
		encodedSuccessResponse := encodeResponse(response)
		// Write headers
		setCommonHeaders(w)
		// Write success response.
		writeSuccessResponse(w, encodedSuccessResponse)
		return
	}
	switch err.ToGoError().(type) {
	case BucketNameInvalid:
		writeErrorResponse(w, r, ErrInvalidBucketName, r.URL.Path)
	case BucketNotFound:
		writeErrorResponse(w, r, ErrNoSuchBucket, r.URL.Path)
	case ObjectNameInvalid:
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
func (api objectAPIHandlers) ListBucketsHandler(w http.ResponseWriter, r *http.Request) {
	// List buckets does not support bucket policies.
	switch getRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case authTypeSigned, authTypePresigned:
		payload, e := ioutil.ReadAll(r.Body)
		if e != nil {
			writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
			return
		}
		// Verify Content-Md5, if payload is set.
		if r.Header.Get("Content-Md5") != "" {
			if r.Header.Get("Content-Md5") != base64.StdEncoding.EncodeToString(sumMD5(payload)) {
				writeErrorResponse(w, r, ErrBadDigest, r.URL.Path)
				return
			}
		}
		// Populate back the payload.
		r.Body = ioutil.NopCloser(bytes.NewReader(payload))
		var s3Error APIErrorCode // API error code.
		validateRegion := false  // Validate region.
		if isRequestSignatureV4(r) {
			s3Error = doesSignatureMatch(hex.EncodeToString(sum256(payload)), r, validateRegion)
		} else if isRequestPresignedSignatureV4(r) {
			s3Error = doesPresignedSignatureMatch(hex.EncodeToString(sum256(payload)), r, validateRegion)
		}
		if s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}

	bucketsInfo, err := api.ObjectAPI.ListBuckets()
	if err == nil {
		// generate response
		response := generateListBucketsResponse(bucketsInfo)
		encodedSuccessResponse := encodeResponse(response)
		// write headers
		setCommonHeaders(w)
		// write response
		writeSuccessResponse(w, encodedSuccessResponse)
		return
	}
	errorIf(err.Trace(), "ListBuckets failed.", nil)
	switch err.ToGoError().(type) {
	case StorageInsufficientReadResources:
		writeErrorResponse(w, r, ErrInsufficientReadResources, r.URL.Path)
	default:
		writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
	}

}

// DeleteMultipleObjectsHandler - deletes multiple objects.
func (api objectAPIHandlers) DeleteMultipleObjectsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	switch getRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case authTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/using-with-s3-actions.html
		if s3Error := enforceBucketPolicy("s3:DeleteObject", bucket, r.URL); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	case authTypePresigned, authTypeSigned:
		if s3Error := isReqAuthenticated(r); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}

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

	// Allocate incoming content length bytes.
	deleteXMLBytes := make([]byte, r.ContentLength)

	// Read incoming body XML bytes.
	_, e := io.ReadFull(r.Body, deleteXMLBytes)
	if e != nil {
		errorIf(probe.NewError(e), "DeleteMultipleObjects failed.", nil)
		writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		return
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
		err := api.ObjectAPI.DeleteObject(bucket, object.ObjectName)
		if err == nil {
			deletedObjects = append(deletedObjects, ObjectIdentifier{
				ObjectName: object.ObjectName,
			})
		} else {
			errorIf(err.Trace(object.ObjectName), "DeleteObject failed.", nil)
			switch err.ToGoError().(type) {
			case BucketNameInvalid:
				deleteErrors = append(deleteErrors, DeleteError{
					Code:    errorCodeResponse[ErrInvalidBucketName].Code,
					Message: errorCodeResponse[ErrInvalidBucketName].Description,
					Key:     object.ObjectName,
				})
			case BucketNotFound:
				deleteErrors = append(deleteErrors, DeleteError{
					Code:    errorCodeResponse[ErrNoSuchBucket].Code,
					Message: errorCodeResponse[ErrNoSuchBucket].Description,
					Key:     object.ObjectName,
				})
			case ObjectNotFound:
				deleteErrors = append(deleteErrors, DeleteError{
					Code:    errorCodeResponse[ErrNoSuchKey].Code,
					Message: errorCodeResponse[ErrNoSuchKey].Description,
					Key:     object.ObjectName,
				})
			case ObjectNameInvalid:
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
func (api objectAPIHandlers) PutBucketHandler(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	// Set http request for signature.
	switch getRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case authTypePresigned, authTypeSigned:
		if s3Error := isReqAuthenticated(r); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}

	// the location value in the request body should match the Region in serverConfig.
	// other values of location are not accepted.
	// make bucket fails in such cases.
	errCode := isValidLocationContraint(r.Body, serverConfig.GetRegion())
	if errCode != ErrNone {
		writeErrorResponse(w, r, errCode, r.URL.Path)
		return
	}
	// Make bucket.
	err := api.ObjectAPI.MakeBucket(bucket)
	if err != nil {
		errorIf(err.Trace(), "MakeBucket failed.", nil)
		switch err.ToGoError().(type) {
		case BucketNameInvalid:
			writeErrorResponse(w, r, ErrInvalidBucketName, r.URL.Path)
		case BucketExists:
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
func (api objectAPIHandlers) PostPolicyBucketHandler(w http.ResponseWriter, r *http.Request) {
	// Here the parameter is the size of the form data that should
	// be loaded in memory, the remaining being put in temporary files.
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

	// Verify policy signature.
	apiErr := doesPolicySignatureMatch(formValues)
	if apiErr != ErrNone {
		writeErrorResponse(w, r, apiErr, r.URL.Path)
		return
	}
	if apiErr = checkPostPolicy(formValues); apiErr != ErrNone {
		writeErrorResponse(w, r, apiErr, r.URL.Path)
		return
	}
	md5Sum, err := api.ObjectAPI.PutObject(bucket, object, -1, fileBody, nil)
	if err != nil {
		errorIf(err.Trace(), "PutObject failed.", nil)
		switch err.ToGoError().(type) {
		case StorageFull:
			writeErrorResponse(w, r, ErrStorageFull, r.URL.Path)
		case BucketNotFound:
			writeErrorResponse(w, r, ErrNoSuchBucket, r.URL.Path)
		case BucketNameInvalid:
			writeErrorResponse(w, r, ErrInvalidBucketName, r.URL.Path)
		case BadDigest:
			writeErrorResponse(w, r, ErrBadDigest, r.URL.Path)
		case IncompleteBody:
			writeErrorResponse(w, r, ErrIncompleteBody, r.URL.Path)
		default:
			writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		}
		return
	}
	if md5Sum != "" {
		w.Header().Set("ETag", "\""+md5Sum+"\"")
	}
	writeSuccessResponse(w, nil)
}

// HeadBucketHandler - HEAD Bucket
// ----------
// This operation is useful to determine if a bucket exists.
// The operation returns a 200 OK if the bucket exists and you
// have permission to access it. Otherwise, the operation might
// return responses such as 404 Not Found and 403 Forbidden.
func (api objectAPIHandlers) HeadBucketHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	switch getRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case authTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/using-with-s3-actions.html
		if s3Error := enforceBucketPolicy("s3:ListBucket", bucket, r.URL); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	case authTypePresigned, authTypeSigned:
		if s3Error := isReqAuthenticated(r); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}

	_, err := api.ObjectAPI.GetBucketInfo(bucket)
	if err != nil {
		errorIf(err.Trace(), "GetBucketInfo failed.", nil)
		switch err.ToGoError().(type) {
		case BucketNotFound:
			writeErrorResponse(w, r, ErrNoSuchBucket, r.URL.Path)
		case BucketNameInvalid:
			writeErrorResponse(w, r, ErrInvalidBucketName, r.URL.Path)
		case StorageInsufficientReadResources:
			writeErrorResponse(w, r, ErrInsufficientReadResources, r.URL.Path)
		default:
			writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		}
		return
	}
	writeSuccessResponse(w, nil)
}

// DeleteBucketHandler - Delete bucket
func (api objectAPIHandlers) DeleteBucketHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	switch getRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case authTypePresigned, authTypeSigned:
		if s3Error := isReqAuthenticated(r); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}

	err := api.ObjectAPI.DeleteBucket(bucket)
	if err != nil {
		errorIf(err.Trace(), "DeleteBucket failed.", nil)
		switch err.ToGoError().(type) {
		case BucketNotFound:
			writeErrorResponse(w, r, ErrNoSuchBucket, r.URL.Path)
		case BucketNotEmpty:
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
