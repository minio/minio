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
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"

	mux "github.com/gorilla/mux"
)

// http://docs.aws.amazon.com/AmazonS3/latest/dev/using-with-s3-actions.html
// Enforces bucket policies for a bucket for a given tatusaction.
func enforceBucketPolicy(bucket string, action string, reqURL *url.URL) (s3Error APIErrorCode) {
	if !IsValidBucketName(bucket) {
		return ErrInvalidBucketName
	}
	// Fetch bucket policy, if policy is not set return access denied.
	policy := globalBucketPolicies.GetBucketPolicy(bucket)
	if policy == nil {
		return ErrAccessDenied
	}

	// Construct resource in 'arn:aws:s3:::examplebucket/object' format.
	resource := AWSResourcePrefix + strings.TrimPrefix(reqURL.Path, "/")

	// Get conditions for policy verification.
	conditions := make(map[string]string)
	for queryParam := range reqURL.Query() {
		conditions[queryParam] = reqURL.Query().Get(queryParam)
	}

	// Validate action, resource and conditions with current policy statements.
	if !bucketPolicyEvalStatements(action, resource, conditions, policy.Statements) {
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
		if s3Error := enforceBucketPolicy(bucket, "s3:GetBucketLocation", r.URL); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	case authTypeSigned, authTypePresigned:
		if s3Error := isReqAuthenticated(r); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		return
	}
	if _, err := objectAPI.GetBucketInfo(bucket); err != nil {
		errorIf(err, "Unable to fetch bucket info.")
		writeErrorResponse(w, r, toAPIErrorCode(err), r.URL.Path)
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
		if s3Error := enforceBucketPolicy(bucket, "s3:ListBucketMultipartUploads", r.URL); s3Error != ErrNone {
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
		// Marker not common with prefix is not implemented.
		if !strings.HasPrefix(keyMarker, prefix) {
			writeErrorResponse(w, r, ErrNotImplemented, r.URL.Path)
			return
		}
	}

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		return
	}
	listMultipartsInfo, err := objectAPI.ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
	if err != nil {
		errorIf(err, "Unable to list multipart uploads.")
		writeErrorResponse(w, r, toAPIErrorCode(err), r.URL.Path)
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

// ListBucketsHandler - GET Service
// -----------
// This implementation of the GET operation returns a list of all buckets
// owned by the authenticated sender of the request.
func (api objectAPIHandlers) ListBucketsHandler(w http.ResponseWriter, r *http.Request) {
	// List buckets does not support bucket policies, no need to enforce it.
	if s3Error := checkAuth(r); s3Error != ErrNone {
		writeErrorResponse(w, r, s3Error, r.URL.Path)
		return
	}

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		return
	}
	bucketsInfo, err := objectAPI.ListBuckets()
	if err != nil {
		errorIf(err, "Unable to list buckets.")
		writeErrorResponse(w, r, toAPIErrorCode(err), r.URL.Path)
		return
	}

	// Generate response.
	response := generateListBucketsResponse(bucketsInfo)
	encodedSuccessResponse := encodeResponse(response)
	// Write headers.
	setCommonHeaders(w)
	// Write response.
	writeSuccessResponse(w, encodedSuccessResponse)
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
		if s3Error := enforceBucketPolicy(bucket, "s3:DeleteObject", r.URL); s3Error != ErrNone {
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
	if _, err := io.ReadFull(r.Body, deleteXMLBytes); err != nil {
		errorIf(err, "Unable to read HTTP body.")
		writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		return
	}

	// Unmarshal list of keys to be deleted.
	deleteObjects := &DeleteObjectsRequest{}
	if err := xml.Unmarshal(deleteXMLBytes, deleteObjects); err != nil {
		errorIf(err, "Unable to unmarshal delete objects request XML.")
		writeErrorResponse(w, r, ErrMalformedXML, r.URL.Path)
		return
	}

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		return
	}
	var deleteErrors []DeleteError
	var deletedObjects []ObjectIdentifier
	// Loop through all the objects and delete them sequentially.
	for _, object := range deleteObjects.Objects {
		err := objectAPI.DeleteObject(bucket, object.ObjectName)
		if err == nil {
			deletedObjects = append(deletedObjects, ObjectIdentifier{
				ObjectName: object.ObjectName,
			})
		} else {
			errorIf(err, "Unable to delete object.")
			deleteErrors = append(deleteErrors, DeleteError{
				Code:    errorCodeResponse[toAPIErrorCode(err)].Code,
				Message: errorCodeResponse[toAPIErrorCode(err)].Description,
				Key:     object.ObjectName,
			})
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
	// PutBucket does not support policies, use checkAuth to validate signature.
	if s3Error := checkAuth(r); s3Error != ErrNone {
		writeErrorResponse(w, r, s3Error, r.URL.Path)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	// Validate if incoming location constraint is valid, reject
	// requests which do not follow valid region requirements.
	if s3Error := isValidLocationConstraint(r); s3Error != ErrNone {
		writeErrorResponse(w, r, s3Error, r.URL.Path)
	}

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		return
	}
	// Proceed to creating a bucket.
	err := objectAPI.MakeBucket(bucket)
	if err != nil {
		errorIf(err, "Unable to create a bucket.")
		writeErrorResponse(w, r, toAPIErrorCode(err), r.URL.Path)
		return
	}
	// Make sure to add Location information here only for bucket
	w.Header().Set("Location", getLocation(r))
	writeSuccessResponse(w, nil)
}

// PostPolicyBucketHandler - POST policy
// ----------
// This implementation of the POST operation handles object creation with a specified
// signature policy in multipart/form-data
func (api objectAPIHandlers) PostPolicyBucketHandler(w http.ResponseWriter, r *http.Request) {
	// Here the parameter is the size of the form data that should
	// be loaded in memory, the remaining being put in temporary files.
	reader, err := r.MultipartReader()
	if err != nil {
		errorIf(err, "Unable to initialize multipart reader.")
		writeErrorResponse(w, r, ErrMalformedPOSTRequest, r.URL.Path)
		return
	}

	fileBody, fileName, formValues, err := extractPostPolicyFormValues(reader)
	if err != nil {
		errorIf(err, "Unable to parse form values.")
		writeErrorResponse(w, r, ErrMalformedPOSTRequest, r.URL.Path)
		return
	}
	bucket := mux.Vars(r)["bucket"]
	formValues["Bucket"] = bucket
	object := formValues["Key"]

	if fileName != "" && strings.Contains(object, "${filename}") {
		// S3 feature to replace ${filename} found in Key form field
		// by the filename attribute passed in multipart
		object = strings.Replace(object, "${filename}", fileName, -1)
	}

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

	// Save metadata.
	metadata := make(map[string]string)
	// Nothing to store right now.

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		return
	}
	md5Sum, err := objectAPI.PutObject(bucket, object, -1, fileBody, metadata)
	if err != nil {
		errorIf(err, "Unable to create object.")
		writeErrorResponse(w, r, toAPIErrorCode(err), r.URL.Path)
		return
	}
	if md5Sum != "" {
		w.Header().Set("ETag", "\""+md5Sum+"\"")
	}
	encodedSuccessResponse := encodeResponse(PostResponse{
		Location: getObjectLocation(bucket, object), // TODO Full URL is preferred
		Bucket:   bucket,
		Key:      object,
		ETag:     md5Sum,
	})

	setCommonHeaders(w)

	writeSuccessResponse(w, encodedSuccessResponse)

	// Fetch object info for notifications.
	objInfo, err := objectAPI.GetObjectInfo(bucket, object)
	if err != nil {
		errorIf(err, "Unable to fetch object info for \"%s\"", path.Join(bucket, object))
		return
	}

	if eventN.IsBucketNotificationSet(bucket) {
		// Notify object created event.
		eventNotify(eventData{
			Type:    ObjectCreatedPost,
			Bucket:  bucket,
			ObjInfo: objInfo,
			ReqParams: map[string]string{
				"sourceIPAddress": r.RemoteAddr,
			},
		})
	}
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
		if s3Error := enforceBucketPolicy(bucket, "s3:ListBucket", r.URL); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	case authTypePresigned, authTypeSigned:
		if s3Error := isReqAuthenticated(r); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		return
	}
	if _, err := objectAPI.GetBucketInfo(bucket); err != nil {
		errorIf(err, "Unable to fetch bucket info.")
		writeErrorResponse(w, r, toAPIErrorCode(err), r.URL.Path)
		return
	}
	writeSuccessResponse(w, nil)
}

// DeleteBucketHandler - Delete bucket
func (api objectAPIHandlers) DeleteBucketHandler(w http.ResponseWriter, r *http.Request) {
	// DeleteBucket does not support bucket policies, use checkAuth to validate signature.
	if s3Error := checkAuth(r); s3Error != ErrNone {
		writeErrorResponse(w, r, s3Error, r.URL.Path)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		return
	}
	// Attempt to delete bucket.
	if err := objectAPI.DeleteBucket(bucket); err != nil {
		errorIf(err, "Unable to delete a bucket.")
		writeErrorResponse(w, r, toAPIErrorCode(err), r.URL.Path)
		return
	}

	// Delete bucket access policy, if present - ignore any errors.
	removeBucketPolicy(bucket, api.ObjectAPI)

	// Delete notification config, if present - ignore any errors.
	removeNotificationConfig(bucket, api.ObjectAPI)

	// Write success response.
	writeSuccessNoContent(w)
}
