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

package cmd

import (
	"encoding/base64"
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

	mux "github.com/gorilla/mux"
	"github.com/minio/minio-go/pkg/set"
)

// http://docs.aws.amazon.com/AmazonS3/latest/dev/using-with-s3-actions.html
// Enforces bucket policies for a bucket for a given tatusaction.
func enforceBucketPolicy(bucket string, action string, reqURL *url.URL) (s3Error APIErrorCode) {
	// Verify if bucket actually exists
	if err := checkBucketExist(bucket, newObjectLayerFn()); err != nil {
		err = errorCause(err)
		switch err.(type) {
		case BucketNameInvalid:
			// Return error for invalid bucket name.
			return ErrInvalidBucketName
		case BucketNotFound:
			// For no bucket found we return NoSuchBucket instead.
			return ErrNoSuchBucket
		}
		errorIf(err, "Unable to read bucket policy.")
		// Return internal error for any other errors so that we can investigate.
		return ErrInternalError
	}

	// Fetch bucket policy, if policy is not set return access denied.
	policy := globalBucketPolicies.GetBucketPolicy(bucket)
	if policy == nil {
		return ErrAccessDenied
	}

	// Construct resource in 'arn:aws:s3:::examplebucket/object' format.
	resource := AWSResourcePrefix + strings.TrimSuffix(strings.TrimPrefix(reqURL.Path, "/"), "/")

	// Get conditions for policy verification.
	conditionKeyMap := make(map[string]set.StringSet)
	for queryParam := range reqURL.Query() {
		conditionKeyMap[queryParam] = set.CreateStringSet(reqURL.Query().Get(queryParam))
	}

	// Validate action, resource and conditions with current policy statements.
	if !bucketPolicyEvalStatements(action, resource, conditionKeyMap, policy.Statements) {
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

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, r, ErrServerNotInitialized, r.URL.Path)
		return
	}

	if s3Error := checkRequestAuthType(r, bucket, "s3:GetBucketLocation", "us-east-1"); s3Error != ErrNone {
		writeErrorResponse(w, r, s3Error, r.URL.Path)
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

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, r, ErrServerNotInitialized, r.URL.Path)
		return
	}

	if s3Error := checkRequestAuthType(r, bucket, "s3:ListBucketMultipartUploads", serverConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponse(w, r, s3Error, r.URL.Path)
		return
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

// ListBucketsHandler - GET Service.
// -----------
// This implementation of the GET operation returns a list of all buckets
// owned by the authenticated sender of the request.
func (api objectAPIHandlers) ListBucketsHandler(w http.ResponseWriter, r *http.Request) {
	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, r, ErrServerNotInitialized, r.URL.Path)
		return
	}

	// ListBuckets does not have any bucket action.
	s3Error := checkRequestAuthType(r, "", "", "us-east-1")
	if s3Error == ErrInvalidRegion {
		// Clients like boto3 send listBuckets() call signed with region that is configured.
		s3Error = checkRequestAuthType(r, "", "", serverConfig.GetRegion())
	}
	if s3Error != ErrNone {
		writeErrorResponse(w, r, s3Error, r.URL.Path)
		return
	}
	// Invoke the list buckets.
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

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, r, ErrServerNotInitialized, r.URL.Path)
		return
	}

	if s3Error := checkRequestAuthType(r, bucket, "s3:DeleteObject", serverConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponse(w, r, s3Error, r.URL.Path)
		return
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

	var wg = &sync.WaitGroup{} // Allocate a new wait group.
	var dErrs = make([]error, len(deleteObjects.Objects))

	// Delete all requested objects in parallel.
	for index, object := range deleteObjects.Objects {
		wg.Add(1)
		go func(i int, obj ObjectIdentifier) {
			defer wg.Done()
			dErr := objectAPI.DeleteObject(bucket, obj.ObjectName)
			if dErr != nil {
				dErrs[i] = dErr
			}
		}(index, object)
	}
	wg.Wait()

	// Collect deleted objects and errors if any.
	var deletedObjects []ObjectIdentifier
	var deleteErrors []DeleteError
	for index, err := range dErrs {
		object := deleteObjects.Objects[index]
		// Success deleted objects are collected separately.
		if err == nil {
			deletedObjects = append(deletedObjects, object)
			continue
		}
		if _, ok := errorCause(err).(ObjectNotFound); ok {
			// If the object is not found it should be
			// accounted as deleted as per S3 spec.
			deletedObjects = append(deletedObjects, object)
			continue
		}
		errorIf(err, "Unable to delete object. %s", object.ObjectName)
		// Error during delete should be collected separately.
		deleteErrors = append(deleteErrors, DeleteError{
			Code:    errorCodeResponse[toAPIErrorCode(err)].Code,
			Message: errorCodeResponse[toAPIErrorCode(err)].Description,
			Key:     object.ObjectName,
		})
	}

	// Generate response
	response := generateMultiDeleteResponse(deleteObjects.Quiet, deletedObjects, deleteErrors)
	encodedSuccessResponse := encodeResponse(response)
	// Write headers
	setCommonHeaders(w)
	// Write success response.
	writeSuccessResponse(w, encodedSuccessResponse)

	// Notify deleted event for objects.
	for _, dobj := range deletedObjects {
		eventNotify(eventData{
			Type:   ObjectRemovedDelete,
			Bucket: bucket,
			ObjInfo: ObjectInfo{
				Name: dobj.ObjectName,
			},
			ReqParams: map[string]string{
				"sourceIPAddress": r.RemoteAddr,
			},
		})
	}
}

// PutBucketHandler - PUT Bucket
// ----------
// This implementation of the PUT operation creates a new bucket for authenticated request
func (api objectAPIHandlers) PutBucketHandler(w http.ResponseWriter, r *http.Request) {
	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, r, ErrServerNotInitialized, r.URL.Path)
		return
	}

	// PutBucket does not have any bucket action.
	if s3Error := checkRequestAuthType(r, "", "", "us-east-1"); s3Error != ErrNone {
		writeErrorResponse(w, r, s3Error, r.URL.Path)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	// Validate if incoming location constraint is valid, reject
	// requests which do not follow valid region requirements.
	if s3Error := isValidLocationConstraint(r); s3Error != ErrNone {
		writeErrorResponse(w, r, s3Error, r.URL.Path)
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
	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, r, ErrServerNotInitialized, r.URL.Path)
		return
	}

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

	if fileName != "" && strings.Contains(formValues["Key"], "${filename}") {
		// S3 feature to replace ${filename} found in Key form field
		// by the filename attribute passed in multipart
		formValues["Key"] = strings.Replace(formValues["Key"], "${filename}", fileName, -1)
	}
	object := formValues["Key"]

	// Verify policy signature.
	apiErr := doesPolicySignatureMatch(formValues)
	if apiErr != ErrNone {
		writeErrorResponse(w, r, apiErr, r.URL.Path)
		return
	}

	policyBytes, err := base64.StdEncoding.DecodeString(formValues["Policy"])
	if err != nil {
		writeErrorResponse(w, r, ErrMalformedPOSTRequest, r.URL.Path)
		return
	}

	postPolicyForm, err := parsePostPolicyForm(string(policyBytes))
	if err != nil {
		writeErrorResponse(w, r, ErrMalformedPOSTRequest, r.URL.Path)
		return
	}

	// Make sure formValues adhere to policy restrictions.
	if apiErr = checkPostPolicy(formValues, postPolicyForm); apiErr != ErrNone {
		writeErrorResponse(w, r, apiErr, r.URL.Path)
		return
	}

	// Use rangeReader to ensure that object size is within expected range.
	lengthRange := postPolicyForm.Conditions.ContentLengthRange
	if lengthRange.Valid {
		// If policy restricted the size of the object.
		fileBody = &rangeReader{
			Reader: fileBody,
			Min:    lengthRange.Min,
			Max:    lengthRange.Max,
		}
	} else {
		// Default values of min/max size of the object.
		fileBody = &rangeReader{
			Reader: fileBody,
			Min:    0,
			Max:    maxObjectSize,
		}
	}

	// Save metadata.
	metadata := make(map[string]string)
	// Nothing to store right now.

	sha256sum := ""

	objInfo, err := objectAPI.PutObject(bucket, object, -1, fileBody, metadata, sha256sum)
	if err != nil {
		errorIf(err, "Unable to create object.")
		writeErrorResponse(w, r, toAPIErrorCode(err), r.URL.Path)
		return
	}
	w.Header().Set("ETag", "\""+objInfo.MD5Sum+"\"")
	w.Header().Set("Location", getObjectLocation(bucket, object))

	// Set common headers.
	setCommonHeaders(w)

	// Write successful response.
	writeSuccessNoContent(w)

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

// HeadBucketHandler - HEAD Bucket
// ----------
// This operation is useful to determine if a bucket exists.
// The operation returns a 200 OK if the bucket exists and you
// have permission to access it. Otherwise, the operation might
// return responses such as 404 Not Found and 403 Forbidden.
func (api objectAPIHandlers) HeadBucketHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, r, ErrServerNotInitialized, r.URL.Path)
		return
	}

	if s3Error := checkRequestAuthType(r, bucket, "s3:ListBucket", serverConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponse(w, r, s3Error, r.URL.Path)
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
	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, r, ErrServerNotInitialized, r.URL.Path)
		return
	}

	// DeleteBucket does not have any bucket action.
	if s3Error := checkRequestAuthType(r, "", "", serverConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponse(w, r, s3Error, r.URL.Path)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	// Attempt to delete bucket.
	if err := objectAPI.DeleteBucket(bucket); err != nil {
		errorIf(err, "Unable to delete a bucket.")
		writeErrorResponse(w, r, toAPIErrorCode(err), r.URL.Path)
		return
	}

	// Delete bucket access policy, if present - ignore any errors.
	_ = removeBucketPolicy(bucket, objectAPI)

	// Delete notification config, if present - ignore any errors.
	_ = removeNotificationConfig(bucket, objectAPI)

	// Delete listener config, if present - ignore any errors.
	_ = removeListenerConfig(bucket, objectAPI)

	// Write success response.
	writeSuccessNoContent(w)
}
