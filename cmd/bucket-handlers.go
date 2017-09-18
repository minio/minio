/*
 * Minio Cloud Storage, (C) 2015, 2016, 2017 Minio, Inc.
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
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"

	mux "github.com/gorilla/mux"
	"github.com/minio/minio-go/pkg/set"
)

// http://docs.aws.amazon.com/AmazonS3/latest/dev/using-with-s3-actions.html
// Enforces bucket policies for a bucket for a given tatusaction.
func enforceBucketPolicy(bucket, action, resource, referer, sourceIP string, queryParams url.Values) (s3Error APIErrorCode) {
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

	if globalBucketPolicies == nil {
		return ErrAccessDenied
	}

	// Fetch bucket policy, if policy is not set return access denied.
	policy := globalBucketPolicies.GetBucketPolicy(bucket)
	if policy == nil {
		return ErrAccessDenied
	}

	// Construct resource in 'arn:aws:s3:::examplebucket/object' format.
	arn := bucketARNPrefix + strings.TrimSuffix(strings.TrimPrefix(resource, "/"), "/")

	// Get conditions for policy verification.
	conditionKeyMap := make(map[string]set.StringSet)
	for queryParam := range queryParams {
		conditionKeyMap[queryParam] = set.CreateStringSet(queryParams.Get(queryParam))
	}

	// Add request referer to conditionKeyMap if present.
	if referer != "" {
		conditionKeyMap["referer"] = set.CreateStringSet(referer)
	}
	// Add request source Ip to conditionKeyMap.
	conditionKeyMap["ip"] = set.CreateStringSet(sourceIP)

	// Validate action, resource and conditions with current policy statements.
	if !bucketPolicyEvalStatements(action, arn, conditionKeyMap, policy.Statements) {
		return ErrAccessDenied
	}
	return ErrNone
}

// Check if the action is allowed on the bucket/prefix.
func isBucketActionAllowed(action, bucket, prefix string) bool {
	if globalBucketPolicies == nil {
		return false
	}

	policy := globalBucketPolicies.GetBucketPolicy(bucket)
	if policy == nil {
		return false
	}
	resource := bucketARNPrefix + path.Join(bucket, prefix)
	var conditionKeyMap map[string]set.StringSet
	// Validate action, resource and conditions with current policy statements.
	return bucketPolicyEvalStatements(action, resource, conditionKeyMap, policy.Statements)
}

// GetBucketLocationHandler - GET Bucket location.
// -------------------------
// This operation returns bucket location.
func (api objectAPIHandlers) GetBucketLocationHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	s3Error := checkRequestAuthType(r, bucket, "s3:GetBucketLocation", globalMinioDefaultRegion)
	if s3Error == ErrInvalidRegion {
		// Clients like boto3 send getBucketLocation() call signed with region that is configured.
		s3Error = checkRequestAuthType(r, "", "s3:GetBucketLocation", serverConfig.GetRegion())
	}
	if s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	if _, err := objectAPI.GetBucketInfo(bucket); err != nil {
		errorIf(err, "Unable to fetch bucket info.")
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// Generate response.
	encodedSuccessResponse := encodeResponse(LocationResponse{})
	// Get current region.
	region := serverConfig.GetRegion()
	if region != globalMinioDefaultRegion {
		encodedSuccessResponse = encodeResponse(LocationResponse{
			Location: region,
		})
	}

	// Write success response.
	writeSuccessResponseXML(w, encodedSuccessResponse)
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
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	if s3Error := checkRequestAuthType(r, bucket, "s3:ListBucketMultipartUploads", serverConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	prefix, keyMarker, uploadIDMarker, delimiter, maxUploads, _ := getBucketMultipartResources(r.URL.Query())
	if maxUploads < 0 {
		writeErrorResponse(w, ErrInvalidMaxUploads, r.URL)
		return
	}
	if keyMarker != "" {
		// Marker not common with prefix is not implemented.
		if !hasPrefix(keyMarker, prefix) {
			writeErrorResponse(w, ErrNotImplemented, r.URL)
			return
		}
	}

	listMultipartsInfo, err := objectAPI.ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
	if err != nil {
		errorIf(err, "Unable to list multipart uploads.")
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}
	// generate response
	response := generateListMultipartUploadsResponse(bucket, listMultipartsInfo)
	encodedSuccessResponse := encodeResponse(response)

	// write success response.
	writeSuccessResponseXML(w, encodedSuccessResponse)
}

// ListBucketsHandler - GET Service.
// -----------
// This implementation of the GET operation returns a list of all buckets
// owned by the authenticated sender of the request.
func (api objectAPIHandlers) ListBucketsHandler(w http.ResponseWriter, r *http.Request) {
	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	// ListBuckets does not have any bucket action.
	s3Error := checkRequestAuthType(r, "", "", globalMinioDefaultRegion)
	if s3Error == ErrInvalidRegion {
		// Clients like boto3 send listBuckets() call signed with region that is configured.
		s3Error = checkRequestAuthType(r, "", "", serverConfig.GetRegion())
	}
	if s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}
	// Invoke the list buckets.
	bucketsInfo, err := objectAPI.ListBuckets()
	if err != nil {
		errorIf(err, "Unable to list buckets.")
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// Generate response.
	response := generateListBucketsResponse(bucketsInfo)
	encodedSuccessResponse := encodeResponse(response)

	// Write response.
	writeSuccessResponseXML(w, encodedSuccessResponse)
}

// DeleteMultipleObjectsHandler - deletes multiple objects.
func (api objectAPIHandlers) DeleteMultipleObjectsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	var authError APIErrorCode
	if authError = checkRequestAuthType(r, bucket, "s3:DeleteObject", serverConfig.GetRegion()); authError != ErrNone {
		// In the event access is denied, a 200 response should still be returned
		// http://docs.aws.amazon.com/AmazonS3/latest/API/multiobjectdeleteapi.html
		if authError != ErrAccessDenied {
			writeErrorResponse(w, authError, r.URL)
			return
		}
	}

	// Content-Length is required and should be non-zero
	// http://docs.aws.amazon.com/AmazonS3/latest/API/multiobjectdeleteapi.html
	if r.ContentLength <= 0 {
		writeErrorResponse(w, ErrMissingContentLength, r.URL)
		return
	}

	// Content-Md5 is requied should be set
	// http://docs.aws.amazon.com/AmazonS3/latest/API/multiobjectdeleteapi.html
	if _, ok := r.Header["Content-Md5"]; !ok {
		writeErrorResponse(w, ErrMissingContentMD5, r.URL)
		return
	}

	// Allocate incoming content length bytes.
	deleteXMLBytes := make([]byte, r.ContentLength)

	// Read incoming body XML bytes.
	if _, err := io.ReadFull(r.Body, deleteXMLBytes); err != nil {
		errorIf(err, "Unable to read HTTP body.")
		writeErrorResponse(w, ErrInternalError, r.URL)
		return
	}

	// Unmarshal list of keys to be deleted.
	deleteObjects := &DeleteObjectsRequest{}
	if err := xml.Unmarshal(deleteXMLBytes, deleteObjects); err != nil {
		errorIf(err, "Unable to unmarshal delete objects request XML.")
		writeErrorResponse(w, ErrMalformedXML, r.URL)
		return
	}

	var wg = &sync.WaitGroup{} // Allocate a new wait group.
	var dErrs = make([]error, len(deleteObjects.Objects))

	// Delete all requested objects in parallel.
	for index, object := range deleteObjects.Objects {
		wg.Add(1)
		go func(i int, obj ObjectIdentifier) {
			defer wg.Done()
			// If the request is denied access, each item
			// should be marked as 'AccessDenied'
			if authError == ErrAccessDenied {
				dErrs[i] = PrefixAccessDenied{
					Bucket: bucket,
					Object: obj.ObjectName,
				}
				return
			}
			objectLock := globalNSMutex.NewNSLock(bucket, obj.ObjectName)
			if timedOutErr := objectLock.GetLock(globalObjectTimeout); timedOutErr != nil {
				dErrs[i] = timedOutErr
			} else {
				defer objectLock.Unlock()

				dErr := objectAPI.DeleteObject(bucket, obj.ObjectName)
				if dErr != nil {
					dErrs[i] = dErr
				}
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

	// Write success response.
	writeSuccessResponseXML(w, encodedSuccessResponse)

	// Get host and port from Request.RemoteAddr failing which
	// fill them with empty strings.
	host, port, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host, port = "", ""
	}

	// Notify deleted event for objects.
	for _, dobj := range deletedObjects {
		eventNotify(eventData{
			Type:   ObjectRemovedDelete,
			Bucket: bucket,
			ObjInfo: ObjectInfo{
				Name: dobj.ObjectName,
			},
			ReqParams: extractReqParams(r),
			UserAgent: r.UserAgent(),
			Host:      host,
			Port:      port,
		})
	}
}

// PutBucketHandler - PUT Bucket
// ----------
// This implementation of the PUT operation creates a new bucket for authenticated request
func (api objectAPIHandlers) PutBucketHandler(w http.ResponseWriter, r *http.Request) {
	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	// PutBucket does not have any bucket action.
	s3Error := checkRequestAuthType(r, "", "", globalMinioDefaultRegion)
	if s3Error == ErrInvalidRegion {
		// Clients like boto3 send putBucket() call signed with region that is configured.
		s3Error = checkRequestAuthType(r, "", "", serverConfig.GetRegion())
	}
	if s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	// Parse incoming location constraint.
	location, s3Error := parseLocationConstraint(r)
	if s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	// Validate if location sent by the client is valid, reject
	// requests which do not follow valid region requirements.
	if !isValidLocation(location) {
		writeErrorResponse(w, ErrInvalidRegion, r.URL)
		return
	}

	bucketLock := globalNSMutex.NewNSLock(bucket, "")
	if bucketLock.GetLock(globalObjectTimeout) != nil {
		writeErrorResponse(w, ErrOperationTimedOut, r.URL)
		return
	}
	defer bucketLock.Unlock()

	// Proceed to creating a bucket.
	err := objectAPI.MakeBucketWithLocation(bucket, "")
	if err != nil {
		errorIf(err, "Unable to create a bucket.")
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// Make sure to add Location information here only for bucket
	w.Header().Set("Location", getLocation(r))

	writeSuccessResponseHeadersOnly(w)
}

// PostPolicyBucketHandler - POST policy
// ----------
// This implementation of the POST operation handles object creation with a specified
// signature policy in multipart/form-data
func (api objectAPIHandlers) PostPolicyBucketHandler(w http.ResponseWriter, r *http.Request) {
	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	// Require Content-Length to be set in the request
	size := r.ContentLength
	if size < 0 {
		writeErrorResponse(w, ErrMissingContentLength, r.URL)
		return
	}

	// Here the parameter is the size of the form data that should
	// be loaded in memory, the remaining being put in temporary files.
	reader, err := r.MultipartReader()
	if err != nil {
		errorIf(err, "Unable to initialize multipart reader.")
		writeErrorResponse(w, ErrMalformedPOSTRequest, r.URL)
		return
	}

	// Read multipart data and save in memory and in the disk if needed
	form, err := reader.ReadForm(maxFormMemory)
	if err != nil {
		errorIf(err, "Unable to initialize multipart reader.")
		writeErrorResponse(w, ErrMalformedPOSTRequest, r.URL)
		return
	}

	// Remove all tmp files creating during multipart upload
	defer form.RemoveAll()

	// Extract all form fields
	fileBody, fileName, fileSize, formValues, err := extractPostPolicyFormValues(form)
	if err != nil {
		errorIf(err, "Unable to parse form values.")
		writeErrorResponse(w, ErrMalformedPOSTRequest, r.URL)
		return
	}

	// Check if file is provided, error out otherwise.
	if fileBody == nil {
		writeErrorResponse(w, ErrPOSTFileRequired, r.URL)
		return
	}

	// Close multipart file
	defer fileBody.Close()

	bucket := mux.Vars(r)["bucket"]
	formValues.Set("Bucket", bucket)

	if fileName != "" && strings.Contains(formValues.Get("Key"), "${filename}") {
		// S3 feature to replace ${filename} found in Key form field
		// by the filename attribute passed in multipart
		formValues.Set("Key", strings.Replace(formValues.Get("Key"), "${filename}", fileName, -1))
	}
	object := formValues.Get("Key")

	successRedirect := formValues.Get("success_action_redirect")
	successStatus := formValues.Get("success_action_status")
	var redirectURL *url.URL
	if successRedirect != "" {
		redirectURL, err = url.Parse(successRedirect)
		if err != nil {
			writeErrorResponse(w, ErrMalformedPOSTRequest, r.URL)
			return
		}
	}

	// Verify policy signature.
	apiErr := doesPolicySignatureMatch(formValues)
	if apiErr != ErrNone {
		writeErrorResponse(w, apiErr, r.URL)
		return
	}

	policyBytes, err := base64.StdEncoding.DecodeString(formValues.Get("Policy"))
	if err != nil {
		writeErrorResponse(w, ErrMalformedPOSTRequest, r.URL)
		return
	}

	postPolicyForm, err := parsePostPolicyForm(string(policyBytes))
	if err != nil {
		writeErrorResponse(w, ErrMalformedPOSTRequest, r.URL)
		return
	}

	// Make sure formValues adhere to policy restrictions.
	if apiErr = checkPostPolicy(formValues, postPolicyForm); apiErr != ErrNone {
		writeErrorResponse(w, apiErr, r.URL)
		return
	}

	// Ensure that the object size is within expected range, also the file size
	// should not exceed the maximum single Put size (5 GiB)
	lengthRange := postPolicyForm.Conditions.ContentLengthRange
	if lengthRange.Valid {
		if fileSize < lengthRange.Min {
			errorIf(err, "Unable to create object.")
			writeErrorResponse(w, toAPIErrorCode(errDataTooSmall), r.URL)
			return
		}

		if fileSize > lengthRange.Max || isMaxObjectSize(fileSize) {
			errorIf(err, "Unable to create object.")
			writeErrorResponse(w, toAPIErrorCode(errDataTooLarge), r.URL)
			return
		}
	}

	// Extract metadata to be saved from received Form.
	metadata, err := extractMetadataFromHeader(formValues)
	if err != nil {
		errorIf(err, "found invalid http request header")
		writeErrorResponse(w, ErrInternalError, r.URL)
		return
	}
	sha256sum := ""

	objectLock := globalNSMutex.NewNSLock(bucket, object)
	if objectLock.GetLock(globalObjectTimeout) != nil {
		writeErrorResponse(w, ErrOperationTimedOut, r.URL)
		return
	}
	defer objectLock.Unlock()

	objInfo, err := objectAPI.PutObject(bucket, object, fileSize, fileBody, metadata, sha256sum)
	if err != nil {
		errorIf(err, "Unable to create object.")
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	w.Header().Set("ETag", `"`+objInfo.ETag+`"`)
	w.Header().Set("Location", getObjectLocation(bucket, object))

	// Get host and port from Request.RemoteAddr.
	host, port, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host, port = "", ""
	}

	// Notify object created event.
	defer eventNotify(eventData{
		Type:      ObjectCreatedPost,
		Bucket:    objInfo.Bucket,
		ObjInfo:   objInfo,
		ReqParams: extractReqParams(r),
		UserAgent: r.UserAgent(),
		Host:      host,
		Port:      port,
	})

	if successRedirect != "" {
		// Replace raw query params..
		redirectURL.RawQuery = getRedirectPostRawQuery(objInfo)
		writeRedirectSeeOther(w, redirectURL.String())
		return
	}

	// Decide what http response to send depending on success_action_status parameter
	switch successStatus {
	case "201":
		resp := encodeResponse(PostResponse{
			Bucket:   objInfo.Bucket,
			Key:      objInfo.Name,
			ETag:     `"` + objInfo.ETag + `"`,
			Location: getObjectLocation(objInfo.Bucket, objInfo.Name),
		})
		writeResponse(w, http.StatusCreated, resp, "application/xml")
	case "200":
		writeSuccessResponseHeadersOnly(w)
	default:
		writeSuccessNoContent(w)
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

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponseHeadersOnly(w, ErrServerNotInitialized)
		return
	}

	if s3Error := checkRequestAuthType(r, bucket, "s3:ListBucket", serverConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponseHeadersOnly(w, s3Error)
		return
	}

	bucketLock := globalNSMutex.NewNSLock(bucket, "")
	if bucketLock.GetRLock(globalObjectTimeout) != nil {
		writeErrorResponseHeadersOnly(w, ErrOperationTimedOut)
		return
	}
	defer bucketLock.RUnlock()

	if _, err := objectAPI.GetBucketInfo(bucket); err != nil {
		errorIf(err, "Unable to fetch bucket info.")
		writeErrorResponseHeadersOnly(w, toAPIErrorCode(err))
		return
	}

	writeSuccessResponseHeadersOnly(w)
}

// DeleteBucketHandler - Delete bucket
func (api objectAPIHandlers) DeleteBucketHandler(w http.ResponseWriter, r *http.Request) {
	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	// DeleteBucket does not have any bucket action.
	if s3Error := checkRequestAuthType(r, "", "", serverConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	bucketLock := globalNSMutex.NewNSLock(bucket, "")
	if bucketLock.GetLock(globalObjectTimeout) != nil {
		writeErrorResponse(w, ErrOperationTimedOut, r.URL)
		return
	}
	defer bucketLock.Unlock()

	// Attempt to delete bucket.
	if err := objectAPI.DeleteBucket(bucket); err != nil {
		errorIf(err, "Unable to delete a bucket.")
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// Delete bucket access policy, if present - ignore any errors.
	_ = removeBucketPolicy(bucket, objectAPI)

	// Notify all peers (including self) to update in-memory state
	S3PeersUpdateBucketPolicy(bucket, policyChange{true, nil})

	// Delete notification config, if present - ignore any errors.
	_ = removeNotificationConfig(bucket, objectAPI)

	// Notify all peers (including self) to update in-memory state
	S3PeersUpdateBucketNotification(bucket, nil)

	// Delete listener config, if present - ignore any errors.
	_ = removeListenerConfig(bucket, objectAPI)

	// Notify all peers (including self) to update in-memory state
	S3PeersUpdateBucketListener(bucket, []listenerConfig{})

	// Write success response.
	writeSuccessNoContent(w)
}
