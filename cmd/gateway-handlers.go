/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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
	"bytes"
	"io"
	"io/ioutil"
	"net/http"

	"encoding/json"
	"encoding/xml"

	router "github.com/gorilla/mux"
	"github.com/minio/minio-go/pkg/policy"
)

// GetObjectHandler - GET Object
// ----------
// This implementation of the GET operation retrieves object. To use GET,
// you must have READ access to the object.
func (api gatewayAPIHandlers) GetObjectHandler(w http.ResponseWriter, r *http.Request) {
	var object, bucket string
	vars := router.Vars(r)
	bucket = vars["bucket"]
	object = vars["object"]

	// Fetch object stat info.
	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	reqAuthType := getRequestAuthType(r)

	switch reqAuthType {
	case authTypePresignedV2, authTypeSignedV2:
		// Signature V2 validation.
		s3Error := isReqAuthenticatedV2(r)
		if s3Error != ErrNone {
			errorIf(errSignatureMismatch, dumpRequest(r))
			writeErrorResponse(w, s3Error, r.URL)
			return
		}
	case authTypeSigned, authTypePresigned:
		s3Error := isReqAuthenticated(r, serverConfig.GetRegion())
		if s3Error != ErrNone {
			errorIf(errSignatureMismatch, dumpRequest(r))
			writeErrorResponse(w, s3Error, r.URL)
			return
		}
	}

	getObjectInfo := objectAPI.GetObjectInfo
	if reqAuthType == authTypeAnonymous {
		getObjectInfo = objectAPI.AnonGetObjectInfo
	}
	objInfo, err := getObjectInfo(bucket, object)
	if err != nil {
		errorIf(err, "Unable to fetch object info.")
		apiErr := toAPIErrorCode(err)
		if apiErr == ErrNoSuchKey {
			apiErr = errAllowableObjectNotFound(bucket, r)
		}
		writeErrorResponse(w, apiErr, r.URL)
		return
	}

	// Get request range.
	var hrange *httpRange
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		if hrange, err = parseRequestRange(rangeHeader, objInfo.Size); err != nil {
			// Handle only errInvalidRange
			// Ignore other parse error and treat it as regular Get request like Amazon S3.
			if err == errInvalidRange {
				writeErrorResponse(w, ErrInvalidRange, r.URL)
				return
			}

			// log the error.
			errorIf(err, "Invalid request range")
		}
	}

	// Validate pre-conditions if any.
	if checkPreconditions(w, r, objInfo) {
		return
	}

	// Get the object.
	var startOffset int64
	length := objInfo.Size
	if hrange != nil {
		startOffset = hrange.offsetBegin
		length = hrange.getLength()
	}
	// Indicates if any data was written to the http.ResponseWriter
	dataWritten := false
	// io.Writer type which keeps track if any data was written.
	writer := funcToWriter(func(p []byte) (int, error) {
		if !dataWritten {
			// Set headers on the first write.
			// Set standard object headers.
			setObjectHeaders(w, objInfo, hrange)

			// Set any additional requested response headers.
			setGetRespHeaders(w, r.URL.Query())

			dataWritten = true
		}
		return w.Write(p)
	})

	getObject := objectAPI.GetObject
	if reqAuthType == authTypeAnonymous {
		getObject = objectAPI.AnonGetObject
	}

	// Reads the object at startOffset and writes to mw.
	if err := getObject(bucket, object, startOffset, length, writer); err != nil {
		errorIf(err, "Unable to write to client.")
		if !dataWritten {
			// Error response only if no data has been written to client yet. i.e if
			// partial data has already been written before an error
			// occurred then no point in setting StatusCode and
			// sending error XML.
			writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		}
		return
	}
	if !dataWritten {
		// If ObjectAPI.GetObject did not return error and no data has
		// been written it would mean that it is a 0-byte object.
		// call wrter.Write(nil) to set appropriate headers.
		writer.Write(nil)
	}
}

// HeadObjectHandler - HEAD Object
// -----------
// The HEAD operation retrieves metadata from an object without returning the object itself.
func (api gatewayAPIHandlers) HeadObjectHandler(w http.ResponseWriter, r *http.Request) {
	var object, bucket string
	vars := router.Vars(r)
	bucket = vars["bucket"]
	object = vars["object"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponseHeadersOnly(w, ErrServerNotInitialized)
		return
	}

	reqAuthType := getRequestAuthType(r)

	switch reqAuthType {
	case authTypePresignedV2, authTypeSignedV2:
		// Signature V2 validation.
		s3Error := isReqAuthenticatedV2(r)
		if s3Error != ErrNone {
			errorIf(errSignatureMismatch, dumpRequest(r))
			writeErrorResponse(w, s3Error, r.URL)
			return
		}
	case authTypeSigned, authTypePresigned:
		s3Error := isReqAuthenticated(r, serverConfig.GetRegion())
		if s3Error != ErrNone {
			errorIf(errSignatureMismatch, dumpRequest(r))
			writeErrorResponse(w, s3Error, r.URL)
			return
		}
	}

	getObjectInfo := objectAPI.GetObjectInfo
	if reqAuthType == authTypeAnonymous {
		getObjectInfo = objectAPI.AnonGetObjectInfo
	}
	objInfo, err := getObjectInfo(bucket, object)
	if err != nil {
		errorIf(err, "Unable to fetch object info.")
		apiErr := toAPIErrorCode(err)
		if apiErr == ErrNoSuchKey {
			apiErr = errAllowableObjectNotFound(bucket, r)
		}
		writeErrorResponse(w, apiErr, r.URL)
		return
	}

	// Validate pre-conditions if any.
	if checkPreconditions(w, r, objInfo) {
		return
	}

	// Set standard object headers.
	setObjectHeaders(w, objInfo, nil)

	// Successful response.
	w.WriteHeader(http.StatusOK)
}

// DeleteMultipleObjectsHandler - deletes multiple objects.
func (api gatewayAPIHandlers) DeleteMultipleObjectsHandler(w http.ResponseWriter, r *http.Request) {
	vars := router.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	if s3Error := checkRequestAuthType(r, bucket, "s3:DeleteObject", serverConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
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

	var dErrs = make([]error, len(deleteObjects.Objects))

	// Delete all requested objects in parallel.
	for index, object := range deleteObjects.Objects {
		dErr := objectAPI.DeleteObject(bucket, object.ObjectName)
		if dErr != nil {
			dErrs[index] = dErr
		}
	}

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
}

// PutBucketPolicyHandler - PUT Bucket policy
// -----------------
// This implementation of the PUT operation uses the policy
// subresource to add to or replace a policy on a bucket
func (api gatewayAPIHandlers) PutBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	if s3Error := checkRequestAuthType(r, "", "", serverConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	vars := router.Vars(r)
	bucket := vars["bucket"]

	// Before proceeding validate if bucket exists.
	_, err := objAPI.GetBucketInfo(bucket)
	if err != nil {
		errorIf(err, "Unable to find bucket info.")
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// If Content-Length is unknown or zero, deny the
	// request. PutBucketPolicy always needs a Content-Length.
	if r.ContentLength == -1 || r.ContentLength == 0 {
		writeErrorResponse(w, ErrMissingContentLength, r.URL)
		return
	}
	// If Content-Length is greater than maximum allowed policy size.
	if r.ContentLength > maxAccessPolicySize {
		writeErrorResponse(w, ErrEntityTooLarge, r.URL)
		return
	}

	// Read access policy up to maxAccessPolicySize.
	// http://docs.aws.amazon.com/AmazonS3/latest/dev/access-policy-language-overview.html
	// bucket policies are limited to 20KB in size, using a limit reader.
	policyBytes, err := ioutil.ReadAll(io.LimitReader(r.Body, maxAccessPolicySize))
	if err != nil {
		errorIf(err, "Unable to read from client.")
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	{
		// FIXME: consolidate bucketPolicy and policy.BucketAccessPolicy so that
		// the verification below is done on the same type.
		// Parse bucket policy.
		policyInfo := &bucketPolicy{}
		err = parseBucketPolicy(bytes.NewReader(policyBytes), policyInfo)
		if err != nil {
			errorIf(err, "Unable to parse bucket policy.")
			writeErrorResponse(w, ErrInvalidPolicyDocument, r.URL)
			return
		}

		// Parse check bucket policy.
		if s3Error := checkBucketPolicyResources(bucket, policyInfo); s3Error != ErrNone {
			writeErrorResponse(w, toAPIErrorCode(err), r.URL)
			return
		}
	}
	policyInfo := &policy.BucketAccessPolicy{}
	if err = json.Unmarshal(policyBytes, policyInfo); err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}
	var policies []BucketAccessPolicy
	for prefix, policy := range policy.GetPolicies(policyInfo.Statements, bucket) {
		policies = append(policies, BucketAccessPolicy{
			Prefix: prefix,
			Policy: policy,
		})
	}
	if err = objAPI.SetBucketPolicies(bucket, policies); err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}
	// Success.
	writeSuccessNoContent(w)
}

// DeleteBucketPolicyHandler - DELETE Bucket policy
// -----------------
// This implementation of the DELETE operation uses the policy
// subresource to add to remove a policy on a bucket.
func (api gatewayAPIHandlers) DeleteBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	if s3Error := checkRequestAuthType(r, "", "", serverConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	vars := router.Vars(r)
	bucket := vars["bucket"]

	// Before proceeding validate if bucket exists.
	_, err := objAPI.GetBucketInfo(bucket)
	if err != nil {
		errorIf(err, "Unable to find bucket info.")
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// Delete bucket access policy, by passing an empty policy
	// struct.
	objAPI.DeleteBucketPolicies(bucket)
	// Success.
	writeSuccessNoContent(w)
}

// GetBucketPolicyHandler - GET Bucket policy
// -----------------
// This operation uses the policy
// subresource to return the policy of a specified bucket.
func (api gatewayAPIHandlers) GetBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	if s3Error := checkRequestAuthType(r, "", "", serverConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	vars := router.Vars(r)
	bucket := vars["bucket"]

	// Before proceeding validate if bucket exists.
	_, err := objAPI.GetBucketInfo(bucket)
	if err != nil {
		errorIf(err, "Unable to find bucket info.")
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	policies, err := objAPI.GetBucketPolicies(bucket)
	if err != nil {
		errorIf(err, "Unable to read bucket policy.")
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}
	policyInfo := policy.BucketAccessPolicy{Version: "2012-10-17"}
	for _, p := range policies {
		policyInfo.Statements = policy.SetPolicy(policyInfo.Statements, p.Policy, bucket, p.Prefix)
	}
	policyBytes, err := json.Marshal(&policyInfo)
	if err != nil {
		errorIf(err, "Unable to read bucket policy.")
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}
	// Write to client.
	w.Write(policyBytes)
}

// GetBucketNotificationHandler - This implementation of the GET
// operation uses the notification subresource to return the
// notification configuration of a bucket. If notifications are
// not enabled on the bucket, the operation returns an empty
// NotificationConfiguration element.
func (api gatewayAPIHandlers) GetBucketNotificationHandler(w http.ResponseWriter, r *http.Request) {
	writeErrorResponse(w, ErrNotImplemented, r.URL)
}

// PutBucketNotificationHandler - Minio notification feature enables
// you to receive notifications when certain events happen in your bucket.
// Using this API, you can replace an existing notification configuration.
// The configuration is an XML file that defines the event types that you
// want Minio to publish and the destination where you want Minio to publish
// an event notification when it detects an event of the specified type.
// By default, your bucket has no event notifications configured. That is,
// the notification configuration will be an empty NotificationConfiguration.
func (api gatewayAPIHandlers) PutBucketNotificationHandler(w http.ResponseWriter, r *http.Request) {
	writeErrorResponse(w, ErrNotImplemented, r.URL)
}

// ListenBucketNotificationHandler - list bucket notifications.
func (api gatewayAPIHandlers) ListenBucketNotificationHandler(w http.ResponseWriter, r *http.Request) {
	writeErrorResponse(w, ErrNotImplemented, r.URL)
}

// DeleteBucketHandler - Delete bucket
func (api gatewayAPIHandlers) DeleteBucketHandler(w http.ResponseWriter, r *http.Request) {
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

	vars := router.Vars(r)
	bucket := vars["bucket"]

	// Attempt to delete bucket.
	if err := objectAPI.DeleteBucket(bucket); err != nil {
		errorIf(err, "Unable to delete a bucket.")
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// Write success response.
	writeSuccessNoContent(w)
}

// ListObjectsV1Handler - GET Bucket (List Objects) Version 1.
// --------------------------
// This implementation of the GET operation returns some or all (up to 1000)
// of the objects in a bucket. You can use the request parameters as selection
// criteria to return a subset of the objects in a bucket.
//
func (api gatewayAPIHandlers) ListObjectsV1Handler(w http.ResponseWriter, r *http.Request) {
	vars := router.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	reqAuthType := getRequestAuthType(r)

	switch reqAuthType {
	case authTypePresignedV2, authTypeSignedV2:
		// Signature V2 validation.
		s3Error := isReqAuthenticatedV2(r)
		if s3Error != ErrNone {
			errorIf(errSignatureMismatch, dumpRequest(r))
			writeErrorResponse(w, s3Error, r.URL)
			return
		}
	case authTypeSigned, authTypePresigned:
		s3Error := isReqAuthenticated(r, serverConfig.GetRegion())
		if s3Error != ErrNone {
			errorIf(errSignatureMismatch, dumpRequest(r))
			writeErrorResponse(w, s3Error, r.URL)
			return
		}
	}

	// Extract all the litsObjectsV1 query params to their native values.
	prefix, marker, delimiter, maxKeys, _ := getListObjectsV1Args(r.URL.Query())

	// Validate all the query params before beginning to serve the request.
	if s3Error := validateListObjectsArgs(prefix, marker, delimiter, maxKeys); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	listObjects := objectAPI.ListObjects
	if reqAuthType == authTypeAnonymous {
		listObjects = objectAPI.AnonListObjects
	}
	// Inititate a list objects operation based on the input params.
	// On success would return back ListObjectsInfo object to be
	// marshalled into S3 compatible XML header.
	listObjectsInfo, err := listObjects(bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		errorIf(err, "Unable to list objects.")
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}
	response := generateListObjectsV1Response(bucket, prefix, marker, delimiter, maxKeys, listObjectsInfo)

	// Write success response.
	writeSuccessResponseXML(w, encodeResponse(response))
}

// HeadBucketHandler - HEAD Bucket
// ----------
// This operation is useful to determine if a bucket exists.
// The operation returns a 200 OK if the bucket exists and you
// have permission to access it. Otherwise, the operation might
// return responses such as 404 Not Found and 403 Forbidden.
func (api gatewayAPIHandlers) HeadBucketHandler(w http.ResponseWriter, r *http.Request) {
	vars := router.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponseHeadersOnly(w, ErrServerNotInitialized)
		return
	}

	reqAuthType := getRequestAuthType(r)

	switch reqAuthType {
	case authTypePresignedV2, authTypeSignedV2:
		// Signature V2 validation.
		s3Error := isReqAuthenticatedV2(r)
		if s3Error != ErrNone {
			errorIf(errSignatureMismatch, dumpRequest(r))
			writeErrorResponse(w, s3Error, r.URL)
			return
		}
	case authTypeSigned, authTypePresigned:
		s3Error := isReqAuthenticated(r, serverConfig.GetRegion())
		if s3Error != ErrNone {
			errorIf(errSignatureMismatch, dumpRequest(r))
			writeErrorResponse(w, s3Error, r.URL)
			return
		}
	}

	getBucketInfo := objectAPI.GetBucketInfo
	if reqAuthType == authTypeAnonymous {
		getBucketInfo = objectAPI.AnonGetBucketInfo
	}

	if _, err := getBucketInfo(bucket); err != nil {
		errorIf(err, "Unable to fetch bucket info.")
		writeErrorResponseHeadersOnly(w, toAPIErrorCode(err))
		return
	}

	writeSuccessResponseHeadersOnly(w)
}

// GetBucketLocationHandler - GET Bucket location.
// -------------------------
// This operation returns bucket location.
func (api gatewayAPIHandlers) GetBucketLocationHandler(w http.ResponseWriter, r *http.Request) {
	vars := router.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}
	reqAuthType := getRequestAuthType(r)

	switch reqAuthType {
	case authTypePresignedV2, authTypeSignedV2:
		// Signature V2 validation.
		s3Error := isReqAuthenticatedV2(r)
		if s3Error != ErrNone {
			errorIf(errSignatureMismatch, dumpRequest(r))
			writeErrorResponse(w, s3Error, r.URL)
			return
		}
	case authTypeSigned, authTypePresigned:
		s3Error := isReqAuthenticated(r, globalMinioDefaultRegion)
		if s3Error == ErrInvalidRegion {
			// Clients like boto3 send getBucketLocation() call signed with region that is configured.
			s3Error = isReqAuthenticated(r, serverConfig.GetRegion())
		}
		if s3Error != ErrNone {
			errorIf(errSignatureMismatch, dumpRequest(r))
			writeErrorResponse(w, s3Error, r.URL)
			return
		}
	}

	getBucketInfo := objectAPI.GetBucketInfo
	if reqAuthType == authTypeAnonymous {
		getBucketInfo = objectAPI.AnonGetBucketInfo
	}

	if _, err := getBucketInfo(bucket); err != nil {
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
