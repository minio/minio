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
	var policies []bucketAccessPolicy
	for prefix, policy := range policy.GetPolicies(policyInfo.Statements, bucket) {
		policies = append(policies, bucketAccessPolicy{
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
