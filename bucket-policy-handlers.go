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
	"crypto/sha256"
	"encoding/hex"
	"io"
	"io/ioutil"
	"net/http"
	"regexp"
	"strings"

	mux "github.com/gorilla/mux"
	"github.com/minio/minio/pkg/fs"
	"github.com/minio/minio/pkg/probe"
	"github.com/minio/minio/pkg/s3/access"
)

// maximum supported access policy size.
const maxAccessPolicySize = 20 * 1024 * 1024 // 20KiB.

// Verify if a given action is valid for the url path based on the
// existing bucket access policy.
func bucketPolicyEvalStatements(action string, resource string, conditions map[string]string, statements []accesspolicy.Statement) bool {
	for _, statement := range statements {
		if bucketPolicyMatchStatement(action, resource, conditions, statement) {
			if statement.Effect == "Allow" {
				return true
			}
			// else statement.Effect == "Deny"
			return false
		}
	}
	// None match so deny.
	return false
}

// Verify if action, resource and conditions match input policy statement.
func bucketPolicyMatchStatement(action string, resource string, conditions map[string]string, statement accesspolicy.Statement) bool {
	// Verify if action matches.
	if bucketPolicyActionMatch(action, statement) {
		// Verify if resource matches.
		if bucketPolicyResourceMatch(resource, statement) {
			// Verify if condition matches.
			if bucketPolicyConditionMatch(conditions, statement) {
				return true
			}
		}
	}
	return false
}

// Verify if given action matches with policy statement.
func bucketPolicyActionMatch(action string, statement accesspolicy.Statement) bool {
	for _, policyAction := range statement.Actions {
		// Policy action can be a regex, validate the action with matching string.
		matched, e := regexp.MatchString(policyAction, action)
		fatalIf(probe.NewError(e), "Invalid pattern, please verify the pattern string.", nil)
		if matched {
			return true
		}
	}
	return false
}

// Verify if given resource matches with policy statement.
func bucketPolicyResourceMatch(resource string, statement accesspolicy.Statement) bool {
	for _, presource := range statement.Resources {
		matched, e := regexp.MatchString(presource, strings.TrimPrefix(resource, "/"))
		fatalIf(probe.NewError(e), "Invalid pattern, please verify the pattern string.", nil)
		// For any path matches, we return quickly and the let the caller continue.
		if matched {
			return true
		}
	}
	return false
}

// Verify if given condition matches with policy statement.
func bucketPolicyConditionMatch(conditions map[string]string, statement accesspolicy.Statement) bool {
	// Supports following conditions.
	// - StringEquals
	// - StringNotEquals
	//
	// Supported applicable condition keys for each conditions.
	// - s3:prefix
	// - s3:max-keys
	var conditionMatches = true
	for condition, conditionKeys := range statement.Conditions {
		if condition == "StringEquals" {
			if conditionKeys["s3:prefix"] != conditions["prefix"] {
				conditionMatches = false
				break
			}
			if conditionKeys["s3:max-keys"] != conditions["max-keys"] {
				conditionMatches = false
				break
			}
		} else if condition == "StringNotEquals" {
			if conditionKeys["s3:prefix"] == conditions["prefix"] {
				conditionMatches = false
				break
			}
			if conditionKeys["s3:max-keys"] == conditions["max-keys"] {
				conditionMatches = false
				break
			}
		}
	}
	return conditionMatches
}

// PutBucketPolicyHandler - PUT Bucket policy
// -----------------
// This implementation of the PUT operation uses the policy
// subresource to add to or replace a policy on a bucket
func (api storageAPI) PutBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	// If Content-Length is unknown or zero, deny the
	// request. PutBucketPolicy always needs a Content-Length if
	// incoming request is not chunked.
	if !contains(r.TransferEncoding, "chunked") {
		if r.ContentLength == -1 || r.ContentLength == 0 {
			writeErrorResponse(w, r, ErrMissingContentLength, r.URL.Path)
			return
		}
		// If Content-Length is greater than maximum allowed policy size.
		if r.ContentLength > maxAccessPolicySize {
			writeErrorResponse(w, r, ErrEntityTooLarge, r.URL.Path)
			return
		}
	}

	// Read access policy up to maxAccessPolicySize.
	// http://docs.aws.amazon.com/AmazonS3/latest/dev/access-policy-language-overview.html
	// bucket policies are limited to 20KB in size, using a limit reader.
	accessPolicyBytes, e := ioutil.ReadAll(io.LimitReader(r.Body, maxAccessPolicySize))
	if e != nil {
		errorIf(probe.NewError(e).Trace(bucket), "Reading policy failed.", nil)
		writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		return
	}

	// Parse access access.
	accessPolicy, e := accesspolicy.Validate(accessPolicyBytes)
	if e != nil {
		writeErrorResponse(w, r, ErrInvalidPolicyDocument, r.URL.Path)
		return
	}

	// If the policy resource has different bucket name, reject it.
	for _, statement := range accessPolicy.Statements {
		for _, resource := range statement.Resources {
			resourcePrefix := strings.SplitAfter(resource, accesspolicy.AWSResourcePrefix)[1]
			if !strings.HasPrefix(resourcePrefix, bucket) {
				writeErrorResponse(w, r, ErrMalformedPolicy, r.URL.Path)
				return
			}
		}
	}

	// Set http request for signature verification.
	auth := api.Signature.SetHTTPRequestToVerify(r)
	if isRequestPresignedSignatureV4(r) {
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
	} else if isRequestSignatureV4(r) {
		sh := sha256.New()
		sh.Write(accessPolicyBytes)
		ok, err := api.Signature.DoesSignatureMatch(hex.EncodeToString(sh.Sum(nil)))
		if err != nil {
			errorIf(err.Trace(string(accessPolicyBytes)), "SaveBucketPolicy failed.", nil)
			writeErrorResponse(w, r, ErrSignatureDoesNotMatch, r.URL.Path)
			return
		}
		if !ok {
			writeErrorResponse(w, r, ErrSignatureDoesNotMatch, r.URL.Path)
			return
		}
	}

	// Save bucket policy.
	err := writeBucketPolicy(bucket, accessPolicyBytes)
	if err != nil {
		errorIf(err.Trace(bucket), "SaveBucketPolicy failed.", nil)
		switch err.ToGoError().(type) {
		case fs.BucketNameInvalid:
			writeErrorResponse(w, r, ErrInvalidBucketName, r.URL.Path)
		default:
			writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		}
		return
	}
	writeSuccessNoContent(w)
}

// DeleteBucketPolicyHandler - DELETE Bucket policy
// -----------------
// This implementation of the DELETE operation uses the policy
// subresource to add to remove a policy on a bucket.
func (api storageAPI) DeleteBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	// Validate incoming signature.
	if s3Error := isReqAuthenticated(api.Signature, r); s3Error != ErrNone {
		writeErrorResponse(w, r, s3Error, r.URL.Path)
		return
	}

	// Delete bucket access policy.
	err := removeBucketPolicy(bucket)
	if err != nil {
		errorIf(err.Trace(bucket), "DeleteBucketPolicy failed.", nil)
		switch err.ToGoError().(type) {
		case fs.BucketNameInvalid:
			writeErrorResponse(w, r, ErrInvalidBucketName, r.URL.Path)
		case fs.BucketPolicyNotFound:
			writeErrorResponse(w, r, ErrNoSuchBucketPolicy, r.URL.Path)
		default:
			writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		}
		return
	}
	writeSuccessNoContent(w)
}

// GetBucketPolicyHandler - GET Bucket policy
// -----------------
// This operation uses the policy
// subresource to return the policy of a specified bucket.
func (api storageAPI) GetBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	// Validate incoming signature.
	if s3Error := isReqAuthenticated(api.Signature, r); s3Error != ErrNone {
		writeErrorResponse(w, r, s3Error, r.URL.Path)
		return
	}

	// Read bucket access policy.
	p, err := readBucketPolicy(bucket)
	if err != nil {
		errorIf(err.Trace(bucket), "GetBucketPolicy failed.", nil)
		switch err.ToGoError().(type) {
		case fs.BucketNameInvalid:
			writeErrorResponse(w, r, ErrInvalidBucketName, r.URL.Path)
		case fs.BucketPolicyNotFound:
			writeErrorResponse(w, r, ErrNoSuchBucketPolicy, r.URL.Path)
		default:
			writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		}
		return
	}
	io.Copy(w, bytes.NewReader(p))
}
