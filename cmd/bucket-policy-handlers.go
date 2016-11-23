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
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	humanize "github.com/dustin/go-humanize"
	mux "github.com/gorilla/mux"
	"github.com/minio/minio-go/pkg/set"
	"github.com/minio/minio/pkg/wildcard"
)

// maximum supported access policy size.
const maxAccessPolicySize = 20 * humanize.KiByte

// Verify if a given action is valid for the url path based on the
// existing bucket access policy.
func bucketPolicyEvalStatements(action string, resource string, conditions map[string]set.StringSet, statements []policyStatement) bool {
	for _, statement := range statements {
		if bucketPolicyMatchStatement(action, resource, conditions, statement) {
			if statement.Effect == "Allow" {
				return true
			}
			// Do not uncomment kept here for readability.
			// else statement.Effect == "Deny"
			return false
		}
	}
	// None match so deny.
	return false
}

// Verify if action, resource and conditions match input policy statement.
func bucketPolicyMatchStatement(action string, resource string, conditions map[string]set.StringSet, statement policyStatement) bool {
	// Verify if action, resource and condition match in given statement.
	return (bucketPolicyActionMatch(action, statement) &&
		bucketPolicyResourceMatch(resource, statement) &&
		bucketPolicyConditionMatch(conditions, statement))
}

// Verify if given action matches with policy statement.
func bucketPolicyActionMatch(action string, statement policyStatement) bool {
	return !statement.Actions.FuncMatch(actionMatch, action).IsEmpty()
}

// Match function matches wild cards in 'pattern' for resource.
func resourceMatch(pattern, resource string) bool {
	return wildcard.Match(pattern, resource)
}

// Match function matches wild cards in 'pattern' for action.
func actionMatch(pattern, action string) bool {
	return wildcard.MatchSimple(pattern, action)
}

// Verify if given resource matches with policy statement.
func bucketPolicyResourceMatch(resource string, statement policyStatement) bool {
	// the resource rule for object could contain "*" wild card.
	// the requested object can be given access based on the already set bucket policy if
	// the match is successful.
	// More info: http://docs.aws.amazon.com/AmazonS3/latest/dev/s3-arn-format.html.
	return !statement.Resources.FuncMatch(resourceMatch, resource).IsEmpty()
}

// Verify if given condition matches with policy statement.
func bucketPolicyConditionMatch(conditions map[string]set.StringSet, statement policyStatement) bool {
	// Supports following conditions.
	// - StringEquals
	// - StringNotEquals
	//
	// Supported applicable condition keys for each conditions.
	// - s3:prefix
	// - s3:max-keys
	var conditionMatches = true
	for condition, conditionKeyVal := range statement.Conditions {
		if condition == "StringEquals" {
			if !conditionKeyVal["s3:prefix"].Equals(conditions["prefix"]) {
				conditionMatches = false
				break
			}
			if !conditionKeyVal["s3:max-keys"].Equals(conditions["max-keys"]) {
				conditionMatches = false
				break
			}
		} else if condition == "StringNotEquals" {
			if !conditionKeyVal["s3:prefix"].Equals(conditions["prefix"]) {
				conditionMatches = false
				break
			}
			if !conditionKeyVal["s3:max-keys"].Equals(conditions["max-keys"]) {
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
func (api objectAPIHandlers) PutBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(w, r, ErrServerNotInitialized, r.URL.Path)
		return
	}

	if s3Error := checkRequestAuthType(r, "", "", serverConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponse(w, r, s3Error, r.URL.Path)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	// Before proceeding validate if bucket exists.
	_, err := objAPI.GetBucketInfo(bucket)
	if err != nil {
		errorIf(err, "Unable to find bucket info.")
		writeErrorResponse(w, r, toAPIErrorCode(err), r.URL.Path)
		return
	}

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
	policyBytes, err := ioutil.ReadAll(io.LimitReader(r.Body, maxAccessPolicySize))
	if err != nil {
		errorIf(err, "Unable to read from client.")
		writeErrorResponse(w, r, toAPIErrorCode(err), r.URL.Path)
		return
	}
	// Parse bucket policy.
	var policy = &bucketPolicy{}
	err = parseBucketPolicy(bytes.NewReader(policyBytes), policy)
	if err != nil {
		errorIf(err, "Unable to parse bucket policy.")
		writeErrorResponse(w, r, ErrInvalidPolicyDocument, r.URL.Path)
		return
	}

	// Parse check bucket policy.
	if s3Error := checkBucketPolicyResources(bucket, policy); s3Error != ErrNone {
		writeErrorResponse(w, r, s3Error, r.URL.Path)
		return
	}

	// Save bucket policy.
	if err = persistAndNotifyBucketPolicyChange(bucket, policyChange{false, policy}, objAPI); err != nil {
		switch err.(type) {
		case BucketNameInvalid:
			writeErrorResponse(w, r, ErrInvalidBucketName, r.URL.Path)
		default:
			writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		}
		return
	}

	// Success.
	writeSuccessNoContent(w)
}

// persistAndNotifyBucketPolicyChange - takes a policyChange argument,
// persists it to storage, and notify nodes in the cluster about the
// change. In-memory state is updated in response to the notification.
func persistAndNotifyBucketPolicyChange(bucket string, pCh policyChange, objAPI ObjectLayer) error {
	// Acquire a write lock on bucket before modifying its
	// configuration.
	bucketLock := nsMutex.NewNSLock(bucket, "")
	bucketLock.Lock()
	// Release lock after notifying peers
	defer bucketLock.Unlock()

	if pCh.IsRemove {
		if err := removeBucketPolicy(bucket, objAPI); err != nil {
			return err
		}
	} else {
		if pCh.BktPolicy == nil {
			return errInvalidArgument
		}
		if err := writeBucketPolicy(bucket, objAPI, pCh.BktPolicy); err != nil {
			return err
		}
	}

	// Notify all peers (including self) to update in-memory state
	S3PeersUpdateBucketPolicy(bucket, pCh)
	return nil
}

// DeleteBucketPolicyHandler - DELETE Bucket policy
// -----------------
// This implementation of the DELETE operation uses the policy
// subresource to add to remove a policy on a bucket.
func (api objectAPIHandlers) DeleteBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(w, r, ErrServerNotInitialized, r.URL.Path)
		return
	}

	if s3Error := checkRequestAuthType(r, "", "", serverConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponse(w, r, s3Error, r.URL.Path)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	// Before proceeding validate if bucket exists.
	_, err := objAPI.GetBucketInfo(bucket)
	if err != nil {
		errorIf(err, "Unable to find bucket info.")
		writeErrorResponse(w, r, toAPIErrorCode(err), r.URL.Path)
		return
	}

	// Delete bucket access policy, by passing an empty policy
	// struct.
	if err := persistAndNotifyBucketPolicyChange(bucket, policyChange{true, nil}, objAPI); err != nil {
		switch err.(type) {
		case BucketPolicyNotFound:
			writeErrorResponse(w, r, ErrNoSuchBucketPolicy, r.URL.Path)
		default:
			writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		}
		return
	}

	// Success.
	writeSuccessNoContent(w)
}

// GetBucketPolicyHandler - GET Bucket policy
// -----------------
// This operation uses the policy
// subresource to return the policy of a specified bucket.
func (api objectAPIHandlers) GetBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(w, r, ErrServerNotInitialized, r.URL.Path)
		return
	}

	if s3Error := checkRequestAuthType(r, "", "", serverConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponse(w, r, s3Error, r.URL.Path)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	// Before proceeding validate if bucket exists.
	_, err := objAPI.GetBucketInfo(bucket)
	if err != nil {
		errorIf(err, "Unable to find bucket info.")
		writeErrorResponse(w, r, toAPIErrorCode(err), r.URL.Path)
		return
	}

	// Read bucket access policy.
	policy, err := readBucketPolicy(bucket, objAPI)
	if err != nil {
		errorIf(err, "Unable to read bucket policy.")
		switch err.(type) {
		case BucketPolicyNotFound:
			writeErrorResponse(w, r, ErrNoSuchBucketPolicy, r.URL.Path)
		default:
			writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
		}
		return
	}

	// Write to client.
	fmt.Fprint(w, policy)
}
