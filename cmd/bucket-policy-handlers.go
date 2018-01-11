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
	"encoding/json"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"runtime"
	"strings"

	humanize "github.com/dustin/go-humanize"
	mux "github.com/gorilla/mux"
	"github.com/minio/minio-go/pkg/policy"
	"github.com/minio/minio/pkg/wildcard"
)

// maximum supported access policy size.
const maxAccessPolicySize = 20 * humanize.KiByte

// Verify if a given action is valid for the url path based on the
// existing bucket access policy.
func bucketPolicyEvalStatements(action string, resource string, conditions policy.ConditionKeyMap,
	statements []policy.Statement) bool {
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
func bucketPolicyMatchStatement(action string, resource string, conditions policy.ConditionKeyMap,
	statement policy.Statement) bool {
	// Verify if action, resource and condition match in given statement.
	return (bucketPolicyActionMatch(action, statement) &&
		bucketPolicyResourceMatch(resource, statement) &&
		bucketPolicyConditionMatch(conditions, statement))
}

// Verify if given action matches with policy statement.
func bucketPolicyActionMatch(action string, statement policy.Statement) bool {
	return !statement.Actions.FuncMatch(actionMatch, action).IsEmpty()
}

// Match function matches wild cards in 'pattern' for resource.
func resourceMatch(pattern, resource string) bool {
	if runtime.GOOS == "windows" {
		// For windows specifically make sure we are case insensitive.
		return wildcard.Match(strings.ToLower(pattern), strings.ToLower(resource))
	}
	return wildcard.Match(pattern, resource)
}

// Match function matches wild cards in 'pattern' for action.
func actionMatch(pattern, action string) bool {
	return wildcard.MatchSimple(pattern, action)
}

func refererMatch(pattern, referer string) bool {
	return wildcard.MatchSimple(pattern, referer)
}

// isIPInCIDR - checks if a given a IP address is a member of the given subnet.
func isIPInCIDR(cidr, ip string) bool {
	// AWS S3 spec says IPs must use standard CIDR notation.
	// http://docs.aws.amazon.com/AmazonS3/latest/dev/example-bucket-policies.html#example-bucket-policies-use-case-3.
	_, cidrNet, err := net.ParseCIDR(cidr)
	if err != nil {
		return false // If provided CIDR can't be parsed no IP will be in the subnet.
	}
	addr := net.ParseIP(ip)
	return cidrNet.Contains(addr)
}

// Verify if given resource matches with policy statement.
func bucketPolicyResourceMatch(resource string, statement policy.Statement) bool {
	// the resource rule for object could contain "*" wild card.
	// the requested object can be given access based on the already set bucket policy if
	// the match is successful.
	// More info: http://docs.aws.amazon.com/AmazonS3/latest/dev/s3-arn-format.html.
	return !statement.Resources.FuncMatch(resourceMatch, resource).IsEmpty()
}

// Verify if given condition matches with policy statement.
func bucketPolicyConditionMatch(conditions policy.ConditionKeyMap, statement policy.Statement) bool {
	// Supports following conditions.
	// - StringEquals
	// - StringNotEquals
	// - StringLike
	// - StringNotLike
	// - IpAddress
	// - NotIpAddress
	//
	// Supported applicable condition keys for each conditions.
	// - s3:prefix
	// - s3:max-keys
	// - s3:aws-Referer
	// - s3:aws-SourceIp

	// The following loop evaluates the logical AND of all the
	// conditions in the statement. Note: we can break out of the
	// loop if and only if a condition evaluates to false.
	for condition, conditionKeyVal := range statement.Conditions {
		prefixConditon := conditionKeyVal["s3:prefix"]
		maxKeyCondition := conditionKeyVal["s3:max-keys"]
		if condition == "StringEquals" {
			// If there is no condition with "s3:prefix" or "s3:max-keys" condition key
			// then there is nothing to check condition against.
			if !prefixConditon.IsEmpty() && !prefixConditon.Equals(conditions["prefix"]) {
				return false
			}
			if !maxKeyCondition.IsEmpty() && !maxKeyCondition.Equals(conditions["max-keys"]) {
				return false
			}
		} else if condition == "StringNotEquals" {
			// If there is no condition with "s3:prefix" or "s3:max-keys" condition key
			// then there is nothing to check condition against.
			if !prefixConditon.IsEmpty() && prefixConditon.Equals(conditions["prefix"]) {
				return false
			}
			if !maxKeyCondition.IsEmpty() && maxKeyCondition.Equals(conditions["max-keys"]) {
				return false
			}
		} else if condition == "StringLike" {
			awsReferers := conditionKeyVal["aws:Referer"]
			// Skip empty condition, it is trivially satisfied.
			if awsReferers.IsEmpty() {
				continue
			}
			// wildcard match of referer in statement was not empty.
			// StringLike has a match, i.e, condition evaluates to true.
			refererFound := false
			for referer := range conditions["referer"] {
				if !awsReferers.FuncMatch(refererMatch, referer).IsEmpty() {
					refererFound = true
					break
				}
			}
			// No matching referer found, so the condition is false.
			if !refererFound {
				return false
			}
		} else if condition == "StringNotLike" {
			awsReferers := conditionKeyVal["aws:Referer"]
			// Skip empty condition, it is trivially satisfied.
			if awsReferers.IsEmpty() {
				continue
			}
			// wildcard match of referer in statement was not empty.
			// StringNotLike has a match, i.e, condition evaluates to false.
			for referer := range conditions["referer"] {
				if !awsReferers.FuncMatch(refererMatch, referer).IsEmpty() {
					return false
				}
			}
		} else if condition == "IpAddress" {
			awsIps := conditionKeyVal["aws:SourceIp"]
			// Skip empty condition, it is trivially satisfied.
			if awsIps.IsEmpty() {
				continue
			}
			// wildcard match of ip if statement was not empty.
			// Find a valid ip.
			ipFound := false
			for ip := range conditions["ip"] {
				if !awsIps.FuncMatch(isIPInCIDR, ip).IsEmpty() {
					ipFound = true
					break
				}
			}
			if !ipFound {
				return false
			}
		} else if condition == "NotIpAddress" {
			awsIps := conditionKeyVal["aws:SourceIp"]
			// Skip empty condition, it is trivially satisfied.
			if awsIps.IsEmpty() {
				continue
			}
			// wildcard match of ip if statement was not empty.
			// Find if nothing matches.
			for ip := range conditions["ip"] {
				if !awsIps.FuncMatch(isIPInCIDR, ip).IsEmpty() {
					return false
				}
			}
		}
	}

	return true
}

// PutBucketPolicyHandler - PUT Bucket policy
// -----------------
// This implementation of the PUT operation uses the policy
// subresource to add to or replace a policy on a bucket
func (api objectAPIHandlers) PutBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	if s3Error := checkRequestAuthType(r, "", "", globalServerConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	// Before proceeding validate if bucket exists.
	_, err := objAPI.GetBucketInfo(bucket)
	if err != nil {
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

	// Parse validate and save bucket policy.
	if s3Error := parseAndPersistBucketPolicy(bucket, policyBytes, objAPI); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	// Success.
	writeSuccessNoContent(w)
}

// DeleteBucketPolicyHandler - DELETE Bucket policy
// -----------------
// This implementation of the DELETE operation uses the policy
// subresource to add to remove a policy on a bucket.
func (api objectAPIHandlers) DeleteBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	if s3Error := checkRequestAuthType(r, "", "", globalServerConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	// Before proceeding validate if bucket exists.
	_, err := objAPI.GetBucketInfo(bucket)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// Delete bucket access policy, by passing an empty policy
	// struct.
	err = persistAndNotifyBucketPolicyChange(bucket, policyChange{
		true, policy.BucketAccessPolicy{},
	}, objAPI)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
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
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	if s3Error := checkRequestAuthType(r, "", "", globalServerConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	// Before proceeding validate if bucket exists.
	_, err := objAPI.GetBucketInfo(bucket)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// Read bucket access policy.
	policy, err := readBucketPolicy(bucket, objAPI)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	policyBytes, err := json.Marshal(&policy)
	if err != nil {
		errorIf(err, "Unable to marshal bucket policy.")
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// Write to client.
	w.Write(policyBytes)
}
