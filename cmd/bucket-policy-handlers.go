/*
 * Minio Cloud Storage, (C) 2015, 2016, 2017, 2018 Minio, Inc.
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
	"net/http"

	humanize "github.com/dustin/go-humanize"
	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/policy"
)

const (
	// As per AWS S3 specification, 20KiB policy JSON data is allowed.
	maxBucketPolicySize = 20 * humanize.KiByte

	// Policy configuration file.
	bucketPolicyConfig = "policy.json"
)

// PutBucketPolicyHandler - This HTTP handler stores given bucket policy configuration as per
// https://docs.aws.amazon.com/AmazonS3/latest/dev/access-policy-language-overview.html
func (api objectAPIHandlers) PutBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PutBucketPolicy")

	defer logger.AuditLog(w, r, "PutBucketPolicy", mustGetClaimsFromToken(r))

	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL, guessIsBrowserReq(r))
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	if s3Error := checkRequestAuthType(ctx, r, policy.PutBucketPolicyAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}

	// Check if bucket exists.
	if _, err := objAPI.GetBucketInfo(ctx, bucket); err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// Error out if Content-Length is missing.
	// PutBucketPolicy always needs Content-Length.
	if r.ContentLength <= 0 {
		writeErrorResponse(w, ErrMissingContentLength, r.URL, guessIsBrowserReq(r))
		return
	}

	// Error out if Content-Length is beyond allowed size.
	if r.ContentLength > maxBucketPolicySize {
		writeErrorResponse(w, ErrEntityTooLarge, r.URL, guessIsBrowserReq(r))
		return
	}

	bucketPolicy, err := policy.ParseConfig(io.LimitReader(r.Body, r.ContentLength), bucket)
	if err != nil {
		writeErrorResponse(w, ErrMalformedPolicy, r.URL, guessIsBrowserReq(r))
		return
	}

	// Version in policy must not be empty
	if bucketPolicy.Version == "" {
		writeErrorResponse(w, ErrMalformedPolicy, r.URL, guessIsBrowserReq(r))
		return
	}

	if err = objAPI.SetBucketPolicy(ctx, bucket, bucketPolicy); err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	globalPolicySys.Set(bucket, *bucketPolicy)
	globalNotificationSys.SetBucketPolicy(ctx, bucket, bucketPolicy)

	// Success.
	writeSuccessNoContent(w)
}

// DeleteBucketPolicyHandler - This HTTP handler removes bucket policy configuration.
func (api objectAPIHandlers) DeleteBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "DeleteBucketPolicy")

	defer logger.AuditLog(w, r, "DeleteBucketPolicy", mustGetClaimsFromToken(r))

	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL, guessIsBrowserReq(r))
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	if s3Error := checkRequestAuthType(ctx, r, policy.DeleteBucketPolicyAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}

	// Check if bucket exists.
	if _, err := objAPI.GetBucketInfo(ctx, bucket); err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	if err := objAPI.DeleteBucketPolicy(ctx, bucket); err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	globalPolicySys.Remove(bucket)
	globalNotificationSys.RemoveBucketPolicy(ctx, bucket)

	// Success.
	writeSuccessNoContent(w)
}

// GetBucketPolicyHandler - This HTTP handler returns bucket policy configuration.
func (api objectAPIHandlers) GetBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetBucketPolicy")

	defer logger.AuditLog(w, r, "GetBucketPolicy", mustGetClaimsFromToken(r))

	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL, guessIsBrowserReq(r))
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	if s3Error := checkRequestAuthType(ctx, r, policy.GetBucketPolicyAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}

	// Check if bucket exists.
	if _, err := objAPI.GetBucketInfo(ctx, bucket); err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// Read bucket access policy.
	bucketPolicy, err := objAPI.GetBucketPolicy(ctx, bucket)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	policyData, err := json.Marshal(bucketPolicy)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// Write to client.
	w.Write(policyData)
}
