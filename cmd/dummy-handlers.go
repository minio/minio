/*
 * MinIO Cloud Storage, (C) 2018, 2019 MinIO, Inc.
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
	"net/http"

	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/bucket/policy"
)

// Data types used for returning dummy tagging XML.
// These variables shouldn't be used elsewhere.
// They are only defined to be used in this file alone.

// GetBucketWebsite  - GET bucket website, a dummy api
func (api objectAPIHandlers) GetBucketWebsiteHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetBucketWebsite")

	defer logger.AuditLog(w, r, "GetBucketWebsite", mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	// Allow getBucketCors if policy action is set, since this is a dummy call
	// we are simply re-purposing the bucketPolicyAction.
	if s3Error := checkRequestAuthType(ctx, r, policy.GetBucketPolicyAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	// Validate if bucket exists, before proceeding further...
	_, err := objAPI.GetBucketInfo(ctx, bucket)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrNoSuchWebsiteConfiguration), r.URL, guessIsBrowserReq(r))
}

// GetBucketAccelerate  - GET bucket accelerate, a dummy api
func (api objectAPIHandlers) GetBucketAccelerateHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetBucketAccelerate")

	defer logger.AuditLog(w, r, "GetBucketAccelerate", mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	// Allow getBucketCors if policy action is set, since this is a dummy call
	// we are simply re-purposing the bucketPolicyAction.
	if s3Error := checkRequestAuthType(ctx, r, policy.GetBucketPolicyAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	// Validate if bucket exists, before proceeding further...
	_, err := objAPI.GetBucketInfo(ctx, bucket)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	const accelerateDefaultConfig = `<?xml version="1.0" encoding="UTF-8"?><AccelerateConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>`
	writeSuccessResponseXML(w, []byte(accelerateDefaultConfig))
}

// GetBucketRequestPaymentHandler - GET bucket requestPayment, a dummy api
func (api objectAPIHandlers) GetBucketRequestPaymentHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetBucketRequestPayment")

	defer logger.AuditLog(w, r, "GetBucketRequestPayment", mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	// Allow getBucketCors if policy action is set, since this is a dummy call
	// we are simply re-purposing the bucketPolicyAction.
	if s3Error := checkRequestAuthType(ctx, r, policy.GetBucketPolicyAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	// Validate if bucket exists, before proceeding further...
	_, err := objAPI.GetBucketInfo(ctx, bucket)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	const requestPaymentDefaultConfig = `<?xml version="1.0" encoding="UTF-8"?><RequestPaymentConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Payer>BucketOwner</Payer></RequestPaymentConfiguration>`

	writeSuccessResponseXML(w, []byte(requestPaymentDefaultConfig))
}

// GetBucketLoggingHandler - GET bucket logging, a dummy api
func (api objectAPIHandlers) GetBucketLoggingHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetBucketLogging")

	defer logger.AuditLog(w, r, "GetBucketLogging", mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	// Allow getBucketCors if policy action is set, since this is a dummy call
	// we are simply re-purposing the bucketPolicyAction.
	if s3Error := checkRequestAuthType(ctx, r, policy.GetBucketPolicyAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	// Validate if bucket exists, before proceeding further...
	_, err := objAPI.GetBucketInfo(ctx, bucket)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	const loggingDefaultConfig = `<?xml version="1.0" encoding="UTF-8"?><BucketLoggingStatus xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><!--<LoggingEnabled><TargetBucket>myLogsBucket</TargetBucket><TargetPrefix>add/this/prefix/to/my/log/files/access_log-</TargetPrefix></LoggingEnabled>--></BucketLoggingStatus>`
	writeSuccessResponseXML(w, []byte(loggingDefaultConfig))
}

// GetBucketReplicationHandler - GET bucket replication, a dummy api
func (api objectAPIHandlers) GetBucketReplicationHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetBucketReplication")

	defer logger.AuditLog(w, r, "GetBucketReplication", mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	// Allow getBucketCors if policy action is set, since this is a dummy call
	// we are simply re-purposing the bucketPolicyAction.
	if s3Error := checkRequestAuthType(ctx, r, policy.GetBucketPolicyAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	// Validate if bucket exists, before proceeding further...
	_, err := objAPI.GetBucketInfo(ctx, bucket)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrReplicationConfigurationNotFoundError), r.URL, guessIsBrowserReq(r))
}

// DeleteBucketWebsiteHandler - DELETE bucket website, a dummy api
func (api objectAPIHandlers) DeleteBucketWebsiteHandler(w http.ResponseWriter, r *http.Request) {
	writeSuccessResponseHeadersOnly(w)
	w.(http.Flusher).Flush()
}

// GetBucketCorsHandler - GET bucket cors, a dummy api
func (api objectAPIHandlers) GetBucketCorsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetBucketCors")

	defer logger.AuditLog(w, r, "GetBucketCors", mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	// Allow getBucketCors if policy action is set, since this is a dummy call
	// we are simply re-purposing the bucketPolicyAction.
	if s3Error := checkRequestAuthType(ctx, r, policy.GetBucketPolicyAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	// Validate if bucket exists, before proceeding further...
	_, err := objAPI.GetBucketInfo(ctx, bucket)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrNoSuchCORSConfiguration), r.URL, guessIsBrowserReq(r))
}
