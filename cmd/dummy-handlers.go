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
	"encoding/xml"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/minio/minio/pkg/bucket/object/tagging"
	"github.com/minio/minio/pkg/bucket/policy"
)

// Data types used for returning dummy tagging XML.
// These variables shouldn't be used elsewhere.
// They are only defined to be used in this file alone.

// GetBucketWebsite  - GET bucket website, a dummy api
func (api objectAPIHandlers) GetBucketWebsiteHandler(w http.ResponseWriter, r *http.Request) {
	writeSuccessResponseHeadersOnly(w)
	w.(http.Flusher).Flush()
}

// GetBucketAccelerate  - GET bucket accelerate, a dummy api
func (api objectAPIHandlers) GetBucketAccelerateHandler(w http.ResponseWriter, r *http.Request) {
	writeSuccessResponseHeadersOnly(w)
	w.(http.Flusher).Flush()
}

// GetBucketRequestPaymentHandler - GET bucket requestPayment, a dummy api
func (api objectAPIHandlers) GetBucketRequestPaymentHandler(w http.ResponseWriter, r *http.Request) {
	writeSuccessResponseHeadersOnly(w)
	w.(http.Flusher).Flush()
}

// GetBucketLoggingHandler - GET bucket logging, a dummy api
func (api objectAPIHandlers) GetBucketLoggingHandler(w http.ResponseWriter, r *http.Request) {
	writeSuccessResponseHeadersOnly(w)
	w.(http.Flusher).Flush()
}

// GetBucketReplicationHandler - GET bucket replication, a dummy api
func (api objectAPIHandlers) GetBucketReplicationHandler(w http.ResponseWriter, r *http.Request) {
	writeSuccessResponseHeadersOnly(w)
	w.(http.Flusher).Flush()
}

// DeleteBucketTaggingHandler - DELETE bucket tagging, a dummy api
func (api objectAPIHandlers) DeleteBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	writeSuccessResponseHeadersOnly(w)
	w.(http.Flusher).Flush()
}

// DeleteBucketWebsiteHandler - DELETE bucket website, a dummy api
func (api objectAPIHandlers) DeleteBucketWebsiteHandler(w http.ResponseWriter, r *http.Request) {
	writeSuccessResponseHeadersOnly(w)
	w.(http.Flusher).Flush()
}

type allowedMethod string

// Define strings
const (
	GET    allowedMethod = http.MethodGet
	PUT    allowedMethod = http.MethodPut
	HEAD   allowedMethod = http.MethodHead
	POST   allowedMethod = http.MethodPost
	DELETE allowedMethod = http.MethodDelete
)

// GetBucketCorsHandler - GET bucket cors, a dummy api
func (api objectAPIHandlers) GetBucketCorsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetBucketCorsHandler")

	type corsRule struct {
		AllowedHeaders []string        `xml:"AllowedHeaders"`
		AllowedMethods []allowedMethod `xml:"AllowedMethod"`
		AllowedOrigins []string        `xml:"AllowedOrigin"`
		ExposeHeaders  []string        `xml:"ExposeHeader"`
		MaxAgeSeconds  int64           `xml:"MaxAgeSeconds"`
	}

	type corsConfiguration struct {
		XMLName  xml.Name   `xml:"CORSConfiguration"`
		CorsRule []corsRule `xml:"CORSRule"`
	}

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

	cors := &corsConfiguration{}
	if err := xml.NewEncoder(w).Encode(cors); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	w.(http.Flusher).Flush()
}

// GetBucketTaggingHandler - GET bucket tagging, a dummy api
func (api objectAPIHandlers) GetBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetBucketTagging")

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	// Allow getBucketTagging if policy action is set, since this is a dummy call
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

	tags := &tagging.Tagging{}
	tags.TagSet.Tags = append(tags.TagSet.Tags, tagging.Tag{})

	if err := xml.NewEncoder(w).Encode(tags); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	w.(http.Flusher).Flush()
}
