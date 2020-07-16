/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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
	"fmt"
	"io"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/bucket/policy"
)

const (
	// Bucket Encryption configuration file name.
	bucketSSEConfig = "bucket-encryption.xml"
)

// PutBucketEncryptionHandler - Stores given bucket encryption configuration
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketEncryption.html
func (api objectAPIHandlers) PutBucketEncryptionHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PutBucketEncryption")

	defer logger.AuditLog(w, r, "PutBucketEncryption", mustGetClaimsFromToken(r))

	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	if !objAPI.IsEncryptionSupported() {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL, guessIsBrowserReq(r))
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	if s3Error := checkRequestAuthType(ctx, r, policy.PutBucketEncryptionAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	// Check if bucket exists.
	if _, err := objAPI.GetBucketInfo(ctx, bucket); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// Parse bucket encryption xml
	encConfig, err := validateBucketSSEConfig(io.LimitReader(r.Body, maxBucketSSEConfigSize))
	if err != nil {
		apiErr := APIError{
			Code:           "MalformedXML",
			Description:    fmt.Sprintf("%s (%s)", errorCodes[ErrMalformedXML].Description, err),
			HTTPStatusCode: errorCodes[ErrMalformedXML].HTTPStatusCode,
		}
		writeErrorResponse(ctx, w, apiErr, r.URL, guessIsBrowserReq(r))
		return
	}

	// Return error if KMS is not initialized
	if GlobalKMS == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrKMSNotConfigured), r.URL, guessIsBrowserReq(r))
		return
	}

	configData, err := xml.Marshal(encConfig)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// Store the bucket encryption configuration in the object layer
	if err = globalBucketMetadataSys.Update(bucket, bucketSSEConfig, configData); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	writeSuccessResponseHeadersOnly(w)
}

// GetBucketEncryptionHandler - Returns bucket policy configuration
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketEncryption.html
func (api objectAPIHandlers) GetBucketEncryptionHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetBucketEncryption")

	defer logger.AuditLog(w, r, "GetBucketEncryption", mustGetClaimsFromToken(r))

	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	if s3Error := checkRequestAuthType(ctx, r, policy.GetBucketEncryptionAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	// Check if bucket exists
	var err error
	if _, err = objAPI.GetBucketInfo(ctx, bucket); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	config, err := globalBucketMetadataSys.GetSSEConfig(bucket)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	configData, err := xml.Marshal(config)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// Write bucket encryption configuration to client
	writeSuccessResponseXML(w, configData)
}

// DeleteBucketEncryptionHandler - Removes bucket encryption configuration
func (api objectAPIHandlers) DeleteBucketEncryptionHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "DeleteBucketEncryption")

	defer logger.AuditLog(w, r, "DeleteBucketEncryption", mustGetClaimsFromToken(r))

	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	if s3Error := checkRequestAuthType(ctx, r, policy.PutBucketEncryptionAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	// Check if bucket exists
	var err error
	if _, err = objAPI.GetBucketInfo(ctx, bucket); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// Delete bucket encryption config from object layer
	if err = globalBucketMetadataSys.Update(bucket, bucketSSEConfig, nil); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	writeSuccessNoContent(w)
}
