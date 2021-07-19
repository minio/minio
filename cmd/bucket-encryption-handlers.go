// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/bucket/policy"
)

const (
	// Bucket Encryption configuration file name.
	bucketSSEConfig = "bucket-encryption.xml"
)

// PutBucketEncryptionHandler - Stores given bucket encryption configuration
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketEncryption.html
func (api objectAPIHandlers) PutBucketEncryptionHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PutBucketEncryption")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	if !objAPI.IsEncryptionSupported() {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	if s3Error := checkRequestAuthType(ctx, r, policy.PutBucketEncryptionAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	// Check if bucket exists.
	if _, err := objAPI.GetBucketInfo(ctx, bucket); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
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
		writeErrorResponse(ctx, w, apiErr, r.URL)
		return
	}

	// Return error if KMS is not initialized
	if GlobalKMS == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrKMSNotConfigured), r.URL)
		return
	}

	configData, err := xml.Marshal(encConfig)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Store the bucket encryption configuration in the object layer
	if err = globalBucketMetadataSys.Update(bucket, bucketSSEConfig, configData); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	writeSuccessResponseHeadersOnly(w)
}

// GetBucketEncryptionHandler - Returns bucket policy configuration
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketEncryption.html
func (api objectAPIHandlers) GetBucketEncryptionHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetBucketEncryption")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	if s3Error := checkRequestAuthType(ctx, r, policy.GetBucketEncryptionAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	// Check if bucket exists
	var err error
	if _, err = objAPI.GetBucketInfo(ctx, bucket); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	config, err := globalBucketMetadataSys.GetSSEConfig(bucket)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	configData, err := xml.Marshal(config)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Write bucket encryption configuration to client
	writeSuccessResponseXML(w, configData)
}

// DeleteBucketEncryptionHandler - Removes bucket encryption configuration
func (api objectAPIHandlers) DeleteBucketEncryptionHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "DeleteBucketEncryption")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	if s3Error := checkRequestAuthType(ctx, r, policy.PutBucketEncryptionAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	// Check if bucket exists
	var err error
	if _, err = objAPI.GetBucketInfo(ctx, bucket); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Delete bucket encryption config from object layer
	if err = globalBucketMetadataSys.Update(bucket, bucketSSEConfig, nil); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	writeSuccessNoContent(w)
}
