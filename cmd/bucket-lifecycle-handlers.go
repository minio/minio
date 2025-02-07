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
	"net/http"
	"strconv"
	"time"

	"github.com/minio/minio/internal/bucket/lifecycle"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/mux"
	"github.com/minio/pkg/v3/policy"
)

const (
	// Lifecycle configuration file.
	bucketLifecycleConfig = "lifecycle.xml"
)

// PutBucketLifecycleHandler - This HTTP handler stores given bucket lifecycle configuration as per
// https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lifecycle-mgmt.html
func (api objectAPIHandlers) PutBucketLifecycleHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PutBucketLifecycle")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	// PutBucketLifecycle always needs a Content-Md5
	if !validateLengthAndChecksum(r) {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMissingContentMD5), r.URL)
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.PutBucketLifecycleAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	// Check if bucket exists.
	rcfg, err := globalBucketObjectLockSys.Get(bucket)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	bucketLifecycle, err := lifecycle.ParseLifecycleConfigWithID(r.Body)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Validate the received bucket policy document
	if err = bucketLifecycle.Validate(rcfg); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Validate the transition storage ARNs
	if err = validateTransitionTier(bucketLifecycle); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Create a map of updated set of rules in request
	updatedRules := make(map[string]lifecycle.Rule, len(bucketLifecycle.Rules))
	for _, rule := range bucketLifecycle.Rules {
		updatedRules[rule.ID] = rule
	}

	// Get list of rules for the bucket from disk
	meta, err := globalBucketMetadataSys.GetConfigFromDisk(ctx, bucket)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	expiryRuleRemoved := false
	if len(meta.LifecycleConfigXML) > 0 {
		var lcCfg lifecycle.Lifecycle
		if err := xml.Unmarshal(meta.LifecycleConfigXML, &lcCfg); err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
		for _, rl := range lcCfg.Rules {
			updRule, ok := updatedRules[rl.ID]
			// original rule had expiry that is no longer in the new config,
			// or rule is present but missing expiration flags
			if (!rl.Expiration.IsNull() || !rl.NoncurrentVersionExpiration.IsNull()) &&
				(!ok || (updRule.Expiration.IsNull() && updRule.NoncurrentVersionExpiration.IsNull())) {
				expiryRuleRemoved = true
			}
		}
	}

	if bucketLifecycle.HasExpiry() || expiryRuleRemoved {
		currtime := time.Now()
		bucketLifecycle.ExpiryUpdatedAt = &currtime
	}

	configData, err := xml.Marshal(bucketLifecycle)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	if _, err = globalBucketMetadataSys.Update(ctx, bucket, bucketLifecycleConfig, configData); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Success.
	writeSuccessResponseHeadersOnly(w)
}

// GetBucketLifecycleHandler - This HTTP handler returns bucket policy configuration.
func (api objectAPIHandlers) GetBucketLifecycleHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetBucketLifecycle")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	var withUpdatedAt bool
	if updatedAtStr := r.Form.Get("withUpdatedAt"); updatedAtStr != "" {
		var err error
		withUpdatedAt, err = strconv.ParseBool(updatedAtStr)
		if err != nil {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidLifecycleQueryParameter), r.URL)
			return
		}
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.GetBucketLifecycleAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	// Check if bucket exists.
	if _, err := objAPI.GetBucketInfo(ctx, bucket, BucketOptions{}); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	config, updatedAt, err := globalBucketMetadataSys.GetLifecycleConfig(bucket)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	// explicitly set ExpiryUpdatedAt nil as its meant for internal consumption only
	config.ExpiryUpdatedAt = nil

	configData, err := xml.Marshal(config)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	if withUpdatedAt {
		w.Header().Set(xhttp.MinIOLifecycleCfgUpdatedAt, updatedAt.Format(iso8601Format))
	}
	// Write lifecycle configuration to client.
	writeSuccessResponseXML(w, configData)
}

// DeleteBucketLifecycleHandler - This HTTP handler removes bucket lifecycle configuration.
func (api objectAPIHandlers) DeleteBucketLifecycleHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "DeleteBucketLifecycle")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	if s3Error := checkRequestAuthType(ctx, r, policy.PutBucketLifecycleAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	// Check if bucket exists.
	if _, err := objAPI.GetBucketInfo(ctx, bucket, BucketOptions{}); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	if _, err := globalBucketMetadataSys.Delete(ctx, bucket, bucketLifecycleConfig); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Success.
	writeSuccessNoContent(w)
}
