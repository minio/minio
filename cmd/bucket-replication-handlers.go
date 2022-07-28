// Copyright (c) 2015-2022 MinIO, Inc.
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
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/minio/minio/internal/bucket/replication"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/bucket/policy"
)

// PutBucketReplicationConfigHandler - PUT Bucket replication configuration.
// ----------
// Add a replication configuration on the specified bucket as specified in https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketReplication.html
func (api objectAPIHandlers) PutBucketReplicationConfigHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PutBucketReplicationConfig")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}
	if globalIsGateway {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}
	if s3Error := checkRequestAuthType(ctx, r, policy.PutReplicationConfigurationAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}
	// Check if bucket exists.
	if _, err := objectAPI.GetBucketInfo(ctx, bucket, BucketOptions{}); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	if versioned := globalBucketVersioningSys.Enabled(bucket); !versioned {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrReplicationNeedsVersioningError), r.URL)
		return
	}
	replicationConfig, err := replication.ParseConfig(io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		apiErr := errorCodes.ToAPIErr(ErrMalformedXML)
		apiErr.Description = err.Error()
		writeErrorResponse(ctx, w, apiErr, r.URL)
		return
	}
	sameTarget, apiErr := validateReplicationDestination(ctx, bucket, replicationConfig, true)
	if apiErr != noError {
		writeErrorResponse(ctx, w, apiErr, r.URL)
		return
	}
	// Validate the received bucket replication config
	if err = replicationConfig.Validate(bucket, sameTarget); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	configData, err := xml.Marshal(replicationConfig)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	if _, err = globalBucketMetadataSys.Update(ctx, bucket, bucketReplicationConfig, configData); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Write success response.
	writeSuccessResponseHeadersOnly(w)
}

// GetBucketReplicationConfigHandler - GET Bucket replication configuration.
// ----------
// Gets the replication configuration for a bucket.
func (api objectAPIHandlers) GetBucketReplicationConfigHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetBucketReplicationConfig")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	// check if user has permissions to perform this operation
	if s3Error := checkRequestAuthType(ctx, r, policy.GetReplicationConfigurationAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}
	// Check if bucket exists.
	if _, err := objectAPI.GetBucketInfo(ctx, bucket, BucketOptions{}); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	config, _, err := globalBucketMetadataSys.GetReplicationConfig(ctx, bucket)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	configData, err := xml.Marshal(config)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Write success response.
	writeSuccessResponseXML(w, configData)
}

// DeleteBucketReplicationConfigHandler - DELETE Bucket replication config.
// ----------
func (api objectAPIHandlers) DeleteBucketReplicationConfigHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "DeleteBucketReplicationConfig")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.PutReplicationConfigurationAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}
	// Check if bucket exists.
	if _, err := objectAPI.GetBucketInfo(ctx, bucket, BucketOptions{}); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	if globalSiteReplicationSys.isEnabled() {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrReplicationDenyEditError), r.URL)
		return
	}
	if _, err := globalBucketMetadataSys.Update(ctx, bucket, bucketReplicationConfig, nil); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Write success response.
	writeSuccessResponseHeadersOnly(w)
}

// GetBucketReplicationMetricsHandler - GET Bucket replication metrics.
// ----------
// Gets the replication metrics for a bucket.
func (api objectAPIHandlers) GetBucketReplicationMetricsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetBucketReplicationMetrics")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	// check if user has permissions to perform this operation
	if s3Error := checkRequestAuthType(ctx, r, policy.GetReplicationConfigurationAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	// Check if bucket exists.
	if _, err := objectAPI.GetBucketInfo(ctx, bucket, BucketOptions{}); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	if _, _, err := globalBucketMetadataSys.GetReplicationConfig(ctx, bucket); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	var usageInfo BucketUsageInfo
	dataUsageInfo, err := loadDataUsageFromBackend(ctx, objectAPI)
	if err == nil && !dataUsageInfo.LastUpdate.IsZero() {
		usageInfo = dataUsageInfo.BucketsUsage[bucket]
	}

	w.Header().Set(xhttp.ContentType, string(mimeJSON))

	enc := json.NewEncoder(w)
	if err = enc.Encode(getLatestReplicationStats(bucket, usageInfo)); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
}

// ResetBucketReplicationStartHandler - starts a replication reset for all objects in a bucket which
// qualify for replication and re-sync the object(s) to target, provided ExistingObjectReplication is
// enabled for the qualifying rule. This API is a MinIO only extension provided for situations where
// remote target is entirely lost,and previously replicated objects need to be re-synced. If resync is
// already in progress it returns an error
func (api objectAPIHandlers) ResetBucketReplicationStartHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ResetBucketReplicationStart")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	durationStr := r.URL.Query().Get("older-than")
	arn := r.URL.Query().Get("arn")
	resetID := r.URL.Query().Get("reset-id")
	if resetID == "" {
		resetID = mustGetUUID()
	}
	var (
		days time.Duration
		err  error
	)
	if durationStr != "" {
		days, err = time.ParseDuration(durationStr)
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, InvalidArgument{
				Bucket: bucket,
				Err:    fmt.Errorf("invalid query parameter older-than %s for %s : %w", durationStr, bucket, err),
			}), r.URL)
		}
	}
	resetBeforeDate := UTCNow().AddDate(0, 0, -1*int(days/24))

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.ResetBucketReplicationStateAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	// Check if bucket exists.
	if _, err := objectAPI.GetBucketInfo(ctx, bucket, BucketOptions{}); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	config, _, err := globalBucketMetadataSys.GetReplicationConfig(ctx, bucket)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	if !config.HasExistingObjectReplication(arn) {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrReplicationNoExistingObjects), r.URL)
		return
	}

	tgtArns := config.FilterTargetArns(
		replication.ObjectOpts{
			OpType:    replication.ResyncReplicationType,
			TargetArn: arn,
		})

	if len(tgtArns) == 0 {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrBadRequest, InvalidArgument{
			Bucket: bucket,
			Err:    fmt.Errorf("Remote target ARN %s missing or ineligible for replication resync", arn),
		}), r.URL)
		return
	}

	if len(tgtArns) > 1 && arn == "" {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrBadRequest, InvalidArgument{
			Bucket: bucket,
			Err:    fmt.Errorf("ARN should be specified for replication reset"),
		}), r.URL)
		return
	}
	var rinfo ResyncTargetsInfo
	target := globalBucketTargetSys.GetRemoteBucketTargetByArn(ctx, bucket, tgtArns[0])
	target.ResetBeforeDate = UTCNow().AddDate(0, 0, -1*int(days/24))
	target.ResetID = resetID
	rinfo.Targets = append(rinfo.Targets, ResyncTarget{Arn: tgtArns[0], ResetID: target.ResetID})
	if err = globalBucketTargetSys.SetTarget(ctx, bucket, &target, true); err != nil {
		switch err.(type) {
		case BucketRemoteConnectionErr:
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrReplicationRemoteConnectionError, err), r.URL)
		default:
			writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		}
	}
	if err := startReplicationResync(ctx, bucket, arn, resetID, resetBeforeDate, objectAPI); err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrBadRequest, InvalidArgument{
			Bucket: bucket,
			Err:    err,
		}), r.URL)
		return
	}

	data, err := json.Marshal(rinfo)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	// Write success response.
	writeSuccessResponseJSON(w, data)
}

// ResetBucketReplicationStatusHandler - returns the status of replication reset.
// This API is a MinIO only extension
func (api objectAPIHandlers) ResetBucketReplicationStatusHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ResetBucketReplicationStatus")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	arn := r.URL.Query().Get("arn")
	var err error

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.ResetBucketReplicationStateAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	// Check if bucket exists.
	if _, err := objectAPI.GetBucketInfo(ctx, bucket, BucketOptions{}); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	if _, _, err := globalBucketMetadataSys.GetReplicationConfig(ctx, bucket); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	globalReplicationPool.resyncState.RLock()
	brs, ok := globalReplicationPool.resyncState.statusMap[bucket]
	globalReplicationPool.resyncState.RUnlock()
	if !ok {
		brs, err = loadBucketResyncMetadata(ctx, bucket, objectAPI)
		if err != nil {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErrWithErr(ErrBadRequest, InvalidArgument{
				Bucket: bucket,
				Err:    fmt.Errorf("No replication resync status available for %s", arn),
			}), r.URL)
			return
		}
	}

	var rinfo ResyncTargetsInfo
	for tarn, st := range brs.TargetsMap {
		if arn != "" && tarn != arn {
			continue
		}
		rinfo.Targets = append(rinfo.Targets, ResyncTarget{
			Arn:             tarn,
			ResetID:         st.ResyncID,
			StartTime:       st.StartTime,
			EndTime:         st.EndTime,
			ResyncStatus:    st.ResyncStatus.String(),
			ReplicatedSize:  st.ReplicatedSize,
			ReplicatedCount: st.ReplicatedCount,
			FailedSize:      st.FailedSize,
			FailedCount:     st.FailedCount,
			Bucket:          st.Bucket,
			Object:          st.Object,
		})
	}
	data, err := json.Marshal(rinfo)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Write success response.
	writeSuccessResponseJSON(w, data)
}
