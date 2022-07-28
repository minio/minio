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
	"bytes"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	jsoniter "github.com/json-iterator/go"
	"github.com/klauspost/compress/zip"
	"github.com/minio/kes"
	"github.com/minio/madmin-go"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/minio/minio/internal/bucket/lifecycle"
	objectlock "github.com/minio/minio/internal/bucket/object/lock"
	"github.com/minio/minio/internal/bucket/versioning"
	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/kms"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/bucket/policy"
	iampolicy "github.com/minio/pkg/iam/policy"
)

const (
	bucketQuotaConfigFile = "quota.json"
	bucketTargetsFile     = "bucket-targets.json"
)

// PutBucketQuotaConfigHandler - PUT Bucket quota configuration.
// ----------
// Places a quota configuration on the specified bucket. The quota
// specified in the quota configuration will be applied by default
// to enforce total quota for the specified bucket.
func (a adminAPIHandlers) PutBucketQuotaConfigHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PutBucketQuotaConfig")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.SetBucketQuotaAdminAction)
	if objectAPI == nil {
		return
	}

	vars := mux.Vars(r)
	bucket := pathClean(vars["bucket"])

	if _, err := objectAPI.GetBucketInfo(ctx, bucket, BucketOptions{}); err != nil {
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
		return
	}

	quotaConfig, err := parseBucketQuota(bucket, data)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	if quotaConfig.Type == "fifo" {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
		return
	}

	updatedAt, err := globalBucketMetadataSys.Update(ctx, bucket, bucketQuotaConfigFile, data)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	bucketMeta := madmin.SRBucketMeta{
		Type:      madmin.SRBucketMetaTypeQuotaConfig,
		Bucket:    bucket,
		Quota:     data,
		UpdatedAt: updatedAt,
	}
	if quotaConfig.Quota == 0 {
		bucketMeta.Quota = nil
	}

	// Call site replication hook.
	if err = globalSiteReplicationSys.BucketMetaHook(ctx, bucketMeta); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Write success response.
	writeSuccessResponseHeadersOnly(w)
}

// GetBucketQuotaConfigHandler - gets bucket quota configuration
func (a adminAPIHandlers) GetBucketQuotaConfigHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetBucketQuotaConfig")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.GetBucketQuotaAdminAction)
	if objectAPI == nil {
		return
	}

	vars := mux.Vars(r)
	bucket := pathClean(vars["bucket"])

	if _, err := objectAPI.GetBucketInfo(ctx, bucket, BucketOptions{}); err != nil {
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	config, _, err := globalBucketMetadataSys.GetQuotaConfig(ctx, bucket)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	configData, err := json.Marshal(config)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Write success response.
	writeSuccessResponseJSON(w, configData)
}

// SetRemoteTargetHandler - sets a remote target for bucket
func (a adminAPIHandlers) SetRemoteTargetHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SetBucketTarget")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))
	vars := mux.Vars(r)
	bucket := pathClean(vars["bucket"])
	update := r.Form.Get("update") == "true"

	if globalIsGateway {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	// Get current object layer instance.
	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.SetBucketTargetAction)
	if objectAPI == nil {
		return
	}

	// Check if bucket exists.
	if _, err := objectAPI.GetBucketInfo(ctx, bucket, BucketOptions{}); err != nil {
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	cred, _, _, s3Err := validateAdminSignature(ctx, r, "")
	if s3Err != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
		return
	}
	password := cred.SecretKey

	reqBytes, err := madmin.DecryptData(password, io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrAdminConfigBadJSON, err), r.URL)
		return
	}
	var target madmin.BucketTarget
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	if err = json.Unmarshal(reqBytes, &target); err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrAdminConfigBadJSON, err), r.URL)
		return
	}
	sameTarget, _ := isLocalHost(target.URL().Hostname(), target.URL().Port(), globalMinioPort)
	if sameTarget && bucket == target.TargetBucket {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrBucketRemoteIdenticalToSource), r.URL)
		return
	}

	target.SourceBucket = bucket
	var ops []madmin.TargetUpdateType
	if update {
		ops = madmin.GetTargetUpdateOps(r.Form)
	} else {
		target.Arn = globalBucketTargetSys.getRemoteARN(bucket, &target)
	}
	if target.Arn == "" {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrAdminConfigBadJSON, err), r.URL)
		return
	}
	if update {
		// overlay the updates on existing target
		tgt := globalBucketTargetSys.GetRemoteBucketTargetByArn(ctx, bucket, target.Arn)
		if tgt.Empty() {
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrRemoteTargetNotFoundError, err), r.URL)
			return
		}
		for _, op := range ops {
			switch op {
			case madmin.CredentialsUpdateType:
				tgt.Credentials = target.Credentials
				tgt.TargetBucket = target.TargetBucket
				tgt.Secure = target.Secure
				tgt.Endpoint = target.Endpoint
			case madmin.SyncUpdateType:
				tgt.ReplicationSync = target.ReplicationSync
			case madmin.ProxyUpdateType:
				tgt.DisableProxy = target.DisableProxy
			case madmin.PathUpdateType:
				tgt.Path = target.Path
			case madmin.BandwidthLimitUpdateType:
				tgt.BandwidthLimit = target.BandwidthLimit
			case madmin.HealthCheckDurationUpdateType:
				tgt.HealthCheckDuration = target.HealthCheckDuration
			}
		}
		target = tgt
	}

	// enforce minimum bandwidth limit as 100MBps
	if target.BandwidthLimit > 0 && target.BandwidthLimit < 100*1000*1000 {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrReplicationBandwidthLimitError, err), r.URL)
		return
	}
	if err = globalBucketTargetSys.SetTarget(ctx, bucket, &target, update); err != nil {
		switch err.(type) {
		case BucketRemoteConnectionErr:
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrReplicationRemoteConnectionError, err), r.URL)
		default:
			writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		}
		return
	}
	targets, err := globalBucketTargetSys.ListBucketTargets(ctx, bucket)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	tgtBytes, err := json.Marshal(&targets)
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrAdminConfigBadJSON, err), r.URL)
		return
	}
	if _, err = globalBucketMetadataSys.Update(ctx, bucket, bucketTargetsFile, tgtBytes); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	data, err := json.Marshal(target.Arn)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	// Write success response.
	writeSuccessResponseJSON(w, data)
}

// ListRemoteTargetsHandler - lists remote target(s) for a bucket or gets a target
// for a particular ARN type
func (a adminAPIHandlers) ListRemoteTargetsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListBucketTargets")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))
	vars := mux.Vars(r)
	bucket := pathClean(vars["bucket"])
	arnType := vars["type"]

	if globalIsGateway {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}
	// Get current object layer instance.
	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.GetBucketTargetAction)
	if objectAPI == nil {
		return
	}
	if bucket != "" {
		// Check if bucket exists.
		if _, err := objectAPI.GetBucketInfo(ctx, bucket, BucketOptions{}); err != nil {
			writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
		if _, err := globalBucketMetadataSys.GetBucketTargetsConfig(bucket); err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
	}
	targets := globalBucketTargetSys.ListTargets(ctx, bucket, arnType)
	data, err := json.Marshal(targets)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	// Write success response.
	writeSuccessResponseJSON(w, data)
}

// RemoveRemoteTargetHandler - removes a remote target for bucket with specified ARN
func (a adminAPIHandlers) RemoveRemoteTargetHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "RemoveBucketTarget")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))
	vars := mux.Vars(r)
	bucket := pathClean(vars["bucket"])
	arn := vars["arn"]

	if globalIsGateway {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}
	// Get current object layer instance.
	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.SetBucketTargetAction)
	if objectAPI == nil {
		return
	}

	// Check if bucket exists.
	if _, err := objectAPI.GetBucketInfo(ctx, bucket, BucketOptions{}); err != nil {
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	if err := globalBucketTargetSys.RemoveTarget(ctx, bucket, arn); err != nil {
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	targets, err := globalBucketTargetSys.ListBucketTargets(ctx, bucket)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	tgtBytes, err := json.Marshal(&targets)
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrAdminConfigBadJSON, err), r.URL)
		return
	}
	if _, err = globalBucketMetadataSys.Update(ctx, bucket, bucketTargetsFile, tgtBytes); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Write success response.
	writeSuccessNoContent(w)
}

// ExportBucketMetadataHandler - exports all bucket metadata as a zipped file
func (a adminAPIHandlers) ExportBucketMetadataHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ExportBucketMetadata")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	bucket := pathClean(r.Form.Get("bucket"))
	if !globalIsErasure {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}
	// Get current object layer instance.
	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.ExportBucketMetadataAction)
	if objectAPI == nil {
		return
	}

	var (
		buckets []BucketInfo
		err     error
	)
	if bucket != "" {
		// Check if bucket exists.
		if _, err := objectAPI.GetBucketInfo(ctx, bucket, BucketOptions{}); err != nil {
			writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
		buckets = append(buckets, BucketInfo{Name: bucket})
	} else {
		buckets, err = objectAPI.ListBuckets(ctx, BucketOptions{})
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
	}

	// Initialize a zip writer which will provide a zipped content
	// of bucket metadata
	zipWriter := zip.NewWriter(w)
	defer zipWriter.Close()
	rawDataFn := func(r io.Reader, filename string, sz int) error {
		header, zerr := zip.FileInfoHeader(dummyFileInfo{
			name:    filename,
			size:    int64(sz),
			mode:    0o600,
			modTime: time.Now(),
			isDir:   false,
			sys:     nil,
		})
		if zerr != nil {
			logger.LogIf(ctx, zerr)
			return nil
		}
		header.Method = zip.Deflate
		zwriter, zerr := zipWriter.CreateHeader(header)
		if zerr != nil {
			logger.LogIf(ctx, zerr)
			return nil
		}
		if _, err := io.Copy(zwriter, r); err != nil {
			logger.LogIf(ctx, err)
		}
		return nil
	}

	cfgFiles := []string{
		bucketPolicyConfig,
		bucketNotificationConfig,
		bucketLifecycleConfig,
		bucketSSEConfig,
		bucketTaggingConfig,
		bucketQuotaConfigFile,
		objectLockConfig,
		bucketVersioningConfig,
		bucketReplicationConfig,
		bucketTargetsFile,
	}
	for _, bi := range buckets {
		for _, cfgFile := range cfgFiles {
			cfgPath := pathJoin(bi.Name, cfgFile)
			bucket := bi.Name
			switch cfgFile {
			case bucketNotificationConfig:
				config, err := globalBucketMetadataSys.GetNotificationConfig(bucket)
				if err != nil {
					logger.LogIf(ctx, err)
					writeErrorResponse(ctx, w, exportError(ctx, err, cfgFile, bucket), r.URL)
					return
				}
				configData, err := xml.Marshal(config)
				if err != nil {
					writeErrorResponse(ctx, w, exportError(ctx, err, cfgFile, bucket), r.URL)
					return
				}
				if err = rawDataFn(bytes.NewReader(configData), cfgPath, len(configData)); err != nil {
					writeErrorResponse(ctx, w, exportError(ctx, err, cfgFile, bucket), r.URL)
					return
				}
			case bucketLifecycleConfig:
				config, err := globalBucketMetadataSys.GetLifecycleConfig(bucket)
				if err != nil {
					if errors.Is(err, BucketLifecycleNotFound{Bucket: bucket}) {
						continue
					}
					logger.LogIf(ctx, err)
					writeErrorResponse(ctx, w, exportError(ctx, err, cfgFile, bucket), r.URL)
					return
				}
				configData, err := xml.Marshal(config)
				if err != nil {
					writeErrorResponse(ctx, w, exportError(ctx, err, cfgFile, bucket), r.URL)
					return
				}
				if err = rawDataFn(bytes.NewReader(configData), cfgPath, len(configData)); err != nil {
					writeErrorResponse(ctx, w, exportError(ctx, err, cfgFile, bucket), r.URL)
					return
				}
			case bucketQuotaConfigFile:
				config, _, err := globalBucketMetadataSys.GetQuotaConfig(ctx, bucket)
				if err != nil {
					if errors.Is(err, BucketQuotaConfigNotFound{Bucket: bucket}) {
						continue
					}
					writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
					return
				}
				configData, err := json.Marshal(config)
				if err != nil {
					writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
					return
				}
				if err = rawDataFn(bytes.NewReader(configData), cfgPath, len(configData)); err != nil {
					writeErrorResponse(ctx, w, exportError(ctx, err, cfgFile, bucket), r.URL)
					return
				}
			case bucketSSEConfig:
				config, _, err := globalBucketMetadataSys.GetSSEConfig(bucket)
				if err != nil {
					if errors.Is(err, BucketSSEConfigNotFound{Bucket: bucket}) {
						continue
					}
					writeErrorResponse(ctx, w, exportError(ctx, err, cfgFile, bucket), r.URL)
					return
				}
				configData, err := xml.Marshal(config)
				if err != nil {
					writeErrorResponse(ctx, w, exportError(ctx, err, cfgFile, bucket), r.URL)
					return
				}
				if err = rawDataFn(bytes.NewReader(configData), cfgPath, len(configData)); err != nil {
					writeErrorResponse(ctx, w, exportError(ctx, err, cfgFile, bucket), r.URL)
					return
				}
			case bucketTaggingConfig:
				config, _, err := globalBucketMetadataSys.GetTaggingConfig(bucket)
				if err != nil {
					if errors.Is(err, BucketTaggingNotFound{Bucket: bucket}) {
						continue
					}
					writeErrorResponse(ctx, w, exportError(ctx, err, cfgFile, bucket), r.URL)
					return
				}
				configData, err := xml.Marshal(config)
				if err != nil {
					writeErrorResponse(ctx, w, exportError(ctx, err, cfgFile, bucket), r.URL)
					return
				}
				if err = rawDataFn(bytes.NewReader(configData), cfgPath, len(configData)); err != nil {
					writeErrorResponse(ctx, w, exportError(ctx, err, cfgFile, bucket), r.URL)
					return
				}
			case objectLockConfig:
				config, _, err := globalBucketMetadataSys.GetObjectLockConfig(bucket)
				if err != nil {
					if errors.Is(err, BucketObjectLockConfigNotFound{Bucket: bucket}) {
						continue
					}
					writeErrorResponse(ctx, w, exportError(ctx, err, cfgFile, bucket), r.URL)
					return
				}

				configData, err := xml.Marshal(config)
				if err != nil {
					writeErrorResponse(ctx, w, exportError(ctx, err, cfgFile, bucket), r.URL)
					return
				}
				if err = rawDataFn(bytes.NewReader(configData), cfgPath, len(configData)); err != nil {
					writeErrorResponse(ctx, w, exportError(ctx, err, cfgFile, bucket), r.URL)
					return
				}
			case bucketVersioningConfig:
				config, _, err := globalBucketMetadataSys.GetVersioningConfig(bucket)
				if err != nil {
					writeErrorResponse(ctx, w, exportError(ctx, err, cfgFile, bucket), r.URL)
					return
				}
				// ignore empty versioning configs
				if config.Status != versioning.Enabled && config.Status != versioning.Suspended {
					continue
				}
				configData, err := xml.Marshal(config)
				if err != nil {
					writeErrorResponse(ctx, w, exportError(ctx, err, cfgFile, bucket), r.URL)
					return
				}
				if err = rawDataFn(bytes.NewReader(configData), cfgPath, len(configData)); err != nil {
					writeErrorResponse(ctx, w, exportError(ctx, err, cfgFile, bucket), r.URL)
					return
				}
			case bucketReplicationConfig:
				config, _, err := globalBucketMetadataSys.GetReplicationConfig(ctx, bucket)
				if err != nil {
					if errors.Is(err, BucketReplicationConfigNotFound{Bucket: bucket}) {
						continue
					}
					writeErrorResponse(ctx, w, exportError(ctx, err, cfgFile, bucket), r.URL)
					return
				}
				configData, err := xml.Marshal(config)
				if err != nil {
					writeErrorResponse(ctx, w, exportError(ctx, err, cfgFile, bucket), r.URL)
					return
				}

				if err = rawDataFn(bytes.NewReader(configData), cfgPath, len(configData)); err != nil {
					writeErrorResponse(ctx, w, exportError(ctx, err, cfgFile, bucket), r.URL)
					return
				}
			case bucketTargetsFile:
				config, err := globalBucketMetadataSys.GetBucketTargetsConfig(bucket)
				if err != nil {
					if errors.Is(err, BucketRemoteTargetNotFound{Bucket: bucket}) {
						continue
					}

					writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
					return
				}
				configData, err := xml.Marshal(config)
				if err != nil {
					writeErrorResponse(ctx, w, exportError(ctx, err, cfgFile, bucket), r.URL)
					return
				}
				if err = rawDataFn(bytes.NewReader(configData), cfgPath, len(configData)); err != nil {
					writeErrorResponse(ctx, w, exportError(ctx, err, cfgFile, bucket), r.URL)
					return
				}
			}
		}
	}
}

// ImportBucketMetadataHandler - imports all bucket metadata from a zipped file and overwrite bucket metadata config
// There are some caveats regarding the following:
// 1. object lock config - object lock should have been specified at time of bucket creation. Only default retention settings are imported here.
// 2. Replication config - is omitted from import as remote target credentials are not available from exported data for security reasons.
// 3. lifecycle config - if transition rules are present, tier name needs to have been defined.
func (a adminAPIHandlers) ImportBucketMetadataHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ImportBucketMetadata")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))
	bucket := pathClean(r.Form.Get("bucket"))

	if !globalIsErasure {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}
	// Get current object layer instance.
	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.ImportBucketMetadataAction)
	if objectAPI == nil {
		return
	}
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
		return
	}
	reader := bytes.NewReader(data)
	zr, err := zip.NewReader(reader, int64(len(data)))
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
		return
	}
	bucketMap := make(map[string]struct{}, 1)

	// import object lock config if any - order of import matters here.
	for _, file := range zr.File {
		slc := strings.Split(file.Name, slashSeparator)
		if len(slc) != 2 { // expecting bucket/configfile in the zipfile
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
			return
		}
		b, fileName := slc[0], slc[1]
		if bucket == "" { // use bucket requested in query parameters if specified. Otherwise default bucket name to directory name within zip
			bucket = b
		}
		switch fileName {
		case objectLockConfig:
			reader, err := file.Open()
			if err != nil {
				writeErrorResponse(ctx, w, importError(ctx, err, file.Name, bucket), r.URL)
				return
			}
			config, err := objectlock.ParseObjectLockConfig(reader)
			if err != nil {
				apiErr := errorCodes.ToAPIErr(ErrMalformedXML)
				apiErr.Description = err.Error()
				writeErrorResponse(ctx, w, apiErr, r.URL)
				return
			}

			configData, err := xml.Marshal(config)
			if err != nil {
				writeErrorResponse(ctx, w, importError(ctx, err, file.Name, bucket), r.URL)
				return
			}
			if _, ok := bucketMap[bucket]; !ok {
				opts := MakeBucketOptions{
					LockEnabled: config.ObjectLockEnabled == "Enabled",
				}
				err = objectAPI.MakeBucketWithLocation(ctx, bucket, opts)
				if err != nil {
					if _, ok := err.(BucketExists); !ok {
						writeErrorResponse(ctx, w, importError(ctx, err, file.Name, bucket), r.URL)
						return
					}
				}
				bucketMap[bucket] = struct{}{}
			}

			// Deny object locking configuration settings on existing buckets without object lock enabled.
			if _, _, err = globalBucketMetadataSys.GetObjectLockConfig(bucket); err != nil {
				writeErrorResponse(ctx, w, importError(ctx, err, file.Name, bucket), r.URL)
				return
			}

			updatedAt, err := globalBucketMetadataSys.Update(ctx, bucket, objectLockConfig, configData)
			if err != nil {
				writeErrorResponse(ctx, w, importError(ctx, err, file.Name, bucket), r.URL)
				return
			}

			// Call site replication hook.
			//
			// We encode the xml bytes as base64 to ensure there are no encoding
			// errors.
			cfgStr := base64.StdEncoding.EncodeToString(configData)
			if err = globalSiteReplicationSys.BucketMetaHook(ctx, madmin.SRBucketMeta{
				Type:             madmin.SRBucketMetaTypeObjectLockConfig,
				Bucket:           bucket,
				ObjectLockConfig: &cfgStr,
				UpdatedAt:        updatedAt,
			}); err != nil {
				writeErrorResponse(ctx, w, importError(ctx, err, file.Name, bucket), r.URL)
				return
			}
		}
	}

	// import versioning metadata
	for _, file := range zr.File {
		slc := strings.Split(file.Name, slashSeparator)
		if len(slc) != 2 { // expecting bucket/configfile in the zipfile
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
			return
		}
		b, fileName := slc[0], slc[1]
		if bucket == "" { // use bucket requested in query parameters if specified. Otherwise default bucket name to directory name within zip
			bucket = b
		}
		switch fileName {
		case bucketVersioningConfig:
			reader, err := file.Open()
			if err != nil {
				writeErrorResponse(ctx, w, importError(ctx, err, file.Name, bucket), r.URL)
				return
			}
			v, err := versioning.ParseConfig(io.LimitReader(reader, maxBucketVersioningConfigSize))
			if err != nil {
				writeErrorResponse(ctx, w, importError(ctx, err, file.Name, bucket), r.URL)
				return
			}
			if _, ok := bucketMap[bucket]; !ok {
				err = objectAPI.MakeBucketWithLocation(ctx, bucket, MakeBucketOptions{})
				if err != nil {
					if _, ok := err.(BucketExists); !ok {
						writeErrorResponse(ctx, w, importError(ctx, err, file.Name, bucket), r.URL)
						return
					}
				}
				bucketMap[bucket] = struct{}{}
			}

			if globalSiteReplicationSys.isEnabled() && v.Suspended() {
				writeErrorResponse(ctx, w, APIError{
					Code:           "InvalidBucketState",
					Description:    "Cluster replication is enabled for this site, so the versioning state cannot be changed.",
					HTTPStatusCode: http.StatusConflict,
				}, r.URL)
				return
			}

			if rcfg, _ := globalBucketObjectLockSys.Get(bucket); rcfg.LockEnabled && v.Suspended() {
				writeErrorResponse(ctx, w, APIError{
					Code:           "InvalidBucketState",
					Description:    "An Object Lock configuration is present on this bucket, so the versioning state cannot be changed.",
					HTTPStatusCode: http.StatusConflict,
				}, r.URL)
				return
			}
			if _, err := getReplicationConfig(ctx, bucket); err == nil && v.Suspended() {
				writeErrorResponse(ctx, w, APIError{
					Code:           "InvalidBucketState",
					Description:    "A replication configuration is present on this bucket, so the versioning state cannot be changed.",
					HTTPStatusCode: http.StatusConflict,
				}, r.URL)
				return
			}

			configData, err := xml.Marshal(v)
			if err != nil {
				writeErrorResponse(ctx, w, importError(ctx, err, file.Name, bucket), r.URL)
				return
			}

			if _, err = globalBucketMetadataSys.Update(ctx, bucket, bucketVersioningConfig, configData); err != nil {
				writeErrorResponse(ctx, w, importError(ctx, err, file.Name, bucket), r.URL)
				return
			}
		}
	}

	for _, file := range zr.File {
		reader, err := file.Open()
		if err != nil {
			writeErrorResponse(ctx, w, importError(ctx, err, file.Name, ""), r.URL)
			return
		}
		sz := file.FileInfo().Size()
		slc := strings.Split(file.Name, slashSeparator)
		if len(slc) != 2 { // expecting bucket/configfile in the zipfile
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
			return
		}
		b, fileName := slc[0], slc[1]
		if bucket == "" { // use bucket requested in query parameters if specified. Otherwise default bucket name to directory name within zip
			bucket = b
		}
		// create bucket if it does not exist yet.
		if _, ok := bucketMap[bucket]; !ok {
			err = objectAPI.MakeBucketWithLocation(ctx, bucket, MakeBucketOptions{})
			if err != nil {
				if _, ok := err.(BucketExists); !ok {
					writeErrorResponse(ctx, w, importError(ctx, err, file.Name, bucket), r.URL)
					return
				}
			}
			bucketMap[bucket] = struct{}{}
		}
		switch fileName {
		case bucketNotificationConfig:
			config, err := event.ParseConfig(io.LimitReader(reader, sz), globalSite.Region, globalNotificationSys.targetList)
			if err != nil {
				apiErr := errorCodes.ToAPIErr(ErrMalformedXML)
				if event.IsEventError(err) {
					apiErr = importError(ctx, err, file.Name, bucket)
				}
				writeErrorResponse(ctx, w, apiErr, r.URL)
				return
			}

			configData, err := xml.Marshal(config)
			if err != nil {
				writeErrorResponse(ctx, w, importError(ctx, err, file.Name, bucket), r.URL)
				return
			}

			if _, err = globalBucketMetadataSys.Update(ctx, bucket, bucketNotificationConfig, configData); err != nil {
				writeErrorResponse(ctx, w, importError(ctx, err, file.Name, bucket), r.URL)
				return
			}
			rulesMap := config.ToRulesMap()
			globalNotificationSys.AddRulesMap(bucket, rulesMap)
		case bucketPolicyConfig:
			// Error out if Content-Length is beyond allowed size.
			if sz > maxBucketPolicySize {
				writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrPolicyTooLarge), r.URL)
				return
			}

			bucketPolicyBytes, err := ioutil.ReadAll(io.LimitReader(reader, sz))
			if err != nil {
				writeErrorResponse(ctx, w, importError(ctx, err, file.Name, bucket), r.URL)
				return
			}

			bucketPolicy, err := policy.ParseConfig(bytes.NewReader(bucketPolicyBytes), bucket)
			if err != nil {
				writeErrorResponse(ctx, w, importError(ctx, err, file.Name, bucket), r.URL)
				return
			}

			// Version in policy must not be empty
			if bucketPolicy.Version == "" {
				writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMalformedPolicy), r.URL)
				return
			}

			configData, err := json.Marshal(bucketPolicy)
			if err != nil {
				writeErrorResponse(ctx, w, importError(ctx, err, file.Name, bucket), r.URL)
				return
			}

			updatedAt, err := globalBucketMetadataSys.Update(ctx, bucket, bucketPolicyConfig, configData)
			if err != nil {
				writeErrorResponse(ctx, w, importError(ctx, err, file.Name, bucket), r.URL)
				return
			}
			// Call site replication hook.
			if err = globalSiteReplicationSys.BucketMetaHook(ctx, madmin.SRBucketMeta{
				Type:      madmin.SRBucketMetaTypePolicy,
				Bucket:    bucket,
				Policy:    bucketPolicyBytes,
				UpdatedAt: updatedAt,
			}); err != nil {
				writeErrorResponse(ctx, w, importError(ctx, err, file.Name, bucket), r.URL)
				return
			}

		case bucketLifecycleConfig:
			bucketLifecycle, err := lifecycle.ParseLifecycleConfig(io.LimitReader(reader, sz))
			if err != nil {
				writeErrorResponse(ctx, w, importError(ctx, err, file.Name, bucket), r.URL)
				return
			}

			// Validate the received bucket policy document
			if err = bucketLifecycle.Validate(); err != nil {
				writeErrorResponse(ctx, w, importError(ctx, err, file.Name, bucket), r.URL)
				return
			}

			// Validate the transition storage ARNs
			if err = validateTransitionTier(bucketLifecycle); err != nil {
				writeErrorResponse(ctx, w, importError(ctx, err, file.Name, bucket), r.URL)
				return
			}

			configData, err := xml.Marshal(bucketLifecycle)
			if err != nil {
				writeErrorResponse(ctx, w, importError(ctx, err, file.Name, bucket), r.URL)
				return
			}

			if _, err = globalBucketMetadataSys.Update(ctx, bucket, bucketLifecycleConfig, configData); err != nil {
				writeErrorResponse(ctx, w, importError(ctx, err, file.Name, bucket), r.URL)
				return
			}
		case bucketSSEConfig:
			// Parse bucket encryption xml
			encConfig, err := validateBucketSSEConfig(io.LimitReader(reader, maxBucketSSEConfigSize))
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
			kmsKey := encConfig.KeyID()
			if kmsKey != "" {
				kmsContext := kms.Context{"MinIO admin API": "ServerInfoHandler"} // Context for a test key operation
				_, err := GlobalKMS.GenerateKey(ctx, kmsKey, kmsContext)
				if err != nil {
					if errors.Is(err, kes.ErrKeyNotFound) {
						writeErrorResponse(ctx, w, importError(ctx, errKMSKeyNotFound, file.Name, bucket), r.URL)
						return
					}
					writeErrorResponse(ctx, w, importError(ctx, err, file.Name, bucket), r.URL)
					return
				}
			}

			configData, err := xml.Marshal(encConfig)
			if err != nil {
				writeErrorResponse(ctx, w, importError(ctx, err, file.Name, bucket), r.URL)
				return
			}

			// Store the bucket encryption configuration in the object layer
			updatedAt, err := globalBucketMetadataSys.Update(ctx, bucket, bucketSSEConfig, configData)
			if err != nil {
				writeErrorResponse(ctx, w, importError(ctx, err, file.Name, bucket), r.URL)
				return
			}

			// Call site replication hook.
			//
			// We encode the xml bytes as base64 to ensure there are no encoding
			// errors.
			cfgStr := base64.StdEncoding.EncodeToString(configData)
			if err = globalSiteReplicationSys.BucketMetaHook(ctx, madmin.SRBucketMeta{
				Type:      madmin.SRBucketMetaTypeSSEConfig,
				Bucket:    bucket,
				SSEConfig: &cfgStr,
				UpdatedAt: updatedAt,
			}); err != nil {
				writeErrorResponse(ctx, w, importError(ctx, err, file.Name, bucket), r.URL)
				return
			}

		case bucketTaggingConfig:
			tags, err := tags.ParseBucketXML(io.LimitReader(reader, sz))
			if err != nil {
				apiErr := errorCodes.ToAPIErrWithErr(ErrMalformedXML, fmt.Errorf("error importing %s with %w", file.Name, err))
				writeErrorResponse(ctx, w, apiErr, r.URL)
				return
			}

			configData, err := xml.Marshal(tags)
			if err != nil {
				writeErrorResponse(ctx, w, importError(ctx, err, file.Name, bucket), r.URL)
				return
			}

			updatedAt, err := globalBucketMetadataSys.Update(ctx, bucket, bucketTaggingConfig, configData)
			if err != nil {
				writeErrorResponse(ctx, w, importError(ctx, err, file.Name, bucket), r.URL)
				return
			}
			// Call site replication hook.
			//
			// We encode the xml bytes as base64 to ensure there are no encoding
			// errors.
			cfgStr := base64.StdEncoding.EncodeToString(configData)
			if err = globalSiteReplicationSys.BucketMetaHook(ctx, madmin.SRBucketMeta{
				Type:      madmin.SRBucketMetaTypeTags,
				Bucket:    bucket,
				Tags:      &cfgStr,
				UpdatedAt: updatedAt,
			}); err != nil {
				writeErrorResponse(ctx, w, importError(ctx, err, file.Name, bucket), r.URL)
				return
			}
		case bucketQuotaConfigFile:
			data, err := ioutil.ReadAll(reader)
			if err != nil {
				writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
				return
			}

			quotaConfig, err := parseBucketQuota(bucket, data)
			if err != nil {
				writeErrorResponse(ctx, w, importError(ctx, err, file.Name, bucket), r.URL)
				return
			}

			if quotaConfig.Type == "fifo" {
				writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
				return
			}

			updatedAt, err := globalBucketMetadataSys.Update(ctx, bucket, bucketQuotaConfigFile, data)
			if err != nil {
				writeErrorResponse(ctx, w, importError(ctx, err, file.Name, bucket), r.URL)
				return
			}

			bucketMeta := madmin.SRBucketMeta{
				Type:      madmin.SRBucketMetaTypeQuotaConfig,
				Bucket:    bucket,
				Quota:     data,
				UpdatedAt: updatedAt,
			}
			if quotaConfig.Quota == 0 {
				bucketMeta.Quota = nil
			}

			// Call site replication hook.
			if err = globalSiteReplicationSys.BucketMetaHook(ctx, bucketMeta); err != nil {
				writeErrorResponse(ctx, w, importError(ctx, err, file.Name, bucket), r.URL)
				return
			}
		}
	}
}

// ReplicationDiffHandler - POST returns info on unreplicated versions for a remote target ARN
// to the connected HTTP client. This is a MinIO only extension
func (a adminAPIHandlers) ReplicationDiffHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ReplicationDiff")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	if globalIsGateway {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}
	// check if user has permissions to perform this operation
	if s3Error := checkRequestAuthType(ctx, r, policy.ListBucketVersionsAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	// Check if bucket exists.
	if _, err := objectAPI.GetBucketInfo(ctx, bucket, BucketOptions{}); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	opts := extractReplicateDiffOpts(r.Form)
	if opts.ARN != "" {
		tgt := globalBucketTargetSys.GetRemoteBucketTargetByArn(ctx, bucket, opts.ARN)
		if tgt.Empty() {
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrInvalidRequest, fmt.Errorf("invalid arn : '%s'", opts.ARN)), r.URL)
			return
		}
	}

	keepAliveTicker := time.NewTicker(500 * time.Millisecond)
	defer keepAliveTicker.Stop()

	diffCh, err := getReplicationDiff(ctx, objectAPI, bucket, opts)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	enc := json.NewEncoder(w)
	for {
		select {
		case entry, ok := <-diffCh:
			if !ok {
				return
			}
			if err := enc.Encode(entry); err != nil {
				return
			}
			if len(diffCh) == 0 {
				// Flush if nothing is queued
				w.(http.Flusher).Flush()
			}
		case <-keepAliveTicker.C:
			if len(diffCh) > 0 {
				continue
			}
			if _, err := w.Write([]byte(" ")); err != nil {
				return
			}
			w.(http.Flusher).Flush()
		case <-ctx.Done():
			return
		}
	}
}
