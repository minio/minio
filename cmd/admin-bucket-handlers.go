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
	"net/http"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/klauspost/compress/zip"
	"github.com/minio/kms-go/kes"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/minio/minio/internal/bucket/lifecycle"
	objectlock "github.com/minio/minio/internal/bucket/object/lock"
	"github.com/minio/minio/internal/bucket/versioning"
	"github.com/minio/minio/internal/event"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/kms"
	"github.com/minio/mux"
	"github.com/minio/pkg/v3/policy"
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
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.SetBucketQuotaAdminAction)
	if objectAPI == nil {
		return
	}

	vars := mux.Vars(r)
	bucket := pathClean(vars["bucket"])

	if _, err := objectAPI.GetBucketInfo(ctx, bucket, BucketOptions{}); err != nil {
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	data, err := io.ReadAll(r.Body)
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
		return
	}

	quotaConfig, err := parseBucketQuota(bucket, data)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
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
	if quotaConfig.Size == 0 && quotaConfig.Quota == 0 {
		bucketMeta.Quota = nil
	}

	// Call site replication hook.
	replLogIf(ctx, globalSiteReplicationSys.BucketMetaHook(ctx, bucketMeta))

	// Write success response.
	writeSuccessResponseHeadersOnly(w)
}

// GetBucketQuotaConfigHandler - gets bucket quota configuration
func (a adminAPIHandlers) GetBucketQuotaConfigHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.GetBucketQuotaAdminAction)
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
	ctx := r.Context()

	vars := mux.Vars(r)
	bucket := pathClean(vars["bucket"])
	update := r.Form.Get("update") == "true"

	// Get current object layer instance.
	objectAPI, _ := validateAdminReq(ctx, w, r, policy.SetBucketTargetAction)
	if objectAPI == nil {
		return
	}

	// Check if bucket exists.
	if _, err := objectAPI.GetBucketInfo(ctx, bucket, BucketOptions{}); err != nil {
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	cred, _, s3Err := validateAdminSignature(ctx, r, "")
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
		var exists bool // true if arn exists
		target.Arn, exists = globalBucketTargetSys.getRemoteARN(bucket, &target, "")
		if exists && target.Arn != "" { // return pre-existing ARN
			data, err := json.Marshal(target.Arn)
			if err != nil {
				writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
				return
			}
			// Write success response.
			writeSuccessResponseJSON(w, data)
			return
		}
	}
	if target.Arn == "" {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrAdminConfigBadJSON, err), r.URL)
		return
	}
	if globalSiteReplicationSys.isEnabled() && !update {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrRemoteTargetDenyAddError, err), r.URL)
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
				if !globalSiteReplicationSys.isEnabled() {
					// credentials update is possible only in bucket replication. User will never
					// know the site replicator creds.
					tgt.Credentials = target.Credentials
					tgt.TargetBucket = target.TargetBucket
					tgt.Secure = target.Secure
					tgt.Endpoint = target.Endpoint
				}
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
		case RemoteTargetConnectionErr:
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
	ctx := r.Context()

	vars := mux.Vars(r)
	bucket := pathClean(vars["bucket"])
	arnType := vars["type"]

	// Get current object layer instance.
	objectAPI, _ := validateAdminReq(ctx, w, r, policy.GetBucketTargetAction)
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
	ctx := r.Context()

	vars := mux.Vars(r)
	bucket := pathClean(vars["bucket"])
	arn := vars["arn"]

	// Get current object layer instance.
	objectAPI, _ := validateAdminReq(ctx, w, r, policy.SetBucketTargetAction)
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
	ctx := r.Context()

	bucket := pathClean(r.Form.Get("bucket"))
	// Get current object layer instance.
	objectAPI, _ := validateAdminReq(ctx, w, r, policy.ExportBucketMetadataAction)
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

	rawDataFn := func(r io.Reader, filename string, sz int) {
		header, zerr := zip.FileInfoHeader(dummyFileInfo{
			name:    filename,
			size:    int64(sz),
			mode:    0o600,
			modTime: time.Now(),
			isDir:   false,
			sys:     nil,
		})
		if zerr == nil {
			header.Method = zip.Deflate
			zwriter, zerr := zipWriter.CreateHeader(header)
			if zerr == nil {
				io.Copy(zwriter, r)
			}
		}
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
			case bucketPolicyConfig:
				config, _, err := globalBucketMetadataSys.GetBucketPolicy(bucket)
				if err != nil {
					if errors.Is(err, BucketPolicyNotFound{Bucket: bucket}) {
						continue
					}
					writeErrorResponse(ctx, w, exportError(ctx, err, cfgFile, bucket), r.URL)
					return
				}
				configData, err := json.Marshal(config)
				if err != nil {
					writeErrorResponse(ctx, w, exportError(ctx, err, cfgFile, bucket), r.URL)
					return
				}
				rawDataFn(bytes.NewReader(configData), cfgPath, len(configData))
			case bucketNotificationConfig:
				config, err := globalBucketMetadataSys.GetNotificationConfig(bucket)
				if err != nil {
					adminLogIf(ctx, err)
					writeErrorResponse(ctx, w, exportError(ctx, err, cfgFile, bucket), r.URL)
					return
				}
				configData, err := xml.Marshal(config)
				if err != nil {
					writeErrorResponse(ctx, w, exportError(ctx, err, cfgFile, bucket), r.URL)
					return
				}
				rawDataFn(bytes.NewReader(configData), cfgPath, len(configData))
			case bucketLifecycleConfig:
				config, _, err := globalBucketMetadataSys.GetLifecycleConfig(bucket)
				if err != nil {
					if errors.Is(err, BucketLifecycleNotFound{Bucket: bucket}) {
						continue
					}
					adminLogIf(ctx, err)
					writeErrorResponse(ctx, w, exportError(ctx, err, cfgFile, bucket), r.URL)
					return
				}
				configData, err := xml.Marshal(config)
				if err != nil {
					writeErrorResponse(ctx, w, exportError(ctx, err, cfgFile, bucket), r.URL)
					return
				}
				rawDataFn(bytes.NewReader(configData), cfgPath, len(configData))
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
				rawDataFn(bytes.NewReader(configData), cfgPath, len(configData))
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
				rawDataFn(bytes.NewReader(configData), cfgPath, len(configData))
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
				rawDataFn(bytes.NewReader(configData), cfgPath, len(configData))
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
				rawDataFn(bytes.NewReader(configData), cfgPath, len(configData))
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
				rawDataFn(bytes.NewReader(configData), cfgPath, len(configData))
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
				rawDataFn(bytes.NewReader(configData), cfgPath, len(configData))
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
				rawDataFn(bytes.NewReader(configData), cfgPath, len(configData))
			}
		}
	}
}

type importMetaReport struct {
	madmin.BucketMetaImportErrs
}

func (i *importMetaReport) SetStatus(bucket, fname string, err error) {
	st := i.Buckets[bucket]
	var errMsg string
	if err != nil {
		errMsg = err.Error()
	}
	switch fname {
	case bucketPolicyConfig:
		st.Policy = madmin.MetaStatus{IsSet: true, Err: errMsg}
	case bucketNotificationConfig:
		st.Notification = madmin.MetaStatus{IsSet: true, Err: errMsg}
	case bucketLifecycleConfig:
		st.Lifecycle = madmin.MetaStatus{IsSet: true, Err: errMsg}
	case bucketSSEConfig:
		st.SSEConfig = madmin.MetaStatus{IsSet: true, Err: errMsg}
	case bucketTaggingConfig:
		st.Tagging = madmin.MetaStatus{IsSet: true, Err: errMsg}
	case bucketQuotaConfigFile:
		st.Quota = madmin.MetaStatus{IsSet: true, Err: errMsg}
	case objectLockConfig:
		st.ObjectLock = madmin.MetaStatus{IsSet: true, Err: errMsg}
	case bucketVersioningConfig:
		st.Versioning = madmin.MetaStatus{IsSet: true, Err: errMsg}
	default:
		st.Err = errMsg
	}
	i.Buckets[bucket] = st
}

// ImportBucketMetadataHandler - imports all bucket metadata from a zipped file and overwrite bucket metadata config
// There are some caveats regarding the following:
// 1. object lock config - object lock should have been specified at time of bucket creation. Only default retention settings are imported here.
// 2. Replication config - is omitted from import as remote target credentials are not available from exported data for security reasons.
// 3. lifecycle config - if transition rules are present, tier name needs to have been defined.
func (a adminAPIHandlers) ImportBucketMetadataHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Get current object layer instance.
	objectAPI, _ := validateAdminReq(ctx, w, r, policy.ImportBucketMetadataAction)
	if objectAPI == nil {
		return
	}
	data, err := io.ReadAll(r.Body)
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
	rpt := importMetaReport{
		madmin.BucketMetaImportErrs{
			Buckets: make(map[string]madmin.BucketStatus, len(zr.File)),
		},
	}

	bucketMap := make(map[string]*BucketMetadata, len(zr.File))

	updatedAt := UTCNow()

	for _, file := range zr.File {
		slc := strings.Split(file.Name, slashSeparator)
		if len(slc) != 2 { // expecting bucket/configfile in the zipfile
			rpt.SetStatus(file.Name, "", fmt.Errorf("malformed zip - expecting format bucket/<config.json>"))
			continue
		}
		bucket := slc[0]
		meta, err := readBucketMetadata(ctx, objectAPI, bucket)
		if err == nil {
			bucketMap[bucket] = &meta
		} else if err != errConfigNotFound {
			rpt.SetStatus(bucket, "", err)
		}
	}

	// import object lock config if any - order of import matters here.
	for _, file := range zr.File {
		slc := strings.Split(file.Name, slashSeparator)
		if len(slc) != 2 { // expecting bucket/configfile in the zipfile
			rpt.SetStatus(file.Name, "", fmt.Errorf("malformed zip - expecting format bucket/<config.json>"))
			continue
		}
		bucket, fileName := slc[0], slc[1]
		if fileName == objectLockConfig {
			reader, err := file.Open()
			if err != nil {
				rpt.SetStatus(bucket, fileName, err)
				continue
			}
			config, err := objectlock.ParseObjectLockConfig(reader)
			if err != nil {
				rpt.SetStatus(bucket, fileName, fmt.Errorf("%s (%s)", errorCodes[ErrMalformedXML].Description, err))
				continue
			}

			configData, err := xml.Marshal(config)
			if err != nil {
				rpt.SetStatus(bucket, fileName, err)
				continue
			}
			if _, ok := bucketMap[bucket]; !ok {
				opts := MakeBucketOptions{
					LockEnabled: config.Enabled(),
					ForceCreate: true, // ignore if it already exists
				}
				err = objectAPI.MakeBucket(ctx, bucket, opts)
				if err != nil {
					rpt.SetStatus(bucket, fileName, err)
					continue
				}
				v, _ := globalBucketMetadataSys.Get(bucket)
				bucketMap[bucket] = &v
			}

			bucketMap[bucket].ObjectLockConfigXML = configData
			bucketMap[bucket].ObjectLockConfigUpdatedAt = updatedAt
			rpt.SetStatus(bucket, fileName, nil)
		}
	}

	// import versioning metadata
	for _, file := range zr.File {
		slc := strings.Split(file.Name, slashSeparator)
		if len(slc) != 2 { // expecting bucket/configfile in the zipfile
			rpt.SetStatus(file.Name, "", fmt.Errorf("malformed zip - expecting format bucket/<config.json>"))
			continue
		}
		bucket, fileName := slc[0], slc[1]
		if fileName == bucketVersioningConfig {
			reader, err := file.Open()
			if err != nil {
				rpt.SetStatus(bucket, fileName, err)
				continue
			}
			v, err := versioning.ParseConfig(io.LimitReader(reader, maxBucketVersioningConfigSize))
			if err != nil {
				rpt.SetStatus(bucket, fileName, err)
				continue
			}
			if _, ok := bucketMap[bucket]; !ok {
				if err = objectAPI.MakeBucket(ctx, bucket, MakeBucketOptions{
					ForceCreate: true, // ignore if it already exists
				}); err != nil {
					rpt.SetStatus(bucket, fileName, err)
					continue
				}
				v, _ := globalBucketMetadataSys.Get(bucket)
				bucketMap[bucket] = &v
			}

			if globalSiteReplicationSys.isEnabled() && v.Suspended() {
				rpt.SetStatus(bucket, fileName, fmt.Errorf("Cluster replication is enabled for this site, so the versioning state cannot be suspended."))
				continue
			}

			if rcfg, _ := globalBucketObjectLockSys.Get(bucket); rcfg.LockEnabled && v.Suspended() {
				rpt.SetStatus(bucket, fileName, fmt.Errorf("An Object Lock configuration is present on this bucket, so the versioning state cannot be suspended."))
				continue
			}
			if rcfg, _ := getReplicationConfig(ctx, bucket); rcfg != nil && v.Suspended() {
				rpt.SetStatus(bucket, fileName, fmt.Errorf("A replication configuration is present on this bucket, so the versioning state cannot be suspended."))
				continue
			}

			configData, err := xml.Marshal(v)
			if err != nil {
				rpt.SetStatus(bucket, fileName, fmt.Errorf("%s (%s)", errorCodes[ErrMalformedXML].Description, err))
				continue
			}

			bucketMap[bucket].VersioningConfigXML = configData
			bucketMap[bucket].VersioningConfigUpdatedAt = updatedAt
			rpt.SetStatus(bucket, fileName, nil)
		}
	}

	for _, file := range zr.File {
		reader, err := file.Open()
		if err != nil {
			rpt.SetStatus(file.Name, "", err)
			continue
		}
		sz := file.FileInfo().Size()
		slc := strings.Split(file.Name, slashSeparator)
		if len(slc) != 2 { // expecting bucket/configfile in the zipfile
			rpt.SetStatus(file.Name, "", fmt.Errorf("malformed zip - expecting format bucket/<config.json>"))
			continue
		}
		bucket, fileName := slc[0], slc[1]

		// create bucket if it does not exist yet.
		if _, ok := bucketMap[bucket]; !ok {
			err = objectAPI.MakeBucket(ctx, bucket, MakeBucketOptions{
				ForceCreate: true, // ignore if it already exists
			})
			if err != nil {
				rpt.SetStatus(bucket, "", err)
				continue
			}
			v, _ := globalBucketMetadataSys.Get(bucket)
			bucketMap[bucket] = &v
		}
		if _, ok := bucketMap[bucket]; !ok {
			continue
		}
		switch fileName {
		case bucketNotificationConfig:
			config, err := event.ParseConfig(io.LimitReader(reader, sz), globalSite.Region(), globalEventNotifier.targetList)
			if err != nil {
				rpt.SetStatus(bucket, fileName, fmt.Errorf("%s (%s)", errorCodes[ErrMalformedXML].Description, err))
				continue
			}

			configData, err := xml.Marshal(config)
			if err != nil {
				rpt.SetStatus(bucket, fileName, err)
				continue
			}

			bucketMap[bucket].NotificationConfigXML = configData
			bucketMap[bucket].NotificationConfigUpdatedAt = updatedAt
			rpt.SetStatus(bucket, fileName, nil)
		case bucketPolicyConfig:
			// Error out if Content-Length is beyond allowed size.
			if sz > maxBucketPolicySize {
				rpt.SetStatus(bucket, fileName, errors.New(ErrPolicyTooLarge.String()))
				continue
			}

			bucketPolicyBytes, err := io.ReadAll(io.LimitReader(reader, sz))
			if err != nil {
				rpt.SetStatus(bucket, fileName, err)
				continue
			}

			bucketPolicy, err := policy.ParseBucketPolicyConfig(bytes.NewReader(bucketPolicyBytes), bucket)
			if err != nil {
				rpt.SetStatus(bucket, fileName, err)
				continue
			}

			// Version in policy must not be empty
			if bucketPolicy.Version == "" {
				rpt.SetStatus(bucket, fileName, errors.New(ErrPolicyInvalidVersion.String()))
				continue
			}

			configData, err := json.Marshal(bucketPolicy)
			if err != nil {
				rpt.SetStatus(bucket, fileName, err)
				continue
			}

			bucketMap[bucket].PolicyConfigJSON = configData
			bucketMap[bucket].PolicyConfigUpdatedAt = updatedAt
			rpt.SetStatus(bucket, fileName, nil)
		case bucketLifecycleConfig:
			bucketLifecycle, err := lifecycle.ParseLifecycleConfig(io.LimitReader(reader, sz))
			if err != nil {
				rpt.SetStatus(bucket, fileName, err)
				continue
			}
			rcfg, err := globalBucketObjectLockSys.Get(bucket)
			if err != nil {
				rpt.SetStatus(bucket, fileName, err)
				continue
			}
			// Validate the received bucket policy document
			if err = bucketLifecycle.Validate(rcfg); err != nil {
				rpt.SetStatus(bucket, fileName, err)
				continue
			}

			// Validate the transition storage ARNs
			if err = validateTransitionTier(bucketLifecycle); err != nil {
				rpt.SetStatus(bucket, fileName, err)
				continue
			}

			configData, err := xml.Marshal(bucketLifecycle)
			if err != nil {
				rpt.SetStatus(bucket, fileName, err)
				continue
			}

			bucketMap[bucket].LifecycleConfigXML = configData
			bucketMap[bucket].LifecycleConfigUpdatedAt = updatedAt
			rpt.SetStatus(bucket, fileName, nil)
		case bucketSSEConfig:
			// Parse bucket encryption xml
			encConfig, err := validateBucketSSEConfig(io.LimitReader(reader, maxBucketSSEConfigSize))
			if err != nil {
				rpt.SetStatus(bucket, fileName, fmt.Errorf("%s (%s)", errorCodes[ErrMalformedXML].Description, err))
				continue
			}

			// Return error if KMS is not initialized
			if GlobalKMS == nil {
				rpt.SetStatus(bucket, fileName, fmt.Errorf("%s", errorCodes[ErrKMSNotConfigured].Description))
				continue
			}
			kmsKey := encConfig.KeyID()
			if kmsKey != "" {
				_, err := GlobalKMS.GenerateKey(ctx, &kms.GenerateKeyRequest{
					Name:           kmsKey,
					AssociatedData: kms.Context{"MinIO admin API": "ServerInfoHandler"}, // Context for a test key operation
				})
				if err != nil {
					if errors.Is(err, kes.ErrKeyNotFound) {
						rpt.SetStatus(bucket, fileName, errKMSKeyNotFound)
						continue
					}
					rpt.SetStatus(bucket, fileName, err)
					continue
				}
			}

			configData, err := xml.Marshal(encConfig)
			if err != nil {
				rpt.SetStatus(bucket, fileName, err)
				continue
			}

			bucketMap[bucket].EncryptionConfigXML = configData
			bucketMap[bucket].EncryptionConfigUpdatedAt = updatedAt
			rpt.SetStatus(bucket, fileName, nil)
		case bucketTaggingConfig:
			tags, err := tags.ParseBucketXML(io.LimitReader(reader, sz))
			if err != nil {
				rpt.SetStatus(bucket, fileName, fmt.Errorf("%s (%s)", errorCodes[ErrMalformedXML].Description, err))
				continue
			}

			configData, err := xml.Marshal(tags)
			if err != nil {
				rpt.SetStatus(bucket, fileName, err)
				continue
			}

			bucketMap[bucket].TaggingConfigXML = configData
			bucketMap[bucket].TaggingConfigUpdatedAt = updatedAt
			rpt.SetStatus(bucket, fileName, nil)
		case bucketQuotaConfigFile:
			data, err := io.ReadAll(reader)
			if err != nil {
				rpt.SetStatus(bucket, fileName, err)
				continue
			}

			_, err = parseBucketQuota(bucket, data)
			if err != nil {
				rpt.SetStatus(bucket, fileName, err)
				continue
			}

			bucketMap[bucket].QuotaConfigJSON = data
			bucketMap[bucket].QuotaConfigUpdatedAt = updatedAt
			rpt.SetStatus(bucket, fileName, nil)
		}
	}

	enc := func(b []byte) *string {
		if b == nil {
			return nil
		}
		v := base64.StdEncoding.EncodeToString(b)
		return &v
	}

	for bucket, meta := range bucketMap {
		err := globalBucketMetadataSys.save(ctx, *meta)
		if err != nil {
			rpt.SetStatus(bucket, "", err)
			continue
		}
		// Call site replication hook.
		if err = globalSiteReplicationSys.BucketMetaHook(ctx, madmin.SRBucketMeta{
			Bucket:           bucket,
			Quota:            meta.QuotaConfigJSON,
			Policy:           meta.PolicyConfigJSON,
			Versioning:       enc(meta.VersioningConfigXML),
			Tags:             enc(meta.TaggingConfigXML),
			ObjectLockConfig: enc(meta.ObjectLockConfigXML),
			SSEConfig:        enc(meta.EncryptionConfigXML),
			UpdatedAt:        updatedAt,
		}); err != nil {
			rpt.SetStatus(bucket, "", err)
			continue
		}
	}

	rptData, err := json.Marshal(rpt.BucketMetaImportErrs)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	writeSuccessResponseJSON(w, rptData)
}

// ReplicationDiffHandler - POST returns info on unreplicated versions for a remote target ARN
// to the connected HTTP client.
func (a adminAPIHandlers) ReplicationDiffHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.ReplicationDiff)
	if objectAPI == nil {
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
				xhttp.Flush(w)
			}
		case <-keepAliveTicker.C:
			if len(diffCh) > 0 {
				continue
			}
			if _, err := w.Write([]byte(" ")); err != nil {
				return
			}
			xhttp.Flush(w)
		case <-ctx.Done():
			return
		}
	}
}

// ReplicationMRFHandler - POST returns info on entries in the MRF backlog for a node or all nodes
func (a adminAPIHandlers) ReplicationMRFHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.ReplicationDiff)
	if objectAPI == nil {
		return
	}

	// Check if bucket exists.
	if bucket != "" {
		if _, err := objectAPI.GetBucketInfo(ctx, bucket, BucketOptions{}); err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
	}

	q := r.Form
	node := q.Get("node")

	keepAliveTicker := time.NewTicker(500 * time.Millisecond)
	defer keepAliveTicker.Stop()

	mrfCh, err := globalNotificationSys.GetReplicationMRF(ctx, bucket, node)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	enc := json.NewEncoder(w)
	for {
		select {
		case entry, ok := <-mrfCh:
			if !ok {
				return
			}
			if err := enc.Encode(entry); err != nil {
				return
			}
			if len(mrfCh) == 0 {
				// Flush if nothing is queued
				xhttp.Flush(w)
			}
		case <-keepAliveTicker.C:
			if len(mrfCh) > 0 {
				continue
			}
			if _, err := w.Write([]byte(" ")); err != nil {
				return
			}
			xhttp.Flush(w)
		case <-ctx.Done():
			return
		}
	}
}
