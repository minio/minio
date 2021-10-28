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
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/minio/madmin-go"

	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/bucket/policy"
	iampolicy "github.com/minio/pkg/iam/policy"
)

// SiteReplicationAdd - PUT /minio/admin/v3/site-replication/add
func (a adminAPIHandlers) SiteReplicationAdd(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SiteReplicationAdd")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, cred := validateAdminReq(ctx, w, r, iampolicy.SiteReplicationAddAction)
	if objectAPI == nil {
		return
	}

	var sites []madmin.PeerSite
	errCode := readJSONBody(ctx, r.Body, &sites, cred.SecretKey)
	if errCode != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(errCode), r.URL)
		return
	}

	status, errInfo := globalSiteReplicationSys.AddPeerClusters(ctx, sites)
	if errInfo.Code != ErrNone {
		logger.LogIf(ctx, errInfo)
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(errInfo.Code, errInfo.Cause), r.URL)
		return
	}

	body, err := json.Marshal(status)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessResponseJSON(w, body)
}

// SRInternalJoin - PUT /minio/admin/v3/site-replication/join
//
// used internally to tell current cluster to enable SR with
// the provided peer clusters and service account.
func (a adminAPIHandlers) SRInternalJoin(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SRInternalJoin")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, cred := validateAdminReq(ctx, w, r, iampolicy.SiteReplicationAddAction)
	if objectAPI == nil {
		return
	}

	var joinArg madmin.SRInternalJoinReq
	errCode := readJSONBody(ctx, r.Body, &joinArg, cred.SecretKey)
	if errCode != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(errCode), r.URL)
		return
	}

	errInfo := globalSiteReplicationSys.InternalJoinReq(ctx, joinArg)
	if errInfo.Code != ErrNone {
		logger.LogIf(ctx, errInfo)
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(errInfo.Code, errInfo.Cause), r.URL)
		return
	}
}

// SRInternalBucketOps - PUT /minio/admin/v3/site-replication/bucket-ops?bucket=x&operation=y
func (a adminAPIHandlers) SRInternalBucketOps(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SRInternalBucketOps")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.SiteReplicationOperationAction)
	if objectAPI == nil {
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	operation := madmin.BktOp(vars["operation"])

	var err error
	switch operation {
	case madmin.MakeWithVersioningBktOp:
		_, isLockEnabled := r.Form["lockEnabled"]
		_, isVersioningEnabled := r.Form["versioningEnabled"]
		opts := BucketOptions{
			Location:          r.Form.Get("location"),
			LockEnabled:       isLockEnabled,
			VersioningEnabled: isVersioningEnabled,
		}
		err = globalSiteReplicationSys.PeerBucketMakeWithVersioningHandler(ctx, bucket, opts)
	case madmin.ConfigureReplBktOp:
		err = globalSiteReplicationSys.PeerBucketConfigureReplHandler(ctx, bucket)
	case madmin.DeleteBucketBktOp:
		err = globalSiteReplicationSys.PeerBucketDeleteHandler(ctx, bucket, false)
	case madmin.ForceDeleteBucketBktOp:
		err = globalSiteReplicationSys.PeerBucketDeleteHandler(ctx, bucket, true)
	default:
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminInvalidArgument), r.URL)
		return
	}
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), r.URL)
		return
	}

}

// SRInternalReplicateIAMItem - PUT /minio/admin/v3/site-replication/iam-item
func (a adminAPIHandlers) SRInternalReplicateIAMItem(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SRInternalReplicateIAMItem")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.SiteReplicationOperationAction)
	if objectAPI == nil {
		return
	}

	var item madmin.SRIAMItem
	errCode := readJSONBody(ctx, r.Body, &item, "")
	if errCode != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(errCode), r.URL)
		return
	}

	var err error
	switch item.Type {
	case madmin.SRIAMItemPolicy:
		var policy *iampolicy.Policy
		if len(item.Policy) > 0 {
			policy, err = iampolicy.ParseConfig(bytes.NewReader(item.Policy))
			if err != nil {
				writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
				return
			}
		}
		err = globalSiteReplicationSys.PeerAddPolicyHandler(ctx, item.Name, policy)
	case madmin.SRIAMItemSvcAcc:
		err = globalSiteReplicationSys.PeerSvcAccChangeHandler(ctx, *item.SvcAccChange)
	case madmin.SRIAMItemPolicyMapping:
		err = globalSiteReplicationSys.PeerPolicyMappingHandler(ctx, *item.PolicyMapping)
	case madmin.SRIAMItemSTSAcc:
		err = globalSiteReplicationSys.PeerSTSAccHandler(ctx, *item.STSCredential)

	default:
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminInvalidArgument), r.URL)
		return
	}
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), r.URL)
		return
	}
}

// SRInternalReplicateBucketItem - PUT /minio/admin/v3/site-replication/bucket-meta
func (a adminAPIHandlers) SRInternalReplicateBucketItem(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SRInternalReplicateIAMItem")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.SiteReplicationOperationAction)
	if objectAPI == nil {
		return
	}

	var item madmin.SRBucketMeta
	errCode := readJSONBody(ctx, r.Body, &item, "")
	if errCode != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(errCode), r.URL)
		return
	}

	var err error
	switch item.Type {
	case madmin.SRBucketMetaTypePolicy:
		var bktPolicy *policy.Policy
		if len(item.Policy) > 0 {
			bktPolicy, err = policy.ParseConfig(bytes.NewReader(item.Policy), item.Bucket)
			if err != nil {
				writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
				return
			}
		}
		err = globalSiteReplicationSys.PeerBucketPolicyHandler(ctx, item.Bucket, bktPolicy)
	case madmin.SRBucketMetaTypeTags:
		err = globalSiteReplicationSys.PeerBucketTaggingHandler(ctx, item.Bucket, item.Tags)
	case madmin.SRBucketMetaTypeObjectLockConfig:
		err = globalSiteReplicationSys.PeerBucketObjectLockConfigHandler(ctx, item.Bucket, item.ObjectLockConfig)
	case madmin.SRBucketMetaTypeSSEConfig:
		err = globalSiteReplicationSys.PeerBucketSSEConfigHandler(ctx, item.Bucket, item.SSEConfig)

	default:
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminInvalidArgument), r.URL)
		return
	}
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInternalError), r.URL)
		return
	}
}

// SiteReplicationDisable - PUT /minio/admin/v3/site-replication/disable
func (a adminAPIHandlers) SiteReplicationDisable(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SiteReplicationDisable")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.SiteReplicationDisableAction)
	if objectAPI == nil {
		return
	}
}

// SiteReplicationInfo - GET /minio/admin/v3/site-replication/info
func (a adminAPIHandlers) SiteReplicationInfo(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SiteReplicationInfo")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.SiteReplicationInfoAction)
	if objectAPI == nil {
		return
	}

	info, err := globalSiteReplicationSys.GetClusterInfo(ctx)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err = json.NewEncoder(w).Encode(info); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
}

func (a adminAPIHandlers) SRInternalGetIDPSettings(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SiteReplicationGetIDPSettings")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.SiteReplicationAddAction)
	if objectAPI == nil {
		return
	}

	idpSettings := globalSiteReplicationSys.GetIDPSettings(ctx)
	if err := json.NewEncoder(w).Encode(idpSettings); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
}

func readJSONBody(ctx context.Context, body io.Reader, v interface{}, encryptionKey string) APIErrorCode {
	data, err := ioutil.ReadAll(body)
	if err != nil {
		return ErrInvalidRequest
	}

	if encryptionKey != "" {
		data, err = madmin.DecryptData(encryptionKey, bytes.NewReader(data))
		if err != nil {
			logger.LogIf(ctx, err)
			return ErrInvalidRequest
		}
	}

	err = json.Unmarshal(data, v)
	if err != nil {
		return ErrAdminConfigBadJSON
	}

	return ErrNone
}
