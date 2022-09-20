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
	"net/http"
	"strings"
	"time"

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
	if err := parseJSONBody(ctx, r.Body, &sites, cred.SecretKey); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	status, err := globalSiteReplicationSys.AddPeerClusters(ctx, sites)
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	body, err := json.Marshal(status)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessResponseJSON(w, body)
}

// SRPeerJoin - PUT /minio/admin/v3/site-replication/join
//
// used internally to tell current cluster to enable SR with
// the provided peer clusters and service account.
func (a adminAPIHandlers) SRPeerJoin(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SRPeerJoin")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, cred := validateAdminReq(ctx, w, r, iampolicy.SiteReplicationAddAction)
	if objectAPI == nil {
		return
	}

	var joinArg madmin.SRPeerJoinReq
	if err := parseJSONBody(ctx, r.Body, &joinArg, cred.SecretKey); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err := globalSiteReplicationSys.PeerJoinReq(ctx, joinArg); err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
}

// SRPeerBucketOps - PUT /minio/admin/v3/site-replication/bucket-ops?bucket=x&operation=y
func (a adminAPIHandlers) SRPeerBucketOps(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SRPeerBucketOps")

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
	default:
		err = errSRInvalidRequest(errInvalidArgument)
	case madmin.MakeWithVersioningBktOp:
		_, isLockEnabled := r.Form["lockEnabled"]
		_, isVersioningEnabled := r.Form["versioningEnabled"]
		_, isForceCreate := r.Form["forceCreate"]
		createdAtStr := strings.TrimSpace(r.Form.Get("createdAt"))
		createdAt, cerr := time.Parse(time.RFC3339Nano, createdAtStr)
		if cerr != nil {
			createdAt = timeSentinel
		}

		opts := MakeBucketOptions{
			Location:          r.Form.Get("location"),
			LockEnabled:       isLockEnabled,
			VersioningEnabled: isVersioningEnabled,
			ForceCreate:       isForceCreate,
			CreatedAt:         createdAt,
		}
		err = globalSiteReplicationSys.PeerBucketMakeWithVersioningHandler(ctx, bucket, opts)
	case madmin.ConfigureReplBktOp:
		err = globalSiteReplicationSys.PeerBucketConfigureReplHandler(ctx, bucket)
	case madmin.DeleteBucketBktOp:
		_, noRecreate := r.Form["noRecreate"]
		err = globalSiteReplicationSys.PeerBucketDeleteHandler(ctx, bucket, DeleteBucketOptions{
			Force:      false,
			NoRecreate: noRecreate,
			SRDeleteOp: getSRBucketDeleteOp(true),
		})
	case madmin.ForceDeleteBucketBktOp:
		_, noRecreate := r.Form["noRecreate"]
		err = globalSiteReplicationSys.PeerBucketDeleteHandler(ctx, bucket, DeleteBucketOptions{
			Force:      true,
			NoRecreate: noRecreate,
			SRDeleteOp: getSRBucketDeleteOp(true),
		})
	case madmin.PurgeDeletedBucketOp:
		globalSiteReplicationSys.purgeDeletedBucket(ctx, objectAPI, bucket)
	}
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
}

// SRPeerReplicateIAMItem - PUT /minio/admin/v3/site-replication/iam-item
func (a adminAPIHandlers) SRPeerReplicateIAMItem(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SRPeerReplicateIAMItem")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.SiteReplicationOperationAction)
	if objectAPI == nil {
		return
	}

	var item madmin.SRIAMItem
	if err := parseJSONBody(ctx, r.Body, &item, ""); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	var err error
	switch item.Type {
	default:
		err = errSRInvalidRequest(errInvalidArgument)
	case madmin.SRIAMItemPolicy:
		if item.Policy == nil {
			err = globalSiteReplicationSys.PeerAddPolicyHandler(ctx, item.Name, nil, item.UpdatedAt)
		} else {
			policy, perr := iampolicy.ParseConfig(bytes.NewReader(item.Policy))
			if perr != nil {
				writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, perr), r.URL)
				return
			}
			if policy.IsEmpty() {
				err = globalSiteReplicationSys.PeerAddPolicyHandler(ctx, item.Name, nil, item.UpdatedAt)
			} else {
				err = globalSiteReplicationSys.PeerAddPolicyHandler(ctx, item.Name, policy, item.UpdatedAt)
			}
		}
	case madmin.SRIAMItemSvcAcc:
		err = globalSiteReplicationSys.PeerSvcAccChangeHandler(ctx, item.SvcAccChange, item.UpdatedAt)
	case madmin.SRIAMItemPolicyMapping:
		err = globalSiteReplicationSys.PeerPolicyMappingHandler(ctx, item.PolicyMapping, item.UpdatedAt)
	case madmin.SRIAMItemSTSAcc:
		err = globalSiteReplicationSys.PeerSTSAccHandler(ctx, item.STSCredential, item.UpdatedAt)
	case madmin.SRIAMItemIAMUser:
		err = globalSiteReplicationSys.PeerIAMUserChangeHandler(ctx, item.IAMUser, item.UpdatedAt)
	case madmin.SRIAMItemGroupInfo:
		err = globalSiteReplicationSys.PeerGroupInfoChangeHandler(ctx, item.GroupInfo, item.UpdatedAt)
	}
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
}

// SRPeerReplicateBucketItem - PUT /minio/admin/v3/site-replication/bucket-meta
func (a adminAPIHandlers) SRPeerReplicateBucketItem(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SRPeerReplicateBucketItem")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.SiteReplicationOperationAction)
	if objectAPI == nil {
		return
	}

	var item madmin.SRBucketMeta
	if err := parseJSONBody(ctx, r.Body, &item, ""); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	var err error
	switch item.Type {
	default:
		err = errSRInvalidRequest(errInvalidArgument)
	case madmin.SRBucketMetaTypePolicy:
		if item.Policy == nil {
			err = globalSiteReplicationSys.PeerBucketPolicyHandler(ctx, item.Bucket, nil, item.UpdatedAt)
		} else {
			bktPolicy, berr := policy.ParseConfig(bytes.NewReader(item.Policy), item.Bucket)
			if berr != nil {
				writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, berr), r.URL)
				return
			}
			if bktPolicy.IsEmpty() {
				err = globalSiteReplicationSys.PeerBucketPolicyHandler(ctx, item.Bucket, nil, item.UpdatedAt)
			} else {
				err = globalSiteReplicationSys.PeerBucketPolicyHandler(ctx, item.Bucket, bktPolicy, item.UpdatedAt)
			}
		}
	case madmin.SRBucketMetaTypeQuotaConfig:
		if item.Quota == nil {
			err = globalSiteReplicationSys.PeerBucketQuotaConfigHandler(ctx, item.Bucket, nil, item.UpdatedAt)
		} else {
			quotaConfig, err := parseBucketQuota(item.Bucket, item.Quota)
			if err != nil {
				writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
				return
			}
			if err = globalSiteReplicationSys.PeerBucketQuotaConfigHandler(ctx, item.Bucket, quotaConfig, item.UpdatedAt); err != nil {
				writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
				return
			}
		}
	case madmin.SRBucketMetaTypeVersionConfig:
		err = globalSiteReplicationSys.PeerBucketVersioningHandler(ctx, item.Bucket, item.Versioning, item.UpdatedAt)
	case madmin.SRBucketMetaTypeTags:
		err = globalSiteReplicationSys.PeerBucketTaggingHandler(ctx, item.Bucket, item.Tags, item.UpdatedAt)
	case madmin.SRBucketMetaTypeObjectLockConfig:
		err = globalSiteReplicationSys.PeerBucketObjectLockConfigHandler(ctx, item.Bucket, item.ObjectLockConfig, item.UpdatedAt)
	case madmin.SRBucketMetaTypeSSEConfig:
		err = globalSiteReplicationSys.PeerBucketSSEConfigHandler(ctx, item.Bucket, item.SSEConfig, item.UpdatedAt)
	}
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
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

func (a adminAPIHandlers) SRPeerGetIDPSettings(w http.ResponseWriter, r *http.Request) {
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

func parseJSONBody(ctx context.Context, body io.Reader, v interface{}, encryptionKey string) error {
	data, err := io.ReadAll(body)
	if err != nil {
		return SRError{
			Cause: err,
			Code:  ErrSiteReplicationInvalidRequest,
		}
	}
	if encryptionKey != "" {
		data, err = madmin.DecryptData(encryptionKey, bytes.NewReader(data))
		if err != nil {
			logger.LogIf(ctx, err)
			return SRError{
				Cause: err,
				Code:  ErrSiteReplicationInvalidRequest,
			}
		}
	}
	return json.Unmarshal(data, v)
}

// SiteReplicationStatus - GET /minio/admin/v3/site-replication/status
func (a adminAPIHandlers) SiteReplicationStatus(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SiteReplicationStatus")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.SiteReplicationInfoAction)
	if objectAPI == nil {
		return
	}
	opts := getSRStatusOptions(r)
	// default options to all if status options are unset for backward compatibility
	var dfltOpts madmin.SRStatusOptions
	if opts == dfltOpts {
		opts.Buckets = true
		opts.Users = true
		opts.Policies = true
		opts.Groups = true
	}
	info, err := globalSiteReplicationSys.SiteReplicationStatus(ctx, objectAPI, opts)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err = json.NewEncoder(w).Encode(info); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
}

// SiteReplicationMetaInfo - GET /minio/admin/v3/site-replication/metainfo
func (a adminAPIHandlers) SiteReplicationMetaInfo(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SiteReplicationMetaInfo")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.SiteReplicationInfoAction)
	if objectAPI == nil {
		return
	}

	opts := getSRStatusOptions(r)
	info, err := globalSiteReplicationSys.SiteReplicationMetaInfo(ctx, objectAPI, opts)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err = json.NewEncoder(w).Encode(info); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
}

// SiteReplicationEdit - PUT /minio/admin/v3/site-replication/edit
func (a adminAPIHandlers) SiteReplicationEdit(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SiteReplicationEdit")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, cred := validateAdminReq(ctx, w, r, iampolicy.SiteReplicationAddAction)
	if objectAPI == nil {
		return
	}
	var site madmin.PeerInfo
	err := parseJSONBody(ctx, r.Body, &site, cred.SecretKey)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	status, err := globalSiteReplicationSys.EditPeerCluster(ctx, site)
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	body, err := json.Marshal(status)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessResponseJSON(w, body)
}

// SRPeerEdit - PUT /minio/admin/v3/site-replication/peer/edit
//
// used internally to tell current cluster to update endpoint for peer
func (a adminAPIHandlers) SRPeerEdit(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SRPeerEdit")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.SiteReplicationAddAction)
	if objectAPI == nil {
		return
	}

	var pi madmin.PeerInfo
	if err := parseJSONBody(ctx, r.Body, &pi, ""); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err := globalSiteReplicationSys.PeerEditReq(ctx, pi); err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
}

func getSRStatusOptions(r *http.Request) (opts madmin.SRStatusOptions) {
	q := r.Form
	opts.Buckets = q.Get("buckets") == "true"
	opts.Policies = q.Get("policies") == "true"
	opts.Groups = q.Get("groups") == "true"
	opts.Users = q.Get("users") == "true"
	opts.Entity = madmin.GetSREntityType(q.Get("entity"))
	opts.EntityValue = q.Get("entityvalue")
	opts.ShowDeleted = q.Get("showDeleted") == "true"
	return
}

// SiteReplicationRemove - PUT /minio/admin/v3/site-replication/remove
func (a adminAPIHandlers) SiteReplicationRemove(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SiteReplicationRemove")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.SiteReplicationRemoveAction)
	if objectAPI == nil {
		return
	}
	var rreq madmin.SRRemoveReq
	err := parseJSONBody(ctx, r.Body, &rreq, "")
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	status, err := globalSiteReplicationSys.RemovePeerCluster(ctx, objectAPI, rreq)
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	body, err := json.Marshal(status)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	writeSuccessResponseJSON(w, body)
}

// SRPeerRemove - PUT /minio/admin/v3/site-replication/peer/remove
//
// used internally to tell current cluster to update endpoint for peer
func (a adminAPIHandlers) SRPeerRemove(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SRPeerRemove")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.SiteReplicationRemoveAction)
	if objectAPI == nil {
		return
	}

	var req madmin.SRRemoveReq
	if err := parseJSONBody(ctx, r.Body, &req, ""); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err := globalSiteReplicationSys.InternalRemoveReq(ctx, objectAPI, req); err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
}
