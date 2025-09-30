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
	"encoding/gob"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/madmin-go/v3"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/mux"
	"github.com/minio/pkg/v3/policy"
)

// SiteReplicationAdd - PUT /minio/admin/v3/site-replication/add
func (a adminAPIHandlers) SiteReplicationAdd(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, cred := validateAdminReq(ctx, w, r, policy.SiteReplicationAddAction)
	if objectAPI == nil {
		return
	}

	var sites []madmin.PeerSite
	if err := parseJSONBody(ctx, r.Body, &sites, cred.SecretKey); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	opts := getSRAddOptions(r)
	status, err := globalSiteReplicationSys.AddPeerClusters(ctx, sites, opts)
	if err != nil {
		adminLogIf(ctx, err)
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

func getSRAddOptions(r *http.Request) (opts madmin.SRAddOptions) {
	opts.ReplicateILMExpiry = r.Form.Get("replicateILMExpiry") == "true"
	return opts
}

// SRPeerJoin - PUT /minio/admin/v3/site-replication/join
//
// used internally to tell current cluster to enable SR with
// the provided peer clusters and service account.
func (a adminAPIHandlers) SRPeerJoin(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, cred := validateAdminReq(ctx, w, r, policy.SiteReplicationAddAction)
	if objectAPI == nil {
		return
	}

	var joinArg madmin.SRPeerJoinReq
	if err := parseJSONBody(ctx, r.Body, &joinArg, cred.SecretKey); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err := globalSiteReplicationSys.PeerJoinReq(ctx, joinArg); err != nil {
		adminLogIf(ctx, err)
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
}

// SRPeerBucketOps - PUT /minio/admin/v3/site-replication/bucket-ops?bucket=x&operation=y
func (a adminAPIHandlers) SRPeerBucketOps(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.SiteReplicationOperationAction)
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
		createdAt, cerr := time.Parse(time.RFC3339Nano, strings.TrimSpace(r.Form.Get("createdAt")))
		if cerr != nil {
			createdAt = timeSentinel
		}

		opts := MakeBucketOptions{
			LockEnabled:       r.Form.Get("lockEnabled") == "true",
			VersioningEnabled: r.Form.Get("versioningEnabled") == "true",
			ForceCreate:       r.Form.Get("forceCreate") == "true",
			CreatedAt:         createdAt,
		}
		err = globalSiteReplicationSys.PeerBucketMakeWithVersioningHandler(ctx, bucket, opts)
	case madmin.ConfigureReplBktOp:
		err = globalSiteReplicationSys.PeerBucketConfigureReplHandler(ctx, bucket)
	case madmin.DeleteBucketBktOp, madmin.ForceDeleteBucketBktOp:
		err = globalSiteReplicationSys.PeerBucketDeleteHandler(ctx, bucket, DeleteBucketOptions{
			Force:      operation == madmin.ForceDeleteBucketBktOp,
			SRDeleteOp: getSRBucketDeleteOp(true),
		})
	case madmin.PurgeDeletedBucketOp:
		globalSiteReplicationSys.purgeDeletedBucket(ctx, objectAPI, bucket)
	}
	if err != nil {
		adminLogIf(ctx, err)
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
}

// SRPeerReplicateIAMItem - PUT /minio/admin/v3/site-replication/iam-item
func (a adminAPIHandlers) SRPeerReplicateIAMItem(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.SiteReplicationOperationAction)
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
			policy, perr := policy.ParseConfig(bytes.NewReader(item.Policy))
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
		adminLogIf(ctx, err)
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
}

// SRPeerReplicateBucketItem - PUT /minio/admin/v3/site-replication/peer/bucket-meta
func (a adminAPIHandlers) SRPeerReplicateBucketItem(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.SiteReplicationOperationAction)
	if objectAPI == nil {
		return
	}

	var item madmin.SRBucketMeta
	if err := parseJSONBody(ctx, r.Body, &item, ""); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if item.Bucket == "" {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errSRInvalidRequest(errInvalidArgument)), r.URL)
		return
	}

	var err error
	switch item.Type {
	default:
		err = globalSiteReplicationSys.PeerBucketMetadataUpdateHandler(ctx, item)
	case madmin.SRBucketMetaTypePolicy:
		if item.Policy == nil {
			err = globalSiteReplicationSys.PeerBucketPolicyHandler(ctx, item.Bucket, nil, item.UpdatedAt)
		} else {
			bktPolicy, berr := policy.ParseBucketPolicyConfig(bytes.NewReader(item.Policy), item.Bucket)
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
				writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
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
	case madmin.SRBucketMetaLCConfig:
		err = globalSiteReplicationSys.PeerBucketLCConfigHandler(ctx, item.Bucket, item.ExpiryLCConfig, item.UpdatedAt)
	}
	if err != nil {
		adminLogIf(ctx, err)
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
}

// SiteReplicationInfo - GET /minio/admin/v3/site-replication/info
func (a adminAPIHandlers) SiteReplicationInfo(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.SiteReplicationInfoAction)
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
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.SiteReplicationAddAction)
	if objectAPI == nil {
		return
	}

	idpSettings := globalSiteReplicationSys.GetIDPSettings(ctx)
	if err := json.NewEncoder(w).Encode(idpSettings); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
}

func parseJSONBody(ctx context.Context, body io.Reader, v any, encryptionKey string) error {
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
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.SiteReplicationInfoAction)
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
		opts.ILMExpiryRules = true
	}
	info, err := globalSiteReplicationSys.SiteReplicationStatus(ctx, objectAPI, opts)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	// Report the ILMExpiryStats only if at least one site has replication of ILM expiry enabled
	var replicateILMExpiry bool
	for _, site := range info.Sites {
		if site.ReplicateILMExpiry {
			replicateILMExpiry = true
			break
		}
	}
	if !replicateILMExpiry {
		// explicitly send nil for ILMExpiryStats
		info.ILMExpiryStats = nil
	}

	if err = json.NewEncoder(w).Encode(info); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
}

// SiteReplicationMetaInfo - GET /minio/admin/v3/site-replication/metainfo
func (a adminAPIHandlers) SiteReplicationMetaInfo(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.SiteReplicationInfoAction)
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
	ctx := r.Context()

	objectAPI, cred := validateAdminReq(ctx, w, r, policy.SiteReplicationAddAction)
	if objectAPI == nil {
		return
	}
	var site madmin.PeerInfo
	err := parseJSONBody(ctx, r.Body, &site, cred.SecretKey)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	opts := getSREditOptions(r)
	status, err := globalSiteReplicationSys.EditPeerCluster(ctx, site, opts)
	if err != nil {
		adminLogIf(ctx, err)
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

func getSREditOptions(r *http.Request) (opts madmin.SREditOptions) {
	opts.DisableILMExpiryReplication = r.Form.Get("disableILMExpiryReplication") == "true"
	opts.EnableILMExpiryReplication = r.Form.Get("enableILMExpiryReplication") == "true"
	return opts
}

// SRPeerEdit - PUT /minio/admin/v3/site-replication/peer/edit
//
// used internally to tell current cluster to update endpoint for peer
func (a adminAPIHandlers) SRPeerEdit(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.SiteReplicationAddAction)
	if objectAPI == nil {
		return
	}

	var pi madmin.PeerInfo
	if err := parseJSONBody(ctx, r.Body, &pi, ""); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err := globalSiteReplicationSys.PeerEditReq(ctx, pi); err != nil {
		adminLogIf(ctx, err)
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
}

// SRStateEdit - PUT /minio/admin/v3/site-replication/state/edit
//
// used internally to tell current cluster to update site replication state
func (a adminAPIHandlers) SRStateEdit(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.SiteReplicationOperationAction)
	if objectAPI == nil {
		return
	}

	var state madmin.SRStateEditReq
	if err := parseJSONBody(ctx, r.Body, &state, ""); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	if err := globalSiteReplicationSys.PeerStateEditReq(ctx, state); err != nil {
		adminLogIf(ctx, err)
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
	opts.ILMExpiryRules = q.Get("ilm-expiry-rules") == "true"
	opts.PeerState = q.Get("peer-state") == "true"
	opts.Entity = madmin.GetSREntityType(q.Get("entity"))
	opts.EntityValue = q.Get("entityvalue")
	opts.ShowDeleted = q.Get("showDeleted") == "true"
	opts.Metrics = q.Get("metrics") == "true"
	return opts
}

// SiteReplicationRemove - PUT /minio/admin/v3/site-replication/remove
func (a adminAPIHandlers) SiteReplicationRemove(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.SiteReplicationRemoveAction)
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
		adminLogIf(ctx, err)
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
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.SiteReplicationRemoveAction)
	if objectAPI == nil {
		return
	}

	var req madmin.SRRemoveReq
	if err := parseJSONBody(ctx, r.Body, &req, ""); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err := globalSiteReplicationSys.InternalRemoveReq(ctx, objectAPI, req); err != nil {
		adminLogIf(ctx, err)
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
}

// SiteReplicationResyncOp - PUT /minio/admin/v3/site-replication/resync/op
func (a adminAPIHandlers) SiteReplicationResyncOp(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.SiteReplicationResyncAction)
	if objectAPI == nil {
		return
	}

	var peerSite madmin.PeerInfo
	if err := parseJSONBody(ctx, r.Body, &peerSite, ""); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	vars := mux.Vars(r)
	op := madmin.SiteResyncOp(vars["operation"])
	var (
		status madmin.SRResyncOpStatus
		err    error
	)
	switch op {
	case madmin.SiteResyncStart:
		status, err = globalSiteReplicationSys.startResync(ctx, objectAPI, peerSite)
	case madmin.SiteResyncCancel:
		status, err = globalSiteReplicationSys.cancelResync(ctx, objectAPI, peerSite)
	default:
		err = errSRInvalidRequest(errInvalidArgument)
	}
	if err != nil {
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

// SiteReplicationDevNull - everything goes to io.Discard
// [POST] /minio/admin/v3/site-replication/devnull
func (a adminAPIHandlers) SiteReplicationDevNull(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	globalSiteNetPerfRX.Connect()
	defer globalSiteNetPerfRX.Disconnect()

	connectTime := time.Now()
	for {
		n, err := io.CopyN(xioutil.Discard, r.Body, 128*humanize.KiByte)
		atomic.AddUint64(&globalSiteNetPerfRX.RX, uint64(n))
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			// If there is a disconnection before globalNetPerfMinDuration (we give a margin of error of 1 sec)
			// would mean the network is not stable. Logging here will help in debugging network issues.
			if time.Since(connectTime) < (globalNetPerfMinDuration - time.Second) {
				adminLogIf(ctx, err)
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				w.WriteHeader(http.StatusNoContent)
			} else {
				w.WriteHeader(http.StatusBadRequest)
			}
			break
		}
	}
}

// SiteReplicationNetPerf - everything goes to io.Discard
// [POST] /minio/admin/v3/site-replication/netperf
func (a adminAPIHandlers) SiteReplicationNetPerf(w http.ResponseWriter, r *http.Request) {
	durationStr := r.Form.Get(peerRESTDuration)
	duration, _ := time.ParseDuration(durationStr)
	if duration < globalNetPerfMinDuration {
		duration = globalNetPerfMinDuration
	}
	result := siteNetperf(r.Context(), duration)
	adminLogIf(r.Context(), gob.NewEncoder(w).Encode(result))
}
