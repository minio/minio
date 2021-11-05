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
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/minio/madmin-go"
	minioClient "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/replication"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/auth"
	sreplication "github.com/minio/minio/internal/bucket/replication"
	"github.com/minio/minio/internal/bucket/versioning"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/bucket/policy"
	iampolicy "github.com/minio/pkg/iam/policy"
)

const (
	srStatePrefix = minioConfigPrefix + "/site-replication"

	srStateFile = "state.json"
)

const (
	srStateFormatVersion1 = 1
)

var (
	errSRCannotJoin     = errors.New("this site is already configured for site-replication")
	errSRDuplicateSites = errors.New("duplicate sites provided for site-replication")
	errSRSelfNotFound   = errors.New("none of the given sites correspond to the current one")
	errSRPeerNotFound   = errors.New("peer not found")
	errSRNotEnabled     = errors.New("site replication is not enabled")
)

func errSRInvalidRequest(err error) SRError {
	return SRError{
		Cause: err,
		Code:  ErrSiteReplicationInvalidRequest,
	}
}

func errSRPeerResp(err error) SRError {
	return SRError{
		Cause: err,
		Code:  ErrSiteReplicationPeerResp,
	}
}

func errSRBackendIssue(err error) SRError {
	return SRError{
		Cause: err,
		Code:  ErrSiteReplicationBackendIssue,
	}
}

func errSRServiceAccount(err error) SRError {
	return SRError{
		Cause: err,
		Code:  ErrSiteReplicationServiceAccountError,
	}
}

func errSRBucketConfigError(err error) SRError {
	return SRError{
		Cause: err,
		Code:  ErrSiteReplicationBucketConfigError,
	}

}

func errSRBucketMetaError(err error) SRError {
	return SRError{
		Cause: err,
		Code:  ErrSiteReplicationBucketMetaError,
	}
}

func errSRIAMError(err error) SRError {
	return SRError{
		Cause: err,
		Code:  ErrSiteReplicationIAMError,
	}
}

var (
	errSRObjectLayerNotReady = SRError{
		Cause: fmt.Errorf("object layer not ready"),
		Code:  ErrServerNotInitialized,
	}
)

func getSRStateFilePath() string {
	return srStatePrefix + SlashSeparator + srStateFile
}

// SRError - wrapped error for site replication.
type SRError struct {
	Cause error
	Code  APIErrorCode
}

func (c SRError) Error() string {
	return c.Cause.Error()
}

func wrapSRErr(err error) SRError {
	return SRError{Cause: err, Code: ErrInternalError}
}

// SiteReplicationSys - manages cluster-level replication.
type SiteReplicationSys struct {
	sync.RWMutex

	enabled bool

	// In-memory and persisted multi-site replication state.
	state srState
}

type srState srStateV1

// srStateV1 represents version 1 of the site replication state persistence
// format.
type srStateV1 struct {
	Name string `json:"name"`

	// Peers maps peers by their deploymentID
	Peers                   map[string]madmin.PeerInfo `json:"peers"`
	ServiceAccountAccessKey string                     `json:"serviceAccountAccessKey"`
}

// srStateData represents the format of the current `srStateFile`.
type srStateData struct {
	Version int `json:"version"`

	SRState srStateV1 `json:"srState"`
}

// Init - initialize the site replication manager.
func (c *SiteReplicationSys) Init(ctx context.Context, objAPI ObjectLayer) error {
	err := c.loadFromDisk(ctx, objAPI)
	if err == errConfigNotFound {
		return nil
	}

	c.RLock()
	defer c.RUnlock()
	if c.enabled {
		logger.Info("Cluster Replication initialized.")
	}

	return err
}

func (c *SiteReplicationSys) loadFromDisk(ctx context.Context, objAPI ObjectLayer) error {
	buf, err := readConfig(ctx, objAPI, getSRStateFilePath())
	if err != nil {
		return err
	}

	// attempt to read just the version key in the state file to ensure we
	// are reading a compatible version.
	var ver struct {
		Version int `json:"version"`
	}
	err = json.Unmarshal(buf, &ver)
	if err != nil {
		return err
	}
	if ver.Version != srStateFormatVersion1 {
		return fmt.Errorf("Unexpected ClusterRepl state version: %d", ver.Version)
	}

	var sdata srStateData
	err = json.Unmarshal(buf, &sdata)
	if err != nil {
		return err
	}

	c.Lock()
	defer c.Unlock()
	c.state = srState(sdata.SRState)
	c.enabled = true
	return nil
}

func (c *SiteReplicationSys) saveToDisk(ctx context.Context, state srState) error {
	sdata := srStateData{
		Version: srStateFormatVersion1,
		SRState: srStateV1(state),
	}
	buf, err := json.Marshal(sdata)
	if err != nil {
		return err
	}

	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return errServerNotInitialized
	}
	err = saveConfig(ctx, objAPI, getSRStateFilePath(), buf)
	if err != nil {
		return err
	}

	for _, e := range globalNotificationSys.ReloadSiteReplicationConfig(ctx) {
		logger.LogIf(ctx, e)
	}

	c.Lock()
	defer c.Unlock()
	c.state = state
	c.enabled = true
	return nil
}

const (
	// Access key of service account used for perform cluster-replication
	// operations.
	siteReplicatorSvcAcc = "site-replicator-0"
)

// AddPeerClusters - add cluster sites for replication configuration.
func (c *SiteReplicationSys) AddPeerClusters(ctx context.Context, sites []madmin.PeerSite) (madmin.ReplicateAddStatus, SRError) {
	// If current cluster is already SR enabled, we fail.
	if c.enabled {
		return madmin.ReplicateAddStatus{}, errSRInvalidRequest(errSRCannotJoin)
	}

	// Only one of the clusters being added, can have any buckets (i.e. self
	// here) - others must be empty.
	selfIdx := -1
	localHasBuckets := false
	nonLocalPeerWithBuckets := ""
	deploymentIDs := make([]string, 0, len(sites))
	deploymentIDsSet := set.NewStringSet()
	for i, v := range sites {
		admClient, err := getAdminClient(v.Endpoint, v.AccessKey, v.SecretKey)
		if err != nil {
			return madmin.ReplicateAddStatus{}, errSRPeerResp(fmt.Errorf("unable to create admin client for %s: %w", v.Name, err))
		}
		info, err := admClient.ServerInfo(ctx)
		if err != nil {
			return madmin.ReplicateAddStatus{}, errSRPeerResp(fmt.Errorf("unable to fetch server info for %s: %w", v.Name, err))
		}

		deploymentID := info.DeploymentID
		if deploymentID == "" {
			return madmin.ReplicateAddStatus{}, errSRPeerResp(fmt.Errorf("unable to fetch deploymentID for %s: value was empty!", v.Name))
		}

		deploymentIDs = append(deploymentIDs, deploymentID)

		// deploymentIDs must be unique
		if deploymentIDsSet.Contains(deploymentID) {
			return madmin.ReplicateAddStatus{}, errSRInvalidRequest(errSRDuplicateSites)
		}
		deploymentIDsSet.Add(deploymentID)

		if deploymentID == globalDeploymentID {
			selfIdx = i
			objAPI := newObjectLayerFn()
			if objAPI == nil {
				return madmin.ReplicateAddStatus{}, errSRObjectLayerNotReady
			}
			res, err := objAPI.ListBuckets(ctx)
			if err != nil {
				return madmin.ReplicateAddStatus{}, errSRBackendIssue(err)
			}
			if len(res) > 0 {
				localHasBuckets = true
			}
			continue
		}

		s3Client, err := getS3Client(v)
		if err != nil {
			return madmin.ReplicateAddStatus{}, errSRPeerResp(fmt.Errorf("unable to create s3 client for %s: %w", v.Name, err))
		}
		buckets, err := s3Client.ListBuckets(ctx)
		if err != nil {
			return madmin.ReplicateAddStatus{}, errSRPeerResp(fmt.Errorf("unable to list buckets for %s: %v", v.Name, err))
		}

		if len(buckets) > 0 {
			nonLocalPeerWithBuckets = v.Name
		}
	}

	// For this `add` API, either all clusters must be empty or the local
	// cluster must be the only one having some buckets.

	if localHasBuckets && nonLocalPeerWithBuckets != "" {
		return madmin.ReplicateAddStatus{}, errSRInvalidRequest(errors.New("Only one cluster may have data when configuring site replication"))
	}

	if !localHasBuckets && nonLocalPeerWithBuckets != "" {
		return madmin.ReplicateAddStatus{}, errSRInvalidRequest(fmt.Errorf("Please send your request to the cluster containing data/buckets: %s", nonLocalPeerWithBuckets))
	}

	// validate that all clusters are using the same (LDAP based)
	// external IDP.
	pass, verr := c.validateIDPSettings(ctx, sites, selfIdx)
	if verr.Cause != nil {
		return madmin.ReplicateAddStatus{}, verr
	}
	if !pass {
		return madmin.ReplicateAddStatus{}, errSRInvalidRequest(fmt.Errorf("All cluster sites must have the same (LDAP) IDP settings."))
	}

	// FIXME: Ideally, we also need to check if there are any global IAM
	// policies and any (LDAP user created) service accounts on the other
	// peer clusters, and if so, reject the cluster replicate add request.
	// This is not yet implemented.

	// VALIDATIONS COMPLETE.

	// Create a common service account for all clusters, with root
	// permissions.

	// Create a local service account.

	// Generate a secret key for the service account.
	var secretKey string
	_, secretKey, err := auth.GenerateCredentials()
	if err != nil {
		return madmin.ReplicateAddStatus{}, SRError{
			Cause: err,
			Code:  ErrInternalError,
		}
	}

	svcCred, err := globalIAMSys.NewServiceAccount(ctx, sites[selfIdx].AccessKey, nil, newServiceAccountOpts{
		accessKey: siteReplicatorSvcAcc,
		secretKey: secretKey,
	})
	if err != nil {
		return madmin.ReplicateAddStatus{}, errSRServiceAccount(fmt.Errorf("unable to create local service account: %w", err))
	}

	// Notify all other Minio peers to reload user the service account
	if !globalIAMSys.HasWatcher() {
		for _, nerr := range globalNotificationSys.LoadServiceAccount(svcCred.AccessKey) {
			if nerr.Err != nil {
				logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
				logger.LogIf(ctx, nerr.Err)
			}
		}
	}

	joinReq := madmin.SRInternalJoinReq{
		SvcAcctAccessKey: svcCred.AccessKey,
		SvcAcctSecretKey: svcCred.SecretKey,
		Peers:            make(map[string]madmin.PeerInfo),
	}
	for i, v := range sites {
		joinReq.Peers[deploymentIDs[i]] = madmin.PeerInfo{
			Endpoint:     v.Endpoint,
			Name:         v.Name,
			DeploymentID: deploymentIDs[i],
		}
	}

	addedCount := 0
	var peerAddErr SRError
	for i, v := range sites {
		if i == selfIdx {
			continue
		}
		admClient, err := getAdminClient(v.Endpoint, v.AccessKey, v.SecretKey)
		if err != nil {
			peerAddErr = errSRPeerResp(fmt.Errorf("unable to create admin client for %s: %w", v.Name, err))
			break
		}
		joinReq.SvcAcctParent = v.AccessKey
		err = admClient.SRInternalJoin(ctx, joinReq)
		if err != nil {
			peerAddErr = errSRPeerResp(fmt.Errorf("unable to link with peer %s: %w", v.Name, err))
			break
		}
		addedCount++
	}

	if peerAddErr.Cause != nil {
		if addedCount == 0 {
			return madmin.ReplicateAddStatus{}, peerAddErr
		}
		// In this case, it means at least one cluster was added
		// successfully, we need to send a response to the client with
		// some details - FIXME: the disks on this cluster would need to
		// be cleaned to recover.
		partial := madmin.ReplicateAddStatus{
			Status:    madmin.ReplicateAddStatusPartial,
			ErrDetail: peerAddErr.Error(),
		}
		return partial, SRError{}
	}

	// Other than handling existing buckets, we can now save the cluster
	// replication configuration state.
	state := srState{
		Name:                    sites[selfIdx].Name,
		Peers:                   joinReq.Peers,
		ServiceAccountAccessKey: svcCred.AccessKey,
	}
	err = c.saveToDisk(ctx, state)
	if err != nil {
		return madmin.ReplicateAddStatus{
			Status:    madmin.ReplicateAddStatusPartial,
			ErrDetail: fmt.Sprintf("unable to save cluster-replication state on local: %v", err),
		}, SRError{}
	}

	result := madmin.ReplicateAddStatus{
		Success: true,
		Status:  madmin.ReplicateAddStatusSuccess,
	}
	initialSyncErr := c.syncLocalToPeers(ctx)
	if initialSyncErr.Code != ErrNone {
		result.InitialSyncErrorMessage = initialSyncErr.Error()
	}

	return result, SRError{}
}

// InternalJoinReq - internal API handler to respond to a peer cluster's request
// to join.
func (c *SiteReplicationSys) InternalJoinReq(ctx context.Context, arg madmin.SRInternalJoinReq) SRError {
	if c.enabled {
		return errSRInvalidRequest(errSRCannotJoin)
	}

	var ourName string
	for d, p := range arg.Peers {
		if d == globalDeploymentID {
			ourName = p.Name
			break
		}
	}
	if ourName == "" {
		return errSRInvalidRequest(errSRSelfNotFound)
	}

	svcCred, err := globalIAMSys.NewServiceAccount(ctx, arg.SvcAcctParent, nil, newServiceAccountOpts{
		accessKey: arg.SvcAcctAccessKey,
		secretKey: arg.SvcAcctSecretKey,
	})
	if err != nil {
		return errSRServiceAccount(fmt.Errorf("unable to create service account on %s: %v", ourName, err))
	}

	// Notify all other Minio peers to reload the service account
	if !globalIAMSys.HasWatcher() {
		for _, nerr := range globalNotificationSys.LoadServiceAccount(svcCred.AccessKey) {
			if nerr.Err != nil {
				logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
				logger.LogIf(ctx, nerr.Err)
			}
		}
	}

	state := srState{
		Name:                    ourName,
		Peers:                   arg.Peers,
		ServiceAccountAccessKey: arg.SvcAcctAccessKey,
	}
	err = c.saveToDisk(ctx, state)
	if err != nil {
		return errSRBackendIssue(fmt.Errorf("unable to save cluster-replication state to disk on %s: %v", ourName, err))
	}
	return SRError{}
}

// GetIDPSettings returns info about the configured identity provider. It is
// used to validate that all peers have the same IDP.
func (c *SiteReplicationSys) GetIDPSettings(ctx context.Context) madmin.IDPSettings {
	return madmin.IDPSettings{
		IsLDAPEnabled:          globalLDAPConfig.Enabled,
		LDAPUserDNSearchBase:   globalLDAPConfig.UserDNSearchBaseDN,
		LDAPUserDNSearchFilter: globalLDAPConfig.UserDNSearchFilter,
		LDAPGroupSearchBase:    globalLDAPConfig.GroupSearchBaseDistName,
		LDAPGroupSearchFilter:  globalLDAPConfig.GroupSearchFilter,
	}
}

func (c *SiteReplicationSys) validateIDPSettings(ctx context.Context, peers []madmin.PeerSite, selfIdx int) (bool, SRError) {
	s := make([]madmin.IDPSettings, 0, len(peers))
	for i, v := range peers {
		if i == selfIdx {
			s = append(s, c.GetIDPSettings(ctx))
			continue
		}

		admClient, err := getAdminClient(v.Endpoint, v.AccessKey, v.SecretKey)
		if err != nil {
			return false, errSRPeerResp(fmt.Errorf("unable to create admin client for %s: %w", v.Name, err))
		}

		is, err := admClient.SRInternalGetIDPSettings(ctx)
		if err != nil {
			return false, errSRPeerResp(fmt.Errorf("unable to fetch IDP settings from %s: %v", v.Name, err))
		}
		s = append(s, is)
	}

	for _, v := range s {
		if !v.IsLDAPEnabled {
			return false, SRError{}
		}
	}
	for i := 1; i < len(s); i++ {
		if s[i] != s[0] {
			return false, SRError{}
		}
	}
	return true, SRError{}
}

// GetClusterInfo - returns site replication information.
func (c *SiteReplicationSys) GetClusterInfo(ctx context.Context) (info madmin.SiteReplicationInfo, err error) {
	c.RLock()
	defer c.RUnlock()
	if !c.enabled {
		return info, nil
	}

	info.Enabled = true
	info.Name = c.state.Name
	info.Sites = make([]madmin.PeerInfo, 0, len(c.state.Peers))
	for _, peer := range c.state.Peers {
		info.Sites = append(info.Sites, peer)
	}
	sort.SliceStable(info.Sites, func(i, j int) bool {
		return info.Sites[i].Name < info.Sites[j].Name
	})

	info.ServiceAccountAccessKey = c.state.ServiceAccountAccessKey
	return info, nil
}

// MakeBucketHook - called during a regular make bucket call when cluster
// replication is enabled. It is responsible for the creation of the same bucket
// on remote clusters, and creating replication rules on local and peer
// clusters.
func (c *SiteReplicationSys) MakeBucketHook(ctx context.Context, bucket string, opts BucketOptions) error {
	// At this point, the local bucket is created.

	c.RLock()
	defer c.RUnlock()
	if !c.enabled {
		return nil
	}

	optsMap := make(map[string]string)
	if opts.Location != "" {
		optsMap["location"] = opts.Location
	}
	if opts.LockEnabled {
		optsMap["lockEnabled"] = ""
	}

	// Create bucket and enable versioning on all peers.
	makeBucketConcErr := c.concDo(
		func() error {
			err := c.PeerBucketMakeWithVersioningHandler(ctx, bucket, opts)
			logger.LogIf(ctx, c.annotateErr("MakeWithVersioning", err))
			return err
		},
		func(deploymentID string, p madmin.PeerInfo) error {
			admClient, err := c.getAdminClient(ctx, deploymentID)
			if err != nil {
				return err
			}

			err = admClient.SRInternalBucketOps(ctx, bucket, madmin.MakeWithVersioningBktOp, optsMap)
			logger.LogIf(ctx, c.annotatePeerErr(p.Name, "MakeWithVersioning", err))
			return err
		},
	)
	// If all make-bucket-and-enable-versioning operations failed, nothing
	// more to do.
	if makeBucketConcErr.allFailed() {
		return makeBucketConcErr
	}

	// Log any errors in make-bucket operations.
	logger.LogIf(ctx, makeBucketConcErr.summaryErr)

	// Create bucket remotes and add replication rules for the bucket on
	// self and peers.
	makeRemotesConcErr := c.concDo(
		func() error {
			err := c.PeerBucketConfigureReplHandler(ctx, bucket)
			logger.LogIf(ctx, c.annotateErr("ConfigureRepl", err))
			return err
		},
		func(deploymentID string, p madmin.PeerInfo) error {
			admClient, err := c.getAdminClient(ctx, deploymentID)
			if err != nil {
				return err
			}

			err = admClient.SRInternalBucketOps(ctx, bucket, madmin.ConfigureReplBktOp, nil)
			logger.LogIf(ctx, c.annotatePeerErr(p.Name, "ConfigureRepl", err))
			return err
		},
	)
	err := makeRemotesConcErr.summaryErr
	if err != nil {
		return err
	}

	return nil
}

// DeleteBucketHook - called during a regular delete bucket call when cluster
// replication is enabled. It is responsible for the deletion of the same bucket
// on remote clusters.
func (c *SiteReplicationSys) DeleteBucketHook(ctx context.Context, bucket string, forceDelete bool) error {
	// At this point, the local bucket is deleted.

	c.RLock()
	defer c.RUnlock()
	if !c.enabled {
		return nil
	}

	op := madmin.DeleteBucketBktOp
	if forceDelete {
		op = madmin.ForceDeleteBucketBktOp
	}

	// Send bucket delete to other clusters.
	cErr := c.concDo(nil, func(deploymentID string, p madmin.PeerInfo) error {
		admClient, err := c.getAdminClient(ctx, deploymentID)
		if err != nil {
			return wrapSRErr(err)
		}

		err = admClient.SRInternalBucketOps(ctx, bucket, op, nil)
		logger.LogIf(ctx, c.annotatePeerErr(p.Name, "DeleteBucket", err))
		return err
	})
	return cErr.summaryErr
}

// PeerBucketMakeWithVersioningHandler - creates bucket and enables versioning.
func (c *SiteReplicationSys) PeerBucketMakeWithVersioningHandler(ctx context.Context, bucket string, opts BucketOptions) error {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return errServerNotInitialized
	}
	err := objAPI.MakeBucketWithLocation(ctx, bucket, opts)
	if err != nil {
		// Check if this is a bucket exists error.
		_, ok1 := err.(BucketExists)
		_, ok2 := err.(BucketAlreadyExists)
		if !ok1 && !ok2 {
			logger.LogIf(ctx, c.annotateErr("MakeBucketErr on peer call", err))
			return wrapSRErr(err)
		}
	} else {
		// Load updated bucket metadata into memory as new
		// bucket was created.
		globalNotificationSys.LoadBucketMetadata(GlobalContext, bucket)
	}

	// Enable versioning on the bucket.
	config, err := globalBucketVersioningSys.Get(bucket)
	if err != nil {
		return wrapSRErr(err)
	}
	if !config.Enabled() {
		verConf := versioning.Versioning{
			Status: versioning.Enabled,
		}
		// FIXME: need to confirm if skipping object lock and
		// versioning-suspended state checks are valid here.
		cfgData, err := xml.Marshal(verConf)
		if err != nil {
			return wrapSRErr(err)
		}
		err = globalBucketMetadataSys.Update(bucket, bucketVersioningConfig, cfgData)
		if err != nil {
			logger.LogIf(ctx, c.annotateErr("Versioning enabling error on peer call", err))
			return wrapSRErr(err)
		}
	}
	return nil
}

// PeerBucketConfigureReplHandler - configures replication remote and
// replication rules to all other peers for the local bucket.
func (c *SiteReplicationSys) PeerBucketConfigureReplHandler(ctx context.Context, bucket string) error {
	creds, err := c.getPeerCreds()
	if err != nil {
		return wrapSRErr(err)
	}

	// The following function, creates a bucket remote and sets up a bucket
	// replication rule for the given peer.
	configurePeerFn := func(d string, peer madmin.PeerInfo) error {
		ep, _ := url.Parse(peer.Endpoint)
		targets := globalBucketTargetSys.ListTargets(ctx, bucket, string(madmin.ReplicationService))
		targetARN := ""
		for _, target := range targets {
			if target.SourceBucket == bucket &&
				target.TargetBucket == bucket &&
				target.Endpoint == ep.Host &&
				target.Secure == (ep.Scheme == "https") &&
				target.Type == madmin.ReplicationService {
				targetARN = target.Arn
				break
			}
		}
		if targetARN == "" {
			bucketTarget := madmin.BucketTarget{
				SourceBucket: bucket,
				Endpoint:     ep.Host,
				Credentials: &madmin.Credentials{
					AccessKey: creds.AccessKey,
					SecretKey: creds.SecretKey,
				},
				TargetBucket:    bucket,
				Secure:          ep.Scheme == "https",
				API:             "s3v4",
				Type:            madmin.ReplicationService,
				Region:          "",
				ReplicationSync: false,
			}
			bucketTarget.Arn = globalBucketTargetSys.getRemoteARN(bucket, &bucketTarget)
			err := globalBucketTargetSys.SetTarget(ctx, bucket, &bucketTarget, false)
			if err != nil {
				logger.LogIf(ctx, c.annotatePeerErr(peer.Name, "Bucket target creation error", err))
				return err
			}
			targets, err := globalBucketTargetSys.ListBucketTargets(ctx, bucket)
			if err != nil {
				return err
			}
			tgtBytes, err := json.Marshal(&targets)
			if err != nil {
				return err
			}
			if err = globalBucketMetadataSys.Update(bucket, bucketTargetsFile, tgtBytes); err != nil {
				return err
			}
			targetARN = bucketTarget.Arn
		}

		// Create bucket replication rule to this peer.

		// To add the bucket replication rule, we fetch the current
		// server configuration, and convert it to minio-go's
		// replication configuration type (by converting to xml and
		// parsing it back), use minio-go's add rule function, and
		// finally convert it back to the server type (again via xml).
		// This is needed as there is no add-rule function in the server
		// yet.

		// Though we do not check if the rule already exists, this is
		// not a problem as we are always using the same replication
		// rule ID - if the rule already exists, it is just replaced.
		replicationConfigS, err := globalBucketMetadataSys.GetReplicationConfig(ctx, bucket)
		if err != nil {
			_, ok := err.(BucketReplicationConfigNotFound)
			if !ok {
				return err
			}
		}
		var replicationConfig replication.Config
		if replicationConfigS != nil {
			replCfgSBytes, err := xml.Marshal(replicationConfigS)
			if err != nil {
				return err
			}
			err = xml.Unmarshal(replCfgSBytes, &replicationConfig)
			if err != nil {
				return err
			}
		}
		err = replicationConfig.AddRule(replication.Options{
			// Set the ID so we can identify the rule as being
			// created for site-replication and include the
			// destination cluster's deployment ID.
			ID: fmt.Sprintf("site-repl-%s", d),

			// Use a helper to generate unique priority numbers.
			Priority: fmt.Sprintf("%d", getPriorityHelper(replicationConfig)),

			Op:         replication.AddOption,
			RuleStatus: "enable",
			DestBucket: targetARN,

			// Replicate everything!
			ReplicateDeletes:        "enable",
			ReplicateDeleteMarkers:  "enable",
			ReplicaSync:             "enable",
			ExistingObjectReplicate: "enable",
		})
		if err != nil {
			logger.LogIf(ctx, c.annotatePeerErr(peer.Name, "Error adding bucket replication rule", err))
			return err
		}
		// Now convert the configuration back to server's type so we can
		// do some validation.
		newReplCfgBytes, err := xml.Marshal(replicationConfig)
		if err != nil {
			return err
		}
		newReplicationConfig, err := sreplication.ParseConfig(bytes.NewReader(newReplCfgBytes))
		if err != nil {
			return err
		}
		sameTarget, apiErr := validateReplicationDestination(ctx, bucket, newReplicationConfig)
		if apiErr != noError {
			return fmt.Errorf("bucket replication config validation error: %#v", apiErr)
		}
		err = newReplicationConfig.Validate(bucket, sameTarget)
		if err != nil {
			return err
		}
		// Config looks good, so we save it.
		replCfgData, err := xml.Marshal(newReplicationConfig)
		if err != nil {
			return err
		}
		err = globalBucketMetadataSys.Update(bucket, bucketReplicationConfig, replCfgData)
		logger.LogIf(ctx, c.annotatePeerErr(peer.Name, "Error updating replication configuration", err))
		return err
	}

	errMap := make(map[string]error, len(c.state.Peers))
	for d, peer := range c.state.Peers {
		if d == globalDeploymentID {
			continue
		}
		if err := configurePeerFn(d, peer); err != nil {
			errMap[d] = err
		}
	}
	return c.toErrorFromErrMap(errMap)
}

// PeerBucketDeleteHandler - deletes bucket on local in response to a delete
// bucket request from a peer.
func (c *SiteReplicationSys) PeerBucketDeleteHandler(ctx context.Context, bucket string, forceDelete bool) error {
	c.RLock()
	defer c.RUnlock()
	if !c.enabled {
		return errSRNotEnabled
	}

	// FIXME: need to handle cases where globalDNSConfig is set.

	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return errServerNotInitialized
	}
	err := objAPI.DeleteBucket(ctx, bucket, DeleteBucketOptions{Force: forceDelete})
	if err != nil {
		return err
	}

	globalNotificationSys.DeleteBucketMetadata(ctx, bucket)

	return nil
}

// IAMChangeHook - called when IAM items need to be replicated to peer clusters.
// This includes named policy creation, policy mapping changes and service
// account changes.
//
// All policies are replicated.
//
// Policy mappings are only replicated when they are for LDAP users or groups
// (as an external IDP is always assumed when SR is used). In the case of
// OpenID, such mappings are provided from the IDP directly and so are not
// applicable here.
//
// Only certain service accounts can be replicated:
//
// Service accounts created for STS credentials using an external IDP: such STS
// credentials would be valid on the peer clusters as they are assumed to be
// using the same external IDP. Service accounts when using internal IDP or for
// root user will not be replicated.
//
// STS accounts are replicated, but only if the session token is verifiable
// using the local cluster's root credential.
func (c *SiteReplicationSys) IAMChangeHook(ctx context.Context, item madmin.SRIAMItem) error {
	// The IAM item has already been applied to the local cluster at this
	// point, and only needs to be updated on all remote peer clusters.

	c.RLock()
	defer c.RUnlock()
	if !c.enabled {
		return nil
	}

	cErr := c.concDo(nil, func(d string, p madmin.PeerInfo) error {
		admClient, err := c.getAdminClient(ctx, d)
		if err != nil {
			return wrapSRErr(err)
		}

		err = admClient.SRInternalReplicateIAMItem(ctx, item)
		logger.LogIf(ctx, c.annotatePeerErr(p.Name, "SRInternalReplicateIAMItem", err))
		return err
	})
	return cErr.summaryErr
}

// PeerAddPolicyHandler - copies IAM policy to local. A nil policy argument,
// causes the named policy to be deleted.
func (c *SiteReplicationSys) PeerAddPolicyHandler(ctx context.Context, policyName string, p *iampolicy.Policy) error {
	var err error
	if p == nil {
		err = globalIAMSys.DeletePolicy(policyName)
	} else {
		err = globalIAMSys.SetPolicy(policyName, *p)
	}
	if err != nil {
		return wrapSRErr(err)
	}

	if p != nil {
		if !globalIAMSys.HasWatcher() {
			// Notify all other MinIO peers to reload policy
			for _, nerr := range globalNotificationSys.LoadPolicy(policyName) {
				if nerr.Err != nil {
					logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
					logger.LogIf(ctx, nerr.Err)
				}
			}
		}
		return nil
	}

	// Notify all other MinIO peers to delete policy
	for _, nerr := range globalNotificationSys.DeletePolicy(policyName) {
		if nerr.Err != nil {
			logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
			logger.LogIf(ctx, nerr.Err)
		}
	}
	return nil
}

// PeerSvcAccChangeHandler - copies service-account change to local.
func (c *SiteReplicationSys) PeerSvcAccChangeHandler(ctx context.Context, change madmin.SRSvcAccChange) error {
	switch {
	case change.Create != nil:
		var sp *iampolicy.Policy
		var err error
		if len(change.Create.SessionPolicy) > 0 {
			sp, err = iampolicy.ParseConfig(bytes.NewReader(change.Create.SessionPolicy))
			if err != nil {
				return wrapSRErr(err)
			}
		}

		opts := newServiceAccountOpts{
			accessKey:     change.Create.AccessKey,
			secretKey:     change.Create.SecretKey,
			sessionPolicy: sp,
			claims:        change.Create.Claims,
		}
		newCred, err := globalIAMSys.NewServiceAccount(ctx, change.Create.Parent, change.Create.Groups, opts)
		if err != nil {
			return wrapSRErr(err)
		}

		// Notify all other Minio peers to reload the service account
		if !globalIAMSys.HasWatcher() {
			for _, nerr := range globalNotificationSys.LoadServiceAccount(newCred.AccessKey) {
				if nerr.Err != nil {
					logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
					logger.LogIf(ctx, nerr.Err)
				}
			}
		}
	case change.Update != nil:
		var sp *iampolicy.Policy
		var err error
		if len(change.Update.SessionPolicy) > 0 {
			sp, err = iampolicy.ParseConfig(bytes.NewReader(change.Update.SessionPolicy))
			if err != nil {
				return wrapSRErr(err)
			}
		}
		opts := updateServiceAccountOpts{
			secretKey:     change.Update.SecretKey,
			status:        change.Update.Status,
			sessionPolicy: sp,
		}

		err = globalIAMSys.UpdateServiceAccount(ctx, change.Update.AccessKey, opts)
		if err != nil {
			return wrapSRErr(err)
		}

		// Notify all other Minio peers to reload the service account
		if !globalIAMSys.HasWatcher() {
			for _, nerr := range globalNotificationSys.LoadServiceAccount(change.Update.AccessKey) {
				if nerr.Err != nil {
					logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
					logger.LogIf(ctx, nerr.Err)
				}
			}
		}

	case change.Delete != nil:
		err := globalIAMSys.DeleteServiceAccount(ctx, change.Delete.AccessKey)
		if err != nil {
			return wrapSRErr(err)
		}

		for _, nerr := range globalNotificationSys.DeleteServiceAccount(change.Delete.AccessKey) {
			if nerr.Err != nil {
				logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
				logger.LogIf(ctx, nerr.Err)
			}
		}

	}

	return nil
}

// PeerPolicyMappingHandler - copies policy mapping to local.
func (c *SiteReplicationSys) PeerPolicyMappingHandler(ctx context.Context, mapping madmin.SRPolicyMapping) error {
	err := globalIAMSys.PolicyDBSet(mapping.UserOrGroup, mapping.Policy, mapping.IsGroup)
	if err != nil {
		return wrapSRErr(err)
	}

	// Notify all other MinIO peers to reload policy
	if !globalIAMSys.HasWatcher() {
		for _, nerr := range globalNotificationSys.LoadPolicyMapping(mapping.UserOrGroup, mapping.IsGroup) {
			if nerr.Err != nil {
				logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
				logger.LogIf(ctx, nerr.Err)
			}
		}
	}
	return nil
}

// PeerSTSAccHandler - replicates STS credential locally.
func (c *SiteReplicationSys) PeerSTSAccHandler(ctx context.Context, stsCred madmin.SRSTSCredential) error {
	// Verify the session token of the stsCred
	claims, err := auth.ExtractClaims(stsCred.SessionToken, globalActiveCred.SecretKey)
	if err != nil {
		logger.LogIf(ctx, err)
		return fmt.Errorf("STS credential could not be verified")
	}

	mapClaims := claims.Map()
	expiry, err := auth.ExpToInt64(mapClaims["exp"])
	if err != nil {
		return fmt.Errorf("Expiry claim was not found")
	}

	// Extract the username and lookup DN and groups in LDAP.
	ldapUser, ok := claims.Lookup(ldapUserN)
	if !ok {
		return fmt.Errorf("Could not find LDAP username in claims")
	}

	// Need to lookup the groups from LDAP.
	ldapUserDN, ldapGroups, err := globalLDAPConfig.LookupUserDN(ldapUser)
	if err != nil {
		return fmt.Errorf("unable to query LDAP server for %s: %v", ldapUser, err)
	}

	cred := auth.Credentials{
		AccessKey:    stsCred.AccessKey,
		SecretKey:    stsCred.SecretKey,
		Expiration:   time.Unix(expiry, 0).UTC(),
		SessionToken: stsCred.SessionToken,
		Status:       auth.AccountOn,
		ParentUser:   ldapUserDN,
		Groups:       ldapGroups,
	}

	// Set these credentials to IAM.
	if err := globalIAMSys.SetTempUser(cred.AccessKey, cred, ""); err != nil {
		return fmt.Errorf("unable to save STS credential: %v", err)
	}

	// Notify in-cluster peers to reload temp users.
	if !globalIAMSys.HasWatcher() {
		for _, nerr := range globalNotificationSys.LoadUser(cred.AccessKey, true) {
			if nerr.Err != nil {
				logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
				logger.LogIf(ctx, nerr.Err)
			}
		}
	}

	return nil
}

// BucketMetaHook - called when bucket meta changes happen and need to be
// replicated to peer clusters.
func (c *SiteReplicationSys) BucketMetaHook(ctx context.Context, item madmin.SRBucketMeta) error {
	// The change has already been applied to the local cluster at this
	// point, and only needs to be updated on all remote peer clusters.

	c.RLock()
	defer c.RUnlock()
	if !c.enabled {
		return nil
	}

	cErr := c.concDo(nil, func(d string, p madmin.PeerInfo) error {
		admClient, err := c.getAdminClient(ctx, d)
		if err != nil {
			return wrapSRErr(err)
		}

		err = admClient.SRInternalReplicateBucketMeta(ctx, item)
		logger.LogIf(ctx, c.annotatePeerErr(p.Name, "SRInternalReplicateBucketMeta", err))
		return err
	})
	return cErr.summaryErr
}

// PeerBucketPolicyHandler - copies/deletes policy to local cluster.
func (c *SiteReplicationSys) PeerBucketPolicyHandler(ctx context.Context, bucket string, policy *policy.Policy) error {
	if policy != nil {
		configData, err := json.Marshal(policy)
		if err != nil {
			return wrapSRErr(err)
		}

		err = globalBucketMetadataSys.Update(bucket, bucketPolicyConfig, configData)
		if err != nil {
			return wrapSRErr(err)
		}
		return nil
	}

	// Delete the bucket policy
	err := globalBucketMetadataSys.Update(bucket, bucketPolicyConfig, nil)
	if err != nil {
		return wrapSRErr(err)
	}

	return nil
}

// PeerBucketTaggingHandler - copies/deletes tags to local cluster.
func (c *SiteReplicationSys) PeerBucketTaggingHandler(ctx context.Context, bucket string, tags *string) error {
	if tags != nil {
		configData, err := base64.StdEncoding.DecodeString(*tags)
		if err != nil {
			return wrapSRErr(err)
		}
		err = globalBucketMetadataSys.Update(bucket, bucketTaggingConfig, configData)
		if err != nil {
			return wrapSRErr(err)
		}
		return nil
	}

	// Delete the tags
	err := globalBucketMetadataSys.Update(bucket, bucketTaggingConfig, nil)
	if err != nil {
		return wrapSRErr(err)
	}

	return nil
}

// PeerBucketObjectLockConfigHandler - sets object lock on local bucket.
func (c *SiteReplicationSys) PeerBucketObjectLockConfigHandler(ctx context.Context, bucket string, objectLockData *string) error {
	if objectLockData != nil {
		configData, err := base64.StdEncoding.DecodeString(*objectLockData)
		if err != nil {
			return wrapSRErr(err)
		}
		err = globalBucketMetadataSys.Update(bucket, objectLockConfig, configData)
		if err != nil {
			return wrapSRErr(err)
		}
		return nil
	}

	return nil
}

// PeerBucketSSEConfigHandler - copies/deletes SSE config to local cluster.
func (c *SiteReplicationSys) PeerBucketSSEConfigHandler(ctx context.Context, bucket string, sseConfig *string) error {
	if sseConfig != nil {
		configData, err := base64.StdEncoding.DecodeString(*sseConfig)
		if err != nil {
			return wrapSRErr(err)
		}
		err = globalBucketMetadataSys.Update(bucket, bucketSSEConfig, configData)
		if err != nil {
			return wrapSRErr(err)
		}
		return nil
	}

	// Delete sse config
	err := globalBucketMetadataSys.Update(bucket, bucketSSEConfig, nil)
	if err != nil {
		return wrapSRErr(err)
	}
	return nil
}

// getAdminClient - NOTE: ensure to take at least a read lock on SiteReplicationSys
// before calling this.
func (c *SiteReplicationSys) getAdminClient(ctx context.Context, deploymentID string) (*madmin.AdminClient, error) {
	creds, err := c.getPeerCreds()
	if err != nil {
		return nil, err
	}

	peer, ok := c.state.Peers[deploymentID]
	if !ok {
		return nil, errSRPeerNotFound
	}

	return getAdminClient(peer.Endpoint, creds.AccessKey, creds.SecretKey)
}

func (c *SiteReplicationSys) getPeerCreds() (*auth.Credentials, error) {
	creds, ok := globalIAMSys.store.GetUser(c.state.ServiceAccountAccessKey)
	if !ok {
		return nil, errors.New("site replication service account not found!")
	}
	return &creds, nil
}

// syncLocalToPeers is used when initially configuring site replication, to
// copy existing buckets, their settings, service accounts and policies to all
// new peers.
func (c *SiteReplicationSys) syncLocalToPeers(ctx context.Context) SRError {
	// If local has buckets, enable versioning on them, create them on peers
	// and setup replication rules.
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return errSRObjectLayerNotReady
	}
	buckets, err := objAPI.ListBuckets(ctx)
	if err != nil {
		return errSRBackendIssue(err)
	}
	for _, bucketInfo := range buckets {
		bucket := bucketInfo.Name

		// MinIO does not store bucket location - so we just check if
		// object locking is enabled.
		lockConfig, err := globalBucketMetadataSys.GetObjectLockConfig(bucket)
		if err != nil {
			if _, ok := err.(BucketObjectLockConfigNotFound); !ok {
				return errSRBackendIssue(err)
			}
		}

		var opts BucketOptions
		if lockConfig != nil {
			opts.LockEnabled = lockConfig.ObjectLockEnabled == "Enabled"
		}

		// Now call the MakeBucketHook on existing bucket - this will
		// create buckets and replication rules on peer clusters.
		err = c.MakeBucketHook(ctx, bucket, opts)
		if err != nil {
			return errSRBucketConfigError(err)
		}

		// Replicate bucket policy if present.
		policy, err := globalPolicySys.Get(bucket)
		found := true
		if _, ok := err.(BucketPolicyNotFound); ok {
			found = false
		} else if err != nil {
			return errSRBackendIssue(err)
		}
		if found {
			policyJSON, err := json.Marshal(policy)
			if err != nil {
				return wrapSRErr(err)
			}
			err = c.BucketMetaHook(ctx, madmin.SRBucketMeta{
				Type:   madmin.SRBucketMetaTypePolicy,
				Bucket: bucket,
				Policy: policyJSON,
			})
			if err != nil {
				return errSRBucketMetaError(err)
			}
		}

		// Replicate bucket tags if present.
		tags, err := globalBucketMetadataSys.GetTaggingConfig(bucket)
		found = true
		if _, ok := err.(BucketTaggingNotFound); ok {
			found = false
		} else if err != nil {
			return errSRBackendIssue(err)
		}
		if found {
			tagCfg, err := xml.Marshal(tags)
			if err != nil {
				return wrapSRErr(err)
			}
			tagCfgStr := base64.StdEncoding.EncodeToString(tagCfg)
			err = c.BucketMetaHook(ctx, madmin.SRBucketMeta{
				Type:   madmin.SRBucketMetaTypeTags,
				Bucket: bucket,
				Tags:   &tagCfgStr,
			})
			if err != nil {
				return errSRBucketMetaError(err)
			}
		}

		// Replicate object-lock config if present.
		objLockCfg, err := globalBucketMetadataSys.GetObjectLockConfig(bucket)
		found = true
		if _, ok := err.(BucketObjectLockConfigNotFound); ok {
			found = false
		} else if err != nil {
			return errSRBackendIssue(err)
		}
		if found {
			objLockCfgData, err := xml.Marshal(objLockCfg)
			if err != nil {
				return wrapSRErr(err)
			}
			objLockStr := base64.StdEncoding.EncodeToString(objLockCfgData)
			err = c.BucketMetaHook(ctx, madmin.SRBucketMeta{
				Type:   madmin.SRBucketMetaTypeObjectLockConfig,
				Bucket: bucket,
				Tags:   &objLockStr,
			})
			if err != nil {
				return errSRBucketMetaError(err)
			}
		}

		// Replicate existing bucket bucket encryption settings
		sseConfig, err := globalBucketMetadataSys.GetSSEConfig(bucket)
		found = true
		if _, ok := err.(BucketSSEConfigNotFound); ok {
			found = false
		} else if err != nil {
			return errSRBackendIssue(err)
		}
		if found {
			sseConfigData, err := xml.Marshal(sseConfig)
			if err != nil {
				return wrapSRErr(err)
			}
			sseConfigStr := base64.StdEncoding.EncodeToString(sseConfigData)
			err = c.BucketMetaHook(ctx, madmin.SRBucketMeta{
				Type:      madmin.SRBucketMetaTypeSSEConfig,
				Bucket:    bucket,
				SSEConfig: &sseConfigStr,
			})
			if err != nil {
				return errSRBucketMetaError(err)
			}
		}
	}

	{
		// Replicate IAM policies on local to all peers.
		allPolicies, err := globalIAMSys.ListPolicies("")
		if err != nil {
			return errSRBackendIssue(err)
		}

		for pname, policy := range allPolicies {
			policyJSON, err := json.Marshal(policy)
			if err != nil {
				return wrapSRErr(err)
			}
			err = c.IAMChangeHook(ctx, madmin.SRIAMItem{
				Type:   madmin.SRIAMItemPolicy,
				Name:   pname,
				Policy: policyJSON,
			})
			if err != nil {
				return errSRIAMError(err)
			}
		}
	}

	{
		// Replicate policy mappings on local to all peers.
		userPolicyMap := make(map[string]MappedPolicy)
		groupPolicyMap := make(map[string]MappedPolicy)
		globalIAMSys.store.rlock()
		errU := globalIAMSys.store.loadMappedPolicies(ctx, stsUser, false, userPolicyMap)
		errG := globalIAMSys.store.loadMappedPolicies(ctx, stsUser, true, groupPolicyMap)
		globalIAMSys.store.runlock()
		if errU != nil {
			return errSRBackendIssue(errU)
		}
		if errG != nil {
			return errSRBackendIssue(errG)
		}

		for user, mp := range userPolicyMap {
			err := c.IAMChangeHook(ctx, madmin.SRIAMItem{
				Type: madmin.SRIAMItemPolicyMapping,
				PolicyMapping: &madmin.SRPolicyMapping{
					UserOrGroup: user,
					IsGroup:     false,
					Policy:      mp.Policies,
				},
			})
			if err != nil {
				return errSRIAMError(err)
			}
		}

		for group, mp := range groupPolicyMap {
			err := c.IAMChangeHook(ctx, madmin.SRIAMItem{
				Type: madmin.SRIAMItemPolicyMapping,
				PolicyMapping: &madmin.SRPolicyMapping{
					UserOrGroup: group,
					IsGroup:     true,
					Policy:      mp.Policies,
				},
			})
			if err != nil {
				return errSRIAMError(err)
			}
		}
	}

	{
		// Check for service accounts and replicate them. Only LDAP user
		// owned service accounts are supported for this operation.
		serviceAccounts := make(map[string]auth.Credentials)
		globalIAMSys.store.rlock()
		err := globalIAMSys.store.loadUsers(ctx, svcUser, serviceAccounts)
		globalIAMSys.store.runlock()
		if err != nil {
			return errSRBackendIssue(err)
		}
		for user, acc := range serviceAccounts {
			claims, err := globalIAMSys.GetClaimsForSvcAcc(ctx, acc.AccessKey)
			if err != nil {
				return errSRBackendIssue(err)
			}
			if _, isLDAPAccount := claims[ldapUserN]; !isLDAPAccount {
				continue
			}
			_, policy, err := globalIAMSys.GetServiceAccount(ctx, acc.AccessKey)
			if err != nil {
				return errSRBackendIssue(err)
			}
			var policyJSON []byte
			if policy != nil {
				policyJSON, err = json.Marshal(policy)
				if err != nil {
					return wrapSRErr(err)
				}
			}
			err = c.IAMChangeHook(ctx, madmin.SRIAMItem{
				Type: madmin.SRIAMItemSvcAcc,
				SvcAccChange: &madmin.SRSvcAccChange{
					Create: &madmin.SRSvcAccCreate{
						Parent:        acc.ParentUser,
						AccessKey:     user,
						SecretKey:     acc.SecretKey,
						Groups:        acc.Groups,
						Claims:        claims,
						SessionPolicy: json.RawMessage(policyJSON),
						Status:        acc.Status,
					},
				},
			})
			if err != nil {
				return errSRIAMError(err)
			}
		}
	}

	return SRError{}
}

// Concurrency helpers

type concErr struct {
	numActions int
	errMap     map[string]error
	summaryErr error
}

func (c concErr) Error() string {
	return c.summaryErr.Error()
}

func (c concErr) allFailed() bool {
	return len(c.errMap) == c.numActions
}

func (c *SiteReplicationSys) toErrorFromErrMap(errMap map[string]error) error {
	if len(errMap) == 0 {
		return nil
	}

	msgs := []string{}
	for d, err := range errMap {
		name := c.state.Peers[d].Name
		msgs = append(msgs, fmt.Sprintf("Site %s (%s): %v", name, d, err))
	}
	return fmt.Errorf("Site replication error(s): %s", strings.Join(msgs, "; "))
}

func (c *SiteReplicationSys) newConcErr(numActions int, errMap map[string]error) concErr {
	return concErr{
		numActions: numActions,
		errMap:     errMap,
		summaryErr: c.toErrorFromErrMap(errMap),
	}
}

// concDo calls actions concurrently. selfActionFn is run for the current
// cluster and peerActionFn is run for each peer replication cluster.
func (c *SiteReplicationSys) concDo(selfActionFn func() error, peerActionFn func(deploymentID string, p madmin.PeerInfo) error) concErr {
	depIDs := make([]string, 0, len(c.state.Peers))
	for d := range c.state.Peers {
		depIDs = append(depIDs, d)
	}
	errs := make([]error, len(c.state.Peers))
	var wg sync.WaitGroup
	for i := range depIDs {
		wg.Add(1)
		go func(i int) {
			if depIDs[i] == globalDeploymentID {
				if selfActionFn != nil {
					errs[i] = selfActionFn()
				}
			} else {
				errs[i] = peerActionFn(depIDs[i], c.state.Peers[depIDs[i]])
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	errMap := make(map[string]error, len(c.state.Peers))
	for i, depID := range depIDs {
		if errs[i] != nil {
			errMap[depID] = errs[i]
		}
	}
	numActions := len(c.state.Peers) - 1
	if selfActionFn != nil {
		numActions++
	}
	return c.newConcErr(numActions, errMap)
}

func (c *SiteReplicationSys) annotateErr(annotation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %s: %v", c.state.Name, annotation, err)
}

func (c *SiteReplicationSys) annotatePeerErr(dstPeer string, annotation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s->%s: %s: %v", c.state.Name, dstPeer, annotation, err)
}

// Other helpers

// newRemoteClusterHTTPTransport returns a new http configuration
// used while communicating with the remote cluster.
func newRemoteClusterHTTPTransport() *http.Transport {
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		TLSClientConfig: &tls.Config{
			RootCAs: globalRootCAs,
		},
	}
	return tr
}

func getAdminClient(endpoint, accessKey, secretKey string) (*madmin.AdminClient, error) {
	epURL, _ := url.Parse(endpoint)
	client, err := madmin.New(epURL.Host, accessKey, secretKey, epURL.Scheme == "https")
	if err != nil {
		return nil, err
	}
	client.SetCustomTransport(newRemoteClusterHTTPTransport())
	return client, nil
}

func getS3Client(pc madmin.PeerSite) (*minioClient.Client, error) {
	ep, _ := url.Parse(pc.Endpoint)
	return minioClient.New(ep.Host, &minioClient.Options{
		Creds:     credentials.NewStaticV4(pc.AccessKey, pc.SecretKey, ""),
		Secure:    ep.Scheme == "https",
		Transport: newRemoteClusterHTTPTransport(),
	})
}

func getPriorityHelper(replicationConfig replication.Config) int {
	maxPrio := 0
	for _, rule := range replicationConfig.Rules {
		if rule.Priority > maxPrio {
			maxPrio = rule.Priority
		}
	}

	// leave some gaps in priority numbers for flexibility
	return maxPrio + 10
}
