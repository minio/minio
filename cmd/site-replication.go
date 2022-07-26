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
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"net/url"
	"reflect"
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
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/bucket/policy"
	bktpolicy "github.com/minio/pkg/bucket/policy"
	iampolicy "github.com/minio/pkg/iam/policy"
)

const (
	srStatePrefix = minioConfigPrefix + "/site-replication"
	srStateFile   = "state.json"
)

const (
	srStateFormatVersion1 = 1
)

var (
	errSRCannotJoin = SRError{
		Cause: errors.New("this site is already configured for site-replication"),
		Code:  ErrSiteReplicationInvalidRequest,
	}
	errSRDuplicateSites = SRError{
		Cause: errors.New("duplicate sites provided for site-replication"),
		Code:  ErrSiteReplicationInvalidRequest,
	}
	errSRSelfNotFound = SRError{
		Cause: errors.New("none of the given sites correspond to the current one"),
		Code:  ErrSiteReplicationInvalidRequest,
	}
	errSRPeerNotFound = SRError{
		Cause: errors.New("peer not found"),
		Code:  ErrSiteReplicationInvalidRequest,
	}
	errSRNotEnabled = SRError{
		Cause: errors.New("site replication is not enabled"),
		Code:  ErrSiteReplicationInvalidRequest,
	}
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

func errSRConfigMissingError(err error) SRError {
	return SRError{
		Cause: err,
		Code:  ErrSiteReplicationConfigMissing,
	}
}

var errSRObjectLayerNotReady = SRError{
	Cause: fmt.Errorf("object layer not ready"),
	Code:  ErrServerNotInitialized,
}

func getSRStateFilePath() string {
	return srStatePrefix + SlashSeparator + srStateFile
}

// SRError - wrapped error for site replication.
type SRError struct {
	Cause error
	Code  APIErrorCode
}

func (c SRError) Error() string {
	if c.Cause != nil {
		return c.Cause.Error()
	}
	return "<nil>"
}

func (c SRError) Unwrap() error {
	return c.Cause
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
	go c.startHealRoutine(ctx, objAPI)

	err := c.loadFromDisk(ctx, objAPI)
	if err == errConfigNotFound {
		return nil
	}

	c.RLock()
	defer c.RUnlock()
	if c.enabled {
		logger.Info("Cluster replication initialized")
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

	if err = saveConfig(ctx, objAPI, getSRStateFilePath(), buf); err != nil {
		return err
	}

	for _, err := range globalNotificationSys.ReloadSiteReplicationConfig(ctx) {
		logger.LogIf(ctx, err)
	}

	c.Lock()
	defer c.Unlock()
	c.state = state
	c.enabled = len(c.state.Peers) != 0
	return nil
}

const (
	// Access key of service account used for perform cluster-replication
	// operations.
	siteReplicatorSvcAcc = "site-replicator-0"
)

// PeerSiteInfo is a wrapper struct around madmin.PeerSite with extra info on site status
type PeerSiteInfo struct {
	madmin.PeerSite
	self         bool
	DeploymentID string
	Replicated   bool // true if already participating in site replication
	Empty        bool // true if cluster has no buckets
}

// getSiteStatuses gathers more info on the sites being added
func (c *SiteReplicationSys) getSiteStatuses(ctx context.Context, sites ...madmin.PeerSite) (psi []PeerSiteInfo, err error) {
	psi = make([]PeerSiteInfo, 0, len(sites))
	for _, v := range sites {
		admClient, err := getAdminClient(v.Endpoint, v.AccessKey, v.SecretKey)
		if err != nil {
			return psi, errSRPeerResp(fmt.Errorf("unable to create admin client for %s: %w", v.Name, err))
		}

		info, err := admClient.ServerInfo(ctx)
		if err != nil {
			return psi, errSRPeerResp(fmt.Errorf("unable to fetch server info for %s: %w", v.Name, err))
		}

		s3Client, err := getS3Client(v)
		if err != nil {
			return psi, errSRPeerResp(fmt.Errorf("unable to create s3 client for %s: %w", v.Name, err))
		}

		buckets, err := s3Client.ListBuckets(ctx)
		if err != nil {
			return psi, errSRPeerResp(fmt.Errorf("unable to list buckets for %s: %v", v.Name, err))
		}

		psi = append(psi, PeerSiteInfo{
			PeerSite:     v,
			DeploymentID: info.DeploymentID,
			Empty:        len(buckets) == 0,
			self:         info.DeploymentID == globalDeploymentID,
		})
	}
	return
}

// AddPeerClusters - add cluster sites for replication configuration.
func (c *SiteReplicationSys) AddPeerClusters(ctx context.Context, psites []madmin.PeerSite) (madmin.ReplicateAddStatus, error) {
	sites, serr := c.getSiteStatuses(ctx, psites...)
	if serr != nil {
		return madmin.ReplicateAddStatus{}, serr
	}
	var (
		currSites            madmin.SiteReplicationInfo
		currDeploymentIDsSet = set.NewStringSet()
		err                  error
	)
	currSites, err = c.GetClusterInfo(ctx)
	if err != nil {
		return madmin.ReplicateAddStatus{}, errSRBackendIssue(err)
	}
	for _, v := range currSites.Sites {
		currDeploymentIDsSet.Add(v.DeploymentID)
	}
	deploymentIDsSet := set.NewStringSet()
	localHasBuckets := false
	nonLocalPeerWithBuckets := ""
	selfIdx := -1
	for i, v := range sites {
		// deploymentIDs must be unique
		if deploymentIDsSet.Contains(v.DeploymentID) {
			return madmin.ReplicateAddStatus{}, errSRDuplicateSites
		}
		deploymentIDsSet.Add(v.DeploymentID)

		if v.self {
			selfIdx = i
			localHasBuckets = !v.Empty
			continue
		}
		if !v.Empty && !currDeploymentIDsSet.Contains(v.DeploymentID) {
			nonLocalPeerWithBuckets = v.Name
		}
	}
	if !currDeploymentIDsSet.IsEmpty() {
		// If current cluster is already SR enabled and no new site being added ,fail.
		if currDeploymentIDsSet.Equals(deploymentIDsSet) {
			return madmin.ReplicateAddStatus{}, errSRCannotJoin
		}
		if len(currDeploymentIDsSet.Intersection(deploymentIDsSet)) != len(currDeploymentIDsSet) {
			diffSlc := getMissingSiteNames(currDeploymentIDsSet, deploymentIDsSet, currSites.Sites)
			return madmin.ReplicateAddStatus{}, errSRInvalidRequest(fmt.Errorf("all existing replicated sites must be specified - missing %s", strings.Join(diffSlc, " ")))
		}
	}

	// validate that all clusters are using the same IDP settings.
	pass, err := c.validateIDPSettings(ctx, sites)
	if err != nil {
		return madmin.ReplicateAddStatus{}, err
	}
	if !pass {
		return madmin.ReplicateAddStatus{}, errSRInvalidRequest(errors.New("all cluster sites must have the same IAM/IDP settings"))
	}

	// For this `add` API, either all clusters must be empty or the local
	// cluster must be the only one having some buckets.
	if localHasBuckets && nonLocalPeerWithBuckets != "" {
		return madmin.ReplicateAddStatus{}, errSRInvalidRequest(errors.New("only one cluster may have data when configuring site replication"))
	}

	if !localHasBuckets && nonLocalPeerWithBuckets != "" {
		return madmin.ReplicateAddStatus{}, errSRInvalidRequest(fmt.Errorf("please send your request to the cluster containing data/buckets: %s", nonLocalPeerWithBuckets))
	}

	// FIXME: Ideally, we also need to check if there are any global IAM
	// policies and any (LDAP user created) service accounts on the other
	// peer clusters, and if so, reject the cluster replicate add request.
	// This is not yet implemented.

	// VALIDATIONS COMPLETE.

	// Create a common service account for all clusters, with root
	// permissions.

	// Create a local service account.

	// Generate a secret key for the service account if not created already.
	var secretKey string
	var svcCred auth.Credentials
	sa, _, err := globalIAMSys.getServiceAccount(ctx, siteReplicatorSvcAcc)
	switch {
	case err == errNoSuchServiceAccount:
		_, secretKey, err = auth.GenerateCredentials()
		if err != nil {
			return madmin.ReplicateAddStatus{}, errSRServiceAccount(fmt.Errorf("unable to create local service account: %w", err))
		}
		svcCred, _, err = globalIAMSys.NewServiceAccount(ctx, sites[selfIdx].AccessKey, nil, newServiceAccountOpts{
			accessKey: siteReplicatorSvcAcc,
			secretKey: secretKey,
		})
		if err != nil {
			return madmin.ReplicateAddStatus{}, errSRServiceAccount(fmt.Errorf("unable to create local service account: %w", err))
		}
	case err == nil:
		svcCred = sa.Credentials
		secretKey = svcCred.SecretKey
	default:
		return madmin.ReplicateAddStatus{}, errSRBackendIssue(err)
	}

	joinReq := madmin.SRPeerJoinReq{
		SvcAcctAccessKey: svcCred.AccessKey,
		SvcAcctSecretKey: secretKey,
		Peers:            make(map[string]madmin.PeerInfo),
	}

	for _, v := range sites {
		joinReq.Peers[v.DeploymentID] = madmin.PeerInfo{
			Endpoint:     v.Endpoint,
			Name:         v.Name,
			DeploymentID: v.DeploymentID,
		}
	}

	addedCount := 0
	var (
		peerAddErr error
		admClient  *madmin.AdminClient
	)

	for _, v := range sites {
		if v.self {
			continue
		}
		switch {
		case currDeploymentIDsSet.Contains(v.DeploymentID):
			admClient, err = c.getAdminClient(ctx, v.DeploymentID)
		default:
			admClient, err = getAdminClient(v.Endpoint, v.AccessKey, v.SecretKey)
		}
		if err != nil {
			peerAddErr = errSRPeerResp(fmt.Errorf("unable to create admin client for %s: %w", v.Name, err))
			break
		}
		joinReq.SvcAcctParent = v.AccessKey
		err = admClient.SRPeerJoin(ctx, joinReq)
		if err != nil {
			peerAddErr = errSRPeerResp(fmt.Errorf("unable to link with peer %s: %w", v.Name, err))
			break
		}
		addedCount++
	}

	if peerAddErr != nil {
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

		return partial, nil
	}

	// Other than handling existing buckets, we can now save the cluster
	// replication configuration state.
	state := srState{
		Name:                    sites[selfIdx].Name,
		Peers:                   joinReq.Peers,
		ServiceAccountAccessKey: svcCred.AccessKey,
	}

	if err = c.saveToDisk(ctx, state); err != nil {
		return madmin.ReplicateAddStatus{
			Status:    madmin.ReplicateAddStatusPartial,
			ErrDetail: fmt.Sprintf("unable to save cluster-replication state on local: %v", err),
		}, nil
	}

	result := madmin.ReplicateAddStatus{
		Success: true,
		Status:  madmin.ReplicateAddStatusSuccess,
	}

	if err := c.syncToAllPeers(ctx); err != nil {
		result.InitialSyncErrorMessage = err.Error()
	}

	return result, nil
}

// PeerJoinReq - internal API handler to respond to a peer cluster's request
// to join.
func (c *SiteReplicationSys) PeerJoinReq(ctx context.Context, arg madmin.SRPeerJoinReq) error {
	var ourName string
	for d, p := range arg.Peers {
		if d == globalDeploymentID {
			ourName = p.Name
			break
		}
	}
	if ourName == "" {
		return errSRSelfNotFound
	}

	_, _, err := globalIAMSys.GetServiceAccount(ctx, arg.SvcAcctAccessKey)
	if err == errNoSuchServiceAccount {
		_, _, err = globalIAMSys.NewServiceAccount(ctx, arg.SvcAcctParent, nil, newServiceAccountOpts{
			accessKey: arg.SvcAcctAccessKey,
			secretKey: arg.SvcAcctSecretKey,
		})
	}
	if err != nil {
		return errSRServiceAccount(fmt.Errorf("unable to create service account on %s: %v", ourName, err))
	}

	state := srState{
		Name:                    ourName,
		Peers:                   arg.Peers,
		ServiceAccountAccessKey: arg.SvcAcctAccessKey,
	}
	if err = c.saveToDisk(ctx, state); err != nil {
		return errSRBackendIssue(fmt.Errorf("unable to save cluster-replication state to disk on %s: %v", ourName, err))
	}
	return nil
}

// GetIDPSettings returns info about the configured identity provider. It is
// used to validate that all peers have the same IDP.
func (c *SiteReplicationSys) GetIDPSettings(ctx context.Context) madmin.IDPSettings {
	s := madmin.IDPSettings{}
	s.LDAP = madmin.LDAPSettings{
		IsLDAPEnabled:          globalLDAPConfig.Enabled,
		LDAPUserDNSearchBase:   globalLDAPConfig.UserDNSearchBaseDistName,
		LDAPUserDNSearchFilter: globalLDAPConfig.UserDNSearchFilter,
		LDAPGroupSearchBase:    globalLDAPConfig.GroupSearchBaseDistName,
		LDAPGroupSearchFilter:  globalLDAPConfig.GroupSearchFilter,
	}
	s.OpenID = globalOpenIDConfig.GetSettings()
	if s.OpenID.Enabled {
		s.OpenID.Region = globalSite.Region
	}
	return s
}

func (c *SiteReplicationSys) validateIDPSettings(ctx context.Context, peers []PeerSiteInfo) (bool, error) {
	s := make([]madmin.IDPSettings, 0, len(peers))
	for _, v := range peers {
		if v.self {
			s = append(s, c.GetIDPSettings(ctx))
			continue
		}

		admClient, err := getAdminClient(v.Endpoint, v.AccessKey, v.SecretKey)
		if err != nil {
			return false, errSRPeerResp(fmt.Errorf("unable to create admin client for %s: %w", v.Name, err))
		}

		is, err := admClient.SRPeerGetIDPSettings(ctx)
		if err != nil {
			return false, errSRPeerResp(fmt.Errorf("unable to fetch IDP settings from %s: %v", v.Name, err))
		}
		s = append(s, is)
	}

	for i := 1; i < len(s); i++ {
		if !reflect.DeepEqual(s[i], s[0]) {
			return false, nil
		}
	}
	return true, nil
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

const (
	makeBucketWithVersion   = "MakeBucketWithVersioning"
	configureReplication    = "ConfigureReplication"
	deleteBucket            = "DeleteBucket"
	replicateIAMItem        = "SRPeerReplicateIAMItem"
	replicateBucketMetadata = "SRPeerReplicateBucketMeta"
)

// MakeBucketHook - called during a regular make bucket call when cluster
// replication is enabled. It is responsible for the creation of the same bucket
// on remote clusters, and creating replication rules on local and peer
// clusters.
func (c *SiteReplicationSys) MakeBucketHook(ctx context.Context, bucket string, opts MakeBucketOptions) error {
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
		optsMap["lockEnabled"] = "true"
		optsMap["versioningEnabled"] = "true"
	}
	if opts.VersioningEnabled {
		optsMap["versioningEnabled"] = "true"
	}
	if opts.ForceCreate {
		optsMap["forceCreate"] = "true"
	}
	createdAt, _ := globalBucketMetadataSys.CreatedAt(bucket)
	optsMap["createdAt"] = createdAt.Format(time.RFC3339Nano)
	opts.CreatedAt = createdAt

	// Create bucket and enable versioning on all peers.
	makeBucketConcErr := c.concDo(
		func() error {
			return c.annotateErr(makeBucketWithVersion, c.PeerBucketMakeWithVersioningHandler(ctx, bucket, opts))
		},
		func(deploymentID string, p madmin.PeerInfo) error {
			admClient, err := c.getAdminClient(ctx, deploymentID)
			if err != nil {
				return err
			}

			return c.annotatePeerErr(p.Name, makeBucketWithVersion, admClient.SRPeerBucketOps(ctx, bucket, madmin.MakeWithVersioningBktOp, optsMap))
		},
		makeBucketWithVersion,
	)

	// Create bucket remotes and add replication rules for the bucket on self and peers.
	makeRemotesConcErr := c.concDo(
		func() error {
			return c.annotateErr(configureReplication, c.PeerBucketConfigureReplHandler(ctx, bucket))
		},
		func(deploymentID string, p madmin.PeerInfo) error {
			admClient, err := c.getAdminClient(ctx, deploymentID)
			if err != nil {
				return err
			}

			return c.annotatePeerErr(p.Name, configureReplication, admClient.SRPeerBucketOps(ctx, bucket, madmin.ConfigureReplBktOp, nil))
		},
		configureReplication,
	)

	if err := errors.Unwrap(makeBucketConcErr); err != nil {
		return err
	}

	if err := errors.Unwrap(makeRemotesConcErr); err != nil {
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
	cerr := c.concDo(nil, func(deploymentID string, p madmin.PeerInfo) error {
		admClient, err := c.getAdminClient(ctx, deploymentID)
		if err != nil {
			return wrapSRErr(err)
		}

		return c.annotatePeerErr(p.Name, deleteBucket, admClient.SRPeerBucketOps(ctx, bucket, op, nil))
	},
		deleteBucket,
	)
	return errors.Unwrap(cerr)
}

// PeerBucketMakeWithVersioningHandler - creates bucket and enables versioning.
func (c *SiteReplicationSys) PeerBucketMakeWithVersioningHandler(ctx context.Context, bucket string, opts MakeBucketOptions) error {
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
			return wrapSRErr(c.annotateErr(makeBucketWithVersion, err))
		}
	} else {
		// Load updated bucket metadata into memory as new
		// bucket was created.
		globalNotificationSys.LoadBucketMetadata(GlobalContext, bucket)
	}

	meta, err := globalBucketMetadataSys.Get(bucket)
	if err != nil {
		return wrapSRErr(c.annotateErr(makeBucketWithVersion, err))
	}

	meta.SetCreatedAt(opts.CreatedAt)

	meta.VersioningConfigXML = enabledBucketVersioningConfig
	if opts.LockEnabled {
		meta.ObjectLockConfigXML = enabledBucketObjectLockConfig
	}

	if err := meta.Save(context.Background(), objAPI); err != nil {
		return wrapSRErr(err)
	}

	globalBucketMetadataSys.Set(bucket, meta)

	// Load updated bucket metadata into memory as new metadata updated.
	globalNotificationSys.LoadBucketMetadata(GlobalContext, bucket)
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
				return c.annotatePeerErr(peer.Name, "Bucket target creation error", err)
			}
			targets, err := globalBucketTargetSys.ListBucketTargets(ctx, bucket)
			if err != nil {
				return err
			}
			tgtBytes, err := json.Marshal(&targets)
			if err != nil {
				return err
			}
			if _, err = globalBucketMetadataSys.Update(ctx, bucket, bucketTargetsFile, tgtBytes); err != nil {
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
		replicationConfigS, _, err := globalBucketMetadataSys.GetReplicationConfig(ctx, bucket)
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
		var (
			ruleID  = fmt.Sprintf("site-repl-%s", d)
			hasRule bool
			opts    = replication.Options{
				// Set the ID so we can identify the rule as being
				// created for site-replication and include the
				// destination cluster's deployment ID.
				ID: ruleID,

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
			}
		)
		ruleARN := targetARN
		for _, r := range replicationConfig.Rules {
			if r.ID == ruleID {
				hasRule = true
				ruleARN = r.Destination.Bucket
			}
		}
		switch {
		case hasRule:
			if ruleARN != opts.DestBucket {
				// remove stale replication rule and replace rule with correct target ARN
				if len(replicationConfig.Rules) > 1 {
					err = replicationConfig.RemoveRule(opts)
				} else {
					replicationConfig = replication.Config{}
				}
				if err == nil {
					err = replicationConfig.AddRule(opts)
				}
			} else {
				err = replicationConfig.EditRule(opts)
			}
		default:
			err = replicationConfig.AddRule(opts)
		}
		if err != nil {
			return c.annotatePeerErr(peer.Name, "Error adding bucket replication rule", err)
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
		sameTarget, apiErr := validateReplicationDestination(ctx, bucket, newReplicationConfig, true)
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

		_, err = globalBucketMetadataSys.Update(ctx, bucket, bucketReplicationConfig, replCfgData)
		return c.annotatePeerErr(peer.Name, "Error updating replication configuration", err)
	}

	c.RLock()
	defer c.RUnlock()
	errMap := make(map[string]error, len(c.state.Peers))
	for d, peer := range c.state.Peers {
		if d == globalDeploymentID {
			continue
		}
		errMap[d] = configurePeerFn(d, peer)
	}
	return c.toErrorFromErrMap(errMap, configureReplication)
}

// PeerBucketDeleteHandler - deletes bucket on local in response to a delete
// bucket request from a peer.
func (c *SiteReplicationSys) PeerBucketDeleteHandler(ctx context.Context, bucket string, opts DeleteBucketOptions) error {
	c.RLock()
	defer c.RUnlock()
	if !c.enabled {
		return errSRNotEnabled
	}

	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return errServerNotInitialized
	}

	if globalDNSConfig != nil {
		if err := globalDNSConfig.Delete(bucket); err != nil {
			return err
		}
	}
	err := objAPI.DeleteBucket(ctx, bucket, opts)
	if err != nil {
		if globalDNSConfig != nil {
			if err2 := globalDNSConfig.Put(bucket); err2 != nil {
				logger.LogIf(ctx, fmt.Errorf("Unable to restore bucket DNS entry %w, please fix it manually", err2))
			}
		}
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
// Service accounts are replicated as long as they are not meant for the root
// user.
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

	cerr := c.concDo(nil, func(d string, p madmin.PeerInfo) error {
		admClient, err := c.getAdminClient(ctx, d)
		if err != nil {
			return wrapSRErr(err)
		}

		return c.annotatePeerErr(p.Name, replicateIAMItem, admClient.SRPeerReplicateIAMItem(ctx, item))
	},
		replicateIAMItem,
	)
	return errors.Unwrap(cerr)
}

// PeerAddPolicyHandler - copies IAM policy to local. A nil policy argument,
// causes the named policy to be deleted.
func (c *SiteReplicationSys) PeerAddPolicyHandler(ctx context.Context, policyName string, p *iampolicy.Policy, updatedAt time.Time) error {
	var err error
	// skip overwrite of local update if peer sent stale info
	if !updatedAt.IsZero() {
		if p, err := globalIAMSys.store.GetPolicyDoc(policyName); err == nil && p.UpdateDate.After(updatedAt) {
			return nil
		}
	}
	if p == nil {
		err = globalIAMSys.DeletePolicy(ctx, policyName, true)
	} else {
		_, err = globalIAMSys.SetPolicy(ctx, policyName, *p)
	}
	if err != nil {
		return wrapSRErr(err)
	}
	return nil
}

// PeerIAMUserChangeHandler - copies IAM user to local.
func (c *SiteReplicationSys) PeerIAMUserChangeHandler(ctx context.Context, change *madmin.SRIAMUser, updatedAt time.Time) error {
	if change == nil {
		return errSRInvalidRequest(errInvalidArgument)
	}
	// skip overwrite of local update if peer sent stale info
	if !updatedAt.IsZero() {
		if ui, err := globalIAMSys.GetUserInfo(ctx, change.AccessKey); err == nil && ui.UpdatedAt.After(updatedAt) {
			return nil
		}
	}

	var err error
	if change.IsDeleteReq {
		err = globalIAMSys.DeleteUser(ctx, change.AccessKey, true)
	} else {
		if change.UserReq == nil {
			return errSRInvalidRequest(errInvalidArgument)
		}
		userReq := *change.UserReq
		if userReq.Status != "" && userReq.SecretKey == "" {
			// Status is set without secretKey updates means we are
			// only changing the account status.
			_, err = globalIAMSys.SetUserStatus(ctx, change.AccessKey, userReq.Status)
		} else {
			_, err = globalIAMSys.CreateUser(ctx, change.AccessKey, userReq)
		}
	}
	if err != nil {
		return wrapSRErr(err)
	}
	return nil
}

// PeerGroupInfoChangeHandler - copies group changes to local.
func (c *SiteReplicationSys) PeerGroupInfoChangeHandler(ctx context.Context, change *madmin.SRGroupInfo, updatedAt time.Time) error {
	if change == nil {
		return errSRInvalidRequest(errInvalidArgument)
	}
	updReq := change.UpdateReq
	var err error

	// skip overwrite of local update if peer sent stale info
	if !updatedAt.IsZero() {
		if gd, err := globalIAMSys.GetGroupDescription(updReq.Group); err == nil && gd.UpdatedAt.After(updatedAt) {
			return nil
		}
	}

	if updReq.IsRemove {
		_, err = globalIAMSys.RemoveUsersFromGroup(ctx, updReq.Group, updReq.Members)
	} else {
		if updReq.Status != "" && len(updReq.Members) == 0 {
			_, err = globalIAMSys.SetGroupStatus(ctx, updReq.Group, updReq.Status == madmin.GroupEnabled)
		} else {
			_, err = globalIAMSys.AddUsersToGroup(ctx, updReq.Group, updReq.Members)
		}
	}
	if err != nil {
		return wrapSRErr(err)
	}
	return nil
}

// PeerSvcAccChangeHandler - copies service-account change to local.
func (c *SiteReplicationSys) PeerSvcAccChangeHandler(ctx context.Context, change *madmin.SRSvcAccChange, updatedAt time.Time) error {
	if change == nil {
		return errSRInvalidRequest(errInvalidArgument)
	}
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
		// skip overwrite of local update if peer sent stale info
		if !updatedAt.IsZero() && change.Create.AccessKey != "" {
			if sa, _, err := globalIAMSys.getServiceAccount(ctx, change.Create.AccessKey); err == nil && sa.UpdatedAt.After(updatedAt) {
				return nil
			}
		}
		opts := newServiceAccountOpts{
			accessKey:     change.Create.AccessKey,
			secretKey:     change.Create.SecretKey,
			sessionPolicy: sp,
			claims:        change.Create.Claims,
		}
		_, _, err = globalIAMSys.NewServiceAccount(ctx, change.Create.Parent, change.Create.Groups, opts)
		if err != nil {
			return wrapSRErr(err)
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
		// skip overwrite of local update if peer sent stale info
		if !updatedAt.IsZero() {
			if sa, _, err := globalIAMSys.getServiceAccount(ctx, change.Update.AccessKey); err == nil && sa.UpdatedAt.After(updatedAt) {
				return nil
			}
		}
		opts := updateServiceAccountOpts{
			secretKey:     change.Update.SecretKey,
			status:        change.Update.Status,
			sessionPolicy: sp,
		}

		_, err = globalIAMSys.UpdateServiceAccount(ctx, change.Update.AccessKey, opts)
		if err != nil {
			return wrapSRErr(err)
		}

	case change.Delete != nil:
		// skip overwrite of local update if peer sent stale info
		if !updatedAt.IsZero() {
			if sa, _, err := globalIAMSys.getServiceAccount(ctx, change.Delete.AccessKey); err == nil && sa.UpdatedAt.After(updatedAt) {
				return nil
			}
		}
		if err := globalIAMSys.DeleteServiceAccount(ctx, change.Delete.AccessKey, true); err != nil {
			return wrapSRErr(err)
		}

	}

	return nil
}

// PeerPolicyMappingHandler - copies policy mapping to local.
func (c *SiteReplicationSys) PeerPolicyMappingHandler(ctx context.Context, mapping *madmin.SRPolicyMapping, updatedAt time.Time) error {
	if mapping == nil {
		return errSRInvalidRequest(errInvalidArgument)
	}
	// skip overwrite of local update if peer sent stale info
	if !updatedAt.IsZero() {
		mp, ok := globalIAMSys.store.GetMappedPolicy(mapping.Policy, mapping.IsGroup)
		if ok && mp.UpdatedAt.After(updatedAt) {
			return nil
		}
	}

	_, err := globalIAMSys.PolicyDBSet(ctx, mapping.UserOrGroup, mapping.Policy, mapping.IsGroup)
	if err != nil {
		return wrapSRErr(err)
	}
	return nil
}

// PeerSTSAccHandler - replicates STS credential locally.
func (c *SiteReplicationSys) PeerSTSAccHandler(ctx context.Context, stsCred *madmin.SRSTSCredential, updatedAt time.Time) error {
	if stsCred == nil {
		return errSRInvalidRequest(errInvalidArgument)
	}
	// skip overwrite of local update if peer sent stale info
	if !updatedAt.IsZero() {
		if u, err := globalIAMSys.GetUserInfo(ctx, stsCred.AccessKey); err == nil {
			ok, _, _ := globalIAMSys.IsTempUser(stsCred.AccessKey)
			if ok && u.UpdatedAt.After(updatedAt) {
				return nil
			}
		}
	}

	// Verify the session token of the stsCred
	claims, err := auth.ExtractClaims(stsCred.SessionToken, globalActiveCred.SecretKey)
	if err != nil {
		return fmt.Errorf("STS credential could not be verified: %w", err)
	}

	mapClaims := claims.Map()
	expiry, err := auth.ExpToInt64(mapClaims["exp"])
	if err != nil {
		return fmt.Errorf("Expiry claim was not found: %v: %w", mapClaims, err)
	}

	cred := auth.Credentials{
		AccessKey:    stsCred.AccessKey,
		SecretKey:    stsCred.SecretKey,
		Expiration:   time.Unix(expiry, 0).UTC(),
		SessionToken: stsCred.SessionToken,
		ParentUser:   stsCred.ParentUser,
		Status:       auth.AccountOn,
	}

	// Extract the username and lookup DN and groups in LDAP.
	ldapUser, isLDAPSTS := claims.Lookup(ldapUserN)
	switch {
	case isLDAPSTS:
		// Need to lookup the groups from LDAP.
		_, ldapGroups, err := globalLDAPConfig.LookupUserDN(ldapUser)
		if err != nil {
			return fmt.Errorf("unable to query LDAP server for %s: %w", ldapUser, err)
		}

		cred.Groups = ldapGroups
	}

	// Set these credentials to IAM.
	if _, err := globalIAMSys.SetTempUser(ctx, cred.AccessKey, cred, stsCred.ParentPolicyMapping); err != nil {
		return fmt.Errorf("unable to save STS credential and/or parent policy mapping: %w", err)
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

	cerr := c.concDo(nil, func(d string, p madmin.PeerInfo) error {
		admClient, err := c.getAdminClient(ctx, d)
		if err != nil {
			return wrapSRErr(err)
		}

		return c.annotatePeerErr(p.Name, replicateBucketMetadata, admClient.SRPeerReplicateBucketMeta(ctx, item))
	},
		replicateBucketMetadata,
	)
	return errors.Unwrap(cerr)
}

// PeerBucketVersioningHandler - updates versioning config to local cluster.
func (c *SiteReplicationSys) PeerBucketVersioningHandler(ctx context.Context, bucket string, versioning *string, updatedAt time.Time) error {
	if versioning != nil {
		// skip overwrite if local update is newer than peer update.
		if !updatedAt.IsZero() {
			if _, updateTm, err := globalBucketMetadataSys.GetVersioningConfig(bucket); err == nil && updateTm.After(updatedAt) {
				return nil
			}
		}
		configData, err := base64.StdEncoding.DecodeString(*versioning)
		if err != nil {
			return wrapSRErr(err)
		}
		_, err = globalBucketMetadataSys.Update(ctx, bucket, bucketVersioningConfig, configData)
		if err != nil {
			return wrapSRErr(err)
		}
		return nil
	}

	return nil
}

// PeerBucketPolicyHandler - copies/deletes policy to local cluster.
func (c *SiteReplicationSys) PeerBucketPolicyHandler(ctx context.Context, bucket string, policy *policy.Policy, updatedAt time.Time) error {
	// skip overwrite if local update is newer than peer update.
	if !updatedAt.IsZero() {
		if _, updateTm, err := globalBucketMetadataSys.GetPolicyConfig(bucket); err == nil && updateTm.After(updatedAt) {
			return nil
		}
	}

	if policy != nil {
		configData, err := json.Marshal(policy)
		if err != nil {
			return wrapSRErr(err)
		}

		_, err = globalBucketMetadataSys.Update(ctx, bucket, bucketPolicyConfig, configData)
		if err != nil {
			return wrapSRErr(err)
		}
		return nil
	}

	// Delete the bucket policy
	_, err := globalBucketMetadataSys.Update(ctx, bucket, bucketPolicyConfig, nil)
	if err != nil {
		return wrapSRErr(err)
	}

	return nil
}

// PeerBucketTaggingHandler - copies/deletes tags to local cluster.
func (c *SiteReplicationSys) PeerBucketTaggingHandler(ctx context.Context, bucket string, tags *string, updatedAt time.Time) error {
	// skip overwrite if local update is newer than peer update.
	if !updatedAt.IsZero() {
		if _, updateTm, err := globalBucketMetadataSys.GetTaggingConfig(bucket); err == nil && updateTm.After(updatedAt) {
			return nil
		}
	}

	if tags != nil {
		configData, err := base64.StdEncoding.DecodeString(*tags)
		if err != nil {
			return wrapSRErr(err)
		}
		_, err = globalBucketMetadataSys.Update(ctx, bucket, bucketTaggingConfig, configData)
		if err != nil {
			return wrapSRErr(err)
		}
		return nil
	}

	// Delete the tags
	_, err := globalBucketMetadataSys.Update(ctx, bucket, bucketTaggingConfig, nil)
	if err != nil {
		return wrapSRErr(err)
	}

	return nil
}

// PeerBucketObjectLockConfigHandler - sets object lock on local bucket.
func (c *SiteReplicationSys) PeerBucketObjectLockConfigHandler(ctx context.Context, bucket string, objectLockData *string, updatedAt time.Time) error {
	if objectLockData != nil {
		// skip overwrite if local update is newer than peer update.
		if !updatedAt.IsZero() {
			if _, updateTm, err := globalBucketMetadataSys.GetObjectLockConfig(bucket); err == nil && updateTm.After(updatedAt) {
				return nil
			}
		}

		configData, err := base64.StdEncoding.DecodeString(*objectLockData)
		if err != nil {
			return wrapSRErr(err)
		}
		_, err = globalBucketMetadataSys.Update(ctx, bucket, objectLockConfig, configData)
		if err != nil {
			return wrapSRErr(err)
		}
		return nil
	}

	return nil
}

// PeerBucketSSEConfigHandler - copies/deletes SSE config to local cluster.
func (c *SiteReplicationSys) PeerBucketSSEConfigHandler(ctx context.Context, bucket string, sseConfig *string, updatedAt time.Time) error {
	// skip overwrite if local update is newer than peer update.
	if !updatedAt.IsZero() {
		if _, updateTm, err := globalBucketMetadataSys.GetSSEConfig(bucket); err == nil && updateTm.After(updatedAt) {
			return nil
		}
	}

	if sseConfig != nil {
		configData, err := base64.StdEncoding.DecodeString(*sseConfig)
		if err != nil {
			return wrapSRErr(err)
		}
		_, err = globalBucketMetadataSys.Update(ctx, bucket, bucketSSEConfig, configData)
		if err != nil {
			return wrapSRErr(err)
		}
		return nil
	}

	// Delete sse config
	_, err := globalBucketMetadataSys.Update(ctx, bucket, bucketSSEConfig, nil)
	if err != nil {
		return wrapSRErr(err)
	}
	return nil
}

// PeerBucketQuotaConfigHandler - copies/deletes policy to local cluster.
func (c *SiteReplicationSys) PeerBucketQuotaConfigHandler(ctx context.Context, bucket string, quota *madmin.BucketQuota, updatedAt time.Time) error {
	// skip overwrite if local update is newer than peer update.
	if !updatedAt.IsZero() {
		if _, updateTm, err := globalBucketMetadataSys.GetQuotaConfig(ctx, bucket); err == nil && updateTm.After(updatedAt) {
			return nil
		}
	}

	if quota != nil {
		quotaData, err := json.Marshal(quota)
		if err != nil {
			return wrapSRErr(err)
		}

		if _, err = globalBucketMetadataSys.Update(ctx, bucket, bucketQuotaConfigFile, quotaData); err != nil {
			return wrapSRErr(err)
		}

		return nil
	}

	// Delete the bucket policy
	_, err := globalBucketMetadataSys.Update(ctx, bucket, bucketQuotaConfigFile, nil)
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

// getAdminClientWithEndpoint - NOTE: ensure to take at least a read lock on SiteReplicationSys
// before calling this.
func (c *SiteReplicationSys) getAdminClientWithEndpoint(ctx context.Context, deploymentID, endpoint string) (*madmin.AdminClient, error) {
	creds, err := c.getPeerCreds()
	if err != nil {
		return nil, err
	}

	if _, ok := c.state.Peers[deploymentID]; !ok {
		return nil, errSRPeerNotFound
	}
	return getAdminClient(endpoint, creds.AccessKey, creds.SecretKey)
}

func (c *SiteReplicationSys) getPeerCreds() (*auth.Credentials, error) {
	u, ok := globalIAMSys.store.GetUser(c.state.ServiceAccountAccessKey)
	if !ok {
		return nil, errors.New("site replication service account not found")
	}
	return &u.Credentials, nil
}

// listBuckets returns a consistent common view of latest unique buckets across
// sites, this is used for replication.
func (c *SiteReplicationSys) listBuckets(ctx context.Context) ([]BucketInfo, error) {
	// If local has buckets, enable versioning on them, create them on peers
	// and setup replication rules.
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return nil, errSRObjectLayerNotReady
	}
	return objAPI.ListBuckets(ctx, BucketOptions{Deleted: true})
}

// syncToAllPeers is used for syncing local data to all remote peers, it is
// called once during initial "AddPeerClusters" request.
func (c *SiteReplicationSys) syncToAllPeers(ctx context.Context) error {
	buckets, err := c.listBuckets(ctx)
	if err != nil {
		return err
	}

	for _, bucketInfo := range buckets {
		bucket := bucketInfo.Name

		// MinIO does not store bucket location - so we just check if
		// object locking is enabled.
		lockConfig, _, err := globalBucketMetadataSys.GetObjectLockConfig(bucket)
		if err != nil {
			if _, ok := err.(BucketObjectLockConfigNotFound); !ok {
				return errSRBackendIssue(err)
			}
		}

		var opts MakeBucketOptions
		if lockConfig != nil {
			opts.LockEnabled = lockConfig.ObjectLockEnabled == "Enabled"
		}

		opts.CreatedAt, _ = globalBucketMetadataSys.CreatedAt(bucket)
		// Now call the MakeBucketHook on existing bucket - this will
		// create buckets and replication rules on peer clusters.
		err = c.MakeBucketHook(ctx, bucket, opts)
		if err != nil {
			return errSRBucketConfigError(err)
		}

		// Replicate bucket policy if present.
		policy, tm, err := globalBucketMetadataSys.GetPolicyConfig(bucket)
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
				Type:      madmin.SRBucketMetaTypePolicy,
				Bucket:    bucket,
				Policy:    policyJSON,
				UpdatedAt: tm,
			})
			if err != nil {
				return errSRBucketMetaError(err)
			}
		}

		// Replicate bucket tags if present.
		tags, tm, err := globalBucketMetadataSys.GetTaggingConfig(bucket)
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
				Type:      madmin.SRBucketMetaTypeTags,
				Bucket:    bucket,
				Tags:      &tagCfgStr,
				UpdatedAt: tm,
			})
			if err != nil {
				return errSRBucketMetaError(err)
			}
		}

		// Replicate object-lock config if present.
		objLockCfg, tm, err := globalBucketMetadataSys.GetObjectLockConfig(bucket)
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
				Type:      madmin.SRBucketMetaTypeObjectLockConfig,
				Bucket:    bucket,
				Tags:      &objLockStr,
				UpdatedAt: tm,
			})
			if err != nil {
				return errSRBucketMetaError(err)
			}
		}

		// Replicate existing bucket bucket encryption settings
		sseConfig, tm, err := globalBucketMetadataSys.GetSSEConfig(bucket)
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
				UpdatedAt: tm,
			})
			if err != nil {
				return errSRBucketMetaError(err)
			}
		}

		quotaConfig, tm, err := globalBucketMetadataSys.GetQuotaConfig(ctx, bucket)
		found = true
		if _, ok := err.(BucketQuotaConfigNotFound); ok {
			found = false
		} else if err != nil {
			return errSRBackendIssue(err)
		}
		if found {
			quotaConfigJSON, err := json.Marshal(quotaConfig)
			if err != nil {
				return wrapSRErr(err)
			}
			err = c.BucketMetaHook(ctx, madmin.SRBucketMeta{
				Type:      madmin.SRBucketMetaTypeQuotaConfig,
				Bucket:    bucket,
				Quota:     quotaConfigJSON,
				UpdatedAt: tm,
			})
			if err != nil {
				return errSRBucketMetaError(err)
			}
		}
	}

	// Order matters from now on how the information is
	// synced to remote sites.

	// Policies should be synced first.
	{
		// Replicate IAM policies on local to all peers.
		allPolicyDocs, err := globalIAMSys.ListPolicyDocs(ctx, "")
		if err != nil {
			return errSRBackendIssue(err)
		}

		for pname, pdoc := range allPolicyDocs {
			policyJSON, err := json.Marshal(pdoc.Policy)
			if err != nil {
				return wrapSRErr(err)
			}
			err = c.IAMChangeHook(ctx, madmin.SRIAMItem{
				Type:      madmin.SRIAMItemPolicy,
				Name:      pname,
				Policy:    policyJSON,
				UpdatedAt: pdoc.UpdateDate,
			})
			if err != nil {
				return errSRIAMError(err)
			}
		}
	}

	// Next should be userAccounts those are local users, OIDC and LDAP will not
	// may not have any local users.
	{
		userAccounts := make(map[string]UserIdentity)
		globalIAMSys.store.rlock()
		err := globalIAMSys.store.loadUsers(ctx, regUser, userAccounts)
		globalIAMSys.store.runlock()
		if err != nil {
			return errSRBackendIssue(err)
		}

		for _, acc := range userAccounts {
			if err := c.IAMChangeHook(ctx, madmin.SRIAMItem{
				Type: madmin.SRIAMItemIAMUser,
				IAMUser: &madmin.SRIAMUser{
					AccessKey:   acc.Credentials.AccessKey,
					IsDeleteReq: false,
					UserReq: &madmin.AddOrUpdateUserReq{
						SecretKey: acc.Credentials.SecretKey,
						Status:    madmin.AccountStatus(acc.Credentials.Status),
					},
				},
				UpdatedAt: acc.UpdatedAt,
			}); err != nil {
				return errSRIAMError(err)
			}
		}
	}

	// Next should be Groups for some of these users, LDAP might have some Group
	// DNs here
	{
		groups := make(map[string]GroupInfo)

		globalIAMSys.store.rlock()
		err := globalIAMSys.store.loadGroups(ctx, groups)
		globalIAMSys.store.runlock()
		if err != nil {
			return errSRBackendIssue(err)
		}

		for gname, group := range groups {
			if err := c.IAMChangeHook(ctx, madmin.SRIAMItem{
				Type: madmin.SRIAMItemGroupInfo,
				GroupInfo: &madmin.SRGroupInfo{
					UpdateReq: madmin.GroupAddRemove{
						Group:    gname,
						Members:  group.Members,
						Status:   madmin.GroupStatus(group.Status),
						IsRemove: false,
					},
				},
				UpdatedAt: group.UpdatedAt,
			}); err != nil {
				return errSRIAMError(err)
			}
		}
	}

	// Service accounts are the static accounts that should be synced with
	// valid claims.
	{
		serviceAccounts := make(map[string]UserIdentity)
		globalIAMSys.store.rlock()
		err := globalIAMSys.store.loadUsers(ctx, svcUser, serviceAccounts)
		globalIAMSys.store.runlock()
		if err != nil {
			return errSRBackendIssue(err)
		}

		for user, acc := range serviceAccounts {
			if user == siteReplicatorSvcAcc {
				// skip the site replicate svc account as it is
				// already replicated.
				continue
			}

			claims, err := globalIAMSys.GetClaimsForSvcAcc(ctx, acc.Credentials.AccessKey)
			if err != nil {
				return errSRBackendIssue(err)
			}

			_, policy, err := globalIAMSys.GetServiceAccount(ctx, acc.Credentials.AccessKey)
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
						Parent:        acc.Credentials.ParentUser,
						AccessKey:     user,
						SecretKey:     acc.Credentials.SecretKey,
						Groups:        acc.Credentials.Groups,
						Claims:        claims,
						SessionPolicy: json.RawMessage(policyJSON),
						Status:        acc.Credentials.Status,
					},
				},
				UpdatedAt: acc.UpdatedAt,
			})
			if err != nil {
				return errSRIAMError(err)
			}
		}
	}

	// Followed by policy mapping for the userAccounts we previously synced.
	{
		// Replicate policy mappings on local to all peers.
		userPolicyMap := make(map[string]MappedPolicy)
		groupPolicyMap := make(map[string]MappedPolicy)
		globalIAMSys.store.rlock()
		errU := globalIAMSys.store.loadMappedPolicies(ctx, regUser, false, userPolicyMap)
		errG := globalIAMSys.store.loadMappedPolicies(ctx, regUser, true, groupPolicyMap)
		globalIAMSys.store.runlock()
		if errU != nil {
			return errSRBackendIssue(errU)
		}

		for user, mp := range userPolicyMap {
			err := c.IAMChangeHook(ctx, madmin.SRIAMItem{
				Type: madmin.SRIAMItemPolicyMapping,
				PolicyMapping: &madmin.SRPolicyMapping{
					UserOrGroup: user,
					IsGroup:     false,
					Policy:      mp.Policies,
				},
				UpdatedAt: mp.UpdatedAt,
			})
			if err != nil {
				return errSRIAMError(err)
			}
		}

		if errG != nil {
			return errSRBackendIssue(errG)
		}

		for group, mp := range groupPolicyMap {
			err := c.IAMChangeHook(ctx, madmin.SRIAMItem{
				Type: madmin.SRIAMItemPolicyMapping,
				PolicyMapping: &madmin.SRPolicyMapping{
					UserOrGroup: group,
					IsGroup:     true,
					Policy:      mp.Policies,
				},
				UpdatedAt: mp.UpdatedAt,
			})
			if err != nil {
				return errSRIAMError(err)
			}
		}
	}

	// and finally followed by policy mappings for for STS users.
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

		for user, mp := range userPolicyMap {
			err := c.IAMChangeHook(ctx, madmin.SRIAMItem{
				Type: madmin.SRIAMItemPolicyMapping,
				PolicyMapping: &madmin.SRPolicyMapping{
					UserOrGroup: user,
					IsGroup:     false,
					Policy:      mp.Policies,
				},
				UpdatedAt: mp.UpdatedAt,
			})
			if err != nil {
				return errSRIAMError(err)
			}
		}

		if errG != nil {
			return errSRBackendIssue(errG)
		}

		for group, mp := range groupPolicyMap {
			err := c.IAMChangeHook(ctx, madmin.SRIAMItem{
				Type: madmin.SRIAMItemPolicyMapping,
				PolicyMapping: &madmin.SRPolicyMapping{
					UserOrGroup: group,
					IsGroup:     true,
					Policy:      mp.Policies,
				},
				UpdatedAt: mp.UpdatedAt,
			})
			if err != nil {
				return errSRIAMError(err)
			}
		}
	}

	return nil
}

// Concurrency helpers

type concErr struct {
	errMap     map[string]error
	summaryErr error
}

func (c concErr) Error() string {
	if c.summaryErr != nil {
		return c.summaryErr.Error()
	}
	return "<nil>"
}

func (c concErr) Unwrap() error {
	return c.summaryErr
}

func (c *SiteReplicationSys) toErrorFromErrMap(errMap map[string]error, actionName string) error {
	if len(errMap) == 0 {
		return nil
	}

	var success int
	msgs := []string{}
	for d, err := range errMap {
		name := c.state.Peers[d].Name
		if err == nil {
			msgs = append(msgs, fmt.Sprintf("'%s' on site %s (%s): succeeded", actionName, name, d))
			success++
		} else {
			msgs = append(msgs, fmt.Sprintf("'%s' on site %s (%s): failed(%v)", actionName, name, d, err))
		}
	}
	if success == len(errMap) {
		return nil
	}
	return fmt.Errorf("Site replication error(s): \n%s", strings.Join(msgs, "\n"))
}

func (c *SiteReplicationSys) newConcErr(errMap map[string]error, actionName string) error {
	return concErr{
		errMap:     errMap,
		summaryErr: c.toErrorFromErrMap(errMap, actionName),
	}
}

// concDo calls actions concurrently. selfActionFn is run for the current
// cluster and peerActionFn is run for each peer replication cluster.
func (c *SiteReplicationSys) concDo(selfActionFn func() error, peerActionFn func(deploymentID string, p madmin.PeerInfo) error, actionName string) error {
	depIDs := make([]string, 0, len(c.state.Peers))
	for d := range c.state.Peers {
		depIDs = append(depIDs, d)
	}
	errs := make([]error, len(c.state.Peers))
	var wg sync.WaitGroup
	wg.Add(len(depIDs))
	for i := range depIDs {
		go func(i int) {
			defer wg.Done()
			if depIDs[i] == globalDeploymentID {
				if selfActionFn != nil {
					errs[i] = selfActionFn()
				}
			} else {
				errs[i] = peerActionFn(depIDs[i], c.state.Peers[depIDs[i]])
			}
		}(i)
	}
	wg.Wait()
	errMap := make(map[string]error, len(c.state.Peers))
	for i, depID := range depIDs {
		errMap[depID] = errs[i]
	}
	return c.newConcErr(errMap, actionName)
}

func (c *SiteReplicationSys) annotateErr(annotation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %s: %w", c.state.Name, annotation, err)
}

func (c *SiteReplicationSys) annotatePeerErr(dstPeer string, annotation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s->%s: %s: %w", c.state.Name, dstPeer, annotation, err)
}

// isEnabled returns true if site replication is enabled
func (c *SiteReplicationSys) isEnabled() bool {
	c.RLock()
	defer c.RUnlock()
	return c.enabled
}

var errMissingSRConfig = fmt.Errorf("Site not found in site replication configuration")

// RemovePeerCluster - removes one or more clusters from site replication configuration.
func (c *SiteReplicationSys) RemovePeerCluster(ctx context.Context, objectAPI ObjectLayer, rreq madmin.SRRemoveReq) (st madmin.ReplicateRemoveStatus, err error) {
	if !c.isEnabled() {
		return st, errSRNotEnabled
	}
	info, err := c.GetClusterInfo(ctx)
	if err != nil {
		return st, errSRBackendIssue(err)
	}
	peerMap := make(map[string]madmin.PeerInfo)
	var rmvEndpoints []string
	siteNames := rreq.SiteNames
	updatedPeers := make(map[string]madmin.PeerInfo)

	for _, pi := range info.Sites {
		updatedPeers[pi.DeploymentID] = pi
		peerMap[pi.Name] = pi
		if rreq.RemoveAll {
			siteNames = append(siteNames, pi.Name)
		}
	}
	for _, s := range siteNames {
		info, ok := peerMap[s]
		if !ok {
			return st, errSRConfigMissingError(errMissingSRConfig)
		}
		rmvEndpoints = append(rmvEndpoints, info.Endpoint)
		delete(updatedPeers, info.DeploymentID)
	}
	var wg sync.WaitGroup
	errs := make(map[string]error, len(c.state.Peers))

	for _, v := range info.Sites {
		wg.Add(1)
		if v.DeploymentID == globalDeploymentID {
			go func() {
				defer wg.Done()
				err := c.RemoveRemoteTargetsForEndpoint(ctx, objectAPI, rmvEndpoints, false)
				errs[globalDeploymentID] = err
			}()
			continue
		}
		go func(pi madmin.PeerInfo) {
			defer wg.Done()
			admClient, err := c.getAdminClient(ctx, pi.DeploymentID)
			if err != nil {
				errs[pi.DeploymentID] = errSRPeerResp(fmt.Errorf("unable to create admin client for %s: %w", pi.Name, err))
				return
			}
			if _, err = admClient.SRPeerRemove(ctx, rreq); err != nil {
				if errors.As(err, &errMissingSRConfig) {
					return
				}
				errs[pi.DeploymentID] = errSRPeerResp(fmt.Errorf("unable to update peer %s: %w", pi.Name, err))
				return
			}
		}(v)
	}
	wg.Wait()

	for dID, err := range errs {
		if err != nil {
			return madmin.ReplicateRemoveStatus{
				ErrDetail: err.Error(),
				Status:    madmin.ReplicateRemoveStatusPartial,
			}, errSRPeerResp(fmt.Errorf("unable to update peer %s: %w", c.state.Peers[dID].Name, err))
		}
	}
	// Update cluster state
	var state srState
	if len(updatedPeers) > 1 {
		state = srState{
			Name:                    info.Name,
			Peers:                   updatedPeers,
			ServiceAccountAccessKey: info.ServiceAccountAccessKey,
		}
	}
	if err = c.saveToDisk(ctx, state); err != nil {
		return madmin.ReplicateRemoveStatus{
			Status:    madmin.ReplicateRemoveStatusPartial,
			ErrDetail: fmt.Sprintf("unable to save cluster-replication state on local: %v", err),
		}, nil
	}

	return madmin.ReplicateRemoveStatus{
		Status: madmin.ReplicateRemoveStatusSuccess,
	}, nil
}

// InternalRemoveReq - sends an unlink request to peer cluster to remove one or more sites
// from the site replication configuration.
func (c *SiteReplicationSys) InternalRemoveReq(ctx context.Context, objectAPI ObjectLayer, rreq madmin.SRRemoveReq) error {
	if !c.isEnabled() {
		return errSRNotEnabled
	}

	ourName := ""
	peerMap := make(map[string]madmin.PeerInfo)
	updatedPeers := make(map[string]madmin.PeerInfo)
	siteNames := rreq.SiteNames

	for _, p := range c.state.Peers {
		peerMap[p.Name] = p
		if p.DeploymentID == globalDeploymentID {
			ourName = p.Name
		}
		updatedPeers[p.DeploymentID] = p
		if rreq.RemoveAll {
			siteNames = append(siteNames, p.Name)
		}
	}
	var rmvEndpoints []string
	var unlinkSelf bool

	for _, s := range siteNames {
		info, ok := peerMap[s]
		if !ok {
			return errMissingSRConfig
		}
		if info.DeploymentID == globalDeploymentID {
			unlinkSelf = true
			continue
		}
		delete(updatedPeers, info.DeploymentID)
		rmvEndpoints = append(rmvEndpoints, info.Endpoint)
	}
	if err := c.RemoveRemoteTargetsForEndpoint(ctx, objectAPI, rmvEndpoints, unlinkSelf); err != nil {
		return err
	}
	var state srState
	if !unlinkSelf {
		state = srState{
			Name:                    c.state.Name,
			Peers:                   updatedPeers,
			ServiceAccountAccessKey: c.state.ServiceAccountAccessKey,
		}
	}

	if err := c.saveToDisk(ctx, state); err != nil {
		return errSRBackendIssue(fmt.Errorf("unable to save cluster-replication state to disk on %s: %v", ourName, err))
	}
	return nil
}

// RemoveRemoteTargetsForEndpoint removes replication targets corresponding to endpoint
func (c *SiteReplicationSys) RemoveRemoteTargetsForEndpoint(ctx context.Context, objectAPI ObjectLayer, endpoints []string, unlinkSelf bool) (err error) {
	targets := globalBucketTargetSys.ListTargets(ctx, "", string(madmin.ReplicationService))
	m := make(map[string]madmin.BucketTarget)
	for _, t := range targets {
		for _, endpoint := range endpoints {
			ep, _ := url.Parse(endpoint)
			if t.Endpoint == ep.Host &&
				t.Secure == (ep.Scheme == "https") &&
				t.Type == madmin.ReplicationService {
				m[t.Arn] = t
			}
		}
		// all remote targets from self are to be delinked
		if unlinkSelf {
			m[t.Arn] = t
		}
	}
	buckets, err := objectAPI.ListBuckets(ctx, BucketOptions{})
	for _, b := range buckets {
		config, _, err := globalBucketMetadataSys.GetReplicationConfig(ctx, b.Name)
		if err != nil {
			if errors.Is(err, BucketReplicationConfigNotFound{Bucket: b.Name}) {
				continue
			}
			return err
		}
		var nRules []sreplication.Rule
		for _, r := range config.Rules {
			if _, ok := m[r.Destination.Bucket]; !ok {
				nRules = append(nRules, r)
			}
		}
		if len(nRules) > 0 {
			config.Rules = nRules
			configData, err := xml.Marshal(config)
			if err != nil {
				return err
			}
			if _, err = globalBucketMetadataSys.Update(ctx, b.Name, bucketReplicationConfig, configData); err != nil {
				return err
			}
		} else {
			if _, err := globalBucketMetadataSys.Update(ctx, b.Name, bucketReplicationConfig, nil); err != nil {
				return err
			}
		}
	}
	for arn, t := range m {
		if err := globalBucketTargetSys.RemoveTarget(ctx, t.SourceBucket, arn); err != nil {
			if errors.Is(err, BucketRemoteTargetNotFound{Bucket: t.SourceBucket}) {
				continue
			}
			return err
		}
	}
	return
}

// Other helpers

func getAdminClient(endpoint, accessKey, secretKey string) (*madmin.AdminClient, error) {
	epURL, _ := url.Parse(endpoint)
	client, err := madmin.New(epURL.Host, accessKey, secretKey, epURL.Scheme == "https")
	if err != nil {
		return nil, err
	}
	client.SetCustomTransport(globalRemoteTargetTransport)
	return client, nil
}

func getS3Client(pc madmin.PeerSite) (*minioClient.Client, error) {
	ep, err := url.Parse(pc.Endpoint)
	if err != nil {
		return nil, err
	}
	return minioClient.New(ep.Host, &minioClient.Options{
		Creds:     credentials.NewStaticV4(pc.AccessKey, pc.SecretKey, ""),
		Secure:    ep.Scheme == "https",
		Transport: globalRemoteTargetTransport,
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

// returns a slice with site names participating in site replciation but unspecified while adding
// a new site.
func getMissingSiteNames(oldDeps, newDeps set.StringSet, currSites []madmin.PeerInfo) []string {
	diff := oldDeps.Difference(newDeps)
	var diffSlc []string
	for _, v := range currSites {
		if diff.Contains(v.DeploymentID) {
			diffSlc = append(diffSlc, v.Name)
		}
	}
	return diffSlc
}

type srBucketMetaInfo struct {
	madmin.SRBucketInfo
	DeploymentID string
}

type srPolicy struct {
	madmin.SRIAMPolicy
	DeploymentID string
}

type srPolicyMapping struct {
	madmin.SRPolicyMapping
	DeploymentID string
}

type srUserInfo struct {
	madmin.UserInfo
	DeploymentID string
}

type srGroupDesc struct {
	madmin.GroupDesc
	DeploymentID string
}

// SiteReplicationStatus returns the site replication status across clusters participating in site replication.
func (c *SiteReplicationSys) SiteReplicationStatus(ctx context.Context, objAPI ObjectLayer, opts madmin.SRStatusOptions) (info madmin.SRStatusInfo, err error) {
	sinfo, err := c.siteReplicationStatus(ctx, objAPI, opts)
	if err != nil {
		return info, err
	}
	info = madmin.SRStatusInfo{
		Enabled:      sinfo.Enabled,
		MaxBuckets:   sinfo.MaxBuckets,
		MaxUsers:     sinfo.MaxUsers,
		MaxGroups:    sinfo.MaxGroups,
		MaxPolicies:  sinfo.MaxPolicies,
		Sites:        sinfo.Sites,
		StatsSummary: sinfo.StatsSummary,
	}
	info.BucketStats = make(map[string]map[string]madmin.SRBucketStatsSummary, len(sinfo.Sites))
	info.PolicyStats = make(map[string]map[string]madmin.SRPolicyStatsSummary)
	info.UserStats = make(map[string]map[string]madmin.SRUserStatsSummary)
	info.GroupStats = make(map[string]map[string]madmin.SRGroupStatsSummary)
	numSites := len(info.Sites)
	for b, stat := range sinfo.BucketStats {
		for dID, st := range stat {
			if st.TagMismatch ||
				st.VersioningConfigMismatch ||
				st.OLockConfigMismatch ||
				st.SSEConfigMismatch ||
				st.PolicyMismatch ||
				st.ReplicationCfgMismatch ||
				st.QuotaCfgMismatch ||
				opts.Entity == madmin.SRBucketEntity {
				if _, ok := info.BucketStats[b]; !ok {
					info.BucketStats[b] = make(map[string]madmin.SRBucketStatsSummary, numSites)
				}
				info.BucketStats[b][dID] = st.SRBucketStatsSummary
			}
		}
	}
	for u, stat := range sinfo.UserStats {
		for dID, st := range stat {
			if st.PolicyMismatch || st.UserInfoMismatch || opts.Entity == madmin.SRUserEntity {
				if _, ok := info.UserStats[u]; !ok {
					info.UserStats[u] = make(map[string]madmin.SRUserStatsSummary, numSites)
				}
				info.UserStats[u][dID] = st.SRUserStatsSummary
			}
		}
	}
	for g, stat := range sinfo.GroupStats {
		for dID, st := range stat {
			if st.PolicyMismatch || st.GroupDescMismatch || opts.Entity == madmin.SRGroupEntity {
				if _, ok := info.GroupStats[g]; !ok {
					info.GroupStats[g] = make(map[string]madmin.SRGroupStatsSummary, numSites)
				}
				info.GroupStats[g][dID] = st.SRGroupStatsSummary
			}
		}
	}
	for p, stat := range sinfo.PolicyStats {
		for dID, st := range stat {
			if st.PolicyMismatch || opts.Entity == madmin.SRPolicyEntity {
				if _, ok := info.PolicyStats[p]; !ok {
					info.PolicyStats[p] = make(map[string]madmin.SRPolicyStatsSummary, numSites)
				}
				info.PolicyStats[p][dID] = st.SRPolicyStatsSummary
			}
		}
	}

	return
}

const (
	replicationStatus = "ReplicationStatus"
)

// siteReplicationStatus returns the site replication status across clusters participating in site replication.
func (c *SiteReplicationSys) siteReplicationStatus(ctx context.Context, objAPI ObjectLayer, opts madmin.SRStatusOptions) (info srStatusInfo, err error) {
	c.RLock()
	defer c.RUnlock()
	if !c.enabled {
		return info, err
	}

	sris := make([]madmin.SRInfo, len(c.state.Peers))
	depIdx := make(map[string]int, len(c.state.Peers))
	i := 0
	for d := range c.state.Peers {
		depIdx[d] = i
		i++
	}

	metaInfoConcErr := c.concDo(
		func() error {
			srInfo, err := c.SiteReplicationMetaInfo(ctx, objAPI, opts)
			if err != nil {
				return err
			}
			sris[depIdx[globalDeploymentID]] = srInfo
			return nil
		},
		func(deploymentID string, p madmin.PeerInfo) error {
			admClient, err := c.getAdminClient(ctx, deploymentID)
			if err != nil {
				return err
			}
			srInfo, err := admClient.SRMetaInfo(ctx, opts)
			if err != nil {
				return err
			}
			sris[depIdx[deploymentID]] = srInfo
			return nil
		},
		replicationStatus,
	)

	if err := errors.Unwrap(metaInfoConcErr); err != nil {
		return info, errSRBackendIssue(err)
	}

	info.Enabled = true
	info.Sites = make(map[string]madmin.PeerInfo, len(c.state.Peers))
	for d, peer := range c.state.Peers {
		info.Sites[d] = peer
	}

	var maxBuckets int
	for _, sri := range sris {
		if len(sri.Buckets) > maxBuckets {
			maxBuckets = len(sri.Buckets)
		}
	}
	// mapping b/w entity and entity config across sites
	bucketStats := make(map[string][]srBucketMetaInfo)
	policyStats := make(map[string][]srPolicy)
	userPolicyStats := make(map[string][]srPolicyMapping)
	groupPolicyStats := make(map[string][]srPolicyMapping)
	userInfoStats := make(map[string][]srUserInfo)
	groupDescStats := make(map[string][]srGroupDesc)

	numSites := len(sris)
	allBuckets := set.NewStringSet() // across sites
	allUsers := set.NewStringSet()
	allUserWPolicies := set.NewStringSet()
	allGroups := set.NewStringSet()
	allGroupWPolicies := set.NewStringSet()

	allPolicies := set.NewStringSet()
	for _, sri := range sris {
		for b := range sri.Buckets {
			allBuckets.Add(b)
		}
		for u := range sri.UserInfoMap {
			allUsers.Add(u)
		}
		for g := range sri.GroupDescMap {
			allGroups.Add(g)
		}
		for p := range sri.Policies {
			allPolicies.Add(p)
		}
		for u := range sri.UserPolicies {
			allUserWPolicies.Add(u)
		}
		for g := range sri.GroupPolicies {
			allGroupWPolicies.Add(g)
		}
	}

	for i, sri := range sris {
		for b := range allBuckets {
			if _, ok := bucketStats[b]; !ok {
				bucketStats[b] = make([]srBucketMetaInfo, numSites)
			}
			si, ok := sri.Buckets[b]
			if !ok {
				si = madmin.SRBucketInfo{Bucket: b}
			}
			bucketStats[b][i] = srBucketMetaInfo{SRBucketInfo: si, DeploymentID: sri.DeploymentID}
		}

		for pname := range allPolicies {
			if _, ok := policyStats[pname]; !ok {
				policyStats[pname] = make([]srPolicy, numSites)
			}

			// if pname is not present in the map, the zero value
			// will be returned.
			pi := sri.Policies[pname]
			policyStats[pname][i] = srPolicy{SRIAMPolicy: pi, DeploymentID: sri.DeploymentID}
		}
		for user := range allUserWPolicies {
			if _, ok := userPolicyStats[user]; !ok {
				userPolicyStats[user] = make([]srPolicyMapping, numSites)
			}
			up := sri.UserPolicies[user]
			userPolicyStats[user][i] = srPolicyMapping{SRPolicyMapping: up, DeploymentID: sri.DeploymentID}
		}
		for group := range allGroupWPolicies {
			if _, ok := groupPolicyStats[group]; !ok {
				groupPolicyStats[group] = make([]srPolicyMapping, numSites)
			}
			up := sri.GroupPolicies[group]
			groupPolicyStats[group][i] = srPolicyMapping{SRPolicyMapping: up, DeploymentID: sri.DeploymentID}
		}
		for u := range allUsers {
			if _, ok := userInfoStats[u]; !ok {
				userInfoStats[u] = make([]srUserInfo, numSites)
			}
			ui := sri.UserInfoMap[u]
			userInfoStats[u][i] = srUserInfo{UserInfo: ui, DeploymentID: sri.DeploymentID}
		}
		for g := range allGroups {
			if _, ok := groupDescStats[g]; !ok {
				groupDescStats[g] = make([]srGroupDesc, numSites)
			}
			gd := sri.GroupDescMap[g]
			groupDescStats[g][i] = srGroupDesc{GroupDesc: gd, DeploymentID: sri.DeploymentID}
		}
	}

	info.StatsSummary = make(map[string]madmin.SRSiteSummary, len(c.state.Peers))
	info.BucketStats = make(map[string]map[string]srBucketStatsSummary)
	info.PolicyStats = make(map[string]map[string]srPolicyStatsSummary)
	info.UserStats = make(map[string]map[string]srUserStatsSummary)
	info.GroupStats = make(map[string]map[string]srGroupStatsSummary)
	// collect user policy mapping replication status across sites
	if opts.Users || opts.Entity == madmin.SRUserEntity {
		for u, pslc := range userPolicyStats {
			if len(info.UserStats[u]) == 0 {
				info.UserStats[u] = make(map[string]srUserStatsSummary)
			}
			var policyMappings []madmin.SRPolicyMapping
			uPolicyCount := 0
			for _, ps := range pslc {
				policyMappings = append(policyMappings, ps.SRPolicyMapping)
				uPolicyCount++
				sum := info.StatsSummary[ps.DeploymentID]
				sum.TotalUserPolicyMappingCount++
				info.StatsSummary[ps.DeploymentID] = sum
			}
			userPolicyMismatch := !isPolicyMappingReplicated(uPolicyCount, numSites, policyMappings)
			for _, ps := range pslc {
				dID := depIdx[ps.DeploymentID]
				_, hasUser := sris[dID].UserPolicies[u]
				info.UserStats[u][ps.DeploymentID] = srUserStatsSummary{
					SRUserStatsSummary: madmin.SRUserStatsSummary{
						PolicyMismatch:   userPolicyMismatch,
						HasUser:          hasUser,
						HasPolicyMapping: ps.Policy != "",
					},
					userPolicy: ps,
				}
				if !userPolicyMismatch || opts.Entity != madmin.SRUserEntity {
					sum := info.StatsSummary[ps.DeploymentID]
					if !ps.IsGroup {
						sum.ReplicatedUserPolicyMappings++
					}
					info.StatsSummary[ps.DeploymentID] = sum
				}
			}
		}

		// collect user info replication status across sites
		for u, pslc := range userInfoStats {
			var uiSlc []madmin.UserInfo
			userCount := 0
			for _, ps := range pslc {
				uiSlc = append(uiSlc, ps.UserInfo)
				userCount++
				sum := info.StatsSummary[ps.DeploymentID]
				sum.TotalUsersCount++
				info.StatsSummary[ps.DeploymentID] = sum
			}
			userInfoMismatch := !isUserInfoReplicated(userCount, numSites, uiSlc)
			for _, ps := range pslc {
				dID := depIdx[ps.DeploymentID]
				_, hasUser := sris[dID].UserInfoMap[u]
				if len(info.UserStats[u]) == 0 {
					info.UserStats[u] = make(map[string]srUserStatsSummary)
				}
				umis, ok := info.UserStats[u][ps.DeploymentID]
				if !ok {
					umis = srUserStatsSummary{
						SRUserStatsSummary: madmin.SRUserStatsSummary{
							HasUser: hasUser,
						},
					}
				}
				umis.UserInfoMismatch = userInfoMismatch
				umis.userInfo = ps
				info.UserStats[u][ps.DeploymentID] = umis
				if !userInfoMismatch || opts.Entity != madmin.SRUserEntity {
					sum := info.StatsSummary[ps.DeploymentID]
					sum.ReplicatedUsers++
					info.StatsSummary[ps.DeploymentID] = sum
				}
			}
		}
	}
	if opts.Groups || opts.Entity == madmin.SRGroupEntity {
		// collect group policy mapping replication status across sites
		for g, pslc := range groupPolicyStats {
			var policyMappings []madmin.SRPolicyMapping
			gPolicyCount := 0
			for _, ps := range pslc {
				policyMappings = append(policyMappings, ps.SRPolicyMapping)
				gPolicyCount++
				sum := info.StatsSummary[ps.DeploymentID]
				sum.TotalGroupPolicyMappingCount++
				info.StatsSummary[ps.DeploymentID] = sum
			}
			groupPolicyMismatch := !isPolicyMappingReplicated(gPolicyCount, numSites, policyMappings)
			if len(info.GroupStats[g]) == 0 {
				info.GroupStats[g] = make(map[string]srGroupStatsSummary)
			}
			for _, ps := range pslc {
				dID := depIdx[ps.DeploymentID]
				_, hasGroup := sris[dID].GroupPolicies[g]
				info.GroupStats[g][ps.DeploymentID] = srGroupStatsSummary{
					SRGroupStatsSummary: madmin.SRGroupStatsSummary{
						PolicyMismatch:   groupPolicyMismatch,
						HasGroup:         hasGroup,
						HasPolicyMapping: ps.Policy != "",
						DeploymentID:     ps.DeploymentID,
					},
					groupPolicy: ps,
				}
				if !groupPolicyMismatch && opts.Entity != madmin.SRGroupEntity {
					sum := info.StatsSummary[ps.DeploymentID]
					sum.ReplicatedGroupPolicyMappings++
					info.StatsSummary[ps.DeploymentID] = sum
				}

			}
		}

		// collect group desc replication status across sites
		for g, pslc := range groupDescStats {
			var gds []madmin.GroupDesc
			groupCount := 0
			for _, ps := range pslc {
				groupCount++
				sum := info.StatsSummary[ps.DeploymentID]
				sum.TotalGroupsCount++
				info.StatsSummary[ps.DeploymentID] = sum
				gds = append(gds, ps.GroupDesc)
			}
			gdMismatch := !isGroupDescReplicated(groupCount, numSites, gds)
			for _, ps := range pslc {
				dID := depIdx[ps.DeploymentID]
				_, hasGroup := sris[dID].GroupDescMap[g]
				if len(info.GroupStats[g]) == 0 {
					info.GroupStats[g] = make(map[string]srGroupStatsSummary)
				}
				gmis, ok := info.GroupStats[g][ps.DeploymentID]
				if !ok {
					gmis = srGroupStatsSummary{
						SRGroupStatsSummary: madmin.SRGroupStatsSummary{
							HasGroup: hasGroup,
						},
					}
				}
				gmis.GroupDescMismatch = gdMismatch
				gmis.groupDesc = ps
				info.GroupStats[g][ps.DeploymentID] = gmis
				if !gdMismatch && opts.Entity != madmin.SRGroupEntity {
					sum := info.StatsSummary[ps.DeploymentID]
					sum.ReplicatedGroups++
					info.StatsSummary[ps.DeploymentID] = sum
				}
			}
		}
	}
	if opts.Policies || opts.Entity == madmin.SRPolicyEntity {
		// collect IAM policy replication status across sites
		for p, pslc := range policyStats {
			var policies []*iampolicy.Policy
			uPolicyCount := 0
			for _, ps := range pslc {
				plcy, err := iampolicy.ParseConfig(bytes.NewReader([]byte(ps.SRIAMPolicy.Policy)))
				if err != nil {
					continue
				}
				policies = append(policies, plcy)
				uPolicyCount++
				sum := info.StatsSummary[ps.DeploymentID]
				sum.TotalIAMPoliciesCount++
				info.StatsSummary[ps.DeploymentID] = sum
			}
			if len(info.PolicyStats[p]) == 0 {
				info.PolicyStats[p] = make(map[string]srPolicyStatsSummary)
			}
			policyMismatch := !isIAMPolicyReplicated(uPolicyCount, numSites, policies)
			for _, ps := range pslc {
				dID := depIdx[ps.DeploymentID]
				_, hasPolicy := sris[dID].Policies[p]
				info.PolicyStats[p][ps.DeploymentID] = srPolicyStatsSummary{
					SRPolicyStatsSummary: madmin.SRPolicyStatsSummary{
						PolicyMismatch: policyMismatch,
						HasPolicy:      hasPolicy,
					},
					policy: ps,
				}
				switch {
				case policyMismatch, opts.Entity == madmin.SRPolicyEntity:
				default:
					sum := info.StatsSummary[ps.DeploymentID]
					if !policyMismatch {
						sum.ReplicatedIAMPolicies++
					}
					info.StatsSummary[ps.DeploymentID] = sum
				}
			}
		}
	}
	if opts.Buckets || opts.Entity == madmin.SRBucketEntity {
		// collect bucket metadata replication stats across sites
		for b, slc := range bucketStats {
			tagSet := set.NewStringSet()
			olockConfigSet := set.NewStringSet()
			policies := make([]*bktpolicy.Policy, numSites)
			replCfgs := make([]*sreplication.Config, numSites)
			quotaCfgs := make([]*madmin.BucketQuota, numSites)
			sseCfgSet := set.NewStringSet()
			versionCfgSet := set.NewStringSet()
			var tagCount, olockCfgCount, sseCfgCount, versionCfgCount int
			for i, s := range slc {
				if s.ReplicationConfig != nil {
					cfgBytes, err := base64.StdEncoding.DecodeString(*s.ReplicationConfig)
					if err != nil {
						continue
					}
					cfg, err := sreplication.ParseConfig(bytes.NewReader(cfgBytes))
					if err != nil {
						continue
					}
					replCfgs[i] = cfg
				}
				if s.Versioning != nil {
					configData, err := base64.StdEncoding.DecodeString(*s.Versioning)
					if err != nil {
						continue
					}
					versionCfgCount++
					if !versionCfgSet.Contains(string(configData)) {
						versionCfgSet.Add(string(configData))
					}
				}
				if s.QuotaConfig != nil {
					cfgBytes, err := base64.StdEncoding.DecodeString(*s.QuotaConfig)
					if err != nil {
						continue
					}
					cfg, err := parseBucketQuota(b, cfgBytes)
					if err != nil {
						continue
					}
					quotaCfgs[i] = cfg
				}
				if s.Tags != nil {
					tagBytes, err := base64.StdEncoding.DecodeString(*s.Tags)
					if err != nil {
						continue
					}
					tagCount++
					if !tagSet.Contains(string(tagBytes)) {
						tagSet.Add(string(tagBytes))
					}
				}
				if len(s.Policy) > 0 {
					plcy, err := bktpolicy.ParseConfig(bytes.NewReader(s.Policy), b)
					if err != nil {
						continue
					}
					policies[i] = plcy
				}
				if s.ObjectLockConfig != nil {
					configData, err := base64.StdEncoding.DecodeString(*s.ObjectLockConfig)
					if err != nil {
						continue
					}
					olockCfgCount++
					if !olockConfigSet.Contains(string(configData)) {
						olockConfigSet.Add(string(configData))
					}
				}
				if s.SSEConfig != nil {
					configData, err := base64.StdEncoding.DecodeString(*s.SSEConfig)
					if err != nil {
						continue
					}
					sseCfgCount++
					if !sseCfgSet.Contains(string(configData)) {
						sseCfgSet.Add(string(configData))
					}
				}
				ss, ok := info.StatsSummary[s.DeploymentID]
				if !ok {
					ss = madmin.SRSiteSummary{}
				}
				// increment total number of replicated buckets
				if len(slc) == numSites {
					ss.ReplicatedBuckets++
				}
				ss.TotalBucketsCount++
				if tagCount > 0 {
					ss.TotalTagsCount++
				}
				if olockCfgCount > 0 {
					ss.TotalLockConfigCount++
				}
				if sseCfgCount > 0 {
					ss.TotalSSEConfigCount++
				}
				if versionCfgCount > 0 {
					ss.TotalVersioningConfigCount++
				}
				if len(policies) > 0 {
					ss.TotalBucketPoliciesCount++
				}
				info.StatsSummary[s.DeploymentID] = ss
			}
			tagMismatch := !isReplicated(tagCount, numSites, tagSet)
			olockCfgMismatch := !isReplicated(olockCfgCount, numSites, olockConfigSet)
			sseCfgMismatch := !isReplicated(sseCfgCount, numSites, sseCfgSet)
			versionCfgMismatch := !isReplicated(versionCfgCount, numSites, versionCfgSet)
			policyMismatch := !isBktPolicyReplicated(numSites, policies)
			replCfgMismatch := !isBktReplCfgReplicated(numSites, replCfgs)
			quotaCfgMismatch := !isBktQuotaCfgReplicated(numSites, quotaCfgs)
			info.BucketStats[b] = make(map[string]srBucketStatsSummary, numSites)
			for i, s := range slc {
				dIdx := depIdx[s.DeploymentID]
				var hasBucket, isBucketMarkedDeleted bool

				bi, ok := sris[dIdx].Buckets[s.Bucket]
				if ok {
					isBucketMarkedDeleted = !bi.DeletedAt.IsZero() && (bi.CreatedAt.IsZero() || bi.DeletedAt.After(bi.CreatedAt))
					hasBucket = !bi.CreatedAt.IsZero()
				}
				quotaCfgSet := hasBucket && quotaCfgs[i] != nil && *quotaCfgs[i] != madmin.BucketQuota{}
				ss := madmin.SRBucketStatsSummary{
					DeploymentID:             s.DeploymentID,
					HasBucket:                hasBucket,
					BucketMarkedDeleted:      isBucketMarkedDeleted,
					TagMismatch:              tagMismatch,
					OLockConfigMismatch:      olockCfgMismatch,
					SSEConfigMismatch:        sseCfgMismatch,
					VersioningConfigMismatch: versionCfgMismatch,
					PolicyMismatch:           policyMismatch,
					ReplicationCfgMismatch:   replCfgMismatch,
					QuotaCfgMismatch:         quotaCfgMismatch,
					HasReplicationCfg:        s.ReplicationConfig != nil,
					HasTagsSet:               s.Tags != nil,
					HasOLockConfigSet:        s.ObjectLockConfig != nil,
					HasPolicySet:             s.Policy != nil,
					HasQuotaCfgSet:           quotaCfgSet,
					HasSSECfgSet:             s.SSEConfig != nil,
				}
				var m srBucketMetaInfo
				if len(bucketStats[s.Bucket]) > dIdx {
					m = bucketStats[s.Bucket][dIdx]
				}
				info.BucketStats[b][s.DeploymentID] = srBucketStatsSummary{
					SRBucketStatsSummary: ss,
					meta:                 m,
				}
			}
			// no mismatch
			for _, s := range slc {
				sum := info.StatsSummary[s.DeploymentID]
				if !olockCfgMismatch && olockCfgCount == numSites {
					sum.ReplicatedLockConfig++
				}
				if !versionCfgMismatch && versionCfgCount == numSites {
					sum.ReplicatedVersioningConfig++
				}
				if !sseCfgMismatch && sseCfgCount == numSites {
					sum.ReplicatedSSEConfig++
				}
				if !policyMismatch && len(policies) == numSites {
					sum.ReplicatedBucketPolicies++
				}
				if !tagMismatch && tagCount == numSites {
					sum.ReplicatedTags++
				}
				info.StatsSummary[s.DeploymentID] = sum
			}
		}
	}

	// maximum buckets users etc seen across sites
	info.MaxBuckets = len(bucketStats)
	info.MaxUsers = len(userInfoStats)
	info.MaxGroups = len(groupDescStats)
	info.MaxPolicies = len(policyStats)
	return
}

// isReplicated returns true if count of replicated matches the number of
// sites and there is atmost one unique entry in the set.
func isReplicated(cntReplicated, total int, valSet set.StringSet) bool {
	if cntReplicated > 0 && cntReplicated < total {
		return false
	}
	if len(valSet) > 1 {
		// mismatch - one or more sites has differing tags/policy
		return false
	}
	return true
}

// isIAMPolicyReplicated returns true if count of replicated IAM policies matches total
// number of sites and IAM policies are identical.
func isIAMPolicyReplicated(cntReplicated, total int, policies []*iampolicy.Policy) bool {
	if cntReplicated > 0 && cntReplicated != total {
		return false
	}
	// check if policies match between sites
	var prev *iampolicy.Policy
	for i, p := range policies {
		if i == 0 {
			prev = p
			continue
		}
		if !prev.Equals(*p) {
			return false
		}
	}
	return true
}

// isPolicyMappingReplicated returns true if count of replicated IAM policy mappings matches total
// number of sites and IAM policy mappings are identical.
func isPolicyMappingReplicated(cntReplicated, total int, policies []madmin.SRPolicyMapping) bool {
	if cntReplicated > 0 && cntReplicated != total {
		return false
	}
	// check if policies match between sites
	var prev madmin.SRPolicyMapping
	for i, p := range policies {
		if i == 0 {
			prev = p
			continue
		}
		if prev.IsGroup != p.IsGroup ||
			prev.Policy != p.Policy ||
			prev.UserOrGroup != p.UserOrGroup {
			return false
		}
	}
	return true
}

func isUserInfoReplicated(cntReplicated, total int, uis []madmin.UserInfo) bool {
	if cntReplicated > 0 && cntReplicated != total {
		return false
	}
	// check if policies match between sites
	var prev madmin.UserInfo
	for i, ui := range uis {
		if i == 0 {
			prev = ui
			continue
		}
		if !isUserInfoEqual(prev, ui) {
			return false
		}
	}
	return true
}

func isGroupDescReplicated(cntReplicated, total int, gds []madmin.GroupDesc) bool {
	if cntReplicated > 0 && cntReplicated != total {
		return false
	}
	// check if policies match between sites
	var prev madmin.GroupDesc
	for i, gd := range gds {
		if i == 0 {
			prev = gd
			continue
		}
		if !isGroupDescEqual(prev, gd) {
			return false
		}
	}
	return true
}

func isBktQuotaCfgReplicated(total int, quotaCfgs []*madmin.BucketQuota) bool {
	numquotaCfgs := 0
	for _, q := range quotaCfgs {
		if q == nil {
			continue
		}
		numquotaCfgs++
	}

	if numquotaCfgs > 0 && numquotaCfgs != total {
		return false
	}
	var prev *madmin.BucketQuota
	for i, q := range quotaCfgs {
		if q == nil {
			return false
		}
		if i == 0 {
			prev = q
			continue
		}
		if prev.Quota != q.Quota || prev.Type != q.Type {
			return false
		}
	}
	return true
}

// isBktPolicyReplicated returns true if count of replicated bucket policies matches total
// number of sites and bucket policies are identical.
func isBktPolicyReplicated(total int, policies []*bktpolicy.Policy) bool {
	numPolicies := 0
	for _, p := range policies {
		if p == nil {
			continue
		}
		numPolicies++
	}
	if numPolicies > 0 && numPolicies != total {
		return false
	}
	// check if policies match between sites
	var prev *bktpolicy.Policy
	for i, p := range policies {
		if p == nil {
			continue
		}
		if i == 0 {
			prev = p
			continue
		}
		if !prev.Equals(*p) {
			return false
		}
	}
	return true
}

// isBktReplCfgReplicated returns true if all the sites have same number
// of replication rules with all replication features enabled.
func isBktReplCfgReplicated(total int, cfgs []*sreplication.Config) bool {
	cntReplicated := 0
	for _, c := range cfgs {
		if c == nil {
			continue
		}
		cntReplicated++
	}

	if cntReplicated > 0 && cntReplicated != total {
		return false
	}
	// check if policies match between sites
	var prev *sreplication.Config
	for i, c := range cfgs {
		if c == nil {
			continue
		}
		if i == 0 {
			prev = c
			continue
		}
		if len(prev.Rules) != len(c.Rules) {
			return false
		}
		if len(c.Rules) != total-1 {
			return false
		}
		for _, r := range c.Rules {
			if !strings.HasPrefix(r.ID, "site-repl-") {
				return false
			}
			if r.DeleteMarkerReplication.Status == sreplication.Disabled ||
				r.DeleteReplication.Status == sreplication.Disabled ||
				r.ExistingObjectReplication.Status == sreplication.Disabled ||
				r.SourceSelectionCriteria.ReplicaModifications.Status == sreplication.Disabled {
				return false
			}
		}
	}
	return true
}

// SiteReplicationMetaInfo returns the metadata info on buckets, policies etc for the replicated site
func (c *SiteReplicationSys) SiteReplicationMetaInfo(ctx context.Context, objAPI ObjectLayer, opts madmin.SRStatusOptions) (info madmin.SRInfo, err error) {
	if objAPI == nil {
		return info, errSRObjectLayerNotReady
	}
	c.RLock()
	defer c.RUnlock()
	if !c.enabled {
		return info, nil
	}
	info.DeploymentID = globalDeploymentID
	if opts.Buckets || opts.Entity == madmin.SRBucketEntity {
		var (
			buckets []BucketInfo
			err     error
		)
		if opts.Entity == madmin.SRBucketEntity {
			bi, err := objAPI.GetBucketInfo(ctx, opts.EntityValue, BucketOptions{Deleted: opts.ShowDeleted})
			if err != nil {
				if isErrBucketNotFound(err) {
					return info, nil
				}
				return info, errSRBackendIssue(err)
			}
			buckets = append(buckets, bi)
		} else {
			buckets, err = objAPI.ListBuckets(ctx, BucketOptions{Deleted: opts.ShowDeleted})
			if err != nil {
				return info, errSRBackendIssue(err)
			}
		}
		info.Buckets = make(map[string]madmin.SRBucketInfo, len(buckets))
		for _, bucketInfo := range buckets {
			bucket := bucketInfo.Name
			bucketExists := bucketInfo.Deleted.IsZero() || (!bucketInfo.Created.IsZero() && bucketInfo.Created.After(bucketInfo.Deleted))
			bms := madmin.SRBucketInfo{
				Bucket:    bucket,
				CreatedAt: bucketInfo.Created.UTC(),
				DeletedAt: bucketInfo.Deleted.UTC(),
				Location:  globalSite.Region,
			}
			if !bucketExists {
				info.Buckets[bucket] = bms
				continue
			}
			// Get bucket policy if present.
			policy, updatedAt, err := globalBucketMetadataSys.GetPolicyConfig(bucket)
			found := true
			if _, ok := err.(BucketPolicyNotFound); ok {
				found = false
			} else if err != nil {
				return info, errSRBackendIssue(err)
			}
			if found {
				policyJSON, err := json.Marshal(policy)
				if err != nil {
					return info, wrapSRErr(err)
				}
				bms.Policy = policyJSON
				bms.PolicyUpdatedAt = updatedAt
			}

			// Get bucket tags if present.
			tags, updatedAt, err := globalBucketMetadataSys.GetTaggingConfig(bucket)
			found = true
			if _, ok := err.(BucketTaggingNotFound); ok {
				found = false
			} else if err != nil {
				return info, errSRBackendIssue(err)
			}
			if found {
				tagBytes, err := xml.Marshal(tags)
				if err != nil {
					return info, wrapSRErr(err)
				}
				tagCfgStr := base64.StdEncoding.EncodeToString(tagBytes)
				bms.Tags = &tagCfgStr
				bms.TagConfigUpdatedAt = updatedAt
			}

			versioningCfg, updatedAt, err := globalBucketMetadataSys.GetVersioningConfig(bucket)
			found = true
			if versioningCfg != nil && versioningCfg.Status == "" {
				found = false
			} else if err != nil {
				return info, errSRBackendIssue(err)
			}
			if found {
				versionCfgData, err := xml.Marshal(versioningCfg)
				if err != nil {
					return info, wrapSRErr(err)
				}
				versioningCfgStr := base64.StdEncoding.EncodeToString(versionCfgData)
				bms.Versioning = &versioningCfgStr
				bms.VersioningConfigUpdatedAt = updatedAt
			}

			// Get object-lock config if present.
			objLockCfg, updatedAt, err := globalBucketMetadataSys.GetObjectLockConfig(bucket)
			found = true
			if _, ok := err.(BucketObjectLockConfigNotFound); ok {
				found = false
			} else if err != nil {
				return info, errSRBackendIssue(err)
			}
			if found {
				objLockCfgData, err := xml.Marshal(objLockCfg)
				if err != nil {
					return info, wrapSRErr(err)
				}
				objLockStr := base64.StdEncoding.EncodeToString(objLockCfgData)
				bms.ObjectLockConfig = &objLockStr
				bms.ObjectLockConfigUpdatedAt = updatedAt
			}

			// Get quota config if present
			quotaConfig, updatedAt, err := globalBucketMetadataSys.GetQuotaConfig(ctx, bucket)
			found = true
			if _, ok := err.(BucketQuotaConfigNotFound); ok {
				found = false
			} else if err != nil {
				return info, errSRBackendIssue(err)
			}
			if found {
				quotaConfigJSON, err := json.Marshal(quotaConfig)
				if err != nil {
					return info, wrapSRErr(err)
				}
				quotaConfigStr := base64.StdEncoding.EncodeToString(quotaConfigJSON)
				bms.QuotaConfig = &quotaConfigStr
				bms.QuotaConfigUpdatedAt = updatedAt
			}

			// Get existing bucket bucket encryption settings
			sseConfig, updatedAt, err := globalBucketMetadataSys.GetSSEConfig(bucket)
			found = true
			if _, ok := err.(BucketSSEConfigNotFound); ok {
				found = false
			} else if err != nil {
				return info, errSRBackendIssue(err)
			}
			if found {
				sseConfigData, err := xml.Marshal(sseConfig)
				if err != nil {
					return info, wrapSRErr(err)
				}
				sseConfigStr := base64.StdEncoding.EncodeToString(sseConfigData)
				bms.SSEConfig = &sseConfigStr
				bms.SSEConfigUpdatedAt = updatedAt
			}

			// Get replication config if present
			rcfg, updatedAt, err := globalBucketMetadataSys.GetReplicationConfig(ctx, bucket)
			found = true
			if _, ok := err.(BucketReplicationConfigNotFound); ok {
				found = false
			} else if err != nil {
				return info, errSRBackendIssue(err)
			}
			if found {
				rcfgXML, err := xml.Marshal(rcfg)
				if err != nil {
					return info, wrapSRErr(err)
				}
				rcfgXMLStr := base64.StdEncoding.EncodeToString(rcfgXML)
				bms.ReplicationConfig = &rcfgXMLStr
				bms.ReplicationConfigUpdatedAt = updatedAt
			}
			info.Buckets[bucket] = bms
		}
	}

	if opts.Policies || opts.Entity == madmin.SRPolicyEntity {
		var allPolicies map[string]PolicyDoc
		if opts.Entity == madmin.SRPolicyEntity {
			if p, err := globalIAMSys.store.GetPolicyDoc(opts.EntityValue); err == nil {
				allPolicies = map[string]PolicyDoc{opts.EntityValue: p}
			}
		} else {
			// Replicate IAM policies on local to all peers.
			allPolicies, err = globalIAMSys.ListPolicyDocs(ctx, "")
			if err != nil {
				return info, errSRBackendIssue(err)
			}
		}
		info.Policies = make(map[string]madmin.SRIAMPolicy, len(allPolicies))
		for pname, policyDoc := range allPolicies {
			policyJSON, err := json.Marshal(policyDoc.Policy)
			if err != nil {
				return info, wrapSRErr(err)
			}
			info.Policies[pname] = madmin.SRIAMPolicy{Policy: json.RawMessage(policyJSON), UpdatedAt: policyDoc.UpdateDate}
		}
	}

	if opts.Users || opts.Entity == madmin.SRUserEntity {
		// Replicate policy mappings on local to all peers.
		userPolicyMap := make(map[string]MappedPolicy)
		if opts.Entity == madmin.SRUserEntity {
			if mp, ok := globalIAMSys.store.GetMappedPolicy(opts.EntityValue, false); ok {
				userPolicyMap[opts.EntityValue] = mp
			}
		} else {
			globalIAMSys.store.rlock()
			stsErr := globalIAMSys.store.loadMappedPolicies(ctx, stsUser, false, userPolicyMap)
			globalIAMSys.store.runlock()
			if stsErr != nil {
				return info, errSRBackendIssue(stsErr)
			}
			globalIAMSys.store.rlock()
			usrErr := globalIAMSys.store.loadMappedPolicies(ctx, regUser, false, userPolicyMap)
			globalIAMSys.store.runlock()
			if usrErr != nil {
				return info, errSRBackendIssue(usrErr)
			}
		}
		info.UserPolicies = make(map[string]madmin.SRPolicyMapping, len(userPolicyMap))
		for user, mp := range userPolicyMap {
			info.UserPolicies[user] = madmin.SRPolicyMapping{
				IsGroup:     false,
				UserOrGroup: user,
				Policy:      mp.Policies,
				UpdatedAt:   mp.UpdatedAt,
			}
		}
		info.UserInfoMap = make(map[string]madmin.UserInfo)
		if opts.Entity == madmin.SRUserEntity {
			if ui, err := globalIAMSys.GetUserInfo(ctx, opts.EntityValue); err == nil {
				info.UserInfoMap[opts.EntityValue] = ui
			}
		} else {
			//  get users/group info on local.
			userInfoMap, err := globalIAMSys.ListUsers(ctx)
			if err != nil {
				return info, errSRBackendIssue(err)
			}
			for user, ui := range userInfoMap {
				info.UserInfoMap[user] = ui
				svcAccts, err := globalIAMSys.ListServiceAccounts(ctx, user)
				if err != nil {
					return info, errSRBackendIssue(err)
				}
				for _, svcAcct := range svcAccts {
					info.UserInfoMap[svcAcct.AccessKey] = madmin.UserInfo{
						Status: madmin.AccountStatus(svcAcct.Status),
					}
				}
				tempAccts, err := globalIAMSys.ListTempAccounts(ctx, user)
				if err != nil {
					return info, errSRBackendIssue(err)
				}
				for _, tempAcct := range tempAccts {
					info.UserInfoMap[tempAcct.Credentials.AccessKey] = madmin.UserInfo{
						Status: madmin.AccountStatus(tempAcct.Credentials.Status),
					}
				}
			}
		}
	}

	if opts.Groups || opts.Entity == madmin.SRGroupEntity {
		// Replicate policy mappings on local to all peers.
		groupPolicyMap := make(map[string]MappedPolicy)
		if opts.Entity == madmin.SRUserEntity {
			if mp, ok := globalIAMSys.store.GetMappedPolicy(opts.EntityValue, true); ok {
				groupPolicyMap[opts.EntityValue] = mp
			}
		} else {
			globalIAMSys.store.rlock()
			stsErr := globalIAMSys.store.loadMappedPolicies(ctx, stsUser, true, groupPolicyMap)
			globalIAMSys.store.runlock()
			if stsErr != nil {
				return info, errSRBackendIssue(stsErr)
			}
			globalIAMSys.store.rlock()
			userErr := globalIAMSys.store.loadMappedPolicies(ctx, regUser, true, groupPolicyMap)
			globalIAMSys.store.runlock()
			if userErr != nil {
				return info, errSRBackendIssue(userErr)
			}
		}

		info.GroupPolicies = make(map[string]madmin.SRPolicyMapping, len(c.state.Peers))
		for group, mp := range groupPolicyMap {
			info.GroupPolicies[group] = madmin.SRPolicyMapping{
				IsGroup:     true,
				UserOrGroup: group,
				Policy:      mp.Policies,
				UpdatedAt:   mp.UpdatedAt,
			}
		}
		info.GroupDescMap = make(map[string]madmin.GroupDesc)
		if opts.Entity == madmin.SRGroupEntity {
			if gd, err := globalIAMSys.GetGroupDescription(opts.EntityValue); err == nil {
				info.GroupDescMap[opts.EntityValue] = gd
			}
		} else {
			//  get users/group info on local.
			groups, errG := globalIAMSys.ListGroups(ctx)
			if errG != nil {
				return info, errSRBackendIssue(errG)
			}
			groupDescMap := make(map[string]madmin.GroupDesc, len(groups))
			for _, g := range groups {
				groupDescMap[g], errG = globalIAMSys.GetGroupDescription(g)
				if errG != nil {
					return info, errSRBackendIssue(errG)
				}
			}
			for group, d := range groupDescMap {
				info.GroupDescMap[group] = d
			}
		}
	}
	return info, nil
}

// EditPeerCluster - edits replication configuration and updates peer endpoint.
func (c *SiteReplicationSys) EditPeerCluster(ctx context.Context, peer madmin.PeerInfo) (madmin.ReplicateEditStatus, error) {
	sites, err := c.GetClusterInfo(ctx)
	if err != nil {
		return madmin.ReplicateEditStatus{}, errSRBackendIssue(err)
	}
	if !sites.Enabled {
		return madmin.ReplicateEditStatus{}, errSRNotEnabled
	}

	var (
		found     bool
		admClient *madmin.AdminClient
	)

	for _, v := range sites.Sites {
		if peer.DeploymentID == v.DeploymentID {
			found = true
			if peer.Endpoint == v.Endpoint {
				return madmin.ReplicateEditStatus{}, errSRInvalidRequest(fmt.Errorf("Endpoint %s entered for deployment id %s already configured in site replication", v.Endpoint, v.DeploymentID))
			}
			admClient, err = c.getAdminClientWithEndpoint(ctx, v.DeploymentID, peer.Endpoint)
			if err != nil {
				return madmin.ReplicateEditStatus{}, errSRPeerResp(fmt.Errorf("unable to create admin client for %s: %w", v.Name, err))
			}
			// check if endpoint is reachable
			if _, err = admClient.ServerInfo(ctx); err != nil {
				return madmin.ReplicateEditStatus{}, errSRPeerResp(fmt.Errorf("Endpoint %s not reachable: %w", peer.Endpoint, err))
			}
		}
	}

	if !found {
		return madmin.ReplicateEditStatus{}, errSRInvalidRequest(fmt.Errorf("%s not found in existing replicated sites", peer.DeploymentID))
	}

	errs := make(map[string]error, len(c.state.Peers))
	var wg sync.WaitGroup

	pi := c.state.Peers[peer.DeploymentID]
	pi.Endpoint = peer.Endpoint

	for i, v := range sites.Sites {
		if v.DeploymentID == globalDeploymentID {
			c.state.Peers[peer.DeploymentID] = pi
			continue
		}
		wg.Add(1)
		go func(pi madmin.PeerInfo, i int) {
			defer wg.Done()
			v := sites.Sites[i]
			admClient, err := c.getAdminClient(ctx, v.DeploymentID)
			if v.DeploymentID == peer.DeploymentID {
				admClient, err = c.getAdminClientWithEndpoint(ctx, v.DeploymentID, peer.Endpoint)
			}
			if err != nil {
				errs[v.DeploymentID] = errSRPeerResp(fmt.Errorf("unable to create admin client for %s: %w", v.Name, err))
				return
			}
			if err = admClient.SRPeerEdit(ctx, pi); err != nil {
				errs[v.DeploymentID] = errSRPeerResp(fmt.Errorf("unable to update peer %s: %w", v.Name, err))
				return
			}
		}(pi, i)
	}

	wg.Wait()
	for dID, err := range errs {
		if err != nil {
			return madmin.ReplicateEditStatus{}, errSRPeerResp(fmt.Errorf("unable to update peer %s: %w", c.state.Peers[dID].Name, err))
		}
	}
	// we can now save the cluster replication configuration state.
	if err = c.saveToDisk(ctx, c.state); err != nil {
		return madmin.ReplicateEditStatus{
			Status:    madmin.ReplicateAddStatusPartial,
			ErrDetail: fmt.Sprintf("unable to save cluster-replication state on local: %v", err),
		}, nil
	}

	result := madmin.ReplicateEditStatus{
		Success: true,
		Status:  fmt.Sprintf("Cluster replication configuration updated with endpoint %s for peer %s successfully", peer.Endpoint, peer.Name),
	}
	return result, nil
}

// PeerEditReq - internal API handler to respond to a peer cluster's request
// to edit endpoint.
func (c *SiteReplicationSys) PeerEditReq(ctx context.Context, arg madmin.PeerInfo) error {
	ourName := ""
	for i := range c.state.Peers {
		p := c.state.Peers[i]
		if p.DeploymentID == arg.DeploymentID {
			p.Endpoint = arg.Endpoint
			c.state.Peers[arg.DeploymentID] = p
		}
		if p.DeploymentID == globalDeploymentID {
			ourName = p.Name
		}
	}
	if err := c.saveToDisk(ctx, c.state); err != nil {
		return errSRBackendIssue(fmt.Errorf("unable to save cluster-replication state to disk on %s: %v", ourName, err))
	}
	return nil
}

const siteHealTimeInterval = 10 * time.Second

func (c *SiteReplicationSys) startHealRoutine(ctx context.Context, objAPI ObjectLayer) {
	healTimer := time.NewTimer(siteHealTimeInterval)
	defer healTimer.Stop()

	for {
		select {
		case <-healTimer.C:
			c.RLock()
			enabled := c.enabled
			c.RUnlock()
			if enabled {
				c.healIAMSystem(ctx, objAPI) // heal IAM system first
				c.healBuckets(ctx, objAPI)   // heal buckets subsequently
			}
			healTimer.Reset(siteHealTimeInterval)

		case <-ctx.Done():
			return
		}
	}
}

type srBucketStatsSummary struct {
	madmin.SRBucketStatsSummary
	meta srBucketMetaInfo
}

type srPolicyStatsSummary struct {
	madmin.SRPolicyStatsSummary
	policy srPolicy
}

type srUserStatsSummary struct {
	madmin.SRUserStatsSummary
	userInfo   srUserInfo
	userPolicy srPolicyMapping
}

type srGroupStatsSummary struct {
	madmin.SRGroupStatsSummary
	groupDesc   srGroupDesc
	groupPolicy srPolicyMapping
}

type srStatusInfo struct {
	// SRStatusInfo returns detailed status on site replication status
	Enabled      bool
	MaxBuckets   int                             // maximum buckets seen across sites
	MaxUsers     int                             // maximum users seen across sites
	MaxGroups    int                             // maximum groups seen across sites
	MaxPolicies  int                             // maximum policies across sites
	Sites        map[string]madmin.PeerInfo      // deployment->sitename
	StatsSummary map[string]madmin.SRSiteSummary // map of deployment id -> site stat
	// BucketStats map of bucket to slice of deployment IDs with stats. This is populated only if there are
	// mismatches or if a specific bucket's stats are requested
	BucketStats map[string]map[string]srBucketStatsSummary
	// PolicyStats map of policy to slice of deployment IDs with stats. This is populated only if there are
	// mismatches or if a specific bucket's stats are requested
	PolicyStats map[string]map[string]srPolicyStatsSummary
	// UserStats map of user to slice of deployment IDs with stats. This is populated only if there are
	// mismatches or if a specific bucket's stats are requested
	UserStats map[string]map[string]srUserStatsSummary
	// GroupStats map of group to slice of deployment IDs with stats. This is populated only if there are
	// mismatches or if a specific bucket's stats are requested
	GroupStats map[string]map[string]srGroupStatsSummary
}

// SRBucketDeleteOp - type of delete op
type SRBucketDeleteOp string

const (
	// MarkDelete creates .minio.sys/buckets/.deleted/<bucket> vol entry to hold onto deleted bucket's state
	// until peers are synced in site replication setup.
	MarkDelete SRBucketDeleteOp = "MarkDelete"

	// Purge deletes the .minio.sys/buckets/.deleted/<bucket> vol entry
	Purge SRBucketDeleteOp = "Purge"
	// NoOp no action needed
	NoOp SRBucketDeleteOp = "NoOp"
)

// Empty returns true if this Op is not set
func (s SRBucketDeleteOp) Empty() bool {
	return string(s) == "" || string(s) == string(NoOp)
}

func getSRBucketDeleteOp(isSiteReplicated bool) SRBucketDeleteOp {
	if !isSiteReplicated {
		return NoOp
	}
	return MarkDelete
}

func (c *SiteReplicationSys) healBuckets(ctx context.Context, objAPI ObjectLayer) error {
	buckets, err := c.listBuckets(ctx)
	if err != nil {
		return err
	}
	for _, bi := range buckets {
		bucket := bi.Name
		info, err := c.siteReplicationStatus(ctx, objAPI, madmin.SRStatusOptions{
			Entity:      madmin.SRBucketEntity,
			EntityValue: bucket,
			ShowDeleted: true,
		})
		if err != nil {
			logger.LogIf(ctx, err)
			continue
		}

		c.healBucket(ctx, objAPI, bucket, info)

		if bi.Deleted.IsZero() || (!bi.Created.IsZero() && bi.Deleted.Before(bi.Created)) {
			c.healVersioningMetadata(ctx, objAPI, bucket, info)
			c.healOLockConfigMetadata(ctx, objAPI, bucket, info)
			c.healSSEMetadata(ctx, objAPI, bucket, info)
			c.healBucketReplicationConfig(ctx, objAPI, bucket, info)
			c.healBucketPolicies(ctx, objAPI, bucket, info)
			c.healTagMetadata(ctx, objAPI, bucket, info)
			c.healBucketQuotaConfig(ctx, objAPI, bucket, info)
		}
		// Notification and ILM are site specific settings.
	}
	return nil
}

func (c *SiteReplicationSys) healTagMetadata(ctx context.Context, objAPI ObjectLayer, bucket string, info srStatusInfo) error {
	bs := info.BucketStats[bucket]

	c.RLock()
	defer c.RUnlock()
	if !c.enabled {
		return nil
	}
	var (
		latestID, latestPeerName string
		lastUpdate               time.Time
		latestTaggingConfig      *string
	)

	for dID, ss := range bs {
		if lastUpdate.IsZero() {
			lastUpdate = ss.meta.TagConfigUpdatedAt
			latestID = dID
			latestTaggingConfig = ss.meta.Tags
		}
		// avoid considering just created buckets as latest. Perhaps this site
		// just joined cluster replication and yet to be sync'd
		if ss.meta.CreatedAt.Equal(ss.meta.TagConfigUpdatedAt) {
			continue
		}
		if ss.meta.TagConfigUpdatedAt.After(lastUpdate) {
			lastUpdate = ss.meta.TagConfigUpdatedAt
			latestID = dID
			latestTaggingConfig = ss.meta.Tags
		}
	}
	latestPeerName = info.Sites[latestID].Name
	var latestTaggingConfigBytes []byte
	var err error
	if latestTaggingConfig != nil {
		latestTaggingConfigBytes, err = base64.StdEncoding.DecodeString(*latestTaggingConfig)
		if err != nil {
			return err
		}
	}
	for dID, bStatus := range bs {
		if !bStatus.TagMismatch {
			continue
		}
		if isBucketMetadataEqual(latestTaggingConfig, bStatus.meta.Tags) {
			continue
		}
		if dID == globalDeploymentID {
			if _, err := globalBucketMetadataSys.Update(ctx, bucket, bucketTaggingConfig, latestTaggingConfigBytes); err != nil {
				logger.LogIf(ctx, fmt.Errorf("Error healing tagging metadata from peer site %s : %w", latestPeerName, err))
			}
			continue
		}

		admClient, err := c.getAdminClient(ctx, dID)
		if err != nil {
			return wrapSRErr(err)
		}
		peerName := info.Sites[dID].Name
		err = admClient.SRPeerReplicateBucketMeta(ctx, madmin.SRBucketMeta{
			Type:   madmin.SRBucketMetaTypeTags,
			Bucket: bucket,
			Tags:   latestTaggingConfig,
		})
		if err != nil {
			logger.LogIf(ctx, c.annotatePeerErr(peerName, replicateBucketMetadata,
				fmt.Errorf("Error healing tagging metadata for peer %s from peer %s : %w", peerName, latestPeerName, err)))
		}
	}
	return nil
}

func (c *SiteReplicationSys) healBucketPolicies(ctx context.Context, objAPI ObjectLayer, bucket string, info srStatusInfo) error {
	bs := info.BucketStats[bucket]

	c.RLock()
	defer c.RUnlock()
	if !c.enabled {
		return nil
	}
	var (
		latestID, latestPeerName string
		lastUpdate               time.Time
		latestIAMPolicy          json.RawMessage
	)

	for dID, ss := range bs {
		if lastUpdate.IsZero() {
			lastUpdate = ss.meta.PolicyUpdatedAt
			latestID = dID
			latestIAMPolicy = ss.meta.Policy
		}
		// avoid considering just created buckets as latest. Perhaps this site
		// just joined cluster replication and yet to be sync'd
		if ss.meta.CreatedAt.Equal(ss.meta.PolicyUpdatedAt) {
			continue
		}
		if ss.meta.PolicyUpdatedAt.After(lastUpdate) {
			lastUpdate = ss.meta.PolicyUpdatedAt
			latestID = dID
			latestIAMPolicy = ss.meta.Policy
		}
	}
	latestPeerName = info.Sites[latestID].Name
	for dID, bStatus := range bs {
		if !bStatus.PolicyMismatch {
			continue
		}
		if strings.EqualFold(string(latestIAMPolicy), string(bStatus.meta.Policy)) {
			continue
		}
		if dID == globalDeploymentID {
			if _, err := globalBucketMetadataSys.Update(ctx, bucket, bucketPolicyConfig, latestIAMPolicy); err != nil {
				logger.LogIf(ctx, fmt.Errorf("Error healing bucket policy metadata from peer site %s : %w", latestPeerName, err))
			}
			continue
		}

		admClient, err := c.getAdminClient(ctx, dID)
		if err != nil {
			return wrapSRErr(err)
		}
		peerName := info.Sites[dID].Name
		if err = admClient.SRPeerReplicateBucketMeta(ctx, madmin.SRBucketMeta{
			Type:      madmin.SRBucketMetaTypePolicy,
			Bucket:    bucket,
			Policy:    latestIAMPolicy,
			UpdatedAt: lastUpdate,
		}); err != nil {
			logger.LogIf(ctx, c.annotatePeerErr(peerName, replicateBucketMetadata,
				fmt.Errorf("Error healing bucket policy metadata for peer %s from peer %s : %w",
					peerName, latestPeerName, err)))
		}
	}
	return nil
}

func (c *SiteReplicationSys) healBucketQuotaConfig(ctx context.Context, objAPI ObjectLayer, bucket string, info srStatusInfo) error {
	bs := info.BucketStats[bucket]

	c.RLock()
	defer c.RUnlock()
	if !c.enabled {
		return nil
	}
	var (
		latestID, latestPeerName string
		lastUpdate               time.Time
		latestQuotaConfig        *string
		latestQuotaConfigBytes   []byte
	)

	for dID, ss := range bs {
		if lastUpdate.IsZero() {
			lastUpdate = ss.meta.QuotaConfigUpdatedAt
			latestID = dID
			latestQuotaConfig = ss.meta.QuotaConfig
		}
		// avoid considering just created buckets as latest. Perhaps this site
		// just joined cluster replication and yet to be sync'd
		if ss.meta.CreatedAt.Equal(ss.meta.QuotaConfigUpdatedAt) {
			continue
		}
		if ss.meta.QuotaConfigUpdatedAt.After(lastUpdate) {
			lastUpdate = ss.meta.QuotaConfigUpdatedAt
			latestID = dID
			latestQuotaConfig = ss.meta.QuotaConfig
		}
	}

	var err error
	if latestQuotaConfig != nil {
		latestQuotaConfigBytes, err = base64.StdEncoding.DecodeString(*latestQuotaConfig)
		if err != nil {
			return err
		}
	}

	latestPeerName = info.Sites[latestID].Name
	for dID, bStatus := range bs {
		if !bStatus.QuotaCfgMismatch {
			continue
		}
		if isBucketMetadataEqual(latestQuotaConfig, bStatus.meta.QuotaConfig) {
			continue
		}
		if dID == globalDeploymentID {
			if _, err := globalBucketMetadataSys.Update(ctx, bucket, bucketQuotaConfigFile, latestQuotaConfigBytes); err != nil {
				logger.LogIf(ctx, fmt.Errorf("Error healing quota metadata from peer site %s : %w", latestPeerName, err))
			}
			continue
		}

		admClient, err := c.getAdminClient(ctx, dID)
		if err != nil {
			return wrapSRErr(err)
		}
		peerName := info.Sites[dID].Name

		if err = admClient.SRPeerReplicateBucketMeta(ctx, madmin.SRBucketMeta{
			Type:      madmin.SRBucketMetaTypeQuotaConfig,
			Bucket:    bucket,
			Quota:     latestQuotaConfigBytes,
			UpdatedAt: lastUpdate,
		}); err != nil {
			logger.LogIf(ctx, c.annotatePeerErr(peerName, replicateBucketMetadata,
				fmt.Errorf("Error healing quota config metadata for peer %s from peer %s : %w",
					peerName, latestPeerName, err)))
		}
	}
	return nil
}

func (c *SiteReplicationSys) healVersioningMetadata(ctx context.Context, objAPI ObjectLayer, bucket string, info srStatusInfo) error {
	c.RLock()
	defer c.RUnlock()
	if !c.enabled {
		return nil
	}
	var (
		latestID, latestPeerName string
		lastUpdate               time.Time
		latestVersioningConfig   *string
	)

	bs := info.BucketStats[bucket]
	for dID, ss := range bs {
		if lastUpdate.IsZero() {
			lastUpdate = ss.meta.VersioningConfigUpdatedAt
			latestID = dID
			latestVersioningConfig = ss.meta.Versioning
		}
		// avoid considering just created buckets as latest. Perhaps this site
		// just joined cluster replication and yet to be sync'd
		if ss.meta.CreatedAt.Equal(ss.meta.VersioningConfigUpdatedAt) {
			continue
		}
		if ss.meta.VersioningConfigUpdatedAt.After(lastUpdate) {
			lastUpdate = ss.meta.VersioningConfigUpdatedAt
			latestID = dID
			latestVersioningConfig = ss.meta.Versioning
		}
	}

	latestPeerName = info.Sites[latestID].Name
	var latestVersioningConfigBytes []byte
	var err error
	if latestVersioningConfig != nil {
		latestVersioningConfigBytes, err = base64.StdEncoding.DecodeString(*latestVersioningConfig)
		if err != nil {
			return err
		}
	}

	for dID, bStatus := range bs {
		if !bStatus.VersioningConfigMismatch {
			continue
		}
		if isBucketMetadataEqual(latestVersioningConfig, bStatus.meta.Versioning) {
			continue
		}
		if dID == globalDeploymentID {
			if _, err := globalBucketMetadataSys.Update(ctx, bucket, bucketVersioningConfig, latestVersioningConfigBytes); err != nil {
				logger.LogIf(ctx, fmt.Errorf("Error healing versioning metadata from peer site %s : %w", latestPeerName, err))
			}
			continue
		}

		admClient, err := c.getAdminClient(ctx, dID)
		if err != nil {
			return wrapSRErr(err)
		}
		peerName := info.Sites[dID].Name
		err = admClient.SRPeerReplicateBucketMeta(ctx, madmin.SRBucketMeta{
			Type:       madmin.SRBucketMetaTypeVersionConfig,
			Bucket:     bucket,
			Versioning: latestVersioningConfig,
			UpdatedAt:  lastUpdate,
		})
		if err != nil {
			logger.LogIf(ctx, c.annotatePeerErr(peerName, replicateBucketMetadata,
				fmt.Errorf("Error healing versioning config metadata for peer %s from peer %s : %w",
					peerName, latestPeerName, err)))
		}
	}
	return nil
}

func (c *SiteReplicationSys) healSSEMetadata(ctx context.Context, objAPI ObjectLayer, bucket string, info srStatusInfo) error {
	c.RLock()
	defer c.RUnlock()
	if !c.enabled {
		return nil
	}
	var (
		latestID, latestPeerName string
		lastUpdate               time.Time
		latestSSEConfig          *string
	)

	bs := info.BucketStats[bucket]
	for dID, ss := range bs {
		if lastUpdate.IsZero() {
			lastUpdate = ss.meta.SSEConfigUpdatedAt
			latestID = dID
			latestSSEConfig = ss.meta.SSEConfig
		}
		// avoid considering just created buckets as latest. Perhaps this site
		// just joined cluster replication and yet to be sync'd
		if ss.meta.CreatedAt.Equal(ss.meta.SSEConfigUpdatedAt) {
			continue
		}
		if ss.meta.SSEConfigUpdatedAt.After(lastUpdate) {
			lastUpdate = ss.meta.SSEConfigUpdatedAt
			latestID = dID
			latestSSEConfig = ss.meta.SSEConfig
		}
	}

	latestPeerName = info.Sites[latestID].Name
	var latestSSEConfigBytes []byte
	var err error
	if latestSSEConfig != nil {
		latestSSEConfigBytes, err = base64.StdEncoding.DecodeString(*latestSSEConfig)
		if err != nil {
			return err
		}
	}

	for dID, bStatus := range bs {
		if !bStatus.SSEConfigMismatch {
			continue
		}
		if isBucketMetadataEqual(latestSSEConfig, bStatus.meta.SSEConfig) {
			continue
		}
		if dID == globalDeploymentID {
			if _, err := globalBucketMetadataSys.Update(ctx, bucket, bucketSSEConfig, latestSSEConfigBytes); err != nil {
				logger.LogIf(ctx, fmt.Errorf("Error healing sse metadata from peer site %s : %w", latestPeerName, err))
			}
			continue
		}

		admClient, err := c.getAdminClient(ctx, dID)
		if err != nil {
			return wrapSRErr(err)
		}
		peerName := info.Sites[dID].Name
		err = admClient.SRPeerReplicateBucketMeta(ctx, madmin.SRBucketMeta{
			Type:      madmin.SRBucketMetaTypeSSEConfig,
			Bucket:    bucket,
			SSEConfig: latestSSEConfig,
			UpdatedAt: lastUpdate,
		})
		if err != nil {
			logger.LogIf(ctx, c.annotatePeerErr(peerName, replicateBucketMetadata,
				fmt.Errorf("Error healing SSE config metadata for peer %s from peer %s : %w",
					peerName, latestPeerName, err)))
		}
	}
	return nil
}

func (c *SiteReplicationSys) healOLockConfigMetadata(ctx context.Context, objAPI ObjectLayer, bucket string, info srStatusInfo) error {
	bs := info.BucketStats[bucket]

	c.RLock()
	defer c.RUnlock()
	if !c.enabled {
		return nil
	}
	var (
		latestID, latestPeerName string
		lastUpdate               time.Time
		latestObjLockConfig      *string
	)

	for dID, ss := range bs {
		if lastUpdate.IsZero() {
			lastUpdate = ss.meta.ObjectLockConfigUpdatedAt
			latestID = dID
			latestObjLockConfig = ss.meta.ObjectLockConfig
		}
		// avoid considering just created buckets as latest. Perhaps this site
		// just joined cluster replication and yet to be sync'd
		if ss.meta.CreatedAt.Equal(ss.meta.ObjectLockConfigUpdatedAt) {
			continue
		}
		if ss.meta.ObjectLockConfig != nil && ss.meta.ObjectLockConfigUpdatedAt.After(lastUpdate) {
			lastUpdate = ss.meta.ObjectLockConfigUpdatedAt
			latestID = dID
			latestObjLockConfig = ss.meta.ObjectLockConfig
		}
	}
	latestPeerName = info.Sites[latestID].Name
	var latestObjLockConfigBytes []byte
	var err error
	if latestObjLockConfig != nil {
		latestObjLockConfigBytes, err = base64.StdEncoding.DecodeString(*latestObjLockConfig)
		if err != nil {
			return err
		}
	}

	for dID, bStatus := range bs {
		if !bStatus.OLockConfigMismatch {
			continue
		}
		if isBucketMetadataEqual(latestObjLockConfig, bStatus.meta.ObjectLockConfig) {
			continue
		}
		if dID == globalDeploymentID {
			if _, err := globalBucketMetadataSys.Update(ctx, bucket, objectLockConfig, latestObjLockConfigBytes); err != nil {
				logger.LogIf(ctx, fmt.Errorf("Error healing objectlock config metadata from peer site %s : %w", latestPeerName, err))
			}
			continue
		}

		admClient, err := c.getAdminClient(ctx, dID)
		if err != nil {
			return wrapSRErr(err)
		}
		peerName := info.Sites[dID].Name
		err = admClient.SRPeerReplicateBucketMeta(ctx, madmin.SRBucketMeta{
			Type:      madmin.SRBucketMetaTypeObjectLockConfig,
			Bucket:    bucket,
			Tags:      latestObjLockConfig,
			UpdatedAt: lastUpdate,
		})
		if err != nil {
			logger.LogIf(ctx, c.annotatePeerErr(peerName, replicateBucketMetadata,
				fmt.Errorf("Error healing object lock config metadata for peer %s from peer %s : %w",
					peerName, latestPeerName, err)))
		}
	}
	return nil
}

func (c *SiteReplicationSys) purgeDeletedBucket(ctx context.Context, objAPI ObjectLayer, bucket string) {
	z, ok := objAPI.(*erasureServerPools)
	if !ok {
		if z, ok := objAPI.(*erasureSingle); ok {
			z.purgeDelete(context.Background(), minioMetaBucket, pathJoin(bucketMetaPrefix, deletedBucketsPrefix, bucket))
		}
		return
	}
	z.purgeDelete(context.Background(), minioMetaBucket, pathJoin(bucketMetaPrefix, deletedBucketsPrefix, bucket))
}

// healBucket creates/deletes the bucket according to latest state across clusters participating in site replication.
func (c *SiteReplicationSys) healBucket(ctx context.Context, objAPI ObjectLayer, bucket string, info srStatusInfo) error {
	bs := info.BucketStats[bucket]
	c.RLock()
	defer c.RUnlock()
	if !c.enabled {
		return nil
	}
	numSites := len(c.state.Peers)
	mostRecent := func(d1, d2 time.Time) time.Time {
		if d1.IsZero() {
			return d2
		}
		if d2.IsZero() {
			return d1
		}
		if d1.After(d2) {
			return d1
		}
		return d2
	}

	var (
		latestID   string
		lastUpdate time.Time
		withB      []string
		missingB   []string
		deletedCnt int
	)
	for dID, ss := range bs {
		if lastUpdate.IsZero() {
			lastUpdate = mostRecent(ss.meta.CreatedAt, ss.meta.DeletedAt)
			latestID = dID
		}
		recentUpdt := mostRecent(ss.meta.CreatedAt, ss.meta.DeletedAt)
		if recentUpdt.After(lastUpdate) {
			lastUpdate = recentUpdt
			latestID = dID
		}
		if ss.BucketMarkedDeleted {
			deletedCnt++
		}
		if ss.HasBucket {
			withB = append(withB, dID)
		} else {
			missingB = append(missingB, dID)
		}
	}

	latestPeerName := info.Sites[latestID].Name
	bStatus := info.BucketStats[bucket][latestID].meta
	isMakeBucket := len(missingB) > 0
	deleteOp := NoOp
	if latestID != globalDeploymentID {
		return nil
	}
	if lastUpdate.Equal(bStatus.DeletedAt) {
		isMakeBucket = false
		switch {
		case len(withB) == numSites && deletedCnt == numSites:
			deleteOp = NoOp
		case len(withB) == 0 && len(missingB) == numSites:
			deleteOp = Purge
		default:
			deleteOp = MarkDelete
		}
	}
	if isMakeBucket {
		var opts MakeBucketOptions
		optsMap := make(map[string]string)
		if bStatus.Location != "" {
			optsMap["location"] = bStatus.Location
			opts.Location = bStatus.Location
		}

		optsMap["versioningEnabled"] = "true"
		opts.VersioningEnabled = true
		opts.CreatedAt = bStatus.CreatedAt
		optsMap["createdAt"] = bStatus.CreatedAt.Format(time.RFC3339Nano)

		if bStatus.ObjectLockConfig != nil {
			config, err := base64.StdEncoding.DecodeString(*bStatus.ObjectLockConfig)
			if err != nil {
				return err
			}
			if bytes.Equal([]byte(string(config)), enabledBucketObjectLockConfig) {
				optsMap["lockEnabled"] = "true"
				opts.LockEnabled = true
			}
		}
		for _, dID := range missingB {
			peerName := info.Sites[dID].Name
			if dID == globalDeploymentID {
				err := c.PeerBucketMakeWithVersioningHandler(ctx, bucket, opts)
				if err != nil {
					return c.annotateErr(makeBucketWithVersion, fmt.Errorf("error healing bucket for site replication %w from %s -> %s",
						err, latestPeerName, peerName))
				}
			} else {
				admClient, err := c.getAdminClient(ctx, dID)
				if err != nil {
					return c.annotateErr(configureReplication, fmt.Errorf("unable to use admin client for %s: %w", dID, err))
				}
				if err = admClient.SRPeerBucketOps(ctx, bucket, madmin.MakeWithVersioningBktOp, optsMap); err != nil {
					return c.annotatePeerErr(peerName, makeBucketWithVersion, err)
				}
				if err = admClient.SRPeerBucketOps(ctx, bucket, madmin.ConfigureReplBktOp, nil); err != nil {
					return c.annotatePeerErr(peerName, configureReplication, err)
				}
			}
		}
		if len(missingB) > 0 {
			// configure replication from current cluster to other clusters
			err := c.PeerBucketConfigureReplHandler(ctx, bucket)
			if err != nil {
				return c.annotateErr(configureReplication, err)
			}
		}
		return nil
	}
	// all buckets are marked deleted across sites at this point. It should be safe to purge the .minio.sys/buckets/.deleted/<bucket> entry
	// from disk
	if deleteOp == Purge {
		for _, dID := range missingB {
			peerName := info.Sites[dID].Name
			if dID == globalDeploymentID {
				c.purgeDeletedBucket(ctx, objAPI, bucket)
			} else {
				admClient, err := c.getAdminClient(ctx, dID)
				if err != nil {
					return c.annotateErr(configureReplication, fmt.Errorf("unable to use admin client for %s: %w", dID, err))
				}
				if err = admClient.SRPeerBucketOps(ctx, bucket, madmin.PurgeDeletedBucketOp, nil); err != nil {
					return c.annotatePeerErr(peerName, deleteBucket, err)
				}
			}
		}
	}
	// Mark buckets deleted on remaining peers
	if deleteOp == MarkDelete {
		for _, dID := range withB {
			peerName := info.Sites[dID].Name
			if dID == globalDeploymentID {
				err := c.PeerBucketDeleteHandler(ctx, bucket, DeleteBucketOptions{
					Force: true,
				})
				if err != nil {
					return c.annotateErr(deleteBucket, fmt.Errorf("error healing bucket for site replication %w from %s -> %s",
						err, latestPeerName, peerName))
				}
			} else {
				admClient, err := c.getAdminClient(ctx, dID)
				if err != nil {
					return c.annotateErr(configureReplication, fmt.Errorf("unable to use admin client for %s: %w", dID, err))
				}
				if err = admClient.SRPeerBucketOps(ctx, bucket, madmin.ForceDeleteBucketBktOp, nil); err != nil {
					return c.annotatePeerErr(peerName, deleteBucket, err)
				}
			}
		}
	}

	return nil
}

func (c *SiteReplicationSys) healBucketReplicationConfig(ctx context.Context, objAPI ObjectLayer, bucket string, info srStatusInfo) error {
	bs := info.BucketStats[bucket]

	c.RLock()
	defer c.RUnlock()
	if !c.enabled {
		return nil
	}

	var replMismatch bool
	for _, ss := range bs {
		if ss.ReplicationCfgMismatch {
			replMismatch = true
			break
		}
	}
	rcfg, _, err := globalBucketMetadataSys.GetReplicationConfig(ctx, bucket)
	if err != nil {
		_, ok := err.(BucketReplicationConfigNotFound)
		if !ok {
			return err
		}
		replMismatch = true
	}

	if rcfg != nil {
		// validate remote targets on current cluster for this bucket
		_, apiErr := validateReplicationDestination(ctx, bucket, rcfg, false)
		if apiErr != noError {
			replMismatch = true
		}
	}

	if replMismatch {
		logger.LogIf(ctx, c.annotateErr(configureReplication, c.PeerBucketConfigureReplHandler(ctx, bucket)))
	}
	return nil
}

func isBucketMetadataEqual(one, two *string) bool {
	switch {
	case one == nil && two == nil:
		return true
	case one == nil || two == nil:
		return false
	default:
		return strings.EqualFold(*one, *two)
	}
}

func (c *SiteReplicationSys) healIAMSystem(ctx context.Context, objAPI ObjectLayer) error {
	info, err := c.siteReplicationStatus(ctx, objAPI, madmin.SRStatusOptions{
		Users:    true,
		Policies: true,
		Groups:   true,
	})
	if err != nil {
		return err
	}
	for policy := range info.PolicyStats {
		c.healPolicies(ctx, objAPI, policy, info)
	}

	for user := range info.UserStats {
		c.healUsers(ctx, objAPI, user, info)
	}
	for group := range info.GroupStats {
		c.healGroups(ctx, objAPI, group, info)
	}
	for user := range info.UserStats {
		c.healUserPolicies(ctx, objAPI, user, info, false)
	}
	for group := range info.GroupStats {
		c.healGroupPolicies(ctx, objAPI, group, info, false)
	}
	for user := range info.UserStats {
		c.healUserPolicies(ctx, objAPI, user, info, true)
	}
	for group := range info.GroupStats {
		c.healGroupPolicies(ctx, objAPI, group, info, true)
	}

	return nil
}

// heal iam policies present on this site to peers, provided current cluster has the most recent update.
func (c *SiteReplicationSys) healPolicies(ctx context.Context, objAPI ObjectLayer, policy string, info srStatusInfo) error {
	// create IAM policy on peer cluster if missing
	ps := info.PolicyStats[policy]

	c.RLock()
	defer c.RUnlock()
	if !c.enabled {
		return nil
	}

	var (
		latestID, latestPeerName string
		lastUpdate               time.Time
		latestPolicyStat         srPolicyStatsSummary
	)
	for dID, ss := range ps {
		if lastUpdate.IsZero() {
			lastUpdate = ss.policy.UpdatedAt
			latestID = dID
			latestPolicyStat = ss
		}
		if !ss.policy.UpdatedAt.IsZero() && ss.policy.UpdatedAt.After(lastUpdate) {
			lastUpdate = ss.policy.UpdatedAt
			latestID = dID
			latestPolicyStat = ss
		}
	}
	if latestID != globalDeploymentID {
		// heal only from the site with latest info.
		return nil
	}
	latestPeerName = info.Sites[latestID].Name
	// heal policy of peers if peer does not have it.
	for dID, pStatus := range ps {
		if dID == globalDeploymentID {
			continue
		}
		if !pStatus.PolicyMismatch && pStatus.HasPolicy {
			continue
		}
		peerName := info.Sites[dID].Name
		err := c.IAMChangeHook(ctx, madmin.SRIAMItem{
			Type:      madmin.SRIAMItemPolicy,
			Name:      policy,
			Policy:    latestPolicyStat.policy.Policy,
			UpdatedAt: lastUpdate,
		})
		if err != nil {
			logger.LogIf(ctx, fmt.Errorf("Error healing IAM policy %s from peer site %s -> site %s : %w", policy, latestPeerName, peerName, err))
		}
	}
	return nil
}

// heal user policy mappings present on this site to peers, provided current cluster has the most recent update.
func (c *SiteReplicationSys) healUserPolicies(ctx context.Context, objAPI ObjectLayer, user string, info srStatusInfo, svcAcct bool) error {
	// create user policy mapping on peer cluster if missing
	us := info.UserStats[user]

	c.RLock()
	defer c.RUnlock()
	if !c.enabled {
		return nil
	}
	var (
		latestID, latestPeerName string
		lastUpdate               time.Time
		latestUserStat           srUserStatsSummary
	)
	for dID, ss := range us {
		if lastUpdate.IsZero() {
			lastUpdate = ss.userPolicy.UpdatedAt
			latestID = dID
			latestUserStat = ss
		}
		if !ss.userPolicy.UpdatedAt.IsZero() && ss.userPolicy.UpdatedAt.After(lastUpdate) {
			lastUpdate = ss.userPolicy.UpdatedAt
			latestID = dID
			latestUserStat = ss
		}
	}
	if latestID != globalDeploymentID {
		// heal only from the site with latest info.
		return nil
	}
	latestPeerName = info.Sites[latestID].Name
	// heal policy of peers if peer does not have it.
	for dID, pStatus := range us {
		if dID == globalDeploymentID {
			continue
		}
		if !pStatus.PolicyMismatch && pStatus.HasPolicyMapping {
			continue
		}
		if isPolicyMappingEqual(pStatus.userPolicy, latestUserStat.userPolicy) {
			continue
		}
		peerName := info.Sites[dID].Name
		err := c.IAMChangeHook(ctx, madmin.SRIAMItem{
			Type: madmin.SRIAMItemPolicyMapping,
			PolicyMapping: &madmin.SRPolicyMapping{
				UserOrGroup: user,
				IsGroup:     false,
				Policy:      latestUserStat.userPolicy.Policy,
			},
			UpdatedAt: lastUpdate,
		})
		if err != nil {
			logger.LogIf(ctx, fmt.Errorf("Error healing IAM user policy mapping for %s from peer site %s -> site %s : %w", user, latestPeerName, peerName, err))
		}
	}
	return nil
}

// heal group policy mappings present on this site to peers, provided current cluster has the most recent update.
func (c *SiteReplicationSys) healGroupPolicies(ctx context.Context, objAPI ObjectLayer, group string, info srStatusInfo, svcAcct bool) error {
	// create group policy mapping on peer cluster if missing
	gs := info.GroupStats[group]

	c.RLock()
	defer c.RUnlock()
	if !c.enabled {
		return nil
	}

	var (
		latestID, latestPeerName string
		lastUpdate               time.Time
		latestGroupStat          srGroupStatsSummary
	)
	for dID, ss := range gs {
		if lastUpdate.IsZero() {
			lastUpdate = ss.groupPolicy.UpdatedAt
			latestID = dID
			latestGroupStat = ss
		}
		if !ss.groupPolicy.UpdatedAt.IsZero() && ss.groupPolicy.UpdatedAt.After(lastUpdate) {
			lastUpdate = ss.groupPolicy.UpdatedAt
			latestID = dID
			latestGroupStat = ss
		}
	}
	if latestID != globalDeploymentID {
		// heal only from the site with latest info.
		return nil
	}
	latestPeerName = info.Sites[latestID].Name
	// heal policy of peers if peer does not have it.
	for dID, pStatus := range gs {
		if dID == globalDeploymentID {
			continue
		}
		if !pStatus.PolicyMismatch && pStatus.HasPolicyMapping {
			continue
		}
		if isPolicyMappingEqual(pStatus.groupPolicy, latestGroupStat.groupPolicy) {
			continue
		}
		peerName := info.Sites[dID].Name

		err := c.IAMChangeHook(ctx, madmin.SRIAMItem{
			Type: madmin.SRIAMItemPolicyMapping,
			PolicyMapping: &madmin.SRPolicyMapping{
				UserOrGroup: group,
				IsGroup:     true,
				Policy:      latestGroupStat.groupPolicy.Policy,
			},
			UpdatedAt: lastUpdate,
		})
		if err != nil {
			logger.LogIf(ctx, fmt.Errorf("Error healing IAM group policy mapping for %s from peer site %s -> site %s : %w", group, latestPeerName, peerName, err))
		}
	}
	return nil
}

// heal user accounts of local users that are present on this site, provided current cluster has the most recent update.
func (c *SiteReplicationSys) healUsers(ctx context.Context, objAPI ObjectLayer, user string, info srStatusInfo) error {
	// create user if missing; fix user policy mapping if missing
	us := info.UserStats[user]

	c.RLock()
	defer c.RUnlock()
	if !c.enabled {
		return nil
	}
	var (
		latestID, latestPeerName string
		lastUpdate               time.Time
		latestUserStat           srUserStatsSummary
	)
	for dID, ss := range us {
		if lastUpdate.IsZero() {
			lastUpdate = ss.userInfo.UserInfo.UpdatedAt
			latestID = dID
			latestUserStat = ss
		}
		if !ss.userInfo.UserInfo.UpdatedAt.IsZero() && ss.userInfo.UserInfo.UpdatedAt.After(lastUpdate) {
			lastUpdate = ss.userInfo.UserInfo.UpdatedAt
			latestID = dID
			latestUserStat = ss
		}
	}
	if latestID != globalDeploymentID {
		// heal only from the site with latest info.
		return nil
	}
	latestPeerName = info.Sites[latestID].Name
	for dID, uStatus := range us {
		if dID == globalDeploymentID {
			continue
		}
		if !uStatus.UserInfoMismatch {
			continue
		}

		if isUserInfoEqual(latestUserStat.userInfo.UserInfo, uStatus.userInfo.UserInfo) {
			continue
		}

		peerName := info.Sites[dID].Name

		u, ok := globalIAMSys.GetUser(ctx, user)
		if !ok {
			continue
		}
		creds := u.Credentials
		// heal only the user accounts that are local users
		if creds.IsServiceAccount() {
			claims, err := globalIAMSys.GetClaimsForSvcAcc(ctx, creds.AccessKey)
			if err != nil {
				logger.LogIf(ctx, fmt.Errorf("Error healing service account %s from peer site %s -> %s : %w", user, latestPeerName, peerName, err))
				continue
			}

			_, policy, err := globalIAMSys.GetServiceAccount(ctx, creds.AccessKey)
			if err != nil {
				logger.LogIf(ctx, fmt.Errorf("Error healing service account %s from peer site %s -> %s : %w", user, latestPeerName, peerName, err))
				continue
			}

			var policyJSON []byte
			if policy != nil {
				policyJSON, err = json.Marshal(policy)
				if err != nil {
					logger.LogIf(ctx, fmt.Errorf("Error healing service account %s from peer site %s -> %s : %w", user, latestPeerName, peerName, err))
					continue
				}
			}

			if err := c.IAMChangeHook(ctx, madmin.SRIAMItem{
				Type: madmin.SRIAMItemSvcAcc,
				SvcAccChange: &madmin.SRSvcAccChange{
					Create: &madmin.SRSvcAccCreate{
						Parent:        creds.ParentUser,
						AccessKey:     creds.AccessKey,
						SecretKey:     creds.SecretKey,
						Groups:        creds.Groups,
						Claims:        claims,
						SessionPolicy: json.RawMessage(policyJSON),
						Status:        creds.Status,
					},
				},
				UpdatedAt: lastUpdate,
			}); err != nil {
				logger.LogIf(ctx, fmt.Errorf("Error healing service account %s from peer site %s -> %s : %w", user, latestPeerName, peerName, err))
			}
			continue
		}
		if creds.IsTemp() && !creds.IsExpired() {
			u, err := globalIAMSys.GetUserInfo(ctx, creds.ParentUser)
			if err != nil {
				logger.LogIf(ctx, fmt.Errorf("Error healing temporary credentials %s from peer site %s -> %s : %w", user, latestPeerName, peerName, err))
				continue
			}
			// Call hook for site replication.
			if err := c.IAMChangeHook(ctx, madmin.SRIAMItem{
				Type: madmin.SRIAMItemSTSAcc,
				STSCredential: &madmin.SRSTSCredential{
					AccessKey:           creds.AccessKey,
					SecretKey:           creds.SecretKey,
					SessionToken:        creds.SessionToken,
					ParentUser:          creds.ParentUser,
					ParentPolicyMapping: u.PolicyName,
				},
				UpdatedAt: lastUpdate,
			}); err != nil {
				logger.LogIf(ctx, fmt.Errorf("Error healing temporary credentials %s from peer site %s -> %s : %w", user, latestPeerName, peerName, err))
			}
			continue
		}
		if err := c.IAMChangeHook(ctx, madmin.SRIAMItem{
			Type: madmin.SRIAMItemIAMUser,
			IAMUser: &madmin.SRIAMUser{
				AccessKey:   user,
				IsDeleteReq: false,
				UserReq: &madmin.AddOrUpdateUserReq{
					SecretKey: creds.SecretKey,
					Status:    latestUserStat.userInfo.Status,
				},
			},
			UpdatedAt: lastUpdate,
		}); err != nil {
			logger.LogIf(ctx, fmt.Errorf("Error healing user %s from peer site %s -> %s : %w", user, latestPeerName, peerName, err))
		}
	}
	return nil
}

func (c *SiteReplicationSys) healGroups(ctx context.Context, objAPI ObjectLayer, group string, info srStatusInfo) error {
	c.RLock()
	defer c.RUnlock()
	if !c.enabled {
		return nil
	}

	var (
		latestID, latestPeerName string
		lastUpdate               time.Time
		latestGroupStat          srGroupStatsSummary
	)
	// create group if missing; fix group policy mapping if missing
	gs, ok := info.GroupStats[group]
	if !ok {
		return nil
	}
	for dID, ss := range gs {
		if lastUpdate.IsZero() {
			lastUpdate = ss.groupDesc.UpdatedAt
			latestID = dID
			latestGroupStat = ss
		}
		if !ss.groupDesc.UpdatedAt.IsZero() && ss.groupDesc.UpdatedAt.After(lastUpdate) {
			lastUpdate = ss.groupDesc.UpdatedAt
			latestID = dID
			latestGroupStat = ss
		}
	}
	if latestID != globalDeploymentID {
		// heal only from the site with latest info.
		return nil
	}
	latestPeerName = info.Sites[latestID].Name
	for dID, gStatus := range gs {
		if dID == globalDeploymentID {
			continue
		}
		if !gStatus.GroupDescMismatch {
			continue
		}

		if isGroupDescEqual(latestGroupStat.groupDesc.GroupDesc, gStatus.groupDesc.GroupDesc) {
			continue
		}
		peerName := info.Sites[dID].Name

		if err := c.IAMChangeHook(ctx, madmin.SRIAMItem{
			Type: madmin.SRIAMItemGroupInfo,
			GroupInfo: &madmin.SRGroupInfo{
				UpdateReq: madmin.GroupAddRemove{
					Group:    group,
					Status:   madmin.GroupStatus(latestGroupStat.groupDesc.Status),
					Members:  latestGroupStat.groupDesc.Members,
					IsRemove: false,
				},
			},
			UpdatedAt: lastUpdate,
		}); err != nil {
			logger.LogIf(ctx, fmt.Errorf("Error healing group %s from peer site %s -> site %s : %w", group, latestPeerName, peerName, err))
		}
	}
	return nil
}

func isGroupDescEqual(g1, g2 madmin.GroupDesc) bool {
	if g1.Name != g2.Name ||
		g1.Status != g2.Status ||
		g1.Policy != g2.Policy {
		return false
	}
	if len(g1.Members) != len(g2.Members) {
		return false
	}
	for _, v1 := range g1.Members {
		var found bool
		for _, v2 := range g2.Members {
			if v1 == v2 {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func isUserInfoEqual(u1, u2 madmin.UserInfo) bool {
	if u1.PolicyName != u2.PolicyName ||
		u1.Status != u2.Status ||
		u1.SecretKey != u2.SecretKey {
		return false
	}
	for len(u1.MemberOf) != len(u2.MemberOf) {
		return false
	}
	for _, v1 := range u1.MemberOf {
		var found bool
		for _, v2 := range u2.MemberOf {
			if v1 == v2 {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func isPolicyMappingEqual(p1, p2 srPolicyMapping) bool {
	return p1.Policy == p2.Policy && p1.IsGroup == p2.IsGroup && p1.UserOrGroup == p2.UserOrGroup
}
