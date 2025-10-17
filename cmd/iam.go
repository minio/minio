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
	"errors"
	"fmt"
	"maps"
	"math/rand"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/arn"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/color"
	"github.com/minio/minio/internal/config"
	xldap "github.com/minio/minio/internal/config/identity/ldap"
	"github.com/minio/minio/internal/config/identity/openid"
	idplugin "github.com/minio/minio/internal/config/identity/plugin"
	xtls "github.com/minio/minio/internal/config/identity/tls"
	"github.com/minio/minio/internal/config/policy/opa"
	polplugin "github.com/minio/minio/internal/config/policy/plugin"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/jwt"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v3/env"
	"github.com/minio/pkg/v3/ldap"
	"github.com/minio/pkg/v3/policy"
	etcd "go.etcd.io/etcd/client/v3"
	"golang.org/x/sync/singleflight"
)

// UsersSysType - defines the type of users and groups system that is
// active on the server.
type UsersSysType string

// Types of users configured in the server.
const (
	// This mode uses the internal users system in MinIO.
	MinIOUsersSysType UsersSysType = "MinIOUsersSys"

	// This mode uses users and groups from a configured LDAP
	// server.
	LDAPUsersSysType UsersSysType = "LDAPUsersSys"
)

const (
	statusEnabled  = "enabled"
	statusDisabled = "disabled"
)

const (
	embeddedPolicyType  = "embedded-policy"
	inheritedPolicyType = "inherited-policy"
)

const (
	maxSVCSessionPolicySize = 4096
)

// IAMSys - config system.
type IAMSys struct {
	// Need to keep them here to keep alignment - ref: https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	// metrics
	LastRefreshTimeUnixNano         uint64
	LastRefreshDurationMilliseconds uint64
	TotalRefreshSuccesses           uint64
	TotalRefreshFailures            uint64

	sync.Mutex

	iamRefreshInterval time.Duration

	LDAPConfig   xldap.Config  // only valid if usersSysType is LDAPUsers
	OpenIDConfig openid.Config // only valid if OpenID is configured
	STSTLSConfig xtls.Config   // only valid if STS TLS is configured

	usersSysType UsersSysType

	rolesMap map[arn.ARN]string

	// Persistence layer for IAM subsystem
	store *IAMStoreSys

	// configLoaded will be closed and remain so after first load.
	configLoaded chan struct{}
}

// IAMUserType represents a user type inside MinIO server
type IAMUserType int

const (
	unknownIAMUserType IAMUserType = iota - 1
	regUser
	stsUser
	svcUser
)

// LoadGroup - loads a specific group from storage, and updates the
// memberships cache. If the specified group does not exist in
// storage, it is removed from in-memory maps as well - this
// simplifies the implementation for group removal. This is called
// only via IAM notifications.
func (sys *IAMSys) LoadGroup(ctx context.Context, objAPI ObjectLayer, group string) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	return sys.store.GroupNotificationHandler(ctx, group)
}

// LoadPolicy - reloads a specific canned policy from backend disks or etcd.
func (sys *IAMSys) LoadPolicy(ctx context.Context, objAPI ObjectLayer, policyName string) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	return sys.store.PolicyNotificationHandler(ctx, policyName)
}

// LoadPolicyMapping - loads the mapped policy for a user or group
// from storage into server memory.
func (sys *IAMSys) LoadPolicyMapping(ctx context.Context, objAPI ObjectLayer, userOrGroup string, userType IAMUserType, isGroup bool) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	return sys.store.PolicyMappingNotificationHandler(ctx, userOrGroup, isGroup, userType)
}

// LoadUser - reloads a specific user from backend disks or etcd.
func (sys *IAMSys) LoadUser(ctx context.Context, objAPI ObjectLayer, accessKey string, userType IAMUserType) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	return sys.store.UserNotificationHandler(ctx, accessKey, userType)
}

// LoadServiceAccount - reloads a specific service account from backend disks or etcd.
func (sys *IAMSys) LoadServiceAccount(ctx context.Context, accessKey string) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	return sys.store.UserNotificationHandler(ctx, accessKey, svcUser)
}

// initStore initializes IAM stores
func (sys *IAMSys) initStore(objAPI ObjectLayer, etcdClient *etcd.Client) {
	if sys.LDAPConfig.Enabled() {
		sys.SetUsersSysType(LDAPUsersSysType)
	}

	if etcdClient == nil {
		var (
			group  *singleflight.Group
			policy *singleflight.Group
		)
		if env.Get("_MINIO_IAM_SINGLE_FLIGHT", config.EnableOn) == config.EnableOn {
			group = &singleflight.Group{}
			policy = &singleflight.Group{}
		}
		sys.store = &IAMStoreSys{
			IAMStorageAPI: newIAMObjectStore(objAPI, sys.usersSysType),
			group:         group,
			policy:        policy,
		}
	} else {
		sys.store = &IAMStoreSys{IAMStorageAPI: newIAMEtcdStore(etcdClient, sys.usersSysType)}
	}
}

// Initialized checks if IAM is initialized
func (sys *IAMSys) Initialized() bool {
	if sys == nil {
		return false
	}
	sys.Lock()
	defer sys.Unlock()
	return sys.store != nil
}

// Load - loads all credentials, policies and policy mappings.
func (sys *IAMSys) Load(ctx context.Context, firstTime bool) error {
	loadStartTime := time.Now()
	err := sys.store.LoadIAMCache(ctx, firstTime)
	if err != nil {
		atomic.AddUint64(&sys.TotalRefreshFailures, 1)
		return err
	}
	loadDuration := time.Since(loadStartTime)

	atomic.StoreUint64(&sys.LastRefreshDurationMilliseconds, uint64(loadDuration.Milliseconds()))
	atomic.StoreUint64(&sys.LastRefreshTimeUnixNano, uint64(loadStartTime.Add(loadDuration).UnixNano()))
	atomic.AddUint64(&sys.TotalRefreshSuccesses, 1)

	if !globalSiteReplicatorCred.IsValid() {
		sa, _, err := sys.getServiceAccount(ctx, siteReplicatorSvcAcc)
		if err == nil {
			globalSiteReplicatorCred.Set(sa.Credentials.SecretKey)
		}
	}

	if firstTime {
		bootstrapTraceMsg(fmt.Sprintf("globalIAMSys.Load(): (duration: %s)", loadDuration))
		if globalIsDistErasure {
			logger.Info("IAM load(startup) finished. (duration: %s)", loadDuration)
		}
	}

	select {
	case <-sys.configLoaded:
	default:
		close(sys.configLoaded)
	}
	return nil
}

// Init - initializes config system by reading entries from config/iam
func (sys *IAMSys) Init(ctx context.Context, objAPI ObjectLayer, etcdClient *etcd.Client, iamRefreshInterval time.Duration) {
	bootstrapTraceMsg("IAM initialization started")
	globalServerConfigMu.RLock()
	s := globalServerConfig
	globalServerConfigMu.RUnlock()

	sys.Lock()
	sys.iamRefreshInterval = iamRefreshInterval
	sys.Unlock()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	var (
		openidInit bool
		ldapInit   bool
		authNInit  bool
		authZInit  bool
	)

	stsTLSConfig, err := xtls.Lookup(s[config.IdentityTLSSubSys][config.Default])
	if err != nil {
		iamLogIf(ctx, fmt.Errorf("Unable to initialize X.509/TLS STS API: %w", err), logger.WarningKind)
	} else {
		if stsTLSConfig.InsecureSkipVerify {
			iamLogIf(ctx, fmt.Errorf("Enabling %s is not recommended in a production environment", xtls.EnvIdentityTLSSkipVerify), logger.WarningKind)
		}
		sys.Lock()
		sys.STSTLSConfig = stsTLSConfig
		sys.Unlock()
	}

	for {
		if !openidInit {
			openidConfig, err := openid.LookupConfig(s,
				xhttp.WithUserAgent(NewHTTPTransport(), func() string {
					return getUserAgent(getMinioMode())
				}), xhttp.DrainBody, globalSite.Region())
			if err != nil {
				iamLogIf(ctx, fmt.Errorf("Unable to initialize OpenID: %w", err), logger.WarningKind)
			} else {
				openidInit = true
				sys.Lock()
				sys.OpenIDConfig = openidConfig
				sys.Unlock()
			}
		}

		if !ldapInit {
			// Initialize if LDAP is enabled
			ldapConfig, err := xldap.Lookup(s, globalRootCAs)
			if err != nil {
				iamLogIf(ctx, fmt.Errorf("Unable to load LDAP configuration (LDAP configuration will be disabled!): %w", err), logger.WarningKind)
			} else {
				ldapInit = true
				sys.Lock()
				sys.LDAPConfig = ldapConfig
				sys.Unlock()
			}
		}

		if !authNInit {
			authNPluginCfg, err := idplugin.LookupConfig(s[config.IdentityPluginSubSys][config.Default],
				NewHTTPTransport(), xhttp.DrainBody, globalSite.Region())
			if err != nil {
				iamLogIf(ctx, fmt.Errorf("Unable to initialize AuthNPlugin: %w", err), logger.WarningKind)
			} else {
				authNInit = true
				setGlobalAuthNPlugin(idplugin.New(GlobalContext, authNPluginCfg))
			}
		}

		if !authZInit {
			authZPluginCfg, err := polplugin.LookupConfig(s, GetDefaultConnSettings(), xhttp.DrainBody)
			if err != nil {
				iamLogIf(ctx, fmt.Errorf("Unable to initialize AuthZPlugin: %w", err), logger.WarningKind)
			} else {
				authZInit = true
			}
			if authZPluginCfg.URL == nil {
				opaCfg, err := opa.LookupConfig(s[config.PolicyOPASubSys][config.Default],
					NewHTTPTransport(), xhttp.DrainBody)
				if err != nil {
					iamLogIf(ctx, fmt.Errorf("Unable to initialize AuthZPlugin from legacy OPA config: %w", err))
				} else {
					authZPluginCfg.URL = opaCfg.URL
					authZPluginCfg.AuthToken = opaCfg.AuthToken
					authZPluginCfg.Transport = opaCfg.Transport
					authZPluginCfg.CloseRespFn = opaCfg.CloseRespFn
					authZInit = true
				}
			}
			if authZInit {
				setGlobalAuthZPlugin(polplugin.New(authZPluginCfg))
			}
		}

		if !openidInit || !ldapInit || !authNInit || !authZInit {
			retryInterval := time.Duration(r.Float64() * float64(3*time.Second))
			if !openidInit {
				logger.Info("Waiting for OpenID to be initialized.. (retrying in %s)", retryInterval)
			}
			if !ldapInit {
				logger.Info("Waiting for LDAP to be initialized.. (retrying in %s)", retryInterval)
			}
			if !authNInit {
				logger.Info("Waiting for AuthN to be initialized.. (retrying in %s)", retryInterval)
			}
			if !authZInit {
				logger.Info("Waiting for AuthZ to be initialized.. (retrying in %s)", retryInterval)
			}
			time.Sleep(retryInterval)
			continue
		}

		break
	}

	// Initialize IAM store
	sys.Lock()

	sys.initStore(objAPI, etcdClient)

	// Initialize RoleARNs
	sys.rolesMap = make(map[arn.ARN]string)

	// From OpenID
	maps.Copy(sys.rolesMap, sys.OpenIDConfig.GetRoleInfo())

	// From AuthN plugin if enabled.
	if authn := newGlobalAuthNPluginFn(); authn != nil {
		maps.Copy(sys.rolesMap, authn.GetRoleInfo())
	}

	sys.printIAMRoles()
	sys.Unlock()

	retryCtx, cancel := context.WithCancel(ctx)

	// Indicate to our routine to exit cleanly upon return.
	defer cancel()

	// Migrate storage format if needed.
	for {
		// Migrate IAM configuration, if necessary.
		if err := saveIAMFormat(retryCtx, sys.store); err != nil {
			if configRetriableErrors(err) {
				retryInterval := time.Duration(r.Float64() * float64(time.Second))
				logger.Info("Waiting for all MinIO IAM sub-system to be initialized.. possible cause (%v) (retrying in %s)", err, retryInterval)
				time.Sleep(retryInterval)
				continue
			}
			iamLogIf(ctx, fmt.Errorf("IAM sub-system is partially initialized, unable to write the IAM format: %w", err), logger.WarningKind)
			return
		}

		break
	}

	cache := sys.store.lock()
	setDefaultCannedPolicies(cache.iamPolicyDocsMap)
	sys.store.unlock()

	// Load IAM data from storage.
	for {
		if err := sys.Load(retryCtx, true); err != nil {
			if configRetriableErrors(err) {
				retryInterval := time.Duration(r.Float64() * float64(time.Second))
				logger.Info("Waiting for all MinIO IAM sub-system to be initialized.. possible cause (%v) (retrying in %s)", err, retryInterval)
				time.Sleep(retryInterval)
				continue
			}
			if err != nil {
				iamLogIf(ctx, fmt.Errorf("Unable to initialize IAM sub-system, some users may not be available: %w", err), logger.WarningKind)
			}
		}
		break
	}

	refreshInterval := sys.iamRefreshInterval
	go sys.periodicRoutines(ctx, refreshInterval)

	bootstrapTraceMsg("finishing IAM loading")
}

const maxDurationSecondsForLog = 5

func (sys *IAMSys) periodicRoutines(ctx context.Context, baseInterval time.Duration) {
	// Watch for IAM config changes for iamStorageWatcher.
	watcher, isWatcher := sys.store.IAMStorageAPI.(iamStorageWatcher)
	if isWatcher {
		go func() {
			ch := watcher.watch(ctx, iamConfigPrefix)
			for event := range ch {
				if err := sys.loadWatchedEvent(ctx, event); err != nil {
					// we simply log errors
					iamLogIf(ctx, fmt.Errorf("Failure in loading watch event: %v", err), logger.WarningKind)
				}
			}
		}()
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Calculate the waitInterval between periodic refreshes so that each server
	// independently picks a (uniformly distributed) random time in an interval
	// of size = baseInterval.
	//
	// For example:
	//
	//    - if baseInterval=10s, then 5s <= waitInterval() < 15s
	//
	//    - if baseInterval=10m, then 5m <= waitInterval() < 15m
	waitInterval := func() time.Duration {
		// Calculate a random value such that 0 <= value < baseInterval
		randAmt := time.Duration(r.Float64() * float64(baseInterval))
		return baseInterval/2 + randAmt
	}

	timer := time.NewTimer(waitInterval())
	defer timer.Stop()

	lastPurgeHour := -1
	for {
		select {
		case <-timer.C:
			// Load all IAM items (except STS creds) periodically.
			refreshStart := time.Now()
			if err := sys.Load(ctx, false); err != nil {
				iamLogIf(ctx, fmt.Errorf("Failure in periodic refresh for IAM (duration: %s): %v", time.Since(refreshStart), err), logger.WarningKind)
			} else {
				took := time.Since(refreshStart).Seconds()
				if took > maxDurationSecondsForLog {
					// Log if we took a lot of time to load.
					logger.Info("IAM refresh took (duration: %.2fs)", took)
				}
			}

			// Run purge routines once in each hour.
			if refreshStart.Hour() != lastPurgeHour {
				lastPurgeHour = refreshStart.Hour()
				// Poll and remove accounts for those users who were removed
				// from LDAP/OpenID.
				if sys.LDAPConfig.Enabled() {
					sys.purgeExpiredCredentialsForLDAP(ctx)
					sys.updateGroupMembershipsForLDAP(ctx)
				}
				if sys.OpenIDConfig.ProviderEnabled() {
					sys.purgeExpiredCredentialsForExternalSSO(ctx)
				}
			}

			timer.Reset(waitInterval())
		case <-ctx.Done():
			return
		}
	}
}

// Prints IAM role ARNs.
func (sys *IAMSys) printIAMRoles() {
	if len(sys.rolesMap) == 0 {
		return
	}
	var arns []string
	for arn := range sys.rolesMap {
		arns = append(arns, arn.String())
	}
	sort.Strings(arns)
	msgs := make([]string, 0, len(arns))
	for _, arn := range arns {
		msgs = append(msgs, color.Bold(arn))
	}

	logger.Info(fmt.Sprintf("%s %s", color.Blue("IAM Roles:"), strings.Join(msgs, " ")))
}

// HasWatcher - returns if the IAM system has a watcher to be notified of
// changes.
func (sys *IAMSys) HasWatcher() bool {
	return sys.store.HasWatcher()
}

func (sys *IAMSys) loadWatchedEvent(ctx context.Context, event iamWatchEvent) (err error) {
	usersPrefix := strings.HasPrefix(event.keyPath, iamConfigUsersPrefix)
	groupsPrefix := strings.HasPrefix(event.keyPath, iamConfigGroupsPrefix)
	stsPrefix := strings.HasPrefix(event.keyPath, iamConfigSTSPrefix)
	svcPrefix := strings.HasPrefix(event.keyPath, iamConfigServiceAccountsPrefix)
	policyPrefix := strings.HasPrefix(event.keyPath, iamConfigPoliciesPrefix)
	policyDBUsersPrefix := strings.HasPrefix(event.keyPath, iamConfigPolicyDBUsersPrefix)
	policyDBSTSUsersPrefix := strings.HasPrefix(event.keyPath, iamConfigPolicyDBSTSUsersPrefix)
	policyDBGroupsPrefix := strings.HasPrefix(event.keyPath, iamConfigPolicyDBGroupsPrefix)

	ctx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()

	switch {
	case usersPrefix:
		accessKey := path.Dir(strings.TrimPrefix(event.keyPath, iamConfigUsersPrefix))
		err = sys.store.UserNotificationHandler(ctx, accessKey, regUser)
	case stsPrefix:
		accessKey := path.Dir(strings.TrimPrefix(event.keyPath, iamConfigSTSPrefix))
		err = sys.store.UserNotificationHandler(ctx, accessKey, stsUser)
	case svcPrefix:
		accessKey := path.Dir(strings.TrimPrefix(event.keyPath, iamConfigServiceAccountsPrefix))
		err = sys.store.UserNotificationHandler(ctx, accessKey, svcUser)
	case groupsPrefix:
		group := path.Dir(strings.TrimPrefix(event.keyPath, iamConfigGroupsPrefix))
		err = sys.store.GroupNotificationHandler(ctx, group)
	case policyPrefix:
		policyName := path.Dir(strings.TrimPrefix(event.keyPath, iamConfigPoliciesPrefix))
		err = sys.store.PolicyNotificationHandler(ctx, policyName)
	case policyDBUsersPrefix:
		policyMapFile := strings.TrimPrefix(event.keyPath, iamConfigPolicyDBUsersPrefix)
		user := strings.TrimSuffix(policyMapFile, ".json")
		err = sys.store.PolicyMappingNotificationHandler(ctx, user, false, regUser)
	case policyDBSTSUsersPrefix:
		policyMapFile := strings.TrimPrefix(event.keyPath, iamConfigPolicyDBSTSUsersPrefix)
		user := strings.TrimSuffix(policyMapFile, ".json")
		err = sys.store.PolicyMappingNotificationHandler(ctx, user, false, stsUser)
	case policyDBGroupsPrefix:
		policyMapFile := strings.TrimPrefix(event.keyPath, iamConfigPolicyDBGroupsPrefix)
		user := strings.TrimSuffix(policyMapFile, ".json")
		err = sys.store.PolicyMappingNotificationHandler(ctx, user, true, regUser)
	}
	return err
}

// HasRolePolicy - returns if a role policy is configured for IAM.
func (sys *IAMSys) HasRolePolicy() bool {
	return len(sys.rolesMap) > 0
}

// GetRolePolicy - returns policies associated with a role ARN.
func (sys *IAMSys) GetRolePolicy(arnStr string) (arn.ARN, string, error) {
	roleArn, err := arn.Parse(arnStr)
	if err != nil {
		return arn.ARN{}, "", fmt.Errorf("RoleARN parse err: %v", err)
	}
	rolePolicy, ok := sys.rolesMap[roleArn]
	if !ok {
		return arn.ARN{}, "", fmt.Errorf("RoleARN %s is not defined.", arnStr)
	}
	return roleArn, rolePolicy, nil
}

// DeletePolicy - deletes a canned policy from backend. `notifyPeers` is true
// whenever this is called via the API. It is false when called via a
// notification from another peer. This is to avoid infinite loops.
func (sys *IAMSys) DeletePolicy(ctx context.Context, policyName string, notifyPeers bool) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	for _, v := range policy.DefaultPolicies {
		if v.Name == policyName {
			if err := checkConfig(ctx, globalObjectAPI, getPolicyDocPath(policyName)); err != nil && err == errConfigNotFound {
				return fmt.Errorf("inbuilt policy `%s` not allowed to be deleted", policyName)
			}
		}
	}

	err := sys.store.DeletePolicy(ctx, policyName, !notifyPeers)
	if err != nil {
		return err
	}

	if !notifyPeers || sys.HasWatcher() {
		return nil
	}

	// Notify all other MinIO peers to delete policy
	for _, nerr := range globalNotificationSys.DeletePolicy(ctx, policyName) {
		if nerr.Err != nil {
			logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
			iamLogIf(ctx, nerr.Err)
		}
	}

	return nil
}

// InfoPolicy - returns the policy definition with some metadata.
func (sys *IAMSys) InfoPolicy(policyName string) (*madmin.PolicyInfo, error) {
	if !sys.Initialized() {
		return nil, errServerNotInitialized
	}

	d, err := sys.store.GetPolicyDoc(policyName)
	if err != nil {
		return nil, err
	}

	pdata, err := json.Marshal(d.Policy)
	if err != nil {
		return nil, err
	}

	return &madmin.PolicyInfo{
		PolicyName: policyName,
		Policy:     pdata,
		CreateDate: d.CreateDate,
		UpdateDate: d.UpdateDate,
	}, nil
}

// ListPolicies - lists all canned policies.
func (sys *IAMSys) ListPolicies(ctx context.Context, bucketName string) (map[string]policy.Policy, error) {
	if !sys.Initialized() {
		return nil, errServerNotInitialized
	}

	return sys.store.ListPolicies(ctx, bucketName)
}

// ListPolicyDocs - lists all canned policy docs.
func (sys *IAMSys) ListPolicyDocs(ctx context.Context, bucketName string) (map[string]PolicyDoc, error) {
	if !sys.Initialized() {
		return nil, errServerNotInitialized
	}

	return sys.store.ListPolicyDocs(ctx, bucketName)
}

// SetPolicy - sets a new named policy.
func (sys *IAMSys) SetPolicy(ctx context.Context, policyName string, p policy.Policy) (time.Time, error) {
	if !sys.Initialized() {
		return time.Time{}, errServerNotInitialized
	}

	updatedAt, err := sys.store.SetPolicy(ctx, policyName, p)
	if err != nil {
		return updatedAt, err
	}

	if !sys.HasWatcher() {
		// Notify all other MinIO peers to reload policy
		for _, nerr := range globalNotificationSys.LoadPolicy(ctx, policyName) {
			if nerr.Err != nil {
				logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
				iamLogIf(ctx, nerr.Err)
			}
		}
	}
	return updatedAt, nil
}

// RevokeTokens - revokes all STS tokens, or those of specified type, for a user
// If `tokenRevokeType` is empty, all tokens are revoked.
func (sys *IAMSys) RevokeTokens(ctx context.Context, accessKey, tokenRevokeType string) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	return sys.store.RevokeTokens(ctx, accessKey, tokenRevokeType)
}

// DeleteUser - delete user (only for long-term users not STS users).
func (sys *IAMSys) DeleteUser(ctx context.Context, accessKey string, notifyPeers bool) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	if err := sys.store.DeleteUser(ctx, accessKey, regUser); err != nil {
		return err
	}

	// Notify all other MinIO peers to delete user.
	if notifyPeers && !sys.HasWatcher() {
		for _, nerr := range globalNotificationSys.DeleteUser(ctx, accessKey) {
			if nerr.Err != nil {
				logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
				iamLogIf(ctx, nerr.Err)
			}
		}
	}

	return nil
}

// CurrentPolicies - returns comma separated policy string, from
// an input policy after validating if there are any current
// policies which exist on MinIO corresponding to the input.
func (sys *IAMSys) CurrentPolicies(policyName string) string {
	if !sys.Initialized() {
		return ""
	}

	policies, _ := sys.store.MergePolicies(policyName)
	return policies
}

func (sys *IAMSys) notifyForUser(ctx context.Context, accessKey string, isTemp bool) {
	// Notify all other MinIO peers to reload user.
	if !sys.HasWatcher() {
		for _, nerr := range globalNotificationSys.LoadUser(ctx, accessKey, isTemp) {
			if nerr.Err != nil {
				logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
				iamLogIf(ctx, nerr.Err)
			}
		}
	}
}

// SetTempUser - set temporary user credentials, these credentials have an
// expiry. The permissions for these STS credentials is determined in one of the
// following ways:
//
// - RoleARN - if a role-arn is specified in the request, the STS credential's
// policy is the role's policy.
//
// - inherited from parent - this is the case for AssumeRole API, where the
// parent user is an actual real user with their own (permanent) credentials and
// policy association.
//
// - inherited from "virtual" parent - this is the case for AssumeRoleWithLDAP
// where the parent user is the DN of the actual LDAP user. The parent user
// itself cannot login, but the policy associated with them determines the base
// policy for the STS credential. The policy mapping can be updated by the
// administrator.
//
// - from `Subject.CommonName` field from the STS request for
// AssumeRoleWithCertificate. In this case, the policy for the STS credential
// has the same name as the value of this field.
//
// - from special JWT claim from STS request for AssumeRoleWithOIDC API (when
// not using RoleARN). The claim value can be a string or a list and refers to
// the names of access policies.
//
// For all except the RoleARN case, the implementation is the same - the policy
// for the STS credential is associated with a parent user. For the
// AssumeRoleWithCertificate case, the "virtual" parent user is the value of the
// `Subject.CommonName` field. For the OIDC (without RoleARN) case the "virtual"
// parent is derived as a concatenation of the `sub` and `iss` fields. The
// policies applicable to the STS credential are associated with this "virtual"
// parent.
//
// When a policyName is given to this function, the policy association is
// created and stored in the IAM store. Thus, it should NOT be given for the
// role-arn case (because the role-to-policy mapping is separately stored
// elsewhere), the AssumeRole case (because the parent user is real and their
// policy is associated via policy-set API) and the AssumeRoleWithLDAP case
// (because the policy association is made via policy-set API).
func (sys *IAMSys) SetTempUser(ctx context.Context, accessKey string, cred auth.Credentials, policyName string) (time.Time, error) {
	if !sys.Initialized() {
		return time.Time{}, errServerNotInitialized
	}

	if newGlobalAuthZPluginFn() != nil {
		// If OPA is set, we do not need to set a policy mapping.
		policyName = ""
	}

	updatedAt, err := sys.store.SetTempUser(ctx, accessKey, cred, policyName)
	if err != nil {
		return time.Time{}, err
	}

	sys.notifyForUser(ctx, cred.AccessKey, true)

	return updatedAt, nil
}

// ListBucketUsers - list all users who can access this 'bucket'
func (sys *IAMSys) ListBucketUsers(ctx context.Context, bucket string) (map[string]madmin.UserInfo, error) {
	if !sys.Initialized() {
		return nil, errServerNotInitialized
	}

	select {
	case <-sys.configLoaded:
		return sys.store.GetBucketUsers(bucket)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ListUsers - list all users.
func (sys *IAMSys) ListUsers(ctx context.Context) (map[string]madmin.UserInfo, error) {
	if !sys.Initialized() {
		return nil, errServerNotInitialized
	}
	select {
	case <-sys.configLoaded:
		return sys.store.GetUsers(), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ListLDAPUsers - list LDAP users which has
func (sys *IAMSys) ListLDAPUsers(ctx context.Context) (map[string]madmin.UserInfo, error) {
	if !sys.Initialized() {
		return nil, errServerNotInitialized
	}

	if sys.usersSysType != LDAPUsersSysType {
		return nil, errIAMActionNotAllowed
	}

	select {
	case <-sys.configLoaded:
		stsMap, err := sys.store.GetAllSTSUserMappings(sys.LDAPConfig.IsLDAPUserDN)
		if err != nil {
			return nil, err
		}
		ldapUsers := make(map[string]madmin.UserInfo, len(stsMap))
		for user, policy := range stsMap {
			ldapUsers[user] = madmin.UserInfo{
				PolicyName: policy,
				Status:     statusEnabled,
			}
		}
		return ldapUsers, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type cleanEntitiesQuery struct {
	Users    map[string]set.StringSet
	Groups   set.StringSet
	Policies set.StringSet
}

// createCleanEntitiesQuery - maps users to their groups and normalizes user or group DNs if ldap.
func (sys *IAMSys) createCleanEntitiesQuery(q madmin.PolicyEntitiesQuery, ldap bool) cleanEntitiesQuery {
	cleanQ := cleanEntitiesQuery{
		Users:    make(map[string]set.StringSet),
		Groups:   set.CreateStringSet(q.Groups...),
		Policies: set.CreateStringSet(q.Policy...),
	}

	if ldap {
		// Validate and normalize users, then fetch and normalize their groups
		// Also include unvalidated users for backward compatibility.
		for _, user := range q.Users {
			lookupRes, actualGroups, _ := sys.LDAPConfig.GetValidatedDNWithGroups(user)
			if lookupRes != nil {
				groupSet := set.CreateStringSet(actualGroups...)

				// duplicates can be overwritten, fetched groups should be identical.
				cleanQ.Users[lookupRes.NormDN] = groupSet
			}
			// Search for non-normalized DN as well for backward compatibility.
			if _, ok := cleanQ.Users[user]; !ok {
				cleanQ.Users[user] = nil
			}
		}

		// Validate and normalize groups.
		for _, group := range q.Groups {
			lookupRes, underDN, _ := sys.LDAPConfig.GetValidatedGroupDN(nil, group)
			if lookupRes != nil && underDN {
				cleanQ.Groups.Add(lookupRes.NormDN)
			}
		}
	} else {
		for _, user := range q.Users {
			info, err := sys.store.GetUserInfo(user)
			var groupSet set.StringSet
			if err == nil {
				groupSet = set.CreateStringSet(info.MemberOf...)
			}
			cleanQ.Users[user] = groupSet
		}
	}
	return cleanQ
}

// QueryLDAPPolicyEntities - queries policy associations for LDAP users/groups/policies.
func (sys *IAMSys) QueryLDAPPolicyEntities(ctx context.Context, q madmin.PolicyEntitiesQuery) (*madmin.PolicyEntitiesResult, error) {
	if !sys.Initialized() {
		return nil, errServerNotInitialized
	}

	if !sys.LDAPConfig.Enabled() {
		return nil, errIAMActionNotAllowed
	}

	select {
	case <-sys.configLoaded:
		cleanQuery := sys.createCleanEntitiesQuery(q, true)
		pe := sys.store.ListPolicyMappings(cleanQuery, sys.LDAPConfig.IsLDAPUserDN, sys.LDAPConfig.IsLDAPGroupDN, sys.LDAPConfig.DecodeDN)
		pe.Timestamp = UTCNow()
		return &pe, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// IsTempUser - returns if given key is a temporary user and parent user.
func (sys *IAMSys) IsTempUser(name string) (bool, string, error) {
	if !sys.Initialized() {
		return false, "", errServerNotInitialized
	}

	u, found := sys.store.GetUser(name)
	if !found {
		return false, "", errNoSuchUser
	}
	cred := u.Credentials
	if cred.IsTemp() {
		return true, cred.ParentUser, nil
	}

	return false, "", nil
}

// IsServiceAccount - returns if given key is a service account
func (sys *IAMSys) IsServiceAccount(name string) (bool, string, error) {
	if !sys.Initialized() {
		return false, "", errServerNotInitialized
	}

	u, found := sys.store.GetUser(name)
	if !found {
		return false, "", errNoSuchUser
	}
	cred := u.Credentials
	if cred.IsServiceAccount() {
		return true, cred.ParentUser, nil
	}

	return false, "", nil
}

// GetUserInfo - get info on a user.
func (sys *IAMSys) GetUserInfo(ctx context.Context, name string) (u madmin.UserInfo, err error) {
	if !sys.Initialized() {
		return u, errServerNotInitialized
	}

	loadUserCalled := false
	select {
	case <-sys.configLoaded:
	default:
		sys.store.LoadUser(ctx, name)
		loadUserCalled = true
	}

	userInfo, err := sys.store.GetUserInfo(name)
	if err == errNoSuchUser && !loadUserCalled {
		sys.store.LoadUser(ctx, name)
		userInfo, err = sys.store.GetUserInfo(name)
	}
	return userInfo, err
}

// QueryPolicyEntities - queries policy associations for builtin users/groups/policies.
func (sys *IAMSys) QueryPolicyEntities(ctx context.Context, q madmin.PolicyEntitiesQuery) (*madmin.PolicyEntitiesResult, error) {
	if !sys.Initialized() {
		return nil, errServerNotInitialized
	}

	select {
	case <-sys.configLoaded:
		cleanQuery := sys.createCleanEntitiesQuery(q, false)
		var userPredicate, groupPredicate func(string) bool
		if sys.LDAPConfig.Enabled() {
			userPredicate = func(s string) bool {
				return !sys.LDAPConfig.IsLDAPUserDN(s)
			}
			groupPredicate = func(s string) bool {
				return !sys.LDAPConfig.IsLDAPGroupDN(s)
			}
		}
		pe := sys.store.ListPolicyMappings(cleanQuery, userPredicate, groupPredicate, nil)
		pe.Timestamp = UTCNow()
		return &pe, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// SetUserStatus - sets current user status, supports disabled or enabled.
func (sys *IAMSys) SetUserStatus(ctx context.Context, accessKey string, status madmin.AccountStatus) (updatedAt time.Time, err error) {
	if !sys.Initialized() {
		return updatedAt, errServerNotInitialized
	}

	if sys.usersSysType != MinIOUsersSysType {
		return updatedAt, errIAMActionNotAllowed
	}

	updatedAt, err = sys.store.SetUserStatus(ctx, accessKey, status)
	if err != nil {
		return updatedAt, err
	}

	sys.notifyForUser(ctx, accessKey, false)
	return updatedAt, nil
}

func (sys *IAMSys) notifyForServiceAccount(ctx context.Context, accessKey string) {
	// Notify all other Minio peers to reload the service account
	if !sys.HasWatcher() {
		for _, nerr := range globalNotificationSys.LoadServiceAccount(ctx, accessKey) {
			if nerr.Err != nil {
				logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
				iamLogIf(ctx, nerr.Err)
			}
		}
	}
}

type newServiceAccountOpts struct {
	sessionPolicy              *policy.Policy
	accessKey                  string
	secretKey                  string
	name, description          string
	expiration                 *time.Time
	allowSiteReplicatorAccount bool // allow creating internal service account for site-replication.

	claims map[string]any
}

// NewServiceAccount - create a new service account
func (sys *IAMSys) NewServiceAccount(ctx context.Context, parentUser string, groups []string, opts newServiceAccountOpts) (auth.Credentials, time.Time, error) {
	if !sys.Initialized() {
		return auth.Credentials{}, time.Time{}, errServerNotInitialized
	}

	if parentUser == "" {
		return auth.Credentials{}, time.Time{}, errInvalidArgument
	}

	if len(opts.accessKey) > 0 && len(opts.secretKey) == 0 {
		return auth.Credentials{}, time.Time{}, auth.ErrNoSecretKeyWithAccessKey
	}
	if len(opts.secretKey) > 0 && len(opts.accessKey) == 0 {
		return auth.Credentials{}, time.Time{}, auth.ErrNoAccessKeyWithSecretKey
	}

	var policyBuf []byte
	if opts.sessionPolicy != nil {
		err := opts.sessionPolicy.Validate()
		if err != nil {
			return auth.Credentials{}, time.Time{}, err
		}
		policyBuf, err = json.Marshal(opts.sessionPolicy)
		if err != nil {
			return auth.Credentials{}, time.Time{}, err
		}
		if len(policyBuf) > maxSVCSessionPolicySize {
			return auth.Credentials{}, time.Time{}, errSessionPolicyTooLarge
		}
	}

	// found newly requested service account, to be same as
	// parentUser, reject such operations.
	if parentUser == opts.accessKey {
		return auth.Credentials{}, time.Time{}, errIAMActionNotAllowed
	}
	if siteReplicatorSvcAcc == opts.accessKey && !opts.allowSiteReplicatorAccount {
		return auth.Credentials{}, time.Time{}, errIAMActionNotAllowed
	}
	m := make(map[string]any)
	m[parentClaim] = parentUser

	if len(policyBuf) > 0 {
		m[policy.SessionPolicyName] = base64.StdEncoding.EncodeToString(policyBuf)
		m[iamPolicyClaimNameSA()] = embeddedPolicyType
	} else {
		m[iamPolicyClaimNameSA()] = inheritedPolicyType
	}

	// Add all the necessary claims for the service account.
	for k, v := range opts.claims {
		_, ok := m[k]
		if !ok {
			m[k] = v
		}
	}

	var accessKey, secretKey string
	var err error
	if len(opts.accessKey) > 0 || len(opts.secretKey) > 0 {
		accessKey, secretKey = opts.accessKey, opts.secretKey
	} else {
		accessKey, secretKey, err = auth.GenerateCredentials()
		if err != nil {
			return auth.Credentials{}, time.Time{}, err
		}
	}
	cred, err := auth.CreateNewCredentialsWithMetadata(accessKey, secretKey, m, secretKey)
	if err != nil {
		return auth.Credentials{}, time.Time{}, err
	}
	cred.ParentUser = parentUser
	cred.Groups = groups
	cred.Status = string(auth.AccountOn)
	cred.Name = opts.name
	cred.Description = opts.description

	if opts.expiration != nil {
		expirationInUTC := opts.expiration.UTC()
		if err := validateSvcExpirationInUTC(expirationInUTC); err != nil {
			return auth.Credentials{}, time.Time{}, err
		}
		cred.Expiration = expirationInUTC
	}

	updatedAt, err := sys.store.AddServiceAccount(ctx, cred)
	if err != nil {
		return auth.Credentials{}, time.Time{}, err
	}

	sys.notifyForServiceAccount(ctx, cred.AccessKey)
	return cred, updatedAt, nil
}

type updateServiceAccountOpts struct {
	sessionPolicy     *policy.Policy
	secretKey         string
	status            string
	name, description string
	expiration        *time.Time
}

// UpdateServiceAccount - edit a service account
func (sys *IAMSys) UpdateServiceAccount(ctx context.Context, accessKey string, opts updateServiceAccountOpts) (updatedAt time.Time, err error) {
	if !sys.Initialized() {
		return updatedAt, errServerNotInitialized
	}

	updatedAt, err = sys.store.UpdateServiceAccount(ctx, accessKey, opts)
	if err != nil {
		return updatedAt, err
	}

	sys.notifyForServiceAccount(ctx, accessKey)
	return updatedAt, nil
}

// ListServiceAccounts - lists all service accounts associated to a specific user
func (sys *IAMSys) ListServiceAccounts(ctx context.Context, accessKey string) ([]auth.Credentials, error) {
	if !sys.Initialized() {
		return nil, errServerNotInitialized
	}

	select {
	case <-sys.configLoaded:
		return sys.store.ListServiceAccounts(ctx, accessKey)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ListTempAccounts - lists all temporary service accounts associated to a specific user
func (sys *IAMSys) ListTempAccounts(ctx context.Context, accessKey string) ([]UserIdentity, error) {
	if !sys.Initialized() {
		return nil, errServerNotInitialized
	}

	select {
	case <-sys.configLoaded:
		return sys.store.ListTempAccounts(ctx, accessKey)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ListSTSAccounts - lists all STS accounts associated to a specific user
func (sys *IAMSys) ListSTSAccounts(ctx context.Context, accessKey string) ([]auth.Credentials, error) {
	if !sys.Initialized() {
		return nil, errServerNotInitialized
	}

	select {
	case <-sys.configLoaded:
		return sys.store.ListSTSAccounts(ctx, accessKey)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ListAllAccessKeys - lists all access keys (sts/service accounts)
func (sys *IAMSys) ListAllAccessKeys(ctx context.Context) ([]auth.Credentials, error) {
	if !sys.Initialized() {
		return nil, errServerNotInitialized
	}

	select {
	case <-sys.configLoaded:
		return sys.store.ListAccessKeys(ctx)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// GetServiceAccount - wrapper method to get information about a service account
func (sys *IAMSys) GetServiceAccount(ctx context.Context, accessKey string) (auth.Credentials, *policy.Policy, error) {
	sa, embeddedPolicy, err := sys.getServiceAccount(ctx, accessKey)
	if err != nil {
		return auth.Credentials{}, nil, err
	}
	// Hide secret & session keys
	sa.Credentials.SecretKey = ""
	sa.Credentials.SessionToken = ""
	return sa.Credentials, embeddedPolicy, nil
}

func (sys *IAMSys) getServiceAccount(ctx context.Context, accessKey string) (UserIdentity, *policy.Policy, error) {
	sa, jwtClaims, err := sys.getAccountWithClaims(ctx, accessKey)
	if err != nil {
		if err == errNoSuchAccount {
			return UserIdentity{}, nil, errNoSuchServiceAccount
		}
		return UserIdentity{}, nil, err
	}
	if !sa.Credentials.IsServiceAccount() {
		return UserIdentity{}, nil, errNoSuchServiceAccount
	}

	var embeddedPolicy *policy.Policy

	pt, ptok := jwtClaims.Lookup(iamPolicyClaimNameSA())
	sp, spok := jwtClaims.Lookup(policy.SessionPolicyName)
	if ptok && spok && pt == embeddedPolicyType {
		policyBytes, err := base64.StdEncoding.DecodeString(sp)
		if err != nil {
			return UserIdentity{}, nil, err
		}
		embeddedPolicy, err = policy.ParseConfig(bytes.NewReader(policyBytes))
		if err != nil {
			return UserIdentity{}, nil, err
		}
	}

	return sa, embeddedPolicy, nil
}

// GetTemporaryAccount - wrapper method to get information about a temporary account
func (sys *IAMSys) GetTemporaryAccount(ctx context.Context, accessKey string) (auth.Credentials, *policy.Policy, error) {
	if !sys.Initialized() {
		return auth.Credentials{}, nil, errServerNotInitialized
	}
	tmpAcc, embeddedPolicy, err := sys.getTempAccount(ctx, accessKey)
	if err != nil {
		if err == errNoSuchTempAccount {
			sys.store.LoadUser(ctx, accessKey)
			tmpAcc, embeddedPolicy, err = sys.getTempAccount(ctx, accessKey)
		}
		if err != nil {
			return auth.Credentials{}, nil, err
		}
	}
	// Hide secret & session keys
	tmpAcc.Credentials.SecretKey = ""
	tmpAcc.Credentials.SessionToken = ""
	return tmpAcc.Credentials, embeddedPolicy, nil
}

func (sys *IAMSys) getTempAccount(ctx context.Context, accessKey string) (UserIdentity, *policy.Policy, error) {
	tmpAcc, claims, err := sys.getAccountWithClaims(ctx, accessKey)
	if err != nil {
		if err == errNoSuchAccount {
			return UserIdentity{}, nil, errNoSuchTempAccount
		}
		return UserIdentity{}, nil, err
	}
	if !tmpAcc.Credentials.IsTemp() {
		return UserIdentity{}, nil, errNoSuchTempAccount
	}

	var embeddedPolicy *policy.Policy

	sp, spok := claims.Lookup(policy.SessionPolicyName)
	if spok {
		policyBytes, err := base64.StdEncoding.DecodeString(sp)
		if err != nil {
			return UserIdentity{}, nil, err
		}
		embeddedPolicy, err = policy.ParseConfig(bytes.NewReader(policyBytes))
		if err != nil {
			return UserIdentity{}, nil, err
		}
	}

	return tmpAcc, embeddedPolicy, nil
}

// getAccountWithClaims - gets information about an account with claims
func (sys *IAMSys) getAccountWithClaims(ctx context.Context, accessKey string) (UserIdentity, *jwt.MapClaims, error) {
	if !sys.Initialized() {
		return UserIdentity{}, nil, errServerNotInitialized
	}

	acc, ok := sys.store.GetUser(accessKey)
	if !ok {
		return UserIdentity{}, nil, errNoSuchAccount
	}

	jwtClaims, err := extractJWTClaims(acc)
	if err != nil {
		return UserIdentity{}, nil, err
	}

	return acc, jwtClaims, nil
}

// GetClaimsForSvcAcc - gets the claims associated with the service account.
func (sys *IAMSys) GetClaimsForSvcAcc(ctx context.Context, accessKey string) (map[string]any, error) {
	if !sys.Initialized() {
		return nil, errServerNotInitialized
	}

	sa, ok := sys.store.GetUser(accessKey)
	if !ok || !sa.Credentials.IsServiceAccount() {
		return nil, errNoSuchServiceAccount
	}

	jwtClaims, err := extractJWTClaims(sa)
	if err != nil {
		return nil, err
	}

	return jwtClaims.Map(), nil
}

// DeleteServiceAccount - delete a service account
func (sys *IAMSys) DeleteServiceAccount(ctx context.Context, accessKey string, notifyPeers bool) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	sa, ok := sys.store.GetUser(accessKey)
	if !ok || !sa.Credentials.IsServiceAccount() {
		return nil
	}

	if err := sys.store.DeleteUser(ctx, accessKey, svcUser); err != nil {
		return err
	}

	if notifyPeers && !sys.HasWatcher() {
		for _, nerr := range globalNotificationSys.DeleteServiceAccount(ctx, accessKey) {
			if nerr.Err != nil {
				logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
				iamLogIf(ctx, nerr.Err)
			}
		}
	}

	return nil
}

// CreateUser - create new user credentials and policy, if user already exists
// they shall be rewritten with new inputs.
func (sys *IAMSys) CreateUser(ctx context.Context, accessKey string, ureq madmin.AddOrUpdateUserReq) (updatedAt time.Time, err error) {
	if !sys.Initialized() {
		return updatedAt, errServerNotInitialized
	}

	if !auth.IsAccessKeyValid(accessKey) {
		return updatedAt, auth.ErrInvalidAccessKeyLength
	}

	if auth.ContainsReservedChars(accessKey) {
		return updatedAt, auth.ErrContainsReservedChars
	}

	if !auth.IsSecretKeyValid(ureq.SecretKey) {
		return updatedAt, auth.ErrInvalidSecretKeyLength
	}

	updatedAt, err = sys.store.AddUser(ctx, accessKey, ureq)
	if err != nil {
		return updatedAt, err
	}

	sys.notifyForUser(ctx, accessKey, false)
	return updatedAt, nil
}

// SetUserSecretKey - sets user secret key
func (sys *IAMSys) SetUserSecretKey(ctx context.Context, accessKey string, secretKey string) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	if sys.usersSysType != MinIOUsersSysType {
		return errIAMActionNotAllowed
	}

	if !auth.IsAccessKeyValid(accessKey) {
		return auth.ErrInvalidAccessKeyLength
	}

	if !auth.IsSecretKeyValid(secretKey) {
		return auth.ErrInvalidSecretKeyLength
	}

	return sys.store.UpdateUserSecretKey(ctx, accessKey, secretKey)
}

// purgeExpiredCredentialsForExternalSSO - validates if local credentials are still valid
// by checking remote IDP if the relevant users are still active and present.
func (sys *IAMSys) purgeExpiredCredentialsForExternalSSO(ctx context.Context) {
	parentUsersMap := sys.store.GetAllParentUsers()
	var expiredUsers []string
	for parentUser, puInfo := range parentUsersMap {
		// There are multiple role ARNs for parent user only when there
		// are multiple openid provider configurations with the same ID
		// provider. We lookup the provider associated with some one of
		// the roleARNs to check if the user still exists. If they don't
		// we can safely remove credentials for this parent user
		// associated with any of the provider configurations.
		//
		// If there is no roleARN mapped to the user, the user may be
		// coming from a policy claim based openid provider.
		roleArns := puInfo.roleArns.ToSlice()
		var roleArn string
		if len(roleArns) == 0 {
			iamLogIf(GlobalContext,
				fmt.Errorf("parentUser: %s had no roleArns mapped!", parentUser))
			continue
		}
		roleArn = roleArns[0]
		u, err := sys.OpenIDConfig.LookupUser(roleArn, puInfo.subClaimValue)
		if err != nil {
			iamLogIf(GlobalContext, err)
			continue
		}
		// If user is set to "disabled", we will remove them
		// subsequently.
		if !u.Enabled {
			expiredUsers = append(expiredUsers, parentUser)
		}
	}

	// We ignore any errors
	_ = sys.store.DeleteUsers(ctx, expiredUsers)
}

// purgeExpiredCredentialsForLDAP - validates if local credentials are still
// valid by checking LDAP server if the relevant users are still present.
func (sys *IAMSys) purgeExpiredCredentialsForLDAP(ctx context.Context) {
	parentUsers := sys.store.GetAllParentUsers()
	var allDistNames []string
	for parentUser, info := range parentUsers {
		if !sys.LDAPConfig.IsLDAPUserDN(parentUser) {
			continue
		}

		if info.subClaimValue != "" {
			// we need to ask LDAP about the actual user DN not normalized DN.
			allDistNames = append(allDistNames, info.subClaimValue)
		} else {
			allDistNames = append(allDistNames, parentUser)
		}
	}

	expiredUsers, err := sys.LDAPConfig.GetNonEligibleUserDistNames(allDistNames)
	if err != nil {
		// Log and return on error - perhaps it'll work the next time.
		iamLogIf(GlobalContext, err)
		return
	}

	// We ignore any errors
	_ = sys.store.DeleteUsers(ctx, expiredUsers)
}

// updateGroupMembershipsForLDAP - updates the list of groups associated with the credential.
func (sys *IAMSys) updateGroupMembershipsForLDAP(ctx context.Context) {
	// 1. Collect all LDAP users with active creds.
	allCreds := sys.store.GetSTSAndServiceAccounts()
	// List of unique LDAP (parent) user DNs that have active creds
	var parentUserActualDNList []string
	// Map of LDAP user (internal representation) to list of active credential objects
	parentUserToCredsMap := make(map[string][]auth.Credentials)
	// DN to ldap username mapping for each LDAP user
	actualDNToLDAPUsernameMap := make(map[string]string)
	// External (actual) LDAP DN to internal normalized representation
	actualDNToParentUserMap := make(map[string]string)
	for _, cred := range allCreds {
		// Expired credentials don't need parent user updates.
		if cred.IsExpired() {
			continue
		}

		if !sys.LDAPConfig.IsLDAPUserDN(cred.ParentUser) {
			continue
		}

		// Check if this is the first time we are
		// encountering this LDAP user.
		if _, ok := parentUserToCredsMap[cred.ParentUser]; !ok {
			// Try to find the ldapUsername for this
			// parentUser by extracting JWT claims
			var (
				jwtClaims *jwt.MapClaims
				err       error
			)

			if cred.SessionToken == "" {
				continue
			}

			if cred.IsServiceAccount() {
				jwtClaims, err = auth.ExtractClaims(cred.SessionToken, cred.SecretKey)
				if err != nil {
					jwtClaims, err = auth.ExtractClaims(cred.SessionToken, globalActiveCred.SecretKey)
				}
			} else {
				var secretKey string
				secretKey, err = getTokenSigningKey()
				if err != nil {
					continue
				}
				jwtClaims, err = auth.ExtractClaims(cred.SessionToken, secretKey)
			}
			if err != nil {
				// skip this cred - session token seems invalid
				continue
			}

			ldapUsername, okUserN := jwtClaims.Lookup(ldapUserN)
			ldapActualDN, okDN := jwtClaims.Lookup(ldapActualUser)
			if !okUserN || !okDN {
				// skip this cred - we dont have the
				// username info needed
				continue
			}

			// Collect each new cred.ParentUser into parentUsers
			parentUserActualDNList = append(parentUserActualDNList, ldapActualDN)

			// Update the ldapUsernameMap
			actualDNToLDAPUsernameMap[ldapActualDN] = ldapUsername

			// Update the actualDNToParentUserMap
			actualDNToParentUserMap[ldapActualDN] = cred.ParentUser
		}
		parentUserToCredsMap[cred.ParentUser] = append(parentUserToCredsMap[cred.ParentUser], cred)
	}

	// 2. Query LDAP server for groups of the LDAP users collected.
	updatedGroups, err := sys.LDAPConfig.LookupGroupMemberships(parentUserActualDNList, actualDNToLDAPUsernameMap)
	if err != nil {
		// Log and return on error - perhaps it'll work the next time.
		iamLogIf(GlobalContext, err)
		return
	}

	// 3. Update creds for those users whose groups are changed
	for _, parentActualDN := range parentUserActualDNList {
		currGroupsSet := updatedGroups[parentActualDN]
		parentUser := actualDNToParentUserMap[parentActualDN]
		currGroups := currGroupsSet.ToSlice()
		for _, cred := range parentUserToCredsMap[parentUser] {
			gSet := set.CreateStringSet(cred.Groups...)
			if gSet.Equals(currGroupsSet) {
				// No change to groups memberships for this
				// credential.
				continue
			}

			// Expired credentials don't need group membership updates.
			if cred.IsExpired() {
				continue
			}

			cred.Groups = currGroups
			if err := sys.store.UpdateUserIdentity(ctx, cred); err != nil {
				// Log and continue error - perhaps it'll work the next time.
				iamLogIf(GlobalContext, err)
			}
		}
	}
}

// NormalizeLDAPAccessKeypairs - normalize the access key pairs (service
// accounts) for LDAP users. This normalizes the parent user and the group names
// whenever the parent user parses validly as a DN.
func (sys *IAMSys) NormalizeLDAPAccessKeypairs(ctx context.Context, accessKeyMap map[string]madmin.SRSvcAccCreate,
) (skippedAccessKeys []string, err error) {
	conn, err := sys.LDAPConfig.LDAP.Connect()
	if err != nil {
		return skippedAccessKeys, err
	}
	defer conn.Close()

	// Bind to the lookup user account
	if err = sys.LDAPConfig.LDAP.LookupBind(conn); err != nil {
		return skippedAccessKeys, err
	}

	var collectedErrors []error
	updatedKeysMap := make(map[string]madmin.SRSvcAccCreate)
	for ak, createReq := range accessKeyMap {
		parent := createReq.Parent
		groups := createReq.Groups

		_, err := ldap.NormalizeDN(parent)
		if err != nil {
			// not a valid DN, ignore.
			continue
		}

		hasDiff := false

		// For the parent value, we require that the parent exists in the LDAP
		// server and is under a configured base DN.
		validatedParent, isUnderBaseDN, err := sys.LDAPConfig.GetValidatedUserDN(conn, parent)
		if err != nil {
			collectedErrors = append(collectedErrors, fmt.Errorf("could not validate parent exists in LDAP directory: %w", err))
			continue
		}
		if validatedParent == nil || !isUnderBaseDN {
			skippedAccessKeys = append(skippedAccessKeys, ak)
			continue
		}

		if validatedParent.NormDN != parent {
			hasDiff = true
		}

		var normalizedGroups []string
		for _, group := range groups {
			// For a group, we store the normalized DN even if it not under a
			// configured base DN.
			validatedGroup, _, err := sys.LDAPConfig.GetValidatedGroupDN(conn, group)
			if err != nil {
				collectedErrors = append(collectedErrors, fmt.Errorf("could not validate group exists in LDAP directory: %w", err))
				continue
			}
			if validatedGroup == nil {
				// DN group was not found in the LDAP directory for access-key
				continue
			}

			if validatedGroup.NormDN != group {
				hasDiff = true
			}
			normalizedGroups = append(normalizedGroups, validatedGroup.NormDN)
		}

		if hasDiff {
			updatedCreateReq := createReq
			updatedCreateReq.Parent = validatedParent.NormDN
			updatedCreateReq.Groups = normalizedGroups

			updatedKeysMap[ak] = updatedCreateReq
		}
	}

	// if there are any errors, return a collected error.
	if len(collectedErrors) > 0 {
		return skippedAccessKeys, fmt.Errorf("errors validating LDAP DN: %w", errors.Join(collectedErrors...))
	}

	// Replace the map values with the updated ones
	maps.Copy(accessKeyMap, updatedKeysMap)

	return skippedAccessKeys, nil
}

func (sys *IAMSys) getStoredLDAPPolicyMappingKeys(ctx context.Context, isGroup bool) set.StringSet {
	entityKeysInStorage := set.NewStringSet()
	cache := sys.store.rlock()
	defer sys.store.runlock()
	cachedPolicyMap := cache.iamSTSPolicyMap
	if isGroup {
		cachedPolicyMap = cache.iamGroupPolicyMap
	}
	cachedPolicyMap.Range(func(k string, v MappedPolicy) bool {
		entityKeysInStorage.Add(k)
		return true
	})

	return entityKeysInStorage
}

// NormalizeLDAPMappingImport - validates the LDAP policy mappings. Keys in the
// given map may not correspond to LDAP DNs - these keys are ignored.
//
// For validated mappings, it updates the key in the given map to be in
// normalized form.
func (sys *IAMSys) NormalizeLDAPMappingImport(ctx context.Context, isGroup bool,
	policyMap map[string]MappedPolicy,
) ([]string, error) {
	conn, err := sys.LDAPConfig.LDAP.Connect()
	if err != nil {
		return []string{}, err
	}
	defer conn.Close()

	// Bind to the lookup user account
	if err = sys.LDAPConfig.LDAP.LookupBind(conn); err != nil {
		return []string{}, err
	}

	// We map keys that correspond to LDAP DNs and validate that they exist in
	// the LDAP server.
	dnValidator := sys.LDAPConfig.GetValidatedUserDN
	if isGroup {
		dnValidator = sys.LDAPConfig.GetValidatedGroupDN
	}

	// map of normalized DN keys to original keys.
	normalizedDNKeysMap := make(map[string][]string)
	var collectedErrors []error
	var skipped []string
	for k := range policyMap {
		_, err := ldap.NormalizeDN(k)
		if err != nil {
			// not a valid DN, ignore.
			continue
		}
		validatedDN, underBaseDN, err := dnValidator(conn, k)
		if err != nil {
			collectedErrors = append(collectedErrors, fmt.Errorf("could not validate `%s` exists in LDAP directory: %w", k, err))
			continue
		}
		if validatedDN == nil || !underBaseDN {
			skipped = append(skipped, k)
			continue
		}

		if validatedDN.NormDN != k {
			normalizedDNKeysMap[validatedDN.NormDN] = append(normalizedDNKeysMap[validatedDN.NormDN], k)
		}
	}

	// if there are any errors, return a collected error.
	if len(collectedErrors) > 0 {
		return []string{}, fmt.Errorf("errors validating LDAP DN: %w", errors.Join(collectedErrors...))
	}

	entityKeysInStorage := sys.getStoredLDAPPolicyMappingKeys(ctx, isGroup)

	for normKey, origKeys := range normalizedDNKeysMap {
		if len(origKeys) > 1 {
			// If there are multiple DN keys that normalize to the same value,
			// check if the policy mappings are equal, if they are we don't need
			// to return an error.
			policiesDiffer := false
			firstMappedPolicies := policyMap[origKeys[0]].policySet()
			for i := 1; i < len(origKeys); i++ {
				otherMappedPolicies := policyMap[origKeys[i]].policySet()
				if !firstMappedPolicies.Equals(otherMappedPolicies) {
					policiesDiffer = true
					break
				}
			}

			if policiesDiffer {
				return []string{}, fmt.Errorf("multiple DNs map to the same LDAP DN[%s]: %v; please remove DNs that are not needed",
					normKey, origKeys)
			}

			if len(origKeys[1:]) > 0 {
				// Log that extra DN mappings will not be imported.
				iamLogEvent(ctx, "import-ldap-normalize: extraneous DN mappings found for LDAP DN[%s]: %v will not be imported", origKeys[0], origKeys[1:])
			}

			// Policies mapped to the DN's are the same, so we remove the extra
			// ones from the map.
			for i := 1; i < len(origKeys); i++ {
				delete(policyMap, origKeys[i])

				// Remove the mapping from storage by setting the policy to "".
				if entityKeysInStorage.Contains(origKeys[i]) {
					// Ignore any deletion error.
					_, delErr := sys.PolicyDBSet(ctx, origKeys[i], "", stsUser, isGroup)
					if delErr != nil {
						logErr := fmt.Errorf("failed to delete extraneous LDAP DN mapping for `%s`: %w", origKeys[i], delErr)
						iamLogIf(ctx, logErr)
					}
				}
			}
		}

		// Replacing origKeys[0] with normKey in the policyMap

		// len(origKeys) is always > 0, so here len(origKeys) == 1
		mappingValue := policyMap[origKeys[0]]
		delete(policyMap, origKeys[0])
		policyMap[normKey] = mappingValue
		iamLogEvent(ctx, "import-ldap-normalize: normalized LDAP DN mapping from `%s` to `%s`", origKeys[0], normKey)

		// Remove the mapping from storage by setting the policy to "".
		if entityKeysInStorage.Contains(origKeys[0]) {
			// Ignore any deletion error.
			_, delErr := sys.PolicyDBSet(ctx, origKeys[0], "", stsUser, isGroup)
			if delErr != nil {
				logErr := fmt.Errorf("failed to delete extraneous LDAP DN mapping for `%s`: %w", origKeys[0], delErr)
				iamLogIf(ctx, logErr)
			}
		}
	}
	return skipped, nil
}

// CheckKey validates the incoming accessKey
func (sys *IAMSys) CheckKey(ctx context.Context, accessKey string) (u UserIdentity, ok bool, err error) {
	if !sys.Initialized() {
		return u, false, nil
	}

	if accessKey == globalActiveCred.AccessKey {
		return newUserIdentity(globalActiveCred), true, nil
	}

	loadUserCalled := false
	select {
	case <-sys.configLoaded:
	default:
		err = sys.store.LoadUser(ctx, accessKey)
		loadUserCalled = true
	}

	u, ok = sys.store.GetUser(accessKey)
	if !ok && !loadUserCalled {
		err = sys.store.LoadUser(ctx, accessKey)
		loadUserCalled = true

		u, ok = sys.store.GetUser(accessKey)
	}

	if !ok && loadUserCalled && err != nil {
		iamLogOnceIf(ctx, err, accessKey)

		// return 503 to application
		return u, false, errIAMNotInitialized
	}

	return u, ok && u.Credentials.IsValid(), nil
}

// GetUser - get user credentials
func (sys *IAMSys) GetUser(ctx context.Context, accessKey string) (u UserIdentity, ok bool) {
	u, ok, _ = sys.CheckKey(ctx, accessKey)
	return u, ok
}

// Notify all other MinIO peers to load group.
func (sys *IAMSys) notifyForGroup(ctx context.Context, group string) {
	if !sys.HasWatcher() {
		for _, nerr := range globalNotificationSys.LoadGroup(ctx, group) {
			if nerr.Err != nil {
				logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
				iamLogIf(ctx, nerr.Err)
			}
		}
	}
}

// AddUsersToGroup - adds users to a group, creating the group if
// needed. No error if user(s) already are in the group.
func (sys *IAMSys) AddUsersToGroup(ctx context.Context, group string, members []string) (updatedAt time.Time, err error) {
	if !sys.Initialized() {
		return updatedAt, errServerNotInitialized
	}

	if auth.ContainsReservedChars(group) {
		return updatedAt, errGroupNameContainsReservedChars
	}

	updatedAt, err = sys.store.AddUsersToGroup(ctx, group, members)
	if err != nil {
		return updatedAt, err
	}

	sys.notifyForGroup(ctx, group)
	return updatedAt, nil
}

// RemoveUsersFromGroup - remove users from group. If no users are
// given, and the group is empty, deletes the group as well.
func (sys *IAMSys) RemoveUsersFromGroup(ctx context.Context, group string, members []string) (updatedAt time.Time, err error) {
	if !sys.Initialized() {
		return updatedAt, errServerNotInitialized
	}

	if sys.usersSysType != MinIOUsersSysType {
		return updatedAt, errIAMActionNotAllowed
	}

	updatedAt, err = sys.store.RemoveUsersFromGroup(ctx, group, members)
	if err != nil {
		return updatedAt, err
	}

	sys.notifyForGroup(ctx, group)
	return updatedAt, nil
}

// SetGroupStatus - enable/disabled a group
func (sys *IAMSys) SetGroupStatus(ctx context.Context, group string, enabled bool) (updatedAt time.Time, err error) {
	if !sys.Initialized() {
		return updatedAt, errServerNotInitialized
	}

	if sys.usersSysType != MinIOUsersSysType {
		return updatedAt, errIAMActionNotAllowed
	}

	updatedAt, err = sys.store.SetGroupStatus(ctx, group, enabled)
	if err != nil {
		return updatedAt, err
	}

	sys.notifyForGroup(ctx, group)
	return updatedAt, nil
}

// GetGroupDescription - builds up group description
func (sys *IAMSys) GetGroupDescription(group string) (gd madmin.GroupDesc, err error) {
	if !sys.Initialized() {
		return gd, errServerNotInitialized
	}

	return sys.store.GetGroupDescription(group)
}

// ListGroups - lists groups.
func (sys *IAMSys) ListGroups(ctx context.Context) (r []string, err error) {
	if !sys.Initialized() {
		return r, errServerNotInitialized
	}

	select {
	case <-sys.configLoaded:
		return sys.store.ListGroups(ctx)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// PolicyDBSet - sets a policy for a user or group in the PolicyDB. This does
// not validate if the user/group exists - that is the responsibility of the
// caller.
func (sys *IAMSys) PolicyDBSet(ctx context.Context, name, policy string, userType IAMUserType, isGroup bool) (updatedAt time.Time, err error) {
	if !sys.Initialized() {
		return updatedAt, errServerNotInitialized
	}

	updatedAt, err = sys.store.PolicyDBSet(ctx, name, policy, userType, isGroup)
	if err != nil {
		return updatedAt, err
	}

	// Notify all other MinIO peers to reload policy
	if !sys.HasWatcher() {
		for _, nerr := range globalNotificationSys.LoadPolicyMapping(ctx, name, userType, isGroup) {
			if nerr.Err != nil {
				logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
				iamLogIf(ctx, nerr.Err)
			}
		}
	}

	return updatedAt, nil
}

// PolicyDBUpdateBuiltin - adds or removes policies from a user or a group
// verified to be an internal IDP user.
func (sys *IAMSys) PolicyDBUpdateBuiltin(ctx context.Context, isAttach bool,
	r madmin.PolicyAssociationReq,
) (updatedAt time.Time, addedOrRemoved, effectivePolicies []string, err error) {
	if !sys.Initialized() {
		err = errServerNotInitialized
		return updatedAt, addedOrRemoved, effectivePolicies, err
	}

	userOrGroup := r.User
	var isGroup bool
	if userOrGroup == "" {
		isGroup = true
		userOrGroup = r.Group
	}

	if isGroup {
		_, err = sys.GetGroupDescription(userOrGroup)
		if err != nil {
			return updatedAt, addedOrRemoved, effectivePolicies, err
		}
	} else {
		var isTemp bool
		isTemp, _, err = sys.IsTempUser(userOrGroup)
		if err != nil && err != errNoSuchUser {
			return updatedAt, addedOrRemoved, effectivePolicies, err
		}
		if isTemp {
			err = errIAMActionNotAllowed
			return updatedAt, addedOrRemoved, effectivePolicies, err
		}

		// When the user is root credential you are not allowed to
		// add policies for root user.
		if userOrGroup == globalActiveCred.AccessKey {
			err = errIAMActionNotAllowed
			return updatedAt, addedOrRemoved, effectivePolicies, err
		}

		// Validate that user exists.
		var userExists bool
		_, userExists = sys.GetUser(ctx, userOrGroup)
		if !userExists {
			err = errNoSuchUser
			return updatedAt, addedOrRemoved, effectivePolicies, err
		}
	}

	updatedAt, addedOrRemoved, effectivePolicies, err = sys.store.PolicyDBUpdate(ctx, userOrGroup, isGroup,
		regUser, r.Policies, isAttach)
	if err != nil {
		return updatedAt, addedOrRemoved, effectivePolicies, err
	}

	// Notify all other MinIO peers to reload policy
	if !sys.HasWatcher() {
		for _, nerr := range globalNotificationSys.LoadPolicyMapping(ctx, userOrGroup, regUser, isGroup) {
			if nerr.Err != nil {
				logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
				iamLogIf(ctx, nerr.Err)
			}
		}
	}

	replLogIf(ctx, globalSiteReplicationSys.IAMChangeHook(ctx, madmin.SRIAMItem{
		Type: madmin.SRIAMItemPolicyMapping,
		PolicyMapping: &madmin.SRPolicyMapping{
			UserOrGroup: userOrGroup,
			UserType:    int(regUser),
			IsGroup:     isGroup,
			Policy:      strings.Join(effectivePolicies, ","),
		},
		UpdatedAt: updatedAt,
	}))

	return updatedAt, addedOrRemoved, effectivePolicies, err
}

// PolicyDBUpdateLDAP - adds or removes policies from a user or a group verified
// to be in the LDAP directory.
func (sys *IAMSys) PolicyDBUpdateLDAP(ctx context.Context, isAttach bool,
	r madmin.PolicyAssociationReq,
) (updatedAt time.Time, addedOrRemoved, effectivePolicies []string, err error) {
	if !sys.Initialized() {
		err = errServerNotInitialized
		return updatedAt, addedOrRemoved, effectivePolicies, err
	}

	var dn string
	var dnResult *ldap.DNSearchResult
	var isGroup bool
	if r.User != "" {
		dnResult, err = sys.LDAPConfig.GetValidatedDNForUsername(r.User)
		if err != nil {
			iamLogIf(ctx, err)
			return updatedAt, addedOrRemoved, effectivePolicies, err
		}
		if dnResult == nil {
			// dn not found - still attempt to detach if provided user is a DN.
			if !isAttach && sys.LDAPConfig.IsLDAPUserDN(r.User) {
				dn = sys.LDAPConfig.QuickNormalizeDN(r.User)
			} else {
				err = errNoSuchUser
				return updatedAt, addedOrRemoved, effectivePolicies, err
			}
		} else {
			dn = dnResult.NormDN
		}
		isGroup = false
	} else {
		var underBaseDN bool
		if dnResult, underBaseDN, err = sys.LDAPConfig.GetValidatedGroupDN(nil, r.Group); err != nil {
			iamLogIf(ctx, err)
			return updatedAt, addedOrRemoved, effectivePolicies, err
		}
		if dnResult == nil || !underBaseDN {
			if !isAttach {
				dn = sys.LDAPConfig.QuickNormalizeDN(r.Group)
			} else {
				err = errNoSuchGroup
				return updatedAt, addedOrRemoved, effectivePolicies, err
			}
		} else {
			// We use the group DN returned by the LDAP server (this may not
			// equal the input group name, but we assume it is canonical).
			dn = dnResult.NormDN
		}
		isGroup = true
	}

	// Backward compatibility in detaching non-normalized DNs.
	if !isAttach {
		var oldDN string
		if isGroup {
			oldDN = r.Group
		} else {
			oldDN = r.User
		}
		if oldDN != dn {
			sys.store.PolicyDBUpdate(ctx, oldDN, isGroup, stsUser, r.Policies, isAttach)
		}
	}

	userType := stsUser
	updatedAt, addedOrRemoved, effectivePolicies, err = sys.store.PolicyDBUpdate(
		ctx, dn, isGroup, userType, r.Policies, isAttach)
	if err != nil {
		return updatedAt, addedOrRemoved, effectivePolicies, err
	}

	// Notify all other MinIO peers to reload policy
	if !sys.HasWatcher() {
		for _, nerr := range globalNotificationSys.LoadPolicyMapping(ctx, dn, userType, isGroup) {
			if nerr.Err != nil {
				logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
				iamLogIf(ctx, nerr.Err)
			}
		}
	}

	replLogIf(ctx, globalSiteReplicationSys.IAMChangeHook(ctx, madmin.SRIAMItem{
		Type: madmin.SRIAMItemPolicyMapping,
		PolicyMapping: &madmin.SRPolicyMapping{
			UserOrGroup: dn,
			UserType:    int(userType),
			IsGroup:     isGroup,
			Policy:      strings.Join(effectivePolicies, ","),
		},
		UpdatedAt: updatedAt,
	}))

	return updatedAt, addedOrRemoved, effectivePolicies, err
}

// PolicyDBGet - gets policy set on a user or group. If a list of groups is
// given, policies associated with them are included as well.
func (sys *IAMSys) PolicyDBGet(name string, groups ...string) ([]string, error) {
	if !sys.Initialized() {
		return nil, errServerNotInitialized
	}

	return sys.store.PolicyDBGet(name, groups...)
}

const sessionPolicyNameExtracted = policy.SessionPolicyName + "-extracted"

// IsAllowedServiceAccount - checks if the given service account is allowed to perform
// actions. The permission of the parent user is checked first
func (sys *IAMSys) IsAllowedServiceAccount(args policy.Args, parentUser string) bool {
	// Verify if the parent claim matches the parentUser.
	p, ok := args.Claims[parentClaim]
	if ok {
		parentInClaim, ok := p.(string)
		if !ok {
			// Reject malformed/malicious requests.
			return false
		}
		// The parent claim in the session token should be equal
		// to the parent detected in the backend
		if parentInClaim != parentUser {
			return false
		}
	} else {
		// This is needed so a malicious user cannot
		// use a leaked session key of another user
		// to widen its privileges.
		return false
	}

	isOwnerDerived := parentUser == globalActiveCred.AccessKey

	var err error
	var svcPolicies []string
	roleArn := args.GetRoleArn()

	switch {
	case isOwnerDerived:
		// All actions are allowed by default and no policy evaluation is
		// required.

	case roleArn != "":
		arn, err := arn.Parse(roleArn)
		if err != nil {
			iamLogIf(GlobalContext, fmt.Errorf("error parsing role ARN %s: %v", roleArn, err))
			return false
		}
		svcPolicies = newMappedPolicy(sys.rolesMap[arn]).toSlice()
	default:
		// Check policy for parent user of service account.
		svcPolicies, err = sys.PolicyDBGet(parentUser, args.Groups...)
		if err != nil {
			iamLogIf(GlobalContext, err)
			return false
		}

		// Finally, if there is no parent policy, check if a policy claim is
		// present.
		if len(svcPolicies) == 0 {
			policySet, _ := policy.GetPoliciesFromClaims(args.Claims, iamPolicyClaimNameOpenID())
			svcPolicies = policySet.ToSlice()
		}
	}

	// Defensive code: Do not allow any operation if no policy is found.
	if !isOwnerDerived && len(svcPolicies) == 0 {
		return false
	}

	var combinedPolicy policy.Policy
	// Policies were found, evaluate all of them.
	if !isOwnerDerived {
		availablePoliciesStr, c := sys.store.MergePolicies(strings.Join(svcPolicies, ","))
		if availablePoliciesStr == "" {
			return false
		}
		combinedPolicy = c
	}

	parentArgs := args
	parentArgs.AccountName = parentUser

	saPolicyClaim, ok := args.Claims[iamPolicyClaimNameSA()]
	if !ok {
		return false
	}

	saPolicyClaimStr, ok := saPolicyClaim.(string)
	if !ok {
		// Sub policy if set, should be a string reject
		// malformed/malicious requests.
		return false
	}

	if saPolicyClaimStr == inheritedPolicyType {
		return isOwnerDerived || combinedPolicy.IsAllowed(parentArgs)
	}

	// 3. If an inline session-policy is present, evaluate it.
	hasSessionPolicy, isAllowedSP := isAllowedBySessionPolicyForServiceAccount(args)
	if hasSessionPolicy {
		return isAllowedSP && (isOwnerDerived || combinedPolicy.IsAllowed(parentArgs))
	}

	// Sub policy not set. Evaluate only the parent policies.
	return (isOwnerDerived || combinedPolicy.IsAllowed(parentArgs))
}

// IsAllowedSTS is meant for STS based temporary credentials,
// which implements claims validation and verification other than
// applying policies.
func (sys *IAMSys) IsAllowedSTS(args policy.Args, parentUser string) bool {
	// 1. Determine mapped policies

	isOwnerDerived := parentUser == globalActiveCred.AccessKey
	var policies []string
	roleArn := args.GetRoleArn()

	switch {
	case isOwnerDerived:
		// All actions are allowed by default and no policy evaluation is
		// required.

	case roleArn != "":
		// If a roleARN is present, the role policy is applied.
		arn, err := arn.Parse(roleArn)
		if err != nil {
			iamLogIf(GlobalContext, fmt.Errorf("error parsing role ARN %s: %v", roleArn, err))
			return false
		}
		policies = newMappedPolicy(sys.rolesMap[arn]).toSlice()

	default:
		// Otherwise, inherit parent user's policy
		var err error
		policies, err = sys.PolicyDBGet(parentUser, args.Groups...)
		if err != nil {
			iamLogIf(GlobalContext, fmt.Errorf("error fetching policies on %s: %v", parentUser, err))
			return false
		}

		// Finally, if there is no parent policy, check if a policy claim is
		// present in the session token.
		if len(policies) == 0 {
			// If there is no parent policy mapping, we fall back to
			// using policy claim from JWT.
			policySet, ok := args.GetPolicies(iamPolicyClaimNameOpenID())
			if !ok {
				// When claims are set, it should have a policy claim field.
				return false
			}
			policies = policySet.ToSlice()
		}
	}

	// Defensive code: Do not allow any operation if no policy is found in the session token
	if !isOwnerDerived && len(policies) == 0 {
		return false
	}

	// 2. Combine the mapped policies into a single combined policy.

	var combinedPolicy policy.Policy
	// Policies were found, evaluate all of them.
	if !isOwnerDerived {
		availablePoliciesStr, c := sys.store.MergePolicies(strings.Join(policies, ","))
		if availablePoliciesStr == "" {
			// all policies presented in the claim should exist
			iamLogIf(GlobalContext, fmt.Errorf("expected policy (%s) missing from the JWT claim %s, rejecting the request", policies, iamPolicyClaimNameOpenID()))

			return false
		}
		combinedPolicy = c
	}

	// 3. If an inline session-policy is present, evaluate it.

	// Now check if we have a sessionPolicy.
	hasSessionPolicy, isAllowedSP := isAllowedBySessionPolicy(args)
	if hasSessionPolicy {
		return isAllowedSP && (isOwnerDerived || combinedPolicy.IsAllowed(args))
	}

	// Sub policy not set, this is most common since subPolicy
	// is optional, use the inherited policies.
	return isOwnerDerived || combinedPolicy.IsAllowed(args)
}

func isAllowedBySessionPolicyForServiceAccount(args policy.Args) (hasSessionPolicy bool, isAllowed bool) {
	hasSessionPolicy = false
	isAllowed = false

	// Now check if we have a sessionPolicy.
	spolicy, ok := args.Claims[sessionPolicyNameExtracted]
	if !ok {
		return hasSessionPolicy, isAllowed
	}

	hasSessionPolicy = true

	spolicyStr, ok := spolicy.(string)
	if !ok {
		// Sub policy if set, should be a string reject
		// malformed/malicious requests.
		return hasSessionPolicy, isAllowed
	}

	// Check if policy is parseable.
	subPolicy, err := policy.ParseConfig(bytes.NewReader([]byte(spolicyStr)))
	if err != nil {
		// Log any error in input session policy config.
		iamLogIf(GlobalContext, err)
		return hasSessionPolicy, isAllowed
	}

	// SPECIAL CASE: For service accounts, any valid JSON is allowed as a
	// policy, regardless of whether the number of statements is 0, this
	// includes `null`, `{}` and `{"Statement": null}`. In fact, MinIO Console
	// sends `null` when no policy is set and the intended behavior is that the
	// service account should inherit parent policy. So when policy is empty in
	// all fields we return hasSessionPolicy=false.
	if subPolicy.Version == "" && subPolicy.Statements == nil && subPolicy.ID == "" {
		hasSessionPolicy = false
		return hasSessionPolicy, isAllowed
	}

	// As the session policy exists, even if the parent is the root account, it
	// must be restricted by it. So, we set `.IsOwner` to false here
	// unconditionally.
	//
	// We also set `DenyOnly` arg to false here - this is an IMPORTANT corner
	// case: DenyOnly is used only for allowing an account to do actions related
	// to its own account (like create service accounts for itself, among
	// others). However when a session policy is present, we need to validate
	// that the action is actually allowed, rather than checking if the action
	// is only disallowed.
	sessionPolicyArgs := args
	sessionPolicyArgs.IsOwner = false
	sessionPolicyArgs.DenyOnly = false

	// Sub policy is set and valid.
	return hasSessionPolicy, subPolicy.IsAllowed(sessionPolicyArgs)
}

func isAllowedBySessionPolicy(args policy.Args) (hasSessionPolicy bool, isAllowed bool) {
	hasSessionPolicy = false
	isAllowed = false

	// Now check if we have a sessionPolicy.
	spolicy, ok := args.Claims[sessionPolicyNameExtracted]
	if !ok {
		return hasSessionPolicy, isAllowed
	}

	hasSessionPolicy = true

	spolicyStr, ok := spolicy.(string)
	if !ok {
		// Sub policy if set, should be a string reject
		// malformed/malicious requests.
		return hasSessionPolicy, isAllowed
	}

	// Check if policy is parseable.
	subPolicy, err := policy.ParseConfig(bytes.NewReader([]byte(spolicyStr)))
	if err != nil {
		// Log any error in input session policy config.
		iamLogIf(GlobalContext, err)
		return hasSessionPolicy, isAllowed
	}

	// Policy without Version string value reject it.
	if subPolicy.Version == "" {
		return hasSessionPolicy, isAllowed
	}

	// As the session policy exists, even if the parent is the root account, it
	// must be restricted by it. So, we set `.IsOwner` to false here
	// unconditionally.
	//
	// We also set `DenyOnly` arg to false here - this is an IMPORTANT corner
	// case: DenyOnly is used only for allowing an account to do actions related
	// to its own account (like create service accounts for itself, among
	// others). However when a session policy is present, we need to validate
	// that the action is actually allowed, rather than checking if the action
	// is only disallowed.
	sessionPolicyArgs := args
	sessionPolicyArgs.IsOwner = false
	sessionPolicyArgs.DenyOnly = false

	// Sub policy is set and valid.
	return hasSessionPolicy, subPolicy.IsAllowed(sessionPolicyArgs)
}

// GetCombinedPolicy returns a combined policy combining all policies
func (sys *IAMSys) GetCombinedPolicy(policies ...string) policy.Policy {
	_, policy := sys.store.MergePolicies(strings.Join(policies, ","))
	return policy
}

// doesPolicyAllow - checks if the given policy allows the passed action with given args. This is rarely needed.
// Notice there is no account name involved, so this is a dangerous function.
func (sys *IAMSys) doesPolicyAllow(policy string, args policy.Args) bool {
	// Policies were found, evaluate all of them.
	return sys.GetCombinedPolicy(policy).IsAllowed(args)
}

// IsAllowed - checks given policy args is allowed to continue the Rest API.
func (sys *IAMSys) IsAllowed(args policy.Args) bool {
	// If opa is configured, use OPA always.
	if authz := newGlobalAuthZPluginFn(); authz != nil {
		ok, err := authz.IsAllowed(args)
		if err != nil {
			authZLogIf(GlobalContext, err)
		}
		return ok
	}

	// Policies don't apply to the owner.
	if args.IsOwner {
		return true
	}

	// If the credential is temporary, perform STS related checks.
	ok, parentUser, err := sys.IsTempUser(args.AccountName)
	if err != nil {
		return false
	}
	if ok {
		return sys.IsAllowedSTS(args, parentUser)
	}

	// If the credential is for a service account, perform related check
	ok, parentUser, err = sys.IsServiceAccount(args.AccountName)
	if err != nil {
		return false
	}
	if ok {
		return sys.IsAllowedServiceAccount(args, parentUser)
	}

	// Continue with the assumption of a regular user
	policies, err := sys.PolicyDBGet(args.AccountName, args.Groups...)
	if err != nil {
		return false
	}

	if len(policies) == 0 {
		// No policy found.
		return false
	}

	// Policies were found, evaluate all of them.
	return sys.GetCombinedPolicy(policies...).IsAllowed(args)
}

// SetUsersSysType - sets the users system type, regular or LDAP.
func (sys *IAMSys) SetUsersSysType(t UsersSysType) {
	sys.usersSysType = t
}

// GetUsersSysType - returns the users system type for this IAM
func (sys *IAMSys) GetUsersSysType() UsersSysType {
	return sys.usersSysType
}

// NewIAMSys - creates new config system object.
func NewIAMSys() *IAMSys {
	return &IAMSys{
		usersSysType: MinIOUsersSysType,
		configLoaded: make(chan struct{}),
	}
}
