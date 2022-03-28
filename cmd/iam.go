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
	"math/rand"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/madmin-go"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/arn"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/color"
	"github.com/minio/minio/internal/jwt"
	"github.com/minio/minio/internal/logger"
	iampolicy "github.com/minio/pkg/iam/policy"
	etcd "go.etcd.io/etcd/client/v3"
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

// IAMSys - config system.
type IAMSys struct {
	sync.Mutex

	iamRefreshInterval time.Duration

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
	regUser IAMUserType = iota
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
func (sys *IAMSys) LoadPolicyMapping(ctx context.Context, objAPI ObjectLayer, userOrGroup string, isGroup bool) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	// In case of LDAP, policy mappings are only applicable to sts users.
	userType := regUser
	if sys.usersSysType == LDAPUsersSysType {
		userType = stsUser
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

// Perform IAM configuration migration.
func (sys *IAMSys) doIAMConfigMigration(ctx context.Context) error {
	return sys.store.migrateBackendFormat(ctx)
}

// initStore initializes IAM stores
func (sys *IAMSys) initStore(objAPI ObjectLayer, etcdClient *etcd.Client) {
	if globalLDAPConfig.Enabled {
		sys.EnableLDAPSys()
	}

	if etcdClient == nil {
		if globalIsGateway {
			if globalGatewayName == NASBackendGateway {
				sys.store = &IAMStoreSys{newIAMObjectStore(objAPI, sys.usersSysType)}
			} else {
				sys.store = &IAMStoreSys{newIAMDummyStore(sys.usersSysType)}
				logger.Info("WARNING: %s gateway is running in-memory IAM store, for persistence please configure etcd",
					globalGatewayName)
			}
		} else {
			sys.store = &IAMStoreSys{newIAMObjectStore(objAPI, sys.usersSysType)}
		}
	} else {
		sys.store = &IAMStoreSys{newIAMEtcdStore(etcdClient, sys.usersSysType)}
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
func (sys *IAMSys) Load(ctx context.Context) error {
	err := sys.store.LoadIAMCache(ctx)
	if err != nil {
		return err
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
	sys.Lock()
	defer sys.Unlock()

	sys.iamRefreshInterval = iamRefreshInterval

	// Initialize IAM store
	sys.initStore(objAPI, etcdClient)

	retryCtx, cancel := context.WithCancel(ctx)

	// Indicate to our routine to exit cleanly upon return.
	defer cancel()

	// allocate dynamic timeout once before the loop
	iamLockTimeout := newDynamicTimeout(5*time.Second, 3*time.Second)

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Migrate storage format if needed.
	for {
		// Hold the lock for migration only.
		txnLk := objAPI.NewNSLock(minioMetaBucket, minioConfigPrefix+"/iam.lock")

		// let one of the server acquire the lock, if not let them timeout.
		// which shall be retried again by this loop.
		lkctx, err := txnLk.GetLock(retryCtx, iamLockTimeout)
		if err != nil {
			logger.Info("Waiting for all MinIO IAM sub-system to be initialized.. trying to acquire lock")
			time.Sleep(time.Duration(r.Float64() * float64(5*time.Second)))
			continue
		}

		if etcdClient != nil {
			// ****  WARNING ****
			// Migrating to encrypted backend on etcd should happen before initialization of
			// IAM sub-system, make sure that we do not move the above codeblock elsewhere.
			if err := migrateIAMConfigsEtcdToEncrypted(retryCtx, etcdClient); err != nil {
				txnLk.Unlock(lkctx.Cancel)
				if errors.Is(err, errEtcdUnreachable) {
					logger.Info("Connection to etcd timed out. Retrying..")
					continue
				}
				logger.LogIf(ctx, fmt.Errorf("Unable to decrypt an encrypted ETCD backend for IAM users and policies: %w", err))
				logger.LogIf(ctx, errors.New("IAM sub-system is partially initialized, some users may not be available"))
				return
			}
		}

		// These messages only meant primarily for distributed setup, so only log during distributed setup.
		if globalIsDistErasure {
			logger.Info("Waiting for all MinIO IAM sub-system to be initialized.. lock acquired")
		}

		// Migrate IAM configuration, if necessary.
		if err := sys.doIAMConfigMigration(retryCtx); err != nil {
			txnLk.Unlock(lkctx.Cancel)
			if configRetriableErrors(err) {
				logger.Info("Waiting for all MinIO IAM sub-system to be initialized.. possible cause (%v)", err)
				continue
			}
			logger.LogIf(ctx, fmt.Errorf("Unable to migrate IAM users and policies to new format: %w", err))
			logger.LogIf(ctx, errors.New("IAM sub-system is partially initialized, some users may not be available"))
			return
		}

		// Successfully migrated, proceed to load the users.
		txnLk.Unlock(lkctx.Cancel)
		break
	}

	// Load IAM data from storage.
	for {
		if err := sys.Load(retryCtx); err != nil {
			if configRetriableErrors(err) {
				logger.Info("Waiting for all MinIO IAM sub-system to be initialized.. possible cause (%v)", err)
				time.Sleep(time.Duration(r.Float64() * float64(5*time.Second)))
				continue
			}
			if err != nil {
				logger.LogIf(ctx, fmt.Errorf("Unable to initialize IAM sub-system, some users may not be available %w", err))
			}
		}
		break
	}

	// Set up polling for expired accounts and credentials purging.
	switch {
	case globalOpenIDConfig.ProviderEnabled():
		go func() {
			ticker := time.NewTicker(sys.iamRefreshInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					sys.purgeExpiredCredentialsForExternalSSO(ctx)
				case <-ctx.Done():
					return
				}
			}
		}()
	case globalLDAPConfig.Enabled:
		go func() {
			ticker := time.NewTicker(sys.iamRefreshInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					sys.purgeExpiredCredentialsForLDAP(ctx)
					sys.updateGroupMembershipsForLDAP(ctx)
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	// Start watching changes to storage.
	go sys.watch(ctx)

	// Load RoleARN
	if roleARN, rolePolicy, enabled := globalOpenIDConfig.GetRoleInfo(); enabled {
		numPolicies := len(strings.Split(rolePolicy, ","))
		validPolicies, _ := sys.store.FilterPolicies(rolePolicy, "")
		numValidPolicies := len(strings.Split(validPolicies, ","))
		if numPolicies != numValidPolicies {
			logger.LogIf(ctx, fmt.Errorf("Some specified role policies (%s) were not defined - role based policies will not be enabled.", rolePolicy))
			return
		}
		sys.rolesMap = map[arn.ARN]string{
			roleARN: rolePolicy,
		}
	}

	sys.printIAMRoles()

	logger.Info("Finished loading IAM sub-system.")
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

func (sys *IAMSys) watch(ctx context.Context) {
	watcher, ok := sys.store.IAMStorageAPI.(iamStorageWatcher)
	if ok {
		ch := watcher.watch(ctx, iamConfigPrefix)
		for event := range ch {
			// we simply log errors
			err := sys.loadWatchedEvent(ctx, event)
			logger.LogIf(ctx, fmt.Errorf("Failure in loading watch event: %v", err))
		}
		return
	}

	// Fall back to loading all items periodically
	ticker := time.NewTicker(sys.iamRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := sys.Load(ctx); err != nil {
				logger.LogIf(ctx, fmt.Errorf("Failure in periodic refresh for IAM: %v", err))
			}
		case <-ctx.Done():
			return
		}
	}
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
func (sys *IAMSys) GetRolePolicy(arnStr string) (string, error) {
	arn, err := arn.Parse(arnStr)
	if err != nil {
		return "", fmt.Errorf("RoleARN parse err: %v", err)
	}
	rolePolicy, ok := sys.rolesMap[arn]
	if !ok {
		return "", fmt.Errorf("RoleARN %s is not defined.", arnStr)
	}
	return rolePolicy, nil
}

// DeletePolicy - deletes a canned policy from backend or etcd.
func (sys *IAMSys) DeletePolicy(ctx context.Context, policyName string, notifyPeers bool) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	err := sys.store.DeletePolicy(ctx, policyName)
	if err != nil {
		return err
	}

	if !notifyPeers || sys.HasWatcher() {
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
func (sys *IAMSys) ListPolicies(ctx context.Context, bucketName string) (map[string]iampolicy.Policy, error) {
	if !sys.Initialized() {
		return nil, errServerNotInitialized
	}

	<-sys.configLoaded

	return sys.store.ListPolicies(ctx, bucketName)
}

// SetPolicy - sets a new named policy.
func (sys *IAMSys) SetPolicy(ctx context.Context, policyName string, p iampolicy.Policy) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	err := sys.store.SetPolicy(ctx, policyName, p)
	if err != nil {
		return err
	}

	if !sys.HasWatcher() {
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
		for _, nerr := range globalNotificationSys.DeleteUser(accessKey) {
			if nerr.Err != nil {
				logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
				logger.LogIf(ctx, nerr.Err)
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

	policies, _ := sys.store.FilterPolicies(policyName, "")
	return policies
}

func (sys *IAMSys) notifyForUser(ctx context.Context, accessKey string, isTemp bool) {
	// Notify all other MinIO peers to reload user.
	if !sys.HasWatcher() {
		for _, nerr := range globalNotificationSys.LoadUser(accessKey, isTemp) {
			if nerr.Err != nil {
				logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
				logger.LogIf(ctx, nerr.Err)
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
func (sys *IAMSys) SetTempUser(ctx context.Context, accessKey string, cred auth.Credentials, policyName string) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	if globalPolicyOPA != nil {
		// If OPA is set, we do not need to set a policy mapping.
		policyName = ""
	}

	err := sys.store.SetTempUser(ctx, accessKey, cred, policyName)
	if err != nil {
		return err
	}

	sys.notifyForUser(ctx, cred.AccessKey, true)

	return nil
}

// ListBucketUsers - list all users who can access this 'bucket'
func (sys *IAMSys) ListBucketUsers(bucket string) (map[string]madmin.UserInfo, error) {
	if !sys.Initialized() {
		return nil, errServerNotInitialized
	}

	<-sys.configLoaded

	return sys.store.GetBucketUsers(bucket)
}

// ListUsers - list all users.
func (sys *IAMSys) ListUsers() (map[string]madmin.UserInfo, error) {
	if !sys.Initialized() {
		return nil, errServerNotInitialized
	}

	<-sys.configLoaded

	return sys.store.GetUsers(), nil
}

// IsTempUser - returns if given key is a temporary user.
func (sys *IAMSys) IsTempUser(name string) (bool, string, error) {
	if !sys.Initialized() {
		return false, "", errServerNotInitialized
	}

	cred, found := sys.store.GetUser(name)
	if !found {
		return false, "", errNoSuchUser
	}

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

	cred, found := sys.store.GetUser(name)
	if !found {
		return false, "", errNoSuchUser
	}

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

	select {
	case <-sys.configLoaded:
	default:
		sys.store.LoadUser(ctx, name)
	}

	return sys.store.GetUserInfo(name)
}

// SetUserStatus - sets current user status, supports disabled or enabled.
func (sys *IAMSys) SetUserStatus(ctx context.Context, accessKey string, status madmin.AccountStatus) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	if sys.usersSysType != MinIOUsersSysType {
		return errIAMActionNotAllowed
	}

	err := sys.store.SetUserStatus(ctx, accessKey, status)
	if err != nil {
		return err
	}

	sys.notifyForUser(ctx, accessKey, false)
	return nil
}

func (sys *IAMSys) notifyForServiceAccount(ctx context.Context, accessKey string) {
	// Notify all other Minio peers to reload the service account
	if !sys.HasWatcher() {
		for _, nerr := range globalNotificationSys.LoadServiceAccount(accessKey) {
			if nerr.Err != nil {
				logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
				logger.LogIf(ctx, nerr.Err)
			}
		}
	}
}

type newServiceAccountOpts struct {
	sessionPolicy *iampolicy.Policy
	accessKey     string
	secretKey     string

	claims map[string]interface{}
}

// NewServiceAccount - create a new service account
func (sys *IAMSys) NewServiceAccount(ctx context.Context, parentUser string, groups []string, opts newServiceAccountOpts) (auth.Credentials, error) {
	if !sys.Initialized() {
		return auth.Credentials{}, errServerNotInitialized
	}

	if parentUser == "" {
		return auth.Credentials{}, errInvalidArgument
	}

	var policyBuf []byte
	if opts.sessionPolicy != nil {
		err := opts.sessionPolicy.Validate()
		if err != nil {
			return auth.Credentials{}, err
		}
		policyBuf, err = json.Marshal(opts.sessionPolicy)
		if err != nil {
			return auth.Credentials{}, err
		}
		if len(policyBuf) > 16*humanize.KiByte {
			return auth.Credentials{}, fmt.Errorf("Session policy should not exceed 16 KiB characters")
		}
	}

	// found newly requested service account, to be same as
	// parentUser, reject such operations.
	if parentUser == opts.accessKey {
		return auth.Credentials{}, errIAMActionNotAllowed
	}

	m := make(map[string]interface{})
	m[parentClaim] = parentUser

	if len(policyBuf) > 0 {
		m[iampolicy.SessionPolicyName] = base64.StdEncoding.EncodeToString(policyBuf)
		m[iamPolicyClaimNameSA()] = "embedded-policy"
	} else {
		m[iamPolicyClaimNameSA()] = "inherited-policy"
	}

	// Add all the necessary claims for the service accounts.
	for k, v := range opts.claims {
		_, ok := m[k]
		if !ok {
			m[k] = v
		}
	}

	var accessKey, secretKey string
	var err error
	if len(opts.accessKey) > 0 {
		accessKey, secretKey = opts.accessKey, opts.secretKey
	} else {
		accessKey, secretKey, err = auth.GenerateCredentials()
		if err != nil {
			return auth.Credentials{}, err
		}
	}
	cred, err := auth.CreateNewCredentialsWithMetadata(accessKey, secretKey, m, secretKey)
	if err != nil {
		return auth.Credentials{}, err
	}
	cred.ParentUser = parentUser
	cred.Groups = groups
	cred.Status = string(auth.AccountOn)

	err = sys.store.AddServiceAccount(ctx, cred)
	if err != nil {
		return auth.Credentials{}, err
	}

	sys.notifyForServiceAccount(ctx, cred.AccessKey)
	return cred, nil
}

type updateServiceAccountOpts struct {
	sessionPolicy *iampolicy.Policy
	secretKey     string
	status        string
}

// UpdateServiceAccount - edit a service account
func (sys *IAMSys) UpdateServiceAccount(ctx context.Context, accessKey string, opts updateServiceAccountOpts) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	err := sys.store.UpdateServiceAccount(ctx, accessKey, opts)
	if err != nil {
		return err
	}

	sys.notifyForServiceAccount(ctx, accessKey)
	return nil
}

// ListServiceAccounts - lists all services accounts associated to a specific user
func (sys *IAMSys) ListServiceAccounts(ctx context.Context, accessKey string) ([]auth.Credentials, error) {
	if !sys.Initialized() {
		return nil, errServerNotInitialized
	}

	<-sys.configLoaded

	return sys.store.ListServiceAccounts(ctx, accessKey)
}

// GetServiceAccount - wrapper method to get information about a service account
func (sys *IAMSys) GetServiceAccount(ctx context.Context, accessKey string) (auth.Credentials, *iampolicy.Policy, error) {
	sa, embeddedPolicy, err := sys.getServiceAccount(ctx, accessKey)
	if err != nil {
		return sa, embeddedPolicy, err
	}
	// Hide secret & session keys
	sa.SecretKey = ""
	sa.SessionToken = ""
	return sa, embeddedPolicy, nil
}

// getServiceAccount - gets information about a service account
func (sys *IAMSys) getServiceAccount(ctx context.Context, accessKey string) (auth.Credentials, *iampolicy.Policy, error) {
	if !sys.Initialized() {
		return auth.Credentials{}, nil, errServerNotInitialized
	}

	sa, ok := sys.store.GetUser(accessKey)
	if !ok || !sa.IsServiceAccount() {
		return auth.Credentials{}, nil, errNoSuchServiceAccount
	}

	var embeddedPolicy *iampolicy.Policy

	jwtClaims, err := auth.ExtractClaims(sa.SessionToken, sa.SecretKey)
	if err != nil {
		jwtClaims, err = auth.ExtractClaims(sa.SessionToken, globalActiveCred.SecretKey)
		if err != nil {
			return auth.Credentials{}, nil, err
		}
	}
	pt, ptok := jwtClaims.Lookup(iamPolicyClaimNameSA())
	sp, spok := jwtClaims.Lookup(iampolicy.SessionPolicyName)
	if ptok && spok && pt == "embedded-policy" {
		policyBytes, err := base64.StdEncoding.DecodeString(sp)
		if err != nil {
			return auth.Credentials{}, nil, err
		}
		embeddedPolicy, err = iampolicy.ParseConfig(bytes.NewReader(policyBytes))
		if err != nil {
			return auth.Credentials{}, nil, err
		}
	}

	return sa, embeddedPolicy, nil
}

// GetClaimsForSvcAcc - gets the claims associated with the service account.
func (sys *IAMSys) GetClaimsForSvcAcc(ctx context.Context, accessKey string) (map[string]interface{}, error) {
	if !sys.Initialized() {
		return nil, errServerNotInitialized
	}

	if sys.usersSysType != LDAPUsersSysType {
		return nil, nil
	}

	sa, ok := sys.store.GetUser(accessKey)
	if !ok || !sa.IsServiceAccount() {
		return nil, errNoSuchServiceAccount
	}

	jwtClaims, err := auth.ExtractClaims(sa.SessionToken, sa.SecretKey)
	if err != nil {
		jwtClaims, err = auth.ExtractClaims(sa.SessionToken, globalActiveCred.SecretKey)
		if err != nil {
			return nil, err
		}
	}
	return jwtClaims.Map(), nil
}

// DeleteServiceAccount - delete a service account
func (sys *IAMSys) DeleteServiceAccount(ctx context.Context, accessKey string, notifyPeers bool) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	sa, ok := sys.store.GetUser(accessKey)
	if !ok || !sa.IsServiceAccount() {
		return nil
	}

	if err := sys.store.DeleteUser(ctx, accessKey, svcUser); err != nil {
		return err
	}

	if notifyPeers && !sys.HasWatcher() {
		for _, nerr := range globalNotificationSys.DeleteServiceAccount(accessKey) {
			if nerr.Err != nil {
				logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
				logger.LogIf(ctx, nerr.Err)
			}
		}
	}

	return nil
}

// CreateUser - create new user credentials and policy, if user already exists
// they shall be rewritten with new inputs.
func (sys *IAMSys) CreateUser(ctx context.Context, accessKey string, ureq madmin.AddOrUpdateUserReq) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	if sys.usersSysType != MinIOUsersSysType {
		return errIAMActionNotAllowed
	}

	if !auth.IsAccessKeyValid(accessKey) {
		return auth.ErrInvalidAccessKeyLength
	}

	if !auth.IsSecretKeyValid(ureq.SecretKey) {
		return auth.ErrInvalidSecretKeyLength
	}

	err := sys.store.AddUser(ctx, accessKey, ureq)
	if err != nil {
		return err
	}

	sys.notifyForUser(ctx, accessKey, false)
	return nil
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
	parentUsers := sys.store.GetAllParentUsers()
	var expiredUsers []string
	for parentUser, expiredUser := range parentUsers {
		u, err := globalOpenIDConfig.LookupUser(parentUser)
		if err != nil {
			logger.LogIf(GlobalContext, err)
			continue
		}
		// If user is set to "disabled", we will remove them
		// subsequently.
		if !u.Enabled {
			expiredUsers = append(expiredUsers, expiredUser)
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
	for parentUser, expiredUser := range parentUsers {
		if !globalLDAPConfig.IsLDAPUserDN(parentUser) {
			continue
		}

		allDistNames = append(allDistNames, expiredUser)
	}

	expiredUsers, err := globalLDAPConfig.GetNonEligibleUserDistNames(allDistNames)
	if err != nil {
		// Log and return on error - perhaps it'll work the next time.
		logger.LogIf(GlobalContext, err)
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
	var parentUsers []string
	// Map of LDAP user to list of active credential objects
	parentUserToCredsMap := make(map[string][]auth.Credentials)
	// DN to ldap username mapping for each LDAP user
	parentUserToLDAPUsernameMap := make(map[string]string)
	for _, cred := range allCreds {
		if !globalLDAPConfig.IsLDAPUserDN(cred.ParentUser) {
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
				jwtClaims, err = auth.ExtractClaims(cred.SessionToken, globalActiveCred.SecretKey)
			}
			if err != nil {
				// skip this cred - session token seems invalid
				continue
			}

			ldapUsername, ok := jwtClaims.Lookup(ldapUserN)
			if !ok {
				// skip this cred - we dont have the
				// username info needed
				continue
			}

			// Collect each new cred.ParentUser into parentUsers
			parentUsers = append(parentUsers, cred.ParentUser)

			// Update the ldapUsernameMap
			parentUserToLDAPUsernameMap[cred.ParentUser] = ldapUsername
		}
		parentUserToCredsMap[cred.ParentUser] = append(parentUserToCredsMap[cred.ParentUser], cred)

	}

	// 2. Query LDAP server for groups of the LDAP users collected.
	updatedGroups, err := globalLDAPConfig.LookupGroupMemberships(parentUsers, parentUserToLDAPUsernameMap)
	if err != nil {
		// Log and return on error - perhaps it'll work the next time.
		logger.LogIf(GlobalContext, err)
		return
	}

	// 3. Update creds for those users whose groups are changed
	for _, parentUser := range parentUsers {
		currGroupsSet := updatedGroups[parentUser]
		currGroups := currGroupsSet.ToSlice()
		for _, cred := range parentUserToCredsMap[parentUser] {
			gSet := set.CreateStringSet(cred.Groups...)
			if gSet.Equals(currGroupsSet) {
				// No change to groups memberships for this
				// credential.
				continue
			}

			cred.Groups = currGroups
			if err := sys.store.UpdateUserIdentity(ctx, cred); err != nil {
				// Log and continue error - perhaps it'll work the next time.
				logger.LogIf(GlobalContext, err)
			}
		}
	}
}

// GetUser - get user credentials
func (sys *IAMSys) GetUser(ctx context.Context, accessKey string) (cred auth.Credentials, ok bool) {
	if !sys.Initialized() {
		return cred, false
	}

	fallback := false
	select {
	case <-sys.configLoaded:
	default:
		sys.store.LoadUser(ctx, accessKey)
		fallback = true
	}

	cred, ok = sys.store.GetUser(accessKey)
	if !ok && !fallback {
		// accessKey not found, also
		// IAM store is not in fallback mode
		// we can try to reload again from
		// the IAM store and see if credential
		// exists now. If it doesn't proceed to
		// fail.
		sys.store.LoadUser(ctx, accessKey)
		cred, ok = sys.store.GetUser(accessKey)
	}

	return cred, ok && cred.IsValid()
}

// Notify all other MinIO peers to load group.
func (sys *IAMSys) notifyForGroup(ctx context.Context, group string) {
	if !sys.HasWatcher() {
		for _, nerr := range globalNotificationSys.LoadGroup(group) {
			if nerr.Err != nil {
				logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
				logger.LogIf(ctx, nerr.Err)
			}
		}
	}
}

// AddUsersToGroup - adds users to a group, creating the group if
// needed. No error if user(s) already are in the group.
func (sys *IAMSys) AddUsersToGroup(ctx context.Context, group string, members []string) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	if sys.usersSysType != MinIOUsersSysType {
		return errIAMActionNotAllowed
	}

	err := sys.store.AddUsersToGroup(ctx, group, members)
	if err != nil {
		return err
	}

	sys.notifyForGroup(ctx, group)
	return nil
}

// RemoveUsersFromGroup - remove users from group. If no users are
// given, and the group is empty, deletes the group as well.
func (sys *IAMSys) RemoveUsersFromGroup(ctx context.Context, group string, members []string) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	if sys.usersSysType != MinIOUsersSysType {
		return errIAMActionNotAllowed
	}

	err := sys.store.RemoveUsersFromGroup(ctx, group, members)
	if err != nil {
		return err
	}

	sys.notifyForGroup(ctx, group)
	return nil
}

// SetGroupStatus - enable/disabled a group
func (sys *IAMSys) SetGroupStatus(ctx context.Context, group string, enabled bool) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	if sys.usersSysType != MinIOUsersSysType {
		return errIAMActionNotAllowed
	}

	err := sys.store.SetGroupStatus(ctx, group, enabled)
	if err != nil {
		return err
	}

	sys.notifyForGroup(ctx, group)
	return nil
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

	<-sys.configLoaded

	return sys.store.ListGroups(ctx)
}

// PolicyDBSet - sets a policy for a user or group in the PolicyDB.
func (sys *IAMSys) PolicyDBSet(ctx context.Context, name, policy string, isGroup bool) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	// Determine user-type based on IDP mode.
	userType := regUser
	if sys.usersSysType == LDAPUsersSysType {
		userType = stsUser
	}

	err := sys.store.PolicyDBSet(ctx, name, policy, userType, isGroup)
	if err != nil {
		return err
	}

	// Notify all other MinIO peers to reload policy
	if !sys.HasWatcher() {
		for _, nerr := range globalNotificationSys.LoadPolicyMapping(name, isGroup) {
			if nerr.Err != nil {
				logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
				logger.LogIf(ctx, nerr.Err)
			}
		}
	}

	return nil
}

// PolicyDBGet - gets policy set on a user or group. If a list of groups is
// given, policies associated with them are included as well.
func (sys *IAMSys) PolicyDBGet(name string, isGroup bool, groups ...string) ([]string, error) {
	if !sys.Initialized() {
		return nil, errServerNotInitialized
	}

	return sys.store.PolicyDBGet(name, isGroup, groups...)
}

// IsAllowedServiceAccount - checks if the given service account is allowed to perform
// actions. The permission of the parent user is checked first
func (sys *IAMSys) IsAllowedServiceAccount(args iampolicy.Args, parentUser string) bool {
	// Now check if we have a subject claim
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

	// Check policy for parent user of service account.
	svcPolicies, err := sys.PolicyDBGet(parentUser, false, args.Groups...)
	if err != nil {
		logger.LogIf(GlobalContext, err)
		return false
	}

	if len(svcPolicies) == 0 {
		// If parent user has no policies, check for OpenID
		// claims/RoleARN in case it exists.
		roleArn := args.GetRoleArn()
		if roleArn != "" {
			arn, err := arn.Parse(roleArn)
			if err != nil {
				logger.LogIf(GlobalContext, fmt.Errorf("error parsing role ARN %s: %v", roleArn, err))
				return false
			}
			svcPolicies = newMappedPolicy(sys.rolesMap[arn]).toSlice()
		} else {
			// If there is no roleArn claim, check the OpenID
			// provider's policy claim.
			policySet, _ := iampolicy.GetPoliciesFromClaims(args.Claims, iamPolicyClaimNameOpenID())
			svcPolicies = policySet.ToSlice()
		}
		if len(svcPolicies) == 0 {
			return false
		}
	}

	// Policies were found, evaluate all of them.
	availablePoliciesStr, combinedPolicy := sys.store.FilterPolicies(strings.Join(svcPolicies, ","), "")
	if availablePoliciesStr == "" {
		return false
	}

	parentArgs := args
	parentArgs.AccountName = parentUser
	// These are dynamic values set them appropriately.
	parentArgs.ConditionValues["username"] = []string{parentUser}
	parentArgs.ConditionValues["userid"] = []string{parentUser}

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

	if saPolicyClaimStr == "inherited-policy" {
		return combinedPolicy.IsAllowed(parentArgs)
	}

	// Now check if we have a sessionPolicy.
	spolicy, ok := args.Claims[iampolicy.SessionPolicyName]
	if !ok {
		return false
	}

	spolicyStr, ok := spolicy.(string)
	if !ok {
		// Sub policy if set, should be a string reject
		// malformed/malicious requests.
		return false
	}

	// Check if policy is parseable.
	subPolicy, err := iampolicy.ParseConfig(bytes.NewReader([]byte(spolicyStr)))
	if err != nil {
		// Log any error in input session policy config.
		logger.LogIf(GlobalContext, err)
		return false
	}

	// This can only happen if policy was set but with an empty JSON.
	if subPolicy.Version == "" && len(subPolicy.Statements) == 0 {
		return combinedPolicy.IsAllowed(parentArgs)
	}

	if subPolicy.Version == "" {
		return false
	}

	return combinedPolicy.IsAllowed(parentArgs) && subPolicy.IsAllowed(parentArgs)
}

// IsAllowedLDAPSTS - checks for LDAP specific claims and values
func (sys *IAMSys) IsAllowedLDAPSTS(args iampolicy.Args, parentUser string) bool {
	// parentUser value must match the ldap user in the claim.
	if parentInClaimIface, ok := args.Claims[ldapUser]; !ok {
		// no ldapUser claim present reject it.
		return false
	} else if parentInClaim, ok := parentInClaimIface.(string); !ok {
		// not the right type, reject it.
		return false
	} else if parentInClaim != parentUser {
		// ldap claim has been modified maliciously reject it.
		return false
	}

	// Check policy for this LDAP user.
	ldapPolicies, err := sys.PolicyDBGet(parentUser, false, args.Groups...)
	if err != nil {
		return false
	}

	if len(ldapPolicies) == 0 {
		return false
	}

	// Policies were found, evaluate all of them.
	availablePoliciesStr, combinedPolicy := sys.store.FilterPolicies(strings.Join(ldapPolicies, ","), "")
	if availablePoliciesStr == "" {
		return false
	}

	hasSessionPolicy, isAllowedSP := isAllowedBySessionPolicy(args)
	if hasSessionPolicy {
		return isAllowedSP && combinedPolicy.IsAllowed(args)
	}

	return combinedPolicy.IsAllowed(args)
}

// IsAllowedSTS is meant for STS based temporary credentials,
// which implements claims validation and verification other than
// applying policies.
func (sys *IAMSys) IsAllowedSTS(args iampolicy.Args, parentUser string) bool {
	// If it is an LDAP request, check that user and group
	// policies allow the request.
	if sys.usersSysType == LDAPUsersSysType {
		return sys.IsAllowedLDAPSTS(args, parentUser)
	}

	var policies []string
	roleArn := args.GetRoleArn()
	if roleArn != "" {
		arn, err := arn.Parse(roleArn)
		if err != nil {
			logger.LogIf(GlobalContext, fmt.Errorf("error parsing role ARN %s: %v", roleArn, err))
			return false
		}
		policies = newMappedPolicy(sys.rolesMap[arn]).toSlice()
	} else {
		// Lookup the parent user's mapping if there's no role-ARN.
		var err error
		policies, err = sys.store.PolicyDBGet(parentUser, false, args.Groups...)
		if err != nil {
			logger.LogIf(GlobalContext, fmt.Errorf("error fetching policies on %s: %v", parentUser, err))
			return false
		}
		if len(policies) == 0 {
			// TODO (deprecated in Dec 2021): Only need to handle
			// behavior for STS credentials created in older
			// releases. Otherwise, reject such cases, once older
			// behavior is deprecated.

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

	combinedPolicy, err := sys.store.GetPolicy(strings.Join(policies, ","))
	if err == errNoSuchPolicy {
		for _, pname := range policies {
			_, err := sys.store.GetPolicy(pname)
			if err == errNoSuchPolicy {
				// all policies presented in the claim should exist
				logger.LogIf(GlobalContext, fmt.Errorf("expected policy (%s) missing from the JWT claim %s, rejecting the request", pname, iamPolicyClaimNameOpenID()))
				return false
			}
		}
		logger.LogIf(GlobalContext, fmt.Errorf("all policies were unexpectedly present!"))
		return false
	}

	// These are dynamic values set them appropriately.
	args.ConditionValues["username"] = []string{parentUser}
	args.ConditionValues["userid"] = []string{parentUser}

	// Now check if we have a sessionPolicy.
	hasSessionPolicy, isAllowedSP := isAllowedBySessionPolicy(args)
	if hasSessionPolicy {
		return isAllowedSP && combinedPolicy.IsAllowed(args)
	}

	// Sub policy not set, this is most common since subPolicy
	// is optional, use the inherited policies.
	return combinedPolicy.IsAllowed(args)
}

func isAllowedBySessionPolicy(args iampolicy.Args) (hasSessionPolicy bool, isAllowed bool) {
	hasSessionPolicy = false
	isAllowed = false

	// Now check if we have a sessionPolicy.
	spolicy, ok := args.Claims[iampolicy.SessionPolicyName]
	if !ok {
		return
	}

	hasSessionPolicy = true

	spolicyStr, ok := spolicy.(string)
	if !ok {
		// Sub policy if set, should be a string reject
		// malformed/malicious requests.
		return
	}

	// Check if policy is parseable.
	subPolicy, err := iampolicy.ParseConfig(bytes.NewReader([]byte(spolicyStr)))
	if err != nil {
		// Log any error in input session policy config.
		logger.LogIf(GlobalContext, err)
		return
	}

	// Policy without Version string value reject it.
	if subPolicy.Version == "" {
		return
	}

	// Sub policy is set and valid.
	return hasSessionPolicy, subPolicy.IsAllowed(args)
}

// GetCombinedPolicy returns a combined policy combining all policies
func (sys *IAMSys) GetCombinedPolicy(policies ...string) iampolicy.Policy {
	_, policy := sys.store.FilterPolicies(strings.Join(policies, ","), "")
	return policy
}

// IsAllowed - checks given policy args is allowed to continue the Rest API.
func (sys *IAMSys) IsAllowed(args iampolicy.Args) bool {
	// If opa is configured, use OPA always.
	if globalPolicyOPA != nil {
		ok, err := globalPolicyOPA.IsAllowed(args)
		if err != nil {
			logger.LogIf(GlobalContext, err)
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
	policies, err := sys.PolicyDBGet(args.AccountName, false, args.Groups...)
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

// EnableLDAPSys - enable ldap system users type.
func (sys *IAMSys) EnableLDAPSys() {
	sys.usersSysType = LDAPUsersSysType
}

// NewIAMSys - creates new config system object.
func NewIAMSys() *IAMSys {
	return &IAMSys{
		usersSysType: MinIOUsersSysType,
		configLoaded: make(chan struct{}),
	}
}
