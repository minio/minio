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
	"strings"
	"sync"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/madmin-go"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/auth"
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
func (sys *IAMSys) LoadGroup(objAPI ObjectLayer, group string) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	return sys.store.GroupNotificationHandler(context.Background(), group)
}

// LoadPolicy - reloads a specific canned policy from backend disks or etcd.
func (sys *IAMSys) LoadPolicy(objAPI ObjectLayer, policyName string) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	return sys.store.PolicyNotificationHandler(context.Background(), policyName)
}

// LoadPolicyMapping - loads the mapped policy for a user or group
// from storage into server memory.
func (sys *IAMSys) LoadPolicyMapping(objAPI ObjectLayer, userOrGroup string, isGroup bool) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	// In case of LDAP, policy mappings are only applicable to sts users.
	userType := regUser
	if sys.usersSysType == LDAPUsersSysType {
		userType = stsUser
	}

	return sys.store.PolicyMappingNotificationHandler(context.Background(), userOrGroup, isGroup, userType)
}

// LoadUser - reloads a specific user from backend disks or etcd.
func (sys *IAMSys) LoadUser(objAPI ObjectLayer, accessKey string, userType IAMUserType) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	return sys.store.UserNotificationHandler(context.Background(), accessKey, userType)
}

// LoadServiceAccount - reloads a specific service account from backend disks or etcd.
func (sys *IAMSys) LoadServiceAccount(accessKey string) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	return sys.store.UserNotificationHandler(context.Background(), accessKey, svcUser)
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
			sys.store = &IAMStoreSys{newIAMDummyStore(sys.usersSysType)}
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

	// Hold the lock for migration only.
	txnLk := objAPI.NewNSLock(minioMetaBucket, minioConfigPrefix+"/iam.lock")

	// allocate dynamic timeout once before the loop
	iamLockTimeout := newDynamicTimeout(5*time.Second, 3*time.Second)

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	for {
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

	go sys.watch(ctx)
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
			logger.LogIf(ctx, err)
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
				logger.LogIf(ctx, err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (sys *IAMSys) loadWatchedEvent(outerCtx context.Context, event iamWatchEvent) (err error) {
	usersPrefix := strings.HasPrefix(event.keyPath, iamConfigUsersPrefix)
	groupsPrefix := strings.HasPrefix(event.keyPath, iamConfigGroupsPrefix)
	stsPrefix := strings.HasPrefix(event.keyPath, iamConfigSTSPrefix)
	svcPrefix := strings.HasPrefix(event.keyPath, iamConfigServiceAccountsPrefix)
	policyPrefix := strings.HasPrefix(event.keyPath, iamConfigPoliciesPrefix)
	policyDBUsersPrefix := strings.HasPrefix(event.keyPath, iamConfigPolicyDBUsersPrefix)
	policyDBSTSUsersPrefix := strings.HasPrefix(event.keyPath, iamConfigPolicyDBSTSUsersPrefix)
	policyDBGroupsPrefix := strings.HasPrefix(event.keyPath, iamConfigPolicyDBGroupsPrefix)

	ctx, cancel := context.WithTimeout(context.Background(), defaultContextTimeout)
	defer cancel()

	if event.isCreated {
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
	} else {
		// delete event
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
	}
	return err
}

// DeletePolicy - deletes a canned policy from backend or etcd.
func (sys *IAMSys) DeletePolicy(policyName string) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	return sys.store.DeletePolicy(context.Background(), policyName)
}

// InfoPolicy - expands the canned policy into its JSON structure.
func (sys *IAMSys) InfoPolicy(policyName string) (iampolicy.Policy, error) {
	if !sys.Initialized() {
		return iampolicy.Policy{}, errServerNotInitialized
	}

	return sys.store.GetPolicy(policyName)
}

// ListPolicies - lists all canned policies.
func (sys *IAMSys) ListPolicies(bucketName string) (map[string]iampolicy.Policy, error) {
	if !sys.Initialized() {
		return nil, errServerNotInitialized
	}

	<-sys.configLoaded

	return sys.store.ListPolicies(context.Background(), bucketName)
}

// SetPolicy - sets a new named policy.
func (sys *IAMSys) SetPolicy(policyName string, p iampolicy.Policy) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	return sys.store.SetPolicy(context.Background(), policyName, p)
}

// DeleteUser - delete user (only for long-term users not STS users).
func (sys *IAMSys) DeleteUser(accessKey string) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	return sys.store.DeleteUser(context.Background(), accessKey, regUser)
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

// SetTempUser - set temporary user credentials, these credentials have an expiry.
func (sys *IAMSys) SetTempUser(accessKey string, cred auth.Credentials, policyName string) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	if globalPolicyOPA != nil {
		// If OPA is set, we do not need to set a policy mapping.
		policyName = ""
	}

	return sys.store.SetTempUser(context.Background(), accessKey, cred, policyName)
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
func (sys *IAMSys) GetUserInfo(name string) (u madmin.UserInfo, err error) {
	if !sys.Initialized() {
		return u, errServerNotInitialized
	}

	select {
	case <-sys.configLoaded:
	default:
		sys.store.LoadUser(context.Background(), name)
	}

	return sys.store.GetUserInfo(name)
}

// SetUserStatus - sets current user status, supports disabled or enabled.
func (sys *IAMSys) SetUserStatus(accessKey string, status madmin.AccountStatus) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	if sys.usersSysType != MinIOUsersSysType {
		return errIAMActionNotAllowed
	}

	return sys.store.SetUserStatus(context.Background(), accessKey, status)
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

	var (
		cred auth.Credentials
	)

	var err error
	if len(opts.accessKey) > 0 {
		cred, err = auth.CreateNewCredentialsWithMetadata(opts.accessKey, opts.secretKey, m, globalActiveCred.SecretKey)
	} else {
		cred, err = auth.GetNewCredentialsWithMetadata(m, globalActiveCred.SecretKey)
	}
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

	return sys.store.UpdateServiceAccount(ctx, accessKey, opts)
}

// ListServiceAccounts - lists all services accounts associated to a specific user
func (sys *IAMSys) ListServiceAccounts(ctx context.Context, accessKey string) ([]auth.Credentials, error) {
	if !sys.Initialized() {
		return nil, errServerNotInitialized
	}

	<-sys.configLoaded

	return sys.store.ListServiceAccounts(ctx, accessKey)
}

// GetServiceAccount - gets information about a service account
func (sys *IAMSys) GetServiceAccount(ctx context.Context, accessKey string) (auth.Credentials, *iampolicy.Policy, error) {
	if !sys.Initialized() {
		return auth.Credentials{}, nil, errServerNotInitialized
	}

	sa, ok := sys.store.GetUser(accessKey)
	if !ok || !sa.IsServiceAccount() {
		return auth.Credentials{}, nil, errNoSuchServiceAccount
	}

	var embeddedPolicy *iampolicy.Policy

	jwtClaims, err := auth.ExtractClaims(sa.SessionToken, globalActiveCred.SecretKey)
	if err == nil {
		pt, ptok := jwtClaims.Lookup(iamPolicyClaimNameSA())
		sp, spok := jwtClaims.Lookup(iampolicy.SessionPolicyName)
		if ptok && spok && pt == "embedded-policy" {
			policyBytes, err := base64.StdEncoding.DecodeString(sp)
			if err == nil {
				p, err := iampolicy.ParseConfig(bytes.NewReader(policyBytes))
				if err == nil {
					policy := iampolicy.Policy{}.Merge(*p)
					embeddedPolicy = &policy
				}
			}
		}
	}

	// Hide secret & session keys
	sa.SecretKey = ""
	sa.SessionToken = ""

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

	jwtClaims, err := auth.ExtractClaims(sa.SessionToken, globalActiveCred.SecretKey)
	if err != nil {
		return nil, err
	}
	return jwtClaims.Map(), nil
}

// DeleteServiceAccount - delete a service account
func (sys *IAMSys) DeleteServiceAccount(ctx context.Context, accessKey string) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	sa, ok := sys.store.GetUser(accessKey)
	if !ok || !sa.IsServiceAccount() {
		return nil
	}

	return sys.store.DeleteUser(ctx, accessKey, svcUser)
}

// CreateUser - create new user credentials and policy, if user already exists
// they shall be rewritten with new inputs.
func (sys *IAMSys) CreateUser(accessKey string, uinfo madmin.UserInfo) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	if sys.usersSysType != MinIOUsersSysType {
		return errIAMActionNotAllowed
	}

	if !auth.IsAccessKeyValid(accessKey) {
		return auth.ErrInvalidAccessKeyLength
	}

	if !auth.IsSecretKeyValid(uinfo.SecretKey) {
		return auth.ErrInvalidSecretKeyLength
	}

	return sys.store.AddUser(context.Background(), accessKey, uinfo)
}

// SetUserSecretKey - sets user secret key
func (sys *IAMSys) SetUserSecretKey(accessKey string, secretKey string) error {
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

	return sys.store.UpdateUserSecretKey(context.Background(), accessKey, secretKey)
}

// purgeExpiredCredentialsForExternalSSO - validates if local credentials are still valid
// by checking remote IDP if the relevant users are still active and present.
func (sys *IAMSys) purgeExpiredCredentialsForExternalSSO(ctx context.Context) {
	parentUsers := sys.store.GetAllParentUsers()
	var expiredUsers []string
	for _, parentUser := range parentUsers {
		userid, err := parseOpenIDParentUser(parentUser)
		if err == errSkipFile {
			continue
		}
		u, err := globalOpenIDConfig.LookupUser(userid)
		if err != nil {
			logger.LogIf(GlobalContext, err)
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
	for _, parentUser := range parentUsers {
		if !globalLDAPConfig.IsLDAPUserDN(parentUser) {
			continue
		}

		allDistNames = append(allDistNames, parentUser)
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
			jwtClaims, err := auth.ExtractClaims(cred.SessionToken, globalActiveCred.SecretKey)
			if err != nil {
				// skip this cred - session token seems
				// invalid
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
func (sys *IAMSys) GetUser(accessKey string) (cred auth.Credentials, ok bool) {
	if !sys.Initialized() {
		return cred, false
	}

	fallback := false
	select {
	case <-sys.configLoaded:
	default:
		sys.store.LoadUser(context.Background(), accessKey)
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
		sys.store.LoadUser(context.Background(), accessKey)
		cred, ok = sys.store.GetUser(accessKey)
	}

	if ok && cred.IsValid() {
		if cred.IsServiceAccount() || cred.IsTemp() {
			policies, err := sys.store.PolicyDBGet(cred.AccessKey, false)
			if err != nil {
				// Reject if the policy map for user doesn't exist anymore.
				logger.LogIf(context.Background(), fmt.Errorf("'%s' user does not have a policy present", cred.ParentUser))
				return auth.Credentials{}, false
			}
			for _, group := range cred.Groups {
				ps, err := sys.store.PolicyDBGet(group, true)
				if err != nil {
					// Reject if the policy map for group doesn't exist anymore.
					logger.LogIf(context.Background(), fmt.Errorf("'%s' group does not have a policy present", group))
					return auth.Credentials{}, false
				}
				policies = append(policies, ps...)
			}
			ok = len(policies) > 0 || globalPolicyOPA != nil
		}
	}
	return cred, ok && cred.IsValid()
}

// AddUsersToGroup - adds users to a group, creating the group if
// needed. No error if user(s) already are in the group.
func (sys *IAMSys) AddUsersToGroup(group string, members []string) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	if sys.usersSysType != MinIOUsersSysType {
		return errIAMActionNotAllowed
	}

	return sys.store.AddUsersToGroup(context.Background(), group, members)
}

// RemoveUsersFromGroup - remove users from group. If no users are
// given, and the group is empty, deletes the group as well.
func (sys *IAMSys) RemoveUsersFromGroup(group string, members []string) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	if sys.usersSysType != MinIOUsersSysType {
		return errIAMActionNotAllowed
	}

	return sys.store.RemoveUsersFromGroup(context.Background(), group, members)
}

// SetGroupStatus - enable/disabled a group
func (sys *IAMSys) SetGroupStatus(group string, enabled bool) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	if sys.usersSysType != MinIOUsersSysType {
		return errIAMActionNotAllowed
	}

	return sys.store.SetGroupStatus(context.Background(), group, enabled)
}

// GetGroupDescription - builds up group description
func (sys *IAMSys) GetGroupDescription(group string) (gd madmin.GroupDesc, err error) {
	if !sys.Initialized() {
		return gd, errServerNotInitialized
	}

	return sys.store.GetGroupDescription(group)
}

// ListGroups - lists groups.
func (sys *IAMSys) ListGroups() (r []string, err error) {
	if !sys.Initialized() {
		return r, errServerNotInitialized
	}

	<-sys.configLoaded

	return sys.store.ListGroups(context.Background())
}

// PolicyDBSet - sets a policy for a user or group in the PolicyDB.
func (sys *IAMSys) PolicyDBSet(name, policy string, isGroup bool) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	// Determine user-type based on IDP mode.
	userType := regUser
	if sys.usersSysType == LDAPUsersSysType {
		userType = stsUser
	}

	return sys.store.PolicyDBSet(context.Background(), name, policy, userType, isGroup)
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

	// Check policy for this service account.
	svcPolicies, err := sys.PolicyDBGet(parentUser, false, args.Groups...)
	if err != nil {
		logger.LogIf(GlobalContext, err)
		return false
	}

	if len(svcPolicies) == 0 {
		return false
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

	policies, ok := args.GetPolicies(iamPolicyClaimNameOpenID())
	if !ok {
		// When claims are set, it should have a policy claim field.
		return false
	}

	// When claims are set, it should have policies as claim.
	if policies.IsEmpty() {
		// No policy, no access!
		return false
	}

	// If policy is available for given user, check the policy.
	mp, ok := sys.store.GetMappedPolicy(args.AccountName, false)
	if !ok {
		// No policy set for the user that we can find, no access!
		return false
	}

	if !policies.Equals(mp.policySet()) {
		// When claims has a policy, it should match the
		// policy of args.AccountName which server remembers.
		// if not reject such requests.
		return false
	}

	combinedPolicy, err := sys.store.GetPolicy(strings.Join(policies.ToSlice(), ","))
	if err == errNoSuchPolicy {
		for pname := range policies {
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
