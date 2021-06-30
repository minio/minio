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
	"strings"
	"sync"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/madmin-go"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/logger"
	iampolicy "github.com/minio/pkg/iam/policy"
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
	// IAM configuration directory.
	iamConfigPrefix = minioConfigPrefix + "/iam"

	// IAM users directory.
	iamConfigUsersPrefix = iamConfigPrefix + "/users/"

	// IAM service accounts directory.
	iamConfigServiceAccountsPrefix = iamConfigPrefix + "/service-accounts/"

	// IAM groups directory.
	iamConfigGroupsPrefix = iamConfigPrefix + "/groups/"

	// IAM policies directory.
	iamConfigPoliciesPrefix = iamConfigPrefix + "/policies/"

	// IAM sts directory.
	iamConfigSTSPrefix = iamConfigPrefix + "/sts/"

	// IAM Policy DB prefixes.
	iamConfigPolicyDBPrefix                = iamConfigPrefix + "/policydb/"
	iamConfigPolicyDBUsersPrefix           = iamConfigPolicyDBPrefix + "users/"
	iamConfigPolicyDBSTSUsersPrefix        = iamConfigPolicyDBPrefix + "sts-users/"
	iamConfigPolicyDBServiceAccountsPrefix = iamConfigPolicyDBPrefix + "service-accounts/"
	iamConfigPolicyDBGroupsPrefix          = iamConfigPolicyDBPrefix + "groups/"

	// IAM identity file which captures identity credentials.
	iamIdentityFile = "identity.json"

	// IAM policy file which provides policies for each users.
	iamPolicyFile = "policy.json"

	// IAM group members file
	iamGroupMembersFile = "members.json"

	// IAM format file
	iamFormatFile = "format.json"

	iamFormatVersion1 = 1
)

const (
	statusEnabled  = "enabled"
	statusDisabled = "disabled"
)

type iamFormat struct {
	Version int `json:"version"`
}

func newIAMFormatVersion1() iamFormat {
	return iamFormat{Version: iamFormatVersion1}
}

func getIAMFormatFilePath() string {
	return iamConfigPrefix + SlashSeparator + iamFormatFile
}

func getUserIdentityPath(user string, userType IAMUserType) string {
	var basePath string
	switch userType {
	case srvAccUser:
		basePath = iamConfigServiceAccountsPrefix
	case stsUser:
		basePath = iamConfigSTSPrefix
	default:
		basePath = iamConfigUsersPrefix
	}
	return pathJoin(basePath, user, iamIdentityFile)
}

func getGroupInfoPath(group string) string {
	return pathJoin(iamConfigGroupsPrefix, group, iamGroupMembersFile)
}

func getPolicyDocPath(name string) string {
	return pathJoin(iamConfigPoliciesPrefix, name, iamPolicyFile)
}

func getMappedPolicyPath(name string, userType IAMUserType, isGroup bool) string {
	if isGroup {
		return pathJoin(iamConfigPolicyDBGroupsPrefix, name+".json")
	}
	switch userType {
	case srvAccUser:
		return pathJoin(iamConfigPolicyDBServiceAccountsPrefix, name+".json")
	case stsUser:
		return pathJoin(iamConfigPolicyDBSTSUsersPrefix, name+".json")
	default:
		return pathJoin(iamConfigPolicyDBUsersPrefix, name+".json")
	}
}

// UserIdentity represents a user's secret key and their status
type UserIdentity struct {
	Version     int              `json:"version"`
	Credentials auth.Credentials `json:"credentials"`
}

func newUserIdentity(cred auth.Credentials) UserIdentity {
	return UserIdentity{Version: 1, Credentials: cred}
}

// GroupInfo contains info about a group
type GroupInfo struct {
	Version int      `json:"version"`
	Status  string   `json:"status"`
	Members []string `json:"members"`
}

func newGroupInfo(members []string) GroupInfo {
	return GroupInfo{Version: 1, Status: statusEnabled, Members: members}
}

// MappedPolicy represents a policy name mapped to a user or group
type MappedPolicy struct {
	Version  int    `json:"version"`
	Policies string `json:"policy"`
}

// converts a mapped policy into a slice of distinct policies
func (mp MappedPolicy) toSlice() []string {
	var policies []string
	for _, policy := range strings.Split(mp.Policies, ",") {
		policy = strings.TrimSpace(policy)
		if policy == "" {
			continue
		}
		policies = append(policies, policy)
	}
	return policies
}

func (mp MappedPolicy) policySet() set.StringSet {
	var policies []string
	for _, policy := range strings.Split(mp.Policies, ",") {
		policy = strings.TrimSpace(policy)
		if policy == "" {
			continue
		}
		policies = append(policies, policy)
	}
	return set.CreateStringSet(policies...)
}

func newMappedPolicy(policy string) MappedPolicy {
	return MappedPolicy{Version: 1, Policies: policy}
}

// IAMSys - config system.
type IAMSys struct {
	sync.Mutex

	usersSysType UsersSysType

	// map of policy names to policy definitions
	iamPolicyDocsMap map[string]iampolicy.Policy
	// map of usernames to credentials
	iamUsersMap map[string]auth.Credentials
	// map of group names to group info
	iamGroupsMap map[string]GroupInfo
	// map of user names to groups they are a member of
	iamUserGroupMemberships map[string]set.StringSet
	// map of usernames/temporary access keys to policy names
	iamUserPolicyMap map[string]MappedPolicy
	// map of group names to policy names
	iamGroupPolicyMap map[string]MappedPolicy

	// Persistence layer for IAM subsystem
	store IAMStorageAPI

	// configLoaded will be closed and remain so after first load.
	configLoaded chan struct{}
}

// IAMUserType represents a user type inside MinIO server
type IAMUserType int

const (
	regularUser IAMUserType = iota
	stsUser
	srvAccUser
)

// key options
type options struct {
	ttl int64 //expiry in seconds
}

// IAMStorageAPI defines an interface for the IAM persistence layer
type IAMStorageAPI interface {
	lock()
	unlock()

	rlock()
	runlock()

	migrateBackendFormat(context.Context) error

	loadPolicyDoc(ctx context.Context, policy string, m map[string]iampolicy.Policy) error
	loadPolicyDocs(ctx context.Context, m map[string]iampolicy.Policy) error

	loadUser(ctx context.Context, user string, userType IAMUserType, m map[string]auth.Credentials) error
	loadUsers(ctx context.Context, userType IAMUserType, m map[string]auth.Credentials) error

	loadGroup(ctx context.Context, group string, m map[string]GroupInfo) error
	loadGroups(ctx context.Context, m map[string]GroupInfo) error

	loadMappedPolicy(ctx context.Context, name string, userType IAMUserType, isGroup bool, m map[string]MappedPolicy) error
	loadMappedPolicies(ctx context.Context, userType IAMUserType, isGroup bool, m map[string]MappedPolicy) error

	loadAll(context.Context, *IAMSys) error

	saveIAMConfig(ctx context.Context, item interface{}, path string, opts ...options) error
	loadIAMConfig(ctx context.Context, item interface{}, path string) error
	deleteIAMConfig(ctx context.Context, path string) error

	savePolicyDoc(ctx context.Context, policyName string, p iampolicy.Policy) error
	saveMappedPolicy(ctx context.Context, name string, userType IAMUserType, isGroup bool, mp MappedPolicy, opts ...options) error
	saveUserIdentity(ctx context.Context, name string, userType IAMUserType, u UserIdentity, opts ...options) error
	saveGroupInfo(ctx context.Context, group string, gi GroupInfo) error

	deletePolicyDoc(ctx context.Context, policyName string) error
	deleteMappedPolicy(ctx context.Context, name string, userType IAMUserType, isGroup bool) error
	deleteUserIdentity(ctx context.Context, name string, userType IAMUserType) error
	deleteGroupInfo(ctx context.Context, name string) error

	watch(context.Context, *IAMSys)
}

// LoadGroup - loads a specific group from storage, and updates the
// memberships cache. If the specified group does not exist in
// storage, it is removed from in-memory maps as well - this
// simplifies the implementation for group removal. This is called
// only via IAM notifications.
func (sys *IAMSys) LoadGroup(objAPI ObjectLayer, group string) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	if globalEtcdClient != nil {
		// Watch APIs cover this case, so nothing to do.
		return nil
	}

	sys.store.lock()
	defer sys.store.unlock()

	err := sys.store.loadGroup(context.Background(), group, sys.iamGroupsMap)
	if err != nil && err != errNoSuchGroup {
		return err
	}

	if err == errNoSuchGroup {
		// group does not exist - so remove from memory.
		sys.removeGroupFromMembershipsMap(group)
		delete(sys.iamGroupsMap, group)
		delete(sys.iamGroupPolicyMap, group)
		return nil
	}

	gi := sys.iamGroupsMap[group]

	// Updating the group memberships cache happens in two steps:
	//
	// 1. Remove the group from each user's list of memberships.
	// 2. Add the group to each member's list of memberships.
	//
	// This ensures that regardless of members being added or
	// removed, the cache stays current.
	sys.removeGroupFromMembershipsMap(group)
	sys.updateGroupMembershipsMap(group, &gi)
	return nil
}

// LoadPolicy - reloads a specific canned policy from backend disks or etcd.
func (sys *IAMSys) LoadPolicy(objAPI ObjectLayer, policyName string) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	sys.store.lock()
	defer sys.store.unlock()

	if globalEtcdClient == nil {
		return sys.store.loadPolicyDoc(context.Background(), policyName, sys.iamPolicyDocsMap)
	}

	// When etcd is set, we use watch APIs so this code is not needed.
	return nil
}

// LoadPolicyMapping - loads the mapped policy for a user or group
// from storage into server memory.
func (sys *IAMSys) LoadPolicyMapping(objAPI ObjectLayer, userOrGroup string, isGroup bool) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	sys.store.lock()
	defer sys.store.unlock()

	if globalEtcdClient == nil {
		var err error
		userType := regularUser
		if sys.usersSysType == LDAPUsersSysType {
			userType = stsUser
		}

		if isGroup {
			err = sys.store.loadMappedPolicy(context.Background(), userOrGroup, userType, isGroup, sys.iamGroupPolicyMap)
		} else {
			err = sys.store.loadMappedPolicy(context.Background(), userOrGroup, userType, isGroup, sys.iamUserPolicyMap)
		}

		if err == errNoSuchPolicy {
			if isGroup {
				delete(sys.iamGroupPolicyMap, userOrGroup)
			} else {
				delete(sys.iamUserPolicyMap, userOrGroup)
			}
		}
		// Ignore policy not mapped error
		if err != nil && err != errNoSuchPolicy {
			return err
		}
	}
	// When etcd is set, we use watch APIs so this code is not needed.
	return nil
}

// LoadUser - reloads a specific user from backend disks or etcd.
func (sys *IAMSys) LoadUser(objAPI ObjectLayer, accessKey string, userType IAMUserType) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	sys.store.lock()
	defer sys.store.unlock()

	if globalEtcdClient == nil {
		err := sys.store.loadUser(context.Background(), accessKey, userType, sys.iamUsersMap)
		if err != nil {
			return err
		}
		err = sys.store.loadMappedPolicy(context.Background(), accessKey, userType, false, sys.iamUserPolicyMap)
		// Ignore policy not mapped error
		if err != nil && err != errNoSuchPolicy {
			return err
		}
		// We are on purpose not persisting the policy map for parent
		// user, although this is a hack, it is a good enough hack
		// at this point in time - we need to overhaul our OIDC
		// usage with service accounts with a more cleaner implementation
		//
		// This mapping is necessary to ensure that valid credentials
		// have necessary ParentUser present - this is mainly for only
		// webIdentity based STS tokens.
		cred, ok := sys.iamUsersMap[accessKey]
		if ok {
			if cred.IsTemp() && cred.ParentUser != "" && cred.ParentUser != globalActiveCred.AccessKey {
				if _, ok := sys.iamUserPolicyMap[cred.ParentUser]; !ok {
					sys.iamUserPolicyMap[cred.ParentUser] = sys.iamUserPolicyMap[accessKey]
				}
			}
		}
	}
	// When etcd is set, we use watch APIs so this code is not needed.
	return nil
}

// LoadServiceAccount - reloads a specific service account from backend disks or etcd.
func (sys *IAMSys) LoadServiceAccount(accessKey string) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	sys.store.lock()
	defer sys.store.unlock()

	if globalEtcdClient == nil {
		err := sys.store.loadUser(context.Background(), accessKey, srvAccUser, sys.iamUsersMap)
		if err != nil {
			return err
		}
	}
	// When etcd is set, we use watch APIs so this code is not needed.
	return nil
}

// Perform IAM configuration migration.
func (sys *IAMSys) doIAMConfigMigration(ctx context.Context) error {
	return sys.store.migrateBackendFormat(ctx)
}

// InitStore initializes IAM stores
func (sys *IAMSys) InitStore(objAPI ObjectLayer) {
	sys.Lock()
	defer sys.Unlock()

	if globalEtcdClient == nil {
		sys.store = newIAMObjectStore(objAPI)
	} else {
		sys.store = newIAMEtcdStore()
	}

	if globalLDAPConfig.Enabled {
		sys.EnableLDAPSys()
	}
}

// Initialized check if IAM is initialized
func (sys *IAMSys) Initialized() bool {
	if sys == nil {
		return false
	}
	sys.Lock()
	defer sys.Unlock()
	return sys.store != nil
}

// Load - loads all credentials
func (sys *IAMSys) Load(ctx context.Context, store IAMStorageAPI) error {
	iamUsersMap := make(map[string]auth.Credentials)
	iamGroupsMap := make(map[string]GroupInfo)
	iamUserPolicyMap := make(map[string]MappedPolicy)
	iamGroupPolicyMap := make(map[string]MappedPolicy)
	iamPolicyDocsMap := make(map[string]iampolicy.Policy)

	store.rlock()
	isMinIOUsersSys := sys.usersSysType == MinIOUsersSysType
	store.runlock()

	if err := store.loadPolicyDocs(ctx, iamPolicyDocsMap); err != nil {
		return err
	}

	// Sets default canned policies, if none are set.
	setDefaultCannedPolicies(iamPolicyDocsMap)

	if isMinIOUsersSys {
		if err := store.loadUsers(ctx, regularUser, iamUsersMap); err != nil {
			return err
		}
		if err := store.loadGroups(ctx, iamGroupsMap); err != nil {
			return err
		}
	}

	// load polices mapped to users
	if err := store.loadMappedPolicies(ctx, regularUser, false, iamUserPolicyMap); err != nil {
		return err
	}

	// load policies mapped to groups
	if err := store.loadMappedPolicies(ctx, regularUser, true, iamGroupPolicyMap); err != nil {
		return err
	}

	if err := store.loadUsers(ctx, srvAccUser, iamUsersMap); err != nil {
		return err
	}

	// load STS temp users
	if err := store.loadUsers(ctx, stsUser, iamUsersMap); err != nil {
		return err
	}

	// load STS policy mappings
	if err := store.loadMappedPolicies(ctx, stsUser, false, iamUserPolicyMap); err != nil {
		return err
	}

	store.lock()
	defer store.unlock()

	for k, v := range iamPolicyDocsMap {
		sys.iamPolicyDocsMap[k] = v
	}

	// Merge the new reloaded entries into global map.
	// See issue https://github.com/minio/minio/issues/9651
	// where the present list of entries on disk are not yet
	// latest, there is a small window where this can make
	// valid users invalid.
	for k, v := range iamUsersMap {
		sys.iamUsersMap[k] = v
	}

	for k, v := range iamUserPolicyMap {
		sys.iamUserPolicyMap[k] = v
	}

	// purge any expired entries which became expired now.
	var expiredEntries []string
	for k, v := range sys.iamUsersMap {
		if v.IsExpired() {
			delete(sys.iamUsersMap, k)
			delete(sys.iamUserPolicyMap, k)
			expiredEntries = append(expiredEntries, k)
			// Deleting on the disk is taken care of in the next cycle
		}
	}

	for _, v := range sys.iamUsersMap {
		if v.IsServiceAccount() {
			for _, accessKey := range expiredEntries {
				if v.ParentUser == accessKey {
					_ = store.deleteUserIdentity(ctx, v.AccessKey, srvAccUser)
					delete(sys.iamUsersMap, v.AccessKey)
				}
			}
		}
	}

	// purge any expired entries which became expired now.
	for k, v := range sys.iamUsersMap {
		if v.IsExpired() {
			delete(sys.iamUsersMap, k)
			delete(sys.iamUserPolicyMap, k)
			// Deleting on the etcd is taken care of in the next cycle
		}
	}

	for k, v := range iamGroupPolicyMap {
		sys.iamGroupPolicyMap[k] = v
	}

	for k, v := range iamGroupsMap {
		sys.iamGroupsMap[k] = v
	}

	sys.buildUserGroupMemberships()
	select {
	case <-sys.configLoaded:
	default:
		close(sys.configLoaded)
	}
	return nil
}

// Init - initializes config system by reading entries from config/iam
func (sys *IAMSys) Init(ctx context.Context, objAPI ObjectLayer) {
	// Initialize IAM store
	sys.InitStore(objAPI)

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

		if globalEtcdClient != nil {
			// ****  WARNING ****
			// Migrating to encrypted backend on etcd should happen before initialization of
			// IAM sub-system, make sure that we do not move the above codeblock elsewhere.
			if err := migrateIAMConfigsEtcdToEncrypted(retryCtx, globalEtcdClient); err != nil {
				txnLk.Unlock(lkctx.Cancel)
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
		if err := sys.store.loadAll(retryCtx, sys); err != nil {
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

	go sys.store.watch(ctx, sys)
}

// DeletePolicy - deletes a canned policy from backend or etcd.
func (sys *IAMSys) DeletePolicy(policyName string) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	if policyName == "" {
		return errInvalidArgument
	}

	sys.store.lock()
	defer sys.store.unlock()

	err := sys.store.deletePolicyDoc(context.Background(), policyName)
	if err == errNoSuchPolicy {
		// Ignore error if policy is already deleted.
		err = nil
	}

	delete(sys.iamPolicyDocsMap, policyName)

	// Delete user-policy mappings that will no longer apply
	for u, mp := range sys.iamUserPolicyMap {
		pset := mp.policySet()
		if pset.Contains(policyName) {
			cr, ok := sys.iamUsersMap[u]
			if !ok {
				// This case can happen when an temporary account
				// is deleted or expired, removed it from userPolicyMap.
				delete(sys.iamUserPolicyMap, u)
				continue
			}
			pset.Remove(policyName)
			// User is from STS if the cred are temporary
			if cr.IsTemp() {
				sys.policyDBSet(u, strings.Join(pset.ToSlice(), ","), stsUser, false)
			} else {
				sys.policyDBSet(u, strings.Join(pset.ToSlice(), ","), regularUser, false)
			}
		}
	}

	// Delete group-policy mappings that will no longer apply
	for g, mp := range sys.iamGroupPolicyMap {
		pset := mp.policySet()
		if pset.Contains(policyName) {
			pset.Remove(policyName)
			sys.policyDBSet(g, strings.Join(pset.ToSlice(), ","), regularUser, true)
		}
	}

	return err
}

// InfoPolicy - expands the canned policy into its JSON structure.
func (sys *IAMSys) InfoPolicy(policyName string) (iampolicy.Policy, error) {
	if !sys.Initialized() {
		return iampolicy.Policy{}, errServerNotInitialized
	}

	sys.store.rlock()
	defer sys.store.runlock()

	var combinedPolicy iampolicy.Policy
	for _, policy := range strings.Split(policyName, ",") {
		if policy == "" {
			continue
		}
		v, ok := sys.iamPolicyDocsMap[policy]
		if !ok {
			return iampolicy.Policy{}, errNoSuchPolicy
		}
		combinedPolicy = combinedPolicy.Merge(v)
	}
	return combinedPolicy, nil
}

// ListPolicies - lists all canned policies.
func (sys *IAMSys) ListPolicies(bucketName string) (map[string]iampolicy.Policy, error) {
	if !sys.Initialized() {
		return nil, errServerNotInitialized
	}

	<-sys.configLoaded

	sys.store.rlock()
	defer sys.store.runlock()

	policyDocsMap := make(map[string]iampolicy.Policy, len(sys.iamPolicyDocsMap))
	for k, v := range sys.iamPolicyDocsMap {
		if bucketName != "" && v.MatchResource(bucketName) {
			policyDocsMap[k] = v
		} else {
			policyDocsMap[k] = v
		}
	}

	return policyDocsMap, nil
}

// SetPolicy - sets a new name policy.
func (sys *IAMSys) SetPolicy(policyName string, p iampolicy.Policy) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	if p.IsEmpty() || policyName == "" {
		return errInvalidArgument
	}

	sys.store.lock()
	defer sys.store.unlock()

	if err := sys.store.savePolicyDoc(context.Background(), policyName, p); err != nil {
		return err
	}

	sys.iamPolicyDocsMap[policyName] = p
	return nil
}

// DeleteUser - delete user (only for long-term users not STS users).
func (sys *IAMSys) DeleteUser(accessKey string) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	if sys.usersSysType != MinIOUsersSysType {
		return errIAMActionNotAllowed
	}

	// First we remove the user from their groups.
	userInfo, getErr := sys.GetUserInfo(accessKey)
	if getErr != nil {
		return getErr
	}

	for _, group := range userInfo.MemberOf {
		removeErr := sys.RemoveUsersFromGroup(group, []string{accessKey})
		if removeErr != nil {
			return removeErr
		}
	}

	// Next we can remove the user from memory and IAM store
	sys.store.lock()
	defer sys.store.unlock()

	for _, u := range sys.iamUsersMap {
		// Delete any service accounts if any first.
		if u.IsServiceAccount() {
			if u.ParentUser == accessKey {
				_ = sys.store.deleteUserIdentity(context.Background(), u.AccessKey, srvAccUser)
				delete(sys.iamUsersMap, u.AccessKey)
			}
		}
		// Delete any associated STS users.
		if u.IsTemp() {
			if u.ParentUser == accessKey {
				_ = sys.store.deleteUserIdentity(context.Background(), u.AccessKey, stsUser)
				delete(sys.iamUsersMap, u.AccessKey)
			}
		}
	}

	// It is ok to ignore deletion error on the mapped policy
	sys.store.deleteMappedPolicy(context.Background(), accessKey, regularUser, false)
	err := sys.store.deleteUserIdentity(context.Background(), accessKey, regularUser)
	if err == errNoSuchUser {
		// ignore if user is already deleted.
		err = nil
	}

	delete(sys.iamUsersMap, accessKey)
	delete(sys.iamUserPolicyMap, accessKey)

	return err
}

// CurrentPolicies - returns comma separated policy string, from
// an input policy after validating if there are any current
// policies which exist on MinIO corresponding to the input.
func (sys *IAMSys) CurrentPolicies(policyName string) string {
	if !sys.Initialized() {
		return ""
	}

	sys.store.rlock()
	defer sys.store.runlock()

	var policies []string
	mp := newMappedPolicy(policyName)
	for _, policy := range mp.toSlice() {
		_, found := sys.iamPolicyDocsMap[policy]
		if found {
			policies = append(policies, policy)
		}
	}
	return strings.Join(policies, ",")
}

// SetTempUser - set temporary user credentials, these credentials have an expiry.
func (sys *IAMSys) SetTempUser(accessKey string, cred auth.Credentials, policyName string) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	ttl := int64(cred.Expiration.Sub(UTCNow()).Seconds())

	// If OPA is not set we honor any policy claims for this
	// temporary user which match with pre-configured canned
	// policies for this server.
	if globalPolicyOPA == nil && policyName != "" {
		mp := newMappedPolicy(policyName)
		combinedPolicy := sys.GetCombinedPolicy(mp.toSlice()...)

		if combinedPolicy.IsEmpty() {
			return fmt.Errorf("specified policy %s, not found %w", policyName, errNoSuchPolicy)
		}

		sys.store.lock()
		defer sys.store.unlock()

		if err := sys.store.saveMappedPolicy(context.Background(), accessKey, stsUser, false, mp, options{ttl: ttl}); err != nil {
			return err
		}

		sys.iamUserPolicyMap[accessKey] = mp

		// We are on purpose not persisting the policy map for parent
		// user, although this is a hack, it is a good enough hack
		// at this point in time - we need to overhaul our OIDC
		// usage with service accounts with a more cleaner implementation
		//
		// This mapping is necessary to ensure that valid credentials
		// have necessary ParentUser present - this is mainly for only
		// webIdentity based STS tokens.
		if cred.IsTemp() && cred.ParentUser != "" && cred.ParentUser != globalActiveCred.AccessKey {
			if _, ok := sys.iamUserPolicyMap[cred.ParentUser]; !ok {
				sys.iamUserPolicyMap[cred.ParentUser] = mp
			}
		}
	} else {
		sys.store.lock()
		defer sys.store.unlock()
	}

	u := newUserIdentity(cred)
	if err := sys.store.saveUserIdentity(context.Background(), accessKey, stsUser, u, options{ttl: ttl}); err != nil {
		return err
	}

	sys.iamUsersMap[accessKey] = cred
	return nil
}

// ListBucketUsers - list all users who can access this 'bucket'
func (sys *IAMSys) ListBucketUsers(bucket string) (map[string]madmin.UserInfo, error) {
	if bucket == "" {
		return nil, errInvalidArgument
	}

	sys.store.rlock()
	defer sys.store.runlock()

	var users = make(map[string]madmin.UserInfo)

	for k, v := range sys.iamUsersMap {
		if v.IsTemp() || v.IsServiceAccount() {
			continue
		}
		var policies []string
		mp, ok := sys.iamUserPolicyMap[k]
		if ok {
			policies = append(policies, mp.toSlice()...)
			for _, group := range sys.iamUserGroupMemberships[k].ToSlice() {
				if nmp, ok := sys.iamGroupPolicyMap[group]; ok {
					policies = append(policies, nmp.toSlice()...)
				}
			}
		}
		var matchesPolices []string
		for _, p := range policies {
			if sys.iamPolicyDocsMap[p].MatchResource(bucket) {
				matchesPolices = append(matchesPolices, p)
			}
		}
		if len(matchesPolices) > 0 {
			users[k] = madmin.UserInfo{
				PolicyName: strings.Join(matchesPolices, ","),
				Status: func() madmin.AccountStatus {
					if v.IsValid() {
						return madmin.AccountEnabled
					}
					return madmin.AccountDisabled
				}(),
				MemberOf: sys.iamUserGroupMemberships[k].ToSlice(),
			}
		}
	}

	return users, nil
}

// ListUsers - list all users.
func (sys *IAMSys) ListUsers() (map[string]madmin.UserInfo, error) {
	if !sys.Initialized() {
		return nil, errServerNotInitialized
	}

	<-sys.configLoaded

	sys.store.rlock()
	defer sys.store.runlock()

	var users = make(map[string]madmin.UserInfo)

	for k, v := range sys.iamUsersMap {
		if !v.IsTemp() && !v.IsServiceAccount() {
			users[k] = madmin.UserInfo{
				PolicyName: sys.iamUserPolicyMap[k].Policies,
				Status: func() madmin.AccountStatus {
					if v.IsValid() {
						return madmin.AccountEnabled
					}
					return madmin.AccountDisabled
				}(),
				MemberOf: sys.iamUserGroupMemberships[k].ToSlice(),
			}
		}
	}

	if sys.usersSysType == LDAPUsersSysType {
		for k, v := range sys.iamUserPolicyMap {
			users[k] = madmin.UserInfo{
				PolicyName: v.Policies,
				Status:     madmin.AccountEnabled,
			}
		}
	}

	return users, nil
}

// IsTempUser - returns if given key is a temporary user.
func (sys *IAMSys) IsTempUser(name string) (bool, string, error) {
	if !sys.Initialized() {
		return false, "", errServerNotInitialized
	}

	sys.store.rlock()
	defer sys.store.runlock()

	cred, found := sys.iamUsersMap[name]
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

	sys.store.rlock()
	defer sys.store.runlock()

	cred, found := sys.iamUsersMap[name]
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
		sys.loadUserFromStore(name)
	}

	if sys.usersSysType != MinIOUsersSysType {
		sys.store.rlock()
		// If the user has a mapped policy or is a member of a group, we
		// return that info. Otherwise we return error.
		var groups []string
		for _, v := range sys.iamUsersMap {
			if v.ParentUser == name {
				groups = v.Groups
				break
			}
		}
		mappedPolicy, ok := sys.iamUserPolicyMap[name]
		sys.store.runlock()
		if !ok {
			return u, errNoSuchUser
		}
		return madmin.UserInfo{
			PolicyName: mappedPolicy.Policies,
			MemberOf:   groups,
		}, nil
	}

	sys.store.rlock()
	defer sys.store.runlock()

	cred, found := sys.iamUsersMap[name]
	if !found {
		return u, errNoSuchUser
	}

	if cred.IsTemp() || cred.IsServiceAccount() {
		return u, errIAMActionNotAllowed
	}

	return madmin.UserInfo{
		PolicyName: sys.iamUserPolicyMap[name].Policies,
		Status: func() madmin.AccountStatus {
			if cred.IsValid() {
				return madmin.AccountEnabled
			}
			return madmin.AccountDisabled
		}(),
		MemberOf: sys.iamUserGroupMemberships[name].ToSlice(),
	}, nil

}

// SetUserStatus - sets current user status, supports disabled or enabled.
func (sys *IAMSys) SetUserStatus(accessKey string, status madmin.AccountStatus) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	if sys.usersSysType != MinIOUsersSysType {
		return errIAMActionNotAllowed
	}

	if status != madmin.AccountEnabled && status != madmin.AccountDisabled {
		return errInvalidArgument
	}

	sys.store.lock()
	defer sys.store.unlock()

	cred, ok := sys.iamUsersMap[accessKey]
	if !ok {
		return errNoSuchUser
	}

	if cred.IsTemp() || cred.IsServiceAccount() {
		return errIAMActionNotAllowed
	}

	uinfo := newUserIdentity(auth.Credentials{
		AccessKey: accessKey,
		SecretKey: cred.SecretKey,
		Status: func() string {
			if status == madmin.AccountEnabled {
				return auth.AccountOn
			}
			return auth.AccountOff
		}(),
	})

	if err := sys.store.saveUserIdentity(context.Background(), accessKey, regularUser, uinfo); err != nil {
		return err
	}

	sys.iamUsersMap[accessKey] = uinfo.Credentials
	return nil
}

type newServiceAccountOpts struct {
	sessionPolicy *iampolicy.Policy
	accessKey     string
	secretKey     string
}

// NewServiceAccount - create a new service account
func (sys *IAMSys) NewServiceAccount(ctx context.Context, parentUser string, groups []string, opts newServiceAccountOpts) (auth.Credentials, error) {
	if !sys.Initialized() {
		return auth.Credentials{}, errServerNotInitialized
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

	sys.store.lock()
	defer sys.store.unlock()

	cr, found := sys.iamUsersMap[parentUser]
	// Disallow service accounts to further create more service accounts.
	if found && cr.IsServiceAccount() {
		return auth.Credentials{}, errIAMActionNotAllowed
	}

	policies, err := sys.policyDBGet(parentUser, false)
	if err != nil {
		return auth.Credentials{}, err
	}
	for _, group := range groups {
		gpolicies, err := sys.policyDBGet(group, true)
		if err != nil && err != errNoSuchGroup {
			return auth.Credentials{}, err
		}
		policies = append(policies, gpolicies...)
	}
	if len(policies) == 0 {
		return auth.Credentials{}, errNoSuchUser
	}

	m := make(map[string]interface{})
	m[parentClaim] = parentUser

	if len(policyBuf) > 0 {
		m[iampolicy.SessionPolicyName] = base64.StdEncoding.EncodeToString(policyBuf)
		m[iamPolicyClaimNameSA()] = "embedded-policy"
	} else {
		m[iamPolicyClaimNameSA()] = "inherited-policy"
	}

	var (
		cred auth.Credentials
	)

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

	u := newUserIdentity(cred)

	if err := sys.store.saveUserIdentity(context.Background(), u.Credentials.AccessKey, srvAccUser, u); err != nil {
		return auth.Credentials{}, err
	}

	sys.iamUsersMap[u.Credentials.AccessKey] = u.Credentials

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

	sys.store.lock()
	defer sys.store.unlock()

	cr, ok := sys.iamUsersMap[accessKey]
	if !ok || !cr.IsServiceAccount() {
		return errNoSuchServiceAccount
	}

	if opts.secretKey != "" {
		if !auth.IsSecretKeyValid(opts.secretKey) {
			return auth.ErrInvalidSecretKeyLength
		}
		cr.SecretKey = opts.secretKey
	}

	switch opts.status {
	// The caller did not ask to update status account, do nothing
	case "":
	// Update account status
	case auth.AccountOn, auth.AccountOff:
		cr.Status = opts.status
	default:
		return errors.New("unknown account status value")
	}

	if opts.sessionPolicy != nil {
		m := make(map[string]interface{})
		err := opts.sessionPolicy.Validate()
		if err != nil {
			return err
		}
		policyBuf, err := json.Marshal(opts.sessionPolicy)
		if err != nil {
			return err
		}
		if len(policyBuf) > 16*humanize.KiByte {
			return fmt.Errorf("Session policy should not exceed 16 KiB characters")
		}

		m[iampolicy.SessionPolicyName] = base64.StdEncoding.EncodeToString(policyBuf)
		m[iamPolicyClaimNameSA()] = "embedded-policy"
		m[parentClaim] = cr.ParentUser
		cr.SessionToken, err = auth.JWTSignWithAccessKey(accessKey, m, globalActiveCred.SecretKey)
		if err != nil {
			return err
		}
	}

	u := newUserIdentity(cr)
	if err := sys.store.saveUserIdentity(context.Background(), u.Credentials.AccessKey, srvAccUser, u); err != nil {
		return err
	}

	sys.iamUsersMap[u.Credentials.AccessKey] = u.Credentials

	return nil
}

// ListServiceAccounts - lists all services accounts associated to a specific user
func (sys *IAMSys) ListServiceAccounts(ctx context.Context, accessKey string) ([]auth.Credentials, error) {
	if !sys.Initialized() {
		return nil, errServerNotInitialized
	}

	<-sys.configLoaded

	sys.store.rlock()
	defer sys.store.runlock()

	var serviceAccounts []auth.Credentials
	for _, v := range sys.iamUsersMap {
		if v.IsServiceAccount() && v.ParentUser == accessKey {
			// Hide secret key & session key here
			v.SecretKey = ""
			v.SessionToken = ""
			serviceAccounts = append(serviceAccounts, v)
		}
	}

	return serviceAccounts, nil
}

// GetServiceAccount - gets information about a service account
func (sys *IAMSys) GetServiceAccount(ctx context.Context, accessKey string) (auth.Credentials, *iampolicy.Policy, error) {
	if !sys.Initialized() {
		return auth.Credentials{}, nil, errServerNotInitialized
	}

	sys.store.rlock()
	defer sys.store.runlock()

	sa, ok := sys.iamUsersMap[accessKey]
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

// DeleteServiceAccount - delete a service account
func (sys *IAMSys) DeleteServiceAccount(ctx context.Context, accessKey string) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	sys.store.lock()
	defer sys.store.unlock()

	sa, ok := sys.iamUsersMap[accessKey]
	if !ok || !sa.IsServiceAccount() {
		return nil
	}

	// It is ok to ignore deletion error on the mapped policy
	err := sys.store.deleteUserIdentity(context.Background(), accessKey, srvAccUser)
	if err != nil {
		// ignore if user is already deleted.
		if err == errNoSuchUser {
			return nil
		}
		return err
	}

	delete(sys.iamUsersMap, accessKey)
	return nil
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

	sys.store.lock()
	defer sys.store.unlock()

	cr, ok := sys.iamUsersMap[accessKey]
	if cr.IsTemp() && ok {
		return errIAMActionNotAllowed
	}

	u := newUserIdentity(auth.Credentials{
		AccessKey: accessKey,
		SecretKey: uinfo.SecretKey,
		Status: func() string {
			if uinfo.Status == madmin.AccountEnabled {
				return auth.AccountOn
			}
			return auth.AccountOff
		}(),
	})

	if err := sys.store.saveUserIdentity(context.Background(), accessKey, regularUser, u); err != nil {
		return err
	}

	sys.iamUsersMap[accessKey] = u.Credentials

	// Set policy if specified.
	if uinfo.PolicyName != "" {
		return sys.policyDBSet(accessKey, uinfo.PolicyName, regularUser, false)
	}
	return nil
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

	sys.store.lock()
	defer sys.store.unlock()

	cred, ok := sys.iamUsersMap[accessKey]
	if !ok {
		return errNoSuchUser
	}

	cred.SecretKey = secretKey
	u := newUserIdentity(cred)
	if err := sys.store.saveUserIdentity(context.Background(), accessKey, regularUser, u); err != nil {
		return err
	}

	sys.iamUsersMap[accessKey] = cred
	return nil
}

func (sys *IAMSys) loadUserFromStore(accessKey string) {
	sys.store.lock()
	// If user is already found proceed.
	if _, found := sys.iamUsersMap[accessKey]; !found {
		sys.store.loadUser(context.Background(), accessKey, regularUser, sys.iamUsersMap)
		if _, found = sys.iamUsersMap[accessKey]; found {
			// found user, load its mapped policies
			sys.store.loadMappedPolicy(context.Background(), accessKey, regularUser, false, sys.iamUserPolicyMap)
		} else {
			sys.store.loadUser(context.Background(), accessKey, srvAccUser, sys.iamUsersMap)
			if svc, found := sys.iamUsersMap[accessKey]; found {
				// Found service account, load its parent user and its mapped policies.
				if sys.usersSysType == MinIOUsersSysType {
					sys.store.loadUser(context.Background(), svc.ParentUser, regularUser, sys.iamUsersMap)
				}
				sys.store.loadMappedPolicy(context.Background(), svc.ParentUser, regularUser, false, sys.iamUserPolicyMap)
			} else {
				// None found fall back to STS users.
				sys.store.loadUser(context.Background(), accessKey, stsUser, sys.iamUsersMap)
				if _, found = sys.iamUsersMap[accessKey]; found {
					// STS user found, load its mapped policy.
					sys.store.loadMappedPolicy(context.Background(), accessKey, stsUser, false, sys.iamUserPolicyMap)
				}
			}
		}
	}

	// Load associated policies if any.
	for _, policy := range sys.iamUserPolicyMap[accessKey].toSlice() {
		if _, found := sys.iamPolicyDocsMap[policy]; !found {
			sys.store.loadPolicyDoc(context.Background(), policy, sys.iamPolicyDocsMap)
		}
	}

	sys.buildUserGroupMemberships()
	sys.store.unlock()
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
		sys.loadUserFromStore(accessKey)
		fallback = true
	}

	sys.store.rlock()
	cred, ok = sys.iamUsersMap[accessKey]
	if !ok && !fallback {
		sys.store.runlock()
		// accessKey not found, also
		// IAM store is not in fallback mode
		// we can try to reload again from
		// the IAM store and see if credential
		// exists now. If it doesn't proceed to
		// fail.
		sys.loadUserFromStore(accessKey)

		sys.store.rlock()
		cred, ok = sys.iamUsersMap[accessKey]
	}
	defer sys.store.runlock()

	if ok && cred.IsValid() {
		if cred.IsServiceAccount() || cred.IsTemp() {
			policies, err := sys.policyDBGet(cred.ParentUser, false)
			if err != nil {
				// Reject if the policy map for user doesn't exist anymore.
				logger.LogIf(context.Background(), fmt.Errorf("'%s' user does not have a policy present", cred.ParentUser))
				return auth.Credentials{}, false
			}
			for _, group := range cred.Groups {
				ps, err := sys.policyDBGet(group, true)
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

	if group == "" {
		return errInvalidArgument
	}

	if sys.usersSysType != MinIOUsersSysType {
		return errIAMActionNotAllowed
	}

	sys.store.lock()
	defer sys.store.unlock()

	// Validate that all members exist.
	for _, member := range members {
		cr, ok := sys.iamUsersMap[member]
		if !ok {
			return errNoSuchUser
		}
		if cr.IsTemp() {
			return errIAMActionNotAllowed
		}
	}

	gi, ok := sys.iamGroupsMap[group]
	if !ok {
		// Set group as enabled by default when it doesn't
		// exist.
		gi = newGroupInfo(members)
	} else {
		mergedMembers := append(gi.Members, members...)
		uniqMembers := set.CreateStringSet(mergedMembers...).ToSlice()
		gi.Members = uniqMembers
	}

	if err := sys.store.saveGroupInfo(context.Background(), group, gi); err != nil {
		return err
	}

	sys.iamGroupsMap[group] = gi

	// update user-group membership map
	for _, member := range members {
		gset := sys.iamUserGroupMemberships[member]
		if gset == nil {
			gset = set.CreateStringSet(group)
		} else {
			gset.Add(group)
		}
		sys.iamUserGroupMemberships[member] = gset
	}

	return nil
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

	if group == "" {
		return errInvalidArgument
	}

	sys.store.lock()
	defer sys.store.unlock()

	// Validate that all members exist.
	for _, member := range members {
		cr, ok := sys.iamUsersMap[member]
		if !ok {
			return errNoSuchUser
		}
		if cr.IsTemp() {
			return errIAMActionNotAllowed
		}
	}

	gi, ok := sys.iamGroupsMap[group]
	if !ok {
		return errNoSuchGroup
	}

	// Check if attempting to delete a non-empty group.
	if len(members) == 0 && len(gi.Members) != 0 {
		return errGroupNotEmpty
	}

	if len(members) == 0 {
		// len(gi.Members) == 0 here.

		// Remove the group from storage. First delete the
		// mapped policy. No-mapped-policy case is ignored.
		if err := sys.store.deleteMappedPolicy(context.Background(), group, regularUser, true); err != nil && err != errNoSuchPolicy {
			return err
		}
		if err := sys.store.deleteGroupInfo(context.Background(), group); err != nil && err != errNoSuchGroup {
			return err
		}

		// Delete from server memory
		delete(sys.iamGroupsMap, group)
		delete(sys.iamGroupPolicyMap, group)
		return nil
	}

	// Only removing members.
	s := set.CreateStringSet(gi.Members...)
	d := set.CreateStringSet(members...)
	gi.Members = s.Difference(d).ToSlice()

	err := sys.store.saveGroupInfo(context.Background(), group, gi)
	if err != nil {
		return err
	}
	sys.iamGroupsMap[group] = gi

	// update user-group membership map
	for _, member := range members {
		gset := sys.iamUserGroupMemberships[member]
		if gset == nil {
			continue
		}
		gset.Remove(group)
		sys.iamUserGroupMemberships[member] = gset
	}

	return nil
}

// SetGroupStatus - enable/disabled a group
func (sys *IAMSys) SetGroupStatus(group string, enabled bool) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	if sys.usersSysType != MinIOUsersSysType {
		return errIAMActionNotAllowed
	}

	sys.store.lock()
	defer sys.store.unlock()

	if group == "" {
		return errInvalidArgument
	}

	gi, ok := sys.iamGroupsMap[group]
	if !ok {
		return errNoSuchGroup
	}

	if enabled {
		gi.Status = statusEnabled
	} else {
		gi.Status = statusDisabled
	}

	if err := sys.store.saveGroupInfo(context.Background(), group, gi); err != nil {
		return err
	}
	sys.iamGroupsMap[group] = gi
	return nil
}

// GetGroupDescription - builds up group description
func (sys *IAMSys) GetGroupDescription(group string) (gd madmin.GroupDesc, err error) {
	if !sys.Initialized() {
		return gd, errServerNotInitialized
	}

	ps, err := sys.PolicyDBGet(group, true)
	if err != nil {
		return gd, err
	}

	policy := strings.Join(ps, ",")

	if sys.usersSysType != MinIOUsersSysType {
		return madmin.GroupDesc{
			Name:   group,
			Policy: policy,
		}, nil
	}

	sys.store.rlock()
	defer sys.store.runlock()

	gi, ok := sys.iamGroupsMap[group]
	if !ok {
		return gd, errNoSuchGroup
	}

	return madmin.GroupDesc{
		Name:    group,
		Status:  gi.Status,
		Members: gi.Members,
		Policy:  policy,
	}, nil
}

// ListGroups - lists groups.
func (sys *IAMSys) ListGroups() (r []string, err error) {
	if !sys.Initialized() {
		return r, errServerNotInitialized
	}

	<-sys.configLoaded

	sys.store.rlock()
	defer sys.store.runlock()

	r = make([]string, 0, len(sys.iamGroupsMap))
	for k := range sys.iamGroupsMap {
		r = append(r, k)
	}

	if sys.usersSysType == LDAPUsersSysType {
		for k := range sys.iamGroupPolicyMap {
			r = append(r, k)
		}
	}

	return r, nil
}

// PolicyDBSet - sets a policy for a user or group in the PolicyDB.
func (sys *IAMSys) PolicyDBSet(name, policy string, isGroup bool) error {
	if !sys.Initialized() {
		return errServerNotInitialized
	}

	sys.store.lock()
	defer sys.store.unlock()

	if sys.usersSysType == LDAPUsersSysType {
		return sys.policyDBSet(name, policy, stsUser, isGroup)
	}

	return sys.policyDBSet(name, policy, regularUser, isGroup)
}

// policyDBSet - sets a policy for user in the policy db. Assumes that caller
// has sys.Lock(). If policy == "", then policy mapping is removed.
func (sys *IAMSys) policyDBSet(name, policyName string, userType IAMUserType, isGroup bool) error {
	if name == "" {
		return errInvalidArgument
	}

	if sys.usersSysType == MinIOUsersSysType {
		if !isGroup {
			if _, ok := sys.iamUsersMap[name]; !ok {
				return errNoSuchUser
			}
		} else {
			if _, ok := sys.iamGroupsMap[name]; !ok {
				return errNoSuchGroup
			}
		}
	}

	// Handle policy mapping removal
	if policyName == "" {
		if sys.usersSysType == LDAPUsersSysType {
			// Add a fallback removal towards previous content that may come back
			// as a ghost user due to lack of delete, this change occurred
			// introduced in PR #11840
			sys.store.deleteMappedPolicy(context.Background(), name, regularUser, false)
		}
		err := sys.store.deleteMappedPolicy(context.Background(), name, userType, isGroup)
		if err != nil && err != errNoSuchPolicy {
			return err
		}
		if !isGroup {
			delete(sys.iamUserPolicyMap, name)
		} else {
			delete(sys.iamGroupPolicyMap, name)
		}
		return nil
	}

	mp := newMappedPolicy(policyName)
	for _, policy := range mp.toSlice() {
		if _, found := sys.iamPolicyDocsMap[policy]; !found {
			logger.LogIf(GlobalContext, fmt.Errorf("%w: (%s)", errNoSuchPolicy, policy))
			return errNoSuchPolicy
		}
	}

	// Handle policy mapping set/update
	if err := sys.store.saveMappedPolicy(context.Background(), name, userType, isGroup, mp); err != nil {
		return err
	}
	if !isGroup {
		sys.iamUserPolicyMap[name] = mp
	} else {
		sys.iamGroupPolicyMap[name] = mp
	}
	return nil
}

// PolicyDBGet - gets policy set on a user or group. If a list of groups is
// given, policies associated with them are included as well.
func (sys *IAMSys) PolicyDBGet(name string, isGroup bool, groups ...string) ([]string, error) {
	if !sys.Initialized() {
		return nil, errServerNotInitialized
	}

	if name == "" {
		return nil, errInvalidArgument
	}

	sys.store.rlock()
	defer sys.store.runlock()

	policies, err := sys.policyDBGet(name, isGroup)
	if err != nil {
		return nil, err
	}

	if !isGroup {
		for _, group := range groups {
			ps, err := sys.policyDBGet(group, true)
			if err != nil {
				return nil, err
			}
			policies = append(policies, ps...)
		}
	}

	return policies, nil
}

// This call assumes that caller has the sys.RLock().
//
// If a group is passed, it returns policies associated with the group.
//
// If a user is passed, it returns policies of the user along with any groups
// that the server knows the user is a member of.
//
// In LDAP users mode, the server does not store any group membership
// information in IAM (i.e sys.iam*Map) - this info is stored only in the STS
// generated credentials. Thus we skip looking up group memberships, user map,
// and group map and check the appropriate policy maps directly.
func (sys *IAMSys) policyDBGet(name string, isGroup bool) (policies []string, err error) {
	if isGroup {
		if sys.usersSysType == MinIOUsersSysType {
			g, ok := sys.iamGroupsMap[name]
			if !ok {
				return nil, errNoSuchGroup
			}

			// Group is disabled, so we return no policy - this
			// ensures the request is denied.
			if g.Status == statusDisabled {
				return nil, nil
			}
		}

		return sys.iamGroupPolicyMap[name].toSlice(), nil
	}

	if name == globalActiveCred.AccessKey {
		return []string{"consoleAdmin"}, nil
	}

	// When looking for a user's policies, we also check if the user
	// and the groups they are member of are enabled.
	var parentName string
	u, ok := sys.iamUsersMap[name]
	if ok {
		if !u.IsValid() {
			return nil, nil
		}
		parentName = u.ParentUser
	}

	mp, ok := sys.iamUserPolicyMap[name]
	if !ok {
		if parentName != "" {
			mp = sys.iamUserPolicyMap[parentName]
		}
	}

	// returned policy could be empty
	policies = append(policies, mp.toSlice()...)

	for _, group := range sys.iamUserGroupMemberships[name].ToSlice() {
		// Skip missing or disabled groups
		gi, ok := sys.iamGroupsMap[group]
		if !ok || gi.Status == statusDisabled {
			continue
		}

		policies = append(policies, sys.iamGroupPolicyMap[group].toSlice()...)
	}

	return policies, nil
}

// IsAllowedServiceAccount - checks if the given service account is allowed to perform
// actions. The permission of the parent user is checked first
func (sys *IAMSys) IsAllowedServiceAccount(args iampolicy.Args, parent string) bool {
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
		if parentInClaim != parent {
			return false
		}
	} else {
		// This is needed so a malicious user cannot
		// use a leaked session key of another user
		// to widen its privileges.
		return false
	}

	// Check policy for this service account.
	svcPolicies, err := sys.PolicyDBGet(parent, false, args.Groups...)
	if err != nil {
		logger.LogIf(GlobalContext, err)
		return false
	}

	if len(svcPolicies) == 0 {
		return false
	}

	var availablePolicies []iampolicy.Policy

	// Policies were found, evaluate all of them.
	sys.store.rlock()
	for _, pname := range svcPolicies {
		p, found := sys.iamPolicyDocsMap[pname]
		if found {
			availablePolicies = append(availablePolicies, p)
		}
	}
	sys.store.runlock()

	if len(availablePolicies) == 0 {
		return false
	}

	combinedPolicy := availablePolicies[0]
	for i := 1; i < len(availablePolicies); i++ {
		combinedPolicy.Statements = append(combinedPolicy.Statements,
			availablePolicies[i].Statements...)
	}

	parentArgs := args
	parentArgs.AccountName = parent

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

	// Policy without Version string value reject it.
	if subPolicy.Version == "" {
		return false
	}

	return combinedPolicy.IsAllowed(parentArgs) && subPolicy.IsAllowed(parentArgs)
}

// IsAllowedLDAPSTS - checks for LDAP specific claims and values
func (sys *IAMSys) IsAllowedLDAPSTS(args iampolicy.Args, parentUser string) bool {
	parentInClaimIface, ok := args.Claims[ldapUser]
	if ok {
		parentInClaim, ok := parentInClaimIface.(string)
		if !ok {
			// ldap parentInClaim name is not a string reject it.
			return false
		}

		if parentInClaim != parentUser {
			// ldap claim has been modified maliciously reject it.
			return false
		}
	} else {
		// no ldap parentInClaim claim present reject it.
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

	var availablePolicies []iampolicy.Policy

	// Policies were found, evaluate all of them.
	sys.store.rlock()
	for _, pname := range ldapPolicies {
		p, found := sys.iamPolicyDocsMap[pname]
		if found {
			availablePolicies = append(availablePolicies, p)
		}
	}
	sys.store.runlock()

	if len(availablePolicies) == 0 {
		return false
	}

	combinedPolicy := availablePolicies[0]
	for i := 1; i < len(availablePolicies); i++ {
		combinedPolicy.Statements =
			append(combinedPolicy.Statements,
				availablePolicies[i].Statements...)
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

	sys.store.rlock()
	defer sys.store.runlock()

	// If policy is available for given user, check the policy.
	mp, ok := sys.iamUserPolicyMap[args.AccountName]
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

	var availablePolicies []iampolicy.Policy
	for pname := range policies {
		p, found := sys.iamPolicyDocsMap[pname]
		if !found {
			// all policies presented in the claim should exist
			logger.LogIf(GlobalContext, fmt.Errorf("expected policy (%s) missing from the JWT claim %s, rejecting the request", pname, iamPolicyClaimNameOpenID()))
			return false
		}
		availablePolicies = append(availablePolicies, p)
	}

	combinedPolicy := availablePolicies[0]
	for i := 1; i < len(availablePolicies); i++ {
		combinedPolicy.Statements = append(combinedPolicy.Statements,
			availablePolicies[i].Statements...)
	}

	// Now check if we have a sessionPolicy.
	spolicy, ok := args.Claims[iampolicy.SessionPolicyName]
	if ok {
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

		// Policy without Version string value reject it.
		if subPolicy.Version == "" {
			return false
		}

		// Sub policy is set and valid.
		return combinedPolicy.IsAllowed(args) && subPolicy.IsAllowed(args)
	}

	// Sub policy not set, this is most common since subPolicy
	// is optional, use the inherited policies.
	return combinedPolicy.IsAllowed(args)
}

// GetCombinedPolicy returns a combined policy combining all policies
func (sys *IAMSys) GetCombinedPolicy(policies ...string) iampolicy.Policy {
	// Policies were found, evaluate all of them.
	sys.store.rlock()
	defer sys.store.runlock()

	var availablePolicies []iampolicy.Policy
	for _, pname := range policies {
		p, found := sys.iamPolicyDocsMap[pname]
		if found {
			availablePolicies = append(availablePolicies, p)
		}
	}

	if len(availablePolicies) == 0 {
		return iampolicy.Policy{}
	}

	combinedPolicy := availablePolicies[0]
	for i := 1; i < len(availablePolicies); i++ {
		combinedPolicy.Statements = append(combinedPolicy.Statements,
			availablePolicies[i].Statements...)
	}

	return combinedPolicy
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

// Set default canned policies only if not already overridden by users.
func setDefaultCannedPolicies(policies map[string]iampolicy.Policy) {
	_, ok := policies["writeonly"]
	if !ok {
		policies["writeonly"] = iampolicy.WriteOnly
	}
	_, ok = policies["readonly"]
	if !ok {
		policies["readonly"] = iampolicy.ReadOnly
	}
	_, ok = policies["readwrite"]
	if !ok {
		policies["readwrite"] = iampolicy.ReadWrite
	}
	_, ok = policies["diagnostics"]
	if !ok {
		policies["diagnostics"] = iampolicy.AdminDiagnostics
	}
	_, ok = policies["consoleAdmin"]
	if !ok {
		policies["consoleAdmin"] = iampolicy.Admin
	}
}

// buildUserGroupMemberships - builds the memberships map. IMPORTANT:
// Assumes that sys.Lock is held by caller.
func (sys *IAMSys) buildUserGroupMemberships() {
	for group, gi := range sys.iamGroupsMap {
		sys.updateGroupMembershipsMap(group, &gi)
	}
}

// updateGroupMembershipsMap - updates the memberships map for a
// group. IMPORTANT: Assumes sys.Lock() is held by caller.
func (sys *IAMSys) updateGroupMembershipsMap(group string, gi *GroupInfo) {
	if gi == nil {
		return
	}
	for _, member := range gi.Members {
		v := sys.iamUserGroupMemberships[member]
		if v == nil {
			v = set.CreateStringSet(group)
		} else {
			v.Add(group)
		}
		sys.iamUserGroupMemberships[member] = v
	}
}

// removeGroupFromMembershipsMap - removes the group from every member
// in the cache. IMPORTANT: Assumes sys.Lock() is held by caller.
func (sys *IAMSys) removeGroupFromMembershipsMap(group string) {
	for member, groups := range sys.iamUserGroupMemberships {
		if !groups.Contains(group) {
			continue
		}
		groups.Remove(group)
		sys.iamUserGroupMemberships[member] = groups
	}
}

// EnableLDAPSys - enable ldap system users type.
func (sys *IAMSys) EnableLDAPSys() {
	sys.usersSysType = LDAPUsersSysType
}

// NewIAMSys - creates new config system object.
func NewIAMSys() *IAMSys {
	return &IAMSys{
		usersSysType:            MinIOUsersSysType,
		iamUsersMap:             make(map[string]auth.Credentials),
		iamPolicyDocsMap:        make(map[string]iampolicy.Policy),
		iamUserPolicyMap:        make(map[string]MappedPolicy),
		iamGroupPolicyMap:       make(map[string]MappedPolicy),
		iamGroupsMap:            make(map[string]GroupInfo),
		iamUserGroupMemberships: make(map[string]set.StringSet),
		configLoaded:            make(chan struct{}),
	}
}
