/*
 * MinIO Cloud Storage, (C) 2018-2019 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"

	"github.com/minio/minio-go/v6/pkg/set"
	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/bucket/policy"
	iampolicy "github.com/minio/minio/pkg/iam/policy"
	"github.com/minio/minio/pkg/madmin"
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

	// IAM groups directory.
	iamConfigGroupsPrefix = iamConfigPrefix + "/groups/"

	// IAM policies directory.
	iamConfigPoliciesPrefix = iamConfigPrefix + "/policies/"

	// IAM sts directory.
	iamConfigSTSPrefix = iamConfigPrefix + "/sts/"

	// IAM Policy DB prefixes.
	iamConfigPolicyDBPrefix         = iamConfigPrefix + "/policydb/"
	iamConfigPolicyDBUsersPrefix    = iamConfigPolicyDBPrefix + "users/"
	iamConfigPolicyDBSTSUsersPrefix = iamConfigPolicyDBPrefix + "sts-users/"
	iamConfigPolicyDBGroupsPrefix   = iamConfigPolicyDBPrefix + "groups/"

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

func getUserIdentityPath(user string, isSTS bool) string {
	basePath := iamConfigUsersPrefix
	if isSTS {
		basePath = iamConfigSTSPrefix
	}
	return pathJoin(basePath, user, iamIdentityFile)
}

func getGroupInfoPath(group string) string {
	return pathJoin(iamConfigGroupsPrefix, group, iamGroupMembersFile)
}

func getPolicyDocPath(name string) string {
	return pathJoin(iamConfigPoliciesPrefix, name, iamPolicyFile)
}

func getMappedPolicyPath(name string, isSTS, isGroup bool) string {
	switch {
	case isSTS:
		return pathJoin(iamConfigPolicyDBSTSUsersPrefix, name+".json")
	case isGroup:
		return pathJoin(iamConfigPolicyDBGroupsPrefix, name+".json")
	default:
		return pathJoin(iamConfigPolicyDBUsersPrefix, name+".json")
	}
}

// UserIdentity represents a user's secret key and their status
type UserIdentity struct {
	Version     int              `json:"version"`
	Credentials auth.Credentials `json:"credentials"`
}

func newUserIdentity(creds auth.Credentials) UserIdentity {
	return UserIdentity{Version: 1, Credentials: creds}
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
	Version int    `json:"version"`
	Policy  string `json:"policy"`
}

func newMappedPolicy(policy string) MappedPolicy {
	return MappedPolicy{Version: 1, Policy: policy}
}

// IAMSys - config system.
type IAMSys struct {
	sync.RWMutex

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
}

// IAMStorageAPI defines an interface for the IAM persistence layer
type IAMStorageAPI interface {
	migrateBackendFormat(ObjectLayer) error

	loadPolicyDoc(policy string, m map[string]iampolicy.Policy) error
	loadPolicyDocs(m map[string]iampolicy.Policy) error

	loadUser(user string, isSTS bool, m map[string]auth.Credentials) error
	loadUsers(isSTS bool, m map[string]auth.Credentials) error

	loadGroup(group string, m map[string]GroupInfo) error
	loadGroups(m map[string]GroupInfo) error

	loadMappedPolicy(name string, isSTS, isGroup bool, m map[string]MappedPolicy) error
	loadMappedPolicies(isSTS, isGroup bool, m map[string]MappedPolicy) error

	loadAll(*IAMSys, ObjectLayer) error

	saveIAMConfig(item interface{}, path string) error
	loadIAMConfig(item interface{}, path string) error
	deleteIAMConfig(path string) error

	savePolicyDoc(policyName string, p iampolicy.Policy) error
	saveMappedPolicy(name string, isSTS, isGroup bool, mp MappedPolicy) error
	saveUserIdentity(name string, isSTS bool, u UserIdentity) error
	saveGroupInfo(group string, gi GroupInfo) error

	deletePolicyDoc(policyName string) error
	deleteMappedPolicy(name string, isSTS, isGroup bool) error
	deleteUserIdentity(name string, isSTS bool) error
	deleteGroupInfo(name string) error

	watch(*IAMSys)
}

// LoadGroup - loads a specific group from storage, and updates the
// memberships cache. If the specified group does not exist in
// storage, it is removed from in-memory maps as well - this
// simplifies the implementation for group removal. This is called
// only via IAM notifications.
func (sys *IAMSys) LoadGroup(objAPI ObjectLayer, group string) error {
	sys.Lock()
	defer sys.Unlock()

	if objAPI == nil || sys.store == nil {
		return errServerNotInitialized
	}

	if globalEtcdClient != nil {
		// Watch APIs cover this case, so nothing to do.
		return nil
	}

	err := sys.store.loadGroup(group, sys.iamGroupsMap)
	if err != nil && err != errConfigNotFound {
		return err
	}

	if err == errConfigNotFound {
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
	sys.Lock()
	defer sys.Unlock()

	if objAPI == nil || sys.store == nil {
		return errServerNotInitialized
	}

	if globalEtcdClient == nil {
		return sys.store.loadPolicyDoc(policyName, sys.iamPolicyDocsMap)
	}

	// When etcd is set, we use watch APIs so this code is not needed.
	return nil
}

// LoadPolicyMapping - loads the mapped policy for a user or group
// from storage into server memory.
func (sys *IAMSys) LoadPolicyMapping(objAPI ObjectLayer, userOrGroup string, isGroup bool) error {
	sys.Lock()
	defer sys.Unlock()

	if objAPI == nil || sys.store == nil {
		return errServerNotInitialized
	}

	if globalEtcdClient == nil {
		var err error
		if isGroup {
			err = sys.store.loadMappedPolicy(userOrGroup, false, isGroup, sys.iamGroupPolicyMap)
		} else {
			err = sys.store.loadMappedPolicy(userOrGroup, false, isGroup, sys.iamUserPolicyMap)
		}

		// Ignore policy not mapped error
		if err != nil && err != errConfigNotFound {
			return err
		}
	}
	// When etcd is set, we use watch APIs so this code is not needed.
	return nil
}

// LoadUser - reloads a specific user from backend disks or etcd.
func (sys *IAMSys) LoadUser(objAPI ObjectLayer, accessKey string, isSTS bool) error {
	sys.Lock()
	defer sys.Unlock()

	if objAPI == nil || sys.store == nil {
		return errServerNotInitialized
	}

	if globalEtcdClient == nil {
		err := sys.store.loadUser(accessKey, isSTS, sys.iamUsersMap)
		if err != nil {
			return err
		}
		err = sys.store.loadMappedPolicy(accessKey, isSTS, false, sys.iamUserPolicyMap)
		// Ignore policy not mapped error
		if err != nil && err != errConfigNotFound {
			return err
		}
	}
	// When etcd is set, we use watch APIs so this code is not needed.
	return nil
}

// Load - loads iam subsystem
func (sys *IAMSys) Load() error {
	// Pass nil objectlayer here - it will be loaded internally
	// from the IAMStorageAPI.
	return sys.store.loadAll(sys, nil)
}

// Perform IAM configuration migration.
func (sys *IAMSys) doIAMConfigMigration(objAPI ObjectLayer) error {
	// Take IAM configuration migration lock
	lockPath := iamConfigPrefix + "/migration.lock"
	objLock := objAPI.NewNSLock(context.Background(), minioMetaBucket, lockPath)
	if err := objLock.GetLock(globalOperationTimeout); err != nil {
		return err
	}
	defer objLock.Unlock()

	return sys.store.migrateBackendFormat(objAPI)
}

// Init - initializes config system from iam.json
func (sys *IAMSys) Init(objAPI ObjectLayer) error {
	if objAPI == nil {
		return errServerNotInitialized
	}

	sys.Lock()
	if globalEtcdClient == nil {
		sys.store = newIAMObjectStore()
	} else {
		sys.store = newIAMEtcdStore()
	}
	sys.Unlock()

	// Migrate IAM configuration
	if err := sys.doIAMConfigMigration(objAPI); err != nil {
		return err
	}

	sys.store.watch(sys)

	return sys.store.loadAll(sys, objAPI)
}

// DeletePolicy - deletes a canned policy from backend or etcd.
func (sys *IAMSys) DeletePolicy(policyName string) error {
	objectAPI := newObjectLayerWithoutSafeModeFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	if policyName == "" {
		return errInvalidArgument
	}

	sys.Lock()
	defer sys.Unlock()

	if sys.store == nil {
		return errServerNotInitialized
	}

	err := sys.store.deletePolicyDoc(policyName)
	switch err.(type) {
	case ObjectNotFound:
		// Ignore error if policy is already deleted.
		err = nil
	}

	delete(sys.iamPolicyDocsMap, policyName)

	// Delete user-policy mappings that will no longer apply
	var usersToDel []string
	var isUserSTS []bool
	for u, mp := range sys.iamUserPolicyMap {
		if mp.Policy == policyName {
			usersToDel = append(usersToDel, u)
			cr, ok := sys.iamUsersMap[u]
			if !ok {
				// This case cannot happen
				return errNoSuchUser
			}
			// User is from STS if the creds are temporary
			isSTS := cr.IsTemp()
			isUserSTS = append(isUserSTS, isSTS)
		}
	}
	for i, u := range usersToDel {
		sys.policyDBSet(u, "", isUserSTS[i], false)
	}

	// Delete group-policy mappings that will no longer apply
	var groupsToDel []string
	for g, mp := range sys.iamGroupPolicyMap {
		if mp.Policy == policyName {
			groupsToDel = append(groupsToDel, g)
		}
	}
	for _, g := range groupsToDel {
		sys.policyDBSet(g, "", false, true)
	}

	return err
}

// InfoPolicy - expands the canned policy into its JSON structure.
func (sys *IAMSys) InfoPolicy(policyName string) ([]byte, error) {
	objectAPI := newObjectLayerWithoutSafeModeFn()
	if objectAPI == nil {
		return nil, errServerNotInitialized
	}

	sys.RLock()
	defer sys.RUnlock()

	v, ok := sys.iamPolicyDocsMap[policyName]
	if !ok {
		return nil, errNoSuchPolicy
	}
	return json.Marshal(v)
}

// ListPolicies - lists all canned policies.
func (sys *IAMSys) ListPolicies() (map[string][]byte, error) {
	objectAPI := newObjectLayerWithoutSafeModeFn()
	if objectAPI == nil {
		return nil, errServerNotInitialized
	}

	var policyDocsMap = make(map[string][]byte)

	sys.RLock()
	defer sys.RUnlock()

	for k, v := range sys.iamPolicyDocsMap {
		data, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		policyDocsMap[k] = data
	}

	return policyDocsMap, nil
}

// SetPolicy - sets a new name policy.
func (sys *IAMSys) SetPolicy(policyName string, p iampolicy.Policy) error {
	objectAPI := newObjectLayerWithoutSafeModeFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	if p.IsEmpty() || policyName == "" {
		return errInvalidArgument
	}

	sys.Lock()
	defer sys.Unlock()

	if sys.store == nil {
		return errServerNotInitialized
	}

	if err := sys.store.savePolicyDoc(policyName, p); err != nil {
		return err
	}

	sys.iamPolicyDocsMap[policyName] = p
	return nil
}

// DeleteUser - delete user (only for long-term users not STS users).
func (sys *IAMSys) DeleteUser(accessKey string) error {
	objectAPI := newObjectLayerWithoutSafeModeFn()
	if objectAPI == nil {
		return errServerNotInitialized
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
	sys.Lock()
	defer sys.Unlock()

	if sys.usersSysType != MinIOUsersSysType {
		return errIAMActionNotAllowed
	}

	if sys.store == nil {
		return errServerNotInitialized
	}

	// It is ok to ignore deletion error on the mapped policy
	sys.store.deleteMappedPolicy(accessKey, false, false)
	err := sys.store.deleteUserIdentity(accessKey, false)
	switch err.(type) {
	case ObjectNotFound:
		// ignore if user is already deleted.
		err = nil
	}

	delete(sys.iamUsersMap, accessKey)
	delete(sys.iamUserPolicyMap, accessKey)

	return err
}

// SetTempUser - set temporary user credentials, these credentials have an expiry.
func (sys *IAMSys) SetTempUser(accessKey string, cred auth.Credentials, policyName string) error {
	objectAPI := newObjectLayerWithoutSafeModeFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	sys.Lock()
	defer sys.Unlock()

	// If OPA is not set we honor any policy claims for this
	// temporary user which match with pre-configured canned
	// policies for this server.
	if globalPolicyOPA == nil && policyName != "" {
		p, ok := sys.iamPolicyDocsMap[policyName]
		if !ok {
			return errInvalidArgument
		}
		if p.IsEmpty() {
			delete(sys.iamUserPolicyMap, accessKey)
			return nil
		}

		if sys.store == nil {
			return errServerNotInitialized
		}

		mp := newMappedPolicy(policyName)
		if err := sys.store.saveMappedPolicy(accessKey, true, false, mp); err != nil {
			return err
		}

		sys.iamUserPolicyMap[accessKey] = mp
	}

	if sys.store == nil {
		return errServerNotInitialized
	}

	u := newUserIdentity(cred)
	if err := sys.store.saveUserIdentity(accessKey, true, u); err != nil {
		return err
	}

	sys.iamUsersMap[accessKey] = cred
	return nil
}

// ListUsers - list all users.
func (sys *IAMSys) ListUsers() (map[string]madmin.UserInfo, error) {
	objectAPI := newObjectLayerWithoutSafeModeFn()
	if objectAPI == nil {
		return nil, errServerNotInitialized
	}

	var users = make(map[string]madmin.UserInfo)

	sys.RLock()
	defer sys.RUnlock()

	if sys.usersSysType != MinIOUsersSysType {
		return nil, errIAMActionNotAllowed
	}

	for k, v := range sys.iamUsersMap {
		if !v.IsTemp() {
			users[k] = madmin.UserInfo{
				PolicyName: sys.iamUserPolicyMap[k].Policy,
				Status: func() madmin.AccountStatus {
					if v.IsValid() {
						return madmin.AccountEnabled
					}
					return madmin.AccountDisabled
				}(),
			}
		}
	}

	return users, nil
}

// IsTempUser - returns if given key is a temporary user.
func (sys *IAMSys) IsTempUser(name string) (bool, error) {
	objectAPI := newObjectLayerWithoutSafeModeFn()
	if objectAPI == nil {
		return false, errServerNotInitialized
	}

	sys.RLock()
	defer sys.RUnlock()

	creds, found := sys.iamUsersMap[name]
	if !found {
		return false, errNoSuchUser
	}

	return creds.IsTemp(), nil
}

// GetUserInfo - get info on a user.
func (sys *IAMSys) GetUserInfo(name string) (u madmin.UserInfo, err error) {
	objectAPI := newObjectLayerWithoutSafeModeFn()
	if objectAPI == nil {
		return u, errServerNotInitialized
	}

	sys.RLock()
	defer sys.RUnlock()

	if sys.usersSysType != MinIOUsersSysType {
		return madmin.UserInfo{
			PolicyName: sys.iamUserPolicyMap[name].Policy,
			MemberOf:   sys.iamUserGroupMemberships[name].ToSlice(),
		}, nil
	}

	creds, found := sys.iamUsersMap[name]
	if !found {
		return u, errNoSuchUser
	}

	if creds.IsTemp() {
		return u, errIAMActionNotAllowed
	}

	u = madmin.UserInfo{
		PolicyName: sys.iamUserPolicyMap[name].Policy,
		Status: func() madmin.AccountStatus {
			if creds.IsValid() {
				return madmin.AccountEnabled
			}
			return madmin.AccountDisabled
		}(),
		MemberOf: sys.iamUserGroupMemberships[name].ToSlice(),
	}
	return u, nil
}

// SetUserStatus - sets current user status, supports disabled or enabled.
func (sys *IAMSys) SetUserStatus(accessKey string, status madmin.AccountStatus) error {
	objectAPI := newObjectLayerWithoutSafeModeFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	if status != madmin.AccountEnabled && status != madmin.AccountDisabled {
		return errInvalidArgument
	}

	sys.Lock()
	defer sys.Unlock()

	if sys.usersSysType != MinIOUsersSysType {
		return errIAMActionNotAllowed
	}

	cred, ok := sys.iamUsersMap[accessKey]
	if !ok {
		return errNoSuchUser
	}

	if cred.IsTemp() {
		return errIAMActionNotAllowed
	}

	uinfo := newUserIdentity(auth.Credentials{
		AccessKey: accessKey,
		SecretKey: cred.SecretKey,
		Status: func() string {
			if status == madmin.AccountEnabled {
				return config.EnableOn
			}
			return config.EnableOff
		}(),
	})

	if sys.store == nil {
		return errServerNotInitialized
	}

	if err := sys.store.saveUserIdentity(accessKey, false, uinfo); err != nil {
		return err
	}

	sys.iamUsersMap[accessKey] = uinfo.Credentials
	return nil
}

// SetUser - set user credentials and policy.
func (sys *IAMSys) SetUser(accessKey string, uinfo madmin.UserInfo) error {
	objectAPI := newObjectLayerWithoutSafeModeFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	u := newUserIdentity(auth.Credentials{
		AccessKey: accessKey,
		SecretKey: uinfo.SecretKey,
		Status:    string(uinfo.Status),
	})

	sys.Lock()
	defer sys.Unlock()

	if sys.usersSysType != MinIOUsersSysType {
		return errIAMActionNotAllowed
	}

	if sys.store == nil {
		return errServerNotInitialized
	}

	cr, ok := sys.iamUsersMap[accessKey]
	if cr.IsTemp() && ok {
		return errIAMActionNotAllowed
	}

	if err := sys.store.saveUserIdentity(accessKey, false, u); err != nil {
		return err
	}

	sys.iamUsersMap[accessKey] = u.Credentials

	// Set policy if specified.
	if uinfo.PolicyName != "" {
		return sys.policyDBSet(accessKey, uinfo.PolicyName, false, false)
	}
	return nil
}

// SetUserSecretKey - sets user secret key
func (sys *IAMSys) SetUserSecretKey(accessKey string, secretKey string) error {
	objectAPI := newObjectLayerWithoutSafeModeFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	sys.Lock()
	defer sys.Unlock()

	if sys.usersSysType != MinIOUsersSysType {
		return errIAMActionNotAllowed
	}

	cred, ok := sys.iamUsersMap[accessKey]
	if !ok {
		return errNoSuchUser
	}

	if sys.store == nil {
		return errServerNotInitialized
	}

	cred.SecretKey = secretKey
	u := newUserIdentity(cred)
	if err := sys.store.saveUserIdentity(accessKey, false, u); err != nil {
		return err
	}

	sys.iamUsersMap[accessKey] = cred
	return nil
}

// GetUser - get user credentials
func (sys *IAMSys) GetUser(accessKey string) (cred auth.Credentials, ok bool) {
	sys.RLock()
	defer sys.RUnlock()

	cred, ok = sys.iamUsersMap[accessKey]
	return cred, ok && cred.IsValid()
}

// AddUsersToGroup - adds users to a group, creating the group if
// needed. No error if user(s) already are in the group.
func (sys *IAMSys) AddUsersToGroup(group string, members []string) error {
	objectAPI := newObjectLayerWithoutSafeModeFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	if group == "" {
		return errInvalidArgument
	}

	sys.Lock()
	defer sys.Unlock()

	if sys.usersSysType != MinIOUsersSysType {
		return errIAMActionNotAllowed
	}

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

	if sys.store == nil {
		return errServerNotInitialized
	}

	if err := sys.store.saveGroupInfo(group, gi); err != nil {
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
	objectAPI := newObjectLayerWithoutSafeModeFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	if group == "" {
		return errInvalidArgument
	}

	sys.Lock()
	defer sys.Unlock()

	if sys.usersSysType != MinIOUsersSysType {
		return errIAMActionNotAllowed
	}

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

	if sys.store == nil {
		return errServerNotInitialized
	}

	if len(members) == 0 {
		// len(gi.Members) == 0 here.

		// Remove the group from storage. First delete the
		// mapped policy.
		err := sys.store.deleteMappedPolicy(group, false, true)
		// No-mapped-policy case is ignored.
		if err != nil && err != errConfigNotFound {
			return err
		}
		err = sys.store.deleteGroupInfo(group)
		if err != nil {
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

	err := sys.store.saveGroupInfo(group, gi)
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
	objectAPI := newObjectLayerWithoutSafeModeFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	sys.Lock()
	defer sys.Unlock()

	if sys.store == nil {
		return errServerNotInitialized
	}

	if sys.usersSysType != MinIOUsersSysType {
		return errIAMActionNotAllowed
	}

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

	if err := sys.store.saveGroupInfo(group, gi); err != nil {
		return err
	}
	sys.iamGroupsMap[group] = gi
	return nil
}

// GetGroupDescription - builds up group description
func (sys *IAMSys) GetGroupDescription(group string) (gd madmin.GroupDesc, err error) {
	ps, err := sys.PolicyDBGet(group, true)
	if err != nil {
		return gd, err
	}

	// A group may be mapped to at most one policy.
	policy := ""
	if len(ps) > 0 {
		policy = ps[0]
	}

	if sys.usersSysType != MinIOUsersSysType {
		return madmin.GroupDesc{
			Name:   group,
			Policy: policy,
		}, nil
	}

	sys.RLock()
	defer sys.RUnlock()

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
	sys.RLock()
	defer sys.RUnlock()

	if sys.usersSysType != MinIOUsersSysType {
		return nil, errIAMActionNotAllowed
	}

	for k := range sys.iamGroupsMap {
		r = append(r, k)
	}
	return r, nil
}

// PolicyDBSet - sets a policy for a user or group in the
// PolicyDB. This function applies only long-term users. For STS
// users, policy is set directly by called sys.policyDBSet().
func (sys *IAMSys) PolicyDBSet(name, policy string, isGroup bool) error {
	objectAPI := newObjectLayerWithoutSafeModeFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	sys.Lock()
	defer sys.Unlock()

	// isSTS is always false when called via PolicyDBSet as policy
	// is never set by an external API call for STS users.
	return sys.policyDBSet(name, policy, false, isGroup)
}

// policyDBSet - sets a policy for user in the policy db. Assumes that caller
// has sys.Lock(). If policy == "", then policy mapping is removed.
func (sys *IAMSys) policyDBSet(name, policy string, isSTS, isGroup bool) error {
	if sys.store == nil {
		return errServerNotInitialized
	}

	if name == "" {
		return errInvalidArgument
	}
	if _, ok := sys.iamPolicyDocsMap[policy]; !ok && policy != "" {
		return errNoSuchPolicy
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
	if policy == "" {
		if err := sys.store.deleteMappedPolicy(name, isSTS, isGroup); err != nil {
			return err
		}
		if !isGroup {
			delete(sys.iamUserPolicyMap, name)
		} else {
			delete(sys.iamGroupPolicyMap, name)
		}
		return nil
	}

	// Handle policy mapping set/update
	mp := newMappedPolicy(policy)
	if err := sys.store.saveMappedPolicy(name, isSTS, isGroup, mp); err != nil {
		return err
	}
	if !isGroup {
		sys.iamUserPolicyMap[name] = mp
	} else {
		sys.iamGroupPolicyMap[name] = mp
	}
	return nil
}

var iamAccountReadAccessActions = iampolicy.NewActionSet(
	iampolicy.ListMultipartUploadPartsAction,
	iampolicy.ListBucketMultipartUploadsAction,
	iampolicy.ListBucketAction,
	iampolicy.HeadBucketAction,
	iampolicy.GetObjectAction,
	iampolicy.GetBucketLocationAction,

	// iampolicy.ListAllMyBucketsAction,
)

var iamAccountWriteAccessActions = iampolicy.NewActionSet(
	iampolicy.AbortMultipartUploadAction,
	iampolicy.CreateBucketAction,
	iampolicy.PutObjectAction,
	iampolicy.DeleteObjectAction,
	iampolicy.DeleteBucketAction,
)

var iamAccountOtherAccessActions = iampolicy.NewActionSet(
	iampolicy.BypassGovernanceModeAction,
	iampolicy.BypassGovernanceRetentionAction,
	iampolicy.PutObjectRetentionAction,
	iampolicy.GetObjectRetentionAction,
	iampolicy.GetObjectLegalHoldAction,
	iampolicy.PutObjectLegalHoldAction,
	iampolicy.GetBucketObjectLockConfigurationAction,
	iampolicy.PutBucketObjectLockConfigurationAction,

	iampolicy.ListenBucketNotificationAction,

	iampolicy.PutBucketLifecycleAction,
	iampolicy.GetBucketLifecycleAction,

	iampolicy.PutBucketNotificationAction,
	iampolicy.GetBucketNotificationAction,

	iampolicy.PutBucketPolicyAction,
	iampolicy.DeleteBucketPolicyAction,
	iampolicy.GetBucketPolicyAction,
)

// GetAccountAccess iterates over all policies documents associated to a user
// and returns if the user has read and/or write access to any resource.
func (sys *IAMSys) GetAccountAccess(accountName, bucket string) (rd, wr, o bool) {
	policies, err := sys.PolicyDBGet(accountName, false)
	if err != nil {
		logger.LogIf(context.Background(), err)
		return false, false, false
	}

	if len(policies) == 0 {
		// No policy found.
		return false, false, false
	}

	// Policies were found, evaluate all of them.
	sys.RLock()
	defer sys.RUnlock()

	var availablePolicies []iampolicy.Policy
	for _, pname := range policies {
		p, found := sys.iamPolicyDocsMap[pname]
		if found {
			availablePolicies = append(availablePolicies, p)
		}
	}

	if len(availablePolicies) == 0 {
		return false, false, false
	}

	combinedPolicy := availablePolicies[0]
	for i := 1; i < len(availablePolicies); i++ {
		combinedPolicy.Statements = append(combinedPolicy.Statements,
			availablePolicies[i].Statements...)
	}

	allActions := iampolicy.NewActionSet(iampolicy.AllActions)
	for _, st := range combinedPolicy.Statements {
		// Ignore if this is not an allow policy statement
		if st.Effect != policy.Allow {
			continue
		}
		// Fast calculation if there is s3:* permissions to any resource
		if !st.Actions.Intersection(allActions).IsEmpty() {
			rd, wr, o = true, true, true
			break
		}
		if !st.Actions.Intersection(iamAccountReadAccessActions).IsEmpty() {
			rd = true
		}
		if !st.Actions.Intersection(iamAccountWriteAccessActions).IsEmpty() {
			wr = true
		}
		if !st.Actions.Intersection(iamAccountOtherAccessActions).IsEmpty() {
			o = true
		}
	}

	return
}

// PolicyDBGet - gets policy set on a user or group. Since a user may
// be a member of multiple groups, this function returns an array of
// applicable policies (each group is mapped to at most one policy).
func (sys *IAMSys) PolicyDBGet(name string, isGroup bool) ([]string, error) {
	if name == "" {
		return nil, errInvalidArgument
	}

	objectAPI := newObjectLayerWithoutSafeModeFn()
	if objectAPI == nil {
		return nil, errServerNotInitialized
	}

	sys.RLock()
	defer sys.RUnlock()

	return sys.policyDBGet(name, isGroup)
}

// This call assumes that caller has the sys.RLock()
func (sys *IAMSys) policyDBGet(name string, isGroup bool) ([]string, error) {
	if isGroup {
		if _, ok := sys.iamGroupsMap[name]; !ok {
			return nil, errNoSuchGroup
		}

		policy := sys.iamGroupPolicyMap[name]
		// returned policy could be empty
		if policy.Policy == "" {
			return nil, nil
		}
		return []string{policy.Policy}, nil
	}

	// When looking for a user's policies, we also check if the
	// user and the groups they are member of are enabled.
	if u, ok := sys.iamUsersMap[name]; !ok {
		return nil, errNoSuchUser
	} else if u.Status == statusDisabled {
		// User is disabled, so we return no policy - this
		// ensures the request is denied.
		return nil, nil
	}

	result := []string{}
	policy := sys.iamUserPolicyMap[name]
	// returned policy could be empty
	if policy.Policy != "" {
		result = append(result, policy.Policy)
	}
	for _, group := range sys.iamUserGroupMemberships[name].ToSlice() {
		// Skip missing or disabled groups
		gi, ok := sys.iamGroupsMap[group]
		if !ok || gi.Status == statusDisabled {
			continue
		}

		p, ok := sys.iamGroupPolicyMap[group]
		if ok && p.Policy != "" {
			result = append(result, p.Policy)
		}
	}
	return result, nil
}

// IsAllowedSTS is meant for STS based temporary credentials,
// which implements claims validation and verification other than
// applying policies.
func (sys *IAMSys) IsAllowedSTS(args iampolicy.Args) bool {
	// If it is an LDAP request, check that user and group
	// policies allow the request.
	if userIface, ok := args.Claims[ldapUser]; ok {
		var user string
		if u, ok := userIface.(string); ok {
			user = u
		} else {
			return false
		}

		var groups []string
		groupsVal := args.Claims[ldapGroups]
		if g, ok := groupsVal.([]interface{}); ok {
			for _, eachG := range g {
				if eachGStr, ok := eachG.(string); ok {
					groups = append(groups, eachGStr)
				}
			}
		} else {
			return false
		}

		sys.RLock()
		defer sys.RUnlock()

		// We look up the policy mapping directly to bypass
		// users exists, group exists validations that do not
		// apply here.
		var policies []iampolicy.Policy
		if policy, ok := sys.iamUserPolicyMap[user]; ok {
			p, found := sys.iamPolicyDocsMap[policy.Policy]
			if found {
				policies = append(policies, p)
			}
		}
		for _, group := range groups {
			policy, ok := sys.iamGroupPolicyMap[group]
			if !ok {
				continue
			}
			p, found := sys.iamPolicyDocsMap[policy.Policy]
			if found {
				policies = append(policies, p)
			}
		}
		if len(policies) == 0 {
			return false
		}
		combinedPolicy := policies[0]
		for i := 1; i < len(policies); i++ {
			combinedPolicy.Statements =
				append(combinedPolicy.Statements,
					policies[i].Statements...)
		}
		return combinedPolicy.IsAllowed(args)
	}

	pnameSlice, ok := args.GetPolicies(iamPolicyClaimName())
	if !ok {
		// When claims are set, it should have a policy claim field.
		return false
	}

	// When claims are set, it should have a policy claim field.
	if len(pnameSlice) == 0 {
		return false
	}

	sys.RLock()
	defer sys.RUnlock()

	// If policy is available for given user, check the policy.
	mp, ok := sys.iamUserPolicyMap[args.AccountName]
	if !ok {
		// No policy available reject.
		return false
	}
	name := mp.Policy

	if pnameSlice[0] != name {
		// When claims has a policy, it should match the
		// policy of args.AccountName which server remembers.
		// if not reject such requests.
		return false
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
			logger.LogIf(context.Background(), err)
			return false
		}

		// Policy without Version string value reject it.
		if subPolicy.Version == "" {
			return false
		}

		// Sub policy is set and valid.
		p, ok := sys.iamPolicyDocsMap[pnameSlice[0]]
		return ok && p.IsAllowed(args) && subPolicy.IsAllowed(args)
	}

	// Sub policy not set, this is most common since subPolicy
	// is optional, use the top level policy only.
	p, ok := sys.iamPolicyDocsMap[pnameSlice[0]]
	return ok && p.IsAllowed(args)
}

// IsAllowed - checks given policy args is allowed to continue the Rest API.
func (sys *IAMSys) IsAllowed(args iampolicy.Args) bool {
	// If opa is configured, use OPA always.
	if globalPolicyOPA != nil {
		ok, err := globalPolicyOPA.IsAllowed(args)
		if err != nil {
			logger.LogIf(context.Background(), err)
		}
		return ok
	}

	// Policies don't apply to the owner.
	if args.IsOwner {
		return true
	}

	// If the credential is temporary, perform STS related checks.
	ok, err := sys.IsTempUser(args.AccountName)
	if err != nil {
		logger.LogIf(context.Background(), err)
		return false
	}
	if ok {
		return sys.IsAllowedSTS(args)
	}

	policies, err := sys.PolicyDBGet(args.AccountName, false)
	if err != nil {
		logger.LogIf(context.Background(), err)
		return false
	}

	if len(policies) == 0 {
		// No policy found.
		return false
	}

	// Policies were found, evaluate all of them.
	sys.RLock()
	defer sys.RUnlock()

	var availablePolicies []iampolicy.Policy
	for _, pname := range policies {
		p, found := sys.iamPolicyDocsMap[pname]
		if found {
			availablePolicies = append(availablePolicies, p)
		}
	}
	if len(availablePolicies) == 0 {
		return false
	}
	combinedPolicy := availablePolicies[0]
	for i := 1; i < len(availablePolicies); i++ {
		combinedPolicy.Statements = append(combinedPolicy.Statements,
			availablePolicies[i].Statements...)
	}
	return combinedPolicy.IsAllowed(args)
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

// NewIAMSys - creates new config system object.
func NewIAMSys() *IAMSys {
	// Check global server configuration to determine the type of
	// users system configured.

	// The default users system
	var utype UsersSysType
	switch {
	case globalLDAPConfig.Enabled:
		utype = LDAPUsersSysType
	default:
		utype = MinIOUsersSysType
	}

	return &IAMSys{
		usersSysType:            utype,
		iamUsersMap:             make(map[string]auth.Credentials),
		iamPolicyDocsMap:        make(map[string]iampolicy.Policy),
		iamUserPolicyMap:        make(map[string]MappedPolicy),
		iamGroupsMap:            make(map[string]GroupInfo),
		iamUserGroupMemberships: make(map[string]set.StringSet),
	}
}
