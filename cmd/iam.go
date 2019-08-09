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
	"strings"
	"sync"

	"github.com/minio/minio-go/v6/pkg/set"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	iampolicy "github.com/minio/minio/pkg/iam/policy"
	"github.com/minio/minio/pkg/madmin"
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
	if objAPI == nil {
		return errInvalidArgument
	}

	sys.Lock()
	defer sys.Unlock()

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
	if objAPI == nil {
		return errInvalidArgument
	}

	sys.Lock()
	defer sys.Unlock()

	if globalEtcdClient == nil {
		return sys.store.loadPolicyDoc(policyName, sys.iamPolicyDocsMap)
	}

	// When etcd is set, we use watch APIs so this code is not needed.
	return nil
}

// LoadUser - reloads a specific user from backend disks or etcd.
func (sys *IAMSys) LoadUser(objAPI ObjectLayer, accessKey string, isSTS bool) error {
	if objAPI == nil {
		return errInvalidArgument
	}

	sys.Lock()
	defer sys.Unlock()

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
	objLock := globalNSMutex.NewNSLock(context.Background(), minioMetaBucket, lockPath)
	if err := objLock.GetLock(globalOperationTimeout); err != nil {
		return err
	}
	defer objLock.Unlock()

	return sys.store.migrateBackendFormat(objAPI)
}

// Init - initializes config system from iam.json
func (sys *IAMSys) Init(objAPI ObjectLayer) error {
	if objAPI == nil {
		return errInvalidArgument
	}

	if globalEtcdClient == nil {
		sys.store = newIAMObjectStore()
	} else {
		sys.store = newIAMEtcdStore()
	}

	doneCh := make(chan struct{})
	defer close(doneCh)

	// Migrating IAM needs a retry mechanism for
	// the following reasons:
	//  - Read quorum is lost just after the initialization
	//    of the object layer.
	for range newRetryTimerSimple(doneCh) {
		// Migrate IAM configuration
		if err := sys.doIAMConfigMigration(objAPI); err != nil {
			if err == errDiskNotFound ||
				strings.Contains(err.Error(), InsufficientReadQuorum{}.Error()) ||
				strings.Contains(err.Error(), InsufficientWriteQuorum{}.Error()) {
				logger.Info("Waiting for IAM subsystem to be initialized..")
				continue
			}
			return err
		}
		break
	}

	sys.store.watch(sys)

	// Initializing IAM needs a retry mechanism for
	// the following reasons:
	//  - Read quorum is lost just after the initialization
	//    of the object layer.
	for range newRetryTimerSimple(doneCh) {
		// Load IAMSys once during boot. Need to pass in
		// objAPI as server has not yet initialized.
		if err := sys.store.loadAll(sys, objAPI); err != nil {
			if err == errDiskNotFound ||
				strings.Contains(err.Error(), InsufficientReadQuorum{}.Error()) ||
				strings.Contains(err.Error(), InsufficientWriteQuorum{}.Error()) {
				logger.Info("Waiting for IAM subsystem to be initialized..")
				continue
			}
			return err
		}
		break
	}

	return nil
}

// DeletePolicy - deletes a canned policy from backend or etcd.
func (sys *IAMSys) DeletePolicy(policyName string) error {
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	if policyName == "" {
		return errInvalidArgument
	}

	err := sys.store.deletePolicyDoc(policyName)
	switch err.(type) {
	case ObjectNotFound:
		// Ignore error if policy is already deleted.
		err = nil
	}

	sys.Lock()
	defer sys.Unlock()

	delete(sys.iamPolicyDocsMap, policyName)
	return err
}

// ListPolicies - lists all canned policies.
func (sys *IAMSys) ListPolicies() (map[string][]byte, error) {
	objectAPI := newObjectLayerFn()
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
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	if p.IsEmpty() || policyName == "" {
		return errInvalidArgument
	}

	if err := sys.store.savePolicyDoc(policyName, p); err != nil {
		return err
	}

	sys.Lock()
	defer sys.Unlock()
	sys.iamPolicyDocsMap[policyName] = p
	return nil
}

// DeleteUser - delete user (only for long-term users not STS users).
func (sys *IAMSys) DeleteUser(accessKey string) error {
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
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

	sys.Lock()
	defer sys.Unlock()

	delete(sys.iamUsersMap, accessKey)
	delete(sys.iamUserPolicyMap, accessKey)

	return err
}

// SetTempUser - set temporary user credentials, these credentials have an expiry.
func (sys *IAMSys) SetTempUser(accessKey string, cred auth.Credentials, policyName string) error {
	objectAPI := newObjectLayerFn()
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

		mp := newMappedPolicy(policyName)
		if err := sys.store.saveMappedPolicy(accessKey, true, false, mp); err != nil {
			return err
		}

		sys.iamUserPolicyMap[accessKey] = mp
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
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return nil, errServerNotInitialized
	}

	var users = make(map[string]madmin.UserInfo)

	sys.RLock()
	defer sys.RUnlock()

	for k, v := range sys.iamUsersMap {
		users[k] = madmin.UserInfo{
			PolicyName: sys.iamUserPolicyMap[k].Policy,
			Status:     madmin.AccountStatus(v.Status),
		}
	}

	return users, nil
}

// SetUserStatus - sets current user status, supports disabled or enabled.
func (sys *IAMSys) SetUserStatus(accessKey string, status madmin.AccountStatus) error {
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	if status != madmin.AccountEnabled && status != madmin.AccountDisabled {
		return errInvalidArgument
	}

	sys.Lock()
	defer sys.Unlock()

	cred, ok := sys.iamUsersMap[accessKey]
	if !ok {
		return errNoSuchUser
	}

	uinfo := newUserIdentity(auth.Credentials{
		AccessKey: accessKey,
		SecretKey: cred.SecretKey,
		Status:    string(status),
	})
	if err := sys.store.saveUserIdentity(accessKey, false, uinfo); err != nil {
		return err
	}

	sys.iamUsersMap[accessKey] = uinfo.Credentials
	return nil
}

// SetUser - set user credentials and policy.
func (sys *IAMSys) SetUser(accessKey string, uinfo madmin.UserInfo) error {
	objectAPI := newObjectLayerFn()
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

	if err := sys.store.saveUserIdentity(accessKey, false, u); err != nil {
		return err
	}
	sys.iamUsersMap[accessKey] = u.Credentials

	// Set policy if specified.
	if uinfo.PolicyName != "" {
		return sys.policyDBSet(objectAPI, accessKey, uinfo.PolicyName, false, false)
	}
	return nil
}

// SetUserSecretKey - sets user secret key
func (sys *IAMSys) SetUserSecretKey(accessKey string, secretKey string) error {
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	sys.Lock()
	defer sys.Unlock()

	cred, ok := sys.iamUsersMap[accessKey]
	if !ok {
		return errNoSuchUser
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
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	if group == "" {
		return errInvalidArgument
	}

	sys.Lock()
	defer sys.Unlock()

	// Validate that all members exist.
	for _, member := range members {
		_, ok := sys.iamUsersMap[member]
		if !ok {
			return errNoSuchUser
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
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	if group == "" {
		return errInvalidArgument
	}

	sys.Lock()
	defer sys.Unlock()

	// Validate that all members exist.
	for _, member := range members {
		_, ok := sys.iamUsersMap[member]
		if !ok {
			return errNoSuchUser
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
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	sys.Lock()
	defer sys.Unlock()

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
	sys.RLock()
	defer sys.RUnlock()

	gi, ok := sys.iamGroupsMap[group]
	if !ok {
		return gd, errNoSuchGroup
	}

	var p []string
	p, err = sys.policyDBGet(group, true)
	if err != nil {
		return gd, err
	}

	policy := ""
	if len(p) > 0 {
		policy = p[0]
	}

	return madmin.GroupDesc{
		Name:    group,
		Status:  gi.Status,
		Members: gi.Members,
		Policy:  policy,
	}, nil
}

// ListGroups - lists groups
func (sys *IAMSys) ListGroups() (r []string) {
	sys.RLock()
	defer sys.RUnlock()

	for k := range sys.iamGroupsMap {
		r = append(r, k)
	}
	return r
}

// PolicyDBSet - sets a policy for a user or group in the
// PolicyDB. This function applies only long-term users. For STS
// users, policy is set directly by called sys.policyDBSet().
func (sys *IAMSys) PolicyDBSet(name, policy string, isGroup bool) error {
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	sys.Lock()
	defer sys.Unlock()

	// isSTS is always false when called via PolicyDBSet as policy
	// is never set by an external API call for STS users.
	return sys.policyDBSet(objectAPI, name, policy, false, isGroup)
}

// policyDBSet - sets a policy for user in the policy db. Assumes that
// caller has sys.Lock().
func (sys *IAMSys) policyDBSet(objectAPI ObjectLayer, name, policy string, isSTS, isGroup bool) error {
	if name == "" || policy == "" {
		return errInvalidArgument
	}
	if _, ok := sys.iamUsersMap[name]; !ok {
		return errNoSuchUser
	}
	if _, ok := sys.iamPolicyDocsMap[policy]; !ok {
		return errNoSuchPolicy
	}
	if _, ok := sys.iamUsersMap[name]; !ok {
		return errNoSuchUser
	}

	mp := newMappedPolicy(policy)
	if err := sys.store.saveMappedPolicy(name, isSTS, isGroup, mp); err != nil {
		return err
	}
	sys.iamUserPolicyMap[name] = mp
	return nil
}

// PolicyDBGet - gets policy set on a user or group. Since a user may
// be a member of multiple groups, this function returns an array of
// applicable policies (each group is mapped to at most one policy).
func (sys *IAMSys) PolicyDBGet(name string, isGroup bool) ([]string, error) {
	if name == "" {
		return nil, errInvalidArgument
	}

	objectAPI := newObjectLayerFn()
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

	if _, ok := sys.iamUsersMap[name]; !ok {
		return nil, errNoSuchUser
	}

	result := []string{}
	policy := sys.iamUserPolicyMap[name]
	// returned policy could be empty
	if policy.Policy != "" {
		result = append(result, policy.Policy)
	}
	for _, group := range sys.iamUserGroupMemberships[name].ToSlice() {
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
	pname, ok := args.Claims[iampolicy.PolicyName]
	if !ok {
		// When claims are set, it should have a "policy" field.
		return false
	}
	pnameStr, ok := pname.(string)
	if !ok {
		// When claims has "policy" field, it should be string.
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

	if pnameStr != name {
		// When claims has a policy, it should match the
		// policy of args.AccountName which server remembers.
		// if not reject such requests.
		return false
	}

	// Now check if we have a sessionPolicy.
	spolicy, ok := args.Claims[iampolicy.SessionPolicyName]
	if !ok {
		// Sub policy not set, this is most common since subPolicy
		// is optional, use the top level policy only.
		p, ok := sys.iamPolicyDocsMap[pnameStr]
		return ok && p.IsAllowed(args)
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
		logger.LogIf(context.Background(), err)
		return false
	}

	// Policy without Version string value reject it.
	if subPolicy.Version == "" {
		return false
	}

	// Sub policy is set and valid.
	p, ok := sys.iamPolicyDocsMap[pnameStr]
	return ok && p.IsAllowed(args) && subPolicy.IsAllowed(args)
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

	// With claims set, we should do STS related checks and validation.
	if len(args.Claims) > 0 {
		return sys.IsAllowedSTS(args)
	}

	sys.RLock()
	defer sys.RUnlock()

	// If policy is available for given user, check the policy.
	if mp, found := sys.iamUserPolicyMap[args.AccountName]; found {
		p, ok := sys.iamPolicyDocsMap[mp.Policy]
		return ok && p.IsAllowed(args)
	}

	// As policy is not available and OPA is not configured,
	// return the owner value.
	return args.IsOwner
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
	if _, ok := sys.iamUserGroupMemberships[group]; !ok {
		return
	}
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
	return &IAMSys{
		iamUsersMap:             make(map[string]auth.Credentials),
		iamPolicyDocsMap:        make(map[string]iampolicy.Policy),
		iamUserPolicyMap:        make(map[string]MappedPolicy),
		iamGroupsMap:            make(map[string]GroupInfo),
		iamUserGroupMemberships: make(map[string]set.StringSet),
	}
}
