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
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/minio/madmin-go"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/logger"
	iampolicy "github.com/minio/pkg/iam/policy"
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
	case svcUser:
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
	case svcUser:
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
	return set.CreateStringSet(mp.toSlice()...)
}

func newMappedPolicy(policy string) MappedPolicy {
	return MappedPolicy{Version: 1, Policies: policy}
}

// key options
type options struct {
	ttl int64 //expiry in seconds
}

type iamWatchEvent struct {
	isCreated bool // !isCreated implies a delete event.
	keyPath   string
}

// iamCache contains in-memory cache of IAM data.
type iamCache struct {
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
}

func newIamCache() *iamCache {
	return &iamCache{
		iamPolicyDocsMap:        map[string]iampolicy.Policy{},
		iamUsersMap:             map[string]auth.Credentials{},
		iamGroupsMap:            map[string]GroupInfo{},
		iamUserGroupMemberships: map[string]set.StringSet{},
		iamUserPolicyMap:        map[string]MappedPolicy{},
		iamGroupPolicyMap:       map[string]MappedPolicy{},
	}
}

// buildUserGroupMemberships - builds the memberships map. IMPORTANT:
// Assumes that c.Lock is held by caller.
func (c *iamCache) buildUserGroupMemberships() {
	for group, gi := range c.iamGroupsMap {
		c.updateGroupMembershipsMap(group, &gi)
	}
}

// updateGroupMembershipsMap - updates the memberships map for a
// group. IMPORTANT: Assumes c.Lock() is held by caller.
func (c *iamCache) updateGroupMembershipsMap(group string, gi *GroupInfo) {
	if gi == nil {
		return
	}
	for _, member := range gi.Members {
		v := c.iamUserGroupMemberships[member]
		if v == nil {
			v = set.CreateStringSet(group)
		} else {
			v.Add(group)
		}
		c.iamUserGroupMemberships[member] = v
	}
}

// removeGroupFromMembershipsMap - removes the group from every member
// in the cache. IMPORTANT: Assumes c.Lock() is held by caller.
func (c *iamCache) removeGroupFromMembershipsMap(group string) {
	for member, groups := range c.iamUserGroupMemberships {
		if !groups.Contains(group) {
			continue
		}
		groups.Remove(group)
		c.iamUserGroupMemberships[member] = groups
	}
}

// policyDBGet - lower-level helper; does not take locks.
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
func (c *iamCache) policyDBGet(mode UsersSysType, name string, isGroup bool) ([]string, error) {
	if isGroup {
		if mode == MinIOUsersSysType {
			g, ok := c.iamGroupsMap[name]
			if !ok {
				return nil, errNoSuchGroup
			}

			// Group is disabled, so we return no policy - this
			// ensures the request is denied.
			if g.Status == statusDisabled {
				return nil, nil
			}
		}

		return c.iamGroupPolicyMap[name].toSlice(), nil
	}

	if name == globalActiveCred.AccessKey {
		return []string{"consoleAdmin"}, nil
	}

	// When looking for a user's policies, we also check if the user
	// and the groups they are member of are enabled.
	var parentName string
	u, ok := c.iamUsersMap[name]
	if ok {
		if !u.IsValid() {
			return nil, nil
		}
		parentName = u.ParentUser
	}

	mp, ok := c.iamUserPolicyMap[name]
	if !ok {
		// Service accounts with root credentials, inherit parent permissions
		if parentName == globalActiveCred.AccessKey && u.IsServiceAccount() {
			// even if this is set, the claims present in the service
			// accounts apply the final permissions if any.
			return []string{"consoleAdmin"}, nil
		}
		if parentName != "" {
			mp = c.iamUserPolicyMap[parentName]
		}
	}

	// returned policy could be empty
	policies := mp.toSlice()

	for _, group := range c.iamUserGroupMemberships[name].ToSlice() {
		// Skip missing or disabled groups
		gi, ok := c.iamGroupsMap[group]
		if !ok || gi.Status == statusDisabled {
			continue
		}

		policies = append(policies, c.iamGroupPolicyMap[group].toSlice()...)
	}

	return policies, nil
}

// IAMStorageAPI defines an interface for the IAM persistence layer
type IAMStorageAPI interface {

	// The role of the read-write lock is to prevent go routines from
	// concurrently reading and writing the IAM storage. The (r)lock()
	// functions return the iamCache. The cache can be safely written to
	// only when returned by `lock()`.
	lock() *iamCache
	unlock()
	rlock() *iamCache
	runlock()

	migrateBackendFormat(context.Context) error

	getUsersSysType() UsersSysType

	loadPolicyDoc(ctx context.Context, policy string, m map[string]iampolicy.Policy) error
	loadPolicyDocs(ctx context.Context, m map[string]iampolicy.Policy) error

	loadUser(ctx context.Context, user string, userType IAMUserType, m map[string]auth.Credentials) error
	loadUsers(ctx context.Context, userType IAMUserType, m map[string]auth.Credentials) error

	loadGroup(ctx context.Context, group string, m map[string]GroupInfo) error
	loadGroups(ctx context.Context, m map[string]GroupInfo) error

	loadMappedPolicy(ctx context.Context, name string, userType IAMUserType, isGroup bool, m map[string]MappedPolicy) error
	loadMappedPolicies(ctx context.Context, userType IAMUserType, isGroup bool, m map[string]MappedPolicy) error

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
}

// iamStorageWatcher is implemented by `IAMStorageAPI` implementers that
// additionally support watching storage for changes.
type iamStorageWatcher interface {
	watch(ctx context.Context, keyPath string) <-chan iamWatchEvent
}

// Set default canned policies only if not already overridden by users.
func setDefaultCannedPolicies(policies map[string]iampolicy.Policy) {
	for _, v := range iampolicy.DefaultPolicies {
		if _, ok := policies[v.Name]; !ok {
			policies[v.Name] = v.Definition
		}
	}
}

// LoadIAMCache reads all IAM items and populates a new iamCache object and
// replaces the in-memory cache object.
func (store *IAMStoreSys) LoadIAMCache(ctx context.Context) error {
	newCache := newIamCache()

	cache := store.lock()
	defer store.unlock()

	if err := store.loadPolicyDocs(ctx, newCache.iamPolicyDocsMap); err != nil {
		return err
	}

	// Sets default canned policies, if none are set.
	setDefaultCannedPolicies(newCache.iamPolicyDocsMap)

	if store.getUsersSysType() == MinIOUsersSysType {
		if err := store.loadUsers(ctx, regUser, newCache.iamUsersMap); err != nil {
			return err
		}
		if err := store.loadGroups(ctx, newCache.iamGroupsMap); err != nil {
			return err
		}
	}

	// load polices mapped to users
	if err := store.loadMappedPolicies(ctx, regUser, false, newCache.iamUserPolicyMap); err != nil {
		return err
	}

	// load policies mapped to groups
	if err := store.loadMappedPolicies(ctx, regUser, true, newCache.iamGroupPolicyMap); err != nil {
		return err
	}

	// load service accounts
	if err := store.loadUsers(ctx, svcUser, newCache.iamUsersMap); err != nil {
		return err
	}

	// load STS temp users
	if err := store.loadUsers(ctx, stsUser, newCache.iamUsersMap); err != nil {
		return err
	}

	// load STS policy mappings
	if err := store.loadMappedPolicies(ctx, stsUser, false, newCache.iamUserPolicyMap); err != nil {
		return err
	}

	newCache.buildUserGroupMemberships()

	cache.iamGroupPolicyMap = newCache.iamGroupPolicyMap
	cache.iamGroupsMap = newCache.iamGroupsMap
	cache.iamPolicyDocsMap = newCache.iamPolicyDocsMap
	cache.iamUserGroupMemberships = newCache.iamUserGroupMemberships
	cache.iamUserPolicyMap = newCache.iamUserPolicyMap
	cache.iamUsersMap = newCache.iamUsersMap

	return nil
}

// IAMStoreSys contains IAMStorageAPI to add higher-level methods on the storage
// layer.
type IAMStoreSys struct {
	IAMStorageAPI
}

// HasWatcher - returns if the storage system has a watcher.
func (store *IAMStoreSys) HasWatcher() bool {
	_, ok := store.IAMStorageAPI.(iamStorageWatcher)
	return ok
}

// GetUser - fetches credential from memory.
func (store *IAMStoreSys) GetUser(user string) (auth.Credentials, bool) {
	cache := store.rlock()
	defer store.runlock()

	c, ok := cache.iamUsersMap[user]
	return c, ok
}

// GetMappedPolicy - fetches mapped policy from memory.
func (store *IAMStoreSys) GetMappedPolicy(name string, isGroup bool) (MappedPolicy, bool) {
	cache := store.rlock()
	defer store.runlock()

	if isGroup {
		v, ok := cache.iamGroupPolicyMap[name]
		return v, ok
	}

	v, ok := cache.iamUserPolicyMap[name]
	return v, ok
}

// GroupNotificationHandler - updates in-memory cache on notification of
// change (e.g. peer notification for object storage and etcd watch
// notification).
func (store *IAMStoreSys) GroupNotificationHandler(ctx context.Context, group string) error {
	cache := store.lock()
	defer store.unlock()

	err := store.loadGroup(ctx, group, cache.iamGroupsMap)
	if err != nil && err != errNoSuchGroup {
		return err
	}

	if err == errNoSuchGroup {
		// group does not exist - so remove from memory.
		cache.removeGroupFromMembershipsMap(group)
		delete(cache.iamGroupsMap, group)
		delete(cache.iamGroupPolicyMap, group)
		return nil
	}

	gi := cache.iamGroupsMap[group]

	// Updating the group memberships cache happens in two steps:
	//
	// 1. Remove the group from each user's list of memberships.
	// 2. Add the group to each member's list of memberships.
	//
	// This ensures that regardless of members being added or
	// removed, the cache stays current.
	cache.removeGroupFromMembershipsMap(group)
	cache.updateGroupMembershipsMap(group, &gi)
	return nil
}

// PolicyDBGet - fetches policies associated with the given user or group, and
// additional groups if provided.
func (store *IAMStoreSys) PolicyDBGet(name string, isGroup bool, groups ...string) ([]string, error) {
	if name == "" {
		return nil, errInvalidArgument
	}

	cache := store.rlock()
	defer store.runlock()

	policies, err := cache.policyDBGet(store.getUsersSysType(), name, isGroup)
	if err != nil {
		return nil, err
	}

	if !isGroup {
		for _, group := range groups {
			ps, err := cache.policyDBGet(store.getUsersSysType(), group, true)
			if err != nil {
				return nil, err
			}
			policies = append(policies, ps...)
		}
	}

	return policies, nil
}

// AddUsersToGroup - adds users to group, creating the group if needed.
func (store *IAMStoreSys) AddUsersToGroup(ctx context.Context, group string, members []string) error {
	if group == "" {
		return errInvalidArgument
	}

	cache := store.lock()
	defer store.unlock()

	// Validate that all members exist.
	for _, member := range members {
		cr, ok := cache.iamUsersMap[member]
		if !ok {
			return errNoSuchUser
		}
		if cr.IsTemp() || cr.IsServiceAccount() {
			return errIAMActionNotAllowed
		}
	}

	gi, ok := cache.iamGroupsMap[group]
	if !ok {
		// Set group as enabled by default when it doesn't
		// exist.
		gi = newGroupInfo(members)
	} else {
		mergedMembers := append(gi.Members, members...)
		uniqMembers := set.CreateStringSet(mergedMembers...).ToSlice()
		gi.Members = uniqMembers
	}

	if err := store.saveGroupInfo(ctx, group, gi); err != nil {
		return err
	}

	cache.iamGroupsMap[group] = gi

	// update user-group membership map
	for _, member := range members {
		gset := cache.iamUserGroupMemberships[member]
		if gset == nil {
			gset = set.CreateStringSet(group)
		} else {
			gset.Add(group)
		}
		cache.iamUserGroupMemberships[member] = gset
	}

	return nil

}

// helper function - does not take any locks. Updates only cache if
// updateCacheOnly is set.
func removeMembersFromGroup(ctx context.Context, store *IAMStoreSys, cache *iamCache, group string, members []string, updateCacheOnly bool) error {
	gi, ok := cache.iamGroupsMap[group]
	if !ok {
		return errNoSuchGroup
	}

	s := set.CreateStringSet(gi.Members...)
	d := set.CreateStringSet(members...)
	gi.Members = s.Difference(d).ToSlice()

	if !updateCacheOnly {
		err := store.saveGroupInfo(ctx, group, gi)
		if err != nil {
			return err
		}
	}
	cache.iamGroupsMap[group] = gi

	// update user-group membership map
	for _, member := range members {
		gset := cache.iamUserGroupMemberships[member]
		if gset == nil {
			continue
		}
		gset.Remove(group)
		cache.iamUserGroupMemberships[member] = gset
	}

	return nil
}

// RemoveUsersFromGroup - removes users from group, deleting it if it is empty.
func (store *IAMStoreSys) RemoveUsersFromGroup(ctx context.Context, group string, members []string) error {
	if group == "" {
		return errInvalidArgument
	}

	cache := store.lock()
	defer store.unlock()

	// Validate that all members exist.
	for _, member := range members {
		cr, ok := cache.iamUsersMap[member]
		if !ok {
			return errNoSuchUser
		}
		if cr.IsTemp() || cr.IsServiceAccount() {
			return errIAMActionNotAllowed
		}
	}

	gi, ok := cache.iamGroupsMap[group]
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
		if err := store.deleteMappedPolicy(ctx, group, regUser, true); err != nil && err != errNoSuchPolicy {
			return err
		}
		if err := store.deleteGroupInfo(ctx, group); err != nil && err != errNoSuchGroup {
			return err
		}

		// Delete from server memory
		delete(cache.iamGroupsMap, group)
		delete(cache.iamGroupPolicyMap, group)
		return nil
	}

	return removeMembersFromGroup(ctx, store, cache, group, members, false)
}

// SetGroupStatus - updates group status
func (store *IAMStoreSys) SetGroupStatus(ctx context.Context, group string, enabled bool) error {
	if group == "" {
		return errInvalidArgument
	}

	cache := store.lock()
	defer store.unlock()

	gi, ok := cache.iamGroupsMap[group]
	if !ok {
		return errNoSuchGroup
	}

	if enabled {
		gi.Status = statusEnabled
	} else {
		gi.Status = statusDisabled
	}

	if err := store.saveGroupInfo(ctx, group, gi); err != nil {
		return err
	}
	cache.iamGroupsMap[group] = gi
	return nil
}

// GetGroupDescription - builds up group description
func (store *IAMStoreSys) GetGroupDescription(group string) (gd madmin.GroupDesc, err error) {
	cache := store.rlock()
	defer store.runlock()

	ps, err := cache.policyDBGet(store.getUsersSysType(), group, true)
	if err != nil {
		return gd, err
	}

	policy := strings.Join(ps, ",")

	if store.getUsersSysType() != MinIOUsersSysType {
		return madmin.GroupDesc{
			Name:   group,
			Policy: policy,
		}, nil
	}

	gi, ok := cache.iamGroupsMap[group]
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

// ListGroups - lists groups. Since this is not going to be a frequent
// operation, we fetch this info from storage, and refresh the cache as well.
func (store *IAMStoreSys) ListGroups(ctx context.Context) (res []string, err error) {
	cache := store.lock()
	defer store.unlock()

	if store.getUsersSysType() == MinIOUsersSysType {
		m := map[string]GroupInfo{}
		err = store.loadGroups(ctx, m)
		if err != nil {
			return
		}
		cache.iamGroupsMap = m

		for k := range cache.iamGroupsMap {
			res = append(res, k)
		}
	}

	if store.getUsersSysType() == LDAPUsersSysType {
		m := map[string]MappedPolicy{}
		err = store.loadMappedPolicies(ctx, stsUser, true, m)
		if err != nil {
			return
		}
		cache.iamGroupPolicyMap = m
		for k := range cache.iamGroupPolicyMap {
			res = append(res, k)
		}
	}

	return
}

// PolicyDBSet - update the policy mapping for the given user or group in
// storage and in cache.
func (store *IAMStoreSys) PolicyDBSet(ctx context.Context, name, policy string, userType IAMUserType, isGroup bool) error {
	if name == "" {
		return errInvalidArgument
	}

	cache := store.lock()
	defer store.unlock()

	// Validate that user and group exist.
	if store.getUsersSysType() == MinIOUsersSysType {
		if !isGroup {
			if _, ok := cache.iamUsersMap[name]; !ok {
				return errNoSuchUser
			}
		} else {
			if _, ok := cache.iamGroupsMap[name]; !ok {
				return errNoSuchGroup
			}
		}
	}

	// Handle policy mapping removal.
	if policy == "" {
		if store.getUsersSysType() == LDAPUsersSysType {
			// Add a fallback removal towards previous content that may come back
			// as a ghost user due to lack of delete, this change occurred
			// introduced in PR #11840
			store.deleteMappedPolicy(ctx, name, regUser, false)
		}
		err := store.deleteMappedPolicy(ctx, name, userType, isGroup)
		if err != nil && err != errNoSuchPolicy {
			return err
		}
		if !isGroup {
			delete(cache.iamUserPolicyMap, name)
		} else {
			delete(cache.iamGroupPolicyMap, name)
		}
		return nil
	}

	// Handle policy mapping set/update
	mp := newMappedPolicy(policy)
	for _, p := range mp.toSlice() {
		if _, found := cache.iamPolicyDocsMap[policy]; !found {
			logger.LogIf(GlobalContext, fmt.Errorf("%w: (%s)", errNoSuchPolicy, p))
			return errNoSuchPolicy
		}
	}

	if err := store.saveMappedPolicy(ctx, name, userType, isGroup, mp); err != nil {
		return err
	}
	if !isGroup {
		cache.iamUserPolicyMap[name] = mp
	} else {
		cache.iamGroupPolicyMap[name] = mp
	}
	return nil

}

// PolicyNotificationHandler - loads given policy from storage. If not present,
// deletes from cache. This notification only reads from storage, and updates
// cache. When the notification is for a policy deletion, it updates the
// user-policy and group-policy maps as well.
func (store *IAMStoreSys) PolicyNotificationHandler(ctx context.Context, policy string) error {
	if policy == "" {
		return errInvalidArgument
	}

	cache := store.lock()
	defer store.unlock()

	err := store.loadPolicyDoc(ctx, policy, cache.iamPolicyDocsMap)
	if err == errNoSuchPolicy {
		// policy was deleted, update cache.
		delete(cache.iamPolicyDocsMap, policy)

		// update user policy map
		for u, mp := range cache.iamUserPolicyMap {
			pset := mp.policySet()
			if !pset.Contains(policy) {
				continue
			}
			_, ok := cache.iamUsersMap[u]
			if !ok {
				// happens when account is deleted or
				// expired.
				delete(cache.iamUserPolicyMap, u)
				continue
			}
			pset.Remove(policy)
			cache.iamUserPolicyMap[u] = newMappedPolicy(strings.Join(pset.ToSlice(), ","))
		}

		// update group policy map
		for g, mp := range cache.iamGroupPolicyMap {
			pset := mp.policySet()
			if !pset.Contains(policy) {
				continue
			}
			pset.Remove(policy)
			cache.iamGroupPolicyMap[g] = newMappedPolicy(strings.Join(pset.ToSlice(), ","))
		}

		return nil
	}
	return err
}

// DeletePolicy - deletes policy from storage and cache.
func (store *IAMStoreSys) DeletePolicy(ctx context.Context, policy string) error {
	if policy == "" {
		return errInvalidArgument
	}

	cache := store.lock()
	defer store.unlock()

	// Check if policy is mapped to any existing user or group.
	users := []string{}
	groups := []string{}
	for u, mp := range cache.iamUserPolicyMap {
		pset := mp.policySet()
		if _, ok := cache.iamUsersMap[u]; !ok {
			// This case can happen when a temporary account is
			// deleted or expired - remove it from userPolicyMap.
			delete(cache.iamUserPolicyMap, u)
			continue
		}
		if pset.Contains(policy) {
			users = append(users, u)
		}
	}
	for g, mp := range cache.iamGroupPolicyMap {
		pset := mp.policySet()
		if pset.Contains(policy) {
			groups = append(groups, g)
		}
	}
	if len(users) != 0 || len(groups) != 0 {
		// error out when a policy could not be deleted as it was in use.
		loggedErr := fmt.Errorf("policy could not be deleted as it is use (users=%s; groups=%s)",
			fmt.Sprintf("[%s]", strings.Join(users, ",")),
			fmt.Sprintf("[%s]", strings.Join(groups, ",")),
		)
		logger.LogIf(GlobalContext, loggedErr)
		return errPolicyInUse
	}

	err := store.deletePolicyDoc(ctx, policy)
	if err == errNoSuchPolicy {
		// Ignore error if policy is already deleted.
		err = nil
	}
	if err != nil {
		return err
	}

	delete(cache.iamPolicyDocsMap, policy)
	return nil
}

// GetPolicy - gets the policy definition. Allows specifying multiple comma
// separated policies - returns a combined policy.
func (store *IAMStoreSys) GetPolicy(name string) (iampolicy.Policy, error) {
	if name == "" {
		return iampolicy.Policy{}, errInvalidArgument
	}

	cache := store.rlock()
	defer store.runlock()

	policies := newMappedPolicy(name).toSlice()
	var combinedPolicy iampolicy.Policy
	for _, policy := range policies {
		if policy == "" {
			continue
		}
		v, ok := cache.iamPolicyDocsMap[policy]
		if !ok {
			return v, errNoSuchPolicy
		}
		combinedPolicy = combinedPolicy.Merge(v)
	}
	return combinedPolicy, nil
}

// SetPolicy - creates a policy with name.
func (store *IAMStoreSys) SetPolicy(ctx context.Context, name string, policy iampolicy.Policy) error {

	if policy.IsEmpty() || name == "" {
		return errInvalidArgument
	}

	cache := store.lock()
	defer store.unlock()

	if err := store.savePolicyDoc(ctx, name, policy); err != nil {
		return err
	}

	cache.iamPolicyDocsMap[name] = policy
	return nil

}

// ListPolicies - fetches all policies from storage and updates cache as well.
// If bucketName is non-empty, returns policies matching the bucket.
func (store *IAMStoreSys) ListPolicies(ctx context.Context, bucketName string) (map[string]iampolicy.Policy, error) {
	cache := store.lock()
	defer store.unlock()

	m := map[string]iampolicy.Policy{}
	err := store.loadPolicyDocs(ctx, m)
	if err != nil {
		return nil, err
	}

	// Sets default canned policies
	setDefaultCannedPolicies(m)

	cache.iamPolicyDocsMap = m

	ret := map[string]iampolicy.Policy{}
	for k, v := range m {
		if bucketName == "" || v.MatchResource(bucketName) {
			ret[k] = v
		}
	}

	return ret, nil
}

// helper function - does not take locks.
func filterPolicies(cache *iamCache, policyName string, bucketName string) (string, iampolicy.Policy) {
	var policies []string
	mp := newMappedPolicy(policyName)
	combinedPolicy := iampolicy.Policy{}
	for _, policy := range mp.toSlice() {
		if policy == "" {
			continue
		}
		p, found := cache.iamPolicyDocsMap[policy]
		if found {
			if bucketName == "" || p.MatchResource(bucketName) {
				policies = append(policies, policy)
				combinedPolicy = combinedPolicy.Merge(p)
			}
		}
	}
	return strings.Join(policies, ","), combinedPolicy
}

// FilterPolicies - accepts a comma separated list of policy names as a string
// and bucket and returns only policies that currently exist in MinIO. If
// bucketName is non-empty, additionally filters policies matching the bucket.
// The first returned value is the list of currently existing policies, and the
// second is their combined policy definition.
func (store *IAMStoreSys) FilterPolicies(policyName string, bucketName string) (string, iampolicy.Policy) {
	cache := store.rlock()
	defer store.runlock()

	return filterPolicies(cache, policyName, bucketName)

}

// GetBucketUsers - returns users (not STS or service accounts) that have access
// to the bucket. User is included even if a group policy that grants access to
// the bucket is disabled.
func (store *IAMStoreSys) GetBucketUsers(bucket string) (map[string]madmin.UserInfo, error) {
	if bucket == "" {
		return nil, errInvalidArgument
	}

	cache := store.rlock()
	defer store.runlock()

	result := map[string]madmin.UserInfo{}
	for k, v := range cache.iamUsersMap {
		if v.IsTemp() || v.IsServiceAccount() {
			continue
		}
		var policies []string
		mp, ok := cache.iamUserPolicyMap[k]
		if ok {
			policies = append(policies, mp.Policies)
			for _, group := range cache.iamUserGroupMemberships[k].ToSlice() {
				if nmp, ok := cache.iamGroupPolicyMap[group]; ok {
					policies = append(policies, nmp.Policies)
				}
			}
		}
		matchedPolicies, _ := filterPolicies(cache, strings.Join(policies, ","), bucket)
		if len(matchedPolicies) > 0 {
			result[k] = madmin.UserInfo{
				PolicyName: matchedPolicies,
				Status: func() madmin.AccountStatus {
					if v.IsValid() {
						return madmin.AccountEnabled
					}
					return madmin.AccountDisabled
				}(),
				MemberOf: cache.iamUserGroupMemberships[k].ToSlice(),
			}
		}
	}

	return result, nil
}

// GetUsers - returns all users (not STS or service accounts).
func (store *IAMStoreSys) GetUsers() map[string]madmin.UserInfo {
	cache := store.rlock()
	defer store.runlock()

	result := map[string]madmin.UserInfo{}
	for k, v := range cache.iamUsersMap {
		if v.IsTemp() || v.IsServiceAccount() {
			continue
		}
		result[k] = madmin.UserInfo{
			PolicyName: cache.iamUserPolicyMap[k].Policies,
			Status: func() madmin.AccountStatus {
				if v.IsValid() {
					return madmin.AccountEnabled
				}
				return madmin.AccountDisabled
			}(),
			MemberOf: cache.iamUserGroupMemberships[k].ToSlice(),
		}
	}

	if store.getUsersSysType() == LDAPUsersSysType {
		for k, v := range cache.iamUserPolicyMap {
			result[k] = madmin.UserInfo{
				PolicyName: v.Policies,
				Status:     madmin.AccountEnabled,
			}
		}
	}

	return result
}

// GetUserInfo - get info on a user.
func (store *IAMStoreSys) GetUserInfo(name string) (u madmin.UserInfo, err error) {
	if name == "" {
		return u, errInvalidArgument
	}

	cache := store.rlock()
	defer store.runlock()

	if store.getUsersSysType() != MinIOUsersSysType {
		// If the user has a mapped policy or is a member of a group, we
		// return that info. Otherwise we return error.
		var groups []string
		for _, v := range cache.iamUsersMap {
			if v.ParentUser == name {
				groups = v.Groups
				break
			}
		}
		mappedPolicy, ok := cache.iamUserPolicyMap[name]
		if !ok {
			return u, errNoSuchUser
		}
		return madmin.UserInfo{
			PolicyName: mappedPolicy.Policies,
			MemberOf:   groups,
		}, nil
	}

	cred, found := cache.iamUsersMap[name]
	if !found {
		return u, errNoSuchUser
	}

	if cred.IsTemp() || cred.IsServiceAccount() {
		return u, errIAMActionNotAllowed
	}

	return madmin.UserInfo{
		PolicyName: cache.iamUserPolicyMap[name].Policies,
		Status: func() madmin.AccountStatus {
			if cred.IsValid() {
				return madmin.AccountEnabled
			}
			return madmin.AccountDisabled
		}(),
		MemberOf: cache.iamUserGroupMemberships[name].ToSlice(),
	}, nil
}

// PolicyMappingNotificationHandler - handles updating a policy mapping from storage.
func (store *IAMStoreSys) PolicyMappingNotificationHandler(ctx context.Context, userOrGroup string, isGroup bool, userType IAMUserType) error {
	if userOrGroup == "" {
		return errInvalidArgument
	}

	cache := store.lock()
	defer store.unlock()

	m := cache.iamGroupPolicyMap
	if !isGroup {
		m = cache.iamUserPolicyMap
	}
	err := store.loadMappedPolicy(ctx, userOrGroup, userType, isGroup, m)
	if err == errNoSuchPolicy {
		// This means that the policy mapping was deleted, so we update
		// the cache.
		delete(m, userOrGroup)
		err = nil
	}
	return err
}

// UserNotificationHandler - handles updating a user/STS account/service account
// from storage.
func (store *IAMStoreSys) UserNotificationHandler(ctx context.Context, accessKey string, userType IAMUserType) error {
	if accessKey == "" {
		return errInvalidArgument
	}

	cache := store.lock()
	defer store.unlock()

	err := store.loadUser(ctx, accessKey, userType, cache.iamUsersMap)
	if err == errNoSuchUser {
		// User was deleted - we update the cache.
		delete(cache.iamUsersMap, accessKey)

		// 1. Start with updating user-group memberships
		if store.getUsersSysType() == MinIOUsersSysType {
			memberOf := cache.iamUserGroupMemberships[accessKey].ToSlice()
			for _, group := range memberOf {
				removeErr := removeMembersFromGroup(ctx, store, cache, group, []string{accessKey}, true)
				if removeErr == errNoSuchGroup {
					removeErr = nil
				}
				if removeErr != nil {
					return removeErr
				}
			}
		}

		// 2. Remove any derived credentials from memory
		if userType == regUser {
			for _, u := range cache.iamUsersMap {
				if u.IsServiceAccount() && u.ParentUser == accessKey {
					delete(cache.iamUsersMap, u.AccessKey)
				}
				if u.IsTemp() && u.ParentUser == accessKey {
					delete(cache.iamUsersMap, u.AccessKey)
				}
			}
		}

		// 3. Delete any mapped policy
		delete(cache.iamUserPolicyMap, accessKey)
		return nil
	}
	if err != nil {
		return err
	}
	if userType != svcUser {
		err = store.loadMappedPolicy(ctx, accessKey, userType, false, cache.iamUserPolicyMap)
		// Ignore policy not mapped error
		if err != nil && err != errNoSuchPolicy {
			return err
		}
	}

	// We are on purpose not persisting the policy map for parent
	// user, although this is a hack, it is a good enough hack
	// at this point in time - we need to overhaul our OIDC
	// usage with service accounts with a more cleaner implementation
	//
	// This mapping is necessary to ensure that valid credentials
	// have necessary ParentUser present - this is mainly for only
	// webIdentity based STS tokens.
	cred, ok := cache.iamUsersMap[accessKey]
	if ok {
		if cred.IsTemp() && cred.ParentUser != "" && cred.ParentUser != globalActiveCred.AccessKey {
			if _, ok := cache.iamUserPolicyMap[cred.ParentUser]; !ok {
				cache.iamUserPolicyMap[cred.ParentUser] = cache.iamUserPolicyMap[accessKey]
			}
		}
	}

	return nil
}

// DeleteUser - deletes a user from storage and cache. This only used with
// long-term users and service accounts, not STS.
func (store *IAMStoreSys) DeleteUser(ctx context.Context, accessKey string, userType IAMUserType) error {
	if accessKey == "" {
		return errInvalidArgument
	}

	cache := store.lock()
	defer store.unlock()

	// first we remove the user from their groups.
	if store.getUsersSysType() == MinIOUsersSysType && userType == regUser {
		memberOf := cache.iamUserGroupMemberships[accessKey].ToSlice()
		for _, group := range memberOf {
			removeErr := removeMembersFromGroup(ctx, store, cache, group, []string{accessKey}, false)
			if removeErr != nil {
				return removeErr
			}
		}
	}

	// Now we can remove the user from memory and IAM store

	// Delete any STS and service account derived from this credential
	// first.
	if userType == regUser {
		for _, u := range cache.iamUsersMap {
			if u.IsServiceAccount() && u.ParentUser == accessKey {
				_ = store.deleteUserIdentity(ctx, u.AccessKey, svcUser)
				delete(cache.iamUsersMap, u.AccessKey)
			}
			// Delete any associated STS users.
			if u.IsTemp() && u.ParentUser == accessKey {
				_ = store.deleteUserIdentity(ctx, u.AccessKey, stsUser)
				delete(cache.iamUsersMap, u.AccessKey)
			}
		}
	}

	// It is ok to ignore deletion error on the mapped policy
	store.deleteMappedPolicy(ctx, accessKey, userType, false)
	delete(cache.iamUserPolicyMap, accessKey)

	err := store.deleteUserIdentity(ctx, accessKey, userType)
	if err == errNoSuchUser {
		// ignore if user is already deleted.
		err = nil
	}
	delete(cache.iamUsersMap, accessKey)

	return err
}

// SetTempUser - saves temporary credential to storage and cache.
func (store *IAMStoreSys) SetTempUser(ctx context.Context, accessKey string, cred auth.Credentials, policyName string) error {
	if accessKey == "" || !cred.IsTemp() || cred.IsExpired() {
		return errInvalidArgument
	}

	ttl := int64(cred.Expiration.Sub(UTCNow()).Seconds())

	cache := store.lock()
	defer store.unlock()

	if policyName != "" {
		mp := newMappedPolicy(policyName)
		_, combinedPolicyStmt := filterPolicies(cache, mp.Policies, "")

		if combinedPolicyStmt.IsEmpty() {
			return fmt.Errorf("specified policy %s, not found %w", policyName, errNoSuchPolicy)
		}

		err := store.saveMappedPolicy(ctx, accessKey, stsUser, false, mp, options{ttl: ttl})
		if err != nil {
			return err
		}

		cache.iamUserPolicyMap[accessKey] = mp

		// We are on purpose not persisting the policy map for parent
		// user, although this is a hack, it is a good enough hack
		// at this point in time - we need to overhaul our OIDC
		// usage with service accounts with a more cleaner implementation
		//
		// This mapping is necessary to ensure that valid credentials
		// have necessary ParentUser present - this is mainly for only
		// webIdentity based STS tokens.
		if cred.ParentUser != "" && cred.ParentUser != globalActiveCred.AccessKey {
			if _, ok := cache.iamUserPolicyMap[cred.ParentUser]; !ok {
				cache.iamUserPolicyMap[cred.ParentUser] = mp
			}
		}
	}

	u := newUserIdentity(cred)
	err := store.saveUserIdentity(context.Background(), accessKey, stsUser, u, options{ttl: ttl})
	if err != nil {
		return err
	}

	cache.iamUsersMap[accessKey] = cred
	return nil
}

// DeleteUsers - given a set of users or access keys, deletes them along with
// any derived credentials (STS or service accounts) and any associated policy
// mappings.
func (store *IAMStoreSys) DeleteUsers(ctx context.Context, users []string) error {
	cache := store.lock()
	defer store.unlock()

	usersToDelete := set.CreateStringSet(users...)
	for user, cred := range cache.iamUsersMap {
		userType := regUser
		if cred.IsServiceAccount() {
			userType = svcUser
		} else if cred.IsTemp() {
			userType = stsUser
		}

		if usersToDelete.Contains(user) || usersToDelete.Contains(cred.ParentUser) {
			// Delete this user account and its policy mapping
			store.deleteMappedPolicy(ctx, user, userType, false)
			delete(cache.iamUserPolicyMap, user)

			// we are only logging errors, not handling them.
			err := store.deleteUserIdentity(ctx, user, userType)
			logger.LogIf(GlobalContext, err)
			delete(cache.iamUsersMap, user)
		}
	}

	return nil
}

// GetAllParentUsers - returns all distinct "parent-users" associated with STS or service
// credentials.
func (store *IAMStoreSys) GetAllParentUsers() []string {
	cache := store.rlock()
	defer store.runlock()

	res := set.NewStringSet()
	for _, cred := range cache.iamUsersMap {
		if cred.IsServiceAccount() || cred.IsTemp() {
			res.Add(cred.ParentUser)
		}
	}

	return res.ToSlice()
}

// SetUserStatus - sets current user status.
func (store *IAMStoreSys) SetUserStatus(ctx context.Context, accessKey string, status madmin.AccountStatus) error {
	if accessKey != "" && status != madmin.AccountEnabled && status != madmin.AccountDisabled {
		return errInvalidArgument
	}

	cache := store.lock()
	defer store.unlock()

	cred, ok := cache.iamUsersMap[accessKey]
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

	if err := store.saveUserIdentity(ctx, accessKey, regUser, uinfo); err != nil {
		return err
	}

	cache.iamUsersMap[accessKey] = uinfo.Credentials
	return nil
}

// AddServiceAccount - add a new service account
func (store *IAMStoreSys) AddServiceAccount(ctx context.Context, cred auth.Credentials) error {
	cache := store.lock()
	defer store.unlock()

	accessKey := cred.AccessKey
	parentUser := cred.ParentUser

	// Found newly requested service account, to be an existing account -
	// reject such operation (updates to the service account are handled in
	// a different API).
	if _, found := cache.iamUsersMap[accessKey]; found {
		return errIAMActionNotAllowed
	}

	// Parent user must not be a service account.
	if cr, found := cache.iamUsersMap[parentUser]; found && cr.IsServiceAccount() {
		return errIAMActionNotAllowed
	}

	// Check that at least one policy is available.
	policies, err := cache.policyDBGet(store.getUsersSysType(), parentUser, false)
	if err != nil {
		return err
	}
	for _, group := range cred.Groups {
		gp, err := cache.policyDBGet(store.getUsersSysType(), group, true)
		if err != nil && err != errNoSuchGroup {
			return err
		}
		policies = append(policies, gp...)
	}
	if len(policies) == 0 {
		return errNoSuchUser
	}

	u := newUserIdentity(cred)
	err = store.saveUserIdentity(ctx, u.Credentials.AccessKey, svcUser, u)
	if err != nil {
		return err
	}

	cache.iamUsersMap[u.Credentials.AccessKey] = u.Credentials

	return nil
}

// UpdateServiceAccount - updates a service account on storage.
func (store *IAMStoreSys) UpdateServiceAccount(ctx context.Context, accessKey string, opts updateServiceAccountOpts) error {
	cache := store.lock()
	defer store.unlock()

	cr, ok := cache.iamUsersMap[accessKey]
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
	if err := store.saveUserIdentity(ctx, u.Credentials.AccessKey, svcUser, u); err != nil {
		return err
	}

	cache.iamUsersMap[u.Credentials.AccessKey] = u.Credentials

	return nil
}

// ListServiceAccounts - lists only service accounts from the cache.
func (store *IAMStoreSys) ListServiceAccounts(ctx context.Context, accessKey string) ([]auth.Credentials, error) {
	cache := store.rlock()
	defer store.runlock()

	var serviceAccounts []auth.Credentials
	for _, v := range cache.iamUsersMap {
		if v.IsServiceAccount() && v.ParentUser == accessKey {
			// Hide secret key & session key here
			v.SecretKey = ""
			v.SessionToken = ""
			serviceAccounts = append(serviceAccounts, v)
		}
	}

	return serviceAccounts, nil
}

// AddUser - adds/updates long term user account to storage.
func (store *IAMStoreSys) AddUser(ctx context.Context, accessKey string, uinfo madmin.UserInfo) error {
	cache := store.lock()
	defer store.unlock()

	cr, ok := cache.iamUsersMap[accessKey]

	// It is not possible to update an STS account.
	if ok && cr.IsTemp() {
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

	if err := store.saveUserIdentity(ctx, accessKey, regUser, u); err != nil {
		return err
	}

	cache.iamUsersMap[accessKey] = u.Credentials

	// Set policy if specified.
	if uinfo.PolicyName != "" {
		policy := uinfo.PolicyName
		// Handle policy mapping set/update
		mp := newMappedPolicy(policy)
		for _, p := range mp.toSlice() {
			if _, found := cache.iamPolicyDocsMap[policy]; !found {
				logger.LogIf(GlobalContext, fmt.Errorf("%w: (%s)", errNoSuchPolicy, p))
				return errNoSuchPolicy
			}
		}

		if err := store.saveMappedPolicy(ctx, accessKey, regUser, false, mp); err != nil {
			return err
		}
		cache.iamUserPolicyMap[accessKey] = mp
	}
	return nil

}

// UpdateUserSecretKey - sets user secret key to storage.
func (store *IAMStoreSys) UpdateUserSecretKey(ctx context.Context, accessKey, secretKey string) error {
	cache := store.lock()
	defer store.unlock()

	cred, ok := cache.iamUsersMap[accessKey]
	if !ok {
		return errNoSuchUser
	}

	cred.SecretKey = secretKey
	u := newUserIdentity(cred)
	if err := store.saveUserIdentity(ctx, accessKey, regUser, u); err != nil {
		return err
	}

	cache.iamUsersMap[accessKey] = cred
	return nil
}

// GetSTSAndServiceAccounts - returns all STS and Service account credentials.
func (store *IAMStoreSys) GetSTSAndServiceAccounts() []auth.Credentials {
	cache := store.rlock()
	defer store.runlock()

	var res []auth.Credentials
	for _, cred := range cache.iamUsersMap {
		if cred.IsTemp() || cred.IsServiceAccount() {
			res = append(res, cred)
		}
	}
	return res
}

// UpdateUserIdentity - updates a user credential.
func (store *IAMStoreSys) UpdateUserIdentity(ctx context.Context, cred auth.Credentials) error {
	cache := store.lock()
	defer store.unlock()

	userType := regUser
	if cred.IsServiceAccount() {
		userType = svcUser
	} else if cred.IsTemp() {
		userType = stsUser
	}

	// Overwrite the user identity here. As store should be
	// atomic, it shouldn't cause any corruption.
	if err := store.saveUserIdentity(ctx, cred.AccessKey, userType, newUserIdentity(cred)); err != nil {
		return err
	}
	cache.iamUsersMap[cred.AccessKey] = cred
	return nil
}

// LoadUser - attempts to load user info from storage and updates cache.
func (store *IAMStoreSys) LoadUser(ctx context.Context, accessKey string) {
	cache := store.lock()
	defer store.unlock()

	_, found := cache.iamUsersMap[accessKey]
	if !found {
		store.loadUser(ctx, accessKey, regUser, cache.iamUsersMap)
		if _, found = cache.iamUsersMap[accessKey]; found {
			// load mapped policies
			store.loadMappedPolicy(ctx, accessKey, regUser, false, cache.iamUserPolicyMap)
		} else {
			// check for service account
			store.loadUser(ctx, accessKey, svcUser, cache.iamUsersMap)
			if svc, found := cache.iamUsersMap[accessKey]; found {
				// Load parent user and mapped policies.
				if store.getUsersSysType() == MinIOUsersSysType {
					store.loadUser(ctx, svc.ParentUser, regUser, cache.iamUsersMap)
				}
				store.loadMappedPolicy(ctx, svc.ParentUser, regUser, false, cache.iamUserPolicyMap)
			} else {
				// check for STS account
				store.loadUser(ctx, accessKey, stsUser, cache.iamUsersMap)
				if _, found = cache.iamUsersMap[accessKey]; found {
					// Load mapped policy
					store.loadMappedPolicy(ctx, accessKey, stsUser, false, cache.iamUserPolicyMap)
				}
			}
		}
	}

	// Load any associated policy definitions
	for _, policy := range cache.iamUserPolicyMap[accessKey].toSlice() {
		if _, found = cache.iamPolicyDocsMap[policy]; !found {
			store.loadPolicyDoc(ctx, policy, cache.iamPolicyDocsMap)
		}
	}
}
