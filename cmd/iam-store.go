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
	"sort"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/config/identity/openid"
	"github.com/minio/minio/internal/jwt"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v2/policy"
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

	minServiceAccountExpiry time.Duration = 15 * time.Minute
	maxServiceAccountExpiry time.Duration = 365 * 24 * time.Hour
)

var errInvalidSvcAcctExpiration = errors.New("invalid service account expiration")

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

func saveIAMFormat(ctx context.Context, store IAMStorageAPI) error {
	bootstrapTraceMsg("Load IAM format file")
	var iamFmt iamFormat
	path := getIAMFormatFilePath()
	if err := store.loadIAMConfig(ctx, &iamFmt, path); err != nil && !errors.Is(err, errConfigNotFound) {
		// if IAM format
		return err
	}

	if iamFmt.Version >= iamFormatVersion1 {
		// Nothing to do.
		return nil
	}

	bootstrapTraceMsg("Write IAM format file")
	// Save iam format to version 1.
	return store.saveIAMConfig(ctx, newIAMFormatVersion1(), path)
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
	UpdatedAt   time.Time        `json:"updatedAt,omitempty"`
}

func newUserIdentity(cred auth.Credentials) UserIdentity {
	return UserIdentity{Version: 1, Credentials: cred, UpdatedAt: UTCNow()}
}

// GroupInfo contains info about a group
type GroupInfo struct {
	Version   int       `json:"version"`
	Status    string    `json:"status"`
	Members   []string  `json:"members"`
	UpdatedAt time.Time `json:"updatedAt,omitempty"`
}

func newGroupInfo(members []string) GroupInfo {
	return GroupInfo{Version: 1, Status: statusEnabled, Members: members, UpdatedAt: UTCNow()}
}

// MappedPolicy represents a policy name mapped to a user or group
type MappedPolicy struct {
	Version   int       `json:"version"`
	Policies  string    `json:"policy"`
	UpdatedAt time.Time `json:"updatedAt,omitempty"`
}

// converts a mapped policy into a slice of distinct policies
func (mp MappedPolicy) toSlice() []string {
	var policies []string
	for _, policy := range strings.Split(mp.Policies, ",") {
		if strings.TrimSpace(policy) == "" {
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
	return MappedPolicy{Version: 1, Policies: policy, UpdatedAt: UTCNow()}
}

// PolicyDoc represents an IAM policy with some metadata.
type PolicyDoc struct {
	Version    int `json:",omitempty"`
	Policy     policy.Policy
	CreateDate time.Time `json:",omitempty"`
	UpdateDate time.Time `json:",omitempty"`
}

func newPolicyDoc(p policy.Policy) PolicyDoc {
	now := UTCNow().Round(time.Millisecond)
	return PolicyDoc{
		Version:    1,
		Policy:     p,
		CreateDate: now,
		UpdateDate: now,
	}
}

// defaultPolicyDoc - used to wrap a default policy as PolicyDoc.
func defaultPolicyDoc(p policy.Policy) PolicyDoc {
	return PolicyDoc{
		Version: 1,
		Policy:  p,
	}
}

func (d *PolicyDoc) update(p policy.Policy) {
	now := UTCNow().Round(time.Millisecond)
	d.UpdateDate = now
	if d.CreateDate.IsZero() {
		d.CreateDate = now
	}
	d.Policy = p
}

// parseJSON parses both the old and the new format for storing policy
// definitions.
//
// The on-disk format of policy definitions has changed (around early 12/2021)
// from policy.Policy to PolicyDoc. To avoid a migration, loading supports
// both the old and the new formats.
func (d *PolicyDoc) parseJSON(data []byte) error {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	var doc PolicyDoc
	err := json.Unmarshal(data, &doc)
	if err != nil {
		err2 := json.Unmarshal(data, &doc.Policy)
		if err2 != nil {
			// Just return the first error.
			return err
		}
		d.Policy = doc.Policy
		return nil
	}
	*d = doc
	return nil
}

// key options
type options struct {
	ttl int64 // expiry in seconds
}

type iamWatchEvent struct {
	isCreated bool // !isCreated implies a delete event.
	keyPath   string
}

// iamCache contains in-memory cache of IAM data.
type iamCache struct {
	updatedAt time.Time

	// map of policy names to policy definitions
	iamPolicyDocsMap map[string]PolicyDoc

	// map of regular username to credentials
	iamUsersMap map[string]UserIdentity
	// map of regular username to policy names
	iamUserPolicyMap map[string]MappedPolicy

	// STS accounts are loaded on demand and not via the periodic IAM reload.
	// map of STS access key to credentials
	iamSTSAccountsMap map[string]UserIdentity
	// map of STS access key to policy names
	iamSTSPolicyMap map[string]MappedPolicy

	// map of group names to group info
	iamGroupsMap map[string]GroupInfo
	// map of user names to groups they are a member of
	iamUserGroupMemberships map[string]set.StringSet
	// map of group names to policy names
	iamGroupPolicyMap map[string]MappedPolicy
	// map of lowercase username to original case username
	iamUserLowercaseMap map[string]string
	// map of lowercase group name to original case group name
	iamGroupLowercaseMap map[string]string
}

func newIamCache() *iamCache {
	return &iamCache{
		iamPolicyDocsMap:        map[string]PolicyDoc{},
		iamUsersMap:             map[string]UserIdentity{},
		iamUserPolicyMap:        map[string]MappedPolicy{},
		iamSTSAccountsMap:       map[string]UserIdentity{},
		iamSTSPolicyMap:         map[string]MappedPolicy{},
		iamGroupsMap:            map[string]GroupInfo{},
		iamUserGroupMemberships: map[string]set.StringSet{},
		iamGroupPolicyMap:       map[string]MappedPolicy{},
		iamUserLowercaseMap:     map[string]string{},
		iamGroupLowercaseMap:    map[string]string{},
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
func (c *iamCache) policyDBGet(store *IAMStoreSys, name string, isGroup bool) ([]string, time.Time, error) {
	if isGroup {
		if store.getUsersSysType() == MinIOUsersSysType {
			g, ok := c.iamGroupsMap[name]
			if !ok {
				return nil, time.Time{}, errNoSuchGroup
			}

			// Group is disabled, so we return no policy - this
			// ensures the request is denied.
			if g.Status == statusDisabled {
				return nil, time.Time{}, nil
			}
		}

		return c.iamGroupPolicyMap[name].toSlice(), c.iamGroupPolicyMap[name].UpdatedAt, nil
	}

	// When looking for a user's policies, we also check if the user
	// and the groups they are member of are enabled.
	u, ok := c.iamUsersMap[name]
	if ok {
		if !u.Credentials.IsValid() {
			return nil, time.Time{}, nil
		}
	}

	// For internal IDP regular/service account user accounts, the policy
	// mapping is iamUserPolicyMap. For STS accounts, the parent user would be
	// passed here and we lookup the mapping in iamSTSPolicyMap.
	mp, ok := c.iamUserPolicyMap[name]
	if !ok {
		// Since user "name" could be a parent user of an STS account, we lookup
		// mappings for those too.
		mp, ok = c.iamSTSPolicyMap[name]
		if !ok {
			// Attempt to load parent user mapping for STS accounts
			store.loadMappedPolicy(context.TODO(), name, stsUser, false, c.iamSTSPolicyMap)
			mp = c.iamSTSPolicyMap[name]
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

	return policies, mp.UpdatedAt, nil
}

func (c *iamCache) updateUserWithClaims(key string, u UserIdentity) error {
	if u.Credentials.SessionToken != "" {
		jwtClaims, err := extractJWTClaims(u)
		if err != nil {
			return err
		}
		u.Credentials.Claims = jwtClaims.Map()
	}
	if u.Credentials.IsTemp() && !u.Credentials.IsServiceAccount() {
		c.iamSTSAccountsMap[key] = u
	} else {
		c.iamUsersMap[key] = u
	}
	c.updatedAt = time.Now()
	return nil
}

// loadCorrectCase - loads the correct capitalization of user or group name from storage.
// Returns input if username or groupname is not found, or if the users system is not LDAP.
// found is false only if the input is not found and the lowercased version is not found either.
//
// Assumes lock is held by caller.
func (c *iamCache) loadCorrectCase(store *IAMStoreSys, accessKey string, isGroup bool) string {
	if store.getUsersSysType() != LDAPUsersSysType {
		return accessKey
	}

	if isGroup {
		if _, ok := c.iamGroupPolicyMap[accessKey]; ok {
			return accessKey
		}
		if group, ok := c.iamGroupLowercaseMap[strings.ToLower(accessKey)]; ok {
			return group
		}
	} else {
		if _, ok := c.iamUsersMap[accessKey]; ok {
			return accessKey
		}
		if user, ok := c.iamUserLowercaseMap[strings.ToLower(accessKey)]; ok {
			return user
		}
	}
	return accessKey
}

// setCorrectCase - sets correct capitalization of user or group name in storage.
// Only sets value if the lowercased version does not already exist.
// Does nothing if the users system is not LDAP.
//
// Assumes lock is held by caller.
func (c *iamCache) setCorrectCase(store *IAMStoreSys, accessKey string, isGroup bool) {
	if store.getUsersSysType() != LDAPUsersSysType {
		return
	}

	lower := strings.ToLower(accessKey)

	if isGroup {
		if _, ok := c.iamGroupPolicyMap[accessKey]; ok {
			c.iamGroupLowercaseMap[lower] = accessKey
			c.updatedAt = time.Now()
		}
	} else {
		if _, ok := c.iamUserLowercaseMap[lower]; !ok {
			c.iamUserLowercaseMap[lower] = accessKey
			c.updatedAt = time.Now()
		}
	}
}

// deleteCorrectCase - deletes correct capitalization of user or group name from storage.
// Does nothing if the users system is not LDAP or if the input is not found.
// Only deletes if the given accesKey matches the correct case
//
// Assumes lock is held by caller.
func (c *iamCache) deleteCorrectCase(store *IAMStoreSys, accessKey string, isGroup bool) {
	if store.getUsersSysType() != LDAPUsersSysType {
		return
	}

	lower := strings.ToLower(accessKey)

	if isGroup {
		if accessKey == c.iamGroupLowercaseMap[accessKey] {
			delete(c.iamGroupLowercaseMap, lower)
			c.updatedAt = time.Now()
		}
	} else {
		if accessKey == c.iamUserLowercaseMap[accessKey] {
			delete(c.iamUserLowercaseMap, lower)
			c.updatedAt = time.Now()
		}
	}
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
	getUsersSysType() UsersSysType
	loadPolicyDoc(ctx context.Context, policy string, m map[string]PolicyDoc) error
	loadPolicyDocs(ctx context.Context, m map[string]PolicyDoc) error
	loadUser(ctx context.Context, user string, userType IAMUserType, m map[string]UserIdentity) error
	loadUsers(ctx context.Context, userType IAMUserType, m map[string]UserIdentity) error
	loadGroup(ctx context.Context, group string, m map[string]GroupInfo) error
	loadGroups(ctx context.Context, m map[string]GroupInfo) error
	loadMappedPolicy(ctx context.Context, name string, userType IAMUserType, isGroup bool, m map[string]MappedPolicy) error
	loadMappedPolicies(ctx context.Context, userType IAMUserType, isGroup bool, m map[string]MappedPolicy) error
	saveIAMConfig(ctx context.Context, item interface{}, path string, opts ...options) error
	loadIAMConfig(ctx context.Context, item interface{}, path string) error
	deleteIAMConfig(ctx context.Context, path string) error
	savePolicyDoc(ctx context.Context, policyName string, p PolicyDoc) error
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
func setDefaultCannedPolicies(policies map[string]PolicyDoc) {
	for _, v := range policy.DefaultPolicies {
		if _, ok := policies[v.Name]; !ok {
			policies[v.Name] = defaultPolicyDoc(v.Definition)
		}
	}
}

// PurgeExpiredSTS - purges expired STS credentials.
func (store *IAMStoreSys) PurgeExpiredSTS(ctx context.Context) error {
	iamOS, ok := store.IAMStorageAPI.(*IAMObjectStore)
	if !ok {
		// No purging is done for non-object storage.
		return nil
	}
	return iamOS.PurgeExpiredSTS(ctx)
}

// LoadIAMCache reads all IAM items and populates a new iamCache object and
// replaces the in-memory cache object.
func (store *IAMStoreSys) LoadIAMCache(ctx context.Context, firstTime bool) error {
	bootstrapTraceMsg := func(s string) {
		if firstTime {
			bootstrapTraceMsg(s)
		}
	}
	bootstrapTraceMsg("loading IAM data")

	newCache := newIamCache()

	loadedAt := time.Now()

	if iamOS, ok := store.IAMStorageAPI.(*IAMObjectStore); ok {
		err := iamOS.loadAllFromObjStore(ctx, newCache)
		if err != nil {
			return err
		}
	} else {

		bootstrapTraceMsg("loading policy documents")
		if err := store.loadPolicyDocs(ctx, newCache.iamPolicyDocsMap); err != nil {
			return err
		}

		// Sets default canned policies, if none are set.
		setDefaultCannedPolicies(newCache.iamPolicyDocsMap)

		if store.getUsersSysType() == MinIOUsersSysType {
			bootstrapTraceMsg("loading regular users")
			if err := store.loadUsers(ctx, regUser, newCache.iamUsersMap); err != nil {
				return err
			}
			bootstrapTraceMsg("loading regular groups")
			if err := store.loadGroups(ctx, newCache.iamGroupsMap); err != nil {
				return err
			}
		}

		bootstrapTraceMsg("loading user policy mapping")
		// load polices mapped to users
		if err := store.loadMappedPolicies(ctx, regUser, false, newCache.iamUserPolicyMap); err != nil {
			return err
		}

		bootstrapTraceMsg("loading group policy mapping")
		// load policies mapped to groups
		if err := store.loadMappedPolicies(ctx, regUser, true, newCache.iamGroupPolicyMap); err != nil {
			return err
		}

		bootstrapTraceMsg("loading service accounts")
		// load service accounts
		if err := store.loadUsers(ctx, svcUser, newCache.iamUsersMap); err != nil {
			return err
		}

		newCache.buildUserGroupMemberships()
	}

	cache := store.lock()
	defer store.unlock()

	// We should only update the in-memory cache if there were no changes
	// to the in-memory cache since the disk loading began. If there
	// were changes to the in-memory cache we should wait for the next
	// cycle until we can safely update the in-memory cache.
	//
	// An in-memory cache must be replaced only if we know for sure that the
	// values loaded from disk are not stale. They might be stale if the
	// cached.updatedAt is more recent than the refresh cycle began.
	if cache.updatedAt.Before(loadedAt) {
		// No one has updated anything since the config was loaded,
		// so we just replace whatever is on the disk into memory.
		cache.iamGroupPolicyMap = newCache.iamGroupPolicyMap
		cache.iamGroupsMap = newCache.iamGroupsMap
		cache.iamPolicyDocsMap = newCache.iamPolicyDocsMap
		cache.iamUserGroupMemberships = newCache.iamUserGroupMemberships
		cache.iamUserPolicyMap = newCache.iamUserPolicyMap
		cache.iamUsersMap = newCache.iamUsersMap
		cache.iamGroupLowercaseMap = newCache.iamGroupLowercaseMap
		cache.iamUserLowercaseMap = newCache.iamUserLowercaseMap
		// For STS policy map, we need to merge the new cache with the existing
		// cache because the periodic IAM reload is partial. The periodic load
		// here is to account for STS policy mapping changes that should apply
		// for service accounts derived from such STS accounts (i.e. LDAP STS
		// accounts).
		for k, v := range newCache.iamSTSPolicyMap {
			cache.iamSTSPolicyMap[k] = v
		}

		cache.updatedAt = time.Now()
	}

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
func (store *IAMStoreSys) GetUser(user string) (UserIdentity, bool) {
	cache := store.rlock()
	defer store.runlock()

	u, ok := cache.iamUsersMap[user]
	if !ok {
		// Check the sts map
		u, ok = cache.iamSTSAccountsMap[user]
	}
	return u, ok
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

		cache.updatedAt = time.Now()
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
	cache.updatedAt = time.Now()
	return nil
}

// PolicyDBGet - fetches policies associated with the given user or group, and
// additional groups if provided.
func (store *IAMStoreSys) PolicyDBGet(name string, groups ...string) ([]string, error) {
	if name == "" {
		return nil, errInvalidArgument
	}

	cache := store.rlock()
	defer store.runlock()

	policies, _, err := cache.policyDBGet(store, name, false)
	if err != nil {
		return nil, err
	}

	for _, group := range groups {
		ps, _, err := cache.policyDBGet(store, group, true)
		if err != nil {
			return nil, err
		}
		policies = append(policies, ps...)
	}

	return policies, nil
}

// AddUsersToGroup - adds users to group, creating the group if needed.
func (store *IAMStoreSys) AddUsersToGroup(ctx context.Context, group string, members []string) (updatedAt time.Time, err error) {
	if group == "" {
		return updatedAt, errInvalidArgument
	}

	cache := store.lock()
	defer store.unlock()

	// Validate that all members exist.
	for _, member := range members {
		u, ok := cache.iamUsersMap[member]
		if !ok {
			return updatedAt, errNoSuchUser
		}
		cr := u.Credentials
		if cr.IsTemp() || cr.IsServiceAccount() {
			return updatedAt, errIAMActionNotAllowed
		}
	}

	gi, ok := cache.iamGroupsMap[group]
	if !ok {
		// Set group as enabled by default when it doesn't
		// exist.
		gi = newGroupInfo(members)
	} else {
		gi.Members = set.CreateStringSet(append(gi.Members, members...)...).ToSlice()
		gi.UpdatedAt = UTCNow()
	}

	if err := store.saveGroupInfo(ctx, group, gi); err != nil {
		return updatedAt, err
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

	cache.updatedAt = time.Now()
	return gi.UpdatedAt, nil
}

// helper function - does not take any locks. Updates only cache if
// updateCacheOnly is set.
func removeMembersFromGroup(ctx context.Context, store *IAMStoreSys, cache *iamCache, group string, members []string, updateCacheOnly bool) (updatedAt time.Time, err error) {
	gi, ok := cache.iamGroupsMap[group]
	if !ok {
		return updatedAt, errNoSuchGroup
	}

	s := set.CreateStringSet(gi.Members...)
	d := set.CreateStringSet(members...)
	gi.Members = s.Difference(d).ToSlice()

	if !updateCacheOnly {
		err := store.saveGroupInfo(ctx, group, gi)
		if err != nil {
			return updatedAt, err
		}
	}
	gi.UpdatedAt = UTCNow()
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

	cache.updatedAt = time.Now()
	return gi.UpdatedAt, nil
}

// RemoveUsersFromGroup - removes users from group, deleting it if it is empty.
func (store *IAMStoreSys) RemoveUsersFromGroup(ctx context.Context, group string, members []string) (updatedAt time.Time, err error) {
	if group == "" {
		return updatedAt, errInvalidArgument
	}

	cache := store.lock()
	defer store.unlock()

	// Validate that all members exist.
	for _, member := range members {
		u, ok := cache.iamUsersMap[member]
		if !ok {
			return updatedAt, errNoSuchUser
		}
		cr := u.Credentials
		if cr.IsTemp() || cr.IsServiceAccount() {
			return updatedAt, errIAMActionNotAllowed
		}
	}

	gi, ok := cache.iamGroupsMap[group]
	if !ok {
		return updatedAt, errNoSuchGroup
	}

	// Check if attempting to delete a non-empty group.
	if len(members) == 0 && len(gi.Members) != 0 {
		return updatedAt, errGroupNotEmpty
	}

	if len(members) == 0 {
		// len(gi.Members) == 0 here.

		// Remove the group from storage. First delete the
		// mapped policy. No-mapped-policy case is ignored.
		if err := store.deleteMappedPolicy(ctx, group, regUser, true); err != nil && !errors.Is(err, errNoSuchPolicy) {
			return updatedAt, err
		}
		if err := store.deleteGroupInfo(ctx, group); err != nil && err != errNoSuchGroup {
			return updatedAt, err
		}

		// Delete from server memory
		delete(cache.iamGroupsMap, group)
		delete(cache.iamGroupPolicyMap, group)
		cache.updatedAt = time.Now()
		return cache.updatedAt, nil
	}

	return removeMembersFromGroup(ctx, store, cache, group, members, false)
}

// SetGroupStatus - updates group status
func (store *IAMStoreSys) SetGroupStatus(ctx context.Context, group string, enabled bool) (updatedAt time.Time, err error) {
	if group == "" {
		return updatedAt, errInvalidArgument
	}

	cache := store.lock()
	defer store.unlock()

	gi, ok := cache.iamGroupsMap[group]
	if !ok {
		return updatedAt, errNoSuchGroup
	}

	if enabled {
		gi.Status = statusEnabled
	} else {
		gi.Status = statusDisabled
	}
	gi.UpdatedAt = UTCNow()
	if err := store.saveGroupInfo(ctx, group, gi); err != nil {
		return gi.UpdatedAt, err
	}

	cache.iamGroupsMap[group] = gi
	cache.updatedAt = time.Now()

	return gi.UpdatedAt, nil
}

// GetGroupDescription - builds up group description
func (store *IAMStoreSys) GetGroupDescription(group string) (gd madmin.GroupDesc, err error) {
	cache := store.rlock()
	defer store.runlock()

	ps, updatedAt, err := cache.policyDBGet(store, group, true)
	if err != nil {
		return gd, err
	}

	policy := strings.Join(ps, ",")

	if store.getUsersSysType() != MinIOUsersSysType {
		return madmin.GroupDesc{
			Name:      group,
			Policy:    policy,
			UpdatedAt: updatedAt,
		}, nil
	}

	gi, ok := cache.iamGroupsMap[group]
	if !ok {
		return gd, errNoSuchGroup
	}

	return madmin.GroupDesc{
		Name:      group,
		Status:    gi.Status,
		Members:   gi.Members,
		Policy:    policy,
		UpdatedAt: gi.UpdatedAt,
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
		cache.updatedAt = time.Now()
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
		cache.updatedAt = time.Now()
		for k := range cache.iamGroupPolicyMap {
			res = append(res, k)
		}
	}

	return
}

// listGroups - lists groups - fetch groups from cache
func (store *IAMStoreSys) listGroups(ctx context.Context) (res []string, err error) {
	cache := store.rlock()
	defer store.runlock()

	if store.getUsersSysType() == MinIOUsersSysType {
		for k := range cache.iamGroupsMap {
			res = append(res, k)
		}
	}

	if store.getUsersSysType() == LDAPUsersSysType {
		for k := range cache.iamGroupPolicyMap {
			res = append(res, k)
		}
	}
	return
}

// PolicyDBUpdate - adds or removes given policies to/from the user or group's
// policy associations.
func (store *IAMStoreSys) PolicyDBUpdate(ctx context.Context, name string, isGroup bool,
	userType IAMUserType, policies []string, isAttach bool) (updatedAt time.Time,
	addedOrRemoved, effectivePolicies []string, err error,
) {
	if name == "" {
		err = errInvalidArgument
		return
	}

	cache := store.lock()
	defer store.unlock()

	if isAttach {
		name = cache.loadCorrectCase(store, name, isGroup)
	}

	var policyMap map[string]MappedPolicy
	if !isGroup {
		if userType == stsUser {
			policyMap = cache.iamSTSPolicyMap
		} else {
			policyMap = cache.iamUserPolicyMap
		}
	} else {
		policyMap = cache.iamGroupPolicyMap
	}

	// Load existing policy mapping
	var mp MappedPolicy
	if !isGroup {
		if userType == stsUser {
			stsMap := map[string]MappedPolicy{}

			// Attempt to load parent user mapping for STS accounts
			e := store.loadMappedPolicy(context.TODO(), name, stsUser, false, stsMap)
			if !isAttach {
				if e != nil {
					// Retry detach with the correct case
					name = cache.loadCorrectCase(store, name, isGroup)
					store.loadMappedPolicy(context.TODO(), name, stsUser, false, stsMap)
				} else if stsMap[name].policySet().IsEmpty() {
					// Mapped policy file exists but is empty: should delete
					store.deleteMappedPolicy(ctx, name, userType, isGroup)
					delete(cache.iamSTSAccountsMap, name)
					name = cache.loadCorrectCase(store, name, isGroup)
					store.loadMappedPolicy(context.TODO(), name, stsUser, false, stsMap)
				}
			}
			mp = stsMap[name]
		} else {
			mp = cache.iamUserPolicyMap[name]
		}
	} else {
		if store.getUsersSysType() == MinIOUsersSysType {
			g, ok := cache.iamGroupsMap[name]
			if !ok {
				err = errNoSuchGroup
				return
			}

			if g.Status == statusDisabled {
				err = errGroupDisabled
				return
			}
		}
		mp = cache.iamGroupPolicyMap[name]
	}

	// Compute net policy change effect and updated policy mapping
	existingPolicySet := mp.policySet()
	policiesToUpdate := set.CreateStringSet(policies...)
	var newPolicySet set.StringSet
	newPolicyMapping := mp
	if isAttach {
		// new policies to attach => inputPolicies - existing (set difference)
		policiesToUpdate = policiesToUpdate.Difference(existingPolicySet)
		// validate that new policies to add are defined.
		for _, p := range policiesToUpdate.ToSlice() {
			if _, found := cache.iamPolicyDocsMap[p]; !found {
				err = errNoSuchPolicy
				return
			}
		}
		newPolicySet = existingPolicySet.Union(policiesToUpdate)
	} else {
		// policies to detach => inputPolicies ∩ existing (intersection)
		policiesToUpdate = policiesToUpdate.Intersection(existingPolicySet)
		newPolicySet = existingPolicySet.Difference(policiesToUpdate)
	}

	// Delete policy and return if the policy no longer exists
	if newPolicySet.IsEmpty() {
		delete(policyMap, name)
		cache.deleteCorrectCase(store, name, isGroup)
		store.deleteMappedPolicy(ctx, name, userType, isGroup)
		if policiesToUpdate.IsEmpty() {
			err = errNoPolicyToAttachOrDetach
		}
		cache.updatedAt = UTCNow()
		updatedAt = cache.updatedAt
		return
	}

	// We return an error if the requested policy update will have no effect.
	if policiesToUpdate.IsEmpty() {
		err = errNoPolicyToAttachOrDetach
		return
	}

	newPolicies := newPolicySet.ToSlice()
	newPolicyMapping.Policies = strings.Join(newPolicies, ",")
	newPolicyMapping.UpdatedAt = UTCNow()
	addedOrRemoved = policiesToUpdate.ToSlice()

	if err = store.saveMappedPolicy(ctx, name, userType, isGroup, newPolicyMapping); err != nil {
		return
	}

	policyMap[name] = newPolicyMapping
	cache.setCorrectCase(store, name, isGroup)

	cache.updatedAt = UTCNow()
	return cache.updatedAt, addedOrRemoved, newPolicies, nil
}

// PolicyDBSet - update the policy mapping for the given user or group in
// storage and in cache. We do not check for the existence of the user here
// since users can be virtual, such as for:
//   - LDAP users
//   - CommonName for STS accounts generated by AssumeRoleWithCertificate
func (store *IAMStoreSys) PolicyDBSet(ctx context.Context, name, policy string, userType IAMUserType, isGroup bool) (updatedAt time.Time, err error) {
	if name == "" {
		return updatedAt, errInvalidArgument
	}

	cache := store.lock()
	defer store.unlock()

	// Handle policy mapping removal.
	if policy == "" {
		if store.getUsersSysType() == LDAPUsersSysType {
			// Add a fallback removal towards previous content that may come back
			// as a ghost user due to lack of delete, this change occurred
			// introduced in PR #11840
			store.deleteMappedPolicy(ctx, name, regUser, false)
		}
		err := store.deleteMappedPolicy(ctx, name, userType, isGroup)
		if err != nil && !errors.Is(err, errNoSuchPolicy) {
			return updatedAt, err
		}
		cache.deleteCorrectCase(store, name, isGroup)
		if !isGroup {
			if userType == stsUser {
				delete(cache.iamSTSPolicyMap, name)
			} else {
				delete(cache.iamUserPolicyMap, name)
			}
		} else {
			delete(cache.iamGroupPolicyMap, name)
		}
		cache.updatedAt = time.Now()
		return cache.updatedAt, nil
	}

	// Handle policy mapping set/update
	mp := newMappedPolicy(policy)
	for _, p := range mp.toSlice() {
		if _, found := cache.iamPolicyDocsMap[p]; !found {
			return updatedAt, errNoSuchPolicy
		}
	}

	if err := store.saveMappedPolicy(ctx, name, userType, isGroup, mp); err != nil {
		return updatedAt, err
	}

	if !isGroup {
		if userType == stsUser {
			cache.iamSTSPolicyMap[name] = mp
		} else {
			cache.iamUserPolicyMap[name] = mp
			cache.setCorrectCase(store, name, false)
		}
	} else {
		cache.iamGroupPolicyMap[name] = mp
		cache.setCorrectCase(store, name, true)
	}
	cache.updatedAt = time.Now()
	return mp.UpdatedAt, nil
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
	if errors.Is(err, errNoSuchPolicy) {
		// policy was deleted, update cache.
		delete(cache.iamPolicyDocsMap, policy)

		// update user policy map
		for u, mp := range cache.iamUserPolicyMap {
			pset := mp.policySet()
			if !pset.Contains(policy) {
				continue
			}
			if store.getUsersSysType() == MinIOUsersSysType {
				_, ok := cache.iamUsersMap[u]
				if !ok {
					// happens when account is deleted or
					// expired.
					delete(cache.iamUserPolicyMap, u)
					continue
				}
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

		cache.updatedAt = time.Now()
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

	// Check if policy is mapped to any existing user or group. If so, we do not
	// allow deletion of the policy. If the policy is mapped to an STS account,
	// we do allow deletion.
	users := []string{}
	groups := []string{}
	for u, mp := range cache.iamUserPolicyMap {
		pset := mp.policySet()
		if store.getUsersSysType() == MinIOUsersSysType {
			if _, ok := cache.iamUsersMap[u]; !ok {
				// This case can happen when a temporary account is
				// deleted or expired - remove it from userPolicyMap.
				delete(cache.iamUserPolicyMap, u)
				continue
			}
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
		return errPolicyInUse
	}

	err := store.deletePolicyDoc(ctx, policy)
	if errors.Is(err, errNoSuchPolicy) {
		// Ignore error if policy is already deleted.
		err = nil
	}
	if err != nil {
		return err
	}

	delete(cache.iamPolicyDocsMap, policy)
	cache.updatedAt = time.Now()

	return nil
}

// GetPolicy - gets the policy definition. Allows specifying multiple comma
// separated policies - returns a combined policy.
func (store *IAMStoreSys) GetPolicy(name string) (policy.Policy, error) {
	if name == "" {
		return policy.Policy{}, errInvalidArgument
	}

	cache := store.rlock()
	defer store.runlock()

	policies := newMappedPolicy(name).toSlice()
	var toMerge []policy.Policy
	for _, policy := range policies {
		if policy == "" {
			continue
		}
		v, ok := cache.iamPolicyDocsMap[policy]
		if !ok {
			return v.Policy, errNoSuchPolicy
		}
		toMerge = append(toMerge, v.Policy)
	}
	if len(toMerge) == 0 {
		return policy.Policy{}, errNoSuchPolicy
	}
	return policy.MergePolicies(toMerge...), nil
}

// GetPolicyDoc - gets the policy doc which has the policy and some metadata.
// Exactly one policy must be specified here.
func (store *IAMStoreSys) GetPolicyDoc(name string) (r PolicyDoc, err error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return r, errInvalidArgument
	}

	cache := store.rlock()
	defer store.runlock()

	v, ok := cache.iamPolicyDocsMap[name]
	if !ok {
		return r, errNoSuchPolicy
	}
	return v, nil
}

// SetPolicy - creates a policy with name.
func (store *IAMStoreSys) SetPolicy(ctx context.Context, name string, policy policy.Policy) (time.Time, error) {
	if policy.IsEmpty() || name == "" {
		return time.Time{}, errInvalidArgument
	}

	cache := store.lock()
	defer store.unlock()

	var (
		d  PolicyDoc
		ok bool
	)
	if d, ok = cache.iamPolicyDocsMap[name]; ok {
		d.update(policy)
	} else {
		d = newPolicyDoc(policy)
	}

	if err := store.savePolicyDoc(ctx, name, d); err != nil {
		return d.UpdateDate, err
	}

	cache.iamPolicyDocsMap[name] = d
	cache.updatedAt = time.Now()

	return d.UpdateDate, nil
}

// ListPolicies - fetches all policies from storage and updates cache as well.
// If bucketName is non-empty, returns policies matching the bucket.
func (store *IAMStoreSys) ListPolicies(ctx context.Context, bucketName string) (map[string]policy.Policy, error) {
	cache := store.lock()
	defer store.unlock()

	m := map[string]PolicyDoc{}
	err := store.loadPolicyDocs(ctx, m)
	if err != nil {
		return nil, err
	}

	// Sets default canned policies
	setDefaultCannedPolicies(m)

	cache.iamPolicyDocsMap = m
	cache.updatedAt = time.Now()

	ret := map[string]policy.Policy{}
	for k, v := range m {
		if bucketName == "" || v.Policy.MatchResource(bucketName) {
			ret[k] = v.Policy
		}
	}

	return ret, nil
}

// ListPolicyDocs - fetches all policy docs from storage and updates cache as well.
// If bucketName is non-empty, returns policy docs matching the bucket.
func (store *IAMStoreSys) ListPolicyDocs(ctx context.Context, bucketName string) (map[string]PolicyDoc, error) {
	cache := store.lock()
	defer store.unlock()

	m := map[string]PolicyDoc{}
	err := store.loadPolicyDocs(ctx, m)
	if err != nil {
		return nil, err
	}

	// Sets default canned policies
	setDefaultCannedPolicies(m)

	cache.iamPolicyDocsMap = m
	cache.updatedAt = time.Now()

	ret := map[string]PolicyDoc{}
	for k, v := range m {
		if bucketName == "" || v.Policy.MatchResource(bucketName) {
			ret[k] = v
		}
	}

	return ret, nil
}

// fetches all policy docs from cache.
// If bucketName is non-empty, returns policy docs matching the bucket.
func (store *IAMStoreSys) listPolicyDocs(ctx context.Context, bucketName string) (map[string]PolicyDoc, error) {
	cache := store.rlock()
	defer store.runlock()
	ret := map[string]PolicyDoc{}
	for k, v := range cache.iamPolicyDocsMap {
		if bucketName == "" || v.Policy.MatchResource(bucketName) {
			ret[k] = v
		}
	}
	return ret, nil
}

// helper function - does not take locks.
func filterPolicies(cache *iamCache, policyName string, bucketName string) (string, policy.Policy) {
	var policies []string
	mp := newMappedPolicy(policyName)
	var toMerge []policy.Policy
	for _, policy := range mp.toSlice() {
		if policy == "" {
			continue
		}
		p, found := cache.iamPolicyDocsMap[policy]
		if !found {
			continue
		}
		if bucketName == "" || p.Policy.MatchResource(bucketName) {
			policies = append(policies, policy)
			toMerge = append(toMerge, p.Policy)
		}
	}
	return strings.Join(policies, ","), policy.MergePolicies(toMerge...)
}

// FilterPolicies - accepts a comma separated list of policy names as a string
// and bucket and returns only policies that currently exist in MinIO. If
// bucketName is non-empty, additionally filters policies matching the bucket.
// The first returned value is the list of currently existing policies, and the
// second is their combined policy definition.
func (store *IAMStoreSys) FilterPolicies(policyName string, bucketName string) (string, policy.Policy) {
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
		c := v.Credentials
		if c.IsTemp() || c.IsServiceAccount() {
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
					if c.IsValid() {
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
	for k, u := range cache.iamUsersMap {
		v := u.Credentials

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
			MemberOf:  cache.iamUserGroupMemberships[k].ToSlice(),
			UpdatedAt: cache.iamUserPolicyMap[k].UpdatedAt,
		}
	}

	return result
}

// GetUsersWithMappedPolicies - safely returns the name of access keys with associated policies
func (store *IAMStoreSys) GetUsersWithMappedPolicies() map[string]string {
	cache := store.rlock()
	defer store.runlock()

	result := make(map[string]string)
	for k, v := range cache.iamUserPolicyMap {
		result[k] = v.Policies
	}
	for k, v := range cache.iamSTSPolicyMap {
		result[k] = v.Policies
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
			if v.Credentials.ParentUser == name {
				groups = v.Credentials.Groups
				break
			}
		}
		for _, v := range cache.iamSTSAccountsMap {
			if v.Credentials.ParentUser == name {
				groups = v.Credentials.Groups
				break
			}
		}
		mappedPolicy, ok := cache.iamUserPolicyMap[name]
		if !ok {
			mappedPolicy, ok = cache.iamSTSPolicyMap[name]
		}
		if !ok {
			// Attempt to load parent user mapping for STS accounts
			store.loadMappedPolicy(context.TODO(), name, stsUser, false, cache.iamSTSPolicyMap)
			mappedPolicy, ok = cache.iamSTSPolicyMap[name]
			if !ok {
				return u, errNoSuchUser
			}
		}

		return madmin.UserInfo{
			PolicyName: mappedPolicy.Policies,
			MemberOf:   groups,
			UpdatedAt:  mappedPolicy.UpdatedAt,
		}, nil
	}

	ui, found := cache.iamUsersMap[name]
	if !found {
		return u, errNoSuchUser
	}
	cred := ui.Credentials
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
		MemberOf:  cache.iamUserGroupMemberships[name].ToSlice(),
		UpdatedAt: cache.iamUserPolicyMap[name].UpdatedAt,
	}, nil
}

// PolicyMappingNotificationHandler - handles updating a policy mapping from storage.
func (store *IAMStoreSys) PolicyMappingNotificationHandler(ctx context.Context, userOrGroup string, isGroup bool, userType IAMUserType) error {
	if userOrGroup == "" {
		return errInvalidArgument
	}

	cache := store.lock()
	defer store.unlock()

	var m map[string]MappedPolicy
	switch {
	case isGroup:
		m = cache.iamGroupPolicyMap
	default:
		m = cache.iamUserPolicyMap
	}
	err := store.loadMappedPolicy(ctx, userOrGroup, userType, isGroup, m)
	if errors.Is(err, errNoSuchPolicy) {
		// This means that the policy mapping was deleted, so we update
		// the cache.
		delete(m, userOrGroup)
		cache.updatedAt = time.Now()

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

	var m map[string]UserIdentity
	switch userType {
	case stsUser:
		m = cache.iamSTSAccountsMap
	default:
		m = cache.iamUsersMap
	}
	err := store.loadUser(ctx, accessKey, userType, m)

	if err == errNoSuchUser {
		// User was deleted - we update the cache.
		delete(m, accessKey)

		// Since cache was updated, we update the timestamp.
		defer func() {
			cache.updatedAt = time.Now()
		}()

		// 1. Start with updating user-group memberships
		if store.getUsersSysType() == MinIOUsersSysType {
			memberOf := cache.iamUserGroupMemberships[accessKey].ToSlice()
			for _, group := range memberOf {
				_, removeErr := removeMembersFromGroup(ctx, store, cache, group, []string{accessKey}, true)
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
			for k, u := range cache.iamUsersMap {
				if u.Credentials.IsServiceAccount() && u.Credentials.ParentUser == accessKey {
					delete(cache.iamUsersMap, k)
				}
			}
			for k, u := range cache.iamSTSAccountsMap {
				if u.Credentials.ParentUser == accessKey {
					delete(cache.iamSTSAccountsMap, k)
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

	// Since cache was updated, we update the timestamp.
	defer func() {
		cache.updatedAt = time.Now()
	}()

	cred := m[accessKey].Credentials
	switch userType {
	case stsUser:
		// For STS accounts a policy is mapped to the parent user (if a mapping exists).
		err = store.loadMappedPolicy(ctx, cred.ParentUser, userType, false, cache.iamSTSPolicyMap)
	case svcUser:
		// For service accounts, the parent may be a regular (internal) IDP
		// user or a "virtual" user (parent of an STS account).
		//
		// If parent is a regular user => policy mapping is done on that parent itself.
		//
		// If parent is "virtual" => policy mapping is done on the virtual
		// parent and that virtual parent is an stsUser.
		//
		// To load the appropriate mapping, we check the parent user type.
		_, parentIsRegularUser := cache.iamUsersMap[cred.ParentUser]
		if parentIsRegularUser {
			err = store.loadMappedPolicy(ctx, cred.ParentUser, regUser, false, cache.iamUserPolicyMap)
		} else {
			err = store.loadMappedPolicy(ctx, cred.ParentUser, stsUser, false, cache.iamSTSPolicyMap)
		}
	case regUser:
		// For regular users, we load the mapped policy.
		err = store.loadMappedPolicy(ctx, accessKey, userType, false, cache.iamUserPolicyMap)
	default:
		// This is just to ensure that we have covered all cases for new
		// code in future.
		panic("unknown user type")
	}
	// Ignore policy not mapped error
	if err != nil && !errors.Is(err, errNoSuchPolicy) {
		return err
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
			_, removeErr := removeMembersFromGroup(ctx, store, cache, group, []string{accessKey}, false)
			if removeErr != nil {
				return removeErr
			}
		}
	}

	// Now we can remove the user from memory and IAM store

	// Delete any STS and service account derived from this credential
	// first.
	if userType == regUser {
		for _, ui := range cache.iamUsersMap {
			u := ui.Credentials
			if u.ParentUser == accessKey {
				switch {
				case u.IsServiceAccount():
					_ = store.deleteUserIdentity(ctx, u.AccessKey, svcUser)
					delete(cache.iamUsersMap, u.AccessKey)
				case u.IsTemp():
					_ = store.deleteUserIdentity(ctx, u.AccessKey, stsUser)
					delete(cache.iamUsersMap, u.AccessKey)
				}
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

	cache.updatedAt = time.Now()

	return err
}

// SetTempUser - saves temporary (STS) credential to storage and cache. If a
// policy name is given, it is associated with the parent user specified in the
// credential.
func (store *IAMStoreSys) SetTempUser(ctx context.Context, accessKey string, cred auth.Credentials, policyName string) (time.Time, error) {
	if accessKey == "" || !cred.IsTemp() || cred.IsExpired() || cred.ParentUser == "" {
		return time.Time{}, errInvalidArgument
	}

	ttl := int64(cred.Expiration.Sub(UTCNow()).Seconds())

	cache := store.lock()
	defer store.unlock()

	if policyName != "" {
		mp := newMappedPolicy(policyName)
		_, combinedPolicyStmt := filterPolicies(cache, mp.Policies, "")

		if combinedPolicyStmt.IsEmpty() {
			return time.Time{}, fmt.Errorf("specified policy %s, not found %w", policyName, errNoSuchPolicy)
		}

		err := store.saveMappedPolicy(ctx, cred.ParentUser, stsUser, false, mp, options{ttl: ttl})
		if err != nil {
			return time.Time{}, err
		}

		cache.iamSTSPolicyMap[cred.ParentUser] = mp
	}

	u := newUserIdentity(cred)
	err := store.saveUserIdentity(ctx, accessKey, stsUser, u, options{ttl: ttl})
	if err != nil {
		return time.Time{}, err
	}

	cache.iamSTSAccountsMap[accessKey] = u
	cache.updatedAt = time.Now()

	return u.UpdatedAt, nil
}

// DeleteUsers - given a set of users or access keys, deletes them along with
// any derived credentials (STS or service accounts) and any associated policy
// mappings.
func (store *IAMStoreSys) DeleteUsers(ctx context.Context, users []string) error {
	cache := store.lock()
	defer store.unlock()

	var deleted bool
	usersToDelete := set.CreateStringSet(users...)
	for user, ui := range cache.iamUsersMap {
		userType := regUser
		cred := ui.Credentials

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

			deleted = true
		}
	}

	if deleted {
		cache.updatedAt = time.Now()
	}

	return nil
}

// ParentUserInfo contains extra info about a the parent user.
type ParentUserInfo struct {
	subClaimValue string
	roleArns      set.StringSet
}

// GetAllParentUsers - returns all distinct "parent-users" associated with STS
// or service credentials, mapped to all distinct roleARNs associated with the
// parent user. The dummy role ARN is associated with parent users from
// policy-claim based OpenID providers.
func (store *IAMStoreSys) GetAllParentUsers() map[string]ParentUserInfo {
	cache := store.rlock()
	defer store.runlock()

	res := map[string]ParentUserInfo{}
	for _, ui := range cache.iamUsersMap {
		cred := ui.Credentials
		// Only consider service account or STS credentials with
		// non-empty session tokens.
		if !(cred.IsServiceAccount() || cred.IsTemp()) ||
			cred.SessionToken == "" {
			continue
		}

		var (
			err    error
			claims map[string]interface{} = cred.Claims
		)

		if cred.IsServiceAccount() {
			claims, err = getClaimsFromTokenWithSecret(cred.SessionToken, cred.SecretKey)
		} else if cred.IsTemp() {
			claims, err = getClaimsFromTokenWithSecret(cred.SessionToken, globalActiveCred.SecretKey)
		}

		if err != nil {
			continue
		}
		if cred.ParentUser == "" {
			continue
		}

		subClaimValue := cred.ParentUser
		if v, ok := claims[subClaim]; ok {
			subFromToken, ok := v.(string)
			if ok {
				subClaimValue = subFromToken
			}
		}

		roleArn := openid.DummyRoleARN.String()
		s, ok := claims[roleArnClaim]
		val, ok2 := s.(string)
		if ok && ok2 {
			roleArn = val
		}
		v, ok := res[cred.ParentUser]
		if ok {
			res[cred.ParentUser] = ParentUserInfo{
				subClaimValue: subClaimValue,
				roleArns:      v.roleArns.Union(set.CreateStringSet(roleArn)),
			}
		} else {
			res[cred.ParentUser] = ParentUserInfo{
				subClaimValue: subClaimValue,
				roleArns:      set.CreateStringSet(roleArn),
			}
		}
	}

	return res
}

// Assumes store is locked by caller. If users is empty, returns all user mappings.
func (store *IAMStoreSys) listUserPolicyMappings(cache *iamCache, users []string,
	userPredicate func(string) bool,
) []madmin.UserPolicyEntities {
	var r []madmin.UserPolicyEntities
	usersSet := set.CreateStringSet(users...)
	for user, mappedPolicy := range cache.iamUserPolicyMap {
		if userPredicate != nil && !userPredicate(user) {
			continue
		}

		if !usersSet.IsEmpty() && !usersSet.Contains(user) {
			continue
		}

		ps := mappedPolicy.toSlice()
		sort.Strings(ps)
		r = append(r, madmin.UserPolicyEntities{
			User:     user,
			Policies: ps,
		})
	}

	stsMap := map[string]MappedPolicy{}
	for _, user := range users {
		// Attempt to load parent user mapping for STS accounts
		store.loadMappedPolicy(context.TODO(), user, stsUser, false, stsMap)
	}

	for user, mappedPolicy := range stsMap {
		if userPredicate != nil && !userPredicate(user) {
			continue
		}

		ps := mappedPolicy.toSlice()
		sort.Strings(ps)
		r = append(r, madmin.UserPolicyEntities{
			User:     user,
			Policies: ps,
		})
	}

	sort.Slice(r, func(i, j int) bool {
		return r[i].User < r[j].User
	})

	return r
}

// Assumes store is locked by caller. If groups is empty, returns all group mappings.
func (store *IAMStoreSys) listGroupPolicyMappings(cache *iamCache, groups []string,
	groupPredicate func(string) bool,
) []madmin.GroupPolicyEntities {
	var r []madmin.GroupPolicyEntities
	groupsSet := set.CreateStringSet(groups...)
	for group, mappedPolicy := range cache.iamGroupPolicyMap {
		if groupPredicate != nil && !groupPredicate(group) {
			continue
		}

		if !groupsSet.IsEmpty() && !groupsSet.Contains(group) {
			continue
		}

		ps := mappedPolicy.toSlice()
		sort.Strings(ps)
		r = append(r, madmin.GroupPolicyEntities{
			Group:    group,
			Policies: ps,
		})
	}

	sort.Slice(r, func(i, j int) bool {
		return r[i].Group < r[j].Group
	})

	return r
}

// Assumes store is locked by caller. If policies is empty, returns all policy mappings.
func (store *IAMStoreSys) listPolicyMappings(cache *iamCache, policies []string,
	userPredicate, groupPredicate func(string) bool,
) []madmin.PolicyEntities {
	queryPolSet := set.CreateStringSet(policies...)

	policyToUsersMap := make(map[string]set.StringSet)
	for user, mappedPolicy := range cache.iamUserPolicyMap {
		if userPredicate != nil && !userPredicate(user) {
			continue
		}

		commonPolicySet := mappedPolicy.policySet()
		if !queryPolSet.IsEmpty() {
			commonPolicySet = commonPolicySet.Intersection(queryPolSet)
		}
		for _, policy := range commonPolicySet.ToSlice() {
			s, ok := policyToUsersMap[policy]
			if !ok {
				policyToUsersMap[policy] = set.CreateStringSet(user)
			} else {
				s.Add(user)
				policyToUsersMap[policy] = s
			}
		}
	}

	if iamOS, ok := store.IAMStorageAPI.(*IAMObjectStore); ok {
		for item := range listIAMConfigItems(context.Background(), iamOS.objAPI, iamConfigPrefix+SlashSeparator+policyDBSTSUsersListKey) {
			user := strings.TrimSuffix(item.Item, ".json")
			if userPredicate != nil && !userPredicate(user) {
				continue
			}

			var mappedPolicy MappedPolicy
			store.loadIAMConfig(context.Background(), &mappedPolicy, getMappedPolicyPath(user, stsUser, false))

			commonPolicySet := mappedPolicy.policySet()
			if !queryPolSet.IsEmpty() {
				commonPolicySet = commonPolicySet.Intersection(queryPolSet)
			}
			for _, policy := range commonPolicySet.ToSlice() {
				s, ok := policyToUsersMap[policy]
				if !ok {
					policyToUsersMap[policy] = set.CreateStringSet(user)
				} else {
					s.Add(user)
					policyToUsersMap[policy] = s
				}
			}
		}
	}

	policyToGroupsMap := make(map[string]set.StringSet)
	for group, mappedPolicy := range cache.iamGroupPolicyMap {
		if groupPredicate != nil && !groupPredicate(group) {
			continue
		}

		commonPolicySet := mappedPolicy.policySet()
		if !queryPolSet.IsEmpty() {
			commonPolicySet = commonPolicySet.Intersection(queryPolSet)
		}
		for _, policy := range commonPolicySet.ToSlice() {
			s, ok := policyToGroupsMap[policy]
			if !ok {
				policyToGroupsMap[policy] = set.CreateStringSet(group)
			} else {
				s.Add(group)
				policyToGroupsMap[policy] = s
			}
		}
	}

	m := make(map[string]madmin.PolicyEntities, len(policyToGroupsMap))
	for policy, groups := range policyToGroupsMap {
		s := groups.ToSlice()
		sort.Strings(s)
		m[policy] = madmin.PolicyEntities{
			Policy: policy,
			Groups: s,
		}
	}
	for policy, users := range policyToUsersMap {
		s := users.ToSlice()
		sort.Strings(s)

		// Update existing value in map
		pe := m[policy]
		pe.Policy = policy
		pe.Users = s
		m[policy] = pe
	}

	policyEntities := make([]madmin.PolicyEntities, 0, len(m))
	for _, v := range m {
		policyEntities = append(policyEntities, v)
	}

	sort.Slice(policyEntities, func(i, j int) bool {
		return policyEntities[i].Policy < policyEntities[j].Policy
	})

	return policyEntities
}

// ListPolicyMappings - return users/groups mapped to policies.
func (store *IAMStoreSys) ListPolicyMappings(q madmin.PolicyEntitiesQuery,
	userPredicate, groupPredicate func(string) bool,
) madmin.PolicyEntitiesResult {
	cache := store.rlock()
	defer store.runlock()

	var result madmin.PolicyEntitiesResult

	isAllPoliciesQuery := len(q.Users) == 0 && len(q.Groups) == 0 && len(q.Policy) == 0

	if len(q.Users) > 0 {
		result.UserMappings = store.listUserPolicyMappings(cache, q.Users, userPredicate)
	}
	if len(q.Groups) > 0 {
		result.GroupMappings = store.listGroupPolicyMappings(cache, q.Groups, groupPredicate)
	}
	if len(q.Policy) > 0 || isAllPoliciesQuery {
		result.PolicyMappings = store.listPolicyMappings(cache, q.Policy, userPredicate, groupPredicate)
	}
	return result
}

// SetUserStatus - sets current user status.
func (store *IAMStoreSys) SetUserStatus(ctx context.Context, accessKey string, status madmin.AccountStatus) (updatedAt time.Time, err error) {
	if accessKey != "" && status != madmin.AccountEnabled && status != madmin.AccountDisabled {
		return updatedAt, errInvalidArgument
	}

	cache := store.lock()
	defer store.unlock()

	ui, ok := cache.iamUsersMap[accessKey]
	if !ok {
		return updatedAt, errNoSuchUser
	}
	cred := ui.Credentials

	if cred.IsTemp() || cred.IsServiceAccount() {
		return updatedAt, errIAMActionNotAllowed
	}

	uinfo := newUserIdentity(auth.Credentials{
		AccessKey: accessKey,
		SecretKey: cred.SecretKey,
		Status: func() string {
			switch string(status) {
			case string(madmin.AccountEnabled), string(auth.AccountOn):
				return auth.AccountOn
			}
			return auth.AccountOff
		}(),
	})

	if err := store.saveUserIdentity(ctx, accessKey, regUser, uinfo); err != nil {
		return updatedAt, err
	}

	if err := cache.updateUserWithClaims(accessKey, uinfo); err != nil {
		return updatedAt, err
	}

	return uinfo.UpdatedAt, nil
}

// AddServiceAccount - add a new service account
func (store *IAMStoreSys) AddServiceAccount(ctx context.Context, cred auth.Credentials) (updatedAt time.Time, err error) {
	cache := store.lock()
	defer store.unlock()

	accessKey := cred.AccessKey
	parentUser := cred.ParentUser

	// Found newly requested service account, to be an existing account -
	// reject such operation (updates to the service account are handled in
	// a different API).
	if su, found := cache.iamUsersMap[accessKey]; found {
		scred := su.Credentials
		if scred.ParentUser != parentUser {
			return updatedAt, fmt.Errorf("%w: the service account access key is taken by another user", errIAMServiceAccountNotAllowed)
		}
		return updatedAt, fmt.Errorf("%w: the service account access key already taken", errIAMServiceAccountNotAllowed)
	}

	// Parent user must not be a service account.
	if u, found := cache.iamUsersMap[parentUser]; found && u.Credentials.IsServiceAccount() {
		return updatedAt, fmt.Errorf("%w: unable to create a service account for another service account", errIAMServiceAccountNotAllowed)
	}

	u := newUserIdentity(cred)
	err = store.saveUserIdentity(ctx, u.Credentials.AccessKey, svcUser, u)
	if err != nil {
		return updatedAt, err
	}

	cache.updateUserWithClaims(u.Credentials.AccessKey, u)

	return u.UpdatedAt, nil
}

// UpdateServiceAccount - updates a service account on storage.
func (store *IAMStoreSys) UpdateServiceAccount(ctx context.Context, accessKey string, opts updateServiceAccountOpts) (updatedAt time.Time, err error) {
	cache := store.lock()
	defer store.unlock()

	ui, ok := cache.iamUsersMap[accessKey]
	if !ok || !ui.Credentials.IsServiceAccount() {
		return updatedAt, errNoSuchServiceAccount
	}
	cr := ui.Credentials
	currentSecretKey := cr.SecretKey
	if opts.secretKey != "" {
		if !auth.IsSecretKeyValid(opts.secretKey) {
			return updatedAt, auth.ErrInvalidSecretKeyLength
		}
		cr.SecretKey = opts.secretKey
	}

	if opts.name != "" {
		cr.Name = opts.name
	}

	if opts.description != "" {
		cr.Description = opts.description
	}

	if opts.expiration != nil {
		expirationInUTC := opts.expiration.UTC()
		if err := validateSvcExpirationInUTC(expirationInUTC); err != nil {
			return updatedAt, err
		}
		cr.Expiration = expirationInUTC
	}

	switch opts.status {
	// The caller did not ask to update status account, do nothing
	case "":
	case string(madmin.AccountEnabled):
		cr.Status = auth.AccountOn
	case string(madmin.AccountDisabled):
		cr.Status = auth.AccountOff
	// Update account status
	case auth.AccountOn, auth.AccountOff:
		cr.Status = opts.status
	default:
		return updatedAt, errors.New("unknown account status value")
	}

	m, err := getClaimsFromTokenWithSecret(cr.SessionToken, currentSecretKey)
	if err != nil {
		return updatedAt, fmt.Errorf("unable to get svc acc claims: %v", err)
	}

	// Extracted session policy name string can be removed as its not useful
	// at this point.
	delete(m, sessionPolicyNameExtracted)

	// sessionPolicy is nil and there is embedded policy attached we remove
	// embedded policy at that point.
	if _, ok := m[policy.SessionPolicyName]; ok && opts.sessionPolicy == nil {
		delete(m, policy.SessionPolicyName)
		m[iamPolicyClaimNameSA()] = inheritedPolicyType
	}

	if opts.sessionPolicy != nil { // session policies is being updated
		if err := opts.sessionPolicy.Validate(); err != nil {
			return updatedAt, err
		}

		policyBuf, err := json.Marshal(opts.sessionPolicy)
		if err != nil {
			return updatedAt, err
		}

		if len(policyBuf) > 2048 {
			return updatedAt, errSessionPolicyTooLarge
		}

		// Overwrite session policy claims.
		m[policy.SessionPolicyName] = base64.StdEncoding.EncodeToString(policyBuf)
		m[iamPolicyClaimNameSA()] = embeddedPolicyType
	}

	cr.SessionToken, err = auth.JWTSignWithAccessKey(accessKey, m, cr.SecretKey)
	if err != nil {
		return updatedAt, err
	}

	u := newUserIdentity(cr)
	if err := store.saveUserIdentity(ctx, u.Credentials.AccessKey, svcUser, u); err != nil {
		return updatedAt, err
	}

	if err := cache.updateUserWithClaims(u.Credentials.AccessKey, u); err != nil {
		return updatedAt, err
	}

	return u.UpdatedAt, nil
}

// ListTempAccounts - lists only temporary accounts from the cache.
func (store *IAMStoreSys) ListTempAccounts(ctx context.Context, accessKey string) ([]UserIdentity, error) {
	cache := store.rlock()
	defer store.runlock()

	userExists := false
	var tempAccounts []UserIdentity
	for _, v := range cache.iamUsersMap {
		isDerived := false
		if v.Credentials.IsServiceAccount() || v.Credentials.IsTemp() {
			isDerived = true
		}

		if !isDerived && v.Credentials.AccessKey == accessKey {
			userExists = true
		} else if isDerived && v.Credentials.ParentUser == accessKey {
			userExists = true
			if v.Credentials.IsTemp() {
				// Hide secret key & session key here
				v.Credentials.SecretKey = ""
				v.Credentials.SessionToken = ""
				tempAccounts = append(tempAccounts, v)
			}
		}
	}

	if !userExists {
		return nil, errNoSuchUser
	}

	return tempAccounts, nil
}

// ListServiceAccounts - lists only service accounts from the cache.
func (store *IAMStoreSys) ListServiceAccounts(ctx context.Context, accessKey string) ([]auth.Credentials, error) {
	cache := store.rlock()
	defer store.runlock()

	var serviceAccounts []auth.Credentials
	for _, u := range cache.iamUsersMap {
		v := u.Credentials
		if accessKey != "" && v.ParentUser == accessKey {
			if v.IsServiceAccount() {
				// Hide secret key & session key here
				v.SecretKey = ""
				v.SessionToken = ""
				serviceAccounts = append(serviceAccounts, v)
			}
		}
	}

	return serviceAccounts, nil
}

// ListSTSAccounts - lists only STS accounts from the cache.
func (store *IAMStoreSys) ListSTSAccounts(ctx context.Context, accessKey string) ([]auth.Credentials, error) {
	cache := store.rlock()
	defer store.runlock()

	var stsAccounts []auth.Credentials
	for _, u := range cache.iamSTSAccountsMap {
		v := u.Credentials
		if accessKey != "" && v.ParentUser == accessKey {
			if v.IsTemp() {
				// Hide secret key & session key here
				v.SecretKey = ""
				v.SessionToken = ""
				stsAccounts = append(stsAccounts, v)
			}
		}
	}

	return stsAccounts, nil
}

// AddUser - adds/updates long term user account to storage.
func (store *IAMStoreSys) AddUser(ctx context.Context, accessKey string, ureq madmin.AddOrUpdateUserReq) (updatedAt time.Time, err error) {
	cache := store.lock()
	defer store.unlock()

	cache.updatedAt = time.Now()

	ui, ok := cache.iamUsersMap[accessKey]

	// It is not possible to update an STS account.
	if ok && ui.Credentials.IsTemp() {
		return updatedAt, errIAMActionNotAllowed
	}

	u := newUserIdentity(auth.Credentials{
		AccessKey: accessKey,
		SecretKey: ureq.SecretKey,
		Status: func() string {
			switch string(ureq.Status) {
			case string(madmin.AccountEnabled), string(auth.AccountOn):
				return auth.AccountOn
			}
			return auth.AccountOff
		}(),
	})

	if err := store.saveUserIdentity(ctx, accessKey, regUser, u); err != nil {
		return updatedAt, err
	}
	if err := cache.updateUserWithClaims(accessKey, u); err != nil {
		return updatedAt, err
	}

	return u.UpdatedAt, nil
}

// UpdateUserSecretKey - sets user secret key to storage.
func (store *IAMStoreSys) UpdateUserSecretKey(ctx context.Context, accessKey, secretKey string) error {
	cache := store.lock()
	defer store.unlock()

	cache.updatedAt = time.Now()

	ui, ok := cache.iamUsersMap[accessKey]
	if !ok {
		return errNoSuchUser
	}
	cred := ui.Credentials
	cred.SecretKey = secretKey
	u := newUserIdentity(cred)
	if err := store.saveUserIdentity(ctx, accessKey, regUser, u); err != nil {
		return err
	}

	return cache.updateUserWithClaims(accessKey, u)
}

// GetSTSAndServiceAccounts - returns all STS and Service account credentials.
func (store *IAMStoreSys) GetSTSAndServiceAccounts() []auth.Credentials {
	cache := store.rlock()
	defer store.runlock()

	var res []auth.Credentials
	for _, u := range cache.iamUsersMap {
		cred := u.Credentials
		if cred.IsTemp() {
			panic("unexpected STS credential found in iamUsersMap")
		}
		if cred.IsServiceAccount() {
			res = append(res, cred)
		}
	}
	for _, u := range cache.iamSTSAccountsMap {
		if !u.Credentials.IsTemp() {
			panic("unexpected non STS credential found in iamSTSAccountsMap")
		}
		res = append(res, u.Credentials)
	}

	return res
}

// UpdateUserIdentity - updates a user credential.
func (store *IAMStoreSys) UpdateUserIdentity(ctx context.Context, cred auth.Credentials) error {
	cache := store.lock()
	defer store.unlock()

	cache.updatedAt = time.Now()

	userType := regUser
	if cred.IsServiceAccount() {
		userType = svcUser
	} else if cred.IsTemp() {
		userType = stsUser
	}
	ui := newUserIdentity(cred)
	// Overwrite the user identity here. As store should be
	// atomic, it shouldn't cause any corruption.
	if err := store.saveUserIdentity(ctx, cred.AccessKey, userType, ui); err != nil {
		return err
	}

	return cache.updateUserWithClaims(cred.AccessKey, ui)
}

// LoadUser - attempts to load user info from storage and updates cache.
func (store *IAMStoreSys) LoadUser(ctx context.Context, accessKey string) {
	cache := store.lock()
	defer store.unlock()

	cache.updatedAt = time.Now()

	_, found := cache.iamUsersMap[accessKey]

	// Check for regular user access key
	if !found {
		store.loadUser(ctx, accessKey, regUser, cache.iamUsersMap)
		if _, found = cache.iamUsersMap[accessKey]; found {
			// load mapped policies
			store.loadMappedPolicy(ctx, accessKey, regUser, false, cache.iamUserPolicyMap)
		}
	}

	// Check for service account
	if !found {
		store.loadUser(ctx, accessKey, svcUser, cache.iamUsersMap)
		if svc, found := cache.iamUsersMap[accessKey]; found {
			// Load parent user and mapped policies.
			if store.getUsersSysType() == MinIOUsersSysType {
				store.loadUser(ctx, svc.Credentials.ParentUser, regUser, cache.iamUsersMap)
				store.loadMappedPolicy(ctx, svc.Credentials.ParentUser, regUser, false, cache.iamUserPolicyMap)
			} else {
				// In case of LDAP the parent user's policy mapping needs to be
				// loaded into sts map
				store.loadMappedPolicy(ctx, svc.Credentials.ParentUser, stsUser, false, cache.iamSTSPolicyMap)
			}
		}
	}

	// Check for STS account
	stsAccountFound := false
	var stsUserCred UserIdentity
	if !found {
		store.loadUser(ctx, accessKey, stsUser, cache.iamSTSAccountsMap)
		if stsUserCred, found = cache.iamSTSAccountsMap[accessKey]; found {
			// Load mapped policy
			store.loadMappedPolicy(ctx, stsUserCred.Credentials.ParentUser, stsUser, false, cache.iamSTSPolicyMap)
			stsAccountFound = true
		}
	}

	// Load any associated policy definitions
	if !stsAccountFound {
		for _, policy := range cache.iamUserPolicyMap[accessKey].toSlice() {
			if _, found = cache.iamPolicyDocsMap[policy]; !found {
				store.loadPolicyDoc(ctx, policy, cache.iamPolicyDocsMap)
			}
		}
	} else {
		for _, policy := range cache.iamSTSPolicyMap[stsUserCred.Credentials.AccessKey].toSlice() {
			if _, found = cache.iamPolicyDocsMap[policy]; !found {
				store.loadPolicyDoc(ctx, policy, cache.iamPolicyDocsMap)
			}
		}
	}
}

func extractJWTClaims(u UserIdentity) (*jwt.MapClaims, error) {
	jwtClaims, err := auth.ExtractClaims(u.Credentials.SessionToken, u.Credentials.SecretKey)
	if err != nil {
		// Session tokens for STS creds will be generated with root secret
		jwtClaims, err = auth.ExtractClaims(u.Credentials.SessionToken, globalActiveCred.SecretKey)
		if err != nil {
			return nil, err
		}
	}
	return jwtClaims, nil
}

func validateSvcExpirationInUTC(expirationInUTC time.Time) error {
	if expirationInUTC.IsZero() || expirationInUTC.Equal(timeSentinel) {
		// Service accounts might not have expiration in older releases.
		return nil
	}

	currentTime := time.Now().UTC()
	minExpiration := currentTime.Add(minServiceAccountExpiry)
	maxExpiration := currentTime.Add(maxServiceAccountExpiry)
	if expirationInUTC.Before(minExpiration) || expirationInUTC.After(maxExpiration) {
		return errInvalidSvcAcctExpiration
	}

	return nil
}
