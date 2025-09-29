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
	"maps"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/config/identity/openid"
	"github.com/minio/minio/internal/jwt"
	"github.com/minio/pkg/v3/env"
	"github.com/minio/pkg/v3/policy"
	"github.com/minio/pkg/v3/sync/errgroup"
	"github.com/puzpuzpuz/xsync/v3"
	"golang.org/x/sync/singleflight"
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
	UpdatedAt   time.Time        `json:"updatedAt"`
}

func newUserIdentity(cred auth.Credentials) UserIdentity {
	return UserIdentity{Version: 1, Credentials: cred, UpdatedAt: UTCNow()}
}

// GroupInfo contains info about a group
type GroupInfo struct {
	Version   int       `json:"version"`
	Status    string    `json:"status"`
	Members   []string  `json:"members"`
	UpdatedAt time.Time `json:"updatedAt"`
}

func newGroupInfo(members []string) GroupInfo {
	return GroupInfo{Version: 1, Status: statusEnabled, Members: members, UpdatedAt: UTCNow()}
}

// MappedPolicy represents a policy name mapped to a user or group
type MappedPolicy struct {
	Version   int       `json:"version"`
	Policies  string    `json:"policy"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// mappedPoliciesToMap copies the map of mapped policies to a regular map.
func mappedPoliciesToMap(m *xsync.MapOf[string, MappedPolicy]) map[string]MappedPolicy {
	policies := make(map[string]MappedPolicy, m.Size())
	m.Range(func(k string, v MappedPolicy) bool {
		policies[k] = v
		return true
	})
	return policies
}

// converts a mapped policy into a slice of distinct policies
func (mp MappedPolicy) toSlice() []string {
	var policies []string
	for policy := range strings.SplitSeq(mp.Policies, ",") {
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
	CreateDate time.Time
	UpdateDate time.Time
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
	iamUserPolicyMap *xsync.MapOf[string, MappedPolicy]

	// STS accounts are loaded on demand and not via the periodic IAM reload.
	// map of STS access key to credentials
	iamSTSAccountsMap map[string]UserIdentity
	// map of STS access key to policy names
	iamSTSPolicyMap *xsync.MapOf[string, MappedPolicy]

	// map of group names to group info
	iamGroupsMap map[string]GroupInfo
	// map of user names to groups they are a member of
	iamUserGroupMemberships map[string]set.StringSet
	// map of group names to policy names
	iamGroupPolicyMap *xsync.MapOf[string, MappedPolicy]
}

func newIamCache() *iamCache {
	return &iamCache{
		iamPolicyDocsMap:        map[string]PolicyDoc{},
		iamUsersMap:             map[string]UserIdentity{},
		iamUserPolicyMap:        xsync.NewMapOf[string, MappedPolicy](),
		iamSTSAccountsMap:       map[string]UserIdentity{},
		iamSTSPolicyMap:         xsync.NewMapOf[string, MappedPolicy](),
		iamGroupsMap:            map[string]GroupInfo{},
		iamUserGroupMemberships: map[string]set.StringSet{},
		iamGroupPolicyMap:       xsync.NewMapOf[string, MappedPolicy](),
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

func (c *iamCache) policyDBGetGroups(store *IAMStoreSys, userPolicyPresent bool, groups ...string) ([]string, error) {
	var policies []string
	for _, group := range groups {
		if store.getUsersSysType() == MinIOUsersSysType {
			g, ok := c.iamGroupsMap[group]
			if !ok {
				continue
			}

			// Group is disabled, so we return no policy - this
			// ensures the request is denied.
			if g.Status == statusDisabled {
				continue
			}
		}

		policy, ok := c.iamGroupPolicyMap.Load(group)
		if !ok {
			continue
		}

		policies = append(policies, policy.toSlice()...)
	}

	found := len(policies) > 0
	if found {
		return policies, nil
	}

	if userPolicyPresent {
		// if user mapping present and no group policies found
		// rely on user policy for access, instead of fallback.
		return nil, nil
	}

	var mu sync.Mutex

	// no mappings found, fallback for all groups.
	g := errgroup.WithNErrs(len(groups)).WithConcurrency(10) // load like 10 groups at a time.

	for index := range groups {
		g.Go(func() error {
			err := store.loadMappedPolicy(context.TODO(), groups[index], regUser, true, c.iamGroupPolicyMap)
			if err != nil && !errors.Is(err, errNoSuchPolicy) {
				return err
			}
			if errors.Is(err, errNoSuchPolicy) {
				return nil
			}
			policy, _ := c.iamGroupPolicyMap.Load(groups[index])
			mu.Lock()
			policies = append(policies, policy.toSlice()...)
			mu.Unlock()
			return nil
		}, index)
	}

	err := errors.Join(g.Wait()...)
	return policies, err
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
func (c *iamCache) policyDBGet(store *IAMStoreSys, name string, isGroup bool, policyPresent bool) ([]string, time.Time, error) {
	if isGroup {
		if store.getUsersSysType() == MinIOUsersSysType {
			g, ok := c.iamGroupsMap[name]
			if !ok {
				if err := store.loadGroup(context.Background(), name, c.iamGroupsMap); err != nil {
					return nil, time.Time{}, err
				}
				g, ok = c.iamGroupsMap[name]
				if !ok {
					return nil, time.Time{}, errNoSuchGroup
				}
			}

			// Group is disabled, so we return no policy - this
			// ensures the request is denied.
			if g.Status == statusDisabled {
				return nil, time.Time{}, nil
			}
		}

		policy, ok := c.iamGroupPolicyMap.Load(name)
		if ok {
			return policy.toSlice(), policy.UpdatedAt, nil
		}
		if !policyPresent {
			if err := store.loadMappedPolicy(context.TODO(), name, regUser, true, c.iamGroupPolicyMap); err != nil && !errors.Is(err, errNoSuchPolicy) {
				return nil, time.Time{}, err
			}
			policy, _ = c.iamGroupPolicyMap.Load(name)
			return policy.toSlice(), policy.UpdatedAt, nil
		}
		return nil, time.Time{}, nil
	}

	// returned policy could be empty, we use set to de-duplicate.
	var policies set.StringSet
	var updatedAt time.Time

	if store.getUsersSysType() == LDAPUsersSysType {
		// For LDAP policy mapping is part of STS users, we only need to lookup
		// those mappings.
		mp, ok := c.iamSTSPolicyMap.Load(name)
		if !ok {
			// Attempt to load parent user mapping for STS accounts
			if err := store.loadMappedPolicy(context.TODO(), name, stsUser, false, c.iamSTSPolicyMap); err != nil && !errors.Is(err, errNoSuchPolicy) {
				return nil, time.Time{}, err
			}
			mp, _ = c.iamSTSPolicyMap.Load(name)
		}
		policies = set.CreateStringSet(mp.toSlice()...)
		updatedAt = mp.UpdatedAt
	} else {
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
		mp, ok := c.iamUserPolicyMap.Load(name)
		if !ok {
			if err := store.loadMappedPolicy(context.TODO(), name, regUser, false, c.iamUserPolicyMap); err != nil && !errors.Is(err, errNoSuchPolicy) {
				return nil, time.Time{}, err
			}
			mp, ok = c.iamUserPolicyMap.Load(name)
			if !ok {
				// Since user "name" could be a parent user of an STS account, we look up
				// mappings for those too.
				mp, ok = c.iamSTSPolicyMap.Load(name)
				if !ok {
					// Attempt to load parent user mapping for STS accounts
					if err := store.loadMappedPolicy(context.TODO(), name, stsUser, false, c.iamSTSPolicyMap); err != nil && !errors.Is(err, errNoSuchPolicy) {
						return nil, time.Time{}, err
					}
					mp, _ = c.iamSTSPolicyMap.Load(name)
				}
			}
		}
		policies = set.CreateStringSet(mp.toSlice()...)

		for _, group := range u.Credentials.Groups {
			g, ok := c.iamGroupsMap[group]
			if ok {
				// Group is disabled, so we return no policy - this
				// ensures the request is denied.
				if g.Status == statusDisabled {
					return nil, time.Time{}, nil
				}
			}

			policy, ok := c.iamGroupPolicyMap.Load(group)
			if !ok {
				if err := store.loadMappedPolicy(context.TODO(), group, regUser, true, c.iamGroupPolicyMap); err != nil && !errors.Is(err, errNoSuchPolicy) {
					return nil, time.Time{}, err
				}
				policy, _ = c.iamGroupPolicyMap.Load(group)
			}

			for _, p := range policy.toSlice() {
				policies.Add(p)
			}
		}
		updatedAt = mp.UpdatedAt
	}

	for _, group := range c.iamUserGroupMemberships[name].ToSlice() {
		if store.getUsersSysType() == MinIOUsersSysType {
			g, ok := c.iamGroupsMap[group]
			if ok {
				// Group is disabled, so we return no policy - this
				// ensures the request is denied.
				if g.Status == statusDisabled {
					return nil, time.Time{}, nil
				}
			}
		}

		policy, ok := c.iamGroupPolicyMap.Load(group)
		if !ok {
			if err := store.loadMappedPolicy(context.TODO(), group, regUser, true, c.iamGroupPolicyMap); err != nil && !errors.Is(err, errNoSuchPolicy) {
				return nil, time.Time{}, err
			}
			policy, _ = c.iamGroupPolicyMap.Load(group)
		}

		for _, p := range policy.toSlice() {
			policies.Add(p)
		}
	}

	return policies.ToSlice(), updatedAt, nil
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
	loadPolicyDocWithRetry(ctx context.Context, policy string, m map[string]PolicyDoc, retries int) error
	loadPolicyDocs(ctx context.Context, m map[string]PolicyDoc) error
	loadUser(ctx context.Context, user string, userType IAMUserType, m map[string]UserIdentity) error
	loadSecretKey(ctx context.Context, user string, userType IAMUserType) (string, error)
	loadUsers(ctx context.Context, userType IAMUserType, m map[string]UserIdentity) error
	loadGroup(ctx context.Context, group string, m map[string]GroupInfo) error
	loadGroups(ctx context.Context, m map[string]GroupInfo) error
	loadMappedPolicy(ctx context.Context, name string, userType IAMUserType, isGroup bool, m *xsync.MapOf[string, MappedPolicy]) error
	loadMappedPolicyWithRetry(ctx context.Context, name string, userType IAMUserType, isGroup bool, m *xsync.MapOf[string, MappedPolicy], retries int) error
	loadMappedPolicies(ctx context.Context, userType IAMUserType, isGroup bool, m *xsync.MapOf[string, MappedPolicy]) error
	saveIAMConfig(ctx context.Context, item any, path string, opts ...options) error
	loadIAMConfig(ctx context.Context, item any, path string) error
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

// LoadIAMCache reads all IAM items and populates a new iamCache object and
// replaces the in-memory cache object.
func (store *IAMStoreSys) LoadIAMCache(ctx context.Context, firstTime bool) error {
	bootstrapTraceMsgFirstTime := func(s string) {
		if firstTime {
			bootstrapTraceMsg(s)
		}
	}
	bootstrapTraceMsgFirstTime("loading IAM data")

	newCache := newIamCache()

	loadedAt := time.Now()

	if iamOS, ok := store.IAMStorageAPI.(*IAMObjectStore); ok {
		err := iamOS.loadAllFromObjStore(ctx, newCache, firstTime)
		if err != nil {
			return err
		}
	} else {
		// Only non-object IAM store (i.e. only etcd backend).
		bootstrapTraceMsgFirstTime("loading policy documents")
		if err := store.loadPolicyDocs(ctx, newCache.iamPolicyDocsMap); err != nil {
			return err
		}

		// Sets default canned policies, if none are set.
		setDefaultCannedPolicies(newCache.iamPolicyDocsMap)

		if store.getUsersSysType() == MinIOUsersSysType {
			bootstrapTraceMsgFirstTime("loading regular users")
			if err := store.loadUsers(ctx, regUser, newCache.iamUsersMap); err != nil {
				return err
			}
			bootstrapTraceMsgFirstTime("loading regular groups")
			if err := store.loadGroups(ctx, newCache.iamGroupsMap); err != nil {
				return err
			}
		}

		bootstrapTraceMsgFirstTime("loading user policy mapping")
		// load polices mapped to users
		if err := store.loadMappedPolicies(ctx, regUser, false, newCache.iamUserPolicyMap); err != nil {
			return err
		}

		bootstrapTraceMsgFirstTime("loading group policy mapping")
		// load policies mapped to groups
		if err := store.loadMappedPolicies(ctx, regUser, true, newCache.iamGroupPolicyMap); err != nil {
			return err
		}

		bootstrapTraceMsgFirstTime("loading service accounts")
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
	if cache.updatedAt.Before(loadedAt) || firstTime {
		// No one has updated anything since the config was loaded,
		// so we just replace whatever is on the disk into memory.
		cache.iamGroupPolicyMap = newCache.iamGroupPolicyMap
		cache.iamGroupsMap = newCache.iamGroupsMap
		cache.iamPolicyDocsMap = newCache.iamPolicyDocsMap
		cache.iamUserGroupMemberships = newCache.iamUserGroupMemberships
		cache.iamUserPolicyMap = newCache.iamUserPolicyMap
		cache.iamUsersMap = newCache.iamUsersMap
		// For STS policy map, we need to merge the new cache with the existing
		// cache because the periodic IAM reload is partial. The periodic load
		// here is to account for STS policy mapping changes that should apply
		// for service accounts derived from such STS accounts (i.e. LDAP STS
		// accounts).
		newCache.iamSTSPolicyMap.Range(func(k string, v MappedPolicy) bool {
			cache.iamSTSPolicyMap.Store(k, v)
			return true
		})

		cache.updatedAt = time.Now()
	}

	return nil
}

// IAMStoreSys contains IAMStorageAPI to add higher-level methods on the storage
// layer.
type IAMStoreSys struct {
	IAMStorageAPI

	group  *singleflight.Group
	policy *singleflight.Group
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
		v, ok := cache.iamGroupPolicyMap.Load(name)
		return v, ok
	}
	return cache.iamUserPolicyMap.Load(name)
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
		cache.iamGroupPolicyMap.Delete(group)

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

	getPolicies := func() ([]string, error) {
		policies, _, err := cache.policyDBGet(store, name, false, false)
		if err != nil {
			return nil, err
		}

		userPolicyPresent := len(policies) > 0

		groupPolicies, err := cache.policyDBGetGroups(store, userPolicyPresent, groups...)
		if err != nil {
			return nil, err
		}

		policies = append(policies, groupPolicies...)
		return policies, nil
	}
	if store.policy != nil {
		val, err, _ := store.policy.Do(name, func() (any, error) {
			return getPolicies()
		})
		if err != nil {
			return nil, err
		}
		res, ok := val.([]string)
		if !ok {
			return nil, errors.New("unexpected policy type")
		}
		return res, nil
	}
	return getPolicies()
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
		cache.iamGroupPolicyMap.Delete(group)
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

	ps, updatedAt, err := cache.policyDBGet(store, group, true, false)
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

// updateGroups updates the group from the persistent store, and also related policy mapping if any.
func (store *IAMStoreSys) updateGroups(ctx context.Context, cache *iamCache) (res []string, err error) {
	groupSet := set.NewStringSet()
	if iamOS, ok := store.IAMStorageAPI.(*IAMObjectStore); ok {
		listedConfigItems, err := iamOS.listAllIAMConfigItems(ctx)
		if err != nil {
			return nil, err
		}
		if store.getUsersSysType() == MinIOUsersSysType {
			groupsList := listedConfigItems[groupsListKey]
			for _, item := range groupsList {
				group := path.Dir(item)
				if err = iamOS.loadGroup(ctx, group, cache.iamGroupsMap); err != nil && !errors.Is(err, errNoSuchGroup) {
					return nil, fmt.Errorf("unable to load the group: %w", err)
				}
				groupSet.Add(group)
			}
		}

		groupPolicyMappingsList := listedConfigItems[policyDBGroupsListKey]
		for _, item := range groupPolicyMappingsList {
			group := strings.TrimSuffix(item, ".json")
			if err = iamOS.loadMappedPolicy(ctx, group, regUser, true, cache.iamGroupPolicyMap); err != nil && !errors.Is(err, errNoSuchPolicy) {
				return nil, fmt.Errorf("unable to load the policy mapping for the group: %w", err)
			}
			groupSet.Add(group)
		}

		return groupSet.ToSlice(), nil
	}

	// For etcd just return from cache.
	for k := range cache.iamGroupsMap {
		groupSet.Add(k)
	}

	cache.iamGroupPolicyMap.Range(func(k string, v MappedPolicy) bool {
		groupSet.Add(k)
		return true
	})

	return groupSet.ToSlice(), nil
}

// ListGroups - lists groups. Since this is not going to be a frequent
// operation, we fetch this info from storage, and refresh the cache as well.
func (store *IAMStoreSys) ListGroups(ctx context.Context) (res []string, err error) {
	cache := store.lock()
	defer store.unlock()

	return store.updateGroups(ctx, cache)
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
		cache.iamGroupPolicyMap.Range(func(k string, _ MappedPolicy) bool {
			res = append(res, k)
			return true
		})
	}
	return res, err
}

// PolicyDBUpdate - adds or removes given policies to/from the user or group's
// policy associations.
func (store *IAMStoreSys) PolicyDBUpdate(ctx context.Context, name string, isGroup bool,
	userType IAMUserType, policies []string, isAttach bool) (updatedAt time.Time,
	addedOrRemoved, effectivePolicies []string, err error,
) {
	if name == "" {
		err = errInvalidArgument
		return updatedAt, addedOrRemoved, effectivePolicies, err
	}

	cache := store.lock()
	defer store.unlock()

	// Load existing policy mapping
	var mp MappedPolicy
	if !isGroup {
		if userType == stsUser {
			stsMap := xsync.NewMapOf[string, MappedPolicy]()

			// Attempt to load parent user mapping for STS accounts
			store.loadMappedPolicy(context.TODO(), name, stsUser, false, stsMap)

			mp, _ = stsMap.Load(name)
		} else {
			mp, _ = cache.iamUserPolicyMap.Load(name)
		}
	} else {
		if store.getUsersSysType() == MinIOUsersSysType {
			g, ok := cache.iamGroupsMap[name]
			if !ok {
				err = errNoSuchGroup
				return updatedAt, addedOrRemoved, effectivePolicies, err
			}

			if g.Status == statusDisabled {
				err = errGroupDisabled
				return updatedAt, addedOrRemoved, effectivePolicies, err
			}
		}
		mp, _ = cache.iamGroupPolicyMap.Load(name)
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
				return updatedAt, addedOrRemoved, effectivePolicies, err
			}
		}
		newPolicySet = existingPolicySet.Union(policiesToUpdate)
	} else {
		// policies to detach => inputPolicies ∩ existing (intersection)
		policiesToUpdate = policiesToUpdate.Intersection(existingPolicySet)
		newPolicySet = existingPolicySet.Difference(policiesToUpdate)
	}
	// We return an error if the requested policy update will have no effect.
	if policiesToUpdate.IsEmpty() {
		err = errNoPolicyToAttachOrDetach
		return updatedAt, addedOrRemoved, effectivePolicies, err
	}

	newPolicies := newPolicySet.ToSlice()
	newPolicyMapping.Policies = strings.Join(newPolicies, ",")
	newPolicyMapping.UpdatedAt = UTCNow()
	addedOrRemoved = policiesToUpdate.ToSlice()

	// In case of detach operation, it is possible that no policies are mapped -
	// in this case, we delete the mapping from the store.
	if len(newPolicies) == 0 {
		if err = store.deleteMappedPolicy(ctx, name, userType, isGroup); err != nil && !errors.Is(err, errNoSuchPolicy) {
			return updatedAt, addedOrRemoved, effectivePolicies, err
		}
		if !isGroup {
			if userType == stsUser {
				cache.iamSTSPolicyMap.Delete(name)
			} else {
				cache.iamUserPolicyMap.Delete(name)
			}
		} else {
			cache.iamGroupPolicyMap.Delete(name)
		}
	} else {
		if err = store.saveMappedPolicy(ctx, name, userType, isGroup, newPolicyMapping); err != nil {
			return updatedAt, addedOrRemoved, effectivePolicies, err
		}
		if !isGroup {
			if userType == stsUser {
				cache.iamSTSPolicyMap.Store(name, newPolicyMapping)
			} else {
				cache.iamUserPolicyMap.Store(name, newPolicyMapping)
			}
		} else {
			cache.iamGroupPolicyMap.Store(name, newPolicyMapping)
		}
	}

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
		if !isGroup {
			if userType == stsUser {
				cache.iamSTSPolicyMap.Delete(name)
			} else {
				cache.iamUserPolicyMap.Delete(name)
			}
		} else {
			cache.iamGroupPolicyMap.Delete(name)
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
			cache.iamSTSPolicyMap.Store(name, mp)
		} else {
			cache.iamUserPolicyMap.Store(name, mp)
		}
	} else {
		cache.iamGroupPolicyMap.Store(name, mp)
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
		cache.iamUserPolicyMap.Range(func(u string, mp MappedPolicy) bool {
			pset := mp.policySet()
			if !pset.Contains(policy) {
				return true
			}
			if store.getUsersSysType() == MinIOUsersSysType {
				_, ok := cache.iamUsersMap[u]
				if !ok {
					// happens when account is deleted or
					// expired.
					cache.iamUserPolicyMap.Delete(u)
					return true
				}
			}
			pset.Remove(policy)
			cache.iamUserPolicyMap.Store(u, newMappedPolicy(strings.Join(pset.ToSlice(), ",")))
			return true
		})

		// update group policy map
		cache.iamGroupPolicyMap.Range(func(g string, mp MappedPolicy) bool {
			pset := mp.policySet()
			if !pset.Contains(policy) {
				return true
			}
			pset.Remove(policy)
			cache.iamGroupPolicyMap.Store(g, newMappedPolicy(strings.Join(pset.ToSlice(), ",")))
			return true
		})

		cache.updatedAt = time.Now()
		return nil
	}
	return err
}

// DeletePolicy - deletes policy from storage and cache. When this called in
// response to a notification (i.e. isFromNotification = true), it skips the
// validation of policy usage and the attempt to delete in the backend as well
// (as this is already done by the notifying node).
func (store *IAMStoreSys) DeletePolicy(ctx context.Context, policy string, isFromNotification bool) error {
	if policy == "" {
		return errInvalidArgument
	}

	cache := store.lock()
	defer store.unlock()

	if !isFromNotification {
		// Check if policy is mapped to any existing user or group. If so, we do not
		// allow deletion of the policy. If the policy is mapped to an STS account,
		// we do allow deletion.
		users := []string{}
		groups := []string{}
		cache.iamUserPolicyMap.Range(func(u string, mp MappedPolicy) bool {
			pset := mp.policySet()
			if store.getUsersSysType() == MinIOUsersSysType {
				if _, ok := cache.iamUsersMap[u]; !ok {
					// This case can happen when a temporary account is
					// deleted or expired - remove it from userPolicyMap.
					cache.iamUserPolicyMap.Delete(u)
					return true
				}
			}
			if pset.Contains(policy) {
				users = append(users, u)
			}
			return true
		})
		cache.iamGroupPolicyMap.Range(func(g string, mp MappedPolicy) bool {
			pset := mp.policySet()
			if pset.Contains(policy) {
				groups = append(groups, g)
			}
			return true
		})
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

// MergePolicies - accepts a comma separated list of policy names as a string
// and returns only policies that currently exist in MinIO. It includes hot loading
// of policies if not in the memory
func (store *IAMStoreSys) MergePolicies(policyName string) (string, policy.Policy) {
	var policies []string
	var missingPolicies []string
	var toMerge []policy.Policy

	cache := store.rlock()
	for _, policy := range newMappedPolicy(policyName).toSlice() {
		if policy == "" {
			continue
		}
		p, found := cache.iamPolicyDocsMap[policy]
		if !found {
			missingPolicies = append(missingPolicies, policy)
			continue
		}
		policies = append(policies, policy)
		toMerge = append(toMerge, p.Policy)
	}
	store.runlock()

	if len(missingPolicies) > 0 {
		m := make(map[string]PolicyDoc)
		for _, policy := range missingPolicies {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = store.loadPolicyDoc(ctx, policy, m)
			cancel()
		}

		cache := store.lock()
		maps.Copy(cache.iamPolicyDocsMap, m)
		store.unlock()

		for policy, p := range m {
			policies = append(policies, policy)
			toMerge = append(toMerge, p.Policy)
		}
	}

	return strings.Join(policies, ","), policy.MergePolicies(toMerge...)
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
		mp, ok := cache.iamUserPolicyMap.Load(k)
		if ok {
			policies = append(policies, mp.Policies)
			for _, group := range cache.iamUserGroupMemberships[k].ToSlice() {
				if nmp, ok := cache.iamGroupPolicyMap.Load(group); ok {
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
		pl, _ := cache.iamUserPolicyMap.Load(k)
		result[k] = madmin.UserInfo{
			PolicyName: pl.Policies,
			Status: func() madmin.AccountStatus {
				if v.IsValid() {
					return madmin.AccountEnabled
				}
				return madmin.AccountDisabled
			}(),
			MemberOf:  cache.iamUserGroupMemberships[k].ToSlice(),
			UpdatedAt: pl.UpdatedAt,
		}
	}

	return result
}

// GetUsersWithMappedPolicies - safely returns the name of access keys with associated policies
func (store *IAMStoreSys) GetUsersWithMappedPolicies() map[string]string {
	cache := store.rlock()
	defer store.runlock()

	result := make(map[string]string)
	cache.iamUserPolicyMap.Range(func(k string, v MappedPolicy) bool {
		result[k] = v.Policies
		return true
	})
	cache.iamSTSPolicyMap.Range(func(k string, v MappedPolicy) bool {
		result[k] = v.Policies
		return true
	})
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
		mappedPolicy, ok := cache.iamUserPolicyMap.Load(name)
		if !ok {
			mappedPolicy, ok = cache.iamSTSPolicyMap.Load(name)
		}
		if !ok {
			// Attempt to load parent user mapping for STS accounts
			store.loadMappedPolicy(context.TODO(), name, stsUser, false, cache.iamSTSPolicyMap)
			mappedPolicy, ok = cache.iamSTSPolicyMap.Load(name)
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
	pl, _ := cache.iamUserPolicyMap.Load(name)
	return madmin.UserInfo{
		PolicyName: pl.Policies,
		Status: func() madmin.AccountStatus {
			if cred.IsValid() {
				return madmin.AccountEnabled
			}
			return madmin.AccountDisabled
		}(),
		MemberOf:  cache.iamUserGroupMemberships[name].ToSlice(),
		UpdatedAt: pl.UpdatedAt,
	}, nil
}

// PolicyMappingNotificationHandler - handles updating a policy mapping from storage.
func (store *IAMStoreSys) PolicyMappingNotificationHandler(ctx context.Context, userOrGroup string, isGroup bool, userType IAMUserType) error {
	if userOrGroup == "" {
		return errInvalidArgument
	}

	cache := store.lock()
	defer store.unlock()

	var m *xsync.MapOf[string, MappedPolicy]
	switch {
	case isGroup:
		m = cache.iamGroupPolicyMap
	case userType == stsUser:
		m = cache.iamSTSPolicyMap
	default:
		m = cache.iamUserPolicyMap
	}
	err := store.loadMappedPolicy(ctx, userOrGroup, userType, isGroup, m)
	if errors.Is(err, errNoSuchPolicy) {
		// This means that the policy mapping was deleted, so we update
		// the cache.
		m.Delete(userOrGroup)
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
		cache.iamUserPolicyMap.Delete(accessKey)

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
					delete(cache.iamSTSAccountsMap, u.AccessKey)
					delete(cache.iamUsersMap, u.AccessKey)
				}
				if store.group != nil {
					store.group.Forget(u.AccessKey)
				}
			}
		}
	}

	// It is ok to ignore deletion error on the mapped policy
	store.deleteMappedPolicy(ctx, accessKey, userType, false)
	cache.iamUserPolicyMap.Delete(accessKey)

	err := store.deleteUserIdentity(ctx, accessKey, userType)
	if err == errNoSuchUser {
		// ignore if user is already deleted.
		err = nil
	}
	if userType == stsUser {
		delete(cache.iamSTSAccountsMap, accessKey)
	}
	delete(cache.iamUsersMap, accessKey)
	if store.group != nil {
		store.group.Forget(accessKey)
	}

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

		cache.iamSTSPolicyMap.Store(cred.ParentUser, mp)
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

// RevokeTokens - revokes all temporary credentials, or those with matching type,
// associated with the parent user.
func (store *IAMStoreSys) RevokeTokens(ctx context.Context, parentUser string, tokenRevokeType string) error {
	if parentUser == "" {
		return errInvalidArgument
	}

	cache := store.lock()
	defer store.unlock()

	secret, err := getTokenSigningKey()
	if err != nil {
		return err
	}

	var revoked bool
	for _, ui := range cache.iamSTSAccountsMap {
		if ui.Credentials.ParentUser != parentUser {
			continue
		}
		if tokenRevokeType != "" {
			claims, err := getClaimsFromTokenWithSecret(ui.Credentials.SessionToken, secret)
			if err != nil {
				continue // skip if token is invalid
			}
			// skip if token type is given and does not match
			if v, _ := claims.Lookup(tokenRevokeTypeClaim); v != tokenRevokeType {
				continue
			}
		}
		if err := store.deleteUserIdentity(ctx, ui.Credentials.AccessKey, stsUser); err != nil {
			return err
		}
		delete(cache.iamSTSAccountsMap, ui.Credentials.AccessKey)
		revoked = true
	}

	if revoked {
		cache.updatedAt = time.Now()
	}

	return nil
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
			cache.iamUserPolicyMap.Delete(user)

			// we are only logging errors, not handling them.
			err := store.deleteUserIdentity(ctx, user, userType)
			iamLogIf(GlobalContext, err)
			if userType == stsUser {
				delete(cache.iamSTSAccountsMap, user)
			}
			delete(cache.iamUsersMap, user)
			if store.group != nil {
				store.group.Forget(user)
			}

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
// policy-claim based OpenID providers. The root credential as a parent
// user is not included in the result.
func (store *IAMStoreSys) GetAllParentUsers() map[string]ParentUserInfo {
	cache := store.rlock()
	defer store.runlock()

	return store.getParentUsers(cache)
}

// assumes store is locked by caller.
func (store *IAMStoreSys) getParentUsers(cache *iamCache) map[string]ParentUserInfo {
	res := map[string]ParentUserInfo{}
	for _, ui := range cache.iamUsersMap {
		cred := ui.Credentials
		// Only consider service account or STS credentials with
		// non-empty session tokens.
		if (!cred.IsServiceAccount() && !cred.IsTemp()) ||
			cred.SessionToken == "" {
			continue
		}

		var (
			err    error
			claims *jwt.MapClaims
		)

		if cred.IsServiceAccount() {
			claims, err = getClaimsFromTokenWithSecret(cred.SessionToken, cred.SecretKey)
		} else if cred.IsTemp() {
			var secretKey string
			secretKey, err = getTokenSigningKey()
			if err != nil {
				continue
			}
			claims, err = getClaimsFromTokenWithSecret(cred.SessionToken, secretKey)
		}

		if err != nil {
			continue
		}
		if cred.ParentUser == "" || cred.ParentUser == globalActiveCred.AccessKey {
			continue
		}

		subClaimValue := cred.ParentUser
		if v, ok := claims.Lookup(subClaim); ok {
			subClaimValue = v
		}
		if v, ok := claims.Lookup(ldapActualUser); ok {
			subClaimValue = v
		}

		roleArn := openid.DummyRoleARN.String()
		s, ok := claims.Lookup(roleArnClaim)
		if ok {
			roleArn = s
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

// GetAllSTSUserMappings - Loads all STS user policy mappings from storage and
// returns them. Also gets any STS users that do not have policy mappings but have
// Service Accounts or STS keys (This is useful if the user is part of a group)
func (store *IAMStoreSys) GetAllSTSUserMappings(userPredicate func(string) bool) (map[string]string, error) {
	cache := store.rlock()
	defer store.runlock()

	stsMap := make(map[string]string)
	m := xsync.NewMapOf[string, MappedPolicy]()
	if err := store.loadMappedPolicies(context.Background(), stsUser, false, m); err != nil {
		return nil, err
	}

	m.Range(func(user string, mappedPolicy MappedPolicy) bool {
		if userPredicate != nil && !userPredicate(user) {
			return true
		}
		stsMap[user] = mappedPolicy.Policies
		return true
	})

	for user := range store.getParentUsers(cache) {
		if _, ok := stsMap[user]; !ok {
			if userPredicate != nil && !userPredicate(user) {
				continue
			}
			stsMap[user] = ""
		}
	}
	return stsMap, nil
}

// Assumes store is locked by caller. If userMap is empty, returns all user mappings.
func (store *IAMStoreSys) listUserPolicyMappings(cache *iamCache, userMap map[string]set.StringSet,
	userPredicate func(string) bool, decodeFunc func(string) string,
) []madmin.UserPolicyEntities {
	stsMap := xsync.NewMapOf[string, MappedPolicy]()
	resMap := make(map[string]madmin.UserPolicyEntities, len(userMap))

	for user, groupSet := range userMap {
		// Attempt to load parent user mapping for STS accounts
		store.loadMappedPolicy(context.TODO(), user, stsUser, false, stsMap)
		decodeUser := user
		if decodeFunc != nil {
			decodeUser = decodeFunc(user)
		}
		blankEntities := madmin.UserPolicyEntities{User: decodeUser}
		if !groupSet.IsEmpty() {
			blankEntities.MemberOfMappings = store.listGroupPolicyMappings(cache, groupSet, nil, decodeFunc)
		}
		resMap[user] = blankEntities
	}

	var r []madmin.UserPolicyEntities
	cache.iamUserPolicyMap.Range(func(user string, mappedPolicy MappedPolicy) bool {
		if userPredicate != nil && !userPredicate(user) {
			return true
		}

		entitiesWithMemberOf, ok := resMap[user]
		if !ok {
			if len(userMap) > 0 {
				return true
			}
			decodeUser := user
			if decodeFunc != nil {
				decodeUser = decodeFunc(user)
			}
			entitiesWithMemberOf = madmin.UserPolicyEntities{User: decodeUser}
		}

		ps := mappedPolicy.toSlice()
		sort.Strings(ps)
		entitiesWithMemberOf.Policies = ps
		resMap[user] = entitiesWithMemberOf
		return true
	})

	stsMap.Range(func(user string, mappedPolicy MappedPolicy) bool {
		if userPredicate != nil && !userPredicate(user) {
			return true
		}

		entitiesWithMemberOf := resMap[user]

		ps := mappedPolicy.toSlice()
		sort.Strings(ps)
		entitiesWithMemberOf.Policies = ps
		resMap[user] = entitiesWithMemberOf
		return true
	})

	for _, v := range resMap {
		if v.Policies != nil || v.MemberOfMappings != nil {
			r = append(r, v)
		}
	}

	sort.Slice(r, func(i, j int) bool {
		return r[i].User < r[j].User
	})

	return r
}

// Assumes store is locked by caller. If groups is empty, returns all group mappings.
func (store *IAMStoreSys) listGroupPolicyMappings(cache *iamCache, groupsSet set.StringSet,
	groupPredicate func(string) bool, decodeFunc func(string) string,
) []madmin.GroupPolicyEntities {
	var r []madmin.GroupPolicyEntities

	cache.iamGroupPolicyMap.Range(func(group string, mappedPolicy MappedPolicy) bool {
		if groupPredicate != nil && !groupPredicate(group) {
			return true
		}

		if !groupsSet.IsEmpty() && !groupsSet.Contains(group) {
			return true
		}

		decodeGroup := group
		if decodeFunc != nil {
			decodeGroup = decodeFunc(group)
		}

		ps := mappedPolicy.toSlice()
		sort.Strings(ps)
		r = append(r, madmin.GroupPolicyEntities{
			Group:    decodeGroup,
			Policies: ps,
		})
		return true
	})

	sort.Slice(r, func(i, j int) bool {
		return r[i].Group < r[j].Group
	})

	return r
}

// Assumes store is locked by caller. If policies is empty, returns all policy mappings.
func (store *IAMStoreSys) listPolicyMappings(cache *iamCache, queryPolSet set.StringSet,
	userPredicate, groupPredicate func(string) bool, decodeFunc func(string) string,
) []madmin.PolicyEntities {
	policyToUsersMap := make(map[string]set.StringSet)
	cache.iamUserPolicyMap.Range(func(user string, mappedPolicy MappedPolicy) bool {
		if userPredicate != nil && !userPredicate(user) {
			return true
		}

		decodeUser := user
		if decodeFunc != nil {
			decodeUser = decodeFunc(user)
		}

		commonPolicySet := mappedPolicy.policySet()
		if !queryPolSet.IsEmpty() {
			commonPolicySet = commonPolicySet.Intersection(queryPolSet)
		}
		for _, policy := range commonPolicySet.ToSlice() {
			s, ok := policyToUsersMap[policy]
			if !ok {
				policyToUsersMap[policy] = set.CreateStringSet(decodeUser)
			} else {
				s.Add(decodeUser)
				policyToUsersMap[policy] = s
			}
		}
		return true
	})

	if iamOS, ok := store.IAMStorageAPI.(*IAMObjectStore); ok {
		for item := range listIAMConfigItems(context.Background(), iamOS.objAPI, iamConfigPrefix+SlashSeparator+policyDBSTSUsersListKey) {
			user := strings.TrimSuffix(item.Item, ".json")
			if userPredicate != nil && !userPredicate(user) {
				continue
			}

			decodeUser := user
			if decodeFunc != nil {
				decodeUser = decodeFunc(user)
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
					policyToUsersMap[policy] = set.CreateStringSet(decodeUser)
				} else {
					s.Add(decodeUser)
					policyToUsersMap[policy] = s
				}
			}
		}
	}
	if iamOS, ok := store.IAMStorageAPI.(*IAMEtcdStore); ok {
		m := xsync.NewMapOf[string, MappedPolicy]()
		err := iamOS.loadMappedPolicies(context.Background(), stsUser, false, m)
		if err == nil {
			m.Range(func(user string, mappedPolicy MappedPolicy) bool {
				if userPredicate != nil && !userPredicate(user) {
					return true
				}

				decodeUser := user
				if decodeFunc != nil {
					decodeUser = decodeFunc(user)
				}

				commonPolicySet := mappedPolicy.policySet()
				if !queryPolSet.IsEmpty() {
					commonPolicySet = commonPolicySet.Intersection(queryPolSet)
				}
				for _, policy := range commonPolicySet.ToSlice() {
					s, ok := policyToUsersMap[policy]
					if !ok {
						policyToUsersMap[policy] = set.CreateStringSet(decodeUser)
					} else {
						s.Add(decodeUser)
						policyToUsersMap[policy] = s
					}
				}
				return true
			})
		}
	}

	policyToGroupsMap := make(map[string]set.StringSet)
	cache.iamGroupPolicyMap.Range(func(group string, mappedPolicy MappedPolicy) bool {
		if groupPredicate != nil && !groupPredicate(group) {
			return true
		}

		decodeGroup := group
		if decodeFunc != nil {
			decodeGroup = decodeFunc(group)
		}

		commonPolicySet := mappedPolicy.policySet()
		if !queryPolSet.IsEmpty() {
			commonPolicySet = commonPolicySet.Intersection(queryPolSet)
		}
		for _, policy := range commonPolicySet.ToSlice() {
			s, ok := policyToGroupsMap[policy]
			if !ok {
				policyToGroupsMap[policy] = set.CreateStringSet(decodeGroup)
			} else {
				s.Add(decodeGroup)
				policyToGroupsMap[policy] = s
			}
		}
		return true
	})

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
func (store *IAMStoreSys) ListPolicyMappings(q cleanEntitiesQuery,
	userPredicate, groupPredicate func(string) bool, decodeFunc func(string) string,
) madmin.PolicyEntitiesResult {
	cache := store.rlock()
	defer store.runlock()

	var result madmin.PolicyEntitiesResult

	isAllPoliciesQuery := len(q.Users) == 0 && len(q.Groups) == 0 && len(q.Policies) == 0

	if len(q.Users) > 0 {
		result.UserMappings = store.listUserPolicyMappings(cache, q.Users, userPredicate, decodeFunc)
	}
	if len(q.Groups) > 0 {
		result.GroupMappings = store.listGroupPolicyMappings(cache, q.Groups, groupPredicate, decodeFunc)
	}
	if len(q.Policies) > 0 || isAllPoliciesQuery {
		result.PolicyMappings = store.listPolicyMappings(cache, q.Policies, userPredicate, groupPredicate, decodeFunc)
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
	m.Delete(sessionPolicyNameExtracted)

	nosp := opts.sessionPolicy == nil || opts.sessionPolicy.Version == "" && len(opts.sessionPolicy.Statements) == 0

	// sessionPolicy is nil and there is embedded policy attached we remove
	// embedded policy at that point.
	if _, ok := m.Lookup(policy.SessionPolicyName); ok && nosp {
		m.Delete(policy.SessionPolicyName)
		m.Set(iamPolicyClaimNameSA(), inheritedPolicyType)
	}

	if opts.sessionPolicy != nil { // session policies is being updated
		if err := opts.sessionPolicy.Validate(); err != nil {
			return updatedAt, err
		}

		if opts.sessionPolicy.Version != "" && len(opts.sessionPolicy.Statements) > 0 {
			policyBuf, err := json.Marshal(opts.sessionPolicy)
			if err != nil {
				return updatedAt, err
			}

			if len(policyBuf) > maxSVCSessionPolicySize {
				return updatedAt, errSessionPolicyTooLarge
			}

			// Overwrite session policy claims.
			m.Set(policy.SessionPolicyName, base64.StdEncoding.EncodeToString(policyBuf))
			m.Set(iamPolicyClaimNameSA(), embeddedPolicyType)
		}
	}

	cr.SessionToken, err = auth.JWTSignWithAccessKey(accessKey, m.Map(), cr.SecretKey)
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

// ListAccessKeys - lists all access keys (sts/service accounts)
func (store *IAMStoreSys) ListAccessKeys(ctx context.Context) ([]auth.Credentials, error) {
	cache := store.rlock()
	defer store.runlock()

	accessKeys := store.getSTSAndServiceAccounts(cache)
	for i, accessKey := range accessKeys {
		accessKeys[i].SecretKey = ""
		if accessKey.IsTemp() {
			secret, err := getTokenSigningKey()
			if err != nil {
				return nil, err
			}
			claims, err := getClaimsFromTokenWithSecret(accessKey.SessionToken, secret)
			if err != nil {
				continue // ignore invalid session tokens
			}
			accessKeys[i].Claims = claims.MapClaims
		}
		accessKeys[i].SessionToken = ""
	}

	return accessKeys, nil
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

	return store.getSTSAndServiceAccounts(cache)
}

func (store *IAMStoreSys) getSTSAndServiceAccounts(cache *iamCache) []auth.Credentials {
	var res []auth.Credentials
	for _, u := range cache.iamUsersMap {
		cred := u.Credentials
		if cred.IsServiceAccount() {
			res = append(res, cred)
		}
	}
	for _, u := range cache.iamSTSAccountsMap {
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
func (store *IAMStoreSys) LoadUser(ctx context.Context, accessKey string) error {
	groupLoad := env.Get("_MINIO_IAM_GROUP_REFRESH", config.EnableOff) == config.EnableOn

	newCachePopulate := func() (val any, err error) {
		newCache := newIamCache()

		// Check for service account first
		store.loadUser(ctx, accessKey, svcUser, newCache.iamUsersMap)

		svc, found := newCache.iamUsersMap[accessKey]
		if found {
			// Load parent user and mapped policies.
			if store.getUsersSysType() == MinIOUsersSysType {
				err = store.loadUser(ctx, svc.Credentials.ParentUser, regUser, newCache.iamUsersMap)
				// NOTE: we are not worried about loading errors from policies.
				store.loadMappedPolicyWithRetry(ctx, svc.Credentials.ParentUser, regUser, false, newCache.iamUserPolicyMap, 3)
			} else {
				// In case of LDAP the parent user's policy mapping needs to be loaded into sts map
				// NOTE: we are not worried about loading errors from policies.
				store.loadMappedPolicyWithRetry(ctx, svc.Credentials.ParentUser, stsUser, false, newCache.iamSTSPolicyMap, 3)
			}
		}

		if !found {
			err = store.loadUser(ctx, accessKey, regUser, newCache.iamUsersMap)
			if _, found = newCache.iamUsersMap[accessKey]; found {
				// NOTE: we are not worried about loading errors from policies.
				store.loadMappedPolicyWithRetry(ctx, accessKey, regUser, false, newCache.iamUserPolicyMap, 3)
			}
		}

		// Check for STS account
		var stsUserCred UserIdentity
		if !found {
			err = store.loadUser(ctx, accessKey, stsUser, newCache.iamSTSAccountsMap)
			if stsUserCred, found = newCache.iamSTSAccountsMap[accessKey]; found {
				// Load mapped policy
				// NOTE: we are not worried about loading errors from policies.
				store.loadMappedPolicyWithRetry(ctx, stsUserCred.Credentials.ParentUser, stsUser, false, newCache.iamSTSPolicyMap, 3)
			}
		}

		// Load any associated policy definitions
		pols, _ := newCache.iamUserPolicyMap.Load(accessKey)
		for _, policy := range pols.toSlice() {
			if _, found = newCache.iamPolicyDocsMap[policy]; !found {
				// NOTE: we are not worried about loading errors from policies.
				store.loadPolicyDocWithRetry(ctx, policy, newCache.iamPolicyDocsMap, 3)
			}
		}

		pols, _ = newCache.iamSTSPolicyMap.Load(stsUserCred.Credentials.AccessKey)
		for _, policy := range pols.toSlice() {
			if _, found = newCache.iamPolicyDocsMap[policy]; !found {
				// NOTE: we are not worried about loading errors from policies.
				store.loadPolicyDocWithRetry(ctx, policy, newCache.iamPolicyDocsMap, 3)
			}
		}

		if groupLoad {
			// NOTE: we are not worried about loading errors from groups.
			store.updateGroups(ctx, newCache)
			newCache.buildUserGroupMemberships()
		}

		return newCache, err
	}

	var (
		val any
		err error
	)
	if store.group != nil {
		val, err, _ = store.group.Do(accessKey, newCachePopulate)
	} else {
		val, err = newCachePopulate()
	}

	// Return error right away if any.
	if err != nil {
		if errors.Is(err, errNoSuchUser) || errors.Is(err, errConfigNotFound) {
			return nil
		}
		return err
	}

	newCache, ok := val.(*iamCache)
	if !ok {
		return nil
	}

	cache := store.lock()
	defer store.unlock()

	// We need to merge the new cache with the existing cache because the
	// periodic IAM reload is partial. The periodic load here is to account.
	newCache.iamGroupPolicyMap.Range(func(k string, v MappedPolicy) bool {
		cache.iamGroupPolicyMap.Store(k, v)
		return true
	})

	maps.Copy(cache.iamGroupsMap, newCache.iamGroupsMap)

	maps.Copy(cache.iamPolicyDocsMap, newCache.iamPolicyDocsMap)

	maps.Copy(cache.iamUserGroupMemberships, newCache.iamUserGroupMemberships)

	newCache.iamUserPolicyMap.Range(func(k string, v MappedPolicy) bool {
		cache.iamUserPolicyMap.Store(k, v)
		return true
	})

	maps.Copy(cache.iamUsersMap, newCache.iamUsersMap)

	maps.Copy(cache.iamSTSAccountsMap, newCache.iamSTSAccountsMap)

	newCache.iamSTSPolicyMap.Range(func(k string, v MappedPolicy) bool {
		cache.iamSTSPolicyMap.Store(k, v)
		return true
	})

	cache.updatedAt = time.Now()

	return nil
}

func extractJWTClaims(u UserIdentity) (jwtClaims *jwt.MapClaims, err error) {
	keys := make([]string, 0, 3)

	// Append credentials secret key itself
	keys = append(keys, u.Credentials.SecretKey)

	// Use site-replication credentials if found
	if globalSiteReplicationSys.isEnabled() {
		secretKey, err := getTokenSigningKey()
		if err != nil {
			return nil, err
		}
		keys = append(keys, secretKey)
	}

	// Iterate over all keys and return with the first successful claim extraction
	for _, key := range keys {
		jwtClaims, err = getClaimsFromTokenWithSecret(u.Credentials.SessionToken, key)
		if err == nil {
			break
		}
	}
	return jwtClaims, err
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
