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
	"errors"
	"fmt"
	"maps"
	"path"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/config"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/kms"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v3/sync/errgroup"
	"github.com/puzpuzpuz/xsync/v3"
)

// IAMObjectStore implements IAMStorageAPI
type IAMObjectStore struct {
	// Protect access to storage within the current server.
	sync.RWMutex

	*iamCache

	usersSysType UsersSysType

	objAPI ObjectLayer
}

func newIAMObjectStore(objAPI ObjectLayer, usersSysType UsersSysType) *IAMObjectStore {
	return &IAMObjectStore{
		iamCache:     newIamCache(),
		objAPI:       objAPI,
		usersSysType: usersSysType,
	}
}

func (iamOS *IAMObjectStore) rlock() *iamCache {
	iamOS.RLock()
	return iamOS.iamCache
}

func (iamOS *IAMObjectStore) runlock() {
	iamOS.RUnlock()
}

func (iamOS *IAMObjectStore) lock() *iamCache {
	iamOS.Lock()
	return iamOS.iamCache
}

func (iamOS *IAMObjectStore) unlock() {
	iamOS.Unlock()
}

func (iamOS *IAMObjectStore) getUsersSysType() UsersSysType {
	return iamOS.usersSysType
}

func (iamOS *IAMObjectStore) saveIAMConfig(ctx context.Context, item any, objPath string, opts ...options) error {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	data, err := json.Marshal(item)
	if err != nil {
		return err
	}
	if GlobalKMS != nil {
		data, err = config.EncryptBytes(GlobalKMS, data, kms.Context{
			minioMetaBucket: path.Join(minioMetaBucket, objPath),
		})
		if err != nil {
			return err
		}
	}
	return saveConfig(ctx, iamOS.objAPI, objPath, data)
}

func decryptData(data []byte, objPath string) ([]byte, error) {
	if utf8.Valid(data) {
		return data, nil
	}

	pdata, err := madmin.DecryptData(globalActiveCred.String(), bytes.NewReader(data))
	if err == nil {
		return pdata, nil
	}
	if GlobalKMS != nil {
		pdata, err = config.DecryptBytes(GlobalKMS, data, kms.Context{
			minioMetaBucket: path.Join(minioMetaBucket, objPath),
		})
		if err == nil {
			return pdata, nil
		}
		pdata, err = config.DecryptBytes(GlobalKMS, data, kms.Context{
			minioMetaBucket: objPath,
		})
		if err == nil {
			return pdata, nil
		}
	}
	return nil, err
}

func (iamOS *IAMObjectStore) loadIAMConfigBytesWithMetadata(ctx context.Context, objPath string) ([]byte, ObjectInfo, error) {
	data, meta, err := readConfigWithMetadata(ctx, iamOS.objAPI, objPath, ObjectOptions{})
	if err != nil {
		return nil, meta, err
	}
	data, err = decryptData(data, objPath)
	if err != nil {
		return nil, meta, err
	}
	return data, meta, nil
}

func (iamOS *IAMObjectStore) loadIAMConfig(ctx context.Context, item any, objPath string) error {
	data, _, err := iamOS.loadIAMConfigBytesWithMetadata(ctx, objPath)
	if err != nil {
		return err
	}
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	return json.Unmarshal(data, item)
}

func (iamOS *IAMObjectStore) deleteIAMConfig(ctx context.Context, path string) error {
	return deleteConfig(ctx, iamOS.objAPI, path)
}

func (iamOS *IAMObjectStore) loadPolicyDocWithRetry(ctx context.Context, policy string, m map[string]PolicyDoc, retries int) error {
	for {
	retry:
		data, objInfo, err := iamOS.loadIAMConfigBytesWithMetadata(ctx, getPolicyDocPath(policy))
		if err != nil {
			if err == errConfigNotFound {
				return errNoSuchPolicy
			}
			retries--
			if retries <= 0 {
				return err
			}
			time.Sleep(500 * time.Millisecond)
			goto retry
		}

		var p PolicyDoc
		err = p.parseJSON(data)
		if err != nil {
			return err
		}

		if p.Version == 0 {
			// This means that policy was in the old version (without any
			// timestamp info). We fetch the mod time of the file and save
			// that as create and update date.
			p.CreateDate = objInfo.ModTime
			p.UpdateDate = objInfo.ModTime
		}

		m[policy] = p
		return nil
	}
}

func (iamOS *IAMObjectStore) loadPolicy(ctx context.Context, policy string) (PolicyDoc, error) {
	var p PolicyDoc

	data, objInfo, err := iamOS.loadIAMConfigBytesWithMetadata(ctx, getPolicyDocPath(policy))
	if err != nil {
		if err == errConfigNotFound {
			return p, errNoSuchPolicy
		}
		return p, err
	}

	err = p.parseJSON(data)
	if err != nil {
		return p, err
	}

	if p.Version == 0 {
		// This means that policy was in the old version (without any
		// timestamp info). We fetch the mod time of the file and save
		// that as create and update date.
		p.CreateDate = objInfo.ModTime
		p.UpdateDate = objInfo.ModTime
	}

	return p, nil
}

func (iamOS *IAMObjectStore) loadPolicyDoc(ctx context.Context, policy string, m map[string]PolicyDoc) error {
	p, err := iamOS.loadPolicy(ctx, policy)
	if err != nil {
		return err
	}
	m[policy] = p
	return nil
}

func (iamOS *IAMObjectStore) loadPolicyDocs(ctx context.Context, m map[string]PolicyDoc) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for item := range listIAMConfigItems(ctx, iamOS.objAPI, iamConfigPoliciesPrefix) {
		if item.Err != nil {
			return item.Err
		}

		policyName := path.Dir(item.Item)
		if err := iamOS.loadPolicyDoc(ctx, policyName, m); err != nil && !errors.Is(err, errNoSuchPolicy) {
			return err
		}
	}
	return nil
}

func (iamOS *IAMObjectStore) loadSecretKey(ctx context.Context, user string, userType IAMUserType) (string, error) {
	var u UserIdentity
	err := iamOS.loadIAMConfig(ctx, &u, getUserIdentityPath(user, userType))
	if err != nil {
		if errors.Is(err, errConfigNotFound) {
			return "", errNoSuchUser
		}
		return "", err
	}
	return u.Credentials.SecretKey, nil
}

func (iamOS *IAMObjectStore) loadUserIdentity(ctx context.Context, user string, userType IAMUserType) (UserIdentity, error) {
	var u UserIdentity
	err := iamOS.loadIAMConfig(ctx, &u, getUserIdentityPath(user, userType))
	if err != nil {
		if err == errConfigNotFound {
			return u, errNoSuchUser
		}
		return u, err
	}

	if u.Credentials.IsExpired() {
		// Delete expired identity - ignoring errors here.
		iamOS.deleteIAMConfig(ctx, getUserIdentityPath(user, userType))
		iamOS.deleteIAMConfig(ctx, getMappedPolicyPath(user, userType, false))
		return u, errNoSuchUser
	}

	if u.Credentials.AccessKey == "" {
		u.Credentials.AccessKey = user
	}

	if u.Credentials.SessionToken != "" {
		jwtClaims, err := extractJWTClaims(u)
		if err != nil {
			if u.Credentials.IsTemp() {
				// We should delete such that the client can re-request
				// for the expiring credentials.
				iamOS.deleteIAMConfig(ctx, getUserIdentityPath(user, userType))
				iamOS.deleteIAMConfig(ctx, getMappedPolicyPath(user, userType, false))
			}
			return u, errNoSuchUser
		}
		u.Credentials.Claims = jwtClaims.Map()
	}

	if u.Credentials.Description == "" {
		u.Credentials.Description = u.Credentials.Comment
	}

	return u, nil
}

func (iamOS *IAMObjectStore) loadUserConcurrent(ctx context.Context, userType IAMUserType, users ...string) ([]UserIdentity, error) {
	userIdentities := make([]UserIdentity, len(users))
	g := errgroup.WithNErrs(len(users))

	for index := range users {
		g.Go(func() error {
			userName := path.Dir(users[index])
			user, err := iamOS.loadUserIdentity(ctx, userName, userType)
			if err != nil && !errors.Is(err, errNoSuchUser) {
				return fmt.Errorf("unable to load the user `%s`: %w", userName, err)
			}
			userIdentities[index] = user
			return nil
		}, index)
	}

	err := errors.Join(g.Wait()...)
	return userIdentities, err
}

func (iamOS *IAMObjectStore) loadUser(ctx context.Context, user string, userType IAMUserType, m map[string]UserIdentity) error {
	u, err := iamOS.loadUserIdentity(ctx, user, userType)
	if err != nil {
		return err
	}
	m[user] = u
	return nil
}

func (iamOS *IAMObjectStore) loadUsers(ctx context.Context, userType IAMUserType, m map[string]UserIdentity) error {
	var basePrefix string
	switch userType {
	case svcUser:
		basePrefix = iamConfigServiceAccountsPrefix
	case stsUser:
		basePrefix = iamConfigSTSPrefix
	default:
		basePrefix = iamConfigUsersPrefix
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for item := range listIAMConfigItems(ctx, iamOS.objAPI, basePrefix) {
		if item.Err != nil {
			return item.Err
		}

		userName := path.Dir(item.Item)
		if err := iamOS.loadUser(ctx, userName, userType, m); err != nil && err != errNoSuchUser {
			return err
		}
	}
	return nil
}

func (iamOS *IAMObjectStore) loadGroup(ctx context.Context, group string, m map[string]GroupInfo) error {
	var g GroupInfo
	err := iamOS.loadIAMConfig(ctx, &g, getGroupInfoPath(group))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchGroup
		}
		return err
	}
	m[group] = g
	return nil
}

func (iamOS *IAMObjectStore) loadGroups(ctx context.Context, m map[string]GroupInfo) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for item := range listIAMConfigItems(ctx, iamOS.objAPI, iamConfigGroupsPrefix) {
		if item.Err != nil {
			return item.Err
		}

		group := path.Dir(item.Item)
		if err := iamOS.loadGroup(ctx, group, m); err != nil && err != errNoSuchGroup {
			return err
		}
	}
	return nil
}

func (iamOS *IAMObjectStore) loadMappedPolicyWithRetry(ctx context.Context, name string, userType IAMUserType, isGroup bool, m *xsync.MapOf[string, MappedPolicy], retries int) error {
	for {
	retry:
		var p MappedPolicy
		err := iamOS.loadIAMConfig(ctx, &p, getMappedPolicyPath(name, userType, isGroup))
		if err != nil {
			if err == errConfigNotFound {
				return errNoSuchPolicy
			}
			retries--
			if retries <= 0 {
				return err
			}
			time.Sleep(500 * time.Millisecond)
			goto retry
		}

		m.Store(name, p)
		return nil
	}
}

func (iamOS *IAMObjectStore) loadMappedPolicyInternal(ctx context.Context, name string, userType IAMUserType, isGroup bool) (MappedPolicy, error) {
	var p MappedPolicy
	err := iamOS.loadIAMConfig(ctx, &p, getMappedPolicyPath(name, userType, isGroup))
	if err != nil {
		if err == errConfigNotFound {
			return p, errNoSuchPolicy
		}
		return p, err
	}
	return p, nil
}

func (iamOS *IAMObjectStore) loadMappedPolicyConcurrent(ctx context.Context, userType IAMUserType, isGroup bool, users ...string) ([]MappedPolicy, error) {
	mappedPolicies := make([]MappedPolicy, len(users))
	g := errgroup.WithNErrs(len(users))

	for index := range users {
		g.Go(func() error {
			userName := strings.TrimSuffix(users[index], ".json")
			userMP, err := iamOS.loadMappedPolicyInternal(ctx, userName, userType, isGroup)
			if err != nil && !errors.Is(err, errNoSuchPolicy) {
				return fmt.Errorf("unable to load the user policy map `%s`: %w", userName, err)
			}
			mappedPolicies[index] = userMP
			return nil
		}, index)
	}

	err := errors.Join(g.Wait()...)
	return mappedPolicies, err
}

func (iamOS *IAMObjectStore) loadMappedPolicy(ctx context.Context, name string, userType IAMUserType, isGroup bool, m *xsync.MapOf[string, MappedPolicy]) error {
	p, err := iamOS.loadMappedPolicyInternal(ctx, name, userType, isGroup)
	if err != nil {
		return err
	}
	m.Store(name, p)
	return nil
}

func (iamOS *IAMObjectStore) loadMappedPolicies(ctx context.Context, userType IAMUserType, isGroup bool, m *xsync.MapOf[string, MappedPolicy]) error {
	var basePath string
	if isGroup {
		basePath = iamConfigPolicyDBGroupsPrefix
	} else {
		switch userType {
		case svcUser:
			basePath = iamConfigPolicyDBServiceAccountsPrefix
		case stsUser:
			basePath = iamConfigPolicyDBSTSUsersPrefix
		default:
			basePath = iamConfigPolicyDBUsersPrefix
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for item := range listIAMConfigItems(ctx, iamOS.objAPI, basePath) {
		if item.Err != nil {
			return item.Err
		}

		policyFile := item.Item
		userOrGroupName := strings.TrimSuffix(policyFile, ".json")
		if err := iamOS.loadMappedPolicy(ctx, userOrGroupName, userType, isGroup, m); err != nil && !errors.Is(err, errNoSuchPolicy) {
			return err
		}
	}
	return nil
}

var (
	usersListKey            = "users/"
	svcAccListKey           = "service-accounts/"
	groupsListKey           = "groups/"
	policiesListKey         = "policies/"
	stsListKey              = "sts/"
	policyDBPrefix          = "policydb/"
	policyDBUsersListKey    = "policydb/users/"
	policyDBSTSUsersListKey = "policydb/sts-users/"
	policyDBGroupsListKey   = "policydb/groups/"
)

func findSecondIndex(s string, substr string) int {
	first := strings.Index(s, substr)
	if first == -1 {
		return -1
	}
	second := strings.Index(s[first+1:], substr)
	if second == -1 {
		return -1
	}
	return first + second + 1
}

// splitPath splits a path into a top-level directory and a child item. The
// parent directory retains the trailing slash.
func splitPath(s string, secondIndex bool) (string, string) {
	var i int
	if secondIndex {
		i = findSecondIndex(s, "/")
	} else {
		i = strings.Index(s, "/")
	}
	if i == -1 {
		return s, ""
	}
	// Include the trailing slash in the parent directory.
	return s[:i+1], s[i+1:]
}

func (iamOS *IAMObjectStore) listAllIAMConfigItems(ctx context.Context) (res map[string][]string, err error) {
	res = make(map[string][]string)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for item := range listIAMConfigItems(ctx, iamOS.objAPI, iamConfigPrefix+SlashSeparator) {
		if item.Err != nil {
			return nil, item.Err
		}

		secondIndex := strings.HasPrefix(item.Item, policyDBPrefix)
		listKey, trimmedItem := splitPath(item.Item, secondIndex)
		if listKey == iamFormatFile {
			continue
		}

		res[listKey] = append(res[listKey], trimmedItem)
	}

	return res, nil
}

const (
	maxIAMLoadOpTime = 5 * time.Second
)

func (iamOS *IAMObjectStore) loadPolicyDocConcurrent(ctx context.Context, policies ...string) ([]PolicyDoc, error) {
	policyDocs := make([]PolicyDoc, len(policies))
	g := errgroup.WithNErrs(len(policies))

	for index := range policies {
		g.Go(func() error {
			policyName := path.Dir(policies[index])
			policyDoc, err := iamOS.loadPolicy(ctx, policyName)
			if err != nil && !errors.Is(err, errNoSuchPolicy) {
				return fmt.Errorf("unable to load the policy doc `%s`: %w", policyName, err)
			}
			policyDocs[index] = policyDoc
			return nil
		}, index)
	}

	err := errors.Join(g.Wait()...)
	return policyDocs, err
}

// Assumes cache is locked by caller.
func (iamOS *IAMObjectStore) loadAllFromObjStore(ctx context.Context, cache *iamCache, firstTime bool) error {
	bootstrapTraceMsgFirstTime := func(s string) {
		if firstTime {
			bootstrapTraceMsg(s)
		}
	}

	if iamOS.objAPI == nil {
		return errServerNotInitialized
	}

	bootstrapTraceMsgFirstTime("loading all IAM items")

	setDefaultCannedPolicies(cache.iamPolicyDocsMap)

	listStartTime := UTCNow()
	listedConfigItems, err := iamOS.listAllIAMConfigItems(ctx)
	if err != nil {
		return fmt.Errorf("unable to list IAM data: %w", err)
	}
	if took := time.Since(listStartTime); took > maxIAMLoadOpTime {
		var s strings.Builder
		for k, v := range listedConfigItems {
			s.WriteString(fmt.Sprintf("    %s: %d items\n", k, len(v)))
		}
		logger.Info("listAllIAMConfigItems took %.2fs with contents:\n%s", took.Seconds(), s.String())
	}

	// Loads things in the same order as `LoadIAMCache()`

	bootstrapTraceMsgFirstTime("loading policy documents")

	policyLoadStartTime := UTCNow()
	policiesList := listedConfigItems[policiesListKey]
	count := 32 // number of parallel IAM loaders
	for {
		if len(policiesList) < count {
			policyDocs, err := iamOS.loadPolicyDocConcurrent(ctx, policiesList...)
			if err != nil {
				return err
			}
			for index := range policiesList {
				if policyDocs[index].Policy.Version != "" {
					policyName := path.Dir(policiesList[index])
					cache.iamPolicyDocsMap[policyName] = policyDocs[index]
				}
			}
			break
		}

		policyDocs, err := iamOS.loadPolicyDocConcurrent(ctx, policiesList[:count]...)
		if err != nil {
			return err
		}

		for index := range policiesList[:count] {
			if policyDocs[index].Policy.Version != "" {
				policyName := path.Dir(policiesList[index])
				cache.iamPolicyDocsMap[policyName] = policyDocs[index]
			}
		}

		policiesList = policiesList[count:]
	}

	if took := time.Since(policyLoadStartTime); took > maxIAMLoadOpTime {
		logger.Info("Policy docs load took %.2fs (for %d items)", took.Seconds(), len(policiesList))
	}

	if iamOS.usersSysType == MinIOUsersSysType {
		bootstrapTraceMsgFirstTime("loading regular IAM users")
		regUsersLoadStartTime := UTCNow()
		regUsersList := listedConfigItems[usersListKey]

		for {
			if len(regUsersList) < count {
				users, err := iamOS.loadUserConcurrent(ctx, regUser, regUsersList...)
				if err != nil {
					return err
				}
				for index := range regUsersList {
					if users[index].Credentials.AccessKey != "" {
						userName := path.Dir(regUsersList[index])
						cache.iamUsersMap[userName] = users[index]
					}
				}
				break
			}

			users, err := iamOS.loadUserConcurrent(ctx, regUser, regUsersList[:count]...)
			if err != nil {
				return err
			}

			for index := range regUsersList[:count] {
				if users[index].Credentials.AccessKey != "" {
					userName := path.Dir(regUsersList[index])
					cache.iamUsersMap[userName] = users[index]
				}
			}

			regUsersList = regUsersList[count:]
		}

		if took := time.Since(regUsersLoadStartTime); took > maxIAMLoadOpTime {
			actualLoaded := len(cache.iamUsersMap)
			logger.Info("Reg. users load took %.2fs (for %d items with %d expired items)", took.Seconds(),
				len(regUsersList), len(regUsersList)-actualLoaded)
		}

		bootstrapTraceMsgFirstTime("loading regular IAM groups")
		groupsLoadStartTime := UTCNow()
		groupsList := listedConfigItems[groupsListKey]
		for _, item := range groupsList {
			group := path.Dir(item)
			if err := iamOS.loadGroup(ctx, group, cache.iamGroupsMap); err != nil && err != errNoSuchGroup {
				return fmt.Errorf("unable to load the group: %w", err)
			}
		}
		if took := time.Since(groupsLoadStartTime); took > maxIAMLoadOpTime {
			logger.Info("Groups load took %.2fs (for %d items)", took.Seconds(), len(groupsList))
		}
	}

	bootstrapTraceMsgFirstTime("loading user policy mapping")
	userPolicyMappingLoadStartTime := UTCNow()
	userPolicyMappingsList := listedConfigItems[policyDBUsersListKey]
	for {
		if len(userPolicyMappingsList) < count {
			mappedPolicies, err := iamOS.loadMappedPolicyConcurrent(ctx, regUser, false, userPolicyMappingsList...)
			if err != nil {
				return err
			}

			for index := range userPolicyMappingsList {
				if mappedPolicies[index].Policies != "" {
					userName := strings.TrimSuffix(userPolicyMappingsList[index], ".json")
					cache.iamUserPolicyMap.Store(userName, mappedPolicies[index])
				}
			}

			break
		}

		mappedPolicies, err := iamOS.loadMappedPolicyConcurrent(ctx, regUser, false, userPolicyMappingsList[:count]...)
		if err != nil {
			return err
		}

		for index := range userPolicyMappingsList[:count] {
			if mappedPolicies[index].Policies != "" {
				userName := strings.TrimSuffix(userPolicyMappingsList[index], ".json")
				cache.iamUserPolicyMap.Store(userName, mappedPolicies[index])
			}
		}

		userPolicyMappingsList = userPolicyMappingsList[count:]
	}

	if took := time.Since(userPolicyMappingLoadStartTime); took > maxIAMLoadOpTime {
		logger.Info("User policy mappings load took %.2fs (for %d items)", took.Seconds(), len(userPolicyMappingsList))
	}

	bootstrapTraceMsgFirstTime("loading group policy mapping")
	groupPolicyMappingLoadStartTime := UTCNow()
	groupPolicyMappingsList := listedConfigItems[policyDBGroupsListKey]
	for _, item := range groupPolicyMappingsList {
		groupName := strings.TrimSuffix(item, ".json")
		if err := iamOS.loadMappedPolicy(ctx, groupName, regUser, true, cache.iamGroupPolicyMap); err != nil && !errors.Is(err, errNoSuchPolicy) {
			return fmt.Errorf("unable to load the policy mapping for the group: %w", err)
		}
	}
	if took := time.Since(groupPolicyMappingLoadStartTime); took > maxIAMLoadOpTime {
		logger.Info("Group policy mappings load took %.2fs (for %d items)", took.Seconds(), len(groupPolicyMappingsList))
	}

	bootstrapTraceMsgFirstTime("loading service accounts")
	svcAccLoadStartTime := UTCNow()
	svcAccList := listedConfigItems[svcAccListKey]
	svcUsersMap := make(map[string]UserIdentity, len(svcAccList))
	for _, item := range svcAccList {
		userName := path.Dir(item)
		if err := iamOS.loadUser(ctx, userName, svcUser, svcUsersMap); err != nil && err != errNoSuchUser {
			return fmt.Errorf("unable to load the service account: %w", err)
		}
	}
	if took := time.Since(svcAccLoadStartTime); took > maxIAMLoadOpTime {
		logger.Info("Service accounts load took %.2fs (for %d items with %d expired items)", took.Seconds(),
			len(svcAccList), len(svcAccList)-len(svcUsersMap))
	}

	bootstrapTraceMsg("loading STS account policy mapping")
	stsPolicyMappingLoadStartTime := UTCNow()
	var stsPolicyMappingsCount int
	for _, svcAcc := range svcUsersMap {
		svcParent := svcAcc.Credentials.ParentUser
		if _, ok := cache.iamUsersMap[svcParent]; !ok {
			stsPolicyMappingsCount++
			// If a service account's parent user is not in iamUsersMap, the
			// parent is an STS account. Such accounts may have a policy mapped
			// on the parent user, so we load them. This is not needed for the
			// initial server startup, however, it is needed for the case where
			// the STS account's policy mapping (for example in LDAP mode) may
			// be changed and the user's policy mapping in memory is stale
			// (because the policy change notification was missed by the current
			// server).
			//
			// The "policy not found" error is ignored because the STS account may
			// not have a policy mapped via its parent (for e.g. in
			// OIDC/AssumeRoleWithCustomToken/AssumeRoleWithCertificate).
			err := iamOS.loadMappedPolicy(ctx, svcParent, stsUser, false, cache.iamSTSPolicyMap)
			if err != nil && !errors.Is(err, errNoSuchPolicy) {
				return fmt.Errorf("unable to load the policy mapping for the STS user: %w", err)
			}
		}
	}
	if took := time.Since(stsPolicyMappingLoadStartTime); took > maxIAMLoadOpTime {
		logger.Info("STS policy mappings load took %.2fs (for %d items)", took.Seconds(), stsPolicyMappingsCount)
	}

	// Copy svcUsersMap to cache.iamUsersMap
	maps.Copy(cache.iamUsersMap, svcUsersMap)

	cache.buildUserGroupMemberships()

	purgeStart := time.Now()

	// Purge expired STS credentials.

	// Scan STS users on disk and purge expired ones.
	stsAccountsFromStore := map[string]UserIdentity{}
	stsAccPoliciesFromStore := xsync.NewMapOf[string, MappedPolicy]()
	for _, item := range listedConfigItems[stsListKey] {
		userName := path.Dir(item)
		// loadUser() will delete expired user during the load.
		err := iamOS.loadUser(ctx, userName, stsUser, stsAccountsFromStore)
		if err != nil && !errors.Is(err, errNoSuchUser) {
			iamLogIf(ctx, err)
		}
		// No need to return errors for failed expiration of STS users
	}

	// Loading the STS policy mappings from disk ensures that stale entries
	// (removed during loadUser() in the loop above) are removed from memory.
	for _, item := range listedConfigItems[policyDBSTSUsersListKey] {
		stsName := strings.TrimSuffix(item, ".json")
		err := iamOS.loadMappedPolicy(ctx, stsName, stsUser, false, stsAccPoliciesFromStore)
		if err != nil && !errors.Is(err, errNoSuchPolicy) {
			iamLogIf(ctx, err)
		}
		// No need to return errors for failed expiration of STS users
	}

	took := time.Since(purgeStart).Seconds()
	if took > maxDurationSecondsForLog {
		// Log if we took a lot of time to load.
		logger.Info("IAM expired STS purge took %.2fs", took)
	}

	// Store the newly populated map in the iam cache. This takes care of
	// removing stale entries from the existing map.
	cache.iamSTSAccountsMap = stsAccountsFromStore

	stsAccPoliciesFromStore.Range(func(k string, v MappedPolicy) bool {
		cache.iamSTSPolicyMap.Store(k, v)
		return true
	})

	return nil
}

func (iamOS *IAMObjectStore) savePolicyDoc(ctx context.Context, policyName string, p PolicyDoc) error {
	return iamOS.saveIAMConfig(ctx, &p, getPolicyDocPath(policyName))
}

func (iamOS *IAMObjectStore) saveMappedPolicy(ctx context.Context, name string, userType IAMUserType, isGroup bool, mp MappedPolicy, opts ...options) error {
	return iamOS.saveIAMConfig(ctx, mp, getMappedPolicyPath(name, userType, isGroup), opts...)
}

func (iamOS *IAMObjectStore) saveUserIdentity(ctx context.Context, name string, userType IAMUserType, u UserIdentity, opts ...options) error {
	return iamOS.saveIAMConfig(ctx, u, getUserIdentityPath(name, userType), opts...)
}

func (iamOS *IAMObjectStore) saveGroupInfo(ctx context.Context, name string, gi GroupInfo) error {
	return iamOS.saveIAMConfig(ctx, gi, getGroupInfoPath(name))
}

func (iamOS *IAMObjectStore) deletePolicyDoc(ctx context.Context, name string) error {
	err := iamOS.deleteIAMConfig(ctx, getPolicyDocPath(name))
	if err == errConfigNotFound {
		err = errNoSuchPolicy
	}
	return err
}

func (iamOS *IAMObjectStore) deleteMappedPolicy(ctx context.Context, name string, userType IAMUserType, isGroup bool) error {
	err := iamOS.deleteIAMConfig(ctx, getMappedPolicyPath(name, userType, isGroup))
	if err == errConfigNotFound {
		err = errNoSuchPolicy
	}
	return err
}

func (iamOS *IAMObjectStore) deleteUserIdentity(ctx context.Context, name string, userType IAMUserType) error {
	err := iamOS.deleteIAMConfig(ctx, getUserIdentityPath(name, userType))
	if err == errConfigNotFound {
		err = errNoSuchUser
	}
	return err
}

func (iamOS *IAMObjectStore) deleteGroupInfo(ctx context.Context, name string) error {
	err := iamOS.deleteIAMConfig(ctx, getGroupInfoPath(name))
	if err == errConfigNotFound {
		err = errNoSuchGroup
	}
	return err
}

// Lists objects in the minioMetaBucket at the given path prefix. All returned
// items have the pathPrefix removed from their names.
func listIAMConfigItems(ctx context.Context, objAPI ObjectLayer, pathPrefix string) <-chan itemOrErr[string] {
	ch := make(chan itemOrErr[string])

	go func() {
		defer xioutil.SafeClose(ch)

		// Allocate new results channel to receive ObjectInfo.
		objInfoCh := make(chan itemOrErr[ObjectInfo])

		if err := objAPI.Walk(ctx, minioMetaBucket, pathPrefix, objInfoCh, WalkOptions{}); err != nil {
			select {
			case ch <- itemOrErr[string]{Err: err}:
			case <-ctx.Done():
			}
			return
		}

		for obj := range objInfoCh {
			if obj.Err != nil {
				select {
				case ch <- itemOrErr[string]{Err: obj.Err}:
				case <-ctx.Done():
					return
				}
			}
			item := strings.TrimPrefix(obj.Item.Name, pathPrefix)
			item = strings.TrimSuffix(item, SlashSeparator)
			select {
			case ch <- itemOrErr[string]{Item: item}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch
}
