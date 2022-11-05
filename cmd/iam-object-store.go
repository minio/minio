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
	"fmt"
	"path"
	"strings"
	"sync"
	"unicode/utf8"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/kms"
	"github.com/minio/minio/internal/logger"
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

func (iamOS *IAMObjectStore) saveIAMConfig(ctx context.Context, item interface{}, objPath string, opts ...options) error {
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

func (iamOS *IAMObjectStore) loadIAMConfigBytesWithMetadata(ctx context.Context, objPath string) ([]byte, ObjectInfo, error) {
	data, meta, err := readConfigWithMetadata(ctx, iamOS.objAPI, objPath, ObjectOptions{})
	if err != nil {
		return nil, meta, err
	}
	if !utf8.Valid(data) && GlobalKMS != nil {
		data, err = config.DecryptBytes(GlobalKMS, data, kms.Context{
			minioMetaBucket: path.Join(minioMetaBucket, objPath),
		})
		if err != nil {
			return nil, meta, err
		}
	}
	return data, meta, nil
}

func (iamOS *IAMObjectStore) loadIAMConfig(ctx context.Context, item interface{}, objPath string) error {
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

func (iamOS *IAMObjectStore) loadPolicyDoc(ctx context.Context, policy string, m map[string]PolicyDoc) error {
	data, objInfo, err := iamOS.loadIAMConfigBytesWithMetadata(ctx, getPolicyDocPath(policy))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchPolicy
		}
		return err
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

func (iamOS *IAMObjectStore) loadPolicyDocs(ctx context.Context, m map[string]PolicyDoc) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for item := range listIAMConfigItems(ctx, iamOS.objAPI, iamConfigPoliciesPrefix) {
		if item.Err != nil {
			return item.Err
		}

		policyName := path.Dir(item.Item)
		if err := iamOS.loadPolicyDoc(ctx, policyName, m); err != nil && err != errNoSuchPolicy {
			return err
		}
	}
	return nil
}

func (iamOS *IAMObjectStore) loadUser(ctx context.Context, user string, userType IAMUserType, m map[string]UserIdentity) error {
	var u UserIdentity
	err := iamOS.loadIAMConfig(ctx, &u, getUserIdentityPath(user, userType))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchUser
		}
		return err
	}

	if u.Credentials.IsExpired() {
		// Delete expired identity - ignoring errors here.
		iamOS.deleteIAMConfig(ctx, getUserIdentityPath(user, userType))
		iamOS.deleteIAMConfig(ctx, getMappedPolicyPath(user, userType, false))
		return nil
	}

	if u.Credentials.AccessKey == "" {
		u.Credentials.AccessKey = user
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

func (iamOS *IAMObjectStore) loadMappedPolicy(ctx context.Context, name string, userType IAMUserType, isGroup bool,
	m map[string]MappedPolicy,
) error {
	var p MappedPolicy
	err := iamOS.loadIAMConfig(ctx, &p, getMappedPolicyPath(name, userType, isGroup))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchPolicy
		}
		return err
	}
	m[name] = p
	return nil
}

func (iamOS *IAMObjectStore) loadMappedPolicies(ctx context.Context, userType IAMUserType, isGroup bool, m map[string]MappedPolicy) error {
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
		if err := iamOS.loadMappedPolicy(ctx, userOrGroupName, userType, isGroup, m); err != nil && err != errNoSuchPolicy {
			return err
		}
	}
	return nil
}

var (
	usersListKey                   = "users/"
	svcAccListKey                  = "service-accounts/"
	groupsListKey                  = "groups/"
	policiesListKey                = "policies/"
	stsListKey                     = "sts/"
	policyDBUsersListKey           = "policydb/users/"
	policyDBSTSUsersListKey        = "policydb/sts-users/"
	policyDBServiceAccountsListKey = "policydb/service-accounts/"
	policyDBGroupsListKey          = "policydb/groups/"

	allListKeys = []string{
		usersListKey,
		svcAccListKey,
		groupsListKey,
		policiesListKey,
		stsListKey,
		policyDBUsersListKey,
		policyDBSTSUsersListKey,
		policyDBServiceAccountsListKey,
		policyDBGroupsListKey,
	}
)

func (iamOS *IAMObjectStore) listAllIAMConfigItems(ctx context.Context) (map[string][]string, error) {
	res := make(map[string][]string)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for item := range listIAMConfigItems(ctx, iamOS.objAPI, iamConfigPrefix+SlashSeparator) {
		if item.Err != nil {
			return nil, item.Err
		}

		found := false
		for _, listKey := range allListKeys {
			if strings.HasPrefix(item.Item, listKey) {
				found = true
				name := strings.TrimPrefix(item.Item, listKey)
				res[listKey] = append(res[listKey], name)
				break
			}
		}

		if !found && (item.Item != "format.json") {
			logger.LogIf(ctx, fmt.Errorf("unknown type of IAM file listed: %v", item.Item))
		}
	}
	return res, nil
}

// Assumes cache is locked by caller.
func (iamOS *IAMObjectStore) loadAllFromObjStore(ctx context.Context, cache *iamCache) error {
	listedConfigItems, err := iamOS.listAllIAMConfigItems(ctx)
	if err != nil {
		return err
	}

	// Loads things in the same order as `LoadIAMCache()`

	policiesList := listedConfigItems[policiesListKey]
	for _, item := range policiesList {
		policyName := path.Dir(item)
		if err := iamOS.loadPolicyDoc(ctx, policyName, cache.iamPolicyDocsMap); err != nil && err != errNoSuchPolicy {
			return err
		}
	}
	setDefaultCannedPolicies(cache.iamPolicyDocsMap)

	if iamOS.usersSysType == MinIOUsersSysType {

		regUsersList := listedConfigItems[usersListKey]
		for _, item := range regUsersList {
			userName := path.Dir(item)
			if err := iamOS.loadUser(ctx, userName, regUser, cache.iamUsersMap); err != nil && err != errNoSuchUser {
				return err
			}
		}

		groupsList := listedConfigItems[groupsListKey]
		for _, item := range groupsList {
			group := path.Dir(item)
			if err := iamOS.loadGroup(ctx, group, cache.iamGroupsMap); err != nil && err != errNoSuchGroup {
				return err
			}
		}
	}

	userPolicyMappingsList := listedConfigItems[policyDBUsersListKey]
	for _, item := range userPolicyMappingsList {
		userName := strings.TrimSuffix(item, ".json")
		if err := iamOS.loadMappedPolicy(ctx, userName, regUser, false, cache.iamUserPolicyMap); err != nil && err != errNoSuchPolicy {
			return err
		}
	}

	groupPolicyMappingsList := listedConfigItems[policyDBGroupsListKey]
	for _, item := range groupPolicyMappingsList {
		groupName := strings.TrimSuffix(item, ".json")
		if err := iamOS.loadMappedPolicy(ctx, groupName, regUser, true, cache.iamGroupPolicyMap); err != nil && err != errNoSuchPolicy {
			return err
		}
	}

	svcAccList := listedConfigItems[svcAccListKey]
	for _, item := range svcAccList {
		userName := path.Dir(item)
		if err := iamOS.loadUser(ctx, userName, svcUser, cache.iamUsersMap); err != nil && err != errNoSuchUser {
			return err
		}
	}

	stsUsersList := listedConfigItems[stsListKey]
	for _, item := range stsUsersList {
		userName := path.Dir(item)
		if err := iamOS.loadUser(ctx, userName, stsUser, cache.iamUsersMap); err != nil && err != errNoSuchUser {
			return err
		}
	}

	stsPolicyMappingsList := listedConfigItems[policyDBSTSUsersListKey]
	for _, item := range stsPolicyMappingsList {
		stsName := strings.TrimSuffix(item, ".json")
		if err := iamOS.loadMappedPolicy(ctx, stsName, stsUser, false, cache.iamUserPolicyMap); err != nil && err != errNoSuchPolicy {
			return err
		}
	}

	cache.buildUserGroupMemberships()
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

// helper type for listIAMConfigItems
type itemOrErr struct {
	Item string
	Err  error
}

// Lists files or dirs in the minioMetaBucket at the given path
// prefix. If dirs is true, only directories are listed, otherwise
// only objects are listed. All returned items have the pathPrefix
// removed from their names.
func listIAMConfigItems(ctx context.Context, objAPI ObjectLayer, pathPrefix string) <-chan itemOrErr {
	ch := make(chan itemOrErr)

	go func() {
		defer close(ch)

		// Allocate new results channel to receive ObjectInfo.
		objInfoCh := make(chan ObjectInfo)

		if err := objAPI.Walk(ctx, minioMetaBucket, pathPrefix, objInfoCh, ObjectOptions{}); err != nil {
			select {
			case ch <- itemOrErr{Err: err}:
			case <-ctx.Done():
			}
			return
		}

		for obj := range objInfoCh {
			item := strings.TrimPrefix(obj.Name, pathPrefix)
			item = strings.TrimSuffix(item, SlashSeparator)
			select {
			case ch <- itemOrErr{Item: item}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch
}
