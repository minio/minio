/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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
	"errors"
	"path"
	"strings"
	"sync"
	"time"

	jwtgo "github.com/dgrijalva/jwt-go"
	"github.com/minio/minio/cmd/kv"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	iampolicy "github.com/minio/minio/pkg/iam/policy"
	"github.com/minio/minio/pkg/madmin"
)

var defaultContextTimeout = 30 * time.Second

// IAMKvStore implements IAMStorageAPI
type IAMKvStore struct {
	sync.RWMutex

	ctx context.Context

	backend kv.Backend
}

func newIAMKvStore(ctx context.Context, backend kv.Backend) *IAMKvStore {
	return &IAMKvStore{backend: backend, ctx: ctx}
}

func (kvs *IAMKvStore) lock() {
	kvs.Lock()
}

func (kvs *IAMKvStore) unlock() {
	kvs.Unlock()
}

func (kvs *IAMKvStore) rlock() {
	kvs.RLock()
}

func (kvs *IAMKvStore) runlock() {
	kvs.RUnlock()
}

func (kvs *IAMKvStore) saveIAMConfig(item interface{}, path string) error {
	data, err := json.Marshal(item)
	if err != nil {
		return err
	}
	if globalConfigEncrypted {
		data, err = madmin.EncryptData(globalActiveCred.String(), data)
		if err != nil {
			return err
		}
	}
	return kvs.backend.Save(kvs.ctx, path, data)
}

func (kvs *IAMKvStore) loadIAMConfig(item interface{}, path string) error {
	pdata, err := kvs.backend.Get(kvs.ctx, path)
	if err != nil {
		return err
	}
	if pdata == nil {
		return errConfigNotFound
	}
	if globalConfigEncrypted {
		pdata, err = madmin.DecryptData(globalActiveCred.String(), bytes.NewReader(pdata))
		if err != nil {
			return err
		}
	}
	return json.Unmarshal(pdata, item)
}

func (kvs *IAMKvStore) deleteIAMConfig(path string) error {
	return kvs.backend.Delete(kvs.ctx, path)
}

func (kvs *IAMKvStore) migrateUsersConfigToV1(ctx context.Context, isSTS bool) error {
	basePrefix := iamConfigUsersPrefix
	if isSTS {
		basePrefix = iamConfigSTSPrefix
	}

	ctx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	users, err := kvs.backend.Keys(ctx, basePrefix)
	if err != nil {
		return err
	}
	for _, user := range users.ToSlice() {
		{
			// 1. check if there is a policy file in the old loc.
			oldPolicyPath := pathJoin(basePrefix, user, iamPolicyFile)
			var policyName string
			err := kvs.loadIAMConfig(&policyName, oldPolicyPath)
			if err != nil {
				switch err {
				case errConfigNotFound:
					// No mapped policy or already migrated.
				default:
					// corrupt data/read error, etc
				}
				goto next
			}

			// 2. copy policy to new loc.
			mp := newMappedPolicy(policyName)
			userType := regularUser
			if isSTS {
				userType = stsUser
			}
			path := getMappedPolicyPath(user, userType, false)
			if err := kvs.saveIAMConfig(mp, path); err != nil {
				return err
			}

			// 3. delete policy file in old loc.
			kvs.backend.Delete(ctx, oldPolicyPath)
		}

	next:
		// 4. check if user identity has old format.
		identityPath := pathJoin(basePrefix, user, iamIdentityFile)
		var cred auth.Credentials
		if err := kvs.loadIAMConfig(&cred, identityPath); err != nil {
			switch err {
			case errConfigNotFound:
				// This case should not happen.
			default:
				// corrupt file or read error
			}
			continue
		}

		// If the file is already in the new format,
		// then the parsed auth.Credentials will have
		// the zero value for the struct.
		var zeroCred auth.Credentials
		if cred.Equal(zeroCred) {
			// nothing to do
			continue
		}

		// Found a id file in old format. Copy value
		// into new format and save it.
		cred.AccessKey = user
		u := newUserIdentity(cred)
		if err := kvs.saveIAMConfig(u, identityPath); err != nil {
			logger.LogIf(ctx, err)
			return err
		}

		// Nothing to delete as identity file location
		// has not changed.
	}
	return nil
}

func (kvs *IAMKvStore) migrateToV1(ctx context.Context) error {
	var iamFmt iamFormat
	path := getIAMFormatFilePath()
	if err := kvs.loadIAMConfig(&iamFmt, path); err != nil {
		switch err {
		case errConfigNotFound:
			// Need to migrate to V1.
		default:
			return err
		}
	} else {
		if iamFmt.Version >= iamFormatVersion1 {
			// Already migrated to V1 of higher!
			return nil
		}
		// This case should not happen
		// (i.e. Version is 0 or negative.)
		return errors.New("got an invalid IAM format version")

	}

	// Migrate long-term users
	if err := kvs.migrateUsersConfigToV1(ctx, false); err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	// Migrate STS users
	if err := kvs.migrateUsersConfigToV1(ctx, true); err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	// Save iam version file.
	if err := kvs.saveIAMConfig(newIAMFormatVersion1(), path); err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	return nil
}

// Should be called under config migration lock
func (kvs *IAMKvStore) migrateBackendFormat(ctx context.Context) error {
	return kvs.migrateToV1(ctx)
}

func (kvs *IAMKvStore) loadPolicyDoc(policy string, m map[string]iampolicy.Policy) error {
	var p iampolicy.Policy
	err := kvs.loadIAMConfig(&p, getPolicyDocPath(policy))
	if err != nil {
		return err
	}
	m[policy] = p
	return nil
}

func (kvs *IAMKvStore) loadPolicyDocs(ctx context.Context, m map[string]iampolicy.Policy) error {
	ctx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	polickvs, err := kvs.backend.Keys(ctx, iamConfigPoliciesPrefix)
	if err != nil {
		return err
	}
	// Reload config and polickvs for all policys.
	for _, policyName := range polickvs.ToSlice() {
		err = kvs.loadPolicyDoc(policyName, m)
		if err != nil {
			return err
		}
	}
	return nil
}

func (kvs *IAMKvStore) loadUser(user string, userType IAMUserType, m map[string]auth.Credentials) error {
	var u UserIdentity
	err := kvs.loadIAMConfig(&u, getUserIdentityPath(user, userType))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchUser
		}
		return err
	}

	if u.Credentials.IsExpired() {
		// Delete expired identity.
		kvs.backend.Delete(kvs.ctx, getUserIdentityPath(user, userType))
		kvs.backend.Delete(kvs.ctx, getMappedPolicyPath(user, userType, false))
		return nil
	}

	// If this is a service account, rotate the session key if we are changing the server creds
	if globalOldCred.IsValid() && u.Credentials.IsServiceAccount() {
		if !globalOldCred.Equal(globalActiveCred) {
			m := jwtgo.MapClaims{}
			stsTokenCallback := func(t *jwtgo.Token) (interface{}, error) {
				return []byte(globalOldCred.SecretKey), nil
			}
			if _, err := jwtgo.ParseWithClaims(u.Credentials.SessionToken, m, stsTokenCallback); err == nil {
				jwt := jwtgo.NewWithClaims(jwtgo.SigningMethodHS512, jwtgo.MapClaims(m))
				if token, err := jwt.SignedString([]byte(globalActiveCred.SecretKey)); err == nil {
					u.Credentials.SessionToken = token
					err := kvs.saveIAMConfig(&u, getUserIdentityPath(user, userType))
					if err != nil {
						return err
					}
				}
			}
		}
	}

	if u.Credentials.AccessKey == "" {
		u.Credentials.AccessKey = user
	}
	m[user] = u.Credentials
	return nil

}

func (kvs *IAMKvStore) loadUsers(ctx context.Context, userType IAMUserType, m map[string]auth.Credentials) error {
	var basePrefix string
	switch userType {
	case srvAccUser:
		basePrefix = iamConfigServiceAccountsPrefix
	case stsUser:
		basePrefix = iamConfigSTSPrefix
	default:
		basePrefix = iamConfigUsersPrefix
	}

	ctx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	users, err := kvs.backend.Keys(ctx, basePrefix)
	if err != nil {
		return err
	}
	// Reload config for all users.
	for _, user := range users.ToSlice() {
		if err = kvs.loadUser(user, userType, m); err != nil {
			return err
		}
	}
	return nil
}

func (kvs *IAMKvStore) loadGroup(group string, m map[string]GroupInfo) error {
	var gi GroupInfo
	err := kvs.loadIAMConfig(&gi, getGroupInfoPath(group))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchGroup
		}
		return err
	}
	m[group] = gi
	return nil

}

func (kvs *IAMKvStore) loadGroups(ctx context.Context, m map[string]GroupInfo) error {
	ctx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	groups, err := kvs.backend.Keys(ctx, iamConfigGroupsPrefix)
	if err != nil {
		return err
	}
	// Reload config for all groups.
	for _, group := range groups.ToSlice() {
		if err = kvs.loadGroup(group, m); err != nil {
			return err
		}
	}
	return nil

}

func (kvs *IAMKvStore) loadMappedPolicy(name string, userType IAMUserType, isGroup bool, m map[string]MappedPolicy) error {
	var p MappedPolicy
	err := kvs.loadIAMConfig(&p, getMappedPolicyPath(name, userType, isGroup))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchPolicy
		}
		return err
	}
	m[name] = p
	return nil

}

func (kvs *IAMKvStore) loadMappedPolicies(ctx context.Context, userType IAMUserType, isGroup bool, m map[string]MappedPolicy) error {
	ctx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	var basePrefix string
	if isGroup {
		basePrefix = iamConfigPolicyDBGroupsPrefix
	} else {
		switch userType {
		case srvAccUser:
			basePrefix = iamConfigPolicyDBServiceAccountsPrefix
		case stsUser:
			basePrefix = iamConfigPolicyDBSTSUsersPrefix
		default:
			basePrefix = iamConfigPolicyDBUsersPrefix
		}
	}
	users, err := kvs.backend.KeysFilter(ctx, basePrefix, kv.KeyTransformTrimJSON)
	if err != nil {
		return err
	}
	// Reload config and polickvs for all users.
	for _, user := range users.ToSlice() {
		if err = kvs.loadMappedPolicy(user, userType, isGroup, m); err != nil {
			return err
		}
	}
	return nil

}

func (kvs *IAMKvStore) loadAll(ctx context.Context, sys *IAMSys) error {
	iamUsersMap := make(map[string]auth.Credentials)
	iamGroupsMap := make(map[string]GroupInfo)
	iamUserPolicyMap := make(map[string]MappedPolicy)
	iamGroupPolicyMap := make(map[string]MappedPolicy)

	kvs.rlock()
	isMinIOUsersSys := sys.usersSysType == MinIOUsersSysType
	kvs.runlock()

	kvs.lock()
	if err := kvs.loadPolicyDocs(ctx, sys.iamPolicyDocsMap); err != nil {
		kvs.unlock()
		return err
	}

	// Sets default canned policies, if none are set.
	setDefaultCannedPolicies(sys.iamPolicyDocsMap)
	kvs.unlock()

	if isMinIOUsersSys {
		// load long term users
		if err := kvs.loadUsers(ctx, regularUser, iamUsersMap); err != nil {
			return err
		}
		if err := kvs.loadGroups(ctx, iamGroupsMap); err != nil {
			return err
		}
	}

	// load polices mapped to users
	if err := kvs.loadMappedPolicies(ctx, regularUser, false, iamUserPolicyMap); err != nil {
		return err
	}
	// load polickvs mapped to groups
	if err := kvs.loadMappedPolicies(ctx, regularUser, true, iamGroupPolicyMap); err != nil {
		return err
	}

	if err := kvs.loadUsers(ctx, srvAccUser, iamUsersMap); err != nil {
		return err
	}

	// load STS temp users
	if err := kvs.loadUsers(ctx, stsUser, iamUsersMap); err != nil {
		return err
	}

	// load STS policy mappings
	if err := kvs.loadMappedPolicies(ctx, stsUser, false, iamUserPolicyMap); err != nil {
		return err
	}

	kvs.lock()
	defer kvs.Unlock()

	// Merge the new reloaded entrkvs into global map.
	// See issue https://github.com/minio/minio/issues/9651
	// where the present list of entrkvs on disk are not yet
	// latest, there is a small window where this can make
	// valid users invalid.
	for k, v := range iamUsersMap {
		sys.iamUsersMap[k] = v
	}

	for k, v := range iamUserPolicyMap {
		sys.iamUserPolicyMap[k] = v
	}

	// purge any expired entrkvs which became expired now.
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

	return nil
}

func (kvs *IAMKvStore) savePolicyDoc(policyName string, p iampolicy.Policy) error {
	return kvs.saveIAMConfig(&p, getPolicyDocPath(policyName))
}

func (kvs *IAMKvStore) saveMappedPolicy(name string, userType IAMUserType, isGroup bool, mp MappedPolicy) error {
	return kvs.saveIAMConfig(mp, getMappedPolicyPath(name, userType, isGroup))
}

func (kvs *IAMKvStore) saveUserIdentity(name string, userType IAMUserType, u UserIdentity) error {
	return kvs.saveIAMConfig(u, getUserIdentityPath(name, userType))
}

func (kvs *IAMKvStore) saveGroupInfo(name string, gi GroupInfo) error {
	return kvs.saveIAMConfig(gi, getGroupInfoPath(name))
}

func (kvs *IAMKvStore) deletePolicyDoc(name string) error {
	err := kvs.deleteIAMConfig(getPolicyDocPath(name))
	if err == errConfigNotFound {
		err = errNoSuchPolicy
	}
	return err
}

func (kvs *IAMKvStore) deleteMappedPolicy(name string, userType IAMUserType, isGroup bool) error {
	err := kvs.deleteIAMConfig(getMappedPolicyPath(name, userType, isGroup))
	if err == errConfigNotFound {
		err = errNoSuchPolicy
	}
	return err
}

func (kvs *IAMKvStore) deleteUserIdentity(name string, userType IAMUserType) error {
	err := kvs.deleteIAMConfig(getUserIdentityPath(name, userType))
	if err == errConfigNotFound {
		err = errNoSuchUser
	}
	return err
}

func (kvs *IAMKvStore) deleteGroupInfo(name string) error {
	err := kvs.deleteIAMConfig(getGroupInfoPath(name))
	if err == errConfigNotFound {
		err = errNoSuchGroup
	}
	return err
}

func (kvs *IAMKvStore) supportsWatch() bool {
	// TODO: ask KS backend?
	return true
}

func (kvs *IAMKvStore) watch(ctx context.Context, sys *IAMSys) {
	kvs.backend.Watch(ctx, iamConfigPrefix, func(event *kv.Event) {
		kvs.lock()
		defer kvs.unlock()
		kvs.reloadFromEvent(sys, event)
	})
}

// sys.RLock is held by caller.
func (kvs *IAMKvStore) reloadFromEvent(sys *IAMSys, event *kv.Event) {
	usersPrefix := strings.HasPrefix(event.Key, iamConfigUsersPrefix)
	groupsPrefix := strings.HasPrefix(event.Key, iamConfigGroupsPrefix)
	stsPrefix := strings.HasPrefix(event.Key, iamConfigSTSPrefix)
	policyPrefix := strings.HasPrefix(event.Key, iamConfigPoliciesPrefix)
	policyDBUsersPrefix := strings.HasPrefix(event.Key, iamConfigPolicyDBUsersPrefix)
	policyDBSTSUsersPrefix := strings.HasPrefix(event.Key, iamConfigPolicyDBSTSUsersPrefix)
	policyDBGroupsPrefix := strings.HasPrefix(event.Key, iamConfigPolicyDBGroupsPrefix)

	switch event.Type {
	case kv.EventTypeCreated:
		switch {
		case usersPrefix:
			accessKey := path.Dir(strings.TrimPrefix(event.Key,
				iamConfigUsersPrefix))
			kvs.loadUser(accessKey, regularUser, sys.iamUsersMap)
		case stsPrefix:
			accessKey := path.Dir(strings.TrimPrefix(event.Key,
				iamConfigSTSPrefix))
			kvs.loadUser(accessKey, stsUser, sys.iamUsersMap)
		case groupsPrefix:
			group := path.Dir(strings.TrimPrefix(event.Key,
				iamConfigGroupsPrefix))
			kvs.loadGroup(group, sys.iamGroupsMap)
			gi := sys.iamGroupsMap[group]
			sys.removeGroupFromMembershipsMap(group)
			sys.updateGroupMembershipsMap(group, &gi)
		case policyPrefix:
			policyName := path.Dir(strings.TrimPrefix(event.Key,
				iamConfigPoliciesPrefix))
			kvs.loadPolicyDoc(policyName, sys.iamPolicyDocsMap)
		case policyDBUsersPrefix:
			policyMapFile := strings.TrimPrefix(event.Key,
				iamConfigPolicyDBUsersPrefix)
			user := strings.TrimSuffix(policyMapFile, ".json")
			kvs.loadMappedPolicy(user, regularUser, false, sys.iamUserPolicyMap)
		case policyDBSTSUsersPrefix:
			policyMapFile := strings.TrimPrefix(event.Key,
				iamConfigPolicyDBSTSUsersPrefix)
			user := strings.TrimSuffix(policyMapFile, ".json")
			kvs.loadMappedPolicy(user, stsUser, false, sys.iamUserPolicyMap)
		case policyDBGroupsPrefix:
			policyMapFile := strings.TrimPrefix(event.Key,
				iamConfigPolicyDBGroupsPrefix)
			user := strings.TrimSuffix(policyMapFile, ".json")
			kvs.loadMappedPolicy(user, regularUser, true, sys.iamGroupPolicyMap)
		}
	case kv.EventTypeDeleted:
		switch {
		case usersPrefix:
			accessKey := path.Dir(strings.TrimPrefix(event.Key,
				iamConfigUsersPrefix))
			delete(sys.iamUsersMap, accessKey)
		case stsPrefix:
			accessKey := path.Dir(strings.TrimPrefix(event.Key,
				iamConfigSTSPrefix))
			delete(sys.iamUsersMap, accessKey)
		case groupsPrefix:
			group := path.Dir(strings.TrimPrefix(event.Key,
				iamConfigGroupsPrefix))
			sys.removeGroupFromMembershipsMap(group)
			delete(sys.iamGroupsMap, group)
			delete(sys.iamGroupPolicyMap, group)
		case policyPrefix:
			policyName := path.Dir(strings.TrimPrefix(event.Key,
				iamConfigPoliciesPrefix))
			delete(sys.iamPolicyDocsMap, policyName)
		case policyDBUsersPrefix:
			policyMapFile := strings.TrimPrefix(event.Key,
				iamConfigPolicyDBUsersPrefix)
			user := strings.TrimSuffix(policyMapFile, ".json")
			delete(sys.iamUserPolicyMap, user)
		case policyDBSTSUsersPrefix:
			policyMapFile := strings.TrimPrefix(event.Key,
				iamConfigPolicyDBSTSUsersPrefix)
			user := strings.TrimSuffix(policyMapFile, ".json")
			delete(sys.iamUserPolicyMap, user)
		case policyDBGroupsPrefix:
			policyMapFile := strings.TrimPrefix(event.Key,
				iamConfigPolicyDBGroupsPrefix)
			user := strings.TrimSuffix(policyMapFile, ".json")
			delete(sys.iamGroupPolicyMap, user)
		}
	}
}
