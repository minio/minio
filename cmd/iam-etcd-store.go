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

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	jwtgo "github.com/dgrijalva/jwt-go"
	"github.com/minio/minio-go/v6/pkg/set"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	iampolicy "github.com/minio/minio/pkg/iam/policy"
	"github.com/minio/minio/pkg/madmin"
)

var defaultContextTimeout = 30 * time.Second

func etcdKvsToSet(prefix string, kvs []*mvccpb.KeyValue) set.StringSet {
	users := set.NewStringSet()
	for _, kv := range kvs {
		// Extract user by stripping off the `prefix` value as suffix,
		// then strip off the remaining basename to obtain the prefix
		// value, usually in the following form.
		//
		//  key := "config/iam/users/newuser/identity.json"
		//  prefix := "config/iam/users/"
		//  v := trim(trim(key, prefix), base(key)) == "newuser"
		//
		user := path.Clean(strings.TrimSuffix(strings.TrimPrefix(string(kv.Key), prefix), path.Base(string(kv.Key))))
		users.Add(user)
	}
	return users
}

func etcdKvsToSetPolicyDB(prefix string, kvs []*mvccpb.KeyValue) set.StringSet {
	items := set.NewStringSet()
	for _, kv := range kvs {
		// Extract user item by stripping off prefix and then
		// stripping of ".json" suffix.
		//
		// key := "config/iam/policydb/users/myuser1.json"
		// prefix := "config/iam/policydb/users/"
		// v := trimSuffix(trimPrefix(key, prefix), ".json")
		key := string(kv.Key)
		item := path.Clean(strings.TrimSuffix(strings.TrimPrefix(key, prefix), ".json"))
		items.Add(item)
	}
	return items
}

// IAMEtcdStore implements IAMStorageAPI
type IAMEtcdStore struct {
	sync.RWMutex

	ctx context.Context

	client *etcd.Client
}

func newIAMEtcdStore(ctx context.Context) *IAMEtcdStore {
	return &IAMEtcdStore{client: globalEtcdClient, ctx: ctx}
}

func (ies *IAMEtcdStore) lock() {
	ies.Lock()
}

func (ies *IAMEtcdStore) unlock() {
	ies.Unlock()
}

func (ies *IAMEtcdStore) rlock() {
	ies.RLock()
}

func (ies *IAMEtcdStore) runlock() {
	ies.RUnlock()
}

func (ies *IAMEtcdStore) saveIAMConfig(item interface{}, path string) error {
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
	return saveKeyEtcd(ies.ctx, ies.client, path, data)
}

func (ies *IAMEtcdStore) loadIAMConfig(item interface{}, path string) error {
	pdata, err := readKeyEtcd(ies.ctx, ies.client, path)
	if err != nil {
		return err
	}

	if globalConfigEncrypted {
		pdata, err = madmin.DecryptData(globalActiveCred.String(), bytes.NewReader(pdata))
		if err != nil {
			return err
		}
	}

	return json.Unmarshal(pdata, item)
}

func (ies *IAMEtcdStore) deleteIAMConfig(path string) error {
	return deleteKeyEtcd(ies.ctx, ies.client, path)
}

func (ies *IAMEtcdStore) migrateUsersConfigToV1(ctx context.Context, isSTS bool) error {
	basePrefix := iamConfigUsersPrefix
	if isSTS {
		basePrefix = iamConfigSTSPrefix
	}

	ctx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	r, err := ies.client.Get(ctx, basePrefix, etcd.WithPrefix(), etcd.WithKeysOnly())
	if err != nil {
		return err
	}

	users := etcdKvsToSet(basePrefix, r.Kvs)
	for _, user := range users.ToSlice() {
		{
			// 1. check if there is a policy file in the old loc.
			oldPolicyPath := pathJoin(basePrefix, user, iamPolicyFile)
			var policyName string
			err := ies.loadIAMConfig(&policyName, oldPolicyPath)
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
			if err := ies.saveIAMConfig(mp, path); err != nil {
				return err
			}

			// 3. delete policy file in old loc.
			deleteKeyEtcd(ctx, ies.client, oldPolicyPath)
		}

	next:
		// 4. check if user identity has old format.
		identityPath := pathJoin(basePrefix, user, iamIdentityFile)
		var cred auth.Credentials
		if err := ies.loadIAMConfig(&cred, identityPath); err != nil {
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
		if err := ies.saveIAMConfig(u, identityPath); err != nil {
			logger.LogIf(ctx, err)
			return err
		}

		// Nothing to delete as identity file location
		// has not changed.
	}
	return nil
}

func (ies *IAMEtcdStore) migrateToV1(ctx context.Context) error {
	var iamFmt iamFormat
	path := getIAMFormatFilePath()
	if err := ies.loadIAMConfig(&iamFmt, path); err != nil {
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
	if err := ies.migrateUsersConfigToV1(ctx, false); err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	// Migrate STS users
	if err := ies.migrateUsersConfigToV1(ctx, true); err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	// Save iam version file.
	if err := ies.saveIAMConfig(newIAMFormatVersion1(), path); err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	return nil
}

// Should be called under config migration lock
func (ies *IAMEtcdStore) migrateBackendFormat(ctx context.Context) error {
	return ies.migrateToV1(ctx)
}

func (ies *IAMEtcdStore) loadPolicyDoc(policy string, m map[string]iampolicy.Policy) error {
	var p iampolicy.Policy
	err := ies.loadIAMConfig(&p, getPolicyDocPath(policy))
	if err != nil {
		return err
	}
	m[policy] = p
	return nil
}

func (ies *IAMEtcdStore) loadPolicyDocs(ctx context.Context, m map[string]iampolicy.Policy) error {
	ctx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	r, err := ies.client.Get(ctx, iamConfigPoliciesPrefix, etcd.WithPrefix(), etcd.WithKeysOnly())
	if err != nil {
		return err
	}

	policies := etcdKvsToSet(iamConfigPoliciesPrefix, r.Kvs)

	// Reload config and policies for all policys.
	for _, policyName := range policies.ToSlice() {
		err = ies.loadPolicyDoc(policyName, m)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ies *IAMEtcdStore) loadUser(user string, userType IAMUserType, m map[string]auth.Credentials) error {
	var u UserIdentity
	err := ies.loadIAMConfig(&u, getUserIdentityPath(user, userType))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchUser
		}
		return err
	}

	if u.Credentials.IsExpired() {
		// Delete expired identity.
		deleteKeyEtcd(ies.ctx, ies.client, getUserIdentityPath(user, userType))
		deleteKeyEtcd(ies.ctx, ies.client, getMappedPolicyPath(user, userType, false))
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
					err := ies.saveIAMConfig(&u, getUserIdentityPath(user, userType))
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

func (ies *IAMEtcdStore) loadUsers(ctx context.Context, userType IAMUserType, m map[string]auth.Credentials) error {
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
	r, err := ies.client.Get(ctx, basePrefix, etcd.WithPrefix(), etcd.WithKeysOnly())
	if err != nil {
		return err
	}

	users := etcdKvsToSet(basePrefix, r.Kvs)

	// Reload config for all users.
	for _, user := range users.ToSlice() {
		if err = ies.loadUser(user, userType, m); err != nil {
			return err
		}
	}
	return nil
}

func (ies *IAMEtcdStore) loadGroup(group string, m map[string]GroupInfo) error {
	var gi GroupInfo
	err := ies.loadIAMConfig(&gi, getGroupInfoPath(group))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchGroup
		}
		return err
	}
	m[group] = gi
	return nil

}

func (ies *IAMEtcdStore) loadGroups(ctx context.Context, m map[string]GroupInfo) error {
	ctx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	r, err := ies.client.Get(ctx, iamConfigGroupsPrefix, etcd.WithPrefix(), etcd.WithKeysOnly())
	if err != nil {
		return err
	}

	groups := etcdKvsToSet(iamConfigGroupsPrefix, r.Kvs)

	// Reload config for all groups.
	for _, group := range groups.ToSlice() {
		if err = ies.loadGroup(group, m); err != nil {
			return err
		}
	}
	return nil

}

func (ies *IAMEtcdStore) loadMappedPolicy(name string, userType IAMUserType, isGroup bool, m map[string]MappedPolicy) error {
	var p MappedPolicy
	err := ies.loadIAMConfig(&p, getMappedPolicyPath(name, userType, isGroup))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchPolicy
		}
		return err
	}
	m[name] = p
	return nil

}

func (ies *IAMEtcdStore) loadMappedPolicies(ctx context.Context, userType IAMUserType, isGroup bool, m map[string]MappedPolicy) error {
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
	r, err := ies.client.Get(ctx, basePrefix, etcd.WithPrefix(), etcd.WithKeysOnly())
	if err != nil {
		return err
	}

	users := etcdKvsToSetPolicyDB(basePrefix, r.Kvs)

	// Reload config and policies for all users.
	for _, user := range users.ToSlice() {
		if err = ies.loadMappedPolicy(user, userType, isGroup, m); err != nil {
			return err
		}
	}
	return nil

}

func (ies *IAMEtcdStore) loadAll(ctx context.Context, sys *IAMSys) error {
	iamUsersMap := make(map[string]auth.Credentials)
	iamGroupsMap := make(map[string]GroupInfo)
	iamUserPolicyMap := make(map[string]MappedPolicy)
	iamGroupPolicyMap := make(map[string]MappedPolicy)

	ies.rlock()
	isMinIOUsersSys := sys.usersSysType == MinIOUsersSysType
	ies.runlock()

	ies.lock()
	if err := ies.loadPolicyDocs(ctx, sys.iamPolicyDocsMap); err != nil {
		ies.unlock()
		return err
	}
	// Sets default canned policies, if none are set.
	setDefaultCannedPolicies(sys.iamPolicyDocsMap)

	ies.unlock()

	if isMinIOUsersSys {
		if err := ies.loadUsers(ctx, regularUser, iamUsersMap); err != nil {
			return err
		}
		if err := ies.loadGroups(ctx, iamGroupsMap); err != nil {
			return err
		}
	}

	// load polices mapped to users
	if err := ies.loadMappedPolicies(ctx, regularUser, false, iamUserPolicyMap); err != nil {
		return err
	}

	// load policies mapped to groups
	if err := ies.loadMappedPolicies(ctx, regularUser, true, iamGroupPolicyMap); err != nil {
		return err
	}

	if err := ies.loadUsers(ctx, srvAccUser, iamUsersMap); err != nil {
		return err
	}

	// load STS temp users
	if err := ies.loadUsers(ctx, stsUser, iamUsersMap); err != nil {
		return err
	}

	// load STS policy mappings
	if err := ies.loadMappedPolicies(ctx, stsUser, false, iamUserPolicyMap); err != nil {
		return err
	}

	ies.lock()
	defer ies.Unlock()

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
	sys.storeFallback = false

	return nil
}

func (ies *IAMEtcdStore) savePolicyDoc(policyName string, p iampolicy.Policy) error {
	return ies.saveIAMConfig(&p, getPolicyDocPath(policyName))
}

func (ies *IAMEtcdStore) saveMappedPolicy(name string, userType IAMUserType, isGroup bool, mp MappedPolicy) error {
	return ies.saveIAMConfig(mp, getMappedPolicyPath(name, userType, isGroup))
}

func (ies *IAMEtcdStore) saveUserIdentity(name string, userType IAMUserType, u UserIdentity) error {
	return ies.saveIAMConfig(u, getUserIdentityPath(name, userType))
}

func (ies *IAMEtcdStore) saveGroupInfo(name string, gi GroupInfo) error {
	return ies.saveIAMConfig(gi, getGroupInfoPath(name))
}

func (ies *IAMEtcdStore) deletePolicyDoc(name string) error {
	err := ies.deleteIAMConfig(getPolicyDocPath(name))
	if err == errConfigNotFound {
		err = errNoSuchPolicy
	}
	return err
}

func (ies *IAMEtcdStore) deleteMappedPolicy(name string, userType IAMUserType, isGroup bool) error {
	err := ies.deleteIAMConfig(getMappedPolicyPath(name, userType, isGroup))
	if err == errConfigNotFound {
		err = errNoSuchPolicy
	}
	return err
}

func (ies *IAMEtcdStore) deleteUserIdentity(name string, userType IAMUserType) error {
	err := ies.deleteIAMConfig(getUserIdentityPath(name, userType))
	if err == errConfigNotFound {
		err = errNoSuchUser
	}
	return err
}

func (ies *IAMEtcdStore) deleteGroupInfo(name string) error {
	err := ies.deleteIAMConfig(getGroupInfoPath(name))
	if err == errConfigNotFound {
		err = errNoSuchGroup
	}
	return err
}

func (ies *IAMEtcdStore) watch(ctx context.Context, sys *IAMSys) {
	for {
	outerLoop:
		// Refresh IAMSys with etcd watch.
		watchCh := ies.client.Watch(ctx,
			iamConfigPrefix, etcd.WithPrefix(), etcd.WithKeysOnly())

		for {
			select {
			case <-ctx.Done():
				return
			case watchResp, ok := <-watchCh:
				if !ok {
					time.Sleep(1 * time.Second)
					// Upon an error on watch channel
					// re-init the watch channel.
					goto outerLoop
				}
				if err := watchResp.Err(); err != nil {
					logger.LogIf(ctx, err)
					// log and retry.
					time.Sleep(1 * time.Second)
					// Upon an error on watch channel
					// re-init the watch channel.
					goto outerLoop
				}
				for _, event := range watchResp.Events {
					ies.lock()
					ies.reloadFromEvent(sys, event)
					ies.unlock()
				}
			}
		}
	}
}

// sys.RLock is held by caller.
func (ies *IAMEtcdStore) reloadFromEvent(sys *IAMSys, event *etcd.Event) {
	eventCreate := event.IsModify() || event.IsCreate()
	eventDelete := event.Type == etcd.EventTypeDelete
	usersPrefix := strings.HasPrefix(string(event.Kv.Key), iamConfigUsersPrefix)
	groupsPrefix := strings.HasPrefix(string(event.Kv.Key), iamConfigGroupsPrefix)
	stsPrefix := strings.HasPrefix(string(event.Kv.Key), iamConfigSTSPrefix)
	policyPrefix := strings.HasPrefix(string(event.Kv.Key), iamConfigPoliciesPrefix)
	policyDBUsersPrefix := strings.HasPrefix(string(event.Kv.Key), iamConfigPolicyDBUsersPrefix)
	policyDBSTSUsersPrefix := strings.HasPrefix(string(event.Kv.Key), iamConfigPolicyDBSTSUsersPrefix)
	policyDBGroupsPrefix := strings.HasPrefix(string(event.Kv.Key), iamConfigPolicyDBGroupsPrefix)

	switch {
	case eventCreate:
		switch {
		case usersPrefix:
			accessKey := path.Dir(strings.TrimPrefix(string(event.Kv.Key),
				iamConfigUsersPrefix))
			ies.loadUser(accessKey, regularUser, sys.iamUsersMap)
		case stsPrefix:
			accessKey := path.Dir(strings.TrimPrefix(string(event.Kv.Key),
				iamConfigSTSPrefix))
			ies.loadUser(accessKey, stsUser, sys.iamUsersMap)
		case groupsPrefix:
			group := path.Dir(strings.TrimPrefix(string(event.Kv.Key),
				iamConfigGroupsPrefix))
			ies.loadGroup(group, sys.iamGroupsMap)
			gi := sys.iamGroupsMap[group]
			sys.removeGroupFromMembershipsMap(group)
			sys.updateGroupMembershipsMap(group, &gi)
		case policyPrefix:
			policyName := path.Dir(strings.TrimPrefix(string(event.Kv.Key),
				iamConfigPoliciesPrefix))
			ies.loadPolicyDoc(policyName, sys.iamPolicyDocsMap)
		case policyDBUsersPrefix:
			policyMapFile := strings.TrimPrefix(string(event.Kv.Key),
				iamConfigPolicyDBUsersPrefix)
			user := strings.TrimSuffix(policyMapFile, ".json")
			ies.loadMappedPolicy(user, regularUser, false, sys.iamUserPolicyMap)
		case policyDBSTSUsersPrefix:
			policyMapFile := strings.TrimPrefix(string(event.Kv.Key),
				iamConfigPolicyDBSTSUsersPrefix)
			user := strings.TrimSuffix(policyMapFile, ".json")
			ies.loadMappedPolicy(user, stsUser, false, sys.iamUserPolicyMap)
		case policyDBGroupsPrefix:
			policyMapFile := strings.TrimPrefix(string(event.Kv.Key),
				iamConfigPolicyDBGroupsPrefix)
			user := strings.TrimSuffix(policyMapFile, ".json")
			ies.loadMappedPolicy(user, regularUser, true, sys.iamGroupPolicyMap)
		}
	case eventDelete:
		switch {
		case usersPrefix:
			accessKey := path.Dir(strings.TrimPrefix(string(event.Kv.Key),
				iamConfigUsersPrefix))
			delete(sys.iamUsersMap, accessKey)
		case stsPrefix:
			accessKey := path.Dir(strings.TrimPrefix(string(event.Kv.Key),
				iamConfigSTSPrefix))
			delete(sys.iamUsersMap, accessKey)
		case groupsPrefix:
			group := path.Dir(strings.TrimPrefix(string(event.Kv.Key),
				iamConfigGroupsPrefix))
			sys.removeGroupFromMembershipsMap(group)
			delete(sys.iamGroupsMap, group)
			delete(sys.iamGroupPolicyMap, group)
		case policyPrefix:
			policyName := path.Dir(strings.TrimPrefix(string(event.Kv.Key),
				iamConfigPoliciesPrefix))
			delete(sys.iamPolicyDocsMap, policyName)
		case policyDBUsersPrefix:
			policyMapFile := strings.TrimPrefix(string(event.Kv.Key),
				iamConfigPolicyDBUsersPrefix)
			user := strings.TrimSuffix(policyMapFile, ".json")
			delete(sys.iamUserPolicyMap, user)
		case policyDBSTSUsersPrefix:
			policyMapFile := strings.TrimPrefix(string(event.Kv.Key),
				iamConfigPolicyDBSTSUsersPrefix)
			user := strings.TrimSuffix(policyMapFile, ".json")
			delete(sys.iamUserPolicyMap, user)
		case policyDBGroupsPrefix:
			policyMapFile := strings.TrimPrefix(string(event.Kv.Key),
				iamConfigPolicyDBGroupsPrefix)
			user := strings.TrimSuffix(policyMapFile, ".json")
			delete(sys.iamGroupPolicyMap, user)
		}
	}
}
