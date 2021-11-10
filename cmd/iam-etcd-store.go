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
	"encoding/json"
	"path"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/kms"
	"github.com/minio/minio/internal/logger"
	iampolicy "github.com/minio/pkg/iam/policy"
	"go.etcd.io/etcd/api/v3/mvccpb"
	etcd "go.etcd.io/etcd/client/v3"
)

var defaultContextTimeout = 30 * time.Second

func etcdKvsToSet(prefix string, kvs []*mvccpb.KeyValue) set.StringSet {
	users := set.NewStringSet()
	for _, kv := range kvs {
		user := extractPathPrefixAndSuffix(string(kv.Key), prefix, path.Base(string(kv.Key)))
		users.Add(user)
	}
	return users
}

// Extract path string by stripping off the `prefix` value and the suffix,
// value, usually in the following form.
//  s := "config/iam/users/foo/config.json"
//  prefix := "config/iam/users/"
//  suffix := "config.json"
//  result is foo
func extractPathPrefixAndSuffix(s string, prefix string, suffix string) string {
	return pathClean(strings.TrimSuffix(strings.TrimPrefix(s, prefix), suffix))
}

// IAMEtcdStore implements IAMStorageAPI
type IAMEtcdStore struct {
	sync.RWMutex

	*iamCache

	usersSysType UsersSysType

	client *etcd.Client
}

func newIAMEtcdStore(client *etcd.Client, usersSysType UsersSysType) *IAMEtcdStore {
	return &IAMEtcdStore{
		iamCache:     newIamCache(),
		client:       client,
		usersSysType: usersSysType,
	}
}

func (ies *IAMEtcdStore) rlock() *iamCache {
	ies.RLock()
	return ies.iamCache
}

func (ies *IAMEtcdStore) runlock() {
	ies.RUnlock()
}

func (ies *IAMEtcdStore) lock() *iamCache {
	ies.Lock()
	return ies.iamCache
}

func (ies *IAMEtcdStore) unlock() {
	ies.Unlock()
}

func (ies *IAMEtcdStore) getUsersSysType() UsersSysType {
	return ies.usersSysType
}

func (ies *IAMEtcdStore) saveIAMConfig(ctx context.Context, item interface{}, itemPath string, opts ...options) error {
	data, err := json.Marshal(item)
	if err != nil {
		return err
	}
	if GlobalKMS != nil {
		data, err = config.EncryptBytes(GlobalKMS, data, kms.Context{
			minioMetaBucket: path.Join(minioMetaBucket, itemPath),
		})
		if err != nil {
			return err
		}
	}
	return saveKeyEtcd(ctx, ies.client, itemPath, data, opts...)
}

func getIAMConfig(item interface{}, data []byte, itemPath string) error {
	var err error
	if !utf8.Valid(data) && GlobalKMS != nil {
		data, err = config.DecryptBytes(GlobalKMS, data, kms.Context{
			minioMetaBucket: path.Join(minioMetaBucket, itemPath),
		})
		if err != nil {
			// This fallback is needed because of a bug, in kms.Context{}
			// construction during migration.
			data, err = config.DecryptBytes(GlobalKMS, data, kms.Context{
				minioMetaBucket: itemPath,
			})
			if err != nil {
				return err
			}
		}
	}
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	return json.Unmarshal(data, item)
}

func (ies *IAMEtcdStore) loadIAMConfig(ctx context.Context, item interface{}, path string) error {
	data, err := readKeyEtcd(ctx, ies.client, path)
	if err != nil {
		return err
	}
	return getIAMConfig(item, data, path)
}

func (ies *IAMEtcdStore) deleteIAMConfig(ctx context.Context, path string) error {
	return deleteKeyEtcd(ctx, ies.client, path)
}

func (ies *IAMEtcdStore) migrateUsersConfigToV1(ctx context.Context) error {
	basePrefix := iamConfigUsersPrefix
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
			err := ies.loadIAMConfig(ctx, &policyName, oldPolicyPath)
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
			userType := regUser
			path := getMappedPolicyPath(user, userType, false)
			if err := ies.saveIAMConfig(ctx, mp, path); err != nil {
				return err
			}

			// 3. delete policy file in old loc.
			deleteKeyEtcd(ctx, ies.client, oldPolicyPath)
		}

	next:
		// 4. check if user identity has old format.
		identityPath := pathJoin(basePrefix, user, iamIdentityFile)
		var cred auth.Credentials
		if err := ies.loadIAMConfig(ctx, &cred, identityPath); err != nil {
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
		if err := ies.saveIAMConfig(ctx, u, identityPath); err != nil {
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
	if err := ies.loadIAMConfig(ctx, &iamFmt, path); err != nil {
		switch err {
		case errConfigNotFound:
			// Need to migrate to V1.
		default:
			// if IAM format
			return err
		}
	}

	if iamFmt.Version >= iamFormatVersion1 {
		// Nothing to do.
		return nil
	}

	if err := ies.migrateUsersConfigToV1(ctx); err != nil {
		logger.LogIf(ctx, err)
		return err
	}

	// Save iam format to version 1.
	if err := ies.saveIAMConfig(ctx, newIAMFormatVersion1(), path); err != nil {
		logger.LogIf(ctx, err)
		return err
	}

	return nil
}

// Should be called under config migration lock
func (ies *IAMEtcdStore) migrateBackendFormat(ctx context.Context) error {
	ies.Lock()
	defer ies.Unlock()
	return ies.migrateToV1(ctx)
}

func (ies *IAMEtcdStore) loadPolicyDoc(ctx context.Context, policy string, m map[string]iampolicy.Policy) error {
	var p iampolicy.Policy
	err := ies.loadIAMConfig(ctx, &p, getPolicyDocPath(policy))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchPolicy
		}
		return err
	}
	m[policy] = p
	return nil
}

func (ies *IAMEtcdStore) getPolicyDocKV(ctx context.Context, kvs *mvccpb.KeyValue, m map[string]iampolicy.Policy) error {
	var p iampolicy.Policy
	err := getIAMConfig(&p, kvs.Value, string(kvs.Key))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchPolicy
		}
		return err
	}
	policy := extractPathPrefixAndSuffix(string(kvs.Key), iamConfigPoliciesPrefix, path.Base(string(kvs.Key)))
	m[policy] = p
	return nil
}

func (ies *IAMEtcdStore) loadPolicyDocs(ctx context.Context, m map[string]iampolicy.Policy) error {
	ctx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	//  Retrieve all keys and values to avoid too many calls to etcd in case of
	//  a large number of policies
	r, err := ies.client.Get(ctx, iamConfigPoliciesPrefix, etcd.WithPrefix())
	if err != nil {
		return err
	}

	// Parse all values to construct the policies data model.
	for _, kvs := range r.Kvs {
		if err = ies.getPolicyDocKV(ctx, kvs, m); err != nil && err != errNoSuchPolicy {
			return err
		}
	}
	return nil
}

func (ies *IAMEtcdStore) getUserKV(ctx context.Context, userkv *mvccpb.KeyValue, userType IAMUserType, m map[string]auth.Credentials, basePrefix string) error {
	var u UserIdentity
	err := getIAMConfig(&u, userkv.Value, string(userkv.Key))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchUser
		}
		return err
	}
	user := extractPathPrefixAndSuffix(string(userkv.Key), basePrefix, path.Base(string(userkv.Key)))
	return ies.addUser(ctx, user, userType, u, m)
}

func (ies *IAMEtcdStore) addUser(ctx context.Context, user string, userType IAMUserType, u UserIdentity, m map[string]auth.Credentials) error {
	if u.Credentials.IsExpired() {
		// Delete expired identity.
		deleteKeyEtcd(ctx, ies.client, getUserIdentityPath(user, userType))
		deleteKeyEtcd(ctx, ies.client, getMappedPolicyPath(user, userType, false))
		return nil
	}
	if u.Credentials.AccessKey == "" {
		u.Credentials.AccessKey = user
	}
	m[user] = u.Credentials
	return nil
}

func (ies *IAMEtcdStore) loadUser(ctx context.Context, user string, userType IAMUserType, m map[string]auth.Credentials) error {
	var u UserIdentity
	err := ies.loadIAMConfig(ctx, &u, getUserIdentityPath(user, userType))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchUser
		}
		return err
	}
	return ies.addUser(ctx, user, userType, u, m)
}

func (ies *IAMEtcdStore) loadUsers(ctx context.Context, userType IAMUserType, m map[string]auth.Credentials) error {
	var basePrefix string
	switch userType {
	case svcUser:
		basePrefix = iamConfigServiceAccountsPrefix
	case stsUser:
		basePrefix = iamConfigSTSPrefix
	default:
		basePrefix = iamConfigUsersPrefix
	}

	cctx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()

	// Retrieve all keys and values to avoid too many calls to etcd in case of
	// a large number of users
	r, err := ies.client.Get(cctx, basePrefix, etcd.WithPrefix())
	if err != nil {
		return err
	}

	// Parse all users values to create the proper data model
	for _, userKv := range r.Kvs {
		if err = ies.getUserKV(ctx, userKv, userType, m, basePrefix); err != nil && err != errNoSuchUser {
			return err
		}
	}
	return nil
}

func (ies *IAMEtcdStore) loadGroup(ctx context.Context, group string, m map[string]GroupInfo) error {
	var gi GroupInfo
	err := ies.loadIAMConfig(ctx, &gi, getGroupInfoPath(group))
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
	cctx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()

	r, err := ies.client.Get(cctx, iamConfigGroupsPrefix, etcd.WithPrefix(), etcd.WithKeysOnly())
	if err != nil {
		return err
	}

	groups := etcdKvsToSet(iamConfigGroupsPrefix, r.Kvs)

	// Reload config for all groups.
	for _, group := range groups.ToSlice() {
		if err = ies.loadGroup(ctx, group, m); err != nil && err != errNoSuchGroup {
			return err
		}
	}
	return nil

}

func (ies *IAMEtcdStore) loadMappedPolicy(ctx context.Context, name string, userType IAMUserType, isGroup bool, m map[string]MappedPolicy) error {
	var p MappedPolicy
	err := ies.loadIAMConfig(ctx, &p, getMappedPolicyPath(name, userType, isGroup))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchPolicy
		}
		return err
	}
	m[name] = p
	return nil
}

func getMappedPolicy(ctx context.Context, kv *mvccpb.KeyValue, userType IAMUserType, isGroup bool, m map[string]MappedPolicy, basePrefix string) error {
	var p MappedPolicy
	err := getIAMConfig(&p, kv.Value, string(kv.Key))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchPolicy
		}
		return err
	}
	name := extractPathPrefixAndSuffix(string(kv.Key), basePrefix, ".json")
	m[name] = p
	return nil
}

func (ies *IAMEtcdStore) loadMappedPolicies(ctx context.Context, userType IAMUserType, isGroup bool, m map[string]MappedPolicy) error {
	cctx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	var basePrefix string
	if isGroup {
		basePrefix = iamConfigPolicyDBGroupsPrefix
	} else {
		switch userType {
		case svcUser:
			basePrefix = iamConfigPolicyDBServiceAccountsPrefix
		case stsUser:
			basePrefix = iamConfigPolicyDBSTSUsersPrefix
		default:
			basePrefix = iamConfigPolicyDBUsersPrefix
		}
	}
	// Retrieve all keys and values to avoid too many calls to etcd in case of
	// a large number of policy mappings
	r, err := ies.client.Get(cctx, basePrefix, etcd.WithPrefix())
	if err != nil {
		return err
	}

	// Parse all policies mapping to create the proper data model
	for _, kv := range r.Kvs {
		if err = getMappedPolicy(ctx, kv, userType, isGroup, m, basePrefix); err != nil && err != errNoSuchPolicy {
			return err
		}
	}
	return nil

}

func (ies *IAMEtcdStore) savePolicyDoc(ctx context.Context, policyName string, p iampolicy.Policy) error {
	return ies.saveIAMConfig(ctx, &p, getPolicyDocPath(policyName))
}

func (ies *IAMEtcdStore) saveMappedPolicy(ctx context.Context, name string, userType IAMUserType, isGroup bool, mp MappedPolicy, opts ...options) error {
	return ies.saveIAMConfig(ctx, mp, getMappedPolicyPath(name, userType, isGroup), opts...)
}

func (ies *IAMEtcdStore) saveUserIdentity(ctx context.Context, name string, userType IAMUserType, u UserIdentity, opts ...options) error {
	return ies.saveIAMConfig(ctx, u, getUserIdentityPath(name, userType), opts...)
}

func (ies *IAMEtcdStore) saveGroupInfo(ctx context.Context, name string, gi GroupInfo) error {
	return ies.saveIAMConfig(ctx, gi, getGroupInfoPath(name))
}

func (ies *IAMEtcdStore) deletePolicyDoc(ctx context.Context, name string) error {
	err := ies.deleteIAMConfig(ctx, getPolicyDocPath(name))
	if err == errConfigNotFound {
		err = errNoSuchPolicy
	}
	return err
}

func (ies *IAMEtcdStore) deleteMappedPolicy(ctx context.Context, name string, userType IAMUserType, isGroup bool) error {
	err := ies.deleteIAMConfig(ctx, getMappedPolicyPath(name, userType, isGroup))
	if err == errConfigNotFound {
		err = errNoSuchPolicy
	}
	return err
}

func (ies *IAMEtcdStore) deleteUserIdentity(ctx context.Context, name string, userType IAMUserType) error {
	err := ies.deleteIAMConfig(ctx, getUserIdentityPath(name, userType))
	if err == errConfigNotFound {
		err = errNoSuchUser
	}
	return err
}

func (ies *IAMEtcdStore) deleteGroupInfo(ctx context.Context, name string) error {
	err := ies.deleteIAMConfig(ctx, getGroupInfoPath(name))
	if err == errConfigNotFound {
		err = errNoSuchGroup
	}
	return err
}

func (ies *IAMEtcdStore) watch(ctx context.Context, keyPath string) <-chan iamWatchEvent {
	ch := make(chan iamWatchEvent)

	// go routine to read events from the etcd watch channel and send them
	// down `ch`
	go func() {
		for {
		outerLoop:
			watchCh := ies.client.Watch(ctx,
				keyPath, etcd.WithPrefix(), etcd.WithKeysOnly())

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
						isCreateEvent := event.IsModify() || event.IsCreate()
						isDeleteEvent := event.Type == etcd.EventTypeDelete

						switch {
						case isCreateEvent:
							ch <- iamWatchEvent{
								isCreated: true,
								keyPath:   string(event.Kv.Key),
							}
						case isDeleteEvent:
							ch <- iamWatchEvent{
								isCreated: false,
								keyPath:   string(event.Kv.Key),
							}
						}

					}
				}
			}
		}
	}()
	return ch

}
