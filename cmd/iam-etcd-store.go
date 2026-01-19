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
	"errors"
	"path"
	"strings"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/kms"
	"github.com/puzpuzpuz/xsync/v3"
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
//
//	s := "config/iam/users/foo/config.json"
//	prefix := "config/iam/users/"
//	suffix := "config.json"
//	result is foo
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

func (ies *IAMEtcdStore) saveIAMConfig(ctx context.Context, item any, itemPath string, opts ...options) error {
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

func getIAMConfig(item any, data []byte, itemPath string) error {
	data, err := decryptData(data, itemPath)
	if err != nil {
		return err
	}
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	return json.Unmarshal(data, item)
}

func (ies *IAMEtcdStore) loadIAMConfig(ctx context.Context, item any, path string) error {
	data, err := readKeyEtcd(ctx, ies.client, path)
	if err != nil {
		return err
	}
	return getIAMConfig(item, data, path)
}

func (ies *IAMEtcdStore) loadIAMConfigBytes(ctx context.Context, path string) ([]byte, error) {
	data, err := readKeyEtcd(ctx, ies.client, path)
	if err != nil {
		return nil, err
	}
	return decryptData(data, path)
}

func (ies *IAMEtcdStore) deleteIAMConfig(ctx context.Context, path string) error {
	return deleteKeyEtcd(ctx, ies.client, path)
}

func (ies *IAMEtcdStore) loadPolicyDocWithRetry(ctx context.Context, policy string, m map[string]PolicyDoc, _ int) error {
	return ies.loadPolicyDoc(ctx, policy, m)
}

func (ies *IAMEtcdStore) loadPolicyDoc(ctx context.Context, policy string, m map[string]PolicyDoc) error {
	data, err := ies.loadIAMConfigBytes(ctx, getPolicyDocPath(policy))
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

	m[policy] = p
	return nil
}

func (ies *IAMEtcdStore) getPolicyDocKV(ctx context.Context, kvs *mvccpb.KeyValue, m map[string]PolicyDoc) error {
	data, err := decryptData(kvs.Value, string(kvs.Key))
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

	policy := extractPathPrefixAndSuffix(string(kvs.Key), iamConfigPoliciesPrefix, path.Base(string(kvs.Key)))
	m[policy] = p
	return nil
}

func (ies *IAMEtcdStore) loadPolicyDocs(ctx context.Context, m map[string]PolicyDoc) error {
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
		if err = ies.getPolicyDocKV(ctx, kvs, m); err != nil && !errors.Is(err, errNoSuchPolicy) {
			return err
		}
	}
	return nil
}

func (ies *IAMEtcdStore) getUserKV(ctx context.Context, userkv *mvccpb.KeyValue, userType IAMUserType, m map[string]UserIdentity, basePrefix string) error {
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

func (ies *IAMEtcdStore) addUser(ctx context.Context, user string, userType IAMUserType, u UserIdentity, m map[string]UserIdentity) error {
	if u.Credentials.IsExpired() {
		// Delete expired identity.
		deleteKeyEtcd(ctx, ies.client, getUserIdentityPath(user, userType))
		deleteKeyEtcd(ctx, ies.client, getMappedPolicyPath(user, userType, false))
		return nil
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
				deleteKeyEtcd(ctx, ies.client, getUserIdentityPath(user, userType))
				deleteKeyEtcd(ctx, ies.client, getMappedPolicyPath(user, userType, false))
			}
			return nil
		}
		u.Credentials.Claims = jwtClaims.Map()
	}
	if u.Credentials.Description == "" {
		u.Credentials.Description = u.Credentials.Comment
	}

	m[user] = u
	return nil
}

func (ies *IAMEtcdStore) loadSecretKey(ctx context.Context, user string, userType IAMUserType) (string, error) {
	var u UserIdentity
	err := ies.loadIAMConfig(ctx, &u, getUserIdentityPath(user, userType))
	if err != nil {
		if errors.Is(err, errConfigNotFound) {
			return "", errNoSuchUser
		}
		return "", err
	}
	return u.Credentials.SecretKey, nil
}

func (ies *IAMEtcdStore) loadUser(ctx context.Context, user string, userType IAMUserType, m map[string]UserIdentity) error {
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

func (ies *IAMEtcdStore) loadUsers(ctx context.Context, userType IAMUserType, m map[string]UserIdentity) error {
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

func (ies *IAMEtcdStore) loadMappedPolicyWithRetry(ctx context.Context, name string, userType IAMUserType, isGroup bool, m *xsync.MapOf[string, MappedPolicy], retries int) error {
	return ies.loadMappedPolicy(ctx, name, userType, isGroup, m)
}

func (ies *IAMEtcdStore) loadMappedPolicy(ctx context.Context, name string, userType IAMUserType, isGroup bool, m *xsync.MapOf[string, MappedPolicy]) error {
	var p MappedPolicy
	err := ies.loadIAMConfig(ctx, &p, getMappedPolicyPath(name, userType, isGroup))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchPolicy
		}
		return err
	}
	m.Store(name, p)
	return nil
}

func getMappedPolicy(kv *mvccpb.KeyValue, m *xsync.MapOf[string, MappedPolicy], basePrefix string) error {
	var p MappedPolicy
	err := getIAMConfig(&p, kv.Value, string(kv.Key))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchPolicy
		}
		return err
	}
	name := extractPathPrefixAndSuffix(string(kv.Key), basePrefix, ".json")
	m.Store(name, p)
	return nil
}

func (ies *IAMEtcdStore) loadMappedPolicies(ctx context.Context, userType IAMUserType, isGroup bool, m *xsync.MapOf[string, MappedPolicy]) error {
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
		if err = getMappedPolicy(kv, m, basePrefix); err != nil && !errors.Is(err, errNoSuchPolicy) {
			return err
		}
	}
	return nil
}

func (ies *IAMEtcdStore) savePolicyDoc(ctx context.Context, policyName string, p PolicyDoc) error {
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
						iamLogIf(ctx, err)
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
