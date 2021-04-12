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
	"unicode/utf8"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	iampolicy "github.com/minio/minio/pkg/iam/policy"
	"github.com/minio/minio/pkg/kms"
	"github.com/minio/minio/pkg/madmin"
)

// IAMObjectStore implements IAMStorageAPI
type IAMObjectStore struct {
	// Protect assignment to objAPI
	sync.RWMutex

	objAPI ObjectLayer
}

func newIAMObjectStore(objAPI ObjectLayer) *IAMObjectStore {
	return &IAMObjectStore{objAPI: objAPI}
}

func (iamOS *IAMObjectStore) lock() {
	iamOS.Lock()
}

func (iamOS *IAMObjectStore) unlock() {
	iamOS.Unlock()
}

func (iamOS *IAMObjectStore) rlock() {
	iamOS.RLock()
}

func (iamOS *IAMObjectStore) runlock() {
	iamOS.RUnlock()
}

// Migrate users directory in a single scan.
//
// 1. Migrate user policy from:
//
// `iamConfigUsersPrefix + "<username>/policy.json"`
//
// to:
//
// `iamConfigPolicyDBUsersPrefix + "<username>.json"`.
//
// 2. Add versioning to the policy json file in the new
// location.
//
// 3. Migrate user identity json file to include version info.
func (iamOS *IAMObjectStore) migrateUsersConfigToV1(ctx context.Context, isSTS bool) error {
	basePrefix := iamConfigUsersPrefix
	if isSTS {
		basePrefix = iamConfigSTSPrefix
	}

	for item := range listIAMConfigItems(ctx, iamOS.objAPI, basePrefix) {
		if item.Err != nil {
			return item.Err
		}

		user := path.Dir(item.Item)
		{
			// 1. check if there is policy file in old location.
			oldPolicyPath := pathJoin(basePrefix, user, iamPolicyFile)
			var policyName string
			if err := iamOS.loadIAMConfig(ctx, &policyName, oldPolicyPath); err != nil {
				switch err {
				case errConfigNotFound:
					// This case means it is already
					// migrated or there is no policy on
					// user.
				default:
					// File may be corrupt or network error
				}

				// Nothing to do on the policy file,
				// so move on to check the id file.
				goto next
			}

			// 2. copy policy file to new location.
			mp := newMappedPolicy(policyName)
			userType := regularUser
			if isSTS {
				userType = stsUser
			}
			if err := iamOS.saveMappedPolicy(ctx, user, userType, false, mp); err != nil {
				return err
			}

			// 3. delete policy file from old
			// location. Ignore error.
			iamOS.deleteIAMConfig(ctx, oldPolicyPath)
		}
	next:
		// 4. check if user identity has old format.
		identityPath := pathJoin(basePrefix, user, iamIdentityFile)
		var cred auth.Credentials
		if err := iamOS.loadIAMConfig(ctx, &cred, identityPath); err != nil {
			switch err {
			case errConfigNotFound:
				// This should not happen.
			default:
				// File may be corrupt or network error
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
		if err := iamOS.saveIAMConfig(ctx, u, identityPath); err != nil {
			logger.LogIf(ctx, err)
			return err
		}

		// Nothing to delete as identity file location
		// has not changed.
	}
	return nil

}

func (iamOS *IAMObjectStore) migrateToV1(ctx context.Context) error {
	var iamFmt iamFormat
	path := getIAMFormatFilePath()
	if err := iamOS.loadIAMConfig(ctx, &iamFmt, path); err != nil {
		switch err {
		case errConfigNotFound:
			// Need to migrate to V1.
		default:
			return err
		}
	} else {
		if iamFmt.Version >= iamFormatVersion1 {
			// Nothing to do.
			return nil
		}
		// This case should not happen
		// (i.e. Version is 0 or negative.)
		return errors.New("got an invalid IAM format version")
	}

	// Migrate long-term users
	if err := iamOS.migrateUsersConfigToV1(ctx, false); err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	// Migrate STS users
	if err := iamOS.migrateUsersConfigToV1(ctx, true); err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	// Save iam format to version 1.
	if err := iamOS.saveIAMConfig(ctx, newIAMFormatVersion1(), path); err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	return nil
}

// Should be called under config migration lock
func (iamOS *IAMObjectStore) migrateBackendFormat(ctx context.Context) error {
	return iamOS.migrateToV1(ctx)
}

func (iamOS *IAMObjectStore) saveIAMConfig(ctx context.Context, item interface{}, objPath string, opts ...options) error {
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

func (iamOS *IAMObjectStore) loadIAMConfig(ctx context.Context, item interface{}, objPath string) error {
	data, err := readConfig(ctx, iamOS.objAPI, objPath)
	if err != nil {
		return err
	}
	if !utf8.Valid(data) {
		if GlobalKMS != nil {
			data, err = config.DecryptBytes(GlobalKMS, data, kms.Context{
				minioMetaBucket: path.Join(minioMetaBucket, objPath),
			})
			if err != nil {
				data, err = madmin.DecryptData(globalActiveCred.String(), bytes.NewReader(data))
				if err != nil {
					return err
				}
			}
		} else {
			data, err = madmin.DecryptData(globalActiveCred.String(), bytes.NewReader(data))
			if err != nil {
				return err
			}
		}
	}
	return json.Unmarshal(data, item)
}

func (iamOS *IAMObjectStore) deleteIAMConfig(ctx context.Context, path string) error {
	return deleteConfig(ctx, iamOS.objAPI, path)
}

func (iamOS *IAMObjectStore) loadPolicyDoc(ctx context.Context, policy string, m map[string]iampolicy.Policy) error {
	var p iampolicy.Policy
	err := iamOS.loadIAMConfig(ctx, &p, getPolicyDocPath(policy))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchPolicy
		}
		return err
	}
	m[policy] = p
	return nil
}

func (iamOS *IAMObjectStore) loadPolicyDocs(ctx context.Context, m map[string]iampolicy.Policy) error {
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

func (iamOS *IAMObjectStore) loadUser(ctx context.Context, user string, userType IAMUserType, m map[string]auth.Credentials) error {
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

	m[user] = u.Credentials
	return nil
}

func (iamOS *IAMObjectStore) loadUsers(ctx context.Context, userType IAMUserType, m map[string]auth.Credentials) error {
	var basePrefix string
	switch userType {
	case srvAccUser:
		basePrefix = iamConfigServiceAccountsPrefix
	case stsUser:
		basePrefix = iamConfigSTSPrefix
	default:
		basePrefix = iamConfigUsersPrefix
	}

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
	m map[string]MappedPolicy) error {

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
		case srvAccUser:
			basePath = iamConfigPolicyDBServiceAccountsPrefix
		case stsUser:
			basePath = iamConfigPolicyDBSTSUsersPrefix
		default:
			basePath = iamConfigPolicyDBUsersPrefix
		}
	}
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

// Refresh IAMSys. If an object layer is passed in use that, otherwise load from global.
func (iamOS *IAMObjectStore) loadAll(ctx context.Context, sys *IAMSys) error {
	return sys.Load(ctx, iamOS)
}

func (iamOS *IAMObjectStore) savePolicyDoc(ctx context.Context, policyName string, p iampolicy.Policy) error {
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

func (iamOS *IAMObjectStore) watch(ctx context.Context, sys *IAMSys) {
	// Refresh IAMSys.
	for {
		time.Sleep(globalRefreshIAMInterval)
		if err := iamOS.loadAll(ctx, sys); err != nil {
			logger.LogIf(ctx, err)
		}
	}
}
