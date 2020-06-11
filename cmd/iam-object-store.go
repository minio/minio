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
	"strings"
	"sync"
	"time"

	jwtgo "github.com/dgrijalva/jwt-go"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	iampolicy "github.com/minio/minio/pkg/iam/policy"
	"github.com/minio/minio/pkg/madmin"
)

// IAMObjectStore implements IAMStorageAPI
type IAMObjectStore struct {
	// Protect assignment to objAPI
	sync.RWMutex

	ctx    context.Context
	objAPI ObjectLayer
}

func newIAMObjectStore(ctx context.Context, objAPI ObjectLayer) *IAMObjectStore {
	return &IAMObjectStore{ctx: ctx, objAPI: objAPI}
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

	for item := range listIAMConfigItems(ctx, iamOS.objAPI, basePrefix, true) {
		if item.Err != nil {
			return item.Err
		}

		user := item.Item

		{
			// 1. check if there is policy file in old location.
			oldPolicyPath := pathJoin(basePrefix, user, iamPolicyFile)
			var policyName string
			if err := iamOS.loadIAMConfig(&policyName, oldPolicyPath); err != nil {
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
			if err := iamOS.saveMappedPolicy(user, userType, false, mp); err != nil {
				return err
			}

			// 3. delete policy file from old
			// location. Ignore error.
			iamOS.deleteIAMConfig(oldPolicyPath)
		}
	next:
		// 4. check if user identity has old format.
		identityPath := pathJoin(basePrefix, user, iamIdentityFile)
		var cred auth.Credentials
		if err := iamOS.loadIAMConfig(&cred, identityPath); err != nil {
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
		if err := iamOS.saveIAMConfig(u, identityPath); err != nil {
			logger.LogIf(context.Background(), err)
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
	if err := iamOS.loadIAMConfig(&iamFmt, path); err != nil {
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
	if err := iamOS.saveIAMConfig(newIAMFormatVersion1(), path); err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	return nil
}

// Should be called under config migration lock
func (iamOS *IAMObjectStore) migrateBackendFormat(ctx context.Context) error {
	return iamOS.migrateToV1(ctx)
}

func (iamOS *IAMObjectStore) saveIAMConfig(item interface{}, path string) error {
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
	return saveConfig(context.Background(), iamOS.objAPI, path, data)
}

func (iamOS *IAMObjectStore) loadIAMConfig(item interface{}, path string) error {
	data, err := readConfig(iamOS.ctx, iamOS.objAPI, path)
	if err != nil {
		return err
	}
	if globalConfigEncrypted {
		data, err = madmin.DecryptData(globalActiveCred.String(), bytes.NewReader(data))
		if err != nil {
			return err
		}
	}
	return json.Unmarshal(data, item)
}

func (iamOS *IAMObjectStore) deleteIAMConfig(path string) error {
	return deleteConfig(iamOS.ctx, iamOS.objAPI, path)
}

func (iamOS *IAMObjectStore) loadPolicyDoc(policy string, m map[string]iampolicy.Policy) error {
	var p iampolicy.Policy
	err := iamOS.loadIAMConfig(&p, getPolicyDocPath(policy))
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
	for item := range listIAMConfigItems(ctx, iamOS.objAPI, iamConfigPoliciesPrefix, true) {
		if item.Err != nil {
			return item.Err
		}

		policyName := item.Item
		err := iamOS.loadPolicyDoc(policyName, m)
		if err != nil {
			return err
		}
	}
	return nil
}

func (iamOS *IAMObjectStore) loadUser(user string, userType IAMUserType, m map[string]auth.Credentials) error {
	var u UserIdentity
	err := iamOS.loadIAMConfig(&u, getUserIdentityPath(user, userType))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchUser
		}
		return err
	}

	if u.Credentials.IsExpired() {
		// Delete expired identity - ignoring errors here.
		iamOS.deleteIAMConfig(getUserIdentityPath(user, userType))
		iamOS.deleteIAMConfig(getMappedPolicyPath(user, userType, false))
		return nil
	}

	// If this is a service account, rotate the session key if needed
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
					err := iamOS.saveIAMConfig(&u, getUserIdentityPath(user, userType))
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

	for item := range listIAMConfigItems(ctx, iamOS.objAPI, basePrefix, true) {
		if item.Err != nil {
			return item.Err
		}

		userName := item.Item
		err := iamOS.loadUser(userName, userType, m)
		if err != nil {
			return err
		}
	}
	return nil
}

func (iamOS *IAMObjectStore) loadGroup(group string, m map[string]GroupInfo) error {
	var g GroupInfo
	err := iamOS.loadIAMConfig(&g, getGroupInfoPath(group))
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
	for item := range listIAMConfigItems(ctx, iamOS.objAPI, iamConfigGroupsPrefix, true) {
		if item.Err != nil {
			return item.Err
		}

		group := item.Item
		err := iamOS.loadGroup(group, m)
		if err != nil {
			return err
		}
	}
	return nil
}

func (iamOS *IAMObjectStore) loadMappedPolicy(name string, userType IAMUserType, isGroup bool,
	m map[string]MappedPolicy) error {

	var p MappedPolicy
	err := iamOS.loadIAMConfig(&p, getMappedPolicyPath(name, userType, isGroup))
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
	for item := range listIAMConfigItems(ctx, iamOS.objAPI, basePath, false) {
		if item.Err != nil {
			return item.Err
		}

		policyFile := item.Item
		userOrGroupName := strings.TrimSuffix(policyFile, ".json")
		err := iamOS.loadMappedPolicy(userOrGroupName, userType, isGroup, m)
		if err != nil && err != errNoSuchPolicy {
			return err
		}
	}
	return nil
}

// Refresh IAMSys. If an object layer is passed in use that, otherwise
// load from global.
func (iamOS *IAMObjectStore) loadAll(ctx context.Context, sys *IAMSys) error {
	iamUsersMap := make(map[string]auth.Credentials)
	iamGroupsMap := make(map[string]GroupInfo)
	iamUserPolicyMap := make(map[string]MappedPolicy)
	iamGroupPolicyMap := make(map[string]MappedPolicy)

	iamOS.rlock()
	isMinIOUsersSys := sys.usersSysType == MinIOUsersSysType
	iamOS.runlock()

	iamOS.lock()
	if err := iamOS.loadPolicyDocs(ctx, sys.iamPolicyDocsMap); err != nil {
		iamOS.unlock()
		return err
	}
	// Sets default canned policies, if none are set.
	setDefaultCannedPolicies(sys.iamPolicyDocsMap)

	iamOS.unlock()

	if isMinIOUsersSys {
		if err := iamOS.loadUsers(ctx, regularUser, iamUsersMap); err != nil {
			return err
		}
		if err := iamOS.loadGroups(ctx, iamGroupsMap); err != nil {
			return err
		}
	}

	// load polices mapped to users
	if err := iamOS.loadMappedPolicies(ctx, regularUser, false, iamUserPolicyMap); err != nil {
		return err
	}

	// load policies mapped to groups
	if err := iamOS.loadMappedPolicies(ctx, regularUser, true, iamGroupPolicyMap); err != nil {
		return err
	}

	if err := iamOS.loadUsers(ctx, srvAccUser, iamUsersMap); err != nil {
		return err
	}

	// load STS temp users
	if err := iamOS.loadUsers(ctx, stsUser, iamUsersMap); err != nil {
		return err
	}

	// load STS policy mappings
	if err := iamOS.loadMappedPolicies(ctx, stsUser, false, iamUserPolicyMap); err != nil {
		return err
	}

	iamOS.lock()
	defer iamOS.unlock()

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
			// Deleting on the disk is taken care of in the next cycle
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

func (iamOS *IAMObjectStore) savePolicyDoc(policyName string, p iampolicy.Policy) error {
	return iamOS.saveIAMConfig(&p, getPolicyDocPath(policyName))
}

func (iamOS *IAMObjectStore) saveMappedPolicy(name string, userType IAMUserType, isGroup bool, mp MappedPolicy) error {
	return iamOS.saveIAMConfig(mp, getMappedPolicyPath(name, userType, isGroup))
}

func (iamOS *IAMObjectStore) saveUserIdentity(name string, userType IAMUserType, u UserIdentity) error {
	return iamOS.saveIAMConfig(u, getUserIdentityPath(name, userType))
}

func (iamOS *IAMObjectStore) saveGroupInfo(name string, gi GroupInfo) error {
	return iamOS.saveIAMConfig(gi, getGroupInfoPath(name))
}

func (iamOS *IAMObjectStore) deletePolicyDoc(name string) error {
	err := iamOS.deleteIAMConfig(getPolicyDocPath(name))
	if err == errConfigNotFound {
		err = errNoSuchPolicy
	}
	return err
}

func (iamOS *IAMObjectStore) deleteMappedPolicy(name string, userType IAMUserType, isGroup bool) error {
	err := iamOS.deleteIAMConfig(getMappedPolicyPath(name, userType, isGroup))
	if err == errConfigNotFound {
		err = errNoSuchPolicy
	}
	return err
}

func (iamOS *IAMObjectStore) deleteUserIdentity(name string, userType IAMUserType) error {
	err := iamOS.deleteIAMConfig(getUserIdentityPath(name, userType))
	if err == errConfigNotFound {
		err = errNoSuchUser
	}
	return err
}

func (iamOS *IAMObjectStore) deleteGroupInfo(name string) error {
	err := iamOS.deleteIAMConfig(getGroupInfoPath(name))
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
func listIAMConfigItems(ctx context.Context, objAPI ObjectLayer, pathPrefix string, dirs bool) <-chan itemOrErr {
	ch := make(chan itemOrErr)

	dirList := func(lo ListObjectsInfo) []string {
		return lo.Prefixes
	}
	filesList := func(lo ListObjectsInfo) (r []string) {
		for _, o := range lo.Objects {
			r = append(r, o.Name)
		}
		return r
	}

	go func() {
		defer close(ch)

		marker := ""
		for {
			lo, err := objAPI.ListObjects(context.Background(),
				minioMetaBucket, pathPrefix, marker, SlashSeparator, maxObjectList)
			if err != nil {
				select {
				case ch <- itemOrErr{Err: err}:
				case <-ctx.Done():
				}
				return
			}

			marker = lo.NextMarker
			lister := dirList(lo)
			if !dirs {
				lister = filesList(lo)
			}
			for _, itemPrefix := range lister {
				item := strings.TrimPrefix(itemPrefix, pathPrefix)
				item = strings.TrimSuffix(item, SlashSeparator)
				select {
				case ch <- itemOrErr{Item: item}:
				case <-ctx.Done():
					return
				}
			}
			if !lo.IsTruncated {
				return
			}
		}
	}()

	return ch
}

func (iamOS *IAMObjectStore) watch(ctx context.Context, sys *IAMSys) {
	// Refresh IAMSys.
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.NewTimer(globalRefreshIAMInterval).C:
			logger.LogIf(ctx, iamOS.loadAll(ctx, sys))
		}
	}
}
