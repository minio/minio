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

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	iampolicy "github.com/minio/minio/pkg/iam/policy"
	"github.com/minio/minio/pkg/madmin"
)

// IAMObjectStore implements IAMStorageAPI
type IAMObjectStore struct {
	// Protect assignment to objAPI
	sync.RWMutex

	objAPI ObjectLayer
}

func newIAMObjectStore() *IAMObjectStore {
	return &IAMObjectStore{objAPI: nil}
}

func (iamOS *IAMObjectStore) getObjectAPI() ObjectLayer {
	iamOS.RLock()
	defer iamOS.RUnlock()
	if iamOS.objAPI != nil {
		return iamOS.objAPI
	}
	return newObjectLayerWithoutSafeModeFn()
}

func (iamOS *IAMObjectStore) setObjectAPI(objAPI ObjectLayer) {
	iamOS.Lock()
	defer iamOS.Unlock()
	iamOS.objAPI = objAPI
}

func (iamOS *IAMObjectStore) clearObjectAPI() {
	iamOS.Lock()
	defer iamOS.Unlock()
	iamOS.objAPI = nil
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
func (iamOS *IAMObjectStore) migrateUsersConfigToV1(isSTS bool) error {
	basePrefix := iamConfigUsersPrefix
	if isSTS {
		basePrefix = iamConfigSTSPrefix
	}

	objAPI := iamOS.getObjectAPI()

	doneCh := make(chan struct{})
	defer close(doneCh)
	for item := range listIAMConfigItems(objAPI, basePrefix, true, doneCh) {
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
			if err := iamOS.saveMappedPolicy(user, isSTS, false, mp); err != nil {
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
		if cred == zeroCred {
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

func (iamOS *IAMObjectStore) migrateToV1() error {
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
	if err := iamOS.migrateUsersConfigToV1(false); err != nil {
		logger.LogIf(context.Background(), err)
		return err
	}
	// Migrate STS users
	if err := iamOS.migrateUsersConfigToV1(true); err != nil {
		logger.LogIf(context.Background(), err)
		return err
	}
	// Save iam format to version 1.
	if err := iamOS.saveIAMConfig(newIAMFormatVersion1(), path); err != nil {
		logger.LogIf(context.Background(), err)
		return err
	}
	return nil
}

// Should be called under config migration lock
func (iamOS *IAMObjectStore) migrateBackendFormat(objAPI ObjectLayer) error {
	iamOS.setObjectAPI(objAPI)
	defer iamOS.clearObjectAPI()
	if err := iamOS.migrateToV1(); err != nil {
		return err
	}
	return nil
}

func (iamOS *IAMObjectStore) saveIAMConfig(item interface{}, path string) error {
	objectAPI := iamOS.getObjectAPI()
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
	return saveConfig(context.Background(), objectAPI, path, data)
}

func (iamOS *IAMObjectStore) loadIAMConfig(item interface{}, path string) error {
	objectAPI := iamOS.getObjectAPI()
	data, err := readConfig(context.Background(), objectAPI, path)
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
	err := deleteConfig(context.Background(), iamOS.getObjectAPI(), path)
	if _, ok := err.(ObjectNotFound); ok {
		return errConfigNotFound
	}
	return err
}

func (iamOS *IAMObjectStore) loadPolicyDoc(policy string, m map[string]iampolicy.Policy) error {
	objectAPI := iamOS.getObjectAPI()
	if objectAPI == nil {
		return errServerNotInitialized
	}

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

func (iamOS *IAMObjectStore) loadPolicyDocs(m map[string]iampolicy.Policy) error {
	objectAPI := iamOS.getObjectAPI()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	doneCh := make(chan struct{})
	defer close(doneCh)
	for item := range listIAMConfigItems(objectAPI, iamConfigPoliciesPrefix, true, doneCh) {
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

func (iamOS *IAMObjectStore) loadUser(user string, isSTS bool, m map[string]auth.Credentials) error {
	objectAPI := iamOS.getObjectAPI()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	var u UserIdentity
	err := iamOS.loadIAMConfig(&u, getUserIdentityPath(user, isSTS))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchUser
		}
		return err
	}

	if u.Credentials.IsExpired() {
		// Delete expired identity - ignoring errors here.
		iamOS.deleteIAMConfig(getUserIdentityPath(user, isSTS))
		iamOS.deleteIAMConfig(getMappedPolicyPath(user, isSTS, false))
		return nil
	}

	if u.Credentials.AccessKey == "" {
		u.Credentials.AccessKey = user
	}
	m[user] = u.Credentials
	return nil
}

func (iamOS *IAMObjectStore) loadUsers(isSTS bool, m map[string]auth.Credentials) error {
	objectAPI := iamOS.getObjectAPI()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	doneCh := make(chan struct{})
	defer close(doneCh)
	basePrefix := iamConfigUsersPrefix
	if isSTS {
		basePrefix = iamConfigSTSPrefix
	}
	for item := range listIAMConfigItems(objectAPI, basePrefix, true, doneCh) {
		if item.Err != nil {
			return item.Err
		}

		userName := item.Item
		err := iamOS.loadUser(userName, isSTS, m)
		if err != nil {
			return err
		}
	}
	return nil
}

func (iamOS *IAMObjectStore) loadGroup(group string, m map[string]GroupInfo) error {
	objectAPI := iamOS.getObjectAPI()
	if objectAPI == nil {
		return errServerNotInitialized
	}

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

func (iamOS *IAMObjectStore) loadGroups(m map[string]GroupInfo) error {
	objectAPI := iamOS.getObjectAPI()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	doneCh := make(chan struct{})
	defer close(doneCh)
	for item := range listIAMConfigItems(objectAPI, iamConfigGroupsPrefix, true, doneCh) {
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

func (iamOS *IAMObjectStore) loadMappedPolicy(name string, isSTS, isGroup bool,
	m map[string]MappedPolicy) error {

	objectAPI := iamOS.getObjectAPI()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	var p MappedPolicy
	err := iamOS.loadIAMConfig(&p, getMappedPolicyPath(name, isSTS, isGroup))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchPolicy
		}
		return err
	}
	m[name] = p
	return nil
}

func (iamOS *IAMObjectStore) loadMappedPolicies(isSTS, isGroup bool, m map[string]MappedPolicy) error {
	objectAPI := iamOS.getObjectAPI()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	doneCh := make(chan struct{})
	defer close(doneCh)
	var basePath string
	switch {
	case isSTS:
		basePath = iamConfigPolicyDBSTSUsersPrefix
	case isGroup:
		basePath = iamConfigPolicyDBGroupsPrefix
	default:
		basePath = iamConfigPolicyDBUsersPrefix
	}
	for item := range listIAMConfigItems(objectAPI, basePath, false, doneCh) {
		if item.Err != nil {
			return item.Err
		}

		policyFile := item.Item
		userOrGroupName := strings.TrimSuffix(policyFile, ".json")
		err := iamOS.loadMappedPolicy(userOrGroupName, isSTS, isGroup, m)
		if err != nil {
			return err
		}
	}
	return nil
}

// Refresh IAMSys. If an object layer is passed in use that, otherwise
// load from global.
func (iamOS *IAMObjectStore) loadAll(sys *IAMSys, objectAPI ObjectLayer) error {
	if objectAPI == nil {
		objectAPI = iamOS.getObjectAPI()
	}
	if objectAPI == nil {
		return errServerNotInitialized
	}
	// cache object layer for other load* functions
	iamOS.setObjectAPI(objectAPI)
	defer iamOS.clearObjectAPI()

	iamUsersMap := make(map[string]auth.Credentials)
	iamGroupsMap := make(map[string]GroupInfo)
	iamPolicyDocsMap := make(map[string]iampolicy.Policy)
	iamUserPolicyMap := make(map[string]MappedPolicy)
	iamGroupPolicyMap := make(map[string]MappedPolicy)

	isMinIOUsersSys := false
	sys.RLock()
	if sys.usersSysType == MinIOUsersSysType {
		isMinIOUsersSys = true
	}
	sys.RUnlock()

	if err := iamOS.loadPolicyDocs(iamPolicyDocsMap); err != nil {
		return err
	}
	// load STS temp users
	if err := iamOS.loadUsers(true, iamUsersMap); err != nil {
		return err
	}
	if isMinIOUsersSys {
		if err := iamOS.loadUsers(false, iamUsersMap); err != nil {
			return err
		}
		if err := iamOS.loadGroups(iamGroupsMap); err != nil {
			return err
		}

		if err := iamOS.loadMappedPolicies(false, false, iamUserPolicyMap); err != nil {
			return err
		}
	}
	// load STS policy mappings
	if err := iamOS.loadMappedPolicies(true, false, iamUserPolicyMap); err != nil {
		return err
	}
	// load policies mapped to groups
	if err := iamOS.loadMappedPolicies(false, true, iamGroupPolicyMap); err != nil {
		return err
	}

	// Sets default canned policies, if none are set.
	setDefaultCannedPolicies(iamPolicyDocsMap)

	sys.Lock()
	defer sys.Unlock()

	sys.iamUsersMap = iamUsersMap
	sys.iamPolicyDocsMap = iamPolicyDocsMap
	sys.iamUserPolicyMap = iamUserPolicyMap
	sys.iamGroupPolicyMap = iamGroupPolicyMap
	sys.iamGroupsMap = iamGroupsMap
	sys.buildUserGroupMemberships()

	return nil
}

func (iamOS *IAMObjectStore) savePolicyDoc(policyName string, p iampolicy.Policy) error {
	return iamOS.saveIAMConfig(&p, getPolicyDocPath(policyName))
}

func (iamOS *IAMObjectStore) saveMappedPolicy(name string, isSTS, isGroup bool, mp MappedPolicy) error {
	return iamOS.saveIAMConfig(mp, getMappedPolicyPath(name, isSTS, isGroup))
}

func (iamOS *IAMObjectStore) saveUserIdentity(name string, isSTS bool, u UserIdentity) error {
	return iamOS.saveIAMConfig(u, getUserIdentityPath(name, isSTS))
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

func (iamOS *IAMObjectStore) deleteMappedPolicy(name string, isSTS, isGroup bool) error {
	err := iamOS.deleteIAMConfig(getMappedPolicyPath(name, isSTS, isGroup))
	if err == errConfigNotFound {
		err = errNoSuchPolicy
	}
	return err
}

func (iamOS *IAMObjectStore) deleteUserIdentity(name string, isSTS bool) error {
	err := iamOS.deleteIAMConfig(getUserIdentityPath(name, isSTS))
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
func listIAMConfigItems(objectAPI ObjectLayer, pathPrefix string, dirs bool,
	doneCh <-chan struct{}) <-chan itemOrErr {

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
		marker := ""
		for {
			lo, err := objectAPI.ListObjects(context.Background(),
				minioMetaBucket, pathPrefix, marker, SlashSeparator, maxObjectList)
			if err != nil {
				select {
				case ch <- itemOrErr{Err: err}:
				case <-doneCh:
				}
				close(ch)
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
				case <-doneCh:
					close(ch)
					return
				}
			}
			if !lo.IsTruncated {
				close(ch)
				return
			}
		}
	}()
	return ch
}

func (iamOS *IAMObjectStore) watch(sys *IAMSys) {
	watchDisk := func() {
		ticker := time.NewTicker(globalRefreshIAMInterval)
		defer ticker.Stop()
		for {
			select {
			case <-GlobalServiceDoneCh:
				return
			case <-ticker.C:
				iamOS.loadAll(sys, nil)
			}
		}
	}
	// Refresh IAMSys in background.
	go watchDisk()
}
