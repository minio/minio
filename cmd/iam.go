/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
	"context"
	"encoding/json"
	"path"
	"strings"
	"sync"
	"time"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/minio/minio-go/pkg/set"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/iam/policy"
	"github.com/minio/minio/pkg/madmin"
)

const (
	// IAM configuration directory.
	iamConfigPrefix = minioConfigPrefix + "/iam"

	// IAM users directory.
	iamConfigUsersPrefix = iamConfigPrefix + "/users/"

	// IAM policies directory.
	iamConfigPoliciesPrefix = iamConfigPrefix + "/policies/"

	// IAM sts directory.
	iamConfigSTSPrefix = iamConfigPrefix + "/sts/"

	// IAM identity file which captures identity credentials.
	iamIdentityFile = "identity.json"

	// IAM policy file which provides policies for each users.
	iamPolicyFile = "policy.json"
)

// IAMSys - config system.
type IAMSys struct {
	sync.RWMutex
	iamUsersMap        map[string]auth.Credentials
	iamPolicyMap       map[string]string
	iamCannedPolicyMap map[string]iampolicy.Policy
}

// Load - loads iam subsystem
func (sys *IAMSys) Load(objAPI ObjectLayer) error {
	return sys.refresh(objAPI)
}

// Init - initializes config system from iam.json
func (sys *IAMSys) Init(objAPI ObjectLayer) error {
	if objAPI == nil {
		return errInvalidArgument
	}

	defer func() {
		// Refresh IAMSys in background.
		go func() {
			ticker := time.NewTicker(globalRefreshIAMInterval)
			defer ticker.Stop()
			for {
				select {
				case <-globalServiceDoneCh:
					return
				case <-ticker.C:
					sys.refresh(objAPI)
				}
			}
		}()
	}()

	doneCh := make(chan struct{})
	defer close(doneCh)

	// Initializing IAM needs a retry mechanism for
	// the following reasons:
	//  - Read quorum is lost just after the initialization
	//    of the object layer.
	retryTimerCh := newRetryTimerSimple(doneCh)
	for {
		select {
		case _ = <-retryTimerCh:
			// Load IAMSys once during boot.
			if err := sys.refresh(objAPI); err != nil {
				if err == errDiskNotFound ||
					strings.Contains(err.Error(), InsufficientReadQuorum{}.Error()) ||
					strings.Contains(err.Error(), InsufficientWriteQuorum{}.Error()) {
					logger.Info("Waiting for IAM subsystem to be initialized..")
					continue
				}
				return err
			}
			return nil
		}
	}
}

// DeleteCannedPolicy - deletes a canned policy.
func (sys *IAMSys) DeleteCannedPolicy(policyName string) error {
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	if policyName == "" {
		return errInvalidArgument
	}

	var err error
	configFile := pathJoin(iamConfigPoliciesPrefix, policyName, iamPolicyFile)
	if globalEtcdClient != nil {
		err = deleteConfigEtcd(context.Background(), globalEtcdClient, configFile)
	} else {
		err = deleteConfig(context.Background(), objectAPI, configFile)
	}

	sys.Lock()
	defer sys.Unlock()

	delete(sys.iamCannedPolicyMap, policyName)
	return err
}

// ListCannedPolicies - lists all canned policies.
func (sys *IAMSys) ListCannedPolicies() (map[string][]byte, error) {
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return nil, errServerNotInitialized
	}

	var cannedPolicyMap = make(map[string][]byte)

	sys.RLock()
	defer sys.RUnlock()

	for k, v := range sys.iamCannedPolicyMap {
		data, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		cannedPolicyMap[k] = data
	}

	return cannedPolicyMap, nil
}

// SetCannedPolicy - sets a new canned policy.
func (sys *IAMSys) SetCannedPolicy(policyName string, p iampolicy.Policy) error {
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	if p.IsEmpty() || policyName == "" {
		return errInvalidArgument
	}

	configFile := pathJoin(iamConfigPoliciesPrefix, policyName, iamPolicyFile)
	data, err := json.Marshal(p)
	if err != nil {
		return err
	}

	if globalEtcdClient != nil {
		err = saveConfigEtcd(context.Background(), globalEtcdClient, configFile, data)
	} else {
		err = saveConfig(context.Background(), objectAPI, configFile, data)
	}
	if err != nil {
		return err
	}

	sys.Lock()
	defer sys.Unlock()

	sys.iamCannedPolicyMap[policyName] = p

	return nil
}

// SetUserPolicy - sets policy to given user name.
func (sys *IAMSys) SetUserPolicy(accessKey, policyName string) error {
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	sys.Lock()
	defer sys.Unlock()

	if _, ok := sys.iamUsersMap[accessKey]; !ok {
		return errNoSuchUser
	}

	if _, ok := sys.iamCannedPolicyMap[policyName]; !ok {
		return errNoSuchPolicy
	}

	data, err := json.Marshal(policyName)
	if err != nil {
		return err
	}

	configFile := pathJoin(iamConfigUsersPrefix, accessKey, iamPolicyFile)
	if globalEtcdClient != nil {
		err = saveConfigEtcd(context.Background(), globalEtcdClient, configFile, data)
	} else {
		err = saveConfig(context.Background(), objectAPI, configFile, data)
	}
	if err != nil {
		return err
	}

	sys.iamPolicyMap[accessKey] = policyName
	return nil
}

// DeleteUser - set user credentials.
func (sys *IAMSys) DeleteUser(accessKey string) error {
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	var err error
	pFile := pathJoin(iamConfigUsersPrefix, accessKey, iamPolicyFile)
	iFile := pathJoin(iamConfigUsersPrefix, accessKey, iamIdentityFile)
	if globalEtcdClient != nil {
		// It is okay to ingnore errors when deleting policy.json for the user.
		_ = deleteConfigEtcd(context.Background(), globalEtcdClient, pFile)
		err = deleteConfigEtcd(context.Background(), globalEtcdClient, iFile)
	} else {
		// It is okay to ingnore errors when deleting policy.json for the user.
		_ = deleteConfig(context.Background(), objectAPI, pFile)
		err = deleteConfig(context.Background(), objectAPI, iFile)
	}

	//
	switch err.(type) {
	case ObjectNotFound:
		err = errNoSuchUser
	}

	sys.Lock()
	defer sys.Unlock()

	delete(sys.iamUsersMap, accessKey)
	delete(sys.iamPolicyMap, accessKey)

	return err
}

// SetTempUser - set temporary user credentials, these credentials have an expiry.
func (sys *IAMSys) SetTempUser(accessKey string, cred auth.Credentials, policyName string) error {
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	sys.Lock()
	defer sys.Unlock()

	// If OPA is not set we honor any policy claims for this
	// temporary user which match with pre-configured canned
	// policies for this server.
	if globalPolicyOPA == nil && policyName != "" {
		p, ok := sys.iamCannedPolicyMap[policyName]
		if !ok {
			return errInvalidArgument
		}
		if p.IsEmpty() {
			delete(sys.iamPolicyMap, accessKey)
			return nil
		}

		data, err := json.Marshal(policyName)
		if err != nil {
			return err
		}

		configFile := pathJoin(iamConfigSTSPrefix, accessKey, iamPolicyFile)
		if globalEtcdClient != nil {
			err = saveConfigEtcd(context.Background(), globalEtcdClient, configFile, data)
		} else {
			err = saveConfig(context.Background(), objectAPI, configFile, data)
		}
		if err != nil {
			return err
		}

		sys.iamPolicyMap[accessKey] = policyName
	}

	configFile := pathJoin(iamConfigSTSPrefix, accessKey, iamIdentityFile)
	data, err := json.Marshal(cred)
	if err != nil {
		return err
	}

	if globalEtcdClient != nil {
		err = saveConfigEtcd(context.Background(), globalEtcdClient, configFile, data)
	} else {
		err = saveConfig(context.Background(), objectAPI, configFile, data)
	}

	if err != nil {
		return err
	}

	sys.iamUsersMap[accessKey] = cred
	return nil
}

// ListUsers - list all users.
func (sys *IAMSys) ListUsers() (map[string]madmin.UserInfo, error) {
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return nil, errServerNotInitialized
	}

	var users = make(map[string]madmin.UserInfo)

	sys.RLock()
	defer sys.RUnlock()

	for k, v := range sys.iamUsersMap {
		users[k] = madmin.UserInfo{
			PolicyName: sys.iamPolicyMap[k],
			Status:     madmin.AccountStatus(v.Status),
		}
	}

	return users, nil
}

// SetUserStatus - sets current user status, supports disabled or enabled.
func (sys *IAMSys) SetUserStatus(accessKey string, status madmin.AccountStatus) error {
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	if status != madmin.AccountEnabled && status != madmin.AccountDisabled {
		return errInvalidArgument
	}

	sys.Lock()
	defer sys.Unlock()

	cred, ok := sys.iamUsersMap[accessKey]
	if !ok {
		return errNoSuchUser
	}

	uinfo := madmin.UserInfo{
		SecretKey: cred.SecretKey,
		Status:    status,
	}

	configFile := pathJoin(iamConfigUsersPrefix, accessKey, iamIdentityFile)
	data, err := json.Marshal(uinfo)
	if err != nil {
		return err
	}

	if globalEtcdClient != nil {
		err = saveConfigEtcd(context.Background(), globalEtcdClient, configFile, data)
	} else {
		err = saveConfig(context.Background(), objectAPI, configFile, data)
	}

	if err != nil {
		return err
	}

	sys.iamUsersMap[accessKey] = auth.Credentials{
		AccessKey: accessKey,
		SecretKey: uinfo.SecretKey,
		Status:    string(uinfo.Status),
	}

	return nil
}

// SetUser - set user credentials.
func (sys *IAMSys) SetUser(accessKey string, uinfo madmin.UserInfo) error {
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	configFile := pathJoin(iamConfigUsersPrefix, accessKey, iamIdentityFile)
	data, err := json.Marshal(uinfo)
	if err != nil {
		return err
	}

	if globalEtcdClient != nil {
		err = saveConfigEtcd(context.Background(), globalEtcdClient, configFile, data)
	} else {
		err = saveConfig(context.Background(), objectAPI, configFile, data)
	}

	if err != nil {
		return err
	}

	sys.Lock()
	defer sys.Unlock()

	sys.iamUsersMap[accessKey] = auth.Credentials{
		AccessKey: accessKey,
		SecretKey: uinfo.SecretKey,
		Status:    string(uinfo.Status),
	}

	return nil
}

// GetUser - get user credentials
func (sys *IAMSys) GetUser(accessKey string) (cred auth.Credentials, ok bool) {
	sys.RLock()
	defer sys.RUnlock()

	cred, ok = sys.iamUsersMap[accessKey]
	return cred, ok && cred.IsValid()
}

// IsAllowed - checks given policy args is allowed to continue the Rest API.
func (sys *IAMSys) IsAllowed(args iampolicy.Args) bool {
	sys.RLock()
	defer sys.RUnlock()

	// If opa is configured, use OPA always.
	if globalPolicyOPA != nil {
		return globalPolicyOPA.IsAllowed(args)
	}

	// If policy is available for given user, check the policy.
	if name, found := sys.iamPolicyMap[args.AccountName]; found {
		p, ok := sys.iamCannedPolicyMap[name]
		return ok && p.IsAllowed(args)
	}

	// As policy is not available and OPA is not configured, return the owner value.
	return args.IsOwner
}

var defaultContextTimeout = 5 * time.Minute

// Similar to reloadUsers but updates users, policies maps from etcd server,
func reloadEtcdUsers(prefix string, usersMap map[string]auth.Credentials, policyMap map[string]string) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultContextTimeout)
	r, err := globalEtcdClient.Get(ctx, prefix, etcd.WithPrefix(), etcd.WithKeysOnly())
	defer cancel()
	if err != nil {
		return err
	}
	// No users are created yet.
	if r.Count == 0 {
		return nil
	}

	users := set.NewStringSet()
	for _, kv := range r.Kvs {
		// Extract user by stripping off the `prefix` value as suffix,
		// then strip off the remaining basename to obtain the prefix
		// value, usually in the following form.
		//
		//  key := "config/iam/users/newuser/identity.json"
		//  prefix := "config/iam/users/"
		//  v := trim(trim(key, prefix), base(key)) == "newuser"
		//
		user := path.Clean(strings.TrimSuffix(strings.TrimPrefix(string(kv.Key), prefix), path.Base(string(kv.Key))))
		if !users.Contains(user) {
			users.Add(user)
		}
	}

	// Reload config and policies for all users.
	for _, user := range users.ToSlice() {
		idFile := pathJoin(prefix, user, iamIdentityFile)
		pFile := pathJoin(prefix, user, iamPolicyFile)
		cdata, cerr := readConfigEtcd(ctx, globalEtcdClient, idFile)
		pdata, perr := readConfigEtcd(ctx, globalEtcdClient, pFile)
		if cerr != nil && cerr != errConfigNotFound {
			return cerr
		}
		if perr != nil && perr != errConfigNotFound {
			return perr
		}
		if cerr == errConfigNotFound && perr == errConfigNotFound {
			continue
		}
		if cerr == nil {
			var cred auth.Credentials
			if err = json.Unmarshal(cdata, &cred); err != nil {
				return err
			}
			cred.AccessKey = user
			if cred.IsExpired() {
				deleteConfigEtcd(ctx, globalEtcdClient, idFile)
				deleteConfigEtcd(ctx, globalEtcdClient, pFile)
				continue
			}
			usersMap[cred.AccessKey] = cred
		}
		if perr == nil {
			var policyName string
			if err = json.Unmarshal(pdata, &policyName); err != nil {
				return err
			}
			policyMap[user] = policyName
		}
	}
	return nil
}

func reloadEtcdPolicies(prefix string, cannedPolicyMap map[string]iampolicy.Policy) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultContextTimeout)
	r, err := globalEtcdClient.Get(ctx, prefix, etcd.WithPrefix(), etcd.WithKeysOnly())
	defer cancel()
	if err != nil {
		return err
	}
	// No users are created yet.
	if r.Count == 0 {
		return nil
	}

	policies := set.NewStringSet()
	for _, kv := range r.Kvs {
		// Extract policy by stripping off the `prefix` value as suffix,
		// then strip off the remaining basename to obtain the prefix
		// value, usually in the following form.
		//
		//  key := "config/iam/policies/newpolicy/identity.json"
		//  prefix := "config/iam/policies/"
		//  v := trim(trim(key, prefix), base(key)) == "newpolicy"
		//
		policyName := path.Clean(strings.TrimSuffix(strings.TrimPrefix(string(kv.Key), prefix), path.Base(string(kv.Key))))
		if !policies.Contains(policyName) {
			policies.Add(policyName)
		}
	}

	// Reload config and policies for all policys.
	for _, policyName := range policies.ToSlice() {
		pFile := pathJoin(prefix, policyName, iamPolicyFile)
		pdata, perr := readConfigEtcd(ctx, globalEtcdClient, pFile)
		if perr != nil {
			return perr
		}
		var p iampolicy.Policy
		if err = json.Unmarshal(pdata, &p); err != nil {
			return err
		}
		cannedPolicyMap[policyName] = p
	}
	return nil
}

func reloadPolicies(objectAPI ObjectLayer, prefix string, cannedPolicyMap map[string]iampolicy.Policy) error {
	marker := ""
	for {
		var lo ListObjectsInfo
		var err error
		lo, err = objectAPI.ListObjects(context.Background(), minioMetaBucket, prefix, marker, "/", 1000)
		if err != nil {
			return err
		}
		marker = lo.NextMarker
		for _, prefix := range lo.Prefixes {
			pFile := pathJoin(prefix, iamPolicyFile)
			pdata, perr := readConfig(context.Background(), objectAPI, pFile)
			if perr != nil {
				return perr
			}
			var p iampolicy.Policy
			if err = json.Unmarshal(pdata, &p); err != nil {
				return err
			}
			cannedPolicyMap[path.Base(prefix)] = p
		}
		if !lo.IsTruncated {
			break
		}
	}
	return nil

}

// reloadUsers reads an updates users, policies from object layer into user and policy maps.
func reloadUsers(objectAPI ObjectLayer, prefix string, usersMap map[string]auth.Credentials, policyMap map[string]string) error {
	marker := ""
	for {
		var lo ListObjectsInfo
		var err error
		lo, err = objectAPI.ListObjects(context.Background(), minioMetaBucket, prefix, marker, "/", 1000)
		if err != nil {
			return err
		}
		marker = lo.NextMarker
		for _, prefix := range lo.Prefixes {
			idFile := pathJoin(prefix, iamIdentityFile)
			pFile := pathJoin(prefix, iamPolicyFile)
			cdata, cerr := readConfig(context.Background(), objectAPI, idFile)
			pdata, perr := readConfig(context.Background(), objectAPI, pFile)
			if cerr != nil && cerr != errConfigNotFound {
				return cerr
			}
			if perr != nil && perr != errConfigNotFound {
				return perr
			}
			if cerr == errConfigNotFound && perr == errConfigNotFound {
				continue
			}
			if cerr == nil {
				var cred auth.Credentials
				if err = json.Unmarshal(cdata, &cred); err != nil {
					return err
				}
				cred.AccessKey = path.Base(prefix)
				if cred.IsExpired() {
					// Delete expired identity.
					objectAPI.DeleteObject(context.Background(), minioMetaBucket, idFile)
					// Delete expired identity policy.
					objectAPI.DeleteObject(context.Background(), minioMetaBucket, pFile)
					continue
				}
				usersMap[cred.AccessKey] = cred
			}
			if perr == nil {
				var policyName string
				if err = json.Unmarshal(pdata, &policyName); err != nil {
					return err
				}
				policyMap[path.Base(prefix)] = policyName
			}
		}
		if !lo.IsTruncated {
			break
		}
	}
	return nil
}

// Set default canned policies only if not already overridden by users.
func setDefaultCannedPolicies(policies map[string]iampolicy.Policy) {
	_, ok := policies["writeonly"]
	if !ok {
		policies["writeonly"] = iampolicy.WriteOnly
	}
	_, ok = policies["readonly"]
	if !ok {
		policies["readonly"] = iampolicy.ReadOnly
	}
	_, ok = policies["readwrite"]
	if !ok {
		policies["readwrite"] = iampolicy.ReadWrite
	}
}

// Refresh IAMSys.
func (sys *IAMSys) refresh(objAPI ObjectLayer) error {
	iamUsersMap := make(map[string]auth.Credentials)
	iamPolicyMap := make(map[string]string)
	iamCannedPolicyMap := make(map[string]iampolicy.Policy)

	if globalEtcdClient != nil {
		if err := reloadEtcdPolicies(iamConfigPoliciesPrefix, iamCannedPolicyMap); err != nil {
			return err
		}
		if err := reloadEtcdUsers(iamConfigUsersPrefix, iamUsersMap, iamPolicyMap); err != nil {
			return err
		}
		if err := reloadEtcdUsers(iamConfigSTSPrefix, iamUsersMap, iamPolicyMap); err != nil {
			return err
		}
	} else {
		if err := reloadPolicies(objAPI, iamConfigPoliciesPrefix, iamCannedPolicyMap); err != nil {
			return err
		}
		if err := reloadUsers(objAPI, iamConfigUsersPrefix, iamUsersMap, iamPolicyMap); err != nil {
			return err
		}
		if err := reloadUsers(objAPI, iamConfigSTSPrefix, iamUsersMap, iamPolicyMap); err != nil {
			return err
		}
	}

	// Sets default canned policies, if none set.
	setDefaultCannedPolicies(iamCannedPolicyMap)

	sys.Lock()
	defer sys.Unlock()

	sys.iamUsersMap = iamUsersMap
	sys.iamPolicyMap = iamPolicyMap
	sys.iamCannedPolicyMap = iamCannedPolicyMap

	return nil
}

// NewIAMSys - creates new config system object.
func NewIAMSys() *IAMSys {
	return &IAMSys{
		iamUsersMap:        make(map[string]auth.Credentials),
		iamPolicyMap:       make(map[string]string),
		iamCannedPolicyMap: make(map[string]iampolicy.Policy),
	}
}
