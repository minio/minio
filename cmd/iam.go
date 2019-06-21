/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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
	"path"
	"strings"
	"sync"
	"time"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/minio/minio-go/v6/pkg/set"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	iampolicy "github.com/minio/minio/pkg/iam/policy"
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

// LoadPolicy - reloads a specific canned policy from backend disks or etcd.
func (sys *IAMSys) LoadPolicy(objAPI ObjectLayer, policyName string) error {
	if objAPI == nil {
		return errInvalidArgument
	}

	sys.Lock()
	defer sys.Unlock()

	prefix := iamConfigPoliciesPrefix
	if globalEtcdClient == nil {
		return reloadPolicy(context.Background(), objAPI, prefix, policyName, sys.iamCannedPolicyMap)
	}

	// When etcd is set, we use watch APIs so this code is not needed.
	return nil
}

// LoadUser - reloads a specific user from backend disks or etcd.
func (sys *IAMSys) LoadUser(objAPI ObjectLayer, accessKey string, temp bool) error {
	if objAPI == nil {
		return errInvalidArgument
	}

	sys.Lock()
	defer sys.Unlock()

	prefix := iamConfigUsersPrefix
	if temp {
		prefix = iamConfigSTSPrefix
	}

	if globalEtcdClient == nil {
		return reloadUser(context.Background(), objAPI, prefix, accessKey, sys.iamUsersMap, sys.iamPolicyMap)
	}
	// When etcd is set, we use watch APIs so this code is not needed.
	return nil
}

// Load - loads iam subsystem
func (sys *IAMSys) Load(objAPI ObjectLayer) error {
	if globalEtcdClient != nil {
		return sys.refreshEtcd()
	}
	return sys.refresh(objAPI)
}

func (sys *IAMSys) reloadFromEvent(event *etcd.Event) {
	eventCreate := event.IsModify() || event.IsCreate()
	eventDelete := event.Type == etcd.EventTypeDelete
	usersPrefix := strings.HasPrefix(string(event.Kv.Key), iamConfigUsersPrefix)
	stsPrefix := strings.HasPrefix(string(event.Kv.Key), iamConfigSTSPrefix)
	policyPrefix := strings.HasPrefix(string(event.Kv.Key), iamConfigPoliciesPrefix)

	ctx, cancel := context.WithTimeout(context.Background(),
		defaultContextTimeout)
	defer cancel()

	switch {
	case eventCreate:
		switch {
		case usersPrefix:
			accessKey := path.Dir(strings.TrimPrefix(string(event.Kv.Key),
				iamConfigUsersPrefix))
			reloadEtcdUser(ctx, iamConfigUsersPrefix, accessKey,
				sys.iamUsersMap, sys.iamPolicyMap)
		case stsPrefix:
			accessKey := path.Dir(strings.TrimPrefix(string(event.Kv.Key),
				iamConfigSTSPrefix))
			reloadEtcdUser(ctx, iamConfigSTSPrefix, accessKey,
				sys.iamUsersMap, sys.iamPolicyMap)
		case policyPrefix:
			policyName := path.Dir(strings.TrimPrefix(string(event.Kv.Key),
				iamConfigPoliciesPrefix))
			reloadEtcdPolicy(ctx, iamConfigPoliciesPrefix,
				policyName, sys.iamCannedPolicyMap)
		}
	case eventDelete:
		switch {
		case usersPrefix:
			accessKey := path.Dir(strings.TrimPrefix(string(event.Kv.Key),
				iamConfigUsersPrefix))
			delete(sys.iamUsersMap, accessKey)
			delete(sys.iamPolicyMap, accessKey)
		case stsPrefix:
			accessKey := path.Dir(strings.TrimPrefix(string(event.Kv.Key),
				iamConfigSTSPrefix))
			delete(sys.iamUsersMap, accessKey)
			delete(sys.iamPolicyMap, accessKey)
		case policyPrefix:
			policyName := path.Dir(strings.TrimPrefix(string(event.Kv.Key),
				iamConfigPoliciesPrefix))
			delete(sys.iamCannedPolicyMap, policyName)
		}
	}
}

// Watch etcd entries for IAM
func (sys *IAMSys) watchIAMEtcd() {
	watchEtcd := func() {
		// Refresh IAMSys with etcd watch.
		for {
			watchCh := globalEtcdClient.Watch(context.Background(),
				iamConfigPrefix, etcd.WithPrefix(), etcd.WithKeysOnly())
			select {
			case <-GlobalServiceDoneCh:
				return
			case watchResp, ok := <-watchCh:
				if !ok {
					time.Sleep(1 * time.Second)
					continue
				}
				if err := watchResp.Err(); err != nil {
					logger.LogIf(context.Background(), err)
					// log and retry.
					time.Sleep(1 * time.Second)
					continue
				}
				for _, event := range watchResp.Events {
					sys.Lock()
					sys.reloadFromEvent(event)
					sys.Unlock()
				}
			}
		}
	}
	go watchEtcd()
}

func (sys *IAMSys) watchIAMDisk(objAPI ObjectLayer) {
	watchDisk := func() {
		ticker := time.NewTicker(globalRefreshIAMInterval)
		defer ticker.Stop()
		for {
			select {
			case <-GlobalServiceDoneCh:
				return
			case <-ticker.C:
				sys.refresh(objAPI)
			}
		}
	}
	// Refresh IAMSys in background.
	go watchDisk()
}

// Init - initializes config system from iam.json
func (sys *IAMSys) Init(objAPI ObjectLayer) error {
	if objAPI == nil {
		return errInvalidArgument
	}

	if globalEtcdClient != nil {
		defer sys.watchIAMEtcd()
	} else {
		defer sys.watchIAMDisk(objAPI)
	}

	doneCh := make(chan struct{})
	defer close(doneCh)

	// Initializing IAM needs a retry mechanism for
	// the following reasons:
	//  - Read quorum is lost just after the initialization
	//    of the object layer.
	for range newRetryTimerSimple(doneCh) {
		if globalEtcdClient != nil {
			return sys.refreshEtcd()
		}
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
		break
	}
	return nil
}

// DeletePolicy - deletes a canned policy from backend or etcd.
func (sys *IAMSys) DeletePolicy(policyName string) error {
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	if policyName == "" {
		return errInvalidArgument
	}

	var err error
	pFile := pathJoin(iamConfigPoliciesPrefix, policyName, iamPolicyFile)
	if globalEtcdClient != nil {
		err = deleteConfigEtcd(context.Background(), globalEtcdClient, pFile)
	} else {
		err = deleteConfig(context.Background(), objectAPI, pFile)
	}

	switch err.(type) {
	case ObjectNotFound:
		// Ignore error if policy is already deleted.
		err = nil
	}

	sys.Lock()
	defer sys.Unlock()

	delete(sys.iamCannedPolicyMap, policyName)
	return err
}

// ListPolicies - lists all canned policies.
func (sys *IAMSys) ListPolicies() (map[string][]byte, error) {
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

// SetPolicy - sets a new canned policy.
func (sys *IAMSys) SetPolicy(policyName string, p iampolicy.Policy) error {
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
		// It is okay to ignore errors when deleting policy.json for the user.
		deleteConfigEtcd(context.Background(), globalEtcdClient, pFile)
		err = deleteConfigEtcd(context.Background(), globalEtcdClient, iFile)
	} else {
		// It is okay to ignore errors when deleting policy.json for the user.
		_ = deleteConfig(context.Background(), objectAPI, pFile)
		err = deleteConfig(context.Background(), objectAPI, iFile)
	}

	//
	switch err.(type) {
	case ObjectNotFound:
		// ignore if user is already deleted.
		err = nil
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

// GetUserPolicy - returns canned policy name associated with a user.
func (sys *IAMSys) GetUserPolicy(accessKey string) (policyName string, err error) {
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return "", errServerNotInitialized
	}

	sys.RLock()
	defer sys.RUnlock()

	if _, ok := sys.iamUsersMap[accessKey]; !ok {
		return "", errNoSuchUser
	}

	if _, ok := sys.iamPolicyMap[accessKey]; !ok {
		return "", errNoSuchUser
	}

	return sys.iamPolicyMap[accessKey], nil
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

// SetUserSecretKey - sets user secret key
func (sys *IAMSys) SetUserSecretKey(accessKey string, secretKey string) error {
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	sys.Lock()
	defer sys.Unlock()

	cred, ok := sys.iamUsersMap[accessKey]
	if !ok {
		return errNoSuchUser
	}

	uinfo := madmin.UserInfo{
		SecretKey: secretKey,
		Status:    madmin.AccountStatus(cred.Status),
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
		SecretKey: secretKey,
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

// IsAllowedSTS is meant for STS based temporary credentials,
// which implements claims validation and verification other than
// applying policies.
func (sys *IAMSys) IsAllowedSTS(args iampolicy.Args) bool {
	pname, ok := args.Claims[iampolicy.PolicyName]
	if !ok {
		// When claims are set, it should have a "policy" field.
		return false
	}
	pnameStr, ok := pname.(string)
	if !ok {
		// When claims has "policy" field, it should be string.
		return false
	}

	sys.RLock()
	defer sys.RUnlock()

	// If policy is available for given user, check the policy.
	name, ok := sys.iamPolicyMap[args.AccountName]
	if !ok {
		// No policy available reject.
		return false
	}

	if pnameStr != name {
		// When claims has a policy, it should match the
		// policy of args.AccountName which server remembers.
		// if not reject such requests.
		return false
	}

	// Now check if we have a sessionPolicy.
	spolicy, ok := args.Claims[iampolicy.SessionPolicyName]
	if !ok {
		// Sub policy not set, this is most common since subPolicy
		// is optional, use the top level policy only.
		p, ok := sys.iamCannedPolicyMap[pnameStr]
		return ok && p.IsAllowed(args)
	}

	spolicyStr, ok := spolicy.(string)
	if !ok {
		// Sub policy if set, should be a string reject
		// malformed/malicious requests.
		return false
	}

	// Check if policy is parseable.
	subPolicy, err := iampolicy.ParseConfig(bytes.NewReader([]byte(spolicyStr)))
	if err != nil {
		// Log any error in input session policy config.
		logger.LogIf(context.Background(), err)
		return false
	}

	// Policy without Version string value reject it.
	if subPolicy.Version == "" {
		return false
	}

	// Sub policy is set and valid.
	p, ok := sys.iamCannedPolicyMap[pnameStr]
	return ok && p.IsAllowed(args) && subPolicy.IsAllowed(args)
}

// IsAllowed - checks given policy args is allowed to continue the Rest API.
func (sys *IAMSys) IsAllowed(args iampolicy.Args) bool {
	// If opa is configured, use OPA always.
	if globalPolicyOPA != nil {
		return globalPolicyOPA.IsAllowed(args)
	}

	// With claims set, we should do STS related checks and validation.
	if len(args.Claims) > 0 {
		return sys.IsAllowedSTS(args)
	}

	sys.RLock()
	defer sys.RUnlock()

	// If policy is available for given user, check the policy.
	if name, found := sys.iamPolicyMap[args.AccountName]; found {
		p, ok := sys.iamCannedPolicyMap[name]
		return ok && p.IsAllowed(args)
	}

	// As policy is not available and OPA is not configured, return the owner value.
	return args.IsOwner
}

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
		if !users.Contains(user) {
			users.Add(user)
		}
	}
	return users
}

// Similar to reloadUsers but updates users, policies maps from etcd server,
func reloadEtcdUsers(prefix string, usersMap map[string]auth.Credentials, policyMap map[string]string) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultContextTimeout)
	defer cancel()
	r, err := globalEtcdClient.Get(ctx, prefix, etcd.WithPrefix(), etcd.WithKeysOnly())
	if err != nil {
		return err
	}
	// No users are created yet.
	if r.Count == 0 {
		return nil
	}

	users := etcdKvsToSet(prefix, r.Kvs)

	// Reload config and policies for all users.
	for _, user := range users.ToSlice() {
		if err = reloadEtcdUser(ctx, prefix, user, usersMap, policyMap); err != nil {
			return err
		}
	}
	return nil
}

func reloadEtcdPolicy(ctx context.Context, prefix string, policyName string,
	cannedPolicyMap map[string]iampolicy.Policy) error {
	pFile := pathJoin(prefix, policyName, iamPolicyFile)
	pdata, err := readConfigEtcd(ctx, globalEtcdClient, pFile)
	if err != nil {
		return err
	}
	var p iampolicy.Policy
	if err = json.Unmarshal(pdata, &p); err != nil {
		return err
	}
	cannedPolicyMap[policyName] = p
	return nil
}

func reloadEtcdPolicies(prefix string, cannedPolicyMap map[string]iampolicy.Policy) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultContextTimeout)
	defer cancel()
	r, err := globalEtcdClient.Get(ctx, prefix, etcd.WithPrefix(), etcd.WithKeysOnly())
	if err != nil {
		return err
	}
	// No users are created yet.
	if r.Count == 0 {
		return nil
	}

	policies := etcdKvsToSet(prefix, r.Kvs)

	// Reload config and policies for all policys.
	for _, policyName := range policies.ToSlice() {
		if err = reloadEtcdPolicy(ctx, prefix, policyName, cannedPolicyMap); err != nil {
			return err
		}
	}
	return nil
}

func reloadPolicy(ctx context.Context, objectAPI ObjectLayer, prefix string,
	policyName string, cannedPolicyMap map[string]iampolicy.Policy) error {
	pFile := pathJoin(prefix, policyName, iamPolicyFile)
	pdata, err := readConfig(context.Background(), objectAPI, pFile)
	if err != nil {
		return err
	}
	var p iampolicy.Policy
	if err = json.Unmarshal(pdata, &p); err != nil {
		return err
	}
	cannedPolicyMap[policyName] = p
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
			if err = reloadPolicy(context.Background(), objectAPI, iamConfigPoliciesPrefix,
				path.Base(prefix), cannedPolicyMap); err != nil {
				return err
			}
		}
		if !lo.IsTruncated {
			break
		}
	}
	return nil

}

func reloadEtcdUser(ctx context.Context, prefix string, accessKey string,
	usersMap map[string]auth.Credentials, policyMap map[string]string) error {
	idFile := pathJoin(prefix, accessKey, iamIdentityFile)
	pFile := pathJoin(prefix, accessKey, iamPolicyFile)
	cdata, cerr := readConfigEtcd(ctx, globalEtcdClient, idFile)
	pdata, perr := readConfigEtcd(ctx, globalEtcdClient, pFile)
	if cerr != nil && cerr != errConfigNotFound {
		return cerr
	}
	if perr != nil && perr != errConfigNotFound {
		return perr
	}
	if cerr == errConfigNotFound && perr == errConfigNotFound {
		return nil
	}
	if cerr == nil {
		var cred auth.Credentials
		if err := json.Unmarshal(cdata, &cred); err != nil {
			return err
		}
		cred.AccessKey = path.Base(accessKey)
		if cred.IsExpired() {
			// Delete expired identity.
			deleteConfigEtcd(ctx, globalEtcdClient, idFile)
			// Delete expired identity policy.
			deleteConfigEtcd(ctx, globalEtcdClient, pFile)
			return nil
		}
		usersMap[cred.AccessKey] = cred
	}
	if perr == nil {
		var policyName string
		if err := json.Unmarshal(pdata, &policyName); err != nil {
			return err
		}
		policyMap[path.Base(accessKey)] = policyName
	}
	return nil
}

func reloadUser(ctx context.Context, objectAPI ObjectLayer, prefix string, accessKey string,
	usersMap map[string]auth.Credentials, policyMap map[string]string) error {
	idFile := pathJoin(prefix, accessKey, iamIdentityFile)
	pFile := pathJoin(prefix, accessKey, iamPolicyFile)
	cdata, cerr := readConfig(ctx, objectAPI, idFile)
	pdata, perr := readConfig(ctx, objectAPI, pFile)
	if cerr != nil && cerr != errConfigNotFound {
		return cerr
	}
	if perr != nil && perr != errConfigNotFound {
		return perr
	}
	if cerr == errConfigNotFound && perr == errConfigNotFound {
		return nil
	}
	if cerr == nil {
		var cred auth.Credentials
		if err := json.Unmarshal(cdata, &cred); err != nil {
			return err
		}
		cred.AccessKey = path.Base(accessKey)
		if cred.IsExpired() {
			// Delete expired identity.
			objectAPI.DeleteObject(context.Background(), minioMetaBucket, idFile)
			// Delete expired identity policy.
			objectAPI.DeleteObject(context.Background(), minioMetaBucket, pFile)
			return nil
		}
		usersMap[cred.AccessKey] = cred
	}
	if perr == nil {
		var policyName string
		if err := json.Unmarshal(pdata, &policyName); err != nil {
			return err
		}
		policyMap[path.Base(accessKey)] = policyName
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
			// Prefix is empty because prefix is already part of the List output.
			if err = reloadUser(context.Background(), objectAPI, "", prefix, usersMap, policyMap); err != nil {
				return err
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

func (sys *IAMSys) refreshEtcd() error {
	iamUsersMap := make(map[string]auth.Credentials)
	iamPolicyMap := make(map[string]string)
	iamCannedPolicyMap := make(map[string]iampolicy.Policy)

	if err := reloadEtcdPolicies(iamConfigPoliciesPrefix, iamCannedPolicyMap); err != nil {
		return err
	}
	if err := reloadEtcdUsers(iamConfigUsersPrefix, iamUsersMap, iamPolicyMap); err != nil {
		return err
	}
	if err := reloadEtcdUsers(iamConfigSTSPrefix, iamUsersMap, iamPolicyMap); err != nil {
		return err
	}

	// Sets default canned policies, if none are set.
	setDefaultCannedPolicies(iamCannedPolicyMap)

	sys.Lock()
	defer sys.Unlock()

	sys.iamUsersMap = iamUsersMap
	sys.iamPolicyMap = iamPolicyMap
	sys.iamCannedPolicyMap = iamCannedPolicyMap

	return nil
}

// Refresh IAMSys.
func (sys *IAMSys) refresh(objAPI ObjectLayer) error {
	iamUsersMap := make(map[string]auth.Credentials)
	iamPolicyMap := make(map[string]string)
	iamCannedPolicyMap := make(map[string]iampolicy.Policy)

	if err := reloadPolicies(objAPI, iamConfigPoliciesPrefix, iamCannedPolicyMap); err != nil {
		return err
	}
	if err := reloadUsers(objAPI, iamConfigUsersPrefix, iamUsersMap, iamPolicyMap); err != nil {
		return err
	}
	if err := reloadUsers(objAPI, iamConfigSTSPrefix, iamUsersMap, iamPolicyMap); err != nil {
		return err
	}

	// Sets default canned policies, if none are set.
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
