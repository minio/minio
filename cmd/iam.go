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
	"sync"
	"time"

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
	iamUsersMap  map[string]auth.Credentials
	iamPolicyMap map[string]iampolicy.Policy
}

// Load - load iam.json
func (sys *IAMSys) Load(objAPI ObjectLayer) error {
	return sys.Init(objAPI)
}

// Init - initializes config system from iam.json
func (sys *IAMSys) Init(objAPI ObjectLayer) error {
	if objAPI == nil {
		return errInvalidArgument
	}

	if err := sys.refresh(objAPI); err != nil {
		return err
	}

	// Refresh IAMSys in background.
	go func() {
		ticker := time.NewTicker(globalRefreshIAMInterval)
		defer ticker.Stop()
		for {
			select {
			case <-globalServiceDoneCh:
				return
			case <-ticker.C:
				logger.LogIf(context.Background(), sys.refresh(objAPI))
			}
		}
	}()
	return nil

}

// SetPolicy - sets policy to given user name.  If policy is empty,
// existing policy is removed.
func (sys *IAMSys) SetPolicy(accessKey string, p iampolicy.Policy) error {
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	configFile := pathJoin(iamConfigUsersPrefix, accessKey, iamPolicyFile)
	data, err := json.Marshal(p)
	if err != nil {
		return err
	}

	sys.Lock()
	defer sys.Unlock()

	if err = saveConfig(context.Background(), objectAPI, configFile, data); err != nil {
		return err
	}

	if p.IsEmpty() {
		delete(sys.iamPolicyMap, accessKey)
	} else {
		sys.iamPolicyMap[accessKey] = p
	}

	return nil
}

// SaveTempPolicy - this is used for temporary credentials only.
func (sys *IAMSys) SaveTempPolicy(accessKey string, p iampolicy.Policy) error {
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	configFile := pathJoin(iamConfigSTSPrefix, accessKey, iamPolicyFile)
	data, err := json.Marshal(p)
	if err != nil {
		return err
	}

	sys.Lock()
	defer sys.Unlock()

	if err = saveConfig(context.Background(), objectAPI, configFile, data); err != nil {
		return err
	}

	if p.IsEmpty() {
		delete(sys.iamPolicyMap, accessKey)
	} else {
		sys.iamPolicyMap[accessKey] = p
	}

	return nil
}

// DeletePolicy - sets policy to given user name.  If policy is empty,
// existing policy is removed.
func (sys *IAMSys) DeletePolicy(accessKey string) error {
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	configFile := pathJoin(iamConfigUsersPrefix, accessKey, iamPolicyFile)

	sys.Lock()
	defer sys.Unlock()

	err := objectAPI.DeleteObject(context.Background(), minioMetaBucket, configFile)

	delete(sys.iamPolicyMap, accessKey)

	return err
}

// DeleteUser - set user credentials.
func (sys *IAMSys) DeleteUser(accessKey string) error {
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	sys.Lock()
	defer sys.Unlock()

	configFile := pathJoin(iamConfigUsersPrefix, accessKey, iamIdentityFile)
	err := objectAPI.DeleteObject(context.Background(), minioMetaBucket, configFile)

	delete(sys.iamUsersMap, accessKey)
	return err
}

// SetTempUser - set temporary user credentials, these credentials have an expiry.
func (sys *IAMSys) SetTempUser(accessKey string, cred auth.Credentials) error {
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	sys.Lock()
	defer sys.Unlock()

	configFile := pathJoin(iamConfigSTSPrefix, accessKey, iamIdentityFile)
	data, err := json.Marshal(cred)
	if err != nil {
		return err
	}

	if err = saveConfig(context.Background(), objectAPI, configFile, data); err != nil {
		return err
	}

	sys.iamUsersMap[accessKey] = cred
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

	sys.Lock()
	defer sys.Unlock()

	if err = saveConfig(context.Background(), objectAPI, configFile, data); err != nil {
		return err
	}

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

	// If policy is available for given user, check the policy.
	if p, found := sys.iamPolicyMap[args.AccountName]; found {
		// If opa is configured, use OPA in conjunction with IAM policies.
		if globalPolicyOPA != nil {
			return p.IsAllowed(args) && globalPolicyOPA.IsAllowed(args)
		}
		return p.IsAllowed(args)
	}

	// If no policies are set, let the policy arrive from OPA if any.
	if globalPolicyOPA != nil {
		return globalPolicyOPA.IsAllowed(args)
	}

	// As policy is not available and OPA is not configured, return the owner value.
	return args.IsOwner
}

// reloadUsers reads an updates users, policies from object layer into user and policy maps.
func reloadUsers(objectAPI ObjectLayer, prefix string, usersMap map[string]auth.Credentials, policyMap map[string]iampolicy.Policy) error {
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
				var p iampolicy.Policy
				if err = json.Unmarshal(pdata, &p); err != nil {
					return err
				}
				policyMap[path.Base(prefix)] = p
			}
		}
		if !lo.IsTruncated {
			break
		}
	}
	return nil
}

// Refresh IAMSys.
func (sys *IAMSys) refresh(objAPI ObjectLayer) error {
	iamUsersMap := make(map[string]auth.Credentials)
	iamPolicyMap := make(map[string]iampolicy.Policy)

	if err := reloadUsers(objAPI, iamConfigUsersPrefix, iamUsersMap, iamPolicyMap); err != nil {
		return err
	}

	if err := reloadUsers(objAPI, iamConfigSTSPrefix, iamUsersMap, iamPolicyMap); err != nil {
		return err
	}

	sys.Lock()
	defer sys.Unlock()

	sys.iamUsersMap = iamUsersMap
	sys.iamPolicyMap = iamPolicyMap

	return nil
}

// NewIAMSys - creates new config system object.
func NewIAMSys() *IAMSys {
	return &IAMSys{
		iamUsersMap:  make(map[string]auth.Credentials),
		iamPolicyMap: make(map[string]iampolicy.Policy),
	}
}
