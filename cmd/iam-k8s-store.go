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
	errorsv1 "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	iampolicy "github.com/minio/minio/pkg/iam/policy"
	corev1 "k8s.io/api/core/v1"
	//metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	//"k8s.io/client-go/tools/clientcmd"
	//"k8s.io/client-go/util/homedir"
)

// IAMK8sStore implements IAMStorageAPI
type IAMK8sStore struct {
	// Protect interation with k8s configmap within single process, but optimistic concurrency
	// control is employed to ensure no transactions interfere with each other in a distributed
	// setup.
	sync.RWMutex

	configMapsClient typedcorev1.ConfigMapInterface
	namespace string
	configMapName string
	maxUpdateAttempts int
}

func newIAMK8sStore() *IAMK8sStore {
	// Currently config vars are harcoded and we use an out-of-cluster client configuration.
	// Actual impl should get config vars from env and use in-cluster client configuration.
	home := homedir.HomeDir()
	kubeconfig := filepath.Join(home, ".kube", "config")
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		panic(err.Error())
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	namespace := "default"
	configMapName := "minioK8sConfigStore"
	var k8sStore = &IAMK8sStore{
		configMapsClient: clientset.CoreV1().ConfigMaps(namespace),
		namespace: namespace,
		configMapName: configMapName,
		maxUpdateAttempts: 10,
	}
	k8sStore.createConfigMapIfNotExists()
	return k8sStore
}

func (iamK8s *IAMK8sStore) createConfigMapIfNotExists() {
	var objectMeta = metav1.ObjectMeta{Name: iamK8s.configMapName, Namespace: iamK8s.namespace}
	var configMap = &corev1.ConfigMap{ObjectMeta: objectMeta}
	_, _ = iamK8s.configMapsClient.Create(context.Background(), configMap, metav1.CreateOptions{})
}

func (iamK8s *IAMK8sStore) lock() {
	iamK8s.Lock()
}

func (iamK8s *IAMK8sStore) unlock() {
	iamK8s.Unlock()
}

func (iamK8s *IAMK8sStore) rlock() {
	iamK8s.RLock()
}

func (iamK8s *IAMK8sStore) runlock() {
	iamK8s.RUnlock()
}

func (iamK8s *IAMK8sStore) migrateUsersConfigToV1(ctx context.Context) error {
	panic("Not implemented")
}

func (iamK8s *IAMK8sStore) migrateToV1(ctx context.Context) error {
	panic("Not implemented")
}

// Should be called under config migration lock
func (iamK8s *IAMK8sStore) migrateBackendFormat(ctx context.Context) error {
	panic("Not implemented")
}

func isRetryable(err error) bool {
	return errorsv1.IsTooManyRequests(err) || errorsv1.IsServiceUnavailable(err) || errorsv1.IsServerTimeout(err)
}

func (iamK8s *IAMK8sStore) saveIAMConfig(ctx context.Context, item interface{}, objPath string, opts ...options) error {
	data, err := json.Marshal(item)
	if err != nil {
		return err
	}
	attempts := 0
	for attempts < iamK8s.maxUpdateAttempts {
		configMap, err := iamK8s.configMapsClient.Get(ctx, iamK8s.configMapName, metav1.GetOptions{})
		if err != nil {
			if isRetryable(err) {
				attempts += 1
				continue
			} else {
				return err
			}
		}
		annotations := configMap.Annotations
		annotations[objPath] = string(data)
		if len(opts) > 0 {
			annotations["ttlExpiry:"+objPath] = strconv.FormatInt(time.Now().Unix() + opts[0].ttl, 10)
		}
		configMapUpdated := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: iamK8s.namespace,
				Name: iamK8s.configMapName,
				ResourceVersion: configMap.ResourceVersion,
				Annotations: annotations,
			},
		}
		_, err = iamK8s.configMapsClient.Update(ctx, configMapUpdated, metav1.UpdateOptions{})
		if err != nil {
			if (errorsv1.IsConflict(err)) {
				attempts += 1
				continue
			} else if (isRetryable(err)) {
				attempts += 1
				time.Sleep(100 * time.Millisecond)
				continue
			} else {
				return err
			}
		} else {
			return nil
		}
	}
	return nil
}

func (iamK8s *IAMK8sStore) loadIAMConfig(ctx context.Context, item interface{}, objPath string) error {
	configMap, err := iamK8s.configMapsClient.Get(ctx, iamK8s.configMapName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	jsonData := configMap.Annotations[objPath]
	if jsonData == "" {
		return errors.New("No entry for key: " + objPath)
	}
	return json.Unmarshal([]byte(jsonData), item)
}

type iamConfigItem struct {
	objPath string
	jsonData string
}

func (iamK8s *IAMK8sStore) listIAMConfigs(ctx context.Context, pathPrefix string) (<-chan iamConfigItem, error) {
	ch := make(chan iamConfigItem)
	configMap, err := iamK8s.configMapsClient.Get(ctx, iamK8s.configMapName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	go func() {
		defer close(ch)

		for objPath, jsonData := range configMap.Annotations {
			trimmedObjPath := strings.TrimPrefix(objPath, pathPrefix)
			trimmedObjPath = strings.TrimSuffix(trimmedObjPath, SlashSeparator)
			if strings.HasPrefix(objPath, pathPrefix) {
				select {
				case ch <- iamConfigItem{objPath: trimmedObjPath, jsonData: jsonData}:
				case <- ctx.Done():
					return
				}
			}
		}
	}()

	return ch, nil
}

func (iamK8s *IAMK8sStore) deleteIAMConfig(ctx context.Context, path string) error {
	configMap, err := iamK8s.configMapsClient.Get(ctx, iamK8s.configMapName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	annotations := configMap.Annotations
	if _, ok := annotations[path]; ok {
		return errConfigNotFound
	} else {
		delete(annotations, path)
		configMapUpdated := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: iamK8s.namespace,
				Name: iamK8s.configMapName,
				ResourceVersion: configMap.ResourceVersion,
				Annotations: annotations,
			},
		}
		_, err = iamK8s.configMapsClient.Update(ctx, configMapUpdated, metav1.UpdateOptions{})
		return err
	}
}

func (iamK8s *IAMK8sStore) loadPolicyDoc(ctx context.Context, policy string, m map[string]iampolicy.Policy) error {
	var p iampolicy.Policy
	err := iamK8s.loadIAMConfig(ctx, &p, getPolicyDocPath(policy))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchPolicy
		}
		return err
	}
	m[policy] = p
	return nil
}

func (iamK8s *IAMK8sStore) loadPolicyDocs(ctx context.Context, m map[string]iampolicy.Policy) error {
	ch, err := iamK8s.listIAMConfigs(ctx, iamConfigPoliciesPrefix)
	if err != nil {
		return err
	}
	for item := range ch {
		policyName := path.Dir(item.objPath)
		var p iampolicy.Policy
		if err := json.Unmarshal([]byte(item.jsonData), &p); err != nil {
			return err
		}
		m[policyName] = p
	}
	return nil
}

func (iamK8s *IAMK8sStore) deleteCredentialsIfExpired(ctx context.Context, userIdentity UserIdentity, user string, userType IAMUserType) bool {
	if userIdentity.Credentials.IsExpired() {
		// Delete expired identity - ignoring errors here.
		iamK8s.deleteIAMConfig(ctx, getUserIdentityPath(user, userType))
		iamK8s.deleteIAMConfig(ctx, getMappedPolicyPath(user, userType, false))
		return true
	}
	return false
}

func (iamK8s *IAMK8sStore) loadUser(ctx context.Context, user string, userType IAMUserType, m map[string]auth.Credentials) error {
	var u UserIdentity
	err := iamK8s.loadIAMConfig(ctx, &u, getUserIdentityPath(user, userType))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchUser
		}
		return err
	}
	if iamK8s.deleteCredentialsIfExpired(ctx, u, user, userType) {
		return nil
	}
	if u.Credentials.AccessKey == "" {
		u.Credentials.AccessKey = user
	}
	m[user] = u.Credentials
	return nil
}

func (iamK8s *IAMK8sStore) loadUsers(ctx context.Context, userType IAMUserType, m map[string]auth.Credentials) error {
	var basePrefix string
	switch userType {
	case srvAccUser:
		basePrefix = iamConfigServiceAccountsPrefix
	case stsUser:
		basePrefix = iamConfigSTSPrefix
	default:
		basePrefix = iamConfigUsersPrefix
	}

	ch, err := iamK8s.listIAMConfigs(ctx, basePrefix)
	if err != nil {
		return err
	}
	for item := range ch {
		user := path.Dir(item.objPath)
		var u UserIdentity
		if err := json.Unmarshal([]byte(item.jsonData), &u); err != nil {
			return err
		}
		if iamK8s.deleteCredentialsIfExpired(ctx, u, user, userType) {
			continue
		}
		if u.Credentials.AccessKey == "" {
			u.Credentials.AccessKey = user
		}
		m[user] = u.Credentials
	}
	return nil
}

func (iamK8s *IAMK8sStore) loadGroup(ctx context.Context, group string, m map[string]GroupInfo) error {
	var g GroupInfo
	err := iamK8s.loadIAMConfig(ctx, &g, getGroupInfoPath(group))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchGroup
		}
		return err
	}
	m[group] = g
	return nil
}

func (iamK8s *IAMK8sStore) loadGroups(ctx context.Context, m map[string]GroupInfo) error {
	ch, err := iamK8s.listIAMConfigs(ctx, iamConfigGroupsPrefix)
	if err != nil {
		return err
	}
	for item := range ch {
		group := path.Dir(item.objPath)
		var g GroupInfo
		if err := json.Unmarshal([]byte(item.jsonData), &g); err != nil {
			return err
		}
		m[group] = g
	}
	return nil
}

func (iamK8s *IAMK8sStore) loadMappedPolicy(ctx context.Context, name string, userType IAMUserType, isGroup bool,
	m map[string]MappedPolicy) error {
	var p MappedPolicy
	err := iamK8s.loadIAMConfig(ctx, &p, getMappedPolicyPath(name, userType, isGroup))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchPolicy
		}
		return err
	}
	m[name] = p
	return nil
}

func (iamK8s *IAMK8sStore) loadMappedPolicies(ctx context.Context, userType IAMUserType, isGroup bool, m map[string]MappedPolicy) error {
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
	ch, err := iamK8s.listIAMConfigs(ctx, basePath)
	if err != nil {
		return err
	}
	for item := range ch {
		policyFile := item.objPath
		var p MappedPolicy
		if err := json.Unmarshal([]byte(item.jsonData), &p); err != nil {
			return err
		}
		userOrGroupName := strings.TrimSuffix(policyFile, ".json")
		m[userOrGroupName] = p
	}
	return nil
}

// Refresh IAMSys. If an object layer is passed in use that, otherwise load from global.
func (iamK8s *IAMK8sStore) loadAll(ctx context.Context, sys *IAMSys) error {
	return sys.Load(ctx, iamK8s)
}

func (iamK8s *IAMK8sStore) savePolicyDoc(ctx context.Context, policyName string, p iampolicy.Policy) error {
	return iamK8s.saveIAMConfig(ctx, &p, getPolicyDocPath(policyName))
}

func (iamK8s *IAMK8sStore) saveMappedPolicy(ctx context.Context, name string, userType IAMUserType, isGroup bool, mp MappedPolicy, opts ...options) error {
	return iamK8s.saveIAMConfig(ctx, mp, getMappedPolicyPath(name, userType, isGroup), opts...)
}

func (iamK8s *IAMK8sStore) saveUserIdentity(ctx context.Context, name string, userType IAMUserType, u UserIdentity, opts ...options) error {
	return iamK8s.saveIAMConfig(ctx, u, getUserIdentityPath(name, userType), opts...)
}

func (iamK8s *IAMK8sStore) saveGroupInfo(ctx context.Context, name string, gi GroupInfo) error {
	return iamK8s.saveIAMConfig(ctx, gi, getGroupInfoPath(name))
}

func (iamK8s *IAMK8sStore) deletePolicyDoc(ctx context.Context, name string) error {
	err := iamK8s.deleteIAMConfig(ctx, getPolicyDocPath(name))
	if err == errConfigNotFound {
		err = errNoSuchPolicy
	}
	return err
}

func (iamK8s *IAMK8sStore) deleteMappedPolicy(ctx context.Context, name string, userType IAMUserType, isGroup bool) error {
	err := iamK8s.deleteIAMConfig(ctx, getMappedPolicyPath(name, userType, isGroup))
	if err == errConfigNotFound {
		err = errNoSuchPolicy
	}
	return err
}

func (iamK8s *IAMK8sStore) deleteUserIdentity(ctx context.Context, name string, userType IAMUserType) error {
	err := iamK8s.deleteIAMConfig(ctx, getUserIdentityPath(name, userType))
	if err == errConfigNotFound {
		err = errNoSuchUser
	}
	return err
}

func (iamK8s *IAMK8sStore) deleteGroupInfo(ctx context.Context, name string) error {
	err := iamK8s.deleteIAMConfig(ctx, getGroupInfoPath(name))
	if err == errConfigNotFound {
		err = errNoSuchGroup
	}
	return err
}

func (iamK8s *IAMK8sStore) watch(ctx context.Context, sys *IAMSys) {
	// Refresh IAMSys.
	for {
		time.Sleep(globalRefreshIAMInterval)
		if err := iamK8s.loadAll(ctx, sys); err != nil {
			logger.LogIf(ctx, err)
		}
	}
}
