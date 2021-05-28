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
	"math"
	"math/rand"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	iampolicy "github.com/minio/minio/pkg/iam/policy"
	corev1 "k8s.io/api/core/v1"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
)
const (
	ttlPrefix = "ttlExp_"
)
// IAMK8sStore implements IAMStorageAPI
type IAMK8sStore struct {
	// Protect interation with k8s configmap within single process, but optimistic concurrency
	// control is employed to ensure no transactions interfere with each other in a distributed
	// setup.
	sync.RWMutex

	configMapsClient  	typedcorev1.ConfigMapInterface
	namespace         	string
	configMapName     	string
	maxRetries 		  	int
	ttlPurgeFrequencyMs float64
}

func newIAMK8sStore() *IAMK8sStore {
	var k8sStore = &IAMK8sStore{
		configMapsClient:  globalK8sClient.CoreV1().ConfigMaps(globalK8sIamStoreConfig.Namespace),
		namespace:         globalK8sIamStoreConfig.Namespace,
		configMapName:     globalK8sIamStoreConfig.ConfigMapName,
		maxRetries: 10,
		ttlPurgeFrequencyMs: 120*1000,
	}
	k8sStore.ensureConfigMapExists()
	go k8sStore.watchExpiredItems()
	logger.Info("New K8S IAM store configured. Namespace: " + globalK8sIamStoreConfig.Namespace +
		", ConfigMap: " + globalK8sIamStoreConfig.ConfigMapName)
	return k8sStore
}

func (iamK8s *IAMK8sStore) ensureConfigMapExists() {
	var objectMeta = metav1.ObjectMeta{Name: iamK8s.configMapName, Namespace: iamK8s.namespace}
	var configMap = &corev1.ConfigMap{ObjectMeta: objectMeta}
	_, err := iamK8s.configMapsClient.Create(context.Background(), configMap, metav1.CreateOptions{})
	if err != nil && !errorsv1.IsAlreadyExists(err) {
		panic(err)
	}
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

// Should be called under config migration lock
func (iamK8s *IAMK8sStore) migrateBackendFormat(ctx context.Context) error {
	return nil
}

func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	return errorsv1.IsTooManyRequests(err) || errorsv1.IsServiceUnavailable(err) || errorsv1.IsServerTimeout(err)
}

func isConflict(err error) bool {
	if err == nil {
		return false
	}
	return errorsv1.IsConflict(err)
}

func backoff(attempt int, backoffTimeMs float64) {
	durationMs := math.Pow(2, float64(attempt)) * backoffTimeMs
	withJitter := rand.Intn(int(durationMs))
	time.Sleep(time.Duration(withJitter) * time.Millisecond)
}

func objPathToK8sValid(objPath string) string {
	objPath = strings.TrimSuffix(objPath, ".json")
	return strings.Replace(objPath, "/", ".", -1)
}

func objPathFromK8sValid(objPath string) string {
	return strings.Replace(objPath, ".", "/", -1)
}

func addTtlPrefix(objPath string) string {
	return ttlPrefix + objPath
}

func trimTtlPrefix(ttlKey string) string {
	return strings.TrimPrefix(ttlKey, ttlPrefix)
}

func hasTtlPrefix(ttlKey string) bool {
	return strings.HasPrefix(ttlKey, ttlPrefix)
}

func (iamK8s *IAMK8sStore) updateConfigMap(ctx context.Context, resourceVersion string, annotations map[string]string) error {
	configMapUpdated := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       iamK8s.namespace,
			Name:            iamK8s.configMapName,
			ResourceVersion: resourceVersion,
			Annotations:     annotations,
		},
	}
	_, err := iamK8s.configMapsClient.Update(ctx, configMapUpdated, metav1.UpdateOptions{})
	retries := 0
	for retries < iamK8s.maxRetries && isRetryable(err) {
		backoff(retries, 1000)
		_, err = iamK8s.configMapsClient.Update(ctx, configMapUpdated, metav1.UpdateOptions{})
		retries += 1
	}
	return err
}

func (iamK8s *IAMK8sStore) getConfigMap(ctx context.Context) (*corev1.ConfigMap, error) {
	configMap, err := iamK8s.configMapsClient.Get(ctx, iamK8s.configMapName, metav1.GetOptions{})
	retries := 0
	for retries < iamK8s.maxRetries && isRetryable(err) {
		backoff(retries, 1000)
		configMap, err = iamK8s.configMapsClient.Get(ctx, iamK8s.configMapName, metav1.GetOptions{})
		retries += 1
	}
	return configMap, err
}

func (iamK8s *IAMK8sStore) saveIamConfigNoConflictRetry(ctx context.Context, objPath string, data string, opts ...options) error {
	configMap, err := iamK8s.getConfigMap(ctx)
	if err != nil {
		return err
	}
	annotations := make(map[string]string)
	if configMap.Annotations != nil {
		annotations = configMap.Annotations
	}
	annotations[objPath] = data
	if len(opts) > 0 {
		annotations[addTtlPrefix(objPath)] = strconv.FormatInt(time.Now().Unix()+opts[0].ttl, 10)
	}
	return iamK8s.updateConfigMap(ctx, configMap.ResourceVersion, annotations)
}

func (iamK8s *IAMK8sStore) saveIAMConfig(ctx context.Context, item interface{}, objPath string, opts ...options) error {
	data, err := json.Marshal(item)
	if err != nil {
		return err
	}
	for attempts := 0; attempts <= iamK8s.maxRetries; attempts++ {
		err = iamK8s.saveIamConfigNoConflictRetry(ctx, objPathToK8sValid(objPath), string(data), opts...)
		if isConflict(err) {
			continue
		} else {
			break
		}
	}
	return err
}

func (iamK8s *IAMK8sStore) loadIAMConfig(ctx context.Context, item interface{}, objPath string) error {
	configMap, err := iamK8s.getConfigMap(ctx)
	if err != nil {
		return err
	}
	data := configMap.Annotations[objPathToK8sValid(objPath)]
	if data == "" {
		return errors.New("No entry for key: " + objPath)
	}
	return json.Unmarshal([]byte(data), item)
}

type iamConfigItem struct {
	objPath string
	data    string
}

type iamConfigListResult struct {
	iamConfigItems  []iamConfigItem
	resourceVersion string
	err             error
}

func (iamK8s *IAMK8sStore) listIAMConfigs(ctx context.Context, pathPrefix string, includeTtlItems bool) iamConfigListResult {
	configItems := []iamConfigItem{}
	configMap, err := iamK8s.getConfigMap(ctx)
	if err != nil {
		return iamConfigListResult{err: err}
	}

	for objPath, data := range configMap.Annotations {
		objPath = objPathFromK8sValid(objPath)
		trimmedObjPath := strings.TrimPrefix(objPath, pathPrefix)
		trimmedObjPath = strings.TrimSuffix(trimmedObjPath, SlashSeparator)
		ttlOk := (includeTtlItems && strings.HasPrefix(trimTtlPrefix(objPath), pathPrefix))
		if strings.HasPrefix(objPath, pathPrefix) || ttlOk {
			configItems = append(configItems, iamConfigItem{objPath: trimmedObjPath, data: data})
		}
	}

	return iamConfigListResult{configItems, configMap.ResourceVersion, nil}
}

func (iamK8s *IAMK8sStore) deleteIAMConfigNoConflictRetry(ctx context.Context, path string) error {
	configMap, err := iamK8s.getConfigMap(ctx)
	if err != nil {
		return err
	}
	annotations := configMap.Annotations
	if _, ok := annotations[path]; ok {
		return errConfigNotFound
	} else {
		delete(annotations, path)
		return iamK8s.updateConfigMap(ctx, configMap.ResourceVersion, annotations)
	}
}

func (iamK8s *IAMK8sStore) deleteIAMConfig(ctx context.Context, path string) (err error) {
	for attempts := 0; attempts <= iamK8s.maxRetries; attempts++ {
		err = iamK8s.deleteIAMConfigNoConflictRetry(ctx, objPathToK8sValid(path))
		if isConflict(err) {
			continue
		} else {
			break
		}
	}
	return err
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
	result := iamK8s.listIAMConfigs(ctx, iamConfigPoliciesPrefix, false)
	if result.err != nil {
		return result.err
	}
	for _, item := range result.iamConfigItems {
		policyName := path.Dir(item.objPath)
		var p iampolicy.Policy
		if err := json.Unmarshal([]byte(item.data), &p); err != nil {
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

	result := iamK8s.listIAMConfigs(ctx, basePrefix, false)
	if result.err != nil {
		return result.err
	}
	for _, item := range result.iamConfigItems {
		user := path.Dir(item.objPath)
		var u UserIdentity
		if err := json.Unmarshal([]byte(item.data), &u); err != nil {
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
	result := iamK8s.listIAMConfigs(ctx, iamConfigGroupsPrefix, false)
	if result.err != nil {
		return result.err
	}
	for _, item := range result.iamConfigItems {
		group := path.Dir(item.objPath)
		var g GroupInfo
		if err := json.Unmarshal([]byte(item.data), &g); err != nil {
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
	result := iamK8s.listIAMConfigs(ctx, basePath, false)
	if result.err != nil {
		return result.err
	}
	for _, item := range result.iamConfigItems {
		policyFile := item.objPath
		var p MappedPolicy
		if err := json.Unmarshal([]byte(item.data), &p); err != nil {
			return err
		}
		m[policyFile] = p
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

func filterExpiredItems(items []iamConfigItem) []iamConfigItem {
	keep := []string{}
	for _, item := range items {
		if hasTtlPrefix(item.objPath) {
			expTime, _ := strconv.ParseInt(item.data, 10, 64)
			if expTime > time.Now().Unix() {
				// has not expired
				keep = append(keep, item.objPath)
				keep = append(keep, trimTtlPrefix(item.objPath))
			}
		}
	}
	filtered := []iamConfigItem{}
	for _, item := range items {
		if contains(keep, item.objPath) {
			filtered = append(filtered, item)
		}
	}
	return filtered
}

func (iamK8s *IAMK8sStore) purgeExpiredItems(ctx context.Context) error {
	annotations := make(map[string]string)
	result := iamK8s.listIAMConfigs(ctx, "", true)
	if result.err != nil {
		return result.err
	}
	filteredConfigItems := filterExpiredItems(result.iamConfigItems)
	for _, configItem := range filteredConfigItems {
		annotations[objPathToK8sValid(configItem.objPath)] = configItem.data
	}
	return iamK8s.updateConfigMap(ctx, result.resourceVersion, annotations)
}

func (iamK8s *IAMK8sStore) watchExpiredItems() {
	ctx := context.Background()
	for {
		backoff(0, iamK8s.ttlPurgeFrequencyMs)
		iamK8s.lock()
		if err := iamK8s.purgeExpiredItems(ctx); err != nil {
			logger.Info("Removing expired items failed with err: ", err.Error())
		}
		iamK8s.unlock()
	}
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
