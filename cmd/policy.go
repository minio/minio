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
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
	miniogopolicy "github.com/minio/minio-go/v6/pkg/policy"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/bucket/policy"
	"github.com/minio/minio/pkg/handlers"
)

// PolicySys - policy subsystem.
type PolicySys struct {
	sync.RWMutex
	bucketPolicyMap map[string]policy.Policy
}

// Set - sets policy to given bucket name.  If policy is empty, existing policy is removed.
func (sys *PolicySys) Set(bucketName string, policy policy.Policy) {
	if globalIsGateway {
		// Set policy is a non-op under gateway mode.
		return
	}

	sys.Lock()
	defer sys.Unlock()

	if policy.IsEmpty() {
		delete(sys.bucketPolicyMap, bucketName)
	} else {
		sys.bucketPolicyMap[bucketName] = policy
	}
}

// Remove - removes policy for given bucket name.
func (sys *PolicySys) Remove(bucketName string) {
	sys.Lock()
	defer sys.Unlock()

	delete(sys.bucketPolicyMap, bucketName)
}

// IsAllowed - checks given policy args is allowed to continue the Rest API.
func (sys *PolicySys) IsAllowed(args policy.Args) bool {
	if globalIsGateway {
		// When gateway is enabled, no cached value
		// is used to validate bucket policies.
		objAPI := newObjectLayerFn()
		if objAPI != nil {
			config, err := objAPI.GetBucketPolicy(GlobalContext, args.BucketName)
			if err == nil {
				return config.IsAllowed(args)
			}
		}
	} else {
		sys.RLock()
		defer sys.RUnlock()

		// If policy is available for given bucket, check the policy.
		if p, found := sys.bucketPolicyMap[args.BucketName]; found {
			return p.IsAllowed(args)
		}
	}

	// As policy is not available for given bucket name, returns IsOwner i.e.
	// operation is allowed only for owner.
	return args.IsOwner
}

// Loads policies for all buckets into PolicySys.
func (sys *PolicySys) load(buckets []BucketInfo, objAPI ObjectLayer) error {
	for _, bucket := range buckets {
		config, err := objAPI.GetBucketPolicy(GlobalContext, bucket.Name)
		if err != nil {
			if _, ok := err.(BucketPolicyNotFound); ok {
				sys.Remove(bucket.Name)
			}
			continue
		}
		// This part is specifically written to handle migration
		// when the Version string is empty, this was allowed
		// in all previous minio releases but we need to migrate
		// those policies by properly setting the Version string
		// from now on.
		if config.Version == "" {
			logger.Info("Found in-consistent bucket policies, Migrating them for Bucket: (%s)", bucket.Name)
			config.Version = policy.DefaultVersion

			if err = savePolicyConfig(GlobalContext, objAPI, bucket.Name, config); err != nil {
				logger.LogIf(GlobalContext, err)
				return err
			}
		}
		sys.Set(bucket.Name, *config)
	}
	return nil
}

// Init - initializes policy system from policy.json of all buckets.
func (sys *PolicySys) Init(buckets []BucketInfo, objAPI ObjectLayer) error {
	if objAPI == nil {
		return errInvalidArgument
	}

	// In gateway mode, we don't need to load the policies
	// from the backend.
	if globalIsGateway {
		return nil
	}

	// Load PolicySys once during boot.
	return sys.load(buckets, objAPI)
}

// NewPolicySys - creates new policy system.
func NewPolicySys() *PolicySys {
	return &PolicySys{
		bucketPolicyMap: make(map[string]policy.Policy),
	}
}

func getConditionValues(r *http.Request, lc string, username string, claims map[string]interface{}) map[string][]string {
	currTime := UTCNow()

	principalType := "Anonymous"
	if username != "" {
		principalType = "User"
	}

	args := map[string][]string{
		"CurrentTime":     {currTime.Format(time.RFC3339)},
		"EpochTime":       {strconv.FormatInt(currTime.Unix(), 10)},
		"SecureTransport": {strconv.FormatBool(r.TLS != nil)},
		"SourceIp":        {handlers.GetSourceIP(r)},
		"UserAgent":       {r.UserAgent()},
		"Referer":         {r.Referer()},
		"principaltype":   {principalType},
		"userid":          {username},
		"username":        {username},
	}

	if lc != "" {
		args["LocationConstraint"] = []string{lc}
	}

	cloneHeader := r.Header.Clone()

	for _, objLock := range []string{
		xhttp.AmzObjectLockMode,
		xhttp.AmzObjectLockLegalHold,
		xhttp.AmzObjectLockRetainUntilDate,
	} {
		if values, ok := cloneHeader[objLock]; ok {
			args[strings.TrimPrefix(objLock, "X-Amz-")] = values
		}
		cloneHeader.Del(objLock)
	}

	for key, values := range cloneHeader {
		if existingValues, found := args[key]; found {
			args[key] = append(existingValues, values...)
		} else {
			args[key] = values
		}
	}

	var cloneURLValues = url.Values{}
	for k, v := range r.URL.Query() {
		cloneURLValues[k] = v
	}

	for _, objLock := range []string{
		xhttp.AmzObjectLockMode,
		xhttp.AmzObjectLockLegalHold,
		xhttp.AmzObjectLockRetainUntilDate,
	} {
		if values, ok := cloneURLValues[objLock]; ok {
			args[strings.TrimPrefix(objLock, "X-Amz-")] = values
		}
		cloneURLValues.Del(objLock)
	}

	for key, values := range cloneURLValues {
		if existingValues, found := args[key]; found {
			args[key] = append(existingValues, values...)
		} else {
			args[key] = values
		}
	}

	// JWT specific values
	for k, v := range claims {
		vStr, ok := v.(string)
		if ok {
			args[k] = []string{vStr}
		}
	}

	return args
}

// getPolicyConfig - get policy config for given bucket name.
func getPolicyConfig(objAPI ObjectLayer, bucketName string) (*policy.Policy, error) {
	// Construct path to policy.json for the given bucket.
	configFile := path.Join(bucketConfigPrefix, bucketName, bucketPolicyConfig)

	configData, err := readConfig(GlobalContext, objAPI, configFile)
	if err != nil {
		if err == errConfigNotFound {
			err = BucketPolicyNotFound{Bucket: bucketName}
		}

		return nil, err
	}

	return policy.ParseConfig(bytes.NewReader(configData), bucketName)
}

func savePolicyConfig(ctx context.Context, objAPI ObjectLayer, bucketName string, bucketPolicy *policy.Policy) error {
	data, err := json.Marshal(bucketPolicy)
	if err != nil {
		return err
	}

	// Construct path to policy.json for the given bucket.
	configFile := path.Join(bucketConfigPrefix, bucketName, bucketPolicyConfig)

	return saveConfig(ctx, objAPI, configFile, data)
}

func removePolicyConfig(ctx context.Context, objAPI ObjectLayer, bucketName string) error {
	// Construct path to policy.json for the given bucket.
	configFile := path.Join(bucketConfigPrefix, bucketName, bucketPolicyConfig)

	if err := objAPI.DeleteObject(ctx, minioMetaBucket, configFile); err != nil {
		if _, ok := err.(ObjectNotFound); ok {
			return BucketPolicyNotFound{Bucket: bucketName}
		}

		return err
	}

	return nil
}

// PolicyToBucketAccessPolicy - converts policy.Policy to minio-go/policy.BucketAccessPolicy.
func PolicyToBucketAccessPolicy(bucketPolicy *policy.Policy) (*miniogopolicy.BucketAccessPolicy, error) {
	// Return empty BucketAccessPolicy for empty bucket policy.
	if bucketPolicy == nil {
		return &miniogopolicy.BucketAccessPolicy{Version: policy.DefaultVersion}, nil
	}

	data, err := json.Marshal(bucketPolicy)
	if err != nil {
		// This should not happen because bucketPolicy is valid to convert to JSON data.
		return nil, err
	}

	var policyInfo miniogopolicy.BucketAccessPolicy
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	if err = json.Unmarshal(data, &policyInfo); err != nil {
		// This should not happen because data is valid to JSON data.
		return nil, err
	}

	return &policyInfo, nil
}

// BucketAccessPolicyToPolicy - converts minio-go/policy.BucketAccessPolicy to policy.Policy.
func BucketAccessPolicyToPolicy(policyInfo *miniogopolicy.BucketAccessPolicy) (*policy.Policy, error) {
	data, err := json.Marshal(policyInfo)
	if err != nil {
		// This should not happen because policyInfo is valid to convert to JSON data.
		return nil, err
	}

	var bucketPolicy policy.Policy
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	if err = json.Unmarshal(data, &bucketPolicy); err != nil {
		// This should not happen because data is valid to JSON data.
		return nil, err
	}

	return &bucketPolicy, nil
}
