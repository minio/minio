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
	"net/http"
	"path"
	"sync"

	miniogopolicy "github.com/minio/minio-go/pkg/policy"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/handlers"
	"github.com/minio/minio/pkg/policy"
)

// PolicySys - policy system.
type PolicySys struct {
	sync.RWMutex
	bucketPolicyMap map[string]policy.Policy
}

// Set - sets policy to given bucket name.  If policy is empty, existing policy is removed.
func (sys *PolicySys) Set(bucketName string, policy policy.Policy) {
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
	sys.RLock()
	defer sys.RUnlock()

	// If policy is available for given bucket, check the policy.
	if p, found := sys.bucketPolicyMap[args.BucketName]; found {
		return p.IsAllowed(args)
	}

	// As policy is not available for given bucket name, returns IsOwner i.e.
	// operation is allowed only for owner.
	return args.IsOwner
}

// Init - initializes policy system from policy.json of all buckets.
func (sys *PolicySys) Init(objAPI ObjectLayer) error {
	if objAPI == nil {
		return errInvalidArgument
	}

	buckets, err := objAPI.ListBuckets(context.Background())
	if err != nil {
		return err
	}

	for _, bucket := range buckets {
		config, err := GetPolicyConfig(objAPI, bucket.Name)
		if err != nil {
			if _, ok := err.(BucketPolicyNotFound); !ok {
				return err
			}
		} else {
			// This part is specifically written to handle migration
			// when the Version string is empty, this was allowed
			// in all previous minio releases but we need to migrate
			// those policies by properly setting the Version string
			// from now on.
			if config.Version == "" {
				logger.Info("Found in-consistent bucket policies, Migrating them for Bucket: (%s)", bucket.Name)
				config.Version = policy.DefaultVersion

				if err = savePolicyConfig(objAPI, bucket.Name, config); err != nil {
					return err
				}
			}

			sys.Set(bucket.Name, *config)
		}
	}

	return nil
}

// NewPolicySys - creates new policy system.
func NewPolicySys() *PolicySys {
	return &PolicySys{
		bucketPolicyMap: make(map[string]policy.Policy),
	}
}

func getConditionValues(request *http.Request, locationConstraint string) map[string][]string {
	args := make(map[string][]string)

	for key, values := range request.Header {
		if existingValues, found := args[key]; found {
			args[key] = append(existingValues, values...)
		} else {
			args[key] = values
		}
	}

	for key, values := range request.URL.Query() {
		if existingValues, found := args[key]; found {
			args[key] = append(existingValues, values...)
		} else {
			args[key] = values
		}
	}

	args["SourceIp"] = []string{handlers.GetSourceIP(request)}

	if locationConstraint != "" {
		args["LocationConstraint"] = []string{locationConstraint}
	}

	return args
}

// GetPolicyConfig - get policy config for given bucket name.
func GetPolicyConfig(objAPI ObjectLayer, bucketName string) (*policy.Policy, error) {
	// Construct path to policy.json for the given bucket.
	configFile := path.Join(bucketConfigPrefix, bucketName, bucketPolicyConfig)

	reader, err := readConfig(context.Background(), objAPI, configFile)
	if err != nil {
		if err == errConfigNotFound {
			err = BucketPolicyNotFound{Bucket: bucketName}
		}

		return nil, err
	}

	return policy.ParseConfig(reader, bucketName)
}

func savePolicyConfig(objAPI ObjectLayer, bucketName string, bucketPolicy *policy.Policy) error {
	data, err := json.Marshal(bucketPolicy)
	if err != nil {
		return err
	}

	// Construct path to policy.json for the given bucket.
	configFile := path.Join(bucketConfigPrefix, bucketName, bucketPolicyConfig)

	return saveConfig(objAPI, configFile, data)
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
	if err = json.Unmarshal(data, &bucketPolicy); err != nil {
		// This should not happen because data is valid to JSON data.
		return nil, err
	}

	return &bucketPolicy, nil
}
