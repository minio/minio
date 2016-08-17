/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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
	"errors"
	"io"
	"path"
	"sync"
)

// Variable represents bucket policies in memory.
var globalBucketPolicies *bucketPolicies

// Global bucket policies list, policies are enforced on each bucket looking
// through the policies here.
type bucketPolicies struct {
	rwMutex *sync.RWMutex

	// Collection of 'bucket' policies.
	bucketPolicyConfigs map[string]*bucketPolicy
}

// Fetch bucket policy for a given bucket.
func (bp bucketPolicies) GetBucketPolicy(bucket string) *bucketPolicy {
	bp.rwMutex.RLock()
	defer bp.rwMutex.RUnlock()
	return bp.bucketPolicyConfigs[bucket]
}

// Set a new bucket policy for a bucket, this operation will overwrite
// any previous bucketpolicies for the bucket.
func (bp *bucketPolicies) SetBucketPolicy(bucket string, policy *bucketPolicy) error {
	bp.rwMutex.Lock()
	defer bp.rwMutex.Unlock()
	if policy == nil {
		return errors.New("invalid argument")
	}
	bp.bucketPolicyConfigs[bucket] = policy
	return nil
}

// Remove bucket policy for a bucket, from in-memory map.
func (bp *bucketPolicies) RemoveBucketPolicy(bucket string) {
	bp.rwMutex.Lock()
	defer bp.rwMutex.Unlock()
	delete(bp.bucketPolicyConfigs, bucket)
}

// Loads all bucket policies from persistent layer.
func loadAllBucketPolicies(objAPI ObjectLayer) (policies map[string]*bucketPolicy, err error) {
	// List buckets to proceed loading all notification configuration.
	buckets, err := objAPI.ListBuckets()
	if err != nil {
		return nil, err
	}
	policies = make(map[string]*bucketPolicy)
	// Loads bucket policy.
	for _, bucket := range buckets {
		var policy *bucketPolicy
		policy, err = readBucketPolicy(bucket.Name, objAPI)
		if err != nil {
			switch err.(type) {
			case BucketPolicyNotFound:
				continue
			}
			return nil, err
		}
		policies[bucket.Name] = policy
	}

	// Success.
	return policies, nil

}

// Intialize all bucket policies.
func initBucketPolicies(objAPI ObjectLayer) error {
	if objAPI == nil {
		return errInvalidArgument
	}
	// Read all bucket policies.
	policies, err := loadAllBucketPolicies(objAPI)
	if err != nil {
		return err
	}

	globalBucketPolicies = &bucketPolicies{
		rwMutex:             &sync.RWMutex{},
		bucketPolicyConfigs: policies,
	}
	return nil
}

// getOldBucketsConfigPath - get old buckets config path. (Only used for migrating old bucket policies)
func getOldBucketsConfigPath() (string, error) {
	configPath, err := getConfigPath()
	if err != nil {
		return "", err
	}
	return path.Join(configPath, "buckets"), nil
}

// readBucketPolicyJSON - reads bucket policy for an input bucket, returns BucketPolicyNotFound
// if bucket policy is not found.
func readBucketPolicyJSON(bucket string, objAPI ObjectLayer) (bucketPolicyReader io.Reader, err error) {
	// Verify bucket is valid.
	if !IsValidBucketName(bucket) {
		return nil, BucketNameInvalid{Bucket: bucket}
	}
	policyPath := pathJoin(bucketConfigPrefix, bucket, policyJSON)
	objInfo, err := objAPI.GetObjectInfo(minioMetaBucket, policyPath)
	if err != nil {
		if _, ok := err.(ObjectNotFound); ok {
			return nil, BucketPolicyNotFound{Bucket: bucket}
		}
		return nil, err
	}
	var buffer bytes.Buffer
	err = objAPI.GetObject(minioMetaBucket, policyPath, 0, objInfo.Size, &buffer)
	if err != nil {
		if _, ok := err.(ObjectNotFound); ok {
			return nil, BucketPolicyNotFound{Bucket: bucket}
		}
		return nil, err
	}

	return &buffer, nil
}

// readBucketPolicy - reads bucket policy for an input bucket, returns BucketPolicyNotFound
// if bucket policy is not found. This function also parses the bucket policy into an object.
func readBucketPolicy(bucket string, objAPI ObjectLayer) (*bucketPolicy, error) {
	// Read bucket policy JSON.
	bucketPolicyReader, err := readBucketPolicyJSON(bucket, objAPI)
	if err != nil {
		return nil, err
	}

	// Parse the saved policy.
	var policy = &bucketPolicy{}
	err = parseBucketPolicy(bucketPolicyReader, policy)
	if err != nil {
		return nil, err

	}
	return policy, nil
}

// removeBucketPolicy - removes any previously written bucket policy. Returns BucketPolicyNotFound
// if no policies are found.
func removeBucketPolicy(bucket string, objAPI ObjectLayer) error {
	// Verify bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}

	policyPath := pathJoin(bucketConfigPrefix, bucket, policyJSON)
	if err := objAPI.DeleteObject(minioMetaBucket, policyPath); err != nil {
		if _, ok := err.(ObjectNotFound); ok {
			return BucketPolicyNotFound{Bucket: bucket}
		}
		return err
	}
	return nil
}

// writeBucketPolicy - save all bucket policies.
func writeBucketPolicy(bucket string, objAPI ObjectLayer, reader io.Reader, size int64) error {
	// Verify if bucket path legal
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}

	policyPath := pathJoin(bucketConfigPrefix, bucket, policyJSON)
	_, err := objAPI.PutObject(minioMetaBucket, policyPath, size, reader, nil)
	return err
}
