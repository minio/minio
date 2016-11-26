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
	"encoding/json"
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

// Represent a policy change
type policyChange struct {
	// isRemove is true if the policy change is to delete the
	// policy on a bucket.
	IsRemove bool

	// represents the new policy for the bucket
	BktPolicy *bucketPolicy
}

// Fetch bucket policy for a given bucket.
func (bp bucketPolicies) GetBucketPolicy(bucket string) *bucketPolicy {
	bp.rwMutex.RLock()
	defer bp.rwMutex.RUnlock()
	return bp.bucketPolicyConfigs[bucket]
}

// Set a new bucket policy for a bucket, this operation will overwrite
// any previous bucket policies for the bucket.
func (bp *bucketPolicies) SetBucketPolicy(bucket string, pCh policyChange) error {
	bp.rwMutex.Lock()
	defer bp.rwMutex.Unlock()

	if pCh.IsRemove {
		delete(bp.bucketPolicyConfigs, bucket)
	} else {
		if pCh.BktPolicy == nil {
			return errInvalidArgument
		}
		bp.bucketPolicyConfigs[bucket] = pCh.BktPolicy
	}
	return nil
}

// Loads all bucket policies from persistent layer.
func loadAllBucketPolicies(objAPI ObjectLayer) (policies map[string]*bucketPolicy, err error) {
	// List buckets to proceed loading all notification configuration.
	buckets, err := objAPI.ListBuckets()
	errorIf(err, "Unable to list buckets.")
	if err != nil {
		return nil, errorCause(err)
	}

	policies = make(map[string]*bucketPolicy)
	var pErrs []error
	// Loads bucket policy.
	for _, bucket := range buckets {
		policy, pErr := readBucketPolicy(bucket.Name, objAPI)
		if pErr != nil {
			// net.Dial fails for rpc client or any
			// other unexpected errors during net.Dial.
			if !isErrIgnored(pErr, errDiskNotFound) {
				if !isErrBucketPolicyNotFound(pErr) {
					pErrs = append(pErrs, pErr)
				}
			}
			// Continue to load other bucket policies if possible.
			continue
		}
		policies[bucket.Name] = policy
	}

	// Look for any errors occurred while reading bucket policies.
	for _, pErr := range pErrs {
		if pErr != nil {
			return policies, pErr
		}
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

	// Populate global bucket collection.
	globalBucketPolicies = &bucketPolicies{
		rwMutex:             &sync.RWMutex{},
		bucketPolicyConfigs: policies,
	}

	// Success.
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
	policyPath := pathJoin(bucketConfigPrefix, bucket, policyJSON)
	objInfo, err := objAPI.GetObjectInfo(minioMetaBucket, policyPath)
	if err != nil {
		if isErrObjectNotFound(err) || isErrIncompleteBody(err) {
			return nil, BucketPolicyNotFound{Bucket: bucket}
		}
		errorIf(err, "Unable to load policy for the bucket %s.", bucket)
		return nil, errorCause(err)
	}
	var buffer bytes.Buffer
	err = objAPI.GetObject(minioMetaBucket, policyPath, 0, objInfo.Size, &buffer)
	if err != nil {
		if isErrObjectNotFound(err) || isErrIncompleteBody(err) {
			return nil, BucketPolicyNotFound{Bucket: bucket}
		}
		errorIf(err, "Unable to load policy for the bucket %s.", bucket)
		return nil, errorCause(err)
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
	policyPath := pathJoin(bucketConfigPrefix, bucket, policyJSON)
	if err := objAPI.DeleteObject(minioMetaBucket, policyPath); err != nil {
		errorIf(err, "Unable to remove bucket-policy on bucket %s.", bucket)
		err = errorCause(err)
		if _, ok := err.(ObjectNotFound); ok {
			return BucketPolicyNotFound{Bucket: bucket}
		}
		return err
	}
	return nil
}

// writeBucketPolicy - save a bucket policy that is assumed to be validated.
func writeBucketPolicy(bucket string, objAPI ObjectLayer, bpy *bucketPolicy) error {
	buf, err := json.Marshal(bpy)
	if err != nil {
		errorIf(err, "Unable to marshal bucket policy '%v' to JSON", *bpy)
		return err
	}
	policyPath := pathJoin(bucketConfigPrefix, bucket, policyJSON)
	if _, err := objAPI.PutObject(minioMetaBucket, policyPath, int64(len(buf)), bytes.NewReader(buf), nil, ""); err != nil {
		errorIf(err, "Unable to set policy for the bucket %s", bucket)
		return errorCause(err)
	}
	return nil
}
