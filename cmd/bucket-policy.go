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
	"reflect"
	"sync"

	"github.com/minio/minio-go/pkg/policy"
	"github.com/minio/minio/pkg/errors"
	"github.com/minio/minio/pkg/hash"
)

const (
	// Static prefix to be used while constructing bucket ARN.
	// refer to S3 docs for more info.
	bucketARNPrefix = "arn:" + eventSource + ":::"

	// Bucket policy config name.
	bucketPolicyConfig = "policy.json"
)

// Global bucket policies list, policies are enforced on each bucket looking
// through the policies here.
type bucketPolicies struct {
	rwMutex *sync.RWMutex

	// Collection of 'bucket' policies.
	bucketPolicyConfigs map[string]policy.BucketAccessPolicy
}

// Fetch bucket policy for a given bucket.
func (bp bucketPolicies) GetBucketPolicy(bucket string) policy.BucketAccessPolicy {
	bp.rwMutex.RLock()
	defer bp.rwMutex.RUnlock()
	return bp.bucketPolicyConfigs[bucket]
}

// Set a new bucket policy for a bucket, this operation will overwrite
// any previous bucket policies for the bucket.
func (bp *bucketPolicies) SetBucketPolicy(bucket string, newpolicy policy.BucketAccessPolicy) error {
	bp.rwMutex.Lock()
	defer bp.rwMutex.Unlock()

	if reflect.DeepEqual(newpolicy, emptyBucketPolicy) {
		return errInvalidArgument
	}
	bp.bucketPolicyConfigs[bucket] = newpolicy

	return nil
}

// Delete bucket policy from struct for a given bucket.
func (bp *bucketPolicies) DeleteBucketPolicy(bucket string) error {
	bp.rwMutex.Lock()
	defer bp.rwMutex.Unlock()
	delete(bp.bucketPolicyConfigs, bucket)
	return nil
}

// Intialize all bucket policies.
func initBucketPolicies(objAPI ObjectLayer) (*bucketPolicies, error) {
	if objAPI == nil {
		return nil, errInvalidArgument
	}

	// List buckets to proceed loading all notification configuration.
	buckets, err := objAPI.ListBuckets()
	if err != nil {
		return nil, errors.Cause(err)
	}

	policies := make(map[string]policy.BucketAccessPolicy)
	// Loads bucket policy.
	for _, bucket := range buckets {
		bp, pErr := ReadBucketPolicy(bucket.Name, objAPI)
		if pErr != nil {
			// net.Dial fails for rpc client or any
			// other unexpected errors during net.Dial.
			if !errors.IsErrIgnored(pErr, errDiskNotFound) {
				if !isErrBucketPolicyNotFound(pErr) {
					return nil, errors.Cause(pErr)
				}
			}
			// Continue to load other bucket policies if possible.
			continue
		}
		policies[bucket.Name] = bp
	}

	// Return all bucket policies.
	return &bucketPolicies{
		rwMutex:             &sync.RWMutex{},
		bucketPolicyConfigs: policies,
	}, nil
}

// readBucketPolicyJSON - reads bucket policy for an input bucket, returns BucketPolicyNotFound
// if bucket policy is not found.
func readBucketPolicyJSON(bucket string, objAPI ObjectLayer) (bucketPolicyReader io.Reader, err error) {
	policyPath := pathJoin(bucketConfigPrefix, bucket, bucketPolicyConfig)

	var buffer bytes.Buffer
	err = objAPI.GetObject(minioMetaBucket, policyPath, 0, -1, &buffer, "")
	if err != nil {
		if isErrObjectNotFound(err) || isErrIncompleteBody(err) {
			return nil, PolicyNotFound{Bucket: bucket}
		}
		errorIf(err, "Unable to load policy for the bucket %s.", bucket)
		return nil, errors.Cause(err)
	}

	return &buffer, nil
}

// ReadBucketPolicy - reads bucket policy for an input bucket, returns BucketPolicyNotFound
// if bucket policy is not found. This function also parses the bucket policy into an object.
func ReadBucketPolicy(bucket string, objAPI ObjectLayer) (policy.BucketAccessPolicy, error) {
	// Read bucket policy JSON.
	bucketPolicyReader, err := readBucketPolicyJSON(bucket, objAPI)
	if err != nil {
		return emptyBucketPolicy, err
	}

	// Parse the saved policy.
	var bp policy.BucketAccessPolicy
	if err = parseBucketPolicy(bucketPolicyReader, &bp); err != nil {
		return emptyBucketPolicy, err

	}
	return bp, nil
}

// removeBucketPolicy - removes any previously written bucket policy. Returns BucketPolicyNotFound
// if no policies are found.
func removeBucketPolicy(bucket string, objAPI ObjectLayer) error {
	policyPath := pathJoin(bucketConfigPrefix, bucket, bucketPolicyConfig)
	err := objAPI.DeleteObject(minioMetaBucket, policyPath)
	if err != nil {
		err = errors.Cause(err)
		if _, ok := err.(ObjectNotFound); ok {
			return BucketPolicyNotFound{Bucket: bucket}
		}
		return err
	}
	return nil
}

// writeBucketPolicy - save a bucket policy that is assumed to be validated.
func writeBucketPolicy(bucket string, objAPI ObjectLayer, bpy policy.BucketAccessPolicy) error {
	buf, err := json.Marshal(bpy)
	if err != nil {
		errorIf(err, "Unable to marshal bucket policy '%#v' to JSON", bpy)
		return err
	}
	policyPath := pathJoin(bucketConfigPrefix, bucket, bucketPolicyConfig)
	hashReader, err := hash.NewReader(bytes.NewReader(buf), int64(len(buf)), "", getSHA256Hash(buf))
	if err != nil {
		errorIf(err, "Unable to set policy for the bucket %s", bucket)
		return errors.Cause(err)
	}

	if _, err = objAPI.PutObject(minioMetaBucket, policyPath, hashReader, nil); err != nil {
		errorIf(err, "Unable to set policy for the bucket %s", bucket)
		return errors.Cause(err)
	}
	return nil
}

// persistAndNotifyBucketPolicyChange - takes a policyChange argument,
// persists it to storage, and notify nodes in the cluster about the
// change. In-memory state is updated in response to the notification.
func persistAndNotifyBucketPolicyChange(bucket string, isRemove bool, bktPolicy policy.BucketAccessPolicy, objAPI ObjectLayer) error {
	if isRemove {
		err := removeBucketPolicy(bucket, objAPI)
		if err != nil {
			return err
		}
	} else {
		if reflect.DeepEqual(bktPolicy, emptyBucketPolicy) {
			return errInvalidArgument
		}
		if err := writeBucketPolicy(bucket, objAPI, bktPolicy); err != nil {
			return err
		}
	}

	// Notify all peers (including self) to update in-memory state
	S3PeersUpdateBucketPolicy(bucket)
	return nil
}
