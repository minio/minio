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

// Variable represents bucket policies in memory.
var globalBucketPolicies *bucketPolicies

// Global bucket policies list, policies are enforced on each bucket looking
// through the policies here.
type bucketPolicies struct {
	rwMutex *sync.RWMutex

	// Collection of 'bucket' policies.
	bucketPolicyConfigs map[string]policy.BucketAccessPolicy
}

// Represent a policy change
type policyChange struct {
	// isRemove is true if the policy change is to delete the
	// policy on a bucket.
	IsRemove bool

	// represents the new policy for the bucket
	BktPolicy policy.BucketAccessPolicy
}

// Fetch bucket policy for a given bucket.
func (bp bucketPolicies) GetBucketPolicy(bucket string) policy.BucketAccessPolicy {
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
		if reflect.DeepEqual(pCh.BktPolicy, emptyBucketPolicy) {
			return errInvalidArgument
		}
		bp.bucketPolicyConfigs[bucket] = pCh.BktPolicy
	}
	return nil
}

// Loads all bucket policies from persistent layer.
func loadAllBucketPolicies(objAPI ObjectLayer) (policies map[string]policy.BucketAccessPolicy, err error) {
	// List buckets to proceed loading all notification configuration.
	buckets, err := objAPI.ListBuckets()
	if err != nil {
		errorIf(err, "Unable to list buckets.")
		return nil, errors.Cause(err)
	}

	policies = make(map[string]policy.BucketAccessPolicy)
	var pErrs []error
	// Loads bucket policy.
	for _, bucket := range buckets {
		bp, pErr := readBucketPolicy(bucket.Name, objAPI)
		if pErr != nil {
			// net.Dial fails for rpc client or any
			// other unexpected errors during net.Dial.
			if !errors.IsErrIgnored(pErr, errDiskNotFound) {
				if !isErrBucketPolicyNotFound(pErr) {
					pErrs = append(pErrs, pErr)
				}
			}
			// Continue to load other bucket policies if possible.
			continue
		}
		policies[bucket.Name] = bp
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

// readBucketPolicyJSON - reads bucket policy for an input bucket, returns BucketPolicyNotFound
// if bucket policy is not found.
func readBucketPolicyJSON(bucket string, objAPI ObjectLayer) (bucketPolicyReader io.Reader, err error) {
	policyPath := pathJoin(bucketConfigPrefix, bucket, bucketPolicyConfig)

	// Acquire a read lock on policy config before reading.
	objLock := globalNSMutex.NewNSLock(minioMetaBucket, policyPath)
	if err = objLock.GetRLock(globalOperationTimeout); err != nil {
		return nil, err
	}
	defer objLock.RUnlock()

	var buffer bytes.Buffer
	err = objAPI.GetObject(minioMetaBucket, policyPath, 0, -1, &buffer)
	if err != nil {
		if isErrObjectNotFound(err) || isErrIncompleteBody(err) {
			return nil, BucketPolicyNotFound{Bucket: bucket}
		}
		errorIf(err, "Unable to load policy for the bucket %s.", bucket)
		return nil, errors.Cause(err)
	}

	return &buffer, nil
}

// readBucketPolicy - reads bucket policy for an input bucket, returns BucketPolicyNotFound
// if bucket policy is not found. This function also parses the bucket policy into an object.
func readBucketPolicy(bucket string, objAPI ObjectLayer) (policy.BucketAccessPolicy, error) {
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
	// Acquire a write lock on policy config before modifying.
	objLock := globalNSMutex.NewNSLock(minioMetaBucket, policyPath)
	if err := objLock.GetLock(globalOperationTimeout); err != nil {
		return err
	}
	defer objLock.Unlock()
	err := objAPI.DeleteObject(minioMetaBucket, policyPath)
	if err != nil {
		errorIf(err, "Unable to remove bucket-policy on bucket %s.", bucket)
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
	// Acquire a write lock on policy config before modifying.
	objLock := globalNSMutex.NewNSLock(minioMetaBucket, policyPath)
	if err = objLock.GetLock(globalOperationTimeout); err != nil {
		return err
	}
	defer objLock.Unlock()

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

func parseAndPersistBucketPolicy(bucket string, policyBytes []byte, objAPI ObjectLayer) APIErrorCode {
	// Parse bucket policy.
	var bktPolicy policy.BucketAccessPolicy
	err := parseBucketPolicy(bytes.NewReader(policyBytes), &bktPolicy)
	if err != nil {
		errorIf(err, "Unable to parse bucket policy.")
		return ErrInvalidPolicyDocument
	}

	// Parse check bucket policy.
	if s3Error := checkBucketPolicyResources(bucket, bktPolicy); s3Error != ErrNone {
		return s3Error
	}

	// Acquire a write lock on bucket before modifying its configuration.
	bucketLock := globalNSMutex.NewNSLock(bucket, "")
	if bucketLock.GetLock(globalOperationTimeout) != nil {
		return ErrOperationTimedOut
	}
	// Release lock after notifying peers
	defer bucketLock.Unlock()

	// Save bucket policy.
	if err = persistAndNotifyBucketPolicyChange(bucket, policyChange{false, bktPolicy}, objAPI); err != nil {
		switch err.(type) {
		case BucketNameInvalid:
			return ErrInvalidBucketName
		case BucketNotFound:
			return ErrNoSuchBucket
		default:
			errorIf(err, "Unable to save bucket policy.")
			return ErrInternalError
		}
	}
	return ErrNone
}

// persistAndNotifyBucketPolicyChange - takes a policyChange argument,
// persists it to storage, and notify nodes in the cluster about the
// change. In-memory state is updated in response to the notification.
func persistAndNotifyBucketPolicyChange(bucket string, pCh policyChange, objAPI ObjectLayer) error {
	if pCh.IsRemove {
		err := removeBucketPolicy(bucket, objAPI)
		if err != nil {
			return err
		}
	} else {
		if reflect.DeepEqual(pCh.BktPolicy, emptyBucketPolicy) {
			return errInvalidArgument
		}
		if err := writeBucketPolicy(bucket, objAPI, pCh.BktPolicy); err != nil {
			return err
		}
	}

	// Notify all peers (including self) to update in-memory state
	S3PeersUpdateBucketPolicy(bucket, pCh)
	return nil
}
