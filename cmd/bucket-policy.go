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
	"io"
	"path"
)

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
	// Verify if bucket actually exists
	if err = isBucketExist(bucket, objAPI); err != nil {
		return nil, err
	}

	policyPath := pathJoin(bucketConfigPrefix, bucket, policyJSON)
	objInfo, err := objAPI.GetObjectInfo(minioMetaBucket, policyPath)
	err = errorCause(err)
	if err != nil {
		if _, ok := err.(ObjectNotFound); ok {
			return nil, BucketPolicyNotFound{Bucket: bucket}
		}
		errorIf(err, "Unable to load policy for the bucket %s.", bucket)
		return nil, err
	}
	var buffer bytes.Buffer
	err = objAPI.GetObject(minioMetaBucket, policyPath, 0, objInfo.Size, &buffer)
	err = errorCause(err)
	if err != nil {
		if _, ok := err.(ObjectNotFound); ok {
			return nil, BucketPolicyNotFound{Bucket: bucket}
		}
		errorIf(err, "Unable to load policy for the bucket %s.", bucket)
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
	// Verify if bucket actually exists
	if err := isBucketExist(bucket, objAPI); err != nil {
		return err
	}

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

// writeBucketPolicy - save all bucket policies.
func writeBucketPolicy(bucket string, objAPI ObjectLayer, reader io.Reader, size int64) error {
	// Verify if bucket actually exists
	if err := isBucketExist(bucket, objAPI); err != nil {
		return err
	}

	policyPath := pathJoin(bucketConfigPrefix, bucket, policyJSON)
	if _, err := objAPI.PutObject(minioMetaBucket, policyPath, size, reader, nil); err != nil {
		errorIf(err, "Unable to set policy for the bucket %s", bucket)
		return errorCause(err)
	}
	return nil
}
