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

package main

import (
	"bytes"
	"path/filepath"
)

// getBucketsConfigPath - get buckets path.
func getOldBucketsConfigPath() (string, error) {
	configPath, err := getConfigPath()
	if err != nil {
		return "", err
	}
	return filepath.Join(configPath, "buckets"), nil
}

// readBucketPolicy - read bucket policy.
func readBucketPolicy(bucket string, objAPI ObjectLayer) ([]byte, error) {
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
	return buffer.Bytes(), nil
}

// removeBucketPolicy - remove bucket policy.
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

// writeBucketPolicy - save bucket policy.
func writeBucketPolicy(bucket string, objAPI ObjectLayer, accessPolicyBytes []byte) error {
	// Verify if bucket path legal
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}

	policyPath := pathJoin(bucketConfigPrefix, bucket, policyJSON)
	_, err := objAPI.PutObject(minioMetaBucket, policyPath, int64(len(accessPolicyBytes)), bytes.NewReader(accessPolicyBytes), nil)
	return err
}
