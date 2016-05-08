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
	"io/ioutil"
	"os"
	"path/filepath"
)

// getBucketsConfigPath - get buckets path.
func getBucketsConfigPath() (string, error) {
	configPath, err := getConfigPath()
	if err != nil {
		return "", err
	}
	return filepath.Join(configPath, "buckets"), nil
}

// getBucketConfigPath - get bucket config path.
func getBucketConfigPath(bucket string) (string, error) {
	bucketsConfigPath, err := getBucketsConfigPath()
	if err != nil {
		return "", err
	}
	return filepath.Join(bucketsConfigPath, bucket), nil
}

// createBucketConfigPath - create bucket config directory.
func createBucketConfigPath(bucket string) error {
	bucketConfigPath, err := getBucketConfigPath(bucket)
	if err != nil {
		return err
	}
	return os.MkdirAll(bucketConfigPath, 0700)
}

// readBucketPolicy - read bucket policy.
func readBucketPolicy(bucket string) ([]byte, error) {
	// Verify bucket is valid.
	if !IsValidBucketName(bucket) {
		return nil, BucketNameInvalid{Bucket: bucket}
	}

	bucketConfigPath, err := getBucketConfigPath(bucket)
	if err != nil {
		return nil, err
	}

	// Get policy file.
	bucketPolicyFile := filepath.Join(bucketConfigPath, "access-policy.json")
	if _, err = os.Stat(bucketPolicyFile); err != nil {
		if os.IsNotExist(err) {
			return nil, BucketPolicyNotFound{Bucket: bucket}
		}
		return nil, err
	}
	return ioutil.ReadFile(bucketPolicyFile)
}

// removeBucketPolicy - remove bucket policy.
func removeBucketPolicy(bucket string) error {
	// Verify bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}

	bucketConfigPath, err := getBucketConfigPath(bucket)
	if err != nil {
		return err
	}

	// Get policy file.
	bucketPolicyFile := filepath.Join(bucketConfigPath, "access-policy.json")
	if _, err = os.Stat(bucketPolicyFile); err != nil {
		if os.IsNotExist(err) {
			return BucketPolicyNotFound{Bucket: bucket}
		}
		return err
	}
	if err := os.Remove(bucketPolicyFile); err != nil {
		return err
	}
	return nil
}

// writeBucketPolicy - save bucket policy.
func writeBucketPolicy(bucket string, accessPolicyBytes []byte) error {
	// Verify if bucket path legal
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}

	// Create bucket config path.
	if err := createBucketConfigPath(bucket); err != nil {
		return err
	}

	bucketConfigPath, err := getBucketConfigPath(bucket)
	if err != nil {
		return err
	}

	// Get policy file.
	bucketPolicyFile := filepath.Join(bucketConfigPath, "access-policy.json")
	if _, err := os.Stat(bucketPolicyFile); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}

	// Write bucket policy.
	return ioutil.WriteFile(bucketPolicyFile, accessPolicyBytes, 0600)
}
