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

	"github.com/minio/minio/pkg/fs"
	"github.com/minio/minio/pkg/probe"
)

// getBucketsConfigPath - get buckets path.
func getBucketsConfigPath() (string, *probe.Error) {
	configPath, err := getConfigPath()
	if err != nil {
		return "", err.Trace()
	}
	return filepath.Join(configPath, "buckets"), nil
}

// createBucketsConfigPath - create buckets directory.
func createBucketsConfigPath() *probe.Error {
	bucketsConfigPath, err := getBucketsConfigPath()
	if err != nil {
		return err
	}
	if e := os.MkdirAll(bucketsConfigPath, 0700); e != nil {
		return probe.NewError(e)
	}
	return nil
}

// getBucketConfigPath - get bucket config path.
func getBucketConfigPath(bucket string) (string, *probe.Error) {
	bucketsConfigPath, err := getBucketsConfigPath()
	if err != nil {
		return "", err.Trace()
	}
	return filepath.Join(bucketsConfigPath, bucket), nil
}

// createBucketConfigPath - create bucket config directory.
func createBucketConfigPath(bucket string) *probe.Error {
	bucketConfigPath, err := getBucketConfigPath(bucket)
	if err != nil {
		return err
	}
	if e := os.MkdirAll(bucketConfigPath, 0700); e != nil {
		return probe.NewError(e)
	}
	return nil
}

// readBucketPolicy - read bucket policy.
func readBucketPolicy(bucket string) ([]byte, *probe.Error) {
	// Verify bucket is valid.
	if !fs.IsValidBucketName(bucket) {
		return nil, probe.NewError(fs.BucketNameInvalid{Bucket: bucket})
	}

	bucketConfigPath, err := getBucketConfigPath(bucket)
	if err != nil {
		return nil, err.Trace()
	}

	// Get policy file.
	bucketPolicyFile := filepath.Join(bucketConfigPath, "access-policy.json")
	if _, e := os.Stat(bucketPolicyFile); e != nil {
		if os.IsNotExist(e) {
			return nil, probe.NewError(fs.BucketPolicyNotFound{Bucket: bucket})
		}
		return nil, probe.NewError(e)
	}

	accessPolicyBytes, e := ioutil.ReadFile(bucketPolicyFile)
	if e != nil {
		return nil, probe.NewError(e)
	}
	return accessPolicyBytes, nil
}

// removeBucketPolicy - remove bucket policy.
func removeBucketPolicy(bucket string) *probe.Error {
	// Verify bucket is valid.
	if !fs.IsValidBucketName(bucket) {
		return probe.NewError(fs.BucketNameInvalid{Bucket: bucket})
	}

	bucketConfigPath, err := getBucketConfigPath(bucket)
	if err != nil {
		return err.Trace(bucket)
	}

	// Get policy file.
	bucketPolicyFile := filepath.Join(bucketConfigPath, "access-policy.json")
	if _, e := os.Stat(bucketPolicyFile); e != nil {
		if os.IsNotExist(e) {
			return probe.NewError(fs.BucketPolicyNotFound{Bucket: bucket})
		}
		return probe.NewError(e)
	}
	return nil
}

// writeBucketPolicy - save bucket policy.
func writeBucketPolicy(bucket string, accessPolicyBytes []byte) *probe.Error {
	// Verify if bucket path legal
	if !fs.IsValidBucketName(bucket) {
		return probe.NewError(fs.BucketNameInvalid{Bucket: bucket})
	}

	// Create bucket config path.
	if err := createBucketConfigPath(bucket); err != nil {
		return err.Trace()
	}

	bucketConfigPath, err := getBucketConfigPath(bucket)
	if err != nil {
		return err.Trace()
	}

	// Get policy file.
	bucketPolicyFile := filepath.Join(bucketConfigPath, "access-policy.json")
	if _, e := os.Stat(bucketPolicyFile); e != nil {
		if !os.IsNotExist(e) {
			return probe.NewError(e)
		}
	}

	// Write bucket policy.
	if e := ioutil.WriteFile(bucketPolicyFile, accessPolicyBytes, 0600); e != nil {
		return probe.NewError(e)
	}

	return nil
}
