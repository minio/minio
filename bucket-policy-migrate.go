/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

const policyJSON = "policy.json"

func getBucketFromPolicyPath(oldPolicyPath string) string {
	bucketPrefix, _ := filepath.Split(oldPolicyPath)
	_, bucketName := filepath.Split(strings.TrimSuffix(bucketPrefix, slashSeparator))
	return bucketName
}

func cleanupOldBucketPolicyConfigs() error {
	// Get old bucket policy config directory.
	oldBucketsConfigDir, err := getOldBucketsConfigPath()
	fatalIf(err, "Unable to fetch buckets config path to migrate bucket policy")

	// Check if config directory holding bucket policy exists before
	// starting to clean up.
	_, err = os.Stat(oldBucketsConfigDir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}

	// WalkFunc that deletes bucket policy files in the config directory
	// after successfully migrating all of them.
	cleanupOldBucketPolicy := func(policyPath string, fileInfo os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// Skip entries that aren't bucket policy files.
		if fileInfo.Name() != "access-policy.json" {
			return nil
		}
		// Delete the bucket policy file from config directory.
		return os.Remove(policyPath)
	}
	return filepath.Walk(oldBucketsConfigDir, cleanupOldBucketPolicy)
}

func migrateBucketPolicyConfig(objAPI ObjectLayer) error {
	// Get old bucket policy config directory.
	oldBucketsConfigDir, err := getOldBucketsConfigPath()
	fatalIf(err, "Unable to fetch buckets config path to migrate bucket policy")

	// Check if config directory holding bucket policy exists before
	// migration.
	_, err = os.Stat(oldBucketsConfigDir)
	if err != nil {
		return nil
	}
	// WalkFunc that migrates access-policy.json to
	// .minio.sys/buckets/bucketName/policy.json on all disks.
	migrateBucketPolicy := func(policyPath string, fileInfo os.FileInfo, err error) error {
		// policyFile - e.g /configDir/sample-bucket/access-policy.json
		if err != nil {
			return err
		}
		// Skip entries that aren't bucket policy files.
		if fileInfo.Name() != "access-policy.json" {
			return nil
		}
		// Get bucketName from old policy file path.
		bucketName := getBucketFromPolicyPath(policyPath)
		// Read bucket policy config from old location.
		policyBytes, err := ioutil.ReadFile(policyPath)
		fatalIf(err, "Unable to read bucket policy to migrate bucket policy", policyPath)
		newPolicyPath := retainSlash(bucketConfigPrefix) + retainSlash(bucketName) + policyJSON
		// Erasure code the policy config to all the disks.
		_, err = objAPI.PutObject(minioMetaBucket, newPolicyPath, int64(len(policyBytes)), bytes.NewReader(policyBytes), nil)
		fatalIf(err, "Unable to write bucket policy during migration.", newPolicyPath)
		return nil
	}
	return filepath.Walk(oldBucketsConfigDir, migrateBucketPolicy)
}
