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

package cmd

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

	// Recursively remove configDir/buckets/ - old bucket policy config location.
	// N B This is called only if all bucket policies were successfully migrated.
	return os.RemoveAll(oldBucketsConfigDir)
}

func migrateBucketPolicyConfig(objAPI ObjectLayer) error {
	// Get old bucket policy config directory.
	oldBucketsConfigDir, err := getOldBucketsConfigPath()
	fatalIf(err, "Unable to fetch buckets config path to migrate bucket policy")

	// Check if config directory holding bucket policy exists before
	// migration.
	_, err = os.Stat(oldBucketsConfigDir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
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
		var metadata map[string]string
		sha256sum := ""
		// Erasure code the policy config to all the disks.
		_, err = objAPI.PutObject(minioMetaBucket, newPolicyPath, int64(len(policyBytes)), bytes.NewReader(policyBytes), metadata, sha256sum)
		fatalIf(err, "Unable to write bucket policy during migration.", newPolicyPath)
		return nil
	}
	return filepath.Walk(oldBucketsConfigDir, migrateBucketPolicy)
}
