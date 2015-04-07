/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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

package file

import (
	"os"
	"path"

	"encoding/json"

	"github.com/minio-io/objectdriver"
)

// GetBucketPolicy - GET bucket policy
func (file *fileDriver) GetBucketPolicy(bucket string) (drivers.BucketPolicy, error) {
	file.lock.Lock()
	defer file.lock.Unlock()

	var p drivers.BucketPolicy
	// verify bucket path legal
	if drivers.IsValidBucket(bucket) == false {
		return drivers.BucketPolicy{}, drivers.BucketNameInvalid{Bucket: bucket}
	}

	// get bucket path
	bucketDir := path.Join(file.root, bucket)
	// check if bucket exists
	if _, err := os.Stat(bucketDir); err != nil {
		return drivers.BucketPolicy{}, drivers.BucketNotFound{Bucket: bucket}
	}

	// get policy path
	bucketPolicy := path.Join(file.root, bucket+"_policy.json")
	filestat, err := os.Stat(bucketPolicy)

	if os.IsNotExist(err) {
		return drivers.BucketPolicy{}, drivers.BucketPolicyNotFound{Bucket: bucket}
	}

	if filestat.IsDir() {
		return drivers.BucketPolicy{}, drivers.BackendCorrupted{Path: bucketPolicy}
	}

	f, err := os.OpenFile(bucketPolicy, os.O_RDONLY, 0666)
	defer f.Close()
	if err != nil {
		return drivers.BucketPolicy{}, drivers.EmbedError(bucket, "", err)
	}
	encoder := json.NewDecoder(f)
	err = encoder.Decode(&p)
	if err != nil {
		return drivers.BucketPolicy{}, drivers.EmbedError(bucket, "", err)
	}

	return p, nil

}

// CreateBucketPolicy - PUT bucket policy
func (file *fileDriver) CreateBucketPolicy(bucket string, p drivers.BucketPolicy) error {
	file.lock.Lock()
	defer file.lock.Unlock()

	// verify bucket path legal
	if drivers.IsValidBucket(bucket) == false {
		return drivers.BucketNameInvalid{Bucket: bucket}
	}

	// get bucket path
	bucketDir := path.Join(file.root, bucket)
	// check if bucket exists
	if _, err := os.Stat(bucketDir); err != nil {
		return drivers.BucketNotFound{
			Bucket: bucket,
		}
	}

	// get policy path
	bucketPolicy := path.Join(file.root, bucket+"_policy.json")
	filestat, ret := os.Stat(bucketPolicy)
	if !os.IsNotExist(ret) {
		if filestat.IsDir() {
			return drivers.BackendCorrupted{Path: bucketPolicy}
		}
	}

	f, err := os.OpenFile(bucketPolicy, os.O_WRONLY|os.O_CREATE, 0600)
	defer f.Close()
	if err != nil {
		return drivers.EmbedError(bucket, "", err)
	}
	encoder := json.NewEncoder(f)
	err = encoder.Encode(p)
	if err != nil {
		return drivers.EmbedError(bucket, "", err)
	}
	return nil
}
