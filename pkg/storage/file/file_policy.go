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
	mstorage "github.com/minio-io/minio/pkg/storage"
)

// GetBucketPolicy - GET bucket policy
func (storage *Storage) GetBucketPolicy(bucket string) (mstorage.BucketPolicy, error) {
	storage.lock.Lock()
	defer storage.lock.Unlock()

	var p mstorage.BucketPolicy
	// verify bucket path legal
	if mstorage.IsValidBucket(bucket) == false {
		return mstorage.BucketPolicy{}, mstorage.BucketNameInvalid{Bucket: bucket}
	}

	// get bucket path
	bucketDir := path.Join(storage.root, bucket)
	// check if bucket exists
	if _, err := os.Stat(bucketDir); err != nil {
		return mstorage.BucketPolicy{}, mstorage.BucketNotFound{Bucket: bucket}
	}

	// get policy path
	bucketPolicy := path.Join(storage.root, bucket+"_policy.json")
	filestat, err := os.Stat(bucketPolicy)

	if os.IsNotExist(err) {
		return mstorage.BucketPolicy{}, mstorage.BucketPolicyNotFound{Bucket: bucket}
	}

	if filestat.IsDir() {
		return mstorage.BucketPolicy{}, mstorage.BackendCorrupted{Path: bucketPolicy}
	}

	file, err := os.OpenFile(bucketPolicy, os.O_RDONLY, 0666)
	defer file.Close()
	if err != nil {
		return mstorage.BucketPolicy{}, mstorage.EmbedError(bucket, "", err)
	}
	encoder := json.NewDecoder(file)
	err = encoder.Decode(&p)
	if err != nil {
		return mstorage.BucketPolicy{}, mstorage.EmbedError(bucket, "", err)
	}

	return p, nil

}

// CreateBucketPolicy - PUT bucket policy
func (storage *Storage) CreateBucketPolicy(bucket string, p mstorage.BucketPolicy) error {
	storage.lock.Lock()
	defer storage.lock.Unlock()

	// verify bucket path legal
	if mstorage.IsValidBucket(bucket) == false {
		return mstorage.BucketNameInvalid{Bucket: bucket}
	}

	// get bucket path
	bucketDir := path.Join(storage.root, bucket)
	// check if bucket exists
	if _, err := os.Stat(bucketDir); err != nil {
		return mstorage.BucketNotFound{
			Bucket: bucket,
		}
	}

	// get policy path
	bucketPolicy := path.Join(storage.root, bucket+"_policy.json")
	filestat, ret := os.Stat(bucketPolicy)
	if !os.IsNotExist(ret) {
		if filestat.IsDir() {
			return mstorage.BackendCorrupted{Path: bucketPolicy}
		}
	}

	file, err := os.OpenFile(bucketPolicy, os.O_WRONLY|os.O_CREATE, 0600)
	defer file.Close()
	if err != nil {
		return mstorage.EmbedError(bucket, "", err)
	}
	encoder := json.NewEncoder(file)
	err = encoder.Encode(p)
	if err != nil {
		return mstorage.EmbedError(bucket, "", err)
	}
	return nil
}
