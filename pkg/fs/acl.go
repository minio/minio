/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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

package fs

import (
	"os"
	"path/filepath"
)

// IsPrivateBucket - is private bucket
func (fs Filesystem) IsPrivateBucket(bucket string) bool {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	// get bucket path
	bucketDir := filepath.Join(fs.path, bucket)
	fi, err := os.Stat(bucketDir)
	if err != nil {
		return true
	}
	return permToACL(fi.Mode()).IsPrivate()
}

// IsPublicBucket - is public bucket
func (fs Filesystem) IsPublicBucket(bucket string) bool {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	// get bucket path
	bucketDir := filepath.Join(fs.path, bucket)
	fi, err := os.Stat(bucketDir)
	if err != nil {
		return true
	}
	return permToACL(fi.Mode()).IsPublicReadWrite()
}

// IsReadOnlyBucket - is read only bucket
func (fs Filesystem) IsReadOnlyBucket(bucket string) bool {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	// get bucket path
	bucketDir := filepath.Join(fs.path, bucket)
	fi, err := os.Stat(bucketDir)
	if err != nil {
		return true
	}
	return permToACL(fi.Mode()).IsPublicRead()
}
