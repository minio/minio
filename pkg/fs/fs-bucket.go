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
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/minio/minio-xl/pkg/probe"
	"github.com/minio/minio/pkg/disk"
)

/// Bucket Operations

// DeleteBucket - delete bucket
func (fs Filesystem) DeleteBucket(bucket string) *probe.Error {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	// verify bucket path legal
	if !IsValidBucket(bucket) {
		return probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	bucketDir := filepath.Join(fs.path, bucket)
	// check bucket exists
	if _, err := os.Stat(bucketDir); os.IsNotExist(err) {
		return probe.NewError(BucketNotFound{Bucket: bucket})
	}
	if err := RemoveAllDirs(bucketDir); err != nil {
		if err == ErrDirNotEmpty || strings.Contains(err.Error(), "directory not empty") {
			return probe.NewError(BucketNotEmpty{Bucket: bucket})
		}
		return probe.NewError(err)
	}
	if err := os.Remove(bucketDir); err != nil {
		if strings.Contains(err.Error(), "directory not empty") {
			return probe.NewError(BucketNotEmpty{Bucket: bucket})
		}
		return probe.NewError(err)
	}
	return nil
}

// ListBuckets - Get service
func (fs Filesystem) ListBuckets() ([]BucketMetadata, *probe.Error) {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	files, err := ioutil.ReadDir(fs.path)
	if err != nil {
		return []BucketMetadata{}, probe.NewError(err)
	}

	var metadataList []BucketMetadata
	for _, file := range files {
		if !file.IsDir() {
			// if files found ignore them
			continue
		}
		if file.IsDir() {
			// if directories found with odd names, skip them too
			if !IsValidBucket(file.Name()) {
				continue
			}
		}
		metadata := BucketMetadata{
			Name:    file.Name(),
			Created: file.ModTime(),
		}
		metadataList = append(metadataList, metadata)
	}
	return metadataList, nil
}

// MakeBucket - PUT Bucket
func (fs Filesystem) MakeBucket(bucket, acl string) *probe.Error {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	stfs, err := disk.Stat(fs.path)
	if err != nil {
		return probe.NewError(err)
	}

	// Remove 5% from total space for cumulative disk space used for journalling, inodes etc.
	availableDiskSpace := (float64(stfs.Free) / (float64(stfs.Total) - (0.05 * float64(stfs.Total)))) * 100
	if int64(availableDiskSpace) <= fs.minFreeDisk {
		return probe.NewError(RootPathFull{Path: fs.path})
	}

	// verify bucket path legal
	if !IsValidBucket(bucket) {
		return probe.NewError(BucketNameInvalid{Bucket: bucket})
	}

	// get bucket path
	bucketDir := filepath.Join(fs.path, bucket)

	// check if bucket exists
	if _, err = os.Stat(bucketDir); err == nil {
		return probe.NewError(BucketExists{
			Bucket: bucket,
		})
	}

	// make bucket
	err = os.Mkdir(bucketDir, aclToPerm(acl))
	if err != nil {
		return probe.NewError(err)
	}
	return nil
}

// GetBucketMetadata - get bucket metadata
func (fs Filesystem) GetBucketMetadata(bucket string) (BucketMetadata, *probe.Error) {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	if !IsValidBucket(bucket) {
		return BucketMetadata{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	// get bucket path
	bucketDir := filepath.Join(fs.path, bucket)
	bucketMetadata := BucketMetadata{}
	fi, err := os.Stat(bucketDir)
	// check if bucket exists
	if os.IsNotExist(err) {
		return BucketMetadata{}, probe.NewError(BucketNotFound{Bucket: bucket})
	}
	if err != nil {
		return BucketMetadata{}, probe.NewError(err)
	}

	bucketMetadata.Name = fi.Name()
	bucketMetadata.Created = fi.ModTime()
	bucketMetadata.ACL = permToACL(fi.Mode())
	return bucketMetadata, nil
}

// permToACL - convert perm to meaningful ACL
func permToACL(mode os.FileMode) BucketACL {
	switch mode.Perm() {
	case os.FileMode(0700):
		return BucketACL("private")
	case os.FileMode(0500):
		return BucketACL("public-read")
	case os.FileMode(0777):
		return BucketACL("public-read-write")
	default:
		return BucketACL("private")
	}
}

// aclToPerm - convert acl to filesystem mode
func aclToPerm(acl string) os.FileMode {
	switch acl {
	case "private":
		return os.FileMode(0700)
	case "public-read":
		return os.FileMode(0500)
	case "public-read-write":
		return os.FileMode(0777)
	default:
		return os.FileMode(0700)
	}
}

// SetBucketMetadata - set bucket metadata
func (fs Filesystem) SetBucketMetadata(bucket string, metadata map[string]string) *probe.Error {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	if !IsValidBucket(bucket) {
		return probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	acl := metadata["acl"]
	if !IsValidBucketACL(acl) {
		return probe.NewError(InvalidACL{ACL: acl})
	}
	// get bucket path
	bucketDir := filepath.Join(fs.path, bucket)
	err := os.Chmod(bucketDir, aclToPerm(acl))
	if err != nil {
		return probe.NewError(err)
	}
	return nil
}
