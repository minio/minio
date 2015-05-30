/*
 * Mini Object Fs, (C) 2015 Minio, Inc.
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

package filesystem

import (
	"os"
	"path"
	"sort"

	"io/ioutil"
	"path/filepath"

	"github.com/minio/minio/pkg/iodine"
	"github.com/minio/minio/pkg/storage/drivers"
)

/// Bucket Operations

// ListBuckets - Get service
func (fs *fsDriver) ListBuckets() ([]drivers.BucketMetadata, error) {
	files, err := ioutil.ReadDir(fs.root)
	if err != nil {
		return []drivers.BucketMetadata{}, iodine.New(err, nil)
	}

	var metadataList []drivers.BucketMetadata
	for _, file := range files {
		if !file.IsDir() {
			return []drivers.BucketMetadata{}, iodine.New(drivers.BackendCorrupted{Path: fs.root}, nil)
		}
		metadata := drivers.BucketMetadata{
			Name:    file.Name(),
			Created: file.ModTime(), // TODO - provide real created time
		}
		metadataList = append(metadataList, metadata)
	}
	return metadataList, nil
}

// CreateBucket - PUT Bucket
func (fs *fsDriver) CreateBucket(bucket, acl string) error {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	// verify bucket path legal
	if drivers.IsValidBucket(bucket) == false {
		return iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, nil)
	}

	// get bucket path
	bucketDir := path.Join(fs.root, bucket)

	// check if bucket exists
	if _, err := os.Stat(bucketDir); err == nil {
		return iodine.New(drivers.BucketExists{
			Bucket: bucket,
		}, nil)
	}

	// make bucket
	err := os.Mkdir(bucketDir, 0700)
	if err != nil {
		return iodine.New(err, nil)
	}
	return nil
}

// GetBucketMetadata -
func (fs *fsDriver) GetBucketMetadata(bucket string) (drivers.BucketMetadata, error) {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	if !drivers.IsValidBucket(bucket) {
		return drivers.BucketMetadata{}, iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, nil)
	}
	// get bucket path
	bucketDir := path.Join(fs.root, bucket)
	bucketMetadata := drivers.BucketMetadata{}

	fi, err := os.Stat(bucketDir)
	// check if bucket exists
	if err == nil {
		bucketMetadata.Name = fi.Name()
		bucketMetadata.Created = fi.ModTime()
		// TODO convert os.FileMode to meaningful ACL's
		bucketMetadata.ACL = drivers.BucketACL("private")
		return bucketMetadata, nil
	}
	return drivers.BucketMetadata{}, iodine.New(err, nil)
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
	case "authenticated-read":
		return os.FileMode(0770)
	default:
		return os.FileMode(0700)
	}
}

// SetBucketMetadata -
func (fs *fsDriver) SetBucketMetadata(bucket, acl string) error {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	if !drivers.IsValidBucket(bucket) {
		return iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, nil)
	}
	if !drivers.IsValidBucketACL(acl) {
		return iodine.New(drivers.InvalidACL{ACL: acl}, nil)
	}
	// get bucket path
	bucketDir := path.Join(fs.root, bucket)
	err := os.Chmod(bucketDir, aclToPerm(acl))
	if err != nil {
		return iodine.New(err, nil)
	}
	return nil
}

// ListObjects - GET bucket (list objects)
func (fs *fsDriver) ListObjects(bucket string, resources drivers.BucketResourcesMetadata) ([]drivers.ObjectMetadata, drivers.BucketResourcesMetadata, error) {
	p := bucketDir{}
	p.files = make(map[string]os.FileInfo)

	if drivers.IsValidBucket(bucket) == false {
		return []drivers.ObjectMetadata{}, resources, iodine.New(drivers.BucketNameInvalid{Bucket: bucket}, nil)
	}
	if resources.Prefix != "" && drivers.IsValidObjectName(resources.Prefix) == false {
		return []drivers.ObjectMetadata{}, resources, iodine.New(drivers.ObjectNameInvalid{Bucket: bucket, Object: resources.Prefix}, nil)
	}

	rootPrefix := path.Join(fs.root, bucket)
	// check bucket exists
	if _, err := os.Stat(rootPrefix); os.IsNotExist(err) {
		return []drivers.ObjectMetadata{}, resources, iodine.New(drivers.BucketNotFound{Bucket: bucket}, nil)
	}

	p.root = rootPrefix
	err := filepath.Walk(rootPrefix, p.getAllFiles)
	if err != nil {
		return []drivers.ObjectMetadata{}, resources, iodine.New(err, nil)
	}

	var metadataList []drivers.ObjectMetadata
	var metadata drivers.ObjectMetadata

	// Populate filtering mode
	resources.Mode = drivers.GetMode(resources)

	var fileNames []string
	for name := range p.files {
		fileNames = append(fileNames, name)
	}
	sort.Strings(fileNames)
	for _, name := range fileNames {
		if len(metadataList) >= resources.Maxkeys {
			resources.IsTruncated = true
			if resources.IsTruncated && resources.IsDelimiterSet() {
				resources.NextMarker = metadataList[len(metadataList)-1].Key
			}
			break
		}
		if name > resources.Marker {
			metadata, resources, err = fs.filter(bucket, name, p.files[name], resources)
			if err != nil {
				return []drivers.ObjectMetadata{}, resources, iodine.New(err, nil)
			}
			if metadata.Bucket != "" {
				metadataList = append(metadataList, metadata)
			}
		}
	}
	sort.Sort(byObjectKey(metadataList))
	return metadataList, resources, nil
}
