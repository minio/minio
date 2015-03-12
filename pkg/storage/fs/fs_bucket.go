/*
 * Mini Object Storage, (C) 2015 Minio, Inc.
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
	"path"
	"sort"
	"strings"

	"io/ioutil"
	"path/filepath"

	mstorage "github.com/minio-io/minio/pkg/storage"
)

/// Bucket Operations

// ListBuckets - Get service
func (storage *Storage) ListBuckets() ([]mstorage.BucketMetadata, error) {
	files, err := ioutil.ReadDir(storage.root)
	if err != nil {
		return []mstorage.BucketMetadata{}, mstorage.EmbedError("bucket", "", err)
	}

	var metadataList []mstorage.BucketMetadata
	for _, file := range files {
		// Skip policy files
		if strings.HasSuffix(file.Name(), "_policy.json") {
			continue
		}
		if !file.IsDir() {
			return []mstorage.BucketMetadata{}, mstorage.BackendCorrupted{Path: storage.root}
		}
		metadata := mstorage.BucketMetadata{
			Name:    file.Name(),
			Created: file.ModTime(), // TODO - provide real created time
		}
		metadataList = append(metadataList, metadata)
	}
	return metadataList, nil
}

// CreateBucket - PUT Bucket
func (storage *Storage) CreateBucket(bucket string) error {
	storage.lock.Lock()
	defer storage.lock.Unlock()

	// verify bucket path legal
	if mstorage.IsValidBucket(bucket) == false {
		return mstorage.BucketNameInvalid{Bucket: bucket}
	}

	// get bucket path
	bucketDir := path.Join(storage.root, bucket)

	// check if bucket exists
	if _, err := os.Stat(bucketDir); err == nil {
		return mstorage.BucketExists{
			Bucket: bucket,
		}
	}

	// make bucket
	err := os.Mkdir(bucketDir, 0700)
	if err != nil {
		return mstorage.EmbedError(bucket, "", err)
	}
	return nil
}

// ListObjects - GET bucket (list objects)
func (storage *Storage) ListObjects(bucket string, resources mstorage.BucketResourcesMetadata) ([]mstorage.ObjectMetadata, mstorage.BucketResourcesMetadata, error) {
	p := bucketDir{}
	p.files = make(map[string]os.FileInfo)

	if mstorage.IsValidBucket(bucket) == false {
		return []mstorage.ObjectMetadata{}, resources, mstorage.BucketNameInvalid{Bucket: bucket}
	}
	if resources.Prefix != "" && mstorage.IsValidObject(resources.Prefix) == false {
		return []mstorage.ObjectMetadata{}, resources, mstorage.ObjectNameInvalid{Bucket: bucket, Object: resources.Prefix}
	}

	rootPrefix := path.Join(storage.root, bucket)
	// check bucket exists
	if _, err := os.Stat(rootPrefix); os.IsNotExist(err) {
		return []mstorage.ObjectMetadata{}, resources, mstorage.BucketNotFound{Bucket: bucket}
	}

	p.root = rootPrefix
	err := filepath.Walk(rootPrefix, p.getAllFiles)
	if err != nil {
		return []mstorage.ObjectMetadata{}, resources, mstorage.EmbedError(bucket, "", err)
	}

	var metadataList []mstorage.ObjectMetadata
	var metadata mstorage.ObjectMetadata

	// Populate filtering mode
	resources.Mode = mstorage.GetMode(resources)

	for name, file := range p.files {
		if len(metadataList) >= resources.Maxkeys {
			resources.IsTruncated = true
			goto ret
		}
		metadata, resources, err = storage.filter(bucket, name, file, resources)
		if err != nil {
			return []mstorage.ObjectMetadata{}, resources, mstorage.EmbedError(bucket, "", err)
		}
		if metadata.Bucket != "" {
			metadataList = append(metadataList, metadata)
		}
	}

ret:
	sort.Sort(byObjectKey(metadataList))
	return metadataList, resources, nil
}
