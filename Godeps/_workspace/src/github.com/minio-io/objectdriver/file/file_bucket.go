/*
 * Minimalist Object File, (C) 2015 Minio, Inc.
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
	"sort"
	"strings"

	"github.com/minio-io/objectdriver"

	"io/ioutil"
	"path/filepath"
)

/// Bucket Operations

// ListBuckets - Get service
func (file *fileDriver) ListBuckets() ([]drivers.BucketMetadata, error) {
	files, err := ioutil.ReadDir(file.root)
	if err != nil {
		return []drivers.BucketMetadata{}, drivers.EmbedError("bucket", "", err)
	}

	var metadataList []drivers.BucketMetadata
	for _, fileName := range files {
		// Skip policy files
		if strings.HasSuffix(fileName.Name(), "_policy.json") {
			continue
		}
		if !fileName.IsDir() {
			return []drivers.BucketMetadata{}, drivers.BackendCorrupted{Path: file.root}
		}
		metadata := drivers.BucketMetadata{
			Name:    fileName.Name(),
			Created: fileName.ModTime(), // TODO - provide real created time
		}
		metadataList = append(metadataList, metadata)
	}
	return metadataList, nil
}

// CreateBucket - PUT Bucket
func (file *fileDriver) CreateBucket(bucket string) error {
	file.lock.Lock()
	defer file.lock.Unlock()

	// verify bucket path legal
	if drivers.IsValidBucket(bucket) == false {
		return drivers.BucketNameInvalid{Bucket: bucket}
	}

	// get bucket path
	bucketDir := path.Join(file.root, bucket)

	// check if bucket exists
	if _, err := os.Stat(bucketDir); err == nil {
		return drivers.BucketExists{
			Bucket: bucket,
		}
	}

	// make bucket
	err := os.Mkdir(bucketDir, 0700)
	if err != nil {
		return drivers.EmbedError(bucket, "", err)
	}
	return nil
}

// ListObjects - GET bucket (list objects)
func (file *fileDriver) ListObjects(bucket string, resources drivers.BucketResourcesMetadata) ([]drivers.ObjectMetadata, drivers.BucketResourcesMetadata, error) {
	p := bucketDir{}
	p.files = make(map[string]os.FileInfo)

	if drivers.IsValidBucket(bucket) == false {
		return []drivers.ObjectMetadata{}, resources, drivers.BucketNameInvalid{Bucket: bucket}
	}
	if resources.Prefix != "" && drivers.IsValidObject(resources.Prefix) == false {
		return []drivers.ObjectMetadata{}, resources, drivers.ObjectNameInvalid{Bucket: bucket, Object: resources.Prefix}
	}

	rootPrefix := path.Join(file.root, bucket)
	// check bucket exists
	if _, err := os.Stat(rootPrefix); os.IsNotExist(err) {
		return []drivers.ObjectMetadata{}, resources, drivers.BucketNotFound{Bucket: bucket}
	}

	p.root = rootPrefix
	err := filepath.Walk(rootPrefix, p.getAllFiles)
	if err != nil {
		return []drivers.ObjectMetadata{}, resources, drivers.EmbedError(bucket, "", err)
	}

	var metadataList []drivers.ObjectMetadata
	var metadata drivers.ObjectMetadata

	// Populate filtering mode
	resources.Mode = drivers.GetMode(resources)

	for name, f := range p.files {
		if len(metadataList) >= resources.Maxkeys {
			resources.IsTruncated = true
			goto ret
		}
		metadata, resources, err = file.filter(bucket, name, f, resources)
		if err != nil {
			return []drivers.ObjectMetadata{}, resources, drivers.EmbedError(bucket, "", err)
		}
		if metadata.Bucket != "" {
			metadataList = append(metadataList, metadata)
		}
	}

ret:
	sort.Sort(byObjectKey(metadataList))
	return metadataList, resources, nil
}
