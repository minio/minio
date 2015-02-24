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

package minioapi

import (
	mstorage "github.com/minio-io/minio/pkg/storage"
)

// Reply date format
const (
	dateFormat = "2006-01-02T15:04:05.000Z"
)

// takes an array of Bucketmetadata information for serialization
// input:
// array of bucket metadata
//
// output:
// populated struct that can be serialized to match xml and json api spec output
func generateBucketsListResult(buckets []mstorage.BucketMetadata) BucketListResponse {
	var listbuckets []*Bucket
	var data = BucketListResponse{}
	var owner = Owner{}

	owner.ID = "minio"
	owner.DisplayName = "minio"

	for _, bucket := range buckets {
		var listbucket = &Bucket{}
		listbucket.Name = bucket.Name
		listbucket.CreationDate = bucket.Created.Format(dateFormat)
		listbuckets = append(listbuckets, listbucket)
	}

	data.Owner = owner
	data.Buckets.Bucket = listbuckets

	return data
}

// takes a set of objects and prepares the objects for serialization
// input:
// bucket name
// array of object metadata
// results truncated flag
//
// output:
// populated struct that can be serialized to match xml and json api spec output
func generateObjectsListResult(bucket string, objects []mstorage.ObjectMetadata, isTruncated bool) ObjectListResponse {
	var contents []*Item
	var owner = Owner{}
	var data = ObjectListResponse{}

	owner.ID = "minio"
	owner.DisplayName = "minio"

	for _, object := range objects {
		var content = &Item{}
		content.Key = object.Key
		content.LastModified = object.Created.Format(dateFormat)
		content.ETag = object.ETag
		content.Size = object.Size
		content.StorageClass = "STANDARD"
		content.Owner = owner
		contents = append(contents, content)
	}
	data.Name = bucket
	data.Contents = contents
	data.MaxKeys = MAX_OBJECT_LIST
	data.IsTruncated = isTruncated
	return data
}
