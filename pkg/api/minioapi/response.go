package minioapi

import (
	mstorage "github.com/minio-io/minio/pkg/storage"
)

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
