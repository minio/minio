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

package main

import (
	"time"

	"github.com/minio/minio/pkg/disk"
)

// MakeBucketArgs - make bucket args.
type MakeBucketArgs struct {
	BucketName string `json:"bucketName"`
}

// DiskInfoArgs - disk info args.
type DiskInfoArgs struct{}

// ServerInfoArgs  - server info args.
type ServerInfoArgs struct{}

// ListBucketsArgs - list bucket args.
type ListBucketsArgs struct{}

// GenericArgs - empty struct
type GenericArgs struct{}

// DiskInfoRep - disk info reply.
type DiskInfoRep struct {
	DiskInfo  disk.Info `json:"diskInfo"`
	UIVersion string    `json:"uiVersion"`
}

// ListBucketsRep - list buckets response
type ListBucketsRep struct {
	Buckets   []BucketInfo `json:"buckets"`
	UIVersion string       `json:"uiVersion"`
}

// ListObjectsArgs - list object args.
type ListObjectsArgs struct {
	BucketName string `json:"bucketName"`
	Prefix     string `json:"prefix"`
}

// ListObjectsRep - list objects response.
type ListObjectsRep struct {
	Objects   []ObjectInfo `json:"objects"`
	UIVersion string       `json:"uiVersion"`
}

// PutObjectURLArgs - args to generate url for upload access.
type PutObjectURLArgs struct {
	TargetHost string `json:"targetHost"`
	BucketName string `json:"bucketName"`
	ObjectName string `json:"objectName"`
}

// PutObjectURLRep - reply for presigned upload url request.
type PutObjectURLRep struct {
	URL       string `json:"url"`
	UIVersion string `json:"uiVersion"`
}

// GetObjectURLArgs - args to generate url for download access.
type GetObjectURLArgs struct {
	TargetHost string `json:"targetHost"`
	BucketName string `json:"bucketName"`
	ObjectName string `json:"objectName"`
}

// GetObjectURLRep - reply for presigned download url request.
type GetObjectURLRep struct {
	URL       string `json:"url"`
	UIVersion string `json:"uiVersion"`
}

// RemoveObjectArgs - args to remove an object
type RemoveObjectArgs struct {
	TargetHost string `json:"targetHost"`
	BucketName string `json:"bucketName"`
	ObjectName string `json:"objectName"`
}

// GenericRep - reply structure for calls for which reply is success/failure
// for ex. RemoveObject MakeBucket
type GenericRep struct {
	UIVersion string `json:"uiVersion"`
}

// BucketInfo container for list buckets metadata.
type BucketInfo struct {
	// The name of the bucket.
	Name string `json:"name"`
	// Date the bucket was created.
	CreationDate time.Time `json:"creationDate"`
}

// ObjectInfo container for list objects metadata.
type ObjectInfo struct {
	// Name of the object
	Key string `json:"name"`
	// Date and time the object was last modified.
	LastModified time.Time `json:"lastModified"`
	// Size in bytes of the object.
	Size int64 `json:"size"`
	// ContentType is mime type of the object.
	ContentType string `json:"contentType"`
}

// LoginArgs - login arguments.
type LoginArgs struct {
	Username string `json:"username" form:"username"`
	Password string `json:"password" form:"password"`
}

// LoginRep - login reply.
type LoginRep struct {
	Token     string `json:"token"`
	UIVersion string `json:"uiVersion"`
}

// ServerInfoRep - server info reply.
type ServerInfoRep struct {
	MinioVersion  string
	MinioMemory   string
	MinioPlatform string
	MinioRuntime  string
	UIVersion     string `json:"uiVersion"`
}
