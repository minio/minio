/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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

import "time"

// PartMetadata - various types of individual part resources
type PartMetadata struct {
	PartNumber   int
	LastModified time.Time
	ETag         string
	Size         int64
}

// ObjectResourcesMetadata - various types of object resources
type ObjectResourcesMetadata struct {
	Bucket               string
	Object               string
	UploadID             string
	StorageClass         string
	PartNumberMarker     int
	NextPartNumberMarker int
	MaxParts             int
	IsTruncated          bool

	Part         []PartMetadata
	EncodingType string
}

// UploadMetadata container capturing metadata on in progress multipart upload in a given bucket
type UploadMetadata struct {
	Object       string
	UploadID     string
	StorageClass string
	Initiated    time.Time
}

// BucketMultipartResourcesMetadata - various types of bucket resources for inprogress multipart uploads
type BucketMultipartResourcesMetadata struct {
	KeyMarker          string
	UploadIDMarker     string
	NextKeyMarker      string
	NextUploadIDMarker string
	EncodingType       string
	MaxUploads         int
	IsTruncated        bool
	Upload             []*UploadMetadata
	Prefix             string
	Delimiter          string
	CommonPrefixes     []string
}

// ListObjectsResult - container for list object request results.
type ListObjectsResult struct {
	IsTruncated bool
	NextMarker  string
	Objects     []ObjectInfo
	Prefixes    []string
}

// CompletePart - completed part container
type CompletePart struct {
	PartNumber int
	ETag       string
}

// completedParts is a sortable interface for Part slice
type completedParts []CompletePart

func (a completedParts) Len() int           { return len(a) }
func (a completedParts) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a completedParts) Less(i, j int) bool { return a[i].PartNumber < a[j].PartNumber }

// CompleteMultipartUpload container for completing multipart upload
type CompleteMultipartUpload struct {
	Parts []CompletePart `xml:"Part"`
}
