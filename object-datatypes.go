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

import "time"

// BucketInfo - bucket name and create date
type BucketInfo struct {
	Name    string
	Created time.Time
}

// ObjectInfo - object info.
type ObjectInfo struct {
	Bucket      string
	Name        string
	ModTime     time.Time
	ContentType string
	MD5Sum      string
	Size        int64
	IsDir       bool
}

// ListPartsInfo - various types of object resources.
type ListPartsInfo struct {
	Bucket               string
	Object               string
	UploadID             string
	StorageClass         string
	PartNumberMarker     int
	NextPartNumberMarker int
	MaxParts             int
	IsTruncated          bool

	Parts        []partInfo
	EncodingType string
}

// ListMultipartsInfo - various types of bucket resources for inprogress multipart uploads.
type ListMultipartsInfo struct {
	KeyMarker          string
	UploadIDMarker     string
	NextKeyMarker      string
	NextUploadIDMarker string
	EncodingType       string
	MaxUploads         int
	IsTruncated        bool
	Uploads            []uploadMetadata
	Prefix             string
	Delimiter          string
	CommonPrefixes     []string
}

// ListObjectsInfo - container for list objects.
type ListObjectsInfo struct {
	IsTruncated bool
	NextMarker  string
	Objects     []ObjectInfo
	Prefixes    []string
}

// partInfo - various types of individual part resources.
type partInfo struct {
	PartNumber   int
	LastModified time.Time
	ETag         string
	Size         int64
}

// uploadMetadata container capturing metadata on in progress multipart upload in a given bucket
type uploadMetadata struct {
	Object       string
	UploadID     string
	StorageClass string
	Initiated    time.Time
}

// completePart - completed part container.
type completePart struct {
	PartNumber int
	ETag       string
}

// completedParts is a sortable interface for Part slice
type completedParts []completePart

func (a completedParts) Len() int           { return len(a) }
func (a completedParts) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a completedParts) Less(i, j int) bool { return a[i].PartNumber < a[j].PartNumber }

// completeMultipartUpload container for completing multipart upload
type completeMultipartUpload struct {
	Parts []completePart `xml:"Part"`
}
