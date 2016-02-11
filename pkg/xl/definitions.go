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

package xl

import "time"

// ObjectMetadata container for object on xl system
type ObjectMetadata struct {
	// version
	Version string `json:"version"`

	// object metadata
	Created time.Time `json:"created"`
	Bucket  string    `json:"bucket"`
	Object  string    `json:"object"`
	Size    int64     `json:"size"`

	// erasure
	DataDisks   uint8 `json:"sys.erasureK"`
	ParityDisks uint8 `json:"sys.erasureM"`
	BlockSize   int   `json:"sys.blockSize"`
	ChunkCount  int   `json:"sys.chunkCount"`

	// checksums
	MD5Sum    string `json:"sys.md5sum"`
	SHA512Sum string `json:"sys.sha512sum"`

	// metadata
	Metadata map[string]string `json:"metadata"`
}

// Metadata container for xl metadata
type Metadata struct {
	Version string `json:"version"`
}

// AllBuckets container for all buckets
type AllBuckets struct {
	Version string                    `json:"version"`
	Buckets map[string]BucketMetadata `json:"buckets"`
}

// BucketMetadata container for bucket level metadata
type BucketMetadata struct {
	Version       string                      `json:"version"`
	Name          string                      `json:"name"`
	ACL           BucketACL                   `json:"acl"`
	Created       time.Time                   `json:"created"`
	Multiparts    map[string]MultiPartSession `json:"multiparts"`
	Metadata      map[string]string           `json:"metadata"`
	BucketObjects map[string]struct{}         `json:"objects"`
}

// ListObjectsResults container for list objects response
type ListObjectsResults struct {
	Objects        map[string]ObjectMetadata `json:"objects"`
	CommonPrefixes []string                  `json:"commonPrefixes"`
	IsTruncated    bool                      `json:"isTruncated"`
}

// MultiPartSession multipart session
type MultiPartSession struct {
	UploadID   string                  `json:"uploadId"`
	Initiated  time.Time               `json:"initiated"`
	Parts      map[string]PartMetadata `json:"parts"`
	TotalParts int                     `json:"total-parts"`
}

// PartMetadata - various types of individual part resources
type PartMetadata struct {
	PartNumber   int
	LastModified time.Time
	ETag         string
	Size         int64
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
	Part []CompletePart
}

// ObjectResourcesMetadata - various types of object resources
type ObjectResourcesMetadata struct {
	Bucket               string
	EncodingType         string
	Key                  string
	UploadID             string
	StorageClass         string
	PartNumberMarker     int
	NextPartNumberMarker int
	MaxParts             int
	IsTruncated          bool

	Part []*PartMetadata
}

// UploadMetadata container capturing metadata on in progress multipart upload in a given bucket
type UploadMetadata struct {
	Key          string
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

// BucketResourcesMetadata - various types of bucket resources
type BucketResourcesMetadata struct {
	Prefix         string
	Marker         string
	NextMarker     string
	Maxkeys        int
	EncodingType   string
	Delimiter      string
	IsTruncated    bool
	CommonPrefixes []string
}
