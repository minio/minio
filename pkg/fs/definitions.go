/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

// BucketACL - bucket level access control
type BucketACL string

// different types of ACL's currently supported for buckets
const (
	BucketPrivate         = BucketACL("private")
	BucketPublicRead      = BucketACL("public-read")
	BucketPublicReadWrite = BucketACL("public-read-write")
)

func (b BucketACL) String() string {
	return string(b)
}

// IsPrivate - is acl Private
func (b BucketACL) IsPrivate() bool {
	return b == BucketACL("private")
}

// IsPublicRead - is acl PublicRead
func (b BucketACL) IsPublicRead() bool {
	return b == BucketACL("public-read")
}

// IsPublicReadWrite - is acl PublicReadWrite
func (b BucketACL) IsPublicReadWrite() bool {
	return b == BucketACL("public-read-write")
}

// BucketMetadata - name and create date
type BucketMetadata struct {
	Name    string
	Created time.Time
	ACL     BucketACL
}

// ObjectMetadata - object key and its relevant metadata
type ObjectMetadata struct {
	Bucket string
	Object string

	ContentType string
	Created     time.Time
	Mode        os.FileMode
	Md5         string
	Size        int64
}

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

	Part         []*PartMetadata
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

type ListObjectsReq struct {
	Bucket    string
	Prefix    string
	Marker    string
	Delimiter string
	MaxKeys   int
}

type ListObjectsResp struct {
	IsTruncated bool
	NextMarker  string
	Objects     []ObjectMetadata
	Prefixes    []string
}

type listServiceReq struct {
	req    ListObjectsReq
	respCh chan ListObjectsResp
}

type listWorkerReq struct {
	req    ListObjectsReq
	respCh chan ListObjectsResp
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

// IsValidBucketACL - is provided acl string supported
func IsValidBucketACL(acl string) bool {
	switch acl {
	case "private":
		fallthrough
	case "public-read":
		fallthrough
	case "public-read-write":
		return true
	case "":
		// by default its "private"
		return true
	default:
		return false
	}
}

// validBucket regexp.
var validBucket = regexp.MustCompile(`^[a-z0-9][a-z0-9\.\-]{1,61}[a-z0-9]$`)

// IsValidBucketName - verify bucket name in accordance with
//  - http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html
func IsValidBucketName(bucket string) bool {
	if strings.TrimSpace(bucket) == "" {
		return false
	}
	if len(bucket) < 3 || len(bucket) > 63 {
		return false
	}
	if bucket[0] == '.' || bucket[len(bucket)-1] == '.' {
		return false
	}
	return validBucket.MatchString(bucket)
}

// IsValidObjectName - verify object name in accordance with
//   - http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
func IsValidObjectName(object string) bool {
	if strings.TrimSpace(object) == "" {
		return true
	}
	if len(object) > 1024 || len(object) == 0 {
		return false
	}
	if !utf8.ValidString(object) {
		return false
	}
	return true
}
