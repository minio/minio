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

package drivers

import (
	"io"
	"regexp"
	"time"
	"unicode/utf8"
)

// Driver - generic API interface for various drivers - donut, file, memory
type Driver interface {
	// Bucket Operations
	ListBuckets() ([]BucketMetadata, error)
	CreateBucket(bucket string) error
	CreateBucketPolicy(bucket string, p BucketPolicy) error
	GetBucketPolicy(bucket string) (BucketPolicy, error)

	// Object Operations
	GetObject(w io.Writer, bucket, object string) (int64, error)
	GetPartialObject(w io.Writer, bucket, object string, start, length int64) (int64, error)
	GetObjectMetadata(bucket string, object string, prefix string) (ObjectMetadata, error)
	ListObjects(bucket string, resources BucketResourcesMetadata) ([]ObjectMetadata, BucketResourcesMetadata, error)
	CreateObject(bucket string, key string, contentType string, md5sum string, data io.Reader) error
}

// BucketMetadata - name and create date
type BucketMetadata struct {
	Name    string
	Created time.Time
}

// ObjectMetadata - object key and its relevant metadata
type ObjectMetadata struct {
	Bucket string
	Key    string

	ContentType string
	Created     time.Time
	Md5         string
	Size        int64
}

// FilterMode type
type FilterMode int

// FilterMode list
const (
	DelimiterPrefixMode FilterMode = iota
	DelimiterMode
	PrefixMode
	DefaultMode
)

// BucketResourcesMetadata - various types of bucket resources
type BucketResourcesMetadata struct {
	Prefix         string
	Marker         string
	Maxkeys        int
	Delimiter      string
	IsTruncated    bool
	CommonPrefixes []string
	Mode           FilterMode

	Policy bool
	// TODO
	Logging      string
	Notification string
}

// GetMode - Populate filter mode
func GetMode(resources BucketResourcesMetadata) FilterMode {
	var f FilterMode
	switch true {
	case resources.Delimiter != "" && resources.Prefix != "":
		f = DelimiterPrefixMode
	case resources.Delimiter != "" && resources.Prefix == "":
		f = DelimiterMode
	case resources.Delimiter == "" && resources.Prefix != "":
		f = PrefixMode
	case resources.Delimiter == "" && resources.Prefix == "":
		f = DefaultMode
	}

	return f
}

// IsDelimiterPrefixSet Delimiter and Prefix set
func (b BucketResourcesMetadata) IsDelimiterPrefixSet() bool {
	return b.Mode == DelimiterPrefixMode
}

// IsDelimiterSet Delimiter set
func (b BucketResourcesMetadata) IsDelimiterSet() bool {
	return b.Mode == DelimiterMode
}

// IsPrefixSet Prefix set
func (b BucketResourcesMetadata) IsPrefixSet() bool {
	return b.Mode == PrefixMode
}

// IsDefault No query values
func (b BucketResourcesMetadata) IsDefault() bool {
	return b.Mode == DefaultMode
}

// IsValidBucket - verify bucket name in accordance with
//  - http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html
func IsValidBucket(bucket string) bool {
	if len(bucket) < 3 || len(bucket) > 63 {
		return false
	}
	if bucket[0] == '.' || bucket[len(bucket)-1] == '.' {
		return false
	}
	if match, _ := regexp.MatchString("\\.\\.", bucket); match == true {
		return false
	}
	// We don't support buckets with '.' in them
	match, _ := regexp.MatchString("^[a-zA-Z][a-zA-Z0-9\\-]+[a-zA-Z0-9]$", bucket)
	return match
}

// IsValidObject - verify object name in accordance with
//   - http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
func IsValidObject(object string) bool {
	if len(object) > 1024 || len(object) == 0 {
		return false
	}
	if !utf8.ValidString(object) {
		return false
	}
	return true
}
