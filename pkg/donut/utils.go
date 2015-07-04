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

package donut

import (
	"regexp"
	"strings"
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

// IsValidObjectName - verify object name in accordance with
//   - http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
func IsValidObjectName(object string) bool {
	if strings.TrimSpace(object) == "" {
		return false
	}
	if len(object) > 1024 || len(object) == 0 {
		return false
	}
	if !utf8.ValidString(object) {
		return false
	}
	return true
}

// IsValidPrefix - verify prefix name is correct, an empty prefix is valid
func IsValidPrefix(prefix string) bool {
	if strings.TrimSpace(prefix) == "" {
		return true
	}
	return IsValidObjectName(prefix)
}
