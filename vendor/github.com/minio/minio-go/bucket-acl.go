/*
 * Minio Go Library for Amazon S3 Compatible Cloud Storage (C) 2015 Minio, Inc.
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

package minio

// BucketACL - Bucket level access control.
type BucketACL string

// Different types of ACL's currently supported for buckets.
const (
	bucketPrivate       = BucketACL("private")
	bucketReadOnly      = BucketACL("public-read")
	bucketPublic        = BucketACL("public-read-write")
	bucketAuthenticated = BucketACL("authenticated-read")
)

// Stringify acl.
func (b BucketACL) String() string {
	if string(b) == "" {
		return "private"
	}
	return string(b)
}

// isValidBucketACL - Is provided acl string supported.
func (b BucketACL) isValidBucketACL() bool {
	switch true {
	case b.isPrivate():
		fallthrough
	case b.isReadOnly():
		fallthrough
	case b.isPublic():
		fallthrough
	case b.isAuthenticated():
		return true
	case b.String() == "private":
		// By default its "private"
		return true
	default:
		return false
	}
}

// isPrivate - Is acl Private.
func (b BucketACL) isPrivate() bool {
	return b == bucketPrivate
}

// isPublicRead - Is acl PublicRead.
func (b BucketACL) isReadOnly() bool {
	return b == bucketReadOnly
}

// isPublicReadWrite - Is acl PublicReadWrite.
func (b BucketACL) isPublic() bool {
	return b == bucketPublic
}

// isAuthenticated - Is acl AuthenticatedRead.
func (b BucketACL) isAuthenticated() bool {
	return b == bucketAuthenticated
}
