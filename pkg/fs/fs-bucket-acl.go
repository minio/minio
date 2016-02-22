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

package fs

// IsPrivateBucket - is private bucket
func (fs Filesystem) IsPrivateBucket(bucket string) bool {
	fs.rwLock.Lock()
	defer fs.rwLock.Unlock()
	bucketMetadata, ok := fs.buckets.Metadata[bucket]
	if !ok {
		return true
	}
	return bucketMetadata.ACL.IsPrivate()
}

// IsPublicBucket - is public bucket
func (fs Filesystem) IsPublicBucket(bucket string) bool {
	fs.rwLock.Lock()
	defer fs.rwLock.Unlock()
	bucketMetadata, ok := fs.buckets.Metadata[bucket]
	if !ok {
		return true
	}
	return bucketMetadata.ACL.IsPublicReadWrite()
}

// IsReadOnlyBucket - is read only bucket
func (fs Filesystem) IsReadOnlyBucket(bucket string) bool {
	fs.rwLock.Lock()
	defer fs.rwLock.Unlock()
	bucketMetadata, ok := fs.buckets.Metadata[bucket]
	if !ok {
		return true
	}
	return bucketMetadata.ACL.IsPublicRead()
}

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
