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

package cmd

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"path"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/skyrings/skyring-common/tools/uuid"
)

const (
	// Minio meta bucket.
	minioMetaBucket = ".minio.sys"
	// Multipart meta prefix.
	mpartMetaPrefix = "multipart"
	// Tmp meta prefix.
	tmpMetaPrefix = "tmp"
)

// validBucket regexp.
var validBucket = regexp.MustCompile(`^[a-z0-9][a-z0-9\.\-]{1,61}[a-z0-9]$`)
var isIPAddress = regexp.MustCompile(`^(\d+\.){3}\d+$`)

// IsValidBucketName verifies a bucket name in accordance with Amazon's
// requirements. It must be 3-63 characters long, can contain dashes
// and periods, but must begin and end with a lowercase letter or a number.
// See: http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
func IsValidBucketName(bucket string) bool {
	// Special case when bucket is equal to 'metaBucket'.
	if bucket == minioMetaBucket {
		return true
	}
	if len(bucket) < 3 || len(bucket) > 63 {
		return false
	}
	if bucket[0] == '.' || bucket[len(bucket)-1] == '.' {
		return false
	}
	return (validBucket.MatchString(bucket) &&
		!isIPAddress.MatchString(bucket) &&
		!strings.Contains(bucket, ".."))
}

// IsValidObjectName verifies an object name in accordance with Amazon's
// requirements. It cannot exceed 1024 characters and must be a valid UTF8
// string.
//
// See:
// http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
//
// You should avoid the following characters in a key name because of
// significant special handling for consistency across all
// applications.
//
// Rejects strings with following characters.
//
// - Backslash ("\")
// - Caret ("^")
// - Grave accent / back tick ("`")
// - Vertical bar / pipe ("|")
//
// Minio does not support object names with trailing "/".
func IsValidObjectName(object string) bool {
	if len(object) == 0 {
		return false
	}
	if strings.HasSuffix(object, slashSeparator) {
		return false
	}
	if strings.HasPrefix(object, slashSeparator) {
		return false
	}
	return IsValidObjectPrefix(object)
}

// IsValidObjectPrefix verifies whether the prefix is a valid object name.
// Its valid to have a empty prefix.
func IsValidObjectPrefix(object string) bool {
	if len(object) > 1024 {
		return false
	}
	if !utf8.ValidString(object) {
		return false
	}
	// Reject unsupported characters in object name.
	if strings.ContainsAny(object, "`^|\\\"") {
		return false
	}
	return true
}

// Slash separator.
const slashSeparator = "/"

// retainSlash - retains slash from a path.
func retainSlash(s string) string {
	return strings.TrimSuffix(s, slashSeparator) + slashSeparator
}

// pathJoin - like path.Join() but retains trailing "/" of the last element
func pathJoin(elem ...string) string {
	trailingSlash := ""
	if len(elem) > 0 {
		if strings.HasSuffix(elem[len(elem)-1], slashSeparator) {
			trailingSlash = "/"
		}
	}
	return path.Join(elem...) + trailingSlash
}

// getUUID() - get a unique uuid.
func getUUID() (uuidStr string) {
	for {
		uuid, err := uuid.New()
		if err != nil {
			errorIf(err, "Unable to initialize uuid")
			continue
		}
		uuidStr = uuid.String()
		break
	}
	return uuidStr
}

// Create an s3 compatible MD5sum for complete multipart transaction.
func completeMultipartMD5(parts ...completePart) (string, error) {
	var finalMD5Bytes []byte
	for _, part := range parts {
		md5Bytes, err := hex.DecodeString(part.ETag)
		if err != nil {
			return "", err
		}
		finalMD5Bytes = append(finalMD5Bytes, md5Bytes...)
	}
	md5Hasher := md5.New()
	md5Hasher.Write(finalMD5Bytes)
	s3MD5 := fmt.Sprintf("%s-%d", hex.EncodeToString(md5Hasher.Sum(nil)), len(parts))
	return s3MD5, nil
}

// byBucketName is a collection satisfying sort.Interface.
type byBucketName []BucketInfo

func (d byBucketName) Len() int           { return len(d) }
func (d byBucketName) Swap(i, j int)      { d[i], d[j] = d[j], d[i] }
func (d byBucketName) Less(i, j int) bool { return d[i].Name < d[j].Name }
