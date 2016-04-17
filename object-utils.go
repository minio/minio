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

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

const (
	slashPathSeparator = "/"
)

// validBucket regexp.
var validBucket = regexp.MustCompile(`^[a-z0-9][a-z0-9\.\-]{1,61}[a-z0-9]$`)

// IsValidBucketName verifies a bucket name in accordance with Amazon's
// requirements. It must be 3-63 characters long, can contain dashes
// and periods, but must begin and end with a lowercase letter or a number.
// See: http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
func IsValidBucketName(bucket string) bool {
	if len(bucket) < 3 || len(bucket) > 63 {
		return false
	}
	if bucket[0] == '.' || bucket[len(bucket)-1] == '.' {
		return false
	}
	return validBucket.MatchString(bucket)
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
// - Left curly brace ("{")
// - Caret ("^")
// - Right curly brace ("}")
// - Grave accent / back tick ("`")
// - Right square bracket ("]")
// - Left square bracket ("[")
// - Tilde ("~")
// - 'Greater Than' symbol (">")
// - 'Less Than' symbol ("<")
// - Vertical bar / pipe ("|")
func IsValidObjectName(object string) bool {
	if len(object) > 1024 || len(object) == 0 {
		return false
	}
	if !utf8.ValidString(object) {
		return false
	}
	// Reject unsupported characters in object name.
	return !strings.ContainsAny(object, "`^*{}[]|\\\"'")
}

// IsValidObjectPrefix verifies whether the prefix is a valid object name.
// Its valid to have a empty prefix.
func IsValidObjectPrefix(object string) bool {
	// Prefix can be empty or "/".
	if object == "" || object == "/" {
		return true
	}
	// Verify if prefix is a valid object name.
	return IsValidObjectName(object)

}

func pathJoin(path1 string, path2 string) string {
	return strings.TrimSuffix(path1, slashPathSeparator) + slashPathSeparator + path2
}
