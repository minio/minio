/*
 * Minio Go Library for Amazon S3 Compatible Cloud Storage (C) 2016 Minio, Inc.
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

import (
	"net/http"
	"time"
)

// copyCondition explanation:
// http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectCOPY.html
//
// Example:
//
//   copyCondition {
//       key: "x-amz-copy-if-modified-since",
//       value: "Tue, 15 Nov 1994 12:45:26 GMT",
//   }
//
type copyCondition struct {
	key   string
	value string
}

// CopyConditions - copy conditions.
type CopyConditions struct {
	conditions []copyCondition
}

// NewCopyConditions - Instantiate new list of conditions.  This
// function is left behind for backward compatibility. The idiomatic
// way to set an empty set of copy conditions is,
//    ``copyConditions := CopyConditions{}``.
//
func NewCopyConditions() CopyConditions {
	return CopyConditions{}
}

// SetMatchETag - set match etag.
func (c *CopyConditions) SetMatchETag(etag string) error {
	if etag == "" {
		return ErrInvalidArgument("ETag cannot be empty.")
	}
	c.conditions = append(c.conditions, copyCondition{
		key:   "x-amz-copy-source-if-match",
		value: etag,
	})
	return nil
}

// SetMatchETagExcept - set match etag except.
func (c *CopyConditions) SetMatchETagExcept(etag string) error {
	if etag == "" {
		return ErrInvalidArgument("ETag cannot be empty.")
	}
	c.conditions = append(c.conditions, copyCondition{
		key:   "x-amz-copy-source-if-none-match",
		value: etag,
	})
	return nil
}

// SetUnmodified - set unmodified time since.
func (c *CopyConditions) SetUnmodified(modTime time.Time) error {
	if modTime.IsZero() {
		return ErrInvalidArgument("Modified since cannot be empty.")
	}
	c.conditions = append(c.conditions, copyCondition{
		key:   "x-amz-copy-source-if-unmodified-since",
		value: modTime.Format(http.TimeFormat),
	})
	return nil
}

// SetModified - set modified time since.
func (c *CopyConditions) SetModified(modTime time.Time) error {
	if modTime.IsZero() {
		return ErrInvalidArgument("Modified since cannot be empty.")
	}
	c.conditions = append(c.conditions, copyCondition{
		key:   "x-amz-copy-source-if-modified-since",
		value: modTime.Format(http.TimeFormat),
	})
	return nil
}
