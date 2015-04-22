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

package api

import (
	"net/http"
	"strings"
)

// Please read for more information - http://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#canned-acl
//
// Here We are only supporting 'acl's through request headers not through their request body
// http://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#setting-acls

// Minio only supports three types for now i.e 'private, public-read, public-read-write'
type ACLType int

const (
	unsupportedACLType ACLType = iota
	privateACLType
	publicReadACLType
	publicReadWriteACLType
)

// Get acl type requested from 'x-amz-acl' header
func getACLType(req *http.Request) ACLType {
	aclHeader := req.Header.Get("x-amz-acl")
	switch {
	case strings.HasPrefix(aclHeader, "private"):
		return privateACLType
	case strings.HasPrefix(aclHeader, "public-read"):
		return publicReadACLType
	case strings.HasPrefix(aclHeader, "public-read-write"):
		return publicReadWriteACLType
	default:
		return unsupportedACLType
	}
}

// ACL type to human readable string
func getACLTypeString(acl ACLType) string {
	switch acl {
	case privateACLType:
		{
			return "private"
		}
	case publicReadACLType:
		{
			return "public-read"
		}
	case publicReadWriteACLType:
		{
			return "public-read-write"
		}
	default:
		return ""
	}
}
