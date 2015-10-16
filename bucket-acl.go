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

package main

import "net/http"

// Please read for more information - http://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#canned-acl
//
// Here We are only supporting 'acl's through request headers not through their request body
// http://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#setting-acls

// Minio only supports three types for now i.e 'private, public-read, public-read-write'

// ACLType - different acl types
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
	if aclHeader != "" {
		switch {
		case aclHeader == "private":
			return privateACLType
		case aclHeader == "public-read":
			return publicReadACLType
		case aclHeader == "public-read-write":
			return publicReadWriteACLType
		default:
			return unsupportedACLType
		}
	}
	// make it default private
	return privateACLType
}

// ACL type to human readable string
func getACLTypeString(acl ACLType) string {
	switch acl {
	case privateACLType:
		return "private"
	case publicReadACLType:
		return "public-read"
	case publicReadWriteACLType:
		return "public-read-write"
	case unsupportedACLType:
		return ""
	default:
		return "private"
	}
}
