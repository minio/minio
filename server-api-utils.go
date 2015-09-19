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

import (
	"encoding/base64"
	"strconv"
	"strings"
)

// isValidMD5 - verify if valid md5
func isValidMD5(md5 string) bool {
	if md5 == "" {
		return true
	}
	_, err := base64.StdEncoding.DecodeString(strings.TrimSpace(md5))
	if err != nil {
		return false
	}
	return true
}

/// http://docs.aws.amazon.com/AmazonS3/latest/dev/UploadingObjects.html
const (
	// maximum object size per PUT request is 5GB
	maxObjectSize = 1024 * 1024 * 1024 * 5
	// mimimum object size per Multipart PUT request is 5MB
	minMultiPartObjectSize = 1024 * 1024 * 5
	// minimum object size per PUT request is 1B
	minObjectSize = 1
)

// isMaxObjectSize - verify if max object size
func isMaxObjectSize(size string) bool {
	i, err := strconv.ParseInt(size, 10, 64)
	if err != nil {
		return true
	}
	if i > maxObjectSize {
		return true
	}
	return false
}

// isMinObjectSize - verify if min object size
func isMinObjectSize(size string) bool {
	i, err := strconv.ParseInt(size, 10, 64)
	if err != nil {
		return true
	}
	if i < minObjectSize {
		return true
	}
	return false
}

// isMinMultipartObjectSize - verify if the uploaded multipart is of minimum size
func isMinMultipartObjectSize(size string) bool {
	i, err := strconv.ParseInt(size, 10, 64)
	if err != nil {
		return true
	}
	if i < minMultiPartObjectSize {
		return true
	}
	return false
}
