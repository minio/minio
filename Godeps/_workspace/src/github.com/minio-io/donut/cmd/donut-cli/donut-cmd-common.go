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

package main

import (
	"strings"

	"net/url"
)

// url2Object converts URL to bucket and objectname
func url2Object(urlStr string) (bucketName, objectName string, err error) {
	u, err := url.Parse(urlStr)
	if u.Path == "" {
		// No bucket name passed. It is a valid case
		return "", "", nil
	}
	splits := strings.SplitN(u.Path, "/", 3)
	switch len(splits) {
	case 0, 1:
		bucketName = ""
		objectName = ""
	case 2:
		bucketName = splits[1]
		objectName = ""
	case 3:
		bucketName = splits[1]
		objectName = splits[2]
	}
	return bucketName, objectName, nil
}

func isStringInSlice(items []string, item string) bool {
	for _, s := range items {
		if s == item {
			return true
		}
	}
	return false
}

func deleteFromSlice(items []string, item string) []string {
	var newitems []string
	for _, s := range items {
		if s == item {
			continue
		}
		newitems = append(newitems, s)
	}
	return newitems
}

func appendUniq(slice []string, i string) []string {
	for _, ele := range slice {
		if ele == i {
			return slice
		}
	}
	return append(slice, i)
}

// Is alphanumeric?
func isalnum(c rune) bool {
	return '0' <= c && c <= '9' || 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z'
}

// isValidDonutName - verify donutName to be valid
func isValidDonutName(donutName string) bool {
	if len(donutName) > 1024 || len(donutName) == 0 {
		return false
	}
	for _, char := range donutName {
		if isalnum(char) {
			continue
		}
		switch char {
		case '-':
		case '.':
		case '_':
		case '~':
			continue
		default:
			return false
		}
	}
	return true
}

// getNodeMap - get a node and disk map through nodeConfig struct
func getNodeMap(node map[string]nodeConfig) map[string][]string {
	nodes := make(map[string][]string)
	for k, v := range node {
		nodes[k] = v.ActiveDisks
	}
	return nodes
}
