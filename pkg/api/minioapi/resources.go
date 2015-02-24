/*
 * Mini Object Storage, (C) 2015 Minio, Inc.
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

package minioapi

import (
	"net/url"
	"strconv"
)

// support bucket resources go here
type bucketResources struct {
	prefix    string
	marker    string
	maxkeys   int
	policy    bool
	delimiter string
	//	uploads   bool - TODO implemented with multipart support
}

// parse bucket url queries
func getBucketResources(values url.Values) (v bucketResources) {
	for key, value := range values {
		switch true {
		case key == "prefix":
			v.prefix = value[0]
		case key == "marker":
			v.marker = value[0]
		case key == "maxkeys":
			v.maxkeys, _ = strconv.Atoi(value[0])
		case key == "policy":
			v.policy = true
		case key == "delimiter":
			v.delimiter = value[0]
		}
	}
	return
}
