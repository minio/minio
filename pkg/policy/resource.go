/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

package policy

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/minio/minio/pkg/wildcard"
)

// ResourceARNPrefix - resource ARN prefix as per AWS S3 specification.
const ResourceARNPrefix = "arn:aws:s3:::"

// Resource - resource in policy statement.
type Resource struct {
	BucketName string
	Pattern    string
}

func (r Resource) isBucketPattern() bool {
	return !strings.Contains(r.Pattern, "/")
}

func (r Resource) isObjectPattern() bool {
	return strings.Contains(r.Pattern, "/") || strings.Contains(r.BucketName, "*")
}

// IsValid - checks whether Resource is valid or not.
func (r Resource) IsValid() bool {
	return r.BucketName != "" && r.Pattern != ""
}

// Match - matches object name with resource pattern.
func (r Resource) Match(resource string) bool {
	return wildcard.Match(r.Pattern, resource)
}

// MarshalJSON - encodes Resource to JSON data.
func (r Resource) MarshalJSON() ([]byte, error) {
	if !r.IsValid() {
		return nil, fmt.Errorf("invalid resource %v", r)
	}

	return json.Marshal(r.String())
}

func (r Resource) String() string {
	return ResourceARNPrefix + r.Pattern
}

// UnmarshalJSON - decodes JSON data to Resource.
func (r *Resource) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	parsedResource, err := parseResource(s)
	if err != nil {
		return err
	}

	*r = parsedResource

	return nil
}

// Validate - validates Resource is for given bucket or not.
func (r Resource) Validate(bucketName string) error {
	if !r.IsValid() {
		return fmt.Errorf("invalid resource")
	}

	if !wildcard.Match(r.BucketName, bucketName) {
		return fmt.Errorf("bucket name does not match")
	}

	return nil
}

// parseResource - parses string to Resource.
func parseResource(s string) (Resource, error) {
	if !strings.HasPrefix(s, ResourceARNPrefix) {
		return Resource{}, fmt.Errorf("invalid resource '%v'", s)
	}

	pattern := strings.TrimPrefix(s, ResourceARNPrefix)
	tokens := strings.SplitN(pattern, "/", 2)
	bucketName := tokens[0]
	if bucketName == "" {
		return Resource{}, fmt.Errorf("invalid resource format '%v'", s)
	}

	return Resource{
		BucketName: bucketName,
		Pattern:    pattern,
	}, nil
}

// NewResource - creates new resource.
func NewResource(bucketName, keyName string) Resource {
	pattern := bucketName
	if keyName != "" {
		if !strings.HasPrefix(keyName, "/") {
			pattern += "/"
		}

		pattern += keyName
	}

	return Resource{
		BucketName: bucketName,
		Pattern:    pattern,
	}
}
