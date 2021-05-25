// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package iampolicy

import (
	"encoding/json"
	"path"
	"strings"

	"github.com/minio/minio/pkg/bucket/policy/condition"
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
	return !strings.Contains(r.Pattern, "/") || r.Pattern == "*"
}

func (r Resource) isObjectPattern() bool {
	return strings.Contains(r.Pattern, "/") || strings.Contains(r.BucketName, "*") || r.Pattern == "*/*"
}

// IsValid - checks whether Resource is valid or not.
func (r Resource) IsValid() bool {
	return r.Pattern != ""
}

// MatchResource matches object name with resource pattern only.
func (r Resource) MatchResource(resource string) bool {
	return r.Match(resource, nil)
}

// Match - matches object name with resource pattern, including specific conditionals.
func (r Resource) Match(resource string, conditionValues map[string][]string) bool {
	pattern := r.Pattern
	for _, key := range condition.CommonKeys {
		// Empty values are not supported for policy variables.
		if rvalues, ok := conditionValues[key.Name()]; ok && rvalues[0] != "" {
			pattern = strings.Replace(pattern, key.VarName(), rvalues[0], -1)
		}
	}
	if cp := path.Clean(resource); cp != "." && cp == pattern {
		return true
	}
	return wildcard.Match(pattern, resource)
}

// MarshalJSON - encodes Resource to JSON data.
func (r Resource) MarshalJSON() ([]byte, error) {
	if !r.IsValid() {
		return nil, Errorf("invalid resource %v", r)
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
func (r Resource) Validate() error {
	if !r.IsValid() {
		return Errorf("invalid resource")
	}
	return nil
}

// parseResource - parses string to Resource.
func parseResource(s string) (Resource, error) {
	if !strings.HasPrefix(s, ResourceARNPrefix) {
		return Resource{}, Errorf("invalid resource '%v'", s)
	}

	pattern := strings.TrimPrefix(s, ResourceARNPrefix)
	tokens := strings.SplitN(pattern, "/", 2)
	bucketName := tokens[0]
	if bucketName == "" {
		return Resource{}, Errorf("invalid resource format '%v'", s)
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
