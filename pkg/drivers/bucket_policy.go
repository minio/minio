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

package drivers

import (
	"encoding/json"
	"io"
	"strings"
)

// User - canonical
type User struct {
	AWS string
}

// Statement - minio policy statement
type Statement struct {
	Sid       string
	Effect    string
	Principal User
	Action    []string
	Resource  []string
	// add Condition struct/var TODO - fix it in future if necessary
}

// BucketPolicy - minio policy collection
type BucketPolicy struct {
	Version   string // date in 0000-00-00 format
	Statement []Statement
}

// Resource delimiter
const (
	MinioResource = "minio:::"
)

// TODO support canonical user
// Principal delimiter
const (
	MinioPrincipal = "minio::"
)

// Action map
var SupportedActionMap = map[string]bool{
	"*":                        true,
	"minio:GetObject":          true,
	"minio:ListBucket":         true,
	"minio:PutObject":          true,
	"minio:CreateBucket":       true,
	"minio:GetBucketPolicy":    true,
	"minio:DeleteBucketPolicy": true,
	"minio:ListAllMyBuckets":   true,
	"minio:PutBucketPolicy":    true,
}

// Effect map
var SupportedEffectMap = map[string]bool{
	"Allow": true,
	"Deny":  true,
}

func isValidAction(action []string) bool {
	for _, a := range action {
		if !SupportedActionMap[a] {
			return false
		}
	}
	return true
}

func isValidEffect(effect string) bool {
	if SupportedEffectMap[effect] {
		return true
	}
	return false
}

func isValidResource(resources []string) bool {
	var ok bool
	for _, resource := range resources {
		switch true {
		case strings.HasPrefix(resource, AwsResource):
			bucket := strings.SplitAfter(resource, AwsResource)[1]
			ok = true
			if len(bucket) == 0 {
				ok = false
			}
		case strings.HasPrefix(resource, MinioResource):
			bucket := strings.SplitAfter(resource, MinioResource)[1]
			ok = true
			if len(bucket) == 0 {
				ok = false
			}
		default:
			ok = false
		}
	}
	return ok
}

func isValidPrincipal(principal string) bool {
	var ok bool
	if principal == "*" {
		return true
	}
	switch true {
	case strings.HasPrefix(principal, AwsPrincipal):
		username := strings.SplitAfter(principal, AwsPrincipal)[1]
		ok = true
		if len(username) == 0 {
			ok = false
		}

	case strings.HasPrefix(principal, MinioPrincipal):
		username := strings.SplitAfter(principal, MinioPrincipal)[1]
		ok = true
		if len(username) == 0 {
			ok = false
		}
	default:
		ok = false
	}
	return ok
}

func parseStatement(statement Statement) bool {
	if len(statement.Sid) == 0 {
		return false
	}
	if len(statement.Effect) == 0 {
		return false
	}
	if !isValidEffect(statement.Effect) {
		return false
	}
	if len(statement.Principal.AWS) == 0 {
		return false
	}
	if !isValidPrincipal(statement.Principal.AWS) {
		return false
	}
	if len(statement.Action) == 0 {
		return false
	}
	if !isValidAction(statement.Action) && !isValidActionS3(statement.Action) {
		return false
	}
	if len(statement.Resource) == 0 {
		return false
	}
	if !isValidResource(statement.Resource) {
		return false
	}
	return true
}

// Parsepolicy - validate request body is proper JSON and in accordance with policy standards
func Parsepolicy(data io.Reader) (BucketPolicy, bool) {
	var policy BucketPolicy
	decoder := json.NewDecoder(data)
	err := decoder.Decode(&policy)
	if err != nil {
		goto error
	}
	if len(policy.Version) == 0 {
		goto error
	}
	_, err = parseDate(policy.Version)
	if err != nil {
		goto error
	}
	if len(policy.Statement) == 0 {
		goto error
	}

	for _, statement := range policy.Statement {
		if !parseStatement(statement) {
			goto error
		}
	}
	return policy, true

error:
	return BucketPolicy{}, false
}
