/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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

package iampolicy

import (
	"encoding/json"
	"io"
	"strings"

	"github.com/minio/minio-go/v6/pkg/set"
	"github.com/minio/minio/pkg/bucket/policy"
)

// DefaultVersion - default policy version as per AWS S3 specification.
const DefaultVersion = "2012-10-17"

// Args - arguments to policy to check whether it is allowed
type Args struct {
	AccountName     string                 `json:"account"`
	Action          Action                 `json:"action"`
	BucketName      string                 `json:"bucket"`
	ConditionValues map[string][]string    `json:"conditions"`
	IsOwner         bool                   `json:"owner"`
	ObjectName      string                 `json:"object"`
	Claims          map[string]interface{} `json:"claims"`
}

// GetPolicies get policies
func (a Args) GetPolicies(policyClaimName string) (set.StringSet, bool) {
	s := set.NewStringSet()
	pname, ok := a.Claims[policyClaimName]
	if !ok {
		return s, false
	}
	pnames, ok := pname.([]string)
	if !ok {
		pnameStr, ok := pname.(string)
		if ok {
			pnames = strings.Split(pnameStr, ",")
		} else {
			return s, false
		}
	}
	for _, pname := range pnames {
		pname = strings.TrimSpace(pname)
		if pname == "" {
			// ignore any empty strings, considerate
			// towards some user errors.
			continue
		}
		s.Add(pname)
	}
	return s, true
}

// Policy - iam bucket iamp.
type Policy struct {
	ID         policy.ID `json:"ID,omitempty"`
	Version    string
	Statements []Statement `json:"Statement"`
}

// IsAllowed - checks given policy args is allowed to continue the Rest API.
func (iamp Policy) IsAllowed(args Args) bool {
	// Check all deny statements. If any one statement denies, return false.
	for _, statement := range iamp.Statements {
		if statement.Effect == policy.Deny {
			if !statement.IsAllowed(args) {
				return false
			}
		}
	}

	// For owner, its allowed by default.
	if args.IsOwner {
		return true
	}

	// Check all allow statements. If any one statement allows, return true.
	for _, statement := range iamp.Statements {
		if statement.Effect == policy.Allow {
			if statement.IsAllowed(args) {
				return true
			}
		}
	}

	return false
}

// IsEmpty - returns whether policy is empty or not.
func (iamp Policy) IsEmpty() bool {
	return len(iamp.Statements) == 0
}

// isValid - checks if Policy is valid or not.
func (iamp Policy) isValid() error {
	if iamp.Version != DefaultVersion && iamp.Version != "" {
		return Errorf("invalid version '%v'", iamp.Version)
	}

	for _, statement := range iamp.Statements {
		if err := statement.isValid(); err != nil {
			return err
		}
	}
	return nil
}

func (iamp *Policy) dropDuplicateStatements() {
redo:
	for i := range iamp.Statements {
		for j, statement := range iamp.Statements[i+1:] {
			if iamp.Statements[i].Effect != statement.Effect {
				continue
			}

			if !iamp.Statements[i].Actions.Equals(statement.Actions) {
				continue
			}

			if !iamp.Statements[i].Resources.Equals(statement.Resources) {
				continue
			}

			if iamp.Statements[i].Conditions.String() != statement.Conditions.String() {
				continue
			}
			iamp.Statements = append(iamp.Statements[:j], iamp.Statements[j+1:]...)
			goto redo
		}
	}
}

// UnmarshalJSON - decodes JSON data to Iamp.
func (iamp *Policy) UnmarshalJSON(data []byte) error {
	// subtype to avoid recursive call to UnmarshalJSON()
	type subPolicy Policy
	var sp subPolicy
	if err := json.Unmarshal(data, &sp); err != nil {
		return err
	}

	p := Policy(sp)
	p.dropDuplicateStatements()
	*iamp = p
	return nil
}

// Validate - validates all statements are for given bucket or not.
func (iamp Policy) Validate() error {
	return iamp.isValid()
}

// ParseConfig - parses data in given reader to Iamp.
func ParseConfig(reader io.Reader) (*Policy, error) {
	var iamp Policy

	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&iamp); err != nil {
		return nil, Errorf("%w", err)
	}

	return &iamp, iamp.Validate()
}
