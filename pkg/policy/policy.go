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
	"io"
)

// DefaultVersion - default policy version as per AWS S3 specification.
const DefaultVersion = "2012-10-17"

// Args - arguments to policy to check whether it is allowed
type Args struct {
	AccountName     string
	Action          Action
	BucketName      string
	ConditionValues map[string][]string
	IsOwner         bool
	ObjectName      string
}

// Policy - bucket policy.
type Policy struct {
	ID         ID `json:"ID,omitempty"`
	Version    string
	Statements []Statement `json:"Statement"`
}

// IsAllowed - checks given policy args is allowed to continue the Rest API.
func (policy Policy) IsAllowed(args Args) bool {
	// Check all deny statements. If any one statement denies, return false.
	for _, statement := range policy.Statements {
		if statement.Effect == Deny {
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
	for _, statement := range policy.Statements {
		if statement.Effect == Allow {
			if statement.IsAllowed(args) {
				return true
			}
		}
	}

	return false
}

// IsEmpty - returns whether policy is empty or not.
func (policy Policy) IsEmpty() bool {
	return len(policy.Statements) == 0
}

// isValid - checks if Policy is valid or not.
func (policy Policy) isValid() error {
	if policy.Version != DefaultVersion && policy.Version != "" {
		return fmt.Errorf("invalid version '%v'", policy.Version)
	}

	for _, statement := range policy.Statements {
		if err := statement.isValid(); err != nil {
			return err
		}
	}

	for i := range policy.Statements {
		for _, statement := range policy.Statements[i+1:] {
			principals := policy.Statements[i].Principal.Intersection(statement.Principal)
			if principals.IsEmpty() {
				continue
			}

			actions := policy.Statements[i].Actions.Intersection(statement.Actions)
			if len(actions) == 0 {
				continue
			}

			resources := policy.Statements[i].Resources.Intersection(statement.Resources)
			if len(resources) == 0 {
				continue
			}

			if policy.Statements[i].Conditions.String() != statement.Conditions.String() {
				continue
			}

			return fmt.Errorf("duplicate principal %v, actions %v, resouces %v found in statements %v, %v",
				principals, actions, resources, policy.Statements[i], statement)
		}
	}

	return nil
}

// MarshalJSON - encodes Policy to JSON data.
func (policy Policy) MarshalJSON() ([]byte, error) {
	if err := policy.isValid(); err != nil {
		return nil, err
	}

	// subtype to avoid recursive call to MarshalJSON()
	type subPolicy Policy
	return json.Marshal(subPolicy(policy))
}

// UnmarshalJSON - decodes JSON data to Policy.
func (policy *Policy) UnmarshalJSON(data []byte) error {
	// subtype to avoid recursive call to UnmarshalJSON()
	type subPolicy Policy
	var sp subPolicy
	if err := json.Unmarshal(data, &sp); err != nil {
		return err
	}

	p := Policy(sp)
	if err := p.isValid(); err != nil {
		return err
	}

	*policy = p

	return nil
}

// Validate - validates all statements are for given bucket or not.
func (policy Policy) Validate(bucketName string) error {
	if err := policy.isValid(); err != nil {
		return err
	}

	for _, statement := range policy.Statements {
		if err := statement.Validate(bucketName); err != nil {
			return err
		}
	}

	return nil
}

// ParseConfig - parses data in given reader to Policy.
func ParseConfig(reader io.Reader, bucketName string) (*Policy, error) {
	var policy Policy

	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&policy); err != nil {
		return nil, err
	}

	err := policy.Validate(bucketName)
	return &policy, err
}
