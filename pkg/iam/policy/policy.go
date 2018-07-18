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

package iampolicy

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/minio/minio/pkg/policy"
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
		return fmt.Errorf("invalid version '%v'", iamp.Version)
	}

	for _, statement := range iamp.Statements {
		if err := statement.isValid(); err != nil {
			return err
		}
	}

	for i := range iamp.Statements {
		for _, statement := range iamp.Statements[i+1:] {
			actions := iamp.Statements[i].Actions.Intersection(statement.Actions)
			if len(actions) == 0 {
				continue
			}

			resources := iamp.Statements[i].Resources.Intersection(statement.Resources)
			if len(resources) == 0 {
				continue
			}

			if iamp.Statements[i].Conditions.String() != statement.Conditions.String() {
				continue
			}

			return fmt.Errorf("duplicate actions %v, resources %v found in statements %v, %v",
				actions, resources, iamp.Statements[i], statement)
		}
	}

	return nil
}

// MarshalJSON - encodes Policy to JSON data.
func (iamp Policy) MarshalJSON() ([]byte, error) {
	if err := iamp.isValid(); err != nil {
		return nil, err
	}

	// subtype to avoid recursive call to MarshalJSON()
	type subPolicy Policy
	return json.Marshal(subPolicy(iamp))
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
	if err := p.isValid(); err != nil {
		return err
	}

	*iamp = p

	return nil
}

// Validate - validates all statements are for given bucket or not.
func (iamp Policy) Validate() error {
	if err := iamp.isValid(); err != nil {
		return err
	}

	for _, statement := range iamp.Statements {
		if err := statement.Validate(); err != nil {
			return err
		}
	}

	return nil
}

// ParseConfig - parses data in given reader to Iamp.
func ParseConfig(reader io.Reader) (*Policy, error) {
	var iamp Policy

	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&iamp); err != nil {
		return nil, err
	}

	return &iamp, iamp.Validate()
}
