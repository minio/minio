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
	"io"
	"strings"

	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/pkg/bucket/policy"
)

// DefaultVersion - default policy version as per AWS S3 specification.
const DefaultVersion = "2012-10-17"

// Args - arguments to policy to check whether it is allowed
type Args struct {
	AccountName     string                 `json:"account"`
	Groups          []string               `json:"groups"`
	Action          Action                 `json:"action"`
	BucketName      string                 `json:"bucket"`
	ConditionValues map[string][]string    `json:"conditions"`
	IsOwner         bool                   `json:"owner"`
	ObjectName      string                 `json:"object"`
	Claims          map[string]interface{} `json:"claims"`
	DenyOnly        bool                   `json:"denyOnly"` // only applies deny
}

// GetPoliciesFromClaims returns the list of policies to be applied for this
// incoming request, extracting the information from input JWT claims.
func GetPoliciesFromClaims(claims map[string]interface{}, policyClaimName string) (set.StringSet, bool) {
	s := set.NewStringSet()
	pname, ok := claims[policyClaimName]
	if !ok {
		return s, false
	}
	pnames, ok := pname.([]interface{})
	if !ok {
		pnameStr, ok := pname.(string)
		if ok {
			for _, pname := range strings.Split(pnameStr, ",") {
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
		return s, false
	}
	for _, pname := range pnames {
		pnameStr, ok := pname.(string)
		if ok {
			for _, pnameStr := range strings.Split(pnameStr, ",") {
				pnameStr = strings.TrimSpace(pnameStr)
				if pnameStr == "" {
					// ignore any empty strings, considerate
					// towards some user errors.
					continue
				}
				s.Add(pnameStr)
			}
		}
	}
	return s, true
}

// GetPolicies returns the list of policies to be applied for this
// incoming request, extracting the information from JWT claims.
func (a Args) GetPolicies(policyClaimName string) (set.StringSet, bool) {
	return GetPoliciesFromClaims(a.Claims, policyClaimName)
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

	// Applied any 'Deny' only policies, if we have
	// reached here it means that there were no 'Deny'
	// policies - this function mainly used for
	// specific scenarios where we only want to validate
	// 'Deny' only policies.
	if args.DenyOnly {
		return true
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

// Merge merges two policies documents and drop
// duplicate statements if any.
func (iamp Policy) Merge(input Policy) Policy {
	var mergedPolicy Policy
	if iamp.Version != "" {
		mergedPolicy.Version = iamp.Version
	} else {
		mergedPolicy.Version = input.Version
	}
	for _, st := range iamp.Statements {
		mergedPolicy.Statements = append(mergedPolicy.Statements, st.Clone())
	}
	for _, st := range input.Statements {
		mergedPolicy.Statements = append(mergedPolicy.Statements, st.Clone())
	}
	mergedPolicy.dropDuplicateStatements()
	return mergedPolicy
}

func (iamp *Policy) dropDuplicateStatements() {
redo:
	for i := range iamp.Statements {
		for j, statement := range iamp.Statements[i+1:] {
			if !iamp.Statements[i].Equals(statement) {
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
