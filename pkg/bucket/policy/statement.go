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

package policy

import (
	"encoding/json"
	"strings"

	"github.com/minio/minio/pkg/bucket/policy/condition"
)

// Statement - policy statement.
type Statement struct {
	SID        ID                  `json:"Sid,omitempty"`
	Effect     Effect              `json:"Effect"`
	Principal  Principal           `json:"Principal"`
	Actions    ActionSet           `json:"Action"`
	Resources  ResourceSet         `json:"Resource"`
	Conditions condition.Functions `json:"Condition,omitempty"`
}

// Equals checks if two statements are equal
func (statement Statement) Equals(st Statement) bool {
	if statement.Effect != st.Effect {
		return false
	}
	if !statement.Principal.Equals(st.Principal) {
		return false
	}
	if !statement.Actions.Equals(st.Actions) {
		return false
	}
	if !statement.Resources.Equals(st.Resources) {
		return false
	}
	if !statement.Conditions.Equals(st.Conditions) {
		return false
	}
	return true
}

// IsAllowed - checks given policy args is allowed to continue the Rest API.
func (statement Statement) IsAllowed(args Args) bool {
	check := func() bool {
		if !statement.Principal.Match(args.AccountName) {
			return false
		}

		if !statement.Actions.Contains(args.Action) {
			return false
		}

		resource := args.BucketName
		if args.ObjectName != "" {
			if !strings.HasPrefix(args.ObjectName, "/") {
				resource += "/"
			}

			resource += args.ObjectName
		}

		if !statement.Resources.Match(resource, args.ConditionValues) {
			return false
		}

		return statement.Conditions.Evaluate(args.ConditionValues)
	}

	return statement.Effect.IsAllowed(check())
}

// isValid - checks whether statement is valid or not.
func (statement Statement) isValid() error {
	if !statement.Effect.IsValid() {
		return Errorf("invalid Effect %v", statement.Effect)
	}

	if !statement.Principal.IsValid() {
		return Errorf("invalid Principal %v", statement.Principal)
	}

	if len(statement.Actions) == 0 {
		return Errorf("Action must not be empty")
	}

	if len(statement.Resources) == 0 {
		return Errorf("Resource must not be empty")
	}

	for action := range statement.Actions {
		if action.isObjectAction() {
			if !statement.Resources.objectResourceExists() {
				return Errorf("unsupported Resource found %v for action %v", statement.Resources, action)
			}
		} else {
			if !statement.Resources.bucketResourceExists() {
				return Errorf("unsupported Resource found %v for action %v", statement.Resources, action)
			}
		}

		keys := statement.Conditions.Keys()
		keyDiff := keys.Difference(actionConditionKeyMap[action])
		if !keyDiff.IsEmpty() {
			return Errorf("unsupported condition keys '%v' used for action '%v'", keyDiff, action)
		}
	}

	return nil
}

// MarshalJSON - encodes JSON data to Statement.
func (statement Statement) MarshalJSON() ([]byte, error) {
	if err := statement.isValid(); err != nil {
		return nil, err
	}

	// subtype to avoid recursive call to MarshalJSON()
	type subStatement Statement
	ss := subStatement(statement)
	return json.Marshal(ss)
}

// UnmarshalJSON - decodes JSON data to Statement.
func (statement *Statement) UnmarshalJSON(data []byte) error {
	// subtype to avoid recursive call to UnmarshalJSON()
	type subStatement Statement
	var ss subStatement

	if err := json.Unmarshal(data, &ss); err != nil {
		return err
	}

	s := Statement(ss)
	if err := s.isValid(); err != nil {
		return err
	}

	*statement = s

	return nil
}

// Validate - validates Statement is for given bucket or not.
func (statement Statement) Validate(bucketName string) error {
	if err := statement.isValid(); err != nil {
		return err
	}

	return statement.Resources.Validate(bucketName)
}

// Clone clones Statement structure
func (statement Statement) Clone() Statement {
	return NewStatement(statement.Effect, statement.Principal.Clone(),
		statement.Actions.Clone(), statement.Resources.Clone(), statement.Conditions.Clone())
}

// NewStatement - creates new statement.
func NewStatement(effect Effect, principal Principal, actionSet ActionSet, resourceSet ResourceSet, conditions condition.Functions) Statement {
	return Statement{
		Effect:     effect,
		Principal:  principal,
		Actions:    actionSet,
		Resources:  resourceSet,
		Conditions: conditions,
	}
}
