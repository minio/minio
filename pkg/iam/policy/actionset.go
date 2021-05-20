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
	"fmt"
	"sort"

	"github.com/minio/minio-go/v7/pkg/set"
)

// ActionSet - set of actions.
type ActionSet map[Action]struct{}

// Clone clones ActionSet structure
func (actionSet ActionSet) Clone() ActionSet {
	return NewActionSet(actionSet.ToSlice()...)
}

// Add - add action to the set.
func (actionSet ActionSet) Add(action Action) {
	actionSet[action] = struct{}{}
}

// IsEmpty - returns if the current action set is empty
func (actionSet ActionSet) IsEmpty() bool {
	return len(actionSet) == 0
}

// Match - matches object name with anyone of action pattern in action set.
func (actionSet ActionSet) Match(action Action) bool {
	for r := range actionSet {
		if r.Match(action) {
			return true
		}

		// This is a special case where GetObjectVersion
		// means GetObject is enabled implicitly.
		switch r {
		case GetObjectVersionAction:
			if action == GetObjectAction {
				return true
			}
		}
	}

	return false
}

// Equals - checks whether given action set is equal to current action set or not.
func (actionSet ActionSet) Equals(sactionSet ActionSet) bool {
	// If length of set is not equal to length of given set, the
	// set is not equal to given set.
	if len(actionSet) != len(sactionSet) {
		return false
	}

	// As both sets are equal in length, check each elements are equal.
	for k := range actionSet {
		if _, ok := sactionSet[k]; !ok {
			return false
		}
	}

	return true
}

// Intersection - returns actions available in both ActionSet.
func (actionSet ActionSet) Intersection(sset ActionSet) ActionSet {
	nset := NewActionSet()
	for k := range actionSet {
		if _, ok := sset[k]; ok {
			nset.Add(k)
		}
	}

	return nset
}

// MarshalJSON - encodes ActionSet to JSON data.
func (actionSet ActionSet) MarshalJSON() ([]byte, error) {
	if len(actionSet) == 0 {
		return nil, Errorf("empty action set")
	}

	return json.Marshal(actionSet.ToSlice())
}

func (actionSet ActionSet) String() string {
	actions := []string{}
	for action := range actionSet {
		actions = append(actions, string(action))
	}
	sort.Strings(actions)

	return fmt.Sprintf("%v", actions)
}

// ToSlice - returns slice of actions from the action set.
func (actionSet ActionSet) ToSlice() []Action {
	actions := []Action{}
	for action := range actionSet {
		actions = append(actions, action)
	}

	return actions
}

// ToAdminSlice - returns slice of admin actions from the action set.
func (actionSet ActionSet) ToAdminSlice() []AdminAction {
	actions := []AdminAction{}
	for action := range actionSet {
		actions = append(actions, AdminAction(action))
	}

	return actions
}

// UnmarshalJSON - decodes JSON data to ActionSet.
func (actionSet *ActionSet) UnmarshalJSON(data []byte) error {
	var sset set.StringSet
	if err := json.Unmarshal(data, &sset); err != nil {
		return err
	}

	if len(sset) == 0 {
		return Errorf("empty action set")
	}

	*actionSet = make(ActionSet)
	for _, s := range sset.ToSlice() {
		actionSet.Add(Action(s))
	}

	return nil
}

// ValidateAdmin checks if all actions are valid Admin actions
func (actionSet ActionSet) ValidateAdmin() error {
	for _, action := range actionSet.ToAdminSlice() {
		if !action.IsValid() {
			return Errorf("unsupported admin action '%v'", action)
		}
	}
	return nil
}

// Validate checks if all actions are valid
func (actionSet ActionSet) Validate() error {
	for _, action := range actionSet.ToSlice() {
		if !action.IsValid() {
			return Errorf("unsupported action '%v'", action)
		}
	}
	return nil
}

// NewActionSet - creates new action set.
func NewActionSet(actions ...Action) ActionSet {
	actionSet := make(ActionSet)
	for _, action := range actions {
		actionSet.Add(action)
	}

	return actionSet
}
