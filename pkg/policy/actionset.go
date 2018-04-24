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
	"sort"

	"github.com/minio/minio-go/pkg/set"
)

// ActionSet - set of actions.
type ActionSet map[Action]struct{}

// Add - add action to the set.
func (actionSet ActionSet) Add(action Action) {
	actionSet[action] = struct{}{}
}

// Contains - checks given action exists in the action set.
func (actionSet ActionSet) Contains(action Action) bool {
	_, found := actionSet[action]
	return found
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
		return nil, fmt.Errorf("empty action set")
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

// UnmarshalJSON - decodes JSON data to ActionSet.
func (actionSet *ActionSet) UnmarshalJSON(data []byte) error {
	var sset set.StringSet
	if err := json.Unmarshal(data, &sset); err != nil {
		return err
	}

	if len(sset) == 0 {
		return fmt.Errorf("empty action set")
	}

	*actionSet = make(ActionSet)
	for _, s := range sset.ToSlice() {
		action, err := parseAction(s)
		if err != nil {
			return err
		}

		actionSet.Add(action)
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
