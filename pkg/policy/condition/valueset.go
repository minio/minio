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

package condition

import (
	"encoding/json"
	"fmt"
)

// ValueSet - unique list of values.
type ValueSet map[Value]struct{}

// Add - adds given value to value set.
func (set ValueSet) Add(value Value) {
	set[value] = struct{}{}
}

// MarshalJSON - encodes ValueSet to JSON data.
func (set ValueSet) MarshalJSON() ([]byte, error) {
	var values []Value
	for k := range set {
		values = append(values, k)
	}

	if len(values) == 0 {
		return nil, fmt.Errorf("invalid value set %v", set)
	}

	return json.Marshal(values)
}

// UnmarshalJSON - decodes JSON data.
func (set *ValueSet) UnmarshalJSON(data []byte) error {
	var v Value
	if err := json.Unmarshal(data, &v); err == nil {
		*set = make(ValueSet)
		set.Add(v)
		return nil
	}

	var values []Value
	if err := json.Unmarshal(data, &values); err != nil {
		return err
	}

	if len(values) < 1 {
		return fmt.Errorf("invalid value")
	}

	*set = make(ValueSet)
	for _, v = range values {
		if _, found := (*set)[v]; found {
			return fmt.Errorf("duplicate value found '%v'", v)
		}

		set.Add(v)
	}

	return nil
}

// NewValueSet - returns new value set containing given values.
func NewValueSet(values ...Value) ValueSet {
	set := make(ValueSet)

	for _, value := range values {
		set.Add(value)
	}

	return set
}
