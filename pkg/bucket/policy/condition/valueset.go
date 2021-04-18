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

// ToSlice converts ValueSet to a slice of Value
func (set ValueSet) ToSlice() []Value {
	var values []Value
	for k := range set {
		values = append(values, k)
	}
	return values
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

// Clone clones ValueSet structure
func (set ValueSet) Clone() ValueSet {
	return NewValueSet(set.ToSlice()...)
}

// NewValueSet - returns new value set containing given values.
func NewValueSet(values ...Value) ValueSet {
	set := make(ValueSet)

	for _, value := range values {
		set.Add(value)
	}

	return set
}
