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
	"strings"
)

// Key - conditional key whose name and it's optional variable.
type Key struct {
	name     KeyName
	variable string
}

// IsValid - checks if key is valid or not.
func (key Key) IsValid() bool {
	for _, name := range AllSupportedKeys {
		if key.name == name {
			return true
		}
	}

	return false
}

// Is - checks if this key has same key name or not.
func (key Key) Is(name KeyName) bool {
	return key.name == name
}

func (key Key) String() string {
	if key.variable != "" {
		return string(key.name) + "/" + key.variable
	}
	return string(key.name)
}

// MarshalJSON - encodes Key to JSON data.
func (key Key) MarshalJSON() ([]byte, error) {
	if !key.IsValid() {
		return nil, fmt.Errorf("unknown key %v", key)
	}

	return json.Marshal(key.String())
}

// VarName - returns variable key name, such as "${aws:username}"
func (key Key) VarName() string {
	return key.name.VarName()
}

// Name - returns key name which is stripped value of prefixes "aws:" and "s3:"
func (key Key) Name() string {
	name := key.name.Name()
	if key.variable != "" {
		return name + "/" + key.variable
	}
	return name
}

// UnmarshalJSON - decodes JSON data to Key.
func (key *Key) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	parsedKey, err := parseKey(s)
	if err != nil {
		return err
	}

	*key = parsedKey
	return nil
}

func parseKey(s string) (Key, error) {
	name, variable := s, ""
	if strings.Contains(s, "/") {
		tokens := strings.SplitN(s, "/", 2)
		name, variable = tokens[0], tokens[1]
	}

	key := Key{
		name:     KeyName(name),
		variable: variable,
	}

	if key.IsValid() {
		return key, nil
	}

	return key, fmt.Errorf("invalid condition key '%v'", s)
}

// NewKey - creates new key
func NewKey(name KeyName, variable string) Key {
	return Key{
		name:     name,
		variable: variable,
	}
}

// KeySet - set representation of slice of keys.
type KeySet map[Key]struct{}

// Add - add a key to key set.
func (set KeySet) Add(key Key) {
	set[key] = struct{}{}
}

// Merge merges two key sets, duplicates are overwritten
func (set KeySet) Merge(mset KeySet) {
	for k, v := range mset {
		set[k] = v
	}
}

// Difference - returns a key set contains difference of two keys.
// Example:
//     keySet1 := ["one", "two", "three"]
//     keySet2 := ["two", "four", "three"]
//     keySet1.Difference(keySet2) == ["one"]
func (set KeySet) Difference(sset KeySet) KeySet {
	nset := make(KeySet)

	for k := range set {
		if _, ok := sset[k]; !ok {
			nset.Add(k)
		}
	}

	return nset
}

// IsEmpty - returns whether key set is empty or not.
func (set KeySet) IsEmpty() bool {
	return len(set) == 0
}

func (set KeySet) String() string {
	return fmt.Sprintf("%v", set.ToSlice())
}

// ToSlice - returns slice of keys.
func (set KeySet) ToSlice() []Key {
	keys := []Key{}

	for key := range set {
		keys = append(keys, key)
	}

	return keys
}

// NewKeySet - returns new KeySet contains given keys.
func NewKeySet(keys ...Key) KeySet {
	set := make(KeySet)
	for _, key := range keys {
		set.Add(key)
	}

	return set
}
