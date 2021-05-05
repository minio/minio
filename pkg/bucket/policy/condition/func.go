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
	"sort"
)

type condition int

const (
	equals condition = iota + 1
	notEquals
	greaterThan
	greaterThanEquals
	lessThan
	lessThanEquals
)

// Function - condition function interface.
type Function interface {
	// evaluate() - evaluates this condition function with given values.
	evaluate(values map[string][]string) bool

	// key() - returns condition key used in this function.
	key() Key

	// name() - returns condition name of this function.
	name() name

	// String() - returns string representation of function.
	String() string

	// toMap - returns map representation of this function.
	toMap() map[Key]ValueSet

	// clone - returns copy of this function.
	clone() Function
}

// Functions - list of functions.
type Functions []Function

// Evaluate - evaluates all functions with given values map. Each function is evaluated
// sequencely and next function is called only if current function succeeds.
func (functions Functions) Evaluate(values map[string][]string) bool {
	for _, f := range functions {
		if !f.evaluate(values) {
			return false
		}
	}

	return true
}

// Keys - returns list of keys used in all functions.
func (functions Functions) Keys() KeySet {
	keySet := NewKeySet()

	for _, f := range functions {
		keySet.Add(f.key())
	}

	return keySet
}

// Clone clones Functions structure
func (functions Functions) Clone() Functions {
	funcs := []Function{}
	for _, f := range functions {
		funcs = append(funcs, f.clone())
	}
	return funcs
}

// Equals returns true if two Functions structures are equal
func (functions Functions) Equals(funcs Functions) bool {
	if len(functions) != len(funcs) {
		return false
	}
	for _, fi := range functions {
		fistr := fi.String()
		found := false
		for _, fj := range funcs {
			if fistr == fj.String() {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// MarshalJSON - encodes Functions to JSON data.
func (functions Functions) MarshalJSON() ([]byte, error) {
	nm := make(map[string]map[Key]ValueSet)

	for _, f := range functions {
		fname := f.name().String()
		if _, ok := nm[fname]; ok {
			for k, v := range f.toMap() {
				nm[fname][k] = v
			}
		} else {
			nm[fname] = f.toMap()
		}
	}

	return json.Marshal(nm)
}

func (functions Functions) String() string {
	funcStrings := []string{}
	for _, f := range functions {
		s := fmt.Sprintf("%v", f)
		funcStrings = append(funcStrings, s)
	}
	sort.Strings(funcStrings)

	return fmt.Sprintf("%v", funcStrings)
}

var conditionFuncMap = map[string]func(Key, ValueSet, string) (Function, error){
	stringEquals:              newStringEqualsFunc,
	stringNotEquals:           newStringNotEqualsFunc,
	stringEqualsIgnoreCase:    newStringEqualsIgnoreCaseFunc,
	stringNotEqualsIgnoreCase: newStringNotEqualsIgnoreCaseFunc,
	binaryEquals:              newBinaryEqualsFunc,
	stringLike:                newStringLikeFunc,
	stringNotLike:             newStringNotLikeFunc,
	ipAddress:                 newIPAddressFunc,
	notIPAddress:              newNotIPAddressFunc,
	null:                      newNullFunc,
	boolean:                   newBooleanFunc,
	numericEquals:             newNumericEqualsFunc,
	numericNotEquals:          newNumericNotEqualsFunc,
	numericLessThan:           newNumericLessThanFunc,
	numericLessThanEquals:     newNumericLessThanEqualsFunc,
	numericGreaterThan:        newNumericGreaterThanFunc,
	numericGreaterThanEquals:  newNumericGreaterThanEqualsFunc,
	dateEquals:                newDateEqualsFunc,
	dateNotEquals:             newDateNotEqualsFunc,
	dateLessThan:              newDateLessThanFunc,
	dateLessThanEquals:        newDateLessThanEqualsFunc,
	dateGreaterThan:           newDateGreaterThanFunc,
	dateGreaterThanEquals:     newDateGreaterThanEqualsFunc,
	// Add new conditions here.
}

// UnmarshalJSON - decodes JSON data to Functions.
func (functions *Functions) UnmarshalJSON(data []byte) error {
	// As string kind, int kind then json.Unmarshaler is checked at
	// https://github.com/golang/go/blob/master/src/encoding/json/decode.go#L618
	// UnmarshalJSON() is not called for types extending string
	// see https://play.golang.org/p/HrSsKksHvrS, better way to do is
	// https://play.golang.org/p/y9ElWpBgVAB
	//
	// Due to this issue, name and Key types cannot be used as map keys below.
	nm := make(map[string]map[string]ValueSet)
	if err := json.Unmarshal(data, &nm); err != nil {
		return err
	}

	if len(nm) == 0 {
		return fmt.Errorf("condition must not be empty")
	}

	funcs := []Function{}
	for nameString, args := range nm {
		n, err := parseName(nameString)
		if err != nil {
			return err
		}

		for keyString, values := range args {
			key, err := parseKey(keyString)
			if err != nil {
				return err
			}

			fn, ok := conditionFuncMap[n.name]
			if !ok {
				return fmt.Errorf("condition %v is not handled", n)
			}

			f, err := fn(key, values, n.qualifier)
			if err != nil {
				return err
			}

			funcs = append(funcs, f)
		}
	}

	*functions = funcs

	return nil
}

// GobEncode - encodes Functions to gob data.
func (functions Functions) GobEncode() ([]byte, error) {
	return functions.MarshalJSON()
}

// GobDecode - decodes gob data to Functions.
func (functions *Functions) GobDecode(data []byte) error {
	return functions.UnmarshalJSON(data)
}

// NewFunctions - returns new Functions with given function list.
func NewFunctions(functions ...Function) Functions {
	return Functions(functions)
}
