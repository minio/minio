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
	"fmt"
	"reflect"
	"strconv"
)

// booleanFunc - Bool condition function. It checks whether Key is true or false.
// https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_condition_operators.html#Conditions_Boolean
type booleanFunc struct {
	k     Key
	value string
}

// evaluate() - evaluates to check whether Key is present in given values or not.
// Depending on condition boolean value, this function returns true or false.
func (f booleanFunc) evaluate(values map[string][]string) bool {
	rvalues := getValuesByKey(values, f.k.Name())
	if len(rvalues) == 0 {
		return false
	}
	return f.value == rvalues[0]
}

// key() - returns condition key which is used by this condition function.
func (f booleanFunc) key() Key {
	return f.k
}

// name() - returns "Bool" condition name.
func (f booleanFunc) name() name {
	return name{name: boolean}
}

func (f booleanFunc) String() string {
	return fmt.Sprintf("%v:%v:%v", boolean, f.k, f.value)
}

// toMap - returns map representation of this function.
func (f booleanFunc) toMap() map[Key]ValueSet {
	if !f.k.IsValid() {
		return nil
	}

	return map[Key]ValueSet{
		f.k: NewValueSet(NewStringValue(f.value)),
	}
}

func (f booleanFunc) clone() Function {
	return &booleanFunc{
		k:     f.k,
		value: f.value,
	}
}

func newBooleanFunc(key Key, values ValueSet, qualifier string) (Function, error) {
	if key != AWSSecureTransport {
		return nil, fmt.Errorf("only %v key is allowed for %v condition", AWSSecureTransport, boolean)
	}

	if len(values) != 1 {
		return nil, fmt.Errorf("only one value is allowed for boolean condition")
	}

	var value Value
	for v := range values {
		value = v
		switch v.GetType() {
		case reflect.Bool:
			if _, err := v.GetBool(); err != nil {
				return nil, err
			}
		case reflect.String:
			s, err := v.GetString()
			if err != nil {
				return nil, err
			}
			if _, err = strconv.ParseBool(s); err != nil {
				return nil, fmt.Errorf("value must be a boolean string for boolean condition")
			}
		default:
			return nil, fmt.Errorf("value must be a boolean for boolean condition")
		}
	}

	return &booleanFunc{key, value.String()}, nil
}

// NewBoolFunc - returns new Bool function.
func NewBoolFunc(key Key, value bool) (Function, error) {
	return newBooleanFunc(key, NewValueSet(NewBoolValue(value)), "")
}
