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
	"net/http"
	"reflect"
	"time"
)

func toDateEqualsFuncString(n name, key Key, value time.Time) string {
	return fmt.Sprintf("%v:%v:%v", n, key, value.Format(time.RFC3339))
}

// dateEqualsFunc - String equals function. It checks whether value by Key in given
// values map is in condition values.
// For example,
//   - if values = ["mybucket/foo"], at evaluate() it returns whether string
//     in value map for Key is in values.
type dateEqualsFunc struct {
	k     Key
	value time.Time
}

// evaluate() - evaluates to check whether value by Key in given values is in
// condition values.
func (f dateEqualsFunc) evaluate(values map[string][]string) bool {
	requestValue, ok := values[http.CanonicalHeaderKey(f.k.Name())]
	if !ok {
		requestValue = values[f.k.Name()]
	}

	if len(requestValue) == 0 {
		return false
	}

	t, err := time.Parse(time.RFC3339, requestValue[0])
	if err != nil {
		return false
	}

	return f.value.Equal(t)
}

// key() - returns condition key which is used by this condition function.
func (f dateEqualsFunc) key() Key {
	return f.k
}

// name() - returns "DateEquals" condition name.
func (f dateEqualsFunc) name() name {
	return dateEquals
}

func (f dateEqualsFunc) String() string {
	return toDateEqualsFuncString(dateEquals, f.k, f.value)
}

// toMap - returns map representation of this function.
func (f dateEqualsFunc) toMap() map[Key]ValueSet {
	if !f.k.IsValid() {
		return nil
	}

	values := NewValueSet()
	values.Add(NewStringValue(f.value.Format(time.RFC3339)))

	return map[Key]ValueSet{
		f.k: values,
	}
}

// dateNotEqualsFunc - String not equals function. It checks whether value by Key in
// given values is NOT in condition values.
// For example,
//   - if values = ["mybucket/foo"], at evaluate() it returns whether string
//     in value map for Key is NOT in values.
type dateNotEqualsFunc struct {
	dateEqualsFunc
}

// evaluate() - evaluates to check whether value by Key in given values is NOT in
// condition values.
func (f dateNotEqualsFunc) evaluate(values map[string][]string) bool {
	return !f.dateEqualsFunc.evaluate(values)
}

// name() - returns "DateNotEquals" condition name.
func (f dateNotEqualsFunc) name() name {
	return dateNotEquals
}

func (f dateNotEqualsFunc) String() string {
	return toDateEqualsFuncString(dateNotEquals, f.dateEqualsFunc.k, f.dateEqualsFunc.value)
}

func valueToTime(n name, values ValueSet) (v time.Time, err error) {
	if len(values) != 1 {
		return v, fmt.Errorf("only one value is allowed for %s condition", n)
	}

	for vs := range values {
		switch vs.GetType() {
		case reflect.String:
			s, err := vs.GetString()
			if err != nil {
				return v, err
			}
			if v, err = time.Parse(time.RFC3339, s); err != nil {
				return v, fmt.Errorf("value %s must be a time.Time string for %s condition: %w", vs, n, err)
			}
		default:
			return v, fmt.Errorf("value %s must be a time.Time for %s condition", vs, n)
		}
	}

	return v, nil

}

// newDateEqualsFunc - returns new DateEquals function.
func newDateEqualsFunc(key Key, values ValueSet) (Function, error) {
	v, err := valueToTime(dateEquals, values)
	if err != nil {
		return nil, err
	}

	return NewDateEqualsFunc(key, v)
}

// NewDateEqualsFunc - returns new DateEquals function.
func NewDateEqualsFunc(key Key, value time.Time) (Function, error) {
	return &dateEqualsFunc{key, value}, nil
}

// newDateNotEqualsFunc - returns new DateNotEquals function.
func newDateNotEqualsFunc(key Key, values ValueSet) (Function, error) {
	v, err := valueToTime(dateNotEquals, values)
	if err != nil {
		return nil, err
	}

	return NewDateNotEqualsFunc(key, v)
}

// NewDateNotEqualsFunc - returns new DateNotEquals function.
func NewDateNotEqualsFunc(key Key, value time.Time) (Function, error) {
	return &dateNotEqualsFunc{dateEqualsFunc{key, value}}, nil
}
