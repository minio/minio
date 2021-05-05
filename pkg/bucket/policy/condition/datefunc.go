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
	"time"
)

type dateFunc struct {
	n     name
	k     Key
	value time.Time
	c     condition
}

func (f dateFunc) evaluate(values map[string][]string) bool {
	rvalues := getValuesByKey(values, f.k.Name())
	if len(rvalues) == 0 {
		return false
	}
	t, err := time.Parse(time.RFC3339, rvalues[0])
	if err != nil {
		return false
	}

	switch f.c {
	case equals:
		return f.value.Equal(t)
	case notEquals:
		return !f.value.Equal(t)
	case greaterThan:
		return t.After(f.value)
	case greaterThanEquals:
		return t.After(f.value) || t.Equal(f.value)
	case lessThan:
		return t.Before(f.value)
	case lessThanEquals:
		return t.Before(f.value) || t.Equal(f.value)
	}

	// This never happens.
	return false
}

func (f dateFunc) key() Key {
	return f.k
}

func (f dateFunc) name() name {
	return f.n
}

func (f dateFunc) String() string {
	return fmt.Sprintf("%v:%v:%v", f.n, f.k, f.value.Format(time.RFC3339))
}

func (f dateFunc) toMap() map[Key]ValueSet {
	if !f.k.IsValid() {
		return nil
	}

	values := NewValueSet()
	values.Add(NewStringValue(f.value.Format(time.RFC3339)))

	return map[Key]ValueSet{
		f.k: values,
	}
}

func (f dateFunc) clone() Function {
	return &dateFunc{
		n:     f.n,
		k:     f.k,
		value: f.value,
		c:     f.c,
	}
}

func valueToTime(n string, values ValueSet) (v time.Time, err error) {
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

func newDateFunc(n string, key Key, values ValueSet, cond condition) (Function, error) {
	v, err := valueToTime(n, values)
	if err != nil {
		return nil, err
	}

	return &dateFunc{
		n:     name{name: n},
		k:     key,
		value: v,
		c:     cond,
	}, nil
}

// newDateEqualsFunc - returns new DateEquals function.
func newDateEqualsFunc(key Key, values ValueSet, qualifier string) (Function, error) {
	return newDateFunc(dateEquals, key, values, equals)
}

// NewDateEqualsFunc - returns new DateEquals function.
func NewDateEqualsFunc(key Key, value time.Time) (Function, error) {
	return &dateFunc{n: name{name: dateEquals}, k: key, value: value, c: equals}, nil
}

// newDateNotEqualsFunc - returns new DateNotEquals function.
func newDateNotEqualsFunc(key Key, values ValueSet, qualifier string) (Function, error) {
	return newDateFunc(dateNotEquals, key, values, notEquals)
}

// NewDateNotEqualsFunc - returns new DateNotEquals function.
func NewDateNotEqualsFunc(key Key, value time.Time) (Function, error) {
	return &dateFunc{n: name{name: dateNotEquals}, k: key, value: value, c: notEquals}, nil
}

// newDateGreaterThanFunc - returns new DateGreaterThan function.
func newDateGreaterThanFunc(key Key, values ValueSet, qualifier string) (Function, error) {
	return newDateFunc(dateGreaterThan, key, values, greaterThan)
}

// NewDateGreaterThanFunc - returns new DateGreaterThan function.
func NewDateGreaterThanFunc(key Key, value time.Time) (Function, error) {
	return &dateFunc{n: name{name: dateGreaterThan}, k: key, value: value, c: greaterThan}, nil
}

// newDateGreaterThanEqualsFunc - returns new DateGreaterThanEquals function.
func newDateGreaterThanEqualsFunc(key Key, values ValueSet, qualifier string) (Function, error) {
	return newDateFunc(dateGreaterThanEquals, key, values, greaterThanEquals)
}

// NewDateGreaterThanEqualsFunc - returns new DateGreaterThanEquals function.
func NewDateGreaterThanEqualsFunc(key Key, value time.Time) (Function, error) {
	return &dateFunc{n: name{name: dateGreaterThanEquals}, k: key, value: value, c: greaterThanEquals}, nil
}

// newDateLessThanFunc - returns new DateLessThan function.
func newDateLessThanFunc(key Key, values ValueSet, qualifier string) (Function, error) {
	return newDateFunc(dateLessThan, key, values, lessThan)
}

// NewDateLessThanFunc - returns new DateLessThan function.
func NewDateLessThanFunc(key Key, value time.Time) (Function, error) {
	return &dateFunc{n: name{name: dateLessThan}, k: key, value: value, c: lessThan}, nil
}

// newDateLessThanEqualsFunc - returns new DateLessThanEquals function.
func newDateLessThanEqualsFunc(key Key, values ValueSet, qualifier string) (Function, error) {
	return newDateFunc(dateLessThanEquals, key, values, lessThanEquals)
}

// NewDateLessThanEqualsFunc - returns new DateLessThanEquals function.
func NewDateLessThanEqualsFunc(key Key, value time.Time) (Function, error) {
	return &dateFunc{n: name{name: dateLessThanEquals}, k: key, value: value, c: lessThanEquals}, nil
}
