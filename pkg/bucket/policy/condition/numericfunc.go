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

type numericFunc struct {
	n     name
	k     Key
	value int
	c     condition
}

func (f numericFunc) evaluate(values map[string][]string) bool {
	rvalues := getValuesByKey(values, f.k.Name())
	if len(rvalues) == 0 {
		return false
	}

	rv, err := strconv.Atoi(rvalues[0])
	if err != nil {
		return false
	}

	switch f.c {
	case equals:
		return rv == f.value
	case notEquals:
		return rv != f.value
	case greaterThan:
		return rv > f.value
	case greaterThanEquals:
		return rv >= f.value
	case lessThan:
		return rv < f.value
	case lessThanEquals:
		return rv <= f.value
	}

	// This never happens.
	return false
}

func (f numericFunc) key() Key {
	return f.k
}

func (f numericFunc) name() name {
	return f.n
}

func (f numericFunc) String() string {
	return fmt.Sprintf("%v:%v:%v", f.n, f.k, f.value)
}

func (f numericFunc) toMap() map[Key]ValueSet {
	if !f.k.IsValid() {
		return nil
	}

	values := NewValueSet()
	values.Add(NewIntValue(f.value))

	return map[Key]ValueSet{
		f.k: values,
	}
}

func (f numericFunc) clone() Function {
	return &numericFunc{
		n:     f.n,
		k:     f.k,
		value: f.value,
		c:     f.c,
	}
}

func valueToInt(n string, values ValueSet) (v int, err error) {
	if len(values) != 1 {
		return -1, fmt.Errorf("only one value is allowed for %s condition", n)
	}

	for vs := range values {
		switch vs.GetType() {
		case reflect.Int:
			if v, err = vs.GetInt(); err != nil {
				return -1, err
			}
		case reflect.String:
			s, err := vs.GetString()
			if err != nil {
				return -1, err
			}
			if v, err = strconv.Atoi(s); err != nil {
				return -1, fmt.Errorf("value %s must be a int for %s condition: %w", vs, n, err)
			}
		default:
			return -1, fmt.Errorf("value %s must be a int for %s condition", vs, n)
		}
	}

	return v, nil

}

func newNumericFunc(n string, key Key, values ValueSet, cond condition) (Function, error) {
	v, err := valueToInt(n, values)
	if err != nil {
		return nil, err
	}

	return &numericFunc{
		n:     name{name: n},
		k:     key,
		value: v,
		c:     cond,
	}, nil
}

// newNumericEqualsFunc - returns new NumericEquals function.
func newNumericEqualsFunc(key Key, values ValueSet, qualifier string) (Function, error) {
	return newNumericFunc(numericEquals, key, values, equals)
}

// NewNumericEqualsFunc - returns new NumericEquals function.
func NewNumericEqualsFunc(key Key, value int) (Function, error) {
	return &numericFunc{n: name{name: numericEquals}, k: key, value: value, c: equals}, nil
}

// newNumericNotEqualsFunc - returns new NumericNotEquals function.
func newNumericNotEqualsFunc(key Key, values ValueSet, qualifier string) (Function, error) {
	return newNumericFunc(numericNotEquals, key, values, notEquals)
}

// NewNumericNotEqualsFunc - returns new NumericNotEquals function.
func NewNumericNotEqualsFunc(key Key, value int) (Function, error) {
	return &numericFunc{n: name{name: numericNotEquals}, k: key, value: value, c: notEquals}, nil
}

// newNumericGreaterThanFunc - returns new NumericGreaterThan function.
func newNumericGreaterThanFunc(key Key, values ValueSet, qualifier string) (Function, error) {
	return newNumericFunc(numericGreaterThan, key, values, greaterThan)
}

// NewNumericGreaterThanFunc - returns new NumericGreaterThan function.
func NewNumericGreaterThanFunc(key Key, value int) (Function, error) {
	return &numericFunc{n: name{name: numericGreaterThan}, k: key, value: value, c: greaterThan}, nil
}

// newNumericGreaterThanEqualsFunc - returns new NumericGreaterThanEquals function.
func newNumericGreaterThanEqualsFunc(key Key, values ValueSet, qualifier string) (Function, error) {
	return newNumericFunc(numericGreaterThanEquals, key, values, greaterThanEquals)
}

// NewNumericGreaterThanEqualsFunc - returns new NumericGreaterThanEquals function.
func NewNumericGreaterThanEqualsFunc(key Key, value int) (Function, error) {
	return &numericFunc{n: name{name: numericGreaterThanEquals}, k: key, value: value, c: greaterThanEquals}, nil
}

// newNumericLessThanFunc - returns new NumericLessThan function.
func newNumericLessThanFunc(key Key, values ValueSet, qualifier string) (Function, error) {
	return newNumericFunc(numericLessThan, key, values, lessThan)
}

// NewNumericLessThanFunc - returns new NumericLessThan function.
func NewNumericLessThanFunc(key Key, value int) (Function, error) {
	return &numericFunc{n: name{name: numericLessThan}, k: key, value: value, c: lessThan}, nil
}

// newNumericLessThanEqualsFunc - returns new NumericLessThanEquals function.
func newNumericLessThanEqualsFunc(key Key, values ValueSet, qualifier string) (Function, error) {
	return newNumericFunc(numericLessThanEquals, key, values, lessThanEquals)
}

// NewNumericLessThanEqualsFunc - returns new NumericLessThanEquals function.
func NewNumericLessThanEqualsFunc(key Key, value int) (Function, error) {
	return &numericFunc{n: name{name: numericLessThanEquals}, k: key, value: value, c: lessThanEquals}, nil
}
